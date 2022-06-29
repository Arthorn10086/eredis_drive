-module(eredis_monitor).
-author("arthorn").

-behaviour(gen_server).

%% API
-export([start_link/1]).
-export([reset_cluster_nodes/1, get_pool_by_slot/1, get_pool_by_ipport/2, update_docker_mapping/1]).

-export([monitor_refresh/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(MONITOR_INTERVAL, 200).
-define(STATE_SUCESS, 'suc').
-define(STATE_FAIL, 'fail').
-define(FAIL_ALARM_INTERVAL, [5000, 60000, 300000, 600000, 1800000, 3600000]).
-define(FAIL_MSG_KEY, 'fail_msg').
-define(TRY_STOP(From, Pid), case From of 'ok' -> eredis:stop(Pid); _ -> poolboy:checkin(From, Pid) end).

-record(state, {pool_args, work_args, cluster_nodes = [], slot_mapping, indexs = [], alarm, docker_mapping, state}).
-record(cluster_node, {address, md5, master, start_slot = -1, end_slot = 0, pool_index = 0, pool = none}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

reset_cluster_nodes(NodesCfg) ->
    gen_server:call(?MODULE, {reload_cluster_nodes, NodesCfg}, 10000).

update_docker_mapping(Mapping) ->
    gen_server:call(?MODULE, {update_docker_mapping, Mapping}, 5000).
%% ----------------------------------------------------
%% @doc
%%       根据slot取pool
%% @end
%% ----------------------------------------------------
get_pool_by_slot(Slot) ->
    case ets:lookup('$eredis_slots_mapping', Slot) of
        [] ->
            throw({error, cluster_node_error});
        [{_, PoolName}] ->
            PoolName
    end.
%% ----------------------------------------------------
%% @doc
%%       根据ipport取pool
%% @end
%% ----------------------------------------------------
get_pool_by_ipport(Ip, Port) ->
    get_pool_by_ipport(Ip, Port, true).
get_pool_by_ipport(Ip, Port, Refresh) ->
    gen_server:call(?MODULE, {get_pool_by_ipport, {Ip, Port, Refresh}}, 5000).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init({{PoolArgs, WorkArgs, NodeCfgs, AlarmMFA}, DockerMapping}) ->
    Nodes = parse_args(NodeCfgs, []),
    Ets = ets:new('$eredis_slots_mapping', [named_table, set, public, {read_concurrency, true}]),
    State = #state{pool_args = PoolArgs, work_args = WorkArgs, slot_mapping = Ets, alarm = AlarmMFA, docker_mapping = DockerMapping},
    State1 = load_cluster(State, Nodes),
    State2 = create_all_pools(State1, State1#state.cluster_nodes, 1),
    create_slot_mapping(State2),
    erlang:send_after(?MONITOR_INTERVAL*30, self(), 'monitor_refresh'),
    {ok, State2#state{state = 'suc'}}.

handle_call({'reload_cluster_nodes', NodesCfg}, _From, State) ->
    Nodes = parse_args(NodesCfg, []),
    Nodes1 = lists:map(fun(Node) ->
        case Node#cluster_node.pool of
            none ->
                Node;
            PoolName ->
                eredis_pool:delete_child(PoolName),
                Node#cluster_node{pool = none, pool_index = 0, start_slot = 0, end_slot = 0}
        end
    end, Nodes),
    State1 = load_cluster(State, Nodes),
    State2 = create_all_pools(State1#state{cluster_nodes = Nodes1, indexs = []}, State1#state.cluster_nodes, 1),
    create_slot_mapping(State2),
    {reply, ok, State1};
handle_call({get_pool_by_ipport, {Ip, Port, Refresh}}, _From, State) ->
    State1 = if
        Refresh ->
            monitor_refresh(State);
        true ->
            State
    end,
    #state{cluster_nodes = ClusterNode} = State1,
    Node = lists:keyfind({Ip, Port}, #cluster_node.address, ClusterNode),
    {reply, Node#cluster_node.pool, State1};
handle_call({update_docker_mapping, Mapping}, _From, State) ->
    {reply, ok, State#state{docker_mapping = Mapping}};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast(_Request, State) ->
    {noreply, State}.

handle_info('monitor_refresh', State) ->
    erlang:send_after(?MONITOR_INTERVAL, self(), 'monitor_refresh'),
    State1 = monitor_refresh(State),
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
parse_args([], R) ->
    lists:reverse(R);
parse_args([NodeCfg | T], R) ->
    Host = proplists:get_value(host, NodeCfg, "127.0.0.1"),
    Port = proplists:get_value(port, NodeCfg, 6379),
    R1 = [#cluster_node{address = {Host, Port}} | R],
    parse_args(T, R1).


%% ----------------------------------------------------
%% @doc
%%       加载集群信息
%% @end
%% ----------------------------------------------------
load_cluster(_, []) ->
    throw({?MODULE, ?FUNCTION_NAME});
load_cluster(State, [ClusterNode | T]) ->
    {Host, Port} = ClusterNode#cluster_node.address,
    PReply = case ClusterNode#cluster_node.pool of
        none ->
            {Host, Port} = ClusterNode#cluster_node.address,
            eredis:start_link([{host, Host}, {port, Port} | State#state.work_args]);
        PoolName ->
            {PoolName, poolboy:checkout(PoolName)}
    end,
    case PReply of
        {From, Pid} when is_pid(Pid) ->
            case eredis:q(Pid, ["CLUSTER", "INFO"]) of
                {ok, ClusterInfo} ->
                    case hd(binary:split(ClusterInfo, <<"\r\n">>)) of
                        <<"cluster_state:ok">> ->
                            case eredis:q(Pid, ["CLUSTER", "SLOTS"]) of
                                {ok, ClusterSlots} ->
                                    ?TRY_STOP(From, Pid),
                                    ClusterNodes = parse_cluster_slot(ClusterSlots, State#state.docker_mapping),
                                    case State#state.state of
                                        ?STATE_FAIL ->
                                            send_alarm(State, ClusterInfo),
                                            lager:error("cluster_info_fail_to_suc,load_num:~p,cluster_info:~p", [erlang:erase(?FAIL_MSG_KEY), ClusterInfo]),
                                            State#state{cluster_nodes = ClusterNodes, state = ?STATE_SUCESS};
                                        _ ->
                                            State#state{cluster_nodes = ClusterNodes}
                                    end;
                                _ ->
                                    ?TRY_STOP(From, Pid),
                                    load_cluster(State, T)
                            end;
                        <<"cluster_state:fail">> ->%%Fail状态，集群故障
                            ?TRY_STOP(From, Pid),
                            cluster_fail(State, ClusterInfo)
                    end;
                _ ->
                    ?TRY_STOP(From, Pid),
                    load_cluster(State, T)
            end;
        _ ->
            load_cluster(State, T)
    end.
%% ----------------------------------------------------
%% @doc
%%        集群故障处理
%% @end
%% ----------------------------------------------------
cluster_fail(State, ClusterInfo) ->
    {NewState, FailCount} = case State#state.state of
        ?STATE_SUCESS ->
            lager:error("cluster_info_fail,cluster_info:~p", [ClusterInfo]),
            erlang:put(?FAIL_MSG_KEY, 1),
            {State#state{state = ?STATE_FAIL}, 1};
        _ ->
            Count = erlang:get(?FAIL_MSG_KEY),
            erlang:put(?FAIL_MSG_KEY, Count + 1),
            {State, Count + 1}
    end,
    case lists:member(FailCount * ?MONITOR_INTERVAL, ?FAIL_ALARM_INTERVAL) of
        true ->
            %%监控报警
            send_alarm(State, ClusterInfo);
        false ->
            ok
    end,
    NewState.
%% ----------------------------------------------------
%% @doc
%%        发送监控通知
%% @end
%% ----------------------------------------------------
send_alarm(State, ClusterInfo) ->
    case State#state.alarm of
        {M, F, A} ->
            spawn(fun() -> M:F(A, ClusterInfo) end);
        _ ->
            ok
    end.
%% ----------------------------------------------------
%% @doc
%%       解析集群节点信息
%% @end
%% ----------------------------------------------------
parse_cluster_slot(ClusterSlots, DockerMapping) ->
    lists:foldl(fun([SSlot1, ESlot1, [Host, Port, NodeMd5] | SlaveInfos], R) ->
        SSlot = binary_to_integer(SSlot1),
        ESlot = binary_to_integer(ESlot1),
        MasterAddress = mapping_address(Host, Port, DockerMapping),
        Master = #cluster_node{address = MasterAddress, md5 = binary_to_list(NodeMd5), master = oneself, start_slot = SSlot, end_slot = ESlot},
        Slaves = [#cluster_node{address = mapping_address(SHost, SPort, DockerMapping), md5 = binary_to_list(SNodeMd5), master = binary_to_list(NodeMd5), start_slot = SSlot, end_slot = ESlot} || [SHost, SPort, SNodeMd5] <- SlaveInfos],
        [Master | Slaves] ++ R
    end, [], ClusterSlots).
%% ----------------------------------------------------
%% @doc
%%
%% @end
%% ----------------------------------------------------
mapping_address(Host, Port, DockerMappingt) ->
    H1 = binary_to_list(Host),
    P1 = binary_to_integer(Port),
    case lists:keyfind(H1, 1, DockerMappingt) of
        {_, Address} ->
            Address;
        _ ->
            {H1, P1}
    end.
%% ----------------------------------------------------
%% @doc
%%        生成集群连接池
%% @end
%% ----------------------------------------------------
create_all_pools(State, [], _) ->
    State;
create_all_pools(State, [ClusterNode | T], Index) ->
    case ClusterNode#cluster_node.master of
        'oneself' ->
            State1 = create_pool(State, ClusterNode, Index),
            create_all_pools(State1, T, Index + 1);
        _ ->
            create_all_pools(State, T, Index)
    end.

create_pool(State, ClusterNode, Index) ->
    #state{cluster_nodes = Nodes, indexs = Indexs, pool_args = PoolArgs, work_args = WorkArgs} = State,
    PoolName = eredis_pool:get_pool_name(Index),
    {Ip, Port} = ClusterNode#cluster_node.address,
    ChildSpec = poolboy:child_spec(PoolName, [{name, {local, PoolName}} | PoolArgs], [{host, Ip}, {port, Port}] ++ WorkArgs),
    {ok, _Child} = eredis_pool:start_child(ChildSpec),
    Node1 = ClusterNode#cluster_node{pool_index = Index, pool = PoolName},
    Nodes1 = lists:keyreplace({Ip, Port}, #cluster_node.address, Nodes, Node1),
    State#state{cluster_nodes = Nodes1, indexs = [Index | Indexs]}.

%% ----------------------------------------------------
%% @doc
%%        创建slot映射
%% @end
%% ----------------------------------------------------
create_slot_mapping(State1) ->
    Mapping = State1#state.slot_mapping,
    Nodes = State1#state.cluster_nodes,
    lists:foreach(fun(Node) ->
        case Node#cluster_node.master of
            'oneself' ->
                SSlot = Node#cluster_node.start_slot,
                ESlot = Node#cluster_node.end_slot,
                PoolName = Node#cluster_node.pool,
                Maps = [{Slot, PoolName} || Slot <- lists:seq(SSlot, ESlot)],
                ets:insert(Mapping, Maps);
            _ ->
                ok
        end
    end, Nodes),
    State1.

%% ----------------------------------------------------
%% @doc
%%        刷新最新的集群信息
%% @end
%% ----------------------------------------------------
monitor_refresh(State) ->
    Nodes = State#state.cluster_nodes,
    %%加载集群信息
    State1 = load_cluster(State, Nodes),
    ClusterNodes = State1#state.cluster_nodes,
    %%检查移除，新增节点
    {RemoveNodes, AddNodes} = lists:foldl(fun(Node, {ReMove, Add}) ->
        case lists:member(Node#cluster_node{pool = none, pool_index = 0}, Add) of
            true ->
                {ReMove, lists:keydelete(Node#cluster_node.address, #cluster_node.address, Add)};
            false ->
                {[Node | ReMove], Add}
        end
    end, {[], ClusterNodes}, Nodes),
    %%移除的主节点删除连接池
    Nodes1 = lists:foldl(fun(Node, R) ->
        case Node#cluster_node.pool of
            none ->
                ok;
            PoolName ->
                eredis_pool:delete_child(PoolName)
        end,
        lists:keydelete(Node#cluster_node.address, #cluster_node.address, R)
    end, Nodes, RemoveNodes),
    RemovePools = [CN#cluster_node.pool_index || CN <- RemoveNodes],
    Indexs = State#state.indexs--RemovePools,
    %%新增主节点创建连接池
    AddMainNodes = [AN || AN <- AddNodes, AN#cluster_node.master =:= 'oneself'],
    AddNum = length(AddMainNodes),
    AddIndexs = lists:sublist(lists:seq(1, length(Indexs) + AddNum)--Indexs, AddNum),
    State2 = lists:foldl(fun({I, N}, S) ->
        create_pool(S, N, I)
    end, State1#state{cluster_nodes = AddNodes ++ Nodes1}, lists:zip(AddIndexs, AddMainNodes)),
    NIndex2 = [N#cluster_node.pool_index || N <- State2#state.cluster_nodes, N#cluster_node.pool_index =/= 0],
    State3 = State2#state{indexs = NIndex2},
    %%更新slot信息
    create_slot_mapping(State3).
