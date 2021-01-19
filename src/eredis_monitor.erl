-module(eredis_monitor).
-author("arthorn").

-behaviour(gen_server).

%% API
-export([start_link/1]).
-export([reset_cluster_nodes/1, get_pool_by_slot/1,get_pool_by_ipport/2]).

-export([monitor_refresh/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {pool_args, work_args, cluster_nodes = [], slot_mapping, indexs = [], alarm}).
-record(cluster_node, {address, md5, master, start_slot = -1, end_slot = 0, pool_index = 0, pool = none}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

reset_cluster_nodes(NodesCfg) ->
    gen_server:call(?MODULE, {reload_cluster_nodes, NodesCfg}, 10000).

get_pool_by_slot(Slot) ->
    case ets:lookup('$eredis_slots_mapping', Slot) of
        [] ->
            throw({error, cluster_node_error});
        [{_, PoolName}] ->
            PoolName
    end.

get_pool_by_ipport(Ip, Port) ->
    gen_server:call(?MODULE, {get_pool_by_ipport, {Ip, Port}}, 5000).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init({PoolArgs, WorkArgs, NodeCfgs, AlarmMFA}) ->
    Nodes = parse_args(NodeCfgs, []),
    Ets = ets:new('$eredis_slots_mapping', [named_table, set, public, {read_concurrency, true}]),
    ClusterNodes = load_cluster(WorkArgs, Nodes, AlarmMFA),
    State = #state{pool_args = PoolArgs, work_args = WorkArgs, cluster_nodes = ClusterNodes, slot_mapping = Ets, alarm = AlarmMFA},
    State1 = create_all_pools(State, ClusterNodes, 1),
    create_slot_mapping(State1),
    erlang:send_after(1000, self(), 'monitor_refresh'),
    {ok, State1}.

handle_call({reload_cluster_nodes, NodesCfg}, _From, State) ->
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
    ClusterNodes = load_cluster(State#state.work_args, Nodes, State#state.alarm),
    State1 = create_all_pools(State#state{cluster_nodes = Nodes1, indexs = []}, ClusterNodes, 1),
    create_slot_mapping(State1),
    {reply, ok, State1};
handle_call({get_pool_by_ipport, {Ip, Port}}, _From, #state{cluster_nodes = ClusterNode} = State) ->
    Node = lists:keyfind({Ip, Port}, #cluster_node.address, ClusterNode),
    {reply, Node#cluster_node.pool, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(monitor_refresh, State) ->
    State1 = monitor_refresh(State),
    erlang:send_after(1000, self(), 'monitor_refresh'),
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
load_cluster(_, [], _AlarmMFA) ->
    throw({?MODULE, ?FUNCTION_NAME});
load_cluster(WorkerArgs, [ClusterNode | T], AlarmMFA) ->
    {Host, Port} = ClusterNode#cluster_node.address,
    case eredis:start_link([{host, Host}, {port, Port} | WorkerArgs]) of
        {ok, Pid} ->
            case eredis:q(Pid, ["CLUSTER", "INFO"]) of
                {ok, ClusterInfo} ->
                    case hd(binary:split(ClusterInfo, <<"\r\n">>, [global])) of
                        <<"cluster_state:ok">> ->
                            case eredis:q(Pid, ["CLUSTER", "SLOTS"]) of
                                {ok, ClusterSlots} ->
                                    eredis:stop(Pid),
                                    parse_cluster_slot(ClusterSlots);
                                _ ->
                                    eredis:stop(Pid),
                                    load_cluster(WorkerArgs, T, AlarmMFA)
                            end;
                        _ ->
                            %%监控报警
                            case AlarmMFA of
                                {M, F, A} ->
                                    spawn(fun() -> M:F(A, ClusterInfo) end);
                                _ ->
                                    ok
                            end,
                            lager:log(error, self(), ClusterInfo),
                            throw({?MODULE, cluster_state_fail})
                    end;
                _ ->
                    eredis:stop(Pid),
                    load_cluster(WorkerArgs, T, AlarmMFA)
            end;
        _ ->
            load_cluster(WorkerArgs, T, AlarmMFA)
    end.


parse_cluster_slot(ClusterSlots) ->
    lists:foldl(fun([SSlot1, ESlot1, [Host, Port, NodeMd5] | SlaveInfos], R) ->
        SSlot = binary_to_integer(SSlot1),
        ESlot = binary_to_integer(ESlot1),
        Master = #cluster_node{address = {binary_to_list(Host), binary_to_integer(Port)}, md5 = binary_to_list(NodeMd5), master = oneself, start_slot = SSlot, end_slot = ESlot},
        Slaves = [#cluster_node{address = {binary_to_list(SHost), binary_to_integer(SPort)}, md5 = binary_to_list(SNodeMd5), master = binary_to_list(NodeMd5), start_slot = SSlot, end_slot = ESlot} || [SHost, SPort, SNodeMd5] <- SlaveInfos],
        [Master | Slaves] ++ R
    end, [], ClusterSlots).
%% ----------------------------------------------------
%% @doc
%%        生成集群连接池
%% @end
%% ----------------------------------------------------
create_all_pools(State, [], _) ->
    State;
create_all_pools(State, [ClusterNode | T], Index) ->
    State1 = case ClusterNode#cluster_node.master of
        'oneself' ->
            create_pool(State, ClusterNode, Index);
        _ ->
            State
    end,
    create_all_pools(State1, T, Index + 1).

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


monitor_refresh(State) ->
    Nodes = State#state.cluster_nodes,
    ClusterNodes = load_cluster(State#state.work_args, Nodes, State#state.alarm),
    ets:insert(State#state.slot_mapping, [{new_node, ClusterNodes}, {old_node, Nodes}]),

    {RemoveNodes, AddNodes} = lists:foldl(fun(Node, {ReMove, Add}) ->
        case lists:member(Node#cluster_node{pool = none, pool_index = 0}, Add) of
            true ->
                {ReMove, lists:keydelete(Node#cluster_node.address, #cluster_node.address, Add)};
            false ->
                {[Node | ReMove], Add}
        end
    end, {[], ClusterNodes}, Nodes),
    ets:insert(State#state.slot_mapping, [{rnode, RemoveNodes}, {anode, AddNodes}]),
    Nodes1 = lists:foldl(fun(Node, R) ->
        case Node#cluster_node.pool of
            none ->
                ok;
            PoolName ->
                eredis_pool:delete_child(PoolName)
        end,
        lists:keydelete(Node#cluster_node.address, #cluster_node.address, R)
    end, Nodes, RemoveNodes),
    ets:insert(State#state.slot_mapping, [{node1, Nodes1}]),
    Indexs = State#state.indexs,
    AddNum = length(AddNodes),
    AddIndexs = lists:sublist(lists:seq(1, length(Indexs) + AddNum)--Indexs, AddNum),
    State1 = lists:foldl(fun({I, N}, S) ->
        create_pool(S, N, I)
    end, State#state{cluster_nodes = Nodes1}, lists:zip(AddIndexs, AddNodes)),
    NIndex2 = [N#cluster_node.pool_index || N <- State1#state.cluster_nodes, N#cluster_node.pool_index =/= 0],
    State2 = State1#state{indexs = NIndex2},
    create_slot_mapping(State2).
