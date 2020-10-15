-module(eredis_monitor).
-author("arthorn").

-behaviour(gen_server).

%% API
-export([start_link/1]).
-export([reset_cluster_nodes/0, get_pool_by_slot/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {pool_args, work_args, cluster_nodes = [], slot_mapping, indexs = []}).
-record(cluster_node, {address, nodecfg, start_slot = 0, end_slot = 0, pool_index = 0, pool = none}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

reset_cluster_nodes() ->
    gen_server:call(?MODULE, {reload_cluster_nodes}, 10000).

get_pool_by_slot(Slot) ->
    case ets:lookup('$eredis_slots_mapping', Slot) of
        [] ->
            throw({error, cluster_node_error});
        [{_, PoolName}] ->
            PoolName
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init({PoolArgs, WorkArgs, NodeCfgs}) ->
    Nodes = parse_args(NodeCfgs, []),
    Ets = ets:new('$eredis_slots_mapping', [named_table, set, protected, {read_concurrency, true}]),
    ClusterNodes = load_cluster(WorkArgs, Nodes),
    State = #state{pool_args = PoolArgs, work_args = WorkArgs, cluster_nodes = Nodes, slot_mapping = Ets},
    State1 = create_all_pools(State, ClusterNodes, 1),
    create_slot_mapping(State1),
    erlang:send_after(10000, self(), 'monitor_refresh'),
    {ok, State1}.

handle_call({reload_cluster_nodes}, _From, State) ->
    Nodes = State#state.cluster_nodes,
    Nodes1 = lists:map(fun(Node) ->
        case Node#cluster_node.pool of
            none ->
                Node;
            PoolName ->
                eredis_pool:delete_child(PoolName),
                Node#cluster_node{pool = none, pool_index = 0, start_slot = 0, end_slot = 0}
        end
    end, Nodes),
    ClusterNodes = load_cluster(State#state.work_args, Nodes),
    State1 = create_all_pools(State#state{cluster_nodes = Nodes1, indexs = []}, ClusterNodes, 1),
    create_slot_mapping(State1),
    {reply, ok, State1};
handle_call(monitor_refresh, _From, State) ->
%%    State1 = monitor_refresh(State),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast(_Request, State) ->
    {noreply, State}.


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
    Host = proplists:get_value(ip, NodeCfg, "127.0.0.1"),
    Port = proplists:get_value(port, NodeCfg, 6379),
    R1 = [#cluster_node{address = {Host, Port}, nodecfg = NodeCfg} | R],
    parse_args(T, R1).


%% ----------------------------------------------------
%% @doc
%%       加载集群信息
%% @end
%% ----------------------------------------------------
load_cluster(_, []) ->
    throw({?MODULE, ?FUNCTION_NAME});
load_cluster(WorkerArgs, [ClusterNode | T]) ->
    {Host, Port} = ClusterNode#cluster_node.address,
    Database = proplists:get_value(database, WorkerArgs, 0),
    Password = proplists:get_value(password, WorkerArgs, ""),
    case eredis:start_link(Host, Port, Database, Password) of
        {ok, Pid} ->
            case eredis:q(Pid, ["CLUSTER", "SLOTS"]) of
                {ok, ClusterInfo} ->
                    eredis:stop(Pid),
                    parse_cluster_info(ClusterInfo);
                _ ->
                    eredis:stop(Pid),
                    load_cluster(WorkerArgs, T)
            end;
        _ ->
            load_cluster(WorkerArgs, T)
    end.


parse_cluster_info(ClusterInfo) ->
    lists:map(fun([SSlot, ESlot, [Address, Port | _] | _]) ->
        {binary_to_integer(SSlot), binary_to_integer(ESlot), binary_to_list(Address), binary_to_integer(Port)}
    end, ClusterInfo).
%% ----------------------------------------------------
%% @doc
%%        生成集群连接池
%% @end
%% ----------------------------------------------------
create_all_pools(State, [], _) ->
    State;
create_all_pools(State, [ClusterNode | T], Index) ->
    State1 = create_pool(State, ClusterNode, Index),
    create_all_pools(State1, T, Index + 1).

create_pool(State, ClusterNode, Index) ->
    #state{cluster_nodes = Nodes, indexs = Indexs, pool_args = PoolArgs, work_args = WorkArgs} = State,
    {SSlot, ESlot, Ip, Port} = ClusterNode,
    Node = case lists:keyfind({Ip, Port}, 2, Nodes) of
        false ->
            #cluster_node{address = {Ip, Port}, nodecfg = [{ip, Ip}, {port, Port}]};
        V ->
            V
    end,
    PoolName = eredis_pool:get_pool_name(Index),
    NodeCfg = Node#cluster_node.nodecfg,
    ChildSpec = poolboy:child_spec(PoolName, [{name, {local, PoolName}} | PoolArgs], NodeCfg ++ WorkArgs),
    {ok, _Child} = eredis_pool:start_child(ChildSpec),
    Node1 = Node#cluster_node{pool_index = Index, pool = PoolName, start_slot = SSlot, end_slot = ESlot},
    Nodes1 = lists:keyreplace({Ip, Port}, 2, Nodes, Node1),
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
        case Node#cluster_node.start_slot of
            0 ->
                ok;
            SSlot ->
                ESlot = Node#cluster_node.end_slot,
                PoolName = Node#cluster_node.pool,
                Maps = [{Slot, PoolName} || Slot <- lists:seq(SSlot, ESlot)],
                ets:insert(Mapping, Maps)
        end
    end, Nodes),
    State1.
