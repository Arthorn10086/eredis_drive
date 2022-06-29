-module(eredis_pool).
-author("arthorn").

-behaviour(supervisor).

%% API
-export([start_single/1, start_cluster/0, start_balance/1]).
-export([start_link/1]).
-export([get_pool_name/1, start_child/1, delete_child/1]).
-export([get_pool/1, get_mode_info/0]).
-export([q/2, q/3, q_async/2, qp/2, qp/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(TIMEOUT, 5000).

%%%===================================================================
%%% API functions
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% 获得连接池
%% @end
%%--------------------------------------------------------------------
get_pool(Key) ->
    case get_mode_info() of
        [{_, 'single', PoolName}] ->
            PoolName;
        [{_, 'single_balance', PoolNum, Pools}] ->
            Index = erlang:phash(Key, PoolNum),
            lists:nth(Index, Pools);
        [{_, 'cluster'}] ->
            Slot = eredis_slot:slot(Key),
            eredis_monitor:get_pool_by_slot(Slot)
    end.
%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
get_pool_name(Index) ->
    list_to_atom("eredis_pool" ++ integer_to_list(Index)).

%%--------------------------------------------------------------------
%% @doc
%% 连接池管理
%% @end
%%--------------------------------------------------------------------
start_child(ChildSpec) ->
    supervisor:start_child(?MODULE, ChildSpec).
delete_child(PoolName) ->
    supervisor:terminate_child(?MODULE, PoolName),
    supervisor:delete_child(?MODULE, PoolName).

%%--------------------------------------------------------------------
%% @doc
%% 同步执行命令
%% @end
%%--------------------------------------------------------------------
q(PoolName, Command) ->
    q(PoolName, Command, ?TIMEOUT).
q(PoolName, Command, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        case gen_server:call(Worker, worker) of
            {ok, Pid} ->
                case eredis:q(Pid, Command, Timeout) of
                    {ok, Info} ->
                        Info;
                    {error, Error} ->
                        case catch binary:split(Error, <<" ">>) of
                            [<<"MOVED">>, _Slot, IPPORT] ->%%集群发生变动，客户端slots映射还未维护
                                [IP1, Port1] = binary:split(IPPORT, <<":">>),
                                q(eredis_monitor:get_pool_by_ipport(binary_to_list(IP1), binary_to_integer(Port1)), Command, Timeout);
                            _ ->%%TODO ASK 指令
                                {error, Error}
                        end
                end;
            Error ->
                Error
        end
    end).
%%--------------------------------------------------------------------
%% @doc
%% 异步执行命令
%% @end
%%--------------------------------------------------------------------
q_async(PoolName, Command) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        case gen_server:call(Worker, worker) of
            {ok, Pid} ->
                eredis:q_noreply(Pid, Command);
            Error ->
                Error
        end
    end).
%%--------------------------------------------------------------------
%% @doc
%%  Pipeline模式 目前只支持单节点
%% @end
%%--------------------------------------------------------------------
qp(PoolName, Pipeline) ->
    qp(PoolName, Pipeline, ?TIMEOUT).

qp(PoolName, Pipeline, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        case gen_server:call(Worker, worker) of
            {ok, Pid} ->
                eredis:qp(Pid, Pipeline, Timeout);
            Error ->
                Error
        end
    end).
%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
start_single({SizeArgs, WorkerArgs, Nodes}) ->
    PoolName = get_pool_name(1),
    set_mode_info({mode, single, PoolName}),
    PoolArgs = [{name, {local, PoolName}},
        {worker_module, eredis_conn}] ++ SizeArgs,
    start_link([poolboy:child_spec(PoolName, PoolArgs, [hd(Nodes) | WorkerArgs])]).
start_balance({SizeArgs, WorkerArgs, Nodes}) ->
    Num = length(Nodes),
    ChildSpecs = lists:map(fun(Index) ->
        PoolName = get_pool_name(Index),
        PoolArgs = [{name, {local, PoolName}},
            {worker_module, eredis_conn}] ++ SizeArgs,
        poolboy:child_spec(PoolName, PoolArgs, [lists:nth(Index, Nodes) | WorkerArgs])
    end, lists:seq(1, Num)),
    Pools = [element(1, Child) || Child <- ChildSpecs],
    set_mode_info({mode, single_balance, Num, Pools}),
    start_link(ChildSpecs).
start_cluster() ->
    set_mode_info({mode, cluster}),
    start_link([]).

start_link(ChildSpecs) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, ChildSpecs).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }} |
    ignore |
    {error, Reason :: term()}).
init(ChildSpecs) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 3,
    MaxSecondsBetweenRestarts = 10,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    {ok, {SupFlags, ChildSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
set_mode_info(ModeInfo) ->
    ets:insert(eredis_drive_sup, ModeInfo).

get_mode_info() ->
    ets:lookup(eredis_drive_sup, mode).