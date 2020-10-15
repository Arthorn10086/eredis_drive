-module(eredis_pool).
-author("arthorn").

-behaviour(supervisor).

%% API
-export([start_single/1, start_cluster/0]).
-export([start_link/1]).
-export([get_pool_name/1, start_child/1, delete_child/1]).

-export([q/2, q/3, q_async/2, qp/2, qp/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(TIMEOUT, 5000).

%%%===================================================================
%%% API functions
%%%===================================================================
get_pool_name(Index) ->
    list_to_atom("eredis_pool" ++ integer_to_list(Index)).

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
                eredis:q(Pid, Command, Timeout);
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
%%  Pipeline模式
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
    PoolArgs = [{name, {local, get_pool_name(1)}},
        {worker_module, eredis_conn}] ++ SizeArgs,
    start_link([poolboy:child_spec(get_pool_name(1), PoolArgs, [hd(Nodes) | WorkerArgs])]).

start_cluster() ->
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