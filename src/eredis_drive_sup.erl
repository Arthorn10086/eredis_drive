%%%-------------------------------------------------------------------
%% @doc eredis_drive top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(eredis_drive_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
%%工作模式
-define(SINGLE, single).
-define(CLUSTER, cluster).
%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: #{id => Id, start => {M, F, A}}
%% Optional keys are restart, shutdown, type, modules.
%% Before OTP 18 tuples must be used to specify a child. e.g.
%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    {ok, Pools} = application:get_env(eredis_drive, pools),
    {ok, Mode} = application:get_env(eredis_drive, mode),
    {ok, DockerMapping} = application:get_env(eredis_drive, docker_mapping),
    ets:new(?MODULE, [named_table, set, public, {read_concurrency, true}]),
    StartSpec = case Mode of
        ?CLUSTER ->
            Pool = {eredis_pool, {eredis_pool, start_cluster, []},
                permanent, 5000, worker, [eredis_pool]},
            Monitor = {eredis_monitor, {eredis_monitor, start_link, [{Pools, DockerMapping}]},
                permanent, 5000, worker, [eredis_monitor]},
            [Pool, Monitor];
        _ ->
            [{eredis_pool, {eredis_pool, start_single, [Pools]},
                permanent, 5000, worker, [eredis_pool]}]
    end,
    {ok, {{one_for_all, 10, 10}, StartSpec}}.

%%====================================================================
%% Internal functions
%%====================================================================
