%%%-------------------------------------------------------------------
%% @doc eredis_drive public API
%% @end
%%%-------------------------------------------------------------------

-module(eredis_drive_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    eredis_drive_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
