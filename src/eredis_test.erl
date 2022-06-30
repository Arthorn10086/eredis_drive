-module(eredis_test).

%%%=======================STATEMENT====================
-description("eredis_test").
-author("arthorn").
-vsn("1.0").

%%%=======================EXPORT=======================
-compile(export_all).

%%%=======================INCLUDE======================

%%%=======================DEFINE======================

%%%=======================RECORD=======================

%%%=======================TYPE=========================
%%-type my_type() :: atom() | integer().


%%%=================EXPORTED FUNCTIONS=================
test_update() ->
    eredis_lib:update(corps, 1, test1),
    
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 1))]),
    eredis_lib:update(corps, 1, fun(_, _) ->
        {ok, test3}
    end, none, []),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 1))]),
    spawn(fun() ->
        eredis_lib:update(corps, 1, fun(_, _) ->
            timer:sleep(3000),
            {ok, ok, test4}
        end, none, [])
    end),
    timer:sleep(100),
    spawn(fun() ->
        eredis_lib:update(corps, 1, fun(_, _) ->
            timer:sleep(1000),
            {ok, ok, test2}
        end, none, [])
    end),
    timer:sleep(1000),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 1))]),
    timer:sleep(1000),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 1))]),
    timer:sleep(1000),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 1))]),
    timer:sleep(1000),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 1))]),
    timer:sleep(1000),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 1))]),
    timer:sleep(1000),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 1))]),
    ok.

transaction1() ->
    %%{ok,Reply}
    HandleF1 = fun(_, [V1, V2]) ->
        {ok, {V1, V2}}
    end,
    R1 = eredis_lib:transaction([{corps, 1, none}, {corps, 2, test}], HandleF1, [], 5000),
    io:format("R1:~p~n", [R1]).
transaction2()->
    %%{ok,Reply,OldV}
    HandleF2 = fun(_, [V1, V2]) ->
        {ok, nochange, [V1, V2]}
    end,
    R2 = eredis_lib:transaction([{corps, 1, none}, {corps, 2, test}], HandleF2, [], 5000),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 1))]),
    io:format("R2:~p~n", [R2]).
transaction3()->
    %%{ok,Reply,NewV}
    HandleF3 = fun(_, [_V1, V2]) ->
        {ok, v1_change, [test1, V2]}
    end,
    R3 = eredis_lib:transaction([{corps, 1, none}, {corps, 2, test}], HandleF3, [], 5000),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 1))]),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 2))]),
    io:format("R2:~p~n", [R3]).
transaction4()->
    %%{ok,Reply,NewV}
    HandleF4 = fun(_, [_V1, _V2]) ->
        {ok, v1_v2_change, [test1, test2]}
    end,
    R3 = eredis_lib:transaction([{corps, 1, none}, {corps, 2, test}], HandleF4, [], 5000),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 1))]),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 2))]),
    io:format("R2:~p~n", [R3]).

transaction5()->
    HandleF4 = fun(_, [_V1, _V2]) ->
        timer:sleep(3000),
        {ok, v1_v2_change, [test1, test2]}
    end,
   spawn(fun()->
       R3=eredis_lib:transaction([{corps, 1, none}, {corps, 2, test}], HandleF4, [], 5000),
       io:format("R3:~p~n", [R3])
       end),
    
    timer:sleep(100),
    spawn(fun() ->
        eredis_lib:update(corps, 1, fun(_, _) ->
            timer:sleep(1000),
            {ok, ok, test2}
        end, none, [])
    end),
    timer:sleep(1000),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 1))]),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 2))]),
    timer:sleep(1000),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 1))]),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 2))]),
    timer:sleep(1000),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 1))]),
    io:format("~p~n", [eredis_lib:getv(eredis_lib:redis_str_key(corps, 2))]).


%%%===================LOCAL FUNCTIONS==================
%% ----------------------------------------------------
%% @doc  
%%  
%% @end
%% ----------------------------------------------------
