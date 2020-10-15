-module(eredis_slot).

%%%=======================STATEMENT====================
-description("eredis_slot").
-author("arthorn").
%%%=======================EXPORT=======================
-export([slot/1]).
-export([get_crc16/1, getCrcValue/2]).

%%%=======================INCLUDE======================

%%%=======================DEFINE======================
-define(REDIS_CLUSTER_HASH_SLOTS, 16384).
%%%=================EXPORTED FUNCTIONS=================
%% ----------------------------------------------------
%% @doc
%%        获取key对应slot
%% @end
%% ----------------------------------------------------
slot(Key)->
    get_crc16(Key) rem ?REDIS_CLUSTER_HASH_SLOTS.

%% ----------------------------------------------------
%% @doc
%%       crc16校验码
%% @end
%% ----------------------------------------------------
get_crc16(V) ->
    getCrcValue(V, 16#FFFF).

getCrcValue([H | T], R) ->
    Ch = H band 16#FF,
    R1 = R bxor Ch,
    R2 = transform(R1),
    getCrcValue(T, R2);
getCrcValue([], R) ->
    R.


%%%===================LOCAL FUNCTIONS==================
for(Max, Max, F, R) -> F(R);

for(I, Max, F, R) -> for(I + 1, Max, F, F(R)).

transform(R1) ->
    for(0, 7, fun(R) ->
        begin
            Flag = R band 16#1,
            if Flag =/= 0 -> (R bsr 1) bxor 16#A001;
                true -> R bsr 1
            end
        end
    end, R1).