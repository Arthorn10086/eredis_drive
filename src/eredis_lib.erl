-module(eredis_lib).

%%%=======================STATEMENT====================
-description("eredis_lib").
-author("arthorn").

%%%=======================EXPORT=======================
-export([sync_command/3, sync_command/4, async_command/3]).
-export([lock/1, lock/2, lock/3, lock/4, unlock/1]).
-export([getv/2, getv/1, gets/1, mget/2]).
-export([delete/2, delete/1, delete1/1]).
-export([dirty_write/2, dirty_write/3, dirty_write/4]).
-export([update/3, update/5, update/6]).
-export([transaction/4]).
-export([redis_str_key/1, redis_str_key/2]).
-export([redis_lock_key/1, redis_lock_key/2]).
-export([term_to_list/1]).
%%%=======================INCLUDE======================

%%%=======================DEFINE======================
-define(TIMEOUT, 5000).
-define(LOCKTIME, 5).
-define(LOCK_SLEEP_TIME, 50).
-define(DEF_VALUE, none).
-define(IGNORE_VALUE, '$ignore').
%%%=======================RECORD=======================

%%%=======================TYPE=========================
%%-type my_type() :: atom() | integer().


%%%=================EXPORTED FUNCTIONS=================
%% ----------------------------------------------------
%% @doc
%%       同步执行命令
%% @end
%% ----------------------------------------------------
sync_command(PoolName, Order, Params) ->
    sync_command(PoolName, Order, Params, ?TIMEOUT).
sync_command(PoolName, Order, Params, TimeOut) ->
    eredis_pool:q(PoolName, [Order | Params], TimeOut).
%% ----------------------------------------------------
%% @doc
%%       异步执行命令
%% @end
%% ----------------------------------------------------
async_command(PoolName, Order, Params) ->
    eredis_pool:q_async(PoolName, [Order | Params]).
%% ----------------------------------------------------
%% @doc
%%       获取 str类型的值
%%       Table 是逻辑表   key是键值   在redis中  组装成  "Table:Key"
%% @end
%% ----------------------------------------------------
getv({Table, Key}) ->
    getv(redis_str_key(Table, Key), 'none');
getv(TableKey) ->
    getv(TableKey, 'none').
getv(TableKey, Def) ->
    case sync_command(eredis_pool:get_pool(TableKey), "GET", [TableKey]) of
        {error, Err} ->
            throw(Err);
        {ok, undefined} ->
            Def;
        {ok, B} ->
            B
    end.

delete(TableKey) ->
    delete(eredis_pool:get_pool(TableKey), TableKey).
delete(Pool, TableKey) ->
    sync_command(Pool, "DEL", [TableKey]).
delete1(TableKey) ->
    Pool = eredis_pool:get_pool(TableKey),
    R = getv(Pool, TableKey),
    delete(Pool, TableKey),
    R.
%% ----------------------------------------------------
%% @doc
%%       获取 str类型的值
%% @end
%% ----------------------------------------------------
gets(TableKeyDefs) ->
    %%获取keys slot映射关系，遍历出所有节点
    {_, NodeKeys} = lists:foldl(fun({T, K, D}, {I, R1}) ->
        TK = redis_str_key(T, K),
        Pool = eredis_pool:get_pool(TK),
        case lists:keytake(Pool, 1, R1) of
            {value, {_, L1, L2}, R11} ->
                {I + 1, [{Pool, [TK | L1], [{I, D} | L2]} | R11]};
            _ ->
                {I + 1, [{Pool, [TK], [{I, D}]} | R1]}
        end
    end, {1, []}, TableKeyDefs),
    %%遍历所有节点，依次get
    Reults = lists:foldl(fun({Pool, Keys, IndexDefs}, Acc) ->
         VL = mget(Pool, Keys),
        merge_index_value_def(VL, IndexDefs, []) ++ Acc
    end, [], NodeKeys),
    %%聚合结果，调整顺序，保证和入参一致
    [V || {_, V} <- lists:keysort(1, Reults)].


%% ----------------------------------------------------
%% @doc
%%      cluster不支持mget， 直接循环get
%%      TODO pipeline
%% @end
%% ----------------------------------------------------
mget(Pool, TableKeys) ->
    lists:map(fun(TK)->
        case sync_command(Pool, "GET", [TK]) of
            {error, Err} ->
                throw(Err);
            {ok, B} ->
                B
        end
    end,TableKeys).
%% ----------------------------------------------------
%% @doc
%%       脏写 str类型
%% @end
%% ----------------------------------------------------
dirty_write({T, K}, Value) ->
    TableKey = redis_str_key(T, K),
    sync_command(eredis_pool:get_pool(TableKey), "SET", [TableKey, Value]);
dirty_write(TableKey, Value) ->
    sync_command(eredis_pool:get_pool(TableKey), "SET", [TableKey, Value]).
dirty_write(TableKey, Value, EX) when is_integer(EX) ->
    sync_command(eredis_pool:get_pool(TableKey), "SET", [TableKey, Value, "EX", EX]);
dirty_write(TableKey, Value, NX) when is_boolean(NX) ->
    if
        NX ->
            sync_command(eredis_pool:get_pool(TableKey), "SET", [TableKey, Value, "NX"]);
        true ->
            dirty_write(TableKey, Value)
    end.
dirty_write(TableKey, Value, NX, EX) ->
    if
        NX ->
            sync_command(eredis_pool:get_pool(TableKey), "SET", [TableKey, Value, "EX", EX, "NX"]);
        true ->
            dirty_write(TableKey, Value, EX)
    end.

%% ----------------------------------------------------
%% @doc
%%       锁  LockTime:锁超时时间  TimeOut:客户端等待超时时间
%%      set nx实现竞争锁  ex超时时间防止死锁  对象存储为进程id 实现持锁人解锁
%%      自旋锁，每隔50毫秒重试一次
%% @end
%% ----------------------------------------------------
lock(Table, Key) ->
    lock(Table, Key, ?LOCKTIME, ?TIMEOUT).
lock(Table, Key, LockTime, TimeOut) ->
    LockKey = redis_lock_key(Table, Key),
    lock(LockKey, LockTime, TimeOut).

lock(LockKey) ->
    lock(LockKey, ?LOCKTIME, ?TIMEOUT).
lock(LockKey, LockTime, TimeOut) ->
    lock_(LockKey, pid_to_list(self()), LockTime, TimeOut).

lock_(SLKey, V, _LockTime, TimeOut) when TimeOut =< 0 ->
    throw({lock_error, SLKey, V});
lock_(SLKey, V, LockTime, TimeOut) ->
    Ms1 = now_millisecond(),
    case sync_command(eredis_pool:get_pool(SLKey), "SET", [SLKey, V, "EX", LockTime, "NX"]) of
        {ok, <<"OK">>} ->
            ok;
        _ ->
            timer:sleep(?LOCK_SLEEP_TIME),
            MS2 = now_millisecond(),
            lock_(SLKey, V, LockTime, TimeOut - (MS2 - Ms1))
    end.
%% ----------------------------------------------------
%% @doc
%%       解除锁
%% ①检查是否是自己持有锁  ②删除锁
%% 解锁分为两步，但是要保证操作的原子性使用lua
%%  eval script numkeys keys args
%% @end
%% ----------------------------------------------------
unlock(LockKey) ->
    LuaScript = "if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end",
    V = list_to_binary(pid_to_list(self())),
    Pool = eredis_pool:get_pool(LockKey),
    case eredis_lib:sync_command(Pool, "EVAL", [LuaScript, 1, LockKey, V]) of
        {ok, _} ->
            ok;
        {error, Err} ->
            lager:error("unlock err:~p reason:~p", [LockKey, Err])
    end,
    ok.

%% ----------------------------------------------------
%% @doc
%%       锁修改
%% @end
%% ----------------------------------------------------
update(Table, Key, V) ->
    update(Table, Key, fun(_, _) -> {ok, ok, V} end, none, []).
update(Table, Key, Fun, Def, Args) ->
    update(Table, Key, Fun, Def, Args, ?TIMEOUT).
update(Table, Key, Fun, Def, Args, TimeOut) ->
    LockKey = redis_lock_key(Table, Key),
    lock(LockKey, ?LOCKTIME, TimeOut),
    TableKey = redis_str_key(Table, Key),
    OldV = getv(TableKey, Def),
    try Fun(Args, OldV) of
        {ok, Reply} ->
            Reply;
        {ok, Reply, OldV} ->
            Reply;
        {ok, Reply, NV} ->
            dirty_write(TableKey, NV),
            Reply;
        Other ->
            throw({?MODULE, ?FUNCTION_NAME, 'bad_return', Other})
    catch
        throw:R ->
            R;
        _EType:Reason ->
            throw({update_error, Table, Key, Def, Args, Reason})
    after
        unlock(LockKey)
    end.
%% ----------------------------------------------------
%% @doc
%%       伪事务 TCC
%% @end
%% ----------------------------------------------------
transaction(TableKeyDefs, F, Args, TimeOut) ->
    TableKeys = [redis_lock_key(T, K) || {T, K, _D} <- TableKeyDefs],
    %%  锁住
    LockKeys = transaction_locks(TableKeys, TimeOut, pid_to_list(self())),
    Values = gets(TableKeyDefs),
    %%  逻辑处理
    try F(Args, Values) of
        {ok, Reply} ->
            unlocks(LockKeys),
            Reply;
        {ok, Reply, Values} ->
            unlocks(LockKeys),
            Reply;
        {ok, Reply, NValues} ->
            %%  Confirm
            confirm(TableKeyDefs, Values, NValues),
            unlocks(LockKeys),
            Reply;
        Err ->
            unlocks(LockKeys),
            Err
    catch
        E1:E2 ->
            lager:error("transaction_error,type:~p,reason:~p,info:~p", [E1, E2, erlang:get_stacktrace()]),
            %%  Cancel
            unlocks(LockKeys)
    end.

%% ----------------------------------------------------
%% @doc
%%      获得redis存储的key
%% @end
%% ----------------------------------------------------
redis_str_key(TableKeys) when is_list(TableKeys) ->
    [redis_str_key(Table, Key) || {Table, Key} <- TableKeys].
redis_str_key(Table, Key) ->
    atom_to_list(Table) ++ ":" ++ term_to_list(Key).
%% ----------------------------------------------------
%% @doc
%%      获得redis锁key
%% @end
%% ----------------------------------------------------
redis_lock_key(TableKeys) ->
    [redis_lock_key(Table, Key) || {Table, Key} <- TableKeys].
redis_lock_key(Table, Key) ->
    "lock:" ++ atom_to_list(Table) ++ ":" ++ term_to_list(Key).
%%%===================LOCAL FUNCTIONS==================
%% ----------------------------------------------------
%% @doc
%%      transaction批量锁  自旋锁
%% @end
%% ----------------------------------------------------
transaction_locks(TableKeys, TimeOut, V) ->
    MS1 = now_millisecond(),
    {Suc, Fail} = lists:foldl(fun(LockKey, {S, F}) ->
        case sync_command(eredis_pool:get_pool(LockKey), "SET", [LockKey, V, "EX", ?LOCKTIME, "NX"]) of
            {ok, <<"OK">>} ->
                {[LockKey | S], F};
            _ ->
                {S, [LockKey | F]}
        end
    end, {[], []}, TableKeys),
    case Fail =:= [] of
        true ->
            Suc;
        false ->
            unlock(Suc),
            MS2 = now_millisecond(),
            transaction_locks(TableKeys, TimeOut - (MS2 - MS1), V)
    end.
%% ----------------------------------------------------
%% @doc
%%      transaction 提交
%% @end
%% ----------------------------------------------------
confirm([],[],[])->
    ok;
confirm([{T, K, _D} | TableKeyDefs], [OldV | Values], [NewV | NValues]) ->
    case NewV of
        ?IGNORE_VALUE ->
            confirm(TableKeyDefs, Values, NValues);
        OldV ->
            confirm(TableKeyDefs, Values, NValues);
        _ ->
            case dirty_write({T, K}, NewV) of
                {ok, _} ->
                    ok;
                {error, ER} ->
                    lager:error("transaction_after_update_dirty_write_error:~p", [ER])
            end,
            confirm(TableKeyDefs, Values, NValues)
    end.
%% ----------------------------------------------------
%% @doc
%%       批量解除锁
%% @end
%% ----------------------------------------------------
unlocks([]) ->
    ok;
unlocks([{_Table, _Key} | _T] = TableKeys) ->
    LockKeys = redis_lock_key(TableKeys),
    unlocks(LockKeys);
unlocks([H | LockKeys]) ->
    unlock(H),
    unlocks(LockKeys).


%% ----------------------------------------------------
%% @doc
%%       获得时间-毫秒
%% @end
%% ----------------------------------------------------
now_millisecond() ->
    {M, S, MS} = os:timestamp(),
    M * 1000000000 + S * 1000 + MS div 1000.

%% ----------------------------------------------------
%% @doc
%%       转字符串
%% @end
%% ----------------------------------------------------
term_to_list(Term) when is_list(Term) ->
    Term;
term_to_list(Term) when is_integer(Term) ->
    integer_to_list(Term);
term_to_list(Term) when is_atom(Term) ->
    atom_to_list(Term);
term_to_list(_Term) ->
    throw("undifined_type").


%% ----------------------------------------------------
%% @doc
%%       合并返回值还有索引
%% @end
%% ----------------------------------------------------
merge_index_value_def([], [], ReplyL) ->
    ReplyL;
merge_index_value_def([V | VL], [{I, DV} | IndexDefs], ReplyL) ->
    R = if
        DV =:= 'ignore' ->
            {I, V};
        V =:= undefined ->
            {I, DV};
        true ->
            {I, V}
    end,
    merge_index_value_def(VL, IndexDefs, [R | ReplyL]).
