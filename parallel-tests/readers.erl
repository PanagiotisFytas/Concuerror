-module(readers).

-export([scenarios/0, test/0, test2/0, test2_no_name/0,
	test3/0, testN/0, testN_no_name/0,
	test4/0, test5/0, test6/0,
    test3m/0, test4m/0, test5m/0]).

-concuerror_options_forced([{scheduling, oldest}]).

scenarios() -> [{test, inf, R} || R <- [dpor,source]].

test() ->
    last_zero(1).	

test2() ->
    last_zero(2).

test2_no_name() ->
    last_zero_no_named(2).

test4() ->
    last_zero(4).

test5() ->
    last_zero(5).

test6() ->
    last_zero(6).

testN() ->
    last_zero(11).

testN_no_name() ->
    last_zero_no_named(7).

test4m() ->
    last_zero_many(4).

test5m() ->
    last_zero_many(5).

test3m() ->
    last_zero_many(3).


test3() ->
    last_zero(3).

last_zero(N) ->
    ets:new(table, [public, named_table]),
    lists:foreach(fun(X) -> ets:insert(table, {X, 0}) end, lists:seq(0,N)),
    spawn(fun() -> reader(N) end),
    lists:foreach(fun(X) -> spawn(fun() -> writer(X) end) end, lists:seq(1,N)),
    receive after infinity -> ok end.

reader(0) -> ok;
reader(N) ->
    [{N, R}] = ets:lookup(table, N),
    case R of
        0 -> ok;
        _ -> reader(N-1)
    end.

writer(X) ->
	[{_, _R}] = ets:lookup(table, 0),
    [{_, R}] = ets:lookup(table, X-1),
    ets:insert(table, {X, R+1}).

last_zero_many(N) ->
    ets:new(table, [public, named_table]),
    lists:foreach(fun(X) -> ets:insert(table, {X, 0}) end, lists:seq(0,N)),
    lists:foreach(fun(X) -> spawn(fun() -> reader(N) end) end, lists:seq
(1,N)),
    lists:foreach(fun(X) -> spawn(fun() -> writer(X) end) end, lists:seq(1,N)),
    receive after infinity -> ok end.


last_zero_no_named(N) ->
    Tid = ets:new(table, [public]),
    lists:foreach(fun(X) -> ets:insert(Tid, {X, 0}) end, lists:seq(0,N)),
    spawn(fun() -> reader(N,Tid) end),
    lists:foreach(fun(X) -> spawn(fun() -> writer(X, Tid) end) end, lists:seq(1,N)),
    receive after infinity -> ok end.

reader(0, _) -> ok;
reader(N, Tid) ->
    [{N, R}] = ets:lookup(Tid, N),
    case R of
        0 -> ok;
        _ -> reader(N-1, Tid)
    end.

writer(X, Tid) ->
	[{_, _R}] = ets:lookup(Tid, 0),
    [{_, R}] = ets:lookup(Tid, X-1),
    ets:insert(Tid, {X, R+1}).
