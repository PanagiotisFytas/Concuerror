-module(i_hate_myself).

-export([i_hate_myself/0]).
-export([scenarios/0]).

-concuerror_options_forced([{parallel, true}, {number_of_schedulers, 1}]).

scenarios() -> [{?MODULE, inf, source}].

i_hate_myself() ->
    Name = list_to_atom(lists:flatten(io_lib:format("~p",[make_ref()]))),
    spawn(fun() -> Name ! message end),
    register(Name, self()),
    receive
        message -> ok
    end.
