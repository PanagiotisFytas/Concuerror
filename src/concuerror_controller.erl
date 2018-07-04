%% -*- erlang-indent-level: 2 -*-

-module(concuerror_controller).

-export([start/1, stop/1]).

-include("concuerror.hrl").

%%------------------------------------------------------------------------------

-spec start([node()]) -> concuerror:exit_status().

start(Nodes) ->
  Fun =
    fun() ->
        initialize_controller(Nodes)
    end,
  spawn_link(Fun).

initialize_controller(Nodes) ->
  Schedulers = get_schedulers(length(Nodes)),
  [InitialScheduler|Rest] = Schedulers,
  InitialScheduler ! start,
  Busy = [InitialScheduler],
  Idle = Rest,
  controller_loop(Schedulers, Idle, Busy).
 
get_schedulers(0) -> [];
get_schedulers(N) ->
  receive
    {scheduler, Pid} ->
      [Pid | get_schedulers(N-1)]
  end.

controller_loop(Schedulers, _, []) ->
  [Scheduler ! finish || Scheduler <- Schedulers],
  [receive finish -> ok end || _ <- Schedulers];
controller_loop(Schedulers, _Idle, _Busy) ->
  receive
    %% {has_more, Scheduler1, TransferableState} ->
    %%   receive
    %%     {has_more, Scheduler2, _TranferableState2} ->
    %%       %TransferableState = _TranferableState2,
    %%       Scheduler2 ! {explore, TransferableState},
    %%       Scheduler1 ! {explore, TransferableState},
    %%       NewBusy = [Scheduler2, Scheduler1],
    %%       NewIdle = [],
    %%       controller_loop(Schedulers, NewIdle, NewBusy)
    %%   end;
    {has_more, Scheduler, TransferableState} ->
      true = lists:member(Scheduler, _Busy),
      _TempBusy = lists:delete(Scheduler, _Busy),
      TempIdle = [Scheduler|_Idle],
      States =
        concuerror_scheduler:distribute_interleavings(TransferableState, length(TempIdle)),
      %% {NewIdle, NewBusy} = start_schedulers(States, TempIdle, TempBusy),
      [Other] = _Idle,
      [H1,H2] = States,
      Scheduler ! {explore, non_stop, H1},
      Other ! {explore, non_stop, H2},
      NewIdle = [],
      NewBusy = [Scheduler, Other],
      controller_loop(Schedulers, NewIdle, NewBusy);
      %% [Scheduler2, Scheduler3] = _Idle,
      %% Scheduler3 ! {explore, TransferableState},
      %% Scheduler2 ! {explore, TransferableState},
      %% Scheduler1 ! {explore, TransferableState},
      %% NewBusy = [Scheduler3, Scheduler2, Scheduler1],
      %% NewIdle = [],
      %% controller_loop(Schedulers, NewIdle, NewBusy);
    {done, Scheduler} ->
      NewBusy = lists:delete(Scheduler, _Busy),
      NewIdle = [Scheduler|_Idle],
      controller_loop(Schedulers, NewIdle, NewBusy)
    %% {finish, Pid} ->
    %%   [Scheduler ! finish || Scheduler <- Schedulers],
    %%   [receive finish -> ok end || _ <- Schedulers],
    %%   Pid ! done
    %% {has_more, Scheduler1, TransferableState} ->
    %%   [Scheduler1] = _Busy,
    %%   [Scheduler2] = _Idle,
    %%   Scheduler2 ! {explore, TransferableState},
    %%   NewBusy = [Scheduler2],
    %%   NewIdle = [Scheduler1],
    %%   controller_loop(Schedulers, NewIdle, NewBusy);
    %% {done, Scheduler1} ->
    %%   NewBusy1 = lists:delete(Scheduler1, _Busy),
    %%   receive
    %%     {done, Scheduler2} ->
    %%       NewBusy2 = lists:delete(Scheduler2, NewBusy1),
    %%       receive
    %%         {done, Scheduler3} ->
    %%           [Scheduler3] = NewBusy2,
    %%           io:fwrite("~nScheduler1:~n~w~nScheduler2:~n~w~nScheduler3:~n~w~n~n", [{Scheduler1, node(Scheduler1)}, {Scheduler2, node(Scheduler2)}, {Scheduler3, node(Scheduler3)}]),
    %%           Scheduler1 ! finish,
    %%           Scheduler2 ! finish,
    %%           Scheduler3 ! finish
    %%       end
    %%   end,
    %%   controller_loop(Schedulers, [], []);
    %% {Pid, finish} ->
    %%   Pid ! done
    %% {done, Scheduler1} ->
    %%   [Scheduler1] = _Busy,
    %%   [Scheduler2] = _Idle,
    %%   Scheduler1 ! finish,
    %%   Scheduler2 ! finish,
    %%   get_exit_status(Schedulers)
    %% {state, Scheduler1, State1} ->
    %%   [Scheduler2] = lists:delete(Scheduler1, Schedulers),
    %%   State2 = 
    %%     receive 
    %%       {state, Scheduler2, St2} ->
    %%         St2
    %%     end,
    %%   Scheduler1 ! {show_exit, {Scheduler1, State1,Scheduler2, State2}};
    %% {change_scheduler, ChangedState, Source} ->
    %%   [Target] = lists:delete(Source, Schedulers),
    %%   receive
    %%     {change_scheduler, ChangedState2, Target} ->
    %%       Target ! ChangedState,
    %%       Source ! ChangedState2
    %%   end;
    %% {procs, Scheduler1, State1} ->
    %%   [Scheduler2] = lists:delete(Scheduler1, Schedulers),
    %%   State2 = 
    %%     receive 
    %%       {procs, Scheduler2, St2} ->
    %%         St2
    %%     end,
    %%   Scheduler1 ! {show_exit, format_(Scheduler1, State1,Scheduler2, State2)};
    %% exit ->
    %%    ok
  end.

distribute_interleavings(Trace, 0) ->
  Trace;
distribute_interleavings(Trace, _AvailableSchedulers) ->
  %exit(Trace).
  io:fwrite("~n~n~p~n~n", [Trace]),
  exit(traceprint).

start_schedulers([], NewIdle, NewBusy) -> {NewIdle, NewBusy};
start_schedulers([State|RestStates], [Scheduler|RestSchedules], Busy) ->
  Scheduler ! {explore, State},
  start_schedulers(RestStates, RestSchedules, [Scheduler|Busy]).
  

%% format_(Scheduler1, [State1], Scheduler2, [State2]) ->
%%   Sort = fun(A, B) -> element(1,A) < element(1,B) end,
%%   L1 = lists:sort(Sort, State1),
%%   L2 = lists:sort(Sort, State2),
%%   {Scheduler1, Scheduler2, format_aux(L1,L2)}.

%% format_aux([],[]) ->
%%   [];
%% format_aux([H1|T1], [H2|T2]) ->
%%   [{H1, "--------------", H2, "||||||||||||||||||||||||||||||||||||||||||||"}|format_aux(T1,T2)].
      
%%------------------------------------------------------------------------------

-spec stop(pid()) -> ok.

stop(Controller) ->
  Controller ! {finish, self()},
  receive
    done -> ok
  end.
