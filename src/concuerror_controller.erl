%% -*- erlang-indent-level: 2 -*-

-module(concuerror_controller).

-export([start/1, stop/1, report_stats/3]).

-include("concuerror.hrl").

-record(controller_status, {
          busy_times       :: maps:map(),
          busy             :: [pid()],
          idle             :: [pid()],
          scheduling_start :: integer()
         }).
%%------------------------------------------------------------------------------

-spec start([node()]) -> concuerror:exit_status().

start(Nodes) ->
  Fun =
    fun() ->
        initialize_controller(Nodes)
    end,
  spawn_link(Fun).

initialize_controller(Nodes) ->
  N = length(Nodes),
  Schedulers = get_schedulers(N, N),
  SchedulerPids = [Pid || {Pid, _} <- Schedulers],
  [InitialScheduler|Rest] = SchedulerPids,
  InitialScheduler ! start,
  SchedulingStart = erlang:monotonic_time(),
  Busy = [InitialScheduler],
  Idle = Rest,
  InitTimes = maps:from_list([{Pid, {SchedId, undefined, 0}} || {Pid, SchedId} <- Schedulers]),
  Fun =
    fun({SchedId, undefined, 0}) ->
        {SchedId, SchedulingStart, 0}
    end,
  BusyTimes = maps:update_with(InitialScheduler, Fun, InitTimes),
  InitialStatus =
    #controller_status{
       busy_times = BusyTimes,
       busy = Busy,
       idle = Idle,
       scheduling_start = SchedulingStart
      },
  controller_loop(InitialStatus).
 
get_schedulers(0, _) -> [];
get_schedulers(I, N) ->
  receive
    {scheduler, Pid} ->
      SchedulerId = N-I+1,
      Pid ! {scheduler_number, N-I+1},
      [{Pid, SchedulerId} | get_schedulers(I-1, N)]
  end.

controller_loop(#controller_status{busy = Busy} = Status)
  when Busy =:= [] ->
  SchedulingEnd = erlang:monotonic_time(),
  #controller_status{
     busy_times = BusyTimes,
     idle = Idle,
     scheduling_start = SchedulingStart
    } = Status,
  [Scheduler ! finish || Scheduler <- Idle],
  [receive finished -> ok end || _ <- Idle],
  receive
    {stop, Pid} ->
      %%SchedulingEnd = erlang:monotonic_time(),
      report_stats(BusyTimes, SchedulingStart, SchedulingEnd),
      Pid ! done
  end;
controller_loop(Status) ->
  #controller_status{
     busy_times = BusyTimes,
     busy = Busy,
     idle = Idle
    } = Status,
  receive
    {request_idle, Scheduler} ->
      true = lists:member(Scheduler, Busy),
      {NewIdle, NewBusy, NewBusyTimes} =
        case Idle of
          [] ->
            Scheduler ! no_idle_schedulers,
            {Idle, Busy, BusyTimes};
          [IdleScheduler|Rest] ->
            Scheduler ! {scheduler, IdleScheduler},
            PeriodStart = erlang:monotonic_time(),
            Fun = 
              fun({SchedId, undefined, Acc}) ->
                  {SchedId, PeriodStart, Acc}
              end,
            NewTimes = maps:update_with(IdleScheduler, Fun, BusyTimes),
            {Rest, [IdleScheduler|Busy], NewTimes}
        end,
      controller_loop(Status#controller_status{
                        busy_times = NewBusyTimes, 
                        busy = NewBusy, 
                        idle = NewIdle
                       });
    %% {has_more, Scheduler, TransferableState} ->
    %%   true = lists:member(Scheduler, Busy),
    %%   _TempBusy = lists:delete(Scheduler, Busy),
    %%   TempIdle = [Scheduler|Idle],
    %%   States =
    %%     concuerror_scheduler:distribute_interleavings(TransferableState, length(TempIdle)),
    %%   %% {NewIdle, NewBusy} = start_schedulers(States, TempIdle, TempBusy),
    %%   [Other] = Idle,
    %%   [H1,H2] = States,
    %%   Scheduler ! {explore, non_stop, H1},
    %%   Other ! {explore, non_stop, H2},
    %%   NewIdle = [],
    %%   NewBusy = [Scheduler, Other],
    %%   controller_loop(Schedulers, NewIdle, NewBusy);
    {done, Scheduler} ->
      PeriodEnd = erlang:monotonic_time(),
      NewBusy = lists:delete(Scheduler, Busy),
      NewIdle = [Scheduler|Idle],
      Fun =
        fun({SchedId, PeriodStart, Acc}) ->
            {SchedId, undefined, Acc + PeriodEnd - PeriodStart} 
        end,
      NewTimes = maps:update_with(Scheduler, Fun, BusyTimes),
      controller_loop(Status#controller_status{
                        busy_times = NewTimes,
                        busy = NewBusy, 
                        idle = NewIdle})
  end.

distribute_interleavings(Trace, 0) ->
  Trace;
distribute_interleavings(Trace, _AvailableSchedulers) ->
  %exit(Trace).
  io:fwrite("~n~n~p~n~n", [Trace]),
  exit(traceprint).

start_schedulers([], NewIdle, NewBusy) -> {NewIdle, NewBusy};
start_schedulers([State|RestStates], [Scheduler|RestSchedulers], Busy) ->
  Scheduler ! {explore, State},
  start_schedulers(RestStates, RestSchedulers, [Scheduler|Busy]).

%%------------------------------------------------------------------------------

-spec stop(pid()) -> ok.

stop(Controller) ->
  Controller ! {stop, self()},
  receive
    done -> ok
  end.

%%------------------------------------------------------------------------------

-spec report_stats(maps:map(), integer(), integer()) -> ok.

report_stats(BusyTimes, Start, End) ->
  %% TODO modify this to be reported through the Logger
  Timediff = erlang:convert_time_unit(End - Start, native, milli_seconds),
  Minutes = Timediff div 60000,
  Seconds = Timediff rem 60000 / 1000,
  io:fwrite("Scheduling Time: ~wm~.3fs~n", [Minutes, Seconds]),
  Fun =
    fun(A,B) ->
        {_,{IdA,_,_}} = A,
        {_,{IdB,_,_}} = B,
        IdA =< IdB
    end,
  report_utilization(lists:sort(Fun, maps:to_list(BusyTimes)), End - Start).

report_utilization([], _) ->
  ok;
report_utilization([{_,{SchedulerId, undefined, RunningTime}}|Rest],
                   TotalRunningTime) ->
  Utilization = RunningTime / TotalRunningTime * 100,
  io:fwrite("Scheduler ~w: ~.2f% Utilization~n", [SchedulerId, Utilization]),
  report_utilization(Rest, TotalRunningTime).
