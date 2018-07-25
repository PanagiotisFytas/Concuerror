%% -*- erlang-indent-level: 2 -*-

-module(concuerror_controller).

-export([start/1, stop/1, report_stats/3]).

-include("concuerror.hrl").

-record(controller_status, {
          schedulers_uptime :: maps:map(),
          busy              :: [{pid(), concuerror_scheduler:reduced_scheduler_state()}],
          idle              :: [pid()],
          idle_frontier     :: [concuerror_scheduler:reduced_scheduler_state()],
          scheduling_start  :: integer()
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
  SchedulerNumbers = maps:from_list(lists:zip(Nodes, lists:seq(1, length(Nodes)))),
  UnsortedSchedulers = get_schedulers(N, SchedulerNumbers),
  Fun =
    fun({_, IdA}, {_, IdB}) ->
        IdA < IdB
    end,
  Schedulers = lists:sort(Fun, UnsortedSchedulers),
  SchedulerPids = [Pid || {Pid, _} <- Schedulers],
  [InitialScheduler|_Rest] = SchedulerPids,
  InitialScheduler ! {start, ?budget / N},
  SchedulingStart = erlang:monotonic_time(),
  InitUptimes = maps:from_list([{Pid, {SchedId, undefined, 0}} || {Pid, SchedId} <- Schedulers]),
  Uptimes = update_scheduler_started(InitialScheduler, InitUptimes),
  IdleFrontier =
    receive
      {budget_exceeded, InitialScheduler, NewFragment} ->
        [NewFragment];
      {done, InitialScheduler} ->
        []
    end,
  NewUptimes = update_scheduler_stopped(InitialScheduler, Uptimes),
  Busy = [],
  Idle = SchedulerPids,
  InitialStatus =
    #controller_status{
       schedulers_uptime = NewUptimes,
       busy = Busy,
       idle = Idle,
       idle_frontier = IdleFrontier,
       scheduling_start = SchedulingStart
      },
  controller_loop(InitialStatus).
 
get_schedulers(0, _) -> [];
get_schedulers(N, SchedulerNumbers) ->
  receive
    {scheduler, Pid} ->
      SchedulerId = maps:get(node(Pid), SchedulerNumbers),
      Pid ! {scheduler_number, SchedulerId},
      [{Pid, SchedulerId} | get_schedulers(N-1, SchedulerNumbers)]
  end.

controller_loop(#controller_status{
                   busy = Busy,
                   idle_frontier = IdleFrontier
                  } = Status)
  when IdleFrontier =:= [] andalso Busy =:= [] ->
  SchedulingEnd = erlang:monotonic_time(),
  #controller_status{
     schedulers_uptime = Uptimes,
     idle = Idle,
     scheduling_start = SchedulingStart
    } = Status,
  [Scheduler ! finish || Scheduler <- Idle],
  [receive finished -> ok end || _ <- Idle],
  receive
    {stop, Pid} ->
      %%SchedulingEnd = erlang:monotonic_time(),
      report_stats(Uptimes, SchedulingStart, SchedulingEnd),
      Pid ! done
  end;
controller_loop(Status) ->
  #controller_status{
     busy = Busy,
     %% idle = Idle,
     idle_frontier = IdleFrontier
    } = Status,
  NewIdleFrontier = partition(IdleFrontier, ?fragmentation_val - length(Busy)),
  NewStatus = assign_work(Status#controller_status{idle_frontier = NewIdleFrontier}),
  wait_scheduler_response(NewStatus).

wait_scheduler_response(Status) ->
  #controller_status{
     schedulers_uptime = Uptimes,
     busy = Busy,
     idle = Idle,
     idle_frontier = IdleFrontier
    } = Status,
  receive
    {claim_ownership, Scheduler, TransferableState} ->
      NewUptimes = update_scheduler_stopped(Scheduler, Uptimes),
      {Scheduler, _UnexploredFragment} = lists:keyfind(Scheduler, 1, Busy), %% maybe use this as well
      NewBusy = lists:keydelete(Scheduler, 1, Busy),
      NewIdle = [Scheduler|Idle],
      BusyFrontier = [Fragment || {_, Fragment} <- NewBusy],
      NewIdleFragment =
        concuerror_scheduler:fix_ownership(TransferableState, IdleFrontier, BusyFrontier),
      NewIdleFrontier = [NewIdleFragment|IdleFrontier],
      controller_loop(Status#controller_status{
                        schedulers_uptime = NewUptimes,
                        busy = NewBusy, 
                        idle = NewIdle,
                        idle_frontier = NewIdleFrontier
                       });
    {budget_exceeded, Scheduler, TransferableState} ->
      NewUptimes = update_scheduler_stopped(Scheduler, Uptimes),
      {Scheduler, _CompletedFragment} = lists:keyfind(Scheduler, 1, Busy),
      NewBusy = lists:keydelete(Scheduler, 1, Busy),
      NewIdle = [Scheduler|Idle],
      BusyFrontier = [Fragment || {_, Fragment} <- NewBusy],
      NewIdleFragment =
        concuerror_scheduler:fix_ownership(TransferableState, IdleFrontier, BusyFrontier),
      NewIdleFrontier = [NewIdleFragment|IdleFrontier],
      controller_loop(Status#controller_status{
                        schedulers_uptime = NewUptimes,
                        busy = NewBusy, 
                        idle = NewIdle,
                        idle_frontier = NewIdleFrontier
                       });
    {done, Scheduler} ->
      NewUptimes = update_scheduler_stopped(Scheduler, Uptimes),
      {Scheduler, _CompletedFragment} = lists:keyfind(Scheduler, 1, Busy),
      %% TODO I must figure out what do with this fragments that holds the
      %% backtack (i.e the nodes that has been explored by that scheduler
      NewBusy = lists:keydelete(Scheduler, 1, Busy),
      %% TODO figure out what to do with the fragment of that scheduler,
      %% its trace holds info about node ownership probably S.O.S.
      %% Probably a master tree is needed that holds all this info
      %% and gets cut down maybe by the #scheduler_state.done.
      %% Most probably i will have to look at the backtrack of the
      %% fragment and use this to update a master tree with the nodes
      %% that have been explored by it 
      NewIdle = [Scheduler|Idle],
      controller_loop(Status#controller_status{
                        schedulers_uptime = NewUptimes,
                        busy = NewBusy, 
                        idle = NewIdle
                       })
  end.

start_schedulers([], NewIdle, NewBusy) -> {NewIdle, NewBusy};
start_schedulers([State|RestStates], [Scheduler|RestSchedulers], Busy) ->
  Scheduler ! {explore, State},
  start_schedulers(RestStates, RestSchedulers, [Scheduler|Busy]).

assign_work(#controller_status{
               idle = Idle,
               idle_frontier = IdleFrontier
              } = Status)
  when Idle =:= [] orelse IdleFrontier =:= []->
  Status;
assign_work(Status) ->
  #controller_status{
     busy = Busy,
     schedulers_uptime = Uptimes,
     idle = [Scheduler|RestIdle],
     idle_frontier = [Fragment|RestIdleFrontier]
    } = Status,
  Scheduler ! {explore, Fragment, ?budget/length([Scheduler|RestIdle])},
  NewUptimes = update_scheduler_started(Scheduler, Uptimes),
  UpdatedStatus =
    Status#controller_status{
      busy = [{Scheduler, Fragment}|Busy],
      schedulers_uptime = NewUptimes,
      idle = RestIdle,
      idle_frontier = RestIdleFrontier
     },
  assign_work(UpdatedStatus).
  
%%------------------------------------------------------------------------------

partition(Frontier, N) ->
  %% it should be N =:= ?fragmentation_val - #of active fragments
  %% need to fix the number to account for active fragments
  AdditionalFragmentsNeeded = N - length(Frontier),
  partition_aux(Frontier, AdditionalFragmentsNeeded, []).

%% TODO figure out if it matter whether I put new fragments in
%% the begining or not
partition_aux([], N, Frontier) when N =/= 0 ->
  %% The frontier cannot be further partitioned (probably).
  Frontier;
partition_aux(Frontier, 0, PartitionedFrontier) ->
  %% when length(Frontier) + length(PartitionedFrontier) = N ->
  Frontier ++ PartitionedFrontier;
partition_aux([Fragment|Rest], FragmentsNeeded, PartitionedFrontier) ->
  true = FragmentsNeeded > 0,
  {UpdatedFragment, NewFragments, FragmentsGotten} =
    concuerror_scheduler:distribute_interleavings(Fragment, FragmentsNeeded),
  NewPartitionedFrontier = NewFragments ++ [UpdatedFragment|PartitionedFrontier],
  partition_aux(Rest, FragmentsNeeded - FragmentsGotten, NewPartitionedFrontier).

%%------------------------------------------------------------------------------

-spec stop(pid()) -> ok.

stop(Controller) ->
  Controller ! {stop, self()},
  receive
    done -> ok
  end.

%%------------------------------------------------------------------------------

update_scheduler_started(Scheduler, Uptimes) ->
  PeriodStart = erlang:monotonic_time(),
  Fun =
    fun({SchedId, undefined, Acc}) ->
        {SchedId, PeriodStart, Acc}
    end,
  maps:update_with(Scheduler, Fun, Uptimes).


update_scheduler_stopped(Scheduler, Uptimes) ->
  PeriodEnd = erlang:monotonic_time(),
  Fun =
    fun({SchedId, PeriodStart, Acc}) ->
        {SchedId, undefined, Acc + PeriodEnd - PeriodStart} 
    end,
  maps:update_with(Scheduler, Fun, Uptimes).

-spec report_stats(maps:map(), integer(), integer()) -> ok.

report_stats(Uptimes, Start, End) ->
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
  report_utilization(lists:sort(Fun, maps:to_list(Uptimes)), End - Start).

report_utilization([], _) ->
  ok;
report_utilization([{_,{SchedulerId, undefined, RunningTime}}|Rest],
                   TotalRunningTime) ->
  Utilization = RunningTime / TotalRunningTime * 100,
  io:fwrite("Scheduler ~w: ~.2f% Utilization~n", [SchedulerId, Utilization]),
  report_utilization(Rest, TotalRunningTime).
