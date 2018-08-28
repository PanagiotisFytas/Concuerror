%% -*- erlang-indent-level: 2 -*-

-module(concuerror_controller).

-export([start/1, stop/1, report_stats/3]).

-include("concuerror.hrl").

-record(controller_status, {
          execution_tree    :: concuerror_scheduler:execution_tree(),
          schedulers_uptime :: maps:map(),
          busy              :: [{pid(), concuerror_scheduler:reduced_scheduler_state()}],
          idle              :: [pid()],
          idle_frontier     :: [concuerror_scheduler:reduced_scheduler_state()],
          scheduling_start  :: integer(),
          budget_exceeded = 0  :: integer(),
          ownership_claims = 0 :: integer()
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
  InitialScheduler ! {start, 0},
  SchedulingStart = erlang:monotonic_time(),
  %% InitUptimes = maps:from_list([{Pid, {SchedId, undefined, 0}} || {Pid, SchedId} <- Schedulers]),
  %% Uptimes = update_scheduler_started(InitialScheduler, InitUptimes),
  {IdleFrontier, ExecutionTree, Duration, InterleavingsExplored} =
    receive
      {budget_exceeded, InitialScheduler, NewFragment, D, IE} ->
        {[NewFragment], concuerror_scheduler:initialize_execution_tree(NewFragment), D, IE};
      {done, InitialScheduler, D, IE} ->
        {[], empty, D, IE}
    end,
  InitUptimes = maps:from_list([{Pid, {SchedId, 0, 0}} || {Pid, SchedId} <- Schedulers]),
  NewUptimes = update_scheduler_stopped(InitialScheduler, InitUptimes, Duration, InterleavingsExplored),
  Busy = [],
  Idle = SchedulerPids,
  InitialStatus =
    #controller_status{
       schedulers_uptime = NewUptimes,
       busy = Busy,
       execution_tree = ExecutionTree,
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
  %% TODO : remove this check
  %% empty = Status#controller_status.execution_tree, %% this does not hold true
  case Status#controller_status.execution_tree =/= empty of
    true ->
      ok;%concuerror_scheduler:print_tree("", Status#controller_status.execution_tree);
    false ->
      ok
  end,
  #controller_status{
     schedulers_uptime = _Uptimes,
     idle = Idle,
     scheduling_start = SchedulingStart
    } = Status,
  [Scheduler ! finish || Scheduler <- Idle],
  [receive finished -> ok 
   after 10000 ->
       exit(sched_responed_timeout)
   end
   || _ <- Idle],
  receive
    {stop, Pid} ->
      %%SchedulingEnd = erlang:monotonic_time(),
      report_stats_parallel(Status, SchedulingStart, SchedulingEnd),
      Pid ! done
  after 50000 ->
      exit(stop_timeout)
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
     execution_tree = ExecutionTree,
     idle = Idle,
     idle_frontier = IdleFrontier,
     budget_exceeded = BE,
     ownership_claims = OC
    } = Status,
  receive
    {claim_ownership, Scheduler, Fragment, Duration, IE} ->
      NewUptimes = update_scheduler_stopped(Scheduler, Uptimes, Duration, IE),
      {Scheduler, OldFragment} = lists:keyfind(Scheduler, 1, Busy), %% maybe use this as well
      NewBusy = lists:keydelete(Scheduler, 1, Busy),
      NewIdle = [Scheduler|Idle],
      {NewIdleFragment, NewExecutionTree} =
        concuerror_scheduler:update_execution_tree(OldFragment, Fragment, ExecutionTree),
      NewIdleFrontier =
        case NewIdleFragment =:= fragment_finished of
          false ->
            [NewIdleFragment|IdleFrontier];
          true ->
            IdleFrontier
        end,
      controller_loop(Status#controller_status{
                        schedulers_uptime = NewUptimes,
                        busy = NewBusy,
                        execution_tree = NewExecutionTree,
                        idle = NewIdle,
                        idle_frontier = NewIdleFrontier,
                        ownership_claims = OC + 1
                       });
    {budget_exceeded, Scheduler, Fragment, Duration, IE} ->
      NewUptimes = update_scheduler_stopped(Scheduler, Uptimes, Duration, IE),
      {Scheduler, OldFragment} = lists:keyfind(Scheduler, 1, Busy),
      NewBusy = lists:keydelete(Scheduler, 1, Busy),
      NewIdle = [Scheduler|Idle],
      {NewIdleFragment, NewExecutionTree} =
        concuerror_scheduler:update_execution_tree(OldFragment, Fragment, ExecutionTree),
      NewIdleFrontier =
        case NewIdleFragment =:= fragment_finished of
          false ->
            [NewIdleFragment|IdleFrontier];
          true ->
            IdleFrontier
        end,
      controller_loop(Status#controller_status{
                        schedulers_uptime = NewUptimes,
                        busy = NewBusy,
                        execution_tree = NewExecutionTree,
                        idle = NewIdle,
                        idle_frontier = NewIdleFrontier,
                        budget_exceeded = BE + 1
                       });
    {done, Scheduler, Duration, IE} ->
      NewUptimes = update_scheduler_stopped(Scheduler, Uptimes, Duration, IE),
      {Scheduler, CompletedFragment} = lists:keyfind(Scheduler, 1, Busy),
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
      NewExecutionTree =
        concuerror_scheduler:update_execution_tree_done(CompletedFragment, ExecutionTree),
      controller_loop(Status#controller_status{
                        schedulers_uptime = NewUptimes,
                        busy = NewBusy,
                        execution_tree = NewExecutionTree,
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
  NewUptimes = Uptimes, %% update_scheduler_started(Scheduler, Uptimes),
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


update_scheduler_stopped(Scheduler, Uptimes, Duration, IE) ->
  %% PeriodEnd = erlang:monotonic_time(),
  %% Fun =
  %%   fun({SchedId, PeriodStart, Acc}) ->
  %%       {SchedId, undefined, Acc + PeriodEnd - PeriodStart} 
  %%   end,
  %% maps:update_with(Scheduler, Fun, Uptimes).
  Fun =
    fun({Id, Acc1, Acc2}) ->
        {Id, Acc1 + Duration, Acc2 + IE} 
    end,
  maps:update_with(Scheduler, Fun, Uptimes).

report_stats_parallel(Status, Start, End) ->
  #controller_status{
     schedulers_uptime = Uptimes,
     budget_exceeded = BE,
     ownership_claims = OC
    } = Status,
  io:fwrite("Budget Exceeded: ~B~nOwnership Claims: ~B~n", [BE, OC]), 
  report_stats(Uptimes, Start, End).

-spec report_stats(maps:map(), integer(), integer()) -> ok.

report_stats(Uptimes, Start, End) ->
  %% TODO modify this to be reported through the Logger
  Timediff = erlang:convert_time_unit(End - Start, native, milli_seconds),
  Minutes = Timediff div 60000,
  Seconds = Timediff rem 60000 / 1000,
  io:fwrite("Scheduling Time: ~wm~.3fs~n", [Minutes, Seconds]),
  Fun =
    fun(A,B) ->
        {_, {IdA, _, _}} = A,
        {_, {IdB, _, _}} = B,
        IdA =< IdB
    end,
  ListUptimes = lists:sort(Fun, maps:to_list(Uptimes)),
  case ListUptimes of
    [] ->
      ok;
    _ ->
      report_utilization(ListUptimes, Timediff),
      report_avg_time(ListUptimes, 0, 0)
  end.

report_utilization([], _) ->
  ok;
report_utilization([{_, {SchedulerId, RunningTime, InterleavingsExplored}}|Rest],
                   TotalRunningTime) ->
  Utilization = RunningTime / TotalRunningTime * 100,
  io:fwrite("Scheduler ~w: ~.2f% Utilization, ~B Interleavings Explored~n", [SchedulerId, Utilization, InterleavingsExplored]),
  report_utilization(Rest, TotalRunningTime).

report_avg_time([], Sum, N) ->
  io:fwrite("Total of ~B interleavings with average duration of ~.3fs", [N, Sum/1000/N]);
report_avg_time([{_, {_, Duration, IE}}|Rest], Sum, N) ->
  report_avg_time(Rest, Sum + Duration, N + IE).
