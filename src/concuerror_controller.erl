%% -*- erlang-indent-level: 2 -*-

-module(concuerror_controller).

-export([start/1, stop/1, report_stats/3]).

-include("concuerror.hrl").

-record(controller_status, {
          execution_tree              :: concuerror_scheduler:execution_tree(),
          schedulers_uptime           :: maps:map(),
          busy                        :: [{pid(), concuerror_scheduler:reduced_scheduler_state()}],
          idle                        :: [pid()],
          idle_frontier               :: queues:queue(),%%[concuerror_scheduler:reduced_scheduler_state()],
          scheduling_start            :: integer(),
          ownership_claims = 0        :: integer(),
          planner                     :: pid(),
          planner_queue = queue:new() :: queues:queue(),
          planner_status = idle       :: busy | idle
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
  [PlannerNode|SchedulerNodes] = Nodes,
  N = length(SchedulerNodes),
  SchedulerNumbers = maps:from_list(lists:zip(Nodes, lists:seq(0, N))),
  UnsortedSchedulers = get_schedulers(N+1, SchedulerNumbers),
  Fun =
    fun({_, IdA}, {_, IdB}) ->
        IdA < IdB
    end,
  [{Planner, _}|Schedulers] = lists:sort(Fun, UnsortedSchedulers),
  SchedulerPids = [Pid || {Pid, _} <- Schedulers],
  [InitialScheduler|_Rest] = SchedulerPids,
  InitialScheduler ! start,
  SchedulingStart = erlang:monotonic_time(),
  %% InitUptimes = maps:from_list([{Pid, {SchedId, undefined, 0}} || {Pid, SchedId} <- Schedulers]),
  %% Uptimes = update_scheduler_started(InitialScheduler, InitUptimes),
  {Duration, InterleavingsExplored} =
    receive
      {exploration_finished, InitialScheduler, ExploredFragment, D, IE} ->
        Planner ! {plan, ExploredFragment},
        {D, IE}
    end,
  {IdleFrontier, ExecutionTree} =
    receive
      {has_more, Planner, NewFragment, _D, _IE} ->
        IF = concuerror_scheduler:distribute_interleavings(NewFragment),
        ET = concuerror_scheduler:initialize_execution_tree(NewFragment),
        {IF, ET};
      {done, InitialScheduler, D, IE} ->
        {[], empty}
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
       idle_frontier = queue:from_list(IdleFrontier),
       planner = Planner,
       scheduling_start = SchedulingStart
      },
  controller_loop(InitialStatus).
 
get_schedulers(0, _) -> [];
get_schedulers(N, SchedulerNumbers) ->
  receive
    {scheduler, Pid} ->
      SchedulerId = maps:get(node(Pid), SchedulerNumbers),
      case SchedulerId of
        0 ->
          Pid ! {planner, SchedulerId};
        _ ->
          Pid ! {scheduler_number, SchedulerId}
      end,
      [{Pid, SchedulerId} | get_schedulers(N-1, SchedulerNumbers)]
  end.

controller_loop(#controller_status{
                   busy = Busy,
                   idle_frontier = IdleQueue,
                   planner_queue = PlannerQueue,
                   planner_status = PlannerStatus
                  } = Status) ->
  case 
    Busy =:= []
    andalso PlannerStatus =:= idle
    andalso queue:is_empty(PlannerQueue)
    andalso queue:is_empty(IdleQueue) 
  of 
    false ->
      controller_loop_aux(Status);
    true ->
      SchedulingEnd = erlang:monotonic_time(),
      %% TODO : remove this check
      %% empty = Status#controller_status.execution_tree, %% this does not hold true
      case Status#controller_status.execution_tree =/= empty of
        true ->
          ok;%%concuerror_scheduler:print_tree("", Status#controller_status.execution_tree);
        false ->
          ok
      end,
      #controller_status{
         schedulers_uptime = _Uptimes,
         idle = Idle,
         scheduling_start = SchedulingStart,
         planner = Planner
        } = Status,
      [Scheduler ! finish || Scheduler <- [Planner|Idle]],
      [receive finished -> ok end || _ <- [Planner|Idle]],
      receive
        {stop, Pid} ->
          %%SchedulingEnd = erlang:monotonic_time(),
          report_stats_parallel(Status, SchedulingStart, SchedulingEnd),
          Pid ! done
      end
  end.

controller_loop_aux(Status) ->
  #controller_status{
     busy = Busy,
     %% idle = Idle,
     execution_tree = ExecutionTree,
     idle_frontier = IdleFrontier,
     planner = Planner,
     planner_queue = PlannerQueue,
     planner_status = PlannerStatus
    } = Status,
  PlannedStatus =
    case PlannerStatus =:= idle andalso not queue:is_empty(PlannerQueue) of
      true ->
        {{value, NextFragment}, Rest} = queue:out(PlannerQueue),
        WuTUpdatedFragment =
          concuerror_scheduler:update_trace_from_exec_tree(NextFragment, ExecutionTree),
        Planner ! {plan, WuTUpdatedFragment},
        Status#controller_status{
          planner_queue = Rest,
          planner_status = busy
         };
      false ->
        Status
    end,
  NewStatus = assign_work(PlannedStatus),
  wait_scheduler_response(NewStatus).

wait_scheduler_response(Status) ->
  #controller_status{
     schedulers_uptime = Uptimes,
     busy = Busy,
     execution_tree = ExecutionTree,
     idle = Idle,
     idle_frontier = IdleFrontier,
     ownership_claims = OC,
     planner = Planner,
     planner_queue = PlannerQueue
    } = Status,
  %% io:fwrite("wait response~n"),
  io:fwrite("OC:~w~n", [OC]),
  receive
    {exploration_finished, Scheduler, ExploredFragment, Duration, IE} ->
      %% io:fwrite("expo finished~n"),
      {Scheduler, OldFragment} = lists:keyfind(Scheduler, 1, Busy),
      NewBusy = lists:keydelete(Scheduler, 1, Busy),
      NewIdle = [Scheduler|Idle],
      NewUptimes = update_scheduler_stopped(Scheduler, Uptimes, Duration, IE),
      NewPlannerQueue = queue:in(ExploredFragment, PlannerQueue),
      controller_loop(Status#controller_status{
                        schedulers_uptime = NewUptimes,
                        busy = NewBusy,
                        idle = NewIdle,
                        ownership_claims = OC + 1,
                        planner_queue = NewPlannerQueue
                       });
    {has_more, Planner, FullNewFragment, Duration, IE} ->
      %% io:fwrite("has more~n"),
      %% NewUptimes = update_scheduler_stopped(Scheduler, Uptimes, Duration, IE),
      io:fwrite("1~n"),
      NewExecutionTree =
        concuerror_scheduler:insert_new_trace(FullNewFragment, ExecutionTree),
      io:fwrite("2~n"),
      NewFragments =
        concuerror_scheduler:distribute_interleavings(FullNewFragment),
      io:fwrite("3~n"),
      NewIdleFrontier =
        case FullNewFragment =:= fragment_finished of
          false ->
            NewFragmentsQueue = queue:from_list(NewFragments),
            queue:join(IdleFrontier, NewFragmentsQueue);
          true ->
            exit(impossible),
            IdleFrontier
        end,
      controller_loop(Status#controller_status{
                        planner_status = idle,
                        execution_tree = NewExecutionTree,
                        idle_frontier = NewIdleFrontier
                       });
    {done, Scheduler, Duration, IE} ->
      %% io:fwrite("done~n"),
      %% NewUptimes = update_scheduler_stopped(Scheduler, Uptimes, Duration, IE),
      NewExecutionTree =
        ExecutionTree,
      %%concuerror_scheduler:update_execution_tree_done(CompletedFragment, ExecutionTree),
      controller_loop(Status#controller_status{
                        execution_tree = NewExecutionTree,
                        planner_status = idle
                       });
    {stop, Pid} ->
      %% io:fwrite("stop~n"),
      SchedulingEnd = erlang:monotonic_time(),
      %% TODO : remove this check
      %% empty = Status#controller_status.execution_tree, %% this does not hold true
      #controller_status{
         schedulers_uptime = _Uptimes,
         idle = Idle,
         busy = Busy,
         scheduling_start = SchedulingStart,
         planner = Planner
        } = Status,
      BusyPids = [Pid || {Pid, _} <- Busy],
      Schedulers = [Scheduler || Scheduler <- [Planner|Idle] ++ BusyPids, is_non_local_process_alive(Scheduler)],
      [Scheduler ! finish || Scheduler <- Schedulers],
      [receive finished -> ok end || _ <- Schedulers],
      report_stats_parallel(Status, SchedulingStart, SchedulingEnd),
      Pid ! done;
    _Other ->
      exit({impossible, _Other})
  end.

is_non_local_process_alive(Pid) ->
  case rpc:pinfo(Pid, status) of
    undefined ->
      false;
    {status, Exited}
      when Exited =:= exiting;
           Exited =:= garbage_collecting ->
      false;
    _ ->
      true
  end.

%% start_schedulers([], NewIdle, NewBusy) -> {NewIdle, NewBusy};
%% start_schedulers([State|RestStates], [Scheduler|RestSchedulers], Busy) ->
%%   Scheduler ! {explore, State},
%%   start_schedulers(RestStates, RestSchedulers, [Scheduler|Busy]).

assign_work(Status) ->
  #controller_status{
     busy = Busy,
     schedulers_uptime = Uptimes,
     idle = Idle,
     idle_frontier = IdleFrontier
    } = Status,
  case Idle =:= [] orelse queue:is_empty(IdleFrontier) of
    true ->
      Status;
    false ->
      [Scheduler|RestIdle] = Idle,
      {{value, Fragment}, RestIdleFrontier} = queue:out(IdleFrontier),
      Scheduler ! {explore, Fragment},
      NewUptimes = Uptimes, %% update_scheduler_started(Scheduler, Uptimes),
      UpdatedStatus =
        Status#controller_status{
          busy = [{Scheduler, Fragment}|Busy],
          schedulers_uptime = NewUptimes,
          idle = RestIdle,
          idle_frontier = RestIdleFrontier
         },
      assign_work(UpdatedStatus)
  end.
  
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
     ownership_claims = OC
    } = Status,
  %% io:fwrite("Ownership Claims: ~B~n", [OC]), 
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
