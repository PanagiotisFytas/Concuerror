%%% @doc Concuerror's scheduler component
%%%
%%% concuerror_scheduler is the main driver of interleaving
%%% exploration.  A rough trace through it is the following:

%%% The entry point is `concuerror_scheduler:run/1` which takes the
%%% options and initializes the exploration, spawning the main
%%% process.  There are plenty of state info that are kept in the
%%% `#scheduler_state` record the most important of which being a list
%%% of `#trace_state` records, recording events in the exploration.
%%% This list corresponds more or less to "E" in the various DPOR
%%% papers (representing the execution trace).

%%% The logic of the exploration goes through `explore_scheduling/1`,
%%% which in turn calls 'explore/1'. Both functions are fairly clean:
%%% as long as there are more processes that can be executed and yield
%%% events, `get_next_event/1` will be returning `ok`, after executing
%%% one of them and doing all necessary updates to the state (adding
%%% new `#trace_state`s, etc).  If `get_next_event/1` returns `none`,
%%% we are at the end of an interleaving (either due to no more
%%% enabled processes or due to "sleep set blocking") and can do race
%%% analysis and report any errors found in the interleaving.  Race
%%% analysis is contained in `plan_more_interleavings/1`, reporting
%%% whether the current interleaving was buggy is contained in
%%% `log_trace/1` and resetting most parts to continue exploration is
%%% contained in `has_more_to_explore/1`.

%%% Focusing on `plan_more_interleavings`, it is composed out of two
%%% steps: first (`assign_happens_before/3`) we assign a
%%% happens-before relation to all events to be able to detect when
%%% races are reversible or not (if two events are dependent not only
%%% directly but also via a chain of dependent events then the race is
%%% not reversible) and then (`plan_more_interleavings/3`) for each
%%% event (`more_interleavings_for_event/6`) we do an actual race
%%% analysis, adding initials or wakeup sequences in appropriate
%%% places in the list of `#trace_state`s.

-module(concuerror_scheduler).

%% User interface
-export([run/1, explain_error/1]).

-export_type([ interleaving_error/0,
               interleaving_error_tag/0,
               interleaving_result/0,
               unique_id/0
             ]).

%% Controller Interface
-export([distribute_interleavings/2]).
-export([ initialize_execution_tree/1,
          update_execution_tree_done/2,
          update_execution_tree/3
        ]).
-export([print_tree/2]).

-export([make_term_transferable/1]).

-export_type([reduced_scheduler_state/0,
              execution_tree/0
             ]).

%% =============================================================================
%% DATA STRUCTURES & TYPES
%% =============================================================================

-include("concuerror.hrl").

%%------------------------------------------------------------------------------

-type interleaving_id() :: pos_integer().

-ifdef(BEFORE_OTP_17).
-type clock_map()           :: dict().
-type message_event_queue() :: queue().
-else.
-type vector_clock()        :: #{actor() => index()}.
-type clock_map()           :: #{actor() => vector_clock()}.
-type message_event_queue() :: queue:queue(#message_event{}).
-endif.

-record(backtrack_entry, {
          conservative = false :: boolean(),
          event                :: event(),
          origin = 1           :: interleaving_id(),
          ownership = disputed :: owned | not_owned | disputed,
          wakeup_tree = []     :: event_tree()
         }).

-type event_tree() :: [#backtrack_entry{}].

-type channel_actor() :: {channel(), message_event_queue()}.

-type unique_id() :: {interleaving_id(), index()}.

-record(trace_state, {
          actors                        :: [pid() | channel_actor()],
          clock_map        = empty_map():: clock_map(),
          done             = []         :: [event()],
          enabled          = []         :: [pid() | channel_actor()],
          index                         :: index(),
          ownership = true              :: boolean(),
          unique_id                     :: unique_id(),
          previous_actor   = 'none'     :: 'none' | actor(),
          scheduling_bound              :: concuerror_options:bound(),
          sleep_set        = []         :: [event()],
          wakeup_tree      = []         :: event_tree()
         }).

-type trace_state() :: #trace_state{}.

-type interleaving_result() ::
        'ok' | 'sleep_set_block' | {[interleaving_error()], [event()]}.

-type interleaving_error_tag() ::
        'abnormal_exit' |
        'abnormal_halt' |
        'deadlock' |
        'depth_bound'.

-type interleaving_error() ::
        {'abnormal_exit', {index(), pid(), term(), [term()]}} |
        {'abnormal_halt', {index(), pid(), term()}} |
        {'deadlock', [pid()]} |
        {'depth_bound', concuerror_options:bound()} |
        'fatal'.

-type scope() :: 'all' | [pid()].

%% DO NOT ADD A DEFAULT VALUE IF IT WILL ALWAYS BE OVERWRITTEN.
%% Default values for fields should be specified in ONLY ONE PLACE.
%% For e.g., user options this is normally in the _options module.
-record(scheduler_state, {
          assertions_only              :: boolean(),
          assume_racing                :: assume_racing_opt(),
          depth_bound                  :: pos_integer(),
          dpor                         :: concuerror_options:dpor(),
          entry_point                  :: mfargs(),
          estimator                    :: concuerror_estimator:estimator(),
          first_process                :: pid(),
          ignore_error                 :: [{interleaving_error_tag(), scope()}],
          interleaving_bound           :: concuerror_options:bound(),
          interleaving_errors          :: [interleaving_error()],
          interleaving_id              :: interleaving_id(),
          keep_going                   :: boolean(),
          logger                       :: pid(),
          last_scheduled               :: pid(),
          need_to_replay               :: boolean(),
          non_racing_system            :: [atom()],
          origin                       :: interleaving_id(),
          print_depth                  :: pos_integer(),
          processes                    :: processes(),
          receive_timeout_total        :: non_neg_integer(),
          report_error                 :: [{interleaving_error_tag(), scope()}],
          scheduling                   :: concuerror_options:scheduling(),
          scheduling_bound_type        :: concuerror_options:scheduling_bound_type(),
          show_races                   :: boolean(),
          strict_scheduling            :: boolean(),
          timeout                      :: timeout(),
          trace                        :: [trace_state()],
          treat_as_normal              :: [atom()],
          use_receive_patterns         :: boolean(),
          use_sleep_sets               :: boolean(),
          use_unsound_bpor             :: boolean(),
          budget = undefined           :: undefined | integer(),
          controller                   :: pid() | undefined,
          parallel                     :: boolean(),
          replay_mode = pseudo         :: pseudo | actual,
          scheduler_number = undefined :: pos_integer() | undefined,
          start_time = undefined       :: undefined | integer(),
          interleavings_explored = 0   :: integer()
          %% TODO check if replay_mode is needed 
         }).
%-------------------------------------------------------------------------------
-ifdef(BEFORE_OTP_17).
-type vector_clock_transferable()        :: [{actor_transferable(), index()}].
-type clock_map_transferable()           :: [{actor_transferable(), vector_clock_transferable()}].
-type message_event_queue_transferable() :: queue().
-else.
-type vector_clock_transferable()        :: [{actor_transferable(), index()}].
-type clock_map_transferable()           :: [{actor_transferable(), vector_clock_transferable()}].
-type message_event_queue_transferable() :: queue:queue(#message_event_transferable{}).
-endif.



-record(backtrack_entry_transferable, {
          conservative = false  :: boolean(),
          event                 :: event_transferable(),
          origin = 1            :: interleaving_id(),          
          ownership = not_owned :: owned | not_owned | disputed,
          wakeup_tree = []      :: event_tree_transferable()
         }).

-type event_tree_transferable() :: [#backtrack_entry_transferable{}].

-type channel_actor_transferable() :: {channel_transferable(),
                                       message_event_queue_transferable()}.

-record(trace_state_transferable, {
          actors                        :: [string() | channel_actor_transferable() ],
          clock_map        = []         :: clock_map_transferable(),
          done             = []         :: [event_transferable()],
          enabled          = []         :: [string() | channel_actor_transferable()],
          index                         :: index(),
          ownership = false             :: boolean(),
          unique_id                     :: unique_id(),
          previous_actor   = 'none'     :: 'none' | actor_transferable(),
          scheduling_bound              :: concuerror_options:bound(),
          sleep_set        = []         :: [event_transferable()],
          wakeup_tree      = []         :: event_tree_transferable()
         }).

%% -type trace_state_transferable() :: #trace_state_transferable{}.

-record(reduced_scheduler_state, {
          backtrack_size  :: pos_integer(),
          dpor            :: concuerror_options:dpor(),
          interleaving_id :: interleaving_id(),
          last_scheduled  :: pid(),
          need_to_replay  :: boolean(),
          origin          :: interleaving_id(),
          processes_ets_tables :: [atom()],
          safe            :: boolean(),
          trace           :: [trace_state()]
         }).

-type reduced_scheduler_state() :: #reduced_scheduler_state{}.
%% TODO: add what was previous called exploring and warnings 

-record(execution_tree, {
          children         = [] :: [execution_tree()],
          finished_children       = [] :: [event_transferable()],
          event                        :: event_transferable(),
          fragments_acessing_node = 1  :: non_neg_integer(),
          next_wakeup_tree        = [] :: event_tree_transferable()
         }).

-type execution_tree() :: #execution_tree{} | empty.

%-------------------------------------------------------------------------------

%% =============================================================================
%% LOGIC (high level description of the exploration algorithm)
%% =============================================================================

-spec run(concuerror_options:options()) -> ok.

run(Options) ->
  process_flag(trap_exit, true),
  put(bound_exceeded, false),
  FirstProcess = concuerror_callback:spawn_first_process(Options),
  EntryPoint = ?opt(entry_point, Options),
  Timeout = ?opt(timeout, Options),
  ok =
    concuerror_callback:start_first_process(FirstProcess, EntryPoint, Timeout),
  SchedulingBound = ?opt(scheduling_bound, Options, infinity),
  InitialTrace =
    #trace_state{
       actors = [FirstProcess],
       enabled = [E || E <- [FirstProcess], enabled(E)],
       index = 1,
       scheduling_bound = SchedulingBound,
       unique_id = {1, 1}
      },
  Logger = ?opt(logger, Options),
  {SchedulingBoundType, UnsoundBPOR} =
    case ?opt(scheduling_bound_type, Options) of
      ubpor -> {bpor, true};
      Else -> {Else, false}
    end,
  {IgnoreError, ReportError} =
    generate_filtering_rules(Options, FirstProcess),
  Controller = ?opt(controller, Options),
  Parallel = ?opt(parallel, Options),
  InitialState =
    #scheduler_state{
       assertions_only = ?opt(assertions_only, Options),
       assume_racing = {?opt(assume_racing, Options), Logger},
       depth_bound = ?opt(depth_bound, Options) + 1,
       dpor = ?opt(dpor, Options),
       entry_point = EntryPoint,
       estimator = ?opt(estimator, Options),
       first_process = FirstProcess,
       ignore_error = IgnoreError,
       interleaving_bound = ?opt(interleaving_bound, Options),
       interleaving_errors = [],
       interleaving_id = 1,
       keep_going = ?opt(keep_going, Options),
       last_scheduled = FirstProcess,
       logger = Logger,
       need_to_replay = false,
       non_racing_system = ?opt(non_racing_system, Options),
       origin = 1,
       print_depth = ?opt(print_depth, Options),
       processes = Processes = ?opt(processes, Options),
       receive_timeout_total = 0,
       report_error = ReportError,
       scheduling = ?opt(scheduling, Options),
       scheduling_bound_type = SchedulingBoundType,
       show_races = ?opt(show_races, Options),
       strict_scheduling = ?opt(strict_scheduling, Options),
       trace = [InitialTrace],
       treat_as_normal = ?opt(treat_as_normal, Options),
       timeout = Timeout,
       use_receive_patterns = ?opt(use_receive_patterns, Options),
       use_sleep_sets = not ?opt(disable_sleep_sets, Options),
       use_unsound_bpor = UnsoundBPOR,
       controller = Controller,
       parallel = Parallel
      },
  case SchedulingBound =:= infinity of
    true ->
      ?unique(Logger, ?ltip, msg(scheduling_bound_tip), []);
    false ->
      ok
  end,
  concuerror_logger:plan(Logger),
  ?time(Logger, "Exploration start"),
  case Parallel of
    false ->
      StartTime = erlang:monotonic_time(),
      Ret = explore_scheduling(InitialState),
      concuerror_callback:cleanup_processes(Processes),
      EndTime = erlang:monotonic_time(),
      concuerror_controller:report_stats(maps:new(), StartTime, EndTime),
      Ret;
    true ->
      Controller ! {scheduler, self()},
      Ret =
        receive
          {scheduler_number, SchedulerNumber} ->
            FixedState =
              InitialState#scheduler_state{
                scheduler_number = SchedulerNumber,
                replay_mode = pseudo
               }, %% TODO fix this
            loop(FixedState)
        end,
      concuerror_callback:cleanup_processes(Processes),
      Controller ! finished,
      Ret
  end.

%%------------------------------------------------------------------------------

explore_scheduling(State) ->
  UpdatedState = explore(State),
  LogState = log_trace(UpdatedState),
  RacesDetectedState = plan_more_interleavings(LogState),
  {HasMore, NewState} = has_more_to_explore(RacesDetectedState),
  %% io:fwrite("State ~n~p~n", [NewState]), %% TODO remove
  case HasMore of
    true -> explore_scheduling(NewState);
    false -> ok%exit(UpdatedState) %% TODO put ok
  end.

%% TODO figure out how to check node ownership:
%% when I receive a trace from the controller, I do not own any trace_state from that trace, but I own
%% the backtrack entries that I receive. Also, I own any tracestate that I generate
%% and do not own any backtrack entries I generate. So I will continue
%% exploring this interleaving: 
%% (1) If I own the trace_state (that has the next backtrack entry to be explored)
%% (owning a trace_state means that it was generated by me when exploring a backtrack 
%% that I owned and therefore it is a "successor" of the backtrack entry that I own) 
%% or (2) If I own the(maybe "a" instead of "the") backtrack entry on a trace_state that I do not own 
%% (then this is a node that I was assigned to explore from the controller) 
%% Otherwise, I will have to ask the controller for the backtrack entries that I own and 
%% those that I do not. Those that I own will be marked so and those that I do not will perhaps be
%% added on my sleep set.
%% In case I need to stop my exploration due to ownership issues, I will have to ask the controlller
%% to claim ownership of the backtrack entries that I do not own and that belong to the traces that I do not own

%% TODO fix ownership when distributing interleavings, maybe use revert_trace
explore_scheduling_parallel(State) ->
  UpdatedState = explore(State),
  #scheduler_state{
     interleavings_explored = IE
    } = State,
  LogState = log_trace(UpdatedState),
  RacesDetectedState = plan_more_interleavings(LogState),
  {HasMore, NewState} = has_more_to_explore(RacesDetectedState),
  Controller = NewState#scheduler_state.controller,
  Duration = erlang:monotonic_time(?time_unit) - State#scheduler_state.start_time,
  case HasMore of
    true ->
      %% TODO add termination when testing something that I do not own
      %% concuerror_callback:reset_processes(State#scheduler_state.processes),
      %% LoadBalancedState = unload_work(NewState),
      case own_next_interleaving(NewState) of
        true ->
          #scheduler_state{
             budget = Budget
            } = State,
          case Duration > Budget of
            false ->
              explore_scheduling_parallel(NewState#scheduler_state{
                                            interleavings_explored = IE + 1,
                                            replay_mode = pseudo
                                           });
            true ->
              Controller ! {budget_exceeded, self(), make_state_transferable(NewState), Duration, IE+1}
          end;
        false ->
          Controller ! {claim_ownership, self(), make_state_transferable(NewState), Duration, IE+1}
      end;
    false ->
      case NewState#scheduler_state.scheduler_number of
        2 ->
          io:fwrite("EXITSTATE ~n~p~n", [NewState]);
        _ ->
          ok
      end,
      %% concuerror_callback:reset_processes(State#scheduler_state.processes),
      Controller ! {done, self(), Duration, IE+1}
  end,
  NewState.

own_next_interleaving(#scheduler_state{trace = [TraceState|_]} = _State) ->
  #trace_state{
     ownership = Ownership,
     wakeup_tree = WuT
    } = TraceState,
  case Ownership of
    true ->
      true;
    false ->
      %% [BacktrackEntry|_] = WuT,
      %% case BacktrackEntry#backtrack_entry.ownership of
      %%   owned ->
      %%     true;
      %%   disputed ->
      %%     false;
      %%   not_owned ->
      %%     %% TODO :
      %%     %% this should be made impossible by filtering (or sorting) not_owned entries
      %%     %% in the find_prefix function
      %%     %% exit(impossible)
      %%     io:fwrite("IMPOSSIBLE!!!!!!~n",[]),
      %%     false
      %% end
      case split_wut_at_ownership(WuT, disputed) of
        {WuT, []} ->
          case split_wut_at_ownership(WuT, owned) of
            {WuT, []} ->
              exit(impossible);
            _ ->
              true
          end;
        {NotDisputed, _} ->
          case split_wut_at_ownership(NotDisputed, owned) of
            {NotDisputed, []} ->
              false;
            _ ->
              true
          end
      end
  end.        

loop(State) ->
  receive
    {start, Budget} ->
      FixedState =
        State#scheduler_state{
          interleavings_explored = 0,
          budget = Budget,
          start_time = erlang:monotonic_time(?time_unit)
         },
      NewState = explore_scheduling_parallel(FixedState),
      loop(NewState);
    {explore, TransferableState, Budget} ->
      StartTime = erlang:monotonic_time(?time_unit),
      ReceivedState = revert_state(State, TransferableState),
      FixedState =
        ReceivedState#scheduler_state{
          interleavings_explored = 0,
          budget = Budget,
          replay_mode = actual,
          start_time = StartTime
         },
      NewState = explore_scheduling_parallel(FixedState),
      loop(NewState);
    %% {explore, non_stop, TransferableState} ->
    %%   ReceivedState = revert_state(State, TransferableState),
    %%   FixedState =
    %%     ReceivedState#scheduler_state{
    %%       replay_mode = actual
    %%      },
    %%   explore_scheduling(FixedState), %% TODO fix this
    %%   #scheduler_state{controller = Controller} = State,
    %%   Controller ! {done, self()},
    %%   loop(State);
    finish ->
      ok
  end.

%%------------------------------------------------------------------------------

size_of_backtrack([]) -> 0;
size_of_backtrack([TraceState|Rest]) ->
  size_of_backtrack_aux(TraceState#trace_state.wakeup_tree) + size_of_backtrack(Rest).

size_of_backtrack_aux([]) -> 0;
size_of_backtrack_aux([BacktrackEntry|Rest]) ->
  Count =
    case determine_ownership(BacktrackEntry) of
      owned ->
        1;
      not_owned ->
        0;
      disputed ->
        1
    end,
  Count
  %  + size_of_backtrack_aux(BacktrackEntry#backtrack_entry.wakeup_tree)
    + size_of_backtrack_aux(Rest).

size_of_backtrack_transferable([]) -> 0;
size_of_backtrack_transferable([TraceState|Rest]) ->
  size_of_backtrack_aux(TraceState#trace_state_transferable.wakeup_tree) + size_of_backtrack_transferable(Rest).


%% %% all this needs to change this is not anymore the work of scheduler
%% unload_work(State) ->
%%   #scheduler_state{
%%      controller = Controller,
%%      trace = Trace,
%%      dpor = DPOR
%%     } = State,
%%   BacktrackPoints = size_of_backtrack(State#scheduler_state.trace),
%%   LoadBalancedState =
%%     case BacktrackPoints > ?balancing_limit
%%       %% andalso State#scheduler_state.scheduler_number =:= 1
%%       %% andalso State#scheduler_state.interleaving_id =:= 1 
%%     of
%%       false ->
%%         State;
%%       true ->
%%         Controller ! {request_idle, self()},
%%         receive
%%           no_idle_schedulers ->
%%             State;
%%           {scheduler, IdleScheduler} ->
%%             {MyTrace, UnloadedTrace} = distribute_interleavings(Trace),
%%             UnloadedState = State#scheduler_state{trace = UnloadedTrace},
%%             IdleScheduler ! {explore, make_state_transferable(UnloadedState)},
%%             NewState = State#scheduler_state{trace = MyTrace},
%%             unload_work(NewState)
%%         end
%%     end,
%%   LoadBalancedState.

%%------------------------------------------------------------------------------

explore(State) ->
  {Status, UpdatedState} =
    try
      get_next_event(State)
    catch
      C:R ->
        S = erlang:get_stacktrace(),
        {{crash, C, R, S}, State}
    end,
  case Status of
    ok -> explore(UpdatedState);
    none -> UpdatedState;
    {crash, Class, Reason, Stack} ->
      FatalCrashState =
        add_error(fatal, discard_last_trace_state(UpdatedState)),
      catch log_trace(FatalCrashState),
      %% TODO fix this i.e the way the controller handles 
      %% scheduler crashes
      case State#scheduler_state.parallel of
        true ->
          State#scheduler_state.controller ! {done, self()};
        false ->
         ok
      end,
      erlang:raise(Class, Reason, Stack)
  end.

%%------------------------------------------------------------------------------

log_trace(#scheduler_state{logger = Logger} = State) ->
  Log =
    case filter_errors(State) of
      [] -> none;
      Errors ->
        case proplists:get_value(sleep_set_block, Errors) of
          {Origin, Sleep} ->
            case State#scheduler_state.dpor =:= optimal of
              true -> ?crash({optimal_sleep_set_block, Origin, Sleep});
              false -> ok
            end,
            sleep_set_block;
          undefined ->
            #scheduler_state{trace = Trace} = State,
            Fold =
              fun(#trace_state{done = [A|_], index = I}, Acc) ->
                  [{I, A}|Acc] %% TODO add and remove the make_term_transferable !!!
              end,
            TraceInfo = lists:foldl(Fold, [], Trace),
            {lists:reverse(Errors), TraceInfo}
        end
    end,
  concuerror_logger:complete(Logger, Log),
  case Log =/= none andalso Log =/= sleep_set_block of
    true when not State#scheduler_state.keep_going ->
      ?unique(Logger, ?lerror, msg(stop_first_error), []),
      State#scheduler_state{trace = []};
    Other ->
      case Other of
        true ->
          ?unique(Logger, ?linfo, "Continuing after error (-k)~n", []);
        false ->
          ok
      end,
      InterleavingId = State#scheduler_state.interleaving_id,
      NextInterleavingId = InterleavingId + 1,
      NextState =
        State#scheduler_state{
          interleaving_errors = [],
          interleaving_id = NextInterleavingId,
          receive_timeout_total = 0
         },
      case NextInterleavingId =< State#scheduler_state.interleaving_bound of
        true -> NextState;
        false ->
          UniqueMsg = "Reached interleaving bound (~p)~n",
          ?unique(Logger, ?lwarning, UniqueMsg, [InterleavingId]),
          NextState#scheduler_state{trace = []}
      end
  end.

%%------------------------------------------------------------------------------

generate_filtering_rules(Options, FirstProcess) ->
  IgnoreErrors = ?opt(ignore_error, Options),
  OnlyFirstProcessErrors = ?opt(first_process_errors_only, Options),
  case OnlyFirstProcessErrors of
    false -> {[{IE, all} || IE <- IgnoreErrors], []};
    true ->
      AllCategories = [abnormal_exit, abnormal_halt, deadlock],
      Ignored = [{IE, all} || IE <- AllCategories],
      Reported = [{IE, [FirstProcess]} || IE <- AllCategories -- IgnoreErrors],
      {Ignored, Reported}
  end.

filter_errors(State) ->
  #scheduler_state{
     ignore_error = Ignored,
     interleaving_errors = UnfilteredErrors,
     logger = Logger,
     report_error = Reported
    } = State,
  TaggedErrors = [{true, E} || E <- UnfilteredErrors],
  IgnoredErrors = update_all_tags(TaggedErrors, Ignored, false),
  ReportedErrors = update_all_tags(IgnoredErrors, Reported, true),
  FinalErrors = [E || {true, E} <- ReportedErrors],
  case FinalErrors =/= UnfilteredErrors of
    true ->
      UniqueMsg = "Some errors were ignored ('--ignore_error').~n",
      ?unique(Logger, ?lwarning, UniqueMsg, []);
    false -> ok
  end,
  FinalErrors.

update_all_tags([], _, _) -> [];
update_all_tags(TaggedErrors, [], _) -> TaggedErrors;
update_all_tags(TaggedErrors, Rules, Value) ->
  [update_tag(E, Rules, Value) || E <- TaggedErrors].

update_tag({OldTag, Error}, Rules, NewTag) ->
  RuleAppliesPred = fun(Rule) -> rule_applies(Rule, Error) end,
  case lists:any(RuleAppliesPred, Rules) of
    true -> {NewTag, Error};
    false -> {OldTag, Error}
  end.

rule_applies({Tag, Scope}, {Tag, _} = Error) ->
  scope_applies(Scope, Error);
rule_applies(_, _) -> false.

scope_applies(all, _) -> true;
scope_applies(Pids, ErrorInfo) ->
  case ErrorInfo of
    {deadlock, Deadlocked} ->
      DPids = [element(1, D) || D <- Deadlocked],
      DPids -- Pids =/= DPids;
    {abnormal_exit, {_, Pid, _, _}} -> lists:member(Pid, Pids);
    {abnormal_halt, {_, Pid, _}} -> lists:member(Pid, Pids);
    _ -> false
  end.

discard_last_trace_state(State) ->
  #scheduler_state{trace = [_|Trace]} = State,
  State#scheduler_state{trace = Trace}.

add_error(Error, State) ->
  add_errors([Error], State).

add_errors(Errors, State) ->
  #scheduler_state{interleaving_errors = OldErrors} = State,
  State#scheduler_state{interleaving_errors = Errors ++ OldErrors}.

%%------------------------------------------------------------------------------

get_next_event(
  #scheduler_state{
     depth_bound = Bound,
     logger = Logger,
     trace = [#trace_state{index = Bound}|_]} = State) ->
  ?unique(Logger, ?lwarning, msg(depth_bound_reached), []),
  NewState =
    add_error({depth_bound, Bound - 1}, discard_last_trace_state(State)),
  {none, NewState};
get_next_event(#scheduler_state{
                  logger = _Logger,
                  parallel = Parallel,
                  trace = [#trace_state{ownership = Own} = Last|Rest]
                 } = State) 
  when Parallel andalso not Own->
  #trace_state{
     done = Done,
     index = _I,
     sleep_set = SleepSet,
     wakeup_tree = WakeupTree
    } = Last,
  case WakeupTree of
    [] ->
      Event = #event{label = make_ref()},
      get_next_event(Event, State);
    _ -> 
      case split_wut_at_ownership(WakeupTree, owned) of
        {Prefix, [#backtrack_entry{event = Event, origin = N} = H|Suffix]} ->
          ?debug(
             _Logger,
             "New interleaving detected in ~p (diverge @ ~p)~n",
             [N, _I]),
          NewDone =
            case Done of
              [] ->
                [];
              [Head|Tail] ->
                [Head|[Entry#backtrack_entry.event || Entry <- Prefix] ++ Tail]
            end,
          UpdatedLast =
            Last#trace_state{
              done = NewDone,
              sleep_set =
                [Entry#backtrack_entry.event || Entry <- Prefix] ++ SleepSet,
              wakeup_tree = [H|Suffix]
             },
          get_next_event(Event, State#scheduler_state{origin = N, trace = [UpdatedLast|Rest]});
        {WakeupTree, []} ->
          Event2 = #event{label = make_ref()},
          get_next_event(Event2, State)
      end
  end;
get_next_event(#scheduler_state{logger = _Logger, trace = [Last|_]} = State) ->
  #trace_state{index = _I, wakeup_tree = WakeupTree} = Last,
  case WakeupTree of
    [] ->
      Event = #event{label = make_ref()},
      get_next_event(Event, State);
    [#backtrack_entry{event = Event, origin = N}|_] ->
      ?debug(
         _Logger,
         "New interleaving detected in ~p (diverge @ ~p)~n",
         [N, _I]),
      get_next_event(Event, State#scheduler_state{origin = N})
  end.

get_next_event(Event, MaybeNeedsReplayState) ->
  State = replay(MaybeNeedsReplayState),
  #scheduler_state{trace = [Last|_]} = State,
  #trace_state{actors = Actors, sleep_set = SleepSet} = Last,
  SortedActors = schedule_sort(Actors, State),
  #event{actor = Actor, label = Label} = Event,
  #trace_state{index = I} = Last,
  case Actor =:= undefined of
    true ->
      AvailableActors = filter_sleep_set(SleepSet, SortedActors),
      free_schedule(Event, AvailableActors, State);
    false ->
      #scheduler_state{
         print_depth = PrintDepth,
         replay_mode = ReplayMode
        } = State,
      #trace_state{index = I} = Last,
      false = lists:member(Actor, SleepSet),
      OkUpdatedEvent =
        case Label =/= undefined of
          true ->
            %% io:fwrite("Node :~p 1111111111111111~n",[node()]), %% TODO remove
            %% TODO check why this seems to work
            NewEvent =
              case ReplayMode of
                pseudo ->
                  get_next_event_backend(Event, State);
                actual ->
                  EventInfo = Event#event.event_info,
                  case EventInfo of
                    #builtin_event{} ->
                      FixedEventInfo = EventInfo#builtin_event{actual_replay = true},
                      {ok, NewE} = get_next_event_backend(Event#event{event_info = FixedEventInfo}, State),
                      NewEInfo = NewE#event.event_info,
                      FixedNewEInfo = NewEInfo#builtin_event{actual_replay = false},
                      {ok, NewE#event{event_info = FixedNewEInfo}};
                    _ ->
                      get_next_event_backend(Event#event{event_info = undefined}, State)
                  end
              end,
              try
                  case ReplayMode of
                    pseudo ->
                      {ok, Event} = NewEvent;
                    actual ->
                      {ok, MaybeEvent} = NewEvent,
                      true = logically_equal(State, Event, MaybeEvent),
                      NewEvent
                  end
              catch
                  _:_ ->
                    New =
                      case NewEvent of
                        {ok, E} -> E;
                        _ -> NewEvent
                      end,
                    Reason = {replay_mismatch, I, Event, New, PrintDepth},
                    ?crash(Reason)
              end;
          false ->
            %% io:fwrite("Node :~p 2222222222222222222~n",[node()]), %% TODO remove
            %% Last event = Previously racing event = Result may differ.
            ResetEvent = reset_event(Event),
            get_next_event_backend(ResetEvent, State)
        end,
      case OkUpdatedEvent of
        {ok, UpdatedEvent} ->
          update_state(UpdatedEvent, State);
        retry ->
          BReason = {blocked_mismatch, I, Event, PrintDepth},
          ?crash(BReason)
      end
  end.

filter_sleep_set([], AvailableActors) -> AvailableActors;
filter_sleep_set([#event{actor = Actor}|SleepSet], AvailableActors) ->
  NewAvailableActors =
    case ?is_channel(Actor) of
      true -> lists:keydelete(Actor, 1, AvailableActors);
      false -> lists:delete(Actor, AvailableActors)
    end,
  filter_sleep_set(SleepSet, NewAvailableActors).

schedule_sort([], _State) -> [];
schedule_sort(Actors, State) ->
  #scheduler_state{
     last_scheduled = LastScheduled,
     scheduling = Scheduling,
     strict_scheduling = StrictScheduling
    } = State,
  Sorted =
    case Scheduling of
      oldest -> Actors;
      newest -> lists:reverse(Actors);
      round_robin ->
        Split = fun(E) -> E =/= LastScheduled end,    
        {Pre, Post} = lists:splitwith(Split, Actors),
        Post ++ Pre
    end,
  case StrictScheduling of
    true when Scheduling =:= round_robin ->
      [LastScheduled|Rest] = Sorted,
      Rest ++ [LastScheduled];
    false when Scheduling =/= round_robin ->
      [LastScheduled|lists:delete(LastScheduled, Sorted)];
    _ -> Sorted
  end.

free_schedule(Event, Actors, State) ->
  #scheduler_state{
     dpor = DPOR,
     estimator = Estimator,
     logger = Logger,
     scheduling_bound_type = SchedulingBoundType,
     trace = [Last|Prev]
    } = State,
  case DPOR =/= none of
    true -> free_schedule_1(Event, Actors, State);
    false ->
      Enabled = [A || A <- Actors, enabled(A)],
      ToBeExplored =
        case SchedulingBoundType =:= delay of
          false -> Enabled;
          true ->
            #trace_state{scheduling_bound = SchedulingBound} = Last,
            ?debug(Logger, "Select ~p of ~p~n", [SchedulingBound, Enabled]),
            lists:sublist(Enabled, SchedulingBound + 1)
        end,
      case ToBeExplored < Enabled of
        true -> bound_reached(Logger);
        false -> ok
      end,
      Eventify = [maybe_prepare_channel_event(E, #event{}) || E <- ToBeExplored],
      FullBacktrack = [#backtrack_entry{event = Ev} || Ev <- Eventify],
      case FullBacktrack of
        [] -> ok;
        [_|L] ->
          _ = [concuerror_logger:plan(Logger) || _ <- L],
          Index = Last#trace_state.index,
          _ = [concuerror_estimator:plan(Estimator, Index) || _ <- L],
          ok
      end,
      NewLast = Last#trace_state{wakeup_tree = FullBacktrack},
      NewTrace = [NewLast|Prev],
      NewState = State#scheduler_state{trace = NewTrace},
      free_schedule_1(Event, Actors, NewState)
  end.

enabled({_,_}) -> true;
enabled(P) -> concuerror_callback:enabled(P).

free_schedule_1(Event, [Actor|_], State) when ?is_channel(Actor) ->
  %% Pending messages can always be sent
  PrepEvent = maybe_prepare_channel_event(Actor, Event),
  {ok, FinalEvent} = get_next_event_backend(PrepEvent, State),
  update_state(FinalEvent, State);
free_schedule_1(Event, [P|ActiveProcesses], State) ->
  case get_next_event_backend(Event#event{actor = P}, State) of
    retry -> free_schedule_1(Event, ActiveProcesses, State);
    {ok, UpdatedEvent} -> update_state(UpdatedEvent, State)
  end;
free_schedule_1(_Event, [], State) ->
  %% Nothing to do, trace is completely explored
  #scheduler_state{logger = _Logger, trace = [Last|_]} = State,
  #trace_state{actors = Actors, sleep_set = SleepSet} = Last,
  NewErrors =
    case SleepSet =/= [] of
      true ->
        ?debug(_Logger, "Sleep set block:~n ~p~n", [SleepSet]),
        [{sleep_set_block, {State#scheduler_state.origin, SleepSet}}];
      false ->
        case concuerror_callback:collect_deadlock_info(Actors) of
          [] -> [];
          Info ->
            ?debug(_Logger, "Deadlock: ~p~n", [[element(1, I) || I <- Info]]),
            [{deadlock, Info}]
        end
    end,
  {none, add_errors(NewErrors, discard_last_trace_state(State))}.

maybe_prepare_channel_event(Actor, Event) ->
  case ?is_channel(Actor) of
    false -> Event#event{actor = Actor};
    true ->
      {Channel, Queue} = Actor,
      MessageEvent = queue:get(Queue),
      Event#event{actor = Channel, event_info = MessageEvent}
  end.      

reset_event(#event{actor = Actor, event_info = EventInfo}) ->
  ResetEventInfo =
    case ?is_channel(Actor) of
      true -> EventInfo;
      false -> undefined
    end,
  #event{
     actor = Actor,
     event_info = ResetEventInfo,
     label = make_ref()
    }.

%%------------------------------------------------------------------------------

update_state(#event{actor = Actor} = Event, State) ->
  #scheduler_state{
     estimator = Estimator,
     logger = Logger,
     scheduling_bound_type = SchedulingBoundType,
     trace = [Last|Prev],
     use_sleep_sets = UseSleepSets
    } = State,
  #trace_state{
     actors      = Actors,
     done        = RawDone,
     index       = Index,
     previous_actor = PreviousActor,
     scheduling_bound = SchedulingBound,
     sleep_set   = SleepSet,
     unique_id   = {InterleavingId, Index} = UID,
     wakeup_tree = WakeupTree
    } = Last,
  ?debug(Logger, "~s~n", [?pretty_s(Index, Event)]),
  concuerror_logger:graph_new_node(Logger, UID, Index, Event),
  Done = reset_receive_done(RawDone, State),
  NextSleepSet =
    case UseSleepSets of
      true ->
        AllSleepSet =
          case WakeupTree of
            [#backtrack_entry{conservative = true}|_] ->
              concuerror_logger:plan(Logger),
              concuerror_estimator:plan(Estimator, Index),
              SleepSet;
            _ -> ordsets:union(ordsets:from_list(Done), SleepSet)
          end,
        update_sleep_set(Event, AllSleepSet, State);
      false -> []
    end,
  {NewLastWakeupTree, NextWakeupTree} =
    case WakeupTree of
      [] -> {[], []};
      [#backtrack_entry{wakeup_tree = NWT}|Rest] -> {Rest, NWT}
    end,
  NewSchedulingBound =
    next_bound(SchedulingBoundType, Done, PreviousActor, SchedulingBound),
  ?trace(Logger, "  Next bound: ~p~n", [NewSchedulingBound]),
  NewLastDone = [Event|Done],
  NextIndex = Index + 1,
  InitNextTrace =
    #trace_state{
       actors      = Actors,
       index       = NextIndex,
       previous_actor = Actor,
       scheduling_bound = NewSchedulingBound,
       sleep_set   = NextSleepSet,
       unique_id   = {InterleavingId, NextIndex},
       wakeup_tree = NextWakeupTree
      },
  NewLastTrace =
    Last#trace_state{
      done = NewLastDone,
      wakeup_tree = NewLastWakeupTree
     },
  UpdatedSpecialNextTrace =
    update_special(Event#event.special, InitNextTrace),
  NextTrace =
    maybe_update_enabled(SchedulingBoundType, UpdatedSpecialNextTrace),
  InitNewState =
    State#scheduler_state{trace = [NextTrace, NewLastTrace|Prev]},
  NewState = maybe_log(Event, InitNewState, Index),
  {ok, NewState}.

maybe_log(#event{actor = P} = Event, State0, Index) ->
  #scheduler_state{
     assertions_only = AssertionsOnly,
     logger = Logger,
     receive_timeout_total = ReceiveTimeoutTotal,
     treat_as_normal = Normal
    } = State0,
  State = 
    case is_pid(P) of
      true -> State0#scheduler_state{last_scheduled = P};
      false -> State0
    end,
  case Event#event.event_info of
    #builtin_event{mfargs = {erlang, halt, [Status|_]}}
      when Status =/= 0 ->
      #event{actor = Actor} = Event,
      add_error({abnormal_halt, {Index, Actor, Status}}, State);
    #exit_event{reason = Reason} = Exit when Reason =/= normal ->
      {Tag, WasTimeout} =
        if tuple_size(Reason) > 0 ->
            T = element(1, Reason),
            {T, T =:= timeout};
           true -> {Reason, false}
        end,
      case is_atom(Tag) andalso lists:member(Tag, Normal) of
        true ->
          ?unique(Logger, ?lwarning, msg(treat_as_normal), []),
          State;
        false ->
          if WasTimeout -> ?unique(Logger, ?ltip, msg(timeout), []);
             Tag =:= shutdown -> ?unique(Logger, ?ltip, msg(shutdown), []);
             true -> ok
          end,
          IsAssertLike =
            case Tag of
              {MaybeAssert, _} when is_atom(MaybeAssert) ->
                case atom_to_list(MaybeAssert) of
                  "assert"++_ -> true;
                  _ -> false
                end;
              _ -> false
            end,
          Report =
            case {IsAssertLike, AssertionsOnly} of
              {false, true} ->
                ?unique(Logger, ?lwarning, msg(assertions_only_filter), []),
                false;
              {true, false} ->
                ?unique(Logger, ?ltip, msg(assertions_only_use), []),
                true;
              _ -> true
            end,
          if Report ->
              #event{actor = Actor} = Event,
              Stacktrace = Exit#exit_event.stacktrace,
              add_error({abnormal_exit, {Index, Actor, Reason, Stacktrace}}, State);
             true -> State
          end
      end;
    #receive_event{message = 'after'} ->
      NewReceiveTimeoutTotal = ReceiveTimeoutTotal + 1,
      Threshold = 50,
      case NewReceiveTimeoutTotal =:= Threshold of
        true ->
          ?unique(Logger, ?ltip, msg(maybe_receive_loop), [Threshold]);
        false -> ok
      end,
      State#scheduler_state{receive_timeout_total = NewReceiveTimeoutTotal};
    _ -> State
  end.

update_sleep_set(NewEvent, SleepSet, State) ->
  #scheduler_state{logger = _Logger} = State,
  Pred =
    fun(OldEvent) ->
        V = concuerror_dependencies:dependent_safe(NewEvent, OldEvent),
        ?debug(_Logger, "     Awaking (~p): ~s~n", [V,?pretty_s(OldEvent)]),
        V =:= false
    end,
  lists:filter(Pred, SleepSet).

update_special(List, TraceState) when is_list(List) ->
  lists:foldl(fun update_special/2, TraceState, List);
update_special(Special, #trace_state{actors = Actors} = TraceState) ->
  NewActors =
    case Special of
      halt -> [];
      {message, Message} ->
        add_message(Message, Actors);
      {message_delivered, MessageEvent} ->
        remove_message(MessageEvent, Actors);
      {message_received, _Message} ->
        Actors;
      {new, SpawnedPid} ->
        Actors ++ [SpawnedPid];
      {system_communication, _} ->
        Actors
    end,
  TraceState#trace_state{actors = NewActors}.

add_message(MessageEvent, Actors) ->
  #message_event{recipient = Recipient, sender = Sender} = MessageEvent,
  Channel = {Sender, Recipient},
  Update = fun(Queue) -> queue:in(MessageEvent, Queue) end,
  Initial = queue:from_list([MessageEvent]),
  insert_message(Channel, Update, Initial, Actors).

insert_message(Channel, Update, Initial, Actors) ->
  insert_message(Channel, Update, Initial, Actors, false, []).

insert_message(Channel, _Update, Initial, [], Found, Acc) ->
  case Found of
    true -> lists:reverse(Acc, [{Channel, Initial}]);
    false -> [{Channel, Initial}|lists:reverse(Acc)]
  end;
insert_message(Channel, Update, _Initial, [{Channel, Queue}|Rest], true, Acc) ->
  NewQueue = Update(Queue),
  lists:reverse(Acc, [{Channel, NewQueue}|Rest]);
insert_message({From, _} = Channel, Update, Initial, [Other|Rest], Found, Acc) ->
  case Other of
    {{_,_},_} ->
      insert_message(Channel, Update, Initial, Rest, Found, [Other|Acc]);
    From ->
      insert_message(Channel, Update, Initial, Rest,  true, [Other|Acc]);
    _ ->
      case Found of
        false ->
          insert_message(Channel, Update, Initial, Rest, Found, [Other|Acc]);
        true ->
          lists:reverse(Acc, [{Channel, Initial},Other|Rest])
      end
  end.

remove_message(#message_event{recipient = Recipient, sender = Sender}, Actors) ->
  Channel = {Sender, Recipient},
  remove_message(Channel, Actors, []).

remove_message(Channel, [{Channel, Queue}|Rest], Acc) ->
  NewQueue = queue:drop(Queue),
  case queue:is_empty(NewQueue) of
    true  -> lists:reverse(Acc, Rest);
    false -> lists:reverse(Acc, [{Channel, NewQueue}|Rest])
  end;
remove_message(Channel, [Other|Rest], Acc) ->
  remove_message(Channel, Rest, [Other|Acc]).

maybe_update_enabled(bpor, TraceState) ->
  #trace_state{actors = Actors} = TraceState,
  Enabled = [E || E <- Actors, enabled(E)],
  TraceState#trace_state{enabled = Enabled};
maybe_update_enabled(_, TraceState) ->
  TraceState.

%%------------------------------------------------------------------------------

plan_more_interleavings(#scheduler_state{dpor = none} = State) ->
  #scheduler_state{logger = _Logger} = State,
  ?debug(_Logger, "Skipping race detection~n", []),
  State;
plan_more_interleavings(State) ->
  #scheduler_state{
     dpor = DPOR,
     logger = Logger,
     trace = RevTrace,
     use_receive_patterns = UseReceivePatterns
    } = State,
  ?time(Logger, "Assigning happens-before..."),
  {RE, UntimedLate} = split_trace(RevTrace),
  {RevEarly, Late} =
    case UseReceivePatterns of
      false ->
        {RE, lists:reverse(assign_happens_before(UntimedLate, RE, State))};
      true ->
        RevUntimedLate = lists:reverse(UntimedLate),
        {ObsLate, Dict} = fix_receive_info(RevUntimedLate),
        {ObsEarly, _} = fix_receive_info(RE, Dict),
        case lists:reverse(ObsEarly) =:= RE of
          true ->
            {RE, lists:reverse(assign_happens_before(ObsLate, RE, State))};
          false ->
            RevHBEarly = assign_happens_before(ObsEarly, [], State),
            RevHBLate = assign_happens_before(ObsLate, RevHBEarly, State),
            {[], lists:reverse(RevHBLate ++ RevHBEarly)}
        end
    end,
  ?time(Logger, "Planning more interleavings..."),
  NewRevTrace =
    case DPOR =:= optimal of
      true ->
        plan_more_interleavings(lists:reverse(RevEarly, Late), [], State);
      false ->
        plan_more_interleavings(Late, RevEarly, State)
    end,
  State#scheduler_state{trace = NewRevTrace}.

%%------------------------------------------------------------------------------

split_trace(RevTrace) ->
  split_trace(RevTrace, []).

split_trace([], UntimedLate) ->
  {[], UntimedLate};
split_trace([#trace_state{clock_map = ClockMap} = State|RevEarlier] = RevEarly,
            UntimedLate) ->
  case is_empty_map(ClockMap) of
    true  -> split_trace(RevEarlier, [State|UntimedLate]);
    false -> {RevEarly, UntimedLate}
  end.

%%------------------------------------------------------------------------------

assign_happens_before(UntimedLate, RevEarly, State) ->
  assign_happens_before(UntimedLate, [], RevEarly, State).

assign_happens_before([], RevLate, _RevEarly, _State) ->
  RevLate;
assign_happens_before([TraceState|Later], RevLate, RevEarly, State) ->
  %% We will calculate two separate clocks for each state:
  %% - ops unavoidably needed to reach the state will make up the 'state' clock
  %% - ops that happened before the state, will make up the 'Actor' clock
  #trace_state{done = [Event|_], index = Index} = TraceState,
  #scheduler_state{logger = _Logger} = State,
  #event{actor = Actor, special = Special} = Event,
  ?debug(_Logger, "HB: ~s~n", [?pretty_s(Index, Event)]),
  %% Start from the latest vector clock of the actor itself
  ClockMap = get_base_clock_map(RevLate, RevEarly),
  ActorLastClock = lookup_clock(Actor, ClockMap),
  %% Add the step itself:
  ActorNewClock = clock_store(Actor, Index, ActorLastClock),
  %% And add all irreversible edges
  IrreversibleClock =
    add_pre_message_clocks(Special, ClockMap, ActorNewClock),
  %% Apart from those, for the Actor clock we need all the ops that
  %% affect the state. That is, anything dependent with the step:
  HappenedBeforeClock =
    update_clock(RevLate ++ RevEarly, Event, IrreversibleClock, State),
  %% The 'state' clock contains the irreversible clock or
  %% 'independent' if no other dependencies were found
  StateClock =
    case IrreversibleClock =:= HappenedBeforeClock of
      true -> independent;
      false -> IrreversibleClock
    end,
  BaseNewClockMap = map_store(state, StateClock, ClockMap),
  NewClockMap = map_store(Actor, HappenedBeforeClock, BaseNewClockMap),
  %% The HB clock should also be added to anything else stemming
  %% from the step (spawns, sends and deliveries)
  FinalClockMap =
    add_new_and_messages(Special, HappenedBeforeClock, NewClockMap),
  ?trace(_Logger, "  SC:~w~n", [StateClock]),
  ?trace(_Logger, "  AC:~w~n", [HappenedBeforeClock]),
  NewTraceState = TraceState#trace_state{clock_map = FinalClockMap},
  assign_happens_before(Later, [NewTraceState|RevLate], RevEarly, State).

get_base_clock_map(RevLate, RevEarly) ->
  case get_base_clock_map(RevLate) of
    {ok, V} -> V;
    none ->
      case get_base_clock_map(RevEarly) of
        {ok, V} -> V;
        none -> empty_map()
      end
  end.

get_base_clock_map([#trace_state{clock_map = ClockMap}|_]) -> {ok, ClockMap};
get_base_clock_map([]) -> none.

add_pre_message_clocks([], _, Clock) -> Clock;
add_pre_message_clocks([Special|Specials], ClockMap, Clock) ->
  NewClock =
    case Special of
      {message_delivered, #message_event{message = #message{id = Id}}} ->
        max_cv(Clock, lookup_clock({Id, sent}, ClockMap));
      _ -> Clock
    end,
  add_pre_message_clocks(Specials, ClockMap, NewClock).

add_new_and_messages([], _Clock, ClockMap) ->
  ClockMap;
add_new_and_messages([Special|Rest], Clock, ClockMap) ->
  NewClockMap =
    case Special of
      {new, SpawnedPid} ->
        map_store(SpawnedPid, Clock, ClockMap);
      {message, #message_event{message = #message{id = Id}}} ->
        map_store({Id, sent}, Clock, ClockMap);
      _ -> ClockMap
    end,
  add_new_and_messages(Rest, Clock, NewClockMap).

update_clock([], _Event, Clock, _State) ->
  Clock;
update_clock([TraceState|Rest], Event, Clock, State) ->
  #trace_state{
     done = [#event{actor = EarlyActor} = EarlyEvent|_],
     index = EarlyIndex
    } = TraceState,
  EarlyClock = lookup_clock_value(EarlyActor, Clock),
  NewClock =
    case EarlyIndex > EarlyClock of
      false -> Clock;
      true ->
        #scheduler_state{assume_racing = AssumeRacing} = State,
        Dependent =
          concuerror_dependencies:dependent(EarlyEvent, Event, AssumeRacing),
        ?debug(State#scheduler_state.logger,
               "    ~s ~s~n",
               begin
                 Star = fun(false) -> " ";(_) -> "*" end,
                 [Star(Dependent), ?pretty_s(EarlyIndex,EarlyEvent)]
               end),
        case Dependent =:= false of
          true -> Clock;
          false ->
            #trace_state{clock_map = ClockMap} = TraceState,
            EarlyActorClock = lookup_clock(EarlyActor, ClockMap),
            max_cv(Clock, clock_store(EarlyActor, EarlyIndex, EarlyActorClock))
        end
    end,
  update_clock(Rest, Event, NewClock, State).

%%------------------------------------------------------------------------------

plan_more_interleavings([], RevEarly, _SchedulerState) ->
  RevEarly;
plan_more_interleavings([TraceState|Later], RevEarly, State) ->
  case skip_planning(TraceState, State) of
    true ->
      plan_more_interleavings(Later, [TraceState|RevEarly], State);
    false ->
      #scheduler_state{logger = _Logger} = State,
      #trace_state{
         clock_map = ClockMap,
         done = [#event{actor = Actor} = _Event|_],
         index = _Index
        } = TraceState,
      StateClock = lookup_clock(state, ClockMap),
      %% If no dependencies were found skip this altogether
      case StateClock =:= independent of
        true ->
          plan_more_interleavings(Later, [TraceState|RevEarly], State);
        false ->
          ?debug(_Logger, "~s~n", [?pretty_s(_Index, _Event)]),
          ActorClock = lookup_clock(Actor, ClockMap),
          %% Otherwise we zero-down to the latest op that happened before
          LatestHBIndex = find_latest_hb_index(ActorClock, StateClock),
          ?trace(_Logger, "   SC:~w~n", [StateClock]),
          ?trace(_Logger, "   AC:~w~n", [ActorClock]),
          ?debug(_Logger, "  Next @ ~w~n", [LatestHBIndex]),
          NewRevEarly =
            more_interleavings_for_event(
              TraceState, RevEarly, LatestHBIndex, StateClock, Later, State),
          plan_more_interleavings(Later, NewRevEarly, State)
      end
  end.

skip_planning(TraceState, State) ->
  #scheduler_state{non_racing_system = NonRacingSystem} = State,
  #trace_state{done = [Event|_]} = TraceState,
  #event{special = Special} = Event,
  case proplists:lookup(system_communication, Special) of
    {system_communication, System} -> lists:member(System, NonRacingSystem);
    none -> false
  end.

more_interleavings_for_event(TraceState, RevEarly, NextIndex, Clock, Later, State) ->
  more_interleavings_for_event(TraceState, RevEarly, NextIndex, Clock, Later, State, []).

more_interleavings_for_event(TraceState, [], _NextIndex, _Clock, _Later,
                             _State, UpdEarly) ->
  [TraceState|lists:reverse(UpdEarly)];
more_interleavings_for_event(TraceState, RevEarly, -1, _Clock, _Later,
                             _State, UpdEarly) ->
  [TraceState|lists:reverse(UpdEarly, RevEarly)];
more_interleavings_for_event(TraceState, [EarlyTraceState|RevEarly], NextIndex,
                             Clock, Later, State, UpdEarly) ->
  #trace_state{
     clock_map = ClockMap,
     done = [#event{actor = Actor} = Event|_]
    } = TraceState,
  #trace_state{
     clock_map = EarlyClockMap,
     done = [#event{actor = EarlyActor} = EarlyEvent|_],
     index = EarlyIndex
    } = EarlyTraceState,
  Action =
    case NextIndex =:= EarlyIndex of
      false -> none;
      true ->
        Dependent =
          case concuerror_dependencies:dependent_safe(EarlyEvent, Event) of
            true -> {true, no_observer};
            Other -> Other
          end,
        case Dependent of
          false -> none;
          irreversible -> update_clock;
          {true, ObserverInfo} ->
            ?debug(State#scheduler_state.logger,
                   "   races with ~s~n",
                   [?pretty_s(EarlyIndex, EarlyEvent)]),
            case
              update_trace(
                EarlyEvent, Event, Clock, EarlyTraceState,
                Later, UpdEarly, RevEarly, ObserverInfo, State)
            of
              skip -> update_clock;
              {UpdatedNewEarly, ConservativeCandidates} ->
                {update, UpdatedNewEarly, ConservativeCandidates}
            end
        end
    end,
  {NewClock, NewNextIndex} =
    case Action =:= none of
      true -> {Clock, NextIndex};
      false ->
        NC = max_cv(lookup_clock(EarlyActor, EarlyClockMap), Clock),
        ActorClock = lookup_clock(Actor, ClockMap),
        NI = find_latest_hb_index(ActorClock, NC),
        ?debug(State#scheduler_state.logger, "  Next @ ~w~n", [NI]),
        {NC, NI}
    end,
  {NewUpdEarly, NewRevEarly} =
    case Action of
      none -> {[EarlyTraceState|UpdEarly], RevEarly};
      update_clock -> {[EarlyTraceState|UpdEarly], RevEarly};
      {update, S, CC} ->
        maybe_log_race(EarlyTraceState, TraceState, State),
        EarlyClock = lookup_clock_value(EarlyActor, Clock),
        NRE = add_conservative(RevEarly, EarlyActor, EarlyClock, CC, State),
        {S, NRE}
    end,
  more_interleavings_for_event(TraceState, NewRevEarly, NewNextIndex, NewClock,
                               Later, State, NewUpdEarly).

update_trace(
  EarlyEvent, Event, Clock, TraceState,
  Later, NewOldTrace, Rest, ObserverInfo, State
 ) ->
  #scheduler_state{
     dpor = DPOR,
     estimator = Estimator,
     interleaving_id = Origin,
     logger = Logger,
     scheduling_bound_type = SchedulingBoundType,
     use_unsound_bpor = UseUnsoundBPOR,
     use_receive_patterns = UseReceivePatterns
    } = State,
  #trace_state{
     done = [#event{actor = EarlyActor} = EarlyEvent|Done] = AllDone,
     index = EarlyIndex,
     previous_actor = PreviousActor,
     scheduling_bound = BaseBound,
     sleep_set = BaseSleepSet,
     wakeup_tree = Wakeup
    } = TraceState,
  Bound = next_bound(SchedulingBoundType, AllDone, PreviousActor, BaseBound),
  DPORInfo =
    {DPOR,
     case DPOR =:= persistent of
       true -> Clock;
       false -> {EarlyActor, EarlyIndex}
     end},
  SleepSet = BaseSleepSet ++ Done,
  RevEvent = update_context(Event, EarlyEvent),
  {MaybeNewWakeup, ConservativeInfo} =
    case Bound < 0 of
      true ->
        {over_bound,
         case SchedulingBoundType =:= bpor of
           true ->
             NotDep = not_dep(NewOldTrace, Later, DPORInfo, RevEvent),
             get_initials(NotDep);
           false ->
             false
         end};
      false ->
        NotDep = not_dep(NewOldTrace, Later, DPORInfo, RevEvent),
        %% io:fwrite("~n~p: Interleaving: ~.B ~nNotDep: ~p~n", [node(),Origin,NotDep]), %% TODO remove
        {Plan, _} = NW =
          case DPOR =:= optimal of
            true ->
              case UseReceivePatterns of
                false ->
                  {insert_wakeup_optimal(SleepSet, Wakeup, NotDep, Bound, Origin), false};
                true ->
                  V =
                    case ObserverInfo =:= no_observer of
                      true -> NotDep;
                      false ->
                        NotObsRaw =
                          not_obs_raw(NewOldTrace, Later, ObserverInfo, Event),
                        NotObs = NotObsRaw -- NotDep,
                        NotDep ++ [EarlyEvent#event{label = undefined}] ++ NotObs
                    end,
                  RevV = lists:reverse(V),
                  {FixedV, ReceiveInfoDict} = fix_receive_info(RevV),
                  {FixedRest, _} = fix_receive_info(Rest, ReceiveInfoDict),
                  show_plan(v, Logger, 0, FixedV),
                  case has_weak_initial_before(lists:reverse(FixedRest), FixedV, Logger) of
                    true -> {skip, false};
                    false ->
                      {insert_wakeup_optimal(Done, Wakeup, FixedV, Bound, Origin), false}
                  end
              end;
            false ->
              Initials = get_initials(NotDep),
              AddCons =
                case SchedulingBoundType =:= bpor of
                  true -> Initials;
                  false -> false
                end,
              case State#scheduler_state.parallel of
                false ->
                  {insert_wakeup_non_optimal(SleepSet, Wakeup, Initials, false, Origin), AddCons};
                true ->
                  Ownership =
                    case TraceState#trace_state.ownership of
                      true ->
                        owned;
                      false ->
                        disputed
                    end,
                  {insert_wakeup_non_optimal_parallel(SleepSet, Wakeup, Initials, false, Origin, Ownership), AddCons}
              end
          end,
        case is_atom(Plan) of
          true -> NW;
          false ->
            show_plan(standard, Logger, EarlyIndex, NotDep),
            NW
        end
    end,
  case MaybeNewWakeup of
    skip ->
      ?debug(Logger, "     SKIP~n",[]),
      skip;
    over_bound ->
      bound_reached(Logger),
      case UseUnsoundBPOR of
        true -> ok;
        false -> put(bound_exceeded, true)
      end,
      {[TraceState|NewOldTrace], ConservativeInfo};
    NewWakeup ->
      NS = TraceState#trace_state{wakeup_tree = NewWakeup},
      concuerror_logger:plan(Logger),
      concuerror_estimator:plan(Estimator, EarlyIndex),
      {[NS|NewOldTrace], ConservativeInfo}
  end.

not_dep(Trace, Later, DPORInfo, RevEvent) ->
  NotDep = not_dep1(Trace, Later, DPORInfo, []),
  lists:reverse([RevEvent|NotDep]).

not_dep1([], [], _DPORInfo, NotDep) ->
  NotDep;
not_dep1([], T, {DPOR, _} = DPORInfo, NotDep) ->
  KeepLooking =
    case DPOR =:= optimal of
      true -> T;
      false -> []
    end,
  not_dep1(KeepLooking,  [], DPORInfo, NotDep);
not_dep1([TraceState|Rest], Later, {DPOR, Info} = DPORInfo, NotDep) ->
  #trace_state{
     clock_map = ClockMap,
     done = [#event{actor = LaterActor} = LaterEvent|_],
     index = LateIndex
    } = TraceState,
  NewNotDep =
    case DPOR =:= persistent of
      true ->
        Clock = Info,
        LaterActorClock = lookup_clock_value(LaterActor, Clock),
        case LateIndex > LaterActorClock of
          true -> NotDep;
          false -> [LaterEvent|NotDep]
        end;
      false ->
        {Actor, Index} = Info,
        LaterClock = lookup_clock(LaterActor, ClockMap),
        ActorLaterClock = lookup_clock_value(Actor, LaterClock),
        case Index > ActorLaterClock of
          false -> NotDep;
          true -> [LaterEvent|NotDep]
        end
    end,
  not_dep1(Rest, Later, DPORInfo, NewNotDep).

update_context(Event, EarlyEvent) ->
  NewEventInfo =
    case Event#event.event_info of
      %% A receive statement...
      #receive_event{message = Msg} = Info when Msg =/= 'after' ->
        %% ... in race with a message.
        case is_process_info_related(EarlyEvent) of
          true -> Info;
          false -> Info#receive_event{message = 'after'}
        end;
      Info -> Info
    end,
  %% The racing event's effect should differ, so new label.
  Event#event{
    event_info = NewEventInfo,
    label = undefined
   }.

is_process_info_related(Event) ->
  case Event#event.event_info of
    #builtin_event{mfargs = {erlang, process_info, _}} -> true;
    _ -> false
  end.

not_obs_raw(NewOldTrace, Later, ObserverInfo, Event) ->
  lists:reverse(not_obs_raw(NewOldTrace, Later, ObserverInfo, Event, [])).

not_obs_raw([], [], _ObserverInfo, _Event, _NotObs) ->
  [];
not_obs_raw([], Later, ObserverInfo, Event, NotObs) ->
  not_obs_raw(Later, [], ObserverInfo, Event, NotObs);
not_obs_raw([TraceState|Rest], Later, ObserverInfo, Event, NotObs) ->
  #trace_state{done = [#event{special = Special} = E|_]} = TraceState,
  case [Id || {message_received, Id} <- Special, Id =:= ObserverInfo] =:= [] of
    true ->
      not_obs_raw(Rest, Later, ObserverInfo, Event, [E|NotObs]);
    false ->
      #event{special = NewSpecial} = Event,
      ObsNewSpecial =
        case lists:keyfind(message_delivered, 1, NewSpecial) of
          {message_delivered, #message_event{message = #message{id = NewId}}} ->
            lists:keyreplace(ObserverInfo, 2, Special, {message_received, NewId});
          _ -> exit(impossible)
        end,
      [E#event{label = undefined, special = ObsNewSpecial}|NotObs]
  end.

has_weak_initial_before([], _, _Logger) ->
  ?debug(_Logger, "No weak initial before~n",[]),
  false;
has_weak_initial_before([TraceState|Rest], V, Logger) ->
  #trace_state{done = [EarlyEvent|Done]} = TraceState,
  case has_initial(Done, [EarlyEvent|V]) of
    true ->
      ?debug(Logger, "Check: ~s~n",[?join([?pretty_s(0,D)||D<-Done],"~n")]),
      show_plan(initial, Logger, 1, [EarlyEvent|V]),
      true;
    false ->
      ?trace(Logger, "Up~n",[]),
      has_weak_initial_before(Rest, [EarlyEvent|V], Logger)
  end.

show_plan(_Type, _Logger, _Index, _NotDep) ->
  ?debug(
     _Logger, "     PLAN (Type: ~p)~n~s",
     begin
       Indices = lists:seq(_Index, _Index + length(_NotDep) - 1),
       IndexedNotDep = lists:zip(Indices, _NotDep),
       [_Type] ++
         [lists:append(
            [io_lib:format("        ~s~n", [?pretty_s(I,S)])
             || {I,S} <- IndexedNotDep])]
     end).

maybe_log_race(EarlyTraceState, TraceState, State) ->
  #scheduler_state{logger = Logger} = State,
  if State#scheduler_state.show_races ->
      #trace_state{
         done = [EarlyEvent|_],
         index = EarlyIndex,
         unique_id = EarlyUID
        } = EarlyTraceState,
      #trace_state{
         done = [Event|_],
         index = Index,
         unique_id = UID
        } = TraceState,
      concuerror_logger:graph_race(Logger, EarlyUID, UID),
      IndexedEarly = {EarlyIndex, EarlyEvent#event{location = []}},
      IndexedLate = {Index, Event#event{location = []}},
      concuerror_logger:race(Logger, IndexedEarly, IndexedLate);
     true ->
      ?unique(Logger, ?linfo, msg(show_races), [])
  end.

insert_wakeup_non_optimal(SleepSet, Wakeup, Initials, Conservative, Origin) ->
  case existing(SleepSet, Initials) of
    true -> skip;
    false -> add_or_make_compulsory(Wakeup, Initials, Conservative, Origin)
  end.

insert_wakeup_non_optimal_parallel(SleepSet, Wakeup, Initials, Conservative, Origin, Ownership) ->
  case existing(SleepSet, Initials) of
    true -> skip;
    false -> add_or_make_compulsory_parallel(Wakeup, Initials, Conservative, Origin, Ownership)
  end.


add_or_make_compulsory(Wakeup, Initials, Conservative, Origin) ->
  add_or_make_compulsory(Wakeup, Initials, Conservative, Origin, []).


add_or_make_compulsory_parallel(Wakeup, Initials, Conservative, Origin, Ownership) ->
  add_or_make_compulsory_parallel(Wakeup, Initials, Conservative, Origin, Ownership, []).


add_or_make_compulsory([], [E|_], Conservative, Origin, Acc) ->
  Entry =
    #backtrack_entry{
       conservative = Conservative,
       event = E, origin = Origin,
       wakeup_tree = []
      },
  lists:reverse([Entry|Acc]);
add_or_make_compulsory([Entry|Rest], Initials, Conservative, Origin, Acc) ->
  #backtrack_entry{conservative = C, event = E, wakeup_tree = []} = Entry,
  #event{actor = A} = E,
  Pred = fun(#event{actor = B}) -> A =:= B end,
  case lists:any(Pred, Initials) of
    true ->
      case C andalso not Conservative of
        true ->
          NewEntry = Entry#backtrack_entry{conservative = false},
          lists:reverse(Acc, [NewEntry|Rest]);
        false -> skip
      end;
    false ->
      NewAcc = [Entry|Acc],
      add_or_make_compulsory(Rest, Initials, Conservative, Origin, NewAcc)
  end.


add_or_make_compulsory_parallel([], [E|_], Conservative, Origin, Ownership, Acc) ->
  Entry =
    #backtrack_entry{
       ownership = Ownership,
       conservative = Conservative,
       event = E, origin = Origin,
       wakeup_tree = []
      },
  lists:reverse([Entry|Acc]);
add_or_make_compulsory_parallel([Entry|Rest], Initials, Conservative, Origin, Ownership, Acc) ->
  #backtrack_entry{conservative = C, event = E, wakeup_tree = []} = Entry,
  #event{actor = A} = E,
  Pred = fun(#event{actor = B}) -> A =:= B end,
  case lists:any(Pred, Initials) of
    true ->
      case C andalso not Conservative of
        true ->
          NewEntry = Entry#backtrack_entry{conservative = false},
          lists:reverse(Acc, [NewEntry|Rest]);
        false -> skip
      end;
    false ->
      NewAcc = [Entry|Acc],
      add_or_make_compulsory_parallel(Rest, Initials, Conservative, Origin, Ownership, NewAcc)
  end.

insert_wakeup_optimal(SleepSet, Wakeup, V, Bound, Origin) ->
  case has_initial(SleepSet, V) of
    true -> skip;
    false -> insert_wakeup(Wakeup, V, Bound, Origin)
  end.

has_initial([Event|Rest], V) ->
  case check_initial(Event, V) =:= false of
    true -> has_initial(Rest, V);
    false -> true
  end;
has_initial([], _) -> false.

insert_wakeup(          _, _NotDep,  Bound, _Origin) when Bound < 0 ->
  over_bound;
insert_wakeup(         [],  NotDep, _Bound,  Origin) ->
  backtrackify(NotDep, Origin);
insert_wakeup([Node|Rest],  NotDep,  Bound,  Origin) ->
  #backtrack_entry{event = Event, origin = M, wakeup_tree = Deeper} = Node,
  case check_initial(Event, NotDep) of
    false ->
      NewBound =
        case is_integer(Bound) of
          true -> Bound - 1;
          false -> Bound
        end,
      case insert_wakeup(Rest, NotDep, NewBound, Origin) of
        Special
          when
            Special =:= skip;
            Special =:= over_bound -> Special;
        NewTree -> [Node|NewTree]
      end;
    NewNotDep ->
      case Deeper =:= [] of
        true  -> skip;
        false ->
          case insert_wakeup(Deeper, NewNotDep, Bound, Origin) of
            Special
              when
                Special =:= skip;
                Special =:= over_bound -> Special;
            NewTree ->
              Entry =
                #backtrack_entry{
                   event = Event,
                   origin = M,
                   wakeup_tree = NewTree},
              [Entry|Rest]
          end
      end
  end.

backtrackify(Seq, Cause) ->
  Fold =
    fun(Event, Acc) ->
        [#backtrack_entry{event = Event, origin = Cause, wakeup_tree = Acc}]
    end,
  lists:foldr(Fold, [], Seq).

check_initial(Event, NotDep) ->
  check_initial(Event, NotDep, []).

check_initial(_Event, [], Acc) ->
  lists:reverse(Acc);
check_initial(Event, [E|NotDep], Acc) ->
  #event{actor = EventActor} = Event,
  #event{actor = EActor} = E,
  case EventActor =:= EActor of
    true -> lists:reverse(Acc,NotDep);
    false ->
      case concuerror_dependencies:dependent_safe(E, Event) =:= false of
        true -> check_initial(Event, NotDep, [E|Acc]);
        false -> false
      end
  end.

get_initials(NotDeps) ->
  get_initials(NotDeps, [], []).

get_initials([], Initials, _) -> lists:reverse(Initials);
get_initials([Event|Rest], Initials, All) ->
  Fold =
    fun(Initial, Acc) ->
        Acc andalso
          concuerror_dependencies:dependent_safe(Initial, Event) =:= false
    end,
  NewInitials =
    case lists:foldr(Fold, true, All) of
      true -> [Event|Initials];
      false -> Initials
    end,
  get_initials(Rest, NewInitials, [Event|All]).            

existing([], _) -> false;
existing([#event{actor = A}|Rest], Initials) ->
  Pred = fun(#event{actor = B}) -> A =:= B end,
  lists:any(Pred, Initials) orelse existing(Rest, Initials).

add_conservative(Rest, _Actor, _Clock, false, _State) ->
  Rest;
add_conservative(Rest, Actor, Clock, Candidates, State) ->
  case add_conservative(Rest, Actor, Clock, Candidates, State, []) of
    abort ->
      ?debug(State#scheduler_state.logger, "  aborted~n",[]),
      Rest;
    NewRest -> NewRest
  end.

add_conservative([], _Actor, _Clock, _Candidates, _State, _Acc) ->
  abort;
add_conservative([TraceState|Rest], Actor, Clock, Candidates, State, Acc) ->
  #scheduler_state{
     interleaving_id = Origin,
     logger = _Logger
    } = State,
  #trace_state{
     done = [#event{actor = EarlyActor} = _EarlyEvent|Done],
     enabled = Enabled,
     index = EarlyIndex,
     previous_actor = PreviousActor,
     sleep_set = BaseSleepSet,
     wakeup_tree = Wakeup
    } = TraceState,
  ?debug(_Logger,
         "   conservative check with ~s~n",
         [?pretty_s(EarlyIndex, _EarlyEvent)]),
  case EarlyActor =:= Actor of
    false -> abort;
    true ->
      case EarlyIndex < Clock of
        true -> abort;
        false ->
          case PreviousActor =:= Actor of
            true ->
              NewAcc = [TraceState|Acc],
              add_conservative(Rest, Actor, Clock, Candidates, State, NewAcc);
            false ->
              EnabledCandidates =
                [C ||
                  #event{actor = A} = C <- Candidates,
                  lists:member(A, Enabled)],
              case EnabledCandidates =:= [] of
                true -> abort;
                false ->
                  SleepSet = BaseSleepSet ++ Done,
                  case
                    insert_wakeup_non_optimal(
                      SleepSet, Wakeup, EnabledCandidates, true, Origin
                     )
                  of
                    skip -> abort;
                    NewWakeup ->
                      NS = TraceState#trace_state{wakeup_tree = NewWakeup},
                      lists:reverse(Acc, [NS|Rest])
                  end
              end
          end
      end
  end.

%%------------------------------------------------------------------------------
has_more_to_explore(State) ->
  #scheduler_state{
     estimator = Estimator,
     scheduling_bound_type = SchedulingBoundType,
     trace = Trace,
     parallel = Parallel
    } = State,
  TracePrefix = 
    case Parallel of 
      false ->
        find_prefix(Trace, SchedulingBoundType);
      true ->
        find_prefix_parallel(Trace)
    end,
  case TracePrefix =:= [] of
    true -> {false, State#scheduler_state{trace = []}};
    false ->
      NewState =
        State#scheduler_state{need_to_replay = true, trace = TracePrefix},
      [Last|_] = TracePrefix,
      TopIndex = Last#trace_state.index,
      concuerror_estimator:restart(Estimator, TopIndex),
      {true, NewState}
  end.

find_prefix([], _SchedulingBoundType) -> [];
find_prefix(Trace, SchedulingBoundType) ->
  [#trace_state{wakeup_tree = Tree} = TraceState|Rest] = Trace,
  case SchedulingBoundType =/= 'bpor' orelse get(bound_exceeded) of
    false ->
      case [B || #backtrack_entry{conservative = false} = B <- Tree] of
        [] -> find_prefix(Rest, SchedulingBoundType);
        WUT -> [TraceState#trace_state{wakeup_tree = WUT}|Rest]
      end;
    true ->
      case Tree =:= [] of
        true -> find_prefix(Rest, SchedulingBoundType);
        false -> Trace
      end
  end.

find_prefix_parallel([]) -> [];
find_prefix_parallel([#trace_state{ownership = false} = TraceState|Rest]) ->
  #trace_state{
     done = Done,
     sleep_set = SleepSet,
     wakeup_tree = Tree
    } = TraceState,
  case Tree =:= [] of
    true ->
      find_prefix_parallel(Rest);
    false ->
      case split_wut_at_ownership(Tree, owned) of
        {Tree, []} ->
          case split_wut_at_ownership(Tree, disputed) of
            {Tree, []} ->
          %% no owned entries
              find_prefix_parallel(Rest);
            {Pr, [DisputedEntry|Suff]} ->
              [H|T] = Done,
              UpdatedTraceState =
                TraceState#trace_state{
                  done =
                    [H|[Entry#backtrack_entry.event || Entry <- Pr] ++ T],
                  sleep_set =
                    [Entry#backtrack_entry.event || Entry <- Pr] ++ SleepSet,
                  wakeup_tree = [DisputedEntry|Suff]
                 },
              [UpdatedTraceState|Rest]
          end;
        {Prefix, [OwnedEntry|Suffix]} ->
          %% put previous wut that are not owned on sleep
          case split_wut_at_ownership(Prefix, disputed) of
            {Prefix, _} ->
              ok;
            _ ->
              %% disputed gets inserted at the end
              exit(impossible)
          end,
          [H|T] = Done,
          UpdatedTraceState =
            TraceState#trace_state{
              done =
                [H|[Entry#backtrack_entry.event || Entry <- Prefix] ++ T],
              sleep_set =
                [Entry#backtrack_entry.event || Entry <- Prefix] ++ SleepSet,
              wakeup_tree = [OwnedEntry|Suffix]
             },
          [UpdatedTraceState|Rest]
      end
  end;
find_prefix_parallel(Trace) ->
  [#trace_state{wakeup_tree = Tree} = TraceState|Rest] = Trace,
  case Tree =:= [] of
    true -> find_prefix_parallel(Rest);
    false -> Trace
  end.

replay(#scheduler_state{need_to_replay = false} = State) ->
  State;
replay(State) ->
  #scheduler_state{interleaving_id = N, logger = Logger, trace = Trace} = State,
  [#trace_state{index = I, unique_id = Sibling} = Last|
   [#trace_state{unique_id = Parent}|_] = Rest] = Trace,
  concuerror_logger:graph_set_node(Logger, Parent, Sibling),
  NewTrace =
    [Last#trace_state{unique_id = {N, I}, clock_map = empty_map()}|Rest],
  S = io_lib:format("New interleaving ~p. Replaying...", [N]),
  ?time(Logger, S),
  NewState = replay_prefix(NewTrace, State#scheduler_state{trace = NewTrace}),
  %% FixedState =
  %%   case State#scheduler_state.replay_mode of
  %%     pseudo ->
  %%       NewState;
  %%     actual ->
  %%       FixedNewTrace = fix_sleep_sets(lists:reverse(NewTrace), [], State),
  %%       NewState#scheduler_state{trace = FixedNewTrace}
  %%   end,
  ?debug(Logger, "~s~n",["Replay done."]),
  NewState#scheduler_state{need_to_replay = false}.

fix_sleep_sets([Last], Acc, _) ->
  [Last|Acc];
fix_sleep_sets([TraceState, NextTraceState|Rest], Acc, State) ->
  #trace_state{
     sleep_set = SleepSet,
     done = Done
    } = TraceState,
  #trace_state{
     sleep_set = NextSleepSet,
     done = NextDone
    } = NextTraceState,
  [NextEvent|NT] = NextDone,
  [Event|T] = Done,
  AllSleepSet =
    ordsets:union(ordsets:from_list(T), SleepSet),
  FixedNextSleepSet = update_sleep_set2(Event, AllSleepSet, State),
  UpdatedNextTraceState =
    NextTraceState#trace_state{
      sleep_set = FixedNextSleepSet%%ordsets:union(FixedNextSleepSet)
     },
  fix_sleep_sets([UpdatedNextTraceState|Rest], [TraceState|Acc], State).

update_sleep_set2(NewEvent, SleepSet, State) ->
  #scheduler_state{logger = _Logger} = State,
  Pred =
    fun(OldEvent) ->
        V = concuerror_dependencies:dependent_safe(NewEvent, OldEvent),
        %%?debug(_Logger, "     Awaking (~p): ~s~n", [V,?pretty_s(OldEvent)]),
        V =:= false
    end,
  lists:filter(Pred, SleepSet).

%% =============================================================================

reset_receive_done([Event|Rest], #scheduler_state{use_receive_patterns = true}) ->
  NewSpecial =
    [patch_message_delivery(S, empty_map()) || S <- Event#event.special],
  [Event#event{special = NewSpecial}|Rest];
reset_receive_done(Done, _) ->
  Done.

fix_receive_info(RevTraceOrEvents) ->
  fix_receive_info(RevTraceOrEvents, empty_map()).

fix_receive_info(RevTraceOrEvents, ReceiveInfoDict) ->
  fix_receive_info(RevTraceOrEvents, ReceiveInfoDict, []).

fix_receive_info([], ReceiveInfoDict, TraceOrEvents) ->
  {TraceOrEvents, ReceiveInfoDict};
fix_receive_info([#trace_state{} = TraceState|RevTrace], ReceiveInfoDict, Trace) ->
  [Event|Rest] = TraceState#trace_state.done,
  {[NewEvent], NewDict} = fix_receive_info([Event], ReceiveInfoDict, []),
  NewTraceState = TraceState#trace_state{done = [NewEvent|Rest]},
  fix_receive_info(RevTrace, NewDict, [NewTraceState|Trace]);
fix_receive_info([#event{} = Event|RevEvents], ReceiveInfoDict, Events) ->
  case has_delivery_or_receive(Event#event.special) of
    true ->
      #event{event_info = EventInfo, special = Special} = Event,
      NewReceiveInfoDict = store_receive_info(EventInfo, Special, ReceiveInfoDict),
      NewSpecial = [patch_message_delivery(S, NewReceiveInfoDict) || S <- Special],
      NewEventInfo =
        case EventInfo of
          #message_event{} ->
            {_, NI} =
              patch_message_delivery({message_delivered, EventInfo}, NewReceiveInfoDict),
            NI;
          _ -> EventInfo
        end,
      NewEvent = Event#event{event_info = NewEventInfo, special = NewSpecial},
      fix_receive_info(RevEvents, NewReceiveInfoDict, [NewEvent|Events]);
    false ->
      fix_receive_info(RevEvents, ReceiveInfoDict, [Event|Events])
  end.

has_delivery_or_receive([]) -> false;
has_delivery_or_receive([{M,_}|_])
  when M =:= message_delivered; M =:= message_received ->
  true;
has_delivery_or_receive([_|R]) -> has_delivery_or_receive(R).

store_receive_info(EventInfo, Special, ReceiveInfoDict) ->
  case [ID || {message_received, ID} <- Special] of
    [] -> ReceiveInfoDict;
    IDs ->
      ReceiveInfo =
        case EventInfo of
          #receive_event{receive_info = RI} -> RI;
          _ -> {system, fun(_) -> true end}
        end,
      Fold = fun(ID,Dict) -> map_store(ID, ReceiveInfo, Dict) end,
      lists:foldl(Fold, ReceiveInfoDict, IDs)
  end.

patch_message_delivery({message_delivered, MessageEvent}, ReceiveInfoDict) ->
  #message_event{message = #message{id = Id}} = MessageEvent,
  ReceiveInfo =
    case map_find(Id, ReceiveInfoDict) of
      {ok, RI} -> RI;
      error -> not_received
    end,
  {message_delivered, MessageEvent#message_event{receive_info = ReceiveInfo}};
patch_message_delivery(Other, _ReceiveInfoDict) ->
  Other.

%% =============================================================================
%% ENGINE (manipulation of the Erlang processes under the scheduler)
%% =============================================================================

replay_prefix(Trace, State) ->
  #scheduler_state{
     entry_point = EntryPoint,
     first_process = FirstProcess,
     processes = Processes,
     timeout = Timeout
    } = State,
  concuerror_callback:reset_processes(Processes),
  ok =
    concuerror_callback:start_first_process(FirstProcess, EntryPoint, Timeout),
  NewState = State#scheduler_state{last_scheduled = FirstProcess},
  replay_prefix_aux(lists:reverse(Trace), NewState).

replay_prefix_aux([_Last], State) ->
  %% TODO : 
  %% when another interleaving is replayed the old interleaving (more correctly
  %% the traces of the first inteleaving that are always before a backtrack)
  %% are never actually replayed therefore when working in parallel mode
  %% pseudo replay_mode cannot currently work. Maybe fix this.
  %%
  %% Last state has to be properly replayed.
  %% case State#scheduler_state.replay_mode =:= actual of
  %%   true ->
  %%     %% State#scheduler_state{replay_mode = pseudo};
  %%   false ->
  %%     State
  %% end;
  State;
replay_prefix_aux([#trace_state{done = [Event|_], index = I} = _|Rest], State) ->
  #scheduler_state{
     logger = _Logger,
     print_depth = PrintDepth,
     replay_mode = ReplayMode,
     parallel = Parallel
    } = State,
  ?debug(_Logger, "~s~n", [?pretty_s(I, Event)]),
  {ok, #event{actor = Actor} = NewEvent} =
    case ReplayMode of 
      pseudo ->
        get_next_event_backend(Event, State);
      actual ->
        EventInfo = Event#event.event_info,
        case EventInfo of
          #builtin_event{} ->
            FixedEventInfo = EventInfo#builtin_event{actual_replay = true},
            {ok, NewE} = get_next_event_backend(Event#event{event_info = FixedEventInfo}, State),
            NewEInfo = NewE#event.event_info,
            FixedNewEInfo = NewEInfo#builtin_event{actual_replay = false},
            {ok, NewE#event{event_info = FixedNewEInfo}};
          _ ->
            get_next_event_backend(Event#event{event_info = undefined}, State)
        end
    end,
  try
    case Parallel of
      false ->
        true = Event =:= NewEvent;
      true ->
        true = logically_equal(State, Event, NewEvent)
    end
  catch
    _C:_R ->
      ?crash({replay_mismatch, I, Event, NewEvent, PrintDepth})
  end,
  NewLastScheduled =
    case is_pid(Actor) of
      true -> Actor;
      false -> State#scheduler_state.last_scheduled
    end,
  NewState = State#scheduler_state{last_scheduled = NewLastScheduled},
  replay_prefix_aux(Rest, maybe_log(Event, NewState, I)).

%%------------------------------------------------------------------------------
%%TODO add more event_info types

logically_equal(Event1, Event2) ->
  logically_equal(#scheduler_state{parallel = true}, Event1, Event2).

logically_equal(#scheduler_state{parallel = Parallel}, _, _)
  when Parallel =:= false ->
  false;
logically_equal(_, Event, NewEvent) ->
  %% #event{event_info = EventInfo} = Event,
  %% #event{event_info = NewEventInfo} = NewEvent,
  %% case EventInfo of
  %%   #builtin_event ->  
  logically_equal_aux(Event, NewEvent).
  
logically_equal_aux([], []) ->
  true;
logically_equal_aux([H|T], [NewH|NewT]) ->
  logically_equal_aux(H, NewH)
    andalso logically_equal_aux(T, NewT);
logically_equal_aux(#builtin_event{} = EventInfo, #builtin_event{} = NewEventInfo) ->
  #builtin_event{
     %% actor = Actor,
     %% extra = Extra,
     %% exiting = Exiting,
     mfargs = MFArgs
     %% result = Result,
     %% status = Status,
     %% trapping = Trapping,
     %% actual_replay = ActualReplay
    } = EventInfo,
  {M, _, _} = MFArgs,
  case M of
    ets -> 
      logically_equal_aux(tuple_to_list(EventInfo#builtin_event{extra = different_ref}),
                          tuple_to_list(NewEventInfo#builtin_event{extra = different_ref}));
    _ ->
      logically_equal_aux(tuple_to_list(EventInfo), tuple_to_list(NewEventInfo))
  end;
logically_equal_aux(Term, NewTerm)
  when is_tuple(Term) andalso is_tuple(NewTerm) ->
  logically_equal_aux(tuple_to_list(Term), tuple_to_list(NewTerm));
logically_equal_aux(Fun, NewFun)
 %% TODO : maybe add more items here
  when is_function(Fun) andalso is_function(NewFun) ->
  erlang:fun_info(Fun, name) =:= erlang:fun_info(NewFun, name)
    andalso erlang:fun_info(Fun, module) =:= erlang:fun_info(NewFun, module)
    andalso erlang:fun_info(Fun, arity) =:= erlang:fun_info(NewFun, arity);
%% TODO remove this
logically_equal_aux(Ref, undefined)
  when is_reference(Ref) ->
  true;
logically_equal_aux(undefined, Ref)
  when is_reference(Ref) ->
  true;
logically_equal_aux(Ref, NewRef)
  when is_reference(Ref) andalso is_reference(NewRef) ->
  true;
logically_equal_aux(Term, NewTerm) ->
  Term =:= NewTerm.

%% =============================================================================
%% INTERNAL INTERFACES
%% =============================================================================

%% Between scheduler and an instrumented process
%%------------------------------------------------------------------------------

get_next_event_backend(#event{actor = Channel} = Event, State)
  when ?is_channel(Channel) ->
  #scheduler_state{timeout = Timeout} = State,
  #event{event_info = MessageEvent} = Event,
  assert_no_messages(),
  UpdatedEvent =
    concuerror_callback:deliver_message(Event, MessageEvent, Timeout),
  {ok, UpdatedEvent};
get_next_event_backend(#event{actor = Pid} = Event, State) when is_pid(Pid) ->
  #scheduler_state{timeout = Timeout} = State,
  assert_no_messages(),
  Pid ! Event,
  concuerror_callback:wait_actor_reply(Event, Timeout).

assert_no_messages() ->
  receive
    Msg -> error({pending_message, Msg})
  after
    0 -> ok
  end.

%%%----------------------------------------------------------------------
%%% Helper functions
%%%----------------------------------------------------------------------

-ifdef(BEFORE_OTP_17).

empty_map() ->
  dict:new().

map_store(K, V, Map) ->
  dict:store(K, V, Map).

map_find(K, Map) ->
  dict:find(K, Map).

is_empty_map(Map) ->
  dict:size(Map) =:= 0.

lookup_clock(P, ClockMap) ->
  case dict:find(P, ClockMap) of
    {ok, Clock} -> Clock;
    error -> clock_new()
  end.

clock_new() ->
  orddict:new().

clock_store(_, 0, VectorClock) ->
  VectorClock;
clock_store(Actor, Index, VectorClock) ->
  orddict:store(Actor, Index, VectorClock).

lookup_clock_value(Actor, VectorClock) ->
  case orddict:find(Actor, VectorClock) of
    {ok, Value} -> Value;
    error -> 0
  end.

max_cv(D1, D2) ->
  Merger = fun(_Key, V1, V2) -> max(V1, V2) end,
  orddict:merge(Merger, D1, D2).

find_latest_hb_index(ActorClock, StateClock) ->
  %% This is the max index that is in the Actor clock but not in the
  %% corresponding state clock.
  Fold =
    fun(K, V, Next) ->
        case orddict:find(K, StateClock) =:= {ok, V} of
          true -> Next;
          false -> max(V, Next)
        end
    end,
  orddict:fold(Fold, -1, ActorClock).

-else.

empty_map() ->
  #{}.

map_store(K, V, Map) ->
  maps:put(K, V, Map).

map_find(K, Map) ->
  maps:find(K, Map).

is_empty_map(Map) ->
  maps:size(Map) =:= 0.

lookup_clock(P, ClockMap) ->
  maps:get(P, ClockMap, clock_new()).

clock_new() ->
  #{}.

clock_store(_, 0, VectorClock) ->
  VectorClock;
clock_store(Actor, Index, VectorClock) ->
  maps:put(Actor, Index, VectorClock).

lookup_clock_value(Actor, VectorClock) ->
  maps:get(Actor, VectorClock, 0).

max_cv(VC1, VC2) ->
  ODVC1 = orddict:from_list(maps:to_list(VC1)),
  ODVC2 = orddict:from_list(maps:to_list(VC2)),
  Merger = fun(_Key, V1, V2) -> max(V1, V2) end,
  MaxVC = orddict:merge(Merger, ODVC1, ODVC2),
  maps:from_list(MaxVC).

find_latest_hb_index(ActorClock, StateClock) ->
  %% This is the max index that is in the Actor clock but not in the
  %% corresponding state clock.
  Fold =
    fun(K, V, Next) ->
        case maps:find(K, StateClock) =:= {ok, V} of
          true -> Next;
          false -> max(V, Next)
        end
    end,
  maps:fold(Fold, -1, ActorClock).

-endif.

next_bound(SchedulingBoundType, Done, PreviousActor, Bound) ->
  case SchedulingBoundType of
    none -> Bound;
    bpor ->
      NonPreemptExplored =
        [E || #event{actor = PA} = E <- Done, PA =:= PreviousActor] =/= [],
      case NonPreemptExplored of
        true -> Bound - 1;
        false -> Bound
      end;
    delay ->
      %% Every reschedule costs.
      Bound - length(Done)
  end.

bound_reached(Logger) ->
  ?unique(Logger, ?lwarning, msg(scheduling_bound_warning), []),
  ?debug(Logger, "OVER BOUND~n",[]),
  concuerror_logger:bound_reached(Logger).

%% =============================================================================
%% Needed for parallel mode
%% =============================================================================
%%------------------------------------------------------------------------------
%% Controller Interface
%%------------------------------------------------------------------------------

-spec initialize_execution_tree(reduced_scheduler_state()) ->
                                 execution_tree().

%% TODO modify in case I need to add sleep sets here
initialize_execution_tree(#reduced_scheduler_state{trace = Trace} = _Fragment) ->
  initialize_execution_tree_aux(lists:reverse(Trace)).

initialize_execution_tree_aux([LastTraceState]) ->
  #trace_state_transferable{
     done = Done
    } = LastTraceState,
  [ActiveEvent|_FinishedEvents] = Done,
  #execution_tree{
     event = ActiveEvent
    };
initialize_execution_tree_aux([TraceState, NextTraceState|Rest]) ->
  #trace_state_transferable{
     done = Done
    } = TraceState,
  [ActiveEvent|_FinishedEvents] = Done,
  #trace_state_transferable{
     done = NextDone,
     wakeup_tree = NextWuT
    } = NextTraceState,
  [_ActiveChild|FinishedChildren] = NextDone,
  ActiveChild = initialize_execution_tree_aux([NextTraceState|Rest]),
  FinishedChildrenTree = [],%%[#execution_tree{event = Ev} || Ev <- FinishedChildren],
  #execution_tree{
     children = FinishedChildrenTree ++ [ActiveChild] ++ wut_to_exec_tree(NextWuT),
     event = ActiveEvent
    }.

insert_wut_into_children2([], Children) ->
  Children;
insert_wut_into_children2(WuT, []) ->
  wut_to_exec_tree(WuT);
insert_wut_into_children2([Entry|Rest], Children) ->
  {UpdatedOrNewChild, ChildrenLeft} = insert_wut_into_children_aux2(Entry, Children),
  [UpdatedOrNewChild|insert_wut_into_children2(Rest, ChildrenLeft)].

insert_wut_into_children_aux2(
  #backtrack_entry_transferable{
     event = Event,
     wakeup_tree = WuT
    } = Entry,
  Children
 ) ->
  case split_children(Event, Children) of
    {Prefix, [Child|Suffix]} ->
      NewTree =
        #execution_tree{
           event = Event,
           children = insert_wut_into_children2(WuT, Child#execution_tree.children)
          },
      {NewTree, Prefix ++ Suffix};
    {Children, []} ->
      NewTree =
        #execution_tree{
           event = Event,
           children = wut_to_exec_tree(WuT)
          },
      %% new wakeup tree inserted
      {NewTree, Children}
  end.

wut_to_exec_tree([]) ->
  [];
wut_to_exec_tree([Entry|Rest]) ->
  [wut_to_exec_tree_aux(Entry)|wut_to_exec_tree(Rest)].

wut_to_exec_tree_aux(
  #backtrack_entry_transferable{
     event = Event,
     wakeup_tree = WuT
    } = _Entry) ->
  #execution_tree{
     event = Event,
     children = wut_to_exec_tree(WuT)
    }.

split_children(ActiveEvent, Children) ->      
  Pred =
    fun(Child) ->
        not logically_equal(Child#execution_tree.event, ActiveEvent)
    end,
  lists:splitwith(Pred, Children).

%%------------------------------------------------------------------------------
-spec print_tree(term(), term()) -> term().

print_tree(Prefix, ExecTree) ->
  #execution_tree{
     children = Children,
     %finished_children = FinishedChildren,
     event = Event
     %next_wakeup_tree = WuT
    } = ExecTree,
  io:fwrite("~sNode: ~p~n", [Prefix, Event#event_transferable.actor]),
  %[io:fwrite("~sFinishedChild: ~p~n", [Prefix, Ev#event_transferable.actor]) || Ev <- FinishedChildren],
  %[io:fwrite("~sWuT: ~p~n", [Prefix, (En#backtrack_entry_transferable.event)#event_transferable.actor]) || En <- WuT],
  ChildPrefix = Prefix ++ "---",
  [print_tree(ChildPrefix, Ch) || Ch <- Children].

print_tree_relative(ExecTree, [TraceState]) ->
  #execution_tree{
     children = Children,
     %% finished_children = FinishedChildren,
     event = Event
     %% next_wakeup_tree = WuT
    } = ExecTree,
  #trace_state_transferable{
     done = Done
    } = TraceState,
  [OldActiveEvent|_] = Done,
  true = logically_equal(OldActiveEvent, Event),
  exit(impossible4),
  io:fwrite("Node: ~p~n", [Event#event_transferable.actor]);
  %% [io:fwrite("~sFinishedChild: ~p~n", ["---", Ev#event_transferable.actor]) || Ev <- FinishedChildren],
  %% [io:fwrite("~sWuT: ~p~n", ["+++", (En#backtrack_entry_transferable.event)#event_transferable.actor]) || En <- WuT];
print_tree_relative(ExecTree, [TraceState, NextTraceState|Rest]) ->
  #execution_tree{
     children = Children,
     %% finished_children = FinishedChildren,
     event = Event
     %% next_wakeup_tree = WuT
    } = ExecTree,
  #trace_state_transferable{
     done = NextDone
    } = NextTraceState,
  #trace_state_transferable{
     done = Done
    } = TraceState,
  [OldNextActiveEvent|_] = NextDone,
  [OldActiveEvent|_] = Done,
  true = logically_equal(OldActiveEvent, Event),
  io:fwrite("*************************************************~n", []),
  io:fwrite("Done:~p~nWuT:~p~n",[TraceState#trace_state_transferable.done, TraceState#trace_state_transferable.wakeup_tree]),
  io:fwrite("Node: ~p~n", [Event]),
  %% [io:fwrite("~sFinishedChild: ~p~n", ["---", Ev]) || Ev <- FinishedChildren],
  %% [io:fwrite("~sWuT: ~p~n", ["+++", (En#backtrack_entry_transferable.event)]) || En <- WuT],
  case split_active_children(OldNextActiveEvent, Children) of
    {Prefix, [OldChild|Suffix]} ->
      print_tree_relative(OldChild, [NextTraceState|Rest]);
    {ActiveChildren, []} ->
      io:fwrite("NOT FOUND!!!!!!~n", []),
      [io:fwrite("~sActiveChild: ~p~n", ["***",(Ch#execution_tree.event)]) || Ch <- Children],
      io:fwrite("*************************************************~n", [])
  end.

    

print_trace([]) ->
  ok;
print_trace([H|T]) ->
  WuT = H#trace_state_transferable.wakeup_tree,
  io:fwrite("Actor~p~n",[(lists:nth(1,H#trace_state_transferable.done))#event_transferable.actor]),
  [io:fwrite("WuT: ~p, ~p~n", [(En#backtrack_entry_transferable.event)#event_transferable.actor, owned(En)]) 
   || En <- WuT],
  print_trace(T).

owned(Entry) ->
  case Entry#backtrack_entry_transferable.ownership of
    owned ->
      'Owned';
    not_owned ->
      'Not Owned';
    disputed ->
      'Disputed'
  end.
%%------------------------------------------------------------------------------

-spec update_execution_tree(reduced_scheduler_state(),
                          reduced_scheduler_state(),
                          execution_tree()) ->
                             {reduced_scheduler_state() | fragment_finished, execution_tree()}.

update_execution_tree(OldFragment, Fragment, ExecutionTree) ->
  #reduced_scheduler_state{
     trace = OldTrace
    } = OldFragment,
  #reduced_scheduler_state{
     backtrack_size = BacktrackSize,
     trace = Trace
    } = Fragment,
  %% io:fwrite("============UPDT=============~n",[]),
  %% print_tree("", ExecutionTree),
  %% io:fwrite("~n"),
  %% print_trace(lists:reverse(OldTrace)),
  %% io:fwrite("~n"),
  %% print_trace(lists:reverse(Trace)),
  %% io:fwrite("~n"),
  {RevNewTrace, NewExecutionTree, BacktrackEntriesRemoved} =
    try
      update_execution_tree_aux(lists:reverse(OldTrace), lists:reverse(Trace), ExecutionTree)
    catch
      C:R:S ->
        io:fwrite("OldTrace:~n", []),
        print_trace(lists:reverse(OldTrace)),
        io:fwrite("NewTrace~n", []),
        print_trace(lists:reverse(Trace)),
        io:fwrite("Tree~n", []),
        print_tree_relative(ExecutionTree, lists:reverse(OldTrace)),
        exit({C,R,S})
    end,
  %% print_trace(RevNewTrace),
  %% io:fwrite("~n"),
  %% print_tree("", NewExecutionTree),
  %% io:fwrite("=============================~n",[]),
  EntriesLeft = BacktrackSize - BacktrackEntriesRemoved,
  try
    EntriesLeft = size_of_backtrack_transferable(lists:reverse(RevNewTrace))
  catch _:_:_ ->
      io:fwrite("Old:~n~p~nNew:~n~p~n", [lists:reverse(Trace), RevNewTrace]),
      io:fwrite("error~n~n"),
      receive
      after 1000 ->
          ok
      end,
      exit({EntriesLeft,size_of_backtrack_transferable(lists:reverse(RevNewTrace))})
  end,
  OwnershipFixedFragment =
    case EntriesLeft =:= 0 of
      false ->
        FixedNewTrace = remove_empty_trace_states(lists:reverse(RevNewTrace)),
        Fragment#reduced_scheduler_state{
          backtrack_size = EntriesLeft,
          trace = FixedNewTrace
         };
      true ->
        fragment_finished
    end,
  %% TODO add check here in case all backtrack entries are removed
  {OwnershipFixedFragment, NewExecutionTree}.

remove_empty_trace_states([#trace_state_transferable{wakeup_tree = []} = _|Rest]) ->
  remove_empty_trace_states(Rest);
remove_empty_trace_states([TraceState|Rest]) ->
  #trace_state_transferable{wakeup_tree = Tree} = TraceState,
  %% TODO remove this asap
  case split_wut_at_ownership(Tree, disputed) of
    {Tree, []} ->
      ok;
    _ ->
      exit(impossible)
  end,
  case split_wut_at_ownership(Tree, owned) of
    {Tree, []} ->
      remove_empty_trace_states(Rest);
    _ ->
      [TraceState|Rest]
  end.


update_execution_tree_aux(
  [_OldTraceState],
  [_TraceState],
  _ExecutionTree
 ) ->
  {[_TraceState], _ExecutionTree,0};
  %%exit(impossible1);
 %% same as below
update_execution_tree_aux(
  [_OldTraceState],
  [_TraceState, _NextTraceState|_Rest],
  _ExecutionTree
 ) ->
  %% This should not be possible because the current events of OldTraceState and TraceState
  %% would be logically equal and since OldTraceState would be the last TraceState
  %% of the initial fragment, its backtrack_entry would have definately been
  %% explore and therefore the current events of those TraceStates would have not been the same
  %% (i.e. logically equal)
  exit(impossible2);
update_execution_tree_aux(
  [OldTraceState, OldNextTraceState|OldRest],
  [TraceState],
  ExecutionTree
 ) ->
  #trace_state_transferable{
     done = OldDone
    } = OldTraceState,
  #trace_state_transferable{
     done = Done
    } = TraceState,
  #trace_state_transferable{
     done = OldNextDone,
     wakeup_tree = OldNextWuT
    } = OldNextTraceState,
  #execution_tree{
     children = Children,
     %finished_children = FinishedChildren,
     event = ActiveEvent
     %next_wakeup_tree = WuT
    } = ExecutionTree,
  %% TODO : maybe remove this check
  [OldActiveEvent2|_] = OldDone,
  [ActiveEvent2|_] = Done,
  true = logically_equal(ActiveEvent2, ActiveEvent),
  true = logically_equal(OldActiveEvent2, ActiveEvent),
  [OldNextActiveEvent|_] = OldNextDone,
  %% ----OLD CODE : TODO : REMOVE
  %% [OldActiveEvent2|_] = OldDone,
  %% [ActiveEvent2|_] = Done,
  %% true = logically_equal(ActiveEvent2, ActiveEvent),
  %% true = logically_equal(OldActiveEvent2, ActiveEvent),
  %% UpdatedExecutionTree =
  %%   update_execution_tree_done_aux([OldTraceState, OldNextTraceState|OldRest], ExecutionTree),
  %% {[TraceState], UpdatedExecutionTree, 0}
  %% ----
  %% The WuT of the central tree is modified to account for the events
  %% of the backtrack of the initial fragment that were explored
  %% NewFinishedChildren = get_finished_children_from_done(NextFinishedEvents, WuT),
  %% ReducedWuT = get_reduced_wut_from_done(NextDone, WuT),
  FinishedWuT = OldNextWuT,
  %%{NewFinishedChildren, ReducedWuT} = get_finished_children_from_wut(FinishedWuT, WuT),
  %% The subtree of the active_children that corresponds to the OldNextActiveEvent
  %% needs to be modified (perhaps even be deleted),
  %% since this suffix of the trace_state is considered done. Also another subtree
  %% that corresponds to the NextActiveEvent needs to be created and be put to the
  %% active_children list
  {UpdatedActiveChildren, MaybeNewFinishedChild} =
    case split_children(OldNextActiveEvent, Children) of
      {Prefix, [OldChild|Suffix]} ->
        %% case update_execution_tree_done_aux([OldNextTraceState|OldRest], OldChild) of
        %%   {node_finished, Event} ->
        %%     exit(impossible),
        %%     {Prefix ++ Suffix, [Event]};
        %%   {maybe_finished, MaybeFinishedChild} ->
        %%     exit(impossible),
        %%     {Prefix ++ [MaybeFinishedChild] ++ Suffix, []};
        %%   UpdatedChild ->
        %%     {Prefix ++ [UpdatedChild] ++ Suffix, []}
        %% end;
        {Prefix ++ [OldChild] ++ Suffix, []};
      {Children, []} ->
        ChildrenEv = [C#execution_tree.event  || C <- Children],
        io:fwrite("OldNextActiveEvent~p~nActiveChildren~p~n",
                  [OldNextActiveEvent, ChildrenEv]),
        io:fwrite("OldTraceLeft: ~p~nTraceLeft: ~p~n", 
                  [
                   [TS#trace_state_transferable.done || TS <- [OldTraceState, OldNextTraceState|OldRest]],
                   [TS#trace_state_transferable.done || TS <- [TraceState]]
                  ]),
        exit({not_found4, erlang:get_stacktrace()}),
        %% true = lists:member(OldNextActiveEvent#event_transferable.actor,
        %%                     [FC#event_transferable.actor || FC <- FinishedChildren]),
        {Children, []}
    end,
  UpdatedExecutionTree =
    ExecutionTree#execution_tree{
      children = UpdatedActiveChildren
      %% finished_children =
      %%  MaybeNewFinishedChild ++ NewFinishedChildren ++ FinishedChildren,
      %% next_wakeup_tree = ReducedWuT
     },  
{[TraceState], UpdatedExecutionTree, 0};
update_execution_tree_aux(
  [OldTraceState, OldNextTraceState|OldRest], 
  [TraceState, NextTraceState|Rest], 
  ExecutionTree
 ) ->
  #trace_state_transferable{
     done = OldDone
    } = OldTraceState,
  #trace_state_transferable{
     done = Done
    } = TraceState,
  #trace_state_transferable{
     done = OldNextDone,
     wakeup_tree = OldNextWuT
    } = OldNextTraceState,
  #trace_state_transferable{
     done = NextDone,
     sleep_set = _Sleep,
     wakeup_tree = NextWuT
    } = NextTraceState,
  #execution_tree{
     children = Children,
     %%finished_children = FinishedChildren,
     event = ActiveEvent
     %%next_wakeup_tree = WuT
    } = ExecutionTree,
  %% TODO : maybe remove this check
  [OldActiveEvent2|_] = OldDone,
  [ActiveEvent2|_] = Done,
  true = logically_equal(ActiveEvent2, ActiveEvent),
  true = logically_equal(OldActiveEvent2, ActiveEvent),
  [NextActiveEvent|_NextFinishedEvents] = NextDone,
  [OldNextActiveEvent|_] = OldNextDone,
  %% I may find some extra entries
  %% at the NextWuT compared to the OldNextWuT. Those entries are the backtrack
  %% points that this fragment will need to request ownership for. If those 
  %% entries are found in the WuT, the ActiveChildren or the FinishedChildren
  %% of the execution_tree, then some other fragment
  %% has claimed ownership and therefore they must be added to the sleep-set of
  %% the fragment and be removed from the wakeup_tree. Otherwise, this
  %% fragment claims ownership over them, the owneship of those entries  gets
  %% modified accordingly and they are inserted in the wakeup_tree of the central
  %% tree.
  %% {OwnedWuT, NotOwnedWuT, DisputedWuT} = split_wut(NextWuT, [], [], []),
  %%NotOwnedWuT = [], %% for the time being, this should hold true for source
  %% TODO : remove this check
  %% ok = assert_equal_wut(OwnedWuT, OldNextWuT), <- this assertion is only correct if
  %% the next events  are logically equal (else OwnedWuT could be smaller than OldNextWuT)
  %%WuTEvents = [Entry#backtrack_entry_transferable.event || Entry <- WuT],
  %% ActiveEvents = [Node#execution_tree.event || Node <- Children],
  %% {NewOwnedWuT, NewNotOwnedWuT} =
  %%   get_ownership(DisputedWuT, ActiveEvents, [], []),
  %% NewNotOwnedEvents = [Entry#backtrack_entry_transferable.event || Entry <- NewNotOwnedWuT],
  %% SleepChildren = filter_children(Children, OwnedWut ++ NewOwnedWuT),
  {WuTInsertedChildren, FixedNextWuT, LocalEntriesRemoved} =
    insert_wut_into_children(NextWuT, Children, [], 0),
  false = have_duplicates(Children),
  false = have_duplicates(WuTInsertedChildren),
  %% catch _:_:_ ->
  %%     io:fwrite("Children: ~n~p~n WuTinsertedChildren: ~n~p~n WuT: ~n~p~n",
  %%               [[Ex#execution_tree.event || Ex <- Children],
  %%                [Ex#execution_tree.event || Ex <- WuTInsertedChildren],
  %%                NextWuT]
  %%              ),
  %%     exit(error)
  %% end,
  try true = length(WuTInsertedChildren) >= length(Children)
  catch _:_:_ ->
      io:fwrite("Children: ~p~n WuTinsertedChildren: ~p~n",
                [[Ex#execution_tree.event || Ex <- Children],
                 [Ex#execution_tree.event || Ex <- WuTInsertedChildren]]
               ),
      exit(error)
  end,
  AllSleep = NextDone,
  UpdatedNextTraceState =
    NextTraceState#trace_state_transferable{
      %%sleep_set =  NewNotOwnedEvents ++ _Sleep,
      %% done =
      %%   NextDone ++ NewNotOwnedEvents,++
      %%   filter_children([Ex#execution_tree.event || Ex <- Children],
      %%                   NextDone ++ NewNotOwnedEvents ++ 
      %%                     [En#backtrack_entry_transferable.event || En <- OwnedWuT ++ NewOwnedWuT]
      %%                  ),
      wakeup_tree = get_full_wut(FixedNextWuT, AllSleep, WuTInsertedChildren)
     },
  case logically_equal(NextActiveEvent, OldNextActiveEvent) of
    true ->
      %% Recursively keep modifying the tree
      %% Find the subtree that needs to be modified
      {Prefix, [NextChild|Suffix]} = 
        try 
          {P, [NC|S]} = split_active_children(NextActiveEvent, WuTInsertedChildren),
          {P, [NC|S]}
        catch _E:_R:_S ->
            ActiveChildrenEv = [C#execution_tree.event  || C <- WuTInsertedChildren],
            io:fwrite("OldNextActiveEvent~p~nNextActiveEvent~p~nActiveChildren~p~n",
                      [OldNextActiveEvent, NextActiveEvent, ActiveChildrenEv]),
            io:fwrite("OldTraceLeft: ~p~nTraceLeft: ~p~n", 
                     [
                      [TS#trace_state_transferable.done || TS <- [OldTraceState, OldNextTraceState|OldRest]],
                      [TS#trace_state_transferable.done || TS <- [TraceState, NextTraceState|Rest]]
                     ]),
            io:fwrite("OldNextTraceStateAll:~p~nNextTraceStateAll:~p~n",
                      [OldNextTraceState, NextTraceState]),
            exit({not_found, {_E,_R,_S}})
        end,
      %% TODO there is a bug here
      %% try split_active_children(NextActiveEvent, ActiveChildren)
      %% catch _:_ ->
      %%     print_tree("", ExecutionTree),
      %%     io:fwrite("~n"),
      %%     print_trace([OldTraceState, OldNextTraceState|OldRest]),
      %%     io:fwrite("~n"),
      %%     print_trace([TraceState, NextTraceState|Rest]),
      %%     io:fwrite("~n"),
      %%     exit(not_found)
      %% end,
      {[UpdatedNextTraceState|UpdatedRest], UpdatedChildren, MaybeNewFinishedChild, EntriesRemoved} =
        case update_execution_tree_aux(
               [OldNextTraceState|OldRest],
               [UpdatedNextTraceState|Rest],
               NextChild
              ) of
          {UpdatedTrace, {node_finished, Event}, N} ->
            exit(impossible5),
            {UpdatedTrace, Prefix ++ Suffix, [Event], N};
          {UpdatedTrace, {maybe_finished, MaybeFinishedChild}, N} ->
            %% case WuT of
            %%   [] ->
            %%     {UpdatedTrace, Prefix ++ Suffix, [MaybeFinishedChild#execution_tree.event], N};
            %%   _ ->
            exit(impossible),
            {UpdatedTrace, Prefix ++ [MaybeFinishedChild] ++ Suffix, [], N};
            %% end;
          {UpdatedTrace, UpdatedChild, N} ->
            {UpdatedTrace, Prefix ++ [UpdatedChild] ++ Suffix, [], N}
        end,
      false = have_duplicates(UpdatedChildren),
      UpdatedExecutionTree =
        ExecutionTree#execution_tree{
          children = UpdatedChildren
          %%finished_children = MaybeNewFinishedChild ++ FinishedChildren,
          %%next_wakeup_tree = WuT ++ NewOwnedWuT
         },
      {[TraceState, UpdatedNextTraceState|UpdatedRest],
       UpdatedExecutionTree,
       EntriesRemoved + LocalEntriesRemoved};
    false ->
      %% The WuT of the central tree is modified to account for the events
      %% of the backtrack of the initial fragment that were explored
      %% NewFinishedChildren = get_finished_children_from_done(NextFinishedEvents, WuT),
      %% ReducedWuT = get_reduced_wut_from_done(NextDone, WuT),
      %% FinishedWuT = get_finished_wut(OldNextWuT, NextWuT, NextActiveEvent),
      %% {NewFinishedChildren, ReducedWuT} = get_finished_children_from_wut(FinishedWuT, WuT),
      %% The subtree of the active_children that corresponds to the OldNextActiveEvent
      %% needs to be modified (perhaps even be deleted),
      %% since this suffix of the trace_state is considered done. Also another subtree
      %% that corresponds to the NextActiveEvent needs to be created and be put to the
      %% active_children list
      {UpdatedChildren, MaybeNewFinishedChild} =
        case split_active_children(OldNextActiveEvent, WuTInsertedChildren) of
          {Prefix, [OldChild|Suffix]} ->
            %% case update_execution_tree_done_aux([OldNextTraceState|OldRest], OldChild) of
            %%   {node_finished, Event} ->
            %%     exit(impossible4),
            %%     {Prefix ++ Suffix, [Event]};
            %%   {maybe_finished, MaybeFinishedChild} ->
            %%     %% case ReducedWuT of
            %%     %%   [] ->
            %%     %%     {Prefix ++ Suffix, [MaybeFinishedChild#execution_tree.event]};
            %%     %%   _ ->
            %%     {Prefix ++ [MaybeFinishedChild] ++ Suffix, []};
            %%   UpdatedChild ->
            %%     {Prefix ++ [UpdatedChild] ++ Suffix, []}
            %% end;
            {WuTInsertedChildren, []};
          {WuTInsertedChildren, []} ->
            %% TODO : maybe remove these checks
            %%[] = OldRest,
            %% ActiveChildrenEv = [C#execution_tree.event  || C <- WuTInsertedChildren],
            %% io:fwrite("OldNextActiveEvent~p~nNextActiveEvent~p~nActiveChildren~p~nFinishedChildren~p~n",
            %%           [OldNextActiveEvent, NextActiveEvent, ActiveChildrenEv, FinishedChildren]),
            %% io:fwrite("OldTraceLeft: ~p~nTraceLeft: ~p~n", 
            %%          [
            %%           [TS#trace_state_transferable.done || TS <- [OldTraceState, OldNextTraceState|OldRest]],
            %%           [TS#trace_state_transferable.done || TS <- [TraceState, NextTraceState|Rest]]
            %%          ]),
            exit({not_found2, erlang:get_stacktrace()})
            %% true = lists:member(OldNextActiveEvent#event_transferable.actor,
            %%                     [FC#event_transferable.actor || FC <- FinishedChildren]),
            %% {WuTInsertedChildren, []}
        end,
      FixedChildren =
        case split_children(NextActiveEvent, UpdatedChildren) of
          {Pref, [NextChild|Suff]} ->
            try [] = NextChild#execution_tree.children
            catch _:_:_ ->
                receive
                after 10000 ->
                    ok
                end,
                exit(error)
            end,
            Pref ++ [initialize_execution_tree_aux([UpdatedNextTraceState|Rest])|Suff];
          {UpdatedChildren, []} ->
            UpdatedChildren ++ [initialize_execution_tree_aux([UpdatedNextTraceState|Rest])]
        end,
      false = have_duplicates(UpdatedChildren),
      false = have_duplicates(FixedChildren),
      UpdatedExecutionTree =
        ExecutionTree#execution_tree{
          children = FixedChildren
          %% finished_children =
          %%   MaybeNewFinishedChild ++ NewFinishedChildren ++ FinishedChildren,
          %% next_wakeup_tree = ReducedWuT ++ NewOwnedWuT %% This is NOT OKAY WuT needs to be reduced
         },
      {[TraceState, UpdatedNextTraceState|Rest], UpdatedExecutionTree, LocalEntriesRemoved}
  end.

have_duplicates([]) ->
  false;
have_duplicates([Child|Rest]) ->
  #execution_tree{
     event = Event
    } = Child,
  case split_children(Event, Rest) of
    {Prefix, [Ch|Suffix]} ->
      true;
    _ ->
      have_duplicates(Rest)
  end.

get_full_wut(CurrentWuT, AllSleep, Children) ->
  %%{OwnedWuT, NotOwnedWuT, []} = split_wut(CurrentWuT),
  ExecTreeWuT = execution_tree_to_wut(filter_children(Children, AllSleep)),
  reclaim_ownership(CurrentWuT, ExecTreeWuT).

reclaim_ownership([], ExecTreeWuT) ->
  ExecTreeWuT;
reclaim_ownership(WuT, []) ->
  %% WuT should be a subtree of ExecTreeWuT
  %% exit(impossible7);
  WuT;
reclaim_ownership([Entry|Rest], ExecTreeWuT) ->
  {Prefix, UpdatedExecTreeEntry, Suffix} = reclaim_ownership_aux(Entry, ExecTreeWuT),
  Prefix ++ [UpdatedExecTreeEntry|reclaim_ownership(Rest, Suffix)].

reclaim_ownership_aux(
  #backtrack_entry_transferable{
     event = Event,
     wakeup_tree = WuT,
     ownership = Ownership
    } = Entry,
  ExecTreeWuT
 ) ->
  %% not_owned = Ownership,
  case split_wut_with(Event, ExecTreeWuT) of
    {Prefix, [EqualEntry|Suffix]} ->
      NewEntry =
        EqualEntry#backtrack_entry_transferable{
          event = Event,
          ownership = Ownership
         },
      {Prefix, NewEntry, Suffix};
    {ExecTreeWuT, []} ->
      io:fwrite("====================~p~n---------------~n~p~n+++++++++++++++++~n",
                [Event, ExecTreeWuT]),
      exit(impossible7)
  end.

split_wut_with(Event, WuT) ->      
  Pred =
    fun(Entry) ->
        not logically_equal(Entry#backtrack_entry_transferable.event, Event)
    end,
  lists:splitwith(Pred, WuT).

filter_children(Children, AllSleep) ->
  AllSleepActors = [Ev#event_transferable.actor || Ev <- AllSleep],
  Pred =
    fun(Child) ->
        Event = Child#execution_tree.event,
        %% TODO check if I need to use logically_equal instead
        Actor = Event#event_transferable.actor,
        not lists:member(Actor, AllSleepActors)
    end,
  lists:filter(Pred, Children).

execution_tree_to_wut([]) ->
  [];
execution_tree_to_wut([Node|Rest]) ->
  #execution_tree{
     event = Event,
     children = Children
    } = Node,
  Entry =
    #backtrack_entry_transferable{
       event = Event
      },
  [Entry|execution_tree_to_wut(Rest)].

insert_wut_into_children([], Children, WuTAcc, N) ->
  {Children,
   lists:reverse(WuTAcc),
   N};
%% insert_wut_into_children(WuT, [], WuTAcc, N) ->
%%   WuT = fix_wut_ownership(WuT, disputed),
%%   FinalWuT = lists:reverse(WuTAcc) ++ fix_wut_ownership(WuT, owned),
%%   {wut_to_exec_tree(FinalWuT),
%%    FinalWuT,
%%    N};
insert_wut_into_children([Entry|Rest], Children, WuTAcc, N) ->
  {Prefix, UpdatedOrNewChild, Suffix} = insert_wut_into_children_aux(Entry, Children),
  case UpdatedOrNewChild of
    {found, Child} ->
      {FixedEntry, Count} =
        case determine_ownership(Entry) =:= disputed of
          true ->
            {fix_wut_ownership(Entry, not_owned), 1};
          false ->
            {Entry, 0}
        end,
      insert_wut_into_children(Rest, Prefix ++ [Child|Suffix], [FixedEntry|WuTAcc], N + Count);
    {new, Child} ->
      Suffix = [],
      FixedEntry =
        case determine_ownership(Entry) =:= disputed of
          true ->
            fix_wut_ownership(Entry, owned);
          false ->
            Entry
        end,
      insert_wut_into_children(Rest, Children ++ [Child], [FixedEntry|WuTAcc], N)
  end.



insert_wut_into_children_aux(
  #backtrack_entry_transferable{
     event = Event
    } = Entry,
  Children
 ) ->
  case split_children(Event, Children) of
    {Prefix, [Child|Suffix]} ->
      {Prefix, {found, Child}, Suffix};
    {Children, []} ->
      NewTree =
        #execution_tree{
           event = Event
          },
      %% new wakeup tree inserted
      {Children, {new, NewTree}, []}
  end.

%% Returns the wakeup_tree entries that have been explored between the 
%% old trace and the new trace, that is the entries of the old wakeup
%% tree that do not exist in the new wakeup tree (except the entry that
%% corresponds to the wakeup tree that is currently been explored).
get_finished_wut(OldWuT, WuT, ActiveEvent) ->
  ActiveActor = ActiveEvent#event_transferable.actor,
  Pred =
    fun(Entry) ->
        Event = Entry#backtrack_entry_transferable.event,
        Actor = Event#event_transferable.actor,
        Actor =/= ActiveActor
    end,
  lists:filter(Pred, OldWuT -- WuT).  

split_wut([], OwnedWuT, NotOwnedWuT, DisputedWuT) ->
  {OwnedWuT, NotOwnedWuT, DisputedWuT};
%% TODO check if I need to reverse these
split_wut([Entry|Rest], OwnedWuT, NotOwnedWuT, DisputedWuT) ->
  case Entry#backtrack_entry_transferable.ownership of
    owned->
      split_wut(Rest, [Entry|OwnedWuT], NotOwnedWuT, DisputedWuT);
    not_owned ->
      split_wut(Rest, OwnedWuT, [Entry|NotOwnedWuT], DisputedWuT);
    disputed ->
      split_wut(Rest, OwnedWuT, NotOwnedWuT, [Entry|DisputedWuT])
  end.

get_ownership(DisputedWuT, [], NewOwnedWuT, NotOwnedWuT) ->
  %% owneship is claimed
  OwnedWut =
    [Entry#backtrack_entry_transferable{ownership = owned} || Entry <- DisputedWuT],
  {OwnedWut ++ NewOwnedWuT, NotOwnedWuT};
get_ownership([], _, NewOwnedWuT, NotOwnedWuT) ->
  {NewOwnedWuT, NotOwnedWuT};
get_ownership([Entry|RestWuT], Events, NewOwnedWuT, NotOwnedWuT) ->
  Pred =
    fun(Event) ->
        not same_actor(Event, Entry#backtrack_entry_transferable.event)
    end,
  case lists:splitwith(Pred, Events) of
    {_AllEvents, []} ->
      %% ownership is claimed
      OwnedEntry = Entry#backtrack_entry_transferable{ownership = owned},
      get_ownership(RestWuT, Events, [OwnedEntry|NewOwnedWuT], NotOwnedWuT);
    {Prefix, [_EqualEvent|Suffix]} ->
      %% ownership is not claimed
      %% TODO check this !!!! ASAP : 
      %% The fact that I add EqualEvent and not NotOwnedEvent leads to more sleep set blocked
      %%NotOwnedEvent = #event_transferable{actor = EqualEvent#event_transferable.actor},
      %% I am doing this in order to not carry unecessary info about the event
      %% TODO make sure that it is okay to do this
      NotOwnedEntry = Entry#backtrack_entry_transferable{ownership = not_owned},
      get_ownership(RestWuT, Prefix ++ Suffix, NewOwnedWuT, [NotOwnedEntry|NotOwnedWuT])
  end.

assert_equal_wut([], []) -> ok;
assert_equal_wut([Entry|Rest], WuT) ->
  Pred =
    fun(BacktrackEntry) ->
        not logically_equal(Entry#backtrack_entry_transferable.event,
                            BacktrackEntry#backtrack_entry_transferable.event)
    end,
  assert_equal_wut(Rest, lists:filter(Pred, WuT)).

%%------------------------------------------------------------------------------

-spec update_execution_tree_done(reduced_scheduler_state(), execution_tree()) ->
                                  execution_tree().

update_execution_tree_done(Fragment, ExecutionTree) ->
  #reduced_scheduler_state{trace = Trace} = Fragment,
  %% io:fwrite("============DONE=============~n",[]),
  %% print_tree("", ExecutionTree),
  %% io:fwrite("~n"),
  %% print_trace(lists:reverse(Trace)),
  %% io:fwrite("~n"),
  try
    case update_execution_tree_done_aux(lists:reverse(Trace), ExecutionTree) of
      {node_finished, _Event} ->
        %% io:fwrite("Node finished:~p~n", [_Ev]),
        %% io:fwrite("============DONE=============~n",[]),
        exit(1),
        empty;
      {maybe_finished, _Tree} ->
        %% exit(2),
        %% empty;
        _Tree;
      UpdatedTree ->
        %% print_tree("", UpdatedTree),
        %% io:fwrite("=============================~n",[]),
        UpdatedTree
    end
  catch
    C:R:S ->
      %% io:fwrite("TRACE~n",[]),
      %% print_trace(lists:reverse(Trace)),
      io:fwrite("TREE~n", []),
      print_tree_relative(ExecutionTree, lists:reverse(Trace)),
      exit({C,R,S})
  end.

update_execution_tree_done_aux([LastTraceState], ExecutionTree) ->
%% this TraceState must have a non-empty wakeup tree since it is the last
%% TraceState of fragment that was send to be explored and this exploration
%% completed. This wakeup three would have been added to the parent node of the
%% subtree ExecutionTree. If I find no more active children or backtrack entries at
%% this node then this subtree is considered finished(no other fragment accessing it)
%% otherwise I return the complete subtree unmodified
  %% #trace_state_transferable{
  %%    done = Done
  %%   } = LastTraceState,
  %% #execution_tree{
  %%    active_children = ActiveChildren,
  %%    event = ActiveEvent,
  %%    next_wakeup_tree = WuT
  %%   } = ExecutionTree,
  %% %% TODO : maybe remove this check
  %% [ActiveEvent2|_] = Done,
  %% true = logically_equal(ActiveEvent2, ActiveEvent),
  %% case WuT =:= [] andalso ActiveChildren =:= [] of
  %%   true ->
  %%     %% this node is done, everything underneath it is explored
  %%     {maybe_finished, ExecutionTree};
  %%   false ->
      ExecutionTree;
  %% end;
update_execution_tree_done_aux([TraceState, NextTraceState|Rest], ExecutionTree) ->
  %% #trace_state_transferable{
  %%    done = Done
  %%   } = TraceState,
  %% #trace_state_transferable{
  %%    done = NextDone,
  %%    wakeup_tree = NextWuT
  %%   } = NextTraceState,
  %% #execution_tree{
  %%    active_children = ActiveChildren,
  %%    finished_children = FinishedChildren,
  %%    event = ActiveEvent,
  %%    next_wakeup_tree = WuT
  %%   } = ExecutionTree,
  %% %% TODO : maybe remove this check
  %% [ActiveEvent2|_] = Done,
  %% true = logically_equal(ActiveEvent2, ActiveEvent),
  %% [NextActiveEvent|_] = NextDone,
  %% %% updates the child tree that corresponds to the next trace_state and perhaps
  %% %% deletes this subtree (if no other fragments access the subsequence that begins
  %% %% with this child)
  %% %% {MaybeNewFinishedChild, UpdatedActiveChildren} =
  %% %%   update_active_child_done([NextTraceState|Rest], ActiveChildren, NextActiveEvent),
  %% %% {Prefix, [NextChild|Suffix]} = split_active_children(NextActiveEvent, ActiveChildren),
  
  %% %% {Prefix, [NextChild|Suffix]} =
  %% %%   split_active_children(NextActiveEvent, ActiveChildren),
  %% %% {UpdatedActiveChildren, MaybeNewFinishedChild} =
  %% %%   case update_execution_tree_done_aux([NextTraceState|Rest], NextChild) of
  %% %%     {node_finished, Event} ->
  %% %%       {Prefix ++ Suffix, [Event]};
  %% %%     UpdatedChild ->
  %% %%       {Prefix ++ [UpdatedChild] ++ Suffix, []}
  %% %%   end,
  %% %% I know NextWuT has been completely explored so I filter its entries from
  %% %% the wakeup_tree of the node
  %% {NewFinishedChildren, UpdatedWuT} = get_finished_children_from_wut(NextWuT, WuT),
  %% {UpdatedActiveChildren, MaybeNewFinishedChild} =
  %%   case split_active_children(NextActiveEvent, ActiveChildren) of
  %%     {Prefix, [NextChild|Suffix]} ->
  %%       case update_execution_tree_done_aux([NextTraceState|Rest], NextChild) of
  %%         %% {node_finished, Event} ->
  %%         %%   {Prefix ++ Suffix, [Event]};
  %%         {maybe_finished, MaybeFinishedChild} ->
  %%           %% case (UpdatedWuT =:= []) and (Prefix =:= []) and (Suffix =:= []) 
  %%           %% of
  %%           %%   true ->
  %%           %%     %% the child is indeed finished
  %%           %%     {Prefix ++ Suffix, [MaybeFinishedChild#execution_tree.event]};
  %%           %%   false ->
  %%           %%     %% the child may not be finished
  %%           {Prefix ++ [MaybeFinishedChild] ++ Suffix, []};
  %%           %% end;
  %%         UpdatedChild ->
  %%           {Prefix ++ [UpdatedChild] ++ Suffix, []}
  %%       end;
  %%     {ActiveChildren, []} ->
  %%       %% TODO : maybe remove these checks
  %%       %% This may mean that a new child must be added
  %%       %% but probably not since it would make sence to
  %%       %% add this child before
  %%       %% [] = Rest,
  %%       %% true = lists:member(NextActiveEvent#event_transferable.actor,
  %%       %%                    [FC#event_transferable.actor || FC <- FinishedChildren]),
  %%       ActiveChildrenEv = [C#execution_tree.event  || C <- ActiveChildren],
  %%       io:fwrite("OldNextActiveEvent~p~nActiveChildren~p~nFinishedChildren~p~nNextWuT~p~n",
  %%                 [NextActiveEvent, ActiveChildrenEv, FinishedChildren, WuT]),
  %%       io:fwrite("OldTraceLeft: ~p~n", 
  %%                 [
  %%                  [TS#trace_state_transferable.done || TS <- [TraceState, NextTraceState|Rest]]
  %%                 ]),
  %%       io:fwrite("OldNextTraceStateAll:~p~n",
  %%                 [NextTraceState]),
  %%       exit({not_found3, erlang:get_stacktrace()}),
  %%       {ActiveChildren, []}
  %%   end,
  %% case UpdatedWuT =:= [] andalso UpdatedActiveChildren =:= [] of
  %%   true ->
  %%     %% this node is done, everything underneath is explored
  %%     %%{node_finished, ActiveEvent};
  %%     %%{maybe_finished, ActiveEvent};
  %%     UpdatedExecutionTree = 
  %%       ExecutionTree#execution_tree{
  %%         active_children = UpdatedActiveChildren,
  %%         finished_children =
  %%           MaybeNewFinishedChild ++ NewFinishedChildren ++ FinishedChildren,
  %%         next_wakeup_tree = UpdatedWuT
  %%        },
  %%     {maybe_finished, UpdatedExecutionTree};
  %%   false ->
  %%     ExecutionTree#execution_tree{
  %%       active_children = UpdatedActiveChildren,
  %%       finished_children =
  %%         MaybeNewFinishedChild ++ NewFinishedChildren ++ FinishedChildren,
  %%       next_wakeup_tree = UpdatedWuT
  %%      }
  %% end.
  ExecutionTree.
%%------------------------------------------------------------------------------
%% UTIL
%%------------------------------------------------------------------------------

split_active_children(ActiveEvent, ActiveChildren) ->      
  Pred =
    fun(Child) ->
        not logically_equal(Child#execution_tree.event, ActiveEvent)
    end,
  lists:splitwith(Pred, ActiveChildren).

get_finished_children_from_done(Done, ExecutionTreeWuT) ->
  Pred =
    fun(Entry) ->
        Event = Entry#backtrack_entry_transferable.event,
        event_is_member(Event, Done)
    end,
  ExploredWuT = lists:filter(Pred, ExecutionTreeWuT),
  %% but add to new finished children only the tail of done
  [Entry#backtrack_entry_transferable.event || Entry <- ExploredWuT].

get_reduced_wut_from_done(Done, ExecutionTreeWuT) ->
  Pred =
    fun(Entry) ->
        Event = Entry#backtrack_entry_transferable.event,
        not event_is_member(Event, Done)
    end,
  %% filter wut with all events from done
  lists:filter(Pred, ExecutionTreeWuT).

get_finished_children_from_wut(FinishedNextWuT, ExecutionTreeWuT) ->
  NewFinishedChildren = [Entry#backtrack_entry_transferable.event || Entry <- FinishedNextWuT],
  Pred =
    fun(Entry) ->
        Event = Entry#backtrack_entry_transferable.event,
        not event_is_member(Event, NewFinishedChildren)
    end,
  UpdatedWuT = lists:filter(Pred, ExecutionTreeWuT),
  {NewFinishedChildren, UpdatedWuT}.

event_is_member(_, []) ->
  false;
event_is_member(Event, [H|T]) ->
  case same_actor(Event, H) of
    true ->
      true;
    false ->
      event_is_member(Event, T)
  end.

same_actor(Event1, Event2) ->
  logically_equal(Event1, Event2).
%%  Event1#event_transferable.actor =:= Event2#event_transferable.actor.

%%------------------------------------------------------------------------------

-spec distribute_interleavings(
        reduced_scheduler_state(),
        pos_integer()
       ) -> {reduced_scheduler_state(), [reduced_scheduler_state()], integer()}.


%% TODO HERE
%% - keep the backtrack size updated
%% - finish the spliting of the fragments
distribute_interleavings(State, FragmentsNeeded) ->
  #reduced_scheduler_state{
     backtrack_size = BacktrackSize,
     dpor = DPOR,
     trace = Trace
    } = State,
  EntriesGiven = min(FragmentsNeeded, BacktrackSize - 1),
  %%BacktrackSize = size_of_backtrack_transferable(Trace),
  {UpdatedTrace , NewFragmentTraces} =
    try
      distribute_interleavings_aux(lists:reverse(Trace),[], EntriesGiven, [], DPOR)
    catch _:_:_ ->
        io:fwrite("~p~nSizeBacktrack: ~w ~n", [lists:reverse(Trace),  BacktrackSize])
    end,
  NewFragments =
    [State#reduced_scheduler_state{
       backtrack_size = 1,
       safe = false,
       trace = T
      } || T <- NewFragmentTraces],
  %% IdleScheduler ! {explore, make_state_transferable(UnloadedState)},
  NewState =
    State#reduced_scheduler_state{
      backtrack_size = BacktrackSize - EntriesGiven,
      safe = false,
      trace = UpdatedTrace
     },
  {NewState, NewFragments, EntriesGiven}.

distribute_interleavings_aux(Trace, RevTracePrefix, 0, FragmentTraces, _) ->
  %% This fragment does not need to be splitted any further since
  %% either there is only one backtrack entry left or no more fragments
  %% are needed (the loop termination condition : the number of nodes 
  %% contained in fragment is =< 1 or Size(frontier) >= n)
  {lists:reverse(Trace) ++ RevTracePrefix, FragmentTraces};
distribute_interleavings_aux([#trace_state_transferable{
                                 wakeup_tree = WuT
                                } = TraceState | Rest],
                             RevTracePrefix,
                             N,
                             FragmentTraces,
                             DPOR)
  when WuT =:= [] ->
  distribute_interleavings_aux(Rest, [TraceState|RevTracePrefix], N, FragmentTraces, DPOR);
distribute_interleavings_aux([TraceState|Rest], RevTracePrefix, N, FragmentTraces, source) ->
  #trace_state_transferable{
     wakeup_tree = WuT,
     sleep_set = SleepSet,
     done = Done
    } = TraceState,
  [H|T] = Done,
  %%Eventify = [Entry#backtrack_entry_transferable.event || Entry <- WuT],
  %%[UnloadedEntry|RestBacktrack] = WuT,
  %%[UnloadedBacktrackEvent|RestBacktrackEvents] = Eventify,
  case split_wut_at_ownership(WuT, owned) of
    {WuT, []} ->
      distribute_interleavings_aux(Rest,
                                   [TraceState|RevTracePrefix],
                                   N,
                                   FragmentTraces,
                                   source);
    {NotOwnedWuT, [OwnedEntry|RestEntries]} ->
      UpdatedTraceState =
        TraceState#trace_state_transferable{
          %%sleep_set = [UnloadedBacktrackEvent|SleepSet],
          wakeup_tree = NotOwnedWuT ++ [fix_wut_ownership(OwnedEntry, not_owned)|RestEntries]%%,
          %%done = [H, UnloadedBacktrackEvent|T]
         },
      NewFragmentTraceState =
        TraceState#trace_state_transferable{
          %% wakeup_tree = [UnloadedEntry],
          %% sleep_set = RestBacktrackEvents ++ SleepSet, %% TODO check if this is needed
          wakeup_tree = NotOwnedWuT ++ [OwnedEntry|fix_wut_ownership(RestEntries, not_owned)]
          %%done = [H|RestBacktrackEvents] ++ T
         },
      NewFragmentTrace = [NewFragmentTraceState|RevTracePrefix],
      distribute_interleavings_aux([UpdatedTraceState|Rest],
                                   RevTracePrefix,
                                   N-1,
                                   [NewFragmentTrace|FragmentTraces],
                                   source)
  end.

split_wut_at_ownership(WuT, O) ->
  Pred =
    fun(Entry) ->
        determine_ownership(Entry) =/= O
    end,
  lists:splitwith(Pred, WuT).

determine_ownership(#backtrack_entry{ownership = O} = _) ->
  O;
determine_ownership(#backtrack_entry_transferable{ownership = O} = _) ->
  O.

fix_wut_ownership([], _) -> [];
fix_wut_ownership(
  [#backtrack_entry_transferable{wakeup_tree = WuT} = Entry|Rest],
  Ownership) ->
  FixedEntry = Entry#backtrack_entry_transferable{
    ownership = Ownership,
    wakeup_tree = fix_wut_ownership(WuT, Ownership)
   },
  [FixedEntry|fix_wut_ownership(Rest, Ownership)];
fix_wut_ownership(#backtrack_entry_transferable{wakeup_tree = WuT} = Entry, Ownership) ->
  Entry#backtrack_entry_transferable{
    ownership = Ownership,
    wakeup_tree = fix_wut_ownership(WuT, Ownership)
   }.

%% distribute_wut(_WuT, 0, DistributedWuTs) ->
%%   DistributedWuTs;
%% distribute_wut([], _N, DistributedWuTs) ->
%%   DistributedWuTs;
%% distribute_wut([BacktrackEntry], N, DistributedWuTs) ->
%%   #backtrack_entry_transferable{wakeup_tree = WuT} = BacktrackEntry,
%%   [
%%   distribute_wut(WuT, N, DistributedWuTs);
%% distribute_wut(Entries, N, DistributedWuTs) ->
  

%% %%------------------------------------------------------------------------------

%% -spec fix_ownership(
%%         reduced_scheduler_state(),
%%         [reduced_scheduler_state()],
%%         [reduced_scheduler_state()]
%%        ) ->
%%                        reduced_scheduler_state().

%% fix_ownership(Fragment, IdleFrontier, BusyFrontier) ->
%%   fix_ownership(Fragment, IdleFrontier ++ BusyFrontier).

%% fix_ownership(Fragment, []) ->
%%   FragmentTrace = Fragment#reduced_scheduler_state.trace,
%%   NewTrace = claim_ownership(FragmentTrace),
%%   Fragment#reduced_scheduler_state{trace = NewTrace};
%% fix_ownership(FragmentRequestingOwnership, [Fragment|Rest]) ->
%%   UpdatedFragmentTrace =
%%     remove_not_owned(
%%       FragmentRequestingOwnership#reduced_scheduler_state.trace,
%%       Fragment#reduced_scheduler_state.trace
%%      ),
%%   UpdatedFragment =
%%     FragmentRequestingOwnership#reduced_scheduler_state{
%%       trace = UpdatedFragmentTrace
%%      },
%%   fix_ownership(UpdatedFragment, Rest).

%% claim_ownership([]) ->
%%   [];
%% claim_ownership([#trace_state_transferable{wakeup_tree = WuT} = Trace|Rest]) ->
%%   UpdatedWuT = [Entry#backtrack_entry_transferable{ownership = true} || Entry <- WuT],
%%   [Trace#trace_state_transferable{wakeup_tree = UpdatedWuT}|claim_ownership(Rest)].

%% remove_not_owned(FragmentRequestingOwnership, Fragment) ->
%%   remove_not_owned(lists:reverse(FragmentRequestingOwnership),
%%                    lists:reverse(Fragment),
%%                    []).

%% remove_not_owned([], _, FixedFragment) ->
%%   FixedFragment;
%% remove_not_owned(Fragment, [], FixedFragmentPrefix) ->
%%   lists:reverse(Fragment) ++ FixedFragmentPrefix;
%% remove_not_owned([#trace_state_transferable{ownership = true} = TraceState|Rest],
%%                  _,
%%                  FixedFragmentPrefix) ->
%%   lists:reverse([TraceState|Rest]) ++ FixedFragmentPrefix;
%% remove_not_owned([TraceStateToBeFixed|RestToBeFixed], [TraceState|Rest], FixedFragmentPrefix) ->
%%   #trace_state_transferable{
%%      done = DoneToBeFixed,
%%      sleep_set = SleepToBeFixed,
%%      wakeup_tree = WuTToBeFixed
%%     } = TraceStateToBeFixed,
%%   #trace_state_transferable{
%%      done = Done,
%%      wakeup_tree = WuT
%%     } = TraceState,
%%   %% I fix the current wakeup_tree by my removing backtrack entries that
%%   %% I find either in the done set or is the backtrack of another fragment
%%   %% and add those in the sleep set
%%   {FixedWut, NewSleep} = remove_not_owned_wut(WuTToBeFixed, WuT, Done),
%%   FixedSleep = SleepToBeFixed ++ NewSleep,
%%   FixedTraceState =
%%     TraceStateToBeFixed#trace_state_transferable{
%%       sleep_set = FixedSleep,
%%       wakeup_tree = FixedWut
%%      },
%%   [EventToBeFixed|_] = DoneToBeFixed,
%%   [Event|_] = Done,
%%   ActorToBeFixed = EventToBeFixed#event_transferable.actor,
%%   Actor = Event#event_transferable.actor,
%%   case ActorToBeFixed =:= Actor of
%%     true ->
%%       remove_not_owned(RestToBeFixed, Rest, [FixedTraceState|FixedFragmentPrefix]);      
%%     false ->
%%       %% !!!!!!!
%%       %% This is just a heuristic, not sure if it is even sound
%%       %% !!!!!!!
%%       %% TODO make sure whether this holds or not
%%       RestFixed =
%%         case exists(EventToBeFixed, Done) of
%%           false ->
%%             Rest;
%%           true ->
%%             remove_all_not_owned(Rest)
%%         end,
%%       %% !!!!!!!
%%       %% TODO check if there is something additional I can remove here
%%       %% 1 2 3 vs 1 2 3' => claim ownership of backtrack underneath
%%       %% probably this only works as a heuristic
%%       %% TODO figure out if this is the correct course of action here
%%       lists:reverse([FixedTraceState|RestFixed]) ++ FixedFragmentPrefix
%%   end.

%% remove_all_not_owned([]) ->
%%   [];
%% remove_all_not_owned([#trace_state_transferable{ownership = true} = TraceState|Rest]) ->
%%   [TraceState|Rest];
%% remove_all_not_owned([#trace_state_transferable{wakeup_tree = WuT} = TraceState|Rest]) ->
%%   FixedWuT = [Entry || Entry <- WuT, Entry#backtrack_entry_transferable.ownership],
%%   FixedTraceState = TraceState#trace_state_transferable{wakeup_tree = FixedWuT},
%%   [FixedTraceState|remove_all_not_owned(Rest)].

%% remove_not_owned_wut(WuTToBeFixed, WuT, Done) ->
%%   remove_not_owned_wut(WuTToBeFixed, WuT, Done, [], []).

%% remove_not_owned_wut([], _, _, ReversedWuT, ReversedSleep) ->
%%   {lists:reverse(ReversedWuT), lists:reverse(ReversedSleep)};
%% remove_not_owned_wut([#backtrack_entry{ownership = true} = BacktrackEntry|Rest],
%%                      WuT,
%%                      Done,
%%                      ReversedWuT,
%%                      ReversedSleep) ->
%%   remove_not_owned_wut(Rest, WuT, Done, BacktrackEntry ++ ReversedWuT, ReversedSleep);
%% remove_not_owned_wut([#backtrack_entry{event = Event} = BacktrackEntry|Rest],
%%                      WuT,
%%                      Done,
%%                      ReversedWuT,
%%                      ReversedSleep) ->
%%   case exists(Event, WuT) orelse exists(Event, Done) of
%%     true ->
%%       remove_not_owned_wut(Rest, WuT, Done, ReversedWuT, Event ++ ReversedSleep);
%%     false ->
%%       remove_not_owned_wut(Rest, WuT, Done, BacktrackEntry ++ ReversedWuT, ReversedSleep)
%%   end.

%% exists(_, []) ->
%%   false;
%% exists(#event_transferable{actor = Actor1} = Event1,
%%        [#backtrack_entry_transferable{event = Event2}|Rest]) ->
%%   Actor2 = Event2#event_transferable.actor,
%%   case Actor1 =:= Actor2 of
%%     true ->
%%       true;
%%     false ->
%%       exists(Event1, Rest)
%%   end;
%% exists(#event_transferable{actor = Actor1} = Event1,
%%        [#event_transferable{actor = Actor2}|Rest]) ->
%%   case Actor1 =:= Actor2 of
%%     true ->
%%       true;
%%     false ->
%%       exists(Event1, Rest)
%%   end.

%%------------------------------------------------------------------------------
%% Playable state -> Tranferable State
%%------------------------------------------------------------------------------

make_state_transferable(State) ->
  #scheduler_state{
     dpor = DPOR,
     interleaving_id = InterleavingId,
     last_scheduled = LastScheduled,
     need_to_replay = NeedToReplay,
     origin = Origin,
     trace = Trace
    } = State,
  ets:new(ets_transferable, [named_table, public]),
  TransferableTrace =
    [make_trace_state_transferable(TraceState) || TraceState <- Trace],
  TransferableState =
    #reduced_scheduler_state{
       backtrack_size = size_of_backtrack(Trace),
       dpor = DPOR,
       interleaving_id = InterleavingId,
       last_scheduled = pid_to_list(LastScheduled),
       need_to_replay = NeedToReplay,
       origin = Origin,
       processes_ets_tables = ets:tab2list(ets_transferable),
       safe = true,
       trace = TransferableTrace
      },
  ets:delete(ets_transferable),
  TransferableState.

make_trace_state_transferable(TraceState) ->
  #trace_state{ 
     actors = Actors,
     clock_map = ClockMap,
     done = Done,
     enabled = Enabled,
     index = Index,
     ownership = Ownership,
     unique_id = UniqueId,
     previous_actor = PreviousActor,
     scheduling_bound = SchedulingBound,
     sleep_set = SleepSet,
     wakeup_tree = WakeupTree
    } = TraceState,
%  exit({ClockMap,?to_list(ClockMap)}),
  ClockMapList = [{make_key_transferable(Key), make_value_transferable(Value)}
                  || {Key, Value} <- ?to_list(ClockMap)],
  #trace_state_transferable{
     actors = [make_actor_transferable(Actor) || Actor <- Actors],
     clock_map = ClockMapList, %TODO maybe change
     done = [make_event_tranferable(Event) || Event <- Done],
     enabled = [make_actor_transferable(Actor) || Actor <- Enabled],
     index = Index,
     ownership = Ownership,
     unique_id = UniqueId,
     previous_actor = make_actor_transferable(PreviousActor),
     scheduling_bound = SchedulingBound,
     sleep_set = [make_event_tranferable(Event) || Event <- SleepSet],
     wakeup_tree = make_wakeup_tree_transferable(WakeupTree, Ownership)
    }.

%% TODO add message_queue_transferable pattern
make_actor_transferable('undefined') -> 'undefined';
make_actor_transferable('none') -> 'none';
make_actor_transferable({Pid1, Pid2}) ->
  {pid_to_list(Pid1), pid_to_list(Pid2)};
make_actor_transferable(Pid) when is_pid(Pid)->
  pid_to_list(Pid);
make_actor_transferable(MessageEventQueue) ->
  TransferableList = 
    [make_message_event_tranferable(Event) || Event <- queue:to_list(MessageEventQueue)],
  queue:from_list(TransferableList).

make_key_transferable('state') -> 'state';
make_key_transferable({Id, sent}) -> {make_id_transferable(Id), sent};
make_key_transferable(Actor) -> make_actor_transferable(Actor). 

%% TODO check if I need to add more patterns here
make_value_transferable('independent') -> 'independent';
make_value_transferable(VectorClock) ->
    [{make_actor_transferable(Actor), Index} || {Actor, Index} <- ?to_list(VectorClock)].

make_message_event_tranferable(MessageEvent) -> 
  #message_event{
     cause_label = CauseLabel,
     ignored = Ignored,
     instant = Instant,
     killing = Killing,
     message = Message,
     receive_info = Recipient_Info,
     recipient = Recipient,
     sender = Sender,
     trapping = Trapping,
     type = Type
    } = MessageEvent,
  #message_event_transferable{
     cause_label = CauseLabel,
     ignored = Ignored,
     instant = Instant,
     killing = Killing,
     message = make_message_tranferable(Message),
     receive_info = Recipient_Info,
     recipient = pid_to_list(Recipient),
     sender = pid_to_list(Sender),
     trapping = Trapping,
     type = Type
    }.

make_message_tranferable('after') ->
  'after';
make_message_tranferable(Message) ->
  #message{
     data = Data,
     id = Id
    } = Message,
  #message_transferable{
     data = make_term_transferable(Data),
     id = make_id_transferable(Id)
    }.

make_id_transferable({Pid, PosInt}) when is_pid(Pid) ->
  {pid_to_list(Pid), PosInt};
make_id_transferable(Other) -> Other.

-spec make_term_transferable(term()) -> term().

%% This looks in the content of the message for pids
%% and converts them to string.
%% TODO: check what else types are needed to be taken
%% into consideration
make_term_transferable(Term) when is_pid(Term) ->
%% Neeed this atom to let concuerror know when to revert a pidlist back to a pid.
%% I could generally avoid this but then I would have a problem when a message is a 
%% string that has the format of a pid. Then concuerror would try to make this message
%% into pid which would have different effects than what the user intended. This atom 
%% coulp perhaps be constructed by a reference or something similar istead, to ensure that
%% the user does not use this atom in a message.
  {concuerror_pid_list, pid_to_list(Term)};
make_term_transferable([]) -> 
  [];
make_term_transferable([H|T]) ->
  [make_term_transferable(H)|make_term_transferable(T)];
make_term_transferable(Term) when is_tuple(Term) ->
  list_to_tuple(make_term_transferable(tuple_to_list(Term)));
make_term_transferable(Term) ->
  Term.

make_event_tranferable(Event) ->
  #event{
     actor = Actor,
     event_info = EventInfo,
     label = Label,
     location = Location,
     special = Special
    } = Event,
  #event_transferable{
     actor = make_actor_transferable(Actor),
     event_info = make_event_info_transferable(EventInfo),
     label = Label,
     location = Location,
     special = make_term_transferable(Special)
    }.

make_event_info_transferable('undefined') -> 'undefined';
make_event_info_transferable(#message_event{} = MessageEvent) ->
  make_message_event_tranferable(MessageEvent);
make_event_info_transferable(#builtin_event{} = BuiltinEvent) ->
  #builtin_event{
     actor = Actor,
     extra = Extra,
     exiting = Exiting,
     mfargs = MFArgs, %% TODO check if I need to fix this as well
     result = Result, %% TODO check if I need to fix this as well
     status = Status,
     trapping = Trapping
    } = BuiltinEvent,
  {Module, Fun, Args} = MFArgs,
  TranferableArgs = [make_term_transferable(Arg) || Arg <- Args],
  Transferable = #builtin_event_transferable{
     actor = pid_to_list(Actor),
     extra = maybe_new_extra_transferable(BuiltinEvent),
     exiting = Exiting,
     mfargs = {Module, Fun, TranferableArgs}, %% TODO check if I need to fix this as well
     result = make_term_transferable(Result), %% TODO check if I need to fix this as well
     status = Status,
     trapping = Trapping
    },
  %% {_A,_B,_C} = MFArgs,
  %% case _B == 'spawn' of
  %%   true -> 
  %%     exit(BuiltinEvent,Transferable);
  %%   false ->
  %%     ok
  %% end,
  Transferable;
make_event_info_transferable(#receive_event{} = ReceiveEvent) ->
  #receive_event{
     message = Message,
     receive_info = ReceiveInfo,
     recipient = Recipient,
     timeout = Timeout,
     trapping = Trapping
    } = ReceiveEvent,
  #receive_event_transferable{
     message = make_message_tranferable(Message),
     receive_info = ReceiveInfo,
     recipient = pid_to_list(Recipient),
     timeout = Timeout,
     trapping = Trapping
    };
make_event_info_transferable(#exit_event{} = ExitEvent) ->
  #exit_event{
     actor = Actor,
     last_status = Running,
     exit_by_signal = ExitBySignal,
     links = Links,
     monitors = Monitors, %% TODO figure out what do with monitors and links
     name = Name,
     reason = Reason,
     stacktrace = StackTrace,
     trapping = Trapping
    } = ExitEvent,
  NewActor =
    case is_pid(Actor) of
      true ->
        pid_to_list(Actor);
      false ->
        Actor
    end,
  #exit_event_transferable{
     actor = NewActor,
     last_status = Running,
     exit_by_signal = ExitBySignal,
     links = [pid_to_list(Link) || Link <- Links],
     monitors = [{Ref, pid_to_list(Pid)} || {Ref, Pid} <- Monitors],
%% TODO figure out what do with monitors and links
     name = Name,
     reason = Reason, %% TODO maybe (probably) need to fix this as well
     stacktrace = StackTrace, %% maybe need to check this as well
     trapping = Trapping
    }.

%% maybe_new_ets_table(
%%   #builtin_event{
%%      extra = Extra,
%%      mfargs = {ets, new, Args}
%%     } = _) ->
%%   ets:insert(ets_transferable, {Extra, make_term_transferable(Args)});
%% maybe_new_ets_table(_) ->
%%   ok.

maybe_new_extra_transferable(
  #builtin_event{
     extra = Extra,
     mfargs = {ets, _, _}
    } = _E) 
  when is_reference(Extra) ->
  %%  exit({_E, ets:tab2list(ets_tid_to_ref)}),
  [{Extra, Ref, TransferableArgs}] = ets:lookup(ets_tid_to_ref, Extra),
  {Ref, TransferableArgs};
maybe_new_extra_transferable(
  #builtin_event{
    extra = Extra
   } = _) ->
  Extra.

make_wakeup_tree_transferable([], _) -> [];
make_wakeup_tree_transferable([Head|Rest], _TraceStateOwnership) ->
  #backtrack_entry{
     conservative = Conservative,
     event = Event,
     origin = Origin,
     ownership = Ownership,
     wakeup_tree = WUT
    } = Head,
  NewHead =
    #backtrack_entry_transferable{
       conservative = Conservative,
       event = make_event_tranferable(Event),
       origin = Origin,
       ownership = 
         case _TraceStateOwnership of
           true ->
             owned;
           false ->
             Ownership
         end, %%Ownership or _TraceStateOwnership,
       wakeup_tree = make_wakeup_tree_transferable(WUT, _TraceStateOwnership) %% this make sense for optimal, maybe need to change
      },
  [NewHead|make_wakeup_tree_transferable(Rest, _TraceStateOwnership)].

%%------------------------------------------------------------------------------
%% Tranferable state -> Playable State
%%------------------------------------------------------------------------------

revert_state(PreviousState, ReducedState) ->
  #reduced_scheduler_state{
     interleaving_id = InterleavingId,
     last_scheduled = LastScheduled,
     need_to_replay = NeedToReplay,
     origin = Origin,
     %% processes_ets_tables = ProcessesEtsTables,
     safe = Safe, %% safe meens that this fragment has not been distributed
     %% TODO maybe remove this
     trace = Trace
    } = ReducedState,
  %% ets:new(ets_transferable, [named_table, public]),
  %% maybe_create_new_tables(ProcessesEtsTables),
  NewState =
    PreviousState#scheduler_state{
    interleaving_id = InterleavingId,
    last_scheduled = list_to_pid(LastScheduled),
    need_to_replay = NeedToReplay,
    origin = Origin,
    trace = [revert_trace_state(TraceState, Safe) || TraceState <- Trace]
   },
  %% ets:delete(ets_transferable),
  NewState.

%% maybe_create_new_tables([]) ->
%%   ok;
%% maybe_create_new_tables([{OldTid, Args}|Rest]) ->
%%   case node(OldTid) =:= node() of
%%     true ->
%%       %% no need to create new ets_table
%%       ok;
%%     false ->
%%       %% create new ets_table
%%       [Name, Options] = revert_term(Args),
%%       NoNameOptions = [O || O <- Options, O =/= named_table],
%%       NewTid = ets:new(Name, NoNameOptions ++ [public]),
%%       %% true = ets:give_away(T, Scheduler, given_to_scheduler), TODO remove
%%       ets:insert(ets_transferable, {OldTid, NewTid}),
%%       maybe_create_new_tables(Rest)       
%%   end.
        
revert_trace_state(TraceState, Safe) ->
  #trace_state_transferable{ 
     actors = Actors,
     clock_map = ClockMapList,
     done = Done,
     enabled = Enabled,
     index = Index,
     ownership = Ownership,
     unique_id = UniqueId,
     previous_actor = PreviousActor,
     scheduling_bound = SchedulingBound,
     sleep_set = SleepSet,
     wakeup_tree = WakeupTree
    } = TraceState,
  ClockMap = ?from_list([{revert_key(Key), revert_value(Value)}
                         || {Key, Value} <- ClockMapList]),
  #trace_state{
     actors = [revert_actor(Actor) || Actor <- Actors],
     clock_map = ClockMap, %TODO maybe change
     done = [revert_event(Event) || Event <- Done],
     enabled = [revert_actor(Actor) || Actor <- Enabled],
     index = Index,
     ownership = Ownership and Safe,
     unique_id = UniqueId,
     previous_actor = revert_actor(PreviousActor),
     scheduling_bound = SchedulingBound,
     sleep_set = [revert_event(Event) || Event <- SleepSet],
     wakeup_tree = revert_wakeup_tree(WakeupTree)
    }.

%% TODO add message_queue_transferable pattern
revert_actor('undefined') -> 'undefined';
revert_actor('none') -> 'none';
revert_actor({PidList1, PidList2}) ->
  {list_to_pid(PidList1), list_to_pid(PidList2)};
revert_actor(PidList) when is_list(PidList) ->
  list_to_pid(PidList);
revert_actor(MessageEventQueue) ->
  RevertedList = 
    [make_message_event_tranferable(Event) || Event <- queue:to_list(MessageEventQueue)],
  queue:from_list(RevertedList).


revert_key('state') -> 'state';
revert_key({Id, sent}) -> {revert_id(Id), sent};
revert_key(Actor) -> revert_actor(Actor).

revert_value('independent') -> 'independent';
revert_value(VectorClockList) ->
  ?from_list([{revert_actor(Actor), Index} || {Actor, Index} <- VectorClockList]).

revert_message_event(MessageEvent) -> 
  #message_event_transferable{
     cause_label = CauseLabel,
     ignored = Ignored,
     instant = Instant,
     killing = Killing,
     message = Message,
     receive_info = Recipient_Info,
     recipient = Recipient,
     sender = Sender,
     trapping = Trapping,
     type = Type
    } = MessageEvent,
  #message_event{
     cause_label = CauseLabel,
     ignored = Ignored,
     instant = Instant,
     killing = Killing,
     message = revert_message(Message),
     receive_info = Recipient_Info,
     recipient = list_to_pid(Recipient),
     sender = list_to_pid(Sender),
     trapping = Trapping,
     type = Type
    }.

revert_message('after') ->
  'after';
revert_message(Message) ->
  #message_transferable{
     data = Data,
     id = Id
    } = Message,
  #message{
     data = revert_term(Data),
     id = revert_id(Id)
    }.

%% TODO check this!!! what happens when Pid is 'user' (e.g)
revert_id({PidList, PosInt}) when not is_atom(PidList) ->
  {list_to_pid(PidList), PosInt};
revert_id(Other) -> Other.


%% This looks in the content of the message for pids
%% and converts them to string.
%% TODO: check what else types are needed to be taken
%% into consideration

revert_term({concuerror_pid_list, PidList}) ->
  list_to_pid(PidList);
revert_term([]) -> 
  [];
revert_term([H|T]) ->
  [revert_term(H)|revert_term(T)];
revert_term(Term) when is_tuple(Term) ->
  list_to_tuple(revert_term(tuple_to_list(Term)));
revert_term(Term) ->
  Term.

revert_event(Event) ->
  #event_transferable{
     actor = Actor,
     event_info = EventInfo,
     label = Label,
     location = Location,
     special = Special
    } = Event,
  #event{
     actor = revert_actor(Actor),
     event_info = revert_event_info(EventInfo),
     label = Label,
     location = Location,
     special = revert_term(Special)
    }.

revert_event_info('undefined') -> 'undefined';
revert_event_info(#message_event_transferable{} = MessageEvent) ->
  revert_message_event(MessageEvent);
revert_event_info(#builtin_event_transferable{} = BuiltinEvent) ->
  #builtin_event_transferable{
     actor = Actor,
     extra = _Extra,
     exiting = Exiting,
     mfargs = MFArgs, %% TODO check if I need to fix this as well
     result = Result, %% TODO check if I need to fix this as well
     status = Status,
     trapping = Trapping
    } = BuiltinEvent,
  {Module, Fun, Args} = MFArgs,
  RevertedArgs = [revert_term(Arg) || Arg <- Args],
  #builtin_event{
     actor = list_to_pid(Actor),
     extra = maybe_change_extra(BuiltinEvent),
     exiting = Exiting,
     mfargs = {Module, Fun, RevertedArgs}, %% TODO check if I need to fix this as well
     result = revert_term(Result), %% TODO check if I need to fix this as well
     status = Status,
     trapping = Trapping
    };
revert_event_info(#receive_event_transferable{} = ReceiveEvent) ->
  #receive_event_transferable{
     message = Message,
     receive_info = ReceiveInfo,
     recipient = Recipient,
     timeout = Timeout,
     trapping = Trapping
    } = ReceiveEvent,
  #receive_event{
     message = revert_message(Message),
     receive_info = ReceiveInfo,
     recipient = list_to_pid(Recipient),
     timeout = Timeout,
     trapping = Trapping
    };
revert_event_info(#exit_event_transferable{} = ExitEvent) ->
  #exit_event_transferable{
     actor = Actor,
     last_status = Running,
     exit_by_signal = ExitBySignal,
     links = Links,
     monitors = Monitors, %% TODO figure out what do with monitors and links
     name = Name,
     reason = Reason,
     stacktrace = StackTrace,
     trapping = Trapping
    } = ExitEvent,
  NewActor =
    case is_reference(Actor) of
      false ->
        list_to_pid(Actor);
      true ->
        Actor
    end,
  #exit_event{
     actor = NewActor,
     last_status = Running,
     exit_by_signal = ExitBySignal,
     links = [list_to_pid(Link) || Link <- Links],
     monitors = [{Ref, list_to_pid(Pid)} || {Ref, Pid} <- Monitors],
%% TODO figure out what do with monitors and links
     name = Name,
     reason = Reason,
     stacktrace = StackTrace, %% maybe need to check this as well
     trapping = Trapping
    }.

maybe_change_extra(
  #builtin_event_transferable{
     extra = {Ref, TransferableArgs},
     mfargs = {ets, _, _}}
  = _) ->
  case ets:lookup(ets_ref_to_tid, Ref) of
    [] ->
      %% this ets table does not exist here
      [Name, Options] = revert_term(TransferableArgs),
      NoNameOptions = [O || O <- Options, O =/= named_table],
      NewTid = ets:new(Name, NoNameOptions ++ [public]),
      %% true = ets:give_away(T, Scheduler, given_to_scheduler), TODO remove
      ets:insert(ets_tid_to_ref, {NewTid, Ref, TransferableArgs}),
      ets:insert(ets_ref_to_tid, {Ref, NewTid, TransferableArgs}),
      NewTid;
    [{Ref, Extra, TransferableArgs}] -> 
      Extra
  end;
maybe_change_extra(#builtin_event_transferable{extra = Extra} = _) ->
   Extra.

revert_wakeup_tree([]) -> [];
revert_wakeup_tree([Head|Rest]) ->
  #backtrack_entry_transferable{
     conservative = Conservative,
     event = Event,
     origin = Origin,
     ownership = Ownership,
     wakeup_tree = WUT
    } = Head,
  NewHead =
    #backtrack_entry{
       conservative = Conservative,
       event = revert_event(Event),
       origin = Origin,
       ownership = Ownership,
       wakeup_tree = revert_wakeup_tree(WUT)
      },
  [NewHead|revert_wakeup_tree(Rest)].

%% =============================================================================

-spec explain_error(term()) -> string().

explain_error({blocked_mismatch, I, Event, Depth}) ->
  EString = concuerror_io_lib:pretty_s(Event, Depth),
  io_lib:format(
    "On step ~p, replaying a built-in returned a different result than"
    " expected:~n"
    "  original:~n"
    "    ~s~n"
    "  new:~n"
    "    blocked~n"
    ?notify_us_msg,
    [I,EString]
   );
explain_error({optimal_sleep_set_block, Origin, Who}) ->
  io_lib:format(
    "During a run of the optimal algorithm, the following events were left in~n"
    "a sleep set (the race was detected at interleaving #~p)~n~n"
    "  ~p~n"
    ?notify_us_msg,
    [Origin, Who]
   );
explain_error({replay_mismatch, I, Event, NewEvent, Depth}) ->
  [EString, NEString] =
    [concuerror_io_lib:pretty_s(E, Depth) || E <- [Event, NewEvent]],
  [Original, New] =
    case EString =/= NEString of
      true -> [EString, NEString];
      false ->
        [io_lib:format("~p",[E]) || E <- [Event, NewEvent]]
    end,
  io_lib:format(
    "On step ~p, replaying a built-in returned a different result than"
    " expected:~n"
    "  original:~n"
    "    ~s~n"
    "  new:~n"
    "    ~s~n"
    ?notify_us_msg,
    [I,Original,New]
   ).

%%==============================================================================

msg(after_timeout_tip) ->
  "You can use e.g. '--after_timeout 5000' to treat after timeouts that exceed"
    " some threshold (here 4999ms) as 'infinity'.~n";
msg(assertions_only_filter) ->
  "Only assertion failures are considered abnormal exits ('--assertions_only').~n";
msg(assertions_only_use) ->
  "A process exited with reason '{{assert*,_}, _}'. If you want to see only"
    " this kind of error you can use the '--assertions_only' option.~n";
msg(depth_bound_reached) ->
  "An interleaving reached the depth bound. This can happen if a test has an"
    " infinite execution. Concuerror is not sound for testing programs with"
    " infinite executions. Consider limiting the size of the test or increasing"
    " the bound ('-h depth_bound').~n";
msg(maybe_receive_loop) ->
  "The trace contained more than ~w receive timeout events"
    " (receive statements that executed their 'after' clause). Concuerror by"
    " default treats 'after' clauses as always possible, so a 'receive loop'"
    " using a timeout can lead to an infinite execution. "
    ++ msg(after_timeout_tip);
msg(scheduling_bound_tip) ->
  "Running without a scheduling_bound corresponds to verification and"
    " may take a long time.~n";
msg(scheduling_bound_warning) ->
  "Some interleavings will not be explored because they exceed the scheduling"
    " bound.~n";
msg(show_races) ->
  "You can see pairs of racing instructions (in the report and"
    " '--graph') with '--show_races true'~n";
msg(shutdown) ->
  "A process exited with reason 'shutdown'. This may happen when a"
    " supervisor is terminating its children. You can use '--treat_as_normal"
    " shutdown' if this is expected behaviour.~n";
msg(stop_first_error) ->
  "Stop testing on first error. (Check '-h keep_going').~n";
msg(timeout) ->
  "A process exited with reason '{timeout, ...}'. This may happen when a"
    " call to a gen_server (or similar) does not receive a reply within some"
    " timeout (5000ms by default). "
    ++ msg(after_timeout_tip);
msg(treat_as_normal) ->
  "Some abnormal exit reasons were treated as normal ('--treat_as_normal').~n".
