-module(concuerror).

%% Main entry point.
-export([run/1]).

%%------------------------------------------------------------------------------

%% Internal export
-export([maybe_cover_compile/0, maybe_cover_export/1]).

%%------------------------------------------------------------------------------

-export_type([exit_status/0]).

-type exit_status() :: 'ok' | 'error' | 'fail'.

-include("concuerror.hrl").

%%------------------------------------------------------------------------------

-spec run(concuerror_options:options()) -> exit_status().

run(RawOptions) ->
  maybe_cover_compile(),
  Status =
    case concuerror_options:finalize(RawOptions) of
      {ok, Options, LogMsgs} ->
        %% I should split finalize into two parts in order not to make  
        %% unnecessary code repetitions
        case ?opt(parallel, Options) of 
          false ->
            start(Options, LogMsgs);
          true ->
            start_parallel(RawOptions, Options)
        end;
      {exit, ExitStatus} -> ExitStatus
    end,
  maybe_cover_export(RawOptions),
  Status.

%%------------------------------------------------------------------------------

start(Options, LogMsgs) ->
  error_logger:tty(false),
  Parallel = ?opt(parallel, Options),
  Processes = ets:new(processes, [public]),
  Estimator = concuerror_estimator:start_link(Options),
  LoggerOptions = [{estimator, Estimator},{processes, Processes}|Options],
  Logger = concuerror_logger:start(LoggerOptions),
  _ = [?log(Logger, Level, Format, Args) || {Level, Format, Args} <- LogMsgs],
  SchedulerOptions = 
    case Parallel of
      false ->
        ProcessSpawner = concuerror_process_spawner:start(Options),
        [{logger, Logger},
         {process_spawner, ProcessSpawner}
         | LoggerOptions];
      true ->
        [{logger, Logger}
         | LoggerOptions]
    end,
  {Pid, Ref} =
    spawn_monitor(concuerror_scheduler, run, [SchedulerOptions]),
  Reason = receive {'DOWN', Ref, process, Pid, R} -> R end,
  SchedulerStatus =
    case Reason =:= normal of
      true -> normal;
      false ->
        ?error(Logger, "~s~n", [explain(Reason)]),
        failed
    end,
  ?trace(Logger, "Reached the end!~n",[]),
  ExitStatus = concuerror_logger:stop(Logger, SchedulerStatus),
  concuerror_estimator:stop(Estimator),
  case Parallel of 
    false ->
      concuerror_process_spawner:stop(?opt(process_spawner, SchedulerOptions));
    true ->
      ok
  end,
  ets:delete(Processes),
  ExitStatus.

%%------------------------------------------------------------------------------

start_parallel(RawOptions, OldOptions) ->
  Nodes = concuerror_nodes:start(RawOptions),
  [Node1,Node2] = Nodes,
  % The the process_spawner starts and stops will be fixed when I make the
  % process_spawner a gen_server
  SpawnerOptions = [{nodes, Nodes}|OldOptions],
  {Controller, ControllerRef} = spawn_monitor(fun() -> controller_initialize() end),
  ProcessSpawner = concuerror_process_spawner:start(SpawnerOptions),
  StartAux =
    fun() ->
	Status =
	  case concuerror_options:finalize(RawOptions) of
	    {ok, Options, LogMsgs} ->
              ParallelOptions = [ {nodes, Nodes}
                                , {controller, Controller}
                                , {process_spawner, ProcessSpawner} 
                                  | Options],
              start(ParallelOptions, LogMsgs);
	    {exit, ExitStatus} -> ExitStatus
	  end,
	  exit(Status)
    end,
  Pid1 = spawn(Node1, StartAux),
  Ref1 = monitor(process, Pid1),
  Pid2 = spawn(Node2, StartAux),
  Ref2 = monitor(process, Pid2),
  ExitStatus = 
    receive
      {'DOWN', Ref1, process, Pid1, ExitStatus1} ->
        ExitStatus1;
      {'DOWN', Ref2, process, Pid2, ExitStatus2} -> 
        ExitStatus2
    end,
  ok = concuerror_nodes:clear(Nodes),
  concuerror_process_spawner:stop([{process_spawner, ProcessSpawner}]),
  ExitStatus.

%%------------------------------------------------------------------------------

-spec maybe_cover_compile() -> 'ok'.

maybe_cover_compile() ->
  Cover = os:getenv("CONCUERROR_COVER"),
  if Cover =/= false ->
      case cover:is_compiled(?MODULE) of
        false ->
          EbinDir = filename:dirname(code:which(?MODULE)),
          _ = cover:compile_beam_directory(EbinDir),
          ok;
        _ -> ok
      end;
     true -> ok
  end.

%%------------------------------------------------------------------------------

-spec maybe_cover_export(term()) -> 'ok'.

maybe_cover_export(Args) ->
  Cover = os:getenv("CONCUERROR_COVER"),
  if Cover =/= false ->
      Hash = binary:decode_unsigned(erlang:md5(term_to_binary(Args))),
      Out = filename:join([Cover, io_lib:format("~.16b",[Hash])]),
      cover:export(Out),
      ok;
     true -> ok
  end.

%%------------------------------------------------------------------------------

explain(Reason) ->
  try
    {Module, Info} = Reason,
    Module:explain_error(Info)
  catch
    _:_ ->
      io_lib:format("~n  Reason: ~p", [Reason])
  end.
