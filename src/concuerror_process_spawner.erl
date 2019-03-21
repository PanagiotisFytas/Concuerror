%% @doc A server that pre-spawns processes used in a test.
%%
%% The reason for this server's existence is to coordinate the
%% spawning of processes. This is required when Concuerror is using
%% slave nodes to explore schedulings in parallel, to ensure that the
%% same (local) PIDs are used in all such nodes.

-module(concuerror_process_spawner).

%% Interface to concuerror.erl
-export([start/1, stop/1, spawn_link/3]).
-export([explain_error/1]).

%%-----------------------------------------------------------------------------

-include("concuerror.hrl").

%%-----------------------------------------------------------------------------

-ifdef(BEFORE_OTP_17).
-type process_queue() :: queue().
-else.
-type process_queue() :: queue:queue(pid()).
-endif.

-record(spawner_state, {
          allocated_processes = maps:new()  :: map(),
          available_processes = queue:new() :: process_queue(),
          max_processes                     :: non_neg_integer(),
          parallel                          :: boolean(),
          slave_spawners = []               :: [pid()]
         }).

%%-----------------------------------------------------------------------------

-spec start(concuerror_options:options()) -> pid().

start(Options) ->
  Parent = self(),
  Fun =
    fun() ->
        State = initialize_spawner(Options),
        Parent ! process_gen_ready,
        case ?opt(parallel, Options) of 
          false ->
            spawner_loop(State);
          true ->
            master_loop(State)
        end
    end,
  P = spawn_link(Fun),
  receive
    process_gen_ready -> P
  end.

initialize_spawner(Options) ->
  MaxProcesses = ?opt(max_processes, Options) + length(registered()),
  Parallel = ?opt(parallel, Options),
  State = #spawner_state{
             max_processes = MaxProcesses,
             parallel = Parallel
            },
  case Parallel of
    false ->
      Fun = fun() -> wait_activation() end,
      AvailableProcesses = [spawn_link(Fun) || _ <- lists:seq(1, MaxProcesses)],
      State#spawner_state{
        available_processes = queue:from_list(AvailableProcesses)
       };
    true ->
      Nodes = ?opt(nodes, Options),
      Master = self(),
      Fun = fun() -> initialize_slave_spawner(Master, MaxProcesses) end,
      Spawners = [spawn_link(Node, Fun) || Node <- Nodes],
      InitialPidList = get_initial_pid(Spawners, []),
      lists:foreach(fun (SP) -> SP ! {start, InitialPidList} end, Spawners),
      wait_for_spawners(Spawners),
      State#spawner_state{
        slave_spawners = Spawners
       }
  end.

wait_activation() ->
  receive
    {activate, {Module, Name, Args}} ->
      erlang:apply(Module, Name, Args);
    stop -> ok;
    {Pid, stop} ->
      Pid ! done
  end.

%%-----------------------------------------------------------------------------

get_initial_pid([], InitialPidLists) ->
  Pids = [list_to_pid(PidList) || PidList <- InitialPidLists],
  pid_to_list(lists:max(Pids));
get_initial_pid(Spawners, InitialPidLists) ->
  receive
    {Spawner, InitialPidList} ->
      true = lists:member(Spawner, Spawners),
      get_initial_pid(lists:delete(Spawner, Spawners), [InitialPidList|InitialPidLists])
  end.

wait_for_spawners([]) ->
  ok;
wait_for_spawners(Spawners) ->
  receive
    {Spawner, ready} ->
      true = lists:member(Spawner, Spawners),
      wait_for_spawners(lists:delete(Spawner, Spawners))
  end.

initialize_slave_spawner(Master, MaxProcesses) ->
  Fun = fun() -> wait_activation() end,
  MyInitialPid = spawn_link(Fun),
  Master ! {self(), pid_to_list(MyInitialPid)},
  receive
    {start, InitialPidList} ->
      InitialPid = list_to_pid(InitialPidList),
      InitialPid = discard_pids(MyInitialPid, InitialPid),
      Master ! {self(), ready},
      AvailableProcesses =
        [InitialPid| [spawn_link(Fun) || _ <- lists:seq(1, MaxProcesses-1)]],
      SlaveState =
        #spawner_state{
           available_processes = queue:from_list(AvailableProcesses), 
           max_processes = MaxProcesses,
           parallel = true
          },
        spawner_loop(SlaveState)
  end.

discard_pids(TargetPid, TargetPid) ->
  TargetPid;
discard_pids(CurrentPid, TargetPid) ->
  CurrentPid ! stop,
  Fun = fun() -> wait_activation() end,
  NextPid = spawn_link(Fun),
  discard_pids(NextPid, TargetPid).

%%-----------------------------------------------------------------------------
  
spawner_loop(State) ->
  #spawner_state{available_processes = ProcessQueue} = State,
  receive
    {Caller, get_new_process, _Symbol} ->
      case queue:out(ProcessQueue) of
        {empty, ProcessQueue} ->
          Caller ! {process_limit_exceeded, State#spawner_state.max_processes - length(registered())},
          spawner_loop(State);
        {{value, Process}, NewProcessQueue} ->
          unlink(Process),
          Caller ! {new_process, Process, self()},
          spawner_loop(State#spawner_state{available_processes = NewProcessQueue})
      end;
    {Pid, cleanup} ->
      %% TODO make this better
      _ = [IdleProcess ! {self(), stop} || IdleProcess <- queue:to_list(ProcessQueue)],
      _ = [receive done -> ok end || _ <- queue:to_list(ProcessQueue)],
      Pid ! done
  end.

master_loop(State) ->
  #spawner_state{
     allocated_processes = ProcessMap,
     slave_spawners = SlaveSpawners
    } = State,
  receive
    {Caller, get_new_process, Symbol} ->
      Key = atom_to_list(node(Caller)) ++ Symbol,
      case maps:take(Key, ProcessMap) of
        %% TODO check that removing the process from the map does not cause any errors
        {Process, NewProcessMap} ->
          unlink(Process),
          Caller ! {new_process, Process, self()},
          master_loop(State#spawner_state{allocated_processes = NewProcessMap});
        error ->
          %% key does not exist
          lists:foreach(fun (SP) -> SP ! {self(), get_new_process, Symbol} end,
                        SlaveSpawners),
          %% try and allocate processes to nodes and symbols
          try receive_new_processes(ProcessMap, Symbol, length(SlaveSpawners)) of UpdatedProcessMap -> 
              {Process, NewProcessMap} = maps:take(Key, UpdatedProcessMap),
              unlink(Process),
              Caller ! {new_process, Process, self()},
              master_loop(State#spawner_state{allocated_processes = NewProcessMap})
          catch
            throw:Reason ->
              Reason = {process_limit_exceeded, State#spawner_state.max_processes - length(registered())},
              Caller ! Reason,
              master_loop(State)
          end
      end;
    {Pid, cleanup} ->
      lists:foreach(fun (SP) -> SP ! {self(), cleanup} end, SlaveSpawners),
      lists:foreach(fun (__) -> receive done -> ok end end, SlaveSpawners),
      Pid ! done
  end.

receive_new_processes(ProcessMap, _, 0) ->
  ProcessMap;
receive_new_processes(ProcessMap, Symbol, N) ->
  receive
    {new_process, Process, Spawner} ->
      link(Process),
      Key = atom_to_list(node(Spawner)) ++ Symbol,
      NewProcessMap = maps:put(Key, Process, ProcessMap),
      receive_new_processes(NewProcessMap, Symbol, N-1);
    {process_limit_exceeded, MaxProcesses} ->
      throw({process_limit_exceeded, MaxProcesses})
  end.

%%-----------------------------------------------------------------------------

-spec spawn_link(pid(), {module(), atom(), [term()]}, string()) -> pid().

spawn_link(ProcessSpawner, MFArgs, Symbol) ->
  ProcessSpawner ! {self(), get_new_process, Symbol},
  receive
    {new_process, Pid, ProcessSpawner} ->
      %% Needed to properly propagate crash messages!
      link(Pid),
      Pid ! {activate, MFArgs},
      Pid;
    {process_limit_exceeded, MaxProcesses} ->
      ?crash({process_limit_exceeded, MaxProcesses})
  end.

%%-----------------------------------------------------------------------------

-spec stop(pid()) -> 'ok'.

stop(ProcessSpawner) ->
  ProcessSpawner ! {self(), cleanup},
  receive
    done -> ok
  end.

%%-----------------------------------------------------------------------------

-spec explain_error(term()) -> string().

explain_error({process_limit_exceeded, MaxProcesses}) ->
  io_lib:format(
    "Your test is using more than ~w processes (--max_processes)."
    " You can specify a higher limit, but consider using fewer processes.",
    [MaxProcesses]).
