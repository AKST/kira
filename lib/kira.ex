defmodule Kira do
  require Logger

  defmodule Util do
    @type result(a) :: {:ok, a} | {:error, any}

    # @spec result_reduce(
    #   lists :: [any],
    #   value :: any,
    #   f :: (any, any -> result(any))
    # ) :: result(any)
    def result_reduce(lists, value, f) do
      Enum.reduce(lists, {:ok, value}, fn
        (next, {:ok, value}) -> f.(next, value)
        (_, e = {:error, _}) -> e
      end)
    end

    def fetch(map, key, error) when is_map(map) do
      case Map.fetch(map, key) do
        {:ok, v} -> {:ok, v}
        :error -> {:error, error}
      end
    end
  end

  defmodule Branch do
    defmodule Defaults do
      def apply_default(_, _), do: {:error, :unimplemented}
    end

    defstruct [
      :name,
      dependencies: [],
      apply: &Defaults.apply_default/2,
      unapply: :undefined,
      on_apply_error: :undefined,
      on_unapply_error: :undefined,
    ]

    @type retry :: {:retry, boolean} | {:retry_after, integer}

    @type dependencies :: %{required(atom) => any}
    @type apply_t :: (any, dependencies -> Util.result(any))
    @type unapply_t :: :undefined | (any, dependencies, any -> Util.result(any))
    @type on_apply_error_t :: :undefined | (any, dependencies, any, integer -> retry)
    @type on_unapply_error_t :: :undefined | (any, dependencies, any, integer -> retry)

    @type t() :: %Branch{
      name: atom,
      dependencies: [atom],
      apply: apply_t,
      unapply: unapply_t,
      on_apply_error: on_apply_error_t,
      on_unapply_error: on_unapply_error_t,
    }
  end

  defmodule BranchState do
    defstruct [
      :branch,
      :awaiting,
      :blocking,
      :awaiting_unapply,
      :blocking_unapply,
      :task,
    ]

    @type errors :: [{any, DateTime.t()}]
    @type resolved :: any
    @type task ::
      :not_started
      | {:running_apply, pid(), errors}
      | {:running_apply_retry, pid(), errors}
      | {:running_unapply, pid(), resolved, errors}
      | {:running_unapply_retry, pid(), resolved, errors}
      | {:failed, errors}
      | {:done_applied, any}
      | :done_unapplied

    @type branches :: %{required(atom()) => Branch.t()}

    @type t() :: %BranchState{
      branch: Branch.t(),
      awaiting: MapSet.t(atom()),
      blocking: MapSet.t(atom()),
      awaiting_unapply: MapSet.t(atom()),
      blocking_unapply: MapSet.t(atom()),
      task: task(),
    }

    @spec create(branch :: Branch.t, branches :: branches()) :: t()
    def create(branch, branches) do
      awaiting = Enum.into(branch.dependencies, MapSet.new())
      blocking = for {t, desc} <- branches,
        Enum.member?(desc.dependencies, branch.name),
        into: MapSet.new(),
        do: t

      %BranchState{
        branch: branch,
        awaiting: awaiting,
        blocking: blocking,
        awaiting_unapply: MapSet.new(),
        blocking_unapply: awaiting,
        task: :not_started}
    end

    @spec apply_ready?(state :: t()) :: boolean()
    def apply_ready?(state) do
      MapSet.size(state.awaiting) == 0
    end

    @spec unapply_ready?(state :: t()) :: boolean()
    def unapply_ready?(state) do
      MapSet.size(state.awaiting_unapply) == 0 && is_complete(state)
    end

    @spec set_task(branch :: t(), task :: task) :: t()
    def set_task(branch, task) do
      %{branch | task: task}
    end

    @spec get_completed(branch :: t()) :: Util.result(any)
    def get_completed(t) do
      case t.task do
        {:done_applied, value} ->
          {:ok, value}

        {:running_unapply, _, value} ->
          {:ok, value}

        _ ->
          {:error, {:unable_to_get_task_value, t.branch.name, t.task}}
      end
    end

    @spec is_complete(branch :: t()) :: boolean
    def is_complete(t) do
      case t.task do
        {:done_applied, value} -> true

        _ -> false
      end
    end

    @spec get_task_pid(branch :: t()) :: Util.result(pid)
    def get_task_pid(branch_state) do
      case branch_state.task do
        {:running_apply, pid, _} ->
          {:ok, pid}

        {:running_apply_retry, pid, _} ->
          {:ok, pid}

        {:running_unapply, pid, _, _} ->
          {:ok, pid}

        {:running_unapply_retry, pid, _, _} ->
          {:ok, pid}

        _ ->
          {:error, {:unable_to_get_task_pid, branch_state.branch.name, branch_state.task}}
      end
    end

    def get_errors(branch_state) do
      case branch_state.task do
        {:running_apply, _, errors} -> errors
        {:running_apply_retry, _, errors} -> errors
        {:running_unapply, _, _, errors} -> errors
        {:running_unapply_retry, _, _, errors} -> errors
        {:failed, errors} -> errors
        _ -> []
      end
    end
  end

  defmodule Progress do
    @moduledoc """
    Originally progress was tracked in RuntimeState as a integer that started
    out as the number of branches, and would indecate completion once equal
    to 0. But that really didn't accomodate for rollbacks, so this data
    type was created to track progression with a richer data type.
    """

    # TODO check if we need the sets of what is doing what
    defstruct [
      :applied,
      :unapplied,
      :total,
      :applies_running,
      :applies_retrying,
      :unapplies_running,
      :unapplies_retrying,
    ]

    @type t :: %Progress{
      total: integer,
      applied: integer,
      unapplied: integer,
      applies_running: MapSet.t(atom),
      applies_retrying: MapSet.t(atom),
      unapplies_running: MapSet.t(atom),
      unapplies_retrying: MapSet.t(atom),
    }

    @spec create(total :: integer) :: t
    def create(total) do
      %Progress{
        total: total,
        applied: 0,
        unapplied: 0,
        applies_running: MapSet.new(),
        applies_retrying: MapSet.new(),
        unapplies_running: MapSet.new(),
        unapplies_retrying: MapSet.new()}
    end

    @spec record_apply_start(progress :: t, name :: atom) :: t
    def record_apply_start(progress, name) do
      %{progress | applies_running: MapSet.put(progress.applies_running, name)}
    end

    @spec record_apply_failure(progress :: t, name :: atom) :: t
    def record_apply_failure(progress, name) do
      %{progress | applies_running: MapSet.delete(progress.applies_running, name)}
    end

    @spec record_apply_done(progress :: t, name :: atom) :: t
    def record_apply_done(progress, name) do
      %{progress |
        applied: progress.applied + 1,
        applies_running: MapSet.delete(progress.applies_running, name)}
    end

    @spec record_unapply_start(progress :: t, name :: atom) :: t
    def record_unapply_start(progress, name) do
      %{progress | unapplies_running: MapSet.put(progress.unapplies_running, name)}
    end

    @spec record_unapply_done(progress :: t, name :: atom) :: t
    def record_unapply_done(progress, name) do
      %{progress |
        unapplied: progress.unapplied + 1,
        unapplies_running: MapSet.delete(progress.unapplies_running, name)}
    end

    @spec apply_done?(progress :: t) :: boolean
    def apply_done?(progress) do
      progress.applied == progress.total
        && MapSet.size(progress.applies_running) == 0
        && MapSet.size(progress.applies_retrying) == 0
    end

    @spec unapply_done?(progress :: t) :: boolean
    def unapply_done?(progress) do
      progress.unapplied == progress.applied
        && MapSet.size(progress.applies_running) == 0
        && MapSet.size(progress.applies_retrying) == 0
        && MapSet.size(progress.unapplies_running) == 0
        && MapSet.size(progress.unapplies_retrying) == 0
    end
  end

  defmodule RuntimeState do
    defstruct [:config, :branch_states, :running, :timeout, :progress]

    @type branches :: %{required(atom()) => BranchState.t}
    @type running :: %{required(pid) => atom}
    @type t() :: %RuntimeState{
      config: any,
      branch_states: branches(),
      timeout: timeout(),
      running: running,
      progress: Progress.t,
    }

    @spec create(config :: any, branch_list :: [Branch.t], timeout :: timeout) :: Util.result(t)
    def create(config, branch_list, timeout) do
      branches =
        for b <- branch_list,
          into: %{},
          do: {b.name, b}

      if Enum.count(branch_list) == Enum.count(branches) do
        branch_states =
          for b <- branch_list,
            into: %{},
            do: {b.name, BranchState.create(b, branches)}

        {:ok, %RuntimeState{
          config: config,
          timeout: timeout,
          running: %{},
          progress: Progress.create(length(branch_list)),
          branch_states: branch_states}}
      else
        {:error, :duplicate_branch_names}
      end
    end

    @spec get_branch(state :: t(), branch_name :: atom) :: Util.result(BranchState.t())
    def get_branch(state, branch_name) do
      fetch_error = runtime_state_error({:failed_to_find_branch, branch_name})
      Util.fetch(state.branch_states, branch_name, fetch_error)
    end

    @spec get_branch_from_pid(state :: t(), pid :: pid) :: Util.result(BranchState.t())
    def get_branch_from_pid(state, pid) do
      fetch_error = runtime_state_error({:failed_to_find_branch_from, pid})
      with {:ok, branch_name} <- Util.fetch(state.running, pid, fetch_error) do
        get_branch(state, branch_name)
      end
    end

    @spec find_apply_ready(state :: t()) :: MapSet.t(atom())
    def find_apply_ready(state) do
      for {t, state} <- state.branch_states,
        BranchState.apply_ready?(state),
        into: MapSet.new(),
        do: t
    end

    @spec find_unapply_ready(state :: t()) :: MapSet.t(atom())
    def find_unapply_ready(state) do
      for {t, state} <- state.branch_states,
        BranchState.unapply_ready?(state),
        into: MapSet.new(),
        do: t
    end

    @spec resolve_dependencies_of(state :: t, branch_name :: atom) :: Util.result(map)
    def resolve_dependencies_of(state, branch_name) do
      with {:ok, b} <- get_branch(state, branch_name) do
        dependencies = b.branch.dependencies

        Util.result_reduce(dependencies, %{}, fn(requirement_name, collected) ->
          with {:ok, branch} <- get_branch(state, requirement_name),
               {:ok, value} <- BranchState.get_completed(branch) do
            {:ok, Map.put(collected, requirement_name, value)}
          end
        end)
      end
    end

    @doc """
    Records the fact that the process has started running, and also prevents
    any of the branches that depend on it from start rolling back.
    """
    @spec mark_as_applying(state :: t, branch_name :: atom, pid :: pid) :: Util.result(t)
    def mark_as_applying(state, branch_name, pid) do
      # Ensures none of it's dependencies can start rolling back while it's running.
      updated_unapply_state = fn(dependent_name, state) ->
        with {:ok, branch} <- get_branch(state, dependent_name) do
          awaiting_unapply = MapSet.put(branch.awaiting_unapply, branch_name)
          branch = %{branch | awaiting_unapply: awaiting_unapply}
          branches = Map.put(state.branch_states, dependent_name, branch)
          {:ok, %{state | branch_states: branches}}
        end
      end

      with {:ok, branch} <- get_branch(state, branch_name),
           errors = BranchState.get_errors(branch),
           {:ok, state} <- set_branch_task(state, branch_name, {:running_apply, pid, errors}),
           {:ok, state} <- Util.result_reduce(branch.blocking_unapply, state, updated_unapply_state) do
        # record the fact the this pid is associated with this branch name
        running = Map.put(state.running, pid, branch_name)
        progress = Progress.record_apply_start(state.progress, branch_name)
        {:ok, %{state | running: running, progress: progress}}
      end
    end

    @spec mark_as_applied(state :: t, branch_name :: atom, value :: any) :: Util.result(t)
    def mark_as_applied(state = %RuntimeState{}, branch_name, value) do
      updated_blocked_state = fn(blocked_name, state) ->
        with {:ok, branch} <- get_branch(state, blocked_name) do
          branch = %{branch | awaiting: MapSet.delete(branch.awaiting, branch_name)}
          branches = Map.put(state.branch_states, blocked_name, branch)
          {:ok, %{state | branch_states: branches}}
        end
      end

      with {:ok, pid} <- get_branch_pid(state, branch_name),
           {:ok, state} <- set_branch_task(state, branch_name, {:done_applied, value}),
           {:ok, branch} <- get_branch(state, branch_name),
           {:ok, state} <- Util.result_reduce(branch.blocking, state, updated_blocked_state) do
        running = Map.drop(state.running, [pid])
        progress = Progress.record_apply_done(state.progress, branch_name)
        {:ok, %{state | running: running, progress: progress}}
      end
    end

    @spec mark_as_unapplied(state :: t, branch_name :: atom) :: Util.result(t)
    def mark_as_unapplied(state = %RuntimeState{}, branch_name) do
      # tells the branches which this branch was dependent on, that
      # it has finished unappling
      updated_blocked_state = fn(blocked_name, state) ->
        with {:ok, branch} <- get_branch(state, blocked_name) do
          branch = %{branch | awaiting_unapply: MapSet.delete(branch.awaiting_unapply, branch_name)}
          branches = Map.put(state.branch_states, blocked_name, branch)
          {:ok, %{state | branch_states: branches}}
        end
      end

      with {:ok, pid} <- get_branch_pid(state, branch_name),
           {:ok, state} <- set_branch_task(state, branch_name, :done_unapplied),
           {:ok, branch} <- get_branch(state, branch_name),
           {:ok, state} <- Util.result_reduce(branch.blocking_unapply, state, updated_blocked_state) do
        running = Map.drop(state.running, [pid])
        progress = Progress.record_unapply_done(state.progress, branch_name)
        {:ok, %{state | running: running, progress: progress}}
      end
    end

    @doc """
    Ensure anything that would be awaiting for this task to be rolled back is no
    longer blocked by the task associated with this branch_name.
    """
    @spec unblock_dependend_unapplys(state :: t, branch_name :: atom) :: Util.result(t)
    def unblock_dependend_unapplys(state, branch_name) do
      updated_unapply_state = fn(blocked_name, state) ->
        with {:ok, branch} <- get_branch(state, blocked_name) do
          awaiting_unapply = MapSet.delete(branch.awaiting_unapply, branch_name)
          branch = %{branch | awaiting_unapply: awaiting_unapply}
          branches = Map.put(state.branch_states, blocked_name, branch)
          {:ok, %{state | branch_states: branches}}
        end
      end

      with {:ok, branch_s} <- get_branch(state, branch_name) do
        Util.result_reduce(branch_s.blocking_unapply, state, updated_unapply_state)
      end
    end

    @doc """
    When you record a failure with a branch it doesn't necessary mean this process
    has completely failed, as there is an oppurtunity for it to be reattmpted. But
    it is important for the exception to be recorded, so if a rollback is necessary
    we can identify the culprit that made the rollback necessary.
    """
    @spec record_failure(state :: t, branch_name :: atom, error :: any) :: Util.result(t)
    def record_failure(state = %RuntimeState{}, branch_name, error) do
      with {:ok, branch} <- get_branch(state, branch_name) do
        error_pair = {error, DateTime.utc_now()}
        errors = [error_pair | BranchState.get_errors(branch)]
        set_branch_task(state, branch_name, {:failed, errors})
      end
    end

    @spec record_apply_retry(state :: t, branch_name :: atom, pid :: pid) :: Util.result(t)
    def record_apply_retry(state = %RuntimeState{}, branch_name, pid) do
      with {:ok, branch} <- get_branch(state, branch_name) do
        errors = BranchState.get_errors(branch)
        set_branch_task(state, branch_name, {:running_apply_retry, pid, errors})
      end
    end

    @spec get_branch_pid(state :: t, branch_name :: atom) :: Util.result(pid)
    def get_branch_pid(state, branch_name) do
      with {:ok, branch} <- get_branch(state, branch_name) do
        BranchState.get_task_pid(branch)
      end
    end

    @spec set_branch_task(state :: t, branch_name :: atom, task :: BranchState.task) :: Util.result(t)
    def set_branch_task(state, branch_name, task) do
      with {:ok, branch} <- get_branch(state, branch_name) do
        branch = BranchState.set_task(branch, task)
        {:ok, %{state | branch_states: Map.put(state.branch_states, branch_name, branch)}}
      end
    end

    @spec get_errors(state :: t) :: [any]
    def get_errors(state) do
      append_if_error = fn b, acc -> BranchState.get_errors(b) ++ acc end

      time_as_unix = fn {_, t} -> DateTime.to_unix(t) end

      state.branch_states
        |> Map.values
        |> Enum.reduce([], append_if_error)
        |> Enum.sort_by(time_as_unix)
    end

    @spec get_done(state :: t) :: %{ required(atom) => any }
    def get_done(state) do
      pairs = Map.to_list(state.branch_states)
      Enum.reduce(pairs, %{}, fn ({k, branch_s}, acc) ->
        case branch_s.task do
          {:done_applied, value} -> Map.put(acc, k, value)
          _anything_else -> acc
        end
      end)
    end

    @spec apply_done?(state :: t) :: boolean()
    def apply_done?(state), do: Progress.apply_done?(state.progress)

    @spec unapply_done?(state :: t) :: boolean()
    def unapply_done?(state), do: Progress.unapply_done?(state.progress)

    def runtime_state_error(error), do: {:error, {:runtime_state_error, error}}
  end

  defmodule Runtime do
    defmodule Unapply do
      @spec unapply_dispatch(
        source :: pid,
        branch :: Branch.t,
        config :: any,
        dependencies :: map(),
        value :: any
      ) :: any
      def unapply_dispatch(source, branch, config, dependencies, value) do
        case branch.unapply do
          unapply when is_function(unapply, 3) ->
            result = unapply.(config, dependencies, value)
            send source, {:unapply_exit, branch, result}
          :undefined ->
            send source, {:unapply_exit, branch, {:ok, :undefined}}
        end
      end

      @spec start_impl(
        state :: RuntimeState.t,
        branch_state :: BranchState.t,
        dependencies :: map,
        value :: any
      ) :: Util.result(RuntimeState.t)
      defp start_impl(state = %RuntimeState{}, branch_state, dependencies, value) do
        branch = branch_state.branch
        pid = spawn_link(__MODULE__, :unapply_dispatch, [self(), branch, state.config, dependencies, value])
        task_s = {:running_unapply, pid, value, BranchState.get_errors(branch_state)}

        with {:ok, state} <- RuntimeState.set_branch_task(state, branch.name, task_s) do
          {:ok, %{state | running: Map.put(state.running, pid, branch.name)}}
        end
      end

      @spec start(state :: RuntimeState.t, branch_name :: atom) :: Util.result(RuntimeState.t)
      def start(state = %RuntimeState{}, branch_name) do
        with {:ok, dependencies} <- RuntimeState.resolve_dependencies_of(state, branch_name),
             {:ok, branch_state} <- RuntimeState.get_branch(state, branch_name),
             {:ok, branch_value} <- BranchState.get_completed(branch_state) do
          start_impl(state, branch_state, dependencies, branch_value)
        end
      end
    end

    defmodule Apply do
      @spec apply_dispatch(source :: pid, branch :: Branch.t, config :: any, dependencies :: map()) :: any
      def apply_dispatch(source, branch, config, dependencies) do
        result = branch.apply.(config, dependencies)
        send source, {:apply_exit, branch, result}
      end

      @spec on_apply_error_dispatch(
        source :: pid,
        branch :: Branch.t,
        config :: any,
        dependencies :: map(),
        errors :: BranchState.errors
      ) :: any
      def on_apply_error_dispatch(source, branch, config, dependencies, errors) do
        case branch.on_apply_error do
          on_apply_error when is_function(on_apply_error, 4) ->
            {latest_error, _} = List.first(errors)
            error_count = Enum.count(errors)
            result = on_apply_error.(config, dependencies, latest_error, error_count)
            send source, {:apply_retry_exit, branch, result}

          :undefined ->
            send source, {:apply_retry_exit, branch, {:retry, false}}

          other ->
            Logger.error "[task_tree.on_apply_error_dispatch] invalid on_apply_error, not retrying"
            send source, {:apply_retry_exit, branch, {:retry, false}}
        end
      end

      @spec start_impl(
        state :: RuntimeState.t,
        branch_state :: BranchState.t,
        dependencies :: map
      ) :: Util.result(RuntimeState.t)
      defp start_impl(state = %RuntimeState{}, branch_state, dependencies) do
        branch = branch_state.branch
        pid = spawn_link(__MODULE__, :apply_dispatch, [self(), branch, state.config, dependencies])
        RuntimeState.mark_as_applying(state, branch.name, pid)
      end

      @spec start(state :: RuntimeState.t, branch_name :: atom) :: Util.result(RuntimeState.t)
      def start(state = %RuntimeState{}, branch_name) do
        with {:ok, dependencies} <- RuntimeState.resolve_dependencies_of(state, branch_name),
             {:ok, branch_state} <- RuntimeState.get_branch(state, branch_name) do
          start_impl(state, branch_state, dependencies)
        end
      end

      @doc """
      Here we'll determine if the process should be reattempted or we need to start a rollback.
      """
      @spec reattmpt_failed(state :: RuntimeState.t, branch :: Branch.t, error :: any) :: Util.result(any)
      def reattmpt_failed(state, branch, error) do
        with {:ok, pid} <- RuntimeState.get_branch_pid(state, branch.name),
             {:ok, state} <- RuntimeState.record_failure(state, branch.name, error),
             {:ok, dependencies} <- RuntimeState.resolve_dependencies_of(state, branch.name),
             {:ok, branch_s} <- RuntimeState.get_branch(state, branch.name) do
          errors = BranchState.get_errors(branch_s)
          args = [self(), branch_s.branch, state.config, dependencies, errors]
          retrying_pid = spawn_link(__MODULE__, :on_apply_error_dispatch, args)

          state = %{state |
            running: state.running
              |> Map.drop([pid])
              |> Map.put(retrying_pid, branch.name),
            progress: state.progress
              |> Progress.record_apply_failure(branch.name)}

          RuntimeState.record_apply_retry(state, branch.name, retrying_pid)
        end
      end
    end

    @spec unapply_loop(state :: RuntimeState.t) :: {:error, any}
    def unapply_loop(state) do
      next_step = receive do
        {:unapply_exit, branch, {:ok, _}} ->
          with {:ok, state} <- RuntimeState.mark_as_unapplied(state, branch.name) do
            state.branch_states[branch.name].blocking_unapply
              |> Enum.filter(fn t -> Enum.empty?(state.branch_states[t].awaiting_unapply) end)
              |> Util.result_reduce(state, &(Unapply.start(&2, &1)))
          end

        {:unapply_exit, _, {:error, _}} ->
          {:error, {:not_implemented, {:unapply_loop, :error_unapply_exit}}}

        {:unapply_exit, b, _} ->
          {:error, {:bad_unapply_exit_value, b}}

        {:apply_exit, branch, {:ok, value}} ->
          with {:ok, state} <- RuntimeState.mark_as_applied(state, branch.name, value) do
            Unapply.start(state, branch.name)
          end

        {:apply_exit, _, {:error, _}} ->
          {:error, {:not_implemented, {:unapply_loop, :bad_apply_exit}}}

        {:apply_retry_exit, _branch, msg} ->
          {:error, {:not_implemented, {:unapply_loop, :apply_retry_exit}}}

        # We're safe to ignore this, because this is the result of a
        # linked child process exiting normally. The reason we receive
        # this is because when we spawn a child with `spawn_link` for
        # the sake of tracking a bad exit.
        {:EXIT, _, :normal} ->
          {:ok, state}

        {:EXIT, _, reason} ->
          {:error, {:not_implemented, {:unapply_loop, :process_down, reason}}}
      after state.timeout ->
        {:error, {:not_implemented, {:timeout, state}}}
      end

      with {:ok, state} <- next_step do
        # TODO(return the first error that caused the rollback)
        if RuntimeState.unapply_done?(state),
          do: {:error, RuntimeState.get_errors(state)},
          else: unapply_loop(state)
      end
    end

    @spec apply_loop(state :: RuntimeState.t) :: Util.result(RuntimeState.t)
    def apply_loop(state = %RuntimeState{}) do
      next_step = receive do
        {:apply_exit, branch, {:ok, value}} ->
          with {:ok, state} <- RuntimeState.mark_as_applied(state, branch.name, value) do
            state.branch_states[branch.name].blocking
              |> Enum.filter(fn t -> Enum.empty?(state.branch_states[t].awaiting) end)
              |> Util.result_reduce(state, &(Apply.start(&2, &1)))
          end

        {:apply_exit, branch, {:error, error}} ->
          Apply.reattmpt_failed(state, branch, error)

        # TODO start unapply here
        # - will need to mark that the process has failed, Apply.reattmpt_failed
        # - start unapply with the value, {:start_unapply, state}
        {:apply_exit, _branch, _} ->
          {:error, :bad_return_value}

        # when we want to retry a previously failed task
        {:apply_retry_exit, branch, {:retry, true}} ->
          Apply.start(state, branch.name)

        # when a task we checked if we should retry decides it cannot be retried we do this.
        {:apply_retry_exit, branch, {:retry, false}} ->
          with {:ok, state} <- RuntimeState.unblock_dependend_unapplys(state, branch.name) do
            {:start_unapply, state}
          end

        # TODO start unapply here
        # - will need to mark that the process has failed, Apply.reattmpt_failed
        # - start unapply with the value, {:start_unapply, state}
        {:apply_retry_exit, _branch, msg} ->
          {:error, {:not_implemented, {:apply_retry_exit, :EXIT, msg}}}

        {:EXIT, _, :normal} ->
          {:ok, state}

        {:EXIT, pid, exception} ->
          with {:ok, branch_s} <- RuntimeState.get_branch_from_pid(state, pid) do
            Apply.reattmpt_failed(state, branch_s.branch, {:expection, exception})
          end
      after state.timeout ->
        {:error, {:not_implemented, {:timeout, state}}}
      end

      case next_step do
        {:ok, state} ->
          if RuntimeState.apply_done?(state),
            do: {:ok, RuntimeState.get_done(state)},
            else: apply_loop(state)

        {:start_unapply, state} ->
          start_unapply_loop(state)

        {:error, _} = e -> e
      end
    end

    @spec start_unapply_loop(state :: RuntimeState.t) :: {:error, any}
    def start_unapply_loop(state) do
      ready_tasks = RuntimeState.find_unapply_ready(state)
      reducer = &Unapply.start(&2, &1)

      with {:ok, state} <- Util.result_reduce(ready_tasks, state, reducer) do
        unapply_loop(state)
      end
    end

    @spec run_tasks(config :: any, tasks :: list(Branch.t()), timeout :: timeout) :: Util.result(map)
    def run_tasks(config, tasks, timeout \\ :infinity) do
      parent = self()

      # run in own process to ensure it's self contained, and we don't
      # leak stuff like process flags or don't get any rogue messages
      # from stuff that would have be intended for the parent.
      own_pid = spawn(fn ->
        Process.flag(:trap_exit, true)

        with {:ok, state} <- RuntimeState.create(config, tasks, timeout) do
          ready_tasks = RuntimeState.find_apply_ready(state)
          if !Enum.empty?(ready_tasks) do
            reducer = &Apply.start(&2, &1)

            with {:ok, state} <- Util.result_reduce(ready_tasks, state, reducer) do
              send(parent, {:exit_value, apply_loop(state)})
            end
          else
            send(parent, {:exit_value, {:error, :unstartable}})
          end
        end
      end)

      own_ref = Process.monitor(own_pid)

      receive do
        {:exit_value, value} ->
          # ensure the monitor process down message isn't leaked
          # into the processes message box and is cleaned up.
          receive do {:DOWN, ^own_ref, :process, ^own_pid, _} -> value end

        # Sometimes shit happens, but if this happens there's a good
        # chance it's a bug in this module, file, etc.
        {:DOWN, ^own_ref, :process, ^own_pid, reason} ->
          # sorry this should never happen <emoji>grimmace</emoji>
          {:error, {:unacceptable_internal_failure, reason}}
      end
    end
  end
end
