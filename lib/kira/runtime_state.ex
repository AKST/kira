defmodule Kira.RuntimeState do
  require Kira.Branch, as: Branch
  require Kira.BranchState, as: BranchState
  require Kira.Progress, as: Progress
  require Kira.Util, as: Util

  defstruct [:config, :branch_states, :running, :timeout, :progress]

  @type branches :: %{required(atom()) => BranchState.t()}
  @type running :: %{required(pid) => atom}
  @type t() :: %__MODULE__{
          config: any,
          branch_states: branches(),
          timeout: timeout(),
          running: running,
          progress: Progress.t()
        }

  @spec create(config :: any, branch_list :: [Branch.t()], timeout :: timeout) :: Util.result(t)
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

      {:ok,
       %__MODULE__{
         config: config,
         timeout: timeout,
         running: %{},
         progress: Progress.create(length(branch_list)),
         branch_states: branch_states
       }}
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

      Util.result_reduce(dependencies, %{}, fn requirement_name, collected ->
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
    updated_unapply_state = fn dependent_name, state ->
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
  def mark_as_applied(state = %__MODULE__{}, branch_name, value) do
    updated_blocked_state = fn blocked_name, state ->
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
  def mark_as_unapplied(state = %__MODULE__{}, branch_name) do
    # tells the branches which this branch was dependent on, that
    # it has finished unappling
    updated_blocked_state = fn blocked_name, state ->
      with {:ok, branch} <- get_branch(state, blocked_name) do
        branch = %{
          branch
          | awaiting_unapply: MapSet.delete(branch.awaiting_unapply, branch_name)
        }

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
    updated_unapply_state = fn blocked_name, state ->
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
  def record_failure(state = %__MODULE__{}, branch_name, error) do
    with {:ok, branch} <- get_branch(state, branch_name) do
      error_pair = {error, DateTime.utc_now()}
      errors = [error_pair | BranchState.get_errors(branch)]
      set_branch_task(state, branch_name, {:failed, errors})
    end
  end

  @spec record_apply_retry(state :: t, branch_name :: atom, pid :: pid) :: Util.result(t)
  def record_apply_retry(state = %__MODULE__{}, branch_name, pid) do
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

  @spec set_branch_task(state :: t, branch_name :: atom, task :: BranchState.task()) ::
          Util.result(t)
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
    |> Map.values()
    |> Enum.reduce([], append_if_error)
    |> Enum.sort_by(time_as_unix)
  end

  @spec get_done(state :: t) :: %{required(atom) => any}
  def get_done(state) do
    pairs = Map.to_list(state.branch_states)

    Enum.reduce(pairs, %{}, fn {k, branch_s}, acc ->
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
