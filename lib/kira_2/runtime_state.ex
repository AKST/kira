defmodule Kira2.RuntimeState do
  require Kira2.TaskDefinition, as: TaskDefinition
  require Kira2.Task, as: Task
  require Kira.Progress, as: Progress
  require Kira.Util, as: Util

  @moduledoc false

  defstruct [:config, :tasks, :pid_cache, :timeout, :progress]

  @type tasks :: %{required(atom()) => Task.t()}
  @type pid_cache :: %{required(pid) => atom}
  @type t() :: %__MODULE__{
          config: any,
          tasks: tasks(),
          timeout: timeout(),
          pid_cache: pid_cache,
          progress: Progress.t()
        }

  @spec create(config :: any, t_defs :: [TaskDefinition.t()], timeout :: timeout) :: Util.result(t)
  def create(config, t_def_list, timeout) do
    t_def_map =
      for td <- t_def_list,
          into: %{},
          do: {td.name, td}

    if Enum.count(t_def_list) == Enum.count(t_def_map) do
      tasks =
        for td <- t_def_list,
            into: %{},
            do: {td.name, Task.create(td, t_def_map)}

      {:ok,
       %__MODULE__{
         tasks: tasks,
         config: config,
         timeout: timeout,
         pid_cache: %{},
         progress: Progress.create(length(t_def_list))
       }}
    else
      {:error, :duplicate_task_names}
    end
  end

  @spec find_task(state :: t(), task_name :: atom) :: Util.result(Task.t())
  def find_task(state, task_name) do
    fetch_error = runtime_state_error({:failed_to_find_task, task_name})
    Util.fetch(state.tasks, task_name, fetch_error)
  end

  @spec find_task_pid(state :: t, task_name :: atom) :: Util.result(pid)
  def find_task_pid(state, task_name) do
    with {:ok, task} <- find_task(state, task_name) do
      Task.get_pid(task)
    end
  end

  @spec find_task_by_pid(state :: t(), pid :: pid) :: Util.result(Task.t())
  def find_task_by_pid(state, pid) do
    fetch_error = runtime_state_error({:failed_to_find_task_from, pid})

    with {:ok, task_name} <- Util.fetch(state.pid_cache, pid, fetch_error) do
      find_task(state, task_name)
    end
  end

  @spec find_apply_ready(state :: t()) :: MapSet.t(atom())
  def find_apply_ready(state) do
    for {t, state} <- state.tasks,
        Task.apply_ready?(state),
        into: MapSet.new(),
        do: t
  end

  @spec find_unapply_ready(state :: t()) :: MapSet.t(atom())
  def find_unapply_ready(state) do
    for {t, state} <- state.tasks,
        Task.unapply_ready?(state),
        into: MapSet.new(),
        do: t
  end

  @spec resolve_dependencies_of(state :: t, task_name :: atom) :: Util.result(map)
  def resolve_dependencies_of(state, task_name) do
    with {:ok, task} <- find_task(state, task_name) do
      dependencies = task.definition.dependencies

      Util.result_reduce(dependencies, %{}, fn requirement_name, collected ->
        with {:ok, o_task} <- find_task(state, requirement_name),
             {:ok, value} <- Task.get_completed(o_task) do
          {:ok, Map.put(collected, requirement_name, value)}
        end
      end)
    end
  end

  @doc """
  Records the fact that the process has started running, and also prevents
  any of the tasks that depend on it from start rolling back.
  """
  @spec mark_as_applying(state :: t, task_name :: atom, pid :: pid) :: Util.result(t)
  def mark_as_applying(state, task_name, pid) do
    block_unapply_for_task = fn other_task ->
      Task.put_in_awaiting_unapply(other_task, task_name)
    end

    with {:ok, task} <- find_task(state, task_name),
         {:ok, state} <- set_task_state(state, task_name, {:running_apply, pid, Task.get_errors(task)}),
         {:ok, state} <- map_tasks(state, task.blocking_unapply, block_unapply_for_task) do
      # record the fact the this pid is associated with this task name
      pid_cache = Map.put(state.pid_cache, pid, task_name)
      progress = Progress.record_apply_start(state.progress, task_name)
      {:ok, %{state | pid_cache: pid_cache, progress: progress}}
    end
  end

  @spec mark_as_applied(state :: t, task_name :: atom, value :: any) :: Util.result(t)
  def mark_as_applied(state = %__MODULE__{}, task_name, value) do
    update_blocking = fn state, task_name ->
      with {:ok, task} <- find_task(state, task_name),
           do: map_tasks(state, task.blocking, &Task.drop_from_awaiting(&1, task_name))
    end

    with {:ok, pid} <- find_task_pid(state, task_name),
         {:ok, state} <- set_task_state(state, task_name, {:done_applied, value}),
         {:ok, state} <- update_blocking.(state, task_name) do
      pid_cache = Map.drop(state.pid_cache, [pid])
      progress = Progress.record_apply_done(state.progress, task_name)
      {:ok, %{state | pid_cache: pid_cache, progress: progress}}
    end
  end

  @spec mark_as_unapplied(state :: t, task_name :: atom) :: Util.result(t)
  def mark_as_unapplied(state = %__MODULE__{}, task_name) do
    with {:ok, pid} <- find_task_pid(state, task_name),
         {:ok, state} <- set_task_state(state, task_name, :done_unapplied),
         {:ok, state} <- unblock_dependend_unapplys(state, task_name) do
      pid_cache = Map.drop(state.pid_cache, [pid])
      progress = Progress.record_unapply_done(state.progress, task_name)
      {:ok, %{state | pid_cache: pid_cache, progress: progress}}
    end
  end

  @doc """
  Ensure anything that would be awaiting for this task to be rolled back is no
  longer blocked by the task associated with this task_name.
  """
  @spec unblock_dependend_unapplys(state :: t, task_name :: atom) :: Util.result(t)
  def unblock_dependend_unapplys(state, task_name) do
    with {:ok, task} <- find_task(state, task_name) do
      map_tasks(state, task.blocking_unapply, &Task.drop_from_awaiting_unapply(&1, task_name))
    end
  end

  @doc """
  When you record a failure with a task it doesn't necessary mean this process
  has completely failed, as there is an oppurtunity for it to be reattmpted. But
  it is important for the exception to be recorded, so if a rollback is necessary
  we can identify the culprit that made the rollback necessary.
  """
  @spec record_failure(state :: t, task_name :: atom, error :: any) :: Util.result(t)
  def record_failure(state = %__MODULE__{}, task_name, error) do
    with {:ok, task} <- find_task(state, task_name) do
      error_pair = {error, DateTime.utc_now()}
      set_task_state(state, task_name, {:failed, [error_pair | Task.get_errors(task)]})
    end
  end

  @spec record_apply_retry(state :: t, task_name :: atom, pid :: pid) :: Util.result(t)
  def record_apply_retry(state = %__MODULE__{}, task_name, pid) do
    with {:ok, task} <- find_task(state, task_name) do
      set_task_state(state, task_name, {:running_apply_retry, pid, Task.get_errors(task)})
    end
  end

  @spec set_task_state(state :: t, task_name :: atom, state :: Task.state()) :: Util.result(t)
  def set_task_state(state, task_name, task_state) do
    with {:ok, task} <- find_task(state, task_name) do
      {:ok, %{state | tasks: Map.put(state.tasks, task_name, Task.set_state(task, task_state))}}
    end
  end

  @spec get_errors(state :: t) :: [any]
  def get_errors(state) do
    append_if_error = fn b, acc -> Task.get_errors(b) ++ acc end

    time_as_unix = fn {_, t} -> DateTime.to_unix(t) end

    state.tasks
    |> Map.values()
    |> Enum.reduce([], append_if_error)
    |> Enum.sort_by(time_as_unix)
  end

  @spec get_done(state :: t) :: %{required(atom) => any}
  def get_done(state) do
    pairs = Map.to_list(state.tasks)

    Enum.reduce(pairs, %{}, fn {k, task}, acc ->
      case task.task do
        {:done_applied, value} -> Map.put(acc, k, value)
        _anything_else -> acc
      end
    end)
  end

  @spec map_tasks(i_state :: t, task_names :: Enum.t(), (Task.t() -> Task.t())) :: Util.result(t)
  def map_tasks(i_state, task_names, update) do
    Util.result_reduce(task_names, i_state, fn task_name, state ->
      with {:ok, task} <- find_task(state, task_name) do
        {:ok, %{state | tasks: Map.put(state.tasks, task_name, update.(task))}}
      end
    end)
  end

  @spec apply_done?(state :: t) :: boolean()
  def apply_done?(state), do: Progress.apply_done?(state.progress)

  @spec unapply_done?(state :: t) :: boolean()
  def unapply_done?(state), do: Progress.unapply_done?(state.progress)

  def runtime_state_error(error), do: {:error, {:runtime_state_error, error}}
end
