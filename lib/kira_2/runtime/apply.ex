defmodule Kira2.Runtime.Apply do
  require Kira2.TaskDefinition, as: TaskDefinition
  require Kira2.Task, as: Task
  require Kira2.Progress, as: Progress
  require Kira2.RuntimeState, as: RuntimeState
  require Kira.Util, as: Util

  require Logger

  @moduledoc false

  @doc """
  The initial entry point for a tasks apply pass.
  """
  @spec apply_dispatch(
          source :: pid,
          task_def :: TaskDefinition.t(),
          config :: any,
          dependencies :: map()
        ) :: any
  def apply_dispatch(source, td, config, dependencies) do
    case td.dispatcher do
      callback when is_function(callback, 2) ->
        result = td.dispatcher.(td.name, {:apply, config, dependencies})
        send(source, {:apply_exit, td, result})

      _other ->
        Logger.error("[task_tree.on_apply_error_dispatch] invalid on_apply_error, not retrying")
        # TODO figure out a more appropiate message type
        send(source, {:apply_exit, td, :exit})
    end
  end

  @doc """
  The entry point on failed task executions.
  """
  @spec on_apply_error_dispatch(
          source :: pid,
          td :: TaskDefinition.t(),
          config :: any,
          dependencies :: map(),
          errors :: Task.errors()
        ) :: any
  def on_apply_error_dispatch(source, td, config, dependencies, errors) do
    case td.dispatcher do
      callback when is_function(callback, 2) ->
        {latest_error, _} = List.first(errors)

        result =
          callback.(td.name, {
            :apply_error,
            Enum.count(errors),
            latest_error,
            {:apply, config, dependencies}
          })

        send(source, {:apply_retry_exit, td, result})

      :undefined ->
        send(source, {:apply_retry_exit, td, {:retry, false}})

      _other ->
        Logger.error("[task_tree.on_apply_error_dispatch] invalid on_apply_error, not retrying")
        send(source, {:apply_retry_exit, td, {:retry, false}})
    end
  end

  @spec start_impl(
          state :: RuntimeState.t(),
          task :: Task.t(),
          dependencies :: map
        ) :: Util.result(RuntimeState.t())
  defp start_impl(state = %RuntimeState{}, task, dependencies) do
    td = task.definition
    pid = spawn_link(__MODULE__, :apply_dispatch, [self(), td, state.config, dependencies])
    RuntimeState.mark_as_applying(state, td.name, pid)
  end

  @spec start(state :: RuntimeState.t(), task_name :: atom) :: Util.result(RuntimeState.t())
  def start(state = %RuntimeState{}, task_name) do
    with {:ok, dependencies} <- RuntimeState.resolve_dependencies_of(state, task_name),
         {:ok, task} <- RuntimeState.find_task(state, task_name) do
      start_impl(state, task, dependencies)
    end
  end

  @doc """
  Here we'll determine if the process should be reattempted or we need to start a rollback.
  """
  @spec reattmpt_failed(state :: RuntimeState.t(), task_def :: TaskDefinition.t(), error :: any) ::
          Util.result(any)
  def reattmpt_failed(state, task_def, error) do
    task_name = task_def.name

    with {:ok, pid} <- RuntimeState.find_task_pid(state, task_name),
         {:ok, state} <- RuntimeState.record_failure(state, task_name, error),
         {:ok, dependencies} <- RuntimeState.resolve_dependencies_of(state, task_name),
         {:ok, task} <- RuntimeState.find_task(state, task_name) do
      errors = Task.get_errors(task)
      args = [self(), task.definition, state.config, dependencies, errors]
      retrying_pid = spawn_link(__MODULE__, :on_apply_error_dispatch, args)

      # TODO move into record_apply_retry ?
      progress = Progress.record_apply_failure(state.progress, task_name, pid, retrying_pid)
      state = %{state | progress: progress}
      RuntimeState.record_apply_retry(state, task_name, retrying_pid)
    end
  end
end
