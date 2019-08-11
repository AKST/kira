defmodule Kira2.Runtime.Unapply do
  require Kira2.TaskDefinition, as: TaskDefinition
  require Kira2.Task, as: Task
  require Kira2.RuntimeState, as: RuntimeState
  require Kira2.Progress, as: Progress
  require Kira.Util, as: Util

  @moduledoc false

  @spec unapply_dispatch(
          source :: pid,
          td :: TaskDefinition.t(),
          config :: any,
          dependencies :: map(),
          value :: any
        ) :: any
  def unapply_dispatch(source, td, config, dependencies, value) do
    case td.dispatcher do
      dispatcher when is_function(dispatcher, 2) ->
        # TODO check the return isn't malformed
        result = dispatcher.(td.name, {:unapply, config, dependencies, value})
        send(source, {:unapply_exit, td, result})

      :undefined ->
        send(source, {:unapply_exit, td, {:ok, :undefined}})

      _other ->
        # TODO think of a better way to handel this
        send(source, {:unapply_exit, td, :exit})
    end
  end

  @spec start(state :: RuntimeState.t(), task_name :: atom) :: Util.result(RuntimeState.t())
  def start(state = %RuntimeState{}, task_name) do
    with {:ok, dependencies} <- RuntimeState.resolve_dependencies_of(state, task_name),
         {:ok, task} <- RuntimeState.find_task(state, task_name),
         {:ok, task_value} <- Task.get_completed(task) do
      td = task.definition

      pid =
        spawn_link(__MODULE__, :unapply_dispatch, [
          self(),
          td,
          state.config,
          dependencies,
          task_value
        ])

      task_state = {:running_unapply, pid, task_value, Task.get_errors(task)}

      with {:ok, state} <- RuntimeState.set_task_state(state, td.name, task_state) do
        # TODO should I be doing this?
        progress = Progress.record_unapply_start(state.progress, task_name, pid)
        {:ok, %{state | progress: progress}}
      end
    end
  end
end
