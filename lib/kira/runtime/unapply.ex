defmodule Kira.Runtime.Unapply do
  require Kira.Branch, as: Branch
  require Kira.BranchState, as: BranchState
  require Kira.Progress, as: Progress
  require Kira.RuntimeState, as: RuntimeState
  require Kira.Util, as: Util

  @spec unapply_dispatch(
          source :: pid,
          branch :: Branch.t(),
          config :: any,
          dependencies :: map(),
          value :: any
        ) :: any
  def unapply_dispatch(source, branch, config, dependencies, value) do
    case branch.unapply do
      unapply when is_function(unapply, 3) ->
        result = unapply.(config, dependencies, value)
        send(source, {:unapply_exit, branch, result})

      :undefined ->
        send(source, {:unapply_exit, branch, {:ok, :undefined}})
    end
  end

  @spec start_impl(
          state :: RuntimeState.t(),
          branch_state :: BranchState.t(),
          dependencies :: map,
          value :: any
        ) :: Util.result(RuntimeState.t())
  defp start_impl(state = %RuntimeState{}, branch_state, dependencies, value) do
    branch = branch_state.branch

    pid =
      spawn_link(__MODULE__, :unapply_dispatch, [
        self(),
        branch,
        state.config,
        dependencies,
        value
      ])

    task_s = {:running_unapply, pid, value, BranchState.get_errors(branch_state)}

    with {:ok, state} <- RuntimeState.set_branch_task(state, branch.name, task_s) do
      {:ok, %{state | running: Map.put(state.running, pid, branch.name)}}
    end
  end

  @spec start(state :: RuntimeState.t(), branch_name :: atom) :: Util.result(RuntimeState.t())
  def start(state = %RuntimeState{}, branch_name) do
    with {:ok, dependencies} <- RuntimeState.resolve_dependencies_of(state, branch_name),
         {:ok, branch_state} <- RuntimeState.get_branch(state, branch_name),
         {:ok, branch_value} <- BranchState.get_completed(branch_state) do
      start_impl(state, branch_state, dependencies, branch_value)
    end
  end
end
