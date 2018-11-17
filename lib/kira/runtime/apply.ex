defmodule Kira.Runtime.Apply do
  require Kira.Branch, as: Branch
  require Kira.BranchState, as: BranchState
  require Kira.Progress, as: Progress
  require Kira.RuntimeState, as: RuntimeState
  require Kira.Util, as: Util

  require Logger

  @spec apply_dispatch(
          source :: pid,
          branch :: Branch.t(),
          config :: any,
          dependencies :: map()
        ) :: any
  def apply_dispatch(source, branch, config, dependencies) do
    result = branch.apply.(config, dependencies)
    send(source, {:apply_exit, branch, result})
  end

  @spec on_apply_error_dispatch(
          source :: pid,
          branch :: Branch.t(),
          config :: any,
          dependencies :: map(),
          errors :: BranchState.errors()
        ) :: any
  def on_apply_error_dispatch(source, branch, config, dependencies, errors) do
    case branch.on_apply_error do
      on_apply_error when is_function(on_apply_error, 4) ->
        {latest_error, _} = List.first(errors)
        error_count = Enum.count(errors)
        result = on_apply_error.(config, dependencies, latest_error, error_count)
        send(source, {:apply_retry_exit, branch, result})

      :undefined ->
        send(source, {:apply_retry_exit, branch, {:retry, false}})

      other ->
        Logger.error("[task_tree.on_apply_error_dispatch] invalid on_apply_error, not retrying")

        send(source, {:apply_retry_exit, branch, {:retry, false}})
    end
  end

  @spec start_impl(
          state :: RuntimeState.t(),
          branch_state :: BranchState.t(),
          dependencies :: map
        ) :: Util.result(RuntimeState.t())
  defp start_impl(state = %RuntimeState{}, branch_state, dependencies) do
    branch = branch_state.branch

    pid = spawn_link(__MODULE__, :apply_dispatch, [self(), branch, state.config, dependencies])

    RuntimeState.mark_as_applying(state, branch.name, pid)
  end

  @spec start(state :: RuntimeState.t(), branch_name :: atom) :: Util.result(RuntimeState.t())
  def start(state = %RuntimeState{}, branch_name) do
    with {:ok, dependencies} <- RuntimeState.resolve_dependencies_of(state, branch_name),
         {:ok, branch_state} <- RuntimeState.get_branch(state, branch_name) do
      start_impl(state, branch_state, dependencies)
    end
  end

  @doc """
  Here we'll determine if the process should be reattempted or we need to start a rollback.
  """
  @spec reattmpt_failed(state :: RuntimeState.t(), branch :: Branch.t(), error :: any) ::
          Util.result(any)
  def reattmpt_failed(state, branch, error) do
    with {:ok, pid} <- RuntimeState.get_branch_pid(state, branch.name),
         {:ok, state} <- RuntimeState.record_failure(state, branch.name, error),
         {:ok, dependencies} <- RuntimeState.resolve_dependencies_of(state, branch.name),
         {:ok, branch_s} <- RuntimeState.get_branch(state, branch.name) do
      errors = BranchState.get_errors(branch_s)
      args = [self(), branch_s.branch, state.config, dependencies, errors]
      retrying_pid = spawn_link(__MODULE__, :on_apply_error_dispatch, args)

      state = %{
        state
        | running:
            state.running
            |> Map.drop([pid])
            |> Map.put(retrying_pid, branch.name),
          progress:
            state.progress
            |> Progress.record_apply_failure(branch.name)
      }

      RuntimeState.record_apply_retry(state, branch.name, retrying_pid)
    end
  end
end
