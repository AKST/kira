defmodule Kira.BranchState do
  require Kira.Branch, as: Branch
  require Kira.Util, as: Util

  @moduledoc false

  defstruct [
    :branch,
    :awaiting,
    :blocking,
    :awaiting_unapply,
    :blocking_unapply,
    :task
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

  @type t() :: %Kira.BranchState{
          branch: Branch.t(),
          awaiting: MapSet.t(atom()),
          blocking: MapSet.t(atom()),
          awaiting_unapply: MapSet.t(atom()),
          blocking_unapply: MapSet.t(atom()),
          task: task()
        }

  @spec create(branch :: Branch.t(), branches :: branches()) :: t()
  def create(branch, branches) do
    awaiting = Enum.into(branch.dependencies, MapSet.new())

    blocking =
      for {t, desc} <- branches,
          Enum.member?(desc.dependencies, branch.name),
          into: MapSet.new(),
          do: t

    %__MODULE__{
      branch: branch,
      awaiting: awaiting,
      blocking: blocking,
      awaiting_unapply: MapSet.new(),
      blocking_unapply: awaiting,
      task: :not_started
    }
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
