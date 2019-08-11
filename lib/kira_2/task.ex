defmodule Kira2.Task do
  require Kira2.TaskDefinition, as: TaskDefinition
  require Kira.Util, as: Util

  @moduledoc false

  defstruct [
    :state,
    :definition,
    :awaiting,
    :blocking,
    :awaiting_unapply,
    :blocking_unapply
  ]

  @type errors :: [{any, DateTime.t()}]
  @type state() ::
          :not_started
          | {:running_apply, pid(), errors}
          | {:running_apply_retry, pid(), errors}
          | {:running_unapply, pid(), any, errors}
          | {:running_unapply_retry, pid(), any, errors}
          | {:failed, errors}
          | {:done_applied, any}
          | :done_unapplied

  @type definitions :: %{required(atom()) => TaskDefinition.t()}

  @type t() :: %__MODULE__{
          state: state(),
          definition: TaskDefinition.t(),
          awaiting: MapSet.t(atom()),
          blocking: MapSet.t(atom()),
          awaiting_unapply: MapSet.t(atom()),
          blocking_unapply: MapSet.t(atom())
        }

  @spec create(task_def :: TaskDefinition.t(), definitions :: definitions()) :: t()
  def create(task_def, definitions) do
    # The tasks blocking this task.
    awaiting = Enum.into(task_def.dependencies, MapSet.new())

    # The tasks blocked by this task.
    blocking =
      for {t, other_def} <- definitions,
          Enum.member?(other_def.dependencies, task_def.name),
          into: MapSet.new(),
          do: t

    %__MODULE__{
      state: :not_started,
      definition: task_def,
      awaiting: awaiting,
      blocking: blocking,
      awaiting_unapply: MapSet.new(),
      blocking_unapply: awaiting
    }
  end

  @spec apply_ready?(task :: t()) :: boolean()
  def apply_ready?(task) do
    MapSet.size(task.awaiting) == 0
  end

  @spec unapply_ready?(task :: t()) :: boolean()
  def unapply_ready?(task) do
    MapSet.size(task.awaiting_unapply) == 0 && is_complete(task)
  end

  @spec set_state(task :: t(), state :: state) :: t()
  def set_state(task, state) do
    %{task | state: state}
  end

  @spec get_completed(task :: t()) :: Util.result(any)
  def get_completed(task) do
    case task.state do
      {:done_applied, value} ->
        {:ok, value}

      {:running_unapply, _, value} ->
        {:ok, value}

      _ ->
        {:error, {:unable_to_get_task_value, task.definition.name, task.state}}
    end
  end

  @spec is_complete(task :: t()) :: boolean
  def is_complete(task) do
    case task.state do
      {:done_applied, _value} -> true
      _ -> false
    end
  end

  @spec get_pid(task :: t()) :: Util.result(pid)
  def get_pid(task) do
    case task.state do
      {:running_apply, pid, _} ->
        {:ok, pid}

      {:running_apply_retry, pid, _} ->
        {:ok, pid}

      {:running_unapply, pid, _, _} ->
        {:ok, pid}

      {:running_unapply_retry, pid, _, _} ->
        {:ok, pid}

      _ ->
        {:error, {:unable_to_get_task_pid, task.definition.name, task.state}}
    end
  end

  @spec get_errors(task :: t) :: errors
  def get_errors(task) do
    case task.state do
      {:running_apply, _, errors} -> errors
      {:running_apply_retry, _, errors} -> errors
      {:running_unapply, _, _, errors} -> errors
      {:running_unapply_retry, _, _, errors} -> errors
      {:failed, errors} -> errors
      _ -> []
    end
  end

  def put_in_awaiting(task, other_task_name) do
    %{task | awaiting: MapSet.put(task.awaiting, other_task_name)}
  end

  def put_in_awaiting_unapply(task, other_task_name) do
    %{task | awaiting_unapply: MapSet.put(task.awaiting_unapply, other_task_name)}
  end

  def put_in_blocking(task, other_task_name) do
    %{task | blocking: MapSet.put(task.blocking, other_task_name)}
  end

  def put_in_blocking_unapply(task, other_task_name) do
    %{task | blocking_unapply: MapSet.put(task.blocking_unapply, other_task_name)}
  end

  def drop_from_awaiting(task, other_task_name) do
    %{task | awaiting: MapSet.delete(task.awaiting, other_task_name)}
  end

  def drop_from_awaiting_unapply(task, other_task_name) do
    %{task | awaiting_unapply: MapSet.delete(task.awaiting_unapply, other_task_name)}
  end

  def drop_from_blocking(task, other_task_name) do
    %{task | blocking: MapSet.delete(task.blocking, other_task_name)}
  end

  def drop_from_blocking_unapply(task, other_task_name) do
    %{task | blocking_unapply: MapSet.delete(task.blocking_unapply, other_task_name)}
  end
end

