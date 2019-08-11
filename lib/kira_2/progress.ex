defmodule Kira2.Progress do
  require Kira.Util, as: Util

  @moduledoc false
  # Originally progress was tracked in RuntimeState as a integer that started
  # out as the number of branches, and would indecate completion once equal
  # to 0. But that really didn't accomodate for rollbacks, so this data
  # type was created to track progression with a richer data type.

  # TODO check if we need the sets of what is doing what
  defstruct [
    :applied,
    :unapplied,
    :total,
    :applies_running,
    :applies_retrying,
    :unapplies_running,
    :unapplies_retrying,
    :pid_cache
  ]

  @type pid_cache :: %{required(pid) => atom}

  @type t :: %__MODULE__{
          total: integer,
          applied: integer,
          unapplied: integer,
          applies_running: MapSet.t(atom),
          applies_retrying: MapSet.t(atom),
          unapplies_running: MapSet.t(atom),
          unapplies_retrying: MapSet.t(atom),
          pid_cache: pid_cache
        }

  @spec create(total :: integer) :: t
  def create(total) do
    %__MODULE__{
      total: total,
      applied: 0,
      unapplied: 0,
      pid_cache: %{},
      applies_running: MapSet.new(),
      applies_retrying: MapSet.new(),
      unapplies_running: MapSet.new(),
      unapplies_retrying: MapSet.new()
    }
  end

  @spec record_apply_start(progress :: t, name :: atom, pid :: pid) :: t
  def record_apply_start(progress, name, pid) do
    %{
      progress
      | applies_running: MapSet.put(progress.applies_running, name),
        pid_cache: Map.put(progress.pid_cache, pid, name)
    }
  end

  @spec record_apply_failure(progress :: t, name :: atom) :: t
  def record_apply_failure(progress, name) do
    %{
      progress
      | applies_running: MapSet.delete(progress.applies_running, name),
        pid_cache: Map.drop(progress.pid_cache, [pid])
    }
  end

  @spec record_apply_done(progress :: t, name :: atom, pid :: pid) :: t
  def record_apply_done(progress, name, pid) do
    %{
      progress
      | applied: progress.applied + 1,
        applies_running: MapSet.delete(progress.applies_running, name),
        pid_cache: Map.drop(progress.pid_cache, [pid])
    }
  end

  @spec record_unapply_start(progress :: t, name :: atom, pid :: pid) :: t
  def record_unapply_start(progress, name, pid) do
    %{
      progress
      | unapplies_running: MapSet.put(progress.unapplies_running, name),
        pid_cache: Map.put(progress.pid_cache, pid, name)
    }
  end

  @spec record_unapply_done(progress :: t, name :: atom, pid :: pid) :: t
  def record_unapply_done(progress, name, pid) do
    %{
      progress
      | unapplied: progress.unapplied + 1,
        unapplies_running: MapSet.delete(progress.unapplies_running, name),
        pid_cache: Map.drop(progress.pid_cache, [pid])
    }
  end

  @spec apply_done?(progress :: t) :: boolean
  def apply_done?(progress) do
    progress.applied == progress.total && MapSet.size(progress.applies_running) == 0 &&
      MapSet.size(progress.applies_retrying) == 0
  end

  @spec unapply_done?(progress :: t) :: boolean
  def unapply_done?(progress) do
    progress.unapplied == progress.applied && MapSet.size(progress.applies_running) == 0 &&
      MapSet.size(progress.applies_retrying) == 0 && MapSet.size(progress.unapplies_running) == 0 &&
      MapSet.size(progress.unapplies_retrying) == 0
  end

  @spec find_task_by_pid(progress :: t(), pid :: pid) :: Util.result(atom)
  def find_task_by_pid(progress, pid) do
    fetch_error = progress_error({:failed_to_find_task_from, pid})
    Util.fetch(progress.pid_cache, pid, fetch_error)
  end

  def progress_error(error), do: {:error, {:progress_error, error}}
end
