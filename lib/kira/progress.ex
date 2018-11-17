defmodule Kira.Progress do
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
    :unapplies_retrying
  ]

  @type t :: %__MODULE__{
          total: integer,
          applied: integer,
          unapplied: integer,
          applies_running: MapSet.t(atom),
          applies_retrying: MapSet.t(atom),
          unapplies_running: MapSet.t(atom),
          unapplies_retrying: MapSet.t(atom)
        }

  @spec create(total :: integer) :: t
  def create(total) do
    %__MODULE__{
      total: total,
      applied: 0,
      unapplied: 0,
      applies_running: MapSet.new(),
      applies_retrying: MapSet.new(),
      unapplies_running: MapSet.new(),
      unapplies_retrying: MapSet.new()
    }
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
    %{
      progress
      | applied: progress.applied + 1,
        applies_running: MapSet.delete(progress.applies_running, name)
    }
  end

  @spec record_unapply_start(progress :: t, name :: atom) :: t
  def record_unapply_start(progress, name) do
    %{progress | unapplies_running: MapSet.put(progress.unapplies_running, name)}
  end

  @spec record_unapply_done(progress :: t, name :: atom) :: t
  def record_unapply_done(progress, name) do
    %{
      progress
      | unapplied: progress.unapplied + 1,
        unapplies_running: MapSet.delete(progress.unapplies_running, name)
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
end
