defmodule Kira2.Runtime do
  # require Kira.Util, as: Util
  # require Kira2.TaskDefinition, as: TaskDef
  # require Kira2.TaskState, as: TaskState

  # @spec run_tasks(config :: any, tasks :: list(TaskDef.t())) :: Util.result(map)
  # @spec run_tasks(config :: any, tasks :: list(TaskDef.t()), timeout :: timeout) ::
  #         Util.result(map)
  # def run_tasks(config, tasks, timeout \\ :infinity) do
  #   parent = self()

  #   # run in own process to ensure it's self contained, and we don't
  #   # leak stuff like process flags or don't get any rogue messages
  #   # from stuff that would have be intended for the parent.
  #   own_pid =
  #     spawn(fn ->
  #       Process.flag(:trap_exit, true)

  #       with {:ok, state} <- TaskState.create(config, tasks, timeout) do
  #         ready_tasks = TaskState.find_apply_ready(state)

  #         if !Enum.empty?(ready_tasks) do
  #           reducer = &Apply.start(&2, &1)

  #           with {:ok, state} <- Util.result_reduce(ready_tasks, state, reducer) do
  #             send(parent, {:exit_value, apply_loop(state)})
  #           end
  #         else
  #           send(parent, {:exit_value, {:error, :unstartable}})
  #         end
  #       end
  #     end)

  #   own_ref = Process.monitor(own_pid)

  #   receive do
  #     {:exit_value, value} ->
  #       # ensure the monitor process down message isn't leaked
  #       # into the processes message box and is cleaned up.
  #       receive do
  #         {:DOWN, ^own_ref, :process, ^own_pid, _} -> value
  #       end

  #     # Sometimes shit happens, but if this happens there's a good
  #     # chance it's a bug in this module, file, etc.
  #     {:DOWN, ^own_ref, :process, ^own_pid, reason} ->
  #       # sorry this should never happen <emoji>grimmace</emoji>
  #       {:error, {:unacceptable_internal_failure, reason}}
  #   end
  # end
end
