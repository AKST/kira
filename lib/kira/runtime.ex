defmodule Kira.Runtime do
  require Kira.Branch, as: Branch
  require Kira.Runtime.Unapply, as: Unapply
  require Kira.Runtime.Apply, as: Apply
  require Kira.RuntimeState, as: RuntimeState
  require Kira.Util, as: Util

  @moduledoc false

  @spec unapply_loop(state :: RuntimeState.t()) :: {:error, any}
  def unapply_loop(state) do
    next_step =
      receive do
        {:unapply_exit, branch, {:ok, _}} ->
          with {:ok, state} <- RuntimeState.mark_as_unapplied(state, branch.name) do
            state.branch_states[branch.name].blocking_unapply
            |> Enum.filter(fn t -> Enum.empty?(state.branch_states[t].awaiting_unapply) end)
            |> Util.result_reduce(state, &Unapply.start(&2, &1))
          end

        {:unapply_exit, _, {:error, _}} ->
          {:error, {:not_implemented, {:unapply_loop, :error_unapply_exit}}}

        {:unapply_exit, b, _} ->
          {:error, {:bad_unapply_exit_value, b}}

        {:apply_exit, branch, {:ok, value}} ->
          with {:ok, state} <- RuntimeState.mark_as_applied(state, branch.name, value) do
            Unapply.start(state, branch.name)
          end

        {:apply_exit, _, {:error, _}} ->
          {:error, {:not_implemented, {:unapply_loop, :bad_apply_exit}}}

        {:apply_retry_exit, _branch, _msg} ->
          {:error, {:not_implemented, {:unapply_loop, :apply_retry_exit}}}

        # We're safe to ignore this, because this is the result of a
        # linked child process exiting normally. The reason we receive
        # this is because when we spawn a child with `spawn_link` for
        # the sake of tracking a bad exit.
        {:EXIT, _, :normal} ->
          {:ok, state}

        {:EXIT, _, reason} ->
          {:error, {:not_implemented, {:unapply_loop, :process_down, reason}}}
      after
        state.timeout ->
          {:error, {:not_implemented, {:timeout, state}}}
      end

    with {:ok, state} <- next_step do
      # TODO(return the first error that caused the rollback)
      if RuntimeState.unapply_done?(state),
        do: {:error, RuntimeState.get_errors(state)},
        else: unapply_loop(state)
    end
  end

  @spec apply_loop(state :: RuntimeState.t()) :: Util.result(RuntimeState.t())
  def apply_loop(state = %RuntimeState{}) do
    next_step =
      receive do
        {:apply_exit, branch, {:ok, value}} ->
          with {:ok, state} <- RuntimeState.mark_as_applied(state, branch.name, value) do
            state.branch_states[branch.name].blocking
            |> Enum.filter(fn t -> Enum.empty?(state.branch_states[t].awaiting) end)
            |> Util.result_reduce(state, &Apply.start(&2, &1))
          end

        {:apply_exit, branch, {:error, error}} ->
          Apply.reattmpt_failed(state, branch, error)

        # TODO start unapply here
        # - will need to mark that the process has failed, Apply.reattmpt_failed
        # - start unapply with the value, {:start_unapply, state}
        {:apply_exit, _branch, _} ->
          {:error, :bad_return_value}

        # when we want to retry a previously failed task
        {:apply_retry_exit, branch, {:retry, true}} ->
          Apply.start(state, branch.name)

        # when a task we checked if we should retry decides it cannot be retried we do this.
        {:apply_retry_exit, branch, {:retry, false}} ->
          with {:ok, state} <- RuntimeState.unblock_dependend_unapplys(state, branch.name) do
            {:start_unapply, state}
          end

        # TODO start unapply here
        # - will need to mark that the process has failed, Apply.reattmpt_failed
        # - start unapply with the value, {:start_unapply, state}
        {:apply_retry_exit, _branch, msg} ->
          {:error, {:not_implemented, {:apply_retry_exit, :EXIT, msg}}}

        {:EXIT, _, :normal} ->
          {:ok, state}

        {:EXIT, pid, exception} ->
          with {:ok, branch_s} <- RuntimeState.get_branch_from_pid(state, pid) do
            Apply.reattmpt_failed(state, branch_s.branch, {:expection, exception})
          end
      after
        state.timeout ->
          {:error, {:not_implemented, {:timeout, state}}}
      end

    case next_step do
      {:ok, state} ->
        if RuntimeState.apply_done?(state),
          do: {:ok, RuntimeState.get_done(state)},
          else: apply_loop(state)

      {:start_unapply, state} ->
        start_unapply_loop(state)

      {:error, _} = e ->
        e
    end
  end

  @spec start_unapply_loop(state :: RuntimeState.t()) :: {:error, any}
  def start_unapply_loop(state) do
    ready_tasks = RuntimeState.find_unapply_ready(state)
    reducer = &Unapply.start(&2, &1)

    with {:ok, state} <- Util.result_reduce(ready_tasks, state, reducer) do
      unapply_loop(state)
    end
  end

  @spec run_tasks(config :: any, tasks :: list(Branch.t())) :: Util.result(map)
  @spec run_tasks(config :: any, tasks :: list(Branch.t()), timeout :: timeout) ::
          Util.result(map)
  def run_tasks(config, tasks, timeout \\ :infinity) do
    parent = self()

    # run in own process to ensure it's self contained, and we don't
    # leak stuff like process flags or don't get any rogue messages
    # from stuff that would have be intended for the parent.
    own_pid =
      spawn(fn ->
        Process.flag(:trap_exit, true)

        with {:ok, state} <- RuntimeState.create(config, tasks, timeout) do
          ready_tasks = RuntimeState.find_apply_ready(state)

          if !Enum.empty?(ready_tasks) do
            reducer = &Apply.start(&2, &1)

            with {:ok, state} <- Util.result_reduce(ready_tasks, state, reducer) do
              send(parent, {:exit_value, apply_loop(state)})
            end
          else
            send(parent, {:exit_value, {:error, :unstartable}})
          end
        end
      end)

    own_ref = Process.monitor(own_pid)

    receive do
      {:exit_value, value} ->
        # ensure the monitor process down message isn't leaked
        # into the processes message box and is cleaned up.
        receive do
          {:DOWN, ^own_ref, :process, ^own_pid, _} -> value
        end

      # Sometimes shit happens, but if this happens there's a good
      # chance it's a bug in this module, file, etc.
      {:DOWN, ^own_ref, :process, ^own_pid, reason} ->
        # sorry this should never happen <emoji>grimmace</emoji>
        {:error, {:unacceptable_internal_failure, reason}}
    end
  end
end
