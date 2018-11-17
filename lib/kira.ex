defmodule Kira do
  require Kira.Branch, as: Branch
  require Kira.Runtime, as: Runtime
  require Kira.Util, as: Util

  @moduledoc """
  The main public interface of this library, with the
  exception of the `Kira.Branch` module.
  """

  @doc """
  Used to execute the tasks you have defined.

  ## Arguments

  - **config**, can be treated as common information shared between tasks.
    This is where you may handle dependency injection or whatever.

  - **tasks**, basically the stuff you care about. Pass in an array of
    `Branch`'s and the library will organise the order in which they'll run.

  - **timeout**, basically a timeout in the case where no activity takes
    place inside the state machine used to coordinate tasks.

  ## Example

  ```ex
  require Kira
  require Logger

  task_a = %Kira.Branch{
    name: :get_a,
    apply: fn (config, deps) ->
      Logger.info(inspect(config))
      {:ok, 2}
    end,
  }

  task_b = %Kira.Branch{
    name: :get_b,
    apply: fn (config, deps) ->
      Logger.info(inspect(config))
      {:ok, 3}
    end,
  }

  task_c = %Kira.Branch{
    name: :get_c,
    dependencies: [:get_a, :get_b],
    apply: fn (config, deps) ->
      Logger.info(inspect(config))
      {:ok, deps[:get_a] + deps[:get_b]}
    end,
  }

  tasks = [task_a, task_b, task_c]
  {:ok, results} = Kira.run_tasks(:my_config, tasks)
  Logger.info(inspect(results))

  # Should log
  # 1: :my_config
  # 2: :my_config
  # 3: :my_config
  # 4: %{ :get_a => 2, :get_b => 3, :get_c => 5 }
  ```
  """
  @spec run_tasks(config :: any, tasks :: list(Branch.t())) :: Util.result(map)
  @spec run_tasks(config :: any, tasks :: list(Branch.t()), timeout :: timeout) ::
          Util.result(map)
  defdelegate run_tasks(config, tasks, timeout \\ :infinity), to: Runtime
end
