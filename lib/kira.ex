defmodule Kira do
  require Kira.Branch, as: Branch
  require Kira.Runtime, as: Runtime
  require Kira.Util, as: Util

  @spec run_tasks(config :: any, tasks :: list(Branch.t())) :: Util.result(map)
  defdelegate run_tasks(config, tasks), to: Runtime

  @spec run_tasks(config :: any, tasks :: list(Branch.t()), timeout :: timeout) :: Util.result(map)
  defdelegate run_tasks(config, tasks, timeout), to: Runtime
end
