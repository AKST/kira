# About

> 🚨 Currently beta grade software, I wouldn't recommend usage in production systems, or your personal projects for the matter 😂

A concurrent task scheduler, intended to maximize through put of
predefined tasks with clear dependencies on each other to run in
parallel where possible. Whilst providing a means to rollback
with as little pain as possible.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `kira` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:kira, "~> 0.1.1"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/kira](https://hexdocs.pm/kira).

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

