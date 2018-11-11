# Kira

> 🚨 Currently beta grade software, I wouldn't recommend usage in production systems

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
    {:kira, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/kira](https://hexdocs.pm/kira).

