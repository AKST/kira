defmodule Kira.Util do
  require Enum

  @moduledoc false

  @type result(a) :: {:ok, a} | {:error, any}

  @spec result_reduce(lists :: Enum.t(), value :: b, f :: (any, b -> result(b))) :: result(b)
        when b: var
  def result_reduce(lists, value, f) do
    Enum.reduce(lists, {:ok, value}, fn
      next, {:ok, value} -> f.(next, value)
      _, e = {:error, _} -> e
    end)
  end

  def fetch(map, key, error) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, v} -> {:ok, v}
      :error -> {:error, error}
    end
  end
end
