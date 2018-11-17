defmodule Kira.Util do
  @type result(a) :: {:ok, a} | {:error, any}

  # @spec result_reduce(
  #   lists :: [any],
  #   value :: any,
  #   f :: (any, any -> result(any))
  # ) :: result(any)
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
