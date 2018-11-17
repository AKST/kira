defmodule Kira.Util.Testing do
  def is_error({:error, _}), do: true
  def is_error(_), do: false

  def put_in_struct(struct, location, value) do
    locator = Enum.map(location, &Access.key/1)
    put_in(struct, locator, value)
  end

  def get_failure_reasons({:error, reasons_with_times}) do
    Enum.map(reasons_with_times, fn {reason, _time} -> reason end)
  end
end
