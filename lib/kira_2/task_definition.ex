defmodule Kira2.TaskDefinition do
  require Kira.Util, as: Util

  defmodule Defaults do
    @moduledoc false
    def default_dispatch({name, _}) do
      {:error, {:not_implemented, name, :default_dispatch, 2}}
    end
  end

  defstruct [
    :name,
    dependencies: [],
    dispatcher: &Defaults.default_dispatch/1
  ]

  @type dependencies(k) :: %{required(k) => any}
  @type apply_result(t) :: Util.result(t) | :not_implemented
  @type unapply_result() :: Util.result(:done) | :not_implemented
  @type error_result :: {:retry, boolean} | {:retry_after, integer} | :not_implemented

  @type apply_payload(k, c) :: {:apply, c, dependencies(k)}
  @type apply_error_payload(k, c) :: {:apply_error, integer, apply_payload(k, c)}

  @type unapply_payload(k, c, r) :: {:unapply, c, dependencies(k), r}
  @type unapply_error_payload(k, c, r) :: {:unapply_error, integer, unapply_payload(k, c, r)}

  @type dispatcher(k, c, r) ::
      (apply_payload(k, c) -> apply_result(r))
    | (unapply_payload(k, c, r) -> unapply_result())
    | (apply_error_payload(k, c) -> error_result())
    | (unapply_error_payload(k, c, r) -> error_result())

  @type t(k, c, r) :: %Kira2.TaskDefinition{
    name: atom,
    dependencies: dependencies(k),
    dispatcher: dispatcher(k, c, r)
  }

  @type t() :: t(atom, any, any)
end
