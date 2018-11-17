defmodule Kira.Branch do
  require Kira.Util, as: Util

  defmodule Defaults do
    @moduledoc false
    def apply_default(_, _), do: {:error, :unimplemented}
  end

  defstruct [
    :name,
    dependencies: [],
    apply: &Defaults.apply_default/2,
    unapply: :undefined,
    on_apply_error: :undefined,
    on_unapply_error: :undefined
  ]

  @type retry :: {:retry, boolean} | {:retry_after, integer}

  @type dependencies :: %{required(atom) => any}
  @type apply_t :: (any, dependencies -> Util.result(any))
  @type unapply_t :: :undefined | (any, dependencies, any -> Util.result(any))
  @type on_apply_error_t :: :undefined | (any, dependencies, any, integer -> retry)
  @type on_unapply_error_t :: :undefined | (any, dependencies, any, integer -> retry)

  @type t() :: %Kira.Branch{
          name: atom,
          dependencies: [atom],
          apply: apply_t,
          unapply: unapply_t,
          on_apply_error: on_apply_error_t,
          on_unapply_error: on_unapply_error_t
        }
end
