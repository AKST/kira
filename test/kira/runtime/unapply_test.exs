defmodule Kira.Runtime.UnapplyTest do
  use ExUnit.Case

  alias Kira, as: T
  alias Kira.Util.Testing, as: Util

  describe "start" do
    test "it works" do
      a = %T.Branch{name: :a, unapply: fn _, _, v -> {:ok, v} end}

      state =
        T.RuntimeState.create(:undefined, [a], :infinity)
        |> elem(1)
        |> Util.put_in_struct([:branch_states, :a, :task], {:done_applied, 12})

      assert Enum.count(state.running) == 0

      {:ok, state} = T.Runtime.Unapply.start(state, :a)
      assert elem(state.branch_states.a.task, 0) == :running_unapply
      assert Enum.count(state.running) == 1
      assert_receive {:unapply_exit, a, {:ok, 12}}
    end

    test "start non existent branch" do
      {:ok, state} = T.RuntimeState.create(:undefined, [], :infinity)
      result = T.Runtime.Unapply.start(state, :a)
      assert Util.is_error(result)
    end
  end
end
