defmodule Kira.Runtime.ApplyTest do
  use ExUnit.Case

  alias Kira, as: T
  alias Kira.Util.Testing, as: Util

  describe "start" do
    test "it works" do
      a = %T.Branch{name: :a, apply: fn _, _ -> {:ok, 12} end}
      {:ok, state} = T.RuntimeState.create(:undefined, [a], :infinity)
      assert state.branch_states.a.task == :not_started
      assert Enum.count(state.running) == 0

      {:ok, state} = T.Runtime.Apply.start(state, :a)
      assert elem(state.branch_states.a.task, 0) == :running_apply
      assert Enum.count(state.running) == 1
      assert_receive {:apply_exit, a, {:ok, 12}}
    end

    test "start non existent branch" do
      {:ok, state} = T.RuntimeState.create(:undefined, [], :infinity)
      result = T.Runtime.Apply.start(state, :a)
      assert Util.is_error(result)
    end
  end
end
