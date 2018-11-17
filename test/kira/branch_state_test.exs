defmodule Kira.BranchStateTest do
  use ExUnit.Case

  alias Kira, as: T

  describe "create" do
    test "simple" do
      b = %T.Branch{name: :b}
      result = T.BranchState.create(b, %{:b => b})

      expect = %T.BranchState{
        branch: b,
        awaiting: MapSet.new(),
        blocking: MapSet.new(),
        awaiting_unapply: MapSet.new(),
        blocking_unapply: MapSet.new(),
        task: :not_started
      }

      assert result == expect
    end

    test "correctly calculates blocking" do
      branch_map = %{
        :a => %T.Branch{name: :a},
        :b => %T.Branch{name: :b, dependencies: [:a]},
        :c => %T.Branch{name: :c, dependencies: [:a]}
      }

      result = T.BranchState.create(branch_map[:a], branch_map).blocking
      expect = MapSet.new([:b, :c])
      assert result == expect
    end

    test "correctly calculates awaiting" do
      branch_map = %{
        :a => %T.Branch{name: :a, dependencies: [:b, :c]},
        :b => %T.Branch{name: :a, dependencies: []},
        :c => %T.Branch{name: :a, dependencies: []}
      }

      result = T.BranchState.create(branch_map[:a], branch_map).awaiting
      expect = MapSet.new([:b, :c])
      assert result == expect
    end
  end

  describe "apply_ready?" do
    test "is not awaiting tasks" do
      branch_state = %T.BranchState{awaiting: MapSet.new()}
      assert T.BranchState.apply_ready?(branch_state)
    end

    test "is awaiting tasks" do
      branch_state = %T.BranchState{awaiting: MapSet.new([1])}
      assert !T.BranchState.apply_ready?(branch_state)
    end
  end
end
