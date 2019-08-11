defmodule Kira2.TaskTest do
  use ExUnit.Case

  alias Kira2, as: T

  describe "create" do
    test "simple" do
      td = %T.TaskDefinition{name: :b}
      result = T.Task.create(td, %{:b => td})

      expect = %T.Task{
        state: :not_started,
        definition: td,
        awaiting: MapSet.new(),
        blocking: MapSet.new(),
        awaiting_unapply: MapSet.new(),
        blocking_unapply: MapSet.new()
      }

      assert result == expect
    end

    test "correctly calculates blocking" do
      branch_map = %{
        :a => %T.TaskDefinition{name: :a},
        :b => %T.TaskDefinition{name: :b, dependencies: [:a]},
        :c => %T.TaskDefinition{name: :c, dependencies: [:a]}
      }

      result = T.Task.create(branch_map[:a], branch_map).blocking
      expect = MapSet.new([:b, :c])
      assert result == expect
    end

    test "correctly calculates awaiting" do
      branch_map = %{
        :a => %T.TaskDefinition{name: :a, dependencies: [:b, :c]},
        :b => %T.TaskDefinition{name: :a, dependencies: []},
        :c => %T.TaskDefinition{name: :a, dependencies: []}
      }

      result = T.Task.create(branch_map[:a], branch_map).awaiting
      expect = MapSet.new([:b, :c])
      assert result == expect
    end
  end

  describe "apply_ready?" do
    test "is not awaiting tasks" do
      branch_state = %T.Task{awaiting: MapSet.new()}
      assert T.Task.apply_ready?(branch_state)
    end

    test "is awaiting tasks" do
      branch_state = %T.Task{awaiting: MapSet.new([1])}
      assert !T.Task.apply_ready?(branch_state)
    end
  end
end
