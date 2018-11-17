defmodule Kira.RuntimeStateTest do
  use ExUnit.Case

  alias Kira.Util.Testing, as: Util
  alias Kira, as: T

  describe "find_apply_ready" do
    test "works correctly" do
      branch_states = %{
        :a => %T.BranchState{awaiting: MapSet.new([:b, :c])},
        :b => %T.BranchState{awaiting: MapSet.new([:c])},
        :c => %T.BranchState{awaiting: MapSet.new([])},
        :d => %T.BranchState{awaiting: MapSet.new([])}
      }

      state = %T.RuntimeState{branch_states: branch_states}
      result = T.RuntimeState.find_apply_ready(state)
      expect = MapSet.new([:c, :d])
      assert result == expect
    end
  end

  describe "create" do
    test "it works" do
      branch_a = %T.Branch{name: :a, dependencies: []}
      branch_b = %T.Branch{name: :b, dependencies: [:a]}

      result = T.RuntimeState.create(0, [branch_a, branch_b], :infinity)

      expect =
        {:ok,
         %T.RuntimeState{
           config: 0,
           timeout: :infinity,
           running: %{},
           progress: T.Progress.create(2),
           branch_states: %{
             :a => %T.BranchState{
               branch: branch_a,
               awaiting: MapSet.new(),
               blocking: MapSet.new([:b]),
               awaiting_unapply: MapSet.new(),
               blocking_unapply: MapSet.new(),
               task: :not_started
             },
             :b => %T.BranchState{
               branch: branch_b,
               awaiting: MapSet.new([:a]),
               blocking: MapSet.new(),
               awaiting_unapply: MapSet.new(),
               blocking_unapply: MapSet.new([:a]),
               task: :not_started
             }
           }
         }}

      assert result == expect
    end

    test "fails with duplicate names" do
      branch_a_1 = %T.Branch{name: :a, dependencies: []}
      branch_a_2 = %T.Branch{name: :a, dependencies: []}
      result = T.RuntimeState.create(0, [branch_a_1, branch_a_2], :infinity)
      assert Util.is_error(result)
    end
  end

  describe "resolve_dependencies_of" do
    test "a branch with no dependencies, works" do
      a = %T.Branch{name: :a}
      {:ok, state} = T.RuntimeState.create(:undefined, [a], :infinity)
      result = T.RuntimeState.resolve_dependencies_of(state, :a)
      assert result == {:ok, %{}}
    end

    test "a branch with dependencies, works" do
      a = %T.Branch{name: :a}
      b = %T.Branch{name: :b, dependencies: [:a]}
      c = %T.Branch{name: :c, dependencies: [:b]}
      d = %T.Branch{name: :d, dependencies: [:b, :c]}

      state =
        T.RuntimeState.create(:undefined, [a, b, c, d], :infinity)
        |> elem(1)
        |> Util.put_in_struct([:branch_states, :a, :task], {:done_applied, 1})
        |> Util.put_in_struct([:branch_states, :b, :task], {:done_applied, 2})
        |> Util.put_in_struct([:branch_states, :c, :task], {:done_applied, 3})

      result = T.RuntimeState.resolve_dependencies_of(state, :d)
      assert result == {:ok, %{:b => 2, :c => 3}}
    end

    test "with invalid branch_name" do
      {:ok, state} = T.RuntimeState.create(:undefined, [], :infinity)
      assert Util.is_error(T.RuntimeState.resolve_dependencies_of(state, :a))
    end
  end

  describe "mark_as_applied" do
    test "updates those dependencies" do
      a = %T.Branch{name: :a}
      b = %T.Branch{name: :b, dependencies: [:a]}

      state =
        T.RuntimeState.create(:undefined, [a, b], :infinity)
        |> elem(1)
        |> Util.put_in_struct([:branch_states, :a, :task], {:running_apply, :undefined, []})

      assert state.branch_states[:b].awaiting === MapSet.new([:a])

      {:ok, state} = T.RuntimeState.mark_as_applied(state, :a, :undefined)
      assert state.branch_states[:b].awaiting === MapSet.new([])
    end
  end

  describe "mark_as_applying" do
    test "updates unapply awaiting dependencies" do
      a = %T.Branch{name: :a}
      b = %T.Branch{name: :b, dependencies: [:a]}
      c = %T.Branch{name: :c, dependencies: [:b]}

      state =
        T.RuntimeState.create(:undefined, [a, b, c], :infinity)
        |> elem(1)
        |> Util.put_in_struct([:branch_states, :a, :task], {:done_applied, :undefined})

      assert state.branch_states[:a].awaiting_unapply === MapSet.new([])
      assert state.branch_states[:c].awaiting === MapSet.new([:b])

      {:ok, state} = T.RuntimeState.mark_as_applying(state, :b, self())
      assert state.branch_states[:a].awaiting_unapply === MapSet.new([:b])
      assert state.branch_states[:c].awaiting === MapSet.new([:b])
    end
  end

  describe "get_done" do
    test "when all done" do
      a = %T.Branch{name: :a}
      b = %T.Branch{name: :b}
      c = %T.Branch{name: :c}

      state =
        T.RuntimeState.create(:undefined, [a, b, c], :infinity)
        |> elem(1)
        |> Util.put_in_struct([:branch_states, :a, :task], {:done_applied, 1})
        |> Util.put_in_struct([:branch_states, :b, :task], {:done_applied, 2})
        |> Util.put_in_struct([:branch_states, :c, :task], {:done_applied, 3})

      expected = %{a: 1, b: 2, c: 3}
      result = T.RuntimeState.get_done(state)
      assert result === expected
    end

    test "when some done" do
      a = %T.Branch{name: :a}
      b = %T.Branch{name: :b}
      c = %T.Branch{name: :c}

      state =
        T.RuntimeState.create(:undefined, [a, b, c], :infinity)
        |> elem(1)
        |> Util.put_in_struct([:branch_states, :a, :task], {:done_applied, 1})
        |> Util.put_in_struct([:branch_states, :c, :task], {:done_applied, 3})

      expected = %{a: 1, c: 3}
      result = T.RuntimeState.get_done(state)
      assert result === expected
    end
  end
end
