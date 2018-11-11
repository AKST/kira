defmodule KiraTest do
  alias Kira, as: T

  defmodule Util do
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

  defmodule BranchStateTest do
    use ExUnit.Case

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

  defmodule RuntimeStateTest do
    use ExUnit.Case

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

  defmodule UnapplyTest do
    use ExUnit.Case

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

  defmodule ApplyTest do
    use ExUnit.Case

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

  defmodule RuntimeTest do
    use ExUnit.Case, async: true
    require Logger

    defmodule Ok do
      def a_branch,
        do: %T.Branch{
          name: :a,
          apply: fn config, _ ->
            send(config, {:collect, :a})
            {:ok, 2}
          end
        }

      def b_branch,
        do: %T.Branch{
          name: :b,
          dependencies: [:a],
          apply: fn config, dependencies ->
            send(config, {:collect, :b})
            {:ok, dependencies[:a] + 2}
          end
        }

      def c_branch,
        do: %T.Branch{
          name: :c,
          dependencies: [:a],
          apply: fn config, dependencies ->
            :timer.sleep(50)
            send(config, {:collect, :c})
            {:ok, dependencies[:a] + 3}
          end
        }

      def d_branch,
        do: %T.Branch{
          name: :d,
          dependencies: [:b, :c],
          apply: fn config, dependencies ->
            send(config, {:collect, :d})
            {:ok, dependencies[:b] + dependencies[:c]}
          end
        }
    end

    defmodule Partial do
      def a_branch,
        do: %T.Branch{
          name: :a,
          apply: fn config, _dependencies ->
            send(config, {:collect, {:apply, :a}})
            {:ok, 2}
          end,
          unapply: fn config, _dependencies, _r ->
            send(config, {:collect, {:unapply, :a}})
            {:ok, :undefined}
          end
        }

      def b_branch,
        do: %T.Branch{
          name: :b,
          dependencies: [:a],
          apply: fn config, _dependencies ->
            send(config, {:collect, {:apply, :b}})
            {:ok, 2}
          end,
          unapply: fn config, _dependencies, _r ->
            send(config, {:collect, {:unapply, :b}})
            {:ok, :undefined}
          end
        }

      def c_branch,
        do: %T.Branch{
          name: :c,
          dependencies: [:a],
          apply: fn config, _dependencies ->
            :timer.sleep(50)
            send(config, {:collect, {:apply, :c}})
            {:ok, 2}
          end,
          unapply: fn config, _dependencies, _r ->
            :timer.sleep(50)
            send(config, {:collect, {:unapply, :c}})
            {:ok, :undefined}
          end
        }

      def d_branch,
        do: %T.Branch{
          name: :d,
          dependencies: [:b, :c],
          apply: fn config, _dependencies ->
            send(config, {:collect, {:apply, :d}})
            {:error, :always_fail}
          end
        }
    end

    def collect_n(0, collected), do: Enum.reverse(collected)

    def collect_n(n, collected) do
      receive do
        {:collect, name} -> collect_n(n - 1, [name | collected])
      after
        500 ->
          raise "abort, #{collected}"
      end
    end

    describe "run_tasks" do
      test "works" do
        result =
          T.Runtime.run_tasks(self(), [Ok.a_branch(), Ok.b_branch(), Ok.c_branch(), Ok.d_branch()])

        assert result == {:ok, %{a: 2, b: 4, c: 5, d: 9}}

        messages = collect_n(4, [])
        assert messages == [:a, :b, :c, :d]
      end

      test "unstartable" do
        result = T.Runtime.run_tasks(self(), [Ok.b_branch(), Ok.c_branch(), Ok.d_branch()])
        expect = {:error, :unstartable}
        assert result == expect
      end

      test "rollsback on failure" do
        result =
          T.Runtime.run_tasks(
            self(),
            [
              Partial.a_branch(),
              Partial.b_branch(),
              Partial.c_branch(),
              Partial.d_branch()
            ],
            1000
          )

        assert Util.is_error(result)
        assert is_list(elem(result, 1))

        messages = collect_n(7, [])

        assert messages == [
                 {:apply, :a},
                 {:apply, :b},
                 {:apply, :c},
                 {:apply, :d},
                 {:unapply, :b},
                 {:unapply, :c},
                 {:unapply, :a}
               ]
      end

      test "apply_exit during rollback" do
        the_culprit = {:failed, :for_some_reason}

        bad_task = %T.Branch{
          name: :bad_task,
          apply: fn _, _ ->
            :timer.sleep(10)
            {:error, the_culprit}
          end,
          unapply: fn _, _, _ -> {:ok, :idk} end
        }

        good_task = %T.Branch{
          name: :good_task,
          apply: fn _, _ ->
            :timer.sleep(75)
            {:ok, :lol}
          end,
          unapply: fn _, _, _ -> {:ok, :idk} end
        }

        # the good_task should exit after the bad_task fails, meaning
        # it'll send a done operation to the rollback loop.
        result = T.Runtime.run_tasks(:nothing, [good_task, bad_task], 1000)
        assert Util.get_failure_reasons(result) === [the_culprit]
      end

      test "rollback without unapply callback" do
        the_culprit = {:failed, :for_some_reason}

        bad_task = %T.Branch{
          name: :bad_task,
          apply: fn _, _ ->
            :timer.sleep(10)
            {:error, the_culprit}
          end
        }

        good_task = %T.Branch{
          name: :good_task,
          apply: fn _, _ ->
            :timer.sleep(75)
            {:ok, :lol}
          end
        }

        # the good_task should exit after the bad_task fails, meaning
        # it'll send a done operation to the rollback loop.
        result = T.Runtime.run_tasks(:nothing, [good_task, bad_task], 1000)
        assert Util.get_failure_reasons(result) === [the_culprit]
      end

      test "will treat a crash as a retry" do
        will_crash = %T.Branch{
          name: :bad_task,
          apply: fn _, _ ->
            420 / 0
          end,
          on_apply_error: fn parent, _, error, _ ->
            send(parent, :did_retry)
            {:retry, false}
          end
        }

        result = T.Runtime.run_tasks(self(), [will_crash], 1000)
        assert_receive :did_retry
        assert length(Util.get_failure_reasons(result)) === 1
        Logger.flush()
      end

      # TODO test retry retrying a rollback
      #
      # TODO test usage of tasks that fail using exceptions
      #
      # TODO test a retry that allows the tasks to recovery
    end
  end
end
