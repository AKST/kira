defmodule Kira.RuntimeTest do
  use ExUnit.Case, async: true

  alias Kira, as: T
  alias Kira.Util.Testing, as: Util

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
      result = T.Runtime.run_tasks(self(), [Ok.a_branch(), Ok.b_branch(), Ok.c_branch(), Ok.d_branch()])

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
          throw("error")
        end,
        on_apply_error: fn parent, _, _error, _ ->
          send(parent, :did_retry)
          {:retry, false}
        end
      }

      result = T.Runtime.run_tasks(self(), [will_crash], 1000)
      assert_receive :did_retry
      assert length(Util.get_failure_reasons(result)) === 1
    end

    @tag :skip
    test "can recover in retry" do
      # on the second call it will succeed
      do_fail =
        (
          outside = self()

          stateful_process =
            spawn(fn ->
              receive do
                :get -> send(outside, {:do, true})
              end

              receive do
                :get -> send(outside, {:do, false})
              end
            end)

          fn ->
            send(stateful_process, :get)

            receive do
              {:do, state} -> state
            end
          end
        )

      will_retry = %T.Branch{
        name: :will_retry,
        apply: fn _, _ ->
          if do_fail.(), do: {:error, 0}, else: {:ok, 0}
        end,
        on_apply_error: fn _parent, _, _error, _ ->
          {:retry, true}
        end
      }

      result = T.Runtime.run_tasks(self(), [will_retry], 1000)
      assert result === %{will_retry: 0}
    end

    # TODO test retry retrying a rollback
    #
    # TODO test usage of tasks that fail using exceptions
    #
    # TODO test a retry that allows the tasks to recovery
  end
end
