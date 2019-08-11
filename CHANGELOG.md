# CHANGELOG

## 0.2.1

Added rollback functionality.

## 0.2.0 (prerelease notes)

This included a mixture of internal refactoring and
simpification of the public API, mostly to reduce
the surface area and allow more with less. So this
is a breaking change.

### API Changes.

`Branch`s are now `TaskDefinition`s and the logic
to handle the apply & unapply events are now all
done with a single function. TaskDefinition are
defined like this:

```ex
task_def = %Kira.TaskDefinition{
  name: :task_a,
  dependencies: [:task_b, :task_c],
  dispatcher: fn (:task_a, event) ->
    case event do
      {:apply, _, %{:task_b => b, :task_c => c}} ->
        {:ok, b + c}

      {:apply_error, n, _} ->
        {:retry, false}

      _ ->
        :not_implemented
    end
  end
}
```

This API change might be superficial, but it's a lot
noisy than the previous API. I'm also under the
impression I'm the only user of the library so yolo.

### Internal Changes

- Renamed a few internal modules
  - Branch -> TaskDefinition
  - BranchState -> Task

- RuntimeState
  - The running field is now moved to Progress as the pid_cache field.
  - Made the module more DRY.
  - Updated referrences to Branch & BranchState.

- Progress now tracks the name associated with a pid,
  instead of runtime state. This makes alot of sense
  as the pid cache (formerly the running field) was
  always together with progress.

### Development Changes

- Updated dialzyer version.

## 0.1.1

- Improved documentation, and hide internal modules
  from the documentation so when you view the docs
  you don't get overwhelmed with a bunch of useless
  info related to the internal implementation.

## 0.1.0

- Base functionality, varying degrees of correctness
