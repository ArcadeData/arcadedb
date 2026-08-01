# #5720 - a script that hits RETURN or BREAK never closes the plans of the statements before it

## Problem

`ScriptExecutionPlan.close()` was a single call:

```java
public void close() {
  lastStep.close();
}
```

That relies on `lastStep` being the tail of the `prev` chain built by `chain()`, so that closing it cascades
over every `ScriptLineStep` and releases the per-statement `InternalExecutionPlan` each one holds.

`executeUntilReturn()` and `doExecute()` break that assumption. On the RETURN path `lastStep` becomes the
`ReturnStep` that `ScriptLineStep.executeUntilReturn()` builds; on the BREAK path it becomes a fresh
`BreakStep`. Neither constructor calls `setPrevious(...)` and neither class overrides `close()`, so
`AbstractExecutionStep.close()` finds `prev == null` and returns immediately. Every script line executed
before the RETURN/BREAK, and every plan it holds, was dropped without `close()`.

## Impact

A resource-release gap, not a crash: result sets, index cursors and iterators held by the per-statement plans
were left to the garbage collector instead of being released deterministically. It scales with the number of
statements that ran before the RETURN/BREAK.

## Root cause

`lastStep` is an execution cursor, not an ownership handle. Ownership of the per-statement plans lives in the
`steps` list, which `chain()` also links through `prev`. Closing the tail of `steps` is therefore the only
entry point that releases everything, and it is the one the old code did not use once `lastStep` had been
swapped.

## Fix

`ScriptExecutionPlan.close()` now closes the tail of `steps` instead of `lastStep`. The walk stays iterative:
`ScriptLineStep.close()` (from #5709) consumes the run of script lines in a loop, so a large batch that
returns early does not push one frame per statement.

`lastStep` is deliberately not closed separately:

- on the normal path it *is* `steps.getLast()`, already released by the chain walk;
- on the RETURN/BREAK paths it is a `ReturnStep` or a `BreakStep`, which hold no resources of their own
  (`ReturnStep` holds a statement, `BreakStep` holds nothing) - closing them is a no-op;
- when it is a step borrowed from a nested plan (an inner `ScriptExecutionPlan`/`IfExecutionPlan` returns its
  own last step), that nested plan is itself held by one of this plan's script lines, so the chain walk
  already releases it. Closing it again here would double-close the inner chain.

`setSteps()` has no caller, so `chain()` is the only writer of `steps` and the list tail is always the chain
tail.

## Verification

New `ScriptExecutionPlanCloseTest` (engine, unit lane), driving `ScriptExecutionPlan` directly over recording
stub plans:

- `aScriptThatReturnsEarlyStillReleasesEveryLineBeforeIt` - RETURN in the middle of the script; every
  recording line is released, tail to head.
- `aScriptThatBreaksStillReleasesEveryLineBeforeIt` - same for the BREAK path.
- `closingALargeScriptThatReturnedEarlyDoesNotOverflowTheStack` - 20,000 lines closed on a thread built with
  an explicit 1 MB stack, so the verdict does not depend on the CI JVM's `-Xss`. Pins the iterative walk that
  #5709 introduced.

All three fail against the old `close()` (nothing is released, so the recording list is empty).

Regression check: `SQLScriptLargeBatchTest`, `ScriptLineStepCloseTest`, and the SQL script/batch test classes
in `engine`.
