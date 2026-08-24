# Issue #6462: async cross-slot bidirectional edge - waitCompletion() / quiesceAsync() races

## Root cause

`DatabaseAsyncExecutorImpl.newEdge()` schedules the incoming-edge cascade task for a bidirectional
cross-slot edge onto the **destination** worker's slot from **inside** the source task's own
callback (`CreateEdgeAsyncTask`'s completion callback schedules `CreateIncomingEdgeAsyncTask`). That
means the cascade can land on the destination worker strictly *after* that worker has already
answered "I'm done" to a concurrent caller, in two different mechanisms:

1. **`DatabaseAsyncExecutorImpl.waitCompletion(long)`** placed exactly one completion marker per
   worker in a single pass. If the destination worker was idle, its marker could fire before the
   cascade (still queued behind the still-running source task on a different worker) ever landed on
   it - so the method could return "all done" while the in-edge was still pending.
   `LocalDatabase.waitForAsyncCompletion()` already re-scans with a `do { ... } while
   (isAsyncProcessing())` loop for exactly this reason (issue #6281), but that loop lives one level
   above the executor - a caller reaching `waitCompletion()` directly through the public
   `DatabaseAsyncExecutor` API bypassed it entirely.
2. **`BucketIndexBuilder.create()`** calls `database.quiesceAsync()` (which parks every worker) but,
   unlike `TypeIndexBuilder` and `RebuildIndexStatement`, never calls
   `database.waitForAsyncCompletion()` first. A destination worker parked while idle does not wait
   for a cascade that lands on it afterwards, so the scan underneath the quiescence can run before
   that cascade's write is committed - the built index misses the entry until a later, unrelated
   drain catches it up.

## Fix

1. `DatabaseAsyncExecutorImpl.waitCompletion(long)` (`engine/src/main/java/com/arcadedb/database/async/DatabaseAsyncExecutorImpl.java`):
   the marker-placement body was extracted into a private `waitCompletionOnePass(...)`, and the public
   method now loops `do { ... } while (isProcessing())`, mirroring the pattern
   `LocalDatabase.waitForAsyncCompletion()` has used since #6281. This closes the race for every
   direct caller of the public API, not only callers that happen to go through `LocalDatabase`.
2. `BucketIndexBuilder.create()` (`engine/src/main/java/com/arcadedb/schema/BucketIndexBuilder.java`):
   added a `database.waitForAsyncCompletion()` call immediately before `database.quiesceAsync()`,
   matching the pattern already used by `TypeIndexBuilder` (line ~207) and `RebuildIndexStatement`
   (line ~146). By the time the quiescence starts, the executor is genuinely idle rather than racing
   it.

Both fixes are the minimal, precedented options from the issue's own "Suggested fix" list (options 1
and 3); the more invasive option (reworking `quiesceWorkers()`'s park scheduling itself) was not
needed once the drain happens first.

## Tests

- `engine/src/test/java/com/arcadedb/database/async/Issue6462CrossSlotWaitCompletionMissesCascadeTest.java`:
  reproduces the `waitCompletion()` race with a plain task scheduled directly at the slot level (no
  graph API, no dependency on which slot a RID happens to hash to) that, from inside its own
  execution, schedules a follow-up task onto a different worker - mirroring `newEdge()`'s callback
  shape. The follow-up is held open with a latch so the buggy and fixed behaviour are distinguished
  deterministically (by what has/has not happened when `waitCompletion()` returns), not by timing.
  Verified TDD-style: fails against the pre-fix code (`Expecting value to be false but was true`),
  passes after the fix.
- `engine/src/test/java/com/arcadedb/index/Issue6462BucketIndexBuilderMissesCrossSlotCascadeTest.java`:
  same reduction applied to `BucketIndexBuilder.create()` - a source task on one worker creates a
  record targeting the OTHER worker's bucket from inside its own execution once released, and the
  index build runs concurrently. Asserts that by the time `create()` returns, both the record and its
  index entry are already present - not merely "eventually" once a later drain catches up. Verified
  TDD-style: fails 3/3 runs against the pre-fix code (`expected: 1L but was: 0L`), passes 3/3 after.

Both tests were run repeatedly (3x) against both the pre-fix and post-fix code to confirm they are
not vacuously passing and are not flaky.

## Verification

- `mvn -pl engine -am test -Dtest='com.arcadedb.database.async.**,com.arcadedb.index.Issue6303*,com.arcadedb.index.Issue6462*' -DexcludedGroups=benchmark,slow,vector` - 69 tests, 0 failures.
- `mvn -pl engine -am test -Dtest='com.arcadedb.index.**,com.arcadedb.schema.**,com.arcadedb.graph.**' -DexcludedGroups=benchmark,slow,vector` - 2213 tests, 0 failures (covers `AsyncWaitCompletionTimeoutTest`, `Issue6303AsyncQuiesceTest`, `AsyncCrossSlotSchedulingDeadlockTest`, and every index/schema/graph regression test in the module).
- `mvn -pl engine -am compile` - clean.

## PR

https://github.com/ArcadeData/arcadedb/pull/6651

## Review cycles

- **Cycle 1** - head `1cb89451075f1409aa37a71de8c678a430acbeb9`: `claude[bot]` reviewed via issue
  comment. Verdict: "No blocking issues found." Traced the happens-before chain of the
  `waitCompletion()` fix independently and confirmed it sound; confirmed the `BucketIndexBuilder`
  fix mirrors `TypeIndexBuilder`/`RebuildIndexStatement` exactly; confirmed both tests avoid
  timing-based flakiness (latch/`completedTaskCount` polling, the one `Thread.sleep` only widens a
  race window rather than gating an assertion, so worst case it fails to reproduce and passes
  vacuously rather than failing falsely). One **non-blocking** observation, skipped (see below).
  No code changes required - working tree stayed clean this cycle - so this is a clean approval on
  cycle 1.

### Deferred / skipped review items

- **Skipped (nitpick, optional, explicitly non-blocking):** the reviewer noted that
  `LocalDatabase.waitForAsyncCompletion()`'s own `do { ... } while (isAsyncProcessing())` loop
  (#6281) is now a "loop-around-a-loop" on top of `waitCompletion()`'s new internal loop, and
  suggested a possible follow-up to simplify it back to a single call. Not applied: the reviewer
  itself flagged this as conditional on "not complicating the interrupt-handling special case"
  `LocalDatabase.waitForAsyncCompletion()` currently has, explicitly called it harmless, and it
  touches a different, separately-reasoned method than either of the two changes this issue is
  about. Left as a possible future cleanup rather than folded into this fix.

## Final state

clean-approval (1 cycle)

## Scope not addressed

`DatabaseAsyncExecutorImpl.quiesceWorkers()` itself (the park-task machinery `quiesceAsync()` is built
on) was left untouched. Draining first, as both fixes now do, removes the specific race the issue
describes without needing to change that already carefully-reasoned, heavily-reviewed method (#6303).
A caller who reaches `quiesceAsync()` directly without draining first (bypassing both
`BucketIndexBuilder`/`TypeIndexBuilder`/`RebuildIndexStatement`, all of which now drain first) could
still hit a variant of the destination-worker-parked-early race; no such direct caller exists in the
codebase today.
