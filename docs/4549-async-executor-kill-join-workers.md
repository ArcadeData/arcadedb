# Fix #4549: `DatabaseAsyncExecutorImpl.kill()` does not join worker threads

## Issue Summary

`kill()` sets `forceShutdown = true` on each `AsyncThread` but then returns immediately.
The comment read "WAIT FOR SHUTDOWN, MAX 1S EACH" yet no `interrupt()` or `join()` call was
present. Worker threads continued running after `kill()` returned, operating on structures
already closed by `LocalDatabase.closeInternal()` and producing spurious errors.

## Root Cause

`kill()` (lines 670-681 before fix) did:
1. Unpublish `executorThreads` (set to null)
2. Set `forceShutdown = true` on each thread

Missing:
- `interrupt()` - without this a thread blocked in `queue.poll(500, MILLISECONDS)` takes up to
  500 ms to notice the flag flip
- `join(1000)` - without this the method returns before threads have stopped

## Fix

In `kill()`, after setting `forceShutdown = true`, call `interrupt()` on every thread (outside
the synchronized block to avoid holding `lifecycleLock` while blocking), then `join(1000)` on
each. The `AsyncThread.run()` already handles `InterruptedException` correctly (clears the queue
and breaks the loop at lines 182-185), so interrupt is always safe to call.

## Files Changed

- `engine/src/main/java/com/arcadedb/database/async/DatabaseAsyncExecutorImpl.java`
  - `kill()` method: interrupt threads, then join with 1-second timeout per thread
- `engine/src/test/java/com/arcadedb/database/async/DatabaseAsyncExecutorKillTest.java` (new)
  - Regression test verifying no async worker threads survive after `kill()` returns

## Test Results

All 30 related tests pass with no regressions (`mvn test -pl engine -Dtest="DatabaseAsync*,AsyncTest,ACIDTransactionTest"`):
- `DatabaseAsyncExecutorKillTest#killWaitsForWorkerThreadsToStop` (new) - fails before fix
  (2 worker threads alive after `kill()`), passes after
- `ACIDTransactionTest` (11 tests, direct `kill()` callers) - green
- `DatabaseAsyncExecutorLifecycleRaceTest#concurrentLifecycleChangesDoNotNPE` - green
- `AsyncTest` (11), `DatabaseAsyncRecordRollbackOnExceptionTest` (4), retry tests - green

## Impact Analysis

`kill()` now interrupts every worker (waking any blocked in the 500 ms `queue.poll`) and
joins with a 1-second timeout each before returning. The interrupt/join loop runs OUTSIDE the
`lifecycleLock` so the lock is not held while blocking - concurrent `getThreadCount`/`getStats`
readers (which already null-check the snapshot) are unaffected since `executorThreads` was set
to null inside the lock before releasing it. `AsyncThread.run()` already handles
`InterruptedException` (clears queue, breaks) so interrupt is always safe. This closes the
window in which `LocalDatabase.kill()` -> `schema.close()`/`fileManager.close()` ran while async
workers were still mutating now-closed structures.

## Review Cycles

### Cycle 1 (PR #4573)

- **gemini-code-assist + claude bot:** `InterruptedException` in the join loop re-asserted the
  interrupt flag mid-loop, so the next `join(1000)` threw immediately and remaining threads were
  skipped without waiting. Adopted the deferred-reinterrupt pattern: record `interrupted` in a
  local, keep joining every thread, re-assert the interrupt status once after the loop. Chosen
  over gemini's `break` because `kill()`'s contract is to stop *all* workers - breaking would
  abandon the remaining threads. Mirrors the single-span interrupt handling in
  `shutdownThreadsLocked()`.
- **claude bot (soft, non-blocking):** added a WARNING log when a thread is still alive after its
  1s join timeout, to aid production diagnosis of hangs.
- **claude bot - "remove this docs file":** declined. Committed `docs/<issue>-*.md` tracking docs
  with `Review cycles` + `Final state` sections are an established repo convention (e.g.
  docs/4397, 4393, 4446, 4332). The stale "not committed" line was the only genuine issue and is
  corrected below.

### Cycle 2 (PR #4573)

- **claude bot:** confirmed all cycle-1 fixes correct and agreed the tracking doc follows the
  established `docs/` convention (59 sibling per-issue docs). Two new items:
  - **Redundant `t.isAlive()` in the test filter** - `Thread.getAllStackTraces()` only reports
    live threads, so the extra `isAlive()` check was dead. Removed; the post-kill filter now
    mirrors the pre-kill one.
  - **`shutdownThreadsLocked()` has the same interrupt-skip pattern** - declined for this PR.
    Reviewer itself labelled it "pre-existing, out of scope." That method interleaves a blocking,
    interruptible `queue.put(FORCE_EXIT)` with `join(10000)` and sits on the production `close()`
    path (not the test-only `kill()` path), so a deferred-reinterrupt refactor there is separate
    work with its own regression surface. Tracked as a follow-up rather than widening #4549.

### Cycle 3 (PR #4573)

- **claude bot:** approved ("Ready to merge"). All fixes confirmed correct, the
  `shutdownThreadsLocked()` follow-up agreed as correctly deferred. One optional observation
  (explicitly "this is fine"): the test asserted only thread death, not post-`kill()` database
  state. `LocalDatabase.kill()` sets `open = false` in its `finally`, so that invariant is
  well-defined - added `assertThat(database.isOpen()).isFalse()` to make the contract explicit.

## Final State

Fix committed and pushed; PR #4573, approved at cycle 3. Cycle-1 (deferred-reinterrupt join loop
+ timeout warning log), cycle-2 (redundant test filter removed), and cycle-3 (post-kill
`isOpen()==false` assertion) review items addressed. One deliberate out-of-scope follow-up noted:
apply the same deferred-reinterrupt pattern to `shutdownThreadsLocked()`.
