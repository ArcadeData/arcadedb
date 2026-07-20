# Issue #5326 - Flaky on CI: applyChanges/page-replay tests

## Symptom

`Issue4712ReplicatedWriteLockTest`, `Issue4510ForceApplyPartialDeltaTest` and `ApplyChangesPartialReplayTest` fail
intermittently on CI with `expected: 2L but was: 1L` on assertions of the form

```java
pageManager.removePageFromCache(pageId);
final ImmutablePage reloaded = pageManager.getImmutablePage(pageId, pageSize, false, true);
assertThat(reloaded.getVersion()).isEqualTo(baseVersion + 1);
```

An A/B re-run of the same commit produced 3 failures and then 0 failures, so the variable is timing, not code.

## Root cause

It is not a test-only defect: the tests were observing a genuine ordering hole in
`TransactionManager.applyChanges`.

A committed page is published to the read cache and to `PageManagerFlushThread.pageIndex` **before** it reaches the
disk (`PageManager.writePages(..., asyncFlush=true)`). `applyChanges`, used for replicated and recovery replay, writes
the page straight to its file via `PageManager.writePageWithLock` and evicts it from the read cache, but never looks at
the flush pipeline. While the older copy is still pending, two things go wrong:

1. `PageManager.loadPage` consults `flushThread.getCachedPageFromMutablePageInQueue(pageId)` **before** reading the
   file. Once `applyChanges` evicts the read cache, every subsequent read resolves the page from the flush queue and
   returns the superseded version - which is exactly the failing assertion.
2. When the flush thread later writes that queued copy, it overwrites the replicated page on disk and rolls the page
   version backwards, re-opening the version-gap cascade the replay exists to close.

Whether either is observed depends on whether the flush thread drains the queue before the assertion runs. On an idle
macOS box it always does; on a loaded Linux CI runner sharing one JVM (`forkCount=1, reuseForks=true`) with 13k
preceding tests, sometimes it does not.

## Fix

`applyChanges` now drains the pipeline for the page it is about to write, before reading its version:

- `PageManagerFlushThread.detachPendingPages(database, pageId)` removes EVERY pending `MutablePage` for the page from
  `pageIndex` and from the queued, in-flight and deferred batches. Copies are matched by `PageId`, not by reference
  identity: successive commits can leave two instances pending at once (the newest in `pageIndex`, an older one still
  in an earlier batch - the two-instance case `removeFromFlushIndex` exists for, #4544), and removing only the indexed
  one would leave the older copy free to write the superseded version over the replicated one. Deferred removals
  release their reserved RAM accounting (#4728).
- `PageManager.materializePendingFlushOfPage(database, pageId)` detaches the copies, waits for the in-flight batch to
  finish (the detach can lose that race, and a write landing after ours would revert the page), writes the most recent
  copy to disk via `flushPage`, releases the superseded copies' WAL acks (exactly-once via `takeWALFile`, as the
  dropped-file purge does) and evicts the read-cache copy.

The pending content is **written**, not discarded: it is the only baseline the WAL delta can be applied on top of,
since the disk may be several versions behind it. The most recent copy is a full page image covering every older one,
so writing it alone puts all the pending content on disk.

`InterruptedException` from the drain is caught in `applyChanges` and rethrown as `WALException` with the interrupt
flag restored, so an interrupted replay fails loud instead of leaving a silent version gap.

## Files changed

- `engine/src/main/java/com/arcadedb/engine/TransactionManager.java`
- `engine/src/main/java/com/arcadedb/engine/PageManager.java`
- `engine/src/main/java/com/arcadedb/engine/PageManagerFlushThread.java`
- `engine/src/test/java/com/arcadedb/engine/Issue5326ApplyChangesPendingFlushTest.java` (new)

No existing test was modified; the three flaky classes are left untouched and now pass deterministically because the
condition they assert on no longer depends on flush timing.

## Test

`Issue5326ApplyChangesPendingFlushTest` is a white-box test in the `com.arcadedb.engine` package. It quiesces the flush
pipeline, then publishes a pending copy of page 0 into `flushThread.pageIndex` to reproduce the between-commit-and-flush
state deterministically, applies a WAL entry at `baseVersion + 1` and asserts that

- no stale copy is left in the pipeline afterwards, and
- both a cache-hit read and a read forced back to disk see `baseVersion + 1`.

Before the fix it fails on the first assertion (the pending copy survives) and on the version assertions.

Two further tests drive the same defect through the real flush pipeline, with flushing suspended so ordinary commits
park page 0 in genuine deferred batches:

- `applyChangesTakesTheDeferredBatchCopyOutOfThePipeline` - one commit; covers the batch removal and the #4728
  deferred-RAM decrement, and asserts that after resuming the batch can no longer write the superseded version.
- `applyChangesTakesEveryPendingCopyOfThePageOutOfThePipeline` - two commits, so two copies of page 0 are pending at
  once; asserts the deferred backlog drops by exactly both copies' size, which is what a `PageId`-matched detach does
  and an identity-matched one does not.

## Verification

- `mvn -pl engine test -Dtest=Issue5326ApplyChangesPendingFlushTest` - fails before the fix, passes after.
- `mvn -pl engine test -Dtest='Issue4712*,Issue4510*,ApplyChanges*,Issue5326*,*PageManager*,*FlushThread*,*WAL*,Issue4928*,Issue4544*,Issue4728*,Issue5068*'`
  - 54 tests, 0 failures.
- `mvn -pl engine test -Dtest='com.arcadedb.engine.**,com.arcadedb.database.**'` - 642 tests, 0 failures; the single
  error is `TimeSeriesEmbeddedBenchmark.run` failing with `No space left on device` on the build machine, unrelated to
  this change.

## Impact

Beyond de-flaking CI, this closes a real durability hole on the replication and crash-recovery path: a replicated page
write could previously be silently reverted by a stale queued flush, which is the version-regression signature behind
the `WALVersionGapException` cascades referenced in #4510 and #5322.
