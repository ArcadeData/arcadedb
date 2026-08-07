# Issue #5665: GraphBatch flush tears down and respawns the whole async worker pool

https://github.com/ArcadeData/arcadedb/issues/5665

## Root cause

`GraphBatch.connectOutgoingEdgesParallel()` (called once per `flush()`) and
`connectIncomingEdgesParallel()` (called once at `close()`) each relaxed and then restored the
shared async executor's WAL policy around their parallel connect phase:

```java
async.setTransactionUseWAL(useWAL);
async.setTransactionSync(walFlush);
... schedule work, waitCompletion() ...
async.setTransactionUseWAL(savedAsyncWAL);
async.setTransactionSync(savedAsyncSync);
```

`DatabaseAsyncExecutorImpl.setTransactionUseWAL()`/`setTransactionSync()` each unconditionally call
`createThreads()`, which tears down the entire worker pool (`shutdownThreadsLocked()`: FORCE_EXIT
every worker, join, drop in-flight/queued tasks) and spawns a brand new one - regardless of whether
the value actually changed. With `connectOutgoingEdgesParallel` running once per flush, a large
multi-flush bulk load (the reporter's 663M-vertex / 14B-edge case: ~140k flushes) recreated the
entire pool 4 times per flush, ~560k pool teardowns for the run.

Two consequences:
- **Solo stream**: pure overhead, plus every recreation drops each worker's warm `DatabaseContext`
  and open transaction, forcing a re-begin.
- **Any concurrency**: a second, unrelated user of `database.async()` on the same database (another
  `GraphBatch`, the async insert API, an async HTTP/gRPC handler) has its in-flight and queued tasks
  force-exited by the first batch's flush - surfacing as `DatabaseOperationException("Async executor
  has been shut down")` for a caller that never touched WAL settings at all.

## Fix

`GraphBatch` now relaxes the async executor's WAL policy **once**, in the constructor (only when
`parallelFlush` is true), and restores it **once**, in `restoreAsyncSettings()` - called from both
`close()` (via `restoreDatabaseSettings()`) and `abandon()` (a batch that never flushes must not
leave the database-wide policy relaxed forever). Both the relax and the restore are guarded on an
actual value change, since the executor's setters are not.

`connectOutgoingEdgesParallel()`/`connectIncomingEdgesParallel()` no longer touch the async
executor's WAL policy at all - they only schedule work and `waitCompletion()`.

This does not touch `DatabaseAsyncExecutorImpl` itself: several existing async regression tests
(`AsyncStallDetectorFalsePositiveTest`, `AsyncCrossSlotSchedulingDeadlockTest`,
`AsyncHelpingAtomicityTest`, `AsyncWedgedWorkerStallTest`, `AsyncShutdownDrainTest`,
`AsyncShutdownEscalationTest`, `AsyncSlowPeerNoFalseStallTest`, `AsyncWaitCompletionTimeoutTest`)
deliberately call `setTransactionUseWAL(true)` purely to force a pool recreation that picks up a
just-lowered `ASYNC_OPERATIONS_QUEUE_SIZE`, "regardless of the previous [parallel] level". Adding a
same-value guard to the shared setters (the "deeper" fix the issue also sketches: apply the WAL
flags per-task instead of per-thread-start) would silently defeat that idiom on any CI runner where
`ASYNC_WORKER_THREADS` already defaults to the value the test sets (routine on 2-vCPU runners, where
the default is `cores - 1 = 1`). The GraphBatch-level fix implemented here is the "cheaper interim
fix" the issue calls out as worth doing regardless of the deeper one, and it fully addresses the
reported symptom (thread-pool churn / "Async executor has been shut down" for concurrent callers)
without touching that shared, widely depended-upon behavior.

## Trade-off accepted

Before the fix, the async executor's relaxed WAL policy was only in effect during the brief window
of an actual parallel connect phase. After the fix, it is in effect for the entire lifetime of a
`parallelFlush=true` `GraphBatch` (construction through close/abandon) - a wider window during which
an unrelated concurrent `database.async()` caller runs under the batch's relaxed durability, not its
own. This is the trade-off the issue explicitly recommends; it replaces near-certain pool churn
(and consequent task loss) on every flush with a bounded, already-logged relaxation
(`GraphBatch: relaxing durability for the bulk load ...`) for the load's duration.

## Tests

`engine/src/test/java/com/arcadedb/graph/Issue5665GraphBatchAsyncPoolChurnTest.java` (new):

- `asyncWorkerPoolSurvivesMultipleParallelFlushesUnchanged`: captures async worker thread identities
  (`AsyncExecutor-<db>-<n>` thread names) after the first flush of a `parallelFlush=true` batch, runs
  several more flushes, and asserts the thread-ID set is unchanged. Fails against the pre-fix code
  (thread IDs differ after every flush); passes against the fix.
- `unrelatedAsyncCallerSurvivesConcurrentParallelFlushes`: a background thread continuously submits
  no-op `database.async().transaction()` work while the main thread runs many `GraphBatch` flushes
  concurrently. Asserts the unrelated caller sees zero errors. Fails against the pre-fix code with
  repeated `DatabaseOperationException: Async executor has been shut down`; passes against the fix.

Both were verified RED (via `git stash` of the production change) before the fix and GREEN after.

## Verification run

- `mvn -pl engine test -Dtest=Issue5665GraphBatchAsyncPoolChurnTest`: 2/2 pass.
- `mvn -pl engine test -Dtest='com.arcadedb.graph.GraphBatch*Test,com.arcadedb.graph.Issue5666ConcurrentGraphBatchTest,com.arcadedb.database.async.*Test'`: all pass (35 test classes, including the
  `AsyncStallDetectorFalsePositiveTest`-style tests that rely on `setTransactionUseWAL`'s pool-recreate
  side effect - untouched by this fix since `DatabaseAsyncExecutorImpl` was not modified).
- `mvn -pl engine test -Dtest='com.arcadedb.graph.*Test'`: full graph package regression (see below).
