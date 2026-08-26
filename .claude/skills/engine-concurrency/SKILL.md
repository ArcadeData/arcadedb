---
name: engine-concurrency
description: Use when adding, reviewing, or debugging parallelism in ArcadeDB engine or server code - covers the dedicated thread pool inventory and sizing, saturation policy, lock-free read patterns, locking rules, Micrometer pool metrics, and the checklist a new pool must satisfy. Also use when deciding where to submit forked work, or when investigating pool saturation (caller_run_fallbacks).
---

# Engine Concurrency and Parallelism

**Core principle:** ArcadeDB avoids the JDK common ForkJoinPool (`ForkJoinPool.commonPool()`) for engine-internal parallelism. The common pool is shared with user-supplied code (Gremlin, Polyglot, custom SQL functions, application JVM) and JDK internals (parallel GC, reference handler), so long-running engine work submitted there starves user code and JDK housekeeping. Engine code that needs parallelism submits to one of the dedicated pools below; the rule is documented at the head of `com.arcadedb.query.QueryEngineManager`'s class Javadoc.

**Existing JDK-common-pool callers** (tagged in source with `NOTE (concurrency)` comments referencing the rule, will migrate as workloads justify it):
- `GraphBatch.parallelSort` (`engine/...graph/GraphBatch.java`)

`ArcadeStateMachine.notifyInstallSnapshotFromLeader` was on this list until issue #6202 gave it a dedicated
single-worker executor (`arcadedb-raft-snapshot-install`); a follower snapshot install is a full database
download, i.e. the longest-running thing the HA layer does, and it no longer parks a common-pool worker for it.

## Dedicated thread pools

| Pool | Module | Purpose | Sizing | Saturation policy |
|---|---|---|---|---|
| `QueryEngineManager` JVM-wide pool | `engine` | Query-time parallelism: graph algorithms (`parallelForRange`), parallel index scans, anything that forks query work | `arcadedb.queryParallelismPoolThreads` (default `max(2, CPU)`) | Bounded queue (`arcadedb.queryParallelismQueueSize`, default 1024), caller-runs rejection, throttled WARNING (60s window) |
| `SparseVectorScoringPool` | `engine` | Reserved for per-segment parallel scoring of `LSM_SPARSE_VECTOR` top-K (dispatch wiring deferred to issue #4085) | `arcadedb.sparseVectorScoringPoolThreads` (default `max(2, CPU)`), 1024 queue; lazy-init via Holder idiom | Bounded queue, caller-runs, throttled WARNING |
| `ParallelScanProducerPool` | `engine` | The BLOCKING producer tasks of a parallel bucket scan (`FetchFromTypeExecutionStep.syncPullParallel`) | `arcadedb.parallelScanProducerPoolThreads` (default `max(2, CPU)`) | **Deviation:** unbounded queue and NO caller-runs - caller-runs on a blocking producer is the #4948 self-deadlock. Back-pressure comes from each query's bounded RESULT queue; `queue_depth` is the saturation signal |
| `AsyncCommandPool` | `engine` | Commands dispatched with `awaitResponse=false` that parse to DDL, which cannot run on an async worker (#6303 item 3) | `arcadedb.asyncCommandPoolThreads` (default `max(2, CPU)`), `arcadedb.asyncCommandQueueSize` (default 1024) | Bounded queue, caller-runs **even on a shut-down pool** (there is no future to cancel and the submitter has already counted the command as in flight), throttled WARNING |
| `DatabaseAsyncExecutor` | `engine` | Per-database async ops (background scheduled tasks, async commit) | `arcadedb.asyncWorkerThreads` (default `CPU - 1`, min 1) | Bounded queue (`arcadedb.asyncOperationsQueueSize`, default 1024) |
| `PageManagerFlushThread` | `engine` | Dedicated single thread for paginated-component page writes | 1 thread | Backpressure via `arcadedb.maxRAMForPageRamUsageInMB` |
| `TransactionManager` Timer | `engine` | Periodic WAL housekeeping | Timer thread | n/a |
| `TimeSeriesEngine` pool | `engine` | Time-series rollup work | configurable | n/a |
| `BackupScheduler` | `server` | Cron-style backup jobs | scheduled executor | n/a |
| `MaterializedViewScheduler` | `server` | Cron-style MV refresh | scheduled executor | n/a |
| Raft HA pools | `ha-raft` | Leader election, log replication | configurable in Raft conf | n/a |
| `ArcadeStateMachine.snapshotInstallExecutor` | `ha-raft` | Leader-initiated follower snapshot install, off the Ratis state-machine thread | 1 worker, 16-deep queue (Ratis serialises installs per division) | Abort, turned into a failed future so Ratis retries - never caller-runs, which would put the download back on the Ratis thread |
| Undertow IO + worker | `server` | HTTP request handling | hardcoded 500 worker threads | Undertow built-in |
| `ServerMonitor` | `server` | Periodic metric collection | scheduled executor | n/a |

## Lock-free read patterns

Used on the read hot paths:

- `AtomicReference<T[]>` swaps for the current snapshot of immutable artifacts. Example: `PaginatedSparseVectorEngine.segments` is an `AtomicReference<PaginatedSegmentReader[]>`; readers grab the snapshot at query start and stay on it for the whole query, while flush/compaction publishes a new array atomically.
- Per-dim `ConcurrentSkipListMap` / per-key `ConcurrentHashMap` for live mutable state. Example: `Memtable.postings` is `ConcurrentHashMap<Integer, ConcurrentSkipListMap<RID, Float>>` - lock-free `put`, weakly-consistent ordered iteration during flush.
- `AtomicLong` version probes for O(1) "has this changed since I last looked?" gates. Example: `FileManager.modificationCount` bumps on every `registerFile` / `dropFile`; `PaginatedSparseVectorEngine.refreshSegmentsFromFileManager` reads it on every `topK` and short-circuits without walking the file list when unchanged.
- Sealed segments are immutable after `b.finish()`; multiple readers share file handles via `PaginatedComponent`'s page cache without per-segment locks.

## Locks

Used sparingly, only for publish-side serialization:

- `ReentrantLock` per engine instance for the publish-side mutex. Example: `PaginatedSparseVectorEngine.mutatorLock` orders flush + compaction segment-array publications so a query never sees an inconsistent intermediate state. Readers do not take this lock - they read the `AtomicReference` snapshot.
- `PageManager`'s per-page latches for read/write coordination on mutable pages.
- HA replication uses `database.getWrappedDatabaseInstance().runWithCompactionReplication(...)` recording sessions; the recording itself is single-threaded per session and the resulting `SCHEMA_ENTRY` ships atomically.

## Metrics (Micrometer)

- `PoolMetrics` MeterBinder registers seven gauges per pool with a `pool=<name>` tag: `size`, `active`, `queue_depth`, `queue_capacity_remaining`, `completed_tasks`, `caller_run_fallbacks`, `reclaimed`.
- Studio surfaces these via the "Executor Pools" card on the Server tab (live numbers).
- A sustained non-zero `caller_run_fallbacks` indicates pool saturation; the engine logs a throttled WARNING the first time and at most once per 60 s afterward.
- `reclaimed` is NOT a fault signal and is not warned about: it counts queued tasks a caller about to wait for them ran itself (`runQueuedTaskOnCaller`, issue #6568). It says the pool was busy, where `caller_run_fallbacks` says it was full.

## The shared base: `com.arcadedb.utility.DedicatedThreadPool`

The four JVM-wide pools above all extend it (issue #6324, item 4). It owns the `ThreadPoolExecutor` construction, the
`PoolStats` record the metrics binder reads, and the caller-runs rejection policy with its throttled saturation
WARNING - all three of which used to exist as four near-identical copies whose wording had already drifted apart.

What a subclass supplies to `super(...)`: the thread-name prefix, the thread count (`autoSizeThreads(configured)` is
the shared "explicit setting, else cores, floor 2"), the queue capacity (`queueSizeOrDefault`, or `UNBOUNDED_QUEUE`), a
`SaturationPolicy`, a `WorkerFactory` (`DedicatedThreadPool::plainWorker`, or a lambda that sets a per-worker
thread-local or subclasses `Thread` so the pool's threads are recognizable by type), the sentence the pool names itself
by in its warning, what the caller-runs fallback costs ITS callers, and the settings the warning tells the operator to
raise.

The constructor refuses the two combinations that cannot mean anything: an unbounded queue with a rejection policy
(it never rejects) and a bounded queue with `SaturationPolicy.NONE` (it must say what happens to a task it refuses).
That is the checklist below being enforced rather than remembered.

Each pool's REASONS stay in its own class javadoc - why blocking producers may not share a pool with non-blocking
compute, why dispatched DDL cannot run on an async worker, why a sparse-vector fan-out must not nest.

## Waiting for a task on a pool your thread may itself be running on (#6568)

**Never park on it. Reclaim it.**

The caller-runs saturation policy is routinely mistaken for the deadlock guarantee here, and it is not one: it fires
only when the queue is **full**. A queue with a thousand free slots accepts the task, parks the submitter, and the
deadlock is that both workers and submitters are now parked on tasks nobody is left to run. Every blocking fan-out has
that shape by construction as soon as two of them nest, and the failure presents as a lane timeout with no failing
assertion - which is how #6568 arrived, as a wedged `ParallelScanSafetyTest`.

- `DedicatedThreadPool.runQueuedTaskOnCaller(task)` takes a task the queue accepted but no worker has started back out
  and runs it on the calling thread. `ThreadPoolExecutor.remove` succeeding is the proof no worker has it, so it runs
  exactly once. It counts into `PoolStats.reclaimedTasks()` (and, like a caller-runs fallback, NOT into
  `completedTasks` - no pool thread ran it).
- It runs the task through `runRejectedTask`, the same hook a caller-runs rejection uses, so a reclaim and a fallback
  are the same execution for every pool. That is what a subclass override has to be able to rely on: `AsyncCommandPool`
  marks the borrowed thread as one of its own there, and the async barrier reads that mark to avoid waiting for the
  command it is itself running. A new override on that hook must therefore be correct for BOTH paths.
- `GraphAlgorithms.awaitFutures(futures, count[, pool])` is the ready-made reclaiming wait, and is what
  `parallelForRange`, `GAVFusedChainOperator` and `PartitionedTriangleOp` use. Prefer it over a bare `Future.get()`
  loop for anything submitted to a `DedicatedThreadPool`.
- "The caller runs chunk 0 itself" (`parallelForRange`, `PartitionedTriangleOp`) is a latency optimisation, not the
  liveness argument. It does not stop chunks 1..N-1 from queueing behind parked workers.
- The other shape that is safe is refusing to nest at all, as `SparseVectorScoringPool.isPoolThread()` does. Use it
  when the nested fan-out has no value; use the reclaiming wait when it does.
- A reclaimed task runs with the CALLER's thread-locals (`DatabaseContext`, transaction, principal). That is the same
  exposure caller-runs already has, so nothing on a caller-runs pool may depend on running on a worker thread.

## When adding new parallelism to engine code

1. Pick the right home pool (`QueryEngineManager` for query-time, `SparseVectorScoringPool` if scoping to sparse vectors, etc.). If none fits, justify a new pool and add a `PoolMetrics` binding for it.
2. Extend `DedicatedThreadPool` rather than building a `ThreadPoolExecutor` by hand. It gives points 2-3 below by construction.
3. Bounded queue + caller-runs rejection so the system degrades to single-threaded under load instead of throwing `RejectedExecutionException` to callers. A deviation (as `ParallelScanProducerPool` has) must be argued in the class javadoc.
4. Throttled WARNING on saturation (60-second window, `WARN_THROTTLE_INTERVAL_MS`) so operators notice without getting spammed.
5. Wire the pool into `PoolMetrics` so the Studio "Executor Pools" card shows it.
6. If using the JDK common ForkJoinPool is unavoidable in the short term, tag the call site with a `NOTE (concurrency)` comment pointing at this skill so the migration stays tracked.
7. If the submitting thread then WAITS for what it submitted, wait with `GraphAlgorithms.awaitFutures` (or reclaim by hand) rather than `Future.get()`. See the #6568 section above - the caller-runs policy is not the guarantee.
