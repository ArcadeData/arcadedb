---
name: engine-concurrency
description: Use when adding, reviewing, or debugging parallelism in ArcadeDB engine or server code - covers the dedicated thread pool inventory and sizing, saturation policy, lock-free read patterns, locking rules, Micrometer pool metrics, and the checklist a new pool must satisfy. Also use when deciding where to submit forked work, or when investigating pool saturation (caller_run_fallbacks).
---

# Engine Concurrency and Parallelism

**Core principle:** ArcadeDB avoids the JDK common ForkJoinPool (`ForkJoinPool.commonPool()`) for engine-internal parallelism. The common pool is shared with user-supplied code (Gremlin, Polyglot, custom SQL functions, application JVM) and JDK internals (parallel GC, reference handler), so long-running engine work submitted there starves user code and JDK housekeeping. Engine code that needs parallelism submits to one of the dedicated pools below; the rule is documented at the head of `com.arcadedb.query.QueryEngineManager`'s class Javadoc.

**Existing JDK-common-pool callers** (tagged in source with `NOTE (concurrency)` comments referencing the rule, will migrate as workloads justify it):
- `GraphBatch.parallelSort` (`engine/...graph/GraphBatch.java`)
- `ArcadeStateMachine.notifyInstallSnapshotFromLeader` (`ha-raft/...`)

## Dedicated thread pools

| Pool | Module | Purpose | Sizing | Saturation policy |
|---|---|---|---|---|
| `QueryEngineManager` JVM-wide pool | `engine` | Query-time parallelism: graph algorithms (`parallelForRange`), parallel index scans, anything that forks query work | `arcadedb.queryParallelismPoolThreads` (default `max(2, CPU)`) | Bounded queue (`arcadedb.queryParallelismQueueSize`, default 1024), caller-runs rejection, throttled WARNING (60s window) |
| `SparseVectorScoringPool` | `engine` | Reserved for per-segment parallel scoring of `LSM_SPARSE_VECTOR` top-K (dispatch wiring deferred to issue #4085) | Hardcoded `max(2, CPU)` threads, 1024 queue; lazy-init via Holder idiom | Bounded queue, caller-runs, throttled WARNING |
| `DatabaseAsyncExecutor` | `engine` | Per-database async ops (background scheduled tasks, async commit) | `arcadedb.asyncWorkerThreads` (default `CPU - 1`, min 1) | Bounded queue (`arcadedb.asyncOperationsQueueSize`, default 1024) |
| `PageManagerFlushThread` | `engine` | Dedicated single thread for paginated-component page writes | 1 thread | Backpressure via `arcadedb.maxRAMForPageRamUsageInMB` |
| `TransactionManager` Timer | `engine` | Periodic WAL housekeeping | Timer thread | n/a |
| `TimeSeriesEngine` pool | `engine` | Time-series rollup work | configurable | n/a |
| `BackupScheduler` | `server` | Cron-style backup jobs | scheduled executor | n/a |
| `MaterializedViewScheduler` | `server` | Cron-style MV refresh | scheduled executor | n/a |
| Raft HA pools | `ha-raft` | Leader election, log replication, snapshot install | configurable in Raft conf | n/a |
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

- `PoolMetrics` MeterBinder registers six gauges per pool with a `pool=<name>` tag: `size`, `active`, `queue_depth`, `queue_capacity_remaining`, `completed_tasks`, `caller_run_fallbacks`.
- Studio surfaces these via the "Executor Pools" card on the Server tab (live numbers).
- A sustained non-zero `caller_run_fallbacks` indicates pool saturation; the engine logs a throttled WARNING the first time and at most once per 60 s afterward.

## When adding new parallelism to engine code

1. Pick the right home pool (`QueryEngineManager` for query-time, `SparseVectorScoringPool` if scoping to sparse vectors, etc.). If none fits, justify a new pool and add a `PoolMetrics` binding for it.
2. Bounded queue + caller-runs rejection so the system degrades to single-threaded under load instead of throwing `RejectedExecutionException` to callers.
3. Throttled WARNING on saturation (60-second window matching the existing pools) so operators notice without getting spammed.
4. Wire the pool into `PoolMetrics` so the Studio "Executor Pools" card shows it.
5. If using the JDK common ForkJoinPool is unavoidable in the short term, tag the call site with a `NOTE (concurrency)` comment pointing at this skill so the migration stays tracked.
