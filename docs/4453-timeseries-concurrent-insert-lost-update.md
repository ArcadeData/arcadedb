# Issue #4453 - Time series: concurrent single-row SQL INSERTs silently lose samples

## Summary

Under many concurrent single-row `INSERT INTO ... SET ts=..., ...` statements
(each in its own transaction), the time series engine silently drops samples.
Clients receive HTTP 200 on every insert, yet the stored row count comes up short.

## Root Cause

In `TimeSeriesShard.appendSamples`, the `appendLock` serializes the
begin->write->commit cycle. However, the page cache is not updated
in a way that is visible to the next serialized append before `appendLock`
is released.

Two concurrent appends both read the same stale `DATA_SAMPLE_COUNT_OFFSET`
value and compute the same `rowOffset`, so the later commit overwrites the
earlier one. `PageManager.updatePageVersion` does not detect the collision
because both outer transactions have the same-versioned dirty pages.

Collision detector output (48-thread run):
```
DIAG COLLISION slot=1:1:0 pageVer=239 prevThread=74 curThread=88
DIAG COLLISION slot=1:1:1 pageVer=240 prevThread=68 curThread=87
```

## Fix

Add per-shard in-memory write tracking (`appendTrackDataPageNum`,
`appendTrackSampleCount`) protected by `appendLock`. The bucket's
`appendSamples` uses this authoritative in-memory position instead
of reading `DATA_SAMPLE_COUNT_OFFSET` from a potentially stale page.

On page overflow, the tracker advances to the next page.
On compaction clear, the tracker is reset to -1 (re-initialized from page on next append).
On append failure, the tracker is reset to -1 for safety.

This approach requires no changes to the transaction or page-cache layer,
and works correctly for HA because the page writes still go through the
nested transaction on `getWrappedDatabaseInstance()`.

## Root Cause (confirmed)

`TimeSeriesEngine.appendSamples` called `TimeSeriesShard.appendSamples` directly
on the CALLER'S thread.  When the caller is inside an enclosing transaction
(e.g. an HTTP command transaction), `LocalDatabase.begin()` pushes a NESTED
`TransactionContext` rather than a fresh level-1 transaction.  The nested commit
defers the page-cache update to the enclosing (outer) transaction's commit, which
occurs OUTSIDE `appendLock`.  A following serialized append therefore reads a stale
`DATA_SAMPLE_COUNT_OFFSET` (or stale `HEADER_DATA_PAGE_COUNT`) and writes to the
same slot, silently overwriting the preceding sample.  `PageManager.updatePageVersion`
does not raise a conflict because the outer transactions each have the modified pages
at the same (stale) base version and their outer commits are no-ops.

A second, independent data-loss path was found during investigation: the
background `TimeSeriesMaintenanceScheduler` fires compaction.  In Phase 4a,
compaction read the last partial data page (phase4aPageCount) as a snapshot under
the writeLock.  In Phase 4b (lock-free), new samples were appended to that same
page.  Phase 4c only read pages AFTER phase4aPageCount, so the Phase-4b appends
to page phase4aPageCount were lost when clearDataPages() reset the logical count.

## Fix

**Fix 1 (slot collision):** `TimeSeriesEngine.appendSamples` now dispatches the
shard write via `CompletableFuture.runAsync(..., shardExecutor).join()`.  The shard
executor thread has no enclosing transaction, so `db.begin()` inside
`TimeSeriesShard.appendSamples` always creates a fresh level-1 transaction that
commits independently to the page cache before `appendLock.unlock()`.  This matches
the existing `appendBatch` path.

**Fix 2 (compaction Phase 4):** Phase 4a no longer reads the last partial page
(`phase4aPageCount`).  Phase 4c, running under the writeLock, re-reads
`phase4aPageCount` (with its fully up-to-date sample count including any appends
during Phase 4b) plus any new pages.

## Affected Files

- `engine/src/main/java/com/arcadedb/engine/timeseries/TimeSeriesEngine.java`
- `engine/src/main/java/com/arcadedb/engine/timeseries/TimeSeriesShard.java`
- `engine/src/test/java/com/arcadedb/engine/timeseries/TimeSeriesConcurrentInsertTest.java`

## Test

`TimeSeriesConcurrentInsertTest.concurrentSingleRowInsertsDoNotLoseSamples`
(48 threads x 5000 inserts with SHARDS=1) - was @Disabled, now enabled.
Passes reliably across multiple runs.
