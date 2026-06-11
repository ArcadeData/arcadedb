# Issue #4545 - `PageManager.deleteFile` doesn't coordinate with `flushThread.pageIndex` / queue

## Summary

`PageManager.deleteFile(database, fileId)` evicted only `readCache` entries for the
dropped fileId. The asynchronous flush thread (`PageManagerFlushThread`) still held
`MutablePage`s for that same fileId inside its `pageIndex` (O(1) lookup map) and its
flush `queue`. Consequences:

- **RAM leak** in `pageIndex`: index entries for the dropped file were never removed.
- A page for a just-dropped file could still be picked up by the flush loop. The
  existing `flushPage` existence guard (`fileManager.existsFile`) silently skips the
  disk write, but the stale `MutablePage`/index entry lingered.
- A concurrent `loadPage` for the same fileId could be served the stale queued page via
  `getCachedPageFromMutablePageInQueue`.

## Root cause

`deleteFile` did not touch the flush thread at all. The class already had a per-database
drain (`PageManagerFlushThread.removeAllPagesOfDatabase`, used by `simulateKillOfDatabase`
and `removeModifiedPagesOfDatabase`) but no per-file equivalent for the file-drop path.

## Fix

1. Added `PageManagerFlushThread.removeAllPagesOfFile(Database, int fileId)`, mirroring the
   existing `removeAllPagesOfDatabase`. It removes matching pages from queued batches and
   purges the `pageIndex` of every entry whose `PageId` belongs to that database+fileId
   (covers queued, in-flight, and suspend-deferred pages).
2. `PageManager.deleteFile` now calls `flushThread.removeAllPagesOfFile(database, fileId)`
   before evicting the `readCache`, draining the flush thread in the same operation that
   drops the file (null-guarded for the pre-`configure()` state).
3. Added a package-private `PageManager.getFlushThread()` accessor so the regression test
   can assert flush-thread state.

Files changed:
- `engine/src/main/java/com/arcadedb/engine/PageManager.java`
- `engine/src/main/java/com/arcadedb/engine/PageManagerFlushThread.java`

## Test

`engine/src/test/java/com/arcadedb/engine/PageManagerDeleteFileFlushCoordinationTest.java`

The test suspends async flushing for the database, schedules a batch of `MutablePage`s for
a synthetic fileId (so they park in `pageIndex`/queue without being written to disk),
asserts they are indexed, then calls `deleteFile` and asserts none remain indexed for that
fileId.

TDD verification:
- With the drain call removed, the test fails on assertion
  *"After deleteFile no page for the dropped fileId may remain in the flush thread index"*.
- With the fix, the test passes.

## Regression runs (engine module)

- `PageManagerDeleteFileFlushCoordinationTest` - pass
- `PageManagerFlushQueueRaceTest`, `PageManagerReadCacheRamAccountingTest`,
  `PageManagerStressTest` (skipped/benchmark), `FileManagerTest`, `DropBucketTest` - pass
- `DropIndexTest`, `LSMTreeIndexTest`, `TypeIndexTest` (exercise `deleteFile` on index drop) - pass

## Impact / monitoring

Low risk: the new per-file drain reuses the same proven pattern as the per-database drain
and only touches the file being dropped. No behavioral change for the common path other
than guaranteeing the flush thread no longer retains pages for dropped files.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/4568 (Closes #4545)

## Review cycles

- cycle 1: head `9c4c4da7` - both bots reviewed. claude (issue comment) and gemini (inline,
  high) flagged that `removeAllPagesOfFile` drained only the live `queue`, leaving
  suspend-deferred batches in `deferredByDatabase` (RAM leak window), and that
  `pagesToFlush.pages` needed a null-guard for the `SHUTDOWN_THREAD` marker. Also: fully
  qualified names in the test and multi-line comments. Addressed in `408777186`: extracted a
  shared `removePagesOfFileFromBatch` helper that null-guards and now also drains
  `deferredByDatabase`; replaced FQNs with imports; trimmed comments/Javadoc to single lines.
  Two items skipped with justification (see deferred notes).
- cycle 2: head `408777186` - gemini re-posted the SAME cycle-1 suggestion (it diffs against
  base, not the prior commit); the committed code already implements that exact change, so the
  comment is a stale duplicate with nothing actionable. The `claude` bot did not post a review
  on this SHA within the 15-minute per-iteration window.

## Deferred items

- `docs/review-deferred-9c4c4da7.md` - records the two review points skipped with rationale
  (keep the package-private `getFlushThread()` test accessor rather than a racy `loadPage`
  black-box assertion; empty drained batch left in queue is safe, same as
  `removeAllPagesOfDatabase`).

## Final state

`timeout` - cycle-1 feedback fully addressed and pushed; cycle-2 surfaced only a stale
gemini duplicate (already satisfied) and no `claude` review landed within the polling window.
PR left open for the developer. Merge is the developer's responsibility.
