# Issue #4546 - `PaginatedComponentFile.rename` closes the channel outside any lock visible to PageManager

- Issue: https://github.com/ArcadeData/arcadedb/issues/4546
- Type: bug fix
- Component: `com.arcadedb.engine.PaginatedComponentFile`
- Severity: MEDIUM

## Summary

`rename(newName)` performs `close() -> Files.move(ATOMIC_MOVE) -> open(newPath)` with no
lock that blocks concurrent flush (`PageManager.flushPage -> file.write(page)`) or load
(`file.read`). An in-flight write/read concurrent with a rename (most commonly compaction
renaming a temporary index file into place) throws
`IllegalArgumentException("... is closed")` from the flush thread, leaving the database in
an inconsistent state.

## Root cause

The `channel`/`file` fields in `PaginatedComponentFile` are mutated by `close()`, `rename()`
and `open()` while the I/O methods (`read`, `write`, `force`, `readPage`, `getSize`,
`getTotalPages`, `calculateChecksum`) read them, with no synchronization between the two
groups. `PageManager.concurrentPageAccess` only serializes I/O per `pageId`; it does not
gate against a rename/close that swaps the channel out from under an in-flight operation on
a different page of the same file.

## Fix

Add a per-file `ReentrantReadWriteLock` to `PaginatedComponentFile`:
- I/O operations acquire the shared READ lock so independent pages still proceed
  concurrently (no throughput regression for the hot path).
- `close()` and `rename()` acquire the exclusive WRITE lock so they cannot run while any
  I/O holds the channel, and so I/O cannot start mid-rename. Once `rename()` reopens the
  channel under the write lock, queued I/O resumes against the new channel.

This keeps the coordination local to the file object (no new dependency on PageManager) and
preserves the existing reopen-and-retry fallbacks.

## Tests

`PaginatedComponentFileRenameConcurrencyTest` (engine) - drives concurrent writers/readers
against a real `PaginatedComponentFile` while a rename runs in a loop, asserting no
`IllegalArgumentException("... is closed")` / `NullPointerException` escapes.

## Verification

- New test `PaginatedComponentFileRenameConcurrencyTest` FAILS on the unpatched code with
  `NullPointerException: ... this.channel is null` (read overlapping a rename), and PASSES
  with the fix.
- Existing `PaginatedComponentFileRoundTripTest` (3 tests) still passes.
- Rename / schema / index regression suites pass: `SchemaTest`, `AlterTypeExecutionTest`,
  `AlterTypeAtomicRepartitionTest`, `LSMSparseVectorIndexLifecycleTest`,
  `WALVersionGapRecoveryTest`, `ApplyChangesPartialReplayTest`, `Issue4420TolerantDeleteTest`,
  `Issue4432CorruptVertexDeleteTest`.
- Note: `Issue4510ForceApplyPartialDeltaTest` is a PRE-EXISTING, non-deterministic
  test-isolation flake (it relies on shared static `PageManager` page-version state that
  bleeds across tests in the same fork). Verified it flakes identically on the unmodified
  `main` baseline (2 of 4 runs fail there with zero source changes). It is unrelated to this
  fix and is left untouched per the no-modify-existing-tests rule.

## Status

- [x] Worktree + branch created
- [x] Tracking doc
- [x] Failing regression test (reproduces NPE)
- [x] Fix (per-file ReentrantReadWriteLock)
- [x] Verify (new test passes, no regressions in rename-related suites)
