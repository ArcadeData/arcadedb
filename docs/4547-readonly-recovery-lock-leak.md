# Issue #4547 - LocalDatabase leaks the lock file when checkForRecovery rejects a READ_ONLY recovering DB

## Summary

`checkForRecovery()` calls `lockDatabase()` (storing `FileLock` + `lockFileIO*` into
instance fields), then if the mode is `READ_ONLY` throws `DatabaseMetadataException`.
The issue reports that the file lock is leaked because the lock-release block in
`closeInternal()` is never reached (`open` is set `false`, short-circuiting future
`close()` calls).

## Analysis

### Affected component
`engine/src/main/java/com/arcadedb/database/LocalDatabase.java`

### Current state on main (post issue #4511)

Issue #4511 (commit 8ee1ebd7d) already added `releaseResourcesOnOpenFailure()` and
wired it into the OUTER catch of `openInternal()`. That release path closes the
`FileLock`, the lock-file I/O channels, the `FileManager` and the `TransactionManager`
on any open failure, so the `READ_WRITE` recovery-failure leak is already covered.

However, there is a remaining gap for the exact `READ_ONLY` scenario the issue
describes:

1. `checkForRecovery()` is invoked only when `mode == READ_WRITE`
   (`openInternal()` guards the call with `if (mode == ComponentFile.MODE.READ_WRITE)`).
2. Therefore, the `READ_ONLY` rejection branch inside `checkForRecovery()`
   (`throw new DatabaseMetadataException("Database needs recovery but has been open in read only mode")`)
   is currently UNREACHABLE from `openInternal()`.
3. The practical effect on main is that a crashed database (with a `database.lck`
   marker present) opened in `READ_ONLY` mode silently SKIPS recovery and opens
   successfully, never validating that recovery is required. The intended guard is
   never enforced.

### Root cause / expected vs. actual

- Expected: opening a not-cleanly-closed database in `READ_ONLY` mode should refuse
  (it needs recovery, which `READ_ONLY` cannot perform) AND must not leak the OS
  file lock.
- Actual on main: the recovery check is skipped for `READ_ONLY`, so the guard never
  fires. If the guard were reached without the `READ_ONLY` gate (as in the affected
  26.x releases), the lock acquired by `lockDatabase()` would leak.

### Fix

Enforce the recovery guard for `READ_ONLY` databases too, while making sure the file
lock acquired during the check is released on rejection:

1. Always run the lock-file presence detection in `checkForRecovery()` regardless of
   mode, so a `READ_ONLY` open of a crashed database is rejected as intended.
2. Acquire the lock and immediately release it (or rely on the outer-catch
   `releaseResourcesOnOpenFailure()`) so no `FileLock` is leaked when the rejection
   fires.

The minimal, safe change is to detect the recovery-needed condition for `READ_ONLY`
WITHOUT acquiring the exclusive write lock at all (a `READ_ONLY` open must not take a
write lock on `database.lck`), so there is nothing to leak; and to route any failure
after a successful `lockDatabase()` through `releaseResourcesOnOpenFailure()`.

## Tests

`engine/src/test/java/com/arcadedb/database/Issue4547ReadOnlyRecoveryLockLeakTest.java`

## Implementation

`engine/src/main/java/com/arcadedb/database/LocalDatabase.java`:

1. `openInternal()` now calls `checkForRecovery()` for BOTH modes (removed the
   `if (mode == READ_WRITE)` gate), so a `READ_ONLY` open of a crashed database is
   actually validated.
2. `checkForRecovery()` now, when the lock marker exists and `mode == READ_ONLY`,
   rejects with `DatabaseMetadataException` BEFORE calling `lockDatabase()`, and
   nulls `lockFile`. Because the exclusive write lock is never acquired on the
   `READ_ONLY` path, there is no OS file lock to leak by construction. The marker is
   left on disk so the next `READ_WRITE` open still recovers.
3. The outer catch in `openInternal()` now re-throws `DatabaseMetadataException`
   unwrapped (alongside the existing `DatabaseOperationException` passthrough), so the
   caller sees the "needs recovery" reason instead of an opaque wrapped message.
   The issue #4511 `releaseResourcesOnOpenFailure()` safety net remains in place.

## Test results

- New test `Issue4547ReadOnlyRecoveryLockLeakTest` (red before fix, green after).
- Regression run (all green): `Issue4511OpenFailureLockLeakTest`, `TransactionTypeTest`,
  `TransactionBucketTest`, `ExternalPropertyTest`, `PolymorphicIndexConflictTest`,
  `FileManagerTest`, `DatabaseFactoryTest`, `ACIDTransactionTest`,
  `Issue4508TornWALRecoveryTest`, `WALVersionGapRecoveryTest`, `CheckDatabaseTest`,
  `TransactionCallbackTest`.

## Pull Request

https://github.com/ArcadeData/arcadedb/pull/4569

## Review cycles

- Cycle 1 (head `e019c234`):
  - `gemini-code-assist`: COMMENTED, no actionable feedback.
  - `claude`: approved correctness/fix/test (posted as a PR issue comment, not a
    SHA-tied review, so the strict gating loop timed out waiting for it). Two
    suggestions:
    - Remove the tracking doc - DISAGREED with justification (committed tracking doc is
      the workflow convention; precedents #4538/#4545/#4546/#4515/#4514). See
      `review-deferred-e019c234.md`.
    - Flatten the test-class Javadoc - APPLIED.

## Deferred items

- `docs/review-deferred-e019c234.md` - records the disagree-with-justification for the
  tracking-doc removal and the applied Javadoc nit.

## Final state

`timeout` (the `claude` reviewer did not post a head-SHA-tied review within the 15-minute
gate; it posted an equivalent PR issue comment, whose actionable nit was applied). PR left
open for the developer to merge.

## Status

- [x] Analysis
- [x] Test (TDD)
- [x] Fix
- [x] Verification
