# Issue #4278 - TransactionContext.lockFilesInOrder silently continues on file migration

## Summary

`TransactionContext.lockFilesInOrder()` silently continues when a locked file has been migrated by
LSM index compaction, while `checkExplicitLocks()` (the explicit-lock path) correctly throws
`ConcurrentModificationException`. This asymmetry allows the default commit path (used by virtually
all real-world writes) to proceed with a stale lock on a dropped file ID, bypassing proper
serialization of the new mutable index file.

## Root cause

In `lockFilesInOrder()`, when a locked file no longer exists:
- If `getMigratedFileId()` returns null → correctly unlocks, rolls back, throws
- If `getMigratedFileId()` returns non-null → falls through silently (the bug)

The new mutable file is never locked during this commit, allowing concurrent transactions to write
to the same index file simultaneously.

## Fix

Minimum patch: mirror the `checkExplicitLocks()` behavior. When a locked file is missing and a
migration mapping exists, unlock files, rollback, emit the same FINE log line, and throw
`ConcurrentModificationException("Error on commit transaction: file '...' has been migrated...")`.
Callers receive a retryable exception (same convention as `DuplicatedKeyException`).

Files changed:
- `engine/src/main/java/com/arcadedb/database/TransactionContext.java` - fix `lockFilesInOrder()`

## Test

`engine/src/test/java/com/arcadedb/index/LockFilesInOrderFileMigrationTest.java`

Test approach:
1. Create type with small-page LSM index
2. Insert records to fill mutable index to >= 2 pages
3. Capture the current mutable file ID
4. Begin a transaction on the main thread and inject the old mutable file ID into modifiedPages
5. Run compaction from a background thread (migrates old → new file, drops old file)
6. Commit the main thread transaction
7. Assert: ConcurrentModificationException is thrown (not silent continuation)

## Impact

- Fixes potential silent data loss / undetected write serialization failure
- Callers using `database.transaction()` are unaffected (the lambda retries on ConcurrentModificationException automatically)
- Direct `begin()/commit()` callers now receive the expected retryable exception

## PR

- https://github.com/ArcadeData/arcadedb/pull/4279
