# Fix #4370 - DatabaseAsync{Create,Update,Delete}Record rollback on exception

## Issue

`DatabaseAsyncCreateRecord`, `DatabaseAsyncUpdateRecord`, and `DatabaseAsyncDeleteRecord`
swallow exceptions without rolling back the shared transaction, contaminating subsequent
tasks in the same async batch.

**Affected files:**
- `engine/src/main/java/com/arcadedb/database/async/DatabaseAsyncCreateRecord.java`
- `engine/src/main/java/com/arcadedb/database/async/DatabaseAsyncUpdateRecord.java`
- `engine/src/main/java/com/arcadedb/database/async/DatabaseAsyncDeleteRecord.java`

## Root Cause

`AsyncThread.run()` in `DatabaseAsyncExecutorImpl` reuses a single long-lived transaction
across batched tasks (periodic commit every N records). Each task's `execute()` method has
its own `catch (Exception e)` block that:

1. Logs the error
2. Calls `async.onError(e)` (global error notification)
3. Calls the per-task `onErrorCallback`
4. **Does NOT roll back the transaction**
5. **Does NOT re-throw**

Because the exception is swallowed, the outer `catch (Throwable e)` in `AsyncThread.run()`
(which DOES call `database.rollback()`) never fires. The transaction retains its dirty
pages and `updatedRecords` map, and subsequent tasks operate on the contaminated state,
leading to silent data corruption or lost writes under load.

## Fix

Added `if (database.isTransactionActive()) database.rollback();` to the catch block of each
of the three task classes, before the error callbacks. This ensures:
- The transaction is cleaned up immediately when a record-level op fails
- Error callbacks are still invoked (no change to notification semantics)
- The outer `AsyncThread` loop starts the next message with a fresh transaction
  (via the `requiresActiveTx()` + `database.begin()` check)

## Regression Tests

`DatabaseAsyncRecordRollbackOnExceptionTest` - three test methods covering:
1. `createRecordExceptionRollsBackTransaction` - injects a `RuntimeException` via
   `AfterRecordCreateListener` on the first create; verifies only the 2 subsequent
   creates are persisted (3 submitted, 1 rolled back, 2 committed)
2. `updateRecordExceptionRollsBackTransaction` - injects an exception via
   `AfterRecordUpdateListener`; verifies the dirty update is NOT persisted (v stays 0)
3. `deleteRecordExceptionRollsBackTransaction` - injects an exception via
   `AfterRecordDeleteListener`; verifies the record is NOT deleted and total count is 2

All three tests use `setCommitEvery(1)` to expose the bug on the very first task.

## Test Results

```
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Regression suite (`ACIDTransactionTest` alone): 11/11 passing.

## Status

- [x] Analysis complete
- [x] Tracking doc created
- [x] Tests written (`DatabaseAsyncRecordRollbackOnExceptionTest`)
- [x] Implementation complete (rollback added to all three catch blocks)
- [x] Tests passing (3/3)
- [ ] PR opened
