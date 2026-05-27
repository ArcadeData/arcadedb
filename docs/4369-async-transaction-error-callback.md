# Fix #4369: `DatabaseAsyncTransaction` silently swallows final-retry failure

## Issue

After retries are exhausted in `DatabaseAsyncTransaction.execute()`, only the global `onError`
listener was notified. The per-task `onErrorCallback` was never called, and no exception was
re-thrown - so the async worker considered the task "complete" and never rolled back the
still-active transaction.

## Root Cause

`DatabaseAsyncTransaction.java` lines 81-83 (before fix):

```java
if (lastException != null)
  async.onError(lastException);
// missing: no rollback, no onErrorCallback, no re-throw
```

The non-`ConcurrentModificationException` catch block (lines 68-78) correctly calls rollback,
notifies the per-task `onErrorCallback`, and re-throws. The retry-exhausted path did none of
these.

## Fix

Mirror the non-CME exception handling: rollback the transaction if still active, call the
per-task `onErrorCallback` if non-null, and re-throw `lastException` so the `AsyncThread`
worker loop also sees the failure and can clean up stale state.

## Files Changed

- `engine/src/main/java/com/arcadedb/database/async/DatabaseAsyncTransaction.java`
- `engine/src/test/java/com/arcadedb/database/async/DatabaseAsyncTransactionRetryExhaustedTest.java` (new)

## Verification

1. New regression test `DatabaseAsyncTransactionRetryExhaustedTest` verifies:
   - Per-task `onErrorCallback` is invoked with the `ConcurrentModificationException`
   - Global `onError` listener is also invoked (existing behavior preserved)
   - The active transaction is rolled back after failure
2. Existing `AsyncTest` suite passes (no regressions).

## Test Results

- `DatabaseAsyncTransactionRetryExhaustedTest`: 1/1 pass
- `AsyncTest`: 11/11 pass
- `DatabaseAsyncExecutorLifecycleRaceTest`: 1/1 pass
