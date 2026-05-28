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

## PR

https://github.com/ArcadeData/arcadedb/pull/4374

## Review Cycles

### Cycle 1 - SHA `5adc3ed3216323343d406209721c17eece1b7cba`

**Reviewer:** gemini-code-assist (COMMENTED)

**Changes applied:**
- Removed redundant `async.onError(lastException)` call before the re-throw (re-throwing already causes `AsyncThread.run()` to call `onError`, so the explicit call caused double notification)
- Replaced `database.getTransaction().isActive()` with `database.isTransactionActive()` for consistency with line 47 of the same file

### Cycle 2 - SHA `b7e08e6f86fba0342aa34e0eae8eedb3a10cf217`

**Reviewer:** gemini-code-assist (COMMENTED)

**Assessment:** Stale repeat of cycle-1 feedback. The code already matches the suggested snippet. No changes needed.

## Final State

`clean-approval` - all review feedback addressed, working tree clean.
