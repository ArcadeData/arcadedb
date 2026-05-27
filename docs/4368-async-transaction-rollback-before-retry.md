# Fix #4368: DatabaseAsyncTransaction does not rollback before retrying after ConcurrentModificationException

## Issue

[#4368](https://github.com/ArcadeData/arcadedb/issues/4368) - When `tx.execute()` throws
`ConcurrentModificationException`, the transaction is still active. On the next retry,
`LocalDatabase.begin()` detects the active transaction and creates a nested sub-transaction
instead of a fresh one. Any partial writes made by the failed attempt remain in the outer
(uncommitted) transaction. That outer transaction is later committed - for example when
`waitCompletion()` sends a `DatabaseAsyncCompletion` semaphore task that calls
`database.commit()` on any active transaction on the thread - causing partial or duplicate
records to be persisted.

## Root Cause

In `DatabaseAsyncTransaction.execute()`, the CME catch block only stored the exception and
continued to the next loop iteration without calling `database.rollback()`. Because
`tx.execute()` may have partially modified data inside the active transaction, that
transaction stays open with uncommitted changes. When the retry calls `database.begin()`,
`LocalDatabase.begin()` pushes the dirty transaction onto the nested-tx stack and starts a
sub-transaction with a fresh view. The retry's successful commit only commits the
sub-transaction; the outer transaction carrying the partial writes is still active and will
be committed later, producing extra records in the database.

## Affected File

- `engine/src/main/java/com/arcadedb/database/async/DatabaseAsyncTransaction.java` (line 64-66)

## Fix

Add `if (database.isTransactionActive()) database.rollback();` in the CME catch block,
before the loop continues to the next retry. This mirrors what the generic Exception handler
already does at line 69-70.

## Test

New test: `engine/src/test/java/com/arcadedb/database/async/DatabaseAsyncTransactionRetryTest.java`

- `asyncTransactionRetriesAfterConcurrentModificationException`: TransactionScope throws CME on
  the first attempt, succeeds on the second. Verifies okCallback is called and no error is
  reported.

## Verification

```
cd engine && mvn test -Dtest=DatabaseAsyncTransactionRetryTest
```

## Test results

- `DatabaseAsyncTransactionRetryTest`: PASS (1 test)
- All async tests (`DatabaseAsync*`, `AsyncTest`): PASS (13 tests)
- Transaction tests (`*Transaction*`): PASS (62/63 tests - 1 pre-existing failure in `ACIDTransactionTest.asyncIOExceptionAfterWALIsWrittenManyRecords` that also fails on main)

## Status

Implementation complete, tests passing.
