# Fix #4368: DatabaseAsyncTransaction does not rollback before retrying after ConcurrentModificationException

## Issue

[#4368](https://github.com/ArcadeData/arcadedb/issues/4368) - When `tx.execute()` throws
`ConcurrentModificationException`, the transaction is still active. The next retry's
`database.begin()` fails with `TransactionException("Transaction already begun")`.

## Root Cause

In `DatabaseAsyncTransaction.execute()`, the CME catch block only stored the exception and
continued to the next loop iteration. It never called `database.rollback()`. Since
`tx.execute()` may have started work inside the active transaction, the transaction remains
open. The next `database.begin()` then throws "Transaction already begun", which is caught
by the generic `Exception` handler and propagated as an error - defeating the entire retry
mechanism.

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
