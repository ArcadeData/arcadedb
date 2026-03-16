# Fix: SQLScript QueryNotIdempotentException (#3664)

## Problem

A SQLScript composed only of `LET` (with `SELECT` sub-queries) and `RETURN` statements throws
`QueryNotIdempotentException` when submitted via the `/query` API endpoint, even though the
script is entirely read-only.  The same script works in ArcadeDB Studio because Studio uses
`/command`, which skips the idempotency check.

## Root Cause

`SQLScriptQueryEngine.query()` iterates every top-level statement and calls
`statement.isIdempotent()`.  Both `LetStatement` and `ReturnStatement` inherited the default
`Statement.isIdempotent()` which unconditionally returns `false`, causing any script that
contains a `LET` or `RETURN` to be rejected as non-idempotent regardless of its actual content.

## Fix

**`LetStatement.java`** — override `isIdempotent()`:
- If the `LET` binds a sub-statement (`LET $x = (SELECT ...)`), delegate to that statement's
  own idempotency.
- If the `LET` binds an expression (literal, variable reference, method call), return `true`
  because expressions never write data.

**`ReturnStatement.java`** — override `isIdempotent()` to return `true` unconditionally:
`RETURN` only reads and outputs already-computed values; it never modifies the database.

## Files Changed

| File | Change |
|------|--------|
| `engine/src/main/java/com/arcadedb/query/sql/parser/LetStatement.java` | Added `isIdempotent()` override |
| `engine/src/main/java/com/arcadedb/query/sql/parser/ReturnStatement.java` | Added `isIdempotent()` override |
| `engine/src/test/java/com/arcadedb/query/sql/SQLScriptTest.java` | Added two regression tests |

## Tests Added

- `queryScriptWithLetAndReturnIsIdempotent` — verifies a read-only script (`LET` + `SELECT` +
  `RETURN`) succeeds via `database.query("sqlscript", ...)`.
- `queryScriptWithWriteStatementIsNotIdempotent` — verifies that a script containing a bare
  `INSERT` is still rejected with `QueryNotIdempotentException`.

## Verification

```
mvn test -pl engine -Dtest="SQLScriptTest,OperationTypeTest,OperationTypeIntegrationTest,SQLScriptAdditionalCoverageTest,ScriptExecutionTest,BatchScriptTest"
# → 112 tests, 0 failures
```
