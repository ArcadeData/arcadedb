# Issue #5347 - NullPointerException when using parameterized JSON with MERGE clause in UPDATE

## Problem

```sql
UPDATE Entity MERGE :payload UPSERT WHERE canonical_id = :id
```

with `payload` bound to a JSON object threw:

```
java.lang.NullPointerException: Cannot invoke "com.arcadedb.query.sql.parser.Json.toMap(...)" because "this.json" is null
```

Inlining the JSON literal in the query text worked.

## Root cause

The SQL grammar accepts `MERGE expression` (`SQLParser.g4`), but `SQLASTBuilder.visitUpdateOperation`
only ever extracted a `Json` literal out of that expression:

- direct `expr.json`, or
- `BaseExpression.expression.json` (map literal).

Any other expression shape - an input parameter (`:payload` / `?`), a LET variable, a sub-query -
silently left `UpdateOperations.json` as `null`. `UpdateExecutionPlanner` then chained
`new UpdateMergeStep(null, ctx)` and `UpdateMergeStep.handleMerge` dereferenced the null `Json`.
So the failure was an unconditional NPE, not a validation error.

## Fix

1. `UpdateOperations` gains an `expression` field (with getter, and wired through
   `copy()`, `equals()`, `hashCode()`, `toString()`, `toJSON()`), used for MERGE when the payload is
   not a JSON literal.
2. `SQLASTBuilder.visitUpdateOperation` now falls through to `ops.expression = expr` instead of
   leaving both fields null.
3. `UpdateMergeStep` gains an `Expression` constructor; `handleMerge` resolves the expression per
   record and accepts `Map`, `Document`, or `Result` payloads, throwing a `CommandExecutionException`
   with a clear message for anything else (instead of an NPE).
4. `UpdateExecutionPlanner` and `MoveVertexExecutionPlanner` pick the right `UpdateMergeStep`
   constructor and raise `CommandExecutionException` if neither payload form is present.

The pre-existing inline-JSON path is untouched: `json != null` still goes through `Json.toMap`.

## Tests

New methods in `engine/src/test/java/com/arcadedb/query/sql/executor/UpdateMergeTest.java`:

- `updateMergeWithNamedParameterMap` - `MERGE :payload` with a map parameter merges into the record.
- `updateMergeWithPositionalParameterMap` - `MERGE ?` with a map parameter.
- `updateMergeWithParameterAndUpsertInsertsNewRecord` - the exact reproduction from the issue,
  `MERGE :payload UPSERT WHERE canonical_id = :id`, inserting a new record (requires a unique index
  on `canonical_id`, as UPSERT does generally).
- `updateMergeWithNonMapParameterFails` - a non-map parameter now yields `CommandExecutionException`
  rather than an NPE.

All four fail on `main` with the NPE and pass with the fix.

## Verification

- `mvn test -Dtest=UpdateMergeTest` in `engine` - 6/6 green.
- `mvn test -Dtest='com.arcadedb.query.sql.**'` - 2259 tests, 0 failures; the 6 errors are
  pre-existing/environmental GraalVM-JavaScript tests (`TriggerSQLTest` JS triggers,
  `SQLVectorHybridSearchBlogPostTest` JS helpers) and one benchmark, all unrelated to this change.

## PR

https://github.com/ArcadeData/arcadedb/pull/5348

## Review cycles

### Cycle 1 - head `5ec4abf`

Reviewers: `gemini-code-assist` (COMMENTED, 1 medium inline), `claude[bot]` (issue comment).

Addressed:

- **`Result` branch leaked metadata** (claude). `Result.toMap()` on an element-backed result returns
  `element.toMap(true)`, injecting `@rid` / `@type` / `@cat`, which the merge loop would have stored on
  the target record. Replaced with `toPropertyMap(Result)`, which iterates `getPropertyNames()` and skips
  `@`-prefixed metadata and `$`-prefixed computed pseudo-properties (`$score`, `$similarity`).
- **Unchecked `Map` cast** (claude). A map with non-String keys previously erased through the cast and blew
  up with a raw `ClassCastException` in the merge loop. `checkStringKeys` now validates and raises
  `CommandExecutionException`, keeping the "expected a map" contract uniform.
- **Defensive null check on `expression`** (gemini). `resolveExpression` now throws
  `CommandExecutionException("Missing payload for UPDATE MERGE")` instead of NPE-ing if both payload fields
  are null.
- **Test coverage gap for the `Document` / `Result` branches** (claude). Added
  `updateMergeWithDocumentParameterDoesNotLeakMetadata`, `updateMergeWithSubQueryDoesNotLeakMetadata`, and
  `updateMergeWithNonStringKeyMapFails`.

The sub-query test surfaced a further gap: a sub-query expression evaluates to a `Collection` of `Result`,
not a bare `Result`, so `UPDATE V MERGE (SELECT ...)` still failed. `resolveExpression` now unwraps a
single-element collection and raises a clear `CommandExecutionException` for zero or many items.

Not addressed:

- claude's note that a `null` payload yields `CommandExecutionException` rather than a no-op. claude itself
  judged the current behavior fine for this fix; changing MERGE null semantics is out of scope for an NPE fix.
- claude's suggestion that `updateMergeWithNonMapParameterFails` also assert the record was not partially
  mutated. The merge loop resolves the whole payload before any `doc.set`, so partial mutation is not
  reachable via that path; claude flagged it as non-essential.

## Impact

Behavior change is limited to `UPDATE ... MERGE <non-JSON-literal>` and
`MOVE VERTEX ... MERGE <non-JSON-literal>`, which previously always threw an NPE.
