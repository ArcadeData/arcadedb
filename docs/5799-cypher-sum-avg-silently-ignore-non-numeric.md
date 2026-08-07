# Issue #5799: `sum()` and `avg()` silently ignore non-numeric inputs

## Root cause

In Cypher:

- `sum(x)` is *not* a Cypher-specific executor. `CypherFunctionFactory` maps it straight to the
  shared SQL aggregate `SQLFunctionSum` (`com.arcadedb.function.sql.math.SQLFunctionSum`) via
  `SQLFunctionBridge`. Its single-argument aggregation path only accumulates when the value
  `instanceof Number`; anything else (STRING, BOOLEAN, ...) falls through both branches of the
  `if`/`else if` and is silently dropped, leaving the accumulator untouched. An all-non-numeric
  input therefore never leaves its default (0), and a mixed input quietly reports the sum of only
  the numeric elements.
- `avg(x)` *is* Cypher-specific (`com.arcadedb.function.agg.CypherAvgFunction`, matching Neo4j's
  "always returns a Double" semantics). It has the identical defect: `execute()` only advances
  `sum`/`count` when `args[0] instanceof Number`, otherwise it just returns `null` without raising
  anything - an all-non-numeric input is indistinguishable from an all-null one.

Both are the same class of bug already fixed once for the numeric math family (`abs()`, `sqrt()`,
...) in issue #5484: a value outside the function's input domain must be a client-facing type
error (HTTP 400), not silently ignored (which used to surface as a misleading 500, or in this case
as a *wrong but plausible* result with no error at all).

## Fix

- `SQLFunctionSum` (shared by SQL's and Cypher's `sum()`): every path that used to silently skip a
  non-numeric, non-null value now throws `IllegalArgumentException` - the existing convention for
  a bad-input SQL function argument (see `SQLFunctionAbsoluteValue`), which the HTTP layer already
  maps to 400. This covers the single-argument aggregate path, the `MultiValue` (list-typed
  argument) path, and the multi-argument per-row path (which previously threw an undecorated
  `ClassCastException` instead of silently ignoring, but still not a friendly client error).
- `CypherAvgFunction` (Cypher-only `avg()`): now uses
  `CypherFunctionHelper.requireNumberArgument()`, the same helper the #5484 numeric family uses,
  which throws `CommandSemanticException` (a `CommandParsingException`, mapped to HTTP 400) with a
  message naming the Cypher type actually received (e.g. `STRING`, `BOOLEAN`).

Both null values and empty input remain unaffected (per the issue, that behavior was already
correct): `sum()` over nulls/empty still returns `0`, `avg()` over nulls/empty still returns
`null`.

### Why two different exception types

`SQLFunctionSum` is genuinely shared with plain SQL (`SELECT sum(...)`), so it follows the
existing SQL-function convention (`IllegalArgumentException`) rather than pulling in the
Cypher-only `CommandSemanticException`/`CypherFunctionHelper`. `CypherAvgFunction` has no SQL
counterpart reachable from Cypher, so it follows the Cypher convention directly. Both map to HTTP
400 at `AbstractServerHttpHandler`, so the externally observable behavior is the same.

### Why `SQLFunctionSum`'s arity was not narrowed to match Cypher

A tempting alternative was to give Cypher its own `CypherSumFunction` (mirroring `CypherAvgFunction`),
which would also naturally reject non-numeric input. This was rejected: `CypherFunctionArityRegistryTest`
pins `sum` in `NARROWER_IN_CYPHER` specifically *because* the shared `SQLFunctionSum` executor is
variadic (`sum(a,b,c)`) while the Cypher parser only allows the single-argument aggregation - and
that test asserts the pin still bites. Introducing a dedicated single-arg-only Cypher executor
would make the parser and executor arities match exactly, breaking that existing regression test
for an unrelated reason. Fixing `SQLFunctionSum` in place keeps its arity (and therefore that
test) untouched.

## Tests added

`engine/src/test/java/com/arcadedb/query/opencypher/functions/OpenCypherAggregatingFunctionsComprehensiveTest.java`:

- `avgRejectsNonNumericStrings`, `avgRejectsBoolean`, `avgRejectsNonNumericListLiteral`
- `sumRejectsNonNumericStrings`, `sumRejectsBoolean`, `sumRejectsMixedNumericAndNonNumeric`,
  `sumRejectsNonNumericListLiteral`

`engine/src/test/java/com/arcadedb/function/sql/math/SQLFunctionSumAverageMultiArgTest.java`:

- `sumMultiArgRejectsNonNumeric` (direct unit test against `SQLFunctionSum`, covering the
  multi-argument per-row path without going through the query engine)

## Verification

1. Wrote the regression tests first, confirmed all 7 new Cypher-level tests failed against the
   unmodified code (`avgRejectsBoolean`, `avgRejectsNonNumericListLiteral`,
   `avgRejectsNonNumericStrings`, `sumRejectsBoolean`, `sumRejectsMixedNumericAndNonNumeric`,
   `sumRejectsNonNumericListLiteral`, `sumRejectsNonNumericStrings`) - the last one failed by
   throwing a bare `ClassCastException` instead of no exception, confirming the direct-list-literal
   shape was already loud but ugly, while the rest silently returned instead of throwing.
2. Implemented the fix in `SQLFunctionSum` and `CypherAvgFunction`.
3. `mvn -pl engine -am test -Dtest=OpenCypherAggregatingFunctionsComprehensiveTest,SQLFunctionSumAverageMultiArgTest` - all pass (new + the pre-existing accumulator/nulls/multi-arg tests).
4. `mvn -pl engine test -Dtest=CypherFunctionArityRegistryTest,CypherNumericFunctionArgumentIssue5484Test,CypherFunctionFactoryTest,CypherFunctionFactoryExtendedTest` - all pass, confirming the `sum` arity pin in `CypherFunctionArityRegistryTest.NARROWER_IN_CYPHER` still holds (untouched by design, see above) and the #5484 numeric-family tests are unaffected.
5. `mvn -pl engine test -Dtest='com.arcadedb.query.opencypher.**,com.arcadedb.function.**'` - 4415 tests, 0 failures, 0 errors.
6. `mvn -pl engine test -Dtest='com.arcadedb.query.sql.**'` - 404 tests, 0 failures, 0 errors (confirms plain SQL `sum()`/`avg()` callers are unaffected beyond the intended behavior change).
7. Searched the repo for other callers of `SQLFunctionSum`/`SQLFunctionAverage`/`CypherAvgFunction` and for `sum(`/`avg(` usages with string literals across wire-protocol test modules (postgresw, mongodbw, graphql, bolt) - none found outside `engine/`.
