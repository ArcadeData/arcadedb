# Issue #5789: DISTINCT and UNION treat `1` and `1.0` as distinct

## Root cause

ArcadeDB's Cypher `=` operator correctly evaluates `1 = 1.0` as `true` (see
`ComparisonExpression.evaluateTernary`, which compares `Number` operands via `longValue()` when
both sides are `Integer`/`Long`, and via `doubleValue()` otherwise). But every duplicate-elimination
path built its hash key from the raw boxed value instead of a value consistent with that equality:

- `ProjectReturnStep` (`RETURN DISTINCT`) built a `String` key via `StringBuilder.append(Object)`,
  which calls `Object.toString()`. `Integer.valueOf(1).toString()` is `"1"`, `Double.valueOf(1.0).toString()`
  is `"1.0"` - different strings, so the two rows were kept as distinct.
- `UnionStep` (`UNION`) built the same kind of `String` key, with the same bug.
- `DistinctAggregationWrapper` (`count(DISTINCT ...)`, `min/max/avg(DISTINCT ...)`, and any
  SQL-bridged aggregate function used with `DISTINCT`) tracked seen argument tuples in a
  `HashSet<List<Object>>`. `Integer.valueOf(1).equals(Double.valueOf(1.0))` is `false` in Java (boxed
  numeric types never compare equal across type), so the two values were never recognized as
  duplicates.
- `CollectDistinctFunction` (`collect(DISTINCT ...)`) had the identical `HashSet<Object>` bug.

## Fix

Added a small canonicalization utility, `com.arcadedb.function.DistinctNumericKey`, that maps any
`Number` to a canonical `Long` (when it represents a finite integer within the range a `double`
represents integers exactly, i.e. `|value| <= 2^53`) or canonical `Double` otherwise. This mirrors
`ComparisonExpression`'s numeric-equality rule closely enough that `Integer`, `Long`, `Float`,
`Double`, and `BigDecimal` values representing the same number all canonicalize to the same key,
while non-numeric values pass through unchanged.

Applied it at all four duplicate-elimination sites:

- `ProjectReturnStep`: the DISTINCT key builder now canonicalizes each returned value before
  appending it to the composite string key.
- `UnionStep`: same change to `buildResultKey`.
- `DistinctAggregationWrapper`: the seen-arguments key is now built from canonicalized arguments;
  the wrapped aggregation function still executes on the original, uncanonicalized arguments.
- `CollectDistinctFunction`: numeric values are now wrapped in a new
  `com.arcadedb.function.DistinctNumberWrapper` (analogous to the existing `IdentifiableWrapper`
  used for RID-based dedup of graph elements) that hashes/compares on the canonical numeric key
  while preserving the original boxed value for the output list, so `collect(DISTINCT [1, 1.0])`
  returns a single value of whichever boxed type was encountered first.

### Scope note: `EagerDistinctCollectOptionalMatchStep`

This class implements the identical `collect(DISTINCT ...)` deduplication pattern (a raw
`HashSet<Object>` per variable) for an OPTIONAL MATCH optimization, but a repo-wide search found
no call site constructing it anywhere in `src/main/java` - it is currently dead code, unreachable
from the planner. Left unchanged since no test can exercise it and touching unreachable code adds
risk without verifiable benefit; flagged here in case a future planner change wires it up, since it
would need the same fix.

**Update (#6336):** the class was deleted. It had been dead since the commit that introduced it - the
`collect(DISTINCT ...)` optimization it was written for landed through `CypherFunctionFactory` and
`AggregationStep` instead - so there is no longer a second copy of this pattern to keep in step.

## Tests

New file: `engine/src/test/java/com/arcadedb/query/opencypher/Issue5789NumericDistinctEqualityTest.java`

Covers, on an UNWIND-generated `[1, 1.0]` (and `[1, 2.0]` as a negative control):

- `UNION` collapses `1` and `1.0` to one row; `UNION ALL` still keeps both; same-type `UNION` still
  dedupes.
- `RETURN DISTINCT` collapses `1`/`1.0` to one row, still keeps numerically-different values apart.
- `count(DISTINCT x)` returns `1` (was `2`).
- `collect(DISTINCT x)` returns a single-element list (was `[1, 1.0]`).
- `min(DISTINCT x)`, `max(DISTINCT x)`, `avg(DISTINCT x)` (shared `DistinctAggregationWrapper` path)
  treat `1` and `1.0` as one value when computing the aggregate.
- Sanity check that `1 = 1.0` still evaluates to `true` (unchanged baseline).

TDD: all 5 tests that assert the fixed behavior were confirmed to fail against the pre-fix code
(reproducing the bug 1:1 with the issue's reported symptoms), then pass after the fix.

## Test results

- New regression test class: 9/9 passing.
- Pre-existing DISTINCT/UNION/aggregation-adjacent test classes re-run for regressions (27 classes,
  252 total test methods including `@Nested` classes): 0 failures, 0 errors.

## Impact analysis

The change only affects the *hash key* used for duplicate elimination; it never changes what value
is returned to the caller (the original boxed value is always preserved/returned). Risk is limited
to the four call sites touched, all scoped to the Cypher engine's optimizer execution path
(`com.arcadedb.query.opencypher.executor.steps`) and its Cypher-only aggregation function package
(`com.arcadedb.function.agg`); neither is used by the SQL engine.

## Recommendations

- `EagerDistinctCollectOptionalMatchStep` was deleted by #6336 (dead since it was written), so the
  second copy of this pattern no longer exists.
