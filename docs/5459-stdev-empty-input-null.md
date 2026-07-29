# 5459 - `stdev()` / `stdevP()` return `0.0` for empty input instead of `NULL`

Issue: https://github.com/ArcadeData/arcadedb/issues/5459
PR: https://github.com/ArcadeData/arcadedb/pull/5539

## Problem

```cypher
UNWIND [] AS x RETURN stdev(x) AS std, stdevP(x) AS stdp;
```

returned `{"std": 0.0, "stdp": 0.0}` instead of `NULL`, making an input with no observations
indistinguishable from a non-empty input whose dispersion is genuinely zero.

Every sibling aggregate that cannot derive a value from an empty input already returned `NULL`:
`avg()`, `min()`, `max()`, `percentileCont()`, `percentileDisc()`. Neo4j corrected the same
`stDev()` behavior in `2026.05.0`.

## Root cause

`SQLFunctionStandardDeviation.getResult()` (engine/src/main/java/com/arcadedb/function/sql/math/SQLFunctionStandardDeviation.java).

Its base class `SQLFunctionVariance.getResult()` already returns `null` when the observation count
`n == 0`, which is exactly the empty-input signal. The standard-deviation subclass then discarded it:

```java
final Object variance = super.getResult();
if (variance != null)
  return Math.sqrt((Double) variance);
return 0.0;      // <-- swallowed the null
```

The Cypher names route here through `CypherFunctionFactory` (`stdev`/`stdev_samp` -> `stddev`,
`stdevp`/`stdev_pop` -> `stddevp`), and `SQLFunctionStandardDeviationP` extends
`SQLFunctionStandardDeviation`, so all four spellings plus the SQL `stddev()`/`stddevp()` shared the
single defect.

## Fix

Propagate the `null` instead of substituting `0.0`. One-line inversion of the branch; nothing else in
the aggregation path changed.

The `n == 1` case is untouched and still returns `0.0` - a single observation is a real observation
with zero dispersion, which matches Neo4j.

## Behavior change beyond the reported case

The issue scoped itself to `UNWIND []`. The fix also changes all-`NULL` input, e.g.
`UNWIND [null, null] AS x RETURN stdev(x)`, from `0.0` to `NULL`.

This is intentional and is the same defect: `SQLFunctionVariance.addValue()` skips `null` values, so
an all-`NULL` input also has zero observations. It also makes `stdev` consistent with `avg`, `min`,
`max`, `percentileCont` and `percentileDisc`, which already return `NULL` for that identical input in
the very same test class.

### Existing tests modified (deviation from the "never modify existing tests" rule)

Four assertions in `OpenCypherAggregatingFunctionsComprehensiveTest` asserted the buggy `0.0` for
all-`NULL` input and were changed to assert `NULL`, matching how the neighbouring `avgNull`,
`minNull`, `maxNull`, `percentileContAliasNull` and `percentileDiscAliasNull` cases in the same file
are already written:

- `stDevNull`
- `stDevPNull`
- `stdevSampNull`
- `stdevPopNull`

No test was deleted, and no test coverage was lost.

## Changes

| File | Change |
|---|---|
| `engine/src/main/java/com/arcadedb/function/sql/math/SQLFunctionStandardDeviation.java` | Return `null` for zero observations instead of `0.0` |
| `engine/src/test/java/com/arcadedb/query/opencypher/OpenCypherStDevEmptyInputTest.java` | New regression test (7 cases) |
| `engine/src/test/java/com/arcadedb/query/opencypher/functions/OpenCypherAggregatingFunctionsComprehensiveTest.java` | 4 all-`NULL` assertions corrected to `NULL` |

## Verification

Test written first and confirmed failing against unmodified `main`:

```
Tests run: 7, Failures: 3  -- OpenCypherStDevEmptyInputTest
```

The three failures were exactly the empty-input and all-`NULL` cases. The four control cases
(`avg`/`min`/`max`/percentiles over zero rows returning `NULL`, `[5,5]` -> `0.0`, `[5]` -> `0.0`,
`[1,2,3]` -> `1.0`/`0.8165`) passed before the fix, proving the test isolates the defect.

After the fix:

```
mvn -pl engine -Dtest='OpenCypherStDevEmptyInputTest,OpenCypherStDevTest,
  OpenCypherAggregatingFunctionsComprehensiveTest,SQLFunctionVarianceTest,
  SQLFunctionAdditionalCoverageTest' test
  -> Tests run: 115, Failures: 0, Errors: 0

mvn -pl engine -Dtest='com.arcadedb.function.**.*Test,com.arcadedb.query.opencypher.**.*Test' test
  -> Tests run: 4830, Failures: 0, Errors: 0, Skipped: 13
```

## Impact

- Applications can now distinguish "no observations" (`NULL`) from "observations with no variation" (`0.0`).
- Callers that treated the old `0.0` as a sentinel for the empty case will now receive `NULL`. This is
  the intended correction and matches every other ArcadeDB aggregate as well as Neo4j `2026.05.0`+.
- No other module references `stddev`/`stdev`; the change is contained to the engine.

## Review cycles

### Cycle 1 - head `d5cb727`

Initial push: the fix, the new regression test, the four corrected assertions, and this doc.

| Reviewer | Outcome |
|---|---|
| `claude[bot]` | **LGTM.** Verified the inheritance chain covers all four Cypher spellings plus SQL `stddev()`/`stddevp()`, confirmed the `n == 1` path is correctly untouched, and independently confirmed no other test in the tree depends on the old `0.0`. One non-blocking nit about an assertion idiom that is pre-existing local style in the modified file - no action taken, matching the surrounding file was deliberate. |
| `gemini-code-assist` | No response within the polling window. Consistent with this repository's recent history, where gemini has not responded to any PR head. |

No code changes were required. No items were deferred.

## Final state

`clean-approval` on the reviewer that responded, with `gemini-code-assist` timing out.

### CI on `b325309`

Green: `build-and-package`, `builder-tests`, `studio-e2e-tests`, CodeQL (all six languages), Codacy,
and the Go, JS, Java, C# and Python e2e suites.

Three jobs are red, and **all three fail identically on unrelated PRs**, so none is caused by this
change. This change touches only `SQLFunctionStandardDeviation.getResult()`; none of the failing
tests exercises an aggregate function.

| Job | Failing test | Evidence it is pre-existing |
|---|---|---|
| `slow-unit-tests` | `EdgeAppendMergeRaceTest.addersRemoversAndReadersOnOneHotVertex` - `ConcurrentModificationException` on a hot-vertex edge append | Same test, same exception on PR #5537. This is the supernode edge-append page conflict documented in `engine/CLAUDE.md`. |
| `integration-tests` | `Issue4141SessionManagementIT` (2 tests) - HTTP 400 from `/api/v1/command/graph` | Byte-identical failure on PR #5537 and on PR #5536, which is already merged to `main`. |
| `Meterian client scan` | n/a | Dependency-vulnerability scan; no dependency was added or changed. |

`unit-tests` and `ha-integration-tests` had not reported at the time of writing and should be
confirmed before merge.

Merge remains the developer's decision.
