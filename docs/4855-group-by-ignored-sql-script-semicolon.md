# Issue #4855: GROUP BY ignored in SQL Script when appending semicolon

https://github.com/ArcadeData/arcadedb/issues/4855

## Symptom

In ArcadeDB Studio's "SQL Script" mode, running:

```sql
SELECT tag FROM tags GROUP BY tag
```

groups correctly. Appending a trailing semicolon:

```sql
SELECT tag FROM tags GROUP BY tag;
```

silently drops the GROUP BY and returns ungrouped, duplicated rows. Wrapping the
same query in `LET a = ...; RETURN $a` was reported as working.

## Root cause

The bug has nothing to do with the semicolon itself, the ANTLR grammar, or script
statement splitting - the AST built for both variants is identical, since the
grammar already treats a trailing `;` as optional statement separator
(`(scriptStatement SEMICOLON?)* EOF`, `SQLParser.g4`).

The real bug is in the SQL **execution plan cache**:

- `SelectExecutionPlanner.createExecutionPlan` (`engine/src/main/java/com/arcadedb/query/sql/executor/SelectExecutionPlanner.java:214-286`)
  builds a `SelectExecutionPlan` and, once built, calls
  `db.getExecutionPlanCache().put(statement.getOriginalStatement(), selectExecutionPlan)`
  to cache it for reuse.
- `ExecutionPlanCache.put()` (`engine/src/main/java/com/arcadedb/query/sql/parser/ExecutionPlanCache.java:94-100`)
  stores `internal.copy(null)`, i.e. a **copy** of the plan, not the plan itself.
  `ExecutionPlanCache.get()` (lines 80-92) likewise returns `result.copy(context)` on
  a cache hit.
- The cache key, `Statement.getOriginalStatement()` (`engine/src/main/java/com/arcadedb/query/sql/parser/Statement.java:156-160`),
  is `originalStatement.toString()` - the **AST's string representation**, not the
  raw source text. Since the AST for `... GROUP BY tag` and `... GROUP BY tag;` is
  identical, both variants collide on the same cache key.
- `AggregateProjectionCalculationStep` (the execution step that implements GROUP BY
  and other aggregations, `engine/src/main/java/com/arcadedb/query/sql/executor/AggregateProjectionCalculationStep.java`)
  extends `ProjectionCalculationStep` but never overrode `copy(CommandContext)`. It
  therefore inherited `ProjectionCalculationStep.copy()`
  (`engine/src/main/java/com/arcadedb/query/sql/executor/ProjectionCalculationStep.java:91-93`):

  ```java
  public ExecutionStep copy(final CommandContext context) {
      return new ProjectionCalculationStep(projection.copy(), context);
  }
  ```

  which reconstructs a **plain, non-aggregating** `ProjectionCalculationStep`,
  silently discarding the `groupBy`, `limit`, `timeoutMillis` and all grouping
  behavior.

Putting it together: the first time a GROUP BY query runs, the freshly built
(correct) plan is used directly for execution, so the first run looks fine. But
the moment it's cached, `put()` copies it through the lossy `copy()` and stores
the corrupted, non-grouping version. Any later execution whose AST normalizes to
the same cache key (with or without a trailing `;`, different casing, etc.) hits
`get()`, receives a copy of the corrupted plan, and silently loses GROUP BY - this
matches the reported "works without `;`, breaks with `;`" sequence, since the
Studio "SQL Script" workflow encourages running the same query repeatedly while
tweaking it.

## Fix

Added an explicit `copy(CommandContext)` override to
`AggregateProjectionCalculationStep` that reconstructs a proper
`AggregateProjectionCalculationStep`, preserving `projection`, `groupBy`
(via the existing `GroupBy.copy()`), `limit`, and `timeoutMillis`:

```java
@Override
public ExecutionStep copy(final CommandContext context) {
  return new AggregateProjectionCalculationStep(projection.copy(), groupBy == null ? null : groupBy.copy(), limit, context, timeoutMillis);
}
```

This is a minimal, targeted fix: it does not touch the cache key logic, the
grammar, or script execution - it fixes the one place where a step's `copy()`
silently downgraded its own type.

## Tests

Added two regression tests to
`engine/src/test/java/com/arcadedb/query/sql/executor/GroupByExecutionTest.java`
(existing tests untouched):

- `groupByRepeatedExecutionUsesCachedPlanCorrectly` - runs the identical
  `SELECT tag FROM Tags GROUP BY tag` query three times via `database.query`,
  asserting each run returns exactly the 2 distinct grouped tags. Fails on the
  2nd/3rd run without the fix (cache hit returns corrupted plan).
- `groupBySqlScriptWithTrailingSemicolonIsNotIgnored` - reproduces the exact
  issue scenario: runs the query once via `sqlscript` without a trailing `;`,
  then again via `sqlscript` with a trailing `;`, asserting the second run is
  still grouped.

Both tests were confirmed to fail before the fix (`expected: 2 but was: 3`,
duplicated ungrouped rows) and pass after it.

## Test results

- `GroupByExecutionTest` (3 tests, including the 2 new ones): PASS
- `GroupByStepTest`, `ExecutionPlanCacheTest`, `SQLScriptTest`,
  `SQLScriptAdditionalCoverageTest`: PASS
- Full `com.arcadedb.query.sql.executor.*Test` package: PASS, no regressions
- Full `com.arcadedb.query.sql.**.*Test` package (parser + executor + related):
  PASS, exit code 0, zero `[ERROR]` lines

## Impact analysis

- The bug affects **any** cached, aggregate/GROUP BY query re-executed under the
  same normalized statement text, not just SQL Script or semicolon usage - this
  fix corrects silent data-correctness issues beyond the reported symptom.
- The fix is localized to one class and does not change the cache key scheme,
  the grammar, or other execution steps. No behavior change for non-aggregate
  queries.
- Low risk: `copy()` is only invoked when caching/reusing execution plans;
  the new override simply mirrors the existing constructor semantics used at
  first-build time (`SelectExecutionPlanner.java:681`).

## Recommendation for future improvement

Other `ExecutionStep` subclasses that carry extra state beyond their parent
class should be audited for similar missing/incorrect `copy()` overrides,
since this class of bug (silent behavior downgrade on plan-cache reuse) is easy
to introduce and easy to miss without a "run the same cached query twice" test.
