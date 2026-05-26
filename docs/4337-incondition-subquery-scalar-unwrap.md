# Fix #4337 — `InCondition` subquery collects `Result` objects, `IN (SELECT …)` always returns empty

## Root Cause

`InCondition.executeQuery()` materialises the subquery `ResultSet` into a `Set<Result>`:

```java
return result.stream().collect(Collectors.toSet());
```

`evaluateExpression()` then takes the `Set<?>` fast-path:

```java
if (iRight instanceof Set<?> set)
    return set.contains(iLeft);
```

`set.contains(iLeft)` calls `iLeft.equals(Result)` (Java `HashSet` logic). A scalar like `String` or `Integer` is never equal to a `Result` object, so this always returns `false`. The downstream unwrapping loop that uses `QueryOperatorEquals.equals()` (which correctly handles `Result` → scalar) is never reached.

## Fix

In `executeQuery`, unwrap single-property non-element `Result` rows to their scalar values before collecting into the `Set`. This makes `set.contains(scalar)` work correctly for the common `IN (SELECT field FROM type)` pattern, while multi-property results and vertex/edge rows pass through unchanged.

## Affected Files

- `engine/src/main/java/com/arcadedb/query/sql/parser/InCondition.java`
- `engine/src/test/java/com/arcadedb/query/sql/InConditionSubqueryTest.java` (new)

## Test Plan

- `IN (SELECT name FROM type)` returns matching rows (via standard planner path)
- `NOT IN (SELECT name FROM type)` returns non-matching rows
- `IN (SELECT age FROM type WHERE ...)` with integer subquery works
- `count(*)` with IN subquery returns correct count
- Direct `InCondition.evaluate()` without planner returns correct match (regression for the `executeQuery` path)

## Test Results

All 5 tests in `InConditionSubqueryTest` pass. No regressions in `QueryTest` (66), `QueryAndIndexesTest` (2), `SQLScriptTest` (27), or related suites.

## PR

https://github.com/ArcadeData/arcadedb/pull/4344

## Review Cycles

### Cycle 1 - HEAD `334640ca4`

- gemini-code-assist reviewed (COMMENTED)
- Feedback: also unwrap `iLeft` at the top of `evaluateExpression` for Set fast-path symmetry when `iLeft` itself is a single-property non-element `Result`
- Applied: yes - added `iLeft` unwrap before the `MultiValue.isMultiValue(iRight)` block
- Follow-up commit: `29b72391e` - "address review: unwrap iLeft Result in evaluateExpression for Set fast-path symmetry"

### Cycle 2 - HEAD `29b72391e`

- gemini-code-assist: no re-review (known behavior - bot does not re-review follow-up pushes)
- claude: no review (bot not installed in this repository)
- Exited loop: known bot limitation, not a code quality concern

## Final State

`timeout` - per-repository bot behavior: only gemini-code-assist is installed, and it does not re-review follow-up commits.
