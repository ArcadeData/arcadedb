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
