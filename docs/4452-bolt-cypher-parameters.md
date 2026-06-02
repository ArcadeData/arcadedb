# Issue #4452 - Bolt Cypher Parameterized Queries Return Empty Results

## Problem

Cypher queries executed via the Neo4j Bolt protocol with parameters (`$param`) return empty results, while the same queries with literal values work correctly.

Affected cases from the issue report:
- `MATCH (n:Tag { name: $nameParam }) RETURN n` - parameterized node property filter
- `MATCH (n) WHERE ID(n) = $parentId RETURN n` - parameterized ID filter
- `MATCH (from)-[:rel*0..]->(x:Tag { name: $nameParam }) WHERE ID(from) = $parentId` - VLP with both

## Root Cause

`ExpandPathStep.matchesTargetProperties` (before commit `2aa5534ce`) did not resolve `ParameterReference` objects stored in the target node pattern's property map. The method directly compared `vertex.get(key)` to the raw `ParameterReference` object:

```java
// BEFORE (broken):
final Object expected = entry.getValue();  // ParameterReference("nameParam")
if (actual == null || !actual.equals(expected))  // "tag2".equals(ParameterReference) = false
    return false;
```

So for VLP queries like `MATCH (from)-[:rel*0..]->(x:Tag { name: $nameParam })`, the target node property filter always rejected every candidate node when the value was a parameter, producing empty results.

## Fix

Already applied in commit `2aa5534ce` (issue #4271, 2026-05-20):
- Added `ParameterReference` resolution from `context.getInputParameters()` in `matchesTargetProperties`
- Added numeric type-safe comparison (Integer vs Long coercion)
- Added support for `currentResult` context for expression-based values

Users on v26.5.1 need to upgrade to v26.6.1+ to get this fix.

## Verification

Five regression tests added to `BoltProtocolIT`:
1. `matchWithParameterPropertyFilter` - basic `MATCH (n:T {prop: $param})`
2. `matchWithWhereParameterStringFilter` - `WHERE n.prop = $param`
3. `matchByIdParameter` - `WHERE ID(n) = $id`
4. `vlpMatchWithParameters` - full VLP pattern with both parameters
5. `vlpMatchWithParametersInTransaction` - same within an explicit transaction

All 76 Bolt tests pass (28s run).
