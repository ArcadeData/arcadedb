# 5481 - `shortestPath` ignores relationship inline `WHERE` (and the property map on the expression form)

Issue: https://github.com/ArcadeData/arcadedb/issues/5481

## Problem

`shortestPath()` / `allShortestPaths()` have two independent evaluators in the OpenCypher engine:

| Evaluator | Used by | Property map `{tag: 'ok'}` | Inline `WHERE` |
|---|---|---|---|
| `executor/steps/ShortestPathStep` | `MATCH p = shortestPath(...)` | enforced (#5096) | ignored |
| `ast/ShortestPathExpression` | `RETURN shortestPath(...)` | ignored | ignored |

Three of the four combinations silently dropped the constraint, so a `shortestPath` that reads as
constrained returned a path through relationships the query excluded.

## Root cause

Both evaluators read only `RelationshipPattern.getTypes()` and `getDirection()` from the pattern:

- `ShortestPathStep.edgePropertyFilters()` looked at `getProperties()` only. When it returned `null`
  the step fell through to `SQLFunctionShortestPath`, a vertex-only BFS that cannot see edge
  properties at all. An inline `WHERE` never switched the step onto the edge-aware BFS, and even on
  the edge-aware BFS the predicate was never evaluated.
- `ShortestPathExpression.evaluate()` always called `SQLFunctionShortestPath` directly. It never
  consulted `getProperties()` nor `getWhereExpression()`, so both constraints were dropped.

Both AST builders (`CypherASTBuilder.visitRelationshipPattern` for the `MATCH` form,
`CypherExpressionBuilder.parseRelationshipPattern` for the expression form, the latter since #5460)
already populate `whereExpression` on the AST node, so the parse side needed no change: the
predicate reached the evaluators and was silently ignored.

## Fix

1. Extracted the per-edge constraint into `ShortestPathStep.EdgeConstraint` - a small holder for the
   inline property map plus the inline `WHERE` predicate, with a `matches(Edge)` method. It is built
   once per source/target pair (`EdgeConstraint.from(rel, inputRow)`) and returns `null` when the
   pattern carries neither constraint, so the unconstrained hot path stays exactly as it was
   (`SQLFunctionShortestPath`, CSR-accelerated).
2. `ShortestPathStep` now routes onto the existing edge-aware BFS whenever an `EdgeConstraint` is
   present (previously: only when a property map was present), and the BFS checks the full
   constraint rather than just the property map. This covers both the optimizer and the legacy
   planner paths, since both construct the same `ShortestPathStep`.
3. `ShortestPathExpression` now builds the same `EdgeConstraint` and delegates to the shared
   edge-aware BFS helpers when one is present, keeping `SQLFunctionShortestPath` for the
   unconstrained case.

## Tests

`engine/src/test/java/com/arcadedb/query/opencypher/CypherShortestPathInlineWhereTest.java`
covers the full 2x2 matrix (both evaluators x property map / inline `WHERE`), plus
`allShortestPaths`, the combination of both constraints, `$parameter` predicates, and unconstrained
control cases.

## Scope note

Sibling work is fixing relationship inline `WHERE` in other contexts (node inline `WHERE`, #5480).
This change is confined to the two `shortestPath` evaluators and does not touch the shared
`MatchRelationshipStep` / pattern-comprehension paths.
