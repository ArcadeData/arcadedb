# Issue #6374: Pattern comprehension ignores label/inline-property constraints on a correlated (outer-bound) leading node

## Root cause

`PatternComprehensionExpression.traversePattern` is the engine's evaluator for pattern
comprehensions like `[(p:Company)-->(x) | x.name]`. When the leading node of the pattern reuses
a variable already bound in the outer scope (e.g. `p` from an enclosing `MATCH`), `resolveVertex`
returns that vertex directly, so the method never routes through `traverseUncorrelatedStart`
(which is where labels/inline properties are enforced for an *unbound* start). The only check
applied to a *correlated* start node was the inline `WHERE` predicate
(`matchesNodeWhereExpression`); `Labels.matches(...)` and `InlineProperties.matches(...)` were
never called on it. Worse, that WHERE-only check lived inside the multi-hop branch, past the
`hopIndex >= pathPattern.getRelationshipCount()` terminal check - so a zero-relationship pattern
comprehension (a single node, no arrows) skipped node-pattern validation entirely, WHERE included.

The sibling construct `exists(...)` / `PatternPredicateExpression.matchesNodePattern` already
enforces labels + inline properties + WHERE on a bound start vertex (fixed for issue #5095), so
the two constructs disagreed.

## Fix

In `engine/src/main/java/com/arcadedb/query/opencypher/ast/PatternComprehensionExpression.java`,
`traversePattern` now resolves the leading node's vertex and validates it (labels, inline
properties, inline WHERE) via a new `matchesStartNodePattern` helper **before** the
zero-relationship terminal check, not just before the mid-pattern edge expansion. This closes both
gaps described in the issue: the label/property omission on a correlated bound start, and the
zero-relationship terminal branch that bypassed the check altogether.

The old separate inline-WHERE-only check further down was removed - it's now subsumed by
`matchesStartNodePattern`.

## Test

`engine/src/test/java/com/arcadedb/query/opencypher/CypherComprehensionBoundStartLabelIssue6374Test.java`
reproduces the issue's exact repro (label + inline property on a correlated leading node,
contrasted against `exists(...)` and the zero-relationship single-node case) and asserts the
comprehension and `exists(...)` agree.

## Verification

- `mvn -o -pl engine -am test -Dtest=CypherComprehensionBoundStartLabelIssue6374Test -Dmaven.repo.local=...`
- Full opencypher package regression: `mvn -o -pl engine -am test -Dtest='com.arcadedb.query.opencypher.**' ...` (see PR for exact command/coverage run)
