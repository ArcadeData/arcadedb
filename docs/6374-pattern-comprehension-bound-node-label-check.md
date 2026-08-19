# Issue #6374: Pattern comprehension ignores label/inline-property constraints on a correlated (outer-bound) leading node

## Root cause

`PatternComprehensionExpression.traversePattern` is the engine's evaluator for pattern
comprehensions like `[(p:Company)-->(x) | x.name]`. When the leading node of the pattern reuses
a variable already bound in the outer scope (e.g. `p` from an enclosing `MATCH`), `resolveVertex`
returns that vertex directly, so the method never routes through `traverseUncorrelatedStart`
(which is where labels/inline properties are enforced for an *unbound* start). The only check
applied to a *correlated* start node was the inline `WHERE` predicate
(`matchesNodeWhereExpression`); `Labels.matches(...)` and `InlineProperties.matches(...)` were
never called on it.

The sibling construct `exists(...)` / `PatternPredicateExpression.matchesNodePattern` already
enforces labels + inline properties + WHERE on a bound start vertex (fixed for issue #5095), so
the two constructs disagreed.

## Fix

In `engine/src/main/java/com/arcadedb/query/opencypher/ast/PatternComprehensionExpression.java`,
`traversePattern` now validates the leading node's constraints (labels, inline properties, inline
WHERE) via a new `matchesStartNodePattern` helper at the same site the old WHERE-only check lived
(`hopIndex == 0 && knownStartVertex == null`, just before the mid-pattern edge expansion). The old
separate inline-WHERE-only check at that site was removed - it's now subsumed by
`matchesStartNodePattern`.

The `hopIndex >= pathPattern.getRelationshipCount()` terminal check ("all hops matched") is
unconditionally the first statement in `traversePattern` and stays there; `matchesStartNodePattern`
does not run ahead of it. This is not a gap in practice: the grammar's `pathPatternNonEmpty` rule
(`nodePattern (relationshipPattern nodePattern)+`) requires at least one relationship, so a parsed
`PatternComprehensionExpression` never has `getRelationshipCount() == 0` and the terminal branch is
never reached at `hopIndex == 0` without first passing through the new check. The issue's suggested
fix mentioned guarding "the zero-relationship terminal branch" too; that branch is unreachable from
any query the parser accepts, so no change was made there. (An earlier draft of this doc claimed the
check had been moved ahead of the terminal branch - that was inaccurate, caught in PR review, and is
corrected here.)

## Test

`engine/src/test/java/com/arcadedb/query/opencypher/CypherComprehensionBoundStartLabelIssue6374Test.java`
reproduces the issue's exact repro (label + inline property on a correlated leading node,
contrasted against `exists(...)`), plus a case pinning the pre-existing inline-WHERE behavior so the
refactor into `matchesStartNodePattern` doesn't regress it.

## Verification

- `mvn -pl engine -am test -Dtest=CypherComprehensionBoundStartLabelIssue6374Test` - green.
- Full `engine` module suite (8789 tests, includes the openCypher TCK): `mvn -pl engine -am test` -
  green except one pre-existing, unrelated failure
  (`Issue6302AlgoGraphDrivenWorkGuardTest.apspObservesTheDeadlineInsideTheTripleLoop`, a timing-based
  APSP deadline test) that reproduces identically on an unmodified `main` checkout.
