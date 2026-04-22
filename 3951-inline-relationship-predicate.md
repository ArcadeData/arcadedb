# Fix: Inline relationship predicate ignored (#3951)

## Root Cause

ArcadeDB's OpenCypher engine was silently ignoring the `WHERE` clause written directly
inside a relationship bracket pattern, for example:

```cypher
MATCH (a)-[r:KNOWS WHERE r.since < 2019]-(b) RETURN ...
```

The ANTLR grammar correctly defines the inline `WHERE expression` in `relationshipPattern`,
but three layers all failed to propagate it:

1. **AST** - `RelationshipPattern` had no field for the WHERE predicate.
2. **Parser** - `CypherASTBuilder.visitRelationshipPattern()` never called `ctx.expression()`.
3. **Executor** - `MatchRelationshipStep` had no code path to evaluate such a predicate.

## Changes

### `RelationshipPattern.java`
- Added `whereExpression: BooleanExpression` field.
- Added `getWhereExpression()` and `hasWhereExpression()` accessors.
- Old two-arg and three-arg constructors delegate to the new four-arg constructor
  (backward compatible).

### `CypherASTBuilder.java` (`visitRelationshipPattern`)
- After extracting variable/types/properties, checks `ctx.expression()` and calls
  the existing `parseBooleanExpression()` helper to produce a `BooleanExpression`.
- Passes it as the `whereExpression` argument to the new `RelationshipPattern` constructor.

### `MatchRelationshipStep.java` (`processStandardPath`)
- Added `matchesEdgeWhereExpression(edge, lastResult)` helper that builds a temporary
  `ResultInternal` with the relationship variable bound to the edge and calls
  `whereExpression.evaluate(tempResult, context)`.
- The call is inserted right after the existing `matchesEdgeProperties` check,
  so it short-circuits before heavier downstream processing.

## Tests

Added `OpenCypherPatternPredicateTest.InlineRelationshipPredicate` (3 methods):

| Test | What it verifies |
|------|-----------------|
| `inlineWhereOnRelationshipIsApplied` | Undirected pattern - only 2018 edge returned |
| `inlineWhereOnRelationshipDirected` | Directed pattern - single row, Alice->Bob:2018 |
| `inlineWhereWithExternalWhereClause` | Inline predicate combined with outer WHERE |

All 3 new tests pass. Full OpenCypher suite (5 377 tests) passes with no regressions.
