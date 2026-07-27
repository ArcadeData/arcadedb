# Issue #5460 - Pattern comprehension ignores relationship inline `WHERE` predicates

## Problem

A relationship inline `WHERE` predicate inside a pattern comprehension was silently discarded:

```cypher
MATCH (a:A {v: 1})
RETURN size([(a)-[r:E WHERE r.tag = 'ok']->(b:A) | b]) AS c;   -- returned 2, Neo4j returns 1
RETURN size([(a)-[r:E WHERE 1=0]->(b:A) | b]) AS c;            -- returned 2, Neo4j returns 0
```

The same syntax works in a regular `MATCH` (fixed for that path by #3953), and the
comprehension-level `WHERE` (after the pattern) also worked. Only the relationship inline form
inside a comprehension was affected.

Distinct from #5111, which covers the inline property-map form `[:E {tag: 'ok'}]`.

## Root cause

Two independent gaps on the pattern-comprehension path, both of which had to be closed:

1. **The predicate never reached the AST.** Pattern comprehensions are parsed by
   `CypherExpressionBuilder`, not by `CypherASTBuilder`. Its private
   `parseRelationshipPattern(...)` read the variable, labels, property map, path length and
   direction, but never read `ctx.expression()` - the inline `WHERE` body. It then called the
   7-argument `RelationshipPattern` constructor, so `whereExpression` was always `null`.
   `CypherASTBuilder.visitRelationshipPattern(...)`, which feeds regular `MATCH`, does read it.

2. **The evaluator never applied it.** `PatternComprehensionExpression` filtered candidate edges
   only through `matchesEdgeProperties(...)` (the inline property map, #5139). Even with a
   populated `whereExpression` on the AST node, nothing consulted it.

## Fix

**`parser/CypherExpressionBuilder.java`** - `parseRelationshipPattern(...)` now parses
`ctx.expression()` and passes it to the 8-argument `RelationshipPattern` constructor. The body is
parsed with the existing `parseExpression(...)` and adapted through `BooleanCoercionExpression`,
the adapter already used by `CypherASTBuilder` for the same purpose. That keeps comparisons,
`AND`/`OR`/`NOT` and boolean-typed properties all working without duplicating boolean-parsing logic.

**`ast/PatternComprehensionExpression.java`** - both expansion paths now apply the predicate
directly after the existing property-map filter:

- `traverseEdges(...)` - single-hop patterns
- `traverseVariableLength(...)` - `*min..max` patterns, where every relationship on the path must
  satisfy the predicate, matching the existing property-map semantics

The new `matchesEdgeWhereExpression(...)` mirrors `MatchRelationshipStep.matchesEdgeWhereExpression`
so both engines share the same semantics. The evaluation row is built once per expansion by
`copyBindings(...)` and only the relationship variable is rebound per candidate edge, so the
predicate sees outer-scope bindings without a per-edge copy of the whole row. When the pattern
carries no inline `WHERE`, no row is allocated at all and the hot path is unchanged.

## Tests

`engine/src/test/java/com/arcadedb/query/opencypher/OpenCypherPatternComprehensionInlineWhereTest.java`
- 11 tests, using the exact data and queries from the issue report.

| Test | Asserts |
|---|---|
| `alwaysFalseInlineWherePredicateFiltersEverything` | `WHERE 1=0` returns 0 |
| `inlineWherePredicateOnRelationshipPropertyIsApplied` | `WHERE r.tag = 'ok'` returns 1 |
| `inlineWherePredicateProjectsTheMatchingRelationship` | the surviving relationship is the right one |
| `inlineWherePredicateCombinesWithComprehensionLevelWhere` | inline and comprehension-level `WHERE` compose |
| `inlineWherePredicateCanReferenceOuterBoundVariable` | the predicate sees outer-scope bindings |
| `inlineWherePredicateAppliesToEveryHopOfAVariableLengthPattern` | `*1..2` filters every hop |
| `inlineWherePredicateAppliesToAnIncomingPattern` | `<-[r:E WHERE ...]-` filters the same relationship |
| `inlineWherePredicateAppliesToAnUndirectedPattern` | direction-agnostic expansion honors the predicate |
| `inlineWherePredicateResolvesAQueryParameter` | `WHERE r.tag = $tag` resolves the query parameter |
| `noInlineWherePredicateStillReturnsEveryRelationship` | control: no predicate, no filtering |
| `comprehensionLevelWhereStillWorks` | control: documented workaround unaffected |

## Verification

Before the fix, 4 of the original 8 tests failed with precisely the reported symptom (`c = 2`
instead of `0` / `1`); the 4 control tests passed. After the fix all 11 pass.

Regression run: full `com.arcadedb.query.opencypher.**` package - **7495 tests, 0 failures**.
The 3 errors reported are `OpenCypherCustomFunctionTest` GraalVM polyglot classloading failures
(`NoClassDefFoundError: org.graalvm.polyglot.Engine$ImplHolder`), confirmed pre-existing by
running that class unchanged on the base checkout.

## Impact and notes

- Queries that previously received unfiltered lists from a pattern comprehension now receive the
  correctly filtered list. Any consumer of that list (`size()`, indexing, `UNWIND`, aggregation)
  changes result accordingly - this is the intended correction, but it is a behavior change for
  queries that were silently relying on the unfiltered output.
- No change to the regular `MATCH` path, which already applied the predicate.

## PR

https://github.com/ArcadeData/arcadedb/pull/5471

## Review cycles

**Cycle 1** - `6b4acb31` - claude reviewed (approving; "correct, performant, and ready to merge"),
with three non-blocking suggestions. gemini-code-assist posted nothing within the 15-minute window,
consistent with its ongoing sunset.

| Suggestion | Decision |
|---|---|
| Broaden test coverage to incoming/`BOTH` directions and a parameter reference in the inline `WHERE` | **Applied.** Three tests added. All pass unchanged, confirming direction handling and parameter resolution already work through the fix - no further code change needed. |
| Collapse the pre-existing inline binding-copy loops in this file onto the new `copyBindings(...)` helper | **Skipped.** A bug fix should not carry surrounding cleanup; the reviewer itself scoped this as a follow-up. The four call sites are untouched by this change, so folding them in would widen the regression surface for no behavioral gain. |
| File a follow-up issue for `exists(pattern)` / `shortestPath` carrying the predicate but ignoring it | **Deferred to the maintainer.** Filing issues is outside the scope authorized for this PR. Documented under Known gaps below. |

## Known gaps

`CypherExpressionBuilder.parseRelationshipPattern` is also used by `exists(pattern)`
(`PatternPredicateExpression`) and by `shortestPath`. Those evaluators do not consult
`getWhereExpression()`, so an inline `WHERE` remains ignored there. The parser change makes the
predicate available on their AST nodes but does not alter their behavior, so this fix is
regression-free for them. Applying it in those evaluators is a separate change and warrants its own
issue.

Separately, node inline `WHERE` - `(b:A WHERE ...)` - is accepted by the grammar but stored nowhere
(`NodePattern` has no such field), so it is dropped on *every* path including regular `MATCH`. That
is a distinct defect and is not addressed here.
