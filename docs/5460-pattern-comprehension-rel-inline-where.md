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
parsed with the existing `parseExpression(...)` and adapted through `BooleanCoercionExpression`.

Note this is not the identical route the `MATCH` path takes: `CypherASTBuilder` uses its private
`parseBooleanExpression(...)`, which builds a structured `BooleanExpression` tree, and falls back to
`BooleanCoercionExpression` only for non-comparison forms. Sharing that method would mean exposing a
private method across builder classes, so the comprehension path coerces instead.
`parseExpression(...)` covers the whole precedence hierarchy including `AND`/`OR`/`NOT`, and the
coercion maps a `NULL` result to "no match", which is the correct Cypher filter semantics. The two
routes were therefore checked for behavioral parity rather than assumed equal - see Tests.

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
- 17 tests, using the exact data and queries from the issue report.

| Test | Asserts |
|---|---|
| `alwaysFalseInlineWherePredicateFiltersEverything` | `WHERE 1=0` returns 0 |
| `inlineWherePredicateOnRelationshipPropertyIsApplied` | `WHERE r.tag = 'ok'` returns 1 |
| `inlineWherePredicateProjectsTheMatchingRelationship` | the surviving relationship is the right one |
| `inlineWherePredicateCombinesWithComprehensionLevelWhere` | inline and comprehension-level `WHERE` compose |
| `inlineWherePredicateCanReferenceOuterBoundVariable` | the predicate sees outer-scope bindings |
| `inlineWherePredicateAppliesToEveryHopOfAVariableLengthPattern` | `*1..2` filters every hop |
| `inlineWherePredicateTruncatesAVariableLengthPathAtTheFirstFailingHop` | a 2-hop chain is cut at the first hop that fails the predicate |
| `inlineWherePredicateAppliesToAnIncomingPattern` | `<-[r:E WHERE ...]-` filters the same relationship |
| `inlineWherePredicateAppliesToAnUndirectedPattern` | direction-agnostic expansion honors the predicate |
| `inlineWherePredicateResolvesAQueryParameter` | `WHERE r.tag = $tag` resolves the query parameter |
| `inlineWherePredicateSupportsConjunction` | `AND` in the predicate body |
| `inlineWherePredicateSupportsDisjunction` | `OR` in the predicate body |
| `inlineWherePredicateExcludesRelationshipsWhoseComparisonIsNull` | a comparison over a missing property is `NULL`, so no match |
| `inlineWhereIsNullPredicateSelectsTheMissingProperty` | `IS NULL` selects the untagged relationship |
| `negatedInlineWherePredicateMatchesTheRegularMatchPath` | `NOT (...)` under three-valued logic gives the same answer as the equivalent `MATCH` |
| `noInlineWherePredicateStillReturnsEveryRelationship` | control: no predicate, no filtering |
| `comprehensionLevelWhereStillWorks` | control: documented workaround unaffected |

## Verification

Before the fix, 4 of the original 8 tests failed with precisely the reported symptom (`c = 2`
instead of `0` / `1`); the 4 control tests passed. After the fix all 17 pass.

Per-hop enforcement was verified by mutation rather than by inspection: temporarily removing the
predicate check from `traverseVariableLength(...)` makes
`inlineWherePredicateTruncatesAVariableLengthPathAtTheFirstFailingHop` fail (it then collects
`badEnd` as well), confirming the test actually pins that behavior. The check was restored
afterwards.

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
- The inline predicate is evaluated before the target vertex is resolved, so it cannot reference the
  far-end node - `-[r:E WHERE b.v = 2]->(b)` does not see `b`. This matches the ordering in
  `MatchRelationshipStep` (inline `WHERE` runs ahead of the target label/property filters), so the
  two engines stay consistent. Predicates on the far end belong in the comprehension-level `WHERE`.

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

**Cycle 2** - `15ee8697` - claude reviewed again (approving; "safe to merge"), three minor notes.
gemini-code-assist again posted nothing.

| Note | Decision |
|---|---|
| The var-length test had only one reachable hop, so it never exercised a path whose *later* hop fails the predicate | **Applied.** Added a two-hop `:C` chain to the fixture and a test asserting the path is truncated at the first failing hop. Verified by mutation (see Verification) that it genuinely fails without per-hop enforcement. The chain is label-scoped so the single-hop tests, which all pin the far end to `:A`, are unaffected. |
| The inline predicate cannot reference the target node | **No change - expected behavior.** Consistent with `MatchRelationshipStep` ordering; now documented under Impact and notes. |
| PR body said 8 tests where the doc and test class say more | **Applied.** PR body corrected to 12. |

**Cycle 3** - `9096d47f` - claude reviewed again (approving), one substantive note plus a doc nit.
gemini-code-assist again posted nothing.

| Note | Decision |
|---|---|
| The comprehension parses the inline `WHERE` through `BooleanCoercionExpression` while `MATCH` uses `parseBooleanExpression(...)`; three-valued logic under `NOT` / `AND` / `OR` / `NULL` was therefore unverified | **Applied.** Five tests added covering `AND`, `OR`, a `NULL` comparison, `IS NULL`, and a `NOT (...)` case asserted equal to the answer the equivalent `MATCH` gives. All pass: the two routes agree, so the divergence is theoretical rather than real. The Fix section above no longer claims the two builders take the identical route. |
| Trim the "Review cycles" section - it reads oddly as a committed repo doc | **Kept.** Checked against the repo: the most recent tracking docs on `main` (`docs/5454-*`, `docs/5453-*`) both carry a `## Review cycles` section, so this matches current convention rather than departing from it. |

**Cycle 4** - `9fa98e2c` - claude reviewed: **LGTM**, no blocking items. gemini-code-assist posted
nothing across all four cycles.

| Note | Decision |
|---|---|
| The test Javadoc carried a copy-pasted `@author` tag, misattributing the file | **Applied.** Tag removed rather than reassigned; 78 of the 183 tests in this package carry no `@author`, so omitting it is within convention. |
| `exists(pattern)` / `shortestPath` now silently ignore a parsed predicate rather than never parsing it - worth its own tracking issue | **Deferred to the maintainer**, as in cycle 1. Recorded under Known gaps. |
| `copyBindings(...)` duplicates property-copy loops in `MatchRelationshipStep` and `buildHopResult` | **Skipped**, as in cycle 1: consolidation is a follow-up, not part of this fix. |

## Final state

`max-cycles-reached` (4 of 4) with a clean approval on the last cycle - the remaining items are all
explicitly non-blocking and either deferred to the maintainer or scoped to follow-ups.

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
