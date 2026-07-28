# Issue #5480 - Node inline `WHERE` predicates are ignored

Issue: https://github.com/ArcadeData/arcadedb/issues/5480
Branch: `fix/5480-node-inline-where-predicates`

## Problem

The node inline `WHERE` predicate, the `WHERE n.v = 2` in `(n:A WHERE n.v = 2)`, is accepted by the
grammar but was not enforced in every context. The issue reported three contexts: plain `MATCH`,
`EXISTS {}` subqueries and pattern comprehensions.

## Reproduction and triage

A reproduction test (`Issue5480NodeInlineWhereTest`) was written first, covering all three reported
contexts. Against `main` at `043309f61` only the pattern-comprehension case reproduced:

| Context | Status on `043309f61` |
|---|---|
| plain `MATCH` | already correct (`InlineNodeWhereHoister`, #5464) |
| `EXISTS {}` / `COUNT {}` | already correct (`PatternPredicateExpression.matchesNodePattern`, #5464) |
| pattern comprehension | **broken** |

The `MATCH` and `EXISTS {}` cases were fixed by #5464 after the issue text was drafted. Their tests
are kept as regressions so the two paths stay covered.

## Root cause

Node and relationship patterns are parsed by two independent builders:

- `parser/CypherASTBuilder` - the `MATCH` / `CREATE` / `MERGE` path. Its `visitNodePattern(...)`
  already parsed `ctx.expression()` into `NodePattern.whereExpression`, and `visitMatchClause(...)`
  hoisted it into the clause `WHERE` via `rewriter/InlineNodeWhereHoister`.
- `parser/CypherExpressionBuilder` - the pattern-comprehension / `shortestPath` path. Its
  `parseNodePattern(...)` built `new NodePattern(variable, labels, properties, propertiesParameterName)`
  and **never read `ctx.expression()`**, so the predicate was dropped at parse time.

Consequently `PatternComprehensionExpression` received a `NodePattern` whose `whereExpression` was
always `null`, and it had no code to enforce one either. #5460 had fixed the same gap for the
*relationship* form on this builder; the node form was left behind.

## Fix

Two files, both minimal:

1. `parser/CypherExpressionBuilder.parseNodePattern(...)` now parses the inline predicate exactly
   like the relationship form directly below it: `new BooleanCoercionExpression(parseExpression(ctx.expression()))`,
   passed to the full `NodePattern` constructor.

2. `ast/PatternComprehensionExpression` now enforces it. `inlineWhereRow(nodePattern, bindings)`
   builds a private evaluation row by copying the visible bindings once per expansion (and returns
   `null` when the pattern carries no predicate), and `matchesNodeWhereExpression(vertex,
   nodePattern, whereEvalRow, context)` rebinds the node variable on that row per candidate and
   evaluates the predicate. This mirrors the `whereEvalRow` reuse the relationship form already
   used, so a high-fan-out expansion copies the bindings once rather than once per candidate. The
   check is called from the three places a node is accepted:
   - `matchesEndPattern(...)` - every hop's target node (fixed-length, variable-length and the
     zero-length case), which now also receives the hoisted evaluation row.
   - `matchesStartPattern(...)` - candidates of an uncorrelated leading node.
   - `traversePattern(...)` - the leading node of the first hop when it is resolved from an
     outer-scope binding. Later hops start from the previous hop's end node, already validated by
     `matchesEndPattern`, so the check is guarded on `hopIndex == 0 && knownStartVertex == null`
     and never runs twice for the same node.

The evaluation row is only allocated when the pattern actually carries a predicate, so the common
predicate-free path stays allocation-free.

## Tests

`engine/src/test/java/com/arcadedb/query/opencypher/Issue5480NodeInlineWhereTest.java` - 10 tests:

- plain `MATCH`: single node, both endpoints of a path, inline combined with a clause-level `WHERE`
- `EXISTS {}`: pattern-only form and explicit-`MATCH` form, matching and non-matching predicate
- `COUNT {}`: predicate applied, and control without predicate
- pattern comprehension: target node, uncorrelated/anchor node, a variable-length hop (which
  exercises the reused evaluation row across candidates), and inline combined with the
  comprehension's trailing `WHERE`

Verification:

- `mvn -o test -Dtest=Issue5480NodeInlineWhereTest` - 10/10 pass (3 failed before the fix)
- `mvn -o test -Dtest='com.arcadedb.query.opencypher.**'` - 7579 tests, 0 failures. The only 3
  errors are `OpenCypherCustomFunctionTest` GraalVM polyglot `NoClassDefFoundError`s, reproduced
  identically on a stashed (unmodified) tree, so they are environmental and pre-existing.

## Scope notes

- `shortestPath` is deliberately **not** touched. `ast/ShortestPathExpression` shares
  `CypherExpressionBuilder.parseNodePattern(...)`, so after this change it receives a node predicate
  it still ignores - the same situation #5460 created for the relationship form. That evaluator is
  the subject of issue #5481 and is being fixed separately.
- No overlap with `InlineNodeWhereHoister` or `PatternPredicateExpression`; neither is modified.

## Impact

Silent wrong answers become correct answers. Any query using `[(a)-[:R]->(x:L WHERE ...) | x]` in
`size()`, `count()`, aggregation or a projection previously saw the unfiltered set.

## PR

https://github.com/ArcadeData/arcadedb/pull/5486

## Review cycles

### Cycle 1 - head `97ab46ec`

- `claude[bot]`: reviewed, **no blocking items**, three minor suggestions.
  1. GC pressure - the end-node check re-copied the bindings per candidate instead of once per
     expansion like the relationship form. **Applied** (`inlineWhereRow(...)` hoisted out of the
     candidate loops), plus a variable-length regression test that would catch a stale-row bug.
  2. Test class javadoc listed `shortestPath` among the covered contexts although no such test
     exists. **Applied** - javadoc rewritten to name the three covered contexts and point at #5481.
  3. `ShortestPathExpression` now receives a parsed-but-ignored node predicate. **No change** - the
     reviewer flagged it as a non-regression; that evaluator is issue #5481, fixed separately.
- `gemini-code-assist`: did not respond within the 15-minute per-cycle window.

### Cycle 2 - head `1af2fe5f`

- Pushed the two applied items above plus the extra variable-length test.
- `claude[bot]`: re-reviewed, **no blocking items**. Confirmed the double-evaluation guard, the
  allocation-free predicate-free path and the row hoisting. Two minor points: the same-hop
  relationship-reference divergence recorded below (agreed it deserves its own issue), and a stale
  test count in the PR description (corrected to 10).
- `gemini-code-assist`: did not respond within the 15-minute per-cycle window, as in cycle 1.

## Deferred items

- A node inline predicate that references the relationship variable of the same hop, e.g.
  `[(a)-[r:E]->(x:A WHERE x.v > r.w) | x]`, sees `r` unbound on the comprehension path, because the
  evaluation row is copied from the bindings visible before the hop. The equivalent `MATCH`
  spelling hoists the predicate into the clause `WHERE`, where `r` is bound, so the two spellings
  can disagree for this shape. Out of scope for #5480 (the predicate being dropped entirely) and it
  needs its own triage, including what `r` should mean on a variable-length hop. On the
  fixed-length path the edge is already in hand at the `matchesEndPattern` call, so binding it into
  the node evaluation row would be cheap; the variable-length path is the part that needs a
  semantic decision. Worth a separate issue.

## Final state

`timeout` - `gemini-code-assist` did not review either head within the per-cycle window.
`claude[bot]` reviewed with no blocking items and its actionable suggestions were applied.
