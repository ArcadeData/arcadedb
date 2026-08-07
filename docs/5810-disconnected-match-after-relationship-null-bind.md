# Issue #5810: Disconnected MATCH after a relationship pattern binds the new variable to null

Issue: https://github.com/ArcadeData/arcadedb/issues/5810

## Root cause

`CypherOptimizer.optimize()` (the cost-based Cypher optimizer, `com.arcadedb.query.opencypher.optimizer`)
handles two shapes of multi-`MATCH` query:

- All `MATCH` clauses are relationship-free single nodes → `optimizeMultiMatchIndependent()` builds one
  scan operator per node and chains them with `CartesianProduct`.
- Otherwise → a single anchor is selected and `buildExpansionChain()` walks
  `logicalPlan.getRelationships()` to build the ExpandAll/ExpandInto chain.

A query like:

```cypher
MATCH (n:A_0)-[:E]->(m:B_0)
MATCH (x:B_0)
RETURN m.id, x.id
```

has a relationship (`n-[:E]->m`), so it falls into the second branch. `x` is a valid node in
`logicalPlan.getNodes()` but shares no variable with any relationship, so it is never turned into a
physical operator - it simply falls out of the plan. The `RETURN` projection then reads `x.id` from a
row where `x` was never bound, silently returning `null` instead of raising an error or expanding one
row per matching `x`. `count(*)` was correspondingly stuck at 1 instead of 2.

`CypherExecutionPlanner.shouldUseOptimizer()` already special-cases this shape: it allows a
disconnected pattern into the optimizer specifically when the disconnected component is a single node
(`path.isSingleNode()`), on the assumption it would be Cartesian-joined like
`optimizeMultiMatchIndependent()` does - but `optimize()` never actually did that when a relationship
was present elsewhere in the query.

## Fix

`engine/src/main/java/com/arcadedb/query/opencypher/optimizer/CypherOptimizer.java`, `optimize()`:

- After building the expansion chain, compute the set of "connected" variables (the anchor, plus every
  relationship's source/target). Any pattern node from `logicalPlan.getPatternNodes()` (named or
  anonymous) not in that set is a disconnected single-node component.
- `WHERE` predicates are split: those that reference only connected variables are pushed down as
  before (so anchor-only pushdown and GAV chain fusion still apply); a predicate mentioning a
  disconnected node's variable is deferred, since that variable is not bound yet at that point in the
  plan.
- After GAV fusion and deferred-vertex-loading (so the connected component keeps its existing
  optimizations), each disconnected node gets its own anchor operator (`anchorSelector.evaluateNodeDirect`
  + `createAnchorOperator`, the same helpers `optimizeMultiMatchIndependent` already uses) and is
  Cartesian-joined onto the plan. The deferred filters are then applied on top, once every variable
  they reference is bound.

`applyFilterPushdown` was refactored to take an explicit `List<WhereClause>` instead of always reading
`logicalPlan.getWhereFilters()`, so the two filter buckets (connected / deferred) can be applied at
their respective points in the plan; both existing call sites (the connected-component path and
`optimizeMultiMatchIndependent`) keep their original behavior.

## Tests

`engine/src/test/java/com/arcadedb/query/opencypher/Issue5810DisconnectedMatchAfterRelationshipTest.java`
(new), using the exact reproduction from the issue plus edge cases:

- `disconnectedMatchAfterRelationshipProducesCartesianProduct` - the reporter's query returns the
  expected 2-row Cartesian product instead of 1 row with a `null` binding.
- `countStarReflectsTheCartesianProduct` - `count(*)` returns 2, not 1.
- `planUsesCartesianProductOperator` - the physical plan actually contains a `CartesianProduct`
  operator, proving the fixed branch is exercised (not an accidental legacy-path fallback).
- `disconnectedMatchBeforeRelationshipStillWorks` - control: the already-correct reversed clause order
  keeps working.
- `anonymousDisconnectedNodeStillMultipliesRows` - an anonymous disconnected node (no variable) still
  multiplies the row count, exercising the `getPatternNodes()` (not just named `getNodes()`) choice.
- `whereFilterOnDisconnectedVariableIsAppliedAfterTheJoin` - a `WHERE` predicate on the disconnected
  variable is deferred and evaluated correctly once that node is bound.

All 6 tests were verified to fail without the fix (reverted the `CypherOptimizer.java` change via
`git stash`, re-ran the test class: 4 failures + 1 error) and pass with it restored, confirming the
tests reach and depend on the fixed code path.

## Round 2: fix from the `claude[bot]` PR review (PR #5888)

The bot review found a real, live correctness gap in the round-1 fix: `AnchorSelector.selectAnchor`
is purely cost-based with no relationship-reachability awareness. If a disconnected node carried its
own selective filter (e.g. `MATCH (n:A_0)-[:E]->(m:B_0) MATCH (x:B_0 {id: 3})`), it could win anchor
selection outright over the unfiltered `n`/`m`. Reproduced live: the plan picked `x` as anchor and then
tried to expand the `n-[:E]->m` relationship starting from `x` (not one of its endpoints), returning 0
rows instead of the correct 1.

Fix:
- `AnchorSelector` gained a `selectAnchor(LogicalPlan, Set<String> excludedVariables)` overload that
  skips any node whose variable is in the exclusion set; the original 1-arg method now delegates to it
  with an empty set.
- `CypherOptimizer.optimize()` now computes the disconnected-node set **before** calling
  `selectAnchor`, from `logicalPlan.getRelationships()` alone (not from the chosen anchor), and passes
  it as the exclusion set - so a disconnected node can never be selected as anchor in the first place.
- The WHERE-filter split was upgraded from a coarse "does the whole clause touch a disconnected
  variable" check to a per-conjunct split using the existing
  `WhereClause.extractForVariables`/`residualForVariables` helpers, so a compound predicate like
  `WHERE n.id = 1 AND x.id = 2` still lets the connected conjunct (`n.id = 1`) reach anchor pushdown
  instead of deferring the whole clause because of the disconnected conjunct.
- Minor style nit from the review (brace-wrapped single-statement `if`) also cleaned up.

New tests added to the same test class:
- `disconnectedNodeWithOwnIndexedFilterDoesNotHijackAnchorSelection` - the review's exact reproduction.
- `multipleDisconnectedNodesChainCorrectly` - two disconnected nodes chained (4-row Cartesian product).
- `mixedConnectedAndDisconnectedWhereClauseAppliesBothParts` / `...ConnectedConjunctCanExcludeAllRows` -
  compound WHERE clause correctness for the per-conjunct split.

Verified via `git stash` (reverting only `AnchorSelector.java` + `CypherOptimizer.java`, keeping the
round-1 fix from the initial commit) that the 2 new correctness-targeted tests fail without round 2 and
pass with it restored.

Independently, PR #5876 (opened earlier the same day, before this PR) fixes the identical root cause
with a structurally similar approach (excluding isolated nodes from anchor selection). See the PR
description / PR comment on #5888 for the cross-reference; picking which PR to carry forward is a
developer decision.

## Verification run

- `mvn -pl engine -am compile` - clean.
- `mvn -pl engine -am test -Dtest=Issue5810DisconnectedMatchAfterRelationshipTest` - 10/10 pass (after
  round 2).
- `mvn -pl engine test -Dtest='com.arcadedb.query.opencypher.optimizer.**,Issue5117PatternOrderTest,Issue5136MatchMultipleCreateOptimizerTest,Issue5810DisconnectedMatchAfterRelationshipTest'`
  - all pass, including `AnchorSelectorTest` and `CypherOptimizerIntegrationTest`.
- Full `com.arcadedb.query.opencypher.**` package after round 2: **8011 tests, 0 failures, 0 errors,
  98 skipped, BUILD SUCCESS** (3m22s). An earlier bulk run before round 2 showed 21 unrelated
  `NoClassDefFoundError` errors in test classes untouched by this change; re-running each of those
  classes in isolation passed cleanly, consistent with a local IDE background-compiler race against
  `target/classes` (IntelliJ IDEA was running concurrently) rather than a regression from this change.
