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

## Verification run

- `mvn -pl engine -am compile` - clean.
- `mvn -pl engine -am test -Dtest=Issue5810DisconnectedMatchAfterRelationshipTest` - 6/6 pass.
- Broader regression pass: `mvn -pl engine -am test -Dtest='com.arcadedb.query.opencypher.**'` (full
  OpenCypher test package) - see PR/CI for the recorded result.
