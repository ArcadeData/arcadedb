# Issue #5691: ExpandInto ignored the "is the edge variable read" check

https://github.com/ArcadeData/arcadedb/issues/5691

## Root cause

`CypherOptimizer.buildExpansionChain` decides whether a hop's edge has to be materialized. On
the `ExpandAll` branch it computed `edgeIsMaterialized` by asking whether anyone actually reads
the relationship variable (`needsEdgeTracking` or `CypherVariableUsage.isEdgeVariableReferenced`).
On the `ExpandInto` branch (bound-target hops - both endpoints already bound, e.g. the closing
edge of a cycle), `createExpandIntoOperator` skipped that question entirely and took
`relationship.getVariable()` straight off the pattern.

So a query like:

```cypher
MATCH (a:Account {code:'HUB'})-[:INITIATED]->(t:Txn {ref:'SHARED'})-[r:SETTLED]->(a)
RETURN count(*) AS c
```

where `r` is named but never read, planned the closing hop as edge-carrying on the ExpandInto
branch even though the identical anonymous hop would have been planned as adjacency-only. Because
`createExpandIntoOperator` only considers the CSR-backed `GAVExpandInto` operator when
`edgeVariable == null`, the named-but-unused variable kept the hop on the OLTP edge-list path
instead of the cheaper binary-search-plus-range-scan CSR path, whenever a `GraphAnalyticalView`
covered the edge type.

## Affected components

- `engine/src/main/java/com/arcadedb/query/opencypher/optimizer/CypherOptimizer.java`
  - `buildExpansionChain`
  - `createExpandIntoOperator`

## Fix

Hoisted the `edgeIsMaterialized` predicate out of the `ExpandAll`-only `else` branch so it is
computed once per hop, before the `ExpandInto`/`ExpandAll` branch decision, and applied it to
both `createExpandAllOperator` (unchanged behavior) and `createExpandIntoOperator` (new
parameter). `createExpandIntoOperator` now nulls `edgeVariable` exactly like
`createExpandAllOperator` already did, when the variable is anonymous or unread.

The uniqueness gate stays independent, as the issue called out: `needsEdgeTracking` and a
non-empty `sameClausePrecedingRelVars` still force the edge-list `ExpandInto` operator regardless
of `edgeIsMaterialized`, because `GAVExpandInto`'s CSR adjacency ids carry no edge identity and
cannot enforce Cypher relationship uniqueness.

Checked `createExpandAllOperator`'s only other caller and every other site in
`com.arcadedb.query.opencypher` that reads `relationship.getVariable()` directly - the remaining
sites are all in the parser (`CypherASTBuilder`, `CypherSemanticValidator`), the referenced-variable
collector, or the legacy step-based executor (`CypherExecutionPlan`, `ShortestPathStep`), none of
which duplicate this optimizer decision. `createExpandAllOperator`/`createExpandIntoOperator` were
the only two physical-operator construction sites, and both now agree.

## Test

Added `engine/src/test/java/com/arcadedb/query/opencypher/CypherExpandIntoUnreadEdgeVariableTest.java`,
modeled on the existing `CypherGAVBoundTargetCardinalityTest`. It builds the same two-hop cycle
fixture, but names the *closing* (bound-target / ExpandInto) hop's relationship variable without
reading it anywhere in the query.

- Confirmed the test fails on pre-fix code: `EXPLAIN` showed `ExpandInto(t)-[r:SETTLED]->(a) ...
  BOUND-TARGET` instead of `GAVExpandInto` (stashed the fix, ran the test, restored the fix).
- After the fix: the plan uses `GAVExpandInto`, and the row count is identical with and without
  the `GraphAnalyticalView` (4, from 2 outbound x 2 return edges).

## Verification

- `mvn -q -pl engine -am compile` - clean compile.
- `mvn -q -pl engine -am test -Dtest=CypherExpandIntoUnreadEdgeVariableTest` - new test passes
  (and was confirmed to fail without the fix).
- `mvn -q -pl engine -am test -Dtest=<16 related GAV/optimizer/ExpandInto test classes>` - all
  pass (`CypherGAVBoundTargetCardinalityTest`, `CypherMultiHopInListIssue5306Test`,
  `PhysicalOperatorTest`, `OpenCypherOptimizerVerificationTest`, `CypherLabelDisjunctionTest`,
  `OpenCypherVariableLengthPathTest`, `CypherGAVFusedChainIssue5746Test`,
  `PatternPredicateGhostEdgeTest`, `MatchRelationshipStepProfilingTest`, `GAVEligibilityTest`,
  `CypherOptimizerIntegrationTest`, `CostModelTest`, `ExpandIntoRuleTest`,
  `MatchGAVExpandIntoTest`, `GraphAnalyticalViewTest`).
- `mvn -q -pl engine -am test -Dtest='com.arcadedb.query.opencypher.**'` - full package,
  7709 tests, 0 failures, 0 errors.

## Impact

Narrow, GAV-path-only optimization as scoped in the issue: without a `GraphAnalyticalView`
covering the edge type, `ExpandInto` still walks the narrowed edge list either way (per #5663,
to know the multiplicity), so there is no behavior change without a view. With a view, a
named-but-unread relationship variable on a bound-target hop no longer forces the edge-list path.

## Scope checklist (from the issue)

- [x] Hoist `edgeIsMaterialized` out of the `ExpandAll` branch and apply it to
      `createExpandIntoOperator`
- [x] Keep the uniqueness gate (`needsEdgeTracking`, `sameClausePrecedingRelVars`) independent of it
- [x] A test asserting from `EXPLAIN` that a named-but-unread rel var on a bound-target hop reaches
      `GAVExpandInto` when a view covers the type, and that the answer is unchanged with and
      without the view
- [x] Checked `createExpandAllOperator`'s callers and other `relationship.getVariable()` read
      sites for the same asymmetry - none found in the optimizer path
