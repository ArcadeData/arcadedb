# Issue #6482: label-disjunction anchor with an incident relationship never reaches the optimizer

## Root cause

`CypherExecutionPlanner.shouldUseOptimizer()` bailed out of the cost-based optimizer for the whole
statement whenever any node in a path had a label disjunction (`n:A|B`) *and* that path had an
incident relationship. So `MATCH (n:A|B {id: $x})-[:REL]->(m) RETURN m` always fell straight to the
legacy execution engine and never got the `NodeByLabelDisjunctionIndexSeek` speedup issue #6397 added
for the relationship-free case (`MATCH (n:A|B {id: $x}) RETURN n`).

The gate existed because `ExpandAll`/`ExpandInto`/`VarLengthExpand` each carry a single `targetLabel`
(pushed by `CypherOptimizer.addTargetLabelFilter`, using `targetNode.getFirstLabel()`) and cannot
represent OR semantics for a node reached over a relationship. That is a real limitation, but it only
applies to a disjunction on a *non-anchor* node - the anchor itself is seeded by
`IndexSelectionRule.createAnchorOperator`, which #6397 already taught to build a
`NodeByLabelDisjunctionScan`/`NodeByLabelDisjunctionIndexSeek` for it, with no `targetLabel` involved at
all.

## Fix

Two changes, working together:

1. **`CypherExecutionPlanner.shouldUseOptimizer()`**: relaxed the per-node gate so a label-disjunction
   node with an incident relationship no longer disqualifies the whole statement at the AST level. This
   gate runs before anchor selection, so it cannot yet know which node a connected component's
   cost-based anchor selection will land on.

2. **`CypherOptimizer.optimize()`**: added a safety check, `hasUnanchoredLabelDisjunction`, run right
   after each connected component's anchor is finalized (after `validateAnchorForUnidirectionalEdges`,
   which can itself re-anchor away from the initially cost-selected node). If any node in that
   component *other than* the chosen anchor still carries a label disjunction, `optimize()` returns
   `null` for the whole statement - the same "not representable" signal the method already used for
   other unsupported shapes - which sends the query back to the legacy pipeline that evaluates every
   alternative correctly (no silent first-label-only filtering).

This preserves correctness for every shape the optimizer cannot yet represent (disjunction on a
non-anchor node - single relationship, chain, or variable-length hop) while unlocking the index-seek
speedup for the common case: a disjunction anchor with an equality predicate, followed by one or more
relationships to plain-labeled or unlabeled nodes.

## Files changed

- `engine/src/main/java/com/arcadedb/query/opencypher/planner/CypherExecutionPlanner.java` - relaxed
  `shouldUseOptimizer()`'s per-node disjunction gate.
- `engine/src/main/java/com/arcadedb/query/opencypher/optimizer/CypherOptimizer.java` - added
  `hasUnanchoredLabelDisjunction()` and its call site in the per-component anchor-selection loop.
- `engine/src/test/java/com/arcadedb/query/opencypher/CypherDisjunctionAnchorWithRelationshipIssue6482Test.java`
  (new) - regression test.

## Tests

New: `CypherDisjunctionAnchorWithRelationshipIssue6482Test`
- `disjunctionAnchorWithEqualityPredicateAndRelationshipUsesTheOptimizer` - inline-property equality on
  a disjunction anchor with a relationship now plans as `NodeByLabelDisjunctionIndexSeek` feeding
  `ExpandAll`, via the cost-based optimizer.
- `disjunctionAnchorViaWhereClauseWithRelationshipUsesTheOptimizer` - same, predicate in `WHERE`.
- `bothAlternativesStillReachTheSameTargetOverTheRelationship` - both disjunction alternatives still
  return the right row through the relationship.
- `aValueNoAlternativeHasReturnsNothingEvenWithARelationship` - a value neither alternative has still
  returns nothing.
- `disjunctionOnTheExpandedNodeDoesNotUseTheOptimizerButStaysCorrect` - the safety-net direction: a
  disjunction on the node a relationship expands *into* (not the anchor) is confirmed to skip the
  optimizer (`doesNotContain("Execution Plan (Cost-Based Optimizer)")`) and still return correct rows
  via the legacy engine.

Existing regression coverage that pins the correctness of the fallback shape end-to-end (unaffected by
this change, re-run to confirm no regression):
`CypherLabelDisjunctionOnExpandedNodeIssue6338Test`, `CypherDisjunctionIndexSeekIssue6397Test`,
`CypherDisjunctionCardinalityIssue6363Test`, `CypherLabelDisjunctionTest`,
`CypherRelationshipTypeDisjunctionTest`.

## Verification

- `mvn -pl engine -am compile` - clean.
- `mvn -pl engine -am test -Dtest=CypherDisjunctionAnchorWithRelationshipIssue6482Test` - 6/6 pass.
- `mvn -pl engine -am test -Dtest=CypherDisjunctionAnchorWithRelationshipIssue6482Test,CypherLabelDisjunctionOnExpandedNodeIssue6338Test,CypherDisjunctionIndexSeekIssue6397Test,CypherDisjunctionCardinalityIssue6363Test,CypherLabelDisjunctionTest,CypherRelationshipTypeDisjunctionTest`
  - all pass.
- `mvn -pl engine -am test -Dtest='com.arcadedb.query.opencypher.**' -DexcludedGroups=benchmark,slow,vector`
  - BUILD SUCCESS; OpenCypher TCK compliance report: 3812/3897 passed, 0 failed, 85 skipped (same skip
    count as before this change - unaffected suite).

## Scope note (from the issue)

Purely a missed-optimization / performance fix, not a correctness fix on its own: a disjunction +
relationship query already returned the right rows via the legacy scan-only path before this change.
Teaching `ExpandAll`/`ExpandInto` to accept a disjunction target (issue #6482's suggested shape #2,
covering a disjunction on a *non-anchor* node) remains out of scope and unaddressed - that shape still
falls back to the legacy engine, correctly, via the new `hasUnanchoredLabelDisjunction` safety check.
