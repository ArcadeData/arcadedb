# Issue #5790: count(*) after OPTIONAL MATCH over an undeclared relationship type causes HTTP 500

https://github.com/ArcadeData/arcadedb/issues/5790

## Root cause

`CypherExecutionPlan.tryDetectStarCountStar()` is one of the `count(*)` push-down detectors
tried by `tryOptimizeCountStar()`. It is the only detector among the five (`tryDetectChainCountStar`,
`tryDetectAntiJoinChainCountStar`, `tryDetectStarCountStar`, `tryDetectTriangleCountStar`,
`tryDetectPairJoinCountStar`) that accepts `OPTIONAL MATCH` clauses - the other four all require
exactly one non-optional `MATCH` clause and bail out otherwise. For a query like

```cypher
MATCH (n:T_0) OPTIONAL MATCH (n)-[:NO_SUCH]->(m:T_0) RETURN count(*) AS c
```

`tryDetectStarCountStar()` finds `n` as the central variable shared by the two `MATCH` clauses and
builds a `DegreeProductOp` with one `Arm` per relationship, marking the arm built from the
`OPTIONAL MATCH` clause as `optional = true`.

`DegreeProductOp.execute()` falls back to the OLTP path whenever there is no mandatory arm (a
correctness requirement documented in issue #5094 - a purely optional star has no degree filter
to exclude zero-degree central nodes, and those nodes must still contribute one null-preserving
row each). The OLTP path, for a single-hop arm, calls `executeOLTPDegreeMap()`, which iterated the
arm's edge type via `Database.iterateType(edgeType, true)` **unconditionally**. `iterateType`
resolves the type name through `Schema.getType()`, which throws `SchemaException("Type with name
'X' was not found")` for an undeclared name.

Every other way of asking the same question - no aggregation, `count(m)`, `sum(1)`, `collect(m)`,
or declaring the relationship type first - either never reaches `DegreeProductOp` at all (the
push-down detectors require `count(*)` specifically) or reaches the ordinary materialization
pipeline, whose `MatchRelationshipStep` already tolerates an undeclared type via vertex-local
edge-list filtering (`EdgeLinkedList.count()`, fixed for issue #4199). Only the `count(*)`
degree-product fast path skipped that guard.

A mandatory (non-optional) arm over an undeclared type never reaches `DegreeProductOp` in the
first place - `tryOptimizeCountStar()` calls `mandatoryPatternElementIsEmpty()` first, which
short-circuits to `ConstantCountStep(0L)` for any *non-optional* `MATCH` clause whose relationship
type set is entirely undeclared. That check deliberately skips `OPTIONAL MATCH` clauses (an
optional arm that matches nothing still contributes its anchor row), which is exactly the gap this
issue falls into.

The multi-hop OLTP fallback (`executeOLTPPerVertex`, used when an arm spans 2+ relationship types)
was already safe: it goes through `Vertex.countEdges()` / `Vertex.getVertices()`, which resolve
edge types via the vertex's own edge-list segments (`EdgeLinkedList.count()`), not via
`Schema.getType()`. Verified with a dedicated reproduction before writing the fix - no additional
guard was needed there.

## Fix

`DegreeProductOp.executeOLTPDegreeMap()` now checks `db.getSchema().existsType(edgeType)` before
calling `db.iterateType(edgeType, true)` for each arm. An undeclared edge type is skipped entirely
for that arm: it contributes no entries to the per-vertex degree map, which is exactly equivalent
to "degree 0 for every vertex" - the same fallback already used to answer this correctly through
the ordinary pipeline.

- A **mandatory** arm over an undeclared type never reaches this code (already short-circuited to
  `ConstantCountStep(0L)` upstream), so the guard only changes behavior for optional arms.
- An **optional** arm with degree 0 (now including "the type doesn't exist") contributes a
  `max(1, 0) = 1` multiplier, per the pre-existing OPTIONAL MATCH semantics of this operator - i.e.
  the anchor row is preserved.

File changed: `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/DegreeProductOp.java`
(+10 lines, a guard clause and its comment - no other files touched).

## Test plan

New file: `engine/src/test/java/com/arcadedb/query/opencypher/Issue5790OptionalMatchCountStarUndeclaredRelTypeTest.java`

- `countStarAfterOptionalMatchOverUndeclaredRelTypeCountsAnchorRow` - one anchor vertex, `count(*)`
  over `OPTIONAL MATCH` on an undeclared type returns `1` instead of throwing.
- `countStarAfterOptionalMatchOverUndeclaredRelTypeOnEmptyGraphIsZero` - empty graph returns `0`
  (already worked before the fix, via the mandatory-arm-is-empty short-circuit on the anchor label;
  kept as a regression guard).
- `countStarAfterOptionalMatchOverUndeclaredRelTypeWithMultipleAnchors` - three anchor vertices,
  `count(*)` returns `3`.
- `controlFormsAlreadyWorkedAndKeepWorking` - `count(m)`, `sum(1)`, and the unaggregated
  `RETURN n.id` forms from the issue's "Controls (work correctly)" table still work after the fix.
- `countStarWithOneDeclaredAndOneUndeclaredOptionalArm` - a star with two `OPTIONAL MATCH` arms,
  one over a declared type with real edges and one over an undeclared type, to confirm the
  undeclared arm's `max(1, 0)` fallback does not disturb the declared arm's real degree count.

### Verification performed

1. TDD: wrote the regression test first, confirmed all `count(*)` scenarios failed with the exact
   `SchemaException`/HTTP-500-shaped `CommandExecutionException` reported in the issue (4 of 5
   tests errored; the empty-graph case already passed via the pre-existing short-circuit).
2. Applied the one-guard fix in `DegreeProductOp.executeOLTPDegreeMap()`.
3. Re-ran the regression test: all 5 pass.
4. Ran the closely related existing tests for regressions: `Issue5094OptionalMatchCountStarTest`,
   `CypherCountPushDownPreconditionsIssue5715Test`, `CypherSubqueryBodyIssue5656Test`,
   `CypherUncorrelatedSubqueryCountPushDownIssue5686Test`, `Issue4199NonExistentEdgeTypeWithRangeTest`
   - all pass.
5. Ran the full `com.arcadedb.query.opencypher.**` test package: 4540 tests, 0 failures, 0 errors.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5914

## Review cycles

Zero review cycles completed. The `claude` bot's review job (`claude-review`,
https://github.com/ArcadeData/arcadedb/actions/runs/31181968311) ran to completion
against head SHA `aea666bd26097c3c451f677c2a0609350a831119` (41 turns, reported
`permission_denials_count: 14`, "No buffered inline comments") but posted **no**
review, review comment, or issue comment anywhere on the PR - verified across all
three surfaces (`gh api .../pulls/5914/reviews`, `.../pulls/5914/comments`,
`.../issues/5914/comments`) after the job's CI check itself showed `pass` (5m1s).
All other CI checks passed (CodeQL, Codacy, Meterian, language analyzers,
`build-and-package`).

This looks like the known MCP-tool-over-denial infrastructure issue tracked
separately (not part of this issue's scope) rather than anything about this PR's
diff - the job's own permission-denial counter is consistent with it being unable
to call whatever tool it needed to post a review/comment.

## Deferred items

None from code review (none was received). The PR is left open for the developer
to either re-trigger `claude-review` (e.g. by pushing a no-op commit) or review and
merge manually.

## Status

**timeout** - the review loop's 15-minute wait window closed with the `claude`
bot's workflow having already run to completion and posted nothing. Per this
skill's constraints, the loop stops here; merge remains the developer's
responsibility. Fix and regression test are implemented, verified locally
(target-package test run: 4540 tests, 0 failures, 0 errors), and pushed to the PR
branch.
