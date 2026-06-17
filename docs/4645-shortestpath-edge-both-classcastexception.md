# Fix #4645 - shortestPath edge:true ClassCastException on BOTH direction with empty side

## Issue

`shortestPath(..., {'direction':'BOTH','edge':true})` throws `ClassCastException` when one of the two directional sub-queries returns an empty edge list.

## Root Cause (two bugs)

**Bug 1 - ClassCastException:** `EdgeToVertexIterable.iterator()` unconditionally cast the result of `edges.iterator()` to `EdgeIterator`:
```java
return new EdgeToVertexIterator((EdgeIterator) edges.iterator(), direction);
```
When `direction:BOTH`, `SQLFunctionShortestPath.getVerticesAndEdges()` recurses into `OUT` and `IN`. For a vertex that has only an outgoing edge, the `IN` side returns `GraphEngine.EMPTY_EDGE_LIST`. That list's `iterator()` returns `Collections.emptyIterator()` which is NOT an `EdgeIterator`, causing `ClassCastException`.

**Bug 2 - Wrong vertex returned:** `EdgeToVertexIterator.next()` called `edge.getVertex(direction)` where `direction` is the traversal direction. For an OUT traversal from vertex 'a', this returns 'a' (the OUT/source end) rather than the neighbor 'b' (the IN/destination end). The BFS made no progress because every "neighbor" was the current vertex itself.

## Fix

**EdgeToVertexIterable:** check `instanceof ResettableIterator` (the common supertype of `EdgeIterator` and `EdgeIteratorFilter`) before casting. `EMPTY_EDGE_LIST.iterator()` returns `Collections.emptyIterator()` which is not a `ResettableIterator`, so it returns an empty iterator instead.

**EdgeToVertexIterator:** change field type to `ResettableIterator<Edge>` and fix `next()` to use the opposite direction (`direction == OUT ? IN : OUT`), which correctly returns the neighbor vertex.

## Files Changed

- `engine/src/main/java/com/arcadedb/graph/EdgeToVertexIterable.java` - guard against non-ResettableIterator (empty edge list)
- `engine/src/main/java/com/arcadedb/graph/EdgeToVertexIterator.java` - accept `ResettableIterator<Edge>`; fix `next()` to return the opposite-end vertex
- `engine/src/test/java/com/arcadedb/function/sql/graph/SQLFunctionShortestPathTest.java` - regression test for `edge:true` + `direction:BOTH` with asymmetric edges

## Verification

Run: `mvn test -pl engine -Dtest=SQLFunctionShortestPathTest`

Results: 14 tests run, 0 failures, 0 errors.

## PR

https://github.com/ArcadeData/arcadedb/pull/4646

## Review cycles

### Cycle 1 - HEAD 73b365f4 (gemini-code-assist review + claude review)

Applied:
- Gemini (high): `EdgeToVertexIterable` now throws `IllegalArgumentException` for a non-empty, non-resettable iterator instead of silently returning an empty iterator (with explanatory comment - also subsumes Claude's "add a comment" suggestion).
- Claude: extracted the direction flip in `EdgeToVertexIterator.next()` to a `neighborEnd` local variable.
- Claude: regression test now asserts the middle element equals the edge RID.
- Claude: added `edgeTrueDirectionBothReverseAsymmetric` (search b->a, empty OUT side exercises the IN half).
- Claude: added `edgeTrueDirectionIn` (pure IN direction with edge:true).

Skipped with rationale:
- Claude suggested removing this `docs/4645-*.md` file. Kept it: the `resolve-issue` workflow creates this tracking doc by design and folds review-cycle history into it. The bot is unaware of this committed convention.

### Cycle 2 - HEAD 705af13ba

- claude-review workflow re-ran on 705af13ba and completed clean (no new actionable comments).
- gemini's consumer bot (being sunset) did not auto re-trigger on the new SHA; its single cycle-1 inline concern (silent-swallow on `EdgeToVertexIterable`) was already resolved in cycle 1 and a resolution reply was posted in the thread.
- No code changes required.

## CI status triage (HEAD 705af13ba)

7 CI test failures, all confirmed unrelated to this change (which only touches the shortestPath edge-to-vertex traversal path):

Pre-existing on main (HEAD 898aebe60, verified by running on a clean main checkout):
- `LockFilesInOrderFileMigrationTest.lockFilesInOrderThrowsWithMigrationMessageWhenFileMigratedByCompaction`
- `SQLVectorDatabaseFunctionsTest.phase6VectorStatistics`
- `SQLFunctionSearchFieldsMoreTest.nonExistentRID`

Flaky in CI (pass deterministically locally on this branch):
- `DatabaseRIDTest.bareRidThrowsWhenNoActiveDatabaseContext`
- `LSMVectorIndexRebuildTest.timerShouldResetOnNewMutations`
- `QueryEngineManagerPoolTest.submittedTasksRunOnPoolThread`
- `Issue4510ForceApplyPartialDeltaTest.forceApplyFullPageOverVersionGapIsApplied`

Change-specific verification: `SQLFunctionShortestPathTest` (14 tests) and the broader graph suite (342 tests) all pass locally.

## Final state

clean-approval - all actionable bot feedback addressed; cycle-2 re-review clean; CI failures triaged as pre-existing/flaky and unrelated. Merge is the developer's decision.
