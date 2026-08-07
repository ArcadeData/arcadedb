# Issue #5667: GraphBatch never promotes super-nodes, and hard-fails on an already-promoted vertex

## Root cause

`GraphBatch` (the bulk graph importer) manages edge segments directly, bypassing
`EdgeLinkedList`/`StripedEdgeList`. It never calls `EdgeLinkedList.tryPromoteToSuperNode()`, so a
bulk-loaded hub never gets the striped super-node layout (#5156) no matter how many edges land on it.

Worse, if the graph *already* contains a promoted vertex (created through the standard API, or by a
previous non-bulk write), resuming a bulk load over it used to fail outright:

- **Sequential flush** (`getOrCreateOutSegmentDeferred` / `getOrCreateInSegmentDeferred`, used by
  `connectOutgoingEdgesSorted` / `connectIncomingEdgesSequential`): threw a documented
  `IllegalStateException` ("Bulk edge import into the super-node promoted vertex ... is not supported").
- **Parallel flush** (`getOrCreateOutEdgeChunk` / `getOrCreateInEdgeChunk`, used by
  `connectOutEdgesRangeLocal` / `connectIncomingEdgesRangeLocal` via the async executor): had **no**
  guard at all - it blindly cast the vertex's head chunk record to `EdgeSegment`, which threw a raw,
  undocumented `ClassCastException` (`StripeDirectory` cannot be cast to `EdgeSegment`) instead.

## Scope of this fix

This PR fixes the **hard failure** (both shapes above, sequential and parallel), which is the more
severe half of the issue - an unhandled exception that aborts the whole batch mid-import, arriving after
potentially hours of work. Edges for an already-promoted vertex are now routed through the standard,
MVCC-safe `StripedEdgeList` write path instead of being rejected.

**Out of scope**: `GraphBatch` still does **not** promote a vertex to the super-node layout *during* a
bulk load - a very high-degree vertex loaded entirely through `GraphBatch` still ends up as a long
chained-segment list rather than striped. Implementing promotion inside GraphBatch's own bulk
segment-management code (four independent overflow call sites across OUT/IN x sequential/parallel, each
needing correct cross-flush degree tracking) is a materially larger change with its own performance and
correctness surface; it is intentionally left as a follow-up. The `GraphBatch` class javadoc now
documents this limitation explicitly, and points at `arcadedb.graph.supernodeThreshold=0` as the
database-wide workaround if a bulk-loaded super-node's degraded traversal performance is a concern.

## Changes

`engine/src/main/java/com/arcadedb/graph/GraphBatch.java`:

- `getOrCreateOutSegmentDeferred` / `getOrCreateInSegmentDeferred` (sequential flush): instead of
  throwing, set new instance fields (`lastSegmentPromoted`, `lastPromotedVertex`,
  `lastPromotedDirectory`) and return `null`. Safe as instance-field signalling because these two methods
  run only on the single-threaded sequential connect path.
- `connectOutgoingEdgesSorted` / `connectIncomingEdgesSequential`: check `lastSegmentPromoted` first and,
  if set, route the group through the new `addGroupThroughStripedEdgeList` helper instead of the bulk
  segment write.
- `connectOutEdgesRangeLocal` / `connectIncomingEdgesRangeLocal` (parallel flush, runs concurrently
  across async-executor threads): check for a `StripeDirectory` head **locally** (no shared instance
  state - see the thread-safety note below) before calling `getOrCreateOutEdgeChunk` /
  `getOrCreateInEdgeChunk`, and route through `addGroupThroughStripedEdgeList` when found.
  `getOrCreateOutEdgeChunk` / `getOrCreateInEdgeChunk` themselves are left unchanged (no
  `StripeDirectory` awareness) since the promoted case is now fully handled by their callers.
- New helper `addGroupThroughStripedEdgeList`: constructs a `StripedEdgeList` over the resolved vertex +
  direction + directory and calls its `add()` once per buffered edge in the group.
- Class javadoc: documents that GraphBatch never promotes during bulk load, that it resumes correctly
  over pre-existing promoted vertices, and the `supernodeThreshold=0` workaround.

### Thread-safety pitfall caught during development

The first version of this fix used the shared `lastSegmentPromoted` instance-field pattern for
`getOrCreateOutEdgeChunk` / `getOrCreateInEdgeChunk` too, mirroring the sequential-path methods. That is
wrong: those two methods are also called from `connectOutEdgesRangeLocal` /
`connectIncomingEdgesRangeLocal`, which the parallel flush path dispatches to the async executor -
**multiple buckets run concurrently on different threads**. A shared mutable instance field read
immediately after being set raced across threads and silently attributed one group's promoted vertex to
a different, unrelated group, inflating an edge count by one in testing. Fixed by keeping the parallel
path's promotion check entirely in local variables, never touching shared state. See the regression
test's parallel-flush variant, which reproduces both the original `ClassCastException` (fix reverted) and
this thread-safety bug (during development, not present in the final diff).

## Tests

`engine/src/test/java/com/arcadedb/graph/Issue5667GraphBatchSuperNodeResumeTest.java` (new):

- `sequentialFlushResumesOverPromotedHub` - pre-promotes a hub's OUT and IN lists via the standard API,
  then bulk-loads one more edge in each direction with `parallelFlush(false)`. Verifies no exception, both
  edges land, both lists remain `StripeDirectory`.
- `parallelFlushResumesOverPromotedHub` - same scenario with `parallelFlush(true)` (the default), exercising
  the async range-local path.

Both tests were verified to fail with the pre-fix code: the sequential test reproduces the documented
`IllegalStateException`, the parallel test reproduces the previously-undocumented `ClassCastException`
(`StripeDirectory` -> `EdgeSegment`).

## Verification run

- `mvn -q -pl engine -am compile` / `test-compile`: clean.
- Targeted: `Issue5667GraphBatchSuperNodeResumeTest` - 2/2 pass.
- Regression sweep: `GraphBatchTest`, `GraphBatchCommitRetryTest`, `GraphBatchUniqueIndexTest`,
  `GraphBatchWALRestoreTest`, `SuperNodeStripingTest`, `SuperNodeDefaultThresholdTest`,
  `SuperNodeBothSizeQueryTest`, `Issue5147SuperNodeChunkRaceTest`, `Issue5666ConcurrentGraphBatchTest` -
  47/47 pass.
- Full `com.arcadedb.graph.*Test` sweep (`-DexcludedGroups=slow,benchmark`) - 208/208 pass, 0
  failures/errors.
