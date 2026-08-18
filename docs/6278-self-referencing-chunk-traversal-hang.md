# Issue #6278: A self-referencing edge-list chunk hangs ordinary traversals

## Root cause

`EdgeVertexIterator.hasNext()` gained a guard in #6276 (issue #6062) against a chunk whose `previous`
pointer names itself: `EdgeLinkedList.containsEdge`/`containsVertex`/`containsLightEdge`/
`getFirstEdgeConnectedToVertex` had always broken out of such a chain, but the entry iterator behind
`EdgeLinkedList.entryIterator()` - what `CHECK DATABASE`'s adjacency probe cache walks a vertex's own
edge list through - did not, so that shape of corruption hung the check instead of letting it report
the damage.

#6276 deliberately left the sibling chunk-hopping iterators alone, since nothing in that PR consumed
them: `EdgeIterator`, `VertexIterator`, `RIDIterator`, and `IteratorFilterBase` (backing
`EdgeIteratorFilter`, `VertexIteratorFilter`, `RIDIteratorFilter`, `EdgeVertexIteratorFilter`). These
back the ordinary traversal APIs - `Vertex.getEdges()`, `getVertices()`,
`GraphEngine.getConnectedVertexRIDs()`, and the SQL/Cypher expansion steps - so the same corruption
still hung an ordinary query in a request thread, even though `CHECK DATABASE` could now survive it.

## Fix

Lifted the guard out of `EdgeVertexIterator.hasNext()` into a shared chunk-hop,
`ResettableIteratorBase.moveToPreviousChunk()`, and switched every chunk-hopping iterator to call it
instead of hopping `currentContainer` directly:

- `EdgeVertexIterator`
- `EdgeIterator`
- `VertexIterator`
- `RIDIterator`
- `IteratorFilterBase` (covers all four `*IteratorFilter` subclasses)

`moveToPreviousChunk()` compares the identity of the chunk being left against the identity of the chunk
hopped to; when they match (a chunk whose `previous` pointer names itself) it sets `currentContainer` to
`null` and the walk ends, matching the existing `EdgeLinkedList` probes and the #6276 guard - a self-loop
ENDS the walk silently rather than being reported, since none of the direct-probe guards it is modeled
on log either. The comparison is one RID identity check per chunk hop, not per entry, so it costs
nothing on the healthy path.

## Tests

New file `engine/src/test/java/com/arcadedb/graph/Issue6278SelfReferencingChunkTraversalIteratorsTest.java`,
modeled on `Issue6062AdjacencyProbeCacheTest.aSelfReferencingChunkEndsTheWalkInsteadOfFeedingItForever`:
plants a self-referencing head chunk (`segment.setPrevious(segment)`) on a hub vertex's IN edge list,
then walks each iterator family under a HARD CAP (`8 * DEGREE`) rather than a timeout - an unterminating
walk hangs the build, not the test, so the assertion is on how many entries were yielded, not on elapsed
time.

Four tests, one per iterator family reached through `EdgeLinkedList`:

- `edgeIteratorEndsTheWalkInsteadOfFeedingItForever` - `EdgeLinkedList.edgeIterator()` -> `EdgeIterator`
- `vertexIteratorEndsTheWalkInsteadOfFeedingItForever` - `EdgeLinkedList.vertexIterator()` -> `VertexIterator`
- `ridIteratorEndsTheWalkInsteadOfFeedingItForever` - `EdgeLinkedList.ridIterator()` -> `RIDIterator`
- `edgeIteratorFilterEndsTheWalkInsteadOfFeedingItForever` - `EdgeLinkedList.edgeIterator(EDGE_TYPE)` ->
  `EdgeIteratorFilter` -> `IteratorFilterBase`

**Proved the tests can fail**: temporarily short-circuited the self-loop check in
`moveToPreviousChunk()` (`if (false && ...)`) and reran - all 4 new tests failed with
`AssertionError` (walked count exceeded `DEGREE`), confirming they are not vacuously green. Restored the
guard afterward; all 4 pass again.

## Verification

- `mvn -pl engine -am compile` - clean compile.
- `mvn -pl engine -am test -Dtest=Issue6278SelfReferencingChunkTraversalIteratorsTest,Issue6062AdjacencyProbeCacheTest`
  - 12/12 pass.
- `mvn -pl engine -am test -Dtest=com.arcadedb.graph.*Test -DexcludedGroups=benchmark,slow,vector`
  - full `com.arcadedb.graph` package: 290/290 pass, no regressions.

## Files changed

- `engine/src/main/java/com/arcadedb/graph/ResettableIteratorBase.java` - new shared
  `moveToPreviousChunk()`.
- `engine/src/main/java/com/arcadedb/graph/EdgeVertexIterator.java` - use the shared method (was the
  original, private copy from #6276).
- `engine/src/main/java/com/arcadedb/graph/EdgeIterator.java` - use the shared method.
- `engine/src/main/java/com/arcadedb/graph/VertexIterator.java` - use the shared method.
- `engine/src/main/java/com/arcadedb/graph/RIDIterator.java` - use the shared method.
- `engine/src/main/java/com/arcadedb/graph/IteratorFilterBase.java` - use the shared method.
- `engine/src/test/java/com/arcadedb/graph/Issue6278SelfReferencingChunkTraversalIteratorsTest.java` -
  new regression test.
