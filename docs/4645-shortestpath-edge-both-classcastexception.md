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

Results: 12 tests run, 0 failures, 0 errors.
