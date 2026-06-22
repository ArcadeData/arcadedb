# Issue #4689 - Cypher MATCH ... RETURN vertex throws NoSuchElementException

## Summary

`MATCH (u:User) RETURN u` throws `java.util.NoSuchElementException` / PostCommandHandler error,
while `MATCH (u:User) RETURN u{.*} as user` works. Same issue with SQL (`SELECT FROM User` fails,
`SELECT field FROM User` works).

Reported in `arcadedata/arcadedb:26.7.1-SNAPSHOT`.

## Analysis

### What the error means

The error path is:
1. NSE thrown inside `PostCommandHandler.execute()` (specifically during result serialization)
2. Caught by `database.transaction()` lambda → wrapped as `TransactionException("Error on executing command", NSE)`
3. `AbstractServerHttpHandler.handleHttp()` catches TransactionException
4. Logs: `"Error on transaction execution (PostCommandHandler): Error on executing command"`
5. Sends HTTP 500 with `"exception": "java.util.NoSuchElementException"` in response body

### Differences between failing and working queries

- `RETURN u` (fails): creates `ResultInternal` with `element=vertex, content={"u":vertex}, _projectionName="u"`
  - Uses element-based serialization path: `setMetadata(vertex)` + early-return via `document.toMap()`
- `RETURN u{.*} as user` (works): creates `ResultInternal` with `content={"user": map}`, no element
  - Uses projection-based serialization path: iterates content properties

### Exhaustive investigation

Investigated the following code paths for NSE sources:

1. **FinalProjectionStep.filterResult()** - no NSE possible; logic creates dual element+content result
2. **FinalProjectionStep.fetchMore()** - safe; uses hasNext() before next()
3. **NodeByLabelScan.execute()** - safe buffer/iterator pattern
4. **MatchNodeStep.fetchMore()** - safe; uses hasNext() before next()
5. **JsonSerializer.serializeResult()** - uses early-return path for _projectionName; no NSE
6. **AbstractQueryHandler.serializeResultSet()** - safe; stream uses tryAdvance()
7. **EdgeIterator/EdgeLinkedList** - safe; uses hasNext() before next()
8. **MultiIterator** - safe; hasNextInternal() guards limit
9. **GraphEngine.countEdges()** - no NSE
10. **FetchFromTypeExecutionStep.syncPullSequential()** - known double-syncPull inefficiency but no NSE for normal scenarios
11. **FetchFromTypeExecutionStep.syncPullParallel()** - safe; parallelScan=false when transaction active

### Structural inconsistency in FinalProjectionStep

When `RETURN u` returns a single Document, `FinalProjectionStep.filterResult()` sets BOTH:
- `content = {"u": vertex}` (via setProperty)
- `element = vertex` (via setElement)

This dual state creates `getPropertyNames()` vs `getProperty()` inconsistency:
- `getPropertyNames()` = element's own properties (name, age) UNION content keys (u)
- `getProperty("name")` returns null (content has "u", not "name")

The serialization works because `serializeResult` uses the `_projectionName` early-return path that calls `document.toMap()` directly, bypassing the inconsistency.

### Reproduction

Extensive testing with the following scenarios - ALL PASS:
- Simple vertices (name + age properties)
- 110 vertices (tests >100 batch pagination)
- Vertices with connected edges
- Default (non-studio) serializer
- Studio serializer
- SQL SELECT FROM type

### Conclusion

The NSE cannot be reproduced with the current codebase. The issue may be:
1. Environment/data-specific (certain property types or database state)
2. Fixed by a recent commit before the investigation
3. Related to a specific client configuration not covered by tests

## Changes Made

Added regression test `Issue4689MatchReturnVertexIT` covering:
- `MATCH (u:IssueUser) RETURN u` - exact failing query from issue
- `MATCH (u:IssueUser) RETURN u{.*} as user` - working workaround
- SQL `SELECT FROM V1` - equivalent SQL case
- SQL `SELECT name FROM V1` - SQL field selection
- 110-vertex test (pagination boundary)
- Vertices with edges
- Default (non-studio) HTTP serializer path

## Test Results

All 7 regression tests pass.

## Location

`server/src/test/java/com/arcadedb/server/http/handler/Issue4689MatchReturnVertexIT.java`
