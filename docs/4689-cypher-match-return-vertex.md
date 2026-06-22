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
2. Caught by `database.transaction()` lambda wrapped as `TransactionException("Error on executing command", NSE)`
3. `AbstractServerHttpHandler.handleHttp()` catches TransactionException
4. Logs: `"Error on transaction execution (PostCommandHandler): Error on executing command"`
5. Sends HTTP 500 with `"exception": "java.util.NoSuchElementException"` in response body

### Differences between failing and working queries

- `RETURN u` (fails): creates `ResultInternal` with `element=vertex, content={"u":vertex}, _projectionName="u"`
  - Uses element-based serialization path: `setMetadata(vertex)` + early-return via `document.toMap()`
- `RETURN u{.*} as user` (works): creates `ResultInternal` with `content={"user": map}`, no element
  - Uses projection-based serialization path: iterates content properties

### Structural inconsistency in FinalProjectionStep

When `RETURN u` returns a single Document, `FinalProjectionStep.filterResult()` sets BOTH:
- `content = {"u": vertex}` (via setProperty)
- `element = vertex` (via setElement)

This dual state creates `getPropertyNames()` vs `getProperty()` inconsistency:
- `getPropertyNames()` = element's own properties (name, age) UNION content keys (u)
- `getProperty("name")` returns null (content has "u", not "name")

The serialization works because `serializeResult` uses the `_projectionName` early-return path that calls `document.toMap()` directly, bypassing the inconsistency. This is a latent fragility worth tracking as a follow-up.

### Exhaustive investigation

Investigated through FinalProjectionStep, NodeByLabelScan, MatchNodeStep, JsonSerializer, HTTP
serialization layer, EdgeIterator, MultiIterator, GraphEngine.countEdges, and
FetchFromTypeExecutionStep. All code paths appear correct and all regression tests pass.

### Conclusion

The NSE cannot be reproduced with the current codebase. The issue may be:
1. Environment/data-specific (certain property types or database state)
2. Fixed by a recent commit before the investigation
3. Related to a specific client configuration not covered by tests

## Changes Made

Added regression test `Issue4689MatchReturnVertexIT` covering:
- `MATCH (u:IssueUser) RETURN u` - exact failing query from issue
- `MATCH (u:IssueUserProj) RETURN u{.*} as user` - working workaround
- SQL `SELECT FROM SqlSelectAll` - equivalent SQL case (self-contained with own type)
- SQL `SELECT name FROM SqlSelectFields` - SQL field selection (self-contained)
- 110-vertex UNWIND test (pagination boundary)
- Vertices with edges (edge-count validation via setMetadata)
- Default (non-studio) HTTP serializer path

## Test Results

All 7 regression tests pass.

## Location

`server/src/test/java/com/arcadedb/server/http/handler/Issue4689MatchReturnVertexIT.java`

---

## PR

https://github.com/ArcadeData/arcadedb/pull/4690

## Review Cycles

### Cycle 1 - HEAD d3ed3e307

**Changes applied from review:**
- Gemini (HIGH x2): Made `sqlSelectFromTypeShouldNotThrow` and `sqlSelectFieldsShouldWork` self-contained with own vertex types (SqlSelectAll, SqlSelectFields)
- Gemini (MEDIUM): Updated URL to use `getDatabaseName()` instead of hardcoded "graph"
- Claude (CORRECTNESS): `cypherMatchReturnVertexProjectionWorkaround` now uses own isolated type (IssueUserProj)
- Claude (CORRECTNESS): Added record count assertions where data is explicitly created
- Claude (STYLE): Replaced FQN imports with proper import statements
- Claude (PERFORMANCE): Added `@Tag("slow")` to 110-vertex test
- PR title updated from `fix(#4689)` to `test(#4689)`

**Deferred items:**
- Claude: Remove tracking doc from repo (contradicts established convention; docs/4274-*, docs/4275-*, docs/4278-* are all committed tracking docs)
- Claude: File follow-up issue for FinalProjectionStep dual element+content state (out of scope for this PR; documented in PR description and this tracking doc)

### Cycle 2 - HEAD 7afd23064

**Changes applied from review:**
- Claude (CRITICAL): `cypherMatchReturnManyVerticesShouldWork` now uses UNWIND for single-command bulk create and asserts count=110
- Claude (CORRECTNESS): `cypherMatchReturnVertexWithEdgesShouldWork` validates edge counts in @out/@in
- Claude (MINOR): Trimmed class-level Javadoc to single line
- Removed `@Tag("slow")` since UNWIND replaced the slow loop
- Gemini carried forward same stale comments (already addressed, replied)

**Deferred items:**
- Claude: Remove tracking doc (same as cycle 1, same rationale)

### Cycle 3 - HEAD 64e6805a2

No new actionable items. Claude had no new review on this SHA. Gemini had only the same carried-forward stale comments about V1 (already addressed and replied). Working tree clean.

## Final State

`clean-approval` after 3 cycles.

**Deferred items (for developer follow-up):**
1. File follow-up issue for `FinalProjectionStep.filterResult()` dual element+content state - latent fragility documented above.
