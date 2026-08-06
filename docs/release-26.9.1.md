# ArcadeDB v.26.9.1 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.9.1 development cycle, so the release notes are ready at tag time.

## A pathologically nested or long Cypher expression is now a parse error, not a StackOverflowError (#5851)

A ~2KB `WHERE` clause with about 1000 nested parentheses crashed the parsing thread with a
`StackOverflowError`: the ANTLR-generated Cypher parser re-enters its `expression` grammar rule, and walks
its full ~10-level operator-precedence cascade, once per nesting level, so a few thousand levels is enough
to exhaust the default JVM thread stack. `AbstractServerHttpHandler` catches `Throwable`, so this degraded
an HTTP request to a 500 rather than killing the worker, but a malformed query should fail with an ordinary
400, and letting an `Error` unwind through the engine on a path no database state has touched is worth
avoiding on its own.

Investigation found the same crash from two independent recursion sites, not one. Nested parentheses (and
equally, list/map literals or function arguments nested inside one another) recurse in the ANTLR-generated
parser itself. A long *flat* chain - thousands of `OR`'d or string-concatenated terms - does not: the
grammar's `(OR expression11)*` and `(PLUS expression5)*` productions are quantifier loops, not
self-referencing rules, so they parse without recursing. They still crashed, one level further down the
pipeline: `ExpressionRewriter`, the shared visitor every `WHERE` condition is normalized/folded/simplified
through, walks the resulting deep expression tree recursively.

Both sites are now bounded by the same new setting, `arcadedb.cypher.maxExpressionDepth` (default 200),
converting either crash into a `CommandParsingException` that names the limit and the setting to raise if a
legitimate query needs it. Real-world queries essentially never nest this deep, so the default leaves a
wide margin; the SQL parser was never at risk from the same input because its hand-written recursive-descent
implementation costs far fewer stack frames per nesting level, not because it enforces a limit of its own.

## Vector index: the location index is laid out in primitive arrays, ~3x less heap

The `LSM_VECTOR` location index is the only mapping from a vector id to the record it belongs to and to the byte
offset of its entry in the index file. Nothing on disk reproduces it, so since 26.8.1 it cannot evict and its size
is a hard requirement rather than a tunable. It used to hold that mapping in a `ConcurrentHashMap<Integer,
VectorLocation>` plus a `ConcurrentHashMap<RID, int[]>` reverse index, which retained about **90 bytes per live
vector** to carry about 20 bytes of payload.

It is now four primitive arrays indexed by vector id - the file offset with its compacted flag packed into one
`long`, the RID's bucket id and position, and one presence bit - at about **21 bytes per id, plus 8 to 16 bytes per
live vector** for the reverse index. A 10M-vector index goes from roughly 900MB resident to roughly 320MB, and the
garbage collector no longer traces one object graph per indexed vector.

- **`estimatedLocationIndexBytes` is now measured, not multiplied out.** It reports the heap the index's own arrays
  occupy instead of `live count x a per-entry constant`. On the same index it steps **down about 3x**. Re-base any
  dashboard or alert wired to it.
  If you tracked this stat across 26.8.1, note that it moved twice in consecutive releases and only one of the two
  moves describes the index: the 3.75x increase in 26.8.1 (24 -> 90 bytes per entry) was an accounting correction
  from the payload size to the retained size, with nothing about the memory it describes having changed. This one is
  a real reduction.
- **The arrays are chunked, and a chunk is released as soon as its last live id is tombstoned.** That preserves the
  property 26.8.1 introduced: residency follows the live vectors, not the ids handed out. A workload that re-embeds
  the same vectors hands out ids monotonically and tombstones the ones it supersedes - 9.3M ids for a 4K live set in
  the case that motivated it - and a flat array indexed by id would have followed the id space instead.
- **The reverse index stores no keys.** A candidate is verified by reading the vector id's RID back out of the
  location arrays, which makes a tombstoned id structurally unreachable through it rather than something callers
  have to re-check.
- **The graph-build snapshot is the same structure now.** A rebuild used to copy the whole live set into a
  `Map<Integer, VectorLocation>` for the duration of the build, on top of the live index and at the same per-vector
  cost. On a 10M-vector index that was a transient ~900MB spike; it is now ~200MB.
- **No new search allocation.** Every reader was moved onto accessors that answer one field without materializing
  anything, so the liveness filter each search applies per traversed graph ordinal costs one presence bit as before.
- `ArcadePageVectorValues` is built through `forSearch(...)` / `forGraphBuild(...)` factories instead of six
  constructors: the two roles now take the same argument types and differ only in whether the reader may
  short-circuit to the vectors persisted inline in the graph file.

[#5588](https://github.com/ArcadeData/arcadedb/issues/5588)
