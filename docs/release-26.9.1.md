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

### Follow-up: the chain-length gap, and a matching guard for SQL

Two things turned out to be wrong in the paragraph above, found while auditing for the same class of bug
elsewhere in both engines.

First, the Cypher fix was incomplete. `ExpressionRewriter`'s guard only fires from
`CypherASTBuilder#visitWhereClause`, so a long OR/AND/NOT/comparison/arithmetic chain written anywhere
*other* than a top-level `WHERE` condition - a `RETURN` projection, an `ORDER BY` item, a function
argument - was never rewritten and so never hit that guard. A 30000-term OR chain in a `RETURN` projection
still overflowed the stack, this time inside `CypherSemanticValidator#checkExpressionScope` - a completely
different recursive walker from `ExpressionRewriter`, invoked during semantic validation on every clause.
Chasing down and patching every such walker individually is exactly the kind of fragile, incomplete-prone
fix that let this one through in the first place, so `CypherExpressionDepthGuard` (the same `ParseTreeListener`
already attached to the parser for nesting depth) now also bounds the term count of every `(OP operand)*`-shaped
grammar rule directly - OR, XOR, AND, `NOT*`, chained comparisons, and the three arithmetic precedence
levels - using each rule's own generated accessor, exactly as every AST builder does. This rejects an
oversized chain during parsing, before any tree is built, regardless of which clause it is in and regardless
of which pass would eventually have walked it.

Second, "the SQL parser was never at risk... not because it enforces a limit of its own" undersold the
actual risk: it isn't at risk of a `StackOverflowError` from nested parentheses (confirmed - its grammar
costs far fewer Java stack frames per nesting level, exactly as claimed), but it is at risk of something
worse. The production SQL parser is `SQLAntlrParser` (ANTLR4-based, like Cypher's), which resolves the
ambiguity between the several grammar rules that all start with a bare `(` - a parenthesized expression,
condition, or sub-statement - by trying a fast SLL prediction first and falling back to full ALL(*)
prediction on failure. For deeply nested parentheses that fallback's cost grows steeply enough that a query
of only a few KB (about 6000 nested parentheses) tied up a worker thread for well over two minutes of CPU
without ever crashing - worse than a fast error, since a slow hang is not distinguishable from a legitimately
slow query. `SQLAntlrParser` now counts parenthesis nesting on the token stream before attempting any parse,
rejecting a query past `arcadedb.sql.maxExpressionDepth` (default 200) in O(n) time - the same depth that
previously burned minutes of CPU is now rejected in single-digit milliseconds. Counting on the token stream
rather than raw characters means a `(` inside a string literal or comment is not miscounted.

## Dense vector graph build: an unconditional counter on the distance path, a build pool half the box, and a progress meter that lied (#5577)

Three findings from a rigorously measured external report on a DEEP-10M (9.99M vectors, 96 dimensions) dense
build. Only one of them is what the issue was opened about.

### The hit/miss counters cost about a quarter of the build

`VectorCache.get()` incremented one of two process-wide `LongAdder`s on every lookup, and a lookup here backs a
single distance evaluation - the class Javadoc says as much. A profile of the graph build attributed **26.4% of
the entire build to `LongAdder.add`**, every sample of it reached through `VectorCache.get`, against 42.1% for
the SIMD distance function the lookups exist to feed. That ratio is not surprising once stated plainly: a locked
compare-and-swap and a 96-dimension SIMD distance are the same order of magnitude, and the CAS gets worse as the
build pool widens while the distance does not.

`LongAdder` is the right structure for a contended counter; the mistake was having any atomic counter on that
path. The counters are now striped one pair per thread onto its own cache line and incremented with plain reads
and writes (opaque access, so a 64-bit value can never be observed torn, but no ordering and no bus lock). An
isolated benchmark of the lookup path - `VectorCacheCounterBenchmark`, `@Tag("benchmark")` - measures 2-5x less
time per lookup depending on thread count. The totals are now approximate: two threads landing on the same
stripe can lose an increment, so a count can come out low but never high. That is the right trade for a
diagnostic counter, and `LongAdder.sum()` was never an atomic snapshot either.

The two dead `AtomicLong` cache counters in `LSMVectorIndexMetrics`, which nothing incremented and whose values
`getStats()` overwrote from the real cache, are gone.

### The build pool was `availableProcessors() / 2`

It was, and it was never a measured choice - it arrived in "fix: use own pool for vector index rebuild" (the
pool exists so a build can be cancelled on close, since JVector's builder only observes an interrupt on its
workers). The reporter's A/B priced the halving at **17.1% of the whole build with recall unchanged**
(0.9502 against 0.9526).

The automatic width is now the core count minus one, and the new setting
`arcadedb.vectorIndex.graphBuildParallelism` overrides it. The core left free is deliberate and is the only
reason not to take them all: a rebuild can fire on a live index at any time and must not be able to occupy
every core the request, I/O and GC threads need. A bulk import with nothing else running should raise it to the
full core count; a latency-sensitive index that must not feel an online rebuild should lower it. The effective
width is reported as `graphBuildParallelism` in `getStats()`.

### There was no silent second phase - the progress meter was saturating

The report described a phase worth ~53% of the build that emitted no log line, ran at full CPU, and did not
respond to threads (1407 s on 6 threads against 1442 s on 12, while the phase next to it scaled 1.92x). Three
rounds of profiling went into explaining it.

It does not exist. The progress monitor polled JVector's `getIdUpperBound()`, which is "highest node id touched
so far + 1", and insertion runs as `IntStream.range(0, size).parallel()`, so a worker reaches the top of the
range within seconds. The meter pinned at 100% with nearly the whole build ahead of it - the first progress line
of a 10M build read 90.7%, and the last one claimed completion with 23 minutes of work left. The "phase
boundary" was the meter saturating, and because it saturates after a fixed amount of *range traversal* rather
than a fixed amount of *work*, it moves earlier in wall-clock time as threads are added. That is what produced
the impossible-looking 1.92x/1.00x split: summing both halves, JVector's real scaling on that A/B is 1.23x.

The build now drives JVector's insertion and its `cleanup()` pass itself (the same two steps `build()` performs,
on the same pool) and counts insertions that have actually returned. Consequences:

- `processedNodes` means what it says, and `processedNodes + insertsInProgress` can no longer exceed the corpus
  size - the invariant the old meter broke, and what the new regression test asserts;
- the boundary between the two phases is logged, with the elapsed time of each, so the post-insertion pass is
  measurable instead of inferable from an absence of output;
- `GraphBuildCallback` gains an `optimizing` phase for that pass. It is not a quick finalisation: on a large
  corpus it can cost as much wall clock as the insertion it follows, and callers that render a progress bar
  should show it rather than reporting the build as complete.
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
- **`countEntries()` on a dense `LSM_VECTOR` is a popcount** over the presence bits - one word per 128 ids -
  instead of a stream over the whole location map.
- **`VectorLocationIndex.getVectorIdsForRid` returns only LIVE ids now, in ascending order.** It used to return
  tombstoned ids as well and require the caller to re-check the location's `deleted` flag. Nothing in the engine
  is affected - both callers re-verified anyway - but the contract of a public method inverted, so embedding code
  that relied on seeing tombstoned ids has to look elsewhere for them (`isDeleted(int)`).
- `ArcadePageVectorValues` is built through `forSearch(...)` / `forGraphBuild(...)` factories instead of six
  constructors: the two roles now take the same argument types and differ only in whether the reader may
  short-circuit to the vectors persisted inline in the graph file.

[#5588](https://github.com/ArcadeData/arcadedb/issues/5588)
