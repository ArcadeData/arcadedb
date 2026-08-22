# ArcadeDB v.26.9.1 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.9.1 development cycle, so the release notes are ready at tag time.

## Postgres wire: `SHORT`/`BYTE`, `DATE`, `DATETIME` and `DECIMAL` no longer change type depending on whether a row was sampled (#6447)

A RowDescription column is typed either from a sample value, when the result set has a row, or from the declared
schema, when it does not - and the two paths have to agree, or a client that prepares against an empty result and
then re-executes against a populated one sees the column's OID change under it. #6411 fixed this for `BINARY`
(`bytea`); four more types had the same shape of bug.

`DECIMAL` was the worst of the four: the schema path answered `DOUBLE`, lossy for an arbitrary-precision
`BigDecimal`, and the value path answered `VARCHAR`, which makes a client treat the column as text. Neither was
right - PostgreSQL already has the correct type for this, `NUMERIC` (OID 1700) - so both paths now point at a new
`PostgresType.NUMERIC` entry with a real binary codec (PostgreSQL's own digit-group-of-4 wire format, not a
lossy float64 detour), plus a text encoder that renders a `BigDecimal` as a plain decimal string rather than
`BigDecimal.toString()`'s occasional scientific notation. Both the codec's decode side (attacker-controlled wire
bytes) and encode side (a `BigDecimal` of unbounded scale or magnitude, which ArcadeDB itself can produce) reject
an out-of-range digit count rather than silently wrapping it through the wire format's `int16` header fields, and
do so before any work proportional to that size runs. The cap - 16,000 total decimal digits - is deliberately
well under real PostgreSQL's own `NUMERIC` limit (131,072 integer digits, 16,383 fractional): `DECIMAL` has no
configured precision/scale limit in ArcadeDB, so this is the boundary between "large value real Postgres would
also accept" and "value this protocol declines" rather than a hard technical ceiling.

`SHORT`/`BYTE` had a narrower bug: the value path widened `Short`/`Byte` to `INTEGER` because there was nowhere
narrower to point it, while the schema path already answered the correct `SMALLINT` (`int2`). The value path now
answers `SMALLINT` too.

`DATE` and `DATETIME` both trace back to the same root cause, from two different directions. `Type.DATE`'s and
`Type.DATETIME`'s runtime representations are configurable per database (`GlobalConfiguration.DATE_IMPLEMENTATION`
/`DATE_TIME_IMPLEMENTATION`) and default to `LocalDate`/`LocalDateTime` - not `java.util.Date`. The value path had
no case at all for a bare `LocalDate`, so a `DATE` column fell all the way through to `VARCHAR` on every
default-configured database; it now resolves to `DATE`, matching the schema path. `java.util.Date` remains a
supported alternative implementation for both types, and it is where the two collide: `Date` is `DATE`'s default
representation too, so a `Date`-configured `DATETIME` column's sampled value cannot be told apart from `DATE` by
value alone. Rather than adding a wrapper type just to carry that distinction through a value with nothing else to
go on, `PostgresNetworkExecutor.getColumns()` now resolves the ambiguity from the schema - the same mechanism it
already used to type an empty `LIST` column from its declared `LIST OF` element type (#5289). That mechanism only
worked for a whole-entity projection (`SELECT FROM Type`) until this change, though: a query that projects specific
columns (`SELECT col FROM Type`, the shape of essentially every real client query) produces rows that carry only
the projected values, not the backing element the lookup needs - so the schema fallback is now also resolved from
the query's `FROM` target when the row itself isn't one, closing the gap for the query shape it needed to cover
most.

> [!IMPORTANT]
> **Behaviour change.** A populated `SHORT`, `BYTE`, `DATE` or `DECIMAL` column now announces `int2`, `date` or
> `numeric` instead of the `int4`/`varchar`/`double`/`varchar` OID it used to. This corrects a client-visible bug
> (the OID depended on whether the result set happened to be empty, and for `DATE` on a default-configured
> database it was `varchar` unconditionally), but a client relying on the old, disagreeing OID for a non-empty
> result sees a different type name and, for `DECIMAL`, a `BigDecimal` instead of a `Double`/`String`.

## A Cypher node's labels are the ones it answers to, and adding one no longer takes one away (#6363)

`labels()` decided a vertex's labels from a single question - does the type have supertypes - and answered "the
supertypes" when it did, "its own name" when it did not. That rule fits only the synthetic `A~B` composite the
multi-label support builds. Under ordinary type inheritance it was wrong in both directions: a vertex of a type
declared `Manager EXTENDS Employee` reported `["Employee"]`, missing the very label `MATCH (n:Manager)` had just
matched it by, and a type extending a composite reported the internal `Author~Topic` name and neither label it
encodes.

The same list is what `SET`, `REMOVE` and `MERGE` build the new type from, so it was written back:
`MATCH (n:Manager) SET n:Extra` moved the record to a freshly invented `Employee~Extra` and `MATCH (n:Manager)`
then returned **0 rows**. Adding a label removed one, silently.

`labels(n)` now returns every type name the node is an instance of, sorted, minus the synthetic composite names,
so `L IN labels(n)` and `n:L` finally answer the same question. A label write is rebuilt from the node's *own*
labels instead: `SET n:Extra` on a `Manager` builds `Extra~Manager` extending both, so the node stays a `Manager`,
stays an `Employee`, and gains `Extra`. Adding a label the node already answers to changes nothing and is not
counted in `labels-added`.

**Breaking change.** `REMOVE n:Employee` on a vertex of type `Manager EXTENDS Employee` is now refused with a
`CommandSemanticException` (HTTP 400) naming both labels, where it previously appeared to succeed and left the
node in the wrong type. There is no correct outcome for it: no type the vertex could be moved to answers *no* to
`:Employee` while still answering *yes* to `:Manager`. Remove the subtype as well (`REMOVE n:Manager, n:Employee`)
or change the hierarchy in SQL. Removing a label the node simply does not have stays a no-op, as in Neo4j.

Two smaller fixes ride along. A label disjunction anchor `(n:A|B)` was costed as if only `:A` existed while
`NodeByLabelDisjunctionScan` visited every type any alternative accepts, which biased join ordering towards
driving from it; the estimate is now summed over exactly the types the scan walks. And `Schema` gained
`getTypeOrNull(String)`, the non-throwing companion of `getType` that the `existsType(x) ? getType(x) : null`
pairs all over the engine wanted - a `default` method, so an out-of-tree `Schema` implementation keeps working.

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
  cost. On a 10M-vector index that was a transient ~900MB spike; it is now ~200MB. The snapshot preserves the
  source ids, so it has the same density characteristic as the live index: a chunk costs the same whether 1 or 128
  of its ids are live, which is cheaper than the map above roughly 23% density and dearer below it. Monotonic id
  assignment keeps a real workload far above that threshold - see [#5870](https://github.com/ArcadeData/arcadedb/issues/5870)
  for making it unconditional.
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

## Redis wire protocol: an unauthenticated RESP array could overflow the connection thread's stack (#5895)

`RedisNetworkExecutor.parseNext()` decoded RESP arrays recursively, once per nesting level, with no bound
on either the nesting depth or the client-declared element count - and it runs before the `NOAUTH` check,
since the whole message has to be parsed before a command exists to reject. On the default JVM stack, about
47KB of `*1\r\n` repeated ~11,860 times was enough to overflow the connection thread with an uncaught
`StackOverflowError`, reachable by anyone who can open the Redis port, no credentials required. Separately,
an unbounded array-length header such as `*2000000000\r\n` started a parse loop the client could keep alive
indefinitely just by trickling bytes, tying up a connection thread for as long as it liked.

Both are now bounded by two new settings mirroring Redis' own protocol limits: `arcadedb.redis.maxMultiBulkDepth`
(default 32 - no real command nests anywhere close to that) and `arcadedb.redis.maxMultiBulkLength` (default
1,048,576, the same hard cap Redis itself enforces on multibulk requests). Either violation now fails fast with
a RESP `-ERR Protocol error: ...` reply and the connection is closed, since the stream position past a rejected
message can no longer be trusted to resynchronize on.

Code review on the fix caught the same bug class one level down: the RESP bulk-string (`$`) length was equally
unbounded, and unlike the array case it sits on the hot path of essentially every command (the command name and
every argument, including `GET`/`SET`'s own payloads, are bulk strings), so it was arguably the bigger exposure
of the two. A `$2000000000\r\n` header could tie up a connection thread the same way, or grow the parse buffer
without bound if the client actually sent the declared bytes. It is now capped by a third setting,
`arcadedb.redis.maxBulkLength` (default 512MB, matching Redis' own `proto-max-bulk-len`). Malformed, non-numeric
lengths (e.g. `$abc\r\n`) also used to throw an uncaught `NumberFormatException` that killed the connection
thread outright; they now get the same clean error-reply-and-close treatment as an oversized length.

## Bulk load: a batch now accounts for the payload line by line, and says what it does not know (#5618)

A 17-million-vertex load stopped on `Unknown temporary ID 'co/32945720' at line 17234792`, for a vertex sitting in
the payload thousands of lines above that edge - and the vertex was not in the database either. Nothing in the
response and nothing in the log could tell whether the server had lost records or the payload had never carried
them: the endpoint reported counts of what it *created*, with nothing to compare them against. Establishing that
two million vertices were missing took a `grep -c` over the user's own file, and even that did not say which side
had dropped them.

**Every answer now carries `linesRead` and `linesSkipped`** - blank lines, plus CSV headers and `---` separators -
on the 200, on the 400 that rejects a record, and on the 408 that reports a truncated upload. `linesRead -
linesSkipped` is the number of records the parser produced, so a client can check it against `verticesCreated +
edgesCreated` and against the line count of the file it sent, without waiting for a support round-trip.

**The server checks the same equality before answering 200.** A line that was read and turned into nothing is a
defect on our side, so the load is now reported as failed (HTTP 500, with the partial-commit counters, and a
warning naming the numbers) instead of returning a success carrying a count that silently misses records.

**`verticesWithoutId`** is reported when a payload creates vertices that declare no `@id` under the default
`refMode=id`. Those vertices are loaded and durable, but nothing in the payload can ever reference them - until
now the only symptom was an unresolvable reference much further down.

**The unresolved-reference message reports what the load knows** instead of asserting a single cause. "Vertices
must appear before edges that reference them" was the whole explanation, and on this report it was the wrong one;
it now states how many ids the payload actually declared, how many of its vertices declared none, and that each
request resolves only the ids of its *own* payload - the trap a client hits when it splits one file across several
requests and expects `@id`s from an earlier one to still resolve.

Finally, **a batch that lands on a follower is relayed to the leader with the length the client announced**, over
a body that refuses to be handed over twice. The relay previously declared no length, which sent it chunked - and
a chunked body announces nothing, which switches off the leader's own truncation check
(`PostBatchHandler.bodyEndedEarly` gives up as soon as there is no announced length). A relayed upload that ended
early therefore came back as a 200 with a partial count: the exact outcome [#5470](https://github.com/ArcadeData/arcadedb/issues/5470)
exists to prevent, on the one path it never covered. The hop is also pinned to HTTP/1.1, since an `h2c` upgrade's
failure mode is re-sending a request whose body cannot be rewound.

[#5618](https://github.com/ArcadeData/arcadedb/issues/5618)

## JSON: a property explicitly set to `null` raised `UnsupportedOperationException` instead of `JSONException` (#5935)

`JSONObject.getElement()` guarded only against an absent property (`object.get(name) == null`). GSON, however,
models an explicit JSON `null` as a regular `JsonNull` entry in the backing map, so `{"a": null}` sailed straight
through the guard and the conversion in `getString("a")` raised GSON's `UnsupportedOperationException: JsonNull`
- not the `JSONException` the getters document and callers catch.

All the raising getters of `JSONObject` (`getString`, `getInt`, `getLong`, `getFloat`, `getDouble`, `getBoolean`,
`getBigDecimal`, `getJSONObject`, `getJSONArray`) now screen the JSON null and answer
`JSONObject[a] is null`, keeping the distinct `JSONObject[a] not found` wording for a genuinely absent property.
A type mismatch is reported the same way (`JSONObject[a] is not a int (...)`), with the original GSON
`UnsupportedOperationException` / `IllegalStateException` / `NumberFormatException` preserved as the cause,
instead of leaking out of the API.

`JSONArray` shared both defects and gets the same treatment: its accessors raise `JSONException` for a null
element, a type mismatch, and an out-of-range position (`JSONArray[3] not found: the array has 3 element(s)`,
previously a bare `IndexOutOfBoundsException`).

`getBoolean()` was a second, quieter hole found in review: GSON's `getAsBoolean()` falls back to
`Boolean.parseBoolean()`, which never raises and answers `false` for anything that is not literally `"true"`. So
`{"name": "Alice"}` read through `getBoolean("name")` returned a silently wrong `false` rather than reporting the
mismatch. It now accepts a JSON boolean and the strings `"true"` / `"false"` (case-insensitive, the form
configuration files and query strings use) and raises `JSONException` for anything else.

The null-tolerant accessors keep answering `null`: `get(name)`, `opt(name)` and `toMap()` are unchanged for a JSON
null, and `isNull(name)` / `has(name)` still report it.

**Behaviour change worth calling out.** The two-argument getters still answer their default for an absent or null
property, but they no longer swallow a *type mismatch*: `getBoolean("flag", false)` on `{"flag": "yes"}` used to
return `false` and now raises. Anything reading loosely-typed external JSON through them is affected - MCP tool
arguments, the MCP configuration flags, and the HTTP handlers that read their payload this way. The default was
only ever meant to cover "not there", not "there but wrong", and a default silently standing in for a value the
caller did supply is how `{"addHierarchy": "yes"}` disabled the setting it was asking to enable (issue #5639).
Booleans-as-integers are not accepted either: `1` is not `true`.

**A malformed payload now answers HTTP 400 instead of 500.** Because more of these paths raise where they
previously returned a silent default, `AbstractServerHttpHandler` gained a `JSONException` arm - wrapped and
un-wrapped, matching how `IllegalArgumentException` and `CommandParsingException` are already treated - reporting
`Invalid JSON payload`. A request whose JSON is missing a property, carries a null where a value is required, or
holds the wrong type for it is a client error, and it used to fall through to the generic `500 Internal error`.

**The MCP JSON-RPC contract is unaffected.** `MCPDispatcher` guards its `params` / `arguments` reads to turn a
malformed member into a `-32600` / `-32602` envelope over HTTP 200, and the setting-key read to keep secret masking
from raising while the request log line is built. Those guards named the Gson exception types the accessors used to
let escape, so they now cover `JSONException` too and keep answering an envelope rather than the transport's 400.

[#5935](https://github.com/ArcadeData/arcadedb/issues/5935)

## Bulk importer: a single out-of-range or malformed CSV/JSON row no longer has to abort the whole job (#5968)

The CSV/JSON importer aborted an entire bulk import on the first bad row - a single out-of-range numeric
value, a duplicate key, or a missing mandatory property threw away an otherwise-successful multi-hour load.
A new opt-in setting, `-onRowError skip` (default remains `abort`, unchanged from today), logs and skips the
offending row instead, reporting the count of skipped rows in the import summary.

Making this safe required per-row transaction ownership: each row now commits or rolls back its own
transaction rather than sharing one whole-file transaction, so a bad row's rollback can never take an
already-committed good row down with it, and can never leave a partially-written "ghost" record behind (a
bucket write whose index entry failed). CSV vertex imports, which normally persist asynchronously in
batches, switch to a synchronous per-vertex save in this mode for the same reason - an async batch rollback
would otherwise take down every other vertex queued in the same uncommitted batch. **That per-row commit is
also the throughput cost of enabling this**: for the whole run, not just around a failing row, `-onRowError
skip` drops effective batching to one row per transaction. For vertices the cost is larger still - dropping
`database.async()` entirely also gives up its worker-thread parallelism, so `-commitEvery`/`-parallel` are
silently inapplicable to vertices while this is enabled. Reserve it for imports where an occasional bad row
is expected and worth tolerating, not as an always-on default.

Skipped rows are counted in the existing `errors` field of the import summary, alongside any other
non-fatal issue an import already reports (e.g. a nested JSON conversion failure) - there is no separate
counter for `-onRowError skip` specifically, so `errors` being nonzero does not by itself distinguish a
skipped row from another kind of already-logged, non-fatal problem. Check the per-row `WARNING` log lines
for the specifics.

Because of that per-row ownership, **`-onRowError skip` requires exclusive control of the transaction** and
is rejected outright if one is already active - including a plain `IMPORT DATABASE ... WITH onRowError=skip`
over HTTP, since `DatabaseAbstractHandler` wraps a command in its own atomic transaction by default. Use it
from an explicit session or with `autoCommit=false` instead; the CLI importer and the embedding
`Importer(Database, String)` constructor (with no transaction already open) are unaffected.

**Behavior change in the default (`abort`) path.** CSV vertex imports persist via `database.async()`, and a
persist-time failure (a missing mandatory property, a unique-index violation, ...) caught only on the async
worker thread used to be logged at `SEVERE` and otherwise ignored - the import reported success even though
some vertices silently failed to persist. That failure is now surfaced and aborts the import with an
`ImportException`, regardless of `-onRowError`. A pipeline that was unknowingly relying on "vertex import
completes even if a few rows fail validation" will now see that import fail loudly instead. Note this still
isn't full atomicity for vertices: they persist in `commitEvery`-sized batches, and a persist-time failure
only rolls back the batch containing the bad record, so earlier batches that already committed stay durable
even though the import as a whole reports failure.

JSON imports have a similar gap, pre-existing rather than introduced here: each top-level record commits in
its own transaction even in default `abort` mode, so a later record's failure does not roll back earlier
records that already imported successfully - unlike CSV documents, which use one whole-file transaction and
so are fully atomic. `abort` mode still fails the import and stops processing further records either way;
only the "nothing at all was imported" guarantee differs between the two formats.

[#5968](https://github.com/ArcadeData/arcadedb/issues/5968)

## Regex-based query and validation surfaces are no longer able to pin a worker thread indefinitely on a pathological pattern (#5886)

A regex like `(.*a){20}$` against a 41-character string triggers catastrophic backtracking in
`java.util.regex`: still running after 30+ seconds for a pattern and input small enough to be typed by
hand. `arcadedb.command.timeout` does not help, because `Matcher` never polls an interrupt flag or checks a
deadline while backtracking - the only thing it calls on every backtracking step is `CharSequence.charAt()`
on the input, and nothing in the surrounding query execution machinery gets a chance to run again until that
call returns on its own. In a deployment where query access reaches low-privilege users, or an application
layer forwards user-supplied patterns, a handful of such operations is enough to permanently tie up worker
threads.

A new utility, `TimeBoundRegex`, closes that gap by wrapping the matched input in a `CharSequence` that throws
once a deadline elapses - the one interception point `java.util.regex` offers, for `matches()`, `replaceAll()`,
and `split()` alike. The deadline comes from a new setting, `arcadedb.command.regexTimeout` (default 1000ms,
per-database), independent of `arcadedb.command.timeout` and active even when that setting is left at its own
default of disabled (`0`). The deadline is checked every 256 `charAt()` calls rather than every call, keeping
the check off the hot path for the overwhelming majority of patterns that never come close to that many
backtracking steps.

**Covered entry points:**
- SQL `MATCHES` and openCypher `=~`
- SQL `LIKE`/`ILIKE`, including the native query engine's `SelectOperator` path
- `text.regexReplace()` and the `.normalize()` SQL method's optional pattern argument
- Full-text search's Lucene `RegexpQuery` (`/pattern/`) and `WildcardQuery` (`*`/`?`) support
- PromQL's `=~`/`!~` label matchers
- Schema-level `REGEXP` property validation (`CREATE PROPERTY ... REGEXP <pattern>`) - notably reachable with
  no query privileges at all, since it runs on every insert/update of a validated property through any write
  path (REST, any wire protocol)

The `.split(delimiter)` SQL method got a different fix: unlike its three siblings (the `split()` function,
`text.split()`, and Cypher's `split()`), which all treat the delimiter as a literal via `Pattern.quote(...)`,
it passed the delimiter straight to `String.split(regex)` unescaped. Matched to its siblings' behavior, which
removes the risk entirely rather than just bounding it.

**Deadline sharing.** A regex evaluated once per row/token/item/property over a scan must not get a fresh
timeout budget per item - a table, index, or document shaped so every item triggers catastrophic backtracking
would then cost up to `itemCount * regexTimeout` instead of one bounded operation. `MATCHES`, `=~`,
`text.regexReplace()`, and `.normalize()`'s pattern argument all share one deadline across an entire query
execution (cached on the `CommandContext`, the same mechanism used to cache `MATCHES`'s compiled `Pattern`);
full-text and schema `REGEXP` validation share one deadline across the whole scan/document they run against;
PromQL shares one deadline across an entire query's lifetime, including every step of a range query, not just
the rows within one step. `LIKE`/`ILIKE` are the one exception: `BinaryCompareOperator`, the interface they
implement, has no `CommandContext` to cache a deadline on (only a `Database`), and widening that interface
across every comparison operator was judged out of proportion here - each row still gets its own
`regexTimeout` budget, so a `LIKE`-heavy scan where many rows are individually catastrophic is bounded per row
but not for the scan as a whole. A parallel bucket scan (SQL's default for a type spread across multiple
buckets) is a second, narrower exception: each worker thread gets its own copy of the `CommandContext`
(deliberately, so workers don't race on a shared, non-thread-safe cache), so it computes its own deadline
independently rather than sharing the sequential-scan path's single deadline - a type scanned in parallel
across N buckets is bounded by `N * regexTimeout` overall, not one shared budget. Unlike the row/item counts
this issue defends against, bucket count is a schema/DDL property, not attacker-controlled, so this is a much
narrower gap than the one this fix closes.

**Upgrade note.** Because the deadline is shared per query/scan rather than per match, and defaults to an
enabled 1000ms, a *legitimate, non-catastrophic* operation that previously ran slowly to completion - a
`MATCHES`/`LIKE` query over a very large table, a full-text or PromQL scan over a lot of data, or `REGEXP`
validation against a large field value - can now fail with `TimeoutException` instead. Raise
`arcadedb.command.regexTimeout` for any database with that kind of workload.

[#5886](https://github.com/ArcadeData/arcadedb/issues/5886)

## A rare `NoSuchElementException` reading a record immediately after committing it under `REPEATABLE_READ` (#5976)

`RecordEncryptionTest.encryption()` intermittently failed on CI with `NoSuchElementException` out of
`MultiIterator.next()`, reading back a `BackAccount` vertex saved by the immediately preceding, already-committed
transaction - single-threaded, no HA, no network. The failure was never reproduced locally, which pointed at a
narrow timing window rather than a deterministic bug.

The real error was hidden. `BucketIterator.fetchNext()` catches any exception thrown while materializing a
record, logs it at `SEVERE`, and skips the slot - so a record that failed to load looked identical to an empty
bucket to every caller, surfacing generically as "no records" instead of whatever actually went wrong.

What actually went wrong: `TransactionContext.getPage()` decides whether a freshly loaded page is worth caching
in its `REPEATABLE_READ` snapshot (`immutablePages`) by checking whether the page is "new" - and used
`PaginatedComponentFile.getTotalPages()` (the physical on-disk file size) to decide. That lags a commit:
`PageManager.writePages()` hands a committed page to the async flush thread and only bumps the in-memory
`PaginatedComponent.pageCount` synchronously, so a just-committed, not-yet-physically-flushed page was
(incorrectly) treated as "new" and left out of the cache. `ImmutableVertex.modify()` then used that same cache
(via `hasPageForRecord()`) to decide whether a record needed a defensive reload - found it missing, and
force-reloaded, which re-invokes every `AfterRecordReadListener` on the same record a second time, re-entrantly,
before the first invocation had returned. The encryption listener decrypts a record's ciphertext in place inside
`onAfterRead()`, starting with `record.asVertex().modify()` - not safe against being re-entered on itself: the
inner (nested) call decrypts the ciphertext first, so the outer call then tries to Base64-decode the now-plaintext
value and throws `IllegalArgumentException`, which `BucketIterator` swallows as described above.

Fixed by having `getPage()` ask the owning `PaginatedComponent` for its own page count (bumped synchronously at
commit, independent of flush timing) instead of the physical file size, so a committed page is cached for
`REPEATABLE_READ` as soon as it is committed. `Issue5976AfterReadReentrancyRaceTest` reproduces the race
deterministically by forcing the page cache to evict aggressively (`MAX_PAGE_RAM=0`) across a few thousand
fresh single-page buckets: 500+ failures per 1000 iterations before the fix, zero after it.

[#5976](https://github.com/ArcadeData/arcadedb/issues/5976)

## `AbstractSQLMethod.getSyntax()` mis-rendered variadic methods (#5972)

The shared default `getSyntax()` implementation built the optional-parameter suffix with a loop bounded by
`i < maxParams`, which is never true once `maxParams` is `-1` - the sentinel every variadic SQL method
(`removeall`, `append`, `include`, `exclude`, `remove`, `transform`) declares for "unlimited". Any variadic
method that does not override `getSyntax()` itself - only `SQLMethodRemoveAll` in practice - therefore rendered
a trailing `[]` regardless of how many extra parameters it actually accepts, e.g.
`<field>.removeall(param1[])` instead of `<field>.removeall(param1[, param2]*)`. Cosmetic only (this string
feeds documentation generation and exception messages, not parsing), but now handled explicitly for both the
"at least one more" and "any number, including zero" shapes.

[#5972](https://github.com/ArcadeData/arcadedb/issues/5972)

## Redis/Bolt wire protocols: bounding the BOLT handshake/auth window (#5978)

Follow-up from the #5912 fix that gave the Redis wire protocol a bounded pre-authentication read timeout (so an
unauthenticated connection that never sends anything can't hold its thread open forever): `BoltNetworkExecutor`
had no equivalent. `negotiateTransport()` only ever bounded the TLS-detection sub-step, then explicitly restored
an infinite timeout before the BOLT handshake and the AUTH/HELLO-LOGON exchange even began - and for a
plaintext-only listener (`arcadedb.bolt.ssl=DISABLED`, the default), no timeout was armed at all. A connected
client that never completed the handshake or never authenticated could hold the connection thread open
indefinitely, the same resource-exhaustion shape #5912 closed for Redis.

Fixed by arming the same bounded `NETWORK_SOCKET_TIMEOUT` window across transport negotiation, the BOLT
handshake and the AUTH/HELLO-LOGON phase - for both plaintext and TLS connections - lifting it to infinite only
once authentication actually succeeds (mirroring `RedisNetworkExecutor.markAuthenticated`/`markUnauthenticated`),
and re-arming it on `LOGOFF`. A `SocketTimeoutException` on that bounded window now closes the connection
cleanly (matching the existing `EOFException`/`SocketException` handling) instead of surfacing as a `SEVERE`
"BOLT connection error" log.

Two other follow-ups filed alongside this one were evaluated and intentionally left as-is: an idle-timeout cap
on already-authenticated Redis connections was judged not worth the deviation from real Redis semantics (which
never times out an idle authenticated client) absent evidence it's a problem in practice; and the swallowed
`SocketException` in `RedisNetworkExecutor.markUnauthenticated()` already carries a comment explaining why that
failure mode is safe to ignore (the socket is already broken/closed) - the new `BoltNetworkExecutor` counterpart
documents the same reasoning.

[#5978](https://github.com/ArcadeData/arcadedb/issues/5978)

## The Cypher count push-downs can anchor on an unlabelled node (#5757)

`MATCH ()-[:TYPE]->() RETURN count(*)` - "how many edges of this type are there" - is the cheapest question
there is to ask a graph, and it was the one shape the CSR count push-downs could not serve. Every operator
walks out from one position of the pattern and enumerates the vertices that position accepts, which it built
from the position's label; an unlabelled anchor left it with no set, which each operator read as an empty one
and answered **0**. Issue #5715 closed that wrong answer by declining to build the operator at all, so the
answer became right and the fast path went away.

The two chain operators now read an absent anchor label for what it means - **every vertex** - on both paths:
the CSR one seeds the whole node domain, and the OLTP one iterates every vertex type in the schema. That
covers the plain chain, the chain with a `<>` inequality (whose self-loop subtraction anchors the same way),
and the anti-join chain. The pair-join and degree-product operators still decline an unlabelled anchor, since
they key a hash join and a degree product on the label itself, and the ordinary pipeline answers those.

**The root cause is separated at the source.** `CSRCountUtils.buildValidBuckets` returned `null` both for "no
label was given, so do not filter" and for "the label is declared on nothing, so nothing matches", and its
callers disagreed about which one they held - that single overload produced *both* wrong answers found in
#5715, one from each end of it. `null` now means only "no filter", an **empty** set means "matches nothing",
and every consumer branches on `null` alone so an empty set falls through to a membership test that keeps
nothing.

**A view over some of the vertex types is no longer used where it cannot answer.** "Every vertex" is a claim
about the graph rather than about a `GraphAnalyticalView`, so an operator anchored on it runs against the OLTP
path unless the view's node domain is every vertex. The same reasoning applies to the per-vertex accelerator
these OLTP paths consult: a view holding a subset of the vertex types answers for the vertices it maps with
the adjacency it holds, which is missing every edge that leaves the view, and the fallback for an unmapped
vertex does not cover it. Those lookups now go through `CSRCountUtils.findAcceleratingProvider`, which
declines a partial view.

[#5757](https://github.com/ArcadeData/arcadedb/issues/5757)

## `CHECK DATABASE FIX` and `REBUILD INDEX` no longer silently lose an index under sustained lock contention (#6040, #6041)

Both `RebuildIndexStatement.buildIndex()` and `DatabaseChecker`'s auto-fix index-rebuild path retry a
`dropIndex()` + `create()` body as one unit when the index's file lock is contended. That retry loop charged a
lock-acquisition failure - the lock timed out before the body ever ran, so nothing was touched - and a failure
raised *after* that same attempt's `dropIndex()` had already committed - the index is now gone - against the
identical, small attempt budget. Exhausting retries while in the second state left the index permanently
missing, discoverable only by noticing it was gone.

Both call sites now track whether the drop actually ran on any attempt and switch to a larger retry budget
(4x) once it has, favoring "never silently lose an index" over a bounded worst-case runtime. Under sustained
contention, `REBUILD INDEX` and `CHECK DATABASE FIX` can now take several minutes per index rather than the
previous ~28 seconds before giving up - `CHECK DATABASE FIX` in particular repeats this per affected index, so
a fix run against a database with several contended indexes can take noticeably longer under load than before
this change. `RebuildIndexStatement.buildIndex()` also now throws on exhaustion instead of silently returning
as if the rebuild had succeeded; `DatabaseChecker`'s warning states which of the two cases occurred instead of
a blanket "lock contention" message that was misleading for the second case.

[#6040](https://github.com/ArcadeData/arcadedb/issues/6040), [#6041](https://github.com/ArcadeData/arcadedb/issues/6041)

## `field.toLowerCase() IN [...]` now uses a `COLLATE CI` index (#6037)

`field.toLowerCase() = 'value'` and `field.toLowerCase() BETWEEN a AND b` already recognized a case-insensitive
index and used it instead of a full scan; the equally common `IN` shape did not, and fell back to a bucket
scan even when a `COLLATE CI` index existed. `InCondition.isIndexAware()` gained the same recognition,
reusing the existing field-pattern check `BinaryCondition`/`BetweenCondition` already share.

[#6037](https://github.com/ArcadeData/arcadedb/issues/6037)

## Super-node promotion no longer silently reorders a vertex's edges (#6044)

Once a vertex crosses `arcadedb.graph.supernodeThreshold` (default 4096 edges per direction) its edge list is
promoted to the striped layout (#5156): N chains, one per stripe, chosen by `hash(neighbour RID)`. Each chain is
still exactly newest-first internally, but `StripedEdgeList` composed them by **concatenation** - the whole of
stripe 0, then the whole of stripe 1, and so on - so the global iteration order became a function of the hash.

That is a behaviour change no reader opts into and none can detect. The classic layout iterates exactly
newest-first, and "the first N edges are the N most recent" is the only thing it ever did; after promotion, an
edge's returned position drifts from its true recency rank by an amount proportional to the vertex **degree** -
worst on precisely the vertices promotion targets, and worse as the graph grows. The reported symptom was a
listing paging the newest 100 chats attached to a location vertex quietly dropping newly created ones once the
location passed the threshold. No exception, no log line.

The read walks - `edgeIterator()`, `vertexIterator()` and `ridIterator()` - now **interleave** the chains
round-robin (new `InterleavedIterator`): one entry from each chain per turn, a chain leaving the rotation when
it is drained. Since each chain is newest-first and the hash spreads entries uniformly, depth *k* in a chain is
global rank *≈ k × stripes*, so the rotation reconstructs an approximate newest-first order whose error is
proportional to the **rank asked for** rather than to the degree. Simulated with the real
`StripeDirectory.stripeOf` hash over sequential neighbour RIDs, 16 stripes, asking for the newest 100:

| degree | order | newest edge at position | of the true newest 100, how many are in the first 100 | worst-placed of those 100 |
|---|---|---|---|---|
| 5,000 | concatenated (before) | 3773 | 4 | 4722 |
| 5,000 | interleaved (now) | 12 | 88 | 140 |
| 20,000 | concatenated (before) | 7516 | 8 | 18782 |
| 20,000 | interleaved (now) | 6 | 82 | 155 |

The last two columns barely move between degree 5,000 and degree 20,000 for the interleaved order and degrade
without bound for the concatenated one. That is the whole point of the change.

**The rotation is per generation, not across all of them.** Generation 0 of the stripe directory is the
pre-promotion chain and holds the *oldest* edges; folding it into the same rotation would hand out ancient
entries in the first positions, which is strictly worse than what it replaces. Generations hold disjoint entries
and are walked newest first, so their groups stay concatenated and every edge of a newer era still precedes every
edge of an older one. A generation contributing a single chain is passed through unwrapped.

**The maintenance walks deliberately keep plain concatenation** - removal, counting, `CHECK DATABASE`'s integrity
walk, export. They consume the whole list, order means nothing to them, and one live cursor with one resident
chunk page beats `stripes` of them. Paging the first N edges is where the locality cost of the rotation is
negligible and the ordering benefit is everything.

**Follow-up (#6048): the OLTP read walks pay that same locality cost too, on an ordinary full traversal.**
`MATCH (h)-[:LINK]->(x) RETURN x` with no `LIMIT`, a Gremlin `out()`, or any other walk that consumes
`edgeIterator()`/`vertexIterator()`/`ridIterator()` to exhaustion goes through the interleaved rotation exactly
like a paged read does, and now keeps `stripes` chunk pages resident and hops across `stripes` files the whole
way through - for an order it never consults, since it reads everything anyway. New setting
`arcadedb.graph.supernodeInterleaveRounds` (default 64) bounds it: past `rounds × stripes` entries of a
generation, `InterleavedIterator` degrades to draining whatever is left of each chain one at a time, recovering
the maintenance walks' one-cursor locality for the remainder of a full walk while a paged read within the
threshold keeps the full rank-fidelity above. The degrade point scales with the live stripe count, not with the
vertex's degree, so a full walk's extra cost for the ordered prefix stays bounded no matter how large the
super-node grows. Set it to `0` to disable interleaving entirely (immediate concatenation, the pre-#6044 order).

Read-side only: no on-disk format change, no `HASH_VERSION` bump, no migration, and a database promoted by an
earlier build gets the new order the moment it is read by this one.

What is now written down rather than inferred: the striped layout guarantees **exactly** newest-first *within* a
stripe chain and within a generation, that the **newest** edge is always inside the first `supernodeStripes`
entries (it is the head of its own chain, and the first turn emits every chain's head), and **approximately**
newest-first globally. It is not an ordering guarantee - it never was, on either layout. An application that
needs an exact order must sort or read through an index on a timestamp property.

**The guarantee is scoped to the OLTP read walks and does not reach a `GraphAnalyticalView`** (#6049). A view
returns neighbours sorted ascending by internal dense node ID, further permuted by `CSRBuilder`'s BFS/RCM
locality pass, which carries no relationship to recency at all - and the Cypher optimizer substitutes
`GAVExpandAll` whenever the edge variable is not captured and a provider exists for the edge types, without
consulting `ORDER BY` or `LIMIT`. So on a promoted super-node with a ready view, `MATCH (h)-[:LINK]->(x) RETURN x
LIMIT 100` returns 100 neighbours with no recency property whatsoever, even though the OLTP walk above would
approximate newest-first. This is documented in `GraphAnalyticalView`'s and `StripedEdgeList`'s javadoc, and in
`GRAPH_SUPERNODE_THRESHOLD`'s setting description, rather than qualified only here.

[#6044](https://github.com/ArcadeData/arcadedb/issues/6044), [#6049](https://github.com/ArcadeData/arcadedb/issues/6049), [#6048](https://github.com/ArcadeData/arcadedb/issues/6048)

## `Type.convert()`: narrowing an array or Collection of `double`/`float`/`long` to a smaller integral array no longer silently corrupts values (#6020)

The scalar narrowing path (`Byte`/`Short`/`Integer`/`Long`) already rejects `NaN` and an out-of-range value
instead of wrapping it (#5905, #5970), but the array-narrowing branches - `double[]`/`float[]`/`long[]` source
to `int[]`/`long[]`/`short[]` target - did a raw element-wise cast with neither check.
`Type.convert(db, new double[]{Double.NaN}, int[].class)` returned `{0}`, and narrowing a `long[]` value like
`3_000_000_000L` to `short[]` wrapped via plain two's-complement truncation to a wrong in-range value, both with
no error. Every element now goes through the same `narrowToIntegral()` the scalar path uses.

Code review on the fix caught the same bug one branch over: the sibling `Collection` (e.g. `List<Double>` - the
shape JSON deserialization typically produces) -> `int[]`/`long[]`/`short[]` branches had the identical raw
`.intValue()`/`.longValue()`/`.shortValue()` cast and were left out of the first pass entirely. Fixed the same way.

[#6020](https://github.com/ArcadeData/arcadedb/issues/6020)

## Vector index test suite: a diagnostic timeout added for a stuck rebuild couldn't fire in the tests it was meant to help diagnose (#6032)

A same-day fix bounded `startAsyncGraphRebuild()`'s semaphore acquire at
`arcadedb.vectorIndex.rebuildPermitTimeoutMs` (default 600s) with a diagnostic `WARNING` on timeout, so a future
stuck rebuild would be traceable instead of hanging silently. But `LSMVectorIndexRebuildTest` and
`LSMVectorIndexRecoveryTest`'s own Awaitility ceiling was a flat 300s - half the production timeout - so their
assertion always gave up and failed *before* the production wait could reach its own timeout and log anything.
Reconfirmed three times (PR #5960, #5980, #6019) with no permit-timeout warning ever appearing in any of the
failing runs. Both test ceilings are now derived from the production setting's own live value plus a 60s margin
instead of a second, independently-drifting hardcoded constant.

[#6032](https://github.com/ArcadeData/arcadedb/issues/6032)

## Polyglot host-class deny-list: the hierarchy check now covers wildcard-only-denied ancestors too (#6045)

Follow-up hardening from #6042. `HostClassLookupFilter`'s ancestor-hierarchy walk (closing GHSA-j57p-qmrh-v7xv)
originally excluded every package-wildcard `DENIED` entry (e.g. `java.security.**`) to avoid false-positiving on
marker interfaces that merely happen to live in a denied package, such as `java.io.Serializable`. That also let a
*capability-bearing* ancestor reachable only through a wildcard entry slip through undetected if inherited by an
allow-listed subclass. Auditing the JDK classpath reachable from `ScriptTriggerExecutor.ALLOWED_PACKAGES` found
the concrete instance: `java.util.PropertyPermission` - admitted by name under `java.util.*` - extends
`java.security.BasicPermission` extends `java.security.Permission`, both wildcard-denied by `java.security.**`
and pinned by no precise entry.

The walk now checks an ancestor against every `DENIED` entry, wildcard or precise, except a short, explicit list
of inert marker interfaces (`Serializable`, `Closeable`, `Cloneable`, `Comparable`, `AutoCloseable`) that grant no
capability of their own - closing the gap without reintroducing the collateral-damage false positives the original
exclusion guarded against. Not a known exploitable bypass in the current built-in allow-lists; a completeness fix
for any embedder configuring a broader one via `HostClassLookupFilter`'s public constructor.

[#6045](https://github.com/ArcadeData/arcadedb/issues/6045)

## Closing a database no longer risks leaving an in-flight vector graph build's thread alive indefinitely (#5872)

`LSMVectorIndex` builds its HNSW graph on a dedicated `ForkJoinPool` for exactly one reason: `shutdownNow()` on
close is supposed to cancel a build still in progress, since JVector's `GraphIndexBuilder` does not observe
`Thread.interrupt()` on the thread that called it, only on the pool's own workers. But the insertion itself is
`buildPool.submit(() -> IntStream.range(...).parallel().forEach(...)).join()`, joined from the calling thread -
external to the pool. `shutdownNow()` only cancels tasks it still finds queued and unstarted; once insertion is
under way, the joiner is parked on that specific task's own completion status, which an external (non-pool)
`ForkJoinTask.join()` waits for without ever checking `Thread.interrupt()`. The original report reproduced a
build thread still alive and parked in `ForkJoinTask.join()` a full 60 seconds after `db.close()` returned, with
every pool worker already idle - `shutdownNow()` had nothing left to cancel, and nothing was ever going to mark
that specific task done.

The fix keeps a handle to the submitted insertion task and, on `releaseBackgroundResources()`, cancels it
directly before shutting the pool down. `ForkJoinTask.cancel()` forces the task's own completion status and wakes
every joiner waiting on it, which `shutdownNow()` cannot do for a task already under way. The resulting
`CancellationException` is now recognized as the expected outcome of a close racing a build, logged once at INFO
rather than surfacing as a SEVERE "Error building graph from scratch" wrapped in an opaque `IndexException`.

[#5872](https://github.com/ArcadeData/arcadedb/issues/5872)

## `batch()` on a gRPC connection now uses the gRPC streaming RPC instead of JSONL over HTTP (#6070)

`RemoteGrpcDatabase.batch()` inherited `RemoteDatabase`'s implementation and returned a loader that posted its
payload as JSONL to `POST /api/v1/batch`, over plain HTTP, whatever protocol the connection spoke. A caller who
opened a gRPC connection got the HTTP transport silently, and nothing on the API surface said so. Meanwhile the
`GraphBatchLoad` client-streaming RPC had been defined in the proto and implemented in `ArcadeDbGrpcService`
since the gRPC module landed, with no client anywhere in the codebase calling it.

`RemoteGrpcDatabase.batch()` now returns `RemoteGrpcGraphBatch`, which carries the load over that RPC. The public
API is unchanged - same methods, same builder options, same results - so the switch is transparent to anything
that only uses `batch()`. What changes underneath is where temporary ids are resolved. Over HTTP each flush is an
independent request the server does not remember, so the loader has to ask for the id mapping back and rewrite
the references of later edges itself; over one stream the server holds the mapping for the whole load, so a
temporary id crosses a chunk boundary untouched. That removes the per-flush round trip, the client-side mapping
arrays, and the ceiling `flushEvery` had on the HTTP path past which the server stops echoing a mapping too large
to consume. The new loader adds `withTimeout()` for the deadline of the whole stream, and applies backpressure
rather than handing gRPC chunks faster than the transport drains them.

Wiring a real client onto the RPC surfaced the gaps that had gone unnoticed while it had none, each of which
only shows on a load big enough to be worth a streaming transport, and each of which the HTTP endpoint had
already had to solve:

- **A load aimed at a follower is now refused instead of taken.** The bulk path mutates state only the leader can
  serialize - the schema dictionary above all - so running it on a follower races the local state-machine apply
  in `Dictionary.getIdByName`, which is the corruption #4122 was about and the reason `PostBatchHandler` relays
  the payload to the leader. `GraphBatchLoad` had no such guard. It cannot relay the way HTTP does, because the
  HA plugin exposes the leader's HTTP address only and relaying a bulk load through a follower would double the
  traffic of the transport chosen to avoid exactly that, so it fails with `FAILED_PRECONDITION` naming the
  leader - before writing anything, so there is nothing to reconcile.
- **The temporary-id mapping is no longer always echoed back in full.** A load of a few million vertices built a
  response past the default 4 MB message limit and failed at the very end, with everything already committed. The
  new `return_id_mapping` option is a tri-state mirroring the HTTP `idMapping` parameter: unset caps it and
  reports `id_mapping_omitted` / `id_mapping_size` past the cap, `true` demands it whatever the size, `false`
  never sends it. The streaming loader sets `false`, having no use for a mapping the server resolves itself.
- **A failed load now reports what it had already committed.** The batch commits incrementally, so an error is
  not a rollback, but a gRPC status carries no message body. The counters ride the trailers of the failed call
  and reach the caller through `getResult()`, which is readable after catching the failure - re-sending a load
  blindly would double everything that did get through. On that trailer the edge count is what the batch
  *flushed*, not what it received: an edge is buffered when its record arrives and reaches the database in a
  later `commitEvery`-sized flush, with the incoming direction connected at close, so counting received edges
  would claim ones that never landed and lead a caller to re-send too little and lose them. The counter
  advances a flush at a time, so a load that dies mid-flush under-reports rather than over-reports - a
  duplicate on re-send is recoverable, a silently missing edge is not. Vertices need no such distinction,
  being counted once their own commit returns.
- **`vertex_batch_size` is configurable.** It was hardcoded at 10,000 with no option to change it, which a
  replicated database needs, one buffer being one Raft entry.
- **The commit-retry knobs are reachable from the Java client.** `PostBatchHandler` had read `commitRetries` and
  `commitRetryDelayMs` as query parameters since they were introduced, but `RemoteGraphBatch.Builder` never
  exposed them, so no Java caller on either transport could set them. `withCommitRetries()` and
  `withCommitRetryDelay()` were added to the shared builder, which closes the gap for HTTP as well. Both are
  `optional` on the wire, because zero is a setting for each of them - no retries at all, and no back-off before
  the first one - and not the same as leaving them alone, which a plain proto3 integer cannot express.

The follower refusal runs *after* the caller has been resolved against the database it named, the way every
other RPC on this service starts. Naming the leader is a fact about the cluster's layout, so a request that
cannot reach the database it asked for is answered about that database rather than told where the leader lives.

[#6070](https://github.com/ArcadeData/arcadedb/issues/6070)

## Full backup is up to 27x faster, and throttles writers for 23x less time (#6072)

**A full backup of a 1.25 GB database went from 18.9 seconds to 0.68 seconds - 27x - for an archive 7.5%
larger.** On the same hardware the throughput scales almost linearly with the thread count, and the CPU spent
is unchanged: the extra threads buy wall-clock, not work.

| | before | after (8 threads) |
|---|---|---|
| Backup duration, 1.25 GB database | 18.9 s | **0.68 s** (27x) |
| Effective throughput | 68 MB/s | **1,876 MB/s** |
| Archive size | 323 MB | 348 MB (+7.5%) |
| Writer throughput *during* the backup | 6,809 rec/s (4.3% of normal) | **123,192 rec/s (77%)** |
| How long writers stay throttled | 48.6 s | **2.1 s** (23x less) |

The last two rows are the ones that matter most in production, and they are why this is worth more than the
headline number suggests. A backup does not merely take time, it *costs* the writers running alongside it:
for its whole duration they are throttled. Shrinking the backup shrinks that window by the same factor, so the
write work a single backup displaces drops by roughly 97x.

The 27x is the eight-thread measurement. The default thread count is half the available processors capped at
8, so what a given machine sees scales with it: about 4x on two cores, 8x on four, 15x on eight, and the full
27x from sixteen cores up. All of these numbers come from `BackupCompressionBenchmark` in the integration
module - they are measurements, not estimates, and the benchmark is in the tree so they can be reproduced.

### What was slow

`FullBackupFormat` streamed every database file into a ZIP at `setLevel(9)`, on the calling thread. Deflate at
level 9 sustains roughly 20-40 MB/s on one core, so the backup was CPU bound long before it was I/O bound, and
a 100 GB database was an hour or more of pure compression.

Duration is not the only cost. The backup runs inside `PageManager.suspendFlushAndExecute`, and while flushing
is suspended, dirty pages pile up in `PageManagerFlushThread.deferredByDatabase` until they hit
`arcadedb.flushSuspendMaxDeferredRAM` (512 MB by default), past which committing threads are throttled instead.
The backup's duration *is* the writer-throttling window, so making the backup n times faster shortens that
window by the same factor. The read lock the backup also takes is not what users feel - it blocks schema
changes and file creation, not transactions.

Two changes, both measured rather than assumed:

**The deflate level is now configurable and defaults to 1 instead of 9**
(`arcadedb.backup.compressionLevel`). On a 1.25 GB database that is 3.1x faster for a 7.5% bigger archive
(323 MB at level 9 against 348 MB at level 1). Level 6 is a reasonable middle - it costs roughly half of
level 9 at the same ratio - and level 9 remains available for anyone who wants the smallest archive.

> **Upgrade note.** This is a change of default, so a backup taken after upgrading is about 7.5% larger than
> the same backup taken before it, with no configuration change on your side. If the backup target is sized
> tightly, set `arcadedb.backup.compressionLevel=9` to keep exactly the previous archive sizes - they are
> still produced several times faster than before, because the parallelism is independent of the level - or
> `=6` for the same size at roughly half the CPU of 9.

**Compression is now parallel** (`arcadedb.backup.compressionThreads`, default `-1` = half the available
processors capped at 8). Splitting the work by archive entry would not have helped, because a database is
often a handful of very large files and sometimes one dominant one, so the split is *inside* each entry: the
reader cuts every file into 1 MB chunks, each chunk is deflated by its own `Deflater` and terminated with
`SYNC_FLUSH`, and the compressed chunks are concatenated back in order. Deflate streams ended that way
concatenate into one valid stream, which is the same construction pigz uses for parallel gzip; the only cost
is that each chunk starts from an empty dictionary, measured at well under 1% of ratio at that chunk size. Peak heap
for the parallel path is bounded by construction at two chunks in flight per thread, each holding a ~1 MB input and a
~1 MB output buffer: ~32 MB of buffers at 8 threads, which is the ~45 MB the benchmark measures minus the ~12 MB
baseline the process uses anyway.
Scaling is close to linear: on the same 1.25 GB database, level 1 takes 6.2 s single-threaded, 2.5 s on 2
threads, 1.3 s on 4 and 0.7 s on 8, against 18.9 s for the old default - **27x end to end**. Peak heap for the
parallel path is bounded by construction at two chunk buffers per thread, measured at 33 MB above baseline on
8 threads.

`java.util.zip.ZipOutputStream` owns its `Deflater` and cannot be handed pre-compressed data, so the parallel
path emits the ZIP container itself (`ParallelZipArchiveWriter`). **The archive format did not change.** Sizes
are unknown when an entry header is written, so the writer uses the streaming form the ZIP specification
provides for exactly that case - general-purpose bit 3, zeroed sizes in the local header, a data descriptor
after the data - which is what `ZipOutputStream` itself emits for a `DEFLATED` entry of unknown size. Old
backups restore, new backups restore through the unchanged restore path, and both are readable by any standard
unzip tool, including the ZIP64 extensions an entry above 4 GB needs. The previous single-threaded writer is
still there and is selected by `arcadedb.backup.compressionThreads = 0`.

The number that matters most is not the backup's own duration but what it does to the writers it runs
alongside. Under a sustained insert load, measuring only inside the backup window: writers sustained 159,560
rec/s with no backup running, 6,809 rec/s (4.3%) for the 48.6 s a level-9 single-threaded backup took, and
123,192 rec/s (77%) for the 2.1 s the new default took. The deferred-RAM high-water mark reached the 512 MB
`flushSuspendMaxDeferredRAM` cap in every backed-up run, confirming writers were being throttled outright -
what changed is how long they were. Commit latency inside the window is barely affected either way
(p95 1,952 us against a 1,627 us baseline, p99 2,123 us against 3,393 us); throughput, not latency, is where
the suspension is paid for.

A third, optional knob caps the rate at which the backup reads the database files
(`arcadedb.backup.maxMBPerSecond`, 0 = unlimited) so a backup cannot saturate the production disk. It is off by
default, and it trades against the flush suspension: throttling makes the backup last longer, and writers are
throttled for the whole of it.

All three are settable per backup as well: on the command line (`-compressionLevel`, `-compressionThreads`,
`-maxMBPerSecond`), through the `Backup` API, and in SQL
(`BACKUP DATABASE ... WITH compressionLevel = 6, compressionThreads = 4`).

### Two defects found on the way

`BACKUP DATABASE ... WITH ...` silently dropped **every** setting. The statement matched the setting name
against `Expression.toString()`, which renders a string quoted, so no `case` ever matched. The visible
consequence was that `WITH encryptionKey = '...'` produced an archive in clear - it restored without the key.
The same statement's `copy()` also dropped the settings map, so a copy taken from the statement cache lost
them again. Both are fixed and covered by a test that proves the archive is genuinely encrypted, by asserting
a restore without the key fails.

An unrecognised setting name is now an error rather than being ignored, which closes the same hole by a
second route: `WITH encryptionkey = '...'`, one wrong character, would otherwise still look like a request
for an encrypted archive and produce a cleartext one. Nothing that worked stops working - until the fix
above, every setting was ignored, so no statement can have depended on one being accepted.

A backup that failed halfway still reported success. `PageManager.suspendFlushAndExecute` runs its callback
through `CodeUtils.executeIgnoringExceptions`, which logs and swallows, so an I/O error mid-backup left the
archive to be finalized with a valid central directory over a truncated set of entries - a backup that looks
valid and is not, which is the worst failure mode this code has. The failure is now carried out of the
suspension by hand, the partial archive is deleted, and only a backup that actually succeeded gets a central
directory written at all: a failed one aborts the writer instead, so even if the delete cannot happen
(permissions, a full or read-only filesystem) what is left is structurally invalid rather than plausible.

[#6072](https://github.com/ArcadeData/arcadedb/issues/6072)

## A backup no longer freezes the database: point-in-time page snapshots replace the flush suspension (#6075)

Phase 1 (#6072) made the backup 27x faster, which shortened the window during which writers are throttled but did
not remove the throttling. This removes it. `PageManager.suspendFlushAndExecute` - which parks the page-flush
thread for the whole of a backup, an HA snapshot ship or an HA database verify, so dirty pages accumulate in RAM
until `arcadedb.flushSuspendMaxDeferredRAM` (512 MB) and committing threads are then throttled outright - is
replaced by a real snapshot primitive: `PageManager.openSnapshot()`, a **page-level copy-on-write shadow**.

While a window is open, the first write to any page that existed at t0 first copies that page's current on-disk
image into a shadow, inside the per-page I/O slot the write already holds. A reader resolves every page as "shadow
if present, data file otherwise" and therefore sees exactly the t0 image, however far the live database has moved
on. Flushing is never suspended, so writers run at full speed for the whole operation.

Three things follow from that, beyond the writer impact:

- **Index compaction runs during a backup again.** The suspension was the only thing stopping `LSMTreeIndex` and
  `LSMVectorIndex` compaction from dropping a file under a running backup (`LSMTreeIndexCompactor` does not take the
  database write lock, so the backup's read lock never excluded it), and both refuse to compact while it is held. A
  long backup silently stopped compaction. Now a file dropped while a window is open simply has its physical
  deletion deferred until the window closes, so compaction proceeds.
- **The snapshot point is a real transaction boundary.** The window opens on a FULL drain of the flush queue, not
  the in-flight batch alone, so the restored database contains every transaction committed before t0 - closing the
  recency gap that could leave a restored backup a few hundred transactions behind. The HA snapshot ship now also
  reports the t0 transaction id (#5277's `last-tx-id.bin` marker) instead of the live counter, so the marker can no
  longer name a transaction whose pages were still in the queue.
- **The page image and its version are provably paired**, because they are read together, under the same per-page
  lock, from the same snapshot. That is the property incremental backup (phase 3) has to be built on: with a fuzzy
  copy, a manifest recorded at a different instant than the image it describes makes the next incremental conclude
  "unchanged" and silently drop a revision.

### Zero cost when no snapshot is open

With no window open the write path pays one volatile field read and a branch the predictor always gets right, inside
a critical section that already performs a page-size (64 KB) `pwrite`: roughly 1 ns against 10-50 us. That is
implementation discipline rather than design - a single nullable field, not a listener list; nothing before the null
check; and the hook goes strictly inside the existing `concurrentPageAccess` write branch, which is the single funnel
both physical page-write call sites already go through, so it needs no new lock and adds no lock-ordering risk.

### The barrier, which is where the correctness lives

Flipping the flag is not the same as opening the window: a writer can already be inside its write section having
read "no window", and would then overwrite its page unshadowed. So every writer is excluded before t0 is stamped,
each by the mechanism that already serializes it - a full flush-queue drain, then a suspension parking the flush
thread on a batch (that is, transaction) boundary, then the transaction manager's apply lock to exclude Raft and
recovery replay, then the global page-manager lock to exclude synchronous commits. Only then are the per-file page
counts and `lastTxId` recorded. Performing these in the other order produces subtly torn snapshots that pass tests
and fail in production, which is why the barrier is what the test suite hammers hardest.

### Reading stays sequential

The obvious snapshot reader takes the per-page I/O slot for every page, turning one streaming `transferTo` into a
lock acquisition per page. It is not needed: a write during the window ALWAYS shadows the page before touching the
file, so a run of pages is read in one bulk call with no lock at all and the shadow re-probed afterwards - any page
a writer touched underneath the read is now in the shadow, and its t0 image is taken from there. A torn read is
therefore always detected, and the common case costs one probe per page against a primitive open-addressing map.

### Bounded, and with a fallback

The shadow is RAM first (`arcadedb.pageSnapshotMaxRAM`, 64 MB) spilling to an append-only scratch file, and capped
overall (`arcadedb.pageSnapshotMaxSize`, 1 GB). Worst case it grows to the working set dirtied during the window,
which on a small very hot database approaches the database size, so an uncapped shadow would reproduce the "snapshot
full" failure of sparse-file snapshots. On breach the window is invalidated and every read fails loudly: a backup
restarts on the suspend-and-freeze path, which throttles writers but always completes. That path stays available
and can be selected outright with `arcadedb.pageSnapshotEnabled = false`. The shadow file is pure scratch - recovery
never reads it, its extension is not a supported component extension, and an orphan left by a crash is deleted at
the next open.

Overlapping windows are supported rather than serialized: each owns its shadow, and a write captures the same
freshly read pre-image into every window that still needs it.

### One incidental change to every backup archive

The two backup writers disagreed about ZIP entry timestamps: the parallel one stamped the source file's
modification time, the single-threaded one left `ZipOutputStream` to stamp the moment the entry was written. Adding
the stream-based entry API the snapshot path needs routed both through one implementation, so they now agree on the
source file's modification time - which is the more useful of the two, and was already what the default
(multi-threaded) writer produced. Nothing about restoring changes; only the timestamps a listing shows for an
archive written with `arcadedb.backup.compressionThreads = 0`.

[#6075](https://github.com/ArcadeData/arcadedb/issues/6075)

## The snapshot backup, measured; an open window is now visible in the metrics; the HA `/checksums` endpoint stops freezing the files (#6116)

Three loose ends left behind by the point-in-time snapshot of #6075.

### The benchmarks #6075 asked for

`Issue6075SnapshotBackupIT` measured the suspension fraction and nothing else. Two harnesses now cover the rest:
`PageSnapshotOverheadBenchmark` (engine) for the write path, the t0 barrier, the flush-thread drain rate and the
`TransactionManager` apply lock, and `PageSnapshotBackupBenchmark` (integration) for what a running backup does to
the writers beside it, shaped like the #6072 harness so the two sets of numbers can be read together. Both are
`@Tag("benchmark")`, so neither runs in CI.

Two of the numbers they produce are worth stating here, because both correct something the #6075 write-up asserted:

- **The t0 barrier is not "a few ms" under load.** With no concurrent writer it is 13-120 us. With one to eight
  writer threads committing continuously it runs to tens of milliseconds, occasionally past 100 ms, and the driver
  is not the flush-queue depth (which stays small) but the barrier RETRYING: it drains the queue in full, and a
  commit landing between that drain and the flush-thread suspension makes it start over. It is still bounded, still
  vastly better than a suspension held for the whole backup, and still the only stall in the design - but its size
  is now known rather than assumed.
- **The copy-on-write shadow can reach the size of the database.** During a throttled backup of a 128 MB database,
  the shadow peaked at 37% of it with a writer pausing 100 ms between transactions, 84% at 20 ms, and 100% with a
  writer running flat out - it holds the pre-image of every page dirtied while the window is open, so a hot
  database rewrites all of them. The 1 GB `arcadedb.pageSnapshotMaxSize` default is therefore not the generous
  ceiling it looks like: on a database much beyond a gigabyte under a heavy write load, expect the window to breach
  it and the backup to restart on the suspend-and-freeze path. Raise it (or accept the fallback) accordingly - and
  the new `arcadedb_engine_snapshot_shadow_usage_percent` gauge below is how to tell which of the two you are
  heading for.

The writer-impact comparison itself, on a 128 MB database rebuilt before each run, with a mixed insert/update load
and the backup throttled to 32 MB/s, counting only the commits that fall inside the backup window:

| | rec/s inside the window | of the no-backup baseline | peak deferred RAM |
|---|---|---|---|
| no backup running | 193,854 | - | 0 |
| backup, snapshot path | 185,120 | **95.5%** | **0** |
| backup, suspend-and-freeze | 5,274 | 2.7% | 513 MB |

513 MB is `arcadedb.flushSuspendMaxDeferredRAM` reached, which is the point at which committing threads stop being
merely slowed and start being throttled outright. On the snapshot path the deferred-RAM gauge never leaves zero -
the claim #6075 rested on, stated as a measurement rather than as a design argument.

### An open window is now visible

`PageManager.getStats()` gained the snapshot readings, so they reach both Prometheus (through
`EngineMetricsBinder`) and Studio's server page (through `Profiler`, which renders every stat it publishes). Per
the counter/gauge distinction of #5636, the per-window state is gauges and only the JVM-wide totals are counters:

| Metric | Type | |
|---|---|---|
| `arcadedb_engine_snapshot_windows_open` | gauge | windows open right now |
| `arcadedb_engine_snapshot_shadow_pages` | gauge | pages held in their shadows |
| `arcadedb_engine_snapshot_shadow_bytes` | gauge | bytes held, RAM plus spill |
| `arcadedb_engine_snapshot_shadow_spilled_bytes` | gauge | of which spilled to disk |
| `arcadedb_engine_snapshot_shadow_usage_percent` | gauge | the fullest window against `pageSnapshotMaxSize` |
| `arcadedb_engine_snapshot_window_age_ms` | gauge | age of the oldest open window |
| `arcadedb_engine_snapshot_windows_opened_total` | counter | windows opened |
| `arcadedb_engine_snapshot_windows_invalidated_total` | counter | windows that lost their point in time |
| `arcadedb_engine_snapshot_preimages_captured_total` | counter | pre-images copied |
| `arcadedb_engine_flush_deferred_bytes` | gauge | dirty bytes deferred by a flush suspension (#6087) |

The alertable ones are the usage percentage and the invalidation counter. A window that breaches its cap is
invalidated and its consumer falls back to the suspend-and-freeze path, which still completes - so before this, a
backup that had quietly gone back to throttling every writer on the server left no trace except a WARNING in the
log.

### `GET /api/v1/ha/snapshot/{db}/checksums` no longer suspends flushing

The last writer-throttling reader in the product. #6075 migrated the backup, the HA verify and the HA snapshot
ship but left this one on `suspendFlushAndExecute`, because it reads the database DIRECTORY rather than the
registered page files and so needed a different shape. It now takes each page file's content from a window and
reads everything the window does not cover - `database.json`, `schema.json`, the `.ts.sealed` time-series stores -
raw, as before, under the same database read lock. A page file created after t0 is left out rather than read live,
matching what the verify endpoint reports; the checksums are byte-for-byte the ones a peer still on the fallback
path computes, so a migrated leader does not report every file as differing.

One transient file was being checksummed that should not have been: the `.pshadow` scratch of an open snapshot
window lives in the database directory, so any node that happened to have a backup running reported a file no peer
had.

[#6116](https://github.com/ArcadeData/arcadedb/issues/6116)

## `RESTORE DOCUMENT/VERTEX/EDGE` now applies the type's schema and fires the create events (#6127)

`RESTORE` deliberately did less than a normal create: it skipped `setDefaultValues()` and `validate()`, and it
fired no create event. The reasoning was that an emergency repair must never be blocked - its most common form is a
structure-only shell restore with no `SET` clause at all, recovering a RID that other records still reference, and
a mandatory-property check would refuse exactly that.

The trade-off does not hold up. A record written past its own `MANDATORY`/`NOTNULL` constraints is **frozen**:
`updateRecord` validates too, so every later UPDATE of it throws until the missing property is supplied. And
nothing downstream catches it - `CHECK DATABASE` is a structural check (page layout, record markers, graph
adjacency, index entries) and never evaluates schema constraints. So the permissive path was not buying a usable
record, it was buying a record you could look at and not touch, with no diagnostic anywhere.

`RESTORE` is now indistinguishable from an `INSERT` at a fixed RID:

- declared default values are applied;
- the record is validated, and a restore that would violate the type is refused instead of persisted;
- the create events fire - both the database-level listeners and the per-type `RecordEventsRegistry` - so anything
  that rebuilds derived state from create events no longer silently drifts after a repair.

On a type with mandatory properties the structure-only form now needs them supplied explicitly:

```sql
RESTORE VERTEX Person RID #12:7 SET name = '<unknown>'
```

which makes the placeholder value the operator's choice rather than a hole in the record. `GraphEngine.restoreVertexAt`,
which builds a property-less shell by design, is subject to the same rule and raises `ValidationException` on such a type.

One intentional difference from `createRecordNoLock` remains, in the other direction: a `beforeCreate` listener that
vetoes the write makes `RESTORE` **raise** rather than return quietly. A repair that reports success without writing
the record is the one outcome this statement must never produce.

If you have an `afterCreate` trigger on a **graph** type, note two interleavings that were previously impossible,
because RESTORE fired no events at all.

`RESTORE EDGE` does not reconnect adjacency - in the case it repairs, a raw record delete never touched the
neighbours, so the endpoints still reference the RID - which means the event now fires on an edge whose endpoints'
adjacency lists the statement has not written. A trigger that assumes "create event fired, therefore both endpoints
already list this edge", which holds for `Vertex.newEdge`, does not hold on this path.

`RESTORE VERTEX` fires the event on the bare vertex, before it rebuilds adjacency from the surviving edges. That is
the INSERT-parity ordering rather than a gap in it - a vertex a plain INSERT creates has no edges either - but it
means a trigger deriving something from adjacency (a degree counter, say) sees zero and is never notified of the
reconnection that follows, since reconnecting adjacency writes no records and so raises no event of its own. Drive
such a trigger from the statement's `reconnectedOutEdges`/`reconnectedInEdges` result instead.

### `Issue5279ConcurrentUpdateTest` re-triaged: a contention regime, not a flake

`growingUpdatesUnderContentionKeepTheDatabaseConsistent` had been written off as a chronic flake. It is neither
flaky nor a hole in the slot-merge logic. Instrumenting a failing run: 146 successful slot rebases, 30 records
spilling out of their page, 16 `ConcurrentModificationException`s reaching the retry loop, 5 of 8 threads out of
the default 3 retries. While the payloads still fit the shared 64 KB page the disjoint-slot merge absorbs
everything; once they outgrow it every record becomes a multi-page chunk record whose updates no merge can replay,
so eight threads on a single-bucket type serialize with one winner per round. With a larger budget every run
converges, `CHECK DATABASE` is clean and every record holds its last written value.

The test now states that contract instead of hiding it: a new `growingUpdatesThatStayInsideTheirPageNeverConflict`
asserts **zero** conflicts at `attempts=1` for the whole in-page growth phase (and proves it stayed in-page by
checking the bucket layout), while the spill-out case keeps an explicit retry budget and asserts convergence plus
structural soundness. [#6129](https://github.com/ArcadeData/arcadedb/issues/6129) tracks extending the slot merge
to chunked and placeholder records, which is what would make the `TX_RETRIES` default enough there.

[#6127](https://github.com/ArcadeData/arcadedb/issues/6127)
## The snapshot shadow cap sizes itself, the t0 barrier stops retrying, and two smaller loose ends (#6125)

Four items the benchmarks of #6116 turned up. None was a correctness bug; two of them contradict an assumption
#6075 shipped on, and could only be stated once there were measurements.

### `arcadedb.pageSnapshotMaxSize` is no longer a flat number

The measurement first: on a 128 MB database with the backup throttled to 32 MB/s, the copy-on-write shadow peaked
at 37% of the database with a writer pausing 100 ms between transactions, 84% at 20 ms, and **100%** with a writer
running flat out. The shadow holds the pre-image of every page dirtied while the window is open, so its ceiling is
the working set of the backup's duration, and a hot database rewrites all of it.

That makes any fixed default simply the database size above which backups silently start falling back to the
suspend-and-freeze path - the very writer throttling #6075 set out to remove. The 1 GB default only reliably
covered databases up to about a gigabyte under sustained writes.

The cap is now sized when the window opens. `arcadedb.pageSnapshotMaxSize` defaults to **-1**, meaning automatic:

```
cap = min( sum(pageCount * pageSize) at t0,   // the ceiling the shadow provably cannot exceed
           usableSpace(spill volume) / 2 )    // so a window can never be why the disk filled
```

The first term is exact rather than a heuristic: the shadow holds one pre-image per page that existed at t0, and
pages appended after t0 need none, so it cannot grow past the t0 size of the page files. A positive value still
means an absolute number of MB and `0` still means uncapped, so no existing configuration changes meaning.

Related, and the reason for the second term: the spill file lands in the database directory, where a breach-sized
shadow competes for disk with the data it is protecting. The new **`arcadedb.pageSnapshotSpillPath`** moves it to
another volume, and the automatic cap is then measured against the free space there.

The two ways a window can lose its point in time are also counted apart now, because they send an operator to
different places: `arcadedb_engine_snapshot_windows_overflowed_total` is a tuning problem (raise the cap, or give
the spill volume room), `arcadedb_engine_snapshot_windows_failed_total` is a disk problem. Their sum remains
`arcadedb_engine_snapshot_windows_invalidated_total`.

### The t0 barrier is single-pass, exact, and measured

#6116 measured the barrier at tens of milliseconds under load against 13 us idle, and found the driver was not the
flush-queue depth - which stays near zero on an SSD - but the barrier **retrying**: it drained the flush queue in
full, and a commit landing between that drain and the flush-thread suspension made it start over. After three
attempts it stopped retrying and stamped a t0 that could sit slightly behind the last committed transaction.

The gap is now closed by construction. `PageManager.publishPages` already holds the global page-manager lock across
*both* halves of publication - the synchronous page write and the flush-queue enqueue - so the barrier takes that
lock (after the apply lock, in the order it already used) and repeats the drain underneath it. Nothing can feed the
pipeline while the lock is held, so that second drain is guaranteed to converge, and it is normally instant because
a first, lock-free drain has already emptied the pipeline. The retry loop and `SNAPSHOT_BARRIER_ATTEMPTS` are gone.

Everything the barrier does under that lock shares one hard 5 s budget, because the lock is JVM-wide and the waits
those steps use elsewhere are either uncapped (the wait for the in-flight flush batch polls until a synchronous
`file.write` returns) or capped only by the 60 s progress-based `arcadedb.flushAllPagesTimeout` - either of which
would let one sick disk stall every committer in the process. Exhausting it is handled per step according to what it
costs: a pipeline that has not drained only means t0 may sit slightly behind the last commit, which is logged and
accepted, while a suspension that cannot be acquired or an in-flight batch that has not landed abandons the window
outright and the consumer falls back to the suspend-and-freeze path - slower for one database rather than briefly
fatal for all of them.

Exactness matters beyond tidiness: the HA snapshot ship writes `lastTxId` into the `last-tx-id.bin` recency marker
a follower is judged by at cold bootstrap, and a marker naming a transaction whose pages were still queued claims
data the archive does not contain.

The obvious inversion still does not work and is now documented as such: suspending the flush thread before the
drain makes the drain unable to ever complete, because a suspended thread defers batches instead of writing them.

The barrier is also reported now, the way a Prometheus timer is - a count and a summed duration, so
`rate(seconds)/rate(count)` is the average - plus the high-water mark a scalar snapshot cannot otherwise carry:

| Metric | Type | |
|---|---|---|
| `arcadedb_engine_snapshot_barrier_count_total` | counter | t0 barriers executed |
| `arcadedb_engine_snapshot_barrier_seconds_total` | counter | total time spent in them |
| `arcadedb_engine_snapshot_barrier_max_seconds` | gauge | the longest one observed |
| `arcadedb_engine_snapshot_barrier_inexact_total` | counter | barriers that could not prove the pipeline was empty |

The last one should stay at zero: no commit can cause it any more, so a non-zero value means index compaction was
feeding the flush pipeline throughout the barrier - harmless for consistency, since those writes go to pages beyond
the t0 page count of a file being built, but worth being able to see.

### `/checksums` validates the database name before taking its branch

`SnapshotHttpHandler` dispatched the `/checksums` sub-path *before* the block that rejects `..`, separators, NUL
and non-ASCII. It was not exploitable - the checksums path gates on an exact-match lookup over the already-open
databases, so a traversal string 404'd and the database was never resolved - but the guarantee lived two calls away
from the handler that needed it. The sub-path is now stripped, the name validated, and only then is a branch
chosen. A malformed name on the checksums route answers 400 instead of 404, which the OpenAPI spec now documents.

### The unused checksum diff is gone

`SnapshotManager.findDifferingFiles()` was called from nothing but its own unit test, while its presence - and the
`/checksums` description - suggested resync could ship only what changed. It has been removed rather than wired in,
for two reasons. Granularity: an ArcadeDB database is usually dominated by one bucket file, so a whole-file
comparison saves nothing the moment a single byte of it changes. Consistency: the checksums come from one
point-in-time window and the ZIP from another, so a file that matched when it was compared can be rewritten before
the transfer starts, and a follower that kept its local copy on the strength of that match would hold a database
torn across two instants. Incremental resync belongs at the **page** level, on the page-version manifest of phase 3
(#6115), where both halves come from the same window. `/checksums` is documented as what it actually is: an
operator diagnostic.

[#6125](https://github.com/ArcadeData/arcadedb/issues/6125)

## The disjoint-slot merge now covers the records that outgrew their page (#6129)

The commit-time disjoint-slot merge (`arcadedb.txPageSlotMerge`, #5381/#5279/#5569) turns a page-level
`ConcurrentModificationException` back into the record-level verdict it should have been: when the only reason a
bucket page conflicts is that another transaction wrote a *different* slot on it, this transaction's slot writes are
replayed on top of the newer committed page instead of failing the whole transaction. Until now it stopped at the
page boundary. A record that grew past the page size became a chunk chain or a placeholder, and from then on every
update of it poisoned its page unconditionally - so on a single-bucket type whose records had all outgrown their
page, concurrent updates of *unrelated* records conflicted for good. That is exactly the false conflict #5279
removed, reappearing one size class up.

Measured on the regression test that first exposed it (8 threads, 40 records of one bucket, each thread rewriting
only its own): once the records had spilled, **280 of 320 commits conflicted** - seven of eight writers lost every
round - and every one of those conflicts was on the single page holding the records' head chunks. With the merge
extended, the same run reports **zero**.

Three shapes are now tracked, each with its own slot *kind* so the replay checks the marker on the committed page and
not only the bytes:

- **The head chunk of a multi-page record.** Its footprint on the page is fixed when the record spills and is never
  grown again - an update rewrites the chunk size, the pointer to the next chunk and the chunk content, all inside
  the record's own bytes - so the page sees a single-slot write that commutes with writes to every other slot.
- **The spill itself**, where a plain record that no longer fits is turned into that head chunk. It is the one
  tracked write whose two images differ in shape, and the replay re-establishes the room the spill used: the slot's
  own footprint, plus the free tail of the page when it is the last record. Leaving this out was not a one-off cost -
  a transaction whose spill loses the race retries and spills again, so under contention the records that have to
  spill could starve instead of converging.
- **The content record behind a placeholder pointer**, in place or growing on its own page. It is an ordinary record
  on that page; only its size marker is negative.

The continuation chunks are still out: they are placed through inline record-table writes that no tracked slot image
accounts for, so every page they land on is poisoned, as is the head chunk's own page when the chain comes back to
it. So is a slot turning into - or being rebuilt from - a placeholder *pointer*, which changes two pages at once.

### The head chunk needed one guarantee the pre-image check cannot give

Every replay is gated on a byte-for-byte pre-image check, which is what tells a false page conflict from a true one
(two transactions writing the *same* record must still conflict, so the application reloads). For a multi-page record
that check can only see the head chunk, because that is the only part of the record living on the page being
replayed: a concurrent transaction that rewrote the record only *past* that chunk would have slipped through it and
been silently overwritten.

`LocalBucket.chunkChainTailFingerprint` closes that. It is a 64-bit FNV-1a over the chain past the head chunk (each
chunk's RID, size and content), taken when the record is taken for update - the same moment its page is pinned - and
again at commit before the chain is rewritten. The head chunk is replayable only while the two agree; when they do
not, the page falls back to the plain retry it took before this change. `Issue6129ChunkedSlotMergeTest` pins it with
two 200 KB values differing only in their last byte, which share their whole head chunk: without the fingerprint that
commit silently drops the other transaction's write.

The cost is proportional to work the update was going to do anyway: an update of a multi-page record rewrites every
chunk of its chain, so the fingerprint walks pages it is about to load and write regardless, and a record that is not
a chunk chain pays nothing beyond reading its own marker.

Worth stating plainly what that trades, because it is the one guarantee this change moves rather than widens. For a
multi-page record, "two transactions writing the same record always conflict" used to hold absolutely - for the blunt
reason that the head chunk's page was poisoned no matter what. It now holds up to a fingerprint collision: ~2^-64 per
pair of concurrent updates to one such record whose head chunks are byte-identical. Deliberate collisions are not a
threat model here: both colliding tails are content of the same record, so producing one requires write access to it,
and whoever has that can already overwrite it by committing last - which is the very outcome a collision would
produce. An installation that wants the absolute form back sets `arcadedb.txPageSlotMerge=false` and gets
unconditional poisoning, at the cost of the false conflicts the mechanism exists to remove.

### `TX_RETRIES` is enough again

`Issue5279ConcurrentUpdateTest.growingUpdatesUnderContentionKeepTheDatabaseConsistent` needed an explicit budget of
50 attempts (#6127 item 3, the re-triage of what used to look like a chronic flake). It now runs at the `TX_RETRIES`
default of 3 - the absence of an `attempts` argument is the assertion.

[#6129](https://github.com/ArcadeData/arcadedb/issues/6129)
## `CHECK DATABASE FIX`: the last two unbounded repair paths, and what the numbers it reports mean (#6136)

#6131 bounded the repair transaction a `CHECK DATABASE ... FIX` builds, committing every
`arcadedb.checkDatabaseRepairBatchPages` dirtied pages so that a large repair on a replicated database no longer
runs for hours and then dies whole at the commit with `ReplicatedEntryTooLargeException`. It deliberately reached
only the post-scan loops. These are the two paths it did not, plus the reporting the split made necessary.

### The index rebuild no longer buffers its whole WAL in leader heap

`BucketIndexBuilder.create()` wraps an index build in `schema.recordFileChanges(...)`, which on a Raft leader marks
the thread so that `commit()` **buffers** instead of replicating: the files being created do not exist on the
followers yet, so a `TX_ENTRY` referring to them would have its pages silently dropped there. `LSMTreeIndex.build()`
then commits once per `IndexBuilder.BUILD_BATCH_SIZE` (5000) records, and every one of those WAL images stayed
resident until the callback returned. Peak leader heap was therefore roughly the whole rebuilt index, plus a repeat
of every page more than one batch touched - on the node that is, by construction, the cluster's leader. A
`CHECK DATABASE FIX` over a large damaged type drops and rebuilds every index on the affected buckets, which is
exactly that shape.

The buffer is now shipped in ordered **instalments** as it fills, so leader heap is bounded by the threshold rather
than by the size of the index. Nothing new goes on the wire: this is the ordered-prefix sequence `splitSchemaEntry`
has produced since #4743, emitted as the payload is produced rather than after it has been built in full. Files
first, so pages have somewhere to land; WAL in the middle; and the fields that *publish* the change - the schema
JSON and the files to retire - only in the session's final entry. Every prefix a follower can be left holding is
therefore self-consistent, and a partially written new file is unreferenced bytes until the last entry publishes
it. Followers need no new code, because that sequence is the one they already receive.

The one difference from a `splitSchemaEntry` split carries the whole safety argument: the last chunk of an
instalment is still marked `moreChunksFollow`, because an instalment is not the end of the change. A follower that
mistook it for a publication would reload its schema from a half-delivered state, and #5443 measured what that
costs - the reload detaches a compacted sub-index it cannot resolve yet, the later real publication reuses the same
in-memory component, and the follower serves only its mutable pages from then on, silently and permanently.

The threshold is derived rather than configured: half the maximum replicated entry size, the same expression the
compaction path already uses for its own chunk budget. Lowering `arcadedb.ha.appendBufferSize` lowers it too, and
that is worth knowing before tuning it down: a smaller buffer means more instalments per rebuild, and therefore more
quorum round trips taken under the write lock (see below).

**What it costs**, stated precisely because it is a trade and not only a heap win. The recording session stays open
for the whole build, so a concurrent writer on the leader still waits on it - and, before that, on the database write
lock the callback holds. Bounding the heap does not shorten that window, and each instalment *lengthens* it: a quorum
round trip is now taken while that write lock is held, where the single final entry had always been submitted after
the callback returned and the lock was released. Normally that is milliseconds against a build measured in minutes;
against a slow or briefly partitioned quorum member it is up to `arcadedb.ha.quorumTimeout` per instalment, and the
whole database's writers wait it out. Accepted, because the alternative is not "no round trips" - without instalments
the one final entry carries the whole index, `splitSchemaEntry` splits it, and the same number of round trips happens
anyway, after the lock but only once the leader has held the entire rebuilt index in heap. Making the window itself
shorter means taking the build out of the recording session altogether, which is a different change on the same code
path.

**If the build fails after an instalment has shipped**, the followers are holding files and pages that the entry
which would have published or retired them never reaches, because it lives after the line that threw - and every
instalment chunk is marked `moreChunksFollow`, so there is no abandonment signal a follower could act on by itself.
The session now sends a compensating removal, and *which* files it retires is the whole correctness of it: one the
leader no longer has is retired (the case `BucketIndexBuilder.create()` produces, since it drops the half-built index
from its own error handler), while one the leader still has is left alone and reported, because retiring that would
take a state both sides agree on and make them disagree.

### The in-scan repairs are bounded too

Two repairs used to write from *inside* the vertex scan, where nothing can commit: `LocalDatabase.scanType` holds
the database read lock for the length of the scan, and the chunk iterator being walked would not survive a commit
taken under it.

- the back-reference fix-up - one write to a **far** vertex per edge found "not connected from the other side",
  each able to allocate a fresh edge-list chunk;
- `resetChain`, which drops an unreadable adjacency chain so it can be rebuilt from the surviving edge records.

Both are now *planned* during the scan and applied after it, through the same page budget as everything else. The
chain resets needed no new state at all: every `resetChain` call site already registered its vertex in one of the
two reconnect sets, so those sets were the complete list. The back-reference fix-up accumulates three RIDs per
defective edge in flat `int[]`/`long[]` arrays - and the `ArrayList<Edge>` the chain rebuild used to fill, which
held a fully materialised record per entry, was replaced by the same structure.

One in-scan write remains and is named rather than implied: the iterator `remove()` that prunes a dangling
adjacency entry. It is not deferrable the way the others were - the removal is made through the chunk iterator's
live position, and replaying it afterwards would turn a linear pass into a quadratic one - and it is the mildest of
the three, rewriting a chunk page the walk is already reading and never allocating one.

### `autoFix` now comes with a breakdown

`autoFix` is a count of repair **actions**, not of corruption instances: after #6131 it is records deleted plus
dangling adjacency entries pruned, so one edge that is both listed in a chain and corrupt as a record contributes
two. A rebuilt chain, meanwhile, contributed nothing at all and was visible only in the warnings. The one number an
operator reads to decide whether a run did anything was a mixture of three repair kinds with different weights, one
of them invisible.

`autoFix` keeps its meaning - existing readers and existing expectations depend on it - and the result now also
carries `removedRecords`, `prunedDanglingEntries` and `reconnectedEdges`. The first two sum to `autoFix`; the third
is deliberately not in that sum, since folding a rebuilt chain in would change every number a current run reports.
All three are always present, zero included. The first is named `removedRecords` rather than `deletedRecords`
because the result already carries `totalDeletedRecords` - records found in the DELETED state by the bucket scan,
nothing to do with repair - and a key one word away from it would read as its non-total sibling.

**One key pair is gone, which is a non-additive change to that map.** `GraphDatabaseChecker.checkVertices` used to
put the `List<Edge>` of reconnected edges under `outEdgesToReconnect`/`inEdgesToReconnect`; both keys are removed
and `reconnectedEdges` carries the count instead. Nothing in the repository read them, and they never reached the
`CHECK DATABASE` command result at all - `DatabaseChecker.updateStats` folds only `Long` values - so an operator
reading the SQL output never saw them. Only a caller invoking `checkVertices` directly as an API could have, and
holding a fully materialised `Edge` per reconnected entry was one of the unbounded-heap sources this change exists
to remove, so they are not coming back.

### The transaction-nesting headroom, measured rather than assumed

#6136 also reported the FIX path as sitting exactly on `DatabaseContext`'s hardcoded limit of 3 nested
transactions - the HTTP handler's, the arm's own, and the repair batch's - leaving no headroom. It does not.
`CheckDatabaseStatement.executeSimple` opens by rolling the caller's transaction back, so the handler's level is
released before the check starts; the arms' `begin()` then finds the last context inactive and reuses it rather
than pushing; and the repair batch commits and re-begins at that same level. The path runs at **one** level with
two to spare, and is now pinned both by sampling the depth it reaches and by running it under a cap of 1, so that a
future change adding a level arrives as a red build naming the cap rather than as a `TransactionException` in a
repair someone is running against a damaged production database.

[#6136](https://github.com/ArcadeData/arcadedb/issues/6136)

## Schema-WAL instalments: an incremental file split, a diagnostic for what a lost leadership leaves behind, and per-database metrics (#6142, #6143, #6144)

Three follow-ups filed against #6136, which made a schema session ship its buffered WAL in ordered *instalments*
instead of holding the whole payload - for an index rebuild, roughly the whole rebuilt index - in leader heap.

### The per-instalment rescan is gone (#6142)

Every instalment has to answer one question: which files has this session created that the followers have not been
told about yet? It answered it by walking the whole of `FileManager.getRecordedChanges()` and filtering out what it
had already shipped, which is O(instalments x file changes). That never mattered for the caller instalments exist
for - an index rebuild records one or two file changes however many instalments its WAL volume produces - but a DDL
that creates many files through the same buffered path would make both factors grow together, on the leader, while
it holds the database write lock.

The split is now carried forward instead of re-derived: `FileManager` keeps the session's creates as a consumable
queue that `drainRecordedCreates()` hands over, so a whole session costs one pass over its file creations. An index
into the cumulative list would not have worked - `dropFile` removes the cancelled create from the middle of it, so a
saved position stops meaning what it meant - and the queue mirrors the cumulative log at every site that touches a
create, including that cancellation and the post-rename name refresh of #4083.

### The files a lost leadership leaves behind can now be found (#6143)

A session that fails after shipping an instalment retires what those instalments announced. That works while the
node still leads. It cannot once leadership has moved - failover, a brief partition, a manual step-down, all
ordinary Raft events - because a node that lost the term can no longer submit anything. The removal fails, and the
files stay on the other nodes referenced by nothing. Nothing reads them, so this costs disk rather than
correctness, but only the node that FAILED logged anything about it, and the nodes actually holding them said
nothing at all.

`CHECK DATABASE` now reports them as `unreferencedFiles`, a result key of its own, and every node publishes its own
count as the `arcadedb.ha.schema.unreferenced_files` gauge - which is the one that matters under HA, since the check
itself runs on the leader for a replicated database. It **reports only**, in both modes: a file whose reference the
walk cannot follow would be data, and reclaiming disk is not worth that risk.

A result key rather than a warning, deliberately. A warning in this checker means the data is suspect, and an
unreferenced file is not a defect in the data: nothing is corrupt, nothing is lost, and supported operations produce
the state (a bucket created with `CREATE BUCKET` and not yet given to a type is exactly this shape, and so is the
file left behind by an index construction that refused its own arguments). Folding it into `warnings` would also
redefine what a clean database is for every caller that reads an empty warning list as the definition.

The classification therefore refuses to guess. It proves three shapes - a file the file manager holds with no
schema component at all (what an abandoned instalment leaves on a follower while it runs), a bucket no type claims,
and an automatic index no type references (what an abandoned rebuild leaves) - and treats everything else as
claimed without checking: the dictionary, a compacted sub-index, a bloom filter, a vector index's graph file, the
time-series internals, a manual index, and the edge-list buckets `GraphEngine` reaches by deriving their names from
the vertex bucket's own. A diagnostic that cries orphan over a healthy database is worse than none, because the
obvious response to it is to delete something; the guard against that is a test that builds one of everything and
demands silence, before and after a reopen.

The compensation's own failure branch is now covered too. It needed a step-down to land between an instalment and
the unwind, so it was previously reasoned about rather than tested; the removal is now submitted through an
injectable submitter and reports what it did, and a submitter that throws is - as far as this code can tell -
exactly a node that has lost the term.

### Instalments are measurable, per database (#6144)

The count was a JVM-wide `AtomicLong` whose only consumer was a test, and the per-instalment detail was a
detailed-level HA log line, which an operator debugging "every write on this database stalled for a while" could
only enable by reproducing the stall.

Instalments are now counted and **timed** per database and exported as `arcadedb.ha.schema.instalments`,
`arcadedb.ha.schema.instalment_time_ms` and `arcadedb.ha.schema.instalment_max_time_ms`, tagged
`database=<name>`. The duration is the number that matters: each instalment is a quorum round trip taken while the
database write lock is held, so it is what a stalled writer is waiting on and what eats into the
`arcadedb.ha.quorumTimeout` budget - and the max is what separates 200 fast round trips from 3 that each waited on
a slow quorum member. A session that shipped instalments also logs one INFO line summarising how many and how long,
so the common case is visible with no metrics backend at all.

[#6142](https://github.com/ArcadeData/arcadedb/issues/6142)
[#6143](https://github.com/ArcadeData/arcadedb/issues/6143)
[#6144](https://github.com/ArcadeData/arcadedb/issues/6144)
## A property `DEFAULT` is now compiled and validated once, at DDL time (#6134)

A `String` default value was stored verbatim by `Property.setDefaultValue()` and evaluated as an SQL expression by
`Property.getDefaultValue()`. Nothing validated it in between, and the evaluation swallowed every exception with a
bare `// IGNORE IT` that fell through to returning the expression's own source text. Three things followed from that.

**A default that does not parse was silently stored as its own source text.** `DEFAULT this is (not parseable` became
the literal string `this is (not parseable` on *every record of the type* - no error at DDL time, no error at insert
time, no log line. A typo in a `CREATE PROPERTY ... DEFAULT` quietly populated a whole type with garbage.

**A bare identifier silently became `null`, forever.** `DEFAULT active` - the obvious thing to type for a literal -
parses fine as a field reference, and the expression is always executed against `(Record) null`, so it resolved to
nothing on every insert. The literal has to be written `DEFAULT 'active'`. This interacted badly with validation: a
`NOTNULL` property whose default evaluated to null had the property *set* to null and was then rejected with "cannot
be null", an error pointing at the caller's data rather than at the broken schema default that caused it.

**The expression was re-parsed on every single record create.** ~3 µs per defaulted `STRING` property, per record: a
1M-record bulk load into a type with three defaulted properties spent ~9 s re-parsing the same three constant
expressions a million times each, on the insert hot path.

The expression is now compiled once, in `setDefaultValue()`, and the compiled form is what each record evaluates. The
compiled **expression** is cached, never the evaluated result, so `DEFAULT sysdate()` still produces a fresh value per
record. Because it is compiled at DDL time it is also *validated* at DDL time: an unparseable default and a bare
identifier are both rejected outright, with a message that says which one it is and, for the identifier, how to quote
it. An expression that parses but cannot be evaluated against a null record - `DEFAULT @rid` - now raises on insert
instead of writing its own source text into the record.

An existing database whose `schema.json` already holds such a default still opens: the schema load path logs a warning
naming the property and keeps the pre-#6134 behaviour for it rather than refusing to hydrate the schema. `ALTER
PROPERTY ... DEFAULT` can repair it, because it no longer evaluates the outgoing default to report it.

### One rule for applying defaults, and it is "a null default fills in nothing"

The rule was written twice - in `LocalDatabase.setDefaultValues`, for every record create, and in `ApplyDefaultsStep`,
for the SQL insert plan and `UPDATE ... APPLY DEFAULTS` - and the two disagreed. `ApplyDefaultsStep` guarded with
`if (defValue != null)`; `setDefaultValues` set unconditionally, and since it runs *after* the execution plan its
unguarded version always won. Both now call `DocumentType.applyDefaultValues()`, and the surviving rule is the guarded
one: a default that evaluates to `null` leaves the property untouched instead of setting it to `null`.

This is a visible change. A property whose default evaluates to null - `DEFAULT null`, or a legacy bare identifier -
is now **absent** from the record rather than present-with-`null`, so `has()` returns false and the key stays out of
the serialized document. In exchange, `NOTNULL` stops firing on it, and a `MANDATORY` property with such a default now
fails as "is mandatory, but not found", pointing at the schema gap that is actually there.

### Schema metadata reports the definition, not an evaluation of it

`SELECT FROM schema:types`, the server's schema info endpoint and the Cypher meta procedures all called
`getDefaultValue()`, which evaluates. Two things were wrong with that. The `DEFAULT_NOT_SET` sentinel leaked straight
through it, so every property *without* a default was reported with `"default": "<DEFAULT_NOT_SET>"` - including in
Studio's schema table. And a `sysdate()` default was reported as a timestamp snapshot taken when the row was fetched,
rather than as the expression that was declared.

There is now a separate `Property.getDefaultValueDefinition()`, which returns what the schema holds - `'active'`,
`sysdate()`, the text a `DEFAULT` clause would take back - and never evaluates anything. Every metadata path uses it;
only the write paths evaluate. `getDefaultValue()` no longer hands out the sentinel either: with no default defined it
returns `null`.

### The OrientDB importer bridges the two meanings of a default

OrientDB stores a property default as a plain value, so a bare `active` there means the literal string. The importer
passed it through unchanged, which under the old behaviour produced a property whose default silently evaluated to
null on every imported record, and under the new one would be rejected. It now imports the value as written when that
is a valid ArcadeDB default expression - `sysdate()`, numbers, already-quoted strings keep their meaning - and quotes
it as a string literal otherwise, logging which defaults it had to translate.

## A SQL trigger body that is a `SELECT` now actually runs, and can veto (#6167)

`SQLTriggerExecutor` ran a trigger's SQL body with `database.command(...).close()`, discarding the result set
without ever iterating it. A `SELECT` result set in ArcadeDB is pull-based - nothing runs until it is pulled - so a
`SELECT`-bodied trigger was a silent no-op: the DDL succeeded, the trigger registered, it fired on schedule, and its
body never executed. `INSERT`/`UPDATE`/`DELETE` bodies were unaffected, because their execution plans run eagerly
when the command is built.

The body's result set is now drained to exhaustion instead of closed unread, which fixes the no-op and also gives a
SQL trigger body the same veto contract the JavaScript executor already had: a body that evaluates to a single
scalar `false` - one row, one real property (`$score`/`$similarity` search-scoring metadata don't count) - aborts
the operation, which is what `SELECT false` or `SELECT <condition> AS ok` looks like it should do. The check is
scoped to bodies that are actually idempotent (`SELECT`), so an existing `UPDATE ... RETURN AFTER <field>` (or
`RETURN BEFORE`) trigger - which can produce that same one-row, one-property shape - is never misread as a veto
regardless of what the returned field holds.

**Upgrade note**: a `BEFORE READ` trigger with a non-trivial `SELECT` body that previously did nothing will now run
its query on every read of the type. This is the fix, not a regression, but it is worth budgeting for if such a
trigger exists in an upgraded database.

[#6134](https://github.com/ArcadeData/arcadedb/issues/6134)

## A constant-false filter and `LIMIT 0` are folded at plan time, so a schema probe stops scanning its target (#6174)

`SELECT * FROM (SELECT name FROM Character) SPARK_GEN_SUBQ_0 WHERE 1=0` is not an academic shape: it is what Spark,
and several BI tools over the Postgres wire, send to discover a query's schema - one probe per pushed-down query,
against the real target. The planner had no notion of a filter that is false for every row, so it built the full
scan and let the filter step drain it: every probe read the whole type to return nothing that the statement itself
did not already say. `LIMIT 0` is the other common spelling of the same intent and cost the same.

Both are now recognised while the plan is built and answered with an `EMPTY RESULT` source step in place of the
target fetch, which is what `EXPLAIN` now shows. Recognition is structural, over the condition tree the statement
wrote - through parentheses, and through AND/OR (an AND is false when any term is, an OR only when every
alternative is) - and folds only comparisons whose **both** operands are literals, evaluated by the very same
`BinaryCondition.evaluate` the filter step would have called. A bound parameter is never folded, because the plan
outlives the execution that bound it; nor is a function call, because nothing on `SQLFunction` marks a function as
pure and classifying a statement must not be a reason to invoke one. `WHERE uuid() = 'x'` and `WHERE ? = 0` are
therefore still planned as scans, as they must be.

What the fold deliberately keeps:

- **The target is still resolved**, so `SELECT FROM ATypeThatDoesNotExist WHERE 1=0` still reports the missing type
  rather than quietly returning nothing. Only the scan is dropped, after the fetch has been planned.
- **Everything downstream of the fetch stays in the plan** and receives no row, which is exactly what it receives
  today from the filter being removed: `SELECT count(*) FROM T WHERE 1=0` still returns its one row with 0, and the
  same statement with a `GROUP BY` still returns none.
- **A `LET` evaluated once per statement still runs**; only the per-record `LET` and the per-record projection work
  disappear, along with the rows that were never going to come back.

The fold applies wherever a `SELECT` plan is built, so a subquery, a `DELETE ... WHERE 1=0` and an
`UPDATE ... WHERE 1=0` are covered by the same code.

The two spellings are not recognised the same way, and it is worth being explicit about the difference. A
constant-false filter is folded only when the statement itself says so, never through a function. A `LIMIT 0`
truncates the result to nothing whatever the filter would have done, so the filter is not evaluated at all - which
means a predicate that raises at runtime stops raising: `SELECT * FROM T WHERE 1/0 = 1` reports a division by zero,
and the same statement with `LIMIT 0` now returns an empty result instead. Nothing else can observe the difference,
since no row was going to come back either way.

One behaviour change worth naming, and one wrong answer found next to it. `count(*)` on a bare type is answered
from the bucket counters by a hardwired plan that replaces the whole step chain - and returned without chaining the
statement's `SKIP` or `LIMIT` at all. So `SELECT count(*) FROM T LIMIT 0` returned one row, and
`SELECT count(*) FROM T SKIP 1` handed back the very row it was told to skip; `SELECT max(indexedProperty) FROM T
SKIP 1`, which is answered the same way from the index, did too. The `LIMIT 0` spelling is now folded away before
the hardwired plan is even considered; the rest is fixed by chaining both steps after the count/max/min, which
costs nothing on a plan that produces a single row. This is the SQL twin of the Cypher defect fixed in #5715, where
the CSR count push-down replaced the whole step chain and `RETURN count(*) LIMIT 0` likewise returned a row.

The Postgres wire protocol reuses this recognition instead of its own copy: the schema-probe detection added in
#6172 now calls `WhereClause.isAlwaysFalse()`, and the ~90 lines that duplicated it in `PostgresNetworkExecutor`
are gone.

[#6174](https://github.com/ArcadeData/arcadedb/issues/6174)

## HA never advertises a client address it cannot attribute to one node (#6183)

`arcadedb.ha.serverList` grew a `grpc:` field alongside `bolt:` in #6091, so a gRPC call refused on a follower can
name an address the caller dials directly. With neither field declared, a peer's endpoint for a protocol is derived
as *that peer's host* plus *this node's own port*, which is right for a homogeneous deployment and wrong for a
cluster whose nodes differ by port rather than by host - several nodes on one machine, which is what a dev or test
deployment usually is. Every peer then derives to the same address, so the "leader" a follower advertises is the
follower itself.

That was tolerable while the only consumer was Bolt's ROUTE response, where a driver that dials a follower for a
write gets an error and re-routes. #6091 raised the stakes: the gRPC refusal puts the address on a trailer and the
client rebuilds a `ServerIsNotTheLeaderException` around it, so a caller written to redirect *automatically* could
dial the same follower, be refused again, and loop.

Two peers can never legitimately answer on one `host:port` for one protocol - they would be fighting over the
socket - so an address claimed by two peers identifies at most one of them and the resolver cannot say which. The
routing table now drops such an address and, when it is the leader's own, is not built at all. Both callers already
handle that: Bolt's ROUTE falls back to advertising this node as READ and ROUTE, never as writer, and the gRPC
refusal falls back to naming the leader's HTTP address with the "use its gRPC port" caveat - what each did before
#6091, which beats a confidently wrong address. A declared address outranks a derived one when the two collide,
since the collision only proves the guess wrong, so declaring the ports of the nodes that differ is enough. The
guard lives in the resolver both protocols share, and a WARNING naming the field to declare is logged once per
protocol.

**A leader-only refusal names the leader wherever it is reported, not just from `graphBatchLoad`.**
`graphBatchLoad` was the only RPC that built the redirect trailers, by hand; a `ServerIsNotTheLeaderException`
raised anywhere else fell through `GrpcErrorMapper` as a plain `NeedRetryException` - `ABORTED`, no address - so a
caller could not tell "the leader is unknown, wait for the election" from "the leader is known and nobody told
you". The mapper now attaches the leader's gRPC and HTTP addresses for that exception, and answers
`FAILED_PRECONDITION` rather than `ABORTED`: retrying as it stands means asking the same follower again, which is
the same reading the HTTP protocol takes when it answers 400 rather than 503. `ArcadeDbGrpcService.notTheLeader`
is now just the wording around that shared mapping.

That covers the RPCs that report through the mapper - `executeCommand`, `createRecord`, `beginTransaction`,
`commitTransaction`, `graphBatchLoad`. The handlers that assemble a status themselves (`updateRecord`,
`lookupByRid`, the streaming and bulk-insert paths) neither check leadership nor mutate schema, so none of them
can raise this exception today; one that grows either has to route its errors through the mapper to stay
redirectable, which the mapper's javadoc now says.

Note that a statement sent to a follower does not normally produce a refusal at all: `RaftReplicatedDatabase`
forwards a DDL or non-idempotent statement to the leader instead of rejecting it, and `graphBatchLoad` refuses
deliberately, because relaying a bulk load through a follower would double the traffic of the transport chosen to
avoid exactly that. What the mapper change covers is the leadership change that lands between the forwarding
decision and the schema write, where the caller is holding a failure it can only act on if it is told which node to
repeat it against.

**Upgrade note**: the status a leader refusal carries changes from `ABORTED` to `FAILED_PRECONDITION` on the
mapper's RPCs, which is client-visible for anyone switching on the gRPC status code. A client reading the
`arcadedb-exception-class` trailer - which is what the bundled Java client does, and what the trailer is for - is
unaffected: it rebuilds `ServerIsNotTheLeaderException` either way. A client that treats `ABORTED` as "retry this
call" needs to treat `FAILED_PRECONDITION` plus a leader-address trailer as "dial that address and repeat it"
instead; retrying as-is was never going to succeed, since it asks the same follower again. `graphBatchLoad`
already answered `FAILED_PRECONDITION` and is unchanged, and on the other RPCs the condition is reachable only
through the leadership-change window described above, so in practice almost nothing is looking at the old code.

[#6183](https://github.com/ArcadeData/arcadedb/issues/6183)

## A concurrent update of a placeholder-backed record is a conflict, not a silent overwrite (#6141)

Two transactions updating the same record could both commit, with the second silently dropping the first write and
no `ConcurrentModificationException` raised anywhere, when that record's content sat behind a **placeholder
pointer**: 8 bytes in the record's own slot, the content on another page.

The reason is that a record update is deferred. `TransactionContext.addUpdatedRecord` pins the record's OWN page at
`save()` time, and the write itself runs at commit, after the file's commit lock is taken - so every other page the
write touches is loaded fresh, at the newest committed version, and can never fail a page-version check. For a
placeholder-backed record that is the whole record: the pinned page holds the pointer, which an update the content
record can absorb never touches (a modified page with an empty modified range is dropped at commit), and the
content page is pinned by nobody. Both writers read it fresh under the lock and the last one won.

The fingerprint #6129 introduced for the chunk chain of a multi-page record answers exactly this question, for
exactly this reason, so it is generalised rather than duplicated. `LocalBucket.chunkChainTailFingerprint` becomes
`offPageContentFingerprint` - "the part of the record that does not live in its own slot" - and covers the
placeholder's content record as well as the chain past a head chunk. It is taken when the record is taken for
update, the same moment its page is pinned, and compared at commit before the write.

The two uses differ in two ways, both deliberate. The placeholder half is captured whether or not the disjoint-slot
merge is enabled: for that record shape it is the only conflict detection there is, while the chain tail is a merge
precondition that the unconditional page poisoning already covers when the merge is off. And a mismatch raises a
retryable `ConcurrentModificationException` rather than merely excluding a page from the merge.

Both arms of the update path are covered: the update the content record absorbs, and the one that no longer fits it
and has to delete the content record and re-spill. An unrelated concurrent update - including one landing on the
content record's own page - still commits, because the fingerprint is about that one record and not about the page.

Since #6149 new placeholders are only produced on a page with a free tail of exactly zero, so what this reaches in
practice is databases written before that change plus that one remaining fallback. The defect itself predates both.

[#6141](https://github.com/ArcadeData/arcadedb/issues/6141)

## A record's continuation chunks keep off the page holding its own head chunk (#6175)

The allocator preferred that page twice over: `findAvailableSpace` starts with "prioritize space in the same page",
and when that fails it scans the free-space statistics from the lowest page id up - so a chain routinely came home
to the page holding its head chunk. Measured on a 9-byte record grown to 200 KB while sharing its page with one
neighbour (64 KB pages): chunk 1 to a new page, chunk 2 to a new page, **chunk 3 to page 0 - the head chunk's own
page** - chunk 4 to a new page.

A continuation chunk is written through inline record-table writes that no tracked slot image accounts for, so the
page that receives one is poisoned for the rest of the transaction. When that page is the head chunk's own, the
poison falls on the one page #6129 built `SLOT_KIND_FIRST_CHUNK`, the chain fingerprint and the region
re-derivation to keep mergeable - a chunked record's update is a single-slot write on it - and it takes every
unrelated record sharing that page down with it. In the measured fixture, a concurrent update of the untouched
neighbour turned into a hard `ConcurrentModificationException`.

`findAvailableSpace` now takes a page to avoid, and the two chunk-allocation loops pass the head chunk's page -
but only when the caller declared that slot replayable. When it did not (a record being created, a spill onto a
page that is poisoned anyway) the page has nothing left to lose and refusing its free tail would cost space for
nothing, which is also what leaves record creation allocating exactly as it did before. Only that one page is
excluded, so the chain's own earlier pages and every other record's pages are reused as they were: rewriting a
chunked record ten times leaves the bucket's page count where it was.

[#6175](https://github.com/ArcadeData/arcadedb/issues/6175)

## A record that shrinks back inside its slot is a plain record again (#6178)

Once a record had spilled it stayed a chunk chain for the rest of its life. However far it shrank back,
`updateMultiPageRecord` freed the continuation chunks and left a chain of exactly one chunk - a marker, a 4-byte
size, an 8-byte next pointer that is 0, and the content. That cost 13 bytes per record permanently, routed every
read and write through the chunk path, and made `CHECK DATABASE` report a multi-page record that was not one.

An update whose content fits the region the record's slot owns on its own page now writes it back as a plain
record and frees the chain. It is the mirror of the spill, with a slot kind of its own
(`SLOT_KIND_CHUNK_COLLAPSED_TO_RECORD`) so the disjoint-slot merge can replay it: the pre-image is a head chunk,
the final image a plain record, and the replay re-derives the region from the committed page exactly as the
head-chunk replay does before writing. The bound is the region and not a byte more - the same rule #6163 sizes the
head chunk with - so a record that shrank a long way but still does not fit its slot stays a chain rather than
claiming a neighbour's bytes.

**This removes the state #6163's grow-back exists to repair, rather than compensating for it.** Content that fits
the head chunk (13 bytes of header) fits the region as a plain record (at most 5 bytes of size marker), so the two
conditions coincide: a head chunk that declares less than its region no longer survives an update, and a persisted
chunk head always fills its region. Measured by instrumenting the grow-back: with the collapse disabled,
`Issue6163HeadChunkRoomTest` alone drives it 12 times, every one of them from a head chunk that had ratcheted down
to 11 bytes; with the collapse enabled, zero. The #6163 machinery stays as the defence for a region that grows for
another reason, and its replay refusal costs nothing.

One chunk head is deliberately never collapsed: the CONTENT record of a placeholder, which
`createRecordInternal` spills into a chain of its own when no page can host it whole. Such a record is identified
by the negative size marker a plain collapse would replace with a positive one, and a positive size marker is
exactly what tells a bucket scan that a slot holds a document of its own.

[#6178](https://github.com/ArcadeData/arcadedb/issues/6178)

## A truncated batch upload no longer applies its own bytes a second time (#6180, #6176)

`POST /api/v1/batch` could load records the client never sent in that request: its own request head and a replay
of records it had already committed. On a type with a unique index the replay collided with what it duplicated and
the load was answered **409 Conflict** for a key the client had sent exactly once, instead of the **408** and the
partial-commit counts a truncated upload is contracted to get. On a type without one - which is what a bulk load
usually has - it duplicated rows and answered 200.

The connection is what lies, and the reader is what asks it. Undertow's `UndertowInputStream` takes a buffer from
the pool *before* the channel read that fails and does not give it back, so the buffer stays on the stream holding
whatever the pool last left in it - on this connection, its own request head and the records already parsed. The
next read is then served from that buffer without the channel being touched at all. What reaches the failure is
`InputStreamReader`, which probes the stream between decodes (`StreamDecoder.inReady` calls `available()`) and
**swallows** the `IOException`: the failure that should have ended the load disappears, and the parser is handed
the replay as payload. Measured on `main` with a 520-record payload sent in two chunks: reads of 8192 and 8172
bytes past the end of the upload, beginning at the very record the second chunk began with, and a total of 40284
bytes consumed against the 23920 the client sent.

The batch handler's own body stream now ends a body that has failed. The first failure is remembered, whether it
surfaces on a read or on a probe; the probe answers -1 - Undertow's own "the body is finished", which the
truncation check already reads - so the reader stops asking instead of swallowing anything, and every later read is
refused with the recorded cause. A load that hits it takes the truncated-body path it was always meant to take:
408, the counts needed to resume, and not one record the client did not send. The forwarding path a follower uses
to relay a batch to the leader reads through the same guarded stream, so a cut upload cannot relay a replay of
itself either.

Every answer now carries `bytesRead`, not only the successful one. On a truncated load that is where a client
learns how far the server got, and it is the number that says the server read no further than the client wrote:
`bytesRead` equal to the bytes sent is now asserted by `Issue5470BatchStreamStallIT` in both of its truncation
tests. The OpenAPI schemas for the batch response and the batch error were completed to match what both bodies
have carried since #5618: `bytesRead`, `linesRead`, `linesSkipped` and `verticesWithoutId`.

[#6180](https://github.com/ArcadeData/arcadedb/issues/6180)
[#6176](https://github.com/ArcadeData/arcadedb/issues/6176)
## A materialized-view refresh is one transaction, so a failed one no longer empties the view (#6203)

A full refresh looked like a single transaction and was not one. `TRUNCATE TYPE ... UNSAFE` commits the caller's
transaction from the inside: `schema.dropIndex()` for every index on the backing type before a single record is
touched, then `commit(); begin()` once per `arcadedb.truncateBatchSize` records, then once more before it rebuilds
the indexes it dropped. Wrapping that in `database.transaction()` bought nothing, and both consequences were
silent.

Every other reader saw the view empty, or holding some prefix of the new rows, for the whole runtime of the
defining query - which for a view worth materialising is exactly the expensive case, and for a PERIODIC view is
every tick. Nothing distinguished that from a legitimately empty or small result.

Worse, a defining query that threw after the truncate had committed left the backing type with zero rows. The view
then reported `ERROR`, or `STALE` once `MaterializedViewChangeListener` overwrote it, and both of those read as
"the data you are looking at is old" when the data was in fact gone. It stayed gone until some later refresh
happened to succeed, which for a MANUAL view may be never.

The refresh now writes the defining query's rows over the previous snapshot's records inside one transaction,
deleting only the surplus and creating only the shortfall. The pass gets the isolation every other ArcadeDB
transaction has: every write stays in the transaction's own page buffer until commit, a concurrent reader sees the
previous snapshot throughout and the new one afterwards, and a failure anywhere rolls the whole pass back onto the
previous snapshot rather than onto nothing. A view's rows also keep their `@rid` across a refresh, where before
every pass gave them new ones.

Neither trigger fires on the trivial case, which is why this survived the existing tests: an unindexed view smaller
than one truncate batch was already atomic. The regression tests cover both - an index on the backing type (the
`dropIndex` commit, at any size) and a view larger than `arcadedb.truncateBatchSize` (the in-scan commit).

**Rewriting in place rather than clearing and repopulating is what keeps an index on the backing type from paying
for the atomicity**, and it makes an indexed refresh cheaper than it was before. Clearing the view would give every
row a new RID, and `TransactionIndexContext` collapses a REMOVE followed by an ADD only on the same (key, RID), so
each pass would leave a full set of real tombstones where the truncate used to drop the index and rebuild it empty.
Measured over 120 refreshes of a 2000-row indexed view: a flat 256KB for the old truncate, 256KB growing to 3.9MB
and still climbing for clear-and-repopulate, and a flat 256KB for the rewrite - because
`DocumentIndexer.updateDocument` finds the key values unchanged for every row a stable view reproduces and skips the
index outright. Zero index writes per pass, against a full rebuild per pass before. The backing buckets stay flat
too, since no record is freed and reallocated. A `@Tag("slow")` soak test guards the bound.

**The shadow-type swap the issue proposed was rejected.** Building into a second backing type and repointing the
view at it reads better on paper - the swap is metadata only and the old rows are dropped as files rather than
deleted one by one - but in this engine the swap has no cheap implementation. Renaming a type renames its buckets,
and `PaginatedComponent.rename()` first waits for every page of the *whole database* to be flushed; paying a
database-wide flush barrier twice per refresh, on every tick of every PERIODIC view, is a worse defect than the one
being fixed. Repointing the view at a differently named backing type avoids the barrier but makes the view's own
name stop being the type name, which is what makes `SELECT FROM <view>` work at all and what every record's `@type`
reports. What the single transaction costs instead is bounded and not new: it holds the pages of the rows it
rewrites, and under HA the pass is one Raft log entry rather than one per truncate batch plus one for the
repopulate. The repopulate was already one unbounded transaction, so a view too large to refresh in one transaction
was already too large to refresh.

### The scheduler no longer pins an abandoned database to a live thread

`MaterializedViewScheduler` guarded against a database abandoned without `close()` by holding it through a
`WeakReference` and cancelling the task once that cleared. It could not clear, for two independent reasons.

The refresh runs a transaction on the scheduler thread, which installs a `DatabaseContext` entry keyed by the
database path holding a strong reference to the database, and the scheduler never removed it - so after the very
first refresh the weak reference was moot. `TimeSeriesMaintenanceScheduler.runMaintenance()` documents the same
hazard and takes the entry back down in a `finally`; the refresh now does too, removing only a context that pass
installed itself.

The task also captured the `MaterializedViewImpl` strongly, and a view holds its own database, so the reference
that was weak was not the only one. Both are weak now. The task keeps the view's name as a `String` and cancels
itself when either reference clears, unregistering itself only while it is still the task registered for that view
- a task that self-cancels after an `ALTER MATERIALIZED VIEW` or a schema reload must leave its replacement
scheduled.

Finally, the scheduler is created per `LocalSchema` but its thread was called `ArcadeDB-MV-Scheduler` with no
discriminator, so a JVM with several open databases got several identically named threads and a thread dump could
not say which was which. The database name is now part of it.

[#6203](https://github.com/ArcadeData/arcadedb/issues/6203)

## A retryable conflict raised while a command runs is retried, and reported as retryable (#6201)

`DatabaseAbstractHandler` runs `execute()` inside the engine's retrying transaction and converted **every**
exception the handler raised into a `TransactionException`. Two dispatchers key on the exception type, and the
wrapper broke both.

`LocalDatabase.transaction(...)` decides what to retry with `catch (NeedRetryException | DuplicatedKeyException)`.
A conflict raised while the command ran - a `save()` losing an MVCC race, an index update hitting a concurrent
key - arrived there already wrapped, matched neither arm, fell into the generic `catch (Throwable)` and
propagated on the first attempt. Only a conflict detected by the wrapper's own `commit()` was ever retried, so
the `retries` a client asks for had no effect on most of the conflict surface.

The HTTP error mapping keys on the type too, and the wrapped arm had no `NeedRetryException` branch, so the same
conflict was answered `500 "Error on transaction commit"` - which names neither the reason nor that the write can
be re-driven. `PostBatchHandler` documents the contract the other way round ("NeedRetryException -> 503, security
-> 403, RecordNotFoundException -> 404, ... are left to the base handler"), and that was true only when no
transaction wrapper was active. A client whose retry policy keys on 503 gave up on a write that would have
succeeded on retry.

The wrapper now rethrows a `RuntimeException` unchanged and wraps only the checked exceptions
`TransactionScope.execute()` cannot declare. Every ArcadeDB exception is unchecked, so both dispatchers see the
type they dispatch on.

**The mapping itself is now written once.** It was three hand-written `instanceof` chains - one for an exception
that reached the boundary as itself, one inside the `CommandExecutionException` arm and one inside the
`TransactionException` arm - and every mapping added since had to be added to each separately: #4350 (duplicated
key), #5064/#5075 (committed remotely), #5191 (parsing), #5219, #5602 (arithmetic), #5935 (malformed JSON), #6191
(not the leader). The half that was missed each time took a second issue to notice, and #6201 is that second
issue for `NeedRetryException` and `RecordNotFoundException`. A single ordered classification, applied to the
exception and - for the generic wrappers only - to its cause, closes those two and every other asymmetry at once:
a wrapped `IllegalArgumentException`, a wrapped `ServerSecurityException` and a wrapped `HttpSessionException`
were each mapped by one or two of the three chains and not the rest. `Issue6201ErrorStatusParityTest` pins the
property rather than the arms - the same exception raised inside and outside the wrapper must answer the same
status - so a mapping added to one branch and not another fails there rather than in a future issue.

One nuance the three chains had disagreed on is now stated rather than inherited: a `CommandExecutionException`
that nothing more specific matched is reported *as itself*, because the engine raises it to say what failed
("Backup failed for database 'x' to directory 'y'") and its cause is often just plumbing; a `TransactionException`
is reported *as its cause*, because that wrapper is put on by the plumbing and its message says nothing the
status label does not. Neither loses information - the error body's `detail` field renders the whole cause chain
either way.

[#6201](https://github.com/ArcadeData/arcadedb/issues/6201)

## The Ratis-initiated snapshot install makes the refusals the manual one makes (#6202)

`ArcadeStateMachine` pulls a database snapshot from the leader on **six** paths, and only the manual one
(`triggerSnapshotDownload()`) checked what it was about to dial. The other five resolved an address and dialled
it with no check, or with only part of one:

| Path | Guarded before |
|---|---|
| `triggerSnapshotDownload()` - watchdog / leader change / persistent-lag recovery | leader role + self address (#6111) |
| `notifyInstallSnapshotFromLeader()` - the Ratis-initiated install, when a follower's log is behind the leader's compacted log | nothing |
| `triggerDatabaseResync(String)` - targeted resync of one quarantined database | nothing |
| `applyInstallDatabaseEntry` - the `forceSnapshot` restore flow | leader role only |
| `installFromLeaderForBootstrap` - the bootstrap-fingerprint-mismatch reinstall | nothing |
| `resyncDatabaseFromLeader` - operator-triggered emergency recovery | leader role + address known |

The address is only as good as the configuration behind it. With no `http` port declared in
`arcadedb.ha.serverList` a peer's HTTP endpoint is derived from that peer's Raft host plus **this** node's HTTP
port, so on a cluster whose nodes share a host - several nodes on one machine, a compose file, a developer laptop
- every peer collapses onto this node's own endpoint, and on a cluster with mixed ports it can name the wrong
peer. Neither outcome reported an error: `reconcileDatabasesFromLeader` succeeded, the install was recorded, the
stale-read floor of #6111 was dropped, and the node returned to the ready set carrying whatever it had copied -
its own incomplete databases, or a peer's that is itself behind.

The two paths that re-resolve the address on **every download attempt** - `SnapshotInstaller.install` takes
address suppliers precisely because leadership can move mid-operation - re-guard it on every attempt too, through
both the HTTP and the HTTPS supplier. Guarding only the call site would leave the refusal a point-in-time check
that attempt 3 walks straight past, and guarding only HTTP would leave it unreachable on an SSL cluster, where
`downloadWithRetry` prefers the HTTPS endpoint and only falls back to HTTP when it comes back null.

All six now ask one helper, so they cannot drift apart, and it refuses three things rather than two: this
node being the leader, an address that is this node's own, and **an address that does not identify a single
peer**. The last is the ambiguity check `selectUnambiguousRouting` makes for client routing tables (#6183),
applied where it matters more - a confidently wrong routing address costs one redirect, a confidently wrong
snapshot source cannot be undone. Refusing leaves the follower out of the ready set and visibly behind, which is
the state it is actually in; Ratis retries the install and the `HealthMonitor` re-arms the manual path.

Two things the same method carried:

- The install ran on the JDK common `ForkJoinPool` (`CompletableFuture.supplyAsync` with no executor), against
  the rule at the head of `QueryEngineManager`'s class javadoc - that pool is shared with Gremlin and Polyglot
  user code and with JDK internals, and a snapshot install is a full database download, the longest-running thing
  the HA layer does. It has a dedicated single-worker executor now (`arcadedb-raft-snapshot-install`), with a
  bounded queue and an abort policy turned into a failed future: caller-runs would put the download back on the
  Ratis thread the offload exists to protect.
- The single-flight `AtomicBoolean` did not serialise. The install proceeds when it *loses* the CAS - standing
  down would report an install it never performed - so it could run concurrently with a request-driven resync
  over one set of database directories. That was argued benign because both pull from the same leader and
  `SnapshotInstaller` swaps atomically, but the argument is about today's installer and would outlive whoever
  remembers it. A lock states the exclusion instead: the install waits for it (it owns its thread now), while the
  request-driven paths fold into whatever holds it, exactly as they already fold into a lost CAS rather than
  parking the single-threaded lifecycle executor.

[#6202](https://github.com/ArcadeData/arcadedb/issues/6202)

## The cluster-verify endpoint reports a peer it could not contact as unverified, not as agreeing (#6221)

`POST /api/v1/cluster/verify/{database}` exists to tell an operator whether the cluster's copies of a database
agree, and it resolved each peer's HTTP endpoint through the best-effort resolver. With no `http` port declared in
`arcadedb.ha.serverList` that endpoint is derived from the peer's Raft host plus **this** node's HTTP port, so on a
cluster whose nodes share a host and differ by port - several nodes on one machine, a compose file, a developer
laptop - every peer collapses onto the address of the node doing the resolving. Two things then went wrong at
once, and both pointed the reassuring way:

- the leader compared itself against itself, matched on every file, and recorded the **peer** as `CONSISTENT`,
  rolling up to `overallStatus: ALL_CONSISTENT` - a clean bill of health from the divergence detector for a node
  that was never contacted, produced at the moment an operator is asking precisely because they suspect
  divergence;
- the query landed back on the leader, where `isLeader()` is still true, so it fanned out again: (N-1) in-flight
  requests per level, each level CRC-ing every byte of every file before recursing, each hop holding an Undertow
  worker thread. The peer connect/read timeouts bound one hop, not the depth.

Every peer is now dialled through one helper, `PeerDialAddress`, which answers "may this node dial that one, and
at which address?" next to the resolution rather than at each call site: it refuses an absent peer, this node
itself, an address that identifies no single peer (#6202) and an address that is this node's own, including the
loopback spellings of #6204. The six snapshot-resync paths of #6202 now delegate to it as well, so the endpoint
that checks the cluster and the paths that repair it cannot drift apart. The fan-out also carries the one-hop
marker the write-forward paths have set since #6191, so a node serving a query a peer fanned out to it answers
with its own checksums instead of fanning out again - the bound no local self-check can provide, for an address
that names the wrong *peer* rather than this node.

**Observable change for HA alerting.** A peer that could not be verified is now reported as
`status: ERROR` with the reason, and `overallStatus` has a third value, `VERIFICATION_INCOMPLETE`:

| outcome | before | now |
|---|---|---|
| every peer compared and agreed | `ALL_CONSISTENT` | `ALL_CONSISTENT` |
| a compared peer's checksums differ | `INCONSISTENCY_DETECTED` | `INCONSISTENCY_DETECTED` |
| a peer could not be verified (unreachable, timed out, or no address identifies it) | `INCONSISTENCY_DETECTED` | `VERIFICATION_INCOMPLETE` |

The guard covers the encrypted endpoint too, which is not the formality it looks like: a peer's HTTPS address is
read from the 5th field of `arcadedb.ha.serverList` where its HTTP address is read from the 3rd, each with its own
derive fallback onto **this** node's port for that protocol. A cluster that declares distinct `http` ports and
omits the `https` ones therefore passes the HTTP check with every peer's HTTPS endpoint still collapsed onto this
node's own. `getUnambiguousPeerHttpsAddress` and `getLocalHttpsAddress` ask the HTTPS endpoint the same two
questions; withheld, it reads to the caller exactly like an absent one, so the dial falls back to the guarded
plain-HTTP endpoint rather than being refused. The snapshot-download paths of #6202 take the guarded HTTPS address
now as well, and so does the leader's automatic resync of a stalled replica (#4728), whose payload - "drop your
copy of every database and download it again" - is the most destructive of the family.

`ALL_CONSISTENT` is no longer reachable while any peer is unverified - an unverified peer is not a consistent one
- and a node being down is no longer reported as a divergence somebody has to go and find. Alerting that keys on
`overallStatus != "ALL_CONSISTENT"` is unaffected; alerting that keys on `INCONSISTENCY_DETECTED` specifically
should add `VERIFICATION_INCOMPLETE` to keep catching "a node is unreachable". Studio shows the new status as a
warning and renders each peer's error alongside its status.

Also in the same change: `retries` on an auto-committed request (`POST /api/v1/command`, `/api/v1/batch`, and
every other handler that auto-commits) defaulted to a hard-coded 1, so nothing retried unless the client asked
for retries it had no reason to know existed. That was dead code until #6201 made the wrapper stop flattening
exceptions, and live afterwards - an MVCC conflict a second attempt would have committed was answered 503 on the
first. It now follows `arcadedb.txRetries`, the attempt count the embedded `Database.transaction(...)` and
`RemoteDatabase` already take, and which an operator can turn per database. The engine still retries only the two
conflict families, caps a duplicated key at one retry, and rolls the transaction back before every attempt.

**Observable change for HTTP clients.** An auto-committed request that hits an MVCC conflict is now re-executed
up to `arcadedb.txRetries` times (default 3) instead of failing on the first attempt, so a write that used to be
answered 503 under contention now usually succeeds. Nothing the failed attempt wrote survives - the engine rolls
back before every retry - but the *command itself* runs again, so a SQL/JavaScript function with a side effect
outside the database (an HTTP call, a file write, a counter in an external system) can now perform that side
effect more than once per request. Set `retries` to 1 in the request payload, or lower `arcadedb.txRetries`, for
commands where that matters.

[#6221](https://github.com/ArcadeData/arcadedb/issues/6221)
## `TRUNCATE TYPE` / `TRUNCATE BUCKET` inside a transaction no longer commit it, so `ROLLBACK` puts the records back (#6220)

`BEGIN; TRUNCATE TYPE Staging UNSAFE; INSERT ...; ROLLBACK;` did not put the old rows back. The statement
committed the caller's transaction from the inside, three ways: `schema.dropIndex()` for every index on the type
- a schema change, applied immediately - before a single record was touched; `commit(); begin()` inside the scan
callback every `arcadedb.truncateBatchSize` (default 1000) records; and one more `commit(); begin()` before the
dropped indexes were rebuilt over what were by then empty buckets. An explicit `ROLLBACK` therefore recovered
only whatever the last, uncommitted batch happened to hold: nothing at all on a type with **any** index, since
the drop and the pre-rebuild commit landed either side of every delete. Nothing reported it - the statement
returned `operation: truncate type` and the rollback returned normally - and the single most natural use of
`TRUNCATE`, reloading a staging table in one unit of work, was exactly the shape that lost data. It is the same
defect that made a failed materialized-view refresh destroy the view in #6203, met there by taking `TRUNCATE` out
of the refresh path; this fixes the statement for everybody else.

Who owns the transaction now decides which of two paths runs:

- **A transaction is already active.** The truncate is one operation inside somebody else's unit of work, so it
  commits nothing and changes no schema: every record is deleted in the caller's transaction with the indexes
  maintained per record, exactly as `DELETE FROM` does, and the caller's `COMMIT` or `ROLLBACK` decides the
  outcome of the whole thing. `arcadedb.truncateBatchSize` is ignored here.
- **No transaction is active.** The statement owns one, and keeps today's fast path in full: drop the indexes,
  delete in small committed batches, rebuild the (empty) indexes. The batching is a throughput and HA concern
  (one small Raft log entry per batch, #4817) and never was an atomicity one, so nothing is lost by giving it up
  in the first case - the caller's own commit is one entry either way.

The result set now carries a `transactional` flag saying which path ran, so a caller can tell whether its
`ROLLBACK` will undo the truncate rather than finding out afterwards.

The second half of the fix is that the fast path is now *reachable*. Deleting a record requires an active
transaction, so with none active every delete used to fail with `Transaction not begun` - the batching and the
index drop/rebuild could only ever run on a caller's transaction, which is precisely where they must not. The
statement opens its own transaction now, commits it on success, rolls it back on failure (leaving the buckets and
the rebuilt indexes agreeing, where before a failure left the index populated with records the caller's commit
was about to delete), and always hands the thread back without a transaction on it.

**Choosing the fast path deliberately.** Embedded, it is `database.command("sql", "TRUNCATE TYPE ...")` with no
`begin()` around it. Over HTTP the request's auto-commit transaction is what makes the statement transactional,
so `"autoCommit": false` in the request payload selects the fast path for a bulk clear that does not need to be
undoable. Anything wrapped in a transaction - `database.transaction(...)`, an HTTP session transaction, a
`sqlscript` between `BEGIN` and `COMMIT` - now gets correctness instead, at the cost of one index maintenance
per record and of holding the deleted pages until commit.

[#6220](https://github.com/ArcadeData/arcadedb/issues/6220)
## `CHECK DATABASE` now reports an orphan edge record, and can reclaim one on request (#6090)

An **orphan edge record** is an edge record that exists physically in an edge-type bucket, whose `@out`/`@in`
name valid vertices, but which no vertex's edge list points back at: `countType()` counts it, no traversal
reaches it. `GraphDatabaseChecker.checkEdges` already scanned every edge record and already probed both
endpoints for a back-reference - the scan and the direction were both right - and then discarded the answer.
Both call sites did nothing but `missingReferenceBack.incrementAndGet()`. No warning named the record, nothing
entered `corruptedRecords`, and so `CHECK DATABASE FIX` never reclaimed one.

The published counter could not stand in for the finding either, because it is not edge-type aware. A
perfectly healthy **unidirectional** edge is legitimately absent from its target's IN list and bumps it by 1; a
genuinely orphaned bidirectional edge bumps it by 2. One aggregate, no breakdown, no RIDs - on a database that
uses unidirectional edge types at all, the signal is buried. The honest way to answer "does my database hold
any?" was to run a full traversal yourself and compare it against `countType`.

The probe is now **direction-aware**, and what it establishes is reported:

- an edge absent from its OUT vertex's OUT list is a defect for every edge type - no outgoing traversal reaches
  it - and is counted under `edgesMissingOutReference`;
- an edge absent from its IN vertex's IN list is a defect only when the type is BIDIRECTIONAL, counted under
  `edgesMissingInReference`;
- an edge **neither** list names is an orphan record: warned with its RID and both endpoints, counted under
  `unreachableEdgeRecords` and named in `unreachableEdgeRecordsFound`.

`missingReferenceBack` is deliberately untouched - both probes still run and it is still bumped once per side
that holds no reference - so anything parsing today's result keeps reading the same number.

The three new counters are **not disjoint**, which matters if you render them as a summary: one orphaned
bidirectional edge increments all three. Each answers "how many edges have this defect" independently, so
summing them double-counts. `unreachableEdgeRecords` is the orphan total; the other two are the per-direction
detail behind it, plus the half-linked edges that are still reachable from one side.

Reclaiming them is a new, explicit clause: **`CHECK DATABASE FIX DELETE ORPHANS`**. It is not folded into plain
`FIX`, and the reason is a data-loss path rather than a taste for options. The detection cannot distinguish
"this record is garbage a failed bulk load left behind" from "this vertex lost its head-chunk pointer, so its
perfectly good edges look unreferenced" - a null head chunk reads exactly like a vertex with no edges. In the
second case the edge records are the only surviving description of that adjacency and the thing `RESTORE
VERTEX` rebuilds it from, so a repair that deleted them by default would turn a recoverable database into an
unrecoverable one. Reporting is therefore always on; removing is always asked for, and `DELETE ORPHANS` without
`FIX` is refused rather than silently implying it.

The classification is conservative by construction: an edge is called unreachable only when **both** probes
positively established the absence. A far endpoint that could not be loaded is already reported as a dangling
link, and one whose list could not be walked is left to `checkVertices`, which runs after this pass and
rebuilds the chain from the surviving edge records - neither may be answered by deleting the record that repair
reads from.

#6089 stopped `GraphBatch` from creating orphans on a failed flush; this is what finds the ones an older build
already left behind.

[#6090](https://github.com/ArcadeData/arcadedb/issues/6090)

## The restore is now the fast half of a recovery too: parallel per entry, and no longer reading the archive 512 bytes at a time (#6086)

Making the backup 27x faster (#6072) moved the bottleneck rather than removing it. On the same 1.25 GB database
the backup took 0.68 s and the restore that undoes it 2.9 s, so the number that actually matters when someone is
recovering - the RTO - had barely moved.

### What the measurement said, and what it contradicted

The issue proposed two candidate diagnoses and asked for the numbers before any code. `RestoreParallelBenchmark`
times the two halves of a restore separately, and they are not close: on that 1.25 GB database, inflating every
entry and throwing the bytes away takes **2.96 s** while writing the same bytes with no inflation takes **0.93 s**.
The restore is inflate-bound, and the "the 8 KB copy buffer is suspiciously small" hypothesis is the wrong one -
raising that buffer to 256 KB is worth 1%.

The read side, however, was worth a great deal, and for a reason no one had looked at: `ZipInputStream` fills its
inflater **512 bytes at a time**, a size its constructor hardcodes with no overload to change it, and the restore
handed it an unbuffered `FileInputStream`. That is one read syscall per 512 compressed bytes for the whole
archive. A 256 KB `BufferedInputStream` underneath it takes the same restore from 5.16 s to 3.96 s, and it is the
only speedup available to the two input sources that cannot be parallel at all.

### Parallel per entry

ZIP entries are independent and each becomes its own file, so they are inflated and written concurrently, largest
first (the duration of a set of very uneven independent tasks is decided by when the biggest one starts). This
needs random access to the archive - `ZipFile` rather than `ZipInputStream` - which turns out to be the larger
half of the win even at one thread, because `ZipFile`'s reader does not have the 512-byte fill.

`arcadedb.restore.threads` sizes the pool: `-1` (the default) is the available processors capped at 8, `0` selects
the sequential walk, `N` a pool of N. It is also settable per restore, on the command line (`-restoreThreads`) and
through the `Restore` API. Unlike a backup, which halves the cores because it runs alongside the workload it is
already throttling, a restore has no such neighbour - the database it is producing is not open yet - so the
automatic sizing takes whole cores.

**The archive format did not change**, and no part of this reads anything the backup did not already write. Old
archives, including single-threaded level-9 ones written before #6072, restore through the parallel path unchanged.

### The numbers

1.25 GB database, 8 threads, macOS/NVMe. Two shapes, because the shape is what decides what per-entry parallelism
can possibly buy:

| configuration | one dominant entry (1 type) | several comparable entries (8 types) |
|---|---|---|
| before #6086 | 5.16 s (248 MB/s) | 4.51 s (258 MB/s) |
| + buffered read, sequential | 3.96 s (1.30x) | 4.17 s (1.08x) |
| parallel, 1 thread | 2.60 s (1.99x) | 2.63 s (1.71x) |
| parallel, 8 threads | **2.51 s (2.05x)** | **0.49 s (9.15x)** |

A database made of one dominant file cannot be split - a ZIP entry is a single deflate stream and has to be
inflated serially - so there the whole win is the reader change and the threads add 4%. This is stated rather
than averaged away because it is the honest shape of the result: intra-entry parallelism, the mirror of what the
backup does, is not available without recording the backup's chunk boundaries in the archive, which would be the
format change this deliberately avoids. On a database spread over several types the restore goes to 0.49 s -
faster than the 0.68 s backup it undoes, which is where the issue wanted it.

Peak heap is bounded by construction and does not move: 11.0 MB in every configuration above, sequential and
parallel alike. One 256 KB copy buffer per pool thread, taken from a pool of exactly that many, plus the JDK
inflater's own buffer per entry in flight - under 3 MB of buffers at 8 threads.

### What stays sequential, and why it is not optional

Per-entry parallelism needs to open the archive at random, which two of the three input sources cannot support: an
archive read over `http(s)` is a one-shot stream, and an encrypted one is a single `CipherInputStream` that only
decrypts front to back. Both fall back to the sequential walk automatically, whatever the thread setting says - a
restore must never fail because of a performance setting - and both are covered by tests that assert the fallback
was taken, not merely that the restore worked.

One thing the parallel path does strictly better: it reads the central directory before it starts, so it validates
every entry name up front and refuses a hostile archive before writing a single file. The sequential walk only
learns a name when it reaches it, so it stops at the bad entry with the good ones already on disk - the
pre-existing behaviour, now pinned by a test so the difference is a recorded decision rather than an accident.

A failure *during* extraction carries one guarantee that a naive pool would not give: when the extractor throws,
no worker is still writing. Interrupting a thread does not come back out of a `FileOutputStream.write()`, so
shutting the pool down is not enough on its own - and the caller's very next move is usually to delete the
destination, which would then race the workers still filling it. The extractor therefore waits for its workers
before propagating the failure.

[#6086](https://github.com/ArcadeData/arcadedb/issues/6086)

## A graph-algorithm knob is bounded by the resource it actually spends (#6216)

Follow-up to #6065, which bounded the *allocation*-shaped numeric knobs of the OpenCypher `algo.*` package.
Review of that work flagged a second family it deliberately left alone: knobs that are unbounded `int`s and
multiply **CPU work** rather than one allocation - `algo.node2vec`'s `walksPerNode`, `walkLength`,
`windowSize` and `negSamples`, `algo.maxKCut`'s `restarts` and `maxIterations`, and
`algo.influenceMaximization`'s `simulations`. Each is read through `extractInt()`, so a value outside `int`
range is already rejected (#5924), but any value up to `Integer.MAX_VALUE` was accepted and drove the loop
count directly.

The reason the issue left this open is that a single hard cap per knob is a guess: "what is a sane maximum
iteration count?" depends on the graph, the hardware and how long the caller is willing to wait, and a
legitimate large-graph run may genuinely want a high value. So instead of one policy for all seven knobs,
each is bounded by the resource it actually spends.

**The parameter's own domain.** Below its minimum, a walk count, restart count or simulation count is not
"a smaller run", it is an answer the algorithm cannot produce - and every one of those values was absorbed
in silence or died in the allocator. `restarts: 0` never entered the restart loop and returned every node in
community 0 with a cut weight of `-1.0`, a wrong answer reported as a successful one. `maxIterations: 0`
skipped the local search entirely, so the "maximum" cut was whatever the random initialisation produced.
`iterations: 0` returned the untrained Xavier initialisation as if it were an embedding. `simulations: 0`
divided the accumulated spread by zero, so every candidate scored `NaN`, no comparison ever won, and the
procedure returned an empty result set as if the graph had no influential node. `walksPerNode: -1` reached
`new int[-4][...]` as a bare `NegativeArraySizeException` that named nothing. A new
`extractInt(value, name, minimum)` overload rejects all of them at the site the extraction already happens,
naming the parameter, its minimum and the value received. `negSamples` keeps a minimum of 0, because plain
Skip-gram without negative sampling is a legitimate configuration; the rest take 1.

**Heap.** `algo.node2vec` computed `final int totalWalks = n * walksPerNode` and then allocated
`new int[totalWalks][walkLen]`. That product wraps: 4 nodes at `walksPerNode: 1073741824` is exactly 2^32, so
`totalWalks` came out as **0**, the matrix was allocated with no rows at all, and the generator died on
`walks[wi++]` with an `ArrayIndexOutOfBoundsException`. Other magnitudes wrapped negative instead. Even
without the wrap the matrix is `walksPerNode x nodeCount` rows of `walkLength` ints, sized entirely by two
knobs with no graph-derived ceiling - the same allocation-DoS shape #6065 closed for the embedding
dimensions. `algo.randomWalk` had the one-dimensional version: `new int[steps + 1]` wraps to
`Integer.MIN_VALUE` at `steps = 2147483647`.

Both are now estimated in saturating `long` arithmetic and checked **before** anything is allocated, against
a new `arcadedb.cypher.algoMaxWorkingMemory` budget that auto-scales with the JVM max heap (one eighth of it,
never below 64MB) and can be raised or switched off (negative = no limit). This follows the house pattern
of `arcadedb.queryMaxHeapElementsAllowedPerOp` and `arcadedb.queryMaxRangeSize` rather than inventing a
per-knob constant: the error names the knobs that produced the estimate and the setting to raise, and
because it is a client's parameter that is out of range it is an `IllegalArgumentException`, so over HTTP it
is a 400 rather than a 500. A second guard, independent of the budget, refuses a walk count past
`Integer.MAX_VALUE` outright - no heap setting makes 2^32 array entries legal.

**Time.** For the remaining knobs there is no honest ceiling, so a large value is not forbidden, it is made
abortable. The hot loops of all three procedures now call a shared `WorkGuard`, which aborts on thread
interruption - matching what `ShortestPathStep` and `SQLFunctionShortestPath` already do on the query path -
and on the `arcadedb.command.timeout` deadline, which until now only the SQL `SELECT` planner honoured. The
guard reads the clock only when a timeout is actually configured, so the default (timeout disabled) costs one
flag test per iteration and no syscall. The interrupt flag is consumed rather than restored: the exception
aborts the whole call, and leaving the flag set would poison the next task to run on a pooled query thread.

Where the checkpoint sits matters as much as that it exists. A checkpoint between walks bounds nothing when a
single walk is the unbounded thing: with `windowSize` clamped to `walkLength`, every position scans the whole
walk as context, so one walk is O(`walkLength`²), and `walkLength` is bounded only by the memory budget - a
memory budget, not a work budget. `CALL algo.node2vec({walksPerNode: 1, walkLength: 5000000, windowSize:
5000000})` on a small graph fits comfortably in the default budget and then runs for hours between two
checkpoints, so neither the timeout nor an interrupt would fire in any useful time frame. Every loop whose
length a knob controls therefore checks *inside* itself, throttled to once every 1024 iterations so the
per-iteration cost stays one AND and one branch: the Skip-gram context loop, the walk-generation step loop,
and `algo.maxKCut`'s per-node scan within a local-search pass.

One correctness bug fell out of the same review. `algo.node2vec` computed its Skip-gram context window as
`Math.min(walkLen - 1, pos + window)`. For a large `windowSize` that addition wraps `int` and comes back
negative, leaving `winEnd` below `winStart`: the training loop then ran at position 0 and nowhere else, and
the procedure quietly returned embeddings that had barely been trained. A window wider than the walk already
spans the whole walk, so `windowSize` is now clamped to `walkLength` - a clamp with a genuinely correct
fallback, unlike an embedding dimension - and a huge window now produces exactly the same embeddings as a
window equal to the walk length. The addition itself is computed in `long` as well, so the wrap is closed as a
class and not just in the instance the clamp happens to cover: `walkLength` is bounded by a heap budget an
operator can raise, and past roughly 1.1 billion the clamped window would reach the same overflow.

Also fixed, in the same file as `simulations`: `algo.influenceMaximization`'s `k` saturates upwards on
purpose ("more seeds than nodes" reads as "as many as exist" and is clamped to the node count), but a
*negative* `k` passed the `Math.min(k, n)` clamp untouched and reached `new int[seedCount]` as another
nameless `NegativeArraySizeException`.

[#6216](https://github.com/ArcadeData/arcadedb/issues/6216)

## A peer address that identifies nobody says so, instead of waiting for an operation to refuse (#6267)

Five follow-ups from #6221 / #6226. Four are visible to an operator; the rest are test hygiene.

**A withheld peer-to-peer endpoint is now reported.** `getUnambiguousPeerHttpAddress` and its HTTPS twin refuse
an address two peers both resolve to by returning `null` (#6202), and every caller then decided for itself
whether to say anything - so the refusal was visible only where one happened to log it, and invisible everywhere
else. Neither existing warning covered it: the derive warnings fire whenever an address is derived **at all**,
which is also what a perfectly healthy homogeneous Kubernetes StatefulSet does, so they cannot distinguish
"deriving, and fine" from "deriving, and two peers just collapsed onto one address". There is now a one-time
WARNING per protocol - modelled on the `warnAmbiguousRouting` of #6183, but for the peer-to-peer endpoints rather
than the client routing tables - naming the peers that could not be told apart, the address they share, and the
`host:{raft:..,http:..}` field to declare in `arcadedb.ha.serverList`. HTTP and HTTPS have separate latches: a
cluster that declares distinct `http` ports and shares an `https` one must still hear about the second.

**Observable change in `GET /api/v1/cluster`.** Each peer entry now carries `httpAddress` and, only when it is
not the peer's alone, `httpAddressAmbiguous: true`. Before this, the status endpoint and the Studio HA panel
displayed a plausible address for every peer with nothing to say that it named none of them, and an operator
found out when a snapshot resync or a cluster verify refused to dial. A correctly declared cluster carries
neither field's flag, so nothing changes for one. Studio renders the flag as a warning line on the node's card.

**The presence matrix dialled the address nothing had checked.** `GET /api/v1/cluster?presence=true` asks every
peer which databases it holds and attributes the answer to that peer - the same unattended dial the verify
endpoint was making before #6221, with the same failure: on a cluster whose peers collapse onto one derived
address, every peer was queried on the leader's own endpoint and reported the leader's database list as its own,
so the matrix showed every database present on every node. It resolves through `PeerDialAddress` now, so a peer
it cannot identify is reported in `unreachable` - with the reason logged - rather than answered for by whoever
picked up.

`RaftClusterStatusExporter.exportClusterStatus()`, a second cluster-status JSON builder that nothing has called,
was removed rather than taught about any of this: the live endpoint is `GetClusterHandler`, and an unreachable
second view of one cluster is how two views drift apart.

**Test-only, in the HA lane.** `BaseRaftHATest.RESYNC_RETRY_TIMEOUT_MS` drops from 120 s to 30 s. #6226 added an
instrument rather than guessing, and it has now reported: across nine full `ha-integration-tests` runs (235 tests
each) not one wait exceeded the 10 s report threshold, and the slowest of the ten classes that use those helpers
took 53 s wall-clock for the whole class, cluster startup and teardown included. 30 s is what the rest of that
class already treats as long enough for a cluster to do anything it is going to do - `waitForReplicationIsCompleted`,
`waitAllReplicasAreConnected` and the leader-election wait all use it - so the one budget with no measurement
behind it was also the only one four times larger than its siblings. The report threshold drops to 5 s with it, to
keep the same resolution for the next cut. `DynamicMembershipTest` no longer leaves its own teardown holding a
peer it evicted to a replica's contract: the base class now waits for, and compares, exactly the servers
`getServerToCheck()` names, which turned a 30 s-per-evicted-server timeout and a `DatabaseAreNotIdentical` charged
to `endTest` into neither. Seven `await().until(() -> findLeaderIndex() >= 0)` wrappers that #6226 made redundant
are gone, and three copies of "only the servers still running" collapse into one helper.

[#6267](https://github.com/ArcadeData/arcadedb/issues/6267)
## The same iteration-knob guard, applied to the fourteen `algo.*` procedures #6216 left out of scope (#6264)

[#6216](https://github.com/ArcadeData/arcadedb/issues/6216) established what an iteration-shaped knob needs -
a domain minimum rejected by name, and a checkpoint inside the loop it drives - and gave both to the three
procedures its parent review had named. Fourteen more carried the identical defect, untouched:
`algo.pageRank`, `algo.personalizedPageRank`, `algo.articleRank`, `algo.eigenvector`, `algo.hits`,
`algo.katz`, `algo.louvain`, `algo.leiden`, `algo.labelPropagation`, `algo.slpa`, `algo.simRank`,
`algo.fastrp`, `algo.hashgnn` and `algo.graphsage`. Every one extracted its knob with a plain
`extractInt(n, "maxIterations")`, and none contained a single checkpoint.

So `CALL algo.pageRank({maxIterations: 0})` returned the *uniform initial rank vector* as though it were a
PageRank result, `algo.louvain` returned every node in its own community, and `algo.fastrp` the untouched
random projection, and `algo.graphsage` the untouched random-Gaussian initial features presented as trained
embeddings. The silent half is the more serious one: an un-iterated centrality is not obviously wrong to a
caller, unlike an exception. All fourteen now reject a value below 1 with a message naming the procedure, the
parameter and the value.

For time there is still no honest ceiling to pick, so a large value is not forbidden but made abortable. Each
iteration loop calls the shared `WorkGuard`, which observes a thread interrupt and the
`arcadedb.command.timeout` deadline; a per-node checkpoint inside each pass bounds the abort latency below a
whole sweep of the graph, throttled to once every 1024 nodes where a node's own work is small and unthrottled
where it is already O(n). Six of the fourteen have no convergence test at all - the CSR PageRank kernel,
`algo.simRank`, `algo.fastrp`, `algo.hashgnn`, `algo.graphsage` and `algo.slpa` - so the knob alone decided
when they stopped and nothing could end the run early.

Two of them hand the work to `GraphAlgorithms`, which lives below the query layer and knew nothing about
deadlines. Rather than couple the OLAP kernels to the query engine, `GraphAlgorithms.pageRank` and
`GraphAlgorithms.labelPropagation` gained an overload taking a `WorkCheckpoint`, a one-method interface in
`com.arcadedb.graph.olap` that the procedures satisfy with a method reference to their own guard. Existing
callers are unchanged and get a checkpoint that never aborts.

`algo.slpa` needed one thing more. Alone among the fourteen its `iterations` buys heap as well as time: every
node keeps a label-memory row of `iterations + 1` ints, so `{iterations: 1000000}` on a 10k-node graph asks
for 40 GB, and at `Integer.MAX_VALUE` the `iterations + 1` wrapped to `Integer.MIN_VALUE` and died as a bare
`NegativeArraySizeException` naming nothing. The footprint is now estimated in saturating `long` arithmetic
and checked against the same budget the walk buffers use, before the first row is allocated. (That budget is
renamed `arcadedb.cypher.algoMaxWorkingMemory` by #6263 below, which landed after this one and generalised it
from walk buffers to the whole working set of a call; the label memory is priced through it unchanged.)

[#6264](https://github.com/ArcadeData/arcadedb/issues/6264)

## A placeholder whose content had to spill into chunks is no longer returned twice by a scan (#6196)

`SELECT FROM Doc` could return the same record twice, under two different RIDs, and `count(@rid)` counted it
twice with it. One record shape reached it: a **placeholder** whose CONTENT record was too large for any page to
hold whole.

A record that outgrows its own page normally becomes a chunk chain in place. When its slot is too small even for
the 14 bytes a chunk header needs - since #6149 the only remaining case, and it takes a page with a free tail of
exactly zero - the slot becomes a 9-byte POINTER instead and the content moves to a record of its own on another
page. Such a content record is not a document; it is reachable only through the pointer, and every reader that
walks a page knows to skip it because its slot carries its size **negated**. That is the entire mechanism, and it
is the one thing a content record bigger than a page cannot have: it has no size in its slot at all, because it is
a chain. `writeMultiPageRecord` stamped the plain `FIRST_CHUNK` marker of an ordinary multi-page record on its
head, and at that moment the information that the slot belonged to somebody else was gone - there was nowhere
else it was written down.

So the bytes were handed out twice: once through the pointer, under the RID the application knows, and once as a
document in their own right under the head chunk's RID. `check()` had the mirror of the same confusion, counting
the record under `totalMultiPageRecords` and never under `totalSurrogateRecords`.

The head chunk of a content record now carries a marker of its own, `FIRST_CHUNK_PLACEHOLDER_CONTENT` (-4), in the
one value the marker namespace still had free between `NEXT_CHUNK` (-3) and the negated sizes (< -5). It costs
nothing in the stored format - one zigzag byte, exactly like the markers either side of it - and it restores the
rule the negated size marker always expressed: a scan, a `count()` and an existence check hand out records, and
this is not one, while everything that walks or rewrites the CHAIN treats the two heads identically.

**Databases written before this** still hold the ambiguous shape, and no reader can tell it from an ordinary
multi-page record - only the pointer that leads to it can. Rather than have every reader tolerate the ambiguity
for ever, `CHECK DATABASE` now follows each placeholder pointer, reports a content record still stored the old way as an error naming its RID, and
`CHECK DATABASE FIX` repairs it by rewriting that one marker. It is a repair and never a deletion: not a byte of
the record, its chunk header or its chain is touched.

**Run that FIX before writing through a full scan of such a database.** Until it has run, the content record is
still handed out under a RID of its own, and that RID looks like any other: an application that scans a type and
updates or deletes what comes back can reach the content record directly, where nothing can tell it apart from a
record and the write lands on content the placeholder pointer still references. This is the behaviour every
release before this one already had - the marker is what closes it, and `CHECK DATABASE FIX` is what applies the
marker to data written earlier. A read-only scan of an unrepaired database merely returns the record twice; a
read-modify-write over one is worth the FIX first.

Following the pointers costs `CHECK DATABASE` one page fetch per placeholder POINTER - every one of them, not only
the chunked case it is looking for, because the marker on the other end is the only thing that tells the two apart.
A pre-#6149 bucket dense with placeholders therefore pays one extra fetch each, of pages the same pass reads
anyway, on an operation that already reads every page of the bucket and walks the full chunk chain of every
multi-page record.

One reporting detail to expect on an unrepaired database: only the TOTALS `CHECK DATABASE` returns are
reconciled, because a content record can sit on a page the walk has already left behind and reconciling as it
went would make the answer depend on the order the allocator happened to place the pages in. The per-page
tallies printed by a verbose run are not, so for such a record a page's own `multiPageRecords` count disagrees
with the top-level `totalSurrogateRecords` it was moved to. That is deliberate: a physical-layout log describes
the page as the walk found it. Both agree again once the FIX has run.

[#6196](https://github.com/ArcadeData/arcadedb/issues/6196)

## The whole working set of a graph-algorithm call is budgeted, not just its walk buffers (#6263)

Follow-up to #6216, which introduced a heap budget for the random-walk buffers of `algo.node2vec` and
`algo.randomWalk`, and to #6065, which capped every embedding-dimension knob at 4096. Between them they left
the *largest* allocation of these procedures outside every budget: the matrices themselves.

A dimension cap bounds one embedding **row** at 32 KB and says nothing about a `nodeCount x dimension`
**matrix**. At `algo.node2vec`'s default `embeddingDimension: 128` the two Skip-gram matrices cost about 2080
bytes per node - 208 MB at 100 000 nodes, 2.1 GB at a million, 21 GB at ten million - which is the same order
as the walk matrix on the same call, at ~3560 bytes per node, that #6216 already refused up front. One
allocation was budgeted and the other, sitting beside it in the same method, was not. `algo.fastrp` made the
gap plainest: it has no walk buffer at all, so no budget of any kind applied to it, and its two
`nodeCount x dimensions` matrices had no ceiling whatsoever. The failure mode was an `OutOfMemoryError` rather
than the client error naming a parameter that #6065 and #6216 both exist to produce - and an
`OutOfMemoryError` on a shared server takes down work that had nothing to do with the query that caused it.

`arcadedb.cypher.algoMaxWalkMemory` is therefore renamed **`arcadedb.cypher.algoMaxWorkingMemory`**. The key
was introduced in this same unreleased version, so no deprecated alias is carried: the concept it implements -
estimate the footprint in saturating `long` arithmetic, reject as a client error before allocating, auto-scale
the default with the JVM heap - was never walk-specific, only its name was. Everything else about it is
unchanged: default `max(64MB, maxHeap/8)`, negative means no limit, and the error is an
`IllegalArgumentException`, so over HTTP it is a 400 rather than a 500.

Reservations now **accumulate over the call** rather than being checked one allocation at a time. That is what
the question "how much heap may this call take?" actually asks, and a single call routinely holds several of
these at once: `algo.node2vec` keeps its walk matrix alive while it trains over two embedding matrices, so
pricing each separately would let a call exceed the budget by however many components it happens to have. The
error names the component that broke the budget, the counts that sized it, and what the call had already
reserved:

```
algo.node2vec(): the embedding matrices would need 8448 bytes (2 matrices of 4 nodes x embeddingDimension=128),
on top of the 14240 bytes this call already reserved, more than the 20000 bytes allowed.
Set arcadedb.cypher.algoMaxWorkingMemory to raise the limit
```

Priced against the budget, all before anything is allocated:

| procedure | working set |
|---|---|
| `algo.node2vec` | walk matrix + 2 x `nodeCount x embeddingDimension` |
| `algo.fastrp` | 2 x `nodeCount x dimensions` |
| `algo.hashgnn` | 2 x `nodeCount x 4 x embeddingDimension` feature matrices + 1 x `nodeCount x embeddingDimension` |
| `algo.graphsage` | 2 x `nodeCount x embeddingDimension` + the layer's `embeddingDimension x 2 x initDim` projection |
| `algo.apsp` | `nodeCount x nodeCount` distance matrix |
| `algo.simRank` | 2 x `nodeCount x nodeCount` similarity matrices |
| `algo.maxFlow` | `nodeCount x nodeCount` capacity + residual matrices |
| `algo.kShortestPaths` | `nodeCount x nodeCount` weight matrix + removed-edge mask |
| `algo.steinerTree` | `terminals x nodeCount` Dijkstra tables + four arrays of one entry per terminal pair |
| `algo.randomWalk` | walk buffer (unchanged from #6216) |
| `algo.slpa` | `nodeCount x (iterations + 1)` label memory (from #6264, repriced through the same budget) |

The last four are the reason the rename is worth doing rather than adding a second walk-shaped key. Their
matrices are sized by the graph alone, with no knob involved anywhere: `algo.simRank(a, b)` answers a question
about exactly two nodes and allocates two full `nodeCount x nodeCount` matrices to do it, because the
similarity of one pair is defined recursively over every pair - 1.6 GB at 10 000 nodes. `algo.apsp` already
documented itself as suitable "up to a few thousand vertices", which was advice rather than a bound. These
algorithms are cubic or worse in time, so the memory bound bites at roughly the scale where the runtime is
already impractical; what changes is that the refusal now names the node count and the setting instead of
arriving as an `OutOfMemoryError` several minutes in.

`algo.hashgnn` turned up one thing worth stating separately: its **feature** matrices, not its embedding
matrix, are the larger pair. They are four times as wide as the embedding, so even stored as `boolean` they
cost half a byte per dimension per node against the embedding's eight - which is why they are priced by name
rather than folded into an "embedding matrices" figure that would understate the call by a factor of two.

`algo.steinerTree` carried the same defect in both of its forms, and is the only knob in the package a caller
supplies as *data* rather than as a number: `terminalNodes` is a list, so nothing validates its length - not
even the node count, since repeating the same vertex is accepted. Its length sizes a `terminals x nodeCount`
pair of Dijkstra tables, and then `t * (t - 1) / 2` terminal pairs across four parallel arrays. That
expression was evaluated in `int`, and the division by 2 happens *after* the product, so it wrapped at 46342
terminals - the result fitting an `int` never saved it - and `new int[pairCount]` died as a bare
`NegativeArraySizeException: -1073716337`, naming nothing. The count is now computed in `long`, priced (the
real figure at that length is 1073767311 pairs, about 43 GB), and refused past `Integer.MAX_VALUE` entries
whatever the heap setting says. The pair arrays include an `Integer[]` index array sorted through a
`Comparator`, which costs 24 bytes an entry against the 4 of the `int` it carries; it is priced at what it
actually costs rather than at what a primitive array would.

[#6263](https://github.com/ArcadeData/arcadedb/issues/6263)

## An index build waits for the async batch, not just for its tasks (#6281)

`CREATE INDEX` builds by scanning the buckets, so everything the asynchronous executor was asked to write has
to be committed before the scan starts. The guard that was supposed to ensure that read the wrong predicate:

```java
while (database.isAsyncProcessing())
  database.async().waitCompletion();
```

`isAsyncProcessing()` answers about *tasks* - queued or executing. An async worker opens a transaction when it
starts and keeps it open across up to `arcadedb.asyncTxBatchSize` (10240) tasks, so a worker whose queue has
drained is still holding every record of the current batch **uncommitted**. On the runs where that predicate
happened to answer `false` - which is a matter of thread scheduling, not of load - the guard was skipped
entirely: the index was built by scanning a bucket that did not contain those records, and they committed
afterwards with no entry ever added for them. The result was an index that is empty, readable, and reported
healthy by `CHECK DATABASE`, while `SELECT ... WHERE id = ?` silently answered nothing for records that were
there. Reproduced with 200 asynchronously inserted records: 0 index entries, 0 rows returned.

The barrier is now unconditional. `waitCompletion()` is the only operation that closes the open batch - it
enqueues a marker *behind* everything already submitted on every worker, and that marker commits - so there is
no cheap predicate to test first, and testing one is what let the barrier be skipped. `isAsyncProcessing()` is
deliberately left answering about tasks: broadening it to "a transaction is open" would make it permanently
true for as long as the workers live. The same barrier now precedes `REBUILD INDEX`, whose scan sees the same
partial view. The rebuild proper does not lose index entries there - those records apply their own staged
entries when they commit - so what it gains is the misplaced-record detection of #832 and the reported totals
being about all of the data. `REBUILD INDEX ... WITH statsOnly = true` was worse off and is the reason the
barrier sits ahead of that branch rather than after it: it recomputes the BM25 corpus counters by scanning the
type and then *overwrites* the counters with what the scan found, so run against an open batch it wrote "0
documents" over counters the records had already bumped. A type holding 200 documents was left scoring every
BM25 query against a corpus of zero, and unlike the index-entry case that did not heal itself.

**One refusal is new.** `CREATE INDEX`, a manual index create and `REBUILD INDEX` now raise a
`NeedRetryException` when they run on one of the async executor's own worker threads - a command dispatched
over HTTP with `awaitResponse=false`, for instance. The barrier cannot be satisfied from inside the thing it
waits for: it enqueues a marker on every worker including the caller's own, and the only consumer of a
worker's queue is that worker, so it would park for ever. `REBUILD INDEX` has refused this since #2097 and
keeps its own message; what is new is that `CREATE INDEX` and the manual index builder refuse it too, where
before they hung. Run the command synchronously instead.

`ACIDTransactionTest.indexCreationWhileAsyncMustFail`, which existed to cover exactly this, had been vacuous
for years: it expected a `NeedRetryException` from a creation that has drained the executor rather than
refusing since long before this release, so its `catch` never fired and its only assertion was the record
count - which an empty index satisfies just as well as a complete one. It now asserts the index.

### The page-flush queue is bounded per database, not per JVM

**Behaviour change worth checking if you run many databases in one JVM with a non-default
`arcadedb.pageFlushQueue`.** That setting was a single JVM-wide queue capacity. #6259 moved the wait for room
in it out of the page-manager lock, so a full queue stopped freezing every committer in the process - but the
coupling itself survived: one database's write burst against a slow volume still consumed the admission budget
of every other database, so a committer on an idle database on an idle volume waited for a disk it has nothing
to do with.

The budget is now per database, held as one count per database that covers a slot from the moment a committer
reserves it to the moment the flush thread polls the batch it became. The shared queue keeps global FIFO order
and carries no capacity of its own - one queue rather than one per database is what avoids having to invent a
fairness policy for a flush thread choosing between queues on every poll.

The trade-off, stated because it changes the shape of a memory ceiling: worst-case flush-pipeline occupancy is
now `pageFlushQueue x open databases` batches rather than a flat `pageFlushQueue`. That is inherent to bounding
per database, and the number being multiplied was never a byte bound to begin with - one batch is one
transaction's dirty pages - but a deployment that sized `pageFlushQueue` against total heap should re-check it.
The bound that *is* expressed in bytes remains `arcadedb.flushSuspendMaxDeferredRAM`. A `pageFlushQueue` of 0
or less, which used to fail at startup on `ArrayBlockingQueue`'s constructor, is now raised to 1: with
admission as the only bound, a budget of 0 would refuse every publication for ever.

One consequence worth knowing about if you run many databases that saturate their budgets at the same time:
the committers waiting for room all park on a single shared monitor, so every batch the flush thread polls
wakes all of them and each re-checks its own database's count before parking again. That is a deliberate
trade - a monitor per database would move a map lookup onto the poll path, which runs per batch, to save work
on the wait path, which only runs for a database already at its bound - and it is the same shape as the
existing deferred-RAM cap. It is a different failure mode from the old single-queue design, though, so it is
named here rather than left to be discovered.

**One published metric changed scale with it.** `pageFlushQueueLength` used to top out at `pageFlushQueue`,
because the queue's capacity was that setting - so `pageFlushQueueLength >= pageFlushQueue` was a natural
"the pipeline is full" alert. It is now a sum across every open database and can run to
`pageFlushQueue x open databases`, which makes that comparison meaningless. The signal it used to carry is
published alongside it as `pageFlushQueueMaxPerDatabase` (and as `flushQueueMaxPerDb` in the profiler dump):
the busiest single database's share, which is exactly what `pageFlushQueue` bounds, so that is the one to
alert on.

[#6281](https://github.com/ArcadeData/arcadedb/issues/6281)

## One `TIMEOUT` clause, honoured inside the scan, on every statement that should accept one (#6304)

Five findings turned up while fixing #6266 and are answered together here: they are unrelated to each other
beyond having been found in the same neighbourhood.

### A count push-down no longer drops a label the pattern wrote down

`MATCH (p1)-[:KNOWS]->(p2), (p1:Person)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2)` counted the comments
whose author is *not* a Person as well, but only when a Graph Analytical View happened to cover the three
edge types. The pair-join operator's CSR build paths read the arm's endpoint straight into the probe lookup
and never applied the endpoint's bucket filter - the table of bucket ids that filter needs was built by the
branch that owned the *other* arm - while the OLTP fallback applied it. Same query, two answers, decided by
whether a view exists.

Two more drops of the same family are closed with it. `buildWithViews`, the branch named in the issue, was
handed the second arm's filter and ignored it. And the detector read node labels off the build pattern only:
the two comma-separated patterns share their endpoint variables, so `(p1:Person)-[:KNOWS]->(p2)` constrains
exactly the variable the other pattern's arm ends on, and that label was dropped on *both* execution paths -
a wrong answer with or without a view.

Where the two patterns label the same variable differently the filter is an intersection, which the operator
carries no room for, so the push-down now declines and the ordinary pipeline answers instead. It declines the
same way for `(a:A:B)` and `(a:A|B)`, which it used to read as `(a:A)` - a filter that is neither.

### `TIMEOUT n RETURN` stops when it says it will, and returns what it has

The clause promises the rows produced so far rather than an exception. It delivered neither half. Its
deadline was deliberately left out of the in-loop guards, because a guard can only stop by throwing - so a
filter that rejects every record still scanned to the end inside one `hasNext()`, which is precisely the
granularity hole #6266 set out to close. And when rows *did* pass the filter, the step marked itself timed
out and then went on returning every remaining row, so nothing was ever truncated.

A guard that reaches a deadline pinned by a `RETURN` clause now raises a distinct
`PartialResultTimeoutException`, which the step owning the clause converts into the end of its result set.
Every other bound keeps raising a plain `TimeoutException`; the distinction is what stops a genuine
`arcadedb.command.timeout` abort from being swallowed into a silently truncated answer. An outer `RETURN`
clause propagates the same meaning into nested plans, so a subquery yields rather than raising too.

**Behaviour change.** `SELECT ... TIMEOUT n RETURN` used to return the complete result set. It now returns a
prefix of it. That is what the clause has always been documented to do, and `EXCEPTION` - the default when
neither token is written - is unaffected.

### A `TIMEOUT` clause no longer outlives the script line that wrote it

Found while reviewing the above, and a regression #6291 shipped: every line of a SQL script is planned against
the *same* command context, so the instant a `TIMEOUT` clause pins landed exactly where the following lines
read it - and those have no timeout step of their own to catch anything. This failed on its second line:

```sql
SELECT FROM Node WHERE v = 1 TIMEOUT 50;
SELECT FROM SomethingSlower;
```

with `the command exceeded the TIMEOUT clause of 50ms`, a bound belonging to a statement that had already
finished. A clause is now scoped to its own line. What the line restores is the command's own
`arcadedb.command.timeout` instant, so that bound stays in force across the whole script - the script is one
command.

### `SELECT ... TIMEOUT n` and `UPDATE ... TIMEOUT n` now mean the same thing

They did not. `UPDATE` measured wall clock from the first pull; `SELECT` charged only the time spent inside
the pipeline, so a client streaming a large result slowly was not billed for its own pauses while the
identical number on an `UPDATE` was. Same syntax, two bounds, and nothing said which one a statement got.

Both are wall clock now, which is what the word means everywhere else in the engine:
`arcadedb.command.timeout`, the ceiling over every statement, has been wall clock since #6266, so the
accumulating variant was the only bound in the engine that was not. The two steps are one.

**Behaviour change.** A consumer that pauses between fetches of a `SELECT ... TIMEOUT n` is now charged for
the pause. A client that reads a large result set slowly and relies on a clause to bound only server-side
work should raise the value or drop the clause.

`TIMEOUT 0` also means one thing now. It disabled the clause on `UPDATE` and expired it on the spot on
`SELECT`; it disables it everywhere, which is what `0` means for `arcadedb.command.timeout` and
`arcadedb.command.regexTimeout` and is the reading that cannot turn a working statement into a failing one.

### SQL `MATCH` and `TRAVERSE` accept `TIMEOUT`

`SELECT ... TIMEOUT 100` parsed and `MATCH {...} RETURN ... TIMEOUT 100` was a syntax error, so bounding one
expensive `MATCH` meant changing a database-wide setting. Both statements now take the clause, with the same
`EXCEPTION | RETURN` strategies and the same wall-clock meaning as everywhere else. The profiled read timeout
a `MatchStatement` was already setting on itself - and which nothing then read - takes effect with it.

### One regex budget per command, not one per feature and per scan worker

`arcadedb.command.regexTimeout` bounds a regex against catastrophic backtracking. The deadline lived in the
command context's opaque value cache under a key each call site chose for itself, which gave a query as many
budgets as it used regex features - and, because a parallel bucket-scan worker gets a `copy()` of the context
and that copy does not carry the cache, one more budget per worker. A type scanned across N buckets was
bounded by `N x regexTimeout`.

The deadline is now a field of the context, resolved once and pinned into the copy exactly as the command
deadline is, so it is one budget for the whole command however the command is decomposed. Nothing to
reconfigure; with the setting disabled the resolution still reads no clock at all.

[#6304](https://github.com/ArcadeData/arcadedb/issues/6304)
## A placeholder's content record can stop being a chunk chain, like every other record (#6286)

#6178 gave a record that shrinks back inside the region its own slot owns a way out of being a chunk chain:
the head chunk is rewritten as a plain record and the chain behind it is freed. One record shape was left
out - the CONTENT record of a placeholder - and the reason was mechanical rather than principled. Such a
record is recognised by the NEGATIVE size marker its slot carries, which is what tells a scan the slot holds
somebody else's content rather than a document of its own; the collapse could only write a plain POSITIVE
size, so collapsing one would have been #6196 all over again on the one record shape that had escaped it.

The collapse now writes the sign the shape calls for. A content record that shrinks back inside its region
becomes the ordinary negated-size record it would have been had it never outgrown its page, instead of
keeping 13 bytes of chunk header for ever and routing every read and write through the chunk path. It is the
same transition the head chunk of a record of its own already made, with its own slot kind
(`SLOT_KIND_CHUNK_COLLAPSED_TO_PLACEHOLDER_CONTENT`) so the commit-time disjoint-slot merge replays it only
onto a committed slot that still carries the marker the write started from - the same bytes behind the other
marker being a different record is the whole reason the kinds exist.

One case stays out of the merge rather than out of the collapse: a database written before #6196 holds a
content head still wearing the ambiguous `FIRST_CHUNK`, which no marker can tell from a record's own. Such a
head is collapsed too - reached through its pointer, the flag says what it is, and the negated marker the
collapse writes ends the ambiguity for good - but the page is poisoned instead of tracked, because neither
collapse kind names that starting marker. The transaction keeps its write and gives up only the merge, on a
shape no current build produces.

[#6286](https://github.com/ArcadeData/arcadedb/issues/6286)

## `CHECK DATABASE` follows a placeholder pointer, and reclaims the chunks nothing points at (#6292, #6293, #6294)

Three things the bucket checker could not say about a bucket, all found while building the #6196 fixtures.

### A dangling placeholder pointer made `count(*)` and a scan disagree for good (#6292)

`check()` classified a placeholder POINTER slot, counted it, and moved on - it never followed the pointer to
ask whether anything was on the other end. So a pointer whose CONTENT record was gone stayed on its page for
ever, and the two counts of one type disagreed permanently while the checker called the database clean:

```
select count(@rid) as c from P   -->  7      (a scan: the pointer resolves to nothing and is skipped)
select count(*)    as c from P   -->  8      (the cached counter: a pointer slot is a record)
check database                   -->  totalErrors: 0
```

The commonest way to produce one was `CHECK DATABASE FIX` itself: the broken-chain branch force-deletes a
content record whose chain cannot be walked, and freed the content's slot only. Corruption and an
interrupted repair reach the same state.

Every pointer is now followed, and its target classified: a content record of either shape (a negated size,
or the `FIRST_CHUNK_PLACEHOLDER_CONTENT` head #6196 added), the pre-#6196 ambiguous head, or nothing a
pointer may lead to. The last is reported with both RIDs and, with `FIX`, the pointer is removed -
reconciling `count(*)` with the scan. It costs nothing new: #6196 already paid one page fetch per pointer to
read that marker, and only the branch that asks this question was missing. Two new report fields,
`danglingPlaceholderPointers` and `danglingPlaceholderPointersFixed`, count them.

The repair frees the pointer's SLOT and nothing else, where the ordinary delete would follow the pointer
first. That distinction is load-bearing: a pointer can be dangling because it was CORRUPTED, in which case it
now names whatever record happens to live at that position, and deleting that record is exactly the damage
the pass exists to prevent.

A pointer left dangling by a deletion the same run made is removed with it and not booked as a second error.
The record had one defect, `FIX` removed it, and the pointer is the rest of that removal.

### The FIX run described the database it found, not the one it left (#6293)

A record deleted during the run was counted in `totalDeletedRecords` AND under the category tally it had held
before, because the slot was classified first and repaired after:

```json
"totalDeletedRecords": 1, "deletedRecordsAfterFix": ["#1:5"],
"totalMultiPageRecords": 1, "totalActiveRecords": 6, "totalAllocatedRecords": 11
```

A re-run reported `totalMultiPageRecords: 0` and `totalAllocatedRecords: 10`. One report described the same
record twice, on exactly the numbers an operator diffs across runs to decide whether a `FIX` did anything.

The obvious repair - decrement the category counter next to each `++totalDeletedRecords` - would have had
four sites each remember which of five categories it had incremented, which is the same duplication one level
down. The classification is a VALUE now: the slot walk decides which category the slot fell into, the
counting happens once at the end of the per-slot block, and a slot the repair removed simply carries the
deleted category instead. The per-page verbose tallies come from the same decision, so they no longer
disagree with the totals about whether a multi-page record is also an active one.

### Orphaned continuation chunks are reclaimed, and three comments stop promising something nothing did (#6294)

Force-deleting a record with a broken chunk chain frees the HEAD slot only. Three places in the tree told the
reader the rest would be "reclaimed by compaction or a database check". Neither ever did: `check()` counted a
`NEXT_CHUNK` slot under `totalChunks` and moved on, and `compressPage` re-flows a page's LIVE slots - an
orphaned chunk still has one, so it was re-flowed along with everything else rather than dropped. The leak
was bounded per incident and permanent, surviving every repair short of an export and reimport.

Reachability cannot be answered from a chunk's own slot - the same asymmetry that made #6196's content
records need a marker - so it is answered from the other end. The chain walk `check()` already makes for
every head now marks the chunks that head reaches, and a head the run repaired away marks nothing, which
frees its chunks at the source as well. After the pass, every `NEXT_CHUNK` slot nothing marked is reported
under `orphanedChunks` and, with `FIX`, freed and counted under `orphanedChunksReclaimed` - the shape
`orphanedEdgeSegments` / `orphanedEdgeSegmentsReclaimed` already has, down to failing closed: an unmarked
LIVE chunk deleted as an orphan is destroyed data, so ANY gap in the marking (a page the walk could not read,
a chain walk stopped by an I/O fault, a slot that could not be classified) disables the sweep entirely rather
than shrinking it.

**An orphan is a COUNT, not an error and not a warning.** Nothing is corrupted by one: no record is wrong, no
query is affected, no two counts disagree - the bucket is carrying dead space. That matches how
`orphanedEdgeSegments` and `orphanedExternalRecords` are already reported, and the scale is what makes it
matter rather than being a matter of taste. The first measurement of a real workload found **243821 orphaned
chunks in a bucket of 1.5 million** (`CRUDTest.multiUpdatesOverlap`, 131072 records put through thirteen
rounds of updates): a leak that has always been there, that nothing collected, and that one warning apiece
would have reported as a quarter of a million strings.

**The reclaim is bounded by memory, not by count.** The enclosing transaction holds a copy of every page it
modifies, so a backlog spread thinly across pages costs pages x pageSize whatever the number of chunks is. A
run frees what fits a 32 MB page budget, reports the rest through the gap between `orphanedChunks` and
`orphanedChunksReclaimed`, and says so in the log; the next `FIX` continues. A bulk repair that converges is
worth more than one that ends as the `OutOfMemoryError` of #4653.

[#6292](https://github.com/ArcadeData/arcadedb/issues/6292)
[#6293](https://github.com/ArcadeData/arcadedb/issues/6293)
[#6294](https://github.com/ArcadeData/arcadedb/issues/6294)
## An edge weight belongs to the edge it was read from, whichever backing answered (#6301)

`algo.steinerTree` paired weights with neighbours **by iteration position**. The adjacency list was built with
the `relTypes` filter applied and in the backing store's order; the weight array beside it was filled
positionally from an *unfiltered* `getEdges(BOTH)` walk in OLTP order. Nothing reconciled the two, so
`adjW[i][j]` was "the weight of the j-th edge I happened to see", not "the weight of the edge to `adj[i][j]`".
That array is what Dijkstra runs on in step 1, so the **tree itself** moved, not only the `weight` column.

Two shapes made it visible. An edge type the caller excluded still lent its weight to an included edge: three
vertices on a `ROAD` path of weight 1 per hop plus one `NOISE` edge of weight 999 hanging off the first
returned `weight: 999.0` and `totalWeight: 1000.0` for a tree that costs 2.0. And the same query answered
differently depending on whether a Graph Analytical View happened to exist, because the CSR neighbour order
need not match the OLTP one - a view is meant to be a transparent accelerator, and here its mere presence made
the procedure prefer a 51.0 tree over the 2.0 one.

The fix removes the second traversal rather than repairing it. `GraphData.weightedAdjacency(direction,
weightProperty, relTypes)` produces the neighbour list and its weights from **one** walk of the same edges -
from the view's columnar edge properties when it has them, from the edge records otherwise - so the pairing is
correct by construction and the two backings return the same multiset of `(neighbour, weight)` pairs.

Two more procedures were reading weights through the same broken helper and are fixed with it:

- **`algo.apsp`** with no `relTypes` filter on a view that materialises edge properties produced an *empty*
  weight row against a non-empty neighbour row, i.e. an `ArrayIndexOutOfBoundsException` for a query that
  worked perfectly without the view. With several edge types it mismatched instead of failing, because the
  adjacency is sorted across types and the weights were concatenated per type.
- **`algo.bellmanford`** fell back to a **unit weight for every edge** whenever the graph was CSR-backed and
  the view had no column for the requested property - silently ignoring `weightProperty` altogether, and only
  when a view existed. It also now builds its edge list in parallel primitive arrays rather than a
  `List<int[]>` and a `List<Double>`, which the O(V x E) relaxation loop reads up to `V - 1` times.

**Two provider-contract changes come with it.** `GraphTraversalProvider.hasEdgeProperties()` now means "edge
property columns are materialised **and** the positional mapping `getEdgeProperty` relies on is exact", and
`GraphAnalyticalView` answers `false` while a delta overlay is active. The columns are aligned with the base
CSR's forward edge slots, while `getNeighborIds` serves the overlay's view of the node - deletions dropped,
additions merged in, the whole list re-sorted - so the n-th neighbour of that list is not the n-th edge of the
column store. Callers (`algo.apsp`, `algo.bellmanford`, `algo.steinerTree`, `SQLFunctionAstar`,
`SQLFunctionBellmanFord`, `algo.dijkstra.singleSource`) now read the edge records in that state, which is
slower and exact, instead of a weight belonging to another edge. This is the same "say unknown rather than
guess" convention `countEdgesBetween` and `getMeanEdgesPerConnectedPair` already use.

The second is `edgeWeightsOf(nodeId, direction, propertyName, defaultWeight, edgeTypes...)`, which returns one
node's neighbours and their weights together and is now **the only supported way to read an edge property
positionally**. Pairing `getNeighborIds` with `getEdgeProperty` by hand is wrong in two ways that produce a wrong
answer rather than an error: a multi-type neighbour list is merged and sorted across types while the property
columns are per type, and `DIRECTION.BOTH` has no column at all - a provider resolves `OUT` and `IN` only, so a
`BOTH` lookup returns `null` for every edge. `bellmanFord()`'s direction argument *defaults* to `BOTH`, so the
plain three-argument `bellmanFord(src, dst, 'weight')` had been unit-weighting the entire graph the moment a
view existed; `astar()` with `direction: 'BOTH'` priced every edge at 0.0, making all paths tie. Its companion
`servesEdgeProperty(propertyName, edgeTypes...)` answers the gating question. `hasEdgeProperties()` answers only "are there edge property columns at all", so a view built
with `.withEdgeProperties("distance")` answered yes to a call asking for `cost` - and since a missing column and
an edge with no value both come back as `null`, the caller could not tell them apart and treated the entire graph
as unweighted. That is the same wrong answer as a misaligned weight, reached from the other direction, and it
silently inverted results: on a graph where `X-Y-Z` costs 2.0 and the direct `X-Z` hop costs 50.0, unit weights
make the 50.0 hop win. `algo.apsp`, `algo.bellmanford`, `algo.steinerTree`, `algo.dijkstra.singleSource`,
`SQLFunctionAstar` and `SQLFunctionBellmanFord` now all gate on it and fall back to the edge records otherwise.
`SQLFunctionBellmanFord` additionally passed `null` as the edge type to `getEdgeProperty`, which can never
resolve a column store, so its CSR path had been using a unit weight for every edge unconditionally.

[#6301](https://github.com/ArcadeData/arcadedb/issues/6301)

## The phase a graph-algorithm call spends its time in is now abortable too (#6295, #6302)

#6216 and #6264 gave every iteration-shaped `algo.*` knob a checkpoint inside the loop it drives, on the
principle that for time there is no honest ceiling to pick, so a long run should be **abortable** rather than
forbidden. Both picked their procedures by asking "does it have an iteration knob?". Two blind spots follow
from that question, and this release closes both.

### The dominant phase is not always the loop the knob drives (#6295)

`arcadedb.command.timeout` set to 1000 ms, on a 2000-node graph:
`CALL algo.hashgnn({embeddingDimension: 4096, iterations: 1})` **ran for 112,988 ms and returned a result** -
the deadline overshot by 113x and never observed. `algo.hashgnn`'s MinHash reduction runs *after* the
`iterations` loop and costs `4 x embeddingDimension²` operations per node, and that bounded per-node figure is
then multiplied by an unbounded node count. The lens missed it because `embeddingDimension` *has* a ceiling
(#6065 capped it at 4096), which made it look handled.

The question that finds this shape is **"which loop over the node count has no checkpoint"**, not "which knob
has no cap". Asking it across the package also found the pre-loop initialisation of `algo.hashgnn`,
`algo.fastrp` and `algo.graphsage`, and `algo.slpa`'s post-processing and its O(degree²) `mostFrequent`. All
now carry a checkpoint.

### A guard belongs wherever the work is unbounded, not only where a knob multiplies it (#6302)

The four densest procedures in the package had no guard at all, and `algo.apsp` makes the case on its own: the
#6263 working-memory budget caps its distance matrix at `arcadedb.cypher.algoMaxWorkingMemory`, whose 64 MB
floor **admits n ≈ 2890**, and 2890³ is ~2.4e10 iterations of the Floyd-Warshall triple loop. That is minutes
of CPU on one query thread during which `Thread.interrupt()`, `arcadedb.command.timeout` and the client
cancelling all did nothing. The budget and the timeout are meant to be complementary halves of one guarantee,
and the memory half was waving through a run the time half could not stop.

A sweep over all 68 procedures asking "can one call of this run for minutes on an accepted input?" settled the
rest in one pass. Every procedure whose dominant loop is superlinear in the graph is now abortable:
`algo.apsp`, `algo.kShortestPaths`, `algo.steinerTree`, `algo.mst`, `algo.bellmanford`, `algo.betweenness`,
`algo.closeness`, `algo.harmonic`, `algo.eccentricity`, `algo.maxFlow`, `algo.msa`, `algo.knn`,
`algo.hierarchicalClustering`, `algo.clique`, `algo.allSimplePaths`, `algo.kTruss`, `algo.triangleCount`,
`algo.localClusteringCoefficient`, `algo.densestSubgraph`, `algo.bipartiteMatching`, `algo.voteRank` and
`algo.richClub`.

Procedures that make a single O(V + E) pass are deliberately left alone: one pass costs what loading the graph
and emitting the rows already cost, so a checkpoint inside it would bound nothing the surrounding pipeline does
not. One gap is named rather than hidden: `algo.localClusteringCoefficient`'s CSR path hands the whole
computation to a `GraphAlgorithms` kernel that counts triangles across a thread pool, and the `WorkCheckpoint`
hook is specified to be called between iterations on the calling thread - covering that kernel is a change to
how it partitions its work, not a checkpoint that can be dropped in.

[#6295](https://github.com/ArcadeData/arcadedb/issues/6295)
[#6302](https://github.com/ArcadeData/arcadedb/issues/6302)

## `algo.mst`'s edge arrays are priced against the working-memory budget (#6300)

#6263 reserved the dense working set of every `algo.*` procedure that builds one, choosing them by two
criteria: sized by a knob, or quadratic in the node count. `algo.mst` is neither - it is linear in the **edge**
count, three parallel arrays plus the index sort, 24 bytes per edge - which reads like the graph paying for
itself. But linear in the edge count is not small: the edge count is the largest linear dimension a graph has,
usually an order of magnitude above the node count, and at 100M edges that is ~2.4 GB requested with no check
and no error naming what asked for it, i.e. exactly the `OutOfMemoryError` shape #6065, #6216 and #6263 exist
to replace with a client error. "Linear" was never the criterion; the criterion is whether the caller can
predict a ceiling.

The reservation is made **as the counting pass runs** rather than once it has finished. Both refuse the same
calls, but a check afterwards first pays in full for a traversal it will then throw away - the same argument
that puts `algo.steinerTree`'s reservation ahead of its adjacency build. The new
`MemoryBudget.capacityFor(bytesPerItem)` is what hands the walk its own stopping rule; the refusal itself still
goes through `MemoryBudget.reserve`, so the message names the same component and the same setting either way.

[#6300](https://github.com/ArcadeData/arcadedb/issues/6300)
## A super-node's read walk keeps its recency order for the whole walk, not just the first 1,024 entries (#6064)

`arcadedb.graph.supernodeInterleaveRounds` (#6048) was a **cliff**. Up to `rounds × stripes` entries - 64 × 16 =
1,024 by default - a promoted super-node's read walk took one entry per stripe chain per turn, which is what
puts an edge of recency rank `r` back at a position of order `r` (#6044). The very next entry froze the
rotation: the chain holding the turn was drained to exhaustion, then the next, then the next. Past 1,024 the
returned position stopped having any relation to recency, and the error grew with the vertex's DEGREE again -
the exact failure #6044 fixed, pushed past a constant.

A query with a small `LIMIT` never reaches the cliff. A query with a **large but still bounded** one - an
export, a batch job, paginated admin tooling, `MATCH (h)-[:LINK]->(x) RETURN x LIMIT 5000` - fell off it
silently: it asked for a bounded result, got one, and the newest edges it expected near the front were spread
across the whole list. The only lever was raising `supernodeInterleaveRounds` for every unbounded read in the
database too, which is precisely the full-walk locality cost #6048 removed.

The rotation now **widens** instead of stopping. Past `rounds × stripes` entries it keeps turning, but takes
`rounds` entries from each chain per turn instead of one, doubling that batch on every completed round. Both
properties hold at once:

- **Locality** (what #6048 was about): the chain switches over a walk of `D` entries drop from `D` to about
  `stripes × log D`, and once the batch outgrows a chunk every visit is a sequential run through one chain, so
  the resident-page pressure the interleaving costs decays instead of persisting. Counted against the previous
  degrade on a 1,000,000-edge walk at the defaults, that is ~1,184 chain switches against ~1,040 - 0.1% of the
  entries walked, either way.
- **Rank fidelity** (what #6064 is about): an entry at depth `d` in its chain is emitted no later than the end
  of the batch that reaches depth `d`, which is at most twice `d`, so its position stays within a constant
  factor of its true rank for the WHOLE walk. Measured on a 3,000-edge super-node at 16 stripes and 4 rounds
  (a 64-entry threshold, deliberately small to put most of the walk past it), the worst position/rank ratio is
  2.4; before, rank 40 came back at position 2,386, a ratio of 58.

No new setting, and nothing has to be told what the query's `LIMIT` was - which is the alternative this
replaces, and it would have meant threading a "how many do you actually need" hint from every query engine's
execution plan down into `EdgeLinkedList`, across an iterator API that has no concept of one.
`supernodeInterleaveRounds` keeps its meaning (how long the walk stays at one entry per turn) and its `0`
still disables interleaving entirely, giving immediate concatenation in the pre-#6044 order. Read-side only:
no on-disk format change, no migration.

[#6064](https://github.com/ArcadeData/arcadedb/issues/6064)
## A schema probe spelled `LIMIT 0` gets a RowDescription over a row source the schema cannot describe (#6185)

`SELECT expand(shortestPath(#1:0, #1:5)) LIMIT 0` is a schema probe, and #6172 had already made a probe
describable no matter how its rows are computed: when the statement returns nothing, the Postgres wire protocol
replays it without the clause that empties it and reads the columns off the first row, which is the only way to
name the columns of a row source that is not a schema type - a graph function, a `TRAVERSE`, a `MATCH`, a constant
table. The trigger for that replay looked only at the `WHERE` clause, so a client that spells the probe `LIMIT 0` -
Tableau and several JDBC/BI tools do - was answered with an empty RowDescription again, exactly as before the fix.

The trigger is now "the statement is empty by construction", the same question the SQL planner asks in #6174 when
it folds the fetch away, and it is read at every level of the query, so `LIMIT 0` on the probed subquery counts as
well as `LIMIT 0` on the statement the client sent. Only a literal `LIMIT 0` qualifies (`Limit.isAlwaysEmpty()`): a
bound `LIMIT ?` belongs to one execution, and the replay must not be triggered by a value the next execution can
change.

The two spellings are deliberately not answered in the same order, because they do not carry the same intent. The
replay evaluates the query's projection for real, once, on one row - the caveat that has been documented on
`sampleProbeColumns` since #6172 - so a projected function with a side effect runs once per probe that takes that
path. `WHERE 1=0` has no purpose other than probing, so it is replayed first, as it always was. `LIMIT 0` is also
how a client asks an expensive query for nothing at all, so it is replayed only after the static resolution of the
row source has come back with nothing: a `LIMIT 0` over a shape the schema can describe is answered from the
schema, without evaluating anything, and only the shapes that cannot be answered any other way are replayed.

[#6185](https://github.com/ArcadeData/arcadedb/issues/6185)
## An always-true filter is dropped at plan time instead of being evaluated once per record (#6184)

The mirror of #6174. That issue taught the planner to recognise a filter that is false for every record; a filter
that is *true* for every record was still built as a filter step and evaluated per record, so `SELECT FROM C` and
`SELECT FROM C WHERE 1=1` - the same query, written twice - produced two different plans:

```
SELECT FROM C                    -> + FETCH FROM TYPE C
                                      + FETCH FROM BUCKET 1 (C_0) ASC

SELECT FROM C WHERE 1=1          -> + FETCH FROM TYPE C WITH FILTER
                                      + SCAN WITH FILTER BUCKET 1 (C_0)
                                        1 = 1
```

The second paid a predicate evaluation per record, and lost the plain bucket fetch, to learn something the
statement had already said.

Nothing was stuck for want of a rule. `BooleanExpression.isAlwaysTrue()` had existed all along and was overridden
by `AndBlock`, `OrBlock`, `ParenthesisBlock` and `NotBlock` - but **no leaf could implement it**, because the
signature took no `CommandContext` and folding a comparison needs one to reach `operator.execute(...)`. So
`BinaryCondition` never overrode it and the recursion always bottomed out at `false`: the method had no consumer
outside the AST's own recursion, and `NOT (1=1)` was not folded as always-*false* either, even though
`NotBlock.isAlwaysFalse` delegates to exactly this method for its negated branch.

`isAlwaysTrue` now takes a `CommandContext`, the shape `isAlwaysFalse` got when it was introduced. The no-argument
form is gone rather than kept alongside it - it had no caller outside the four AST classes that recursed into it,
so two forms would only have been two things to keep consistent. `BinaryCondition` folds both verdicts through one
private helper under the same restriction: **both** operands must be `Expression.isLiteral()`, which excludes a
bound parameter (the plan outlives the execution that bound it) and a function call (nothing on `SQLFunction` marks
a function as pure). `WHERE ? = 1` and `WHERE uuid() IS NOT NULL` are therefore still planned as scans, as they
must be.

The planner drops the where clause in `init()`, before the clauses are rearranged into index searches, so
everything downstream sees the statement it would have seen without a `WHERE` at all: the projected properties
computed for column pushdown, the choice between `FetchFromTypeWithFilterStep` and `FetchFromTypeExecutionStep`,
and the hardwired `count(*)` plan. `NOT (1=0)` folds to always-true and `NOT (1=1)` to the always-false
`EMPTY RESULT` of #6174, both for free through the existing `NotBlock` delegation.

Dropping a filter is a wider step than folding one to an empty result: an always-false fold can only be wrong by
returning too few rows, while dropping an always-true filter changes which fetch step is chosen. The regression
test therefore compares the folded plan against the plan of the same statement written without its `WHERE` - step
class by step class, so the fetch step and the bucket steps under it both have to agree - and then compares the
records returned, their order, and the `readRecord` count.

One place the filter survives the whole-clause fold is behind an index. `WHERE 1=1 AND indexedProperty = 'x'` is not
true for every record, so the clause stays - and then the index search takes the second term as its key and hands the
`1=1` back as a *residual condition*, which was chained as a `FILTER ITEMS WHERE` step and evaluated once per index
entry. A residual that is true for every record is now recognised as no filter at all, on both the single-index and
the parallel-index paths, so the plan is the one the same statement without the constant term would have got. The
index itself was never at risk: it was selected before and after, the residual only rode along behind it.

One `OrBlock` detail changed with it: an empty disjunction used to answer `isAlwaysTrue()` with `true`. It now
answers `false`, matching what `isAlwaysFalse` has always answered for the same block. Neither verdict reads it as
the neutral element, even though that reading (an `OR` with no alternatives is `FALSE`) is available: the parser
cannot produce an empty `OrBlock`, both answers are load-bearing - one drops the filter, the other replaces the plan
with an empty source - and deducing either for a block that can only arrive by accident would trade correctness for
an optimisation nothing can trigger. An empty `AndBlock`, by contrast, is exactly what a filter with no terms left
looks like, so it does answer `isAlwaysTrue`, and that is what lets an emptied residual condition drop its step.

**API note for code that extends the SQL parser**: `BooleanExpression.isAlwaysTrue()` is replaced by
`isAlwaysTrue(CommandContext)` rather than kept alongside it, so an out-of-tree subclass that overrode the no-argument
form no longer overrides anything. This is a compile error against the new signature and a silently inert override
only if the subclass keeps the old method as an unrelated one; nothing inside ArcadeDB called the no-argument form
outside the four AST classes that recursed into it.

The value here is smaller than #6174's: nothing sends `WHERE 1=1` as a probe the way Spark sends `WHERE 1=0`, so
this is symmetry and a small constant per record rather than a full scan avoided.

[#6184](https://github.com/ArcadeData/arcadedb/issues/6184)
## The HTTP query endpoints now have a hard row ceiling a caller cannot widen (#5719)

**Breaking change.** `arcadedb.server.httpQueryMaxResultRows` is a new server setting, defaulting to
**1,000,000**, that bounds the number of rows a single HTTP query or command response materializes. A
deployment that relies on an HTTP caller pulling an unbounded result in one response - typically by sending
`"limit": -1` - has to raise it or set it to `-1` to keep that working.

The ceiling closes the gap that #5716 left open. `arcadedb.server.httpQueryDefaultLimit` (default 20,000)
caps only the callers that state no limit of their own: since #5716 a request carrying a `limit` field, and a
query carrying its own `LIMIT`, are both honored as written, because a response silently cut below what the
caller asked for is exactly the defect #5711 fixed. The consequence is that an authenticated caller writing
`SELECT FROM HugeType LIMIT 100000000` - or an application that builds `LIMIT n` from user input - could make
the server serialize an arbitrarily large result into one JSON document. No privilege boundary moved (`limit:
-1` was always available to the same actor), but the bar to reaching it accidentally dropped, and in a
multi-tenant deployment the implicit 20,000 cap had been doing real protective work.

The new setting is the HTTP twin of `arcadedb.server.grpcQueryMaxResultRows` and behaves the same way. A
stated cap at or below the ceiling is the caller's own bound and is honored silently, truncation reported with
`truncated` exactly as before. A larger one - or an unlimited `-1`/`0` - cannot widen the response past the
ceiling, and a result that would exceed it **fails with HTTP 413 naming the setting** instead of being
truncated: a truncated response indistinguishable from a complete one is what #5711 removed, and
re-introducing it for the callers #5716 protects would only have moved it. The refusal is raised from inside
the handler, so an auto-committed write command whose result nobody will ever see is rolled back rather than
committed behind an error status.

A finite default was chosen over a disabled one because every other resource ceiling the server ships -
`httpBodyContentMaxSize`, `grpcQueryMaxResultRows`, `grpcStreamMaxMaterializedRows` - is finite by default, and
a ceiling an operator has to discover before it protects anything protects nobody. At 1,000,000 it sits 50x
above the default page cap, so a realistic paging or export client never meets it.

The two settings cannot be configured into disagreeing. `httpQueryDefaultLimit` is itself lowered to the
ceiling when it is configured above it (or left unlimited while the ceiling is not), so a caller that states
no limit at all is never refused - it gets the ordinary reported truncation it has always got, at the lower of
the two values. The refusal is for a caller that asked to go past the ceiling, never for one served by the
default.

Two things the ceiling deliberately does not bound, both worth knowing before sizing it. It bounds the size of
a response, not the peak cost of refusing one: the rows are serialized up to the ceiling before the result
turns out to exceed it, and the work is then thrown away, so a client that routinely brushes the ceiling
produces real garbage rather than merely a smaller payload - the same trade the gRPC path makes, and the price
of never truncating silently. And it bounds the response, not the work a command does to produce it: a write
that returns its rows (`UPDATE ... RETURN AFTER`) applies to every matching record before the response is
built, and is then rolled back with the refusal, so the ceiling is not a guard against write amplification.

Two paths that used to escape any bound are bounded at the fetch rather than at serialization: the `LIMIT` the
POST endpoints push down into a command that states none is now capped by the ceiling (an unlimited `limit`
used to push nothing down at all), and `profileExecution: detailed`, which drains the whole result set into
memory before serializing it, drains at most one row past the cap the response will honor. The TimeSeries
query endpoint enforces the ceiling on both of its shapes - the raw rows, and the buckets of an `aggregation`
request, which reads no `limit` at all and so had the ceiling as its only possible bound - but only over the
response it builds: `TimeSeriesEngine.query()` materializes the whole requested range before any limit is
known, so bounding that fetch needs an engine-level change and is left as follow-up work.

On the status code: 413 is conventionally about an oversized *request* body, and no standard code says "the
response you asked for is too large to send". 413 is the closest fit and keeps the refusal in the 4xx range,
where it belongs - the request is answerable, just not as written - but a client that maps status codes
mechanically should note that the fix is to narrow or page the query, not to shrink the request body. The
error body distinguishes the two: this refusal names `arcadedb.server.httpQueryMaxResultRows` in its `error`
field and carries the ceiling in `exceptionArgs`, in every server mode.

[#5719](https://github.com/ArcadeData/arcadedb/issues/5719)




### An index build now holds the asynchronous workers still, and asynchronously dispatched SQL DDL runs instead of being refused

Three things changed here, two of which are worth checking against how you run builds.

**An index build parks the database's async workers for its whole duration.** It always needed to: the build
scans the buckets, and a record written by an async worker during that scan is in neither the scan nor the
index - it was saved before the index existed, so it staged no entry for it either, and the result is an index
that is readable, reported healthy, and answers lookups with nothing. `REBUILD INDEX` and `CHECK DATABASE ...
FIX` already parked the workers; the ordinary `CREATE INDEX` path did not park them at all, and now does. The
pause is also gated rather than fired and forgotten - each worker commits its open batch and confirms it has
parked before the scan starts, where previously the build began whether or not any worker had reached it.

The operational consequence, named because it is bigger than "index builds pay a short barrier": while a build
runs, tasks submitted to `async()` for that database queue up behind the parked workers, and once a worker's
queue fills the backpressure reaches the submitting thread too. On a long build that is the database's whole
asynchronous ingestion path stalled for the duration. There is no cheaper correct arrangement - releasing the
workers between the index's registration and the scan would index a record written in that window twice, once
by its own staged operation and once by the scan - so a build is now something to schedule off peak write load
rather than merely something it was advisable to.

**`CREATE INDEX` and `REBUILD INDEX` sent with `awaitResponse=false` now work.** They used to hang, and since
26.9.1's #6281 they were refused with a clear `NeedRetryException` and a "run it synchronously" workaround: the
statement ran on one of the async workers, and the barrier it needs enqueues a task on every worker including
that one. A statement that parses to DDL is now dispatched to a small JVM-wide pool whose threads are not
workers of any executor, so the barrier is satisfiable. Everything that is not DDL still runs on the workers
exactly as before - that matters, because a worker owns a batch transaction and is the unit
`ThreadBucketSelectionStrategy` pins a bucket to, so "as many workers as buckets" stays the way to keep
concurrent asynchronous writers from contending.

**This covers `sql` and `sqlscript` only.** The routing decides by PARSING the statement, which is cheap and
exact for SQL and available for nothing else, so DDL-equivalent statements dispatched asynchronously in
Cypher, Gremlin, GraphQL or the Mongo dialect keep the behaviour they have today: they run on a worker and are
refused there by #6281's guard. That is a refusal, not the old hang, and the workaround is unchanged - send
them with `awaitResponse=true`.

Two new settings size the pool: `arcadedb.asyncCommandPoolThreads` (0 = cores, min 2) and
`arcadedb.asyncCommandQueueSize` (1024). **Size them knowing what the fallback does**, because it is easy to
miss until it happens under load: when the queue fills, the statement runs on the SUBMITTING thread rather
than being dropped, and for `POST /command` that thread is an HTTP worker already inside the request's own
transaction wrapper. So a saturated pool turns "fire this index build and don't wait" into an HTTP request
that blocks for the whole build - correct, never lossy, and slow in a way the caller explicitly asked not to
be. The `pool=async_command` caller-runs gauge counts exactly that, and is the number to alert on.

**A callback that throws no longer reaches the executor-wide error handler.** An exception escaping the
`onError` of an `async().command(...)` callback used to propagate into the worker loop, which reported it to
the handler registered with `async().onError(...)` and rolled back the worker's in-flight batch. It is now
contained and logged at WARNING. The batch of unrelated tasks surviving somebody else's callback bug is the
point; the change is mentioned because a deployment that watched only the executor-wide handler will now find
that class of failure in the log instead.

[#6303](https://github.com/ArcadeData/arcadedb/issues/6303)
## An ambiguous HA endpoint is reported again when the collision changes, not once per node lifetime (#6297)

The warning #6267 added for a withheld peer-to-peer endpoint - two peers resolving to one `host:port`, so
the address identifies at most one of them and nothing can say which - was logged at most once per
`RaftHAServer`, which in a server process means once for its lifetime. That is right up until the operator
acts on it. They read the line, declare the two ports it named, and the *next* collision - a different pair,
or a peer added at runtime onto an address already taken - is the one nobody is told about, because the latch
is spent.

The latch is now the rendered collision list itself. An unchanged verdict stays quiet however often the
resync and verify paths ask, a changed one is reported whatever produced it, and a pass that finds no
collision clears the memory, so a misconfiguration that is fixed and later reintroduced is announced rather
than swallowed as "already reported". That last part needs the callers to hand over the *clean* views too,
which is the half that was missing: both `getRoutingTable` and the per-peer accessor used to consult the
warning only when the current view was already ambiguous, so the stored verdict outlived the collision it
described. It covered HTTPS as well, since only the group-wide HTTP resolver was unconditional. The client
routing-table warning now also names the peers that collided and the address they share, which its
peer-to-peer twin already did. `httpAddressAmbiguous` on `GET /api/v1/cluster` is unchanged and remains the
always-current view, recomputed per request.

### `EXPLAIN` and `PROFILE` now name the RID push-down (#6279)

`MATCH NODE` gained an `[id: <rid>]` marker alongside the existing `[index: ]`, `[partition: ]` and
`[filter: ]`, so a plan says whether the `ID(x) = <literal|parameter>` equality was lifted out of the `WHERE`
clause and into the node lookup at plan time:

```
+ MATCH NODE (a) [id: #1:0]
+ MATCH NODE (b) [id: #4:0]
```

That is the optimization #3216 was closed on, and its resolution asks users to write the query so it fires -
with parameters or literals rather than a value only known per row. Until now the plan was the one place that
would not confirm it had. `EXPLAIN` answers without running the query; a push-down that can only be resolved
per row (#3864) is named as `[id: per-row]` rather than valued.

### The test suite

The rest of this work is internal. A server test's teardown reset the configuration before the cleanup that
resolves its paths from it, and because `SERVER_DATABASE_DIRECTORY` defaults to
`${arcadedb.server.rootPath}/databases` over a root path that defaults to null - and an unresolvable variable
resolves to the empty string rather than failing - the cleanup was asking to recursively delete `/databases0`
while the test's own `./target/databases0` survived teardown untouched. The order is fixed and a guard now
refuses a path that did not resolve, so a future reordering degrades to a logged no-op instead of a
`deleteRecursively` at the filesystem root.

Alongside it, the wall-clock pass of #6260 was extended to the `server` and `network` suites, and the
`@Timeout` annotations in the engine suite were classified the same way: a bound that separates a bounded
operation from an unbounded one is a tripwire and is sized clear of an honest budget plus a stop-the-world
pause, while a bound that *is* the assertion - a laziness or complexity claim with no other practical
expression - is measured on the stall-discounted clock instead. Two tests that reported coverage they did not
have were repaired rather than deleted: one asserted only that two elapsed times were not negative, the other
could not tell the count push-down from the full type scan it exists to replace.

[#6297](https://github.com/ArcadeData/arcadedb/issues/6297)
[#6280](https://github.com/ArcadeData/arcadedb/issues/6280)
[#6279](https://github.com/ArcadeData/arcadedb/issues/6279)
[#6270](https://github.com/ArcadeData/arcadedb/issues/6270)

## `CHECK DATABASE` can now reclaim one of the three shapes of unreferenced file it reports (#6189)

#6143 gave an operator a way to *find* the paginated files a node holds that no schema component claims - what an
abandoned schema-WAL instalment sequence leaves on a follower that lost leadership mid-session, among other causes -
but it never deleted anything. Finding them was progress; an operator who read the `unreferencedFiles` result key or
the SEVERE line still had to stop the node and remove the files by hand. That was the right call for a diagnostic,
but it left the actual problem open: this database had no unreferenced-file garbage collection at all.

An **inferred**, always-on reclaim was considered and deliberately not built: a follower tracking which files its own
abandoned instalments created, and dropping the ones a later schema reload still does not reference. It would be a
file deletion driven by inference, unattended, on a replica, on a code path whose history is silent divergence rather
than exceptions - a bad ratio for a payoff that is only wasted disk.

What shipped instead is **operator-triggered**: a new opt-in clause, `CHECK DATABASE FIX RECLAIM UNREFERENCED
FILES`, on the same pattern #6090's `DELETE ORPHANS` already established - its own clause, meaningless without `FIX`,
refused with a clear error rather than silently implied. The finding is logged and recorded in the
`unreferencedFiles` result key before any file is touched, and what was actually removed is reported back under its
own `reclaimedUnreferencedFiles` key, seeded empty rather than left absent so a clean run reads "none" rather than
"nothing was asked."

It reclaims exactly **one** of the three shapes `UnreferencedFiles` can prove: a file the file manager holds with no
schema component built for it at all - the shape an abandoned instalment sequence leaves. That is the only one a raw
file delete can never turn into a worse state, because by construction nothing in the schema's registries names it.
The other two provable shapes - a bucket, or an automatic index, that a schema component still names but no type
claims - stay report-only: their file is only half of what would need to go, and unregistering the component too is
what `DROP BUCKET` and the index tooling already do safely. `UnreferencedFiles.UnreferencedFile` now carries that
distinction as a `Kind` enum alongside its human-phrased reason, so a caller can act on the shape rather than parse
the sentence.

[#6189](https://github.com/ArcadeData/arcadedb/issues/6189)

## An update that shrinks onto a chunk boundary frees the chunks it drops, and one `FIX` clears the backlog (#6319, #6320, #6314)

### Ordinary updates leaked continuation chunks (#6319)

A record too big for its page is stored as a chain of chunks, and an update rewrites the chain it already
has. Which chunk is the last one, and which chunks are therefore no longer needed, were decided by two
different comparisons: the chain was CUT whenever the new content ended at or before the chunk being
written, and the tail was FREED only when it ended strictly before it. Content ending exactly ON a chunk
boundary fell between them - cut, and freed by nothing. The chunks past the cut kept their slots and their
pointers to one another, reachable from no record, for the life of the bucket.

That is not a corner case to be hit by luck. A chain grown one field at a time has a boundary exactly where
that field's bytes were appended, so a record that later loses a field lands on one by construction:

```
records still stored as a chain across the shrink round : 7359
orphaned chunks they left behind                        : 14719     (every one of them, its whole tail)
```

Measured on a scaled-down `CRUDTest.multiUpdatesOverlap`; at full scale the same workload carried 243821
orphans in a bucket of 1545495 chunk slots. Both decisions now come from the same condition, and the same
run leaks nothing. Existing backlogs are reclaimed by `CHECK DATABASE FIX`, which #6294 taught to find them.

### `CHECK DATABASE FIX` bounds the memory of every repair it makes, and finishes in one run (#6320)

#6294 bounded the orphaned-chunk sweep with a memory budget of its own, because the transaction a repair
runs in keeps a copy of every page it modifies. The RECORD repairs of the same pass - the force-deletes of
records whose slot offset, size or chunk chain cannot be read, each taking its record's page plus every page
its chain touches - were bounded by nothing, and could reach the `OutOfMemoryError` of #4653 through the
other loop.

A second budget would have been the wrong answer twice over: what is scarce is ONE pool, so two bounds over
it cannot both be right, and a repair that STOPS when the pool is spent leaves an operator running `FIX`
over and over to converge. The mechanism that gives the pool back already existed for the graph repairs
(`arcadedb.checkDatabaseRepairBatchPages`, #6128) and now bounds every bucket repair as well: records,
dangling placeholder pointers and orphaned chunks alike. One run repairs everything a bucket has wrong, with
the memory bounded throughout, and the sweep no longer leaves a backlog for the next `FIX`.

**Behaviour change worth scripting around.** A bucket repair is no longer all-or-nothing. It used to join
whatever transaction the caller had open - through HTTP a command always arrives with one - so nothing was
durable until the whole command finished and a mid-run failure rolled back everything. Each bucket now
repairs in a transaction of its own, committed in batches, so a run that fails part-way leaves the batches
before it applied. That is the same trade #6128 made for the graph repairs and the reason the memory is
bounded at all; the counters in the report say what was done, and a re-run continues from there. Setting
`arcadedb.checkDatabaseRepairBatchPages` to 0 restores the single all-or-nothing transaction, memory cost
included.

The batch is taken between whole units of repair work and never inside one, so no record is ever left
half-repaired, and the counter `count(*)` answers from is invalidated when a repair STARTS rather than only
when it ends - a batch that has committed has really removed those records, and the counter must not go on
serving the number from before it.

### A component must believe its own file about how to address it (#6314)

`PaginatedComponent` turns a page number into a file offset with a page size it is CONSTRUCTED with, while
the file's real page size is baked into its name. The two TimeSeries components discarded the value parsed
off the name and re-derived it from the live `arcadedb.bucketDefaultPageSize` instead, so a database whose
configuration changed between two runs reopened its `.tstb`/`.tstd` files at a stride they were never
written with. Nothing downstream could catch it - the page manager resolves a page with the caller's page
size - so the failure was a misaligned read of real bytes rather than an error, with the component's page
count computed from the wrong divisor on top of it. Both now pass the parsed page size and version through,
as every other component's factory handler already did, and the agreement is asserted once in
`PaginatedComponent` next to the file-id check #6283 added.

The by-id `FileManager.getOrCreateFile` had the mirror of #6283's hazard on the other key: an id already
registered handed back whatever file was registered under it, without ever looking at the path it was asked
for. Its one production caller, the HA follower's file-creation path, already checked exactly this itself
(#6063); the check now belongs to the API, throwing the same `SchemaException` so the quarantine-and-resync
that path keys on is unaffected.

[#6319](https://github.com/ArcadeData/arcadedb/issues/6319)
[#6320](https://github.com/ArcadeData/arcadedb/issues/6320)
[#6314](https://github.com/ArcadeData/arcadedb/issues/6314)

### A repair pass that fails anywhere but its batch commit no longer abandons its transaction (#6342)

Every repair pass of `GraphDatabaseChecker` - the vertex arm, the edge arm and the orphaned-edge-segment
reclaim - opened a transaction of its own and committed it as the LAST statement of its `try`, with a
`finally` that filled in the returned counters and nothing else. So the ONE failure that was already safe was
a batch commit that throws: `LocalDatabase.commit()` disposes its own context in a `finally` whether or not
the write succeeded, which is what `CheckDatabaseRepairBatchFailureTest` pins. Every OTHER way the body could
throw - a scan that fails, an unreadable page, an `IllegalStateException` out of a repair - left the pass's
own transaction open on the thread.

What that costs depends on who is underneath. `CHECK DATABASE` arrives through the HTTP handler with a
transaction already open, so the pass's own is NESTED on top of it: the handler's cleanup `rollback()` -
running because of the very exception that caused this - then popped the abandoned nested transaction and
left the handler's, which is the opposite of what it intended, and the caller was left holding work it
believes it has just discarded. Embedded, with nothing underneath, the next user of that thread inherited an
in-flight transaction from a repair that had already failed.

The answer is not `commit()` in a `finally`, and not acting on `database.isTransactionActive()`: under an
outer transaction that question answers "yes" for somebody else's, so cleaning up on that evidence rolls back
work the pass never made. `RepairTransaction` - introduced for the bucket repairs of #6320 and now shared by
all four passes - tracks ownership explicitly, dropping it before each batch commit and taking it back after,
so the cleanup can only ever touch a transaction the pass itself opened and still holds. A pass that fails
part-way now rolls back the batch in flight and keeps the ones already committed, which is the semantics
#6128 gave these repairs in the first place.

### The schema dictionary's truncation guard now reads the bytes, not the outcome of a page read (#6341)

A dictionary page the component's page count claims but that is not really there must stop the load: an empty
page contributes zero names, so every name after it comes back with an id lower by however many the missing
page held, and those ids are written inside records. Silently renumbering them is the one outcome that class
exists to prevent.

The guard inferred "the page is not there" from the page manager refusing the read, and the page manager
refuses only a page past the END of the file. It cannot refuse a hole INSIDE it - and a hole is exactly what
the scenario the guard names produces. A partial replication replay writes page N without the ones before it,
and writing at offset `N * pageSize` extends the file over every page it skipped rather than leaving the file
short. Those pages read back as zeroes, a zero page declares a content size of zero, and the load took that
for a legal empty page: no error, no names from it, and every name after it renumbered. Reproduced by
shipping one replicated dictionary page one further along than the follower expected.

Every dictionary page ever written carries the four-byte legacy counter, on the append path and on the
whole-file rewrite alike, so a content size below it is not a legal empty page - it is a page nobody wrote.
`reload()` now says so, which is a statement about the bytes rather than about whether a read happened to
succeed, and therefore also covers an unwritten image reaching it by any other route. Page 0 keeps its one
deliberate exemption: materialising it for a file shorter than one page is what keeps a database killed
mid-write openable, and there is nothing before it to renumber.

[#6342](https://github.com/ArcadeData/arcadedb/issues/6342)
[#6341](https://github.com/ArcadeData/arcadedb/issues/6341)

## A `REBUILD` setting is evaluated and validated, so a typo stops selecting the other behaviour (#6359)

`REBUILD INDEX` and `REBUILD TYPE` read their `WITH` settings by evaluating the setting's expression and
handing the value to one of two shared readers on `DDLStatement`. Both previously read the expression a
different way, and both let bad input through without saying so.

The user-visible change is the boolean one. `Boolean.parseBoolean` answers `false` for anything it does not
recognise, so `REBUILD INDEX i WITH statsOnly = yes` did not fail - it silently selected the OTHER behaviour,
which for `statsOnly` is the difference between recomputing index statistics and rebuilding the entire index.
Boolean settings now go through `parseBooleanSetting`, which accepts a boolean or `true`/`false` and refuses
everything else by name. A statement carrying such a typo reports it rather than quietly doing the more
expensive and different thing.

Two numeric fixes come with it. `REBUILD TYPE ... WITH batchSize` read `Expression.value`, which is null for
every numeric literal the parser builds, so it refused every value it was given, legal ones included, with
`got: null`. And `WITH batchSize = -1` reached `Integer.parseInt` as the string `"0 - 1"`, because a unary
minus was modelled as a subtraction from zero - a raw `NumberFormatException` naming neither the setting nor
the problem. Numeric settings now go through `parsePositiveIntSetting`, which names the statement, the setting
and the value, and a negative literal is now the number the user typed (see below).

Evaluating rather than rendering is also what makes `WITH batchSize = :size` resolve a bound parameter:
rendering an expression answers with the placeholder text, which no amount of parsing turns into an integer.

## A negative literal is a number, and the parentheses a statement was written with survive (#6359)

The SQL parser modelled a unary minus as a subtraction from zero, so the AST for `-1` was `0 - 1` and that is
what `Expression.toString()` produced. That rendering is not cosmetic: it is what `EXPLAIN` prints, what an
unaliased projection is named after, and what a statement re-reading one of its own settings parses. The minus
is now folded into the literal, keeping the exact numeric type the arithmetic used to produce.

Measuring that turned up the same defect for the parentheses a statement was written with, which the renderer
dropped: `(1 + 2) * 3` rendered as `1 + 2 * 3`, which reads back as 7 rather than 9, and `2 * -1` rendered as
`2 * 0 - 1`, which reads back as -1 rather than -2. Parentheses are now printed where dropping them could
change the meaning - around a compound arithmetic operand - and not where they are decoration, so
`SELECT (name) FROM V` keeps the column name it has always had.

[#6359](https://github.com/ArcadeData/arcadedb/issues/6359)

## `CHECK DATABASE` on a TimeSeries type: a `DEEP` tier, a `FIX` that repairs derived bookkeeping, and a sealed block that knows where it is (#6360)

#6340 gave TimeSeries its first coverage in `CHECK DATABASE`, and deliberately left three questions open. This
closes all three.

### `CHECK DATABASE ... DEEP` decodes the data instead of reconciling what describes it

The default tier reads every byte of every sealed store to verify each block's CRC32. That proves the bytes are
the bytes that were written, and proves nothing about whether they mean what the block claims. Three things
every read path answers queries from, without ever looking at a value, are checked only under the new `DEEP`
clause:

- **sorted timestamps** - the range iterator binary-searches a block's timestamps, so an unsorted block silently
  returns a subset of the rows that match a query;
- **the declared per-column min/max/sum** - the aggregation push-down answers `MIN`/`MAX`/`SUM`/`AVG` straight
  from them without decompressing anything, so a wrong statistic is a wrong answer nothing later contradicts;
- **the declared distinct tag values** - block-level pruning SKIPS a whole block whose declaration does not list
  the value being filtered on, so a value present in the data and missing from the declaration hides those rows
  entirely.

Every one of those is a wrong ANSWER rather than an error, which is the category of damage a checker exists for.

```sql
CHECK DATABASE DEEP
CHECK DATABASE TYPE Metrics FIX DEEP
```

The per-block CRC pass deliberately did NOT move behind the new clause. It is the same cost class as the record
scan the checker already runs over every bucket of every document type, and a default tier that skipped it would
answer "clean" having read only the directory.

### `FIX` now repairs the derived bookkeeping, and still never touches a sample

Three things a TimeSeries type stores are derived from data they merely describe, and all three are now
repaired: the mutable bucket's page-0 counters (sample count, min and max timestamp), the sealed store's header
block count and global timestamp bounds, and the tail of an interrupted sealed append. None of that is cosmetic -
`loadDirectory` reads the sealed global bounds out of the header rather than recomputing them, so a range query
pruned against a wrong bound silently misses data the file holds, and `appendBlock` writes at the END of the
sealed file, so a tail nothing can read makes every block appended after it unreadable too.

The tail repair is narrower than it sounds, on purpose. Only a tail that STARTS with a block magic is dropped:
that one is a block the directory scan recognised and could not read to the end of, so it is incomplete by its
own evidence. A tail that does not start with one could equally be a COMPLETE block whose magic took a bit flip,
which a hex editor can still recover, so it is reported and left exactly where it is.

Everything else is reported and left alone, and that is the answer rather than a deferral: a sealed block is the
only copy of the samples in it, so "repair" there means choosing which samples to throw away. Under HA a sealed
store is derived and a node can rebuild one by recompacting from its replicated mutable pages, which makes
discarding one recoverable rather than automatic, and that is an operator's decision.

Two new result keys: `repairedTimeSeries` (a count) and `timeSeriesRepairs` (one line per repair). Neither folds
into `autoFix`, which has always been the record-action count.

### A sealed block now records where it is and which CRC guards it

`BlockEntry.blockStartOffset` and `storedCRC` were assigned in exactly one place, inside `loadDirectory()` - so
a block this process WROTE carried zero in both, while the constructor pre-set `crcValidated = true`. Since
`commitTempCompactionFile` installs a rewritten directory without re-reading it, that was the state of the
entire live directory after every compaction, retention pass and downsampling cycle. Nothing broke only because
the two readers of those fields short-circuit on the flag that was hiding them; clearing it produced
`CRC mismatch in sealed store block at offset 0 (stored=0x0, ...)` on a perfectly healthy store. Both fields are
now set by every path that produces an entry, which also lets the check hold the directory in memory against the
file.

## The `??` operator returns its left operand again, and a mistake in a function call is answered as one (#6382, #6385, #6388, #6389, #6390, #6393)

The SQL null-coalescing operator `??` always returned its **right** operand, whatever the left one was. The
grammar declares the alternative and `MathExpression.Operator` carries a working `NULL_COALESCING`, but nothing
in between built the node: with no visitor for it, ANTLR's default `visitChildren` returns the last child it
visited, so the left operand was gone from the tree before anything could evaluate it. `'left' ?? 'right'` was
`'right'`, and `null ?? 'right'` agreed only by coincidence - which is the worst shape for a defect, because a
projection, a `WHERE` term or a `SET` value written `a ?? fallback` read as "the fallback is always taken". It
now evaluates left-to-right and short-circuits: the right operand, possibly a function call or a sub-query, is
not evaluated when the fallback is not taken. `toString()` had no case for the operator either, so the
rendering lost it too.

The A* heuristic stored the **source** vertex's coordinate where the current node's belongs, for three or more
axes (the two-axis branch was already right). `h(n)` was therefore the same number for every node - an
admissible heuristic that guides nothing, so the search did the work of a plain Dijkstra while claiming to be
informed. `dijkstra` delegates to A*, so any call supplying axis coordinates degraded with it.

`CONTAINSTEXT` on a full-text index split its literal on the first `:` and looked the remainder up as a
field-qualified key. A single-property index stores unprefixed tokens only, so an ordinary value carrying a
colon - a time, a timestamp, a `ns:name` - asked for a key the corpus never had and matched **nothing**. A
colon now introduces a qualifier only when the text before it names an indexed property, and on a
single-property index it is dropped even then, matching the normalization the Lucene-backed path already
applied; everything else is literal text handed to the analyzer, which is what the operator documents its
argument to be.

The rest is one theme across three issues: a raw JDK exception - an HTTP 500 - for ordinary valid input, where
a typed argument error (HTTP 400) is the answer. `duration(1,'year')`, `ts.timeBucket('0s', ts)`,
`ts.lag(v,-1,ts)`, `ts.lag(value)` without a timestamp, `date()` with a malformed pattern or unknown zone,
`[1,null,3].join('-')`, `map(1,'a')`, `[1,2,3,4].asMap()`, `'x'.convert('nope')`, `decode('!!!','base64')`,
`format('%d','x')`, `max([1,'a'])`, `bool_and([1,2,3])`, `nullField.lastindexof('a')` and
`'abcdef'.substring(2.5)` each threw, and `bool_and(5)` was worse: it returned a confident `true` for input it
never looked at. `sum()` was hardened against non-numeric input in #5799 and its siblings were not, so `avg`,
`variance`/`stddev`/`median`, `percentile` and the time-series aggregates answered a `ClassCastException` where
`sum` answered cleanly; the guard now lives on `SQLFunctionAbstract` and they all share it. Nulls are still
skipped, which is the documented aggregation behaviour.

**Behaviour change.** `map()` refuses a non-STRING key, `map(null, 'a')` included, where the raw cast used to
store `(String) null` and build a map with a key nobody can reference. This is deliberately stricter than the
`[...].asMap()` method, which converts what it is handed: `map()` takes key/value pairs the caller wrote out one
by one, so a non-string among them is a mistake in the query worth reporting, while `asMap()` is documented as
turning an arbitrary list *into* a map.

**Breaking change.** `sysdate()` takes a **zone id** as its only argument, per its documented syntax
`sysdate([<zoneid>])`, and now applies it: it used to read the zone from the *second* argument, so the
one-argument form silently dropped it and answered server-local time. A second argument is now refused rather
than accepted and ignored. Formatting is `.format()`'s job - `sysdate().format('yyyy-MM-dd')` - so a call
written `sysdate('yyyy-MM-dd')`, which never formatted anything, is now an error naming the unknown zone.

Three resource limits ride along, all reachable from caller-supplied text: the date formatter cache is bounded
(past the ceiling a formatter is still built, just not remembered), `format()` refuses a field width over a
million characters instead of allocating it, and a character index is parsed from a bounded literal rather than
whatever length the query text carries.

One adjacent defect surfaced with the A* fix and is repaired with it: of the five heuristics on three or more
axes, `EUCLIDEAN` was the only one that dropped `dFactor`, which nobody could see while every `h(n)` was the
same number. It now scales like its two-axis twin and like the other four.

[#6382](https://github.com/ArcadeData/arcadedb/issues/6382)
[#6385](https://github.com/ArcadeData/arcadedb/issues/6385)
[#6388](https://github.com/ArcadeData/arcadedb/issues/6388)
[#6389](https://github.com/ArcadeData/arcadedb/issues/6389)
[#6390](https://github.com/ArcadeData/arcadedb/issues/6390)
[#6393](https://github.com/ArcadeData/arcadedb/issues/6393)

## A documented A* option is applied, and CONTAINSTEXT answers the same question whatever the index's shape (#6414)

Four follow-ups from #6408, independent of each other.

`astar`'s `customHeuristicFormula` was documented in the function's own syntax string, listed among its accepted
options, and read from the caller's map into a field that **nothing ever consulted**. There was no `CUSTOM` constant
to dispatch to either, so a query supplying one silently got `MANHATTAN` - the default - with no error and no
warning. The option now names a SQL function that computes `h(n)`, called as
`fn(currentVertex, parentVertex, targetVertex, sourceVertex, depth, dFactor)` and returning a number. It is
consulted before any vertex-axis test, so a custom formula works with no `vertexAxisNames` declared at all and owns
`h(n)` outright: no axis is read and no tie-breaker is layered on its answer. Every way of getting it wrong is now an
error the caller sees - an unknown function name, `heuristicFormula:'CUSTOM'` with no function named, a built-in
formula named alongside a custom one (a contradiction, not a precedence question), or a function that returns
something that is not a number. `dijkstra`, which has no heuristic, keeps rejecting the option outright.

`CONTAINSTEXT` on a **multi-property** full-text index was two defects, and both are now fixed.

A condition on one of the index's properties was not pushed down at all: the planner required the *first* index
property to be constrained and then every following one, so `WHERE title CONTAINSTEXT 'java'` on an index over
`(title, content)` fell back to a full scan. That is not only slower - the scan evaluates `CONTAINSTEXT` as
`String.contains`, where the index matches analyzer *tokens*, so the same operator over the same data answered
differently depending only on how the index had been declared. `label CONTAINSTEXT 'ava'` found a row whose label is
`java` on a multi-property index and found nothing on a single-property one, and nothing in the query text told the
user which they were getting.

A condition on **both** properties was worse: it *was* pushed down, but the lookup read only the first key and the
planner had already removed both conditions from the residual filter, so the second was dropped entirely.
`WHERE title CONTAINSTEXT 'java' AND content CONTAINSTEXT 'zzz'` returned every row of the type - the second
condition matches nothing - rather than none.

A full-text index key is one query string per indexed property, not a composite key whose used prefix has to be
contiguous, and a multi-property index already stores every token twice, once qualified as `field:token` and once
unprefixed. The key is now POSITIONAL: `keys[i]` is the text to find in the i-th indexed property and a `null` slot
leaves that property unconstrained, so any subset of the properties in any position is an exact lookup, and several
of them are a conjunction. Every whitespace-separated word is bound to the property its condition names, not just
the first. A single-property index builds the same one-element key it always did, and a one-element key keeps its
historical "text to find in any indexed property" meaning, so nothing that passed a single query string changes.

> [!IMPORTANT]
> **Behaviour change.** `CONTAINSTEXT` on a property covered by a multi-property full-text index now matches analyzer
> tokens (case-insensitive, whole-token) rather than substrings, which is what the same operator has always done on a
> single-property index. A query relying on the substring fallback - `title CONTAINSTEXT 'ava'` matching `java` -
> stops matching there. Use a `SEARCH_INDEX(...)` wildcard, or `LIKE '%ava%'`, for substring search. The operator's
> meaning is now decided by whether a full-text index covers the property, which `EXPLAIN` answers, and no longer by
> how many properties that index happens to span.

Two smaller items ride along. The "is this qualifier really a field?" rule now lives in one place consulted by both
full-text query paths, rather than in two copies that drifted apart once already - #6382 existed because the
Lucene-backed executor had the rule and the direct `get()` path did not. And the designated regression test for
#1814 wrote its dynamic default as `default "sysdate('YYYY-MM-DD')"`, a double-quoted SQL *string literal*: the
call never happened, the literal text was stored verbatim, and the test asserted only that a constant string is not
null - so it could not have failed if dynamic-default evaluation regressed. It now uses an expression and asserts
the stored value parses as a date, before and after `APPLY DEFAULTS`, with a sentinel in between that the expression
cannot produce. Deliberately not an equality against today's date: the two transactions can straddle midnight.

[#6414](https://github.com/ArcadeData/arcadedb/issues/6414)

## JSONL import fails loudly on a broken record instead of reporting a silently-shrunken graph as a success (#6468)

`JsonlImporterFormat` caught every per-record exception - a bad property value, a missing type, a malformed old-RID
field - logged it at `SEVERE`, and moved on; `load()` then committed and returned normally regardless. A vertex
that failed to import left its old RID out of the in-memory RID map used to resolve edge endpoints, so every edge
referencing it hit the same swallowed "vertex not found" path and vanished too, with the import's own counters
reporting nothing wrong. A single bad record could silently take an unbounded number of good edges down with it.

`loadDocument`/`loadVertex`/`loadEdge` no longer catch their own errors: every failure - including the two
"out/in vertex not found" edge paths - now propagates to a single, centralized catch in `load()`'s dispatch loop,
which logs it once, counts it in `context.errors`, and then does one of two things depending on the existing
`-onRowError` setting (already respected by the CSV and JSON importers, previously ignored by JSONL entirely):

- **`abort`** (the default): the whole import fails with an `ImportException`, and the in-flight batch is rolled
  back rather than committed, so a failed import no longer leaves a partially-written database behind.
- **`skip`**: the failing record is discarded and the import continues. Getting this right required more than
  catching the exception: a vertex/document whose `save()` succeeds before a later step fails (originally, the old
  RID was parsed *after* `save()`) used to leave an orphaned, uncounted record in the database anyway, unreachable
  by the RID map - the same cascade, one step later. The old-RID field is now parsed and validated before `save()`
  is called, and `-onRowError skip` commits each record in its own transaction (rather than riding along in the
  periodic every-1000-records batch) so a failed record's rollback can never be durably persisted by a later
  commit.

> [!IMPORTANT]
> **Behaviour change.** JSONL import now fails by default (`-onRowError abort`, unchanged as the default value) as
> soon as any record cannot be imported, instead of silently skipping it and reporting success. A workflow that
> relied on the old silently-tolerant behavior should pass `-onRowError skip` (`IMPORT DATABASE ... WITH
> onRowError = 'skip'`) to keep importing past bad records - the `errors` counter in the import result reports how
> many were skipped.

[#6468](https://github.com/ArcadeData/arcadedb/issues/6468)
## An idle Postgres connection blocks instead of polling, a blob is `bytea`, and the system catalog answers by shape (#6410, #6411, #6412)

`PostgresNetworkExecutor.readMessage()` opened with a non-blocking read: `if (!channel.inputHasData())
{ sleep(100); return false; }`. Every **idle authenticated** connection woke its thread ten times a second to ask
an empty socket whether anything had arrived - and a Postgres client pool is expected to hold long-lived,
mostly-idle connections, which is the shape this cost the most on. The read now blocks: a server-side `close()`
breaks it (the cancel-request path already relied on exactly this to interrupt another connection's executor), and
a clean client close still produces the `EOFException` the method already handled. The same fix closes a second
hole: a client that vanished without sending Terminate used to leave its thread polling a socket that could never
produce another byte, for the life of the server, because `available()` answers 0 on a closed peer exactly as it
does on an idle one.

`PostgresType` typed a `byte[]` as `"char"[]` (OID 1002) from a sampled row and as `varchar` (OID 1043) from the
schema, so a BINARY column's announced OID depended on whether the result set happened to be empty. Neither answer
was right - PostgreSQL has `bytea` (OID 17), and typing a blob as an array of one-byte characters makes a client
decode arbitrary bytes as text. `BYTEA` is now its own `PostgresType` entry, with the `\x<hex>` text format, the
raw-bytes binary format, and both text input formats (hex and the pre-9.0 octal escape) accepted back.

The `pg_catalog`/`information_schema` queries a client's driver sends to discover schemas, tables and columns used
to be matched by roughly 150 lines of exact-string equality, several arms gated on `application_name` being
literally `"dbvis"` - so the same question from any other tool fell through and got nothing. They are now answered
by the *shape* of the query: which relations the FROM clause names, and the client's own projection - CASE
expressions, a window function, a one-level derived table - evaluated against rows built from ArcadeDB's own
schema. That is why `DatabaseMetaData.getTables()` now answers `TABLE_TYPE = 'TABLE'` for every JDBC-based tool:
the driver's own `CASE c.relkind WHEN 'r' THEN 'TABLE' ...` produces that string against a row saying
`relkind = 'r'`; the emulation never spells it itself. A shape outside what the evaluator reads is declined and
answered with the same empty result set `pg_catalog` has always given for something it does not understand, rather
than a guessed or invented row.

> [!IMPORTANT]
> **Behaviour change.** The emulated schema list is now exactly one schema, named after the connected database -
> matching what `current_schema()` already answered. The removed code disagreed with itself and with that function:
> one arm reported every ArcadeDB *type* as a schema, another reported every *database on the server* as one, which
> also told any authenticated user the names of databases they had no access to.

Also new: `arcadedb.network.maxPreAuthConnections` (default 500, per listener) caps how many connections a binary
wire-protocol listener (Postgres, Redis, BOLT) may hold before authentication - the pre-auth *timeout* added in
#6377/#5912/#5978 bounded how long a connection could stay unauthenticated, not how many could exist at once, so a
client opening connections faster than the timeout reaps them still drove thread and file-descriptor count
arbitrarily high. Past the cap the listener closes the socket immediately rather than accepting it and answering
with an error, since writing an error is an I/O the refused peer could stall, on the one thread that must never be
stallable.

[#6410](https://github.com/ArcadeData/arcadedb/issues/6410),
[#6411](https://github.com/ArcadeData/arcadedb/issues/6411),
[#6412](https://github.com/ArcadeData/arcadedb/issues/6412)

## The async executor no longer tears down and respawns its whole worker pool to flip a durability flag (#6509)

`DatabaseAsyncExecutorImpl.setTransactionUseWAL()`/`setTransactionSync()` unconditionally called `createThreads()`,
which shuts down and respawns every worker thread of the database's async pool - not just the caller's. `GraphBatch`
calls both setters once per batch session to relax and then restore durability for a bulk load, so opening or
closing a batch churned the entire pool. Any *other* concurrent user of `database.async()` - another `GraphBatch`,
an async insert, an index compaction - had its in-flight and queued tasks force-exited mid-flight, surfacing as
`"Async executor has been shut down"` for callers that had nothing to do with the batch.

#5665 diagnosed this and shipped a cheap interim mitigation (GraphBatch toggling the flags once per batch instead of
once per flush), explicitly deferring the real fix: `AsyncThread.run()` applied both flags only once, at thread
start, so respawning the thread was the only way a change could ever become visible to it. A production log (#6505)
showed the interim mitigation is not enough under real multi-writer concurrency: six threads on one database,
including a `GraphBatch` import running alongside a `DELETE ... BATCH` loop, produced 2,183 `InterruptedIOException`s
over about seven hours from the pool teardown interrupting unrelated in-progress LSM compactions, and one of those
races escalated into fencing the entire database when a commit's `onAfterCommit()` hook tried to schedule a
compaction at the exact moment the pool was mid-recreate (fixed separately by #6505 hardening the compaction
scheduler against that specific escalation).

The proper fix, per #5665's own diagnosis: `AsyncThread.executeTask()` now re-applies `transactionUseWAL`/
`transactionSync` to the worker's transaction before every task, instead of `run()` applying them once. The setters
are now plain volatile writes with no `createThreads()` call at all, so a changed durability policy is picked up by
whichever worker runs the next task - and by the commit that follows it - without any thread ever being torn down,
and without disturbing whatever unrelated work is queued on the rest of the pool.

Re-stamping the flags onto an already-open transaction is not, by itself, enough: `useWAL`/`walFlush` are read only
at commit time and then govern the *whole* accumulated transaction, which can span up to `ASYNC_TX_BATCH_SIZE`
(10,240 by default) tasks from unrelated callers sharing the same worker slot. Without a further guard, a flag flip
landing mid-transaction would silently carry earlier tasks - queued under the old policy - into a commit governed by
the new one, downgrading (or upgrading) their durability without anyone asking for it. The pool-teardown this PR
removes had incidentally prevented that: a flag flip forced every worker's pending transaction to commit under its
original flags before the new thread began a fresh one. `executeTask()` now preserves that "a flag change always
starts a fresh transaction" property explicitly - closing out the currently open transaction, under its own
unmodified flags, before applying a changed policy to a new one - instead of relying on thread teardown for it.

That boundary-forcing commit closes out *earlier* tasks' work, not the task whose flag check triggered it - so its
failure (a genuine `ConcurrentModificationException`, say, under the exact multi-writer contention this fix targets)
must not be attributed to that task, which has not run yet. Left unguarded, the failure would propagate to
`executeTask()`'s own catch block, and the triggering task would still reach `completed()` having never reached
`execute()` - a task silently marked done without ever running. The boundary commit's failure is now caught
separately, reported through the executor's `onError`, and the triggering task still gets its own attempt on a
freshly begun transaction.

`transactionUseWAL`/`transactionSync` are volatile and can be changed concurrently by any other thread - the exact
multi-writer scenario this fix is about - so reading them once for the boundary check and again for the flag stamp
left a narrow race: a flip landing between the two reads let the check see the transaction's flags still matching
the about-to-be-stale value (skipping the boundary commit) while the stamp went on to apply the new value anyway,
reintroducing the same durability-mixing failure through a race instead of a guaranteed ordering. Both reads now
share one snapshot taken at the top of `executeTask()`, so the check and the stamp always agree.

Worth knowing when tuning `parallelLevel`/queue sizing around concurrent bulk loads: the boundary-forcing commit is
paid per worker slot, not per database, so two `GraphBatch` sessions (or a batch and a direct caller) that land on
the *same* slot and keep flipping the durability policy in opposite directions can degrade that slot toward
`commitEvery=1` - each task forcing its own boundary. Correctness over batching is the right trade, and it is still
strictly better than the pre-fix full pool teardown, but a workload that provokes this is a sign the workers sharing
that slot would benefit from more parallelism (raising `parallelLevel`) rather than sharing one.

[#6509](https://github.com/ArcadeData/arcadedb/issues/6509)

## A dangling vertex reference is reported as a not-found, not as a conflict to retry (#6572)

An adjacency entry whose target vertex record was removed without the entry being disconnected - the documented
legacy of a pre-#5670 best-effort edge delete - hands a caller a handle to a record that is not there. Deleting
through it (`for (Vertex v : src.getVertices(OUT, "E1")) v.delete()`) reached
`GraphEngine.getEdgeHeadChunkForWrite`, whose lazy load of the vertex raised `RecordNotFoundException`, and that was
converted wholesale into the retryable `ConcurrentModificationException` the method exists to produce for a head
CHUNK that a concurrent commit has not published yet.

Two things were wrong at once, and both of them hurt a batch job. The type said "retry me" for a record that is
gone, so every attempt failed identically, the retry budget was spent on a foregone conclusion and the whole
transaction rolled back - one stale reference killing an entire nightly sweep, permanently, with no work
committed. And the advice said `CHECK DATABASE RECORD <rid> FIX`, which rebuilds the edge list *of* that record
from the surviving edges - while that record is precisely the one that no longer exists.

The evidence separating the two cases was already on the exception: the RID it names. A missing chunk (or stripe
head) really can be the publication window; the vertex the caller asked to modify cannot. When the cause names the
vertex's own RID, `getEdgeHeadChunkForWrite` now raises `VertexNotFoundException` - a `RecordNotFoundException`,
deliberately not a `NeedRetryException` - saying that the vertex does not exist and that it was reached through a
stale reference, and pointing at `CHECK DATABASE FIX`, which sweeps and drops the entries that point at missing
records. The RECORD scope is ruled out in words rather than rendered as a runnable command, since the entry to drop
lives on the vertex at the *other* end and no index maps a vertex back to the lists that name it. Everything else
keeps the #5670 conflict and the #5764 scoped advice unchanged.

Deleting a vertex whose record is already gone therefore stays an ERROR rather than becoming a silent no-op: that
is what `bucket.deleteRecord` answers for every other record shape, and for this vertex a few steps further down
the same delete. What changes is only that the error says what is wrong. Callers that walk possibly-stale
references can now tell "this will never work" from "try again" by catching `RecordNotFoundException`, and skip the
entry instead of losing the batch.

One related tolerance moves with it: `deleteEdge` already treats an endpoint vertex that is gone as "nothing to
disconnect on that side", decided by an `existsRecord` probe. The probe and the head-pointer read are two separate
reads, so the same fact can now surface between them as `VertexNotFoundException`; `disconnectEndpoint` tolerates it
identically rather than promoting a window the probe would have absorbed into a hard failure.

[#6572](https://github.com/ArcadeData/arcadedb/issues/6572)

## Both sides of an edge list answer a missing vertex the same way, and a delete decides it up front (#6586)

#6572 split "the vertex record is gone" out of `GraphEngine.getEdgeHeadChunkForWrite`'s retryable conflict using
the RID the `RecordNotFoundException` names. That split fires only when the head-pointer read has to go back to the
bucket, so two adjacent paths reached the same fact - a caller writing the edge list of a vertex whose record does
not exist - without going through the discriminator, and each still answered it in its own, worse way.

The first is the delete. A **materialised** handle - a RID whose content was loaded earlier in the same transaction
and used after the record went away - answers `getOutEdgesHeadChunk()` straight out of the buffer it already holds,
touches no bucket and raises nothing. The whole removal walk then ran on those stale heads, disconnecting every edge
from its far endpoint, and only the re-read at the end of the delete noticed. What it reported was
`ConcurrentModificationException: Vertex #4:0 was deleted by a concurrent transaction while its edges were being
removed` - retryable, so the caller spent its budget re-running a transaction that could only fail identically; and
untrue, since a single-threaded run reaches it too, with the same wording whether the record was removed by a
concurrent writer, by this very transaction, or never existed at all. It also carried no repair advice, unlike every
other failure that delete can produce.

`deleteVertex` now settles it before the walk, by reading the bucket's slot rather than trusting a handle: the
vertex record either has a slot or it does not, and a `VertexNotFoundException` naming `CHECK DATABASE FIX` is
raised immediately instead of after a full traversal that is about to be rolled back. The check reads only the slot
marker, so a vertex with a corrupt or truncated body is unaffected - it stays deletable, as #4420 and #4432 require -
and it is not tolerated under `force` either, for the reason #6572 spells out. The re-read at the end of the delete
keeps a conflict only for a record other than the vertex (a continuation chunk a concurrent commit is republishing);
a vertex that has vanished gets the same non-retryable answer as everywhere else, and the `ClassCastException` arm
next to it no longer asserts a slot reuse this frame cannot establish.

The second is the append. `GraphEngine.getOrCreateEdgeList` read the head RID *outside* its `try`, so on a vertex
whose record is gone the lazy load escaped raw as `RecordNotFoundException: Record #4:0 not found`. The verdict was
right - it was never retryable - but nothing else was there: it did not say the missing record is a vertex, that it
was an *endpoint* of an edge being created (the common shape is `connectIncomingEdge`, where the target is gone
rather than the vertex the caller named), which side of the list was being written, or what to run. It now raises
the same `VertexNotFoundException`, with the same message shape and the same advice as the removal side.

One catchable type therefore covers the whole surface: removing an entry from an edge list, adding one to it, and
deleting the vertex outright. An application sweeping possibly-stale references catches `VertexNotFoundException`
(or `RecordNotFoundException`) once and skips the entry, instead of matching on messages or on the accident of which
read noticed first. An unreadable *chunk* of a vertex that does exist keeps the retryable
`ConcurrentModificationException` and #5764's scoped advice, on both paths.

[#6586](https://github.com/ArcadeData/arcadedb/issues/6586)
