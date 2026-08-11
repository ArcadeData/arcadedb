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
