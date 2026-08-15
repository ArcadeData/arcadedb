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
