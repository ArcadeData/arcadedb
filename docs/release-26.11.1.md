# ArcadeDB v.26.11.1 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.11.1 development cycle, so the release notes are ready at tag time.

## Breaking Changes (migration notes)

### A plain `=` / `IN` on a `COLLATE ci` index is case sensitive (#9403)

A `COLLATE ci` index folds its keys, and until now the planner returned every case variant for `name = 'JOHN'` while the
same predicate over a subquery (a scan) returned only the exact-case rows, so the answer depended on whether the planner
picked the index. The index no longer changes the answer: a plain `=` or `IN` still uses the index but is re-checked on
the rows it returns, so `name = 'JOHN'` matches `'JOHN'` only, with or without the index.

**Who sees a different answer.** A query that relied on `name = 'john'` finding `'John'` through a `COLLATE ci` index now
returns fewer rows. Write `name.toLowerCase() = 'john'` (or `name.toLowerCase() IN ['john', 'mary']`) to keep the
case-insensitive lookup; that spelling is served by the index and needs no re-check when the literal is lower case.

### Weighted path finders share one edge-weight rule (#9443)

Every weighted shortest-path finder now reads an edge's weight the same way:

- an edge without the weight property, or whose value is not a number, weighs **1** (the unweighted hop);
- an edge whose weight is **negative, NaN or infinite is not walked**. Negative weights remain the business of
  `bellmanFord()` / `algo.bellmanford`, which are unchanged.

Before, the finders disagreed, so the same query answered different distances and paths depending on the entry point:

| finder | missing weight before | negative weight before |
|---|---|---|
| `dijkstra()`, `astar()`, `algo.dijkstra`, `algo.astar` | **0** (every unweighted edge was free) | **walked** |
| `duanSSSP()` | 1 | **walked** |
| `algo.kShortestPaths`, `algo.steinerTree` | 1 | **walked** |
| `algo.dijkstra.singleSource` | 1 | skipped (a NaN weight was walked) |
| `cchShortestPath()`, `algo.cch.shortestPath` | 1 | skipped |

Walking a negative weight breaks the invariant Dijkstra and A* settle vertices by, so the answer was an arbitrary path
rather than a shortest one; it is now a shortest path over the edges that can be walked.

**Who sees a different answer.** A graph where some of the routed edges have no weight property: `dijkstra()`,
`astar()`, `algo.dijkstra` and `algo.astar` used to treat them as free, and now count each as one hop. The `weight`
yielded by `algo.dijkstra` and `algo.astar` changes accordingly. A graph with negative weights now routes around those
edges; use `bellmanFord()` when they are meant to be walked.

**`duanSSSP()` accepts an edge type filter.** Its fourth argument may now be an options map,
`{ direction, edgeTypeNames }`, like `dijkstra()` and `cchShortestPath()`; a plain direction string still works. It now
answers through the same engine as `cchShortestPath()` (a Customizable Contraction Hierarchy when a Graph Analytical
View keeps one, bidirectional Dijkstra otherwise) and honours the command timeout. Among several paths of the same total
weight it may return a different one than before, since the search is no longer a one-directional Dijkstra.

`algo.steinerTree` also picks the cheapest of several parallel edges for the tree it reports, rather than the first
one the adjacency listed.

**Embedded API.** The `protected static final float MIN` constant of `SQLFunctionHeuristicPathFinderAbstract` is gone:
it was the 0 weight of an edge without one. A custom subclass that referenced it reads the shared rule from
`com.arcadedb.graph.EdgeWeight` instead (`MISSING`, `of(Object)`, `isWalkable(double)`).

### `BACKUP DATABASE` applies the retention policy (#9844)

The SQL `BACKUP DATABASE` statement now prunes the database's archive directory after a successful backup, the way the
control plane's `trigger backup` has since #7472 and the scheduler always has. It writes into the same
`<backup directory>/<database>` they prune, and retention works off what is in the directory rather than what is
registered with the scheduler, so a database that is absent from the auto-backup configuration is pruned by the server
level defaults. Before, repeated statements against such a database grew the directory without bound.

**Who sees a different result.** An installation with auto-backup enabled that runs `BACKUP DATABASE` for a database
beyond the `maxFiles` (or tiered) retention of its effective policy now keeps only that many archives, and the older ones
are deleted after the statement. The archive just written is always kept. Archives that do not follow the
`<db>-backup-<timestamp>.zip` convention, such as one named on purpose with `BACKUP DATABASE 'before-the-upgrade.zip'`,
are invisible to retention and are never deleted. With auto-backup disabled nothing is pruned, as before.

### A MANDATORY or default-valued property cannot reach a LIGHTWEIGHT edge type (#9846)

A lightweight edge allocates no record, so nothing validates it: a `MANDATORY` property was never required and a default
never applied, on a subtype that inherited them from its super type. The type's own `CREATE PROPERTY` already refused any
property on a lightweight type; the same rule now covers the properties that arrive from a super type. These are refused
with a `SchemaException` naming the property and the type:

- `CREATE EDGE TYPE Child EXTENDS Parent LIGHTWEIGHT`, and `ALTER TYPE Child SUPERTYPE +Parent`, when `Parent` declares or
  inherits a `MANDATORY` or default-valued property (a refused `CREATE` leaves no type behind);
- declaring a property `MANDATORY` or giving it a default on a type that has a lightweight edge type below it;
- `ALTER TYPE Child WITH lightweight = true` on a type that declares or inherits such a property.

Optional properties, and properties that only constrain a value that is present (`NOT NULL`, `MIN`, `MAX`, `REGEXP`),
stay legal under a lightweight subtype. A schema written before this check that already holds the combination still
opens; it is only the DDL that is refused now.

### `values()` on a sub-query row lists exactly the values of `keys()` (#9843)

On a `Result` receiver (a sub-query row, a `LET` variable) `values()` used to answer the record's `@cat`, `@type` and
`@rid` (and an edge's `@in` and `@out`) as unlabelled values, so a two-property document had 2 keys and 5 values, and on a
row that overrode a property of its record it answered the record's value instead of the row's. It now answers one value
per key, in the order of `keys()`, and the row's own value where the row has one.

### A bare `ORDER BY @rid` on a MATCH that returns columns is refused (#9911)

When a SQL `MATCH` returns columns (`RETURN a.x AS x, ...`), `$paths` or `$patterns`, each row is a new object built from
the matched aliases and has no `@rid` of its own, so `ORDER BY @rid` sorted on null and silently kept the pattern order.
It now fails at planning time with a message naming the forms that work: `ORDER BY <alias>.@rid` or `ORDER BY` a
returned column (`RETURN n.@rid AS noteRid ... ORDER BY noteRid`). The same applies to the other bare record attributes,
such as `@type`. `RETURN $elements`, `$pathElements` and `RETURN expand(...)` return records, so `ORDER BY @rid` keeps
working there.

**Who sees a different answer.** A query that used a bare `ORDER BY @rid` on such a MATCH, for example as the cursor of a
keyset pagination, now fails instead of returning rows in an order the engine never applied.

### Cypher: a path variable over a quantified group binds one PATH, and ignored GQL constructs are refused (#9940)

`MATCH p = ((m)-[r]->(n)){2}`, a path variable over nothing but a quantified group, used to bind a `LIST<PATH>` with one
path per repetition (#4531). It now binds one `PATH` running through every repetition, as any other path variable does
and as Neo4j Cypher 25 does (`valueType(p)` is `PATH NOT NULL`, `length(p)` is 2). Group variables declared inside the
group (`m`, `r`, `n` here) are still lists. A query that read `p` as a list, for example `size(p)` or `p[0]`, must now
use `length(p)`, `nodes(p)` and `relationships(p)`. The path a `shortestPath()` / `allShortestPaths()` binds is typed
`PATH` too: `valueType(p)` answers `PATH NOT NULL` instead of a LIST type, and `p IS TYPED PATH` is true.

Several GQL / Cypher 25 constructs that the parser accepted and the engine silently ignored are now refused with a
`FeatureNotImplemented` error naming the construct. A query that used them returned rows the construct did not ask for,
and now fails instead:

- the path search prefixes `ANY SHORTEST`, `ALL SHORTEST`, `SHORTEST k [GROUPS]` and `ANY [k]` (use `shortestPath()` /
  `allShortestPaths()`; `ALL` and `ALL PATHS` keep working)
- the match mode `REPEATABLE ELEMENTS` / `REPEATABLE ELEMENT BINDINGS` (`DIFFERENT RELATIONSHIPS` keeps working)
- `x IS DIRECTED` (a label named DIRECTED is written `:DIRECTED` or ``IS `DIRECTED` ``)
- `USE` naming a graph other than the current database
- a quantified group nested inside another, and a variable-length relationship inside a single-hop group

An `ORDER BY` / `SKIP` / `OFFSET` / `LIMIT` written after `RETURN` (`RETURN ... LIMIT 2 OFFSET 1`) is now applied after the
RETURN's sort, as one page: sort, then skip, then limit. It used to run before the sort and could drop the wrong row.
This is a fix that changes results: such a query can now return different rows than before.

`LET`, `IS [NFC|NFD|NFKC|NFKD] NORMALIZED`, `USE` of the current database and the conditional query
`WHEN ... THEN ... ELSE ...` are now executed. As for any query, every parameter a conditional query names must be bound, also
the parameters of the branches that are not taken. A `LET` that names a variable already in scope is refused
("Variable 'n' already declared"), as in Neo4j.

### The `path.*` procedures answer as `apoc.path.*` does (#10025, #10048, #10049)

The Cypher procedures `path.expand`, `path.expandconfig`, `path.spanningtree`, `path.subgraphnodes` and `path.subgraphall`
were checked, option by option, against Neo4j 5.26 with APOC 5.26.30. Where they differed, they now answer as APOC does.

**`path.expandconfig` and `path.expand` return every trail by default (#10049).** The `uniqueness` option of
`path.expandconfig` is read, with APOC's nine values (`NODE_GLOBAL`, `NODE_LEVEL`, `NODE_PATH`, `NODE_RECENT`,
`RELATIONSHIP_GLOBAL`, `RELATIONSHIP_LEVEL`, `RELATIONSHIP_PATH`, `RELATIONSHIP_RECENT`, `NONE`, case-insensitive) and APOC's
default, `RELATIONSHIP_PATH`: a path may pass through a node more than once, but never walks the same relationship twice.
Until now the breadth-first walk was always `NODE_GLOBAL` (each node reached once, along its shortest route) and the
depth-first walk, and `path.expand`, always `NODE_PATH`; the option was ignored. `path.expand` is now APOC's: breadth-first,
`RELATIONSHIP_PATH`. A value that is no uniqueness is refused, where APOC quietly uses its default. `path.spanningtree`,
`path.subgraphnodes` and `path.subgraphall` are `NODE_GLOBAL` whatever the option says, as in APOC.

**Who sees a different answer.** A call on a graph with a cycle or with several routes to a node returns more paths than
before, and the number grows quickly with `maxLevel`. To keep the old answer:

- `path.expandconfig(n, {...})` as it was: add `uniqueness: 'NODE_GLOBAL'`
- `path.expandconfig(n, {bfs: false, ...})` as it was: add `uniqueness: 'NODE_PATH'`
- `path.expand(n, rels, labels, min, max)` as it was: call `path.expandconfig(n, {relationshipFilter: rels, labelFilter: labels,
  minLevel: min, maxLevel: max, bfs: false, uniqueness: 'NODE_PATH'})`

The walk now produces its paths as the query reads them, so a `limit` (or a `LIMIT` after the call) ends it instead of trimming
the answer of a walk that ran to the end, and the branches it holds are charged to the query's heap
(`arcadedb.queryMaxHeapElementsAllowedPerOp` and `arcadedb.queryMaxHeapRAM`): a trail walk too wide for them fails the query
rather than the server. The same cap applies to a breadth-first walk of a very wide component, `path.spanningtree` included:
a level with more branches than `arcadedb.queryMaxHeapElementsAllowedPerOp` (auto-scaled with the heap, never below 500000)
fails the query with a heap-limit error that names the setting; raise it to walk such a component. Whatever the uniqueness,
a walk never leaves a node by the relationship it arrived by, as in APOC.

**New options (#10049).** `endNodes`, `terminatorNodes`, `allowlistNodes` (also `whitelistNodes`) and `denylistNodes` (also
`blacklistNodes`) name nodes where the `>`, `/`, `+` and `-` operators of `labelFilter` name labels, and combine with it: a
node is an answer, and the walk goes on past it, only if every filter says so. They are read by `path.expandconfig`,
`path.spanningtree`, `path.subgraphnodes` and `path.subgraphall`. `optional: true` answers one row with a null path (or node)
when the walk finds nothing. A negative `maxLevel` or `limit` is no bound (APOC's `-1`), where it used to stop the walk at the
start node or fail; `limit` is now read by `path.subgraphnodes` and `path.subgraphall` too (it bounds the nodes).

**`path.subgraphall` answers every relationship between its nodes (#10048).** `relationships` was the list of relationships the
walk followed. It is now every relationship, of any type, between two of the returned nodes, whatever the
`relationshipFilter`: `path.subgraphall(a, {relationshipFilter: 'R>'})` over `(a)-[:R]->(b)`, `(b)-[:S]->(a)` returns the `S` too.
A call that yields only `nodes` is unchanged and still reads no edge record.

**A node a filter turns away is spent (#10049).** In the node-global walks (`path.spanningtree`, `path.subgraphnodes`,
`path.subgraphall`, and `path.expandconfig` with `uniqueness: 'NODE_GLOBAL'`) a node is used up when it is first met, as APOC
does, so one that a depth-dependent `sequence` turned away at a hop is not welcome again at another. A node that is
allowed is unaffected.

**Filter syntax (#10025).** `relationshipFilter` and `labelFilter` read APOC's syntax. Before, the direction markers and the
label operators were read as part of the name, so `'ROAD>'` or `'+Person'` matched nothing, and a comma separated alternatives
rather than the hops of a `sequence`. A relationship type or a label that is not in the schema is refused (Neo4j answers an empty
result), `minLevel` of `path.spanningtree`, `path.subgraphnodes` and `path.subgraphall` is 0 or 1 only, and a `config` that is not
a map is refused.

## Fixes

- **The BM25 corpus counters of a `FULL_TEXT` index move when the transaction commits, on every node (#10018).** They used to be
  bumped while the document was being indexed and never undone, so a rolled-back insert, or one retried after a
  `ConcurrentModificationException`, inflated `totalDocs` and `sumDocLength`, and the next search paid a whole-type scan to
  repair them. A transaction now accumulates its effect and the commit applies it with the record counts. On an HA cluster
  the transaction carries that effect to the other nodes, so replicas no longer score the same query with different corpus
  statistics, and a `REBUILD INDEX ... WITH statsOnly = true` run on the leader reaches the followers (a schema reload on a
  follower keeps its live counters unless the persisted ones carry a newer `ft_countersEpoch`, which a recompute on the leader
  advances). A node that does not advertise `tx-fulltext-corpus-deltas` makes the leader withhold the new `TX_ENTRY` section
  and log a warning: a rolling upgrade behaves as it did until every node is upgraded. In that window the other nodes do not
  count those transactions. The first search of a node repairs a difference in the number of documents; a rewrite that keeps
  the number of documents changes only the total length of the documents, which that check cannot see, so run
  `REBUILD INDEX ... WITH statsOnly = true` on the leader once every node is upgraded.
- **The repair of the BM25 corpus counters made by a search is kept, and the counters can be inspected (#10019).** The repair
  made by the first search is written with the schema, at the next schema save or when the database closes, so it is paid once
  and not after every clean restart. After a crash, or a process that is killed, the pages are restored from the log but the
  counters can lag them until the first search's drift check repairs them, as before. The drift warning names the logical
  index and the exact `REBUILD INDEX` statement to run, and ``SELECT FROM schema:index:`<name>` `` now carries a `bm25` map
  (`totalDocs`, `sumDocLength`, `avgDocLength`, `countersValid`, `staleCheckDone`, `scans`, `epoch`) that reads the counters
  without running the drift check.
- **`vector.sum`, `vector.avg`, `vector.min` and `vector.max` aggregate when written with the dotted name (#9768).** In SQL
  they answered one NULL row per record (`vectorSum(...)` and `` `vector.sum`(...) `` were fine) and in openCypher
  (`RETURN vector.sum(d.v)`) they had no working spelling at all. A dotted call is now a function call exactly when the
  function is registered, in both languages, so the next namespace somebody adds cannot drift again. A property named like
  a namespace keeps its own methods (`vector.size()` on a `vector` column), while a registered name (`vector.max()`) wins
  over the method of a property called `vector`, as it already did at run time; back-tick the property to address it.
- **A map with a non-String key renders as JSON (#9805).** A `MAP` property accepts an Integer key and a Gremlin
  `elementMap()` is keyed by the `T` enum, but `Result.toJSON()`, `convert.toJson()`, `asJSON()` and the Postgres wire threw
  `ClassCastException` for such a row (and a `null` key an `IllegalArgumentException`). The member name is now the text of
  the key, a `null` key being named `"null"`, as the HTTP serializer already did. `JSONObject(Map)` takes any `Map<?, ?>`.
- **Redis `HDEL` replies the number of records it removed, and a multi-key `HDEL` / `HSET` is one unit of work (#9837).** A
  delete a `BeforeRecordDeleteListener` vetoed was counted as a deletion on every branch. On the Redis protocol, `HDEL` by
  index key left the first keys deleted behind an error reply and was not retried on a conflict, and the transient `HSET`
  left the first JSON objects stored behind an error reply. A new `DatabaseInternal.deleteRecordReportingVeto(Record)` keeps
  the answer `Record.delete()` throws away, for any caller that counts deletions.
- **A datetime written through the line protocol or gRPC is stored, or named and dropped (#9809).** A quoted datetime sent to a
  `DATETIME`, `DATE`, `DATETIME_SECOND`, `DATETIME_MICROS` or `DATETIME_NANOS` field failed the whole write with a raw
  `ClassCastException` wrapped in `IOException: Failed to append timeseries samples`, answered as a server error, while the same
  text through `INSERT` was accepted. The gateway all those writers share now reads the value through the conversion `INSERT`
  uses (a number stays a count in the column's unit, text of digits is that count, any other date or instant is read as SQL
  reads it), parses text sent to a numeric column or to a typed `BOOLEAN` tag (a tag sent as `"true"` used to be stored as
  `false`), and drops a sample whose value its column cannot store instead of failing the request (which was answered with a
  server error, `500`; the request is now answered `400` like any other partial write, and the rest of it is stored, so a client
  that ignores `dropped` and `unreadableValues` loses those samples without noticing). The dropped sample is
  reported like an undeclared key: the HTTP `400` carries `unreadableValues` (`<measurement>.<column> (tag|field): <reason>`,
  an additional member written only when it is not empty, so a client parsing the body today is not affected),
  the gRPC `TimeSeriesWriteSummary` has the new `unreadable_values` field and `TimeSeriesWriteSummary.unreadableValues()` on
  the Java clients. The engine API names the column too, instead of a `ClassCastException`. A `BOOLEAN` column keeps a
  boolean and reads `true` / `false` in any case; any other text or a number is refused instead of being stored as `false`, so
  a client that sends `1` or `0` for a boolean field now has those samples reported in `unreadableValues` and not stored.
- **The remote driver keeps `@rid`, `@type`, `@in` and `@out` columns of a projection (#9804).** `SELECT @rid FROM Person`
  over `RemoteDatabase` answered a row with no columns, and a Gremlin `elementMap()` lost the `@rid` and `@type` it names its
  tokens with: the driver removed every metadata name from any row carrying the `@props` type hint, which a `@rid` column
  arms by itself. A row without `@cat` is a projection and its column names belong to the query; only the hint is consumed.
- **`arcadedb.grpc.compression.enabled` is applied (#9782).** It was read and printed in the startup line, but both
  compression registries were installed whatever it said and `compression.force` did not consult it. Off, the endpoint
  refuses a compressed request (`UNIMPLEMENTED`) and never compresses a response; `compression.force=true` with
  `enabled=false` is ignored with a warning, and the startup line says `compression: disabled`. A deployment that set
  `enabled=false` and relied on it being ignored must either remove it or stop sending compressed requests.
- **A map nested in a Gremlin result is named like the row (#9781).** `project('x').by(elementMap())`, `group().by(..).by(elementMap())`,
  `elementMap().fold()` and `path().by(elementMap())` kept `T.id` and `T.label` as `id` and `label`, so a property called `id`
  replaced the record id, an edge's endpoints disagreed with the edge, and keys that print alike (the Integer 1 and the Long 1)
  were merged. The names of the row (`@rid`, `@type`, `1:Integer` / `1:Long`) now apply to every map a result holds, in a list,
  a set or a `Path` (which stays a `Path`), so the embedded `Result`, its JSON and the HTTP answer agree. A nested map that
  used to render `id` / `label` for the tokens now renders `@rid` / `@type`.
- **A MATCH node with `rid:` honours its `type:` and `bucket:` (#9912).** `{type: Person, rid: #12:0}` returned `#12:0`
  whatever its type, and as the first node of a longer pattern on a large type it could ignore the `rid:` and start from every
  record of the type. The record is now fetched by RID and must match the type (subtypes included) and the bucket, or the
  node matches nothing. A node bound by `rid:` is the preferred starting point of the pattern; `type:` with `bucket:` applies
  both on the starting node too; a `rid:` computed from another alias (`{"@rid": $matched.a.ref}`) is resolved after that alias.
- **A MATCH node filtered by `SEARCH_INDEX()` is streamed (#9908).** It was loaded entirely before the pattern started,
  whatever the `LIMIT`, because the planner read the function's "cannot estimate" as "very few rows" (geospatial predicates
  too). It is now streamed, still preferred as a starting point over a scan of a large type. An equality on an indexed
  property now also helps MATCH choose its starting point (the index count used to be computed and discarded), so
  `SEARCH_INDEX(...) AND code = 'X'` is bounded by the index on `code`.
