# ArcadeDB v.26.10.2 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.10.2 development cycle, so the release notes are ready at tag time.

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

### openCypher temporal values are stored natively (#8572, #9462)

openCypher used to persist `time()`, `localtime()`, zoned `datetime()` and `duration()` as Strings, and guessed the
type back from the text when it read an undeclared property. A String such as `"09:00-17:00"` therefore read back as
a time, and `WHERE l.hours = '09:00-17:00'` matched nothing. These values are now stored natively, in four new
property types: `OFFSET_TIME`, `LOCAL_TIME`, `ZONED_DATETIME` and `DURATION`. A String is never reinterpreted.
`LSM_TREE` and `HASH` indexes support the new types. `ZONED_DATETIME` and `OFFSET_TIME` are indexed by instant.

**Who has to change anything:**

- **A database written by an older release that holds Cypher temporals in undeclared properties.** Those values are
  Strings on disk and now read back as Strings. Declare the property with the new type and copy the values through
  a parked property. `UPDATE T SET p = p` does not work: once `p` is declared the read already converts it, so the
  value looks unchanged and the write is skipped.

  ```sql
  BEGIN;
  UPDATE Place SET hours_old = hours;
  UPDATE Place REMOVE hours;
  COMMIT;

  CREATE PROPERTY Place.hours OFFSET_TIME;

  BEGIN;
  UPDATE Place SET hours = hours_old WHERE hours_old IS NOT NULL;
  UPDATE Place REMOVE hours_old;
  COMMIT;
  ```

- **A zoned `datetime()` written to a `DATETIME` property** is stored as an instant and reads back as the UTC wall
  clock. Before, it kept the local wall clock.

### SQL `UPSERT` requires a UNIQUE index (#9339, #9342)

`UPDATE ... UPSERT WHERE` without a sub-query accepted any index, so an upsert matched through a non-unique index could
silently create duplicates. It now requires an equality on every property of a UNIQUE index, as the sub-query form
already did. A non-unique index, a partial match of a composite key, or a range, `IN` or `OR` condition is refused,
and the error now states the real requirement.

### SQL `sum()` over an empty or all-NULL group returns NULL (#9351, #9384)

SQL `sum()` now answers NULL for an empty or all-NULL group, like `avg()`, `min()`, `max()` and the aggregation
push-down. It used to answer 0. Cypher `sum()` keeps returning 0, as the openCypher specification requires.

### SQL `k = <list>` no longer expands the list (#9370, #9385)

An index lookup expanded a list on the right of `=` into one lookup per element, so `k = [1, 2]` behaved like
`k IN [1, 2]`. Only `IN` and the other multi-value operators expand a list now. `=` and the range operators compare
the field with the list as a value, which matches nothing on a scalar field, so `=` and `<>` stay complements. Use
`IN` to match any element of a list.

### Time-series writes refuse what they used to store silently (#8647, #8646, #9365, #9310, #9323)

- **Empty tag values (#8647, #9364).** A line-protocol, gRPC or remote write whose tag has an empty value
  (`cpu,host= value=1`) used to store `host=""`. It is now refused with HTTP 400 / `INVALID_ARGUMENT`, as InfluxDB
  refuses it. To write a sample without a tag, leave the tag out.
- **Undeclared tag or field keys (#8646, #9366, #9365, #9384).** A sample carrying a tag or field key the type does
  not declare in that role (for example a misspelled tag) used to be stored without that key. It is now dropped and
  reported as a partial write: HTTP 400 with `dropped > 0` and `undeclaredKeys`, or gRPC
  `TimeSeriesWriteSummary.undeclared_keys`. SQL `INSERT INTO <timeseries type>` with an undeclared property fails the
  statement, and Prometheus remote write drops the series carrying an undeclared label (the valid series of the batch
  are kept). A sample that simply omits a declared key is still accepted. The new database setting
  `arcadedb.timeSeriesUndeclaredKeys` (`reject` by default, or `ignore`) restores the old behavior for schema-less
  producers such as Telegraf.
- **Prometheus empty label values (#9363, #9384)** are absent labels: stored as null, never counted as undeclared,
  and selected by `{label=""}`, including rows already stored with `""`.
- **Codecs (#9310, #9384).** A TIMESERIES column codec the storage layer cannot encode or round-trip for the column
  type is refused at `CREATE`. Types already persisted keep loading, and an import falls back to the default codec.
- **Truncated line protocol (#9323, #9392).** A line cut right after a tag or field `,` separator is refused.

### SQL classifies a statement by everything it runs, nested statements included (#9628, #9687)

SQL decided whether a statement was a read, a write or a DDL from its top-level node only. A write placed in a `LET`,
a projection, a `FROM` sub-query, a `CASE`, a `RETURN` or a script block was therefore reported as a read, and it ran
durably through the read-only paths: `LET $x = (INSERT ...)`, `SELECT ... LET $y = (DELETE ...)` and
`SELECT FROM (CREATE TYPE ...)` all went through `GET`/`POST /api/v1/query`. A statement now counts everything nested
inside it.

**Who has to change anything:**

- **A script that hides a write inside a read.** `Database.query()`, `GET`/`POST /api/v1/query` and MCP identities
  without write permission now refuse it. Send it as a command (`Database.command()`, `POST /api/v1/command`).
- **A DDL inside `IF`, `FOREACH` or `WHILE`.** These blocks now report what their bodies do, so a DDL inside one needs
  `allowSchemaChange` on MCP, the same as the DDL on its own.

`EXPLAIN` stays read-only, as before. `INSERT ... FROM SELECT` and `MOVE VERTEX` are classified as before: a nested
read adds nothing to a statement that already writes.

### openCypher `MERGE` matches exactly what `MATCH` matches (#9693, #9696)

`MERGE` compared its candidates with its own equality, which disagreed with `MATCH` in two ways. It now uses the
comparison `MATCH` applies to an inline property, for nodes and relationships, with and without an index, and in the
`merge.relationship` procedure:

- **A stored `FLOAT` is found (#9616).** `MERGE (n:T {v: 0.1})` on a node stored with `FLOAT` `0.1` used to miss it,
  so it created a duplicate, or raised a duplicated-key error under a UNIQUE index. `MATCH` found it all along.
- **A temporal against the text of a `STRING` property (#9617).** `MERGE (p:S {d: datetime(...)})` on a property
  declared `STRING` used to match the stored text, and ran `ON MATCH SET` on a node no `MATCH` could find. `MERGE` and
  `MATCH` now agree: both find the stored text, as described in the next entry (#9695).

### openCypher patterns find what they stored in a declared `STRING` property (#9695, #9700)

A property declared `STRING` stores the text of whatever a pattern writes into it, so `MERGE (n:S {v: 1})` stores
`'1'`. No later `MATCH` or `MERGE` with the same pattern found it, so every repeated `MERGE` created a duplicate, or
raised a duplicated-key error under a UNIQUE index. Now, on a property declared `STRING` (directly, on a supertype, or
by Cypher DDL such as `IS NOT NULL`, `NODE KEY` or `IS TYPED`), a stored text also equals a non-text value when it is
exactly the text a write of that value would store. This applies to inline patterns, `MERGE` (with and without an
index), `merge.relationship`, `=`, `<>`, `IN` and joins.

**Who sees a different answer.** A query against a declared `STRING` property that compares it with a number, a
boolean or a temporal: `MATCH (n:S) WHERE n.v = 1` now finds the node holding `'1'`. A property with no declaration
keeps strict Cypher typing, so `'1' = 1` stays false there. NaN never matches. Ordering (`<`, `>`), `STARTS WITH` and
`CONTAINS` do not coerce. `n['v']`, `properties(n).v`, chained property access and the simple `CASE n.v WHEN ...` form
compare the same way (#9701, #9732).

The same rule now covers the other declared scalar types (#9794, #9820): a property declared `INTEGER`, `LONG`,
`FLOAT`, `DOUBLE`, `BOOLEAN`, `DATE` or `DATETIME` stores the value a written text converts to, and a pattern or `=`
with that text finds it again. `CREATE PROPERTY I.p INTEGER` followed by `MATCH (n:I) WHERE n.p = '5'` finds the node
holding 5, and repeated `MERGE` no longer duplicates it. A text the declared type cannot read (`'five'`) matches
nothing. This includes the `INTEGER` declaration openCypher `CREATE INDEX` infers from existing data (#9935).

### `DROP CONSTRAINT <name>` drops what the constraint created (#9646, #9707)

`DROP CONSTRAINT <name>` on a named Cypher constraint did nothing, leaving its unique index or NOT NULL flag in place.
Each named constraint now records what its `CREATE CONSTRAINT` created, and `DROP` removes exactly that. Anything
declared before it (a SQL `MANDATORY` flag, an existing unique index), or still needed by another named constraint on
the same type, is kept. Creating a constraint under a name already in use now raises an error (`IF NOT EXISTS` makes it
a no-op), and dropping an unknown name raises an error unless `IF EXISTS` is given. Dropping by index name still works.

**Who has to change anything:** a named constraint created by an older release has no record, so
`DROP CONSTRAINT <name>` now raises for it. Drop it by its index name, or through SQL. Cypher `CREATE INDEX <name>`
keeps its name, so `DROP INDEX <name>` drops it (#9705, #9742). Dropping a named `IS TYPED` constraint lifts the
declared type, and `SHOW CONSTRAINTS` / `SHOW INDEXES` list what exists (#9706, #9732).

### HA snapshot ship compresses at DEFLATE level 1 by default (#9295, #9296)

A follower's full-snapshot resync was bound by one leader thread compressing at the JDK default level (6), and a
second follower waited on a per-database lock for the whole transfer. The new setting
`arcadedb.ha.snapshotCompressionLevel` (`-1` to `9`, `0` ships the files stored) defaults to **1**, the fastest
level, and the lock no longer covers the transfer, so two followers are served concurrently (bounded by
`arcadedb.ha.snapshotMaxConcurrent`). A resync over a WAN or metered link now sends more bytes. Set
`arcadedb.ha.snapshotCompressionLevel=6` (or `-1`) to restore the previous behavior.

### HA: a follower restarted in place is no longer reformatted automatically (#8901, #9001)

A follower whose Raft layer was restarted in place, and which has since taken no replicated entry and seen no new
leader, is no longer reformatted automatically when it looks stuck at a stale term: that signature also matches a
dead replication path, where a reformat does not help. The node keeps raising the `follower-stuck-at-stale-term`
alert, logs a WARNING every 5 minutes, and must be restarted by hand. A node that was never restarted in place keeps
the #4741 self-heal. See also the new `GET /api/v1/cluster` fields below (#9399).

### Graph Analytical View over a subset of vertex types (#9301, #9320)

A view created with `VERTEX TYPES` over only some of the vertex types was chosen by edge type alone, so SQL
`out()`/`MATCH` and Cypher `MATCH` answered 0 for edges leaving the covered types. Such a partial view no longer
accelerates count push-downs, SQL traversal, shortest paths or `algo.*` procedures, because an edge type declares
no endpoints and the walk cannot be proven to stay inside the view. A view covering every vertex type is unaffected.

### `GraphBatch.withLightEdges(true)` needs a `LIGHTWEIGHT` edge type (#9378, #9383, #9398)

`GraphBatch` with the deprecated `withLightEdges(true)` flag now throws for a property-less edge into an edge type
that does not declare `LIGHTWEIGHT`, instead of creating light edges in a type that does not expect them. The batch
HTTP endpoint answers 400. Declare the edge type `LIGHTWEIGHT`.

### Gremlin Server refuses a channelizer that cannot hide error details in production mode (#9639, #9750)

Production-mode error concealment on the Gremlin Server (#9317) only covered TinkerPop's default
`WebSocketChannelizer`. Any other channelizer (`WsAndHttpChannelizer`, `HttpChannelizer`, the deprecated
`UnifiedChannelizer` or a custom class) logged one WARNING at startup and then sent engine error text to clients,
stored duplicated-key values included. With `arcadedb.server.mode=production`, such a channelizer, whether set by
`channelizer:` in `gremlin-server.yaml` or by `gremlin.channelizer`, now **fails the start** with an error naming the
channelizer. Outside production mode it still starts, with a WARNING that production mode will refuse it.

**Who has to change anything:** a production server configured with a non-default channelizer does not start after
the upgrade. Remove the `channelizer` entry, or set it to
`org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer`. A server started in development mode and switched
to production at runtime is refused while the Gremlin Server runs such a channelizer (#9749, #9759).

### HA: a follower's leader refusal is a redirect on Bolt, MongoDB and Redis (#9845, #9869)

A write sent to a follower is refused with the leader's address. Bolt, MongoDB and Redis reported that refusal as a
retryable conflict (`DeadlockDetected`, `112 WriteConflict`, `TRYAGAIN`), so drivers retried in place against the same
follower. A refusal that names the leader is now a redirect:

| protocol | before | now |
|---|---|---|
| Bolt | `Neo.TransientError.Transaction.DeadlockDetected` | `Neo.ClientError.Cluster.NotALeader` |
| MongoDB | `112 WriteConflict` | `10107 NotWritablePrimary` |
| Redis | `TRYAGAIN` | `READONLY` |
| PostgreSQL | `40001` | `40001` (the protocol has no redirect code) |

All four now name the leader in the error text, in production mode too. Bolt names the leader's Bolt address from the
routing table when it knows it, and the HTTP address otherwise. During an election, when no leader is known, the
refusal keeps its old retryable answer, as HTTP does.

**Who has to change anything:** a `bolt://` (not `neo4j://`) or plain MongoDB client no longer retries a leader
refusal automatically; send writes to the leader, or use a routing driver. A Redis client that handled `TRYAGAIN`
must handle `READONLY`.

The `/ws` insert endpoint follows the same rule (#9868, #9931). A refused row, a `per_row` row's commit and a
`per_batch` chunk's commit now report the code `NOT_LEADER` instead of `DB_ERROR`, with a new `leaderAddress` field kept
outside the message so production mode does not hide it. A refused `per_stream` commit frame answers "Not the leader"
with the same field instead of "Internal error". A client that matched `DB_ERROR` to retry must handle `NOT_LEADER`.

The gRPC streaming inserts (`InsertBidirectional`, `InsertStream`, `BulkInsert`) follow it too (#9930, #9945). A refused
row and a refused chunk or stream commit now report `InsertError.code = NOT_LEADER` instead of `DB_ERROR` (or
`COMMIT_FAILED` on `InsertStream`), with the leader's address in the new `InsertError.leader_address` field (proto
field 5, wire compatible). A client that matched `DB_ERROR` or `COMMIT_FAILED` to retry must handle `NOT_LEADER`.

### SQL `include()` and `exclude()` return a map (#9840, #9873)

`include()` and `exclude()` built a new record and gave it the source record's identity, so calling `save()` on a
projection overwrote the stored record with the projected properties and orphaned its edges. They now always return a
map:

- the map carries no `@rid`, `@type` or `@cat` unless one of the names is a metadata field
  (`exclude('@cat', 'secret')` answers `@rid`, `@type` and the remaining properties);
- projections work on edges, which used to fail with "Cannot copy an edge";
- `UPDATE V SET copy = @this.exclude('secret')` stores the projection as an embedded map. Before, it stored a link
  back to the record itself;
- a chained access on a projection reads the map, so `@this.include('n', 'm').m` works, but a traversal such as
  `.out()` must come before the projection;
- `Map` and `Result` elements of a collection are projected one by one instead of being dropped.

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

### An ISO duration text is read as Neo4j reads it (#10055)

`duration('<text>')`, a `DURATION` property set from text and a gRPC value tagged `duration` parse the ISO 8601 form with the
grammar Neo4j applies. Refused now, where they used to be accepted:

- a text with no component, `P`, `PT`, and a `T` with no time component after it, `P1YT`, which were read as a zero
  duration (or as `P1Y`);
- a malformed number, such as `P.5Y`, `PT1.S` or `P-Y` (the last one used to be reported as an overflow);
- a fraction on any component but the last one present: `P1.5Y2M` is refused, `P1.5Y` and `P1Y2M1.5W` are accepted.

Newly accepted, as in Neo4j: a sign before the `P` (`-P1Y2D` negates every component, `+P1Y`), a sign on a component
(`PT+1S`) and a decimal comma (`PT1,5S`). The text a stored `DURATION` is written as is unaffected.

### Embedded API changes

- **A variable-length Cypher hop whose consumer cannot tell two paths between the same pair of vertices apart reports each end vertex once (#10062).**
  `MATCH (a)-[:LINK*1..D]->(b) RETURN DISTINCT b` and `count(DISTINCT b)` walked every path, so the time grew with the number of
  paths (about 3x to 8x per extra level on a dependency-like graph of 200,000 vertices and 1.8 million edges, where depth 9
  did not finish in 120 s), while `TRAVERSE ... MAXDEPTH` answered the same question in constant time per level. The hop now
  walks a breadth-first search with a visited set, lazily and level by level, so a `LIMIT` stops it early, and the same closure
  at depth 12 takes about half a second. It applies to a statement that is one non-optional `MATCH` with one hop followed by
  `RETURN`, with no named path, the relationship variable unused and a lower bound of 0 or 1, whose `RETURN` is `DISTINCT`
  over the start and end vertices or their properties, or groups by them and aggregates them with `count`, `collect`, `sum` or
  `avg` of `DISTINCT` values, or `min` / `max`. The answers are those of path enumeration in every direction and path mode (an
  undirected `TRAIL` pattern with a lower bound of 1 reports the start vertex only when a cycle of at most the upper bound
  passes through it). `EXPLAIN` shows `[DISTINCT ENDPOINTS]`. A plain `RETURN b`, `count(b)`, `size(r)`, a second `MATCH`, a
  `WITH` or a lower bound of 2 or more keep enumerating paths. The order of the rows (and of a `collect(DISTINCT ...)` list),
  which was never specified, now follows the breadth-first order.
- **`Schema.alterMaterializedView()` (#8726, #9381)** on an embedded database now refuses a null mode and a negative
  interval (`IllegalArgumentException`) and an interval on a MANUAL or INCREMENTAL mode (`SchemaException`). It used
  to store them. SQL `ALTER MATERIALIZED VIEW` is unaffected. `RemoteSchema.alterMaterializedView()` is now
  implemented: it sends the equivalent `ALTER MATERIALIZED VIEW` statement instead of throwing
  `UnsupportedOperationException`.
- **`Schema.unregisterFunctionLibrary()` (#8879, #9423)** of a stored (js, sql, cypher) library is now saved to
  `schema.json` and replicated. Calling it directly on an HA follower now throws the "schema changes need the leader"
  error, like every other programmatic DDL, instead of silently diverging that follower.
- **Closed component files (#9306, #9445).** `getSize()`, `getTotalPagesFromChannel()`, `calculateChecksum()` and
  `readPage()` on a file closed on purpose throw `IllegalArgumentException("... is closed")`, like `read` and
  `readPages` already did. They used to throw a `NullPointerException`.
- **Schema setters on an HA follower (#9747, #9765).** Calling a `Property` setter, `Schema.setExtension()`, a schema
  date setting or a type setter such as `EdgeType.setLightweight()` directly on a follower now throws the "schema
  changes need the leader" error, like every other programmatic DDL, instead of changing that follower alone. A named
  `GraphAnalyticalViewBuilder.build()` on a follower therefore fails when it persists the view.
- **`Schema.setZoneId()` checks `UPDATE_DATABASE_SETTINGS` (#9747, #9765)**, like `setDateFormat()` and
  `setDateTimeFormat()`. A restricted embedded user without that permission is now refused.

### Behavior changes worth knowing

- **Unbounded time-series reads stream (#9420, #9421).** `SELECT FROM <timeseries type>` without `LIMIT`, and the
  gRPC time-series stream without a row limit, now stream rows instead of loading the whole range first (a 50M-point
  type no longer runs out of heap). A retention pass during such a read gives a shorter answer, since the removed
  rows are gone. A downsample during it fails the read with a retryable error (HTTP 503 / gRPC retry category),
  possibly after some rows were sent.
- **LSM index name after compaction and `RENAME TYPE` (#9213, #9435).** A compacted bucket index now keeps its name
  across a restart and an HA schema reload. After `RENAME TYPE`, a restart keeps the bucket-index name from before the
  rename, as the live server and HA followers already did.
- **HA vector index build (#8905, #9431).** On a replicated database, a vector index build on a node whose graph was
  not loaded no longer builds the graph during the build: the first search builds it, as every other node already
  does. Each bulk-load chunk is capped at half the replicated entry size (at most 64MB), so a build no longer fails
  with an oversized Raft entry.
- **PostgreSQL `statement_timeout` is enforced (#9329, #9394)** instead of stored and ignored. The earlier of it and
  `arcadedb.command.timeout` wins.
- **gRPC writes whose answer was lost are no longer retryable (#8822, #9470).** When the server answers UNAVAILABLE
  with no further detail, an admin write through `RemoteGrpcServer` or a streaming write (`ingestStream`,
  `ingestBidi`, `timeSeriesWriteStream`, graph batch loads) now throws a non-retryable `RemoteException`. It used to
  throw `NeedRetryException`, but the server may already have applied the write, or part of a stream. A retry could
  apply it twice. Reads and `validate_only` dry runs stay retryable. A client-side deadline on these writes and on
  commit is still a `TimeoutException`, and its message now says that the write may already have been applied.
- **Streamed query errors in production mode (#8899, #9475).** On the query NDJSON stream, the in-band
  `error.message` is now the classified label, as in the buffered response body, not the raw exception text. The
  query error line also gains `error`, `requestId`, `status`, `detail` (outside production mode only) and
  `retryAfter` (integer seconds, for a 503 or a 409 for a request still in flight). The batch error line gains
  `status` and `retryAfter`.
- **openCypher label expressions on variable-length relationships (#9117, #9477).** Expressions such as `[:!R*]`,
  `[:%*]`, `[:(R|S)&!T*]` and `shortestPath((a)-[:!R*]-(b))` now run. Before, they were refused at parse time. An
  expression that needs a predicate (`!`, `%`, or `&` mixed with `|`) is checked at every hop, so it does not get
  the faster `VarLengthExpand` path. `[:!R*]` names no required type, so it expands over every relationship type.
  A plain type list such as `[:R|S*]` is unchanged and keeps the faster path.
- **HA commits fail fast on a closed state machine (#9411, #9511).** A server whose local Raft state machine is
  closed and not yet restarted now refuses commits immediately with a retryable `NeedRetryException` ("its Raft state
  machine is closed and has not been restarted yet"). Before, each commit waited for the full quorum timeout. This
  also applies during the short window of an in-place Ratis restart. The "releasing entry unpublished" warning is
  now logged at most once a minute per database, with the number of releases it stands for.
- **HA replication limits come from the server configuration (#9430, #9497).** The vector bulk-load chunk size, the
  time-series HA compaction guard and the sealed-slice budget are now sized against the server's configuration, the
  same source as the Raft entry cap. A limit such as `arcadedb.ha.appendBufferSize` or
  `arcadedb.ha.tsMaxSealedInlineSize` set only in `server-configuration.json` used to be ignored by these paths.
- **PromQL `bool` is a keyword after an operator (#9644, #9722).** As in Prometheus, `bool` right after a binary
  operator is now read as the comparison modifier (see New Features), so a metric literally named `bool` can no longer
  be the right operand: `a + bool` and `a > bool` are parse errors. `boolean_metric`, `bool:rule` and `bool` as the
  left operand are unaffected. `bool` after a non-comparison operator fails with "bool modifier can only be used on
  comparison operators".
- **SQL `INSERT` into a time-series DATETIME column with a number (#9643, #9726).** A numeric literal is read the way
  a document property of the same type reads it: epoch milliseconds for `DATETIME`, epoch days for `DATE`, so both
  round-trip unchanged. For `DATETIME_SECOND`, `DATETIME_MICROS` and `DATETIME_NANOS` the number is also read as epoch
  milliseconds and stored in the column's unit, so it does not read back as the same number. Pass a date string or a
  value in milliseconds. An instant the column's precision cannot hold (a far-future `DATETIME_NANOS`) is refused
  instead of clamped, for document properties too (#9725, #9730).
- **openCypher reads a `DATE` as a date under any `arcadedb.dateImplementation` (#9647, #9704).** With a non-default
  `dateImplementation` (`java.util.Date`, `Calendar`, `LocalDateTime`), a property declared `DATE` read back as an
  instant, so `RETURN n.d` gave a datetime and comparisons with `date()` failed. It now reads back as a Cypher date.
- **openCypher `IS :: <type>` refuses an attribute the new type cannot use (#9645, #9708).** Re-typing a property
  copied every attribute of the old declaration, so a STRING `REGEXP` or `MAX` length landed on a `LONG`. Attributes
  that do not depend on the type (`MANDATORY`, `NOTNULL`, `READONLY`, `HIDDEN`, ...) are always kept. `OF`, `REGEXP`,
  `MIN`/`MAX` and `DEFAULT` are kept only when the new type reads them the same way. Otherwise the statement is refused
  with an error naming each attribute, and the old declaration is left unchanged. Drop the attribute first. A
  `DOUBLE`/`FLOAT` `DEFAULT` of NaN or Infinity is still refused with a raw number-format error rather than the named
  one. `IS :: LIST<T>` is supported (#9709, #9732).
- **PromQL refuses a comparison between two scalars without `bool` (#9723, #9744).** `1 > 0` used to answer 0 or 1.
  As in Prometheus, it now fails with "comparisons between scalars must use BOOL modifier" (HTTP 400 `bad_data` on
  `/prom/api/v1/query` and `/query_range`, an error in SQL `promql()`). The check applies to any scalar operand, such
  as `abs(-1) > 0` or `(1 > bool 0) == 1`. Write `1 > bool 0`.
- **PromQL refuses a set operator with a scalar operand (#9743, #9787).** `and`, `or` and `unless` with a scalar on
  either side answered NaN. As in Prometheus, they now fail with `set operator "and" not allowed in binary scalar
  expression` (HTTP 400 on `/query` and `/query_range`, an error in SQL `promql()`), also when the scalar is computed
  or the operator is nested in a larger expression. Set operators between two vectors are unchanged.
- **Prometheus remote write and read check what they are sent (#9624, #9755).** A remote-write 2.0 body used to
  decode to zero series and be answered 204, so the sender counted the batch as delivered and dropped it. Both
  endpoints now read the `proto=` parameter of the `Content-Type` and answer **415** for a message they do not
  implement, which makes a 2.0 sender fall back to 1.0. A non-empty write body carrying no remote-write 1.0 field is
  answered **400** instead of 204. A request carrying only metric metadata, and an empty request, are still 204. A
  malformed or truncated protobuf body is answered 400 instead of 500 on both endpoints.
- **SQL reads time-series `DATE` and `DATETIME` columns as dates (#9725, #9730).** `SELECT`, `min()`/`max()` and
  `GROUP BY` results return them as the same Java type a document property of that type returns, instead of raw epoch
  numbers, so HTTP and Studio JSON shows date strings where it showed numbers. The engine API, line protocol, gRPC and
  the HTTP time-series endpoints still use epoch numbers.
- **A `REGEXP` no value of the property's type can match is refused (#9853, #9871).** A pattern on a number or a
  temporal property, such as `[A-Z]+` on a `LONG` or `DATE`, or `[0-9]{5}` on a `BYTE`, was accepted and then refused
  every write. `CREATE PROPERTY`, `ALTER PROPERTY` and `Property.setRegexp()` now refuse it. A pattern the check cannot
  decide within its budget is accepted with a WARNING naming the property and the `ALTER PROPERTY ... REGEXP null`
  fix. A database whose schema already holds such a pattern still opens, so it can be repaired. A few patterns no
  real value matches are still accepted silently (for example `'--'` on a `LONG`).
- **Nested projection prefix wildcards (#9841, #9872).** `@this:{s*}` returned a null column named `s`. It now selects
  every property whose name starts with `s`, on records, maps and plain result rows, and honours excludes and nested
  expansion in the same braces. Two forms that used to return null or nothing now fail with a parse error: an alias on
  a wildcard (`{se* as k}`) and a wildcard after anything but a plain property name (`{a.b*}`, `{!a.b*}`).
- **Lightweight edges carry their metadata (#9839, #9874).** A lightweight edge read from the graph returned `{}` from
  `toMap(true)`, so `include()`, `exclude()` and result maps saw no `@rid`, `@type`, `@in` or `@out`. Both lightweight
  edge classes now answer the same keys as a record-backed edge when metadata is asked for, and nothing otherwise. On
  the HTTP `graph` and `studio` serializers a lightweight edge's `p` object now carries those keys, as a record-backed
  edge's already did. A new lightweight edge put into a `JSONObject` renders `{}`.
- **`/ws` insert sessions are listed and can be terminated in every mode (#9838, #9876).** A `/ws` insert chunk showed
  up in `list queries`, and could be stopped with `terminate query`, only under transaction mode `none`. Every chunk
  is now registered, including under the default `per_stream`. Terminating a chunk undoes what its mode allows
  (`per_stream` rolls back the whole session, `per_batch` the chunk, `per_row` keeps the rows already committed),
  closes the session, and sends the client an error frame saying so; the connection can open a new session.
- **gRPC streaming loads are listed and can be terminated (#9875, #9933).** `InsertStream`, `InsertBidirectional`,
  `GraphBatchLoad` and `TimeSeriesWriteStream` now appear in `list queries` from their first chunk to the end of the
  stream, and `terminate query` stops them, also while idle between chunks. A terminated load ends with `CANCELLED`
  and undoes what its mode allows: an insert rolls back its open transaction (`per_stream`, `per_request`) or batch
  (`per_batch`; earlier batches stay), rows committed under `per_row` stay, a load inside a client transaction rolls
  that transaction back (a later commit answers `committed=false`), a graph batch drops its buffer, and a time-series
  stream stops appending. The unary `BulkInsert` can now also be stopped between rows.
- **PromQL label matchers read typed tags the same way (#9833, #9880).** Only `=` read its value in the tag's declared
  type, so `!=`, `=~` and `!~` could disagree with it on a non-string tag, and `/series` compared even `=` as text. All
  four matchers now share one rule on `/query`, remote read and `/series`, so `=` and `!=` always select complementary
  sets. One deliberate difference from Prometheus: `=~"007"` selects a `LONG` tag holding 7. A `DATETIME` matcher is
  now read the way `INSERT` reads it rather than in the JVM time zone, and so is the HTTP and gRPC JSON tag filter
  (#9879, see Bug Fixes).
- **PromQL refuses a range vector or a string as an operator operand (#9788, #9884).** `m[5m] + 1`, `m[5m] and m` and
  `"a" + 1` answered a scalar NaN, and `-m[5m]` returned the range vector unchanged. They now fail with Prometheus'
  error messages.
- **PromQL refuses a function or aggregation argument of the wrong type (#9883, #9923).** `sum(m[5m])`, `sum(1)`,
  `abs(m[5m])`, `abs("a")` (and `ceil`, `floor`, `round`) and a `round` precision that is not a scalar answered an empty
  or unchanged result. They now fail with Prometheus' error messages (HTTP 400 `bad_data`, an error in SQL `promql()`).
  A wrong argument count is refused too: `abs()` used to fail with `NoSuchElementException`, and extra arguments were
  ignored.
- **PromQL `abs`, `ceil`, `floor` and `round` refuse a scalar argument (#9924, #9947).** `abs(-1)` used to answer the
  scalar 1. As in Prometheus, these functions now accept only an instant vector, and a scalar argument in any shape
  (`abs(-1)`, `round(1 + 1)`, `ceil((1 > bool 0))`) fails with `expected type instant vector in call to function "abs",
  got scalar` (HTTP 400 `bad_data` on `/prom/api/v1/query` and `/query_range`, an error in SQL `promql()`). An instant
  vector, including one built from vector/scalar arithmetic, works as before. A query that applied these functions to a
  scalar must drop the function call or apply it to a vector.
- **A PromQL range query refuses a range vector or a string (#9922, #9948).** `query_range?query=m[5m]` and
  `query_range?query="a"` answered `200` with an empty matrix. As in Prometheus, they now fail with `invalid expression
  type "range vector" for range query, must be Scalar or instant Vector` (or `"string"`), HTTP 400 `bad_data`, and the
  engine's `evaluateRange` throws the same error. Instant queries still accept both and return a matrix or a string.

## Security

- **Long-lived wire connections re-check the user (#9279).** PostgreSQL, Bolt, MongoDB and Redis sessions re-resolve
  their user against the live security state on every request, as HTTP does. Deleting a user, changing a password or
  revoking a database grant now applies to connections that are already open. A Bolt transaction is rolled back when
  its authorization is gone.
- **WebSocket sessions (#9311, #9340, #9312, #9424).** `/ws` insert frames and `SUBSCRIBE` re-validate the principal
  on every frame: a dropped or re-passworded user has its session rolled back and the channel closed, and a revoked
  grant refuses `chunk` and `commit`. A `/ws` insert session whose command failed is refused from then on instead of
  autocommitting later rows.
- **GraphQL type definitions need `UPDATE_SCHEMA` (#9280).** A type definition replaces the database's GraphQL
  schema but was classified as a read, so a read-only user could define one through the query endpoint.
- **`PROFILE` follows the statement it wraps (#9313, #9340).** `PROFILE INSERT` is refused on the read-only query
  endpoint, routed to the leader under HA, and gated by MCP and streaming. `EXPLAIN` stays read-only.
- **`DELETE_RECORD` is checked first (#9305, #9340)**, on the record's bucket, before any index or graph cleanup.
- **LOAD CSV settings are security-sensitive (#9371).** The settings that control what LOAD CSV may reach (remote
  URLs, local files, import directory, blocked IP ranges) need `updateSecurity` to change through `ALTER DATABASE`.
- **Gremlin Server error concealment (#9317, #9424).** In production mode the default WebSocket channelizer answers
  server failures with the shared concealed message and logs the real text.
- **Lightweight edge types enforce create and delete grants (#9619, #9679).** A lightweight edge has no record, so the
  record-level checks never ran for it. A user refused `createRecord` or `deleteRecord` on a lightweight edge type
  could still create and delete its edges, through SQL, the Java API, openCypher, Gremlin and `GraphBatch`. Both
  grants are now checked. As with a record-backed edge type, a user refused delete on a lightweight edge type can no
  longer delete a vertex that holds edges of that type.
- **Support diagnostics redact `arcadedb.server.defaultDatabases` (#9625, #9678).** Secrets were masked by setting
  name only, so a password inside the value of `-Darcadedb.server.defaultDatabases=Universe[albert:einstein]` was
  written in clear to `diagnostics.json`, uploaded to the portal automatically, and put in the support bundle. Every
  ArcadeDB `-D` setting in the JVM arguments now goes through its own setting's redaction, so database and user names
  stay and passwords are masked. A `defaultDatabases` value in log lines, thread dumps and messages is masked too.
  **Rotate any password configured this way on a server that already uploaded diagnostics.**
- **Studio AI answers can no longer run HTML (#9626, #9682).** The AI Assistant answer and the profiler's AI analysis
  were rendered with `marked`, which passes raw HTML through. A value stored in a database, or the AI vendor, could
  run script in the Studio page and take the viewer's session token. Both now use Studio's escape-first markdown
  renderer, and `marked` is no longer shipped. AI answers render a fixed markdown subset (headings, lists, block
  quotes, code blocks with a copy button, tables, http(s) links). Raw HTML in an answer shows as text, other link
  schemes show as text, and nested lists and quotes render one level deep.
- **A type's EXTERNAL bucket follows the type's permissions (#9637, #9686).** The paired `_ext` bucket that holds a
  type's EXTERNAL property values was missing from the per-file permission map, so any user could read, update or
  delete records in it directly. It now carries the type's permissions. A user allowed to update the type may store
  and clear EXTERNAL values. A direct insert into an `_ext` bucket is refused for every user. The same gap made
  `LOCK TYPE` or `LOCK BUCKET` followed by an EXTERNAL write fail at commit with "not all the modified resources were
  locked". The lock now covers the paired bucket.
- **AI Assistant reads of the portal and gateway are capped (#9627, #9754).** The AI Assistant read the answers of
  the portal and the AI gateway with no size limit, so a misbehaving or hostile endpoint could exhaust the server's
  heap. A streamed line is now capped at 4M characters, an error body at 64KB, and a buffered answer at 16MB. Going
  past a cap ends the request with an error instead of reading on. Gateway text copied into an error message, a log
  line or the answer shown to the user is cut to its first 1KB.

## New Features

- **Lift a quarantine on a sole voter (#9449, #9499).** A quarantined database on a node that is the only voter of
  its cluster had no way out: the node stayed not-ready and its Raft log was never checkpointed. The new root-only
  `POST /api/v1/cluster/accept-diverged/{database}` (gRPC `AcceptDivergedDatabase`, `RemoteServer` and
  `RemoteGrpcServer` client methods) accepts the local copy as the truth and lifts the quarantine and its read floor
  in one write. Every use is logged at WARNING with the user, the caller's address and the applied index. A node that
  has peers is refused with 409 / `FAILED_PRECONDITION`, since a resync from a peer is the way out there. The
  sole-voter log line and cluster alert now point to this route.
- **In-place Ratis restart counters (#9429, #9504).** `GET /api/v1/cluster` gains `localInPlaceRestarts {recovered,
  reformatted}`, and Micrometer/Prometheus export `arcadedb.ha.in_place_restarts.recovered` and `.reformatted`.
- **Customizable Contraction Hierarchies (#9437, #9441).** A Graph Analytical View can keep a CCH per weight
  property, so weighted point-to-point shortest paths are answered in time bounded by the hierarchy depth rather than
  by the distance: `CREATE GRAPH ANALYTICAL VIEW roads EDGE TYPES (ROAD) UPDATE MODE SYNCHRONOUS CCH (distance)`,
  queried with `cchShortestPath()` or `algo.cch.shortestPath`.
- **Graph Analytical View without vertex properties (#9831, #9855).** `CREATE GRAPH ANALYTICAL VIEW v PROPERTIES ()`
  builds a view with no vertex property columns, for algorithms such as `algo.pagerank()` that need only the graph's
  structure. It used to fail with a parse error. Leaving out the clause still copies every vertex property.
- **HA follower state in `GET /api/v1/cluster` (#9399, #9013, #8953, #9451).** Two new fields:
  `localReplicationPathUnproven` (the node was restarted in place and has received no replicated entry since) and
  `localLeaderUnreachableSinceRestart`. The second raises the new `follower-leader-unreachable-since-restart` alert,
  critical when a leader is known and warning when none is. The `follower-stuck-at-stale-term` alert now recommends
  "restart by hand" or "will self-heal" from the first.
- **HA executor metrics (#8856, #9417).** The `snapshot_install`, `channel_recovery`, `stalled_resync`,
  `database_deleter` and `sm_lifecycle` executors are published as `arcadedb.executor.*` rows in `/api/v1/metrics`
  and in Studio's Executor Pools card, with a rejected-task count (new Rejected column) and, for
  `channel_recovery`, a coalesced-task count. An occupied unbounded queue now reports `-1` free slots instead of about
  2.1 billion. The counters reset on an in-place Ratis restart.
- **Native image includes HA, metrics and tracing (#9407, #9426).** The GraalVM native image now ships Raft HA,
  metrics and tracing (about 907 MiB; the builder needs about 16 GiB of memory).
- **Label propagation tie-break (#9285, #9286).** `algo.labelpropagation` accepts a `tieBreakProperty`: the smallest
  property value wins a tie. Missing values sort last.
- **Sorted index build from SQL (#9291, #9299).** `CREATE INDEX ... METADATA {"buildMode": "SORTED"}` selects the
  sorted build (UNIQUE and NOTUNIQUE), which now handles low-cardinality keys with far less spilling.
- **`CHECK DATABASE` reports unreadable stripe directories (#9266, #9374)** under the new `unreadableStripeDirectories`
  key, one RID per directory this release cannot read. Stripe directories of a supported version are now validated.
- **Studio keyboard access (#8740, #9376, #9375, #9509).** The saved-query and history sidebar entries and their run
  and delete actions, the Reference panel's section headers, examples and function entries, and the sample-database
  cards (quick start and import dialog) are real buttons, reachable with Tab and announced by screen readers.
- **Gremlin `hasLabel` (#9335, #9393)** recognizes vertex types created earlier in the same traversal.
- **PromQL `bool` comparison modifier (#9644, #9722).** `a > bool 1` answers 0 or 1 for every sample instead of
  filtering, and drops the metric name, as Prometheus does. It works for vector-scalar, scalar-vector and
  vector-vector comparisons, in range queries, and through `/prom/api/v1/query`, `/query_range` and SQL `promql()`.
  Vector matching modifiers (`on`, `ignoring`, `group_left`) are still not supported.

## Performance

- **LSM index scans no longer go quadratic after a hot key (#9461, #9463).** Full and range scans, `countEntries()`
  and index-ordered `ORDER BY` on a non-unique index with a low-cardinality key (status, type, country), and FULL
  geospatial and full-text indexes with frequent tokens, slowed down for the rest of the scan after one large key
  group.
- **Bucket scan read-ahead is bounded by bytes (#9404, #9433).** A scan over large multi-page records could hold about
  100MB per bucket. Read-ahead is now bounded per bucket by `arcadedb.queryBatchMaxBytes` (1MB, 200KB in `low-ram`),
  across the JVM by the new `arcadedb.queryScanReadAheadMaxRAM` (200MB, 16MB in `low-ram`), and by the query heap
  budget. Scans of ordinary records still batch 1,024 rows.
- **Parallel GROUP BY (#9402, #9415)** no longer charges one copy of the groups per worker, so the query heap budget
  no longer refuses on 4+ cores what 1-2 cores ran.
- **Bolt (#9165, #9353)** buffers RECORD messages into a few large socket writes instead of three writes per row.
- **Vector index rebuilds (#7260, #9289)** are no longer deferred forever on a large index: the rebuild gate swaps
  the resident graph for its persisted twin when that lets the rebuild fit. New stat `residentGraphDemotions`.
- **Analytical counts:** LSQB Q9 through a Graph Analytical View (#9282, #9284, about 4x faster), LSQB Q8 back on the
  exact anti-join count push-down (#9354), and the partitioned triangle count reading each vertex once (#9400, #9415).

## High Availability

- **Vectors inserted on a follower no longer replace other vectors (#9428, #9507).** A replicated vector index page
  did not move the follower's vector id allocator past the ids it carried, so the next insert committed through that
  follower reused an id the leader had already allocated and replaced that vector on every node: `countEntries()`
  stayed flat and the superseded vectors dropped out of search. The same happened to a leader insert after a
  `build()` on a follower. **An index that already lost entries this way stays short after the upgrade and has to be
  rebuilt.**
- **Buckets named `.snapshot...` survive a snapshot install (#9446, #9494).** The installer recognized its own
  entries by the `.snapshot` name prefix, so the swap, rollback and live-file cleanup skipped the files of a bucket
  whose name starts with it. It now matches its entries by exact name.
- **`db.migrateLegacyTemporals()` reaches every node (#9712, #9748).** The call rewrote the records on every node,
  but the database's switch to native temporal storage was saved on the leader only, so the followers stayed legacy.
  A String shaped like a temporal and written after the migration then read back as a String on the leader and as a
  temporal on the followers. The switch is now replicated with the schema, and survives a follower restart. Calling
  it directly on a follower is refused, like every other schema change.
- **Schema settings changed outside a DDL reach every node (#9747, #9765).** `ALTER PROPERTY` constraints (`MANDATORY`,
  `NOTNULL`, `READONLY`, `MIN`/`MAX`, `REGEXP`, custom values) and the matching `Property` setters, schema extensions,
  the `ALTER DATABASE` date and date-time formats and `setZoneId`, `ALTER TYPE ... WITH lightweight / unique`,
  `ALTER TIMESERIES TYPE ... DOWNSAMPLING POLICY`, the bucket selection strategy and the repartition flag were saved to
  the leader's `schema.json` only. A constraint therefore went unenforced on a node that became leader after a
  failover, and date settings answered differently per node. They now replicate with the schema change itself.
- **HA checksums leave out node-local files (#9638, #9752).** `/checksums` reported files every node writes for
  itself and never ships (`statistics.json`, `cached-count.json`, `database.lck`, Graph Analytical View `.csr` and
  contraction-hierarchy `.cchorder` files), so healthy nodes could report different checksums. Vector index side
  files and `last-tx-id.bin` are left out too (#9751, #9757).
- **`ALTER TYPE ... NAME` reaches every node (#9762, #9890).** The rename and its files moved on the leader only. The
  rename now travels with the replicated schema change, and each follower renames the same bucket, index and edge
  files before reloading the schema. While any peer runs an older build that cannot apply a rename, or cannot be
  reached, the rename is **refused up front with HTTP 409** and nothing changes, so a type cannot be renamed while a
  node is down. Once a rename has been replicated, the cluster cannot be downgraded past this release on its retained
  log. No setting overrides this refusal, unlike the security-entry gate: to rename a type while a peer is gone for
  good, remove that peer from the Raft configuration first (#9891, #9952). A follower that crashes part way through
  moving a rename's files recovers its type, buckets, index and records when the rename is replayed.
- **`ALTER DATABASE` and `set database setting` reach every node (#9763, #9907).** They wrote the database's
  `configuration.json` on the node that ran them only, so followers, and a new leader after a failover, kept the old
  value (for `arcadedb.dateImplementation` and `arcadedb.dateTimeImplementation` that changed what reads returned).
  The changed settings now replicate through Raft as their own log entry and every node applies exactly those keys, so
  a setting a node holds only locally is kept. During a rolling upgrade the change is **refused (HTTP 409 / gRPC
  `FAILED_PRECONDITION`)** until every node runs a build that understands it. Path-valued settings
  (`arcadedb.externalPropertyBucketPath`, `arcadedb.pageSnapshotSpillPath`,
  `arcadedb.opencypher.loadCsv.importDirectory`) now apply on every node, so the path must exist on all of them. If the
  command fails with a replication timeout whose outcome is unknown, the node that ran it settles the setting on what
  its own `configuration.json` holds: the new value if the change was already applied there, otherwise the previous
  one until the change commits, so that node never stays alone on a value the rest of the cluster does not hold.
  Re-run the command to be sure (#9904, #9920). When the change is refused outright, the node puts the previous value
  back only while the setting still holds the value the command stored, so a change to the same setting replicated
  from another node in the meantime is no longer overwritten by the undo (#9919, #9950).
- **A JSONL import restores triggers, views, continuous aggregates and functions on every node (#9764, #9885).** They
  were restored on the leader only. The restore now ships as a replicated schema change, and if it fails part way the
  members restored before the failure are replicated too. The restore holds the database write lock while it runs.
- **A schema change no longer breaks time-series writes on HA followers (#9886, #9921).** Every schema change a
  follower applied (creating a continuous aggregate, or any unrelated DDL) closed the files of each time-series type
  that already held data, so the next write to that type failed, the follower quarantined the database and resynced
  from the leader. A schema reload now keeps those files open, and a schema load that fails part way leaves the
  previous time-series types usable.
- **A snapshot install no longer reuses the leader's vector graph unverified (#9758, #9889).** The snapshot shipped a
  vector index's `.vecgraph` pages without the manifest that vouches for them, so the follower reused them and logged
  the #6106 warning. The file is now shipped empty, and the follower builds and verifies its own graph on the first
  search, which costs one local graph build. A follower that installed a snapshot before the upgrade keeps reusing its
  graph until the next graph persist, `REBUILD INDEX` or snapshot install (counted in `unverifiedGraphReuses`).
- **A full backup no longer restores a vector graph unverified (#9888, #9918).** The backup archived a vector index's
  `.vecgraph` pages without their manifest, so a restored database reused them unverified (counted in
  `unverifiedGraphReuses`, logged as a WARNING). The component is now archived empty on both backup paths, and the
  restored index builds and verifies its own graph on the first search, which costs one graph build per vector index
  after a restore (avoiding it is tracked in #9917). Archives taken before this release still restore the old way;
  run `REBUILD INDEX` on their vector indexes after restoring.
- **No double-applied commit after an in-place restart (#8785, #9406).** The replacement state machine replays the
  log and applies a commit the old one never applied; the committing thread now waits for that apply instead of
  publishing the same commit as well.
- **After-commit callbacks fire on HA commits (#8784, #9410).** Several replicated commit paths ended without firing
  the after-commit callbacks, moving the commit counter, or marking saved records clean. That also lost Redis
  transactions that only changed in-memory values under HA.
- **Bootstrap fingerprints wait for in-flight flushes (#8843, #9414)**, bounded at 5 s, so two nodes holding the same
  data no longer report different fingerprints.
- **Orphan compaction temporaries are swept at open (#8849, #9413).** Leftover `*.temp_<ext>` index-compaction files,
  from a follower resynced by an older leader, a crash mid-compaction, or a restored backup, are deleted on a
  read-write open.
- **Snapshot checksums (#9307, #9448)** leave out the snapshot installer's own files, so identical nodes agree.
- **A sole voter no longer quarantines a database (#9308, #9450)** on an unexpected apply error, which left the node
  not-ready for good. The entry fails, its submitter gets the error, and a SEVERE alert warns that the database may
  no longer match the log.
- **Streamed batch through a follower (#9216, #9440).** A follower relays a streamed `/api/v1/batch` load to the
  leader over a full-duplex connection, so the client sees the leader's progress lines, and an in-band 413, while it
  is still uploading.
- **Interrupted snapshot reads (#9306, #9445).** An interrupt that closed a data file's channel no longer invalidates
  open snapshot windows or aborts a backup or HA ship: the channel is reopened and the operation retried.
- **A follower's refusal of leader-only work is answered at once (#9995).** `Database.transaction()` and `COMMIT RETRY` retried
  every retryable error, so a follower refusing a schema change (or another leader-only operation) and naming the leader went
  through every `txRetries` attempt and retry delay before the caller learnt where to send it, on every transport that runs work
  through `Database.transaction()` (the HTTP auto-commit retry, the MCP tools). Such a refusal is now rethrown on the first
  attempt; one raised while no leader is known is still retried, since this node may win the election. A Java caller can tell
  the two apart with the new `NeedRetryException.isRetryableOnThisNode()`.

## Bug Fixes

### openCypher

- Count push-downs count parallel and reciprocal relationships per relationship (#9298, #9300), apply the anti-join
  inequality in either order (#9345), weigh every path of a partitioned triangle count (#9350), and take only
  undirected, correctly labeled cycles in the triangle push-down (#9387, #9388, #9409).
- An edge type with no record is no longer assumed empty: light edges keep no record (#9389, #9409).
- The `CALL ... YIELD` count-only fast path counts every invocation of a chained CALL, and applies to `count(*)` only
  (#9341, #9383, #9453, #9456). `OPTIONAL CALL` answers a row of nulls when the procedure yields nothing (#9456).
- A variable reused as both a node and a relationship in a pattern predicate or comprehension is refused (#9292,
  #9297).
- `MERGE` compares temporal values the way `MATCH` does (#9334, #9344) and treats `-0.0` and `0.0` as equal (#9303,
  #9395). Durations stored in short ISO form (`P1D`) are read back correctly, and duration arithmetic is exact and
  raises on overflow (#9338, #9344).
- `MERGE ... ON MATCH SET` / `ON CREATE SET` writes `0.0` over a stored `-0.0` again, like a plain `SET` (#9618,
  #9713). The skip for an unchanged value treated the two zeros as equal since #9303.
- `EXPLAIN` and `PROFILE` output, `CREATE` profile timings and parser error positions no longer follow the JVM locale
  (#9649, #9702): under de-DE they printed `63,00` instead of `63.00`, and under a Thai-digit locale Thai digits.
- A `DATE` or `DATETIME` stored under a non-default `arcadedb.dateImplementation` / `dateTimeImplementation`, including
  `java.util.Calendar`, compares with `date()` / `datetime()` in `MATCH`, `WHERE` and `MERGE` (#9647, #9704). SQL `=`
  compares a `Calendar` by its instant, so a scan and an index lookup agree.
- `REQUIRE n.x IS :: TYPE` keeps the property's other constraints (#9304, #9395).
- A fused analytical-view chain no longer drops a pattern variable bound below it (#9390, #9409), and an undirected
  `ExpandInto` from a vertex to itself matches only self loops (#9349, #9383).

### SQL and indexes

- Index point lookups and `Database.lookupByKey` no longer lose a key, or serve another key's record, while a commit
  is being published (#9369, #9385, #9397, #9415).
- `vectorNeighbors()` tops up candidates a concurrent commit deleted (#9368, #9385). BINARY vector search reranks
  past deleted candidates (#9396, #9408). A sparse-vector update answers on its new dimensions (#9343, #9408).
- Sparse vector searches no longer answer short when a concurrent commit deletes their candidates (#9692, #9698).
  `LSM_SPARSE_VECTOR` rescoring (`topK`, grouped searches) and `vector.sparseNeighbors` widen the candidate pool and
  ask again until they have `k` live results or the index really holds fewer (#9634, #9635).
- `vector.neighbors` and `vector.sparseNeighbors` with `groupBy` no longer answer short when a concurrent commit
  deletes a candidate (#9697, #9790). A group could lose a member or its place, and a deleted record could take a group
  slot of its own. A short grouped answer is now searched again with a wider budget, still capped at `k` groups of
  `groupSize`, and with an allow-list too. `vector.recommend` with `groupBy` gets the same fix.
- `sum()` and `avg()` over BYTE values work (#9281, #9288). The anti-join chain push-down declines shapes it cannot
  answer exactly (#9277, #9278, #9290).
- Console `LOAD` with `set language = sqlscript` runs the whole file as one script, so `LET` variables survive into
  `FOREACH`, and `FOREACH` over null iterates zero times (#9454, #9455).
- The BM25 corpus counters no longer race with concurrent writers (#9337, #9393).
- A COLLATE ci index is skipped by native `select()` range cursors and Gremlin range `has()` (#9302, #9395).
- FP16 and INT8 sparse vector searches no longer miss a document that belongs in the top-K (#9834, #9878). The
  block-max bounds used for pruning came from the original float weight, while the search scored the decoded one, which
  can be slightly higher. Segments already on disk are corrected when read; no rebuild is needed.
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

### Storage

- A schema reload no longer lets two transactions take the same new page and RID (#9835, #9877). The reloaded bucket
  started with an empty page-reservation state; it now shares the reservations of the bucket it replaces.
- An interrupt while a resume re-queues its deferred flush batches no longer abandons them (#9832, #9882). Their pages
  stayed counted as pending forever, so every later backup, freeze or close of the database timed out.
- Dropping a database while its flushes are suspended releases the WAL acknowledgements of the batches held back
  during the suspension (#9881, #9928), so the drop no longer spends about 2 seconds retrying in the transaction
  manager. A plain close keeps them on purpose: those pages never reached the disk, so the WAL is preserved and
  replayed on the next open. Such a close still waits `arcadedb.flushAllPagesTimeout` before giving up on them.
- Closing or dropping the last open database inside `PageManager.suspendFlushAndExecute` (as a backup, snapshot or
  verify callback can) no longer fails with a `NullPointerException` when the suspension is released (#9927, #9946).
  A concurrent shutdown can no longer null the flush thread between a check and its use in the other page-manager
  operations either.
- A subtype that inherits an EXTERNAL property stores its values in its own `_ext` bucket (#9683, #9815). They were
  written inline, because only the properties the subtype declares itself were checked. Updates and deletes find the
  external value of a subtype record, also when the record was loaded through a polymorphic query on the parent, and a
  delete between `ALTER PROPERTY ... EXTERNAL false` and the `REBUILD TYPE` that follows no longer leaks it. An
  embedded document whose type has an EXTERNAL property no longer fails the save with "record has no target bucket":
  its values are stored inline in the owner record. **Subtype records written before the upgrade keep their values
  inline (they read normally) until `REBUILD TYPE <Subtype>` moves them.**
- **A schema change still pending when the database closes is saved (#10059).** A DDL inside an open transaction, or one
  postponed by a bulk schema change, is written when the database closes. That save ran once the database already reported
  itself closed, where an index cannot be serialized, so as soon as the schema held one index the save failed, was logged at
  `SEVERE` (`Error saving schema configuration during close`) and the change was lost: after a reopen the type, or the index,
  was gone. The save now runs as the last step before the database is marked closed, together with the BM25 corpus counters.
- **`CHECK DATABASE` reads each record of a type with a `MANDATORY` or `NOTNULL` property once (#7981).** The existence
  constraint check added in 26.10.1 walked and deserialised every record of those types a second time. A run that only
  reports now asks the question while it decodes the record content, which every run does anyway, so the report has no
  `Checking existence constraints` step of its own. `FIX DELETE INVALID RECORDS` keeps that step: its delete passes need a scan
  of their own. The findings are unchanged.

### Time series and Prometheus

- `/prom/api/v1/series` applies the label matchers of `match[]` (#9386, #9408).
- The Simple8b codec accepts integers outside the 60-bit range (#9309, #9392).
- Hourly retention handles the repeated DST hour (#9336, #9393).
- Cancelling a gRPC stream inside a transaction stops the producer at its next batch (#8752, #9392).
- An ungrouped `min()`/`max()` of a LONG time-series column answers the exact stored value past 2^53 (#9710, #9719).
  Since #9688 it came back rounded, as a `Double` or as a value the `WHERE` clause excludes. Grouped and
  time-bucketed `min`/`max`, and `sum`, of such values are exact too (#9718, #9730).
- SQL `INSERT` into a time-series `DATETIME`, `DATE`, `DATETIME_SECOND`, `DATETIME_MICROS` or `DATETIME_NANOS` column,
  field or tag, no longer fails with `ClassCastException` (#9643, #9726). An unreadable value is refused with an
  error naming the column. Filtering such a column by a date string or a temporal literal matches the rows `INSERT`
  stored (#9724, #9730).
- Prometheus `remote_read` no longer merges distinct series whose label values contain `|` or are empty in different
  labels (#9623, #9727). Empty label values are absent labels, as in Prometheus.
- The sealed time-series store reads and writes its header, block directory and tag metadata completely (#9640,
  #9728). A partial write could leave a header half old and half new, and a short read while loading the directory
  silently dropped every later block.
- An interrupt on a thread reading a sealed time-series store no longer breaks the store for every other thread until
  the next compaction (#9621, #9729). The shared file channel is reopened and the read retried; a store closed on
  purpose, or whose file was deleted, is never reopened.
- The JSON tag filter of `POST /ts/query`, `GET /ts/latest`, the Grafana query and the gRPC `TimeSeriesQuery` and
  `TimeSeriesLatest` calls reads a `DATE` or `DATETIME` tag value in the database's time zone (#9879, #9929). It used the
  JVM's, so on a server whose JVM zone differs from the database's the filter missed the rows it named.
  `TimeSeriesGateway.andTag()` and `buildTagFilter()` (embedded API) now take the `Database` as an argument.
- `SELECT count(*)` on a time-series type no longer counts samples twice while a compaction finishes (#9716, #9746).
  The sealed and mutable counts of each shard are now read together under the compaction lock, and so are the
  per-shard figures in `schema:types`. The reporter also saw an overcount on a time-windowed count once. It could not
  be reproduced, and an audit found every compaction, merge, retention and downsampling step publishing under the
  same lock; a regression test now pins that (#9745, #9785). That overcount remains unexplained.
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

### Wire protocols and server

- PostgreSQL: `SHOW` answers the command tag, a refused `SET` keeps its message, `DISCARD`, `DEALLOCATE` and `CLOSE`
  are handled, and `search_path` is accepted (#9326, #9327, #9328, #9329, #9394).
- PostgreSQL accepts PostgreSQL's own transaction-control spellings, in the simple and the extended protocol (#9631,
  #9753): `START TRANSACTION`, `ABORT`, `BEGIN [WORK|TRANSACTION]` with isolation modes (what Go's `BeginTx` sends),
  and `COMMIT`/`ROLLBACK ... AND NO CHAIN`. They used to fail with `42601`. `READ COMMITTED` and `READ UNCOMMITTED` begin
  at read-committed, `REPEATABLE READ` at repeatable-read. `SERIALIZABLE`, `READ ONLY` and `AND CHAIN` are refused with
  `0A000` (feature not supported).
- GraphQL selections that share a response key are merged, in introspection and in data queries (#8744, #9358,
  #9356, #9395).
- `arcadedb.grpc.host` is honored by the standard gRPC server (#9318, #9436), and the `arcadedb.grpc.*` settings, including
  `arcadedb.grpc.tls.enabled`, are read from `server-configuration.json` and environment variables (#9316, #9340).
  Before, only `-D` reached them, so asking for TLS in the configuration file left a plaintext endpoint listening.
- The Gremlin plugin fails its start on a port bind error and advertises the port it actually bound (#9319, #9344).
- A Redis `MULTI`/`EXEC` block retried by the HTTP auto-commit wrapper no longer applies its `INCR` twice or loses
  the value its `GETDEL` claimed (#9322, #9344).
- With `arcadedb.dateImplementation=java.time.LocalDateTime`, a `DATE` no longer reads back as a 1970 timestamp
  (#9324, #9436). With `arcadedb.dateTimeImplementation=java.time.ZonedDateTime`, a stored `DATETIME` compares
  equal to the literal for its own instant (#9325, #9436).
- The AI chat answers every gateway `tool_call` with a result, even when the tool cannot run, so the model loop is
  never left waiting (#8721, #9359).
- The Java remote client no longer crashes with `ArrayIndexOutOfBoundsException` or `IllegalArgumentException` on a
  malformed or `|`-containing `DuplicatedKeyException` from the server: it raises the typed exception, or falls back
  to the generic `RemoteException` / `NeedRetryException`. An HA follower keeps the exception type when the key value
  contains `|` (#9473, #9492).
- Python driver: `TransactionContext.__enter__` rolls back when interrupted after `begin()` (#9321, #9436).
- **A map with a non-String key renders as JSON (#9805).** A `MAP` property accepts an Integer key and a Gremlin
  `elementMap()` is keyed by the `T` enum, but `Result.toJSON()`, `convert.toJson()`, `asJSON()` and the Postgres wire threw
  `ClassCastException` for such a row (and a `null` key an `IllegalArgumentException`). The member name is now the text of
  the key, a `null` key being named `"null"`, as the HTTP serializer already did. `JSONObject(Map)` takes any `Map<?, ?>`.
- **Redis `HDEL` replies the number of records it removed, and a multi-key `HDEL` / `HSET` is one unit of work (#9837).** A
  delete a `BeforeRecordDeleteListener` vetoed was counted as a deletion on every branch. On the Redis protocol, `HDEL` by
  index key left the first keys deleted behind an error reply and was not retried on a conflict, and the transient `HSET`
  left the first JSON objects stored behind an error reply. A new `DatabaseInternal.deleteRecordReportingVeto(Record)` keeps
  the answer `Record.delete()` throws away, for any caller that counts deletions.
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
- **`INSERT INTO` a TIMESERIES type reads its values as every other ingestion surface does (#10064).** The SQL path converted each
  value with a routine of its own, so one value written to one column was stored, or refused, depending on whether it came over SQL
  or over the line protocol, gRPC or Prometheus remote write. `INSERT` now goes through the conversion those share
  (`ColumnDefinition.storableValue`): a text longer than a row can hold is refused by the statement, naming the column, instead of
  failing inside the append; and a `Float` sent to a `DOUBLE` column over the line protocol, gRPC or Prometheus is stored as its
  decimal (`0.05f` as `0.05`, as `INSERT` has stored it since #7609), not as its single precision rounding error
  (`0.05000000074505806`). An out-of-range number into an `INTEGER`, `SHORT` or `BYTE` column was already refused by the property
  conversion of the document the statement builds, and still is.
- **A leader refusal never names the node that is refusing (#10066).** On a cluster whose peers share a host and declare no HTTP
  port a follower resolves every peer's HTTP address, the leader's included, to its own (#6191), so the refusal of a
  `SecurityManager` user mutation on a follower and of a gRPC admin RPC (`CreateUser`, `SaveGroup`, `CreateDatabase`, ...) sent the
  client back to the node that had just refused it. They now name the leader as the schema and forwarding refusals do: its
  HTTP address when that identifies it, its server name when it does not. The `arcadedb-leader-http-address` trailer of a gRPC
  refusal and the redirect sentence of a Gremlin one follow the same rule. A correctly configured cluster is unaffected.
- **A Cypher `CREATE` / `MERGE` with a new label or relationship type inside a client transaction on a follower works again (#10067).**
  Since #10040 such a write runs in the transaction the client opened (Bolt or Postgres `BEGIN`, an HTTP session) instead of being
  forwarded to the leader, and a label with no type yet is a schema change only the leader may make: the first use of a new label
  inside an explicit transaction on a connection to a follower was refused with `Changes to the schema must be executed on the
  leader server`, where it succeeded before #10040. The follower now has the leader create the type (an idempotent
  `CREATE ... TYPE IF NOT EXISTS`) and carries on once it has applied it: the data stays in the client's transaction and commits or
  rolls back with it, while the type, as everywhere, outlives a rollback. A transaction that had already written commits too when the
  new type was the only schema change since it began; otherwise (someone else's DDL landed meanwhile, a type that takes several schema
  entries such as one extending an indexed type, or a replica that did not apply the type within `arcadedb.ha.quorumTimeout`) that
  statement fails with a retryable error, the type exists by then, and the retry goes through. A schema change made any other way on
  a follower (embedded code outside a client transaction) is still refused.
- **`CREATE ... TYPE ... IF NOT EXISTS` holds against a concurrent creation (#10067).** It checked that the type was missing and then
  created it, two steps, so two statements racing on one name had the loser fail with `Cannot create type '...' because already
  exists`. The loser now gets the same answer as a statement that finds the type there (`created: false`), and exactly one statement
  reports `created: true`. Without `IF NOT EXISTS` a type that is already there is still an error.
- **The cluster credential is built in one place for every peer dial (#10068).** The follower's command forward
  (`LeaderCommandForwarder`), its batch forward (`PostBatchHandler`) and the support peer query spelled `X-ArcadeDB-Cluster-Token`
  and `X-ArcadeDB-Forwarded-User` by hand, outside the guard #9999 put on the `ha-raft` dials. They now build their request with
  the same `PeerRequest` those use (moved to the `server` module), so the pair always travels together or not at all, and the
  forward that relays a client's Basic or API-token credential (#7516) is an explicit second shape of it. All three also say the
  once-per-JVM warning that an SSL cluster logs when a peer without a declared `https` port is dialled over plain HTTP, which puts
  the cluster token on the wire in clear text; nothing changes on the wire.
