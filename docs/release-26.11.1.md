# ArcadeDB v.26.11.1 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.11.1 development cycle, so the release notes are ready at tag time.

## Breaking Changes (migration notes)

### Cypher: durations are not comparable, and a local datetime is not a zoned one (#10200)

Cypher now compares temporal values as Neo4j does:

- two durations are equal or not, component by component, but are **not comparable**: `<` and `>` answer `null`, and
  `<=` / `>=` answer `true` only for two equal durations, `null` otherwise. `ORDER BY`, `min()` and `max()` still order
  them, by average length (a month counts 365.2425 / 12 days), then by months, then by days;
- a `localdatetime()` and a `datetime()` are two types: `=` is `false`, `<>` is `true` and `<` / `>` are `null`;
- values of two temporal types sort by type in `ORDER BY` (`DATETIME`, `LOCAL DATETIME`, `DATE`, `TIME`, `LOCAL TIME`,
  `DURATION`, all after paths and before strings) instead of failing with "Cannot compare DateTime with CypherDate".

**Who sees a different answer.** A range filter on a duration, `WHERE s.d > duration({days: 2})`, used to compare
component by component and now keeps no row, with or without an index on the property. Filter on a number instead,
such as the duration's average length in seconds, `s.d.months * 2629746 + s.d.days * 86400 + s.d.seconds > 172800`, or a
single component when the durations hold nothing else (`s.d.days > 2` for durations of whole days). A `DATETIME` property read from the database is the exception to the
second point: ArcadeDB stores it as an instant and it still compares with a zoned `datetime()` literal or parameter by
instant, so `WHERE e.createdAt > datetime('2024-01-01T00:00Z')` is unchanged. So does a `java.time.LocalDateTime` query
parameter.

### Cypher: duration arithmetic is exact, and calendar units follow the local calendar (#10198)

- `duration * number` and `duration / number` are exact down to the nanosecond, truncated toward zero:
  `duration('P1D') / 24` is `PT1H` (it was `PT59M59.999999999S`). An `INTEGER` operand is no longer rounded through a
  double, and a `FLOAT` operand is the decimal it is written as.
- `duration.between()`, `duration.inMonths()` and `duration.inDays()` count months and days on the calendar of the first
  argument's time zone: across a daylight saving change, noon to noon is `P1D` (it was `PT23H`).
- `sum()` and `avg()` accept durations (`sum()` in SQL too). Mixing numbers and durations in one aggregation is an error.

### Cypher: temporal constructors (#10199)

- A `millisecond`, `microsecond` or `nanosecond` given with a base value (`time:`, `datetime:`, ...) replaces the whole
  fraction of second: `localtime({time: localtime('12:30:45.123456789'), nanosecond: 5})` is `12:30:45.000000005`
  (it was `12:30:45.123456005`). The `*.truncate()` adjustment map is unchanged.
- `datetime({epochSeconds | epochMillis, timezone})` reads the instant in that time zone (the zone was ignored).
- `date({date: ..., dayOfWeek: n})` moves the date within its ISO week, and `dayOfQuarter` within its quarter.

### PostgreSQL wire: `regtype` and `format_type()` print PostgreSQL's type names (#10228)

`format_type()` and the `regtype` cast now answer the name PostgreSQL prints for a type - `integer`,
`character varying`, `double precision`, `boolean` - instead of its internal name (`int4`, `varchar`, `float8`, `bool`),
as PostgreSQL does and as `psql`'s `\d` shows in its Type column. `pg_type.typname` is unchanged. A client that compared
the old text, such as `atttypid::regtype = 'int4'` written as a string comparison, sees the SQL name instead; compare OIDs
(`atttypid = 'int4'::regtype`) to be independent of the spelling.

### SQL: string methods count and cut characters, not UTF-16 code units (#10248)

`length()`, `left()`, `right()`, `subString()`, `indexOf()`, `lastIndexOf()` and `charAt()` count Unicode characters, as
openCypher's `size()`, `left()`, `right()` and `substring()` already did (#8998). A character outside the Basic Multilingual
Plane, such as an emoji, is one character: `'\uD83D\uDE00a'.length()` is 2 (it was 3), `.left(1)` is the emoji (it was the high
half of its surrogate pair, which compared equal to `'?'`) and `.indexOf('a')` is 1 (it was 2). A search never matches inside a
surrogate pair. Text made of characters in the Basic Multilingual Plane, which is almost all text, answers exactly as before.

**Who sees a different answer.** A query that keeps a position or a length in a property and uses it later on a text with
emojis or other characters outside the Basic Multilingual Plane (`WHERE s.length() > 140` on tweets, `s.substring(0, 280)`).
`text.charAt()`, `text.indexOf()`, `text.lpad()` and `text.rpad()` follow since #10378, see below.

**Cost.** `length()` is constant time on Latin-1 text, which is how the JVM stores most of it. A position past the start of text with other characters (`charAt(i)`, `indexOf(s, i)`) is found by walking to it, so a query that reads every character of a long text of that kind by position costs more than before; `left()`, `right()` and `subString()` cost what they cut.

### SQL: `UPDATE ... SET` reads the row as it was before the statement (#10194)

Every right-hand side of a `SET` reads the record as it was before the statement, as in SQL, instead of the record with the
earlier items already applied: `UPDATE T SET a = b, b = a` swaps the two columns (it left both equal to `b`), and
`UPDATE T SET b = 7, a = b` gives `a` the old value of `b`. A statement with a single `SET` item, and one whose items do not
read a property another item writes, answers exactly as before. Two items on the same property still apply in order, and a
compound operator extends the value its property holds by then: `SET a = 1, a += 2` gives 3.

**Who sees a different answer.** A statement that relied on a later item seeing an earlier one (`SET b = 7, a = b` meaning
"both seven"). Write the value twice (`SET b = 7, a = 7`) or run two statements.

### SQL: an explicit `NULL` is stored as `NULL`, a default fills only a property that is left out (#10194)

`INSERT INTO T SET id = 1, df = null`, `INSERT INTO T (id, df) VALUES (1, null)`, `INSERT INTO T CONTENT {"id": 1, "df": null}`,
`INSERT INTO T FROM SELECT ...` of a `NULL`, and the Java `set("df", null)` store `NULL` on a property that has a `DEFAULT`
(they stored the default). Leaving the property out still stores the default, and `UPDATE ... SET ... APPLY DEFAULTS` still
fills the properties that are null, as that is what it asks for.

**Who sees a different answer.** An insert that passed `NULL` to get the default, and a `NOTNULL` property with a default,
which now refuses an explicit `NULL` ("cannot be null") instead of silently filling it. Leave the column out of the statement,
or run `UPDATE ... APPLY DEFAULTS` afterwards.

### SQL: a transaction cannot be committed after a statement failed halfway (#10194)

A multi-row `INSERT`, `UPDATE` or `DELETE` that fails after part of its records were written, inside a transaction the caller
keeps (for example `INSERT INTO T (id, k) VALUES (1, 7), (2, 7)` on a unique `k`, whose error the caller catches), used to leave
the rows written before the error, and the commit published half a statement. The engine has no savepoint to take the statement
back to, so the transaction is now marked rollback-only: `commit()` throws a `TransactionException` ("Transaction cannot be
committed: a statement failed after part of its records were written ...") and nothing of the transaction is published. A
statement that fails having written nothing, which includes every single-row refusal because the refused record retracts
itself, leaves the transaction usable. A statement that owns its transaction (auto-transaction mode) rolls back whole, as before.
A `sqlscript` that handles its own failures (`COMMIT RETRY n ELSE ... AND CONTINUE | FAIL`) is unchanged; openCypher and a script
without such a block are covered since #10379, see below.

**Who sees a different answer.** A caller that catches the error of a multi-row statement inside its own transaction and goes
on to commit. Roll the transaction back and retry, or issue the rows as separate statements.

### openCypher and SQL scripts: a transaction cannot be committed after a statement failed halfway, whatever the language (#10379)

The rule of #10194 belongs to the transaction, not to SQL. It was enforced by the SQL engine alone, so
`MATCH (n:T) SET n.v = 1` run in openCypher (or by Bolt inside `BEGIN`), and `UPDATE T SET v = 1;` run as a `sqlscript`, inside a
transaction the caller keeps, failed after their first row and the caller's commit published that row. The check now sits where
`database.command()` and `database.query()` dispatch every language, so any engine, including a later one, answers the same:
`commit()` throws a `TransactionException` and nothing of the transaction is published. A statement that fails having written
nothing leaves the transaction usable, and so does an openCypher statement whose own existence-constraint check took back every
record it had created (#7945). A `sqlscript` whose writes are all inside a `BEGIN ... COMMIT RETRY n` block, which rolls back its
failed attempts and answers the failure in its `ELSE` branch, is unchanged; a write outside such a block is covered. The SQL,
openCypher and script engines run a write statement to its end inside the call; a Gremlin traversal runs while its result is read,
so its result carries the same check.

The check runs at every call, so a statement that a function or a procedure runs through the database, whose failure the outer
statement catches and carries on from, marks the caller's transaction too when it wrote before it failed.

**Who sees a different answer.** A caller that catches the error of a multi-row openCypher or script statement inside its own
transaction and goes on to commit. Roll the transaction back and retry, or issue the rows as separate statements.

### openCypher: `DISTINCT`, `GROUP BY`, `UNION`, `min()` and `max()` key a `BINARY` or `ARRAY_OF_*` value by its content (#10380)

A `BINARY` value and the `ARRAY_OF_SHORTS`, `ARRAY_OF_INTEGERS`, `ARRAY_OF_LONGS`, `ARRAY_OF_FLOATS` and `ARRAY_OF_DOUBLES` values
are Java arrays, which Java compares by identity, so two records holding the very bytes `=` reported equal were two groups and two
`DISTINCT` rows, and `min()` / `max()` answered whichever row came first. They are now one value in all four places: `=`,
`DISTINCT` / `GROUP BY` / `UNION` / `collect(DISTINCT)` / `count(DISTINCT)` / `coll.toSet()` / `coll.indexOf()`, `ORDER BY`, and
`min()` / `max()` / `coll.min()` / `coll.max()`. A `BINARY` value orders by its bytes, unsigned (as in SQL); the other arrays order
element by element like a list. Comparing two embedded documents with `=` no longer fails with a `NullPointerException`.

**Who sees a different answer.** A query that counted or grouped such values and got one row per record, and `ORDER BY` over an
`ARRAY_OF_*` property, whose order changed from one run to the next. A `FLOAT` value, alone or in an array, is now keyed by its
decimal form, as `=` reads it, so `DISTINCT` and `GROUP BY` put `0.1` stored as a `FLOAT` and `0.1` stored as a `DOUBLE` in one
group (they were two, though `=` called them equal).

### SQL and openCypher: `text.charAt()`, `text.indexOf()`, `text.lpad()`, `text.rpad()` and the `size()` method count characters (#10378)

#10248 moved the string methods to Unicode characters but left four `text.*` functions and `size()` on UTF-16 code units, so
`text.charAt('a<emoji>b', 1)` returned half a surrogate pair (stored as `?`), `text.indexOf('a<emoji>b', 'b')` answered 3 for a
string whose `size()` is 3, `text.lpad` / `text.rpad` measured an emoji as two characters and did not pad, and `'a<emoji>b'.size()`
was 4 where `length()` is 3. They count characters now, like their neighbours. A pad character outside the Basic Multilingual Plane
is one character. Text made of characters in the Basic Multilingual Plane answers exactly as before. `size()` of a `NULL` stays 0,
as `size()` also serves collections, where a missing one has size 0; use `length()` for a string that may be `NULL`. The distance
and similarity functions (`text.hammingDistance()`, `text.levenshteinDistance()`, `text.jaroWinklerDistance()`,
`text.sorensenDiceSimilarity()`) still compare UTF-16 code units (#10417).

### gRPC: `BeginTransaction` refuses `SERIALIZABLE` (#10386)

A gRPC `BeginTransaction` that asks for `SERIALIZABLE`, or for an isolation value this server does not know (a newer client's
enum constant), is refused with `INVALID_ARGUMENT` and the message the other surfaces use: `Invalid isolationLevel
'SERIALIZABLE': accepted values are [READ_COMMITTED, REPEATABLE_READ]`. It used to begin at `REPEATABLE_READ` (an unknown value
at `READ_COMMITTED`) and answer success, which is the one failure a client cannot detect: it asked for write-skew prevention and
was told the transaction started. HTTP, SQL, openCypher and PostgreSQL already refused a level the engine cannot provide, and
they now parse it through one `IsolationLevels.parse()` in the engine, so SQL `BEGIN ISOLATION SERIALIZABLE` no longer
fails with the JVM's `No enum constant ...` and openCypher `START TRANSACTION ISOLATION SERIALIZABLE` says the same words.
The openCypher message is no longer prefixed with "Invalid transaction isolation level", and the level is reported
upper-cased whatever the case it was typed in: a client that matches the old wording has to match the new one.
`READ_UNCOMMITTED`, the proto default, still begins at the stronger `READ_COMMITTED`, reported in `effective_isolation`. The
throttled server-side warning #10322 added for `SERIALIZABLE` is gone with the case it reported.

**Who sees a different answer.** A hand-written gRPC client that sends `SERIALIZABLE`. The first-party `RemoteGrpcDatabase` never
did. Ask for `REPEATABLE_READ`.

### Plugins: a route another plugin or the server already holds fails the server start (#10385)

`PluginRoutes` is one namespace for every plugin now. A second plugin declaring ANOTHER method on a path merges into the same
entry. Declaring a method already declared on that path, a path that fills a route the server registers for itself (`GET
/api/v1/server`, `/api/v1/query/mydb`), or a prefix equal to one of the server's mounts (`/api/v1`, `/api/v1/ai`, `/ws`), throws
an `IllegalStateException` naming both parties from `registerAPI`, so the server does not start. It used to replace the first
plugin's route silently, which then answered `405` naming the other plugin's method, or shadow a core route, or replace the whole
core router, with no log line: the audit skipped every handler `PluginRoutes` produces. A plugin still implementing the
deprecated `registerAPI(HttpServer, PathHandler)` is held to the same rule.

**Who sees a different answer.** A third-party plugin that relied on replacing a route. The plugins shipped with ArcadeDB are
unaffected.

### SQL, HTTP: a `DATETIME` string with an offset is stored as the UTC wall clock of the instant it names (#10489)

`2020-01-02T03:04:05+02:00` written to a `DATETIME` property as a string (an HTTP JSON value, a SQL literal or parameter,
`Document.set(String)`) is stored as `2020-01-02T01:04:05`, exactly as the same instant is when a client sends it typed (a Java
`OffsetDateTime` or `ZonedDateTime`, a Bolt `DateTime` with an offset or a zone id). It was stored as `03:04:05`, the offset
dropped, so an HTTP client and a Bolt client in UTC+2 wrote values two hours apart. A string with no offset, and one in UTC (`Z`,
`+00:00`: what Cypher's `datetime()` renders), keeps its wall clock. A `DATE` still keeps the day written. `date()` and
`.asDatetime()` read a string the same way.

**Who sees a different answer.** A client that sends offset-bearing strings with a non-zero offset to a `DATETIME` property. Values
already stored are not rewritten: those that came from a client in another time zone hold the wall clock of that zone, and need to
be rewritten if they are to be read as UTC. The bulk path (`GraphBatch`) still rebases onto the database's zone, the same value as
the rule above for a UTC database (#10498).

### Vector search: a `PRODUCT` index walks on its PQ codes and re-ranks, and its graph file is small (#10514)

SQL `vector.neighbors`, the openCypher procedure `db.index.vector.queryNodes` and the fluent `select().nearestTo()` search a
`PRODUCT` index by walking the graph on the in-memory PQ codes, keeping `k * rerankOversample` candidates and scoring those on the
stored vectors. They used to score every node the walk visited on the stored vectors, as a `NONE` index does, so the PQ codes built
the graph and never searched it. The distances returned are still the exact ones. `rerankOversample` is a new index setting
(default 10; `CREATE INDEX ... METADATA { "rerankOversample": 30 }`), and `vector.neighbors` takes `rerank` per query. `0` returns
the PQ scores alone, without reading a stored vector, at a recall of about 0.7 on SIFT-1M with 32 subspaces whatever the beam: the
distances are then PQ estimates. The grouped search (`groupBy`) and the other quantizations keep their exact walk.

The graph file of a `PRODUCT` index no longer stores the PQ codes of every node's neighbours (jvector FusedPQ) unless the new
`fusedPQ` index setting is `true`: the `.vecgraph` was about 8x a `NONE` index's and nothing read the codes. Files written
earlier keep them and are searched through them. `pqTrainingLimit` now sets the size of the random sample the PQ codebook is
trained on; it was accepted and ignored. The Java `findNeighborsFromVectorApproximate(query, k)` and `select().approximate(true)`
keep the PQ-scores-only answer, and take `rerankOversample` / `.rerank()` to re-rank.

**Who sees a different answer.** A `PRODUCT` index searched through SQL, openCypher or the select: the ranking can differ from the
exact walk's: jvector alone measured recall@10 0.9873 against 0.9897 at a beam of 100 on SIFT-1M with 32 subspaces (#10514), and
more is lost at fewer subspaces. Raise `rerank`
(or `rerankOversample` on the index) and `efSearch` to close the gap; `INT8` is the quantization to pick when the exact walk is
what matters.

## Fixes

- **#10514** A `PRODUCT` vector index searches with its PQ codes (see the migration note above), its graph file is about the size
  of a `NONE` index's instead of 8x, `pqTrainingLimit` is honoured, the PQ-only search can re-rank on the stored vectors, and the
  FusedPQ reader that was switched off for returning a single result is back behind the `fusedPQ` index setting: it was handed a
  graph view the walk never reads.
- **#10488** A list nested in a list or in a map HTTP parameter is stored as a list, like the same value written as a literal or
  from Java, whether the property is declared or not. It was stored as the primitive `long[]` or `double[]` the request parser
  leaves at any depth, so `=`, `.type()` and `GROUP BY` saw a different value for the same data.
- **#10490** A `CREATE INDEX` (or a super-type index propagated to a type) inside a transaction that had written to the indexed
  type no longer loses every committed record when that transaction rolls back. The committed records are indexed in a transaction
  of their own and only what the caller created, updated or deleted is queued on the caller's transaction. A lookup found nothing
  and a `UNIQUE` index accepted a duplicate of a committed key.
- **#10491** A vertex written right after `ALTER TYPE U SUPERTYPE +Base`, `Base` being a vertex type a Graph Analytical View lists
  and `U` an existing type holding vertices, is no longer missing from the view and from `algo.wcc`. The same loss applied to an
  update or a delete of such a vertex.

- **#10384** A route a plugin declares `GET` answers `HEAD` too, with the same handler, and its `405` names `GET, HEAD` in `Allow`.
  `HEAD /prometheus` was a `405` since #10157, so the `wget --spider` style healthcheck the core routes were fixed to pass (#8133)
  failed on the scrape endpoint. A plugin that declares `HEAD` itself keeps its own handler. The plugin conformance tests now
  assert that every route answering `GET` answers `HEAD`, and the two core probe routes keep their `HEAD` registrations.
- **#10388** A request no longer reopens a database whose files are being deleted. `drop database` (not replicated) and the drop
  an overwrite restore performs closed the instance and then deleted its directory, and until the delete reached `schema.json`
  the directory still held a database: a request in that window loaded it, answering a torn-database error, serving a database
  whose files vanished, or leaving an open instance deregistered behind its back that refused the name when it was created
  again. The name is now held from before the drop starts until the files are gone: a request is answered `404`, and so is every
  other caller, and a create of the name is refused with a `409`. A request is also refused while a restore holds the name it
  would open, the way a create already was. A replicated drop already closed, deregistered and renamed the directory aside under
  the registry lock. A drop that fails halfway still leaves the closed instance registered, with the files the delete left, for
  the next request to reopen, as before; it is now logged at `SEVERE`.

- **#10264** A projection holding a `SELECT` subquery (`SELECT id, (SELECT count(*) FROM Owner) AS oc FROM Base`) can be stored:
  `CREATE MATERIALIZED VIEW` (default refresh or `REFRESH INCREMENTAL`) and `INSERT ... FROM` kept failing with a
  `ClassCastException` on the subquery's rows, which are now stored as a list of maps.
- **#10269** A `NOTUNIQUE_HASH` index no longer lists a record twice when its key changed away and back in one transaction
  (`1 -> 4 -> 1`), which left the record under a key it no longer held after a later delete or key change
  (`SELECT k FROM T WHERE k = 1` returned a record with `k = 2`). A single or composite key alike.
- **#10255** openCypher: a triangle written as one `MATCH` (`MATCH (a)--(b)--(c)--(a) RETURN count(*)`, with no `WHERE` or
  with an order on one property such as `a.id < b.id AND b.id < c.id`) is counted by a sorted intersection of the adjacency
  lists, in parallel over the query pool: 5 ms on a Graph Analytical View and about 40 ms without one, where it took about
  1,750 ms. The count keeps the pattern's own semantics (parallel edges, self-loop walks, and equal, null, `NaN` or
  mixed-type values drop the triangle). A Graph Analytical View node column no longer converts a value it cannot hold as it
  is (a `2.5` or a `NaN` in an integer column, a `FLOAT` in a double one, a number in a string one): the value is read from
  the record instead.
- **#10254** openCypher: a list kept next to an aggregate (`WITH r1, collect(DISTINCT b) AS r2`), or in a `DISTINCT`, is no
  longer canonicalized and hashed again on every row, which made the grouping quadratic in the size of the list.
- **#10249** A Graph Analytical View no longer ends up a few edges short when several threads commit at the same time and some
  of the commits delete edges. A transaction reported its change to the view after releasing its locks, so two commits that
  conflicted could reach the view in the opposite order, and an edge created in the record slot a deleted one had just freed
  (the same RID) was withdrawn by the deletion that arrived after it. The changes now reach the view in the order their
  transactions committed, for `SYNCHRONOUS` (broken since the views exist) and `ASYNCHRONOUS` (since #10212) alike.
  `OrderedAfterCommitCallback` gives any after-commit callback the same guarantee. A commit on a table the view covers now
  returns once the commits ahead of it have reached the view (a few microseconds when they are running; a transaction that
  never reports, because its thread died, costs the commits behind it one wait of 30 seconds, logged as a WARNING, and is then
  skipped, and its change reaches the view out of order if it ever comes: the commit order is guaranteed until such a WARNING is logged;
  `arcadedb.gavCommitOrderMaxWait` sets the wait in milliseconds, 0 to not wait at all).
- **#10188** A `NOTUNIQUE` LSM-tree index no longer returns entries its records do not have once it has been compacted:
  `TypeIndex.iterator()`, `countEntries()`, range scans and `ORDER BY` on the indexed property returned extra rows after
  updates. A key with many values is stored in chunks that can run over a data page of the compacted series, and the scan
  applied the deletions of the newer pages to the first page of the key only. Point lookups were not affected.
- **#10228** PostgreSQL wire: `psql`'s `\dt`, `\d <table>` and `\d+ <table>`, and psqlodbc's `SQLColumns` and
  `SQLTables(SQL_ALL_TABLE_TYPES)`, answer from the emulated `pg_catalog` instead of empty results or a syntax error. The
  catalog reads parenthesised joins, applies join conditions (an INNER JOIN's `ON` filters, a LEFT JOIN that does not
  match reads NULL), models `pg_am` and the column defaults of `pg_attrdef`, rewrites `OPERATOR(pg_catalog.=)` and
  `COLLATE`, and evaluates `UNION`/`INTERSECT`/`EXCEPT` over catalog queries and literal `SELECT`s, aggregates with or
  without `GROUP BY`, and correlated sub-selects. Two columns named alike (`SELECT NULL, NULL, relkind`) are both returned.
  A catalog query that would take more work than one statement may spend fails with SQLSTATE `54000` rather than
  answering empty.

- **#10199** `datetime()` reads years outside 0000-9999 (`'-0001-12-31T23:59:59Z'`, `'+10000-01-01T00:00Z'`), and
  `time()` accepts a zone name as its `timezone` (the offset that zone has now).
- **#10203** `algo.triangleCount` and the OLTP path of `algo.localClusteringCoefficient` count triangles and degrees on the
  simple undirected graph: a self-loop, a parallel edge or an edge in both directions no longer adds triangles. The
  triangle count also no longer allocates an `n x n` bit matrix, so its working memory is the adjacency.
