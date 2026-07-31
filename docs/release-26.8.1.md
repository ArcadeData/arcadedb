# ArcadeDB v.26.8.1 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.8.1 development cycle, so the release notes are ready at tag time.

### Fixes

#### MongoDB protocol: field names and filter values can no longer inject SQL

A MongoDB command is translated into a SQL statement, and the field names and values taken off the wire were
embedded in it without escaping. Both could close their own quoting and append clauses of their own, so any
client able to send an `update` or `delete` could rewrite the statement it was translated into:

- A filter **value** containing a single quote closed the string literal, so
  `updateMany({name: "v1' OR 'x' = 'x"}, ...)` produced `WHERE name = 'v1' OR 'x' = 'x'` and updated **every**
  document instead of the intended one.
- A `$unset` or `$inc` **field name** containing a back-tick closed the identifier and named further properties,
  so a single crafted key removed a property the client never asked for.
- A filter **field name** was appended unquoted and could introduce a predicate of its own.

Values are now escaped for the literal they sit in and every field name is back-tick quoted, one dot-separated
segment at a time so that navigation into an embedded document keeps working.

#### Back-tick quoted names containing a backslash are no longer mis-parsed

A schema object name may contain a back-tick or a backslash, and the SQL spelling of such a name escapes it with
a leading backslash. Escaping the back-tick but not the backslash left that encoding ambiguous, with two
consequences.

A name that arrived already escaped was escaped a second time, so the spelling grew one backslash on every
parse and re-emission and the name no longer resolved:

```sql
-- the type named a`b
SELECT FROM `a\`b`
-- re-emitted as `a\\`b`, and addressing the type reported: Type with name 'a\`b' was not found
```

A name ending in a backslash left the closing back-tick indistinguishable from an escaped one, so the quoted
token ran past the end of the name and absorbed the SQL that followed it:

```sql
-- SET ` is consumed into the type name instead of parsing as a clause
UPDATE `T\` SET `v` = 1
```

The same applied to a bucket or index addressed through a `schema:<kind>:<name>` target.

Both are fixed: a backslash now escapes the character that follows it, in the lexer and in the quoting applied
by the engine and by the Studio, so the encoding is unambiguous in both directions.

#### Concurrent writes to unrelated records of the same page no longer conflict

ArcadeDB detects write conflicts per *page*, so two transactions that touched the same bucket page raised a
`ConcurrentModificationException` even when they wrote completely unrelated records that merely happened to
share it. On a type with few buckets and many concurrent writers this made a retry pointless: the retry ran
straight into the same collision ([#5279](https://github.com/ArcadeData/arcadedb/issues/5279)).

All three halves of that are gone in 26.8.1:

- **Inserts** into one page now reserve their slot per in-flight transaction, so concurrent inserts get
  different positions (and different RIDs) instead of all being handed the same one, and the commit-time
  disjoint-slot merge replays them on top of each other.
- **Updates** are replayed by that same merge whenever they stayed inside the page, which now includes a
  record that GREW - a longer string, one more property - and not only an overwrite of the same size or
  smaller. Growth is the normal update shape, so leaving it out kept concurrent updates of unrelated records
  conflicting for good.
- **Deletes** of a plain in-place record are replayed too
  ([#5569](https://github.com/ArcadeData/arcadedb/issues/5569)). Such a delete only zeroes one slot-table
  entry, so it commutes with writes to every other slot - yet it used to take the whole page out of the merge,
  which meant that deleting record A made every concurrent transaction that merely updated record B on the
  same page fail. The pre-image is still compared byte for byte, so deleting a record another transaction is
  updating remains a real conflict.

Measured on the reported workload (one single bucket, `attempts=1`, no retry):

| Scenario | Before | After |
|---|---|---|
| Concurrent inserts | ~1750 conflicts / 2000 | 0 |
| Concurrent sub-graph creation (6 vertices + 5 edges per transaction) | ~270 / 320 | 0 |
| 10 transactions updating 10 different records of one page | 9 failed / 10 | 0 |
| Sustained updates, 8 writers on their own records of one page | ~2083 / 2880 | 0 |
| 8 deletes + 8 updates of 16 different records of one page | 15 failed / 16 | 0 |
| 10 transactions deleting 10 different records of one page | 9 failed / 10 | 0 |

A `ConcurrentModificationException` is still raised - by design - when two transactions really write the
**same** record: a byte-for-byte pre-image check makes sure no concurrent write is ever silently overwritten.
The same goes for the write shapes no merge can replay (a placeholder or multi-page record, a record that has
to spill out of its page). Nothing changes for single-writer workloads, and no application change is needed.
The merge can be switched off with `arcadedb.txPageSlotMerge=false`.

#### `dateTimeImplementation=java.time.Instant` no longer breaks reading DATETIME values

`arcadedb.dateTimeImplementation` accepts `java.time.Instant`, and embedded getters returned one correctly, but
the moment a DATETIME column crossed the JSON boundary it threw
`UnsupportedTemporalTypeException: Unsupported field: YearOfEra`. An `Instant` is a point on the timeline with
no date or time-of-day fields of its own, so the schema-wide pattern (`yyyy-MM-dd HH:mm:ss`) had nothing to
format. That took out every read path built on JSON: HTTP and the remote driver, Studio, `toJSON()`,
`Result.toJSON()` and the SQL `.format()` method.

`Instant` is now anchored to UTC before the pattern is applied - the same anchor the rest of the engine already
uses for timestamps - so it renders exactly like `LocalDateTime` and no output changes for anyone not using the
setting. `java.time.Instant` is also listed in the `arcadedb.dateTimeImplementation` description, which had
omitted it despite the type being supported.
#### Query and HTTP metrics survive an in-process server restart

The server added a Micrometer registry to the JVM-wide global registry on every start and removed none on
stop. Since a meter registered on that composite holds one child per backing registry and reports the value
of a single, arbitrarily chosen child, a restart in the same JVM left `arcadedb.query.duration` and
`arcadedb.http.requests` reporting from a child registry that had never recorded them - reading back as 0 -
while the gauges bound at startup kept reporting the *stopped* server
([#5565](https://github.com/ArcadeData/arcadedb/issues/5565)).

The metrics subsystem is now dismantled when the server stops: its registry is removed and closed, the
meters it registered are deregistered, the query and HTTP timer caches are dropped and the engine stops
timing queries. Several servers sharing one JVM (HA, embedded) are reference counted, so a stopping node
never strips the meters its siblings publish, and meters registered before the first server started are
left untouched.

Two consequences for embedded and multi-server setups. A restart in the same process now **resets the
counters**, because the values belong to the server that recorded them: a scraper that outlives a restart
sees the series start from zero rather than continue. And while a server is up it owns the meters on the
global registry, so an application that wants its own meters to survive the server's shutdown should
register them before starting it, or on its own registry added to the composite.

Only production deployments running a single server per JVM were unaffected.

### New Features

#### Bloom filters on compacted LSM indexes (enabled by default)

An LSM index lookup walks every compacted series from newest to oldest. A series' root page already rules out
a key outside its key *range*, but not a key inside the range that the series simply does not hold - so that
series still costs a root-page search and a data-page read to discover nothing. Each compacted series now
carries a bloom filter that answers the question from a single 8 KB page.

The workload this is for is a bulk load into a **unique** index, where the duplicate check for every incoming
record misses in *every* series by definition
([#5517](https://github.com/ArcadeData/arcadedb/issues/5517), out of
[#5470](https://github.com/ArcadeData/arcadedb/issues/5470)).

Measured on 2M keys across 9 series (`LSMTreeBloomFilterBenchmark`, run with `-Dgroups=benchmark`):

| | filters off | filters on | |
|---|---|---|---|
| absent-key lookups (a duplicate check) | 199,743/s | 386,138/s | **1.9x** |
| pages read for those lookups | 1,490 | 705 | **2.1x fewer** |
| bytes read for those lookups | 372 MB | 176 MB | |
| present-key lookups | 244,000/s | 281,150/s | 1.2x |

- **On by default** at a 1% target false-positive rate: `arcadedb.indexBloomFilterRate=0.01`. Set it to `0`
  to switch the feature off entirely.
- **Costs about 1.2 bytes per key** on disk - roughly 3% of the index it describes - in a separate
  `<index>_bf.bfidx` component next to the compacted index. A false positive only costs the page read that
  would have happened anyway.
- **No rebuild and no migration.** Filters are written by compaction, so an existing index gains them at its
  next compaction. They replicate over HA and are included in backups like any other component, and are
  dropped with the index.
- **Helps most when compacted series overlap in key range**, which is what keys that do not arrive already
  sorted produce (an email, a UUID, a business id). Keys inserted in ascending order give each series a
  disjoint slice that its root page already rules out, leaving the filters little to save. Range scans and
  cursors never consult them: a filter can answer for one key, never for an interval.
- **Observability:** `bloomSkippedSeries` and `bloomProbedSeries` in the index statistics report how many
  series the filters spared and how many were still read.

Backward and forward compatible with no version bump and no rollout gate: an older ArcadeDB does not
recognise the `.bfidx` extension, never opens the file, and reads the index exactly as it does today. Note
that a downgraded build compacts *without* maintaining the filters, so if you downgrade and later come back,
prefer running a full compaction (or setting `arcadedb.indexBloomFilterRate=0`) on the upgraded build.

#### The schema dictionary is no longer capped at a single page

Every type name and property name in a database is mapped to a small integer id, and records carry that id
instead of the name. That table lived in **one page**: 327,668 usable bytes, or 48,396 short names measured.
Past it, `CREATE PROPERTY` and inserting a document with a new field name failed permanently with
`No space left in dictionary file`, with no way to grow and no way back - entries are never reclaimed, so a
schemaless workload with dynamic field names walked steadily toward a wall it could not retreat from
([#5560](https://github.com/ArcadeData/arcadedb/pull/5560)).

Names now roll over onto further pages, so the cap is gone.

- **No migration, and existing databases are not rewritten.** Every page carries the same 4-byte header the
  single page always carried, so a dictionary written by an earlier version *is* a dictionary of one page and
  loads unchanged. It gains rollover on the next write that needs it.
- **Appending a name is no longer quadratic.** The in-RAM name list copied its whole backing array per
  append, which the old 48k ceiling had been hiding. Growing to 500,000 names took **11.2s** of pure array
  copying; it now takes **2ms**.
- **New databases use a 65,536-byte dictionary page** instead of 327,680. That size existed only to make the
  one available page hold as much as possible; now that pages roll over, a smaller page means a new name
  dirties and flushes 5x less. Existing databases keep the page size they were created with. One consequence:
  on a new database a single *identifier* cannot exceed `pageSize - 12` bytes, about 65Kb rather than 327Kb.
  Only type and property names ever enter the dictionary - a string *value* is only ever looked up, never
  inserted - so this is not a limit on your data.

**Rolling upgrade: upgrade followers before, or together with, the leader.** Dictionary pages replicate as
raw pages. Once a leader's dictionary grows past its first page it ships page 1 and beyond to its followers,
and a follower still running an older build writes those pages but reloads only page 0. Its in-RAM dictionary
is then missing every name past the first page, and each record referencing one fails with
`Dictionary item with id N is not valid`.

For the same reason, a database that has rolled over can no longer be opened by an older ArcadeDB at all -
loudly, not silently. Such a database could not have existed before this change, since the write would have
been refused. To check whether a given database has rolled over, divide its dictionary file size by the page
size in its own file name:

```
$ ls -l <database>/dictionary.*.dict
-rw-r--r--  1 arcadedb  arcadedb  131072  dictionary.0.65536.v1.dict
```

131072 / 65536 = 2 pages, so that one has rolled over. A file at or under one page is still single-page and
remains downgradable.

### Improvements

#### A failed bulk load answers immediately instead of waiting for the rest of the upload

`POST /api/v1/batch` rejects a payload it cannot use - an edge naming a vertex the file never created, a
vertex after the first edge, an unparseable RID - and until now it did so only after reading the *rest* of the
upload. Closing Undertow's request stream reads the body to the end first, so on a 25M-line load the client
was told nothing for fifteen minutes and then nothing at all: by then the response had been started and the
status could no longer be set (`UT000002: The response has already been started`). An exact diagnosis the
server had reached in two minutes never left the machine, and the load looked like a hang that died without a
reason (issue #5470).

The verdict is now delivered as soon as it is reached, because nothing about the remaining bytes can change
it: the unread remainder is discarded rather than drained, and the connection is retired instead. A load that
reads its body to the end keeps its connection reusable exactly as before, and so does one whose payload had
already fully arrived when it was rejected - a remainder that is merely sitting in a buffer is consumed
without waiting for the network, so a client sending small batches does not lose its pooled connection every
time one is refused. Only a remainder that has not arrived, or exceeds 64 KB, costs the connection.

One consequence worth knowing: declining to read the rest of a body means closing a connection the client may
still be writing to, and the TCP reset that follows can discard bytes the client had already received. A
client that is no longer mid-upload - it had finished sending, or stopped - always gets the error. A client
still streaming when the load is refused usually gets it too, because an ordinary HTTP client reads while it
uploads, but not reliably: measured against the JDK client it loses the response body to the reset roughly one
time in five, and a client that writes its whole payload before reading anything loses it every time. What no
client waits for any more is the upload itself, and the exact reason is always in the server log. Delivering
it reliably would mean reading the remainder first, which is the quarter of an hour this replaced.

Two related corrections:

- **A valid line is no longer blamed for a truncated upload.** The truncation check exists because a peer that
  disappears can leave Undertow's buffer holding bytes the client never sent, which surfaces as a malformed
  record; it is now consulted only for a record that failed to *parse*, which is the only failure a cut body
  can fabricate. A well-formed record with invalid content is final and reported as itself, instead of as
  "the last record read is not part of the payload" - which pointed the user at a line that was fine.
- **The check never blocks.** It establishes truncation from the end of the stream, which a departed peer
  reaches without any waiting, so a slow-but-alive client can no longer stall the response.

A batch that fails still connects the incoming edges of what it already committed before it answers, because
skipping that would leave persisted edges without back-pointers. On a large load that pass takes a while, so
it now says so in the log rather than looking like a fresh hang.
#### TimeSeries TAG columns are dictionary-encoded: 36x less page traffic on a tag-heavy schema

A TimeSeries mutable row is fixed-stride, so a `STRING` TAG column reserved the widest value it could ever
hold - `2 + MAX_STRING_BYTES`, 258 bytes - whether the tag was `us-east-1` or empty. Tags are low-cardinality
by definition, which is what makes them tags, so nearly all of that was padding that still had to be written,
flushed and shipped through the WAL ([#5519](https://github.com/ArcadeData/arcadedb/issues/5519)).

A TAG column now holds a 4-byte id into a per-**type** append-only dictionary component (`.tstd`, one file per
TimeSeries type, shared by every shard):

| arm | tags | fields | stride | rows per 64K page |
|---|---|---|---|---|
| 1 tag, 3 fields | 1 | 3 | 290 B → **36 B** | 225 → **1819** |
| 10 tags, 3 fields | 10 | 3 | 2612 B → **72 B** | 25 → **909** |
| 10 tags, 10 fields | 10 | 10 | 2668 B → **128 B** | 24 → **511** |

On `TimeSeriesTagStrideBenchmark`, the ten-tag arm went from writing 50.0 MB of pages for 2.1 MB of payload
(23x amplification) to 1.4 MB (0.7x), and from 29.9 ms to 6.0 ms. The single-tag arm improves too - it was
paying the same reservation for its one column - so a tag-heavy schema does not converge on a narrow one: the
stride ratio between them goes from 9.0x to 2.0x, which is the honest cost of nine more columns rather than
nine more paddings. Corroborated independently on real TSBS data (2,592,000 points through the primitive batch
API) by @tae898 in the issue.

- **Ids are 4 bytes, not the 2 the sealed `DictionaryCodec` uses.** That trades ~900 rows per page for ~1250
  in exchange for removing the overflow-past-65535 question entirely. Id 0 is reserved for null/empty, so the
  old round trip is unchanged.
- **STRING *fields* stay inline.** A field is where high-cardinality text belongs; interning it would grow the
  dictionary without bound.
- **`arcadedb.timeSeriesTagDictionaryMaxSize`** caps distinct values per type, default 1M (roughly 100MB). The
  dictionary is held in RAM, so this turns a mis-declared high-cardinality TAG into a clear error instead of
  unbounded growth.
- **Existing types keep the inline layout.** The mutable row format is versioned per type, so a database
  written by an earlier build opens and reads unchanged, and there is no in-place migration. A new TimeSeries
  type gets the encoding; an existing one has to be recreated to gain it. If you are benchmarking, point the
  harness at a fresh database or you will measure the old layout and see no change.
- **A probe that computes the stride itself has the mirror problem.** A harness carrying the old
  `8 + 258 * tags + 8 * fields` formula reports the layout this replaced and turns a real improvement into what
  looks like a measurement error. The encoded formula is `8 + 4 * tags + 8 * fields`. Both failure modes produce
  a confident null result from a harness that looks correct in isolation.

### Breaking Changes

#### SQL: inside a back-tick quoted name a backslash escapes the next character

A backslash used to stand for itself unless it preceded a back-tick, so a name carrying one could be written
bare. It now always escapes the character that follows it, which is what makes the closing back-tick
unambiguous, and a literal backslash has to be doubled:

```sql
-- before
SELECT FROM `C:\data`
-- now
SELECT FROM `C:\\data`
```

A single backslash immediately before the closing back-tick escapes it, so the name is left unterminated and
the statement is rejected rather than silently absorbing what follows. Only names that actually contain a
backslash are affected; every other quoted name, including one containing an escaped back-tick, is unchanged.

The Postgres wire protocol already read back-tick identifiers this way; it now also writes them this way, so a
double-quoted Postgres name carrying a backslash survives the translation instead of losing it.

#### Cypher: an unbound `$parameter` is now an error, not null

A Cypher query that references a `$name` the caller never bound used to evaluate it to null. Null is a legal
value everywhere, so the query ran to completion against a value nobody supplied: a filter matched nothing, a
predicate came out false, and each caller absorbed that into its neutral answer. A de-duplicating

```cypher
WHERE NOT EXISTS { MATCH (a)-[:E {id: $id}]->(b) } CREATE ...
```

guard degraded into an unconditional `CREATE` rather than failing. ArcadeDB now raises the same error Neo4j
does, with the same wording ([#5501](https://github.com/ArcadeData/arcadedb/issues/5501),
[#5561](https://github.com/ArcadeData/arcadedb/issues/5561)):

```
Expected parameter(s): id
```

- **HTTP** answers `400` with `exception: com.arcadedb.exception.CommandParameterMissingException`.
- **Bolt** answers Neo4j's own `Neo.ClientError.Statement.ParameterMissing`, not `SyntaxError`: the query text
  is valid, only the value is missing, and drivers key off that distinction.
- **Bound to null is not unbound.** A caller that explicitly passes `null` means it, and the query runs.
- **`EXPLAIN` is exempt**, as in Neo4j - inspecting a plan before the values are known is the point of it.
  `PROFILE` executes, so it is not exempt.
- **`SESSION SET $x = ...` counts as a binding** for every later statement on that session, and `SESSION RESET`
  / `SESSION CLOSE` unbind it again. A request that arrives without its session does not inherit the session's
  parameters and will therefore report them missing rather than silently reading null.

To keep the old behaviour for a specific query, bind the name explicitly to `null`.

## Vector index: the location cache no longer evicts, and `arcadedb.vectorIndex.locationCacheSize` is ignored

**Behaviour change for operators.** `arcadedb.vectorIndex.locationCacheSize` (and the per-index
`locationCacheSize` metadata) used to cap the in-memory location index of an `LSM_VECTOR` index and let it evict.
That was never a cache bound. A location is the only record of which record a vector id belongs to and where its
entry sits in the index file, and nothing on disk maps a vector id back to an offset, so an evicted entry could
not be recovered - and every reader reads a missing location as deleted. A cap of 100 over 1000 live vectors made
`countEntries()` report 100, and a query whose neighbours had been evicted dropped them
([#5568](https://github.com/ArcadeData/arcadedb/issues/5568)).

The limit dates from when the index held one location per *write*, so it grew with the write history. Issue #5516
removed that: a tombstoned id releases its location, so residency now follows the number of **live** vectors.

- **The setting is ignored** and reported once per index at `WARNING`. The `low-ram` profile no longer sets it.
- **Plan for ~90 bytes per live vector** - about 90MB per million, 900MB at 10 million. This is the figure
  `getStats()` now reports as `estimatedLocationIndexBytes`; it previously quoted the 24-byte payload and so
  under-estimated the footprint several-fold.
  [#5588](https://github.com/ArcadeData/arcadedb/issues/5588) tracks bringing that down to ~20 bytes by laying
  the locations out in primitive arrays.
- **An index that appeared to work under a cap on a large corpus may now need a larger heap** instead of silently
  under-reporting its size and losing vectors from searches. That is the intended trade: a wrong answer is worse
  than a visible memory requirement. Size it before upgrading if you had capped a very large corpus - 100M live
  vectors is ~9GB resident.
- **`estimatedLocationIndexBytes` steps up ~3.75x on upgrade** (24 -> 90 bytes per entry). Nothing changed about
  the memory it describes; the stat used to quote the payload rather than the retained heap. Any dashboard or
  alert threshold wired to it needs re-basing, and the same is true of `countEntries()` and `totalVectors` on an
  index that had been capped - those were reporting the cap, not the index.
- A compaction transiently holds two copies of the location set while it publishes the new one.

## Vector index: `countEntries()` is no longer torn by a concurrent graph rebuild

A rebuild used to clear the live location index and refill it entry by entry. The write lock it held only
serializes writers; `countEntries()` and `getStats()` take no lock, so they observed the map mid-refill and
reported an arbitrary fraction of its real content - a different wrong number on each run, most visibly right
after a `compact()`, because a compaction *is* a rebuild. A rebuild now publishes a fully populated replacement
with a single reference assignment, so a reader sees either the whole old set or the whole new one, and the
compaction publishes it inside the same critical section that swaps the data file - closing a window in which a
search could resolve pre-compaction offsets against the new file
([#5568](https://github.com/ArcadeData/arcadedb/issues/5568)).

## Geospatial index: a point now costs one index entry instead of eleven

A `GEOSPATIAL` index decomposed every shape into GeoHash cells and stored the WHOLE ancestor chain, so a single
point wrote one entry per tree level - `precision` of them, 11 by default. Everything an index write costs was
therefore multiplied by 11: the per-record work, the WAL, the pages a cluster replicates, and the compaction that
has to merge them all again. Worse, the cells at the top of the tree are continent-sized and shared by the whole
dataset, so a handful of keys collected one posting per record and grew without bound; each compaction rewrote
those complete lists, which is why a bulk load with a geospatial index got slower the longer it ran and finally
failed with `ReplicatedEntryTooLargeException`
([#5478](https://github.com/ArcadeData/arcadedb/issues/5478)).

That layout comes from Lucene, where a term's postings are a compressed doc-id list. On an LSM-Tree - a sorted
key/value store - "every cell below C" is simply the key range `[C, C+\uFFFF]`, so the ancestors do not need to be
materialised at all. A geospatial index now stores only the cells the decomposition stops at (**exactly one** for a
point) and answers a query with a prefix RANGE SCAN over the covering cells of the search shape, plus an exact
lookup on the covering cells that still have children - those can only match a shape indexed at a coarser
resolution, such as a stored polygon.

On a 1M-point load into one country-sized box, measured in a single JVM run (`GeoIndexIngestBenchmark`):

| arm | wall clock | index entries |
|---|---|---|
| no index (the floor) | 10.8 s | - |
| new layout | 13.0 s | 1,000,000 |
| old layout | 22.4 s | 11,000,000 |

The index costs **5.3x less** than it did (2.2 s of overhead against 11.6 s), the whole load is 1.7x faster, and
the index is 11x smaller on disk.

Two query-side consequences come with it:

- **Selectivity.** The old layout answered a query by looking up each covering cell exactly, including the
  level-1 cell, which returned every record on the same continent as candidates for the SQL predicate to
  re-check. A prefix scan returns what is actually under the queried cells.
- **A POINT search shape works.** Every cell of a point's covering carries a null `shapeRel`, and the old lookup
  loop skipped exactly those, so `geo.equals` / `geo.contains` against an indexed type found nothing and had to
  fall back to a full scan. The walk no longer filters on `shapeRel` - the cell iterator only ever yields cells
  that do intersect the shape.

> **Upgrading? Your existing geospatial indexes change behaviour the moment the jar is swapped, before any
> rebuild.** The `shapeRel` fix above lives in the shared query walk, so an index still on the old layout also
> stops skipping those covering cells. A `geo.equals` / `geo.contains` query against it goes from returning
> **nothing** to returning the right rows - the reason those two predicates were documented as index-less - and
> every other query on it pays a few more lookups and carries a few more candidates into the predicate. Results
> stay a superset that the predicate re-checks, so nothing gets worse than it was; the ingest and selectivity
> gains still need the rebuild below.

**Existing indexes keep working and are not rewritten.** The layout is recorded per index (`tokenization` in the
schema), a definition written before this release loads as `FULL` and keeps reading and writing the ancestor
chain, so nothing on disk is reinterpreted. To move an existing index to the compact layout - which is where the
ingest and selectivity gains are - rebuild it:

```sql
REBUILD INDEX `Address[location]`
```

Opening a database **says so**, once per logical index and not again on every schema reload:

```
WARNI [LocalSchema] Index 'Address[location]' of database 'mydb' should be rebuilt: This geospatial index uses
the legacy FULL cell layout: ... Run: REBUILD INDEX `Address[location]`
```

The same advice reaches Studio: the Indexes tab shows a banner with the ready-to-run `REBUILD INDEX` for each
affected index and flags the rows, and the type detail marks the index too. Under the hood this is a general
mechanism, not a geospatial one - `IndexInternal.getUpgradeWarning()` defaults to `null` and is surfaced as
`upgradeWarning` on `schema:indexes`, `schema:index:<name>` and `schema:types`, so any future layout change can
use the same channel.

A row `LIMIT` is now ignored by a geospatial index rather than truncating its candidates. What the index returns
is a superset the `geo.*` predicate re-checks, so cutting it at N before the filter runs drops rows that would
have survived - silently, and only on some queries. `IndexInternal.isResultApproximate()` marks such an index and
both `Index.get(keys, limit)` and the `TypeIndex` fan-out over it now return every candidate; the limit applies to
the filtered rows, as it always did through SQL.

`REBUILD INDEX` also no longer resets a non-default GeoHash `precision` back to 11, the same defect fixed for
`FULL_TEXT` in #4732. Its cause was one level up: `TypeIndexBuilder` declared a `metadata` field that shadowed
`IndexBuilder`'s, so `withMetadata()` wrote to one and `create()` read the other for every index type without a
dedicated builder subclass. The duplicate field is gone.
## Cypher: a non-numeric argument to `abs()` and friends is a client error, not a 500

`RETURN abs('hello')` answered HTTP `500 Cannot execute command` with an otherwise perfectly good message,
`abs() requires a numeric argument`. The type check was right; only its class was wrong, and a `500` tells a
client the server broke when in fact the query did. Neo4j reports these as `Neo.ClientError.Statement.TypeError`
([#5484](https://github.com/ArcadeData/arcadedb/issues/5484)).

Every function declared as `f(input :: INTEGER | FLOAT)` now raises `CommandSemanticException`, so HTTP answers
`400` and Bolt answers a client error: `abs`, `ceil`, `ceiling`, `floor`, `sqrt`, `sign`, `round`, `isNaN`,
`exp`, `log`, `ln`, `log10`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `cot`, `coth`, `sinh`,
`cosh`, `tanh`, `degrees`, `radians`, `haversin`, plus the `math.*` extensions. The message is now phrased with
the vocabulary of the language, matching the one `size()` and `head()` already used:

```
Type mismatch: abs() expects an INTEGER or a FLOAT argument but got STRING
```

- **A literal is rejected before the query runs**, as in Neo4j, so `MATCH (n:Nothing) RETURN abs('hello')` fails
  even though the function would never be called. Both paths raise the same exception with the same wording, and
  every argument position is covered: `atan2('hello', 1)` and `round(x, 2, 'SIDEWAYS')` fail there too, not only
  the single-argument functions. That pass walks `RETURN` and `WITH` projections, which is the scope `size()` and
  `head()` have always used; anywhere else the runtime check reports the same error with the same message when the
  function runs.
- **`round()` also covers its other two arguments**: a non-numeric precision, and a rounding mode outside
  `UP, DOWN, CEILING, FLOOR, HALF_UP, HALF_DOWN, HALF_EVEN` (whose message now lists them).
- **Null propagation is untouched**: `abs(null)` still answers `null`. In the two-argument `atan2()` both
  arguments are type-checked before null decides the answer, so a bad one is still reported when the other is
  null.
- **The `math.*` extensions no longer leak `NumberFormatException`** for an unparseable argument. They still
  accept a numeric *string* (`math.sigmoid('1.5')` works), which the Cypher-standard functions never did and
  still do not (`abs('1.5')` is a type error). That asymmetry is deliberate: the `math.*` extensions have always
  parsed strings and queries depend on it, while the standard ones follow the Cypher signature. Only a string is
  parsed now, where before any value at all was run through `toString()` first, so a type whose text happens to
  look like a number is a type error rather than a silent coercion.
- **The wrong number of arguments is now caught while parsing**, with a message naming the function and the
  count it expects (`Function 'abs' expects 1 argument but got 2`) - the same sentence the functions' own guards
  use, so it does not matter which caught it. `distance()` was declared as taking exactly two arguments although
  it has always accepted an optional unit; the declaration was corrected rather than the behaviour, and every
  other variadic function was swept for the same mistake and found correct.

## Partitioned types: lookups on a secondary index no longer read the wrong bucket

A type using `partitioned(...)` bucket selection places each record in the bucket its **partition** key hashes to,
but every index lookup was pruned to the bucket the **lookup** key hashed to. For the partition index itself those
are the same bucket; for any other index of the type they are unrelated, so the pruned search read a bucket the
record was not in. The lookup silently returned nothing, and because the commit-time duplicate check reads through
the same path, a secondary `UNIQUE` index stopped rejecting duplicates
([#5589](https://github.com/ArcadeData/arcadedb/issues/5589)).

The bucket-selection contract now carries the property names the key values belong to
(`BucketSelectionStrategy.getBucketIdByKeys(List, Object[], boolean)`), so a partitioning strategy verifies the
lookup covers exactly its partition properties before pruning and otherwise declines, which fans the search out
across every bucket - correct, only slower. Pruning on the partition key itself is unchanged, including composite
partition keys, and the SQL and Cypher planner pruning rules are unaffected.

- **Databases that ran `partitioned(...)` on a type carrying more than one index may already hold duplicates in a
  secondary `UNIQUE` index**, admitted while the check was reading the wrong bucket. The constraint is enforced
  again from this release, but existing rows are not retro-validated: check those indexes for duplicate keys and
  `REBUILD INDEX` them.
- The single-argument `getBucketIdByKeys(Object[], boolean)` and `DocumentType.getBucketIndexByKeys(Object[],
  boolean)` are deprecated. They still compile, and they never prune, since the keys alone cannot be verified
  against the partition properties.

## Partitioned types: a lookup key boxed differently than the stored value now finds its record

Follow-up to the above. `partitioned(...)` derives the bucket from `Object.hashCode()` on both sides, but the two
sides saw differently boxed objects: placement hashes the value **after** the schema coerced it to the declared
property type, while a lookup hashed whatever the caller passed. `Long.hashCode(v)` is `(int) (v ^ (v >>> 32))`
and `Integer.hashCode(v)` is `v`, so the two agree only for positive values below 2^31. On a `LONG` partition key
every negative value, and every value outside the `int` range, pruned to a bucket the record had never been placed
in ([#5595](https://github.com/ArcadeData/arcadedb/issues/5595)).

This was not limited to the Java index API: a plain `SELECT FROM T WHERE id = -5` hits it, because a SQL integer
literal that fits an `int` arrives as an `Integer` and the planner prunes buckets through the same strategy.

The lookup key is now converted to the declared property type - the very coercion the write path applies - before
being hashed. Placement is untouched, so **no existing database needs a repartition**. Where the stored form cannot
be reproduced the strategy declines to prune and the search fans out across every bucket, which is correct and only
slower:

- the partition property is not declared in the schema, so the record kept whatever Java type the writer used and
  there is no conversion target;
- the key does not coerce to the declared type at all;
- the partition index declares `COLLATE CI`. Case folding is an index-level normalisation that placement never
  applied, so `'Hello'` and `'hello'` are a single index key living in two different buckets. Only a change to
  placement could reconcile that, and that would force a repartition, so such a partition is no longer pruned.
  (Bucket counts that are a power of two up to 32 hid this by arithmetic accident: flipping the case of an ASCII
  letter shifts the Java string hash by a multiple of 32.)

## Partitioned types: an unusable partition key is refused instead of quietly breaking `UNIQUE`

Third and last of the series, and the one that closes the hole rather than the symptom
([#5603](https://github.com/ArcadeData/arcadedb/issues/5603)). On a partitioned type a `UNIQUE` index is a set of
per-bucket sub-indexes, so the constraint is global only while every record carrying one index key lands in one
bucket. Placement hashes the value; the index decides two keys are equal a different way. Where those disagree the
same key lives in several buckets and **each of them accepts its own copy**. Measured against a round-robin control
that rejects the duplicate every time, a partitioned type admitted 3 of 4 rescaled `DECIMAL` values, 3 of 6
identical `BINARY` values and 3 of 6 spellings of one instant:

- **`BINARY`** - the stored form is a `byte[]`, which inherits identity `hashCode`, so every single write of the
  same bytes drew a fresh bucket;
- **`DECIMAL`** - `BigDecimal.hashCode` folds in the scale, so `1.1` and `1.10` hash apart while the index compares
  them equal;
- **`DATE`/`DATETIME` under a zone-carrying `dateTimeImplementation`** (`ZonedDateTime`, `Calendar`) - only the
  instant reaches disk, so the writer's zone is hashed at placement and gone on the way back. This covers every
  datetime precision (`DATETIME_SECOND`, `DATETIME_MICROS`, `DATETIME_NANOS`), which the deserializer reads back
  through the configured implementation just like the base type. The zone-free implementations (`java.util.Date`,
  `Instant`, `LocalDate`, `LocalDateTime`) round-trip unchanged and are unaffected.

Such a partition is now never pruned, which restores the constraint by making the uniqueness check fan out, and
**`ALTER TYPE ... BucketSelectionStrategy partitioned(...)` refuses the configuration outright** rather than
attaching it and letting the damage surface at read time - the common root behind #5589 and #5595. `COLLATE CI`,
already unprunable since #5595, is refused on the same footing. Placement is untouched, so no existing database
needs a repartition.

- **An existing database in one of these states still opens.** The same check runs when the schema is read back,
  but only logs a warning: a refusal at load would turn a slow database into an unopenable one. Once open, its
  `UNIQUE` index is enforced again. Duplicates already on disk are not retro-validated - scan those indexes and
  `REBUILD INDEX` them.
- **A second index on non-partition properties is a warning, not a refusal.** Those lookups fan out (#5589), which
  is correct but no faster than not partitioning; the schema is otherwise perfectly reasonable, so it is reported
  and accepted.
- **`ALTER TYPE ... BucketSelectionStrategy` now says what actually went wrong.** Every failure used to be
  rewritten as `implementation '...' was not found`, so a `partitioned('x')` rejected for want of a unique index on
  `x` sent you hunting for a typo in a name that was perfectly valid. A genuinely unknown implementation still
  reports that it cannot be found. Only the message changed: the refusal is still a client error and still answers
  HTTP 400.

Two states the issue asked about turned out not to be reachable, and are pinned by tests rather than fixed: an
**undeclared partition property** cannot occur, because the strategy demands a unique automatic index and an index
cannot be created on a property that does not exist; and a **`DATETIME` key under a non-default
`dateTimeImplementation` does not shift bucket across a rebuild**, because the write path already truncates to the
declared precision, so a freshly built record and one deserialized from disk hash identically.

## Commit-time page merges now prove their coverage instead of trusting every writer

The two commit-time page merges - the commutative edge-append merge and the disjoint-slot merge - resolve a page
version conflict by discarding this transaction's whole image of the page and replaying only its *tracked* writes
on top of the newer committed version. That is sound only when every byte the transaction wrote to that page
belongs to a tracked write. Until now nothing checked it: the invariant was maintained by hand, through
`poisonEdgeAppendPage`/`poisonSlotRebasePage` calls scattered across the writers. A writer that forgot one
committed a page from which its own change had silently vanished - no exception, no log line, and the merged bytes
then replicated faithfully to every follower. The same gap had already been found and patched three times by
people reading the code rather than by a test.

The eligibility test is now the other way round: a byte counts as replayable only while its writer says so
(`MutablePage.beginCoveredWrite`), and a page that carries even one byte written outside such a declaration is
refused. `LocalBucket` declares exactly the writes each merge re-applies - the single-slot insert, the in-page
update (overwrite or growth), the plain-record delete, the in-place rewrite behind a tracked edge append, and
`compressPage`, whose defrag the commit path re-runs on the rebased page. Everything else - the inline
record-table writes of the multi-page writers, a placeholder, a record spilling out of its page, a
stripe-directory update co-located with a segment append, a `GraphBatch` bulk load, any writer added in the
future - leaves the page undeclared and falls back to the ordinary retry it would have taken before the merges
existed ([#5596](https://github.com/ArcadeData/arcadedb/issues/5596)).

- **No configuration change and no new failure mode.** The existing poison calls stay as the fast, precise path;
  the coverage proof only ever turns a would-be silent lost write into a retry.
- **Merge rates are unchanged on the workloads the merges were built for** (concurrent appends to one super-node,
  concurrent updates, inserts and deletes of unrelated records sharing a page): those pages are fully declared.
- **Cost is two ints per page and one OR per page write.**

## The page-merge counters are now visible to an operator

`mergesDeclinedByCoverage`, introduced above as *the* signal that some writer is dirtying a mergeable page without
declaring it, could not actually be read outside a debugger or a unit test: `Profiler` surfaced exactly one
page-manager contention stat, `concurrentModificationExceptions`, and none of the three merge counters. The advice
that came with it - "watch it next to `edgeAppendMerges`/`txPageSlotMerges`: a jump here with a dip there is
exactly the shape of a forgotten declaration" - was therefore unfollowable in production
([#5608](https://github.com/ArcadeData/arcadedb/issues/5608)).

All three now reach the `PAGE-MANAGER` block of the text dump, the `/api/v1/server` JSON, the Studio metrics table
and `/prometheus` (`arcadedb.engine.page.merges.edge.append`, `.slot`, `.declined`). They are also **rate-tracked**
alongside `concurrentModificationExceptions`, because the signal is a derivative: monotonic counters cannot show a
jump-with-a-dip without an operator sampling and subtracting them by hand.

Two smaller hardenings shipped with it:

- **The compression a merge owes the page it re-derives is now structural.** `LocalBucket.compressPage` declares
  its writes covered by *every* merge, which is true only because the compression is re-run on the rebased page.
  That re-run moved from the commit loop into `rebaseEdgeAppends`/`rebaseSlots` themselves, so reordering or
  optimising the commit loop can no longer invalidate the declaration - the consequence being a lost defrag, which
  no correctness assertion would have caught. A regression test now asserts a merged page is committed hole-free.
- **A diagnostic can no longer replace the conflict it is reporting.** `reportCoverageDecline` runs inside the
  commit loop's `catch (ConcurrentModificationException)`, one statement before the rethrow, and reached a bucket
  lookup that raised `SchemaException` for an unknown id. Escaping there, it would have been wrapped as a plain
  `TransactionException` - turning a conflict the caller would have retried into a hard failure. The diagnostic is
  now total by construction.
- **An unresolvable edge bucket is the retryable conflict it always claimed to be.** The lookup above sat under a
  `bucket == null` branch documented to "treat a missing one as a retryable conflict", which could never run: the
  single-argument `getBucketById` raises before it can return null, so the `ConcurrentModificationException` that
  branch promised was never the exception anyone actually got. The lookup now asks for the null its own contract
  was written against, and every caller of it - tracking, poisoning and the rebase itself - retries as designed.

## A `STRING` property no longer reads back as a geometry

Deserializing any string looked at its first characters and, when they were `POINT`, `CIRCLE`, `LINESTRING`,
`POLYGON`, `ENVELOPE` or `BUFFER`, parsed the value as WKT and handed back a spatial4j `Shape` instead of the
`String` that had been written ([#5600](https://github.com/ArcadeData/arcadedb/issues/5600)). A property declared
`STRING` therefore did not round-trip: `getString()` returned spatial4j's `Pt(x=..,y=..)` form, which is not valid
WKT, so feeding it back into a geo function failed. The trigger was a prefix match on arbitrary user text, not a
schema decision, so a `description` column holding `"POLYGON shaped, see attached"` went through it too - and the
prefix chain ran on **every** string read in the database.

The storage layer no longer changes the declared type of a value: a string reads back as the string that was
written. The conversion happens where a geometry is actually asked for - `GeoUtils.parseGeometry()`,
`GeoUtils.parseJtsGeometry()` and the geospatial index all accept a WKT string - so a database written before
26.2.1, when shapes were stored as WKT text, keeps working unchanged; `geo.point()` likewise still returns WKT and
is still indexable and queryable. Only code that relied on `document.get("wktColumn") instanceof Shape` sees a
difference, and it now gets the type the schema declares.

## GEOSPATIAL: `precision` is settable from SQL, and an ignored `METADATA` is now an error

`CREATE INDEX ... GEOSPATIAL METADATA {"precision": 6}` parsed, ran, and silently built the index at the default
precision of 11: `CreateIndexStatement` forwarded `METADATA` for `LSM_VECTOR`, `FULL_TEXT` and `LSM_SPARSE_VECTOR`
and fell through to a bare `create()` for everything else, so only the Java API could set it. Precision drives cell
resolution (11 is about 2.4 m, 6 about 1.2 km) and therefore index size and query selectivity.

`GEOSPATIAL` now has a builder of its own, `TypeGeoIndexBuilder`, in the same shape as the full-text and vector
ones, and `withType(GEOSPATIAL)` returns it - so `precision` and `tokenization` are settable from SQL and from the
Java API alike:

```sql
CREATE INDEX ON Location (coords) GEOSPATIAL METADATA {"precision": 6}
```

A `METADATA` clause the index cannot use is now reported instead of dropped. An unknown key (`{"precisin": 6}`), a
precision that is not a whole number in 1-12, an invalid tokenization, or a `METADATA` on an index type that has
no settings at all raise a `CommandSQLParsingException`. Silently ignoring the clause is what kept this gap
invisible in the first place.

> **Breaking change, at execution time.** `CREATE INDEX ... UNIQUE METADATA {"test": 3}` - a `METADATA` clause on
> a plain `LSM_TREE` or `HASH` index, which has no settings to configure - used to be accepted and ignored, and
> now fails the statement. The SQL **grammar** is unchanged, so such a statement still parses; only running it
> is refused. If a schema script carries a `METADATA` that was never doing anything, drop the clause.

One more sharp edge went with it: **`withType()` no longer leaves the original builder unconfigured.** It returns
a specialised subclass for `LSM_VECTOR`, `FULL_TEXT`, `LSM_SPARSE_VECTOR` and now `GEOSPATIAL`, so a caller that
ignored the return value (`builder.withType(X); builder.create();`) hit `indexType was not specified`. The type is
recorded on the original builder as well.

## GEOSPATIAL: an area shape indexes fewer cells

The FRONTIER layout introduced above stores the cells a shape's decomposition stops at. A complete set of sibling
cells is now collapsed into its parent, recursively - the reduction Lucene calls `pruneLeafyBranches` and applies
by default on its own indexing path. A parent covers the union of its children, so the cover can only grow and a
match is never lost; the `geo.*` predicate post-filters the superset either way. Measured on the frontier cell
count: 57% fewer for a small square, 74% fewer for a jagged outline, and unchanged for a linestring or a wide
rectangle. A point decomposes into a chain of single-child cells and is never affected, so the one-entry-per-point
guarantee stands.

Lucene's own implementation buffers the entire decomposition in a list, which is why its javadoc warns against the
option "for high precision (low distErrPct) shapes". ArcadeDB's is a streaming walk: only the frontier tokens on
the current root-to-leaf path can still be revoked by an ancestor collapsing, bounding what is held to
`subCellsSize * detailLevel` tokens regardless of shape size. The two produce identical token sets, which is
asserted directly against Lucene in `LSMTreeGeoIndexCellPruningTest`.

This changes what a **new** index writes for area shapes; an index already in the `FULL` layout is untouched, and
`FRONTIER` has not shipped in any release, so no published database holds the unpruned form.

> Running a **nightly 26.8.1-SNAPSHOT** build from between the `FRONTIER` change and this one is the one case that
> needs attention: a geospatial index written by those builds holds unpruned cells for its area shapes, while this
> build recomputes the pruned - smaller - set when a record is deleted, which would leave the extra entries behind.
> `REBUILD INDEX` on such an index rewrites it in the current layout. Point-only indexes are unaffected, since
> pruning never applies to them.

## Cypher: the follow-ups left by the `abs()` fix

Five loose ends recorded while fixing [#5484](https://github.com/ArcadeData/arcadedb/issues/5484), collected as
[#5602](https://github.com/ArcadeData/arcadedb/issues/5602) and closed together.

**The guard that was supposed to stop the `distance()` mistake from recurring now actually compares something.**
`FunctionValidator`'s `minArgs`/`maxArgs` went from unused metadata to a hard parse-time gate in #5484, which is
how a declaration narrower than the function's real signature started rejecting valid queries. The drift guard
added alongside it compared a registered signature against the bounds its executor declares - and asserted it had
compared *zero* of them, because no executor declared any, and the seven functions that reach a SQL function
through `SQLFunctionBridge` (`count`, `distance`, `stdev`, `stdev_pop`, `stdev_samp`, `stdevp`, `sum` - `distance`
among them, the one entry that was actually wrong) had nothing to delegate to. Every Cypher executor now declares
its argument-count contract and enforces it *from that declaration* (`Function.checkArity`), so there is one
number per function rather than a hand-written `if` beside a hand-written bound; the bridge passes the wrapped SQL
function's through. All 129 registered names are checked at build time. Two are pinned as deliberately narrower in
Cypher than in SQL - `count` (whose star form is a separate parser construct) and `sum` (SQL's is variadic per
row) - and the pin itself is asserted to still bite.

- **A wrong argument count is now a client error everywhere.** The shared function layer previously answered
  `CommandExecutionException` (HTTP `500`) from its own runtime guards while the parser answered
  `CommandSemanticException` (HTTP `400`) for the same mistake. Both now say
  `Function 'x' expects N arguments but got M` with the client-error class. `Function.validateArgs()` - the entry
  point the `CALL` path uses - runs the same check rather than its own, so calling a function through `CALL` with
  the wrong number of arguments no longer reports a different message, or a `500` where an expression gave `400`.

  > **The exception type changed, not only the status.** A wrong argument count now raises
  > `CommandSemanticException`, which extends `CommandParsingException` - a different branch of the hierarchy from
  > the `CommandExecutionException` the old runtime guards threw (and from the `IllegalArgumentException`
  > `validateArgs()` threw). Embedded code that catches `CommandExecutionException` around a call in order to catch
  > a bad argument count will no longer see it, and should catch `CommandParsingException` instead. This is the
  > opposite of the arithmetic change below, which deliberately stayed inside `CommandExecutionException`: an
  > arithmetic failure genuinely happens while executing, whereas a wrong argument count is a property of the query
  > text that the parser already rejects, so the two belong on different branches.

**Parse-time argument validation is no longer confined to `RETURN` and `WITH`.** `MATCH (n:Nothing) WHERE
abs('x') > 0 RETURN n` ran to completion - and, matching no row, looked like a success - while the identical call
in a `RETURN` was rejected before the query started. The clause an expression sits in has no bearing on whether
the call is valid, so the same checks now run over `WHERE`, `UNWIND`, `SET`, `CREATE`, `MERGE`, `DELETE`,
`FOREACH`, `ORDER BY`, `SKIP`/`LIMIT` and inline pattern properties, through one traversal rather than a
per-clause recursion. No check is new; only its reach. A call that does execute still fails with the same message
from the function's own guard.

> **Potentially breaking.** A query that today runs to completion because its bad call sits in a clause the
> validation never walked - or in a branch that never executes, such as a `WHERE` on a pattern that matches no row -
> is now rejected before it starts. The call was always wrong and would always have failed had it run; what changes
> is that the failure is no longer conditional on the data. If a query of yours starts failing, the error names the
> function and what it expected.

**`charLength()` and `isNormalized()` work; `charAt()` is gone.** All three were registered as known to the
parser with no executor behind them, so a call parsed and then failed at execution with the confusing `Unknown
function` - right after the parser had declared the name valid. `charLength` is now an alias of the
already-implemented `char_length`, `isNormalized(input[, normalForm])` is implemented as the boolean counterpart
of `normalize()` (same `NFC, NFD, NFKC, NFKD` form names, same error for an unknown one), and `charAt` - which
names no function in Neo4j either - is unregistered, so it is rejected up front with the ordinary unknown-function
error. An unknown-function error now also echoes the spelling you wrote rather than the folded one.

`normalize()` also becomes STRING-only, matching both its new counterpart and Neo4j's `f(input :: STRING)`
declaration: it used to `toString()` whatever arrived, so `normalize(123)` quietly answered `'123'` instead of
raising the type error. A non-STRING argument is now a client error, the same treatment `size()` and `head()` got
in #5477 and #5476.

**Case folding no longer depends on the server's default locale.** #5484 fixed this for function names, where a
Turkish default made `"ISNAN".toLowerCase()` the dotless `"ısnan"` and `RETURN ISNAN(1.0)` an unknown function.
The same pattern survived in procedure names, variable names, `IS ::` type names, the `EXPLAIN`/`PROFILE` prefix
scan, temporal unit names, vector metric names and the graph functions' direction argument. All of them fold with
`Locale.ROOT` now, and a test reads the sources to keep a new one from slipping in - the two forms behave
identically under every locale CI runs in, so nothing else would notice.

**An arithmetic error is a client error, not a 500.** `abs(-9223372036854775808)`, `9223372036854775807 + 1` and
`1 / 0` all have no representable answer, which is decided by the values the caller supplied and not by anything
wrong with the server; all three answered HTTP `500`. Neo4j classifies the whole category as
`Neo.ClientError.Statement.ArithmeticError`. The engine now raises `ArithmeticErrorException` for 64-bit overflow
and for division or modulo by zero (including `duration(...) / 0`, which used to escape as a raw
`java.lang.ArithmeticException`), HTTP answers `400`, and Bolt answers
`Neo.ClientError.Statement.ArithmeticError`.

- **Nothing changes for embedded code**: `ArithmeticErrorException` extends `CommandExecutionException`, the
  class [#5164](https://github.com/ArcadeData/arcadedb/issues/5164) and
  [#5494](https://github.com/ArcadeData/arcadedb/issues/5494) settled on, so existing catch blocks are unaffected.
- **Floating-point arithmetic is untouched**: `1.0 / 0.0` is still `Infinity` and `0.0 / 0.0` still `NaN`, as
  IEEE 754 and Neo4j require.
- **A retryable conflict still wins over it** in the Bolt classification, so a driver's managed-transaction retry
  is not lost.
- **HTTP and Bolt only.** The other wire protocols (Postgres, MongoDB, Redis, GraphQL, Gremlin) still report an
  arithmetic error through their generic execution-error handling; only the two paths a Cypher statement normally
  arrives on make the distinction.
## `countEntries()` no longer counts tombstones as live entries

On any LSM index, deleting records did not bring `countEntries()` back down to the number of live entries: the
count settled on a residual that only a full compaction cleared. Deleting every record of a type left the index
reporting `1` with zero records in the database ([#5601](https://github.com/ArcadeData/arcadedb/issues/5601)).

The tombstones were not the problem - the cursor already skips dead keys, and the work it skipped is accounted in
the `deadEntriesSkipped` stat. The count incremented once per `next()` call, and an LSM index cursor answers
`hasNext()` optimistically: it reports on how many underlying page cursors are still live, not on whether any of
them still holds a surviving RID, so `next()` legitimately returns `null` once a trailing run of tombstones leaves
nothing to emit. That `null` was counted as an entry.

`countEntries()` now counts values rather than calls, and closes its cursor when it is done - a full walk of every
index being the last place that should leak a compacted-series retire guard. The contract is now stated on
`Index.countEntries()`: it is the number of LIVE entries, "entry" is the index's own unit (one per analyzed token
for full-text, one per posting for sparse vectors, one per covering cell for geospatial), and only `HASH` answers
in constant time.

## Geospatial queries stream instead of materialising every candidate

A geospatial query decomposes its search shape into covering cells - a 10x10-degree box resolves into roughly 4,200
of them - and answers each with one prefix range scan or one exact lookup on the underlying LSM-Tree. Every one of
those scans was drained into a candidate set, which was then copied into a list of index entries, before the caller
saw the first row; the SQL layer then loaded every candidate RECORD of every bucket into a second list before the
`geo.*` predicate re-checked the first one. A `LIMIT 10` over a wide-area query paid for the entire candidate set
([#5601](https://github.com/ArcadeData/arcadedb/issues/5601)).

The whole chain is now lazy. The index cursor opens one cell scan at a time and closes it as soon as it is drained,
the SQL function chains the per-bucket cursors the same way, and a consumer that stops early stops the covering-cell
walk with it. Deduplication is still a set of RIDs - a polygon decomposes into many cells, so the same record can be
reached through several of them - but it now holds only what has actually been emitted rather than a second parallel
copy of the whole candidate set. Results are unchanged: the index still answers with a superset that the `geo.*`
predicate re-checks, and a row limit is still never applied to the candidates.

Because an abandoned cursor now can leave an underlying scan open, `FETCH FROM INDEXED FUNCTION` releases it on
`close()` and `reset()`, matching what the regular index fetch step already did: a compacted-series cursor registers
with its file so a full compaction defers dropping it.
## Studio can create a vector index, and a vector index can no longer be created without `dimensions`

Studio's "Add Index" dialog offered `LSM_VECTOR` in the algorithm list but built the statement without a
`METADATA` clause, which the engine refuses outright - and the dialog had no input for the settings the error
message asked for. Creating a dense vector index from the UI was simply impossible
([#5607](https://github.com/ArcadeData/arcadedb/issues/5607)). The dialog now collects **dimensions** (required),
**similarity**, **max connections**, **beam width** and **quantization**, and the sparse branch gained the
matching **weight quantization** selector.

The underlying contract was loose in three places, and all three are tightened:

- **`dimensions` is now enforced, everywhere.** It is the one vector setting with no usable default: every write
  and every graph build compares the candidate vector's length against it, so an index created with `dimensions`
  unset accepted writes and indexed nothing, forever, without a single warning. `CREATE INDEX ... LSM_VECTOR
  METADATA {}` and the equivalent builder call are now refused at creation time. Indexes created before this
  release are untouched - the check runs only when a new index is built.
- **The `CREATE INDEX` error message told the truth about only one of the four settings it named.** It asked for
  `dimensions`, `similarity`, `maxConnections` and `beamWidth`; the last three have defaults (`COSINE`, `32`,
  `100`). It now names `dimensions` as the requirement and lists the rest as optional with their defaults.
- **Index creation failures carry their reason again.** Any error raised while building an index was rewrapped
  as a bare `Error on creating index on type 'X', properties [y]`, discarding the cause's message on the way to
  the SQL and HTTP layers. The reason is now part of the message.
- **A `METADATA` value the vector builder cannot read is a 400, not a 500.** Every value in that clause comes
  from the statement, so an unparsable number (`{"dimensions": "abc"}` escaped as a raw
  `NumberFormatException`), an unknown `similarity` or `quantization` name, or an out-of-range `pqClusters` is a
  client mistake. They are reported as parsing errors now, the same treatment the `GEOSPATIAL` metadata gets.

## Server and cluster status endpoints scope their per-database output to the caller

The routes that enumerate the whole database registry rather than naming one database in the path now reduce
every per-database entry they emit to the databases the caller is authorized for. This covers the `databases`
array and the database-scoped `alerts` of `GET /api/v1/cluster`, the `ha.databases` array of
`GET /api/v1/server?mode=cluster`, and the `metrics.sparseVectorIndexes` map of `GET /api/v1/server`. The
server-level fields of those responses are unchanged and stay readable by any authenticated user - in
particular `ha.leaderAddress` and `ha.replicaAddresses`, which the remote driver reads on every connection to
route requests, and which therefore cannot be restricted to root.

Two cluster endpoints move behind the root check that the seven mutating Raft endpoints already use:

- **`POST /api/v1/cluster/bootstrap-state`** is a peer-to-peer RPC with no browser or driver consumer, and each
  call computes a SHA-256 over every database directory on the node. Peers are unaffected: they reach it with
  the cluster token forwarded as root.
- **`GET /api/v1/cluster?presence=true`** fans that RPC out to every peer to build the presence matrix. The
  matrix answers a whole-cluster question, and every remedy it points to (resync, transfer leadership) is
  itself root-only. The cheap `GET /api/v1/cluster` poll without the parameter is unchanged. Note the root
  check runs before the leader check, so a non-root caller passing the parameter to a **follower** now gets
  `403` where it previously got a `200` with no matrix in it - tooling that polls followers with the flag
  will see the change. In Studio the matrix is loaded by an explicit button, not by the cluster tab's
  auto-poll, so a non-root operator's tab keeps working.

`GET /api/v1/cluster` also stops listing reserved internal databases such as the Raft control directory
`.raft`, matching what the presence matrix and the bootstrap-state RPC already did.

**Full Changelog**: https://github.com/ArcadeData/arcadedb/compare/26.7.2...26.8.1
