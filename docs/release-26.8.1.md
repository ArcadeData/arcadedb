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

## Vector index: the location cache is gone, and `locationCacheSize` is refused

**Behaviour change for operators.** `arcadedb.vectorIndex.locationCacheSize` (and the per-index
`locationCacheSize` metadata) used to cap the in-memory location index of an `LSM_VECTOR` index and let it evict.
That was never a cache bound. A location is the only record of which record a vector id belongs to and where its
entry sits in the index file, and nothing on disk maps a vector id back to an offset, so an evicted entry could
not be recovered - and every reader reads a missing location as deleted. A cap of 100 over 1000 live vectors made
`countEntries()` report 100, and a query whose neighbours had been evicted dropped them
([#5568](https://github.com/ArcadeData/arcadedb/issues/5568)). The same cap arriving through the `METADATA`
clause did the same thing to a 200-vector index: 10 counted, and a search probing a vector with its own embedding
answered with a different vertex ([#5559](https://github.com/ArcadeData/arcadedb/issues/5559)).

The limit dates from when the index held one location per *write*, so it grew with the write history. Issue #5516
removed that: a tombstoned id releases its location, so residency now follows the number of **live** vectors.

- **`CREATE INDEX ... METADATA {"locationCacheSize": N}` now fails** for any positive `N`, with a message naming
  the reason and the figure to size against, instead of being accepted and quietly ignored. Same for
  `TypeLSMVectorIndexBuilder.withLocationCacheSize(N)`, which is deprecated. Accepting it would have left a bound
  in the schema that `schema:indexes` keeps echoing while nothing enforces it - the same silence in the metadata
  that the bug had in the data. **Remove the key from any `CREATE INDEX` script before upgrading.** `-1` and `0`
  ("no limit") are still accepted, so an unset builder and a metadata copy keep working.
- **The global setting is still tolerated** - a startup line carrying it must not stop a server booting - and is
  reported once per index at `WARNING`. So is a schema written by an older version, or the database would not
  open. Neither has any effect. The `low-ram` profile no longer sets it.
- **The bounded backend inside `VectorLocationIndex` is deleted**, not just unused: there is one
  `ConcurrentHashMap`-backed implementation and no `maxSize`, so truncation is structurally impossible rather
  than prevented by policy. That also drops a mode branch from each of the ten methods that had to choose a
  backend, and with it a `Collections.synchronizedMap` monitor that `countEntries()` and `getStats()` used to
  serialize on.
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

## BREAKING: the monotonic engine metrics are Prometheus counters now, and no longer rewind on a database close

Every never-decreasing engine total - page-cache hits and misses, pages read and written, WAL bytes, MVCC
conflicts, the three page-merge counters, transactions, queries, commands - was registered with Micrometer as a
**gauge** ([#5636](https://github.com/ArcadeData/arcadedb/issues/5636)). In Prometheus that means no `_total`
suffix and no type hint that `rate()` and `increase()` are the correct functions over the series, so a dashboard
built on `arcadedb_engine_mvcc_conflicts` showed a line that only goes up and said nothing about whether contention
was rising *now*. They are `FunctionCounter`s from this release, which renames the exported series:

| before | after |
| --- | --- |
| `arcadedb_engine_page_cache_hits` | `arcadedb_engine_page_cache_hits_total` |
| `arcadedb_engine_page_cache_misses` | `arcadedb_engine_page_cache_misses_total` |
| `arcadedb_engine_pages_read` | `arcadedb_engine_pages_read_total` |
| `arcadedb_engine_pages_written` | `arcadedb_engine_pages_written_total` |
| `arcadedb_engine_wal_bytes_written` | `arcadedb_engine_wal_bytes_written_total` |
| `arcadedb_engine_mvcc_conflicts` | `arcadedb_engine_mvcc_conflicts_total` |
| `arcadedb_engine_page_merges_edge_append` | `arcadedb_engine_page_merges_edge_append_total` |
| `arcadedb_engine_page_merges_slot` | `arcadedb_engine_page_merges_slot_total` |
| `arcadedb_engine_page_merges_declined` | `arcadedb_engine_page_merges_declined_total` |
| `arcadedb_engine_tx_write` | `arcadedb_engine_tx_write_total` |
| `arcadedb_engine_tx_read` | `arcadedb_engine_tx_read_total` |
| `arcadedb_engine_tx_rollbacks` | `arcadedb_engine_tx_rollbacks_total` |
| `arcadedb_engine_queries` | `arcadedb_engine_queries_total` |
| `arcadedb_engine_commands` | `arcadedb_engine_commands_total` |

**Existing dashboards and alerts on those names need updating.** The three genuinely instantaneous readings -
`arcadedb_engine_wal_files`, `arcadedb_engine_files_open` and `arcadedb_engine_databases` - go up and down, so they
stay gauges and keep their names.

The rename would have made things worse on its own, because six of those totals were not actually monotonic. The
per-database counters (`tx.write`, `tx.read`, `tx.rollbacks`, `queries`, `commands`, `wal.bytes.written`) were
summed over the **currently open** databases only, so closing or dropping one made the JVM-wide total go
*backwards* - which Prometheus reads as a counter reset, fabricating a rate spike on the next scrape. `Profiler`
now folds a departing database's counters into a retained baseline, so the totals only ever grow for the lifetime
of the JVM.

This changes `Profiler.toJSON()` for every consumer, not just Prometheus, and each of them was quietly wrong
before: the counters it reports are now all-time JVM totals rather than a sum over the databases that happen to be
open. `GET /api/v1/server` (and so Studio's Database Operations table) no longer shows its query and transaction
counts drop when a database is dropped, and its per-minute rates no longer skip a window to avoid publishing a
negative delta. The **query profiler** benefits most: it records a snapshot at start and another at stop and hands
both to Studio to subtract, so a database closing inside the recording window used to make that subtraction come
out short, or negative. The AI chat handler embeds the same JSON descriptively and is unaffected either way.
`Profiler.unregisterDatabase()` is also synchronized now, having been mutating
a plain `LinkedHashSet` that the synchronized `toJSON()` iterates.

## Studio shows a profiler counter sitting at zero instead of hiding it

The profiler-details table skipped any stat whose `count` or `space` was `0`, so an operator could not tell "this
is zero" from "this is not reported" ([#5636](https://github.com/ArcadeData/arcadedb/issues/5636)). For a health
signal whose good state *is* zero - page merges declined, transaction rollbacks - that is backwards. Zero renders
as zero now; only a stat that reports no numeric member at all is still omitted.

## The "not found" message for a missing bucket reaches the user again

`Schema.getBucketById(int)` and `getBucketByName(String)` raise a `SchemaException` in exactly the cases a caller
would test for `null`, so every `if (bucket == null)` written after one of them was dead code
([#5636](https://github.com/ArcadeData/arcadedb/issues/5636)). #5608 fixed one such branch; it had siblings, and
one of them cost a real diagnosis:

- **Reading an `EXTERNAL` property whose paired bucket is not loaded.** The dead branch held guidance written for
  exactly that situation - *"if the bucket was tiered to a secondary path, set `arcadedb.externalPropertyBucketPath`
  to the same value used at creation time and reopen the database"*. A user who hit it got a bare `Bucket with id
  'N' was not found` instead. The write path had no check at all and raised the same bare exception; both paths
  now share one lookup and one message.
- **`TRUNCATE BUCKET`** reported a missing bucket twice (`Bucket not found: Bucket with id '9999' was not found`).
- **`SELECT FROM schema:types`** raised outright when any type mapped a primary bucket to an external bucket that
  was not loaded, instead of skipping that one mapping as the surrounding code intended.
- **`MATCH {bucket: unknown}`** lost its own message and paid for two schema lookups to do it.
- **`SELECT FROM bucket:<id>`** raised a raw `SchemaException` for an unknown id while `SELECT FROM bucket:<name>`
  reported `Bucket 'x' does not exist` - the same mistake with two error contracts.
- **`INSERT INTO bucket:? FROM SELECT ...`** never resolved its parameter at all: it read the literal bucket name
  where its sibling calls the parameter resolver, so the statement failed even for a bucket that exists.

**Watch this if you assert on exception types.** Making those messages reachable also changes what is thrown on
these paths. An unknown bucket in SQL now raises `CommandExecutionException` or `CommandSQLParsingException`
carrying the specific message, where several of these paths previously let a `SchemaException` escape from the
schema layer; an `EXTERNAL` property whose bucket is not loaded raises `SerializationException` with the recovery
instructions on both the read and the write side. This is the intended outcome - a schema-internal exception
reaching the SQL layer was the defect - but code catching `SchemaException` around these calls needs updating.

The API shape is what kept inviting the mistake, so the pattern is closed rather than the instances: `Schema` now
exposes null-returning `getBucketByIdIfExists(int)` and `getBucketByNameIfExists(String)` - named after the
`getFileByIdIfExists(int)` already on the interface - and the throwing forms document that they throw.

Both in-tree implementors (`LocalSchema`, `RemoteSchema`) are updated. Note the two new interface methods are
source-incompatible for anyone implementing `com.arcadedb.schema.Schema` outside the project: such an
implementation needs the two methods added before it compiles against this release.

## An index cursor never hands out a null entry, and a restarted index scan releases its cursors

Iterating an LSM-Tree index cursor could yield a `null` element ([#5635](https://github.com/ArcadeData/arcadedb/issues/5635)).
`hasNext()` answered on how many underlying page cursors were still live, not on whether any of them still held a
surviving RID, so a scan that ended on a run of tombstoned keys said "yes" and then handed the caller nothing.
`IndexCursor` is an `Iterator<Identifiable>`, so `for (final Identifiable r : cursor)` yielded that null, and the
callers that had noticed each carried their own guard against it.

`hasNext()` now prefetches: it runs the merge until it holds an entry it can actually emit, so it is exact, and
`next()` is a pure drain that throws `NoSuchElementException` once exhausted - the contract every other index cursor
already honoured. Two consequences were user-visible:

- `SELECT min(...)` / `SELECT max(...)` read the key off the cursor after `next()`. The trailing null still moved the
  cursor, so on a type whose lowest (or highest) keys had all been deleted the answer was one of those **deleted**
  keys.
- A delete-heavy index reported one entry too many in `countEntries()` - the residual #5601 chased, and the reason it
  survived a full compaction that had already dropped every tombstone.

`getRecord()` and `getKeys()` are settled at the same time: they describe the entry `next()` **last returned**. The old
implementation peeked at the not-yet-consumed value, so a caller reading them right after `next()` saw the following
row.

Separately, `FETCH FROM INDEX` now releases its cursors on `reset()`, not only on `close()`. A restart rebuilds every
cursor from scratch, so the previous run's had to go: a compacted-series cursor stays registered with its file and
`dropRetiredCompactedIndexes` skips a retired file that still has one, for the lifetime of the database. The pending
cursors were not even dropped, so a restarted scan replayed the old, partly consumed ones before reaching the new. The
same release now covers the per-value cursors of a `key IN [...]` lookup, which nothing had ever closed, and the
cursors held by `MIN`/`MAX`, by the full-text term walks, and by the Cypher `NodeIndexSeek` / `NodeIndexRangeScan`
operators - all of which stop before exhaustion by design.

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

## Partitioned types survive a restart, and a later `CREATE INDEX` says what it cost the partitioning

Two loose ends of the partitioned-strategy series, both about what happens to a type *after* it has been configured
([#5637](https://github.com/ArcadeData/arcadedb/issues/5637)).

**`ALTER TYPE ... BucketSelectionStrategy` is persisted.** The strategy was set in memory and nothing wrote it out,
so a type partitioned by that DDL alone came back **`round-robin` after a restart** - unless some later, unrelated
schema mutation happened to flush the configuration first. This hit *correctly* configured types, which is what
makes it worse than the states #5603 refuses: new records were placed round-robin among rows the partition hash had
placed, every partition-aware lookup silently fanned out, and nothing warned. It is the one schema mutator that
left the write to somebody else - the flag *about* the partitioning (`needsRepartition`) saved itself while the
partitioning did not. Databases whose strategy never reached `schema.json` simply need the `ALTER TYPE` re-issued;
placement of existing records is unaffected either way, since the hash is the same one.

The fix is on the mutator, not on `partitioned`, so `thread` stops being lost across a restart on the same terms.
`round-robin` is the default and is deliberately still absent from `schema.json`, which is how reverting to it
persists.

**An index created on an already-partitioned type is diagnosed when it is created.** #5603 refuses an unsuitable
partition at assignment time, but the same state was reachable by reordering the DDL - attach the strategy first,
then create the index that makes it unprunable:

```sql
ALTER TYPE T BucketSelectionStrategy `partitioned('name')`;   -- accepted
DROP INDEX `T[name]`;
CREATE INDEX ON T (name COLLATE CI) UNIQUE;                   -- used to be silent
```

Correctness always held - the strategy declines to prune, so lookups fan out and `UNIQUE` stays global - but
between the `CREATE INDEX` and the next restart nothing said the partitioning had stopped doing anything. The same
applied to an index on non-partition properties, which drew the fan-out advisory at assignment time and nothing
when it arrived later. `CREATE INDEX` now re-runs the same check and reports the result. It never refuses: at
assignment time the strategy is what was asked for and a blocked one is pure cost, whereas here the index is what
was asked for and it is useful, so the partitioning is what gives way. An index change that leaves the partition
exactly as suitable as it was stays quiet.

**A partitioned type whose index has been dropped opens again.** Binding the strategy demanded the unique automatic
index on the partition properties, and it did so on every rebind - including the one the schema loader performs on
every open. With the strategy now reaching `schema.json`, a `DROP INDEX` on the partition key made the next open
throw *from inside the loader*, which aborts everything it has not reached yet: the remaining types' strategies,
the triggers, the function libraries, the extensions, and the compaction file-migration map WAL recovery redirects
through - reported only as `Error on loading schema. The schema will be reset`. Binding no longer validates
anything; the requirement moved to the suitability check, which refuses it at assignment time exactly as before and
warns about it on load. The type keeps its partitioned placement, so records written after the index was dropped
still land where a lookup would look for them. Any other unusable strategy - an implementation class that no longer
resolves, say - now falls back to `round-robin` for that one type with a warning naming it, instead of costing the
rest of the schema.
## Cypher: a subquery body is validated like the query around it

The widening of #5602 left one place the walk could not reach, and the class it was written on said so in the
opposite direction: the bodies of `EXISTS { }`, `COUNT { }` and `COLLECT { }` were "parsed on their own and
validated then". They were not. Each of the three keeps its body as text and re-parses it once per outer row, and
a body that cannot run is absorbed into the expression's neutral value - `false`, `0`, an empty list. A `CALL { }`
body was never handed to this phase at all ([#5626](https://github.com/ArcadeData/arcadedb/issues/5626)).

So `MATCH (n:P) WHERE abs('x') > 0 RETURN n` was rejected before it started, while the identical call one level in,
`MATCH (n:P) WHERE EXISTS { MATCH (m:P) WHERE abs('x') > 0 RETURN m } RETURN n`, was accepted - the very
clause-dependent asymmetry #5602 set out to remove. Neo4j type-checks a subquery body exactly as it does the query
around it.

The three expressions now carry their body as an AST alongside the text, built from the parse tree ANTLR already
produced for it rather than by re-lexing, and the traversal descends into it - as it does into a `CALL { }` body.
Crossing into a body changes the variable scope, so a check that reads variable kinds (`type()` wants a
relationship, `p.name` needs `p` not to be a path) re-binds itself to the kinds the body declares, over the ones it
inherits; an implicit `CALL { }` imports nothing, so a name the body binds for itself shadows the outer one rather
than answering for it. A body's kinds are built by walking its clauses in order, so the two spellings of the same
import - `CALL (p) { ... }` and a leading `WITH p` - answer alike, while `WITH 1 AS p` stops `p` being a path. Each
branch of a `UNION` is a scope in its own right: a variable only one branch declares is checked against the kind
that branch gives it, instead of being dropped for the branches disagreeing about it.

Two expression positions that were leaves for the same reason are covered too: an `EXISTS { }` written as a bare
`WHERE` predicate, and a function call used as one (`WHERE isEmpty(x)`), each used to get an anonymous
`BooleanExpression` adapter that the traversal could only treat as a leaf. Both are now the ordinary
`BooleanCoercionExpression` - byte-for-byte the same behaviour, minus the blind spot. Procedure `CALL` arguments
and the `LOAD CSV FROM` url expression are walked as well.

Working out what a name is - a node, a relationship, a path - was written twice, once for a statement and once
for a subquery body, and the two had already drifted. They are one construction now, which closed an asymmetry
older than this issue: the statement's copy dropped every kind at a `WITH *`, because it kept only what the
projection names, while the body's copy passed the incoming scope through.

> **Potentially breaking, in the same way #5602 was.** A query whose bad call sits inside a subquery body is now
> rejected before it starts rather than failing at runtime - or, where the subquery matched no row, rather than
> quietly answering `false` / `0` / `[]`. The call was always wrong. One shape worth calling out: `type(b)` where
> `b` is a node, written inside a subquery, is now the type error it already was outside one.
>
> A second shape comes from the shared scope construction: a kind now survives `WITH *`, so
> `MATCH p = (a)-[:KNOWS]->(b) WITH * RETURN p.name` is rejected as the path-property access it always was,
> where before the `WITH *` made the engine forget `p` was a path and the query failed at runtime instead. A
> projection that names what it keeps is unaffected - `WITH 1 AS p` still stops `p` being a path.

## An index `METADATA` key is now either applied or reported, and a reopened vector index keeps its metric

> **Upgrading? Two things change behaviour.** Both are detailed below, in full, but in short:
>
> 1. **A `CREATE INDEX ... METADATA` clause carrying an unknown or malformed key is now refused** (HTTP 400 from SQL,
>    `IllegalArgumentException` from the Java builders' `withMetadata(JSONObject)`). Such a key never did anything, but a
>    statement that used to succeed can now fail. Existing indexes are untouched and reading a persisted definition
>    stays tolerant.
> 2. **An existing `EUCLIDEAN` or `DOT_PRODUCT` vector index changes its search results on the first reopen** - it has
>    been scoring with COSINE since the restart after it was created, and now scores with the metric it was created
>    with. Nothing to re-create or rebuild. COSINE indexes are unaffected.

`CREATE INDEX ... METADATA {...}` read the keys it knew with `if (json.has(...))` and dropped the rest, on every
index type except `GEOSPATIAL` ([#5639](https://github.com/ArcadeData/arcadedb/issues/5639)). A typo was therefore
indistinguishable from a correct clause:

```sql
-- succeeded, reported success, and built a COSINE index
CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {"dimensions": 384, "similarty": "EUCLIDEAN"}
```

An unknown key is now refused with the list of the ones the index type accepts, as an HTTP 400. This holds for
`LSM_VECTOR`, `LSM_SPARSE_VECTOR` and `FULL_TEXT`; `GEOSPATIAL` already behaved this way, and an index type with no
settings at all already refused a `METADATA` clause outright. A value of the wrong shape is refused the same way, and
is no longer coerced: `{"dimensions": 8.5}` used to create an 8-dimension index, and `{"addHierarchy": "yes"}` used to
*disable* the setting being asked for, because a string that is not `"true"` reads as `false`.

**Upgrade note.** This is the one behaviour change to be aware of: a stored `CREATE INDEX` script or migration that
carried a stray or misspelled `METADATA` key used to run and is now refused. That is the point of the change - the key
was never doing anything - but a statement that "worked" before can now fail, so check any generated DDL against the
accepted keys, which the error message lists. Existing indexes are untouched; only new `CREATE INDEX` statements are
validated, and reading a persisted definition stays tolerant of the structural keys it carries.

Two consequences for **embedded (Java) callers** specifically:

- The strict reading applies to `TypeLSMVectorIndexBuilder.withMetadata(JSONObject)` and its full-text, sparse and
  geospatial counterparts, not only to SQL. A caller that passed an extra key for forward compatibility now gets an
  exception. Note in particular `buildGraphNow`, which is a directive of the SQL layer rather than an index setting: the
  SQL path consumes and removes it before the builder sees it, so a Java caller passing it through `withMetadata` is
  passing a key the builder has never understood, and now hears about it. Restoring an exported definition has its own
  entry point, `withPersistedMetadata(JSONObject)`, which tolerates the structural keys such a definition carries.
- `BucketLSMVectorIndexBuilder` no longer exposes its settings as public fields (`dimensions`, `similarityFunction`,
  `encoding`, `maxConnections`, ...). Those fields *were* the defect - a setting added to the metadata had to be
  remembered there too, and two never were - so they are gone rather than kept in sync. Every fluent `withX()` method is
  preserved (with `withEfSearch` added), and `getVectorMetadata()` returns the whole configuration as one object.

Four dense-vector settings were unreachable behind that silence, and are now settable and persisted:

- **`efSearch`** and **`inactivityRebuildTimeoutMs`** were never read from the clause, so the search-time
  recall/latency knob could only be set per query. Two of ArcadeDB's own tests believed they had disabled the
  inactivity rebuild through `METADATA` and had not.
- **`neighborOverflowFactor`** and **`alphaDiversityRelaxation`** were read, then lost one hop later: the settings
  travelled from the type-level builder to the per-bucket index through a hand-written field-by-field copy, and that
  copy had fallen behind. `TRUNCATE TYPE` and `REBUILD INDEX` lost the same four settings when re-creating an index.
  Every setting now travels as one `LSMVectorIndexMetadata`, so there is no second field list to keep in sync.

**A `EUCLIDEAN` or `DOT_PRODUCT` vector index came back up as `COSINE` after a restart.** The persisted definition
names the metric `similarityFunction` while the reader looked only for `similarity`, so nothing restored it and every
search after a reopen scored with the wrong metric against a graph built with the right one. Both spellings are read
now. The persisted definition also carries every remaining knob, so a setting no longer silently reverts to its
default on the next restart.

**Upgrade note, and the one with visible effects.** If you have an existing `EUCLIDEAN` or `DOT_PRODUCT` vector index,
its searches have been scoring with COSINE since the first restart after it was created. On the first reopen after this
upgrade it scores with the metric it was created with - so **distances and result ordering will change**, and they
change to what was asked for. Nothing needs re-creating, re-importing or rebuilding: the graph was always built with
the correct metric, only the search side disagreed with it, and the fix is applied on open. An index created with the
default COSINE is unaffected in every respect. If you have been compensating for the old behaviour anywhere - a tuned
distance threshold, a golden-file test of result order - that is the thing to re-check.

Finally, restoring a vector index from an exported definition (`IMPORT DATABASE` of a JSONL dump) goes through a
distinct entry point: an exported definition legitimately carries structural keys - `type`, `bucket`, `version`, ... -
that the `METADATA`-clause reader has to reject as typos.

## Cypher: a hop onto an already-bound vertex counts every relationship joining the pair

> **Upgrading? Row counts can go up, and that is the fix.** A pattern with a hop onto an already-bound vertex - most
> commonly a cycle, whose closing hop always has both endpoints bound - was returning fewer rows than it should
> wherever parallel edges join the same pair. It returns them all now, so `count(*)`, `collect()` and `sum()` over
> such a pattern report larger numbers than they did in 26.7.2. The old numbers were under-counts, not a different
> convention: a saved report, a golden-file test or a threshold calibrated against them is the thing to re-check. A
> graph with at most one edge per pair per type is unaffected in every respect - there was nothing to under-count.

A pattern relationship matches once per relationship. Two parallel edges between the same two vertices are two
matches, whether or not the pattern names them - `MATCH (a)-[:R]->(b)` over a pair joined twice returns two rows,
the same as Neo4j. The optimizer's operator for the hop whose far end is already bound did not: built as a
semi-join ("is this pair connected?"), it answered once per input row and threw the rest of the pair away
([#5663](https://github.com/ArcadeData/arcadedb/issues/5663)).

The shape where it shows is a cycle, because the hop that closes one always has both endpoints bound:

```cypher
CREATE (a:Account {code: 'HUB'})
CREATE (t:Txn {ref: 'SHARED'})
CREATE (a)-[:INITIATED {kind: 'payment'}]->(t)
CREATE (a)-[:INITIATED {kind: 'refund'}]->(t)
CREATE (t)-[:INITIATED {ref: 'REVERSED'}]->(a)

MATCH (a:Account {code: 'HUB'})-[r1:INITIATED]->(t:Txn {ref: 'SHARED'})-[r2:INITIATED]->(a)
RETURN r1.kind
```

The cycle can be walked two ways, one per first-hop edge, each closing through the single returning edge -
relationship uniqueness is satisfied because `r1` and `r2` bind different edges in both walks. The answer was one
row. Anything aggregating over such a pattern (`count(*)`, `collect(r1)`, `sum`) silently under-reported wherever
parallel edges exist between a pair, which is normal in transaction and payment graphs. The step-by-step executor
had always got this right, so the same query could answer differently depending on which plan was chosen.

The operator is an expansion now: it walks the relationships joining the pair and emits one row per relationship,
binding the relationship variable and enforcing relationship uniqueness against the hops that precede it in the
same `MATCH` clause - including when the hop is anonymous, whose edge a later hop in the clause must still not
reuse. It keeps what made it fast: the source's edge list is filtered on the neighbour pointer held in the edge
segment, so an edge that does not reach the target costs a pointer comparison rather than a record load, and only
the edges that do reach it are ever materialised. An undirected hop reaches a self-loop from both adjacency lists
and still reports it once. The CSR-backed variant reads the multiplicity off the sorted adjacency array instead of
stopping at the first hit, and now steps aside for the edge-list walk when the hop has to tell one relationship
from another - which adjacency ids cannot do.

A graph analytical view keeps its pending deletions per `(source, target)` pair, because the CSR holds adjacency
ids and has no edge identity to key on, so deleting one of several parallel edges masks all of them. That is why
`isConnectedTo` can report a pair as gone while two of its edges remain. A boolean can absorb that; a row count
cannot. A masked pair is therefore reported as "cannot say", and the hop falls back to the edge list, which is
exact - so a `SYNCHRONOUS` view that has seen such a delete answers the pattern with the right number of rows
rather than none.

One related source of confusion is gone with it: two equally cheap hops were tie-broken by a `HashSet` iteration
order over identity hash codes, so the same query could be planned differently from run to run. Ties now fall to
the order the hops are written in.

## `.github/dependency-review-config.yml` expresses the policy the action actually reads

The file was organised into `security:`, `licensing:`, `packages:`, `changes:`, `exemptions:`, `notifications:`,
`advanced:` and `reporting:` sections. `actions/dependency-review-action` takes a **flat** configuration and silently
drops every key it does not know, so the whole file was inert while reading as an enforced supply-chain policy: the
minimum versions for `jquery`, `bootstrap` and `datatables.net` and the denial of the compromised `event-stream` and
`flatmap-stream` were never applied. Had the file been live it would also have rejected dependencies the project
permits, since its `licensing.deny` listed `EPL-1.0`, `EPL-2.0` and `LGPL-2.1` - which `CLAUDE.md` allows and the
current tree already ships.

The policy is now written in the schema the action reads (`fail-on-severity`, `fail-on-scopes`, `deny-licenses`,
`deny-packages`), aligned with the allowed/forbidden lists in `CLAUDE.md`, and the workflow step no longer passes
inline inputs that would override the file. The per-package minimum versions moved to the workflow's existing
`Validate Package.json` step, which is the only place they can actually be enforced.

Deliberately, **nothing that currently passes CI starts failing**: `fail-on-scopes` stays at `runtime`, the action's
default and therefore what this job has effectively been enforcing all along. The old file asked for `development` too,
and expressing that would have been faithful to its intent, but it would have tightened a shared gate as a side effect
of repairing a config that enforced nothing - a decision worth taking on its own merits. Development dependencies are
still covered by the `npm audit --audit-level=moderate` step of the same workflow.

## An index cursor is `AutoCloseable`, and a leaked one no longer pins a retired index file forever

Three releases in a row have fixed instances of the same defect - an `IndexCursor` obtained, driven partway and
dropped ([#5601](https://github.com/ArcadeData/arcadedb/issues/5601),
[#5609](https://github.com/ArcadeData/arcadedb/issues/5609),
[#5635](https://github.com/ArcadeData/arcadedb/issues/5635)). Closing one matters: a scan over an LSM index holds one
underlying cursor per compacted series, each registered with its file, and `dropRetiredCompactedIndexes` will not
physically drop a file that still has one. Draining a cursor releases those registrations as each series is exhausted,
so only a cursor abandoned **partway** leaks - which is exactly the shape a `LIMIT`, an `exists()` or an early `break`
produces. They kept recurring because nothing fired: an abandoned cursor looks exactly like correct code, and every
site so far was found by reading ([#5662](https://github.com/ArcadeData/arcadedb/issues/5662)).

`Cursor` now extends `AutoCloseable`, so an abandoned one is a static-analysis and IDE finding rather than something
only a careful read can catch, and a cursor can be used with try-with-resources. `close()` is declared without a
checked exception, so a `try (final IndexCursor c = index.iterator(true))` does not force the caller into a
`catch (Exception)`.

That is a compile-time signal, and a compile-time signal proves nothing about the sites nobody has read yet. So the
counter behind the retire guard was replaced with **weak** references to the live cursors: a cursor abandoned without
`close()` stops pinning its file once the garbage collector reaches it, and the reclaim is logged with the index name.
A missed `close()` used to be permanent - the retired file stayed on disk for the lifetime of the database with
nothing left in the process that could ever release it, and nothing to say so.

The call sites found in this pass and now releasing their cursor:

- The native `Select` API. `exists()` returns on the first match and `count()` stops at its `LIMIT`, both abandoning
  the scan, and `SelectIterator.close()` documented itself as having "nothing to release in the serial
  implementation" while holding the source index cursor.
- `SubQueryStep` never closed the plan it wraps, so closing a result set reached only the outer plan's steps. A
  `DELETE ... WHERE` is built exactly this way - the index scan inside the sub-plan, the `LIMIT` outside it - so its
  scan was released only when it happened to run to exhaustion.
- `DeleteFromIndexStep` had no `close()` at all. Two smaller repairs travel with it, neither of which changes what a
  working statement does: an `IOException` out of its initialisation printed a stack trace to stdout and carried on
  with a null cursor, so the caller would have seen a `NullPointerException` instead of the failure that explained it;
  and the branch for an index without ordered iterations opened a `range()` cursor and overwrote it with the
  `iterator()` one on the next line - it could never have worked, since both of those calls raise
  `UnsupportedOperationException` on the only index that reaches it, so it was removed in favour of the error that
  names the condition.
- The Gremlin index-filter step, `TypeIndex`, the unique-key check at commit time, the edge upsert lookup, and the
  full-text scoring, explain and more-like-this walks.

Two smaller corrections travel with them. The vector search cursor returned the backing list's own iterator from
`iterator()`, so a for-each got a second, independent traversal that never moved the cursor's position - `getRecord()`
reported nothing during the loop, and mixing it with `next()` read some RIDs twice; it now iterates itself, like every
other cursor, and `IndexCursorCollection` does the same. And `MultiIndexCursor.getComparator()` /
`getBinaryKeyTypes()` answered by probing the children for one that still had something left, which since #5635 means
running page reads and tombstone skips inside two plain accessors; both now answer from state sampled at construction,
and the key types no longer come back null once the cursor is exhausted.
## Cypher: a subquery body is now part of the query rather than a string it carries

> **Upgrade note - behaviour change.** A query whose `EXISTS { }`, `COUNT { }` or `COLLECT { }` body **fails on real
> data** now returns that error instead of a wrong answer. Until now any exception raised while the body ran was
> swallowed and answered with the expression's neutral value - `false`, `0`, `[]` - so a body that failed on some rows
> and not others produced results that looked complete. If a body of yours errors on a subset of rows (an `abs()` over
> a property that is occasionally a string or null is the usual shape), the query that used to return rows will now
> raise. That is the point: the old answer was wrong, not merely quiet. Neo4j raises here too. The failures worth
> knowing about are below.

`EXISTS { }`, `COUNT { }` and `COLLECT { }` did not hold their body as part of the query. They held it as **text**,
edited that text once per outer row to correlate it, ran it through `database.query("opencypher", ...)` as a
standalone statement, and absorbed any failure into the expression's neutral value. Three things follow from that,
and all three are fixed.

**A failing body is reported instead of being answered.** A body that could not run was answered exactly like a body
that did not match: `false` for `EXISTS`, `0` for `COUNT`, `[]` for `COLLECT`, with nothing above `FINE` to say so.
The two are not the same thing, and the difference is load-bearing:

```cypher
MATCH (a), (b) WHERE NOT EXISTS { MATCH (a)-[:E {id: $id}]->(b) } CREATE (a)-[:E {id: $id}]->(b)
```

If that body threw for any reason, `EXISTS` answered `false`, `NOT EXISTS` answered `true`, and a de-duplicating
guard degraded into an unconditional `CREATE`. It also meant a retryable `ConcurrentModificationException` raised
inside a body could never reach the server's automatic retry, since it had already been swallowed. Failures now
propagate, as they do in Neo4j. **This is a behaviour change:** a query whose subquery body fails on real data - a
type error on a property, a lock timeout, a security violation - now returns that error instead of a wrong answer.

**The body is run, not rewritten.** What executes is the parsed body, with the outer row handed to it as a seed row -
the same mechanism a `CALL { }` clause already used. The text rewriting is gone from that path, and with it the class
of bugs it produced: an injected `AND` binding tighter than an inner `OR` (#4995/#5165), an inline pattern predicate
mistaken for the clause-level `WHERE` (#5464), a clause keyword missing from a keyword table (#5461), a `SET` inside
user data read as a clause (#5541). Two blind spots that a text scan could not fix are fixed as a consequence: an
update keyword inside a comment, and `COLLECT { WITH 1 AS set RETURN set }`, are no longer rejected as writes.

**Every validation phase reaches inside a body.** Twelve run on a statement and ten of them used to stop at the
subquery boundary, so a mistake rejected when written one way was accepted written one level in:

```cypher
-- rejected
MATCH (n:P) RETURN count(count(n)) AS r
-- accepted, until now
MATCH (n:P) RETURN COUNT { MATCH (m:P) RETURN count(count(m)) } AS r
```

The same held for a negative `SKIP`/`LIMIT`, a duplicated column name, `RETURN *` with nothing in scope, and the
column-name and `UNION`/`UNION ALL` checks of a `UNION` written as a body. The boundary is now a property of the
validator itself rather than something each phase has to know about, so a phase added later is inside a body from
the day it is written.

One check widened beyond subqueries as part of this. A repeated relationship variable - `(a)-[r]->()<-[r]-()`, a
pattern asking for a relationship that is two different ones at once - was rejected only when written as the query's
own `MATCH`. It is now rejected wherever the pattern appears, including a `WHERE` pattern predicate and a subquery
body, matching Neo4j. Those two spellings used to answer "no match" instead.
## Deleting an edge no longer leaves its back-reference behind under concurrency

Removing an edge disconnects it from both endpoints and then deletes the edge record. The disconnection walked the
endpoint's edge-list chain with best-effort reads: the head chunk came from a helper that answers `null` when it
cannot be loaded, the chain hops used a plain lookup, and `deleteEdge` wrapped the lot in a
`catch (RecordNotFoundException)`. All three read "chunk unreadable" as "nothing to remove here" - and then the edge
record was deleted anyway.

Under concurrency a chunk is regularly unreadable for reasons that say nothing about the graph. A commit publishes
its pages one at a time and a reader takes no commit lock, so a vertex page can expose a new edge-list head RID a
moment before that head's own page becomes visible; and a chunk emptied by another transaction is relinked out of
the chain while a walker is still following a pointer to it. Hitting either window ended the removal without having
removed anything, so the back-reference outlived its edge: the endpoint reported one edge too many
(`countEdges`, `both()`, `in()`/`out()`) and `check database` reported one broken link. On a hot vertex - the
super-node shape this happens on - the window is narrow, which is why it surfaced as a rare, unexplained off-by-one
rather than as a reproducible failure.

The append path already answered exactly this window with a retryable `ConcurrentModificationException`. The removal
path now does the same: a head chunk that is present but unreadable, and a chain hop that cannot be read, are
retryable conflicts, so the transaction re-reads a consistent view and completes the removal instead of committing a
half-done one. `null` from the write-side head lookup now means one thing only - the vertex has no edge list in that
direction, so there is genuinely nothing to remove. Tolerance for an endpoint **vertex** that no longer exists is
unchanged: there is nothing to disconnect from a vertex that is gone.

**Visible effect, and it is not limited to deleting an edge.** Three operations disconnect an edge and therefore
share the new contract:

- `edge.delete()` / `DELETE EDGE`,
- moving an edge, which disconnects it the same way,
- **`vertex.delete()` / `DELETE VERTEX`** - the widest reach of the three. Deleting a vertex disconnects each of its
  edges from the vertex at the *other* end, so the strict read lands on a **neighbour's** edge list. A vertex that is
  itself perfectly healthy can now fail to delete because a neighbour's list could not be read at that moment.

Any of them racing a concurrent write can now raise a `ConcurrentModificationException` where it previously
"succeeded". That is a `NeedRetryException`, so the standard retry loop (`database.transaction(...)`, and the
server's auto-retry for single-request commands) absorbs it. A client-managed explicit transaction spanning several
requests sees it and should retry the transaction, which is the same contract concurrent updates have always had.
Best-effort callers are unaffected: iteration, counting and the opportunistic pruning of an already-dangling
reference during a read still skip a momentarily unreadable chunk rather than failing.

The other side of that trade, taken deliberately: an edge list that is not transiently invisible but genuinely
broken is indistinguishable from one at that moment, so the operations above now fail instead of completing and
leaving the back-reference behind. For `DELETE VERTEX` that means a healthy vertex next to a corrupted one is not
deletable by the normal path until the corruption is repaired - succeeding would delete the edge record while the
neighbour keeps pointing at it, dangling a reference on a vertex nobody asked to touch. `CHECK DATABASE` remains the
repair path: it rebuilds a chain that cannot be loaded and drops the references into it.

**If you hit that, the recovery is `CHECK DATABASE ... FIX` and then retry the delete.** The symptom is a delete that
keeps failing with `ConcurrentModificationException` on the same vertex however often it is retried, which is what
tells a broken list apart from ordinary contention (that one succeeds on a retry). The repair is never blocked by the
delete being blocked: `CHECK DATABASE` reads edge lists through the best-effort reader, so it can still walk, rebuild
and re-link a chain that the delete path now refuses.

Note also that on a hot super-node under heavy concurrent delete-and-append the transient window is now answered with
a retry rather than passing silently, so those transactions retry slightly more often than before. That is the
intended shift - from "quietly wrong" to "occasionally repeated" - and it lands on exactly the super-node shape the
bug affected. `arcadedb.txRetryDelay` and `arcadedb.txRetries` are the tuning levers if a workload needs them.
## Deleting a vertex no longer loses its own edges when a chunk is momentarily unreadable

The change above closed that window for the edge lists a delete reaches through a *neighbour*. It left open the one
a vertex delete reaches on its own list, which is where the loss was larger.

`DELETE VERTEX` first collects the edges to remove by walking the vertex's own outgoing and incoming lists, then
deletes each collected edge, then deletes the vertex record. That collection used the best-effort reader - the one
that answers `null` when a head chunk cannot be loaded - and wrapped the whole walk in a blanket
`catch (Exception)`. So a chunk that could not be read at that instant meant **no edges collected**, or only the
ones seen before the hole, and the vertex record was deleted on top of that empty or partial view. The delete
reported success while the edges outlived their endpoint: an edge record whose `out`/`in` names a record that no
longer exists, and a back-reference still sitting on a neighbour nobody touched. `check database` reported them as
invalid links.

This is the same timing window as above, not a fact about the graph: a commit publishes its pages one at a time and
the reader takes no commit lock, so a vertex page can expose a head RID before that head's own page is visible, and
a chunk emptied by another transaction is relinked out of the chain while a walker is still following a pointer to
it. Measured on a two-vertex graph with the source's outgoing head chunk made unreadable, the delete succeeded, the
vertex was gone, and the edge record and the neighbour's degree were both still there.

The collection now reads the list the way a removal must. An unreadable head chunk, or an unreadable hop in the
middle of the chain, is a retryable `ConcurrentModificationException`: the transaction rolls back whole and re-reads
a consistent view instead of deleting a vertex it never finished disconnecting. The same applies on a promoted
super-node, whose list is several per-stripe chains: a read is allowed to skip a stripe it cannot load, and that
skip used to cost the delete a whole stripe's worth of edges in silence. Two things stay deliberately
tolerant. A single entry whose **edge record** cannot be resolved is still skipped - that costs one already-dangling
pointer rather than the whole remaining list, which is how every other reader in the engine treats it. And draining
the chunk records at the very end is still best-effort in every mode: by then each edge has been disconnected from
both endpoints and the vertex record is about to go, so a chunk that cannot be read there leaves orphaned chunk
records for `CHECK DATABASE` to reclaim, never a surviving reference.

**Visible effect.** `vertex.delete()` / `DELETE VERTEX` can now raise a `ConcurrentModificationException` where it
previously "succeeded" - the same contract, and the same standard retry loops, described for edge deletion above.
As there, an edge list that is genuinely broken rather than transiently invisible is indistinguishable at that
moment, so the delete fails once the retries are spent instead of quietly leaving ghost edges behind.
`CHECK DATABASE ... FIX` is the repair path and is never blocked by the delete being blocked: it rebuilds an
unloadable chain from the surviving edge records, after which the delete goes through normally.

**`force` is now the escape hatch it always claimed to be.** The internal forced delete - the path that removes a
record whose own body cannot be assembled - keeps the old tolerance for every one of these reads, and extends it to
one place it never covered: disconnecting a collected edge from the vertex at its *other* end. Since the change
above made that removal strict, a broken **neighbour** blocked a forced delete exactly as it blocked an ordinary
one, which is precisely what `force` exists to override. It no longer does; the reference it could not remove is
logged as a warning naming the edge, and `CHECK DATABASE` cleans it up.

One more silent-loss route closed with it: the collection now reads the edge-list heads from the vertex instance the
running transaction holds. A handle obtained before an edge was appended in the same transaction still named the
previous head, so the newest edges were invisible to the walk - and, once again, the vertex was deleted anyway.

## A `HASH` index can key on a `LINK`, and an unsupported key type is refused at creation

`CREATE INDEX ON INITIATED (`@out`, `@in`) UNIQUE_HASH` - the structural way to enforce edge de-duplication - was
accepted, and then every insert failed:

```
com.arcadedb.index.IndexException: Unsupported key type for hash index 'INITIATED_0_...' (fileId=5): 14 (0x0E) while
parsing entry at content offset 11. Loaded key types=[14(0x0E)=INVALID, 14(0x0E)=INVALID]. This means the index
metadata or a bucket page is corrupted; rebuild the index (DROP and recreate it).
```

A `LINK` (and therefore an edge type's `@out`/`@in`) encodes as `TYPE_RID`, which the hash bucket's key-size, compare
and validation paths did not recognise: only `TYPE_COMPRESSED_RID` was handled, so every key-length computation over
such an entry fell through to the "corrupted" branch. Endpoint-keyed de-duplication was therefore `LSM_TREE`-only, and
the failure said nothing about it.

`LINK` keys are now stored the same way the bucket already stores its entry values - as varint-compressed RIDs. That
was the cheaper of the two fixes to make correct: the fixed 4+8 byte `TYPE_RID` form costs 12 bytes per key column,
against 2-7 for the varint form, so on the composite `(@out, @in)` key that motivates such an index it is roughly half
the key bytes, and half the pages. Both encodings are deterministic and injective, so hashing and key comparison are
unaffected. The metadata page keeps recording the schema type, so the on-disk format is unchanged and
`getKeyTypes()`/`getBinaryKeyTypes()` still report `LINK`/`TYPE_RID` - the compression is internal to the bucket.

The second half of the report was the message itself. Nothing was corrupt: the index had never been able to hold that
key type, so "rebuild the index (DROP and recreate it)" pointed at a remedy that could not work and implied data loss
that had not happened. A key type a hash index cannot encode is now refused when the index is created, before any file
exists, naming the type and the supported ones:

```
Cannot create index 'Doc_0_...' of type HASH because the key type LIST cannot be used as a HASH index key.
Supported key types are: BOOLEAN, INTEGER, SHORT, LONG, FLOAT, DOUBLE, DATETIME, STRING, BINARY, LINK, BYTE, DATE,
DECIMAL, DATETIME_MICROS, DATETIME_NANOS, DATETIME_SECOND. Create the index as LSM_TREE instead
```

The runtime error that remains - reachable only from a damaged metadata page, or an index created before that check -
now names the key column, states that no record data is lost, and distinguishes the two causes instead of asserting
corruption.

**Downgrade note.** Nothing needs migrating on upgrade: the metadata page records the same byte it always did, and a
`LINK` HASH index created before this release cannot hold data anyway - it failed on the first insert - so an existing
one simply starts working. The compatibility runs one way only, though. A `LINK` HASH index created *with* this
release is not readable by an earlier build: the old loader does not recognise `TYPE_RID` as a key type and flags the
metadata page as corrupt. The database still opens and every other index is unaffected, but that one index has to be
dropped before going back. `LSM_TREE` indexes on the same properties are unaffected in both directions.

## HTTP: a truncated query response is no longer indistinguishable from a complete one

The HTTP query and command endpoints serialize at most a fixed number of rows into a response, 20,000 by default.
The cap was applied after the engine had produced the rows and was reported nowhere: same `200`, same body shape,
no flag and no count. A caller that paged by writing `LIMIT` into its own SQL - the obvious thing to do - got
20,000 rows for any page larger than that and nothing in the answer said so
([#5711](https://github.com/ArcadeData/arcadedb/issues/5711)).

Two things were wrong, and both are fixed.

**A limit the caller stated is now honored as written.** The row cap is the `limit` field of the request when
there is one; otherwise it is the server default, raised to the LIMIT the query itself carries. So
`SELECT i FROM R LIMIT 30000` now returns 30,000 rows over HTTP exactly as it does embedded, while a query
stating no limit at all is still capped. The query's LIMIT only ever raises the cap: a smaller LIMIT is already
enforced by the engine, so the serializer has nothing to add. This also removes an asymmetry between the two
surfaces: `GET /query` already read the LIMIT back from the execution plan, `POST /query` and `POST /command`
did not.

**When the default cap does bite, the response says so.** The query and command endpoints now always report:

```json
{ "result": [ ... ], "limit": 20000, "returned": 20000, "truncated": true }
```

`truncated` is true only when the cap stopped the serialization with at least one row still pending, so a result
that ends exactly at the cap is not flagged. The server also logs a warning naming the database, the cap and the
query - but only when the *default* did the cutting, since truncation against a `limit` the caller asked for is
the expected outcome.

The flag is exact for the row-oriented serializers (`record`, `studio` and the default one), where `returned` also
never exceeds `limit`. With `serializer: "graph"` the cap counts graph *elements* instead of rows, so two things
differ: `returned` can come back above `limit`, because the row that reaches the cap is expanded whole; and since
the same vertex reached twice is serialized once, `truncated` is best-effort there - a result whose rows dedup
down to fewer elements than the cap can read `false`. A paging client should use a row-oriented serializer.

`POST /timeseries/{db}/query` reports `limit` and `truncated` the same way, logs the same warning when its own
default did the cutting, reads the same setting instead of its own hardcoded 20,000, and no longer answers a
non-positive `limit` with zero rows (it used to compute `min(rows, -1)`, so `limit: -1` returned nothing at all).

The cap is now configurable with **`arcadedb.server.httpQueryDefaultLimit`** (default 20000, `-1` for unlimited);
it applies only to callers that state no limit of their own.

One gap is worth knowing about: a query's LIMIT raises the cap only for a language that exposes it on the
execution plan. SQL does; Cypher on the non-EXPLAIN path builds no plan, so `MATCH ... RETURN n LIMIT 30000` is
still cut at the default - now with `truncated: true` and a warning rather than silently. Send `limit` in the
request for those languages.

**Java remote driver.** `RemoteDatabase` warns when the server reports a truncated result instead of handing back
a partial `ResultSet` that looks complete, and `setMaxResultRows(Integer)` sets the cap for that connection -
`-1` removes it, so a query with no LIMIT of its own returns every row like the embedded API does. Beware that
removing the cap makes the server materialize the whole result set in memory before answering.

**Studio** marks the row count with `(truncated)` when the result it is showing was cut short.

Three smaller behaviour changes come with this. A serializer name other than `graph`, `studio` or `record` used
to emit one row *above* the cap; it now emits exactly the cap, like the others. On `GET /query`, an explicit
`limit` query parameter now wins over the LIMIT in the query text (it used to be the other way round), which is
what the POST endpoints have always done. And `"limit": 0` now means unlimited everywhere, as it already did in
the serializer: it used to be pushed down as a literal `LIMIT 0` and return no row at all for a query that
carried no LIMIT of its own, while returning every row for a query that did.
## A `HASH` index refuses a page size its bucket pages cannot address

A `HASH` index accepted any page size:

```java
database.getSchema().buildTypeIndex("Entry", new String[] { "k" })
    .withType(Schema.INDEX_TYPE.HASH).withUnique(true)
    .withPageSize(131_072)     // accepted without complaint
    .create();
```

and then corrupted itself on insert:

```
com.arcadedb.index.IndexException: Detected cycle in hash index 'Entry_0_...' (fileId=2)
  overflow chain at page 17328897 (totalPages=3). The index is corrupted, please rebuild it
  (DROP and recreate it).
```

The page number in that message is not a page. Everything inside a hash bucket page - the slot directory entries, the
`dataEnd` marker, the entry count - is written as a `short` and read back through `& 0xFFFF`, so the largest offset a
bucket page can address is 65535. Above a 65536-byte page the offsets truncate: `dataEnd` wraps to a low value, the
free-space calculation reports the page as almost empty, and the next entry is written over the page header - including
the overflow pointer. The cycle detector then reports the resulting garbage pointer as a corrupted chain, which is a
true statement about a database that a legal-looking configuration had already destroyed.

The page size is now validated where the file is created, so no `HASH` index file can exist with a size its own reader
cannot address:

```
Cannot create index 'Entry_0_...' of type HASH with a page size of 131072 bytes: a hash bucket page addresses its
entries with 16-bit offsets, so the page size must be between 256 and 65536 bytes. Use LSM_TREE if a bigger page is
required
```

The floor is checked too - below it the metadata page itself does not fit - and an index whose file already declares an
illegal page size (created by an earlier release) is reported rather than refused: the database still opens, a `SEVERE`
line names the index at load, and `CHECK DATABASE` lists the page size instead of the cyclic chain it causes, so the
report points at the cause rather than the symptom.

`REBUILD INDEX` is the remedy for such an index, and it repairs it: a rebuild drops the index and recreates it carrying
the old configuration over, so an unaddressable page size would have been handed straight back to the guard that just
started refusing it - deleting the index and then failing to build its replacement. The rebuild now asks the index for
a page size it is legal to *create* with, which for a `HASH` index whose current one is unaddressable is the default.
The same accessor is used when an index is propagated to a new bucket or to a sub type, so neither operation can fail
because of a page size inherited from an older release.

### `withPageSize()` on a `HASH` index means what it says

The defect had one page size it could not reach, and that is why it stayed hidden. `IndexBuilder.pageSize` was
initialised to the LSM default (262144), and the hash factory read that exact value back as "the caller did not ask for
one", remapping it to the hash default of 65536. So the most natural oversized value to try was the single value that
was silently made safe, while 131072 or 100000 corrupted.

The builder now carries an explicit unset marker, so asking for a page size is distinguishable from not asking. Two
consequences for callers:

- `withPageSize(262_144)` on a `HASH` index is now refused with the message above, where before it silently produced a
  65536-byte index. Not passing a page size still yields the hash default, unchanged.
- `CREATE INDEX ... UNIQUE_HASH` and a `HASH` index inherited from a super type used to hardcode the LSM default as
  their stand-in for "unset"; both now leave it unset, and the inherited one carries over the page size of the index it
  is propagating rather than resetting it.
Widening the on-page fields to 32 bits would lift the ceiling instead, at 2 bytes per slot on every bucket. The
measurements in #5712 point the other way - smaller pages are consistently faster for this index - so the ceiling is
not worth paying for.

### BREAKING: `withPageSize(0)` now means "use the default", for every index type

This one is not about `HASH`, and it is the change most likely to reach code that has nothing to do with this fix.
`IndexBuilder.withPageSize()` accepted a non-positive page size and passed it through to the component, where the page
arithmetic divides by it. It now means "unset", so **every** index type - `LSM_TREE`, `FULL_TEXT`, `GEOSPATIAL`,
`LSM_VECTOR`, `LSM_SPARSE_VECTOR` as well as `HASH` - falls back to its own default instead.

Nothing could have relied on the old behaviour usefully: a zero page size produced a broken file rather than a small
one. But a caller that passed `0` expecting a failure now silently gets a working index at the default size, so if you
build indexes through the embedded API with a computed page size, check that the computation cannot yield zero.

An index that already exists on disk is unaffected either way - the page size is read from the component file, not from
a builder.

## `LSM_VECTOR`: a search aimed at deleted vectors now finds the survivors instead of nothing

Deleting a vector does not take it out of the HNSW graph. It tombstones the location and leaves the node in place until
the next graph rebuild, which for any index above a thousand vectors is thousands of mutations away. A query whose
vector landed in a neighbourhood that had been deleted therefore walked into a region where nothing was left to return,
and answered accordingly - `[]`, while the rest of the index was still there and still searchable with any other query.
An application that deletes a topical cluster (a tenant, a document set, a time range) leaves exactly that hole, in
exactly the place queries about that topic land.

Two independent causes, both fixed:

- **The traversal was told every node was an acceptable answer.** JVector's beam stops as soon as it holds enough
  *acceptable* candidates, so a beam that filled with tombstones declared itself finished; the post-filter then removed
  them one by one and the caller got a short list, or an empty one. Search now passes a filter that accepts only
  ordinals still mapping to a live vector - the same predicate the post-filter applies, so nothing that would have
  survived is lost. Tombstones are still *traversed* (JVector expands a rejected node, it just does not collect it), so
  the vectors behind the hole stay reachable and the beam keeps walking until it has found them.
- **A tombstone was scored through a placeholder vector.** The placeholder was a vector of `Float.MIN_NORMAL`s, chosen
  on the theory that it would score very low. It does not: cosine cancels the magnitude out, and the squared magnitude
  underflows to zero in float, so the similarity came back `Infinity`. Every tombstone was therefore the *best*
  candidate in the beam, displacing the real neighbours, and on a JVM with assertions enabled the search failed
  outright on JVector's own `0 <= score <= 1` check. A node whose vector cannot be read is now scored at the floor of
  the metric instead, so it sorts behind every real candidate rather than ahead of them.

The brute-force fallback that backs up a degraded graph search now sizes its expectation on the live vector count
rather than on the graph's ordinal count, and no longer requires the result to be under 80% of what is available - a
guard that evaluated to `0 < 0` when a single vector survived, and suppressed the only thing left that could answer the
query.

`select().vectorNeighbors()` on a `PRODUCT`-quantized index is where this was most visible, because that path has
neither a delta scan nor a brute-force fallback behind it: it returned the empty list directly.

`getStats()` gains a `bruteForceScans` counter. The brute-force scan is the fallback a k-NN query takes when the graph
walk could not fill it, and it reads every vector in the index - by far the most expensive thing this index does, and
until now visible only as a `WARNING` in the log. If filtered vector queries are slow, look there first.

One part of this is rebuild-gated. An unreadable vector is no longer encoded into the PQ code table under its
placeholder, but an index built before this release keeps the codes it already has, so PQ navigation past such a
vector stays slightly degraded until the next graph rebuild. Results are correct either way - what a query may return
is decided by the live-vector filter, not by the codes.
## Deleting a vertex no longer loses an edge appended while the delete was running

The two changes above closed this window from the removal side: a piece of an edge list that cannot be read is now a
retryable conflict rather than "nothing to remove here". They could not close it from the *append* side, and the same
corruption arrived from there.

`DELETE VERTEX` is a read-modify-write over the whole edge list: collect every edge, disconnect each one from the
vertex at its other end, drop the chunk records, delete the vertex record. Only the *write* half left an MVCC
footprint. The collection walked the chain through a plain record lookup, which under `READ_COMMITTED` does not retain
the pages it reads, while the removals and the chunk drain captured each page only later - at whatever version it had
by then. An edge appended in between was therefore already part of the page the delete rewrote and then deleted: the
commit-time version check compared the newer version against itself, found no conflict, and committed. The vertex was
gone, the chunk holding the new edge was gone with it, and the edge record survived with a live `out` and an `in`
naming a record that no longer exists. `check database` reports it as an invalid link.

Nothing in the collection could have caught it: the edge did not exist when the walk ran. Measured on four hubs of a
hundred edges each with six appender threads racing four deleter threads, a surviving edge appeared in roughly one
round in three, and reproduced identically on the commit before #5680 - it is a pre-existing defect, not a regression
from that fix.

A vertex delete now pins, in its own transaction, everything its edge list can grow through, at the version it reads
the list at:

- **Every chunk page of the list**, not just the head. An appender resolves the head from its own handle of the
  vertex, so a handle taken before a head flip appends into a chunk that is no longer the head but is still in the
  chain. This costs nothing at the peak - the drain deletes every one of those chunks anyway, so their pages end up in
  the transaction regardless; the pin only brings them in early enough to be worth something. On a promoted super-node
  (#5156) the same applies to the stripe directory and to every chain of every generation.
- **The vertex's own head pointers**, re-read through their page just before the record is deleted. An append that
  finds the head chunk full writes no chunk at all: it creates a new one and records it in the vertex record, so the
  chunk pins see nothing. If either head moved since the collection, the delete is refused.

**Visible effect.** As with the two changes above, `vertex.delete()` / `DELETE VERTEX` can now raise a retryable
`ConcurrentModificationException` where it previously "succeeded" while losing an edge. It is a `NeedRetryException`,
so `database.transaction(...)` and the server's auto-retry for single-request commands absorb it - the retry re-reads
the list, finds the appended edge, and deletes it with the rest. The one place it surfaces is the one described for
the two changes above: a **client-managed explicit transaction over `RemoteDatabase`** spans several HTTP requests, so
its commit is not auto-retried and the exception reaches the caller, who should retry the transaction. That is the
contract concurrent updates have always had; what is new is that a vertex delete racing an append is now one of the
operations that can raise it, instead of quietly committing a graph with an edge pointing at a vertex that is gone.
Both checks are skipped under the internal `force` delete, which is unchanged: `force` means "this record is known
broken, get it out", and the surviving references are the price its caller accepts.

Two tolerances are deliberately preserved. A vertex whose own record buffer cannot be decoded has no head to compare,
so the head check does not apply to it - failing there would make it undeletable again, which is what #4420 and #4432
fixed. And under `force`, a pin that cannot complete still leaves a usable list behind, so the tolerant walk collects
what it can and the chunk drain still runs.

**Not addressed here.** `deleteVertex` still materialises every edge into a list before deleting any of them. That is
unchanged behaviour and predates all of this; the two-phase shape exists precisely *because* the removals relink and
delete chunks underneath the walk, so streaming it means walking a list while it is being restructured - the class of
problem this whole series is about. It is tracked separately.
## Cypher: the two count push-downs now agree, and neither drops a `SKIP` or scans to answer 0

Counting in Cypher was served by two independent push-downs - the O(1) `Type.count()` one and the CSR one that
propagates path counts through the adjacency arrays - reached from different places and agreeing on neither their
preconditions nor their entry points. Issue #5715 collected five consequences of that; fixing them uncovered two more.

### `RETURN count(*) LIMIT 0` returns no rows

`SKIP`, `LIMIT` and `ORDER BY` are fields of the statement rather than clauses in its clause list, so the check that
was supposed to see them - a walk of the clause list - could not. The CSR push-down returns a step that replaces the
whole chain, so the `SKIP` and `LIMIT` steps were never built and

```cypher
MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) AS c LIMIT 0
```

came back with a row holding the count. So did `SKIP 1`. Neo4j applies both to the one-row aggregate result and
returns nothing for either, and so does the other push-down, so the engine disagreed with itself.

Both are now applied to the row a push-down produces rather than used to refuse the statement that carries them, so a
count written with a harmless `LIMIT 1` keeps the fast path instead of falling back to a scan. `ORDER BY` still sends
the statement to the ordinary pipeline, which is the only thing here that knows what an aggregate alias sorts by.

### `MATCH (m:Label) RETURN count(*)` no longer scans

Three shapes were answered by a full scan for a number the type counter already held:

- `MATCH (m:Big) RETURN count(m)` written on its own. The type-count push-down was only reachable from the planner
  path a top-level query takes when the cost-based optimizer *declines* it - and a single-label `MATCH ... RETURN
  count(v)` is exactly what the optimizer accepts. The same body inside `COLLECT { }` was answered in O(1); the plain
  query was not.
- `MATCH (m:Big) RETURN count(*)`, and `MATCH (:Big) RETURN count(*)`. The type-count detector required the argument
  to name the MATCH variable, and every `count(*)` detector required at least one relationship, so a single-node
  pattern with `count(*)` - the spelling Neo4j documents - was claimed by neither.
- `COUNT { MATCH (m:Big) }` and `COUNT { (m:Big) }`. A body with no `RETURN` of its own is normalised to one row per
  match and the expression counted those rows. `COUNT { }` now asks for the number rather than for the rows, so a body
  whose `RETURN` preserves the row count - absent, or neither aggregating nor `DISTINCT` - is answered by the same
  push-downs.

Both push-downs are now reached through one entry point, from every planner path, which is also what keeps their
preconditions from drifting apart again.

### A pattern that cannot match is answered without reading anything

The CSR push-down was applied with no cost check at all. 100 vertices with no edge between them cost 200 record reads
to answer 0, and an edge type absent from the schema cost the same. A count over a pattern whose non-optional part
names a node label or relationship type that is undeclared, or that holds no record, is now answered with 0 directly.

A `LIGHTWEIGHT` edge type is excluded from that test: it stores its edges in the vertices' edge lists and writes no
edge record, so its counter is 0 while its edges are there to be counted.

### Two wrong answers, found while doing the above

Both come from the same place - the helper that builds a node label's bucket set returned `null` both for "no label
was given, so do not filter" and for "the label is declared but does not exist, so nothing matches", and its callers
did not agree on which one they had been handed.

- `MATCH (a)-[:LINKS]->(b) RETURN count(*)` answered **0**. Every CSR operator walks out from one labelled position of
  the pattern and enumerates that label's buckets; an unlabelled anchor left it with no set to enumerate, which each
  operator read as an empty one. A pattern with no label on its first node is no longer given to these operators.
- `MATCH (a:Q)-[:LINKS]->(b:NoSuchLabel) RETURN count(*)` answered the count **without the filter** - 2, for a pattern
  that produces no row at all. The undeclared hop label is now recognised as unmatchable.

Neither shape had a test. Both are counted correctly now, and a count over a pattern that does match is unchanged.
### `CHECK DATABASE RECORD <rid>`: check and repair named records only (#5680)

`CHECK DATABASE` could be narrowed to a `TYPE` or a `BUCKET`, but not to a record. That became the sharp edge of
the strict vertex delete above: a genuinely broken edge chain makes its vertex undeletable until
`CHECK DATABASE ... FIX` rebuilds the adjacency from the surviving edge records, and the smallest way to ask for
that repair was two full passes over the whole vertex type. The new `RECORD` scope visits only the records listed:

```sql
CHECK DATABASE RECORD #12:3 FIX
CHECK DATABASE RECORD #12:3, #12:9 FIX
```

The per-record work and the fix are identical to the type-wide run, including rebuilding a vertex's edge list from
the surviving edge records. It combines with `FIX` and `COMPRESS`, and is rejected when combined with `TYPE` or
`BUCKET` - `RECORD` is already the narrowest scope, so the combination has no sensible meaning and silently
letting one win would run a check nobody asked for. The database-wide passes (buckets, external properties,
indexes) and the orphaned-segment reclaim are skipped, since none can be narrowed to a record.

Three costs a record scope does **not** bound, worth knowing before reaching for it:

- `COMPRESS` is unaffected by the scope and still compresses the **whole database**. It is opt-in, but it is the
  one clause that breaks the "naming a record bounds the cost" promise, so do not pair `RECORD ... COMPRESS`
  expecting a cheap run;
- rebuilding an adjacency means finding every surviving edge that points at the vertex, and no index maps
  endpoints back to edges, so the scoped run saves the vertex passes but still scans the edge types - once per
  distinct vertex **type** named, so naming ten vertices of one type costs one sweep while naming one vertex of
  each of three types costs three;
- if a listed record turns out to be *corrupted*, every index on its bucket is dropped and rebuilt, which is a
  full bucket scan. That matches the type-wide semantics and only fires on genuine corruption - an edge-list
  rebuild alone deletes no record and triggers none of it - but `RECORD` bounds the check, not necessarily the fix.
## Copying a type copies its records and its index definitions, not just their names

`Schema.copyType()` - what `DocumentType` → `VertexType` conversion goes through - was reported as losing the page
size of every index on the copy (#5723). It was, and that turned out to be the least of it. The same thirty lines held
three defects, and the two that were not reported are the ones that made the copy wrong rather than differently tuned.

**The copy had no data in its indexes.** The indexes were created inside the transaction that copies the records.
`TypeIndexBuilder.create()` builds each bucket's index in its own transaction (`joinCurrent=false`), which from inside
an enclosing transaction neither sees the records still pending in it nor commits the entries it produces. So every
index on the copy came out empty, and any query the planner routed through one answered nothing - silently, since an
empty index is indistinguishable from a type with no matching rows:

```java
schema.copyType("Doc", "Doc2", LocalVertexType.class, 1, 65_536, 1_000);
database.query("sql", "SELECT FROM Doc2 WHERE k = 'v1'");   // 0 rows, and the record is right there
```

The index build now runs after the record-copy transaction has committed.

**The copied records were empty.** `ImmutableDocument.propertiesAsMap()` was the one accessor of that class that did
not lazy-load: it answered `Collections.emptyMap()` whenever the record had not been materialised yet, which is the
state of every record handed out by `iterateType()` and `scanBucket()`. `copyType()` reads each source record that
way, so it faithfully created the right *number* of records, each with no properties. The same silent emptiness
affected a `DetachedDocument` built off a record straight from a scan. `propertiesAsMap()` now loads first, like
`get()`, `has()`, `getPropertyNames()` and `toJSON()` already did; the empty map remains the answer only for a record
that genuinely cannot be materialised.

**And the index definitions were rebuilt from three attributes.** The copy went through the three-argument
`createTypeIndex` overload, so the index type, the uniqueness flag and the property list survived and everything else
was replaced by a default: the tuned page size from the report, the null strategy, the collations that make a lookup
case-insensitive, and the entire type-specific configuration. That last one is not a tuning difference - a copied
`FULL_TEXT` index tokenized and ranked with the default analyzer instead of the configured one, a copied `GEOSPATIAL`
index dropped to the default geohash resolution, and a copied `LSM_VECTOR` index came up with `dimensions` 0.

Carrying a definition into a new index file now has an accessor of its own, `IndexInternal.getMetadataForNewFile()`,
alongside the `getPageSizeForNewFile()` that #5713 introduced for the page size. It exists because `getMetadata()`
cannot answer this question for the wrapper index types: a full-text index keeps its analyzers and BM25 parameters in
its own `FullTextIndexMetadata` and a geospatial one keeps its resolution and layout in plain fields, while
`getMetadata()` delegates to the underlying LSM-Tree and hands back a definition with none of it. `IndexMetadata.copy()`
is now polymorphic across all five metadata classes, so each carries its own settings and there is no second field list
to forget. Per-index runtime state deliberately does not ride along: the copy starts empty, so inheriting the dense
vector index's build state or the full-text corpus counters would describe a different set of records.

A user-supplied index name is the one attribute not carried over. It is unique across the schema and still belongs to
the source type, so the copy takes the auto-derived `NewType[properties]` form rather than colliding with it.

**One behaviour change reaches outside `copyType()`.** `LSMVectorIndexMetadata.copy()` already existed and carried only
the collations; routing it through the shared `copyCommonTo()` means it now also carries the user-supplied index name.
Its other caller is `TRUNCATE TYPE`, which drops and recreates each index from its own definition - so a manually named
`LSM_VECTOR` index now keeps its name across a truncate, where before it came back under the auto-derived
`Type[properties]` form. Nothing else was affected: every other index type already kept its name there, because that
path hands the original metadata object straight back rather than copying it.

The other sites that rebuild an index from its own definition - `TRUNCATE TYPE`, `CHECK DATABASE FIX`, and the
propagation to a freshly added bucket or sub type - still read `getMetadata()` and so still lose a full-text or
geospatial configuration. They are unchanged here and tracked separately: each is a distinct user-visible operation and
deserves its own regression test rather than riding along with this one. `REBUILD INDEX` is already correct; it
reconstructs the typed metadata from the persisted JSON.

**Full Changelog**: https://github.com/ArcadeData/arcadedb/compare/26.7.2...26.8.1

## `CREATE INDEX IF NOT EXISTS` no longer reports success for an index that is not the one asked for

`IF NOT EXISTS` matched a pre-existing index on the indexed property set alone. The property set is what the index
name is derived from, and it says nothing about what the index does, so a `NOTUNIQUE` index answered "already there"
to a request for a `UNIQUE` one:

```sql
CREATE INDEX ON T (Scalar) NOTUNIQUE;
CREATE INDEX IF NOT EXISTS ON T (Scalar) UNIQUE;   -- reported success, created no constraint
INSERT INTO T SET Scalar = 'x';
INSERT INTO T SET Scalar = 'x';                    -- accepted
```

Schema-migration DDL uses `IF NOT EXISTS` precisely so it can be re-applied, so tightening an index from `NOTUNIQUE`
to `UNIQUE` did nothing while the application carried on believing a uniqueness constraint protected its data. Issue
#5675. The same statement without the guard reported the clash, which is what made the guard the thing suppressing an
error the server already knew how to raise.

`IF NOT EXISTS` is now satisfied only when the existing index provides what was asked for: the same index kind, and a
uniqueness constraint at least as strong.

- A `UNIQUE` index still satisfies a `NOTUNIQUE` request. It indexes exactly the same keys, so the statement stays the
  no-op it was, and it is never weakened into a plain index.
- A `NOTUNIQUE` index no longer satisfies a `UNIQUE` request. The statement is refused, naming both definitions.
- An index of a different KIND on the same properties - a `FULL_TEXT` index where a range index was asked for, or the
  reverse - is refused for the same reason. That one was silent too.

### An existing index is never dropped implicitly any more

The refusal is the answer even in the case the request could technically be granted by rebuilding. Underneath,
`TypeIndexBuilder.create()` with `withIgnoreIfExists(true)` used to drop the existing index and rebuild it with the
requested definition. That is not a recoverable operation: if the rebuild then fails on the data already stored - two
records sharing the key the new index has to keep unique - the type is left with **no index at all** and an error. On
an index the type only inherited it took away the parent type's index instead, which is issue #4083.

So `withIgnoreIfExists` now means only what it says. Callers that genuinely mean "make this the definition" opt in
explicitly:

```java
database.getSchema().buildTypeIndex("T", new String[] { "Scalar" })
    .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
    .withReplaceIfIncompatible(true)     // new: may take away the plain index on those properties
    .create();
```

and even then the replacement is undone if the new index cannot be built, so a failed upgrade costs an error rather
than an index. An inherited index is still never replaced.

### Cypher: a uniqueness constraint over a plain index still upgrades it

`CREATE CONSTRAINT ... REQUIRE ... IS UNIQUE` and `... IS NODE KEY` are statements about the data rather than requests
to create an index if one is missing, so they are the callers that opt into the replacement above. Neo4j keeps the
range index and the constraint as two separate objects; ArcadeDB has one index per property set and a unique index
covers both roles, so the upgrade is the equivalent end state and a Neo4j migration script that creates indexes and
then constraints keeps working. Cypher's own `CREATE INDEX` does not opt in: it finds a unique index sufficient and
leaves it alone rather than replacing it with a plain one.

### A manual index name that already names a different index

Reachable only through `CREATE INDEX <name> IF NOT EXISTS ...`, since the auto-derived name is built from the property
list. A name that already belongs to an index on other properties used to satisfy the guard; it is now reported, since
two different property sets under one name are two different indexes.

### The index kind is now read before the existing-index lookup

`TypeIndexBuilder.create()` validates `indexType` at the top, because deciding whether the index already on those
properties covers the request needs to know what the request is. One embedded-API shape changes as a result: a builder
with **no** `withType(...)` and `withIgnoreIfExists(true)` used to hand back the existing index on those properties,
and now raises `DatabaseMetadataException` the way it always did when no index was there. Every in-tree caller sets the
type; a get-or-create helper that relied on omitting it has to pass the type it expects.

A failed replacement restores the page size along with the rest of the definition, via `getPageSizeForNewFile()` - the
same accessor a rebuild uses, so a page size the current file carries but creation would refuse cannot turn the restore
into a second failure (#5713).

### BREAKING: `getOrCreateTypeIndex` no longer upgrades an incompatible index

This one surfaces at runtime rather than at compile time, so it is the change embedded callers are most likely to meet
without warning.

`LocalSchema.getOrCreateTypeIndex(...)` sets `withIgnoreIfExists(true)`, so it inherits the change above: asked for a
`UNIQUE` index where a `NOTUNIQUE` one already covers those properties, it used to drop and rebuild, and now raises
`IllegalArgumentException` naming both definitions. That is the point of the fix - the rebuild is what could leave the
type with no index - but embedded callers using it as an "ensure this index" helper across a schema change are the ones
who will meet the new exception. Either drop the old index explicitly, or build through
`buildTypeIndex(...).withReplaceIfIncompatible(true)` if replacing it really is what you mean.

The replacement restores the previous definition on failure, including a manual index name (carried on the index
metadata, the same route `CREATE INDEX <name>` uses) and the page size. It is not atomic, though: the drop and the
build are separate schema changes, so a process that dies between them leaves the type without an index on those
properties until one is created again. Only an explicit `withReplaceIfIncompatible` is exposed to that window.

## An after-read listener that rewrites a record no longer corrupts it

An `AfterRecordReadListener` may return a *different* record than the one it was handed - that is how the documented
record-encryption recipe decrypts on the way out. The buffer built from that replacement was wrong in three ways
(#5755), and the worst of them was live rather than latent.

The buffer was not positioned at the properties. `BinarySerializer.serializeDocument()` hands it back at position 1
when it can copy the record's own buffer, but at 0 when it had to re-serialise the properties of a dirty record - the
closing `header.flip()`. In the second case the record-type byte was still ahead of the read cursor, so `toJSON()`,
`getPropertyNames()`, `toMap()`, `has()` and `get()` consumed it as the first byte of the header size and answered
nonsense. It stayed hidden because the only in-tree listener of that shape decrypts *vertices*, where
`ImmutableVertex` re-parses its edge-pointer prefix from position 1 afterwards and repositions as a side effect.

The record also aliased `DatabaseContext.getTemporaryBuffer1()`, the per-thread scratch buffer the serializer clears
and hands out again on every call. A decrypted record held across an unrelated `save()` on the same thread had its
content rewritten underneath it and started answering with **the other record's values**. That one reached the
encryption recipe as written, vertices included.

Finally, `BaseRecord.reload()` handled the identical case by taking the returned record's own `getBuffer()`, which on
a dirty record still holds the *pre*-modification content: a reload silently discarded what the listener did and
handed back the raw stored value - ciphertext, for the encryption recipe - and threw `NullPointerException` outright
for a replacement the listener had built from scratch rather than by `modify()`.

The postcondition of `ImmutableDocument.checkForLazyLoading()` is now uniform and documented - *on return with a
non-null buffer, the buffer is positioned at the start of the properties* - and `reload()` obeys the same contract.

### The record a listener returns must be mutable

`reload()` now renders the replacement through the serializer, the way `checkForLazyLoading()` always did, rather than
taking its buffer. That is a real narrowing: a listener returning a different **immutable** record with a valid buffer
used to be accepted on the `reload()` path and now raises `ClassCastException`, matching what the lazy-load path has
always required. `AfterRecordReadListener.onAfterRead()` documents it: a returned record that is not the one received
must be mutable - typically `record.modify()`, or built from scratch, which is equally supported - and returning
`null` filters the record away.
## A refused vertex delete names the command that repairs it, and the conflict keeps its cause

Follow-ups from #5680, whose two halves landed separately (#5707 made `deleteVertex` strict, #5710 added the
`CHECK DATABASE RECORD` scope), so the seams between them were never closed (#5764).

### The repair advice names the scoped command, with the RID substituted

Every recovery hint `GraphEngine.deleteVertex` emitted predated the scope it was written for, so they all pointed at
a whole-database or whole-type run - two full passes over the vertex type plus an edge sweep - while the operator was
holding the one piece of information that makes the repair cheap. And the retryable arm rethrew the conflict bare, so
what actually reached a human was `getEdgeHeadChunkForWrite`'s "concurrent commit in flight", which says nothing about
recovery at all.

A delete refused because the vertex's own list cannot be walked now answers with the command to run:

```
Edge list IN of vertex #12:3 is not fully visible yet (concurrent commit in flight): ...
  If it persists once the retries are spent the list is genuinely broken: run `CHECK DATABASE RECORD #12:3 FIX` to
  rebuild its edge list from the surviving edge records, then retry the delete (the scope saves the vertex passes,
  not the edge sweep the rebuild needs)
```

When the conflict comes from disconnecting a collected edge at its **other** end, the list that needs rebuilding
belongs to the neighbour, so that is the RID named - repairing the healthy vertex under delete would do nothing.

The `force` arms keep the whole-database form, deliberately: by the time they log, the delete has gone through, so
the record a scoped check would be aimed at no longer exists. They say what the scoped form would have bought had it
been run first.

### `ConcurrentModificationException` carries the failure that produced it

`NeedRetryException` already declared a `(message, cause)` constructor; `ConcurrentModificationException` exposed only
`(String)`, so every site that raised one from a caught exception - eight across `GraphEngine`, `EdgeLinkedList` and
`StripedEdgeList` - discarded the original stack. A conflict is normally absorbed by the retry and never seen, so the
single run that does surface one is the retry-exhausted run: exactly the run whose stack trace has to be diagnosable.
The cause is now kept. The exception class on the wire is unchanged, so nothing about HTTP status mapping or the
remote driver's typed reconstruction moves; only the `detail` chain in a development-mode error body gets longer.

### `CHECK DATABASE` reports a corrupt document the same way whichever scope found it

The type-wide document check added to the `warnings` and `corruptedRecords` sets without touching `totalWarnings` and
`totalCorruptedRecords`, so a run listed the finding while reporting zero of both - and, since the retained sets are
capped and the totals are not, that divergence grew with the number of findings. It also called a document a
"vertex", and said it was "removing it" when nothing on that path removes anything (`corruptedRecords` only drives the
end-of-run index rebuild). Both paths now emit `document <rid> cannot be loaded`, count it, and honour `maxWarnings`.

If you parse `CHECK DATABASE` output, note the two changes: the message text for an unloadable document, and
`totalWarnings`/`totalCorruptedRecords` now being non-zero on a type-wide run that finds one.

### The scoped vertex check runs one pass where the type-wide runs two - and that is correct

Recorded rather than changed, since the asymmetry looks like a gap. The type-wide arm materialises each record from
the raw page view in a bucket scan and then again through the connectivity walk; the scoped arm appears to run only
the second. But `lookupByRID` *is* the first - it performs the same
`newImmutableRecord(type, rid, bucket.getRecord(rid).copyOfContent())` - and the connectivity walk opens with the same
`asVertex(true)`, whose `loadContent` flag is what forces the decode. `LocalBucket.getRecord` and `LocalBucket.scan`
resolve placeholders and multi-page chains identically and skip the same slot markers, so no corruption shape reaches
one enumeration and not the other. Pinned by a test that corrupts a record and asserts both scopes report it.

## Manual indexes: creating one no longer fences the database, and a guarded request answers for the index asked for

Two defects, one on top of the other, on the `Schema.buildManualIndex(...)` path - the index kind that is not bound to a
type and is populated by the application itself. Issue #5765.

### Creating a manual index worked at all only by accident

`ManualIndexBuilder.create()` registered the new index's file under `index instanceof PaginatedComponent`. That test
never matched: `LSMTreeIndex` and `HashIndex` **wrap** their paginated component rather than being one, so the file
stayed unknown to the schema. The very next step - the commit that creates it - then failed resolving that file id,
**after** its WAL append, which fences the database for recovery (#5053):

```java
database.getSchema().buildManualIndex("idx", new Type[] { Type.STRING })
    .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
// DatabaseOperationException: database is fenced after a failure past the WAL commit point
```

The registration now goes through `index.getComponent()`, the same accessor the type-index path uses. Three smaller
defects on the same path, each of which the first one hid:

- `Schema.createManualIndex(indexType, ...)` dropped its `indexType` argument on the floor, so every call through that
  deprecated overload failed with a `NullPointerException` inside the index factory.
- A **unique** manual index could not commit an entry. Both the commit-time lock collection and the uniqueness check
  resolved the index's type name to reach the polymorphic index over the same properties; a manual index has neither,
  and the null reached `getType()`. A manual index is now recognised as its own whole search space, which it is.
- `withNullStrategy(null)` installed the null instead of leaving the default, and the index constructor then refused
  it with "Index null strategy is null". `null` now means "not specified", the same convention `withPageSize` follows.

An index kind that cannot be built without a type - `FULL_TEXT`, `GEOSPATIAL`, `LSM_VECTOR`, `LSM_SPARSE_VECTOR` - is
now refused by name, instead of failing inside the factory with a `NullPointerException` or a `ClassCastException`.

### `withIgnoreIfExists` on a manual index follows the same rule as everywhere else

The guarded branch still carried the two defects #5675 removed from `TypeIndexBuilder`: a request differing only in
uniqueness **dropped** the existing index and recreated it, behind a dead `x != null && x == null` null-strategy test,
and the index kind was not compared at all. A manual index is worse off than a type index there - its entries are not
derived from any record, so the drop destroys the only copy and no rebuild can bring them back.

It now answers the same way the type path does: the existing index is returned when it provides what was asked for
(same kind, uniqueness at least as strong), and the mismatch is reported otherwise. There is no
`withReplaceIfIncompatible` on this path: that setter raises `UnsupportedOperationException` on a manual builder,
because the replacement it authorises elsewhere is a rebuild and here it would be a silent delete.

## `CREATE INDEX IF NOT EXISTS` also compares the settings the `METADATA` clause named

The guard compared the structural definition only - the index kind and its uniqueness - so an existing index configured
differently satisfied a request that spelled out other settings:

```sql
CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {"dimensions": 384};
CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR METADATA {"dimensions": 768};  -- silent no-op
```

Every vector written afterwards is 768 long, none of them is indexed, and nothing said so. Same for full-text
analyzers, geospatial precision and the rest. Issue #5765.

**Only the settings the clause actually named are compared.** A statement that names none compares none and stays the
plain no-op it has always been, which is what keeps this from reaching any statement that did not ask for it. A
statement that names one gets an answer about it, whether or not a rebuild would have been needed to change it: writing
a value into a statement and having it silently discarded is the surprise being removed, and a caller who means the
no-op leaves the key out.

### BREAKING: a guarded create naming a setting MAY NOW RAISE where it used to be a no-op

This is a semantic change to a path whose whole point is idempotence, so it is the one to check before upgrading. Any
re-runnable script of the shape

```sql
CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR METADATA {"dimensions": 384, "efSearch": 120};
```

now raises `IllegalArgumentException` (HTTP 400) if the index already there was built with **any** different value for
a key the clause names - including the runtime-tunable ones a rebuild would not be needed to change (`efSearch`,
`mutationsBeforeRebuild`, `inactivityRebuildTimeoutMs`, the cache sizes). Previously every one of those was discarded
in silence.

Two ways to keep such a script idempotent: drop the keys whose value you do not actually require, leaving only the ones
the index must have; or align the value with what the existing index carries. The error names each differing setting
with both values, so it says which of the two applies.

The clause is read through the index type's own reader before comparing, so the spellings that reader accepts are the
value they denote rather than a difference: `{"dimensions": "384"}` matches 384, and `{"similarity": "euclidean"}`
matches `EUCLIDEAN`. A mismatch is reported naming each differing setting with both values, as an HTTP 400.

### Progress dots no longer go to stdout

`CREATE INDEX` and `REBUILD INDEX` printed a dot to `System.out` every 100k indexed records. Inside a server process
that reaches nobody while still costing a flush on the build path; it is a log line now. The pollable live progress of
`REBUILD INDEX` (#5376) is unchanged.
*(Superseded below: the corollary is that the type-wide arm's first pass was the redundant one, and #5773 removed
it. Both arms now run exactly one pass.)*

## `CHECK DATABASE` stops looking at every vertex and edge twice

Follow-ups from #5764 (#5773). The observation above cuts the other way too: if one pass answers everything for the
scoped arm, the type-wide arm's **first** pass is doing work its second pass already does.

### The redundant pass is gone

`checkVertices` and `checkEdges` opened with a raw bucket scan that built every record and decoded it, then ran the
real connectivity/endpoint walk through `scanType` - which performs the identical `newImmutableRecord(...)` from the
identical raw page view and opens with the identical `asVertex(true)`/`asEdge(true)`. Nothing the first pass could
see escaped the second: a construction failure lands inside `LocalBucket.scan`'s per-slot `try` and is routed to the
error callback that warns and flags the record, and a decode failure lands in the walk's own `catch`. The dropped
pass in fact saw *less*, since it passed a `null` error callback and so only logged a failure inside the bucket
machinery itself (placeholder resolution, a multi-page chain read) instead of reporting it.

Measured rather than assumed, because "materialises every record twice" makes the saving sound larger than it is:
on a warm 200k-vertex / 200k-edge graph the per-type steps went 173 ms -> 162 ms (vertices) and 155 ms -> 142 ms
(edges), and a full `CHECK DATABASE` 509 ms -> 472 ms - about 7%. The pass is cheap because `asVertex(true)` parses
the edge pointers, not the properties. What it mostly buys is correctness of the report, below.

**Visible change:** the progress `total` for a vertex/edge step is now the record count, where it was twice the
record count (it was written as literally `2 * countType`). A progress bar reading it will simply be accurate.

### `CHECK DATABASE` no longer says it is "removing" records it does not remove

The dropped pass emitted `vertex <rid> cannot be loaded, removing it` (and the edge equivalent). Removal happens only
under `FIX`, so a plain `CHECK DATABASE` announced a removal and removed nothing. #5764 fixed exactly this wording on
the document arm; the vertex and edge arms had the conditional version of the same defect, and both messages leave
with the pass that emitted them. The surviving wording is `vertex <rid> cannot be loaded (error: ...)`, which names
the failure as well.

**If you parse `CHECK DATABASE` output:** a corrupt vertex or edge now produces **one** warning per record instead
of two, and the `, removing it` message is gone. `corruptedRecords` is a set, so the corrupted total is unchanged;
`totalWarnings` drops by one per corrupt record. `FIX` still deletes exactly what it deleted before.

### The two warning/corrupted counters answer the same question

Two smaller alignments in the same files:

- `GraphDatabaseChecker.addCorrupted` incremented unconditionally once past its retention cap, while the
  `DatabaseChecker` twin added by #5764 checked `contains` first - so a RID still present in the retained set was
  counted again by one and not by the other. `checkEdges` really does flag one RID twice (an edge whose IN and OUT
  vertices are both gone), so the two helpers disagreed on the same input. They now share the `contains`-checking
  behaviour: exact de-duplication while under the cap, degrading past it only for RIDs the cap refused to retain.
- `totalWarnings` counted occurrences while the retained `warnings` is a `Set`, so two findings that render to the
  same message collapsed to one line and counted two - the total could exceed the retained size on a run nowhere
  near its cap. Both sides now count distinct messages, in `GraphDatabaseChecker` and in `DatabaseChecker`.

Both are a **reported-statistic change**, so they are worth knowing about if you consume `CHECK DATABASE` output
programmatically rather than reading it: `totalWarnings` now answers "how many distinct messages", where it used to
answer "how many times something was reported". On a run whose findings all render differently - the normal case -
the number is unchanged. On one with repeats it drops, and it can no longer exceed the size of the `warnings` set
on an uncapped run. `totalCorruptedRecords` gains the same guarantee past the cap.

The four hand-written copies of that rule are gone with them, into `CollectionUtils.addBounded`, which is now the
one place the retain-and-de-duplicate policy is written down. Two of the four had drifted apart, which is what made
the counters disagree in the first place.

### `CHECK DATABASE` at `verboseLevel 0` no longer logs the warnings it had to drop

Also an alignment, and the one behaviour change here an operator could notice directly. When a run exceeds its
warning cap, the message that cannot be retained is logged instead, so it is not lost silently. The two arms
disagreed about whether `verboseLevel 0` switched that off: the graph arm logged regardless, the document arm
honoured the flag. They now both honour it, on the grounds that a caller passing `0` asked for no logging - and the
retained set plus `totalWarnings` still report that something was dropped either way.

If you run a **capped** check **quietly** and relied on the dropped messages appearing in the log, pass a
`verboseLevel` above zero. A default `CHECK DATABASE` is unaffected: it runs at verbosity 1.

## Deleting a vertex no longer disconnects its edges from itself

`GraphEngine.deleteVertex` walked the vertex's edge lists, then handed every edge to `deleteEdge`, which disconnects
it from **both** endpoints. One of those two endpoints is always the vertex being deleted - and its lists are dropped
in their entirety a few lines later, by `deleteRemainingChunks`. So for every edge the delete walked a chain from the
head probing each chunk for that entry, anchored and copied the chunk that held it, compacted it and wrote it back,
over a list that was about to be deleted wholesale (#5760).

Each edge is now disconnected from the vertex at its **other** end only. Matched by the RID recorded on the edge
rather than by which list it was found in, so an entry whose `out`/`in` does not name the vertex being deleted is
still disconnected normally; a self-loop, whose both endpoints are that vertex, skips both sides, which is correct
because both of its lists go.

The self-side **read** is untouched. The strict collection walk - a chunk that cannot be read is a retryable conflict,
only `force` absorbs it (#5670/#5680), every page of the list pinned so an edge appended behind the walk turns the
delete into a retry (#5725) - is what decides which edges exist to disconnect, and all of it stays.

100k edges into one hub, on an Apple M-series laptop:

| layout | before | after |
| --- | --- | --- |
| promoted super-node (striped, #5156) | 2374 ms | 446 ms |
| classic single chain | 350 ms | 308 ms |

The striped layout is where it hurt: `StripedEdgeList.removeEdge` resolves one chain per generation for every single
removal, and generation 0 is the whole pre-promotion chain. On the classic layout the emptied head chunk keeps the
per-edge probe short, so there was less to win.

### The removal walk streams

Skipping the self side also removed the reason the walk could not. `deleteVertex` used to materialise every edge of
the vertex into an `ArrayList` and delete them in a second pass, and it had to: while the removals still rewrote this
vertex's own list, relinking and deleting chunks underneath the iterator, one pass would have been a walk over a list
being restructured - the class of problem #5155, #5670 and #5680 were all about. The delete now writes the far
endpoints' lists and the edge records and never the list it is reading, so the second pass is gone, and with it a
live `Edge` object per edge held for the whole delete. On a million-edge super-node that accumulator was tens of
megabytes of retained heap; there is no longer a per-degree allocation on the path at all.

There is no behaviour change for callers: `deleteVertex` disconnects, deletes and tolerates exactly what it did
before, including under `force`.

### One observable worth knowing if you register delete listeners

A self-loop is reachable from **both** of its vertex's edge lists, so deleting that vertex walks it twice and the
record-delete pipeline - including `BeforeRecordDeleteListener.onBeforeDelete` and the type-level delete events -
runs for it **twice**. The second pass disconnects nothing (both endpoints are the vertex being deleted, so both
are skipped) and the record removal is absorbed, so the graph outcome is correct either way; a listener that is not
idempotent is the only thing that can notice.

This is **not new** in this release - the previous two-phase walk collected the self-loop from each list and called
`delete()` on it twice as well, and `deleteRecordNoLock` fires the event before anything can short-circuit. It is
recorded here because it was never written down, and because it holds for lightweight self-loops too, where there
is no record to go missing on the second pass. Pinned as an exact count by
`Issue5760VertexDeleteSelfSideSkipTest.aSelfLoopIsWalkedFromBothListsSoItsDeleteEventFiresTwice`.
