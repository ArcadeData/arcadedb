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

Both halves of that are gone in 26.8.1:

- **Inserts** into one page now reserve their slot per in-flight transaction, so concurrent inserts get
  different positions (and different RIDs) instead of all being handed the same one, and the commit-time
  disjoint-slot merge replays them on top of each other.
- **Updates** are replayed by that same merge whenever they stayed inside the page, which now includes a
  record that GREW - a longer string, one more property - and not only an overwrite of the same size or
  smaller. Growth is the normal update shape, so leaving it out kept concurrent updates of unrelated records
  conflicting for good.

Measured on the reported workload (one single bucket, `attempts=1`, no retry):

| Scenario | Before | After |
|---|---|---|
| Concurrent inserts | ~1750 conflicts / 2000 | 0 |
| Concurrent sub-graph creation (6 vertices + 5 edges per transaction) | ~270 / 320 | 0 |
| 10 transactions updating 10 different records of one page | 9 failed / 10 | 0 |
| Sustained updates, 8 writers on their own records of one page | ~2083 / 2880 | 0 |

A `ConcurrentModificationException` is still raised - by design - when two transactions really write the
**same** record: a byte-for-byte pre-image check makes sure no concurrent write is ever silently overwritten.
The same goes for the write shapes no merge can replay (a delete, a placeholder or multi-page record, a
record that has to spill out of its page). Nothing changes for single-writer workloads, and no application
change is needed. The merge can be switched off with `arcadedb.txPageSlotMerge=false`.

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

The second of those reaches an **existing index too, without rebuilding it**: dropping the `shapeRel` filter is
in the shared query walk, so an index still on the old layout now issues exact lookups on covering cells it used
to skip. That is what makes a point search shape work against it - the results were previously empty and are now
correct - at the cost of a few more lookups and a few more candidates per query on those indexes. Both layouts
still return a superset that the SQL predicate re-checks, so no query changes its answer for the worse.

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

`REBUILD INDEX` also no longer resets a non-default GeoHash `precision` back to 11, the same defect fixed for
`FULL_TEXT` in #4732. Its cause was one level up: `TypeIndexBuilder` declared a `metadata` field that shadowed
`IndexBuilder`'s, so `withMetadata()` wrote to one and `create()` read the other for every index type without a
dedicated builder subclass. The duplicate field is gone.

**Full Changelog**: https://github.com/ArcadeData/arcadedb/compare/26.7.2...26.8.1
