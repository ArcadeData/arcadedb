# ArcadeDB v.26.8.1 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.8.1 development cycle, so the release notes are ready at tag time.

### Fixes

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

### Breaking Changes

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

**Full Changelog**: https://github.com/ArcadeData/arcadedb/compare/26.7.2...26.8.1
