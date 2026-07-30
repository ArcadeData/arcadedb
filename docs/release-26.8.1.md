# ArcadeDB v.26.8.1 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.8.1 development cycle, so the release notes are ready at tag time.

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
client that reads while it uploads - any ordinary HTTP client, including the Java one used by
`RemoteGraphBatch` - is handed the 400 mid-upload and is unaffected. A client that writes its whole payload
before reading anything may see a connection reset instead of the error body; the reason is always in the
server log, and the failure is immediate rather than a quarter of an hour of silence.

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
