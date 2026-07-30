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
it: the connection is marked non-persistent and the unread remainder is discarded rather than drained. A load
that reads its body to the end keeps its connection reusable exactly as before.

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

**Full Changelog**: https://github.com/ArcadeData/arcadedb/compare/26.7.2...26.8.1
