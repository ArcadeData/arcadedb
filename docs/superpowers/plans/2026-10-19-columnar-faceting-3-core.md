# Columnar Faceting - Core (plan 3 of 6) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** a `COLUMNAR` index type backed by an async, node-local, on-disk columnar projection per bucket (segments, global dictionaries, overlay, manifest), kept current by dirty bucket pages from local commit, Raft apply and WAL replay, recovered without full rebuilds, exactly equivalent to `GROUP BY` once caught up.

**Architecture:** `ColumnarIndex` is a thin `IndexInternal` over an empty LSM shell (the `LSMSparseVectorIndex` pattern) so the schema, file ids and HA DDL keep working unchanged; all data lives in `<databasePath>/columnar/`, a plain directory the `FileManager`, the snapshot ship, the backup and the checksum scan never see. One `ColumnarIndexManager` per `LocalDatabase` owns one `ColumnarProjection` per bucket; commit paths only set bits in epoch-based dirty page sets, and a JVM-wide apply pool re-reads those pages from committed state into an in-memory overlay that a maintenance pool flushes into immutable segments published by an atomic manifest rename.

**Tech Stack:** Java 21, ArcadeDB engine internals, plan 2 codecs (`com.arcadedb.engine.codec`) and block format/cache (`com.arcadedb.engine.block`), `DedicatedThreadPool`, Micrometer (server module only), JUnit 5 + AssertJ, `BaseRaftHATest`.

**Spec:** `docs/superpowers/specs/2026-10-18-columnar-faceting-design.md` (sections 5, 6, 9, 10). Cross-plan contract: `docs/superpowers/plans/2026-10-19-columnar-faceting-0-roadmap.md` ("Plan 3 produces", "Plan 2 produces", "Metrics", "Shared test fixtures").

## Spike results

**Verdict: GO.** Criterion: projection with a native match source (match + count of one facet, K = 1) has a lower median than one `GROUP BY` with the same `WHERE`, for both predicates. Both full runs agree (v2 method, 1,000,000 records). Neither predicate is within noise (median gap far above 10% of `groupByMs`). Plans 3 onwards may start (roadmap gate).

Two assumptions behind the verdict, stated up front:

1. The projection side is a direct Java call and excludes the SQL front end (parse, plan, result set) that a real faceted query pays. The measured SQL floor (`SELECT brand, count(*) AS cnt FROM Asset WHERE fileSize < 0 GROUP BY brand`, zero rows) is 0.43 to 0.55 ms; adding it to the projection side does not change the verdict (line below).
2. The broad gate (`status = 'published'`, 70% of records) assumes `status` is itself a projected column, so the native match is a scan of the 2-bit `status` column. An unindexed predicate on a property that is not projected falls back to the record scan, which costs about the same as the GROUP BY.

### Gate, K = 1 (ms, medians with min in parentheses)

Selective: `fileSize < 10000`, 9,868 matches (0.99%), native match = index range on `fileSize` into a bitmap. Broad: `status = 'published'`, 700,191 matches (70.02%), native match = scan of the `status` column.

| Predicate | Run | native match + count | groupByMs (`GROUP BY brand`) | Gap as share of groupByMs | Verdict | match + count + SQL floor |
|---|---|---|---|---|---|---|
| selective | 1 | 2.670 (min 2.480) | 4.442 (min 3.949) | -39.9% | GO | 3.182 (SQL floor 0.433) |
| selective | 2 | 3.043 (min 2.845) | 4.779 (min 4.230) | -36.3% | GO | 3.671 (SQL floor 0.551) |
| broad | 1 | 4.716 (min 4.570) | 32.245 (min 30.608) | -85.4% | GO | 5.184 (SQL floor 0.455) |
| broad | 2 | 4.845 (min 4.800) | 34.882 (min 34.078) | -86.1% | GO | 5.331 (SQL floor 0.473) |

Gate including the SQL floor (reported, not decisive): GO for both predicates in both runs. Selective is the closer one: projection 2.67 to 3.04 ms (3.18 to 3.67 ms with floor) against 4.44 to 4.78 ms; the worst projection-with-floor run (3.376 ms run 1; 3.921 ms run 2) is still below the median GROUP BY (4.442; 4.779), so selective is a real but modest win (1.6x to 1.7x without the floor, 1.3x to 1.4x with it), while broad is a large one (6.8x to 7.2x without the floor, 6.2x to 6.5x with it).

Split of the projection time (run 1): selective match 2.42 ms + count 0.25 ms; broad match 2.79 ms + count 1.93 ms. The count is 5.6% (selective) and 6.0% (broad) of `groupByMs`.

### Environment

| Item | Value |
|---|---|
| Date | 2026-10-21 (run 1 12:01 CEST, 82 s; run 2 12:04 CEST, 89 s) |
| Commit | `2eed9d4a93` (spike files uncommitted on top) |
| JVM | OpenJDK 25.0.2 (2026-01-20) |
| CPU | Apple M3 Pro, 12 cores |
| Heap | JVM default (no `-Xmx`) |
| `RECORDS` | 1,000,000 |
| Page size | 65536 bytes, 3631 pages (237,961,216 bytes), 275.4 records per page average |
| Method | 6 measured runs after 2 warmups, balanced order, stall-discounted nanoseconds shown as ms, median of an even count = mean of the middle two; every count path asserted equal to its SQL oracle |
| Machine load | No other Maven or surefire process was running at either start. Load averages (1/5/15) at run 1 start: 2.66 / 2.62 / 2.66 (other non-Maven processes present); at run 2 start: 4.52 / 3.76 / 3.13. Run 2 ran under slightly higher load, which fits its 3% to 14% higher medians (selective projection +14%, broad projection +2.7%, GROUP BY +7.6% selective and +8.2% broad); the ratios agree. |
| Raw reports | scratchpad `columnar-spike-v2-run1.md` and `columnar-spike-v2-run2.md` (copies of `engine/target/columnar-spike/results.md`) |

### K = 5 facet page (match once + 5 counts, vs 5 GROUP BYs back to back)

Facets: brand (200), status (3), mimeType (12), author (about 96.5k distinct), tags (multi-valued, 1000, GROUP BY via `UNWIND`).

| Predicate | Run | projection page | GROUP BY page | Speedup |
|---|---|---|---|---|
| selective | 1 | 4.307 (min 4.050) | 42.529 (min 37.954) | 9.9x |
| selective | 2 | 4.713 (min 4.446) | 45.174 (min 42.292) | 9.6x |
| broad | 1 | 24.817 (min 24.678) | 867.543 (min 860.074) | 35x |
| broad | 2 | 25.120 (min 24.880) | 914.446 (min 910.224) | 36x |

Per facet (run 1, ms, projection count only, match excluded): selective brand 0.244, status 0.244, mimeType 0.242, author 0.410, tags 0.838 (match 2.35 once); broad brand 1.93, status 1.83, mimeType 1.85, author 3.98, tags 12.48 (match 2.78 once). The multi-valued `tags` facet dominates the broad page (12.5 of 24.8 ms) but its GROUP BY with `UNWIND` costs 607 ms (run 1), so it is the facet where the projection gains most (49x).

### Block variant (off-heap blocks of 16384 rows, brand and author columns; status, mimeType, tags stay heap)

| Predicate | Run | K = 1 native match + countBlocks | heap K = 1 | K = 5 page with blocks | heap K = 5 page | Overhead K = 1 count / K = 5 page |
|---|---|---|---|---|---|---|
| selective | 1 | 3.947 | 2.670 | 7.108 | 4.307 | +546% / +65.0% |
| selective | 2 | 4.340 | 3.043 | 7.564 | 4.713 | +512% / +60.5% |
| broad | 1 | 5.506 | 4.716 | 24.846 | 24.817 | +42.8% / +0.1% |
| broad | 2 | 5.592 | 4.845 | 25.325 | 25.120 | +40.3% / +0.8% |

The block variant still beats GROUP BY K = 1 and K = 5 on both predicates (selective 3.9 to 4.3 ms vs 4.4 to 4.8 ms is the thinnest margin: about 10%, within the 10% noise band, so treat selective K = 1 with blocks as break-even against GROUP BY, while K = 5 is 6x ahead). Adding the SQL floor, selective K = 1 with blocks loses in run 2 (4.340 + 0.551 = 4.891 ms vs 4.779 ms) and only just wins in run 1 (3.947 + 0.433 = 4.380 ms vs 4.442 ms). Block segments are about half the heap size (brand 1.00 MB vs 1.94 MB; author 2.09 MB vs 3.07 MB).

### Sizes per segment (identical in both runs; serialized, dictionary included)

| Segment | Bytes | Bytes per record | Presence share | Share of bucket | Bit width | Dictionary size |
|---|---|---|---|---|---|---|
| brand | 1,946,688 | 1.947 | 48.50% | 0.82% | 8 | 200 |
| status | 1,194,122 | 1.194 | 79.06% | 0.50% | 2 | 3 |
| mimeType | 1,444,220 | 1.444 | 65.37% | 0.61% | 4 | 12 |
| author | 4,709,809 | 4.710 | 20.04% | 1.98% | 17 | 96,513 |
| tags (multi-valued) | 8,706,104 | 8.706 | 10.84% | 3.66% | 10 | 1,000 |

Segment build (outside the measured rounds): brand 125 ms (run 1) and 139 ms (run 2), 7.2 to 8.0 M records/s; status 94 ms, mimeType 85 ms, author 128 ms, tags 219 ms (4.6 M records/s). Block conversion from the heap segment: brand 21 ms, author 12 ms.

### Page re-read (spec 6.3)

Warm pages (the benchmark just scanned them), so a lower bound of a cold re-read. 200 pages re-read, evenly spread over 3631, 275.4 records per page.

| Run | Total | Per page | Per record |
|---|---|---|---|
| 1 | 7.012 ms | 35.06 us | 0.127 us |
| 2 | 7.404 ms | 37.02 us | 0.134 us |

A full re-read of all 3631 pages is about 127 to 134 ms single-threaded. Input for `COLUMNAR_FLUSH_INTERVAL` / apply sizing unchanged from the first spike (33 us per page there): one apply thread keeps up with a 1000 ms interval for dirty sets of this size.

### Memory (spec 5.7)

| Metric | Value |
|---|---|
| brand `int[cardinality]` | 800 bytes |
| author `int[nominal 100,000]` | 400,000 bytes (measured cardinality 96,513: 386,052 bytes) |
| Match bitmap per query | 929,536 bytes (3631 pages x 32 words x 8) |
| Measured per K = 1 query | 930,336 bytes |
| Measured per K = 5 page | 1,320,448 bytes (five count arrays + one bitmap) |
| Spec extrapolation, measured author | 308,841,600 bytes (4 bytes x 96,513 x 16 tasks x 50 queries) |
| Spec extrapolation, spec figure | 3,200,000,000 bytes (4 bytes x 1,000,000 ordinals x 16 tasks x 50 queries) |

### Concurrent K = 5 page, 8 threads x 20 iterations (160 queries per phase)

Dedicated fixed pool owned by the benchmark; per-call count arrays and bitmaps; 12 cores. The broad GROUP BY scans in parallel inside the JVM-wide `ParallelScanProducerPool` while each projection query runs on one thread, so the queries/s ratio reflects total CPU cost, not per-query parallel latency. The selective GROUP BY is index-served and sequential.

| Predicate | Method | Run | queries/s | p50 ms | p95 ms |
|---|---|---|---|---|---|
| selective | projection | 1 | 1430.5 | 4.397 | 8.038 |
| selective | projection | 2 | 1394.2 | 4.739 | 9.669 |
| selective | GROUP BY | 1 | 145.8 | 52.455 | 63.323 |
| selective | GROUP BY | 2 | 133.2 | 57.841 | 69.356 |
| broad | projection | 1 | 267.2 | 30.001 | 32.821 |
| broad | projection | 2 | 263.2 | 30.293 | 32.570 |
| broad | GROUP BY | 1 | 3.4 | 2311.920 | 2470.805 |
| broad | GROUP BY | 2 | 3.0 | 2601.495 | 2833.091 |

Under 8 clients the projection keeps its single-client latency (selective p50 4.4 to 4.7 ms vs 4.3 to 4.7 ms alone; broad 30 ms vs 25 ms alone) and is about 10x (selective) and about 80x (broad) ahead on throughput. The broad GROUP BY page collapses to 3 queries/s because 8 clients share the 12-worker scan pool.

### Cold (engine page cache only: database reopened before every cell; the OS file cache is not emptied)

Segments stay in memory across the reopen, so the projection rows say nothing about a cold projection store (segment files read from disk), which is not covered. They do include the engine-side cold cost of the native match: the selective match reads the `fileSize` index through the cold page cache, and its cold K = 1 is 4.186 / 4.345 ms vs 2.670 / 3.043 ms warm (+57% / +43%); the broad match reads only the in-memory `status` segment (5.152 / 4.865 ms vs 4.716 / 4.845 ms warm, +9% / +0%).

| Predicate | Method | Run | K = 1 ms | K = 5 ms |
|---|---|---|---|---|
| selective | projection | 1 / 2 | 4.186 / 4.345 | 4.917 / 5.353 |
| selective | GROUP BY | 1 / 2 | 109.179 / 99.179 | 49.983 / 53.151 |
| broad | projection | 1 / 2 | 5.152 / 4.865 | 24.746 / 26.040 |
| broad | GROUP BY | 1 / 2 | 54.684 / 66.272 | 886.650 / 934.142 |

Cold vs warm GROUP BY, per cell (run 1 / run 2): selective K = 1 24.6x / 20.8x (109.179 / 4.442, 99.179 / 4.779); broad K = 1 1.7x / 1.9x (54.684 / 32.245, 66.272 / 34.882); selective K = 5 1.2x / 1.2x (49.983 / 42.529, 53.151 / 45.174); broad K = 5 1.0x / 1.0x (886.650 / 867.543, 934.142 / 914.446). The cold ordering is odd: cold selective K = 1 (99 to 109 ms) is about 2x slower than cold broad K = 1 (55 to 66 ms) and than cold selective K = 5, which runs five queries (50 to 53 ms). The cold penalty concentrates in the first query after a reopen (index and record page faults plus an empty plan cache), so the K = 1 vs K = 5 and selective vs broad ordering in the cold table is not a per-query cost comparison. The warm projection advantage is a floor of the cold one only for the GROUP BY side; the cold projection figures above include the native index match but not a cold segment read.

### SQL match source (run 1 method), for reference

Same count code, match by `SELECT @rid FROM Asset WHERE ...` into the bitmap. Selective match + count: 8.037 ms (run 1) and 9.580 ms (run 2) vs groupBy 4.442 / 4.779: NO-GO on selective (1.8x and 2.0x slower). Broad: 123.779 ms and 137.905 ms vs 32.245 / 34.882: NO-GO (3.8x and 4.0x slower). This is exactly the first spike's outcome; the native match source is what flips it.

### Superseded method (first spike run, SQL match source, NO-GO)

Not predictive of the real system: the SQL `SELECT @rid` materialized one result row per match, which was 125 ms of the 128 ms on the broad predicate, while the counting itself cost 5 to 9% of one GROUP BY. The table is kept for the record.

| Predicate | Run | matchMs median / min | countMs median / min | match + count median / min | groupByMs median / min | (match + count) / groupBy |
|---|---|---|---|---|---|---|
| selective | 1 | 16.766 / 13.631 | 0.552 / 0.228 | 17.318 / 13.859 | 12.176 / 10.761 | 1.42 |
| selective | 2 | 13.905 / 10.681 | 0.957 / 0.223 | 14.858 / 10.904 | 10.931 / 8.826 | 1.36 |
| broad | 1 | 125.344 / 123.520 | 3.061 / 3.046 | 128.395 / 126.648 | 37.655 / 36.025 | 3.41 |
| broad | 2 | 125.161 / 123.762 | 3.050 / 3.008 | 128.205 / 126.770 | 39.171 / 37.036 | 3.27 |

(The first spike ran while another Maven build loaded the machine, which is why its absolute GROUP BY figures are higher than above.)

### Observations for plan 3

- **The match source decides the feature.** With a native match (index range into a bitmap: 2.4 to 2.8 ms for 9,868 matches; column scan: 2.8 ms for 700k matches) the projection is 1.6x to 7x faster than one GROUP BY at K = 1 and 10x to 36x at K = 5. Plan 3 and plan 4 must not route the match through `SELECT @rid`; the page-bitmap match is part of the design, not an optimization. The SQL front end adds about 0.45 to 0.55 ms (floor) per query on top.
- **Presence encoding.** Presence (bitmap + page row starts, 2048 slots per page, about 275 used) is 48.5% of brand, 65% of mimeType and 79% of status. For narrow columns it is larger than the values (status: 2 bits of value vs about 7.6 bits of presence per record). In absolute terms all five segments are 0.5% to 3.7% of the bucket, so raw `long[]` stripes remain acceptable as a starting value; but presence is shared by every column of a bucket, so plan 3 should store it once per bucket projection (not once per column) and pick a compact per-page slot list or RoaringBitmap only if plan 2's block format makes that free. RoaringBitmap is still not justified by any counting figure (count is 5.6% to 6% of GROUP BY).
- **Block size and decode share.** Off-heap 16384-row blocks halve segment bytes but cost +40% on a broad K = 1 count (2.75 ms vs 1.93 ms: 0.8 ms of decode over 1M rows) and +512% on a selective one (1.62 ms vs 0.25 ms, i.e. about 1.4 ms is a full decode of the 62 blocks even though only 9,868 rows are read; the matches are spread over every block, so block skipping would not help). Decode share of the block count time is therefore about 30% (broad) and about 85% (selective). On the K = 5 page the block overhead is +60% selective but +0.1% to +0.8% broad, because only two of five facets use blocks and decode is a fixed per-column cost. Plan 3 should (a) decode blocks lazily by match density (use point reads when matches per block are few: here 160 of 16384 rows) and (b) keep low-cardinality columns (status, mimeType) as a heap or direct-decoded form. `COLUMNAR_SEGMENT_BLOCK_ROWS` stays 16384: the block count is tied to decode cost per block, not to a size pressure.
- **Multi-valued layout.** `tags` (about 3 values per record, dictionary 1000) takes 8.706 bytes per record (3.66% of the bucket, 4.5x brand): about 3.75 bytes of packed ordinals (3 x 10 bits), 4 bytes of `int[]` row offsets and about 0.94 bytes of presence, so about 46% of the segment is the raw offset array and offsets dominate the size: delta or bit-pack them (plan 2 codec) and 219 to 226 ms to build (4.4 to 4.6 M records/s). Counting costs 0.84 ms (selective) and 12.5 ms (broad), 3x (selective, 0.838 vs 0.244 ms) to 6.5x (broad, 12.476 vs 1.932 ms) of a single-valued column, against 18 to 19 ms and 607 to 642 ms for `GROUP BY ... UNWIND`: the largest relative win of the page (22x selective, 49x broad). The layout (value list per record) is fine; plan 3 should keep it and count each document once per distinct value (dedup at write, spec 5.3 and 7.4: count documents, not values), as `SpikeMultiSegment` does. This equals the `UNWIND` oracle only without in-record duplicates; plan 3's equivalence test must cover a record with a duplicate tag.
- **Count array for 100,000 values.** `int[96,513]` is 386 KB per query and counts in 0.41 ms (selective) to 3.98 ms (broad); the cost is probably the count array allocation and top-N selection rather than the counting itself (inferred from the cardinality, not measured: the spike does not time those steps separately). A sparse counter was not measured: dense is adequate at 100k; sparse not measured. The memory risk is the spec extrapolation (309 MB for 16 tasks x 50 queries at 96k ordinals; 3.2 GB at 1M ordinals). Plan 3 should keep dense counting up to a documented cardinality threshold (the figures support at least 100k) and add a sparse or hash counter for the selective case only when matches are far below cardinality (here 9,868 matches against 96k values) or ordinals exceed that threshold, and should reuse count arrays through a per-thread pool to cap the 16 x 50 product.
- **Selective queries are O(bucket slots), not O(matches).** Selective was measured only at 1% selectivity. Count plus top-10 costs 0.244 / 0.244 / 0.242 ms for brand / status / mimeType (bit widths 8 / 2 / 4), i.e. independent of column width: the walk over all stripe words dominates, and the match bitmap allocation (0.93 MB zeroed per query) scales the same way. Plan 3 must implement spec 7.4's point-read / sparse-iteration path (skip empty match words or pages, or iterate a sorted position list) before relying on very selective queries (0.01%) or large buckets (50M rows), where this floor can exceed an index-served GROUP BY.
- **Costs the projection side does not model.** The measured projection side excludes: the live-bitmap AND, multiple segments per bucket (`COLUMNAR_FLUSH_MAX_ROWS` 65536 gives about 16 segments per 1M rows, each with its own presence, plus a count merge), dirty-page patching (spec 7.3), the per-bucket security check (spec 7.2) and facet result serialization beyond the floor. On the selective predicate the headroom against GROUP BY with the SQL floor included is about 1.1 to 1.3 ms (4.442 - 3.182 in run 1, 4.779 - 3.671 in run 2); that headroom is plan 3's whole budget for all of these.
- **Segment build** is cheap (85 to 226 ms per column per million records, the maximum being tags in run 2), so flush cost does not argue for changing the flush defaults.

Defaults that depend on these numbers (change them in Task 9 and in this table, never silently). The spike did not argue for changing any starting value; the notes record what the numbers say:

| Default | Starting value | Spike input that tunes it | Spike note |
|---|---|---|---|
| `COLUMNAR_SEGMENT_BLOCK_ROWS` | 16384 | segment bytes per record, count time per segment | 1.2 to 8.7 bytes per record by column; block decode is about 1.4 ms per 1M rows for brand with a naive decoder (one absolute `getLong` per value), an upper bound for plan 2 codecs (the 0.8 ms broad figure is net of cheaper per-row reads, not a decode cost): keep |
| `COLUMNAR_FLUSH_MAX_ROWS` | 65536 | segment size, flush cost | build 4.6 to 11.7 M records/s, 65536 rows build in under 15 ms: keep |
| `COLUMNAR_FLUSH_INTERVAL` | 1000 ms | page re-read cost per page (spec 6.3) | 35 to 37 us per page (warm, lower bound): keep |
| `COLUMNAR_APPLY_THREADS` / `COLUMNAR_APPLY_INTERVAL` | 2 / 100 ms | microseconds per re-read page | 0.13 us per record, 275 records per page: 2 threads are ample, keep |
| Presence encoding (raw `long[maxRecordsInPage/64]` stripe per page vs compact per-page slot list) | raw stripes | presence share of segment bytes | 48.5% to 79% share for narrow columns, but segments are 0.5% to 3.7% of the bucket: keep raw, store presence once per bucket projection, revisit only if segment memory matters |
| `long[]` stripes vs RoaringBitmap | `long[]` stripes | "observations for plan 3" (no new dependency without a measured need) | match bitmap 0.93 MB per query and count is 6% of GROUP BY: no measured need, keep |
| Segment reads: `FileChannel` positional reads vs mmap | `FileChannel` | segment heap bytes, count time | segments are 1.2 to 8.7 MB per million records and heap counts take 0.25 to 12.5 ms; off-heap block decode costs +40% to +512% on K = 1 brand: keep, measured again by Task 20 `ColumnarCountBenchmark` phases (c) cold block cache and (d) fresh `SegmentReader.open` per round |

Plan 3 count overhead (many segments, overlay, cold block cache), Task 20 `ColumnarCountBenchmark`, 2026-10-22, one run, medians of 11 rounds after 3 warm-ups, stall-discounted ms, `forEachOrdinal` only (no match, no SQL): 1,048,576 Assets, (b)-(d) 16 build segments of about 65,900 rows (the build cuts at the first page boundary past 65,536) plus an overlay of 5,000 rows, (a) the records left after (b) merged into one segment of 948,719 rows, then 10% of them deleted, so both layouts have dead ratio 0.1 (the match sets of (a) are about 9% smaller). brand 1%: (a) 0.368, (b) 0.542, (c) 3.643, (d) 3.889. brand 70%: (a) 3.244, (b) 3.687, (c) 6.321, (d) 6.737. tags 1%: (a) 0.382, (b) 1.177, (c) 3.366, (d) 3.545. tags 70%: (a) 6.580, (b) 7.444, (c) 17.058, (d) 17.323. Many segments cost +13% to +47% warm on brand and +13% to +208% on tags (1% is dominated by per-segment fixed costs); a cold block cache adds 2.2 to 9.6 ms; a fresh `SegmentReader.open` per round adds a further 0.2 to 0.4 ms over 16 segments.

Plan 3 ingest overhead (spec 2 "ingest cost unchanged"), Task 20 `ColumnarIngestOverheadBenchmark`, 2026-10-22: 200,000 inserts then 100,000 updates (transactions of 500) of `Asset` without (A) and with (B) the five COLUMNAR indexes, one warm-up run per variant, then 5 alternating A/B pairs, each run on a fresh database; stall-discounted ms, median (min to max). Inserts: A 365 (362 to 390), B 402 (398 to 448), +10% with disjoint ranges, so a real cost of the insert phase, not separated here between the page marking in `commit()` and the apply passes running concurrently on the apply pool (the allocation below does not move). Updates: A 1,684 (1,617 to 1,770), B 1,674 (1,633 to 1,771), unchanged. Bytes allocated by the committing thread inside `commit()`, median per commit: inserts A 152,416, B 152,428; updates 11,217,464 both, unchanged. Apply catch-up after the updates (B only, until `awaitCaughtUp`): 440 (348 to 483), on the apply pool, off the ingest path.

Plan 3 ingest overhead, final fix batch 3, 2026-10-23 (same benchmark, now A / B / B' where B' = B with `suspendApply()` held across both phases, 5 rotating triples, plus committer CPU and JVM GC per phase; medians, stall-discounted ms). Before the batch 3 changes: inserts A 382, B 432 (+13%), B' 399 (+4.5%); updates A 1,447, B 1,550 (+7.1%), B' 1,460 (+0.9%); committer CPU inserts A 377, B 416, B' 398 ms; GC within 1 collection. Reading: with the apply held the overhead nearly vanishes, so the commit path is clean (spec 2 as worded: no extra page write, lock or retry) and the cost is the background apply competing for CPU. Changes: adaptive apply pacing (the interval doubles up to 5 x `applyInterval` while every pass leaves marks behind, back to the base when a pass drains everything or the oldest mark is older than 500 ms; lag bound 500 ms plus one pass at the default) and one header walk per record for all projected columns (`ColumnarValueExtractor.RecordReader`, replacing two walks per column). After, two runs: inserts A 382 / 425, B 405 / 468 (+6.0% / +10.1%), B' 397 / 435 (+3.9% / +2.4%); updates A 1,475 / 1,508, B 1,542 / 1,577 (+4.5% / +4.6%), B' 1,460 / 1,510 (-1.0% / +0.1%). The machine drifted between the two runs (A inserts 382 vs 425); the update overhead halved reliably, the insert overhead dropped from +13% to +6-10%. Bytes allocated by the committer per commit unchanged (145,6xx inserts, 11,196,xxx updates). Residual: background apply CPU on a busy JVM, not the commit path.

## Pre-flight revisions (2026-10-21)

Rulings of the pre-flight scan (`.superpowers/sdd/2026-10-19-columnar-faceting-3-core/preflight.md`, section 4), accepted by the controller and applied to the tasks below. Each task text is authoritative; this list is the index.

- **R1** Segment files are immutable: the BlockCache file id belongs to a ref-counted `SegmentFile` shared by `withDead` copies; reader `close()` only decrements; `invalidateFile` runs once, when the file is deleted. Tasks 4, 6, 8, 10, 17, 24.
- **R2** `ProjectionState` gets an `onLastRelease` callback; the projection keeps the retained states and calls `ColumnarStore.deleteUnreferenced` (which returns the deleted names). Tasks 5, 6, 8.
- **R3** Task 3 creates the COMPLETE `ColumnarStats` (all cells, readers, `dictionaryCompaction`) with `ColumnarStatsTest`; Task 18 keeps only `ColumnarMetrics` and `ArcadeDBServer`; Task 24 no longer touches `ColumnarStats`. Tasks 3, 18, 24.
- **R4** Task 11 creates a stub `ColumnarIndexManager` plus the `LocalDatabase` field and getter; Task 12 completes it. Tasks 11, 12.
- **R5** `extractor(String typeName)` stays; the extractor resolves the dictionary per record bucket (`DictionaryResolver`), `extractForApply` takes the projection's dictionary, and `extract(Document, String, GlobalDictionary, ValueSink)` is added for plan 4 (roadmap "Plan 3 additions"). Tasks 3, 12, 15.
- **R6** ORDINAL accepts every value `DictionaryValueCodec.isSupported` accepts; test `stringPropertyHoldingANumberKeysLikeGroupBy`. Tasks 3, 19.
- **R7** External-property buckets: second `DirtyPageTracker` (256 slots) and second `ContentOwnerMap` per projection, manifest `externalOwnerMapFile()`, pending positions are primary positions only. Tasks 5, 6, 7, 8, 12, 13, 15, 16.
- **R8** Only the `NOWAL` file records an uncovered no-WAL write (`noWalPending()` removed from the manifest): Task 13 writes it, a clean close deletes it (execution ruling, Task 13 note; Task 10 no longer deletes it), Task 16 reads it. Tasks 5, 10, 13, 16.
- **R9** `bucketDir(databasePath, bucketFileId)` = `columnar/b<fileId>`; the bucket name lives in the manifest only. Tasks 5, 13, 16, 17.
- **R10** `GlobalDictionary.create(Path)` writes a new generation file; rebuild and compaction stage on it, never on `createInMemory`. Tasks 2, 16, 24.
- **R11** Kill point `dictionary.appendPartial` placed in `GlobalDictionary.getOrAdd` between the record header and its payload. Tasks 2, 5, 21.
- **R12** `forEachOrdinal(matches, c)` visits exactly `matches AND live()`, consults presence and dead set only for `matches.pages()`; `live()` cached per reader; FacetOracle no longer ANDs live; roadmap "Plan 3 must also provide" notes it. Tasks 4, 6, 19.
- **R13** `ColumnarProjection.applyLock` held for a whole apply pass; compaction takes it and builds NEW remapped `OverlayPageBlock`s, never mutating published ones. Tasks 6, 15, 24.
- **R14** After a flush, `tracker.flushed` keeps unflushed every page re-applied after the freeze (refined by V-B2: tracked in `appliedSinceFreeze`, not inferred from live-overlay rows); test `pageReappliedAfterFreezeStaysUnflushed`. Tasks 7, 10, 15.
- **R15** `RecoverySetFile.write(Path, int[] fileIds, int[][] pages, int[] counts)` replaces the nonexistent `IntObjectView`. Task 16.
- **R16** READ_ONLY: `markUnusable` records the reason and serves the fallback, never schedules a rebuild or writes; Review Focus 5 test also asserts `rebuildsTotal() == 0` with a deleted manifest. Tasks 8, 12, 16.
- **R17** Task 16 owns `create` rebuilds: `ColumnarIndex.build()` and the reconciler's `ADD_COLUMN` call `ColumnarRebuilder.schedule(manager, projection, "create")`, which coalesces; startup rule 2 (`missing`) applies only at open; tests asserting `create == 1` create one index. Tasks 11, 16, 17, 23.
- **R18** Task 12 keeps five lifecycle tests; `suspendApplyHoldsPassesUntilClosed` moves to Task 13; `cleanCloseFlushesAndLeavesEmptyRecoverySet`, `readOnlyOpenNeverWritesTheProjection`, `killDoesNoIo` move to Task 16 (same test class). Review Focus 5 points to Task 16. Tasks 12, 13, 16.
- **R19** `DatabaseChecker` "Rebuilding indexes" loop skips indexes that are not maintained in transaction; the `check_fix` event covers COLUMNAR. Task 17.
- **R20** `ColumnarReconciler.hierarchyDiff` over per-type fingerprints emits `HIERARCHY_SOURCE_CHANGED`. Task 17.
- **R21** Placeholders are built with the `sealFirstPage` technique (factored into a static helper in Task 14); `ColumnarTestData.placeholderRatio`; Task 20 sets it to 0.02. Tasks 14, 15, 19, 20, Review Focus 4.
- **R22** `ColumnarTestData` draws tags with replacement and every 50th record carries one tag twice; `FacetOracleSelfTest.duplicateTagInOneRecordCountsOnce`. Task 19.
- **R23** Row blocks cache verified ENCODED sub-columns as `byte[]`, decoded forms under a separate column id; point mode never decodes; sub-columns of width <= 8 bits are never decoded. Task 4.
- **R24 (deliberate spec deviation)** Spec 6.4 says a bucket truncate drops the bucket's projection; plan 3 instead lets the projection empty itself through the ordinary deletes of `TruncateRecordDeleter` (exact, no hook). Cost: one apply pass that re-reads every page of the truncated bucket (O(pages)) instead of an O(1) drop; counts are exact either way. Task 17.
- **R25 (deliberate spec deviation)** Spec 2 anti-goal "the commit path does nothing beyond setting a bit": a `setUseWAL(false)` commit writes the `NOWAL` marker synchronously (tmp, force, rename, directory fsync) on the commit thread, at most once per flush epoch per projection. Cost: one small fsynced file write per flush interval for no-WAL writers only; the alternative was an O(bucket pages) page-version sweep on every unclean open after a no-WAL write. Task 13.
- **R26** `startApplying()` goes after the whole `if (recoveryPending) { ... }` block of `openInternal`, not inside it. Task 12.
- **R27** WAL housekeeping hook in `cleanWALFiles(dropFiles, force, syncDataOnDrop)`: `if (dropFiles && !persistColumnarRecoverySet()) return false;` after the optional `syncFiles()`, not inside its condition. Task 16.
- **R28** `CypherSchemaCommands` exhaustive switches gain `COLUMNAR`; `GraphBatch` skips non-maintained index types. Task 11.
- **R29** `LSMVectorIndex.rebuildSemaphoreForTesting()` (package-private) plus a test helper in `com.arcadedb.index.vector`. Task 16.
- **R30** Compaction tests use 2,000 brands and delete 1,200 (ratio 0.6, 800 survivors). Task 24.
- **R31** Stale line numbers, method names and test globs refreshed (section 2 of the scan): `PoolStats.poolSize()`, `lastRaftLogIndex(RaftHAServer)`, `getCommittedPageCount()`, no glob that matches no class. Tasks 3, 9, 11-18, 23, 25.

Other scan findings applied without a ruling number: overlay age `ColumnarOverlay.oldestRowMs()` (P39, Tasks 6, 10); `ColumnarApplyPool.register/unregister` for the ticker (P53, Tasks 9, 12); `drop()` defers deletion to the last release (P52, Tasks 8, 17); the commit-path test seam between `publishPages` and the second mark moves to Task 13 (P71); `GraphOlapVectorOpsFilteredTest` joins the gate (P80, Task 25); section 3 spike edits (presence once per segment, point-mode cache, duplicate tags, counting benchmark, cold store, oracle-only `SELECT @rid`, plan 4 counters note in the roadmap).


Verification pass fixes (`.superpowers/sdd/2026-10-19-columnar-faceting-3-core/plan3-verify.md`, all accepted):

- **V-B1** A bucket with no manifest and no records is published empty at `open` and is usable; `missing` only for a bucket with records. Tasks 8, 16.
- **V-B2** `flushed` keeps pages from `appliedSinceFreeze` (deletes-only re-applies included); `supersededInFrozen` hides frozen-overlay values updated or deleted after the freeze and becomes the new segment's initial dead set. Refines R14 (spec 6.5 and 5.2 decide: R14's live-overlay test missed deletes). Tasks 10, 15.
- **V-B3** NOWAL write and delete are atomic under `noWalMarkerLock` with `noWalMarkerEpoch`; the flush deletes only when every no-WAL mark was drained before the freeze (`noWalEpochAtLastDrain`). Tasks 10, 13, 15.
- **V-B4** `ColumnarProjection.addColumn`; `register` adds columns to an existing projection; `ADD_COLUMN` schedules `create` only when the manifest lacks the column and no rebuild is queued or running. Tasks 8, 12, 17.
- **V-B5** Reconcile also requested after a successful `saveConfiguration(boolean)`. Task 17.
- **V-B6** Flush, merge and compaction share one per-projection maintenance flag; compaction requires no frozen overlay and remaps live-overlay pages only. Tasks 9, 24.
- **V-B7** `ProjectionState` receives its file-name set from the manifest. Tasks 6, 8.
- **V-M1** Task 4 ascending test no longer names the overlay view. Task 4.
- **V-M2** `BlockCache.containsForTesting` / `entriesForTesting` test accessors. Task 4.
- **V-M3** Matched-pages test counts per-file entries, not JVM-wide misses. Task 4.
- **V-M4** Invalidation test asserts `entriesForTesting(oldCacheFileId) == 0`. Task 4.
- **V-M5** `SegmentReader.Meta` record declared; the first `SegmentFile` reference belongs to the reader. Task 4.
- **V-M6** `ColumnarManifest` constructor and `withGeneration`. Task 5.
- **V-M7** `RowBuffer.java` listed; `PositionBitmap.wrapShared`. Task 6.
- **V-M8** `ProjectionState.release` releases `SegmentFile` references, never closes shared readers. Task 6.
- **V-M9** Task 7 Consumes names Task 1 for `PositionBitmap`. Task 7.
- **V-M10** Open-time corruption only records the reason; the manager installs the requester after open and startup rule 2. Tasks 8, 16.
- **V-M11** `drop()` invalidates segment cache ids before deleting the directory; `publishLock()` accessor; orphan cleanup at writable open. Task 8.
- **V-M12** Cardinality check in `ColumnarIndexFactoryHandler.checkCardinality`. Task 11.
- **V-M13** `commitDoesNoIndexWork` uses `TransactionIndexContext.getTotalEntries()`. Task 11.
- **V-M14** `ColumnarRebuilder.schedule(manager, projection, reason)` everywhere. Tasks 11, 17, R17 line.
- **V-M15** `cleanOpenStartsApplying` asserts `isApplyingForTesting()`. Task 12.
- **V-M16** `flushNow` runs the NOWAL rule on an empty overlay; `acquire()` instead of `snapshot()`; `onMarkForTesting` hook. Tasks 12, 13.
- **V-M17** `placeholderRatio` semantics (seal between transactions, fillers are MISSING, single bucket only). Task 19.
- **V-M18** An update whose old value is in a segment marks that row dead. Task 15.
- **V-M19** `build()` skips projections already published empty. Task 16.
- **V-M20** `stats.recoveryPages(n)` at startup; helper method name `rebuildSemaphore()`. Task 16.
- **V-M21** Hierarchy test awaits one event per DDL statement. Task 17.
- **V-M22** `stats.mergeCount("Asset")`. Task 20.

## Global Constraints

Copied from the roadmap; every task implicitly includes them.

- **No git commits.** The user commits after review. Where the plan template says "Commit", the step is "Stop: changes ready for review", listing the files touched.
- No em dash characters in code comments, docs or messages.
- Java style of the repo: `final` on variables and parameters where possible; single-statement `if` without braces; imports, never fully qualified names; no `System.out` left behind; no Claude author tags.
- JSON via `com.arcadedb.serializer.json.JSONObject` / `JSONArray`, using getters with defaults.
- Primitive arrays over collections of boxed values on every hot path.
- Tests: AssertJ style `assertThat(x).isTrue();`. Slow tests `@Tag("slow")`, benchmarks `@Tag("benchmark")`. Never assert raw wall-clock time; use `com.arcadedb.utility.StallAwareStopwatch`.
- Test servers only through fixture bases (`BaseGraphServerTest`, `StaticBaseServerTest`, `BaseRaftHATest`); URLs from bound ports; extra ports from `allocateFreePorts` / `allocateFixturePorts`.
- Module test runs use `-am` (`mvn -o -pl engine -am test -Dtest=... -Dsurefire.failIfNoSpecifiedTests=false`), and `verify` when gremlin or graphql are in the reactor. Skip lanes with `-DexcludedGroups`, never `-Dtest='!Foo'`.
- No `ForkJoinPool.commonPool()`; background work on a dedicated pool following `.claude/skills/engine-concurrency/SKILL.md`.
- No new dependency unless a plan says so; RoaringBitmap only after plan 1 measures a need (Apache 2.0, update `ATTRIBUTIONS.md` and `NOTICE`).
- After every Java change: compile the touched modules and fix all errors before the next step.
- Plan 1 `Spike*` classes are throwaway test code: never import them.
- Corrupt block data is reported with plan 2's unchecked `com.arcadedb.engine.block.BlockCorruptionException`, never with a `BufferUnderflowException` or an allocation sized from unverified bytes.
- All columnar meters register on `Metrics.globalRegistry` with a `database` tag. Normal open and crash recovery never increment `arcadedb.columnar.rebuilds`.

Plan-wide decisions (the spec leaves them to the builder, section 13):

- Apply passes run on one JVM-wide pool (`ColumnarApplyPool`), at most one pass per database at a time: single writer per database, no thread per tenant.
- Flush, merge, rank-view rebuild and background rebuild run on a second JVM-wide pool (`ColumnarMaintenancePool`), so a long merge never delays apply.
- Rebuild is per bucket and covers all its columns; while it runs, the previous valid state (if any) keeps serving, so `isUsable()` is false only when no valid state exists.
- Columns are keyed by the `ColumnarIndex` file id, not by property name, so a property rename is a definition update only.
- A dictionary is stored with its bucket's projection (one per bucket and property, with generations `dict-<indexFileId>.<gen>.dic`; with the default `typeDefaultBuckets = 1` that is one per type and property). That keeps every publish, compaction included, a single manifest swap (spec 5.5). Readers decode a snapshot with `ColumnarSnapshot.dictionary(property)`, never with `ColumnarProjection.dictionary` (roadmap "Dictionary scope change"). A type with several buckets, or a polymorphic query over subtypes, merges ordinals through plan 4's `CrossBucketOrdinalMap` (spec 5.4); plan 3 only guarantees what that map needs: dictionaries are append-only within a generation, `size()` is a volatile published prefix, and a compaction is visible as a new generation in the next acquired snapshot.
- `ALTER PROPERTY ... TYPE` does not exist today (`AlterPropertyStatement` has no type branch): the "type change rebuilds the column" rule is implemented in the reconciler and proven by its unit test with a synthetic definition change.

## Review Focus

1. **A COLUMNAR index on a supertype, records in subtype buckets.** A person expects `Asset` facets to include `Image extends Asset` records exactly as polymorphic `GROUP BY` does. Test: Task 20 `polymorphicSubtypeBucketsAreProjected`.
2. **Values that GROUP BY keys but the column kind cannot hold** (DECIMAL 1.5 in a LONG column, a map inside a tags list, a string in a schemaless-converted field). Expected: counted as missing, `conversion.failures` incremented, never a crash or a silent wrong bucket. Test: Task 3 `unconvertibleValueIsMissingAndCounted`, oracle excludes them the same way.
3. **CREATE INDEX while writers are committing into the same type.** Expected: the build converges to the oracle, no record lost between the build scan and the first apply pass. Test: Task 16 `buildConcurrentWithWritesConverges`.
4. **A record that becomes a placeholder, then a chunk chain, then shrinks back.** Since #6149 a record that outgrows its page becomes a chunk chain, so a placeholder is built only on a page with no free tail (`BucketPageLayoutTestSupport.sealFirstPage`, static helper from Task 14). Expected: the stale content-page owner mapping is dropped, and a later write on the old content page does not re-apply a wrong owner. Test: Task 15 `recordShrinkingBackDropsStaleOwnerMapping`.
5. **A database opened READ_ONLY with a projection on disk.** Expected: facets readable, no file in `columnar/` written or deleted, no apply pass scheduled, no rebuild counted even with a deleted manifest (`rebuildsTotal() == 0`). Test: Task 16 `readOnlyOpenNeverWritesTheProjection` (class `ColumnarIndexManagerLifecycleTest`).

---

## File structure

All engine paths are under `engine/src/main/java/com/arcadedb/` (main) and `engine/src/test/java/com/arcadedb/` (test).

| File | Responsibility |
|---|---|
| `index/columnar/PositionBitmap.java` | Sparse per-bucket bitmap, one stripe per page, copy-on-write snapshots |
| `index/columnar/DictionaryValueCodec.java` | Type-tagged bytes of a dictionary value (String, Long, BigDecimal, Double, Boolean, RID) |
| `index/columnar/GlobalDictionary.java` | Chunked append-only ordinals, volatile size, append-only file, rank view |
| `index/columnar/ColumnKind.java`, `OrdinalConsumer.java`, `ValueSink.java` | Contract enums and callbacks |
| `index/columnar/ColumnDefinition.java` | Record: one indexed property of one bucket |
| `index/columnar/ColumnarValueExtractor.java` | GROUP BY conversion rules (spec 5.3) |
| `index/columnar/ColumnReader.java`, `ColumnarSegmentView.java`, `ColumnarSnapshot.java` | Read contract |
| `index/columnar/SegmentFormat.java` | Magics, versions, file names |
| `index/columnar/SegmentWriter.java`, `SegmentReader.java` | Immutable segment files over plan 2 codecs and `BlockHeader`, read through `BlockCache` |
| `index/columnar/SegmentFile.java`, `RowValues.java` | Ref-counted shared file identity of a segment (BlockCache file id, invalidated once on delete, R1); reusable row holder |
| `index/columnar/DeadSetFile.java` | Persisted complement of a segment's live bitmap, one versioned file per publish |
| `index/columnar/ColumnarManifest.java` | Manifest JSON, atomic publish (spec 5.5) |
| `index/columnar/ColumnarStore.java` | Directory layout, fsync helpers, unreferenced file deletion, rebuild-reason marker |
| `index/columnar/ContentOwnerMap.java` | Content page to owner positions reverse map (placeholders, chunks, external) |
| `index/columnar/OverlayPageBlock.java`, `ColumnarOverlay.java`, `RowBuffer.java` | Copy-on-write in-memory newest segment; per-page staging of `RowValues` |
| `index/columnar/ProjectionState.java` | Immutable published state, ref-counted |
| `index/columnar/ColumnarProjection.java` | One per bucket: state, dictionaries, tracker, flush/merge entry points |
| `index/columnar/DirtyPageTracker.java` | Epoch-based dirty pages, in-flight and unflushed sets (spec 6.2) |
| `index/columnar/ColumnarApplyPool.java`, `ColumnarMaintenancePool.java` | Dedicated pools |
| `index/columnar/ColumnarApplier.java` | One apply pass: page re-read, placeholder/chunk/external resolution, no-op skip |
| `index/columnar/ColumnarFlusher.java`, `MergePolicy.java` | Flush and merge |
| `index/columnar/RecoverySetFile.java` | Persisted dirty and unflushed pages (spec 6.5) |
| `index/columnar/ColumnarRebuilder.java` | Background rebuild, own semaphore |
| `index/columnar/ColumnarSchemaEvent.java`, `ColumnarReconciler.java` | Schema events table (spec 6.4) |
| `index/columnar/ColumnarIndexManager.java` | Per database owner; lifecycle; hooks |
| `index/columnar/ColumnarStats.java` | Engine-side counters per database and type (complete from Task 3, R3) |
| `index/columnar/ColumnarTestHooks.java` | Static volatile kill points (production no-ops) |
| `index/columnar/ColumnarIndex.java`, `ColumnarIndexFactoryHandler.java` | Index type |
| `schema/ColumnarIndexMetadata.java`, `schema/TypeColumnarIndexBuilder.java` | METADATA and builder |
| Modified: `schema/Schema.java`, `schema/LocalSchema.java`, `schema/TypeIndexBuilder.java`, `index/TypeIndex.java`, `query/sql/parser/CreateIndexStatement.java`, `query/opencypher/query/CypherSchemaCommands.java`, `database/DocumentIndexer.java`, `graph/GraphBatch.java`, `database/TransactionContext.java`, `engine/TransactionManager.java`, `engine/LocalBucket.java`, `engine/DatabaseChecker.java`, `database/LocalDatabase.java`, `index/vector/LSMVectorIndex.java` (test accessor only), `GlobalConfiguration.java` | Hooks |
| Modified test support: `engine/src/test/java/com/arcadedb/database/BucketPageLayoutTestSupport.java` (static `sealFirstPage(Database, String)`, Task 14) | Placeholder construction shared by Tasks 14, 15, 19 |
| `server/src/main/java/com/arcadedb/server/monitor/ColumnarMetrics.java` | Micrometer binder, `database` tag |
| Modified: `server/.../monitor/PoolMetrics.java`, `server/.../ArcadeDBServer.java` | Pool and binder registration |
| Modified: `ha-raft/.../SnapshotInstaller.java`, `ha-raft/.../SnapshotHttpHandler.java`, `integration/.../backup/format/FullBackupFormat.java` | Snapshot install marker, explicit exclusion guards |
| Test fixtures: `index/columnar/FacetOracle.java`, `index/columnar/ColumnarTestData.java`, `index/columnar/KillPoint.java` | Shared by plans 4-6 |
| Benchmarks: `index/columnar/ColumnarIngestOverheadBenchmark.java`, `index/columnar/ColumnarCountBenchmark.java` (`@Tag("benchmark")`, report only) | Spec 2 ingest goal; count overhead over many segments and a cold store (Task 20) |

## Contract additions (members added, nothing renamed)

| Where | Addition | Why |
|---|---|---|
| `PositionBitmap` | `PositionBitmap snapshot()`, `int maxRecordsInPage()`, `boolean isEmpty()`, `void clearPage(int page)`, `void setPage(int page)` | Copy-on-write publication; page-granular pending sets |
| `GlobalDictionary` | `int compare(int ordinalA, int ordinalB)` | Ranks of two ordinals newer than the rank view can tie |
| `ValueSink` | interface: `ordinal(int)`, `ordinals(int[] sorted, int count)`, `longValue(long)`, `doubleValue(double)`, `missing()`, `unknownValue(Object normalized)` | Read-only extraction (patching) meets values the dictionary does not hold yet |
| `ColumnarSegmentView` | `PositionBitmap live(int fromPage, int toPageExclusive)` | Bounded materialization of a large segment |
| `ColumnarSnapshot` | `GlobalDictionary dictionary(String property)`: the dictionary generation the snapshot's segments are encoded with | Dictionary compaction (Task 24) swaps generations; a reader must decode with the pair it acquired, never with `ColumnarProjection.dictionary` (which answers the current generation) |
| `PositionBitmap` (roadmap, for plan 4) | `PositionBitmap copy()`: deep, independent copy | Task 1 |
| `ColumnarSegmentView` | `live()` is computed once per reader and cached (immutable per dead version, R12) | Task 4 |
| `ColumnarValueExtractor` (roadmap, for plan 4) | `long toColumnLong(String property, Object value)`, `TimeUnit dateUnit(String property)` | Task 3 |
| `ColumnarIndexManager` (roadmap, for plan 4) | `Semaphore fallbackScans()` (permits `COLUMNAR_MAX_FALLBACK_SCANS`, plan 4's key; plan 3 reads it with default 2 until plan 4 adds the key), `suspendApply()` public | Task 12 |
| `ColumnReader` (roadmap, for plan 4) | `forEachOrdinal` visits positions in ascending order (javadoc and test); it visits exactly `matches AND live()` of its view (R12), so a caller never ANDs live itself | Task 4 |
| `ColumnarValueExtractor` (roadmap "Plan 3 additions", for plan 4) | `extract(Document record, String property, GlobalDictionary dictionary, ValueSink sink)`: read-only extraction against the snapshot's dictionary generation (R5); `ColumnarValueExtractor.DictionaryResolver` | Task 3 |
| `GlobalDictionary` | `static GlobalDictionary create(Path file)`: a new generation file, header written and fsynced (R10) | Task 2 |
| `ColumnarManifest` | `String externalOwnerMapFile()` (R7); `noWalPending()` removed (R8) | Task 5 |
| `ColumnarOverlay` | `long oldestRowMs()` (P39) | Task 6 |
| `ColumnarApplyPool` | `register(Runnable tick)` / `unregister(Runnable tick)` (P53) | Task 9 |
| `ColumnarProjection` | `ReentrantLock applyLock()` held for a whole apply pass (R13); `DirtyPageTracker externalTracker()`, `ContentOwnerMap externalOwners()` (R7) | Tasks 8, 15 |
| `ColumnarStats` | readers `applyPasses()`, `rebuildsTotal()`, `placeholdersResolvedCount(type)`, `recoveryPagesCount()`, `mergeCount(type)`, `flushCount(type)`, `unreadableCount(type)`, `overlayRows(type)`; `dictionaryCompaction(type, property, reclaimed)` (R3) | Task 3 |
| Fixtures (roadmap, for plan 4) | `FacetOracle.expectedCounts(Database, String type, String property, String where)`; `ColumnarTestData(long seed)` with `insertAssets(...)`, `mutate(...)` | Task 19 |
| `ColumnarIndexManager` | `markPagesWithoutWAL(int fileId, int[] pageNumbers, int count)`, `requestReconcile()`, `persistRecoverySet()`, `awaitCaughtUp(long timeoutMs)`, `flushNow()`, `getStats()`, `addSchemaEventListener(Consumer<ColumnarSchemaEvent>)`, `compactDictionaryNow(int bucketId, String property)` (Task 24 test hook) | No-WAL recovery (6.5), schema reconcile, housekeeping hook, tests, delivery of hierarchy events to plan 5 |
| `ColumnarIndexManager` (roadmap, for plan 5) | `watchFiles(int[], Runnable)` / `unwatchFiles(Runnable)`, `suspendApply(): AutoCloseable`, `conversionFailures(String, String)`; `onSchemaEvent` carries `HIERARCHY_SOURCE_CHANGED`; `long watchGeneration(Runnable)` (synchronous per-watcher counter bumped by `markPages`, so plan 5 can tell at query start that a watched change is not yet processed; additive, to be recorded in the roadmap) | Tasks 12, 13, 17 |
| `GlobalDictionary` (roadmap, for plan 5) | `int[] sortedOrdinals()`, strings by `String.compareTo` | Task 2 |
| `FacetOracle` (roadmap, for plan 5) | `awaitCaughtUp(Database)` | Task 19 |
| `ColumnarSchemaEvent` | record components `(kind, bucketFileId, indexFileId, reason, typeName)` | Hierarchy events name a type, not a bucket |
| `GlobalConfiguration` | `COLUMNAR_APPLY_INTERVAL` 100 ms, `COLUMNAR_MAINTENANCE_THREADS` 2, `COLUMNAR_REBUILD_CONCURRENCY` 1, `COLUMNAR_SEGMENT_BLOCK_ROWS` 16384, `COLUMNAR_MERGE_MAX_SEGMENTS` 10, `COLUMNAR_DICTIONARY_COMPACT_RATIO` 0.5 | Builder decisions of spec 13; spec 5.4 compaction trigger |
| Metrics | `arcadedb.columnar.cardinality.exceeded` (gauge 0/1 per database, type, property); `arcadedb.columnar.rebuilds` gains reason `corrupt` (a manifest, segment, dead set, dictionary or owner map that fails its CRC or format check), distinct from `missing`; counters `dictionary.compactions`, `dictionary.ordinals.reclaimed` | Spec 5.1 warning metric; corruption and absence are different operator signals; spec 5.4 compaction |

---

### Task 1: PositionBitmap

**Files:**
- Create: `engine/src/main/java/com/arcadedb/index/columnar/PositionBitmap.java`
- Modify: `engine/src/main/java/com/arcadedb/graph/olap/simd/GraphOlapVectorOps.java`, `ScalarGraphOlapVectorOps.java`, `SimdGraphOlapVectorOps.java` (spec 5.6: the counting kernels reuse this interface, which already has `bitmaskAnd/Or/Not/Popcount`; add the long-typed filtered variants `void bitmaskAndNot(long[] a, long[] b, long[] out, int length)` and `int bitmaskAndPopcount(long[] a, long[] b, int length)` to the interface and both implementations)
- Test: `engine/src/test/java/com/arcadedb/index/columnar/PositionBitmapTest.java`, `engine/src/test/java/com/arcadedb/graph/olap/simd/GraphOlapVectorOpsFilteredTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces (roadmap contract plus additions):

```java
public final class PositionBitmap {
  public PositionBitmap(int maxRecordsInPage);         // maxRecordsInPage % 64 == 0, else IllegalArgumentException
  public void set(long position); public boolean get(long position); public void clear(long position);
  public void setPage(int page); public void clearPage(int page);     // whole stripe
  public void and(PositionBitmap other); public void andNot(PositionBitmap other); public void or(PositionBitmap other);
  public long cardinality(); public long andCardinality(PositionBitmap other);
  public void forEach(LongConsumer position);          // ascending
  public int[] pages();                                // ascending, pages with at least one bit
  public PositionBitmap snapshot();                    // O(pages) reference copy; later writes to either side copy the touched stripe first
  public PositionBitmap copy();                        // deep copy: every stripe cloned, no sharing (plan 4 mutates it freely)
  public int maxRecordsInPage(); public boolean isEmpty();
}
```

Layout: `int[] pageNumbers` (sorted), `long[][] stripes` (each `long[maxRecordsInPage / 64]`), `int size`, `int[] stripeGeneration` plus `int generation`; a stripe whose generation is older than the bitmap's current generation is shared and is cloned before a write. `page = (int) (position / maxRecordsInPage)`, `slot = (int) (position % maxRecordsInPage)`, matching `LocalBucket.recordPosition` (`LocalBucket.java:519`). A stripe that becomes all zero after `clear`/`and`/`andNot` is removed. Combining bitmaps of different `maxRecordsInPage` throws `IllegalArgumentException`. Stripe-wise `and`/`andNot`/`or`/`andCardinality` call `GraphOlapVectorOpsProvider.getInstance()` (`bitmaskAnd`, `bitmaskAndNot`, `bitmaskOr`, `bitmaskAndPopcount`); TimeSeriesVectorOps was the other candidate and was not picked because it lacks `Not` and popcount over two masks.

- [ ] **Step 1: Write the failing tests**

```java
@Test void setGetClearAcrossPages()            // maxRecordsInPage 2048: set 0, 2047, 2048, 5_000_000_000L; get true; clear 2047; cardinality 3; pages() == {0,1,2441406}
@Test void externalBucketStripeIsFourWords()   // new PositionBitmap(256): set(255), set(256) -> pages {0,1}; cardinality 2
@Test void rejectsSlotCountNotMultipleOf64()   // new PositionBitmap(100) -> IllegalArgumentException
@Test void booleanOpsMatchJavaBitSet()         // seeded Random(42), 10_000 positions in 20 pages each side; and/andNot/or/andCardinality equal BitSet results
@Test void emptiedStripeIsRemoved()            // set(5); clear(5) -> pages().length == 0, isEmpty() true
@Test void forEachIsAscending()                // positions collected equal sorted input
@Test void snapshotIsIsolatedFromLaterWrites() // s = b.snapshot(); b.set(x); b.clear(y) -> s.get(x) false, s.get(y) true; and writes to s do not reach b
@Test void snapshotSharesUntouchedStripes()    // after snapshot + one write in page 3, every other stripe array is the same instance (reflection-free: package-private stripeForTesting(page))
@Test void copyIsDeepAndIndependent()          // c = b.copy(); c.set(x) -> !b.get(x); stripeForTesting(page) of c and b are different instances
@Test void mismatchedSlotCountsAreRefused()    // new PositionBitmap(2048).and(new PositionBitmap(256)) -> IllegalArgumentException
// GraphOlapVectorOpsFilteredTest
@Test void andNotAndAndPopcountAgreeWithScalar() // Random(3), lengths 0..1000: SIMD and scalar implementations equal a plain loop
```

- [ ] **Step 2: Run to verify they fail**

Run: `mvn -o -pl engine -am test -Dtest='PositionBitmapTest,GraphOlapVectorOpsFilteredTest,*GraphOlapVectorOps*Test' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL, `PositionBitmap` does not exist.

- [ ] **Step 3: Implement `PositionBitmap`** as specified. Binary search on `pageNumbers`; `and`/`or` are sorted merges over the page arrays; `Long.bitCount` for cardinality.

- [ ] **Step 4: Run to verify they pass** (same command). Expected: PASS.

- [ ] **Step 5: Stop: changes ready for review** - `PositionBitmap.java`, the three `GraphOlapVectorOps` files, `PositionBitmapTest.java`, `GraphOlapVectorOpsFilteredTest.java`.

---

### Task 2: GlobalDictionary

**Files:**
- Create: `index/columnar/DictionaryValueCodec.java`, `index/columnar/GlobalDictionary.java`
- Test: `index/columnar/GlobalDictionaryTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces:

```java
final class DictionaryValueCodec {                       // type byte + payload; values are already Type.normalizeForKey()'d
  static final byte STRING = 0, LONG = 1, DECIMAL = 2, DOUBLE = 3, BOOLEAN = 4, RID = 5;
  static boolean isSupported(Object normalized);         // String, Long, BigDecimal, Double (NaN/Inf kept by normalizeForKey), Boolean, RID
  static int encode(Object value, Binary out);           // returns bytes written
  static Object decode(Binary in);
  static int compareValues(Object a, Object b);          // type byte order, then String.compareTo / natural order
}
public final class GlobalDictionary {
  public static GlobalDictionary open(Path file, long validBytes, int validEntries) throws IOException; // truncates the file to validBytes (manifest says so)
  public static GlobalDictionary create(Path file) throws IOException; // new generation file (refuses an existing one), header written and fsynced; rebuild (Task 16) and compaction (Task 24) stage on it (R10)
  public static GlobalDictionary createInMemory();       // tests and the creation-time cardinality check only (Task 11); never published
  public int getOrAdd(Object value);                     // writer only; appends to the in-memory chunk and the unsynced file tail
  public int ordinalOf(Object value);                    // -1 when absent; lock-free
  public Object valueOf(int ordinal);                    // IndexOutOfBoundsException beyond size()
  public int size();                                     // volatile published size
  public int rank(int ordinal);
  public int compare(int ordinalA, int ordinalB);
  public DurableState fsync() throws IOException;        // forces the appended tail; returns the durable (bytes, entries) pair to record in the manifest
  public record DurableState(long bytes, int entries) {} // captured together under the writer's sequence lock (plan 3 Task 2 review)
  public static GlobalDictionary openReadOnly(Path file, long validBytes, int entries) throws IOException; // READ only, no tail truncation, new values refused (R16)
  public void rebuildRankView();                         // maintenance pool
  public int[] sortedOrdinals();                         // the rank view's ordinals in value order (String.compareTo for strings); a defensive copy is NOT made, callers must not mutate
  public long heapBytes();
  public void close();
}
```

Decisions:
- Values: `Object[][] chunks` of 4096 entries, appended only; `volatile int size` written after the slot (readers see a consistent prefix, spec 5.4). Lookup: an open-addressing `int[] table` of ordinals keyed by `value.hashCode()`, replaced (never mutated in place) when load exceeds 0.5, published through a `volatile` reference, so `ordinalOf` is lock-free. Single writer is asserted with a captured owner thread only when `-ea`.
- File `dict-<indexFileId>.<generation>.dic` (generation 0 at creation; Task 24 compaction writes generation n+1 and leaves n until no manifest or snapshot names it): header `magic 0x41434431 ("ACD1"), version 1`, then records `[varint length][type byte][payload]`. A record is valid only up to `validBytes` from the manifest; anything after is a crash tail and is truncated at `open`.
- Rank view: `int[] rankOfOrdinal` for the first `rankViewSize` ordinals, values sorted by `DictionaryValueCodec.compareValues`: first by type byte, then within a type `String.compareTo` for strings (roadmap: plan 5 relies on it), natural order for Long/BigDecimal/Double/Boolean, `RID.compareTo`; `int[] sortedOrdinals`. `rank(o)` returns `2 * r` inside the view and `2 * insertionPoint + 1` for newer ordinals (insertion point = number of view values smaller, by binary search on `sortedOrdinals`). `compare(a, b)`: ranks when they differ, else `DictionaryValueCodec.compareValues(valueOf(a), valueOf(b))`.
- `rebuildRankView` is scheduled by the projection when `size() - rankViewSize > max(1024, rankViewSize / 8)`.
- `getOrAdd` writes a record in two `FileChannel` writes: first `[varint length][type byte]`, then the payload. Task 5 places the kill point `"dictionary.appendPartial"` between them (R11), so keep them as two separate writes.
- Persisting a staged dictionary (R10): a rebuild or a compaction calls `create(file)` for the new generation, appends through `getOrAdd`, then `fsync()` before the publish names it; there is no copy from an in-memory dictionary.

- [ ] **Step 1: Write the failing tests**

```java
@Test void ordinalsAreDenseInInsertOrderAndNeverReused()  // "b","a","c","a" -> 0,1,2,1; size 3; valueOf(1) == "a"
@Test void ordinalOfAbsentIsMinusOne()
@Test void mixedValueTypesRoundTrip()                     // String, 12L, new BigDecimal("1.5"), 2.5d, true, new RID(3, 7) survive fsync + reopen
@Test void readersSeeAConsistentPrefixWhileWriterAppends() // writer adds 200_000 values; 4 reader threads loop: s = size(); valueOf(s - 1) never null, ordinalOf(valueOf(i)) == i for i < s
@Test void crashTailBeyondManifestLengthIsTruncated()     // add 10, fsync -> len L; add 5 more without fsync, append 3 garbage bytes; open(file, L, 10) -> size 10; next getOrAdd returns 10
@Test void rankViewOrdersByValue()                        // insert "m","a","z"; rebuildRankView; rank("a") < rank("m") < rank("z")
@Test void sortedOrdinalsFollowStringCompareTo()          // insert "b","B","a","\u00e9"; rebuildRankView; values of sortedOrdinals() == the input sorted with String::compareTo ("B","a","b","\u00e9")
@Test void newOrdinalsRankBetweenViewNeighbours()          // view over "a","c"; add "b" -> rank(a) < rank(b) < rank(c); add "bb" -> rank(b) == rank(bb) and compare(b, bb) < 0
@Test void unsupportedValueIsRefused()                    // getOrAdd(Map.of()) -> IllegalArgumentException
@Test void stagedGenerationSurvivesReopen()               // create(dict-7.1.dic); add 500 values; d = fsync(); close; open(file, d.bytes(), d.entries()) -> size 500, valueOf(i) equal; create on an existing file -> IOException
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest=GlobalDictionaryTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL (missing class).
- [ ] **Step 3: Implement `DictionaryValueCodec` and `GlobalDictionary`.** File writes through one `FileChannel` opened `CREATE, WRITE, READ` (`CREATE_NEW` for `create`); `fsync` is `channel.force(false)`.
- [ ] **Step 4: Run to verify they pass.** Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `DictionaryValueCodec.java`, `GlobalDictionary.java`, `GlobalDictionaryTest.java`.

---

### Task 3: ColumnarValueExtractor (GROUP BY conversion rules)

**Files:**
- Create: `index/columnar/ColumnKind.java`, `OrdinalConsumer.java`, `ValueSink.java`, `ColumnDefinition.java`, `ColumnarValueExtractor.java`, `ColumnarStats.java` (COMPLETE, R3: every cell later tasks write or read, so no later task adds cells)
- Modify: `database/DocumentIndexer.java:912` (make `getPropertyValue(Document, String)` `public static`; it is a private INSTANCE method calling `getNestedPropertyValue`, so keep a private instance delegate)
- Test: `index/columnar/ColumnarValueExtractorTest.java` (extends `TestHelper`), `index/columnar/ColumnarStatsTest.java`

**Interfaces:**
- Consumes: `GlobalDictionary` (Task 2).
- Produces (`ColumnarStats` first, then the extractor types):

```java
public final class ColumnarStats {                     // per database; pure LongAdder / AtomicLong cells keyed by type (and property, reason); no Micrometer here (Task 18 binds it)
  // writers
  public void rebuild(String type, String reason);
  public void conversionFailure(String type, String property);
  public void applyPass();                             // one per apply pass that ran (any projection)
  public void applyPages(String type, long n); public void placeholdersResolved(String type, long n); public void flush(String type); public void merge(String type);
  public void recoveryPages(long n);
  public void recordsReread(String type, long n);      // every slot re-read by an apply pass (numerator of re-read amplification, spec 6.3 / 9)
  public void flushDuration(String type, long nanos); public void mergeDuration(String type, long nanos);
  public void unreadable(String type); public void rebuildPagesReused(String type, long n);
  public void dictionaryCompaction(String type, String property, long reclaimed); // dictionary.compactions +1, dictionary.ordinals.reclaimed += reclaimed (Task 24)
  // gauge sources, set by the owner of the value (projection / manager)
  public void setApplyLagMs(String type, long ms); public void setOverlayRows(String type, long rows); public void setSegments(String type, int n);
  public void setDictionarySize(String type, String property, long n); public void setCardinalityExceeded(String type, String property, boolean exceeded);
  public void setDeadRatio(String type, double ratio); public void setOpenFiles(String type, int n);
  // readers (tests of Tasks 10-24 and the Task 18 binder)
  public long rebuilds(String type, String reason); public long rebuildsTotal();
  public long conversionFailures(String type, String property);
  public long applyPasses(); public long applyPagesCount(String type); public long placeholdersResolvedCount(String type);
  public long flushCount(String type); public long mergeCount(String type); public long recoveryPagesCount();
  public long recordsRereadCount(String type); public long unreadableCount(String type); public long rebuildPagesReusedCount(String type);
  public long flushDurationCount(String type); public long flushDurationTotalNanos(String type); public long mergeDurationCount(String type); public long mergeDurationTotalNanos(String type);
  public long dictionaryCompactions(String type, String property); public long dictionaryOrdinalsReclaimed(String type, String property);
  public long applyLagMs(String type); public long overlayRows(String type); public int segments(String type); public long dictionarySize(String type, String property);
  public boolean cardinalityExceeded(String type, String property); public double deadRatio(String type); public int openFiles(String type);
  public void forEachKey(StatsKeyConsumer consumer);  // (kind, type, property, reason) for binder discovery
  @FunctionalInterface public interface StatsKeyConsumer { void accept(String kind, String type, String property, String reason); }
}
```

Cell names (the `kind` passed to `forEachKey`) are exactly the Task 18 meter names without the `arcadedb.columnar.` prefix (`rebuilds`, `dictionary.compactions`, `dictionary.ordinals.reclaimed`, `apply.pages`, `apply.placeholders.resolved`, `flush.count`, `merge.count`, `conversion.failures`, `recovery.pages`, `apply.records.reread`, `apply.unreadable`, `rebuild.pages.reused`, `flush.duration`, `merge.duration`, `apply.lag.ms`, `overlay.rows`, `segments`, `segments.dead.ratio`, `dictionary.size`, `cardinality.exceeded`, `files.open`), so Task 18 never needs a cell that is not here. `applyPasses` is engine-only (no meter).

Extractor types:

```java
public enum ColumnKind { ORDINAL, ORDINAL_LIST, LONG, DOUBLE;
  public static ColumnKind of(Type propertyType, boolean byItem); }   // IndexException for unsupported (see table)
@FunctionalInterface public interface OrdinalConsumer { void accept(long position, int ordinal); }
public interface ValueSink { void ordinal(int o); void ordinals(int[] sorted, int count); void longValue(long v);
  void doubleValue(double v); void missing(); void unknownValue(Object normalized); }
public record ColumnDefinition(int indexFileId, String property, Type propertyType, ColumnKind kind, boolean byItem,
    ChronoUnit precision) { public JSONObject toJSON(); public static ColumnDefinition fromJSON(JSONObject json); }
public final class ColumnarValueExtractor {               // one per type (roadmap `extractor(String typeName)`); dictionaries are per BUCKET and property (R5)
  @FunctionalInterface public interface DictionaryResolver { GlobalDictionary dictionary(int bucketId, String property); } // Task 12: (b, p) -> manager.projection(b).dictionary(p)
  ColumnarValueExtractor(String typeName, ColumnDefinition[] columns, DictionaryResolver dictionaries, ColumnarStats stats);
  public void extract(Document record, String property, ValueSink sink);          // read-only: dictionary of record.getIdentity().getBucketId(); never grows it, unknown -> unknownValue
  public void extract(Document record, String property, GlobalDictionary dictionary, ValueSink sink); // read-only against an explicit generation: plan 4 patching passes ColumnarSnapshot.dictionary(property)
  void extractForApply(Document record, ColumnDefinition column, GlobalDictionary dictionary, ValueSink sink); // apply thread only: getOrAdd on the projection's current writer dictionary (Task 15 passes projection.dictionary(property))
  public long toColumnLong(String property, Object value);  // the LONG-column encoding of a literal (range bounds, histogram bounds): same rules as the table below; IllegalArgumentException when not convertible or the column is not LONG
  public TimeUnit dateUnit(String property);                // DATE -> DAYS, DATETIME_SECOND -> SECONDS, DATETIME -> MILLISECONDS, DATETIME_MICROS -> MICROSECONDS, DATETIME_NANOS -> NANOSECONDS; null for non-date columns
}
```

Conversion table (raw value read with `DocumentIndexer.getPropertyValue`, then `Type.normalizeForKey` at `schema/Type.java:2322`, which is what `GroupByKey` does in `AggregateRowEvaluator:450`; note it keys Double 2.0 as Long 2 and 1.5 as BigDecimal):

| Property type | Kind | Value accepted | Anything else |
|---|---|---|---|
| STRING, BOOLEAN, LINK | ORDINAL | every normalized value `DictionaryValueCodec.isSupported` accepts: `String`, `Long`, `BigDecimal`, `Double` (NaN/Inf), `Boolean`, `RID` (R6: a STRING property holding `5` keys `Long 5`, exactly as GROUP BY) | missing + `conversion.failures` (a `Map`, an embedded document, a list) |
| BYTE, SHORT, INTEGER, LONG | LONG | normalized `Long` | missing + failure (a non-integral `BigDecimal`) |
| FLOAT, DOUBLE | DOUBLE | any `Number` -> `doubleValue()`; Float widened like `Type.widenFloat` (`schema/Type.java:1817`) | missing + failure |
| DATE | LONG | `DateUtils.dateToEpochDays(v)` (`utility/DateUtils.java:218`) | missing + failure |
| DATETIME, DATETIME_MICROS, DATETIME_NANOS, DATETIME_SECOND | LONG | `DateUtils.dateTimeToTimestamp(v, DateUtils.getPrecisionFromType(type))` (`:295`, `:953`) | missing + failure |
| LIST / ARRAY_OF_* with `BY ITEM` | ORDINAL_LIST | each item normalized and supported; sorted, deduplicated ordinals; failing items dropped and counted; zero items left -> missing | - |
| LIST without `BY ITEM`, MAP, EMBEDDED, DECIMAL, BINARY | - | refused at index creation (`IndexException` from `ColumnKind.of`) | - |

`null` or an absent property -> `missing()` without a failure. `stats.conversionFailure(typeName, property)` per failure.

- [ ] **Step 1: Write the failing tests**

```java
@Test void stringIsOrdinalOfNormalizedValue()          // "Nike" -> ordinal 0; second record "Nike" -> 0
@Test void integralNumbersShareOneLongKey()            // LONG property set to Integer 12, then Long 12 -> longValue(12) both
@Test void dateTimeUsesThePropertyPrecision()          // DATETIME_MICROS value -> epoch micros equal DateUtils.dateTimeToTimestamp(v, MICROS)
@Test void listByItemIsSortedAndDeduplicated()         // ["b","a","b"] -> ordinals sorted, count 2
@Test void unconvertibleValueIsMissingAndCounted()     // schemaless Map inside tags list + a non-integral BigDecimal in a LONG column -> missing(); stats.conversionFailures("Asset","fileSize") == 1.
                                                       // Planting the raw value: save a MutableDocument while "fileSize" is NOT yet in the schema (schemaless, set(new BigDecimal("1.5"))), THEN createProperty("fileSize", LONG); the stored value stays BigDecimal
@Test void stringPropertyHoldingANumberKeysLikeGroupBy() // R6: STRING property brand set to Integer 5 through a schemaless save -> ordinal of Long 5; GROUP BY key of the same record is Long 5; no conversion failure
@Test void readOnlyExtractionNeverGrowsTheDictionary() // extract() of an unseen value -> unknownValue("new"); dictionary.size() unchanged
@Test void dictionaryIsResolvedPerBucket()            // R5: type with 2 buckets, resolver gives one dictionary per bucket; "Nike" in bucket A and "Adidas","Nike" in bucket B -> each record's ordinal is the one of its own bucket's dictionary
@Test void explicitDictionaryOverloadUsesIt()          // extract(record, "brand", otherGeneration, sink) answers otherGeneration's ordinal, not the resolver's
@Test void matchesGroupByKeysOnRandomData()            // ColumnarTestData-like 2_000 docs; for brand and tags: multiset of extracted values (mapped back through valueOf) == GROUP BY keys of
                                                       // SELECT brand AS v, count(*) AS c FROM Asset GROUP BY brand  /  SELECT v, count(*) AS c FROM (SELECT tags.asSet() AS v FROM Asset UNWIND v) GROUP BY v
@Test void toColumnLongMatchesExtraction()            // for fileSize 12 (Integer), publishDate (LocalDateTime) and a DATE value: toColumnLong(p, v) == the longValue the extractor emits for a record holding v
@Test void toColumnLongRefusesUnconvertible()          // toColumnLong("fileSize", "abc") and toColumnLong("brand", 1) -> IllegalArgumentException
@Test void dateUnitPerPropertyType()                   // DATE DAYS, DATETIME MILLISECONDS, DATETIME_MICROS MICROSECONDS, fileSize null
@Test void unsupportedPropertyTypesAreRefused()        // ColumnKind.of(Type.DECIMAL,false), of(Type.MAP,false), of(Type.LIST,false) -> IndexException
// ColumnarStatsTest
@Test void countersAreKeyedByTypeAndReason()           // rebuild("Asset","create") x2, rebuild("Asset","corrupt") -> rebuilds(..,"create") == 2, rebuildsTotal() == 3, other type 0
@Test void readersReturnWhatWritersAdded()             // every writer above once (applyPass, applyPages 5, placeholdersResolved 2, flush, merge, recoveryPages 7, unreadable, dictionaryCompaction(.., 600) ...) -> each reader answers it
@Test void forEachKeyListsEveryRegisteredCell()        // after one write per cell: forEachKey names every kind of the cell-name list above, with its type/property/reason
@Test void cellsAreAllocationFreeAfterFirstUse()       // second write to the same key allocates nothing (ThreadMXBean allocated bytes delta == 0 over 10_000 calls)
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest='ColumnarValueExtractorTest,ColumnarStatsTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement** `ColumnarStats` (complete, as listed: later tasks only call it, Task 18 only binds it), the four small types and the extractor. Reuse a per-extractor `int[]` scratch for list ordinals, sort with `Arrays.sort(int[], 0, n)` and dedup in place.
- [ ] **Step 4: Run to verify they pass.** Also run `mvn -o -pl engine -am test -Dtest='*IndexByItem*Test,*ByItem*Test,Issue*TypeIndex*Test,IndexKeyEqualityTest,CheckDatabaseFixDocumentTypeTest' -Dsurefire.failIfNoSpecifiedTests=false` (the visibility change; these classes exercise `DocumentIndexer`). Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - the five new classes, `ColumnarStats.java`, `DocumentIndexer.java`, `ColumnarValueExtractorTest.java`, `ColumnarStatsTest.java`.

---

### Task 4: Segment writer and reader

**Files:**
- Create: `index/columnar/SegmentFormat.java`, `ColumnReader.java`, `ColumnarSegmentView.java`, `SegmentWriter.java`, `SegmentReader.java`, `SegmentFile.java`, `RowValues.java`
- Modify: `engine/block/BlockCache.java` (test accessors only, V-M2: `public boolean containsForTesting(long fileId, long blockId, int column)` and `public long entriesForTesting(long fileId)`)
- Test: `index/columnar/SegmentRoundTripTest.java`, `index/columnar/SegmentCorruptionTest.java`

**Interfaces:**
- Consumes: plan 2 `NumericCodecChooser.encodeBest/get/decode/count`, `BitPackedCodec.encode(long[], ..)` (ordinals widened into a reusable `long[]` scratch), `BlockHeader.forColumns/read(buffer, expectedMagic)/verifyPayload/columnOffset/columnSize/totalSize/crc32`, `BlockCache.instance().get/newFileId/invalidateFile/hits/misses` plus the new `containsForTesting/entriesForTesting` (V-M2), `BlockCorruptionException`; `PositionBitmap` (Task 1); `ColumnDefinition`, `ColumnKind` (Task 3).
- Produces:

```java
public interface ColumnReader { /* exactly the roadmap contract; javadoc of forEachOrdinal(matches, consumer): visits exactly the positions of matches AND live() of its view (R12: dead rows are never visited, callers never AND live themselves), in ascending order, each once (ORDINAL_LIST: once per ordinal, ordinals ascending within a position); it consults presence and the dead set only for matches.pages() and never materializes live() */ }
public interface ColumnarSegmentView { PositionBitmap live() /* computed once per reader and cached: immutable per dead version */; PositionBitmap live(int fromPage, int toPageExclusive);
  ColumnReader column(String property); long minLong(String property); long maxLong(String property); }
final class SegmentFormat {
  static final int FILE_MAGIC = 0x41435347;          // "ACSG"
  static final int BLOCK_MAGIC = 0x41434231;         // "ACB1"
  static final int VERSION = 1;
  static String segmentFileName(long segmentId);     // "seg-%016x.acs"
  static String deadFileName(long segmentId, int version); // "seg-%016x.dead.%d"
}
final class SegmentWriter implements AutoCloseable {
  SegmentWriter(Path file, long segmentId, int maxRecordsInPage, ColumnDefinition[] columns, int blockRows);
  void addRow(long position, long pageVersion, RowValues values);   // positions strictly ascending, else IllegalStateException
  SegmentReader.Meta finish() throws IOException;    // writes footer, fsyncs the file, returns SegmentReader.Meta (V-M5)
}
final class SegmentReader implements ColumnarSegmentView, AutoCloseable {
  static SegmentReader open(Path file, PositionBitmap dead) throws IOException;  // BlockCorruptionException on any mismatch
  record Meta(long segmentId, long rows, int minPage, int maxPage) {}  // V-M5, consumed by Task 5 SegmentEntry
  long segmentId(); long rowCount(); long deadCount(); int minPage(); int maxPage();
  long pageVersion(int page);                         // -1 when the segment does not cover the page
  int row(long position);                             // -1 when absent
  SegmentReader withDead(PositionBitmap dead);        // same SegmentFile (retained, no cache invalidation), new live view
  SegmentFile file();
  void close();                                       // releases its SegmentFile reference only; NEVER invalidates the cache (R1)
}
final class SegmentFile {                             // one per segment file on disk, shared by every SegmentReader over it (withDead copies included)
  static SegmentFile open(Path file);                 // cacheFileId = BlockCache.instance().newFileId(), ref count 1; the reader created by SegmentReader.open owns this first reference (V-M5)
  Path path(); long cacheFileId();
  SegmentFile retain(); void release();               // ref count; reaching 0 does NOT delete or invalidate
  void deleted();                                     // called by the owner AFTER the file was deleted from disk (Tasks 6/8 last release, 10 merge, 17 drop, 24 compaction): BlockCache.instance().invalidateFile(cacheFileId) exactly once
}
```

R1: segment files are immutable, so a cache file id never needs invalidating while its file exists. `invalidateFile` bumps a JVM-wide epoch and sweeps every entry, so calling it per reader `close()` or per `withDead` (every apply pass with a delete) would wipe a live segment's cache every 100 ms and refuse admission of in-flight loads in every database. It runs only from `SegmentFile.deleted()`.

`RowValues` is a reusable mutable holder (`final class RowValues { int[] ordinal; int[][] list; int[] listCount; long[] longs; double[] doubles; boolean[] present; }`, one slot per column), owned by the caller.

File layout of `seg-<id>.acs` (one segment, all columns of the bucket, spec 5.2):
1. File header block (BlockHeader magic `FILE_MAGIC`): segmentId, maxRecordsInPage, rowCount, column definitions JSON (UTF-8).
2. Presence: per page block of 1024 pages, `BLOCK_MAGIC`, columns `[pageNumbers int[], pageRowStart int[], stripes long[]]`; row of a position = `pageRowStart[i] + popcount(stripe bits below slot)`. Stored ONCE per segment and shared by all its columns (spike: presence is 48% to 79% of a narrow column, spec 5.2), raw `long[]` stripes (spike defaults table).
3. Page versions: one block per presence block, `long[]` per covered page (spec 6.5 per-page versions).
4. Row blocks of `blockRows` rows: one `BlockHeader` per row block with `3 * columns` sub-columns: `[k*3]` value-presence bits, `[k*3+1]` values (ORDINAL/LONG: `NumericCodecChooser.encodeBest`; DOUBLE: raw big-endian doubles; ORDINAL_LIST: concatenated ordinals with `BitPackedCodec`), `[k*3+2]` list offsets (`NumericCodecChooser`, empty for other kinds).
5. Footer: block directory `(kind, firstRow, offset, size)` plus per column `min`, `max` (LONG/DOUBLE as `Double.doubleToLongBits` for DOUBLE; ORDINAL: min/max ordinal), then `int footerOffset`, `int footerCrc32`, `int FILE_MAGIC`.

Reads: `FileChannel` positional reads (opened per cache miss, closed after, so no file handle is held per segment - spec anti-goal on open files). Cache (R23, spike observation 3): each row block's verified ENCODED sub-columns are cached as `byte[]` under column id `k` (`BlockCache.instance().get(segmentFile.cacheFileId(), blockIndex, k, loader, sizer)`), decoded forms under column id `k + 3 * columns`; ORDINAL / ORDINAL_LIST decode to `int[]`, LONG to `long[]`. Point mode reads the encoded `byte[]` with `NumericCodecChooser.get` and never decodes; a sub-column whose encoded width is <= 8 bits is never decoded, dense mode walks it with `get` too (a 2-bit column decoded to `long[16384]` would cost 32x its encoded size in the shared cache). Entries above `maxBytes / 16` are returned uncached by plan 2 (a decoded `long[16384]` is 128 KB: fine at the 16 MB floor). The cache file id comes from the shared `SegmentFile` (R1), never invalidated on `close`. Footer and file header are kept on heap (sizes bounded by blocks, not by rows). `forEachOrdinal(matches, c)` (R12) walks `matches.pages()` only: per matched page it reads the presence block and the dead set of that page, ANDs the match stripe with presence and NOT dead, and for the row blocks those positions fall in uses dense mode when matches in the block exceed 1/8 of its rows, point mode otherwise. `live()` = presence AND NOT dead, computed once per reader (lazily, then cached: a reader's dead set never changes, `withDead` makes a new reader).

Plan 2 deltas to handle: (a) `BlockHeader.crc32` throws `IllegalArgumentException` for a region past the limit, so bounds-check `footerOffset` (and every directory `offset + size` against the file size) before computing a CRC, throwing `BlockCorruptionException`; (b) codec `get` relies on JVM bounds checks (Blockwise checks the index only), so at block load verify `NumericCodecChooser.count(encoded) == rows of the block` and wrap any codec `IllegalArgumentException` / `IndexOutOfBoundsException` into `BlockCorruptionException`.

- [ ] **Step 1: Write the failing tests**

```java
// SegmentRoundTripTest
@Test void allKindsRoundTrip()                   // 4 columns (ORDINAL, ORDINAL_LIST, LONG, DOUBLE), 50_000 rows over sparse positions (gaps of 1..9), blockRows 16384; every row read back equals the input; missing values isPresent false
@Test void rowByRankOverPresence()               // row(position) == index in input order; row(absent) == -1
@Test void minMaxPerColumnSkipping()             // minLong/maxLong of LONG column equal input extremes
@Test void forEachOrdinalDenseAndPointAgree()    // matches = all rows vs every 50th row: consumer receives exactly matching (position, ordinal) pairs
@Test void forEachOrdinalVisitsPositionsAscending() // matches spanning 3 row blocks and both dense and point modes (Task 6 adds the same assertion for OverlayPageBlock views): every position > the previous one (list column: non-decreasing, ordinals ascending within a position)
@Test void deadPositionsLeaveTheLiveView()       // dead = {p1,p2}; live().get(p1) false; live().cardinality() == rows - 2
@Test void pageVersionsAreRecorded()             // pageVersion(page) == value passed to addRow for that page; uncovered page -> -1
@Test void decodedBlocksGoThroughTheBlockCache() // second full read: BlockCache.instance().hits() grows by at least the number of row blocks read (the counter is JVM-wide: never assert misses unchanged)
@Test void noFileHandleIsHeldBetweenReads()      // open, read, then Files.delete(file) succeeds on every OS (close-per-read); a later read of a cached block still answers
@Test void positionsMustAscend()                 // addRow(10), addRow(5) -> IllegalStateException
@Test void presenceIsStoredOncePerSegment()      // a 1-column and a 4-column segment over the same positions: their presence block bytes are equal
@Test void pointModeNeverCachesADecodedBlock()   // matches every 500th row (point mode) over an uncached segment: BlockCache holds entries only under column ids < 3 * columns (encoded), none under k + 3 * columns
@Test void narrowColumnIsNeverDecoded()          // ORDINAL column with 3 distinct values (width 2), dense matches: no entry under its decoded column id; ordinals still exact
@Test void forEachOrdinalSkipsDeadRows()         // R12: dead = {p1,p2}, matches = all -> consumer never receives p1, p2
@Test void forEachOrdinalTouchesOnlyMatchedPages() // matches on 2 of 200 pages, cold cache: `entriesForTesting(cacheFileId)` == presence blocks + row blocks of those 2 pages (per-file count, not the JVM-wide counter, V-M3) (live() never materialized)
@Test void closeAndWithDeadNeverInvalidateTheCache() // R1: read, withDead(..), close the first reader: a re-read through the copy is a cache hit; `SegmentFile.deleted()` -> `entriesForTesting(oldCacheFileId) == 0` (V-M4)
// SegmentCorruptionTest
@Test void footerOffsetPastTheEndIsBlockCorruption() // footerOffset rewritten to fileSize + 100 -> BlockCorruptionException, not IllegalArgumentException
@Test void codecCountMismatchIsBlockCorruption()   // rewrite a block's encoded count (CRC recomputed) -> BlockCorruptionException at block load
@Test void flippedPayloadByteIsBlockCorruption() // flip one byte inside a row block -> reading that column throws BlockCorruptionException
@Test void truncatedFileIsBlockCorruption()      // truncate last 7 bytes -> open throws BlockCorruptionException (footer magic/CRC)
@Test void corruptColumnSizeNeverAllocates()     // flip a size byte in a BlockHeader -> BlockCorruptionException, not OutOfMemoryError/BufferUnderflowException
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest='SegmentRoundTripTest,SegmentCorruptionTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement** `SegmentFormat`, the two interfaces, `RowValues`, `SegmentFile`, `SegmentWriter` (buffers one row block in primitive arrays, encodes and writes it when full), `SegmentReader`.
- [ ] **Step 4: Run to verify they pass.** Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - the seven classes (`SegmentFormat`, `ColumnReader`, `ColumnarSegmentView`, `SegmentWriter`, `SegmentReader`, `SegmentFile`, `RowValues`), both tests.

---

### Task 5: Store, dead sets and manifest (spec 5.5)

**Files:**
- Create: `index/columnar/ColumnarStore.java`, `DeadSetFile.java`, `ColumnarManifest.java`, `ColumnarTestHooks.java`
- Modify: `index/columnar/GlobalDictionary.java` (R11: `ColumnarTestHooks.reach("dictionary.appendPartial")` in `getOrAdd` between the record header write and the payload write, Task 2 keeps them as two writes)
- Test: `index/columnar/ColumnarManifestTest.java`, `index/columnar/ColumnarStoreTest.java`, `index/columnar/GlobalDictionaryTest.java` (one added test); fixture `index/columnar/KillPoint.java`

**Interfaces:**
- Consumes: `SegmentReader.Meta` (Task 4), `PositionBitmap`, `GlobalDictionary` (Task 2).
- Produces:

```java
public final class ColumnarStore {
  public static final String ROOT_DIR = "columnar";                           // <databasePath>/columnar, no dot: FileManager.scanDirectoryForComponentFiles skips it (FileManager.java:169-189)
  public static final String REBUILD_MARKER = "REBUILD";                      // <root>/REBUILD holds a reason name
  public static final String NOWAL_MARKER = "NOWAL";                          // <bucketDir>/NOWAL: the ONLY record of a no-WAL write not covered by a flush (R8; Task 13 writes, Task 10 deletes, Task 16 reads)
  public static Path bucketDir(Path databasePath, int bucketFileId);          // columnar/b<fileId> (R9): keyed by file id only, so ALTER TYPE ... NAME (bucket files renamed in place, file id kept) keeps the data; the bucket name lives in the manifest
  public static void fsyncFile(Path file) throws IOException; public static void fsyncDirectory(Path dir) throws IOException; // dir fsync tolerated as no-op where unsupported (Windows)
  public static void writeAtomically(Path target, byte[] content) throws IOException;      // tmp, force, ATOMIC_MOVE, fsync dir
  public static void prepareForSnapshotInstall(Path databasePath) throws IOException;     // deletes columnar/ if present, writes columnar/REBUILD = "snapshot_install", fsynced
  public static String readRebuildReason(Path databasePath);                               // null when absent
  public static void clearRebuildReason(Path databasePath) throws IOException;
  public static boolean isProjectionEntry(String relativeName);                           // "columnar" or "columnar/..." (guards for snapshot ship and backup)
  static List<String> deleteUnreferenced(Path bucketDir, Set<String> referenced, Set<String> heldByReaders); // returns the names deleted, so the caller can SegmentFile.deleted() each deleted segment (R1, R2); markers (NOWAL, manifest.json, tmp files) are never touched
}
final class DeadSetFile { static void write(Path file, PositionBitmap dead) throws IOException; static PositionBitmap read(Path file, int maxRecordsInPage) throws IOException; } // BlockHeader-framed, CRC verified
public final class ColumnarManifest {
  record SegmentEntry(long segmentId, String file, long rows, int deadVersion, long deadRows, int minPage, int maxPage) {}
  record DictionaryEntry(int indexFileId, int generation, String file, int entries, long bytes) {}
  ColumnarManifest(long generation, int bucketFileId, String bucketName, int maxRecordsInPage, ColumnDefinition[] columns, List<SegmentEntry> segments,
      List<DictionaryEntry> dictionaries, String ownerMapFile, String externalOwnerMapFile, long lastTransactionId); // V-M6
  ColumnarManifest withGeneration(long next);                                  // V-M6
  int formatVersion();  /* 1 */ long generation(); int bucketFileId(); String bucketName(); int maxRecordsInPage();
  ColumnDefinition[] columns(); List<SegmentEntry> segments(); List<DictionaryEntry> dictionaries();
  String ownerMapFile();                                                       // ContentOwnerMap snapshot of this generation (Task 6)
  String externalOwnerMapFile();                                               // R7: owner map of the paired external-property bucket; null when the bucket has none
  long lastTransactionId();                                                    // informational: database last tx id when published (no task reads it for recovery)
  // no noWalPending(): the NOWAL file is the single source of truth (R8)
  JSONObject toJSON(); static ColumnarManifest fromJSON(JSONObject json);      // getters with defaults
  void publish(Path bucketDir) throws IOException;                             // writeAtomically("manifest.json")
  static ColumnarManifest load(Path bucketDir) throws IOException;             // null when absent; a bad JSON or formatVersion != 1 -> BlockCorruptionException
}
public final class ColumnarTestHooks {                                         // production: all null
  public static volatile Consumer<String> killPoint;
  static void reach(String name);                                              // calls killPoint if set; KillPoint may throw SimulatedCrash
  public static final class SimulatedCrash extends RuntimeException {}
}
```

Publish order (spec 5.5), used by flush, merge, rebuild and dictionary compaction: (1) every new segment file already fsynced by `SegmentWriter.finish`; (2) new dead files written and fsynced; (3) every dictionary `fsync()` (its returned `DurableState` pair is what the manifest's `DictionaryEntry(entries, bytes)` records) whose new entries a new segment references, lengths recorded; (4) owner map file (and the external owner map file, R7) written and fsynced; (5) `manifest.json` via `writeAtomically`; (6) only then files no manifest names and no live snapshot holds are deleted. Kill points: `"publish.segmentsSynced"`, `"publish.dictionariesSynced"`, `"publish.manifestTmpWritten"`, `"publish.manifestRenamed"`, plus `"dictionary.appendPartial"` inside `GlobalDictionary.getOrAdd` (R11, used by Task 21).

`KillPoint` (test fixture): `static KillPoint arm(String name)` installs `ColumnarTestHooks.killPoint`; the reaching thread blocks until `releaseAsCrashed()`, then throws `SimulatedCrash`; `awaitReached(long ms)` returns boolean; `disarm()` in `@AfterEach`.

- [ ] **Step 1: Write the failing tests**

```java
// ColumnarManifestTest
@Test void jsonRoundTrip()                         // every field survives toJSON/fromJSON
@Test void publishIsAtomic()                       // publish gen 1; arm "publish.manifestTmpWritten" in a second publish on another thread; releaseAsCrashed -> load() returns gen 1, not a partial file
@Test void missingFieldsUseDefaults()              // fromJSON({"formatVersion":1}) -> empty segments, generation 0
@Test void unknownFormatVersionIsCorruption()      // {"formatVersion":99} -> BlockCorruptionException
// ColumnarStoreTest
@Test void projectionDirIsInvisibleToFileManager() // create columnar/ with files in a TestHelper database dir; reopen; database.getFileManager().getFiles() names no path under columnar/
@Test void unreferencedFilesAreDeletedButHeldOnesStay()
@Test void snapshotInstallPreparationWipesAndMarks() // existing columnar/x -> prepareForSnapshotInstall -> only columnar/REBUILD remains, readRebuildReason == "snapshot_install"
@Test void isProjectionEntryMatchesOnlyTheRoot()   // "columnar" true, "columnar/a" true, "columnar.bucket" false, "Asset_0.1.65536.v0.bucket" false
@Test void bucketDirIsKeyedByFileIdOnly()          // R9: bucketDir(db, 7) == db/columnar/b7; a manifest published there with bucketName "Asset_0" loads with that name after the type is renamed (directory unchanged)
@Test void deleteUnreferencedReturnsDeletedNames() // R2: referenced {a}, held {b}, on disk {a,b,c,NOWAL,manifest.json} -> returns [c]; NOWAL and manifest.json kept
@Test void externalOwnerMapFileRoundTrips()        // R7: toJSON/fromJSON keeps externalOwnerMapFile; absent -> null
// GlobalDictionaryTest (added)
@Test void appendPartialKillPointLeavesATruncatableTail() // arm "dictionary.appendPartial"; getOrAdd("x") on another thread; releaseAsCrashed -> file holds a header without payload; open(file, L, n) with the last fsynced length -> size n
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest='ColumnarManifestTest,ColumnarStoreTest,GlobalDictionaryTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement** the four classes, the `KillPoint` fixture and the `getOrAdd` kill point.
- [ ] **Step 4: Run to verify they pass.** Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `ColumnarStore.java`, `DeadSetFile.java`, `ColumnarManifest.java`, `ColumnarTestHooks.java`, `GlobalDictionary.java`, `KillPoint.java`, the three tests.

---

### Task 6: Overlay, content owner map, projection state and snapshot

**Files:**
- Create: `index/columnar/OverlayPageBlock.java`, `ColumnarOverlay.java`, `ContentOwnerMap.java`, `ProjectionState.java`, `ColumnarSnapshot.java`, `RowBuffer.java` (V-M7)
- Modify: `index/columnar/PositionBitmap.java` (V-M7: package-private `static PositionBitmap wrapShared(int maxRecordsInPage, int[] pages, long[][] stripes, int count)`: every stripe starts shared, so a write clones it)
- Test: `index/columnar/ColumnarOverlayTest.java`, `index/columnar/ContentOwnerMapTest.java`, `index/columnar/ProjectionStateTest.java`

**Interfaces:**
- Consumes: Tasks 1, 3, 4, 5.
- Produces:

```java
final class OverlayPageBlock {                        // immutable rows of ONE bucket page whose live value lives in the overlay
  static OverlayPageBlock build(int page, int maxRecordsInPage, ColumnDefinition[] columns, long pageVersion, RowBuffer rows);
  int page(); long pageVersion(); int rowCount(); long[] liveStripe();
  // never mutated after build (R13): a published view may share it with a snapshot; a remap (Task 24) builds a NEW block
}
final class ColumnarOverlay {                         // owned by the apply thread
  ColumnarOverlay(LongSupplier clockMs);
  void replacePage(OverlayPageBlock block);           // null rows -> page removed from the overlay
  OverlayPageBlock page(int page);
  long rows();
  long oldestRowMs();                                 // P39: clock time of the first replacePage since creation or the last freeze; 0 when empty (Task 10 shouldFlush)
  ColumnarSegmentView publish();                      // immutable view: sorted int[] pages + OverlayPageBlock[] shared by reference
  ColumnarOverlay freeze();                           // hands the current content to a flush and starts empty; the frozen view stays in ProjectionState until the segment replaces it
}
public final class ContentOwnerMap {                  // primitive open addressing; owner positions are bucket positions
  public void setOwnerPages(long owner, int[] contentPages, int count);   // replaces the owner's previous pages
  public void removeOwner(long owner);
  public int owners(int contentPage, long[] out);     // returns count; out sized by ownerCountUpperBound(contentPage)
  public int ownerCountUpperBound(int contentPage);
  public void writeTo(Path file) throws IOException; public static ContentOwnerMap readFrom(Path file) throws IOException; // BlockHeader-framed
  public ContentOwnerMap copy();                      // for publication with a manifest
}
final class ProjectionState {                         // immutable
  ProjectionState(long generation, SegmentReader[] segments, ColumnarSegmentView[] overlays, Map<String, GlobalDictionary> dictionaries, Set<String> files, boolean usable,
      Consumer<ProjectionState> onLastRelease);       // files (V-B7): names of every segment, dead set, dictionary and owner map file this state reads, computed by the projection from the manifest it built the state from
                                                      // dictionaries: the generation the segments are encoded with; onLastRelease runs once when the count reaches 0 (R2)
  ProjectionState retain(); void release();           // ref count; 0 -> releases the SegmentFile references it took in its constructor (segments[i].file().retain()), never SegmentReader.close(); readers are shared by consecutive states and closed by the projection when no retained state names them (V-M8); then calls onLastRelease
  Set<String> files();                                // returns the constructor's files set (V-B7)
}
public interface ColumnarSnapshot extends AutoCloseable { /* roadmap contract */ GlobalDictionary dictionary(String property); }
```

Overlay rules: `replacePage` keeps every slot whose live value is in the overlay (changed now, or changed in an earlier pass and still current); slots unchanged against the segments are not stored. `publish()` copies at most `pages` references (bounded by `COLUMNAR_FLUSH_MAX_ROWS`). Within one page a position appears once, so at most one live value per position holds across segments and overlay (spec 5.2). The published view's `live()` wraps the blocks' `liveStripe()` arrays without copying them (they are immutable) through `PositionBitmap.wrapShared` (V-M7); its `forEachOrdinal(matches, c)` follows R12 (`matches AND live()`, matched pages only, ascending).

File deletion (R2): `ProjectionState` holds no path. Its owner (`ColumnarProjection`, Task 8) passes `onLastRelease`, keeps the set of retained states, and on the callback calls `ColumnarStore.deleteUnreferenced(bucketDir, current manifest files, union of files() of every still-retained state)`, then `SegmentFile.deleted()` for each deleted segment name (R1). Task 6's test drives the same callback by hand.

R7: a projection of a bucket with external properties holds a second `ContentOwnerMap` for the external file (owner positions are primary positions, content pages are external page numbers), persisted as `ColumnarManifest.externalOwnerMapFile()`.

`ContentOwnerMap`: `int[] keys` (content page, -1 empty), `int[] head` into a `long[] ownerPool` of fixed 4-long nodes with a next index, plus `LongLongHashMap ownerToFirstPageNode` (`utility/LongLongHashMap`) to remove an owner's old pages; growth doubles; no boxing.

- [ ] **Step 1: Write the failing tests**

```java
// ColumnarOverlayTest
@Test void replacePageSupersedesPreviousRows()     // replace page 3 twice; publish().column("brand").ordinal(p) is the second value
@Test void publishedViewIsImmutable()              // v = publish(); replacePage(...); v still answers the old value
@Test void freezeHandsContentToFlushAndStartsEmpty()
@Test void liveIsExactlyTheStoredSlots()
@Test void overlayForEachOrdinalIsAscending()      // pages replaced out of order (7, 2, 5): forEachOrdinal over the published view visits positions ascending
@Test void oldestRowAgeRestartsAtFreeze()          // fake clock: replacePage at 1000, 1500 -> oldestRowMs() == 1000; freeze(); empty -> 0; replacePage at 2000 -> 2000
@Test void publishedLiveSharesTheStripes()         // publish().live() stripes are the blocks' liveStripe() instances (package-private stripeForTesting)
// ContentOwnerMapTest
@Test void ownersOfASharedContentPage()            // owners 10 and 20 both on page 7 -> owners(7) == {10,20}
@Test void setOwnerPagesReplacesOldPages()         // owner 10 pages {7,8} then {9} -> owners(7) empty, owners(9) == {10}
@Test void survivesWriteAndRead()
@Test void scalesWithoutBoxing()                   // 1_000_000 owners: structural, not a Runtime heap delta (flaky): package-private heapBytesForTesting() summed from the array lengths is < 64 bytes per owner
// ProjectionStateTest
@Test void segmentFilesOutliveReleaseUntilLastReader() // R2: onLastRelease = test deleter calling ColumnarStore.deleteUnreferenced(dir, manifestFiles, {}); retain twice, drop the segment from the manifest set, release once -> file exists; release again -> deleted, SegmentFile.deleted() called once
@Test void stillRetainedStateKeepsItsFiles()        // two states sharing segment s; the older reaches 0 while the newer is retained -> s kept (heldByReaders = union of retained files())
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest='ColumnarOverlayTest,ContentOwnerMapTest,ProjectionStateTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement** the five types (`RowBuffer` is the per-page staging of `RowValues`).
- [ ] **Step 4: Run to verify they pass.** Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - the six classes and three tests.

---

### Task 7: DirtyPageTracker (spec 6.2)

> Execution notes (Tasks 4-6 reviews): `ContentOwnerMap` is single-writer and NOT thread safe; every reader that is not the apply thread (e.g. `pendingPositions(owners)` on query threads) reads it under the projection's `ownersLock` read lock (Task 8).


**Files:**
- Create: `index/columnar/DirtyPageTracker.java`
- Test: `index/columnar/DirtyPageTrackerTest.java`

**Interfaces:**
- Consumes: `PositionBitmap` (Task 1) and `ContentOwnerMap` (Task 6), only for `pendingPositions` / `addPendingOwners`.
- Produces:

```java
final class DirtyPageTracker {
  DirtyPageTracker(int maxRecordsInPage, LongSupplier clockMs);
  void mark(int[] pages, int count);                  // commit path: the ONLY cost added there
  int drainInto(IntHashSet target);                   // apply: seal the current epoch, wait for its writers, move bits epoch -> inFlight, return count
  void completed(int[] pages, int count);             // apply: pages applied to the overlay -> inFlight -> unflushed
  void flushed(int[] pages, int count, IntPredicate stillInLiveOverlay); // flush published: remove from unflushed every page for which the predicate is FALSE (R14: a page re-applied into the live overlay after the freeze stays unflushed for its newer value)
  void collectRecoverySet(IntHashSet out);            // epochs + inFlight + unflushed, read in that order (movement direction)
  PositionBitmap pendingPositions(ContentOwnerMap owners); // epochs + inFlight, every slot of each page, plus owners of those pages (primary tracker)
  void addPendingOwners(ContentOwnerMap owners, PositionBitmap out); // R7, external tracker: sets only the owners (primary positions) of its pending pages into out; never whole pages
  long oldestPendingEpochStartMs();                   // 0 when no undrained epoch and nothing in flight
  boolean isEmpty();
}
```

Algorithm decisions:
- An epoch is `{ final long startMs; final AtomicReferenceArray<AtomicLongArray> chunks /* 1024 words = 65_536 pages per chunk, created by CAS */; final AtomicInteger writers; }`. `startMs` is set by the first `mark` into the epoch (CAS from 0), so an idle epoch reports no lag.
- `mark`: `for (;;) { e = current; e.writers.incrementAndGet(); if (e == current) break; e.writers.decrementAndGet(); }`, set the page bits with `getAndAccumulate(word, bit, OR)`, then `e.writers.decrementAndGet()`. One increment pair per call, not per page.
- `drainInto`: `old = current; current = new Epoch(); while (old.writers.get() != 0) Thread.onSpinWait();` then for each non-zero word: OR into `inFlight` (plain, apply-thread owned, guarded by `movementLock`) and only then clear it from the epoch: the "clear before re-read" of spec 6.2 is this drain, which runs before the pass reads any page.
- Every move adds to the destination before removing from the source, under `movementLock` (a `ReentrantLock` the commit path never takes); `collectRecoverySet` takes `movementLock` and reads in movement order, so a page is always in at least one set it reads.
- `pendingPositions` reads epochs, then `inFlight` (the caller reads the `ProjectionState` afterwards, Task 8).
- External-property buckets (R7): page 5 of the external file and page 5 of the primary must not collide, so a projection whose bucket has a paired external bucket owns a SECOND tracker, `new DirtyPageTracker(256, clock)` (external pages hold 256 slots, the primary 2048: one `PositionBitmap` cannot hold both). Its pages translate to owners only (`addPendingOwners` with the external owner map), so every pending position handed to a reader is a primary position. `IntHashSet` (`utility/IntHashSet.java`) has no ordered iteration: callers copy to `int[]` and `Arrays.sort` before visiting pages ascending.

- [ ] **Step 1: Write the failing tests**

```java
@Test void markThenDrainReturnsEachPageOnce()
@Test void markDuringDrainIsNeverLost()            // 8 marking threads x 100_000 random pages, apply thread drains in a loop; union of drained == union of marked
@Test void reMarkAfterDrainIsPickedUpNextPass()    // drain {5}; mark {5}; drain -> {5} again (ordering protocol)
@Test void recoverySetCoversEverySetAPageIsIn()    // pages in epoch, inFlight, unflushed each appear; a page moving between sets concurrently is never absent from 1_000 collected samples
@Test void lagIsOldestUndrainedEpochStart()        // fake clock: mark at 1000, drain at 1500 (inFlight), mark at 1700 -> oldestPendingEpochStartMs()==1000 until completed(), then 1700
@Test void idleEpochReportsNoLag()                 // drain with nothing marked -> 0
@Test void pendingPositionsCoverWholePagesAndOwners() // maxRecordsInPage 2048, page 2 dirty, owner 10 on content page 2 -> cardinality 2048 + 1 (owner on page 0)
@Test void pageReappliedAfterFreezeStaysUnflushed()   // R14: completed {5, 6}; flushed({5, 6}, 2, p -> p == 5) -> collectRecoverySet holds 5, not 6
@Test void externalTrackerAddsOnlyOwners()            // R7: tracker(256) marks external page 3; owner 4100 on external content page 3 -> addPendingOwners sets exactly {4100} into a PositionBitmap(2048); no slot of primary page 3 set
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest=DirtyPageTrackerTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement `DirtyPageTracker`.**
- [ ] **Step 4: Run to verify they pass.** Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `DirtyPageTracker.java`, `DirtyPageTrackerTest.java`.

---

### Task 8: ColumnarProjection

> Execution notes (Tasks 3-6 reviews): (a) `ColumnarOverlay` constructor is `ColumnarOverlay(int maxRecordsInPage, ColumnDefinition[] columns, LongSupplier clockMs)`. (b) `ColumnarProjection` owns `ReentrantReadWriteLock ownersLock` per owner map (primary and external): the apply pass takes the write lock only around `setOwnerPages`/`removeOwner`; `acquire()`'s `pendingPositions(owners)` and the flush's owner-map `copy()`/`writeTo` take the read lock. (c) Bucket directories are created with `ColumnarStore.createBucketDir`. (d) `stateReleased` (from `ProjectionState.onLastRelease`, which runs on whichever thread releases last, often a query thread) calls `deleteUnreferenced` under `publishLock`. (e) The extractor's `extract()` must never be handed a bucket without a projection or a record with a null identity (it would NPE): resolve and skip first. (f) `addColumn` (V-B4) and RENAME re-key the overlay: the overlay's `ColumnDefinition[]` and the apply-built blocks must agree (they compare with `Arrays.equals` on the record, property name included), so an ADD_COLUMN/RENAME replaces the overlay columns before the next apply pass builds blocks.


**Files:**
- Create: `index/columnar/ColumnarProjection.java`
- Test: `index/columnar/ColumnarProjectionTest.java`

**Interfaces:**
- Consumes: Tasks 2-7.
- Produces:

```java
public final class ColumnarProjection {
  static ColumnarProjection open(DatabaseInternal db, LocalBucket bucket, ColumnDefinition[] columns, ColumnarStats stats); // bucketDir = ColumnarStore.bucketDir(databasePath, bucket.getFileId()) (R9); loads manifest, segments, dead sets, dictionaries, owner maps; never rebuilds by itself: a failure while opening only RECORDS the reason (unusableReason()), Task 16 startup rules decide; when no manifest exists and `bucket.getCommittedPageCount() == 0` (or the bucket holds no record), it publishes an empty generation-1 manifest (writable databases) and is usable at once, `unusableReason()` null (spec 6.4 CREATE_EMPTY); `missing` is recorded only for a bucket that has records (V-B1)
  public ColumnarSnapshot acquire();                 // pending (both trackers) read BEFORE the state reference; state.retain()
  public GlobalDictionary dictionary(String property);
  int bucketId(); int maxRecordsInPage(); ColumnDefinition[] columns();
  DirtyPageTracker tracker(); ContentOwnerMap owners(); ColumnarOverlay overlay();
  DirtyPageTracker externalTracker(); ContentOwnerMap externalOwners(); int externalFileId(); // R7: null / null / -1 when the bucket has no paired external bucket
  ColumnarManifest manifest();
  String unusableReason();                           // last reason passed to markUnusable, null when usable
  ReentrantLock publishLock();                       // package-private (V-M11): flush, merge, rebuild and compaction publish under it
  void publishOverlay();                             // apply thread, end of pass: new ProjectionState with overlay.publish()
  void applyDeadPositions(PositionBitmap deadNow);   // clears live bits in the segments holding them (copy-on-write dead sets)
  long liveValueLookup(long position, int column, RowValues out); // newest-first: overlay, then segments; returns segment id or -1 when absent
  boolean isUsable();                                // a valid manifest was loaded or a rebuild published
  void addColumn(ColumnDefinition column);           // V-B4: definition only, state unchanged; the column reads as missing until the next rebuild publishes it
  void markUnusable(String rebuildReason);           // "missing" or "corrupt": records the reason and (writable databases only) calls the rebuild requester; READ_ONLY: records the reason and serves the fallback, never calls it, never writes (R16)
  void setRebuildRequester(Consumer<String> requester); // package-private hook; default no-op; Task 16 installs (reason) -> ColumnarRebuilder.schedule(manager, this, reason); tests install a recorder
  void close();
  void drop();                                       // P52: marks dropped, stops publishing; bucketDir is deleted only when the last retained state releases (immediately when none is retained)
}
```

`acquire()` returns a snapshot whose `dictionary(property)` is the state's dictionary generation and whose `segments()` are the state's `SegmentReader`s (with their current dead sets) followed by the overlay views (frozen overlay being flushed first, then the live overlay), `pendingPositions()` = `tracker.pendingPositions(owners)` plus `externalTracker.addPendingOwners(externalOwners, ..)` when present (R7, primary positions only), `oldestPendingEpochStartMs()` = the older of both trackers. A `BlockCorruptionException` while opening (manifest, segment, dead set, dictionary or owner map) -> records `unusableReason` `corrupt` (or `missing` when there is no manifest on a bucket with records) without calling the requester (V-M10; whether it is counted is decided at open by Task 16 startup rules, R17).

File lifetime (R1, R2): the projection keeps a `Set<ProjectionState>` of retained states and a `Map<String, SegmentFile>` of the segment files it opened. Every state is built with `onLastRelease = this::stateReleased`; `stateReleased` calls `ColumnarStore.deleteUnreferenced(bucketDir, current manifest files, union of files() of the still-retained states)` and `SegmentFile.deleted()` for each deleted segment name (the only `invalidateFile` path). Each state is built with the file names of the manifest it was published with (V-B7). After `drop()`, the last release calls `SegmentFile.deleted()` for every opened segment, then deletes `bucketDir` (V-M11). On a writable open, after loading, `deleteUnreferenced(bucketDir, manifest files, {})` removes crash orphans (for example a `dict-<id>.<n+1>.dic` left by a crashed rebuild or compaction, which `GlobalDictionary.create` would otherwise refuse) (V-M11).

- [ ] **Step 1: Write the failing tests** (projection built by hand from `SegmentWriter` and an overlay, no apply thread yet)

```java
@Test void snapshotListsSegmentsThenOverlay()
@Test void snapshotHoldsFilesUntilClosed()           // acquire; publish a manifest without segment 1; segment 1 file exists until snapshot.close()
@Test void pendingIsReadBeforeState()                // tracker page moved inFlight -> applied -> published between the two reads cannot vanish: reader sees it either pending or in state (stress 10_000 iterations with a test barrier)
@Test void corruptSegmentMakesProjectionUnusable()   // flip a byte in the footer, open -> isUsable() false, unusableReason() == "corrupt" (not "missing"); on a writable database markUnusable("corrupt") calls the recording requester once
@Test void deadPositionsAreCopyOnWrite()             // snapshot taken before applyDeadPositions still counts the position live
@Test void emptyBucketWithoutManifestIsUsable()     // V-B1: empty bucket, no manifest -> open: isUsable() true, unusableReason() null, manifest.json generation 1 with no segments written
@Test void readOnlyMarkUnusableSchedulesNothing()    // R16: READ_ONLY database, manifest deleted -> open: isUsable() false, unusableReason() == "missing"; install a recording requester, markUnusable("corrupt") -> requester never called; no file created under columnar/
@Test void dropDefersDeletionToLastRelease()         // P52: acquire; drop() -> bucketDir exists; snapshot.close() -> bucketDir gone
@Test void releasedSegmentInvalidatesItsCacheOnce()  // R1: publish a state without segment 1 while a snapshot holds it; close the snapshot -> file deleted and BlockCache entries of its cacheFileId gone; applyDeadPositions alone never invalidates
@Test void externalPendingOwnersArePrimaryPositions() // R7: projection over a bucket with an external bucket; mark external page 0 -> snapshot.pendingPositions() holds the owner positions, maxRecordsInPage() == the primary's
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest=ColumnarProjectionTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement `ColumnarProjection`.** Publication of `ProjectionState` through an `AtomicReference`; a `ReentrantLock publishLock` serializes apply-pass publish, flush publish and merge publish (readers never take it, engine-concurrency "Locks").
- [ ] **Step 4: Run to verify they pass.** Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `ColumnarProjection.java`, `ColumnarProjectionTest.java`.

---

### Task 9: Configuration keys and dedicated pools

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` (new entries after `PARALLEL_SCAN_PRODUCER_POOL_THREADS`, `:353`; plan 2's `BLOCK_CACHE_MAX_BYTES` sits at `:318`)
- Create: `index/columnar/ColumnarApplyPool.java`, `index/columnar/ColumnarMaintenancePool.java`
- Modify: `server/src/main/java/com/arcadedb/server/monitor/PoolMetrics.java:91-111`
- Modify: `.claude/skills/engine-concurrency/SKILL.md` (pool inventory table: two new rows, the deviation argued)
- Test: `engine/src/test/java/com/arcadedb/index/columnar/ColumnarPoolsTest.java`, `server/src/test/java/com/arcadedb/server/monitor/PoolMetricsColumnarTest.java`

**Interfaces:**
- Produces:

| Key | Name | Scope | Type | Default |
|---|---|---|---|---|
| `COLUMNAR_FLUSH_INTERVAL` | `arcadedb.columnar.flushInterval` | DATABASE | Long ms | 1000 |
| `COLUMNAR_FLUSH_MAX_ROWS` | `arcadedb.columnar.flushMaxRows` | DATABASE | Integer | 65536 |
| `COLUMNAR_MERGE_DEAD_RATIO` | `arcadedb.columnar.mergeDeadRatio` | DATABASE | Float | 0.3 |
| `COLUMNAR_MERGE_MAX_SEGMENTS` | `arcadedb.columnar.mergeMaxSegments` | DATABASE | Integer | 10 |
| `COLUMNAR_MAX_CARDINALITY` | `arcadedb.columnar.maxCardinality` | DATABASE | Integer | 1000000 |
| `COLUMNAR_SEGMENT_BLOCK_ROWS` | `arcadedb.columnar.segmentBlockRows` | DATABASE | Integer | 16384 |
| `COLUMNAR_APPLY_INTERVAL` | `arcadedb.columnar.applyInterval` | DATABASE | Long ms | 100 |
| `COLUMNAR_APPLY_THREADS` | `arcadedb.columnar.applyThreads` | JVM | Integer | 2 |
| `COLUMNAR_MAINTENANCE_THREADS` | `arcadedb.columnar.maintenanceThreads` | JVM | Integer | 2 |
| `COLUMNAR_REBUILD_CONCURRENCY` | `arcadedb.columnar.rebuildConcurrency` | JVM | Integer | 1 |
| `COLUMNAR_DICTIONARY_COMPACT_RATIO` | `arcadedb.columnar.dictionaryCompactRatio` | DATABASE | Float | 0.5 |

```java
public final class ColumnarApplyPool extends DedicatedThreadPool {        // Holder idiom like ParallelScanProducerPool
  public static ColumnarApplyPool getInstance();
  public PoolStats getPoolStats();
  public void register(Runnable tick);                // P53: the ticker calls every registered tick each COLUMNAR_APPLY_INTERVAL; Task 12's manager registers its own tick (which submits a pass when it has pending work)
  public void unregister(Runnable tick);              // idempotent; a tick unregistered is never called again after this returns
}
public final class ColumnarMaintenancePool extends DedicatedThreadPool { public static ColumnarMaintenancePool getInstance(); public PoolStats getPoolStats(); }
```

Both pools: `UNBOUNDED_QUEUE`, `SaturationPolicy.NONE`, javadoc argues the deviation: a submitter is the apply ticker or a pass finishing, never a user thread, and caller-runs would run a page re-read or a merge on a commit or Raft apply thread; back-pressure is "at most one queued pass per database" (a CAS flag in the manager) and "at most one of flush, merge or dictionary compaction per projection" (one per-projection maintenance flag, V-B6). One JVM-wide ticker (daemon `ScheduledExecutorService` with one thread named `ArcadeDB-ColumnarTicker`, lazily created, part of `ColumnarApplyPool`) calls every tick registered through `register(Runnable)` every `COLUMNAR_APPLY_INTERVAL` (the pool knows no manager type: Task 12's `ColumnarIndexManager` registers a tick that submits a pass when it has pending work, and unregisters on close/kill/drop). Registered ticks live in a copy-on-write array, so the ticker never locks. PoolMetrics names: `columnar_apply`, `columnar_maintenance`.

- [ ] **Step 1: Write the failing tests**

```java
// ColumnarPoolsTest
@Test void poolsAreSingletonsWithConfiguredSize()   // getInstance() same instance; getPoolStats().poolSize() <= COLUMNAR_APPLY_THREADS (PoolStats is the record (poolSize, activeThreads, queueDepth, queueCapacityRemaining, completedTasks, callerRunFallbacks, reclaimedTasks))
@Test void tickerRunsRegisteredTicksUntilUnregistered() // register(counter); await >= 2 calls (CountDownLatch); unregister; count stays put over the next 3 intervals
@Test void neverRunsOnTheSubmittingThread()         // fill both pools with 1_000 blocking tasks; submit one more from the test thread; it does not run on the test thread
@Test void configurationDefaults()                  // each key's default equals the table above
// PoolMetricsColumnarTest (server)
@Test void columnarPoolsAreExported()               // new PoolMetrics().bindTo(new SimpleMeterRegistry()) -> gauges with tag pool=columnar_apply and pool=columnar_maintenance
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl server -am test -Dtest='ColumnarPoolsTest,PoolMetricsColumnarTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement** keys, pools, PoolMetrics bindings and the skill table rows.
- [ ] **Step 4: Run to verify they pass.** Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `GlobalConfiguration.java`, both pools, `PoolMetrics.java`, `SKILL.md`, both tests.

---

### Task 10: Flush and merge

> Execution note (Task 8 round 2): the flush's `overlay.freeze()` holds `applyLock`, like every other overlay change (else a freeze between `addColumn`'s re-key and its swap is lost). Every publish that replaces the segment set OR the dictionaries map holds `applyLock`.


> Execution notes (Task 8 review): (a) Every new file a flush/merge writes is reserved through `ColumnarProjection` (`reserveSegmentId()`, `reserveDictionaryGeneration(indexFileId)`, or `holdFiles(...)` for other names) BEFORE it is created and released (`releaseFiles`) after publish or abort; a writer whose `publish` returns false after `drop()` deletes its own files. (b) Every publish that changes the segment set holds `applyLock` (order `applyLock -> publishLock`); `publish` asserts it. (c) The frozen overlay view is inserted before the live view; add `LOOKUP_FROZEN` to `liveValueLookup` and make `addColumn` re-key the frozen overlay too.


> Execution notes (Tasks 5-6 reviews): (a) The frozen view must hide `supersededInFrozen`: wrap the published `ColumnarSegmentView` (`live()` = `inner.live().copy()` then `andNot(hidden.snapshot())`, cached per publish; `forEachOrdinal(matches, c)` = `inner.forEachOrdinal(matches.copy().andNot(hidden), c)`), rebuilt on every `publishOverlay`, or add `ColumnarOverlay.publish(PositionBitmap hidden)`. (b) Dead-set versions start at 1 (0 = no dead file), including the initial `supersededInFrozen` dead set. (c) Flush/merge reach the kill points `publish.segmentsSynced` and `publish.dictionariesSynced` (Task 21 arms them). (d) The owner-map `copy()`/`writeTo` runs under `ownersLock` read lock. (e) Check `shouldFlush`: a deletes-only workload has `rows == 0` but changed dead sets, unflushed pages and NOWAL that still need persisting.


**Files:**
- Create: `index/columnar/ColumnarFlusher.java`, `index/columnar/MergePolicy.java`
- Modify: `index/columnar/ColumnarProjection.java` (adds `final AtomicLong noWalEpoch`, `long noWalEpoch()`, `long bumpNoWalEpoch()`, `long noWalEpochAtLastDrain` written by the apply pass BEFORE `drainInto`, and `final Object noWalMarkerLock` with `long noWalMarkerEpoch` (epoch at which NOWAL was last written, -1 after a delete); Task 13's `markPagesWithoutWAL` bumps the epoch and writes the marker, the flush deletes it (R8, V-B3). Also adds `IntHashSet appliedSinceFreeze` (pages the apply pass completed after the last freeze, cleared at freeze, guarded by `publishLock`) and `PositionBitmap supersededInFrozen` (V-B2))
- Test: `index/columnar/ColumnarFlusherTest.java`, `index/columnar/MergePolicyTest.java`

**Interfaces:**
- Consumes: Tasks 4-9.
- Produces:

```java
final class ColumnarFlusher {
  static boolean shouldFlush(ColumnarProjection p, long nowMs, long intervalMs, int maxRows); // rows >= maxRows, or rows > 0 and nowMs - overlay.oldestRowMs() >= intervalMs (P39, Task 6)
  static void flush(ColumnarProjection p, long lastTransactionId) throws IOException;      // maintenance pool; lastTransactionId is written to the manifest for information only
  static void merge(ColumnarProjection p, long[] segmentIds) throws IOException;           // maintenance pool
}
final class MergePolicy {
  static long[] select(List<ColumnarManifest.SegmentEntry> segments, float deadRatio, int maxSegments); // empty = nothing to do
}
```

Flush: `overlay.freeze()` under `publishLock` (record `p.noWalEpochAtLastDrain` at that moment, clear `appliedSinceFreeze` and `supersededInFrozen`); write one segment from the frozen pages ascending; publish (Task 5 order) with the dead sets changed since the last publish and the owner map copies (primary and external, R7); then under `publishLock` swap the frozen view for the new `SegmentReader` and call `tracker.flushed(frozenPages, n, appliedSinceFreeze::contains)` where `appliedSinceFreeze` is an `IntHashSet` the apply pass fills (under `publishLock`, in Task 15 step 4) with every page it completed after the last freeze, cleared at freeze (R14 as refined by V-B2: any re-apply after the freeze, rows or deletes, keeps the page unflushed; external tracker likewise with its own pages). `ColumnarProjection` keeps `PositionBitmap supersededInFrozen`: positions whose `liveValueLookup` answered the frozen overlay and that the apply pass updated or deleted; the published frozen view hides them (its `live()` is `blocks AND NOT supersededInFrozen`), and the swap installs them as the new segment's initial dead set before publishing it (spec 5.2: one live value per position); finally, under `noWalMarkerLock`, delete `<bucketDir>/NOWAL` (`ColumnarStore.NOWAL_MARKER`, R8) and set `noWalMarkerEpoch = -1` only if `noWalEpoch() ==` the `noWalEpochAtLastDrain` of the last pass completed before the freeze (every no-WAL mark was drained into the frozen overlay) and the tracker holds no undrained page (V-B3). Kill points `"flush.segmentWritten"` plus the publish points.

Merge: `select` returns segments whose `deadRows / rows >= deadRatio` (each alone), else, when there are more than `maxSegments`, the smallest adjacent run of `maxSegments / 2 + 1` segments by row count. The merged segment keeps only live rows, ascending by position (k-way merge over `SegmentReader.live()` page ranges, `live(fromPage, toPage)` in windows of 1024 pages so heap stays bounded); page versions are carried per page from the newest input covering it. Kill points `"merge.segmentWritten"` plus the publish points. Input segments leave the manifest at publish and are deleted by the projection's last-release path (Task 8), which calls `SegmentFile.deleted()` (R1). Both increment `stats.flush(type)` / `stats.merge(type)` and record `flushDuration` / `mergeDuration` (`ColumnarStats`, Task 3).

- [ ] **Step 1: Write the failing tests**

```java
// ColumnarFlusherTest (TestHelper; projection driven by hand-filled overlay)
@Test void flushPublishesASegmentAndEmptiesTheOverlay()
@Test void readersSeeTheSameCountsAcrossAFlush()    // snapshot before, during (frozen) and after give equal per-ordinal counts
@Test void flushWaitsForInterval()                  // fake clock: shouldFlush false at rows 10 / age 500ms (age = now - overlay.oldestRowMs()), true at age 1000ms, true at rows 65_536 regardless of age
@Test void pageReappliedDuringFlushStaysUnflushed()  // R14/V-B2: freeze with page 5; before the swap a re-apply of page 5 that only DELETES (no live-overlay block); after flush collectRecoverySet still holds 5, a page only in the frozen overlay is gone
@Test void updateOfAFrozenPositionIsNotDoubleCounted() // V-B2: update p after the freeze, before the swap: counts equal the oracle before and after the swap (old value hidden by supersededInFrozen, then dead in the new segment)
@Test void noWalMarkKeptWhenItsPagesWereNotYetApplied() // V-B3: no-WAL mark before the freeze whose pages were not yet drained -> flush keeps NOWAL
@Test void noWalMarkerDeletedOnlyWhenNoLaterNoWalMark() // R8: NOWAL present, flush -> deleted; NOWAL present + no-WAL epoch bumped after the freeze -> kept
@Test void flushCountsInStats()                     // stats.flushCount(type) == 1, flushDurationCount(type) == 1
@Test void mergeDropsDeadRowsAndKeepsLiveValues()   // 3 segments, 40% dead in one -> merged rowCount == live rows; counts unchanged
@Test void mergeKeepsNewestPageVersion()
@Test void mergedInputsAreDeletedAfterLastReader()  // snapshot held across the merge -> input files exist; close -> deleted (R1/R2 path)
// MergePolicyTest
@Test void deadRatioSelectsSingleSegment()
@Test void tooManySegmentsSelectsSmallestAdjacentRun() // 12 segments of sizes ..., maxSegments 10 -> 6 smallest adjacent
@Test void healthySetSelectsNothing()
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest='ColumnarFlusherTest,MergePolicyTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement** both classes.
- [ ] **Step 4: Run to verify they pass.** Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `ColumnarFlusher.java`, `MergePolicy.java`, `ColumnarProjection.java`, both tests.

---

### Task 11: COLUMNAR index type, metadata, builder, SQL

**Files:**
- Modify: `schema/Schema.java:560-589` (enum constant `COLUMNAR`; `buildCanShareCallerTransaction()` false for it; new `isMaintainedInTransaction()` and `isPlannable()`, both `this != COLUMNAR`)
- Create: `schema/ColumnarIndexMetadata.java`, `schema/TypeColumnarIndexBuilder.java`, `index/columnar/ColumnarIndex.java`, `index/columnar/ColumnarIndexFactoryHandler.java`
- Modify: `schema/LocalSchema.java:554-559` (register handler), `:3810-3846` (load branch: `configuredIndexType` COLUMNAR wraps the loaded `LSMTreeIndex`, like the `LSM_SPARSE_VECTOR` branch at `:3838`)
- Modify: `schema/TypeIndexBuilder.java:216-233` (`withType(COLUMNAR)` returns `TypeColumnarIndexBuilder`; add `withColumnarType()` like `withGeoType()` at `:258`)
- Modify: `query/sql/parser/CreateIndexStatement.java:100-123` (validate), `:256-284` (type parsing), `:413-520` (METADATA branch like GEOSPATIAL), messages listing types gain `COLUMNAR`; the grammar rule `indexType: UNIQUE | NOTUNIQUE | FULL_TEXT | identifier` (`SQLParser.g4:653`) needs no change
- Modify: `query/opencypher/query/CypherSchemaCommands.java:440-464` (R28: the exhaustive `switch` expressions over `Schema.INDEX_TYPE` in `indexType` and `indexProvider` stop compiling when `COLUMNAR` is added: `case COLUMNAR -> "COLUMNAR"` and `case COLUMNAR -> "columnar-1.0"`)
- Modify: `index/TypeIndex.java:892-897` (`isReadyForQueries()`: `first.getType() == null || !first.getType().isPlannable()` -> not ready for queries)
- Modify: `database/DocumentIndexer.java` (skip `!index.getType().isMaintainedInTransaction()` before any key extraction in `getInvolvedIndexes` `:78`, `createDocument` `:92`, `addToIndex` `:103`, `removeFromIndex` `:111`, `snapshotAfterBuild` `:575`, `updateDocument` `:682`, `deleteDocument` `:792`)
- Modify: `graph/GraphBatch.java` (`indexEdgeProperties` `:1592`, called at `:1524`: skip the same index types; R28, for symmetry: `ColumnarIndex.put` is a no-op anyway, so this path is the one other route that reaches it)
- Create (stub, R4): `index/columnar/ColumnarIndexManager.java` with the constructor `ColumnarIndexManager(LocalDatabase db)` and no-op `register(ColumnarIndex)`, `unregister(ColumnarIndex)`, `requestReconcile()`; Task 12 completes the class
- Modify: `database/LocalDatabase.java` (R4: field `private ColumnarIndexManager columnarIndexManager` created in `openInternal()` (`:4333`, the path both `open()` and `create()` take) before `schema.load` (`:4368`), so the load branch can register, and `public ColumnarIndexManager getColumnarIndexManager()` next to `getTransactionManager()` `:1277`; Task 12 adds the lifecycle calls)
- Test: `index/columnar/ColumnarIndexDdlTest.java`, `index/columnar/ColumnarIndexOptOutTest.java`

**Interfaces:**
- Consumes: `ColumnKind.of`, `ColumnarValueExtractor`, `ColumnarStats` (Task 3), `GlobalDictionary.createInMemory` (Task 2); `ColumnarIndexManager.register/unregister/requestReconcile` from the stub this task creates (R4), reached through `((LocalDatabase) db.getEmbedded()).getColumnarIndexManager()`.
- Produces:

```java
public class ColumnarIndexMetadata extends IndexMetadata {
  public static final String HIERARCHY = "hierarchy", MAX_CARDINALITY = "maxCardinality";
  public JSONObject getHierarchy();                  // {"vertexType","key","parentEdge"[,"parentDirection"]}, null when absent; stored as given for plan 5 (plan 5 adds getHierarchyDefinition() on top, this accessor keeps its type)
  public int getMaxCardinality();                    // 0 = use COLUMNAR_MAX_CARDINALITY
  // user keys: hierarchy (object with vertexType, key, parentEdge, all required strings, plus optional parentDirection string that plan 5 validates as IN/OUT; any other key or a non-string -> IllegalArgumentException), maxCardinality (int > 0)
}
public class TypeColumnarIndexBuilder extends TypeIndexBuilder {
  public TypeColumnarIndexBuilder withMetadata(JSONObject json);   // fromUserMetadata(json, COLUMNAR)
  public TypeColumnarIndexBuilder withHierarchy(String vertexType, String key, String parentEdge);
  public TypeColumnarIndexBuilder withMaxCardinality(int max);
  @Override public TypeIndex create();               // refuses: unique, more than one property ("one index per property", spec 5.1), unsupported type (ColumnKind.of), cardinality over the limit
}
public final class ColumnarIndex implements IndexInternal {
  ColumnarIndex(LSMTreeIndex shell, ColumnarIndexMetadata metadata);            // load
  ColumnarIndex(DatabaseInternal db, String name, String filePath, ComponentFile.MODE mode, int pageSize, Type[] keyTypes, ColumnarIndexMetadata metadata); // create: new LSMTreeIndex(db, name, false, filePath, mode, keyTypes, pageSize, NULL_STRATEGY.SKIP) as the shell, as LSMSparseVectorIndex.java:170-183
  ColumnDefinition columnDefinition();
  // put/remove/putReplay/removeReplay: no-op (DocumentIndexer and GraphBatch skip the type, so normally unreached); get(...) -> IndexException "COLUMNAR indexes answer facets, not key lookups"
  // build(...) -> 0 and requestReconcile() here; Task 16 changes it to ColumnarRebuilder.schedule(manager, projection, "create") per bucket projection (R17, V-M14); isUnique false; getType COLUMNAR; supportsOrderedIterations false
  // toJSON(): type, bucket, properties, nullStrategy, unique=false, hierarchy, maxCardinality; drop(): shell drop + manager.requestReconcile(); close()/onSuperseded(): shell only, never the projection
}
public final class ColumnarIndexFactoryHandler implements IndexFactoryHandler { public IndexInternal create(IndexBuilder<? extends Index> builder); }
```

Cardinality check at creation (spec 5.1): it lives in `public static void ColumnarIndexFactoryHandler.checkCardinality(Database db, String type, ColumnDefinition column, int limit)`, called by `TypeColumnarIndexBuilder.create()` (V-M12: the extractor constructor is package-private in `index.columnar`). Only for ORDINAL / ORDINAL_LIST, scan the type polymorphically with a throwaway `ColumnarValueExtractor` (a fresh `new ColumnarStats()`, no manager stats exist yet, and a `DictionaryResolver` returning one `GlobalDictionary.createInMemory()` per property, so every value arrives as `unknownValue`) into a `HashSet` of normalized values and stop at `limit + 1`: over the limit -> `IndexException("Property 'x' has more than N distinct values ...")`. Above the limit later: the index keeps working, `cardinality.exceeded` gauge goes to 1 (Task 18).

- [ ] **Step 1: Write the failing tests**

```java
// ColumnarIndexDdlTest (TestHelper)
@Test void createIndexColumnarBySql()             // CREATE INDEX ON Asset (brand) COLUMNAR -> schema index type COLUMNAR, one ColumnarIndex per bucket
@Test void byItemList()                           // CREATE INDEX ON Asset (tags BY ITEM) COLUMNAR
@Test void hierarchyMetadataIsPersisted()         // METADATA {"hierarchy":{"vertexType":"Tag","key":"path","parentEdge":"ParentOf"}}; reopenDatabase(); getHierarchy() equal
@Test void unknownMetadataKeyIsAParsingError()    // METADATA {"foo":1} -> CommandSQLParsingException
@Test void compositeKeyIsRefused()                // CREATE INDEX ON Asset (brand, author) COLUMNAR -> CommandSQLParsingException/IndexException naming "one index per property"
@Test void unsupportedTypeIsRefused()             // DECIMAL property -> IndexException
@Test void cardinalityLimitRefusesCreation()      // maxCardinality 10, 11 distinct brands -> IndexException; 10 distinct -> created
@Test void survivesReopen()                       // reopenDatabase(); index still COLUMNAR with same file id
@Test void propertyNamedAfterKeywordStillParses() // property "columnar": CREATE INDEX ON Asset (columnar) COLUMNAR parses
// ColumnarIndexOptOutTest
@Test void plannerNeverSelectsIt()                // EXPLAIN SELECT FROM Asset WHERE brand = 'x' shows no index step (only COLUMNAR on brand)
@Test void typeIndexIsNotReadyForQueries()        // getIndexesByProperties(...).isReadyForQueries() false; lookupByKey refuses it like other non-key types
@Test void commitDoesNoIndexWork()                // after 1_000 inserts the shell LSM has 0 entries and the transaction recorded no index operation (`TransactionIndexContext.getTotalEntries() == 0`, existing, `:454`, V-M13)
@Test void buildDoesNotShareCallerTransaction()   // Schema.INDEX_TYPE.COLUMNAR.buildCanShareCallerTransaction() false
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest='ColumnarIndexDdlTest,ColumnarIndexOptOutTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement** all listed changes.
- [ ] **Step 4: Run to verify they pass**, then regression: `mvn -o -pl engine -am test -Dtest='CreateIndex*Test,*IndexBuilder*Test,Issue*TypeIndex*Test,IndexKeyEqualityTest,*ByItem*Test,LSMTreeGeoIndex*Test,LSMSparseVectorIndex*Test,*GraphBatch*Test,*Cypher*Index*Test' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `Schema.java`, `LocalSchema.java`, `TypeIndexBuilder.java`, `CreateIndexStatement.java`, `CypherSchemaCommands.java`, `TypeIndex.java`, `DocumentIndexer.java`, `GraphBatch.java`, `LocalDatabase.java`, the four new classes, `ColumnarIndexManager.java` (stub), both tests.

---

### Task 12: ColumnarIndexManager lifecycle

> Execution notes (Task 10 review): an `IllegalStateException` from a busy maintenance slot (`tryBeginMaintenance`) is not a failure; a flush/merge that keeps failing gets exponential backoff in the manager; a merge that keeps throwing `BlockCorruptionException` requests a `corrupt` rebuild.


> Execution notes (Task 9 review): the apply tick registered with `ColumnarApplyPool.register(tick, intervalMs)` takes NO `publishLock`/`applyLock`/database lock (it only CASes a flag and submits a pass); `close()`/`kill()` call `unregister` with no projection lock held (unregister waits for an in-flight tick, bounded by the tick's brevity). The per-database `COLUMNAR_APPLY_INTERVAL` is passed as the `intervalMs` supplier read from the database's configuration.


> Execution note (Task 8 review): if `RecoverySetFile` lives inside `b<id>/`, it must be named by the manifest or added to `ColumnarStore.isNeverDeleted`, or the first scanning release deletes it.


**Files:**
- Complete: `index/columnar/ColumnarIndexManager.java` (Task 11 created the stub with the constructor and no-op `register`/`unregister`/`requestReconcile`, R4)
- Modify: `database/LocalDatabase.java` - the field and `getColumnarIndexManager()` (next to `getTransactionManager()` `:1277`) and its creation in `openInternal()` (`:4333`) before `schema.load` (`:4368`) already exist from Task 11; this task adds: `startApplying()` AFTER the whole `if (recoveryPending) { ... }` block (`:4383-4393`), not inside it (R26: `schema.openDeferredTimeSeriesEngines()` at `:4392` is inside the `if`, and a clean open must start applying too); `close()` in `closeDurableParts` (`:3917`) before the index loop; `kill()` in `kill()` (`:486-506`) before `schema.close()` (`:511`); drop path: `close()` then `FileUtils.deleteRecursively` already removes `columnar/` (`:435-441`)
- Create (stub): `index/columnar/ColumnarApplier.java` (`static int runPass(ColumnarIndexManager, ColumnarProjection)` that only drains and completes, Task 15 replaces the body)
- Test: `index/columnar/ColumnarIndexManagerLifecycleTest.java` (Tasks 13 and 16 add the tests moved out of this task, R18)

**Interfaces:**
- Consumes: Tasks 3 (`ColumnarStats`, `ColumnarValueExtractor.DictionaryResolver`), 8-11 (`ColumnarApplyPool.register/unregister`).
- Produces (roadmap members plus additions):

```java
public final class ColumnarIndexManager {
  ColumnarIndexManager(LocalDatabase db);
  public ColumnarProjection projection(int bucketId);           // null when the bucket has no COLUMNAR index
  public ColumnarValueExtractor extractor(String typeName);     // null when the type has none; built with DictionaryResolver (bucketId, p) -> projection(bucketId).dictionary(p) (R5), so one extractor serves every bucket and subtype bucket of the type
  public void markPages(int fileId, int[] pageNumbers, int count);
  public void markPagesWithoutWAL(int fileId, int[] pageNumbers, int count);
  public void onSchemaEvent(ColumnarSchemaEvent event);
  public void requestReconcile();                                // AtomicLong bump only
  public boolean persistRecoverySet();                           // false when the write failed (Task 16)
  public boolean awaitCaughtUp(long timeoutMs) throws InterruptedException; // every projection usable, tracker empty, overlay published
  public void flushNow() throws InterruptedException;            // tests and close; runs the flush path, NOWAL rule included, even when the overlay is empty (V-M16)
  public ColumnarStats getStats();
  void register(ColumnarIndex index);                            // V-B4: opens the bucket's projection, or calls projection.addColumn when it exists; never touches the reconciler's last definition map
  void unregister(ColumnarIndex index);
  void startApplying();                                          // after WAL replay
  boolean isApplyingForTesting();                                // package-private (V-M15): the tick is registered with ColumnarApplyPool
  void close();                                                  // unregister the tick, drain (bounded 30 s), flush every overlay, publish; from Task 16 on also writes an empty recovery set (RecoverySetFile is Task 16's)
  void kill();                                                   // unregister the tick, stop scheduling, no I/O, no wait
  public AutoCloseable suspendApply();                           // public; holds every apply pass of this database until closed (lag observable); used by plan 4 tests
  public Semaphore fallbackScans();                              // per database, permits = COLUMNAR_MAX_FALLBACK_SCANS (plan 4's key; default 2 read through GlobalConfiguration once plan 4 adds it, constant 2 before)
  public long conversionFailures(String type, String property);  // delegates to ColumnarStats
  public void watchFiles(int[] fileIds, Runnable onChange);      // plan 5: markPages on a watched file with no projection runs onChange (coalesced, on ColumnarApplyPool), all three paths
  public void unwatchFiles(Runnable onChange);
  public long watchGeneration(Runnable onChange);                 // plan 5: incremented synchronously inside markPages (on the marking thread, before it returns) for every hit on a file the watcher watches; 0 for an unknown watcher
  public void addSchemaEventListener(Consumer<ColumnarSchemaEvent> listener); // plan 5: receives HIERARCHY_SOURCE_CHANGED (Task 17)
}
```

Watched files (plan 5): a second `volatile Runnable[][] watchersByFileId` consulted only when `byFileId[fileId]` is null; a hit increments the watcher's `AtomicLong generation` (read by `watchGeneration`, so a query that starts after a commit returned always sees the bump: plan 5's exactness for tree changes depends on it), sets a per-watcher `AtomicBoolean pending`, and the ticker runs the `Runnable` on `ColumnarApplyPool` (coalesced, never on the committing thread). Fast path for `markPages`: `volatile ColumnarProjection[] byFileId` indexed by file id (grown on register under a lock; external-property bucket ids point at the owning primary projection, flagged in a parallel `boolean[] externalFileId`); a null slot returns at once, so a commit on a non-columnar bucket costs one array read. R7: a mark on an external file id goes to that projection's `externalTracker()`, never to its primary tracker (page N of the two files must not collide). Scheduling (P53): the constructor creates one `Runnable tick` and `startApplying()` calls `ColumnarApplyPool.getInstance().register(tick)`; the tick submits at most one pass per database (CAS flag) when any tracker has pending work. Apply passes call `ColumnarApplier`, a stub here that drains each tracker (`drainInto`) and calls `completed` on the drained pages without re-reading them, so trackers empty; Task 15 replaces it. READ_ONLY databases (R16): `startApplying()` never registers the tick, `close()` never writes, `markUnusable` never schedules.

- [ ] **Step 1: Write the failing tests**

```java
@Test void managerExistsForEveryOpenDatabase()
@Test void projectionPerBucketOnlyForColumnarBuckets()  // type with 3 buckets + COLUMNAR on brand -> 3 projections; another type -> null
@Test void dropDatabaseRemovesTheProjection()           // drop() -> columnar/ gone
@Test void conversionFailuresDelegatesToStats()
@Test void fallbackScansIsOnePerDatabaseWithTwoPermits() // same instance on repeated calls, availablePermits() == 2, another database gets its own
@Test void extractorResolvesTheRecordsBucketDictionary() // R5: type with 2 buckets: extractor("Asset") gives a record of bucket 1 the ordinal of projection(bucket1).dictionary
@Test void cleanOpenStartsApplying()                    // R26, V-M15: create, close cleanly, reopen (no recovery) -> manager.isApplyingForTesting() true (tick registered with ColumnarApplyPool); a forced markPages on the bucket file id drains within awaitCaughtUp(10_000)
```

Moved out (R18): `suspendApplyHoldsPassesUntilClosed` to Task 13 (needs commit marks); `cleanCloseFlushesAndLeavesEmptyRecoverySet`, `readOnlyOpenNeverWritesTheProjection`, `killDoesNoIo` to Task 16 (need real passes, flush and the recovery file). They live in this same test class.

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest=ColumnarIndexManagerLifecycleTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement** the manager (apply passes call into `ColumnarApplier`, the draining stub described above, until Task 15) and the `LocalDatabase` wiring.
- [ ] **Step 4: Run to verify they pass**, then `mvn -o -pl engine -am test -Dtest='ACIDTransactionTest,Issue4511OpenFailureLockLeakTest,FailedOpenPreservesWalTest,Issue5067KillLockSymmetryTest,ColumnarPoolsTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `ColumnarIndexManager.java`, `ColumnarApplier.java` (stub), `LocalDatabase.java`, the test.

---

### Task 13: Dirty page hooks on commit, Raft apply and WAL replay

> Execution note (Task 12): any test step that expects `flushNow` (or a flush) to remove `NOWAL` is superseded by the NOWAL ruling: assert instead that a flush KEEPS `NOWAL` and that a clean close (after every page was flushed) removes it.


> Execution notes (Task 10 review): mark the pages BEFORE `bumpNoWalEpoch()` (bumping first lets a pass drain before the mark lands). Ruled: the NOWAL marker is deleted only by a clean close after the database flushed every page (the flush never deletes it), because a no-WAL commit's bucket pages are flushed asynchronously by the PageManager and a crash could leave the projection newer than the bucket; an unclean open with NOWAL present rebuilds with reason `unclean_no_wal`.


> Execution note (Task 7 review, CRITICAL ordering fix, ruled): mark #1 (before `publishPages`) can be drained by an apply pass that still reads the old page, and housekeeping may select and drop the WAL file before mark #2 runs, so a crash then loses the commit from the projection. Fix (D): `WALFile` gets a pending-marks counter (`AtomicInteger pendingColumnarMarks`), incremented together with the WAL write of a commit that touches a projected bucket and decremented after mark #2 (in `finally`); `TransactionManager.cleanWALFiles` selects a file for dropping only when `getPendingPagesToFlush() == 0 && pendingColumnarMarks == 0` (checked before the Task 16 sample, as today). The Raft-apply path does the same around its two marks. Test: a commit paused at the P71 seam between `publishPages` and mark #2 keeps its WAL file from being selected; after mark #2 it can be dropped.


**Files:**
- Modify: `database/TransactionContext.java` - in `publishCommittedPagesInternal` (`:2879`), call `markCommittedPages(pagesToPublish)` once immediately BEFORE the `publishPages(...)` call (`:2946`) and once immediately AFTER it. The first mark covers the window in which the flush thread could write the page and a WAL file become droppable before the second mark runs (Task 16 persists marks, so a crash there still re-reads the page); the second is the ordering-protocol mark of spec 6.2 (after visibility). With `changes.result == null` (no WAL) call `markPagesWithoutWAL` instead. Between `publishPages` and the second mark place the test seam (P71, used by Task 21 `killBetweenCommitPublishAndSecondMark`): `static volatile Runnable afterPublishBeforeSecondMarkForTesting` (null in production, one volatile read).
- Modify: `engine/TransactionManager.java` `applyChangesPages` (`:1372` to about `:1600`): collect `(fileId, pageNumber)` of every page the segment loop (`:1395`) reaches - applied, equal-version re-applied, and the strictly-older skip (about `:1449-1478`, spec 6.2 "the mark goes before the skip"; the gap `continue`s and the throws that follow must not bypass it) - into a reusable per-call `int[]` grouped by file; call `markPages` per file in a `finally` after the loop, i.e. after the pages are written. One path serves follower Raft apply (`ArcadeStateMachine.applyTxEntry` `:3711` -> `applyChanges` `:4050`) and WAL replay (`ownWALReplayThread`, field `:164`, set `:896`); `applyChanges(tx, Map, boolean)` is at `:1130`.
- Modify: `index/columnar/ColumnarIndexManager.java` (`markPagesWithoutWAL` NOWAL write)
- Test: `index/columnar/ColumnarDirtyPageHooksTest.java`; `index/columnar/ColumnarIndexManagerLifecycleTest.java` (receives `suspendApplyHoldsPassesUntilClosed`, R18)

**Interfaces:**
- Consumes: `ColumnarIndexManager.markPages/markPagesWithoutWAL` (Task 12), reached through the existing `embedded` field of `TransactionContext` (`:407`) when it is a `LocalDatabase`; `ColumnarProjection.bumpNoWalEpoch()` (Task 10); `ColumnarStore.NOWAL_MARKER`, `bucketDir(databasePath, bucketFileId)` (Task 5).
- Produces: `TransactionContext.markCommittedPages(List<MutablePage>)` (private), grouping by file id into a reusable `int[]` field; the test seam above; `static volatile IntConsumer onMarkForTesting` in `ColumnarIndexManager` (called with the file id inside `markPages`, null in production, V-M16).

R7: a mark on an external-property bucket's file id lands in the owning projection's `externalTracker()` (manager routing, Task 12); this task only passes `(fileId, pages)`.

No-WAL handling (spec 6.5, R8): `markPagesWithoutWAL` sets bits like `markPages`, calls `projection.bumpNoWalEpoch()`, and under `projection.noWalMarkerLock`, when `noWalMarkerEpoch == -1`, writes `<bucketDir>/NOWAL` (`ColumnarStore.NOWAL_MARKER`) synchronously with `ColumnarStore.writeAtomically` and sets `noWalMarkerEpoch` to the bumped value, before returning (V-B3: the check and the write are atomic with the flush's delete). Only no-WAL commits pay that, at most once per flush interval. The `NOWAL` file is the single record of an uncovered no-WAL write: Task 10's flush deletes it, Task 16's startup reads it; the manifest has no such field.

**Deliberate spec deviation (R25).** Spec 2's anti-goal says the commit path does nothing beyond setting a bit. A `setUseWAL(false)` commit here performs one synchronous `writeAtomically` (tmp file, force, atomic rename, directory fsync) on the commit thread, at most once per flush epoch per projection. Cost: one small fsynced write per flush interval, for no-WAL writers only; WAL commits are unaffected. The alternative, an O(bucket pages) page-version sweep on every unclean open after a no-WAL write, was rejected.

- [ ] **Step 1: Write the failing tests** (apply held with `try (AutoCloseable s = manager.suspendApply())` so the tracker can be inspected)

```java
@Test void localCommitMarksEveryModifiedBucketPage()   // insert 3 records spanning 2 pages -> tracker holds exactly those 2 pages
@Test void nonColumnarBucketsAreNotTracked()
@Test void externalBucketPagesGoToTheExternalTracker() // R7: EXTERNAL property indexed; update only its value -> externalTracker() holds the external page, the primary tracker does not hold the same page number
@Test void watchedFileWithoutProjectionNotifiesOnCommit()     // watchFiles({tagBucketId}, r): insert a Tag vertex -> watchGeneration(r) already greater when commit() returns (before r runs); r runs (CountDownLatch), once per pass even for 100 pages; unwatchFiles(r) -> no further runs
@Test void watchedFileNotifiesOnReplayAndRaftApply()          // the replayed-transaction fixture below on a watched file -> r runs
@Test void markHappensAfterPublication()               // ColumnarIndexManager.onMarkForTesting reads the page through the PageManager: the read version equals the committed one on the second mark
@Test void testSeamRunsBetweenPublishAndSecondMark()    // afterPublishBeforeSecondMarkForTesting records the tracker state: the page is already marked once (first mark) when the seam runs
@Test void replayedTransactionMarksAppliedAndSkippedPages() // build a WALFile.WALTransaction with one page older than disk (skipped) and one newer; TransactionManager.applyChanges(tx, Map.of(), false) -> both pages marked
@Test void noWalCommitWritesMarkerOncePerFlushEpoch()  // database.setUseWAL(false) on the tx; 50 commits -> NOWAL written once (mtime unchanged after the first); after flushNow() NOWAL removed
@Test void markCostIsOneArrayReadForOtherBuckets()     // structural: markPages on an unknown file id allocates nothing (ThreadMXBean allocated bytes delta == 0 over 10_000 calls)
// ColumnarIndexManagerLifecycleTest (moved from Task 12, R18)
@Test void suspendApplyHoldsPassesUntilClosed()         // with the hold open, insert -> manager.projection(bucketId).acquire().oldestPendingEpochStartMs() > 0 (V-M16) and tracker not empty; close -> awaitCaughtUp true (the Task 12 stub pass drains the tracker)
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest='ColumnarDirtyPageHooksTest,ColumnarIndexManagerLifecycleTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement** both hooks, the test seam and the NOWAL write.
- [ ] **Step 4: Run to verify they pass**, then regression of the touched paths: `mvn -o -pl engine -am test -Dtest='ACIDTransactionTest,TransactionManager*Test,*WAL*Test,*Recovery*Test,Issue7126*Test' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `TransactionContext.java`, `TransactionManager.java`, `ColumnarIndexManager.java`, both tests.

---

### Task 14: LocalBucket committed page slot API

**Files:**
- Modify: `engine/LocalBucket.java` (new public method near `scan`, `:836`)
- Modify (test support, R21): `engine/src/test/java/com/arcadedb/database/BucketPageLayoutTestSupport.java` - factor the `sealFirstPage(String)` technique (`:101`) into `public static RID sealFirstPage(Database database, String typeName)` (plus the `fillFirstPage` / `bucketStats` it needs, as static overloads taking the database); the protected instance method delegates, so its existing users (`Issue6149PlaceholderPrefersChunksTest`, `Issue6129ChunkedSlotMergeTest`, `Issue5279ConcurrentUpdateTest`) are unchanged. Since #6149 a record that outgrows its page becomes a chunk chain; a placeholder is produced only on a page with no free tail, which is what `sealFirstPage` builds. Tasks 15 and 19 reuse this helper.
- Test: `engine/LocalBucketCommittedPageVisitorTest.java`

**Interfaces:**
- Consumes: existing private helpers `getRecordPositionInPage` (`:6417`), `readPlaceholderPointer` (`:2288`), `isChunkHead` (`:747`), `isPlaceholderContent` (`:759`), the package-private 3-arg `getRecordInternal(rid, true, false)` (`:2541`), `loadMultiPageRecord` (`:5568` / `:5621`), the chunk chain walk of `walkChunkChainTail` (`:1091`); the existing `PaginatedComponent.getCommittedPageCount()` (`:229`) for the page count (P72: no new count method).
- Produces:

```java
public interface CommittedSlotVisitor {
  int EMPTY = 0, RECORD = 1, CONTENT = 2;              // CONTENT: placeholder content (< -5 or -4) or NEXT_CHUNK, not a record of its own
  void onSlot(long position, int kind, Binary content, int[] offPagePages, int offPageCount);
}
public long visitCommittedPage(int pageNumber, CommittedSlotVisitor visitor); // returns the page version read; reads through PageManager.getImmutablePage at the committed version (no transaction pin), resolves RECORD_PLACEHOLDER_POINTER (-1) and FIRST_CHUNK (-2) owners fully; offPagePages = pages holding the placeholder target or every chunk after the head
// page count: the existing getCommittedPageCount() (PaginatedComponent:229), not a new getTotalCommittedPages()
```

Placeholder pointers and their content always share the bucket (`RID(rid.getBucketId(), ...)`, `:2591`), so `offPagePages` are page numbers of the same file. A chain that cannot be walked yields kind RECORD with `content == null` (the applier then treats the position as unreadable: keep the previous live value, count `apply.unreadable`, retry next pass).

- [ ] **Step 1: Write the failing tests**

```java
@Test void plainRecordsAreVisitedWithContent()
@Test void deletedSlotsAreEmpty()
@Test void placeholderPointerOwnerGetsTargetPage()     // BucketPageLayoutTestSupport.sealFirstPage(database, type) then grow a record of page 0 (no free tail -> placeholder, not a chunk chain) -> owner slot RECORD with offPagePages == {target page}; target slot CONTENT
@Test void staticSealFirstPageMatchesTheInstanceForm() // the static helper leaves page 0 with no free tail (bucketStats totalMultiPageRecords > 0), as the instance method does
@Test void chunkedRecordReportsEveryChunkPage()        // 200 KB record -> RECORD with all chunk pages; chunk slots CONTENT
@Test void legacyPlaceholderContentHeadIsContent()     // FIRST_CHUNK_PLACEHOLDER_CONTENT (-4) slot -> CONTENT
@Test void externalBucketPagesAreVisitedToo()          // paired external bucket (maxRecordsInPage 256): positions use 256
@Test void versionIsTheCommittedOne()
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest=LocalBucketCommittedPageVisitorTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement** `visitCommittedPage` reusing the private helpers (no change to existing behavior) and the static `sealFirstPage` helper.
- [ ] **Step 4: Run to verify they pass**, plus `mvn -o -pl engine -am test -Dtest='LocalBucket*Test,*Placeholder*Test,*Chunk*Test,Issue5279ConcurrentUpdateTest' -Dsurefire.failIfNoSpecifiedTests=false` (the last one and `Issue6149PlaceholderPrefersChunksTest` / `Issue6129ChunkedSlotMergeTest`, matched by the globs, use the refactored helper). Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `LocalBucket.java`, `BucketPageLayoutTestSupport.java`, the test.

---

### Task 15: Apply pass

> Execution notes (Task 14 review): consume `content` and copy `offPagePages[0..offPageCount)` INSIDE `CommittedSlotVisitor.onSlot` (the array is reused across slots); a RECORD with null content means unreadable: keep the previous live value, count `apply.unreadable`, never touch the owner map for it, and re-mark that page for a later pass with backoff (a permanently corrupt slot is never re-marked by a commit) - after N attempts request a `corrupt` rebuild of the projection.


> Execution notes (Task 10 review): (superseded by the NOWAL ruling in Task 13: `noWalEpochAtLastDrain` no longer exists, the pass does no NOWAL epoch bookkeeping); `pagesApplied`/`externalPagesApplied` are called inside the same `applyLock` hold as `completed`; `supersedeFrozen` is called for every LOOKUP_FROZEN position the pass writes over (including rows of a whole-page re-apply that are not no-op skipped); `publishOverlay` holds `applyLock`.


> Execution notes (Task 8 review): a column added by `addColumn` (V-B4) has no writer dictionary (`dictionary(p) == null`): stage it as missing and never call the extractor with a null dictionary. The full lock order is `applyLock -> publishLock -> owner lock -> tracker movementLock`: never take an owner lock while holding `movementLock`. `liveValueLookup` reads segments under `applyLock`, which every segment-set-changing publish also holds.


> Execution notes (Task 6 review): `ColumnarOverlay.replacePage` makes the new block the page's WHOLE overlay content, so the apply pass must seed the `RowBuffer` from the page's current overlay block (`readRow` + `put` for every row not deleted now; add a `RowBuffer.seed(OverlayPageBlock)` helper rather than duplicating it) before staging changed values; staging only changed values without seeding silently drops unchanged earlier-pass rows. Owner-map mutations (`setOwnerPages`/`removeOwner`) run under the projection's `ownersLock` write lock.


**Files:**
- Complete: `index/columnar/ColumnarApplier.java` (replaces Task 12's draining stub)
- Modify: `ColumnarIndexManager.java` (real passes, flush and merge wiring), `ColumnarProjection.java` (R13: `private final ReentrantLock applyLock` and `ReentrantLock applyLock()`)
- Test: `index/columnar/ColumnarApplierTest.java`

**Interfaces:**
- Consumes: Tasks 6-8, 10, 12-14; `BinarySerializer.findExistingExternalRids(Database, Document)` (`serializer/BinarySerializer.java:1679`); `LocalDocumentType.getExternalBucketIdFor/isExternalBucket` (`schema/LocalDocumentType.java:2481-2486`; `getExternalBucketIdFor` returns a boxed `Integer`, null when none); `ColumnarValueExtractor.extractForApply(record, column, projection.dictionary(property), sink)` (R5: always the projection's own writer dictionary); `BucketPageLayoutTestSupport.sealFirstPage(Database, String)` (Task 14, tests).
- Produces: `final class ColumnarApplier { static int runPass(ColumnarIndexManager manager, ColumnarProjection projection) }` returning pages applied.

One pass per projection, on `ColumnarApplyPool`, with `DatabaseContext.INSTANCE.init(db)` and removal at the end as `LSMSparseVectorIndex.java:504-509` does, holding `projection.applyLock()` for the whole pass (R13: the pass boundary Task 24's compaction publish waits on; readers never take it):
1. `tracker.drainInto(pages)` (clear before re-read, spec 6.2), and `externalTracker().drainInto(externalPages)` when present (R7). Copy each `IntHashSet` into an `int[]` and `Arrays.sort` it (no ordered iteration). External-bucket pages are translated to owners through `projection.externalOwners()` (the external owner map, built in step 3 from `findExistingExternalRids`); content pages (pages whose slots are all CONTENT, or listed in `owners`) are translated to owner positions through `ContentOwnerMap.owners`.
2. For each primary page ascending: `bucket.visitCommittedPage(page, visitor)`; for each RECORD slot deserialize the `Binary` with the database serializer into a `Document` of the bucket's type; `extractForApply` every column; compare with `projection.liveValueLookup(position, ...)`; equal -> no-op (spec 6.3 "skips no-op changes", the vertex head-pointer churn of `GraphEngine.java:814-900`); different -> staged into the page's `OverlayPageBlock`; a changed value whose `liveValueLookup` answered a segment id adds the position to `deadNow` (one live value per position, spec 5.2, V-M18), and one answered by the frozen overlay adds it to `supersededInFrozen` (V-B2); an updated position whose old value is in a segment or in the frozen overlay is therefore always superseded. EMPTY slot whose position is live -> dead (delete, or a slot freed for reuse; a reuse is handled because a later RECORD on the same position is just a new value). Owner pages from step 1 are re-read by RID with `getRecordInternal` (they live on clean pages). That read goes through `database.getTransaction().getPage`, i.e. an implicit transaction on the pool thread, while primary pages are read with no transaction pin: harmless under READ_COMMITTED (it does not cache), but end any implicit transaction before the pass returns.
3. Update `ContentOwnerMap`: owners with off-page pages get `setOwnerPages`; owners now plain, or deleted, get `removeOwner`; owners with external pages update `externalOwners()` the same way (R7). Count `stats.placeholdersResolved(type, n)` per owner resolved through the map, `stats.recordsReread`, `stats.unreadable` per unreadable chain.
4. `projection.applyDeadPositions`, `overlay.replacePage` per page (always a NEW `OverlayPageBlock`), `projection.publishOverlay()`, then `tracker.completed(pages)` and `externalTracker().completed(externalPages)` (after publish, Task 7 ordering) and, under `publishLock`, add the completed pages to `appliedSinceFreeze` (V-B2), `stats.applyPages(type, n)`, `stats.applyPass()`, `stats.setOverlayRows(type, overlay.rows())`.
5. Kill point `"apply.beforePublish"`.

- [ ] **Step 1: Write the failing tests** (`TestHelper`; apply driven synchronously with `manager.awaitCaughtUp(10_000)`)

```java
@Test void insertsAppearInTheProjection()
@Test void updateReplacesTheLiveValue()                // one live value per position: count of old value drops by 1, new value +1
@Test void deleteRemovesThePosition()
@Test void slotReuseAfterDeleteIsAnOverwrite()         // delete then insert reusing the same RID position (TestHelper.restoreRecordAtLowLevel) -> the new value only
@Test void recordGrowingIntoPlaceholderIsFollowed()    // placeholder built with sealFirstPage(database, type) then a growth (R21; a plain growth makes a chunk chain since #6149); update brand so only the content page changes: projection shows the new brand; tracker drained the content page and the owner was resolved via the map
@Test void chunkedRecordUpdateOnTailChunkIsFollowed()
@Test void recordShrinkingBackDropsStaleOwnerMapping() // Review Focus 4: placeholder (sealFirstPage + growth) -> chunk chain (grow beyond the content page) -> shrink back to a plain record (#6178 collapse); then write an unrelated record onto the old content page -> owner map has no entry for it, the original owner is not re-read (stats.placeholdersResolvedCount(type) unchanged)
@Test void externalPropertyChangeIsFollowed()          // EXTERNAL property indexed; update only its value -> projection current
@Test void edgeOnVertexIsANoOp()                       // add 100 edges to a vertex with COLUMNAR on Vertex.brand -> overlay rows unchanged (stats.overlayRows(type) equal before/after)
@Test void unreadableChainKeepsPreviousValue()         // stats.unreadableCount(type) == 1, previous value still counted
@Test void passHoldsTheApplyLock()                     // R13: KillPoint "apply.beforePublish" blocks a pass -> projection.applyLock().isLocked() true from another thread; released after the pass
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest=ColumnarApplierTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement `ColumnarApplier`** and wire passes, flush (`shouldFlush` checked at the end of each pass, flush submitted to `ColumnarMaintenancePool`) and merge (`MergePolicy.select` after each flush) in the manager.
- [ ] **Step 4: Run to verify they pass.** Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `ColumnarApplier.java`, `ColumnarIndexManager.java`, `ColumnarProjection.java`, the test.

---

### Task 16: Recovery, rebuild and the WAL housekeeping hook (spec 6.5)

> Execution note (Task 14 review): rebuild reuse that skips a page when its version equals `SegmentReader.pageVersion(page)` must exclude pages that hold placeholder/chunk owners in the ContentOwnerMap (their content pages can change without the primary page's version moving), or compare the owners' content-page versions too.


> Execution note (Task 13 review): `persistRecoverySet()` is also called by the Raft checkpoint (`ArcadeStateMachine.takeSnapshot`, Task 22 note) and must be safe to call from that thread at any time; on a clean open a stray NOWAL written after `manager.close()` must not be treated as unclean unless the open is unclean.


> Execution note (Task 12 review): the rebuild requester (`ColumnarProjection.setRebuildRequester`) must tolerate a request that arrives on a projection that is closing/closed/stopped (no-op), because `requestRebuild` is check-then-act; the rebuild keeps overlay rows applied after the projection's routes were published.


> Execution note (Task 12): a commit can land between `ColumnarIndexManager.close()` and the database write lock; the clean-close path persists what the trackers still hold INSIDE `LocalDatabase.closeUnderWriteLock`, after `waitAllPagesOfDatabaseAreFlushed`/`syncFiles` (the same hook point as the NOWAL delete), where commits are excluded; never an unconditional empty recovery set, and never in `close()`/`closeDurableParts` before the write lock.


> Execution note (Task 10 review): every manifest writer holds `applyLock`, takes its generation from `ColumnarProjection.nextGeneration()` (which honors the generation floor after an uncertain publish), and uses the same phase-based failure handling as `ColumnarFlusher` (delete only before the manifest rename is attempted).


> Execution note (Task 9 review): a rebuild on `ColumnarMaintenancePool` must not park a worker on `REBUILD_SEMAPHORE` (1 permit vs 2 maintenance threads would stall every flush in the JVM): use `tryAcquire` and re-submit later (or a coalescing rebuild queue); rebuilds are bounded by R17 coalescing.


> Execution notes (Task 8 review): rebuild files are reserved/held through `ColumnarProjection` like flush files (Task 10 note a); a rebuild publish holds `applyLock`; schema and `check_fix` rebuilds must not go through `markUnusable`, so the previous valid state keeps serving until the rebuilt one publishes.


> Execution note (Task 7 review): the recovery sample is crash-safe only together with Task 13's pending-marks guard on `WALFile` (a WAL file is never selected for dropping while a commit in it has not finished mark #2). `DirtyPageTracker.collectRecoverySet` guarantees: every page whose mark returned before the sample started is in the sample, or was flushed by an apply that read it after it became visible.


**Files:**
- Create: `index/columnar/RecoverySetFile.java`, `index/columnar/ColumnarRebuilder.java`
- Modify: `engine/TransactionManager.java` `cleanWALFiles(dropFiles, force, syncDataOnDrop)` (`:2165`): insert `if (dropFiles && !persistColumnarRecoverySet()) return false;` AFTER the statement `if (dropFiles && syncDataOnDrop && !database.getFileManager().syncFiles()) return false;` (about `:2191`) and before the drop loop, as its own statement, NOT nested in that condition (R27): the housekeeping and rotation callers use the 2-arg form (`syncDataOnDrop = false`, `:402`, `:411`, `:1639`, `:1648`) and must persist too. Same abort semantics as the fsync failure (the pass returns false and retries later). `persistColumnarRecoverySet()` is a private helper: `database` is a `LocalDatabase` -> `getColumnarIndexManager().persistRecoverySet()`, else true.
- Modify: `ColumnarIndexManager.java` (`persistRecoverySet`, `startApplying` recovery merge, rebuild scheduling, `close()` writes the empty recovery set, READ_ONLY rules)
- Modify: `index/columnar/ColumnarIndex.java` (R17: `build(...)` calls `ColumnarRebuilder.schedule(manager, projection, "create")` for each bucket projection of the index instead of only `requestReconcile()`; still returns 0; skips projections that are already usable and empty, V-B1/V-M19: published empty by `open`)
- Modify: `ColumnarIndexManager.java` installs the rebuild requester `(reason) -> ColumnarRebuilder.schedule(manager, projection, reason)` on each projection AFTER `open` and after startup rule 2 ran (V-M10), so a live DDL register never schedules `missing`
- Modify: `index/vector/LSMVectorIndex.java` (R29: package-private `static Semaphore rebuildSemaphoreForTesting()` returning the `private static final REBUILD_SEMAPHORE` at `:168`; nothing else)
- Test: `index/columnar/RecoverySetFileTest.java`, `index/columnar/ColumnarRebuilderTest.java`, `engine/src/test/java/com/arcadedb/index/vector/LSMVectorIndexTestAccess.java` (helper `public static Semaphore rebuildSemaphore()` over `rebuildSemaphoreForTesting()`, R29, V-M20), `engine/src/test/java/com/arcadedb/engine/ColumnarTransactionManagerTestAccess.java` (public static `rotateAndDropWAL(TransactionManager)` over the package-private `rotateAndDropWALForTesting()` `:2234`, and `cleanWALFiles(TransactionManager, boolean dropFiles, boolean force)` over `cleanWALFilesForTesting(dropFiles, force, false)` `:2135`, the 2-arg path; Task 21 reuses it), `index/columnar/ColumnarIndexManagerLifecycleTest.java` (receives three tests from Task 12, R18)

**Interfaces:**
- Consumes: Tasks 2 (`GlobalDictionary.create`), 5, 7, 8, 13 (`NOWAL`), 14 (`visitCommittedPage`, `getCommittedPageCount()`), 15.
- Produces:

```java
final class RecoverySetFile {                         // columnar/recovery.pages: magic, version, per file (fileId, count, int[] pages), CRC32; primary AND external file ids (R7)
  static void write(Path file, int[] fileIds, int[][] pages, int[] counts) throws IOException; // R15; writeAtomically
  static Map<Integer, int[]> read(Path file) throws IOException;         // cold path, boxing acceptable; empty map when absent; BlockCorruptionException when bad
}
final class ColumnarRebuilder {
  static final Semaphore REBUILD_SEMAPHORE = new Semaphore(GlobalConfiguration.COLUMNAR_REBUILD_CONCURRENCY.getValueAsInteger()); // separate from LSMVectorIndex.REBUILD_SEMAPHORE (spec 5.6)
  static void schedule(ColumnarIndexManager manager, ColumnarProjection projection, String reason); // reasons: create, snapshot_install, missing, corrupt, unclean_no_wal, schema, check_fix
}
```

Scheduling and counting (R17): `schedule` coalesces while a rebuild of the same projection is queued or running (a per-projection CAS flag; a request arriving while one RUNS sets a "rerun" bit so the scan restarts once afterwards), and `stats.rebuild(type, reason)` +1 once per rebuild that actually runs, with the reason of the request that started it. Every path that turns a new index into a build goes here with `create`: `ColumnarIndex.build()` (leader or embedded DDL) and, from Task 17, the reconciler's `ADD_COLUMN` (follower schema entry). Creating five COLUMNAR indexes on a populated type therefore runs one rebuild when the first is still queued. READ_ONLY (R16): `schedule` returns at once, no counter, no file.

Startup (`startApplying`, after WAL replay, after the `if (recoveryPending)` block of `LocalDatabase.openInternal`, `:4383-4393`, R26):
1. `columnar/REBUILD` marker present -> every projection rebuilds with its reason (`snapshot_install`), marker cleared after all published.
2. Per projection, ONLY at open (never for a projection registered later by a live DDL, R17): `unusableReason()` (Task 8) is `missing` while the schema has the index on a bucket with records -> rebuild `missing`; `corrupt` (a file failing its CRC or format check) -> `corrupt`; `<bucketDir>/NOWAL` present (`ColumnarStore.NOWAL_MARKER`, R8) -> `unclean_no_wal`. (An empty bucket never reaches this rule: Task 8 `open` already published it empty, V-B1.)
3. Otherwise mark `recovery.pages[fileId]` into the matching tracker (primary or external, R7; WAL replay marks are already there from Task 13). `stats.recoveryPages(n)` with the number of pages marked (V-M20). No rebuild, no counter: a clean open or a crash recovery never increments `rebuilds`.
READ_ONLY (R16): steps 1-3 only record reasons (`markUnusable` serves the fallback); nothing is scheduled, cleared or written.

Housekeeping: `persistRecoverySet` collects `DirtyPageTracker.collectRecoverySet` of every tracker of every projection (primary and external) and writes them with `RecoverySetFile.write`; kill point `"recoverySet.persisted"` after the rename and before returning to `cleanWALFiles`. `close()` writes an empty recovery set after the final flush (writable databases only).

Rebuild (on `ColumnarMaintenancePool`, holding `REBUILD_SEMAPHORE`): staging dictionaries created with `GlobalDictionary.create(file)` for a new generation file in the bucket directory (R10; fsynced at publish, Task 5 order, never `createInMemory`), scan pages `0..bucket.getCommittedPageCount()` with `visitCommittedPage` into `SegmentWriter`s of `COLUMNAR_FLUSH_MAX_ROWS` rows; for `check_fix` and `schema` with a previous valid state, a page whose current version equals `SegmentReader.pageVersion(page)` of the previous state copies its rows (read back into `RowValues` from the previous `SegmentReader`) instead of extracting (spec 6.5 "skip unchanged pages", `stats.rebuildPagesReused`). While it runs, apply passes keep updating the old state and add every page they apply to `appliedDuringRebuild`; at publish, under `publishLock`, the new state replaces the old one and `appliedDuringRebuild` is marked dirty again, so nothing applied during the scan is lost. Files of the old state go through the last-release path (Task 8, R1/R2).

- [ ] **Step 1: Write the failing tests**

```java
// RecoverySetFileTest
@Test void roundTrip()                                 // fileIds {3, 9}, pages {{1,5,7},{2}} -> read gives the same map
@Test void corruptFileIsBlockCorruption(); @Test void absentFileIsEmpty();
// ColumnarRebuilderTest (TestHelper)
@Test void createOnExistingDataBuildsInBackground()   // 20_000 records, then CREATE INDEX COLUMNAR (one index) -> awaitCaughtUp; FacetOracle equal; stats.rebuilds("Asset","create") == 1, rebuilds("Asset","missing") == 0
@Test void createRequestsCoalesce()                    // R17: all ColumnarRebuilder.REBUILD_SEMAPHORE permits held by the test; CREATE five COLUMNAR indexes on a populated Asset; release -> rebuilds("Asset","create") == 1, oracle equal for all five properties
@Test void buildConcurrentWithWritesConverges()        // Review Focus 3: 4 writer threads insert/update for the whole build; after they stop, awaitCaughtUp -> oracle equal
@Test void missingProjectionIsRebuiltAndCounted()      // close, delete columnar/b<fileId>, reopen -> rebuilds("Asset","missing") == 1, oracle equal
@Test void corruptSegmentIsRebuiltAsCorrupt()          // flip a byte in a segment footer, reopen -> rebuilds("Asset","corrupt") == 1, rebuilds("Asset","missing") == 0, oracle equal
@Test void corruptManifestIsRebuiltAsCorrupt()         // garbage manifest.json -> rebuilds(...,"corrupt") == 1
@Test void corruptDictionaryIsRebuiltAsCorrupt()       // dictionary file shorter than the manifest's validBytes -> rebuilds(...,"corrupt") == 1
@Test void noWalMarkerRebuildsUncleanNoWal()           // R8: close, write <bucketDir>/NOWAL by hand, reopen -> rebuilds(...,"unclean_no_wal") == 1, NOWAL gone after publish
@Test void snapshotInstallMarkerRebuildsWithReason()   // close, ColumnarStore.prepareForSnapshotInstall(path), reopen -> rebuilds(...,"snapshot_install") == 1, marker gone after
@Test void rebuildUsesItsOwnSemaphore()                // R29: hold all LSMVectorIndexTestAccess.rebuildSemaphore() permits -> columnar rebuild still completes; release them in finally
@Test void unchangedPagesAreReusedOnCheckFixRebuild()  // rebuild with reason check_fix on an unchanged bucket -> stats.rebuildPagesReusedCount(type) == total pages, oracle equal
@Test void cleanReopenDoesNotRebuild()                 // rebuildsTotal() unchanged across close/reopen
@Test void stagedDictionaryIsAGenerationFile()          // R10: after a missing rebuild the manifest names dict-<indexFileId>.<gen>.dic and that file exists with the manifest's byte length
@Test void housekeepingPersistsBeforeDrop()            // R27: dirty pages pending, ColumnarTransactionManagerTestAccess.cleanWALFiles(tm, true, true) (syncDataOnDrop false, the rotation path) -> recovery.pages holds them before the WAL file is gone
// ColumnarIndexManagerLifecycleTest (moved from Task 12, R18)
@Test void cleanCloseFlushesAndLeavesEmptyRecoverySet() // insert, close, reopen -> manifest has segments, recovery.pages empty, stats.rebuildsTotal() == 0
@Test void readOnlyOpenNeverWritesTheProjection()       // Review Focus 5, R16: close, open READ_ONLY; list columnar/ file names and mtimes before/after a query of the snapshot -> unchanged; no pass scheduled (stats.applyPasses() == 0); then close, delete one manifest.json, open READ_ONLY again -> unusable, rebuildsTotal() == 0, no file created
@Test void killDoesNoIo()                               // arm a kill point in flush, kill(): no further file appears in columnar/
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest='RecoverySetFileTest,ColumnarRebuilderTest,ColumnarIndexManagerLifecycleTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement** both classes, the housekeeping hook, the startup sequence, the `build()` change, the READ_ONLY rules and the `LSMVectorIndex` accessor.
- [ ] **Step 4: Run to verify they pass** (same command, which re-runs the whole `ColumnarIndexManagerLifecycleTest` including the Task 12 and 13 methods), plus `mvn -o -pl engine -am test -Dtest='TransactionManager*Test,Issue8626*Test,*WALRotation*Test,ColumnarIndexDdlTest' -DexcludedGroups=benchmark -Dsurefire.failIfNoSpecifiedTests=false` (no engine `Issue5277*` class exists; the `LSMVectorIndex` change is an accessor only, covered by compilation). Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `RecoverySetFile.java`, `ColumnarRebuilder.java`, `TransactionManager.java`, `ColumnarIndexManager.java`, `ColumnarIndex.java`, `LSMVectorIndex.java`, `LSMVectorIndexTestAccess.java`, `ColumnarTransactionManagerTestAccess.java`, the three tests.

---

### Task 17: Schema events and reconcile (spec 6.4)

> Execution note (Task 12 review): the manager's per-type extractor cache must be invalidated by supertype changes (ALTER TYPE SUPERTYPE) through the reconciler.


> Execution notes (Task 6 review): RENAME keeps data but the overlay compares `ColumnDefinition` records including the property name; RENAME must re-key the projection's overlay columns (see Task 8 note f) so the next apply pass does not throw on `replacePage`.


**Files:**
- Create: `index/columnar/ColumnarSchemaEvent.java`, `index/columnar/ColumnarReconciler.java`
- Modify: `schema/LocalSchema.java` - end of `rebuildBucketTypeMap()` (`:5965`, non-staging branch), `endStagedPublication()` (`:1067`, when the staged generation was published) and after a successful write in `saveConfiguration(boolean)` (`:4677`, the path every successful DDL persists through): `requestReconcile()` (V-B5)
- Modify: `engine/DatabaseChecker.java` - (a) after `bucket.check(verboseLevel, fix)` (`:2420`) with `fix == true`, `onSchemaEvent(ColumnarSchemaEvent.rebuild(bucket.getFileId(), "check_fix"))`; (b) R19: in the "Rebuilding indexes" selection (`:474-540`, the `affectedIndexes.removeIf(...)` filter that already drops manual indexes), also drop every index whose `getType()` is not `isMaintainedInTransaction()`, so `CHECK DATABASE FIX` never drops and re-creates a COLUMNAR shell index (a new file id would read as `ADD_COLUMN` and count a spurious second rebuild with reason `create`); the `check_fix` event of (a) covers the projection
- Modify: `index/columnar/ColumnarIndexManager.java` (reconcile entry, event dispatch, listeners), `index/columnar/ColumnarProjection.java` (`RENAME` updates manifest columns and bucket name at next publish)
- Test: `index/columnar/ColumnarReconcilerTest.java`

**Interfaces:**
- Consumes: `ColumnarRebuilder.schedule` (Task 16), `ColumnarProjection.drop()` (Task 8, deferred deletion), `ColumnarIndexMetadata.getHierarchy()` (Task 11).
- Produces:

```java
public record ColumnarSchemaEvent(Kind kind, int bucketFileId, int indexFileId, String reason, String typeName) {
  public enum Kind { DROP_PROJECTION, DROP_COLUMN, ADD_COLUMN, RETYPE_COLUMN, RENAME, CREATE_EMPTY, REBUILD, HIERARCHY_SOURCE_CHANGED }
  public static ColumnarSchemaEvent rebuild(int bucketFileId, String reason);
}
final class ColumnarReconciler {
  static List<ColumnarSchemaEvent> diff(Map<Integer, ColumnDefinition[]> current, Map<Integer, ColumnDefinition[]> fromSchema, IntPredicate bucketIsEmpty);
  static Map<String, String> hierarchyFingerprints(Schema schema, Collection<ColumnarIndexMetadata> withHierarchy); // R20: for every type named by a getHierarchy() (vertexType, parentEdge): "exists|bucketIds|key name:type" (absent type -> "absent")
  static List<ColumnarSchemaEvent> hierarchyDiff(Map<String, String> before, Map<String, String> after);           // one HIERARCHY_SOURCE_CHANGED(typeName) per type whose fingerprint differs, appeared or disappeared
}
```

`diff`'s `current` is the reconciler's own map from the previous reconcile, not the projections' columns (V-B4). `ADD_COLUMN` schedules `create` only when the projection's published manifest lacks that index file id AND no rebuild of the projection is queued or running; otherwise it is a no-op (the leader's `build()` already covered it).

The manager keeps the last `hierarchyFingerprints` map next to the definition map; each reconcile computes both diffs (R20: the definition maps hold only COLUMNAR columns, so `diff` alone cannot see a change to a hierarchy source type).

Reconcile runs on the apply pool before the next pass when the schema generation moved; it rebuilds the definition map from `ColumnarIndex` instances in the schema (keyed by bucket file id, columns by index file id) and applies `diff`:

| Spec 6.4 event | How it reaches the projection | Action |
|---|---|---|
| Drop type, drop property, drop index | Index gone from schema (leader DDL or follower schema entry) | `DROP_COLUMN`; last column -> `DROP_PROJECTION` (directory deleted after readers release) |
| Rename type or property | Same index file id, new property name / type name | `RENAME`: manifest columns and bucket name updated at next publish, data unchanged; the directory `columnar/b<fileId>` is keyed by file id (R9), so `ALTER TYPE ... NAME` (bucket files renamed in place, `LocalDocumentType.java:717`) never reads as `missing` |
| `ALTER PROPERTY` type change | Same index file id, different `propertyType` | `RETYPE_COLUMN` -> rebuild reason `schema` |
| `ALTER TYPE` adding buckets (`addBucket`, `LocalDocumentType.java:1642`) | New bucket with the index | `CREATE_EMPTY` (empty bucket: no rebuild, no counter) |
| Remove bucket | Bucket gone | `DROP_PROJECTION` |
| New index on a bucket with records | New index file id | `ADD_COLUMN` -> `ColumnarRebuilder.schedule(manager, projection, "create")` when the manifest lacks the column and no rebuild is queued or running (R17, V-B4, V-M14: the same coalescing entry `ColumnarIndex.build()` uses on the leader, so a node that ran both counts one rebuild) |
| Bucket truncate | `TruncateRecordDeleter` deletes through ordinary transactions (`TruncateBucketStatement.java:107-113`) | Dirty pages; the projection empties by deletes, no event (deliberate deviation R24, below) |
| `check(fix=true)` | `DatabaseChecker.java:2420`; COLUMNAR shell indexes are excluded from the checker's index rebuild (R19) | `REBUILD` reason `check_fix`, exactly one rebuild per affected bucket |
| Backup restore | Restored directory has no `columnar/` (backup ships registered files only, `FullBackupFormat.java:357`) | `missing` at open (Task 16) |
| Hierarchy vertex/edge type change | A type named by any `ColumnarIndexMetadata.getHierarchy()` (`vertexType`, `parentEdge`) dropped, renamed, buckets changed, or its `key` property changed: `hierarchyDiff` over the fingerprints (R20) | `HIERARCHY_SOURCE_CHANGED` (typeName set) delivered through `onSchemaEvent` to every `addSchemaEventListener` listener; no projection change (plan 5 rebuilds `parents[]`) |

`DROP_PROJECTION` calls `ColumnarProjection.drop()`, which deletes the directory only after the last reader releases (P52).

**Deliberate spec deviation (R24).** Spec 6.4 says "bucket truncate: drop that bucket's projection". Plan 3 does not hook truncate: `TruncateRecordDeleter` deletes through ordinary transactions, the pages go dirty, and the next apply pass turns every position dead. The result is the same exact state with no extra hook. Cost: one apply pass that re-reads every page of the truncated bucket (O(pages)) instead of an O(1) drop of the directory; counts stay exact either way.

- [ ] **Step 1: Write the failing tests**

```java
// unit, pure diff
@Test void dropLastColumnDropsProjection(); @Test void renameKeepsData(); @Test void retypeRebuildsWithSchemaReason();
@Test void newEmptyBucketCreatesWithoutRebuild(); @Test void newIndexOnDataRebuildsWithCreateReason();
@Test void hierarchyDiffFlagsChangedTypesOnly()        // R20, pure: before {Tag:a, ParentOf:b}, after {Tag:a2, ParentOf:b} -> one event, typeName "Tag"; Tag absent after -> one event
@Test void hierarchySourceTypeChangeIsForwarded()       // index with hierarchy {Tag, path, ParentOf}; ALTER TYPE Tag BUCKET +Tag_2 -> await one HIERARCHY_SOURCE_CHANGED(Tag); then DROP PROPERTY Tag.path -> a second one (V-M21); projection generation unchanged
// integration (TestHelper)
@Test void dropIndexDropsTheProjectionDirectory()
@Test void dropTypeDropsEveryBucketProjection()
@Test void renamePropertyKeepsCounts()                 // ALTER PROPERTY Asset.brand NAME maker -> counts unchanged, no rebuild counted
@Test void renameTypeKeepsTheProjection()              // R9: ALTER TYPE Asset NAME Item; reopen -> same columnar/b<fileId> directory, rebuildsTotal() unchanged, counts unchanged
@Test void alterTypeAddBucketCreatesEmptyProjection()  // ALTER TYPE Asset BUCKET +Asset_extra -> projection(newBucketId) usable, rebuilds total unchanged
@Test void truncateBucketEmptiesByDeletes()            // TRUNCATE BUCKET -> oracle all zero (R24: no projection drop, no rebuild counted)
@Test void checkFixRebuildsAffectedBuckets()           // CHECK DATABASE FIX on a damaged Asset bucket -> rebuilds(...,"check_fix") == affected buckets of Asset, rebuilds(...,"create") unchanged, the COLUMNAR index keeps its file id (R19)
@Test void addColumnAfterLeaderBuildDoesNotRebuildTwice() // V-B4: CREATE INDEX on data, build() rebuild completes, then reconcile runs -> rebuilds("Asset","create") == 1
@Test void dropIndexWhileReadingDefersDeletion()       // P52: acquire a snapshot; DROP INDEX -> directory still there; snapshot.close() -> gone
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest=ColumnarReconcilerTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement** event record, reconciler (both diffs) and the hooks (two `LocalSchema` sites, two `DatabaseChecker` changes).
- [ ] **Step 4: Run to verify they pass**, plus `mvn -o -pl engine -am test -Dtest='LocalSchema*Test,DatabaseChecker*Test,*SchemaReload*Test,Issue8635*Test' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - the two classes, `LocalSchema.java`, `DatabaseChecker.java`, `ColumnarIndexManager.java`, `ColumnarProjection.java`, the test.

---

### Task 18: Metrics

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/monitor/ColumnarMetrics.java`
- Modify: `server/src/main/java/com/arcadedb/server/ArcadeDBServer.java:1215-1224` (create, keep in a field, close in `stopMetrics()` (`:1273`) like `haReplicationMetrics` (closed at `:1293`))
- Test: `server/src/test/java/com/arcadedb/server/monitor/ColumnarMetricsIT.java` (extends `BaseGraphServerTest`)
- Not touched (R3): `ColumnarStats.java` is complete since Task 3 (every cell, reader and `forEachKey` this binder needs); `ColumnarStatsTest` lives in Task 3. If a meter below has no cell there, fix Task 3's class, do not add an ad hoc cell here.

**Interfaces:**
- Consumes: `ColumnarStats` (Task 3) through `ColumnarIndexManager.getStats()`; plan 2 `BlockCache.instance().hits()/misses()/usedBytes()`; `EngineMetricsBinder`, `HAReplicationMetrics` in `server/monitor` as patterns.
- Produces:

```java
public final class ColumnarMetrics implements AutoCloseable {
  public ColumnarMetrics(ArcadeDBServer server);       // binds to Metrics.globalRegistry; refresh scheduler every 5 s (one daemon thread, as HAReplicationMetrics)
}
```

Meters (all with tag `database`, plus `type`, and `property` / `reason` where listed): counters `arcadedb.columnar.rebuilds{type,reason}` (reason in `create | snapshot_install | missing | corrupt | unclean_no_wal | schema | check_fix`), `dictionary.compactions{type,property}`, `dictionary.ordinals.reclaimed{type,property}`, `apply.pages{type}`, `apply.placeholders.resolved{type}`, `flush.count{type}`, `merge.count{type}`, `conversion.failures{type,property}`, `recovery.pages`, `apply.records.reread{type}` (re-read amplification = this over `apply.pages`), `apply.unreadable{type}`, `rebuild.pages.reused{type}`; timers `flush.duration{type}`, `merge.duration{type}`; gauges `apply.lag.ms{type}`, `overlay.rows{type}`, `segments{type}`, `segments.dead.ratio{type}`, `dictionary.size{type,property}`, `cardinality.exceeded{type,property}`, `files.open{type}`. Apply throughput is the rate of `apply.pages`. Spec 9's cache hit rate comes from plan 2's JVM-wide `BlockCache`: function counters `arcadedb.columnar.cache.hits` / `cache.misses` and gauge `cache.used.bytes`, registered once by the binder with tag `database="*"` (the cache is shared by every database; this is the only columnar meter without a real database name). Query-side spec 9 meters (budget waits, patch-budget overflows, fallback scans, patched) are plan 4's. The binder keeps a strong reference to each database's `ColumnarStats` (Micrometer function meters hold weak references, see `EngineMetricsBinder` javadoc), registers new keys on each refresh and removes the meters of a closed database.

- [ ] **Step 1: Write the failing tests**

```java
// ColumnarMetricsIT (server)
@Test void rebuildCounterHasDatabaseTypeReasonTags()   // CREATE INDEX on data -> Metrics.globalRegistry.find("arcadedb.columnar.rebuilds").tags("database", db, "type","Asset","reason","create").functionCounter().count() == 1
@Test void prometheusEndpointExposesColumnarMeters()   // GET getServerHttpUrl(0, "/prometheus") body contains arcadedb_columnar_rebuilds_total{ and database="<name>"
@Test void restartDoesNotIncrementRebuilds()           // restart server -> counter for every reason unchanged
@Test void corruptReasonIsItsOwnSeries()               // corrupt a segment, restart -> series reason="corrupt" == 1 and reason="missing" absent or 0
@Test void closedDatabaseMetersAreRemoved()
@Test void everySpecSection9MeterIsRegistered()       // after inserts, a flush and a merge: each name of the list above is found for the database (cache meters for database="*"); flush.duration count > 0
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl server -am verify -DskipITs=false -Dit.test=ColumnarMetricsIT -Dfailsafe.failIfNoSpecifiedTests=false -Dtest=ColumnarStatsTest -Dsurefire.failIfNoSpecifiedTests=false` (`ColumnarStatsTest` from Task 3 re-runs as the binder's input check). Expected: FAIL (the IT).
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run to verify they pass** (same command). Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `ColumnarMetrics.java`, `ArcadeDBServer.java`, `ColumnarMetricsIT.java`.

---

### Task 19: Shared fixtures FacetOracle and ColumnarTestData

**Files:**
- Create: `engine/src/test/java/com/arcadedb/index/columnar/FacetOracle.java`, `engine/src/test/java/com/arcadedb/index/columnar/ColumnarTestData.java`
- No pom change: the engine test-jar is already produced (`engine/pom.xml:144`) and ha-raft (`pom.xml:~76`), server and integration already depend on it
- Test: `index/columnar/FacetOracleSelfTest.java`

**Interfaces:**
- Consumes: `ColumnarIndexManager`, `ColumnarSnapshot`, `ColumnReader.forEachOrdinal` (Tasks 4, 6, 8, 12), `ColumnarValueExtractor` (Task 3), `BucketPageLayoutTestSupport.sealFirstPage(Database, String)` (Task 14).
- Produces (reused by plans 4, 5, 6):

```java
public final class FacetOracle {
  public static final Object MISSING = new Object();                                                       // key under which records with no (convertible) value are counted
  public static Map<Object, Long> expectedCounts(Database db, String type, String property, String where);   // GROUP BY, polymorphic; BY ITEM inferred from the COLUMNAR index on the property; where may be null
  public static Map<Object, Long> projectionCounts(Database db, String type, String property, String where, boolean patchPending); // through ColumnarSnapshot of every polymorphic bucket
  public static void awaitCaughtUp(Database db);                                                           // ColumnarIndexManager.awaitCaughtUp(30_000) of db's embedded LocalDatabase, AssertionError on timeout
  public static void assertEquivalent(Database db, String type, String property, String where);            // awaitCaughtUp(db) then expectedCounts == projectionCounts
  public static void assertEquivalentWhileLagging(Database db, String type, String property, String where); // no wait, patchPending = true
  public static Map<Long, Long> expectedHistogram(Database db, String type, String property, String where);           // LONG columns: value -> count
  public static Map<Long, Long> projectionHistogram(Database db, String type, String property, String where);
}
public final class ColumnarTestData {
  public ColumnarTestData(long seed);                               // all randomness from new Random(seed)
  // knobs, public mutable fields with these defaults:
  public int brands = 200; public int tagVocabulary = 2_000; public int maxTags = 8;
  public double bigRecordRatio = 0.0;  /* pads body to force chunks (> page, 5 x page); since #6149 growth makes chunk chains, never placeholders */
  public double placeholderRatio = 0.0; /* R21, V-M17: every 1/placeholderRatio records, between transactions, call sealFirstPage(db, "Asset") (seals the last page of the single bucket) and grow one earlier record of that page; filler records count as MISSING on both oracle sides; ignored when buckets > 1 */
  public double deleteRatio = 0.0; public double reuseRatio = 0.0; public boolean externalBody = false;
  public boolean asVertices = false; public int edgesPerVertex = 0;
  public int buckets = 1;                                           // > 1 creates Asset with BUCKETS n (multi-bucket tests of plans 4 and 5)
  public void createSchema(Database db);                            // Asset(brand STRING, tags LIST OF STRING, fileSize LONG, publishDate DATETIME, author STRING, body STRING [EXTERNAL]) + COLUMNAR on brand, tags BY ITEM, fileSize, publishDate, author; Asset is a VERTEX type when asVertices (so Task 20 can CREATE VERTEX TYPE Image EXTENDS Asset)
  // tags: drawn WITH replacement from tagVocabulary, and every 50th record carries one of its tags twice (R22: an in-record duplicate must count the document once)
  public List<RID> insertAssets(Database db, int count);            // in transactions of 500
  public void mutate(Database db, int operations);                  // insert, update (incl. growth/shrink), delete, slot reuse, edges, by the knobs
}
```

Oracle queries: terms `SELECT <p> AS v, count(*) AS c FROM <type> [WHERE] GROUP BY <p>`; by item `SELECT v, count(*) AS c FROM (SELECT <p>.asSet() AS v FROM <type> [WHERE] UNWIND v) GROUP BY v` (`SQLMethodAsSet` exists). Both sides map keys through `Type.normalizeForKey` (so a DOUBLE column compares through the same normalization: Double 2.0 keys as Long 2), drop `null` into `missing`, and drop values `ColumnarValueExtractor` would reject (counted missing on both sides, Review Focus 2; with R6 an ORDINAL column accepts every `DictionaryValueCodec.isSupported` value, so a STRING property holding a number is counted on both sides, not dropped). Projection side: match set from `SELECT @rid FROM <type> [WHERE]` into one `PositionBitmap` per bucket; per segment view `forEachOrdinal(matches, ...)` (R12: the reader already restricts to `matches AND live()`, the oracle never ANDs live itself); values decoded with `snapshot.dictionary(property)`; with `patchPending`, matches inside `pendingPositions()` are removed from the segment pass and counted by reading the record and `extractor.extract(record, property, snapshot.dictionary(property), sink)` instead.

The `SELECT @rid` match is oracle-only; no production path in plans 3-4 builds a match set from SQL rows (spike: NO-GO with it).

- [ ] **Step 1: Write the failing tests**

```java
@Test void oracleAgreesWithItselfOnStaticData()      // insertAssets(db, 5_000), assertEquivalent for brand, tags, fileSize; expectedCounts(db,"Asset","tags",null) counts documents per tag (BY ITEM inferred)
@Test void oracleDetectsADeliberateDivergence()      // corrupt one overlay value through a test hook -> assertEquivalent throws AssertionError (proves the oracle can fail)
@Test void seededDataIsReproducible()                // same seed -> identical brand counts
@Test void awaitCaughtUpWaitsForPendingPages()       // with manager.suspendApply() held for 200 ms on another thread, awaitCaughtUp(db) returns only after the hold closes and the tracker is empty
@Test void duplicateTagInOneRecordCountsOnce()       // R22: insert tags ["a","a","b"] -> expectedCounts and projectionCounts both give a=1, b=1
@Test void seededDataContainsInRecordDuplicates()    // insertAssets(db, 1_000) -> at least one record whose tags list has a repeated value
@Test void placeholderRatioProducesPlaceholders()   // R21: placeholderRatio 0.05 -> BucketPageLayoutTestSupport.bucketStats(db, "Asset") reports totalPlaceholderRecords > 0; assertEquivalent for brand still holds
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest=FacetOracleSelfTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement both fixtures.**
- [ ] **Step 4: Run to verify they pass.** Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `FacetOracle.java`, `ColumnarTestData.java`, `FacetOracleSelfTest.java`.

---

### Task 20: Engine equivalence integration tests

**Files:**
- Test: `index/columnar/ColumnarEquivalenceTest.java` (`@Tag("slow")` on the randomized methods), `index/columnar/ColumnarIngestOverheadBenchmark.java` and `index/columnar/ColumnarCountBenchmark.java` (both `@Tag("benchmark")`, report only)
- Modify: this plan's "Spike results" section (the "Plan 3 count overhead" line) with the `ColumnarCountBenchmark` medians

**Interfaces:** consumes Task 19; the count benchmark also uses `ColumnarSnapshot`, `ColumnReader.forEachOrdinal`, `SegmentReader.open`, `BlockCache.instance().clear()` and `com.arcadedb.utility.StallAwareStopwatch.effectiveMs()`.

- [ ] **Step 1: Write the tests**

```java
@Test void staticLoadAtLagZero()                        // 20_000 records, all five properties equivalent
@ParameterizedTest @ValueSource(longs = {1, 7, 42, 1234}) @Tag("slow")
void randomizedOperationsAtLagZero(long seed)           // knobs: bigRecordRatio 0.05, placeholderRatio 0.02 (R21), deleteRatio 0.1, reuseRatio 0.05, externalBody true, asVertices true, edgesPerVertex 3; 20 rounds x 2_000 ops; after each round assertEquivalent for brand, tags, author; histograms for fileSize, publishDate
@Test @Tag("slow") void randomizedWhileLagging()         // suspendApply() mid-round; assertEquivalentWhileLagging after every 200 ops; close the hold; assertEquivalent
@Test void withWhereClause()                            // WHERE fileSize > 1_000_000 AND brand <> 'b7'
@Test void polymorphicSubtypeBucketsAreProjected()      // Review Focus 1: CREATE VERTEX TYPE Image EXTENDS Asset; records in Image; Asset facets include them, equal to polymorphic GROUP BY
@Test void schemaEventsInTheMiddleOfOperations()        // rename property, add bucket, drop + recreate index between rounds; equivalence after each
@Test void multipleFlushesAndMergesKeepEquivalence()    // flushMaxRows 1_000, mergeMaxSegments 3 (database-scoped settings) -> stats.mergeCount("Asset") > 0 (V-M22) and equivalence holds
@Test void multiBucketTypeAtLagZero()                    // ColumnarTestData.buckets = 3: assertEquivalent for brand, tags, fileSize (one projection and one dictionary per bucket)
// ColumnarIngestOverheadBenchmark (spec 2 goal "ingest cost unchanged", spec 10 benchmarks): 200_000 inserts plus 100_000 updates of Asset with and without the five COLUMNAR indexes, StallAwareStopwatch effectiveMs per phase, commit-thread allocated bytes (ThreadMXBean) per commit; logged through LogManager, nothing asserted
// ColumnarCountBenchmark (spike observations 7 and 8, spec 5.2 and 10): 1_000_000 Assets; brand and tags counts for a 1% and a 70% match set through forEachOrdinal over
//   (a) one merged segment, (b) 16 segments of 65_536 rows plus an overlay of 5_000 rows, both with dead ratio 0.1,
//   (c) as (b) with BlockCache.instance().clear() before each measured round (cold block cache, OS cache warm), (d) as (c) with a fresh SegmentReader.open per round;
//   StallAwareStopwatch effectiveMs medians per phase logged through LogManager, nothing asserted
```

- [ ] **Step 2: Run** - `mvn -o -pl engine -am test -Dtest=ColumnarEquivalenceTest -DexcludedGroups=benchmark -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS (fix production code, never the oracle, on a failure). Then the benchmarks once, for the record: `mvn -o -pl engine -am test -Dtest='ColumnarIngestOverheadBenchmark,ColumnarCountBenchmark' -DexcludedGroups=none -Dsurefire.failIfNoSpecifiedTests=false`; copy the ingest log lines into the review notes and the count medians (phases a-d) into this plan's "Spike results" as "plan 3 count overhead".
- [ ] **Step 3: Stop: changes ready for review** - `ColumnarEquivalenceTest.java`, `ColumnarIngestOverheadBenchmark.java`, `ColumnarCountBenchmark.java`, this plan file (Spike results line) and any production fix.

---

### Task 21: Engine crash tests

**Files:**
- Test: `index/columnar/ColumnarCrashRecoveryTest.java` (`@Tag("slow")`)
- Reuses (no new seam or helper): kill point `"dictionary.appendPartial"` (Task 5, R11), `TransactionContext.afterPublishBeforeSecondMarkForTesting` (Task 13, P71), `ColumnarTransactionManagerTestAccess` in `com.arcadedb.engine` (Task 16)

Mechanics: `KillPoint.arm(name)`; trigger the operation (`manager.flushNow()` on another thread, or enough writes for a merge); `awaitReached(10_000)`; `((DatabaseInternal) database).kill()` (the crash simulation of `ACIDTransactionTest.java:155-176`); `releaseAsCrashed()`; reopen with `database = factory.open()`; `FacetOracle.assertEquivalent`; assert `rebuilds` total unchanged for crash points (recovery, not rebuild).

- [ ] **Step 1: Write the tests**

```java
@ParameterizedTest @ValueSource(strings = {"flush.segmentWritten", "publish.segmentsSynced", "publish.dictionariesSynced", "publish.manifestTmpWritten", "publish.manifestRenamed"})
void killDuringFlushRecoversWithoutRebuild(String point)
@ParameterizedTest @ValueSource(strings = {"merge.segmentWritten", "publish.manifestTmpWritten", "publish.manifestRenamed"})
void killDuringMergeRecoversWithoutRebuild(String point)
@Test void killDuringDictionaryAppendTruncatesTheTail()        // kill point "dictionary.appendPartial" (placed by Task 5 in GlobalDictionary.getOrAdd: header written, payload not) -> reopen: dictionary size == manifest entries, oracle equal
@Test void killAfterRecoverySetPersistedBeforeWalDrop()        // point "recoverySet.persisted" during ColumnarTransactionManagerTestAccess.rotateAndDropWAL(tm) (Task 16 helper over the package-private rotateAndDropWALForTesting())
@Test void killAfterWalDropIsCoveredByThePersistedSet()        // rotateAndDropWAL completes (WAL gone), kill before any flush -> reopen: pages come from recovery.pages, oracle equal, stats.recoveryPagesCount() > 0
@Test void killBetweenCommitPublishAndSecondMark()             // TransactionContext.afterPublishBeforeSecondMarkForTesting (Task 13) blocks the committing thread; forced WAL drop; kill -> page still recovered (the first mark)
@Test void killWithNoWalWritesRebuildsUncleanNoWal()           // setUseWAL(false) writes, kill -> reopen: rebuilds(...,"unclean_no_wal") == 1, oracle equal
@Test void crashRecoveryNeverRebuilds()                        // plain kill after 10_000 committed writes -> rebuilds total unchanged, oracle equal
```

- [ ] **Step 2: Run** - `mvn -o -pl engine -am test -Dtest=ColumnarCrashRecoveryTest -DexcludedGroups=benchmark -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS.
- [ ] **Step 3: Stop: changes ready for review** - `ColumnarCrashRecoveryTest.java`, any production fix.

---

### Task 22: HA - exclusion from snapshots and backups, snapshot install marker

> Execution note (Task 13 review, CRITICAL, ruled): a follower writes no local WAL, so the Task 16 `cleanWALFiles` hook never covers replicated changes; `ArcadeStateMachine.takeSnapshot` (ha-raft) returns an index that lets Ratis purge the log while the marks of applied entries may live only in memory. Fix: in `takeSnapshot`, after reading `appliedIndex` and BEFORE `registerSnapshotMarker`, call `ColumnarIndexManager.persistRecoverySet()` for every open database; if any returns false, return `RaftLog.INVALID_LOG_INDEX` (abort the checkpoint, same semantics as the `cleanWALFiles` hook). The apply `finally` marks before `lastAppliedIndex` advances, so every mark for an index <= appliedIndex happens-before the persist. Kill test: follower kill -9 right after `takeSnapshot`, before a projection flush -> restart is equivalent with zero rebuilds.


**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java:686` - after `cleanupWalFiles(dbPath)` call `ColumnarStore.prepareForSnapshotInstall(dbPath)`; same call where `recoverPendingSnapshotSwaps` rolls an INSTALLED swap forward
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java:1129` (`addPageFilesToZip`) and `integration/src/main/java/com/arcadedb/integration/backup/format/FullBackupFormat.java:357` - skip any name with `ColumnarStore.isProjectionEntry` (defensive: the projection is not a registered file today, the guard keeps it that way)
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ColumnarSnapshotExclusionTest.java`, `integration/src/test/java/com/arcadedb/integration/backup/ColumnarBackupExclusionTest.java`

- [ ] **Step 1: Write the failing tests**

```java
// ColumnarSnapshotExclusionTest (unit, embedded database with a COLUMNAR index and a flushed projection)
@Test void snapshotZipHasNoProjectionEntry()        // SnapshotHttpHandler.addPageFilesToZip + addConfigurationToZip into a ByteArrayOutputStream ZIP -> no entry name for which isProjectionEntry is true
@Test void installPreparationMarksSnapshotInstall() // run the swap path fixture used by Issue6760PartialSnapshotInstallTest -> columnar/REBUILD == "snapshot_install"
// ColumnarBackupExclusionTest
@Test void backupArchiveHasNoProjectionEntry()      // BACKUP DATABASE -> zip entries never under columnar/
@Test void restoredDatabaseRebuildsAsMissing()      // restore into a new path, open -> rebuilds(...,"missing") == 1, oracle equal
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl ha-raft,integration -am test -Dtest='ColumnarSnapshotExclusionTest,ColumnarBackupExclusionTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL (`installPreparationMarksSnapshotInstall`, `restoredDatabaseRebuildsAsMissing` at least).
- [ ] **Step 3: Implement** the three edits. `prepareForSnapshotInstall` must run before the database is reopened after the swap (`reopenDatabaseUnderSnapshotRecovery`, about `:695`, for the live path; the INSTALLED roll-forward at `:1655` reopens right after too): the call at `:686` and the one in `recoverPendingSnapshotSwaps` (`:1299` / `:1336`, INSTALLED `:1655`) both satisfy that. `addPageFilesToZip` (`:1129`) and `addConfigurationToZip` (`:1210`) are static package-private, so the unit test stays in `com.arcadedb.server.ha.raft`.
- [ ] **Step 4: Run to verify they pass**, plus `mvn -o -pl ha-raft -am test -Dtest='Issue6760*Test,Issue7769*Test,Issue9446*Test,SnapshotManager*Test,Issue7444*Test' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `SnapshotInstaller.java`, `SnapshotHttpHandler.java`, `FullBackupFormat.java`, both tests.

---

### Task 23: HA integration tests (BaseRaftHATest)

**Files:**
- Test (all in `ha-raft/src/test/java/com/arcadedb/server/ha/raft/`): `ColumnarFollowerConvergenceIT.java`, `ColumnarForwardedWritesIT.java`, `ColumnarSnapshotInstallRebuildIT.java`, `ColumnarLeaderFailoverIT.java`, `ColumnarNoRaftTrafficIT.java`; small helper `ColumnarHATestSupport.java` (`new ColumnarTestData(seed).createSchema/insertAssets/mutate` against `getServerDatabase(leader, db)`, plus `assertEquivalentOn(int serverIndex)` = `FacetOracle.assertEquivalent(getServerDatabase(i, db), ...)` for brand, tags, fileSize)

All: `getServerCount()` = 3, `HA_QUORUM` majority, `@Timeout(180)`, waits via `waitForReplicationIsCompleted` / `assertClusterConsistency` (`BaseRaftHATest.java:438, 886`), then `ColumnarIndexManager.awaitCaughtUp(30_000)` on each node, never sleeps.

- [ ] **Step 1: Write the tests**

```java
// ColumnarFollowerConvergenceIT
@Test void followersConvergeThroughRaftApply()        // schema + COLUMNAR on leader; 5_000 inserts, 1_000 updates, 500 deletes on leader; every node equivalent; followers' stats.applyPages > 0 (pages came from TransactionManager.applyChanges)
@Test void createIndexOnFollowerSchemaEntryBuilds()   // data first (schema WITHOUT the five COLUMNAR indexes), then ONE CREATE INDEX ON Asset (brand) COLUMNAR on the leader -> every node rebuilds("Asset","create") == 1 (leader through ColumnarIndex.build, followers through the reconciler's ADD_COLUMN, both via the coalescing ColumnarRebuilder.schedule, R17) and is equivalent
@Test void followerRestartRecoversWithoutRebuild()    // restartServer(follower) -> equivalent; rebuilds total unchanged on it
// ColumnarForwardedWritesIT
@Test void writesSentToAFollowerAreProjectedEverywhere() // commands posted to a follower's HTTP (getServerHttpUrl(follower, "/api/v1/command/<db>")) are forwarded to the leader; leader projection (local commit path) and followers (Raft apply) equivalent
// ColumnarSnapshotInstallRebuildIT
@Test void snapshotInstallTriggersRebuildWithReason() // as Issue4749SnapshotInstallClosesRegisteredDatabaseIT: SnapshotInstaller.install(...) on a follower -> follower rebuilds("Asset","snapshot_install") == 1; equivalent; writes after install converge
@Test void snapshotInstallMidApply()                  // suspendApply() on the follower with pending pages, install, close the hold -> equivalent, no stale overlay survives (projection generation restarted)
// ColumnarLeaderFailoverIT
@Test void leaderFailoverMidApply()                   // suspendApply() on all nodes; 3_000 writes; stop leader (getServer(leader).stop()); new leader via findLeaderIndex(); 2_000 more writes; close the holds; survivors equivalent; restart the old leader -> equivalent, rebuilds total unchanged on it
// ColumnarNoRaftTrafficIT
@Test void projectionIsNeverReplicated()              // after convergence: idx0 = lastRaftLogIndex(getRaftPlugin(leader).getRaftHAServer()) (the helper takes a RaftHAServer, BaseRaftHATest.java:1138, not an index); manager.flushNow() on every node, force merges (mergeMaxSegments 1), trigger a rebuild with CHECK DATABASE FIX on a follower; the leader's lastRaftLogIndex == idx0 and on every follower's RaftHAServer too
@Test void checksumsIgnoreTheProjection()             // checkDatabasesAreIdentical() passes although columnar/ differs per node (generations differ after the forced rebuild)
```

- [ ] **Step 2: Run** - `mvn -o -pl ha-raft -am verify -DskipITs=false -Dit.test='Columnar*IT' -Dfailsafe.failIfNoSpecifiedTests=false -Dtest=ColumnarSnapshotExclusionTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS. Before believing a wall of red, `lsof -nP -iTCP:2480-2489 -sTCP:LISTEN`.
- [ ] **Step 3: Stop: changes ready for review** - the six ha-raft test files and any production fix.

---

### Task 24: Dictionary compaction (spec 5.4)

> Execution note (Task 10 review): every manifest writer holds `applyLock`, takes its generation from `ColumnarProjection.nextGeneration()` (which honors the generation floor after an uncertain publish), and uses the same phase-based failure handling as `ColumnarFlusher` (delete only before the manifest rename is attempted).


> Execution note (Task 8 review): the compaction's new dictionary generation is reserved via `reserveDictionaryGeneration`; replaced generations are closed by `ColumnarProjection.stateReleased` once no retained state references them.


> Execution notes (Task 6 review): the remap builds new overlay blocks the same way as the apply pass: seed from the current block (`RowBuffer.seed`), never mutate published blocks (R13).


**Files:**
- Create: `index/columnar/DictionaryCompactor.java`
- Modify: `ColumnarIndexManager.java` (trigger, `compactDictionaryNow`), `ColumnarProjection.java` (writer dictionary switch); NOT `ColumnarStats.java`: `dictionaryCompaction(type, property, reclaimed)` and its readers exist since Task 3 (R3)
- Test: `index/columnar/DictionaryCompactorTest.java`, `index/columnar/DictionaryCompactionCrashTest.java` (`@Tag("slow")`)

**Interfaces:**
- Consumes: Tasks 2 (`GlobalDictionary.create`, R10), 3 (`ColumnarStats.dictionaryCompaction`), 4 (`SegmentFile.deleted()` via the last-release path, R1), 5, 6 (`OverlayPageBlock.build`), 8, 9 (`COLUMNAR_DICTIONARY_COMPACT_RATIO`), 10 (publish order), 15 (`ColumnarProjection.applyLock()`, the apply pass boundary, R13).
- Produces:

```java
final class DictionaryCompactor {
  static double deadOrdinalRatio(ColumnarProjection p, String property);   // 1 - referenced / size, referenced = ordinals of live rows in every segment and overlay view
  static boolean shouldCompact(ColumnarProjection p, String property, float ratio); // size >= 1024 and deadOrdinalRatio >= ratio
  static void compact(ColumnarProjection p, String property) throws IOException;   // maintenance pool
}
// ColumnarIndexManager, public test hook used by plan 4 (CrossBucketOrdinalMapTest) and plan 5:
public boolean compactDictionaryNow(int bucketId, String property) throws InterruptedException; // runs compact on the maintenance pool and waits; false when below 1024 entries or nothing reclaimable
```

Algorithm:
1. Referenced set: a `long[]` bitset over ordinals (`size / 64` words), filled by `forEachOrdinal(live(), ...)` on every segment of a retained state, plus the overlay views; no per-ordinal object.
2. New dictionary generation `n + 1`: `GlobalDictionary.create(dict-<indexFileId>.<n+1>.dic)` (R10), surviving values appended in old ordinal order (so ordinals stay insert-ordered and dense), `int[] remap` old to new (-1 dropped); fsynced before publish.
3. Rewrite every segment holding the column: a new segment per old one, same positions, page versions and other columns, the ORDINAL / ORDINAL_LIST sub-columns remapped (list ordinals re-sorted after remap). Segments are written outside any lock; queries keep running on the old state.
4. Publish, holding `projection.applyLock()` (Task 15, R13: the pass boundary, so no pass is staging ordinals of the old generation) and then `publishLock`: ordinals the apply thread added to generation `n` since step 1 are appended to generation `n + 1` and to `remap`; every live-overlay page holding the column (no frozen overlay exists, see Trigger, V-B6) is rebuilt as a NEW `OverlayPageBlock` with remapped ordinals (a copy, bounded by `COLUMNAR_FLUSH_MAX_ROWS`; published blocks are never mutated, because a snapshot acquired before the swap still decodes them with generation `n`), and the overlay's page entries are replaced by the new blocks; one manifest swap names the new segments and `DictionaryEntry(generation n + 1)` (Task 5 order, kill points `"compact.dictionaryWritten"`, `"compact.segmentsWritten"`, then the `publish.*` points). The apply thread's writer dictionary switches to `n + 1` in the same critical section.
5. Readers that acquired before the swap keep the old segments and generation `n` through `ColumnarSnapshot.dictionary(property)` until they close; Task 6's ref count deletes the old files after the last release. A query never waits on compaction (only the apply pass does, for step 4).

Trigger: after each merge that dropped rows, at most once per 10 minutes per projection and property, the manager submits `shouldCompact` to `ColumnarMaintenancePool`; never concurrently with a rebuild, flush or merge of the same projection: compaction, flush and merge share the per-projection maintenance flag of Task 9 (at most one of flush, merge, compaction per projection), and step 4 additionally requires that no frozen overlay exists (V-B6). Counters `dictionary.compactions` and `dictionary.ordinals.reclaimed` through `stats.dictionaryCompaction(type, property, reclaimed)` (Task 3).

- [ ] **Step 1: Write the failing tests**

```java
// DictionaryCompactorTest (TestHelper)
@Test void deadRatioCountsOnlyLiveReferences()        // R30: 2_000 brands (size >= 1024, so shouldCompact can be true), delete every record of 1_200 of them, merge -> deadOrdinalRatio == 0.6 (+- 0 exact)
@Test void compactionDropsUnreferencedOrdinals()      // same data, ratio 0.5 reached -> compact: dictionary(brand).size() == 800, stats.dictionaryOrdinalsReclaimed(type, "brand") == 1_200, dictionaryCompactions == 1
@Test void equivalenceAfterCompaction()               // FacetOracle.assertEquivalent for brand and tags (BY ITEM) after compaction, then after 2_000 more mutate() ops
@Test void readersKeepTheOldPairUntilReleased()       // acquire s1; compact; acquire s2: s1.dictionary(brand) != s2.dictionary(brand); counting through s1 still answers the pre-compaction values; old dict file exists until s1.close()
@Test void overlayBlocksAreCopiedNotRemappedInPlace()  // R13: overlay holds unflushed rows; acquire s1; compact; s1's overlay view still decodes its ordinals with generation n to the right values (the block instances s1 holds are unchanged)
@Test void queriesDoNotBlockDuringCompaction()        // KillPoint arm "compact.segmentsWritten" (blocks the compactor) -> acquire() and a full count complete on the test thread; disarm
@Test void ordinalsAddedDuringCompactionSurvive()     // block at "compact.segmentsWritten", insert 50 new brands, release -> all 50 present after publish, oracle equal
@Test void compactionNeverOverlapsAFlush()           // V-B6: arm flush.segmentWritten, request compaction: it waits until the flush published, then the oracle is equal
@Test void belowThresholdDoesNothing()                // ratio 0.3 with COLUMNAR_DICTIONARY_COMPACT_RATIO 0.5 -> shouldCompact false
@Test void keyDefault()                               // GlobalConfiguration.COLUMNAR_DICTIONARY_COMPACT_RATIO default 0.5
@Test void compactDictionaryNowIsSynchronous()        // R30 data (2_000 brands, 1_200 deleted): compactDictionaryNow(bucketId, "brand") true; the next acquire() sees generation n + 1; with 1_000 brands it returns false (below 1024 entries)
// DictionaryCompactionCrashTest
@ParameterizedTest @ValueSource(strings = {"compact.dictionaryWritten", "compact.segmentsWritten", "publish.manifestTmpWritten", "publish.manifestRenamed"})
void killMidCompactionRecoversWithoutRebuild(String point) // kill (Task 21 mechanics) -> reopen: one generation named by the manifest, orphan generation files deleted, rebuilds total unchanged, oracle equal
```

- [ ] **Step 2: Run to verify they fail** - `mvn -o -pl engine -am test -Dtest='DictionaryCompactorTest,DictionaryCompactionCrashTest' -DexcludedGroups=benchmark -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement `DictionaryCompactor`** and the trigger.
- [ ] **Step 4: Run to verify they pass** (same command), plus `-Dtest='ColumnarEquivalenceTest,ColumnarCrashRecoveryTest'` with the same flags. Expected: PASS.
- [ ] **Step 5: Stop: changes ready for review** - `DictionaryCompactor.java`, `ColumnarIndexManager.java`, `ColumnarProjection.java`, both tests.

---

### Task 25: Gate verification

- [ ] **Step 1: Engine columnar suite** - `mvn -o -pl engine -am test -Dtest='com.arcadedb.index.columnar.*Test,LocalBucketCommittedPageVisitorTest,GraphOlapVectorOpsFilteredTest,*GraphOlapVectorOps*Test' -DexcludedGroups=benchmark -Dsurefire.failIfNoSpecifiedTests=false` (P80: Task 1's SIMD test lives in `graph.olap.simd`, outside the columnar package). Expected: PASS.
- [ ] **Step 2: Touched engine areas** - `mvn -o -pl engine -am test -Dtest='ACIDTransactionTest,TransactionManager*Test,*Recovery*Test,*WALRotation*Test,Issue8626*Test,Issue7126*Test,LocalSchema*Test,DatabaseChecker*Test,*SchemaReload*Test,CreateIndex*Test,*IndexBuilder*Test,Issue*TypeIndex*Test,IndexKeyEqualityTest,*ByItem*Test,LocalBucket*Test,*Placeholder*Test,*Chunk*Test,*GraphBatch*Test,*Cypher*Index*Test,SelectStatementExecutionTest,PartitionPruningPlannerTest' -DexcludedGroups=benchmark -Dsurefire.failIfNoSpecifiedTests=false` (every pattern matches at least one class; the old `TypeIndex*Test`, `DocumentIndexer*Test`, `SelectExecutionPlanner*Test` matched none and ran nothing). Expected: PASS.
- [ ] **Step 3: Server and HA** - `mvn -o -pl server,ha-raft,integration -am verify -DskipITs=false -Dit.test='Columnar*IT' -Dfailsafe.failIfNoSpecifiedTests=false -Dtest='Columnar*Test,PoolMetrics*Test' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS (roadmap gate: oracle green at lag 0, ha-raft ITs green).
- [ ] **Step 4: Hygiene** - `grep -rnP "System\.out|\x{2014}" engine/src/main/java/com/arcadedb/index/columnar server/src/main/java/com/arcadedb/server/monitor/ColumnarMetrics.java` returns nothing; no `ForkJoinPool.commonPool` in the new package.
- [ ] **Step 5: Stop: changes ready for review** - full list of files from Tasks 1-24 (including `CypherSchemaCommands.java`, `GraphBatch.java`, `LSMVectorIndex.java`, `BucketPageLayoutTestSupport.java`, `LSMVectorIndexTestAccess.java`, `ColumnarTransactionManagerTestAccess.java`).

---

## Self-review notes

- Spec coverage: 5.1 (Task 11), 5.2 (Tasks 4, 6, 8, 10), 5.3 (Tasks 3, 4), 5.4 (Task 2; compaction Task 24), 5.5 (Task 5), 5.6 (Tasks 1 SIMD kernels via `GraphOlapVectorOps`, 9, 16), 5.7 heap bounds (Tasks 2, 4, 6 structure), 6.1-6.3 (Tasks 7, 13, 14, 15), 6.4 (Task 17), 6.5 (Tasks 13, 16, 21), 6.6 (Tasks 22, 23), 9 engine-side meters incl. re-read amplification, durations, dead ratio, cache hit rate, open files (Task 18), 10 unit/integration/crash/HA and ingest-overhead benchmark (Tasks 1-24). Cross-bucket ordinal merging is plan 4's `CrossBucketOrdinalMap`. Security, memory budget, calendar and parser-keyword tests beyond `COLUMNAR` belong to plan 4.
