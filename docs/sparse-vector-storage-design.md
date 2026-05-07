# Sparse Vector Storage - Design

**Status:** shipped on `main` via [#4078](https://github.com/ArcadeData/arcadedb/pull/4078) on 2026-05-05 (commit `c75f93268`). Wire-protocol coverage extended in [#4079](https://github.com/ArcadeData/arcadedb/pull/4079) (Bolt) on 2026-05-05. Partition pruning added in [#4119](https://github.com/ArcadeData/arcadedb/pull/4119) on 2026-05-06.
**Date:** 2026-05-06 (last updated)
**Issue:** [#4068](https://github.com/ArcadeData/arcadedb/issues/4068) - **closed**
**Follow-up:** [#4085](https://github.com/ArcadeData/arcadedb/issues/4085) - per-segment parallel top-K scoring (deferred Step 5)
**Branch (historical):** `sparse-vector-v2`. The branch name is historical; this design is the only and definitive version of `LSM_SPARSE_VECTOR` storage. The earlier composite-key prototype was treated as an MVP and was deleted in the same PR that landed this design.

---

## Progress

Tracked here so this file is the single source of truth on the branch. Updated as PRs land.

| Phase | Scope | Status | Tests | Notes |
|---|---|---|---|---|
| Phase 1 | New on-disk segment format (writer + reader, no integration) | done (local) | 27 unit + 1 `@slow` | 10M postings in 326 ms, 3.41 bytes/posting; sample read 1M postings in 46 ms |
| Phase 2 | BMW DAAT cursor + per-segment scoring (serial) | done (local) | 7 BMW + 2 `@benchmark` | 100k corpus 1 ms vs BF 7 ms (7x); 1M 4 ms vs 6 ms (1.5x); per-segment parallelism deferred to a 2.1 follow-up |
| Phase 3 | Memtable + flush + compaction + engine orchestrator | done (local) | 28 unit | put/remove/topK/flush/compact/reopen end-to-end; `SourceCursor` abstraction merges memtable + sealed segments; concurrency smoke test passes. |
| Phase 4 | Wire `LSM_SPARSE_VECTOR` to the new engine + delete MVP | done (local) | 235 index tests pass (62 new + 173 prior) | 945-line MVP composite-key wrapper rewritten to 758 lines; data path now goes through `SparseVectorEngine`; LSM-Tree shell retained as IndexInternal scaffolding only (no postings); `flush()` hook integrates with `LocalDatabase.closeInternal` for clean-shutdown durability. |
| Phase 4.1 | Per-transaction durability (flush-on-commit) | done (local) | 264 sparse-vector tests pass | `addAfterCommitCallbackIfAbsent` keyed by index name registers exactly one engine flush per committed transaction; the callback fires synchronously after `commit2ndPhase` so durability matches ArcadeDB's `asyncFlush=true` page commits (microsecond-scale window between the transaction record landing in the WAL and the segment file being fsynced). |
| Phase 5 step 1 | New page-component-backed segment format | done (local) | 5 new + 264 legacy (269 total) | `SparseSegmentComponent extends PaginatedComponent` (one component per segment file, reuses ArcadeDB's page cache + page WAL + HA replication + backup pipeline). `SparseSegmentBuilder` allocates pages inside a transaction; the transaction's commit-2nd-phase records every page in the regular WAL with no sparse-vector-specific code. `PaginatedSegmentReader` + `PaginatedSegmentDimCursor` consume blocks via `BasePage.readByteArray` so cold reads pay one page-load and hot reads stay memory-resident. `ComponentFactory` handler + `LocalDatabase.SUPPORTED_FILE_EXT` entry registered for the `.sparseseg` extension so files reload at startup automatically. New `SparseSegmentComponentTest` exercises build / multi-dim / tombstones / multi-block-pages / forward seek / database-reopen against the real ArcadeDB transaction pipeline. The legacy FileChannel-based `SparseSegmentWriter` / `SparseSegmentReader` stay untouched on this commit so all 264 prior tests stay green. |
| Phase 5 step 2 | Switch `LSMSparseVectorIndex` to a `PaginatedSparseVectorEngine` | done (local) | 269 sparse-vector tests pass | New `PaginatedSparseVectorEngine` runs flush + compaction inside `database.transaction(...)`, owns segments as `SparseSegmentComponent` instances (FileManager-tracked, page-WAL-durable). `LSMSparseVectorIndex` swapped over; the post-commit flush callback from Phase 4.1 is gone (no longer needed - page WAL is the durability point). **Benchmark vs Phase 4.1**: load 4062 ms → 2559 ms (39k docs/sec, 62% faster), index query 27 ms → **8.9 ms** (3x faster, matches the pre-durability baseline), speedup vs brute force 24.6x → **74.8x**, MVP 1213 ms → **136x improvement on the index path**. The page cache turns hot block reads into in-memory hits; per-commit fsync is gone (page WAL handles durability inline). |
| Phase 5 steps 3 + 4 | Delete legacy code + tests | done (local) | 228 sparse-vector + benchmark green | Deleted 7 main-code files (`SparseSegmentWriter`, file-based `SparseSegmentReader` and `SegmentDimCursor`, file-based `DimMetadata`, legacy `SparseVectorEngine`, `FlushWorker`, `CompactionWorker`) and 9 legacy-only tests (Roundtrip, Seek, Large, Compaction, Flush, Engine, EngineConcurrency, BmwScorer, BmwScorerBenchmark). Added `BmwScorerCorrectnessTest` (4 tests) which exercises BMW vs brute-force agreement against the page-component path so we keep the scoring-correctness invariant. `BruteForceScorer` migrated to `PaginatedSegmentReader[]`. The `compareRid` helper moved from `SparseSegmentWriter` to `SparseSegmentBuilder` so all callers have a single canonical home. |
| Phase 5 step 5 | Large-corpus benchmark + auto-flush | done (local) | `LSMSparseVectorIndexLargeBenchmark` | Tests at 1M and 10M docs - 30 nnz/doc, 30k-dim vocab, 10 nnz query, K=10. First 10M attempt OOM'd at 6M docs because the engine had no auto-flush and held every posting in the memtable. Added a threshold-based auto-flush: `PaginatedSparseVectorEngine.maybeFlush()` is called from a per-transaction post-commit callback (`addAfterCommitCallbackIfAbsent` keyed by index name) and flushes only when the memtable has accumulated ≥ 1M postings. Memtable heap is bounded at ~100 MiB regardless of total insert volume. **Scaling table after the multi-page dim_index fix (Phase 5 step 8) and orphan-drop:** {100k → 8.21 ms vs 670 ms = **82x**}, {1M → 14.43 ms vs 4722 ms = **327x**}, {10M → 792 ms vs 51,155 ms = **65x**}. Index latency grows sub-linearly with corpus while brute force scales linearly. The earlier numbers in this row (1.03 ms / 4716x at 1M, 1.20 ms / 41613x at 10M) were measured before the dim_index page-overflow fix and the orphan-drop in `flush()`: every flush silently failed (dim_index > 65528 bytes for 30k-dim corpora), no segments existed, and the index path fell back to a single small memtable scan that happened to be very fast for ≤10 query dims. With real segments now persisted on disk and read via BMW DAAT cursors, the numbers above are the production-relevant ones. Multi-segment compaction (currently only manual via `compactAll`/`compactOldest`) is the next lever to push 10M back into the single-digit-ms range; tracked as a follow-up. |
| Phase 5 step 6 | Parallel scoring | per-bucket fan-out shipped via #4085; per-segment RID-range partitioning still open as a follow-up | `SparseVectorParallelFanoutTest` (2 tests) | **Per-bucket parallelism shipped.** `SQLFunctionVectorSparseNeighbors.executeWithIndexes` now submits each per-bucket `LSMSparseVectorIndex.topK(...)` call to the dedicated `SparseVectorScoringPool` when the type has more than one bucket; single-bucket types take the inline path so they don't pay scheduler overhead for nothing. Per-bucket sub-indexes are independent (different RIDs, no shared mutable state, no tombstone or cross-segment-dim coordination) so the dispatch is correctness-trivial. The `SPARSE_VECTOR_SCORING_POOL_THREADS` and `SPARSE_VECTOR_SCORING_QUEUE_SIZE` operator knobs landed in `GlobalConfiguration` alongside the wiring; auto-size to `max(2, cores)` and 1024 queue when `0`. **Per-segment within-index parallelism (RID-range partitioning) deferred** - the original design-doc plan for #4085 is correct but requires partition-aware cursors and per-partition top-K merge, while the absolute gain is small relative to network + serialization noise the serial 10M benchmark already lands inside of. Tracked as a future enhancement if profiling shows single-index queries (single-bucket types or pre-narrowed by partition pruning) are a bottleneck. |
| Phase 5 step 7 | HA replication smoke test | done (local) | `RaftSparseVectorReplicationIT` (3-server, every server must serve top-K, 5/5 stable runs); `RaftIndexCompactionReplicationIT.lsmVectorReplication` + `lsmVectorCompactionReplication` re-enabled and green | Wired the engine's flush + compaction through `DatabaseInternal.runWithCompactionReplication(...)` (the same hook `LSMTreeIndex` uses for compacted-file replication) for both `LSM_SPARSE_VECTOR` (`PaginatedSparseVectorEngine`) and `LSM_VECTOR` (`LSMVectorIndex.compact`). Three engine-side fixes were needed for sparse: (1) call `database.getWrappedDatabaseInstance().runWithCompactionReplication(...)` so the call reaches the Raft override instead of the no-op default; (2) `database.getPageManager().waitAllPagesOfDatabaseAreFlushed(database)` after the inner transaction commits so the synthetic WAL `serializeFilePagesAsWal` reads in the same recording session sees on-disk pages instead of zeros from the async page-cache writer; (3) a query-time `refreshSegmentsFromFileManager()` lifts replicated components from the FileManager into the engine's in-memory `segments` snapshot (followers don't go through `appendSegment`). The `LSM_VECTOR` fix is the same shape: wrap `LSMVectorIndexCompactor.compact(this)` in `runWithCompactionReplication` + drain. Read-side robustness: refresh skips a sparseseg file that fails header validation - common while a freshly-arrived component is briefly empty between createNewFiles and the WAL apply on the follower - so transient races can't crash a query. Build-side robustness: the flush callback drops its just-created `SparseSegmentComponent` if the build throws, so a partial file cannot survive the recording session for the next refresh scan to trip over. |
| Phase 5 step 8 | Multi-page `dim_index` | done (local) | sparse + HA tests; 1M / 10M benchmarks now build real segments | The original `SparseSegmentBuilder.writeDimIndex` capped the dim_index at one page (~6552 entries with the default 64 KiB page). Any segment with a wider vocabulary failed `b.finish()`; the `flush()` orphan-drop above kept that silent at the engine level but meant a high-vocab corpus (e.g. 30k-dim SPLADE) silently fell back to memtable-only retrieval and inflated the original "{1M → 1.03 ms / 4716x}" numbers. The builder now packs `dim_index` entries densely across as many contiguous pages as needed, with the count header on page 0 only; the reader walks page-by-page once `p + DIM_INDEX_ENTRY_SIZE` would overflow the current page. No on-disk format change is required because the per-entry layout is unchanged and the page count is derivable from the segment's `total_dims`. |
| Phase 5 step 9 | Auto-compaction gate at flush time | done (local) | `LSMSparseVectorIndexLifecycleTest.flushTriggersSizeTieredCompaction` + 1M / 10M benchmarks rerun | Without an auto-compaction trigger, a long bulk-load left the engine with one segment per 1M-posting flush (≈30 segments at 10M, scaling linearly with corpus). Every BMW DAAT cursor merged across all of them, dragging 10M query latency to 792 ms / 65x speedup. First cut was a count-tiered gate ("after every flush, if segments &gt;= 8, merge oldest 4"); then promoted to **size-tiered (STCS)**: tier 0 holds segments with `<= DEFAULT_TIER_BASE_POSTINGS` (1M), each subsequent tier `i` holds segments with postings in `[base * fanout^i, base * fanout^(i+1))` where `fanout = DEFAULT_TIER_FANOUT = 4`. After every flush the engine cascades `compactSizeTiered()`: groups segments by tier and merges the oldest `fanout` from any tier that overflows; the merged segment naturally promotes to the next tier by virtue of its larger posting count. Steady-state segment count is bounded at `(fanout - 1) * log_fanout(N / base)`. **Why size-tiered over count-tiered**: count-tiered kept rewriting the same large compacted segment over and over - write amplification scaled with corpus. Size-tiered amortizes write amplification at `O(log_fanout(N/base))` per posting. **Benchmark numbers (size-tiered baseline)**: 100k → 8.21 ms vs 670 ms = **82x**; 1M → **10.09 ms** vs 4885 ms = **484x**; 10M → **59.01 ms** vs 48,897 ms = **829x**. **10M bulk-load**: count-tiered 764 s → size-tiered **503 s (-34%)** for the same query latency. HA replication keeps working unchanged because `compactInputs` already shipped its segment-set swap through `runWithCompactionReplication`. **2026-05-05 rerun (post `payloadScratch` sizing fix + day's reliability hardening)**: 10M → **62.98 ms vs 48,299 ms = 766.9x**, bulk-load **465.3 s**. The previously-quoted "1.20 ms / 41,613x" headline at 10M was never real - it was measured before the `payloadScratch` was correctly sized for the worst-case dim_index packing, so high-vocab (30k-dim) flushes overflowed the buffer, the orphan-drop in `flush()` swallowed every flush silently, and the index path fell back to a small in-memory memtable scan that happened to be very fast for ≤10 query dims. With the buffer sized at `max(estimateBlockPayloadSize, pageContentSize)` real segments now persist on disk and the BMW DAAT cursors actually do work. The post-fix number is the production-relevant one and matches the size-tiered baseline within ~7%. |
| Validation | End-to-end benchmark on 100k corpus | done (local) | `LSMSparseVectorIndexBenchmark` | After flush-on-commit: index 27 ms/query, brute force 666 ms/query, **24.6x speedup**; load 100k docs at 25k docs/sec. Without flush-on-commit (memtable-only baseline) the index path was 9 ms/query - the slowdown is the cost of int8 segment decompression per query, which becomes the dominant overhead at small corpus size. The MVP's same benchmark was 1213 ms WAND vs 730 ms brute force, so the new backend is still ~45x faster on the index path with full durability. |
| Reliability hardening (post-PR review) | Defensive fixes from code-review rounds | done (local) | All 47 sparse-vector unit tests + HA replication green | (1) `payloadScratch` resized to `max(estimateBlockPayloadSize, pageContentSize)` so the dim_index packer never overflows for high-vocab corpora. (2) `writeDimTrailer` reuses the per-builder `payloadBuf` instead of `ByteBuffer.allocate` per dim, dropping ~30k allocations per flush at SPLADE-scale. (3) `seekTo` in `PaginatedSegmentDimCursor` uses the per-dim skip list (binary search + ≤ stride linear scan) instead of an O(blocks) walk. (4) `computeFileFingerprint` now uses an order-invariant 64-bit hash (splitmix64-mixed file-id sum) instead of plain integer addition, plus a `FileManager.modificationCount` O(1) fast path so a steady-state `topK` no longer pays an O(total registered files) walk per query. (5) `BlockHeader`, `SkipEntry`, `MemtablePosting` are records. (6) Component-leak guard around `compactInputs` (drain → publish region) so a throw between build success and `replaceSegments` cannot leak a registered component. (7) `SparseSegmentBuilder.close()` throws on incomplete builds (`addSuppressed`-safe under try-with-resources) so the bug surfaces at the build site instead of as a CRC mismatch much later. (8) Bounds checks on `postingCount` / `df` in `loadDimMetadata`. (9) `BruteForceScorer` moved to `src/test/java` (test-only). (10) `SPARSE_VECTOR_SCORING_*` config knobs removed pending #4085 dispatch wiring; pool uses hardcoded defaults. (11) `LSMSparseVectorIndex.get(keys)` now throws `UnsupportedOperationException` instead of silently returning empty (the shell never receives postings). (12) `Memtable.heapBytesEstimate` removed (unused in flush trigger; `totalPostings()` is the only signal). (13) Follower-skip log raised to FINE so operators troubleshooting "follower missing data" have something to grep. |
| Wire-protocol coverage | Bolt driver support for `LSM_SPARSE_VECTOR` | shipped via [#4079](https://github.com/ArcadeData/arcadedb/pull/4079) on 2026-05-05 | covered by existing Bolt tests | `BoltNetworkExecutor`'s type recognition extended so Neo4j-driver clients can read sparse-vector index types alongside dense `LSM_VECTOR`. No format changes; pure protocol surface. |
| Partition pruning | Narrow per-bucket fan-out by partition key | shipped via [#4119](https://github.com/ArcadeData/arcadedb/pull/4119) on 2026-05-06 | `PartitionPruningVectorSparseNeighborsTest` (+ dense-vector twin) | `SelectExecutionPlanner.derivePartitionPrunedClusters` now stashes a partition-pruned bucket set on `CommandContext`; `SQLFunctionVectorAbstract.narrowAllowedBucketIdsByPartitionHint` consumes it before invoking the per-bucket sparse-vector sub-index walk. A query like `SELECT vector.sparseNeighbors('Doc[embedding]', :q, k) FROM Doc WHERE tenant_id = 'X'` on a `partitioned('tenant_id')` type now hits exactly one bucket's segment set instead of fanning out across every bucket. Without a partition-key WHERE, behavior is unchanged (full fan-out). Issue [#4087](https://github.com/ArcadeData/arcadedb/issues/4087) closed. |

Open decisions from the "Open questions" section near the bottom of this file are tracked inline; resolutions get folded into the relevant section as they're settled.

---

## Goals

Build a purpose-built storage component for `LSM_SPARSE_VECTOR` that scales to 100M-1B+ sparse postings and matches the throughput of Lucene / Tantivy / Qdrant `SPARSE_WAND` in the same regime. Specifically:

- **10-50x retrieval speedup** vs the current `LSM_SPARSE_VECTOR` MVP at corpora past 10M sparse vectors. The 100k benchmark in `LSMSparseVectorIndexBenchmark` previously embarrassed the MVP at 1213 ms WAND vs 730 ms brute force; the new backend lands at 9 ms WAND vs 732 ms brute force on the same workload (81x speedup, ~135x improvement vs MVP), and the design scales to 100M-1B.
- **5-10x storage reduction** vs the current composite-key-on-LSMTreeIndex layout via VarInt-delta RID compression and int8 weight quantization.
- **O(1) first-query latency post-reopen** via persistent on-disk block-max metadata. Today the wrapper rebuilds `dimMaxWeight` by scanning the entire index on the first WAND query.
- **Linear core scaling** via per-segment parallel scoring.
- **Full ACID + WAL + HA + backup** integration, no special cases.

The current `LSM_SPARSE_VECTOR` storage (delivered by PR [#4070](https://github.com/ArcadeData/arcadedb/pull/4070), merged 2026-05-04) is treated as a throwaway prototype. The PR shipped the public API (SQL, Java builder, Studio dialog, `vector.sparseNeighbors`, `vector.fuse`, `groupBy`) but the on-disk format has not been part of any release tag, so this design ships with no migration path: existing data on the MVP format is recreated from source. The user-facing index name `LSM_SPARSE_VECTOR` stays; only the storage backend is replaced.

## Non-goals

- Persisted approximate posting-list summaries beyond block-max (e.g. learned-cluster compression, ColBERT-style late interaction).
- IDF rebalancing across segments. IDF is computed against per-segment `df` summed at query time; no global IDF index.
- Cross-collection / cross-type sparse search. Single type, single index. (Within-type partition pruning *is* supported as of [#4119](https://github.com/ArcadeData/arcadedb/pull/4119) - see the Partition pruning row in the Progress table.)
- GPU-accelerated build or scoring.
- BlockMax-MaxScore in addition to BlockMax-WAND. Single algorithm for v1.

---

## Why a new storage component

The current MVP layers on top of `LSMTreeIndex` with composite key `(int dim_id, RID rid, float weight)`. That works for correctness but is structurally wrong for a sparse-retrieval storage layer:

1. **Per-block max_weight cannot be stored** without modifying `LSMTreeIndex`'s page header, which is shared across every other index type. Adding sparse-specific fields to the shared format is a hard no.
2. **VarInt-delta RID compression** and **int8 weight quantization** require contiguous, immutable, sparse-shaped blocks. `LSMTreeIndex`'s `(key -> RID)` entry layout cannot be compressed at the block level without breaking its own search algorithm.
3. **Skip lists per posting list** make no sense at the `LSMTreeIndex` layer because `LSMTreeIndex` does not have the concept of "all entries for one dim form a posting list" - to it, each `(dim, rid, weight)` tuple is just a key.
4. **N-way merge during compaction** must produce sparse-shaped output (per-dim posting lists with new block-max values, not just a sorted list of entries), so the compactor itself must be sparse-aware.
5. **Reading a block header without decompressing** the block payload is the BMW skip primitive. `LSMTreeIndex`'s page format does not separate block-level metadata from entry payloads in a way that lets us skip without reading.

The new component reuses ArcadeDB's `PaginatedComponent` infrastructure (page allocator, mmap, page cache, fsync, WAL hookup) but defines its own page payload semantics. Same pattern as `LSMVectorIndex` (which does not extend `LSMTreeIndex` either, instead going through `PaginatedComponent` directly with its own JVector-backed pages).

---

## Architecture overview

```
┌─────────────────────────────────────────────────────────────────────┐
│  LSMSparseVectorIndex (wrapper, IndexInternal)                      │
│  - schema, public put/remove/topK API                               │
│  - holds the engine below                                           │
└────────────────────────┬────────────────────────────────────────────┘
                         │
            ┌────────────┴─────────────────────────────────┐
            │                                              │
            ▼                                              ▼
┌─────────────────────────┐              ┌──────────────────────────────┐
│  Memtable (mutable)     │              │  Segment Set (immutable)     │
│  - Map<dim, SortedMap   │              │  ┌────────────┐              │
│        <RID, weight>>   │              │  │ segment 0  │ (.sparseseg) │
│  - tombstones included  │              │  │ segment 1  │              │
│  - flushed on size      │              │  │ ...        │              │
│  - WAL-backed inserts   │              │  │ segment N  │              │
└─────────────────────────┘              │  └────────────┘              │
                                         └──────────────────────────────┘
                         │                              │
                         └──────────────┬───────────────┘
                                        ▼
                         ┌─────────────────────────────┐
                         │  WAND/BMW DAAT cursor       │
                         │  - per-dim cursors per src  │
                         │  - merged across memtable   │
                         │    and all segments         │
                         │  - block-max + skip list    │
                         │    drive pivot decisions    │
                         └─────────────────────────────┘

Background compaction worker:
  picks K small segments → N-way merge over (dim, RID) →
  recompute block-max, regenerate skip list, recompress →
  atomically swap in the merged segment, drop the inputs.
```

Lifecycle:

```
write path:
  put(dim, rid, weight)
    -> append (dim, rid, weight) to WAL
    -> insert into memtable: dimMap.computeIfAbsent(dim).put(rid, weight)
    -> on memtable threshold (size or entry count):
       -> serialize memtable as a new sealed segment file
       -> add segment to active set
       -> truncate WAL up to flushed position
       -> reset memtable

delete path:
  remove(dim, rid)
    -> append tombstone to WAL
    -> insert (dim, rid, TOMBSTONE) into memtable
    -> compaction physically drops the (dim, rid) pair when it merges
       past the tombstone in older segments

read path:
  topK(query, k):
    cursors = []
    for each dim d in query:
      cursors.add(MergedDimCursor(memtable, segments, d))
    run BMW DAAT loop over cursors (see Read Path section)

compaction:
  background thread picks K small segments,
  N-way-merges into one larger segment,
  recomputes block-max + skip lists + recompresses,
  atomically swaps in the merged segment, drops the inputs,
  fsyncs and updates the manifest.
```

---

## On-disk file format

One file per sealed segment. File extension: `.sparseseg`. Page-aligned (page size from `GlobalConfiguration.SPARSE_VECTOR_PAGE_SIZE`, default 64 KiB).

### File layout

```
+-------------------------------------------------------+
| File header (page 0)                                  |
|   magic (8B): "ASPV0001"                              |
|   format_version (4B): 1                              |
|   page_size (4B)                                      |
|   block_size (4B): postings per block (default 128)   |
|   skip_stride (4B): blocks per skip entry (default 8) |
|   weight_quantization (1B): 0=fp32, 1=int8, 2=fp16    |
|   rid_compression (1B): 0=raw, 1=varint-delta         |
|   reserved (14B)                                      |
|   manifest_offset (8B): file offset to manifest       |
|   total_postings (8B)                                 |
|   total_dims (4B)                                     |
|   created_at (8B): epoch millis                       |
|   crc32 (4B): of header excluding crc                 |
+-------------------------------------------------------+
| Posting list region                                   |
|   per-dim posting list, packed contiguously:          |
|     +-----------------------------+                   |
|     | dim_header                  |                   |
|     |   dim_id (4B)               |                   |
|     |   block_count (4B)          |                   |
|     |   posting_count (4B)        |                   |
|     |   df (4B): same as count    |                   |
|     |   global_max_weight (4B)    |                   |
|     | block_offsets (block_count  |                   |
|     |   * 8B): file offset of     |                   |
|     |   each block                |                   |
|     | skip_list (~block_count /   |                   |
|     |   skip_stride entries):     |                   |
|     |   { first_RID (12B),        |                   |
|     |     max_weight_to_end (4B), |                   |
|     |     block_index (4B) }      |                   |
|     +-----------------------------+                   |
|     [ block 0 ] [ block 1 ] ...                       |
+-------------------------------------------------------+
| Manifest (last page block)                            |
|   dim_index_offset (8B): start of per-dim list of     |
|     (dim_id, dim_header_offset) pairs sorted by dim   |
|   segment_id (8B): unique sequence number, monotonic  |
|   parent_segments (variable): segment_ids merged      |
|     into this one (empty for memtable flush)          |
|   tombstone_floor_segment (8B): tombstones older      |
|     than this segment can be dropped during           |
|     compaction (dropped tombstone watermark)          |
|   reserved + crc32                                    |
+-------------------------------------------------------+
```

### Block format

Each block holds up to `block_size` postings (default 128) for one dim, sorted by RID ascending.

```
+--------------------------------------------------+
| block_header                                     |
|   first_RID (12B)                                |
|   last_RID (12B)                                 |
|   posting_count (2B): 1..block_size              |
|   max_weight (4B): the BMW upper bound           |
|   weight_min (4B): for int8 dynamic range        |
|   weight_max (4B): for int8 dynamic range        |
|   has_tombstones (1B)                            |
|   reserved (1B)                                  |
| compressed_rids (variable):                      |
|   first_RID stored raw (12B already in header)   |
|   subsequent RIDs as VarInt deltas               |
| compressed_weights (variable):                   |
|   if int8: 1B per posting, decoded as            |
|     weight_min + (b/255) * (weight_max-weight_min)|
|   if fp16: 2B per posting                        |
|   if fp32: 4B per posting                        |
|   sentinel value 0x80 (int8) / 0xFE00 (fp16) /   |
|     NaN (fp32) marks a tombstone                 |
+--------------------------------------------------+
```

A block never spans across pages: we round up to the next page boundary so each block is contiguously addressable. Blocks are typically much smaller than a page; multiple blocks pack into one page until full.

### Skip list

For dim `d` with `B` blocks, the skip list holds `ceil(B / skip_stride)` entries (default `skip_stride=8`):

```
{ first_RID:        12B   // first RID of block (i * skip_stride)
  max_weight_to_end: 4B   // max(block_max_weight) over blocks
                          // [i*skip_stride, B)
  block_index:       4B }  // i * skip_stride
```

Stored sorted by `first_RID` ascending. Binary search finds the entry covering a target RID. The `max_weight_to_end` lets a cursor compute "what is the upper bound on this dim's contribution to any posting at RID >= current?" in O(log B).

### Compression details

**RID compression - VarInt delta:**
- First RID stored raw (12 bytes).
- Each subsequent RID stored as a VarInt-encoded delta from its predecessor.
- ArcadeDB RIDs are `(short bucketId, long position)` (12 bytes raw). Within a posting list for one dim, RIDs are typically clustered by bucket; the delta is small so VarInt averages 2-4 bytes per RID. Expected compression: ~4-6x on RIDs.

**Weight quantization - int8 with per-block dynamic range:**
- Each block records `weight_min` and `weight_max` in its header.
- Each posting's weight stored as 1 byte: `b = round(255 * (weight - weight_min) / (weight_max - weight_min))`.
- Decode: `weight = weight_min + (b / 255.0f) * (weight_max - weight_min)`.
- Maximum quantization error per block: `(weight_max - weight_min) / 510`. Negligible for SPLADE/BM25-style weights, which typically span a narrow range within any given block.
- Optional fp16 mode (configurable per-index) for callers that need full half-precision. fp32 mode preserves the input exactly.
- Sentinel byte `0x80` reserved for tombstone marker (see deletes).

**Net storage** for a sparse vector with 100 nnz, average bucket-clustered RIDs, fp32 source weights:
- Raw composite-key MVP: 100 * (12 RID + 4 weight + ~12 entry overhead) = ~2800 bytes
- New format, int8: 12 (first_RID) + 99 * 3 (avg VarInt RID delta) + 100 * 1 (int8 weight) + ~32 (block header) = ~441 bytes
- ~6.3x reduction.

---

## In-memory structures

### Memtable

```java
final class Memtable {
  // Per-dim sorted by RID ascending. ConcurrentSkipListMap chosen for sorted
  // iteration during flush plus lock-free concurrent insert.
  final ConcurrentHashMap<Integer, ConcurrentSkipListMap<RID, Float>> postings;
  // Running per-dim max live weight, maintained incrementally on put for an
  // O(1) BMW upper bound at cursor construction time.
  final ConcurrentHashMap<Integer, Float> dimMaxWeight;
  final AtomicLong totalPostings;
  final AtomicLong tombstoneCount;
}
```

Flush trigger: `totalPostings >= flushThreshold` (default 1M postings). The
earlier draft also tracked an approximate `heapBytesEstimate` as an alternative
trigger but `totalPostings` ended up being the sole decision input, so the
estimate was dropped to keep the memtable surface narrow.

Tombstones live in the memtable as `(dim, rid)` entries with a sentinel `Float.intBitsToFloat(0xFFFFFFFF)` (or simpler: a parallel `Set<(dim, rid)>`). At flush time, tombstones become tombstone-marked postings in the new segment.

### Segment set

```java
final class SegmentSet {
  // Sorted by recency descending. Reads check newest first; deletes shadow
  // older entries.
  final List<SealedSegment> segments;
  final AtomicReference<long[]> activeSegmentIds;  // for atomic swap during compaction
}
```

A `SealedSegment` holds:
- the file handle (via `PaginatedComponent`)
- in-memory cached: dim_index (dim_id -> dim_header_offset), block_offsets per dim (loaded on demand), skip lists per dim (loaded on demand), per-dim global_max_weight (loaded eagerly at segment open since it's small)

### Cached per-index state

```java
// df[d] = total postings under dim d across all live segments + memtable, minus tombstones.
// Maintained incrementally on every put/remove and on every compaction.
final ConcurrentHashMap<Integer, AtomicLong> df;

// N = total distinct (segments-and-memtable) RIDs that have at least one non-tombstoned posting.
// Used as the IDF normalizer. Same maintenance as df.
final AtomicLong totalDocuments;

// Cached IDF values, invalidated on every put/remove. Lazy-recomputed on next IDF query.
final AtomicReference<Map<Integer, Float>> idfCache;
```

The `df` and `totalDocuments` counters are persisted alongside segment manifests so a reopen is O(segments) not O(postings).

---

## Read path: WAND DAAT with BlockMax-WAND

### Per-dim cursor

```java
final class DimCursor {
  final int dim;
  final float queryWeight;

  // One cursor per source: memtable (if dim present) + every sealed segment.
  // SourceCursor abstracts iteration over a single source.
  final SourceCursor[] sources;

  // Heap of source cursors keyed by their currentRID ascending.
  // The dim cursor's currentRID is sources[0].currentRID (head of heap).
  final PriorityQueue<SourceCursor> live;

  RID currentRid;
  float currentWeight;       // weight at currentRid in the latest source that defines it
  float upperBoundRemaining; // max possible weight contribution from currentRid to end of dim
}
```

Each `SourceCursor` wraps a `MemtableSourceCursor` (sorted iteration over `ConcurrentSkipListMap`) or a `SegmentSourceCursor` (block-by-block walk through a segment's posting list for one dim).

`upperBoundRemaining` is computed lazily: per source, the source's own block-level `max_weight_to_end` is known via skip-list lookup at `currentRID`. The dim cursor takes the max of those across sources. This is the BMW pivot bound.

When the merged DimCursor's `currentRID` advances, every source whose `currentRID == dimCursor.currentRID` advances (latest source's weight wins on conflict), and the upper bound is recomputed.

### BMW DAAT loop

```
heap = top-K min-heap (by score ascending)
threshold = -infinity

cursors = [DimCursor for each query dim, sorted by initial currentRID]

while at least one cursor is live:
  # Sort cursors by currentRID ascending. (Insertion sort, mostly already
  # sorted from previous iteration; same trick as the MVP.)
  sort_cursors_by_current_rid(cursors)

  # Compute pivot via prefix sum of upperBoundRemaining * queryWeight.
  prefix = 0
  pivot = -1
  for i in 0..len(cursors):
    prefix += cursors[i].queryWeight * cursors[i].upperBoundRemaining
    if prefix > threshold:
      pivot = i
      break

  if pivot == -1:
    break  # no remaining doc can beat threshold

  pivot_rid = cursors[pivot].currentRid

  if cursors[0].currentRid == pivot_rid:
    # Score the pivot doc.
    score = sum of cursors[i].queryWeight * cursors[i].currentWeight
            for i where cursors[i].currentRid == pivot_rid

    # Tombstone short-circuit: if any aligned cursor at pivot is a tombstone,
    # the doc is deleted and not scored.
    if not_tombstoned(cursors at pivot_rid):
      if heap.size < k:
        heap.push((pivot_rid, score))
        if heap.size == k: threshold = heap.peek().score
      elif score > threshold:
        heap.pop()
        heap.push((pivot_rid, score))
        threshold = heap.peek().score

    # Advance all cursors at pivot_rid.
    for each cursor with currentRid == pivot_rid:
      cursor.advance()  # within source, may trigger block-skip via skip list

  else:
    # Skip cursors[0..pivot-1] forward to pivot_rid via their per-dim skip lists.
    # Each cursor uses BMW: if its NEXT block's max_weight cannot push the doc
    # over threshold even when summed with all other cursors' upper bounds,
    # skip the entire block via the block_offsets array. Otherwise fall through
    # to a per-posting advance within the block.
    for i in 0..pivot:
      if cursors[i].currentRid < pivot_rid:
        cursors[i].seek_to_or_past(pivot_rid)

  # Compact: drop exhausted cursors.
  drop_exhausted(cursors)

return heap sorted by score descending
```

### Block-skip path (the BMW win)

`SourceCursor.advance()` for a `SegmentSourceCursor`:

1. If the next posting is in the current block, decompress the next RID + weight, advance.
2. If we've exhausted the current block, look at the NEXT block's header (a single page read, no decompression).
3. If `next_block.max_weight * cursor.queryWeight + sum_of_other_dims_upper_bounds < threshold`:
   - skip the block entirely via the skip list.
   - jump to the first block whose contribution could still beat threshold.
4. Otherwise, decompress the next block's compressed_rids + compressed_weights into a per-cursor working buffer, continue posting-at-a-time.

This is where the 10-50x speedup comes from. A selective query reads only block headers (a tiny fraction of the total file) for most blocks, and decompresses the payload of only the few blocks that matter.

### Per-segment parallelism

Top-level `topK`:

```
fork-join: for each (segment OR memtable):
  run a BMW DAAT scoring pass restricted to that source,
  produce a local top-K heap.

merge: combine all local heaps into one global heap of size K.
```

Per-segment scoring is lock-free (segments are immutable). Memtable scoring takes a read lock on the memtable. Linear core scaling up to memory-bandwidth bound.

For very small queries (k=10, few query dims), the per-segment cost is small and parallel dispatch overhead dominates. A query-time heuristic gates parallelism: serial below `parallel_threshold_postings = sum(estimated_postings_per_dim_per_segment)`, parallel above.

### IDF preprocessing

Under `modifier=IDF`:

```
for each query dim d:
  effective_weight[d] = query_weight[d] * idf(N, df[d])

run BMW DAAT with effective_weight as queryWeight per cursor.
```

`N` and `df` are cached at the index level, invalidated on put/remove, recomputed lazily. No per-query scan.

---

## Write path

```
put(dim_indices[], weights[], rid):
  for i in 0..len(dim_indices):
    dim = dim_indices[i]; w = weights[i]
    if w == 0: continue
    if w < 0 or NaN: throw IndexException

    # WAL: durable record of the change before we mutate in-memory state.
    wal.append(SparsePut(dim, rid, w))

    # Memtable insert. ConcurrentSkipListMap.put is atomic.
    memtable.postings
      .computeIfAbsent(dim, k -> new ConcurrentSkipListMap<>())
      .put(rid, w)

    # Per-dim incremental counters.
    df[dim].increment()
    totalDocuments.increment_if_new_rid(rid)
    idfCache.invalidate()

    # Trip flush check.
    if memtable.shouldFlush():
      schedule_flush()
```

Atomicity:
- WAL append is the durability point. Crash before WAL append: caller sees the error.
- Crash after WAL append, before memtable insert: WAL replay reapplies on startup.
- Crash after memtable insert, before flush: WAL replay reapplies; the duplicate insert into a freshly-empty memtable is idempotent.

### Flush

```
flush():
  # Snapshot the memtable atomically.
  snapshot = memtable.swap_with_empty()

  # Write a new sealed segment.
  segment_id = next_segment_id()
  builder = new SealedSegmentBuilder(segment_id, parent_segments=[])

  for each dim in snapshot, sorted by dim_id ascending:
    posting_list = snapshot[dim]  # SortedMap<RID, weight>

    # Group into blocks of block_size.
    for each block in chunked(posting_list, block_size):
      compute first_RID, last_RID, max_weight, weight_min, weight_max
      compress RIDs (varint delta)
      compress weights (int8 with per-block range, or fp16, or fp32)
      builder.write_block(...)

    # Skip list every skip_stride blocks: cumulative max_weight from this
    # block to end is precomputed.
    builder.write_skip_list(...)
    builder.write_dim_header(dim_id, ...)

  # Manifest with segment_id, parent_segments=[], tombstone_floor.
  builder.write_manifest()
  builder.fsync()

  # Atomic swap: append to active segments, truncate WAL up to flushed
  # position, rebuild idfCache lazily.
  segmentSet.append(builder.seal())
  wal.truncate_up_to(snapshot.last_wal_position)
```

The WAL is shared with the rest of the database, not per-index, so "truncate" really means "advance the per-index checkpoint within the WAL". Standard ArcadeDB pattern.

---

## Compaction

Background worker. Runs on its own thread; trigger conditions are size-tiered (default: `compact when N segments at level L exceed compaction_factor=4`).

```
compact(input_segments[]):
  # input_segments are sorted by segment_id ascending (oldest first).
  # Output gets a new segment_id and lists the inputs as parent_segments.

  builder = new SealedSegmentBuilder(next_segment_id(), parent_segments=input_segments)

  # N-way merge over (dim, RID).
  cursors = [SegmentSourceCursor(s) for s in input_segments]

  for each dim in union of cursors' dims, sorted ascending:
    dim_cursors = [c for c in cursors where c has postings under dim]

    # N-way RID merge for this dim.
    while any of dim_cursors has more postings:
      smallest_rid = min(c.currentRid for c in active dim_cursors)
      candidates = [c for c in dim_cursors if c.currentRid == smallest_rid]

      # Latest source wins. Sources are sorted oldest -> newest, so newest
      # source's weight wins on conflict. Tombstone wins over everything.
      winner = candidates[-1]   # newest

      if winner.weight is TOMBSTONE:
        # Drop entirely if all input segments older than tombstone_floor.
        # Otherwise emit the tombstone.
        if all(c.segment_id <= tombstone_floor for c in candidates):
          continue  # physically drop
        else:
          builder.append_tombstone(dim, smallest_rid)
      else:
        builder.append_posting(dim, smallest_rid, winner.weight)

      # Advance every cursor that was at smallest_rid.
      for c in candidates: c.advance()

    builder.finalize_dim(dim)  # writes block headers, skip list

  builder.write_manifest()
  builder.fsync()

  # Atomic swap. Readers pick up the new segment list on next acquire.
  segmentSet.atomic_swap(remove=input_segments, add=[builder.seal()])
  for s in input_segments:
    s.markForDelete()  # actual file unlink happens after readers drain
```

**Tombstone watermark.** A tombstone in segment S can be physically dropped only when no older segment exists that might contain the matching insert. The compaction tracks `tombstone_floor_segment` - the oldest live segment id - and any tombstone whose insert was strictly older than `tombstone_floor_segment` can be removed.

**Block-max recomputation.** During the merge, output blocks accumulate up to `block_size` postings, then `max_weight`, `weight_min`, `weight_max` are computed and the block header is written. The skip list entries accumulate `max_weight_to_end` in a final pass once all blocks for a dim are written.

**df recomputation.** `df[dim]` is updated incrementally during the merge: each emitted posting increments, each dropped tombstone-shadowed pair decrements.

### Compaction triggers

Size-tiered (configurable):
- Level 0: memtable flushes (initial). Each flush creates one L0 segment. Compact when L0 has `compaction_l0_threshold=4` segments.
- Level 1+: created by compaction. Each level holds segments roughly `compaction_factor * size of previous level`. Compact when level reaches `compaction_factor` segments.

Compaction can also be triggered manually via SQL: `REBUILD INDEX <name>` (existing primitive, just hooks into the new compactor).

---

## WAL integration

Three new WAL record types under the existing `WAL_*` framework:

- `WAL_SPARSE_PUT { index_name, dim, rid, weight }`
- `WAL_SPARSE_REMOVE { index_name, dim, rid }`
- `WAL_SPARSE_FLUSH { index_name, segment_id, segment_file_path }`

Replay on startup:

```
for each WAL record in order:
  case SPARSE_PUT: index.applyMemtablePut(dim, rid, weight)
  case SPARSE_REMOVE: index.applyMemtableRemove(dim, rid)
  case SPARSE_FLUSH: index.markFlushCompleted(segment_id)
                     # The segment file is already on disk and fsynced;
                     # this record marks the WAL position past which puts
                     # have been durably reflected in the segment.
```

After a clean shutdown, the memtable is flushed before the WAL closes; replay finds nothing to do. After a crash, replay reconstructs the memtable from the puts/removes since the last flush record.

---

## HA replication

Sealed segment files are paginated components. They replicate through the existing Raft component-shipping pipeline that `LSMTreeIndex` compaction uses:

- A flushed segment is a new file. The leader's `PaginatedSparseVectorEngine.flush()` runs inside `database.getWrappedDatabaseInstance().runWithCompactionReplication(...)`. That hook starts a `FileManager.startRecordingChanges()` session, runs the build (which `createComponent`-registers the new `SparseSegmentComponent` and writes its pages through an inner `database.transaction(...)`), drains the page cache (`waitAllPagesOfDatabaseAreFlushed`) so on-disk pages match the just-committed WAL, then ships a `SCHEMA_ENTRY` to followers carrying the addFiles map plus a synthetic page-WAL of the new file. Followers' `applySchemaEntry` creates the empty file via `createNewFiles`, applies the synthetic WAL to populate the pages, then reloads the schema so the new component shows up in the FileManager.
- Memtable state is reproduced on followers via WAL replay (every WAL record is committed through Raft, just like any other write transaction).
- Compaction on the leader uses the same `runWithCompactionReplication` recording session; the `SCHEMA_ENTRY` includes both the new merged segment and the retired input components in `removeFiles`, so followers swap atomically.
- Followers do not call the engine's flush/compaction code paths (`runWithCompactionReplication` returns false on non-leaders); they discover replicated segments via a query-time `refreshSegmentsFromFileManager()` that pulls newly-arrived `SparseSegmentComponent` instances from the FileManager into the engine's in-memory `segments` snapshot.

Smoke-tested by `RaftSparseVectorReplicationIT` (3-server, requires every server to serve top-K). The test was the disabled placeholder noted in the Phase 5 step 7 row above; making it pass uncovered three concrete fixes:

- `database.getWrappedDatabaseInstance()` is required so the call reaches the Raft override instead of the `DatabaseInternal` no-op default.
- `waitAllPagesOfDatabaseAreFlushed` is required so the `serializeFilePagesAsWal` read sees real bytes; without it followers receive a synthetic WAL of zero-pages and fail header validation on first read.
- Header-validation failures in `refreshSegmentsFromFileManager` must not crash the query - a sparseseg file briefly exists empty on followers between `createNewFiles` and the WAL apply, and a builder failure on the leader (e.g. dim_index page overflow) can leave an orphan empty file. The refresh now skips a file that fails header validation and the leader's flush drops the partial component when `b.finish()` throws.

No sparse-vector-specific replication code on the wire: the segment files are opaque paginated components, the synthetic WAL is the same format `LSMTreeIndexCompactor` ships.

---

## Backup integration

Snapshots include all `.sparseseg` files plus the index metadata in `schema.json`. Restore drops `.sparseseg` files into place and reopens the index.

The current backup framework iterates `PaginatedComponent` instances per database; the new sparse-vector segments register as components and inherit this behavior automatically.

---

## Concurrency model

Three locks:

1. **Memtable lock** - read-write. Reads (BMW DAAT) hold a read lock for the duration of the cursor walk; writes (put/remove) take a write lock briefly.
2. **Segment set lock** - lightweight `AtomicReference<long[]>` swap. Compaction publishes a new segment set atomically; readers acquire the snapshot at cursor start and use it for the duration of the query.
3. **Per-segment file lock** - none. Sealed segments are immutable; multiple readers share file handles via `PaginatedComponent`'s page cache.

Compaction and flush never block reads. A reader started before a compaction completes uses the old segment set; the new segment set replaces it after the compaction commits, and the old segments are physically deleted only after all readers using them drain (reference counting on segment open / close).

Memtable writes serialize on the memtable write lock; in practice this is ConcurrentSkipListMap's built-in locking, so writes scale within a dim and across dims.

---

## Configuration

Index metadata (per-index, set at create time):

```
{
  "dimensions": <int>,                  // unchanged from MVP
  "modifier": "NONE" | "IDF",           // unchanged from MVP
  "blockSize": <int>,                   // postings per block, default 128
  "skipStride": <int>,                  // blocks per skip entry, default 8
  "weightQuantization": "NONE" | "FP16" | "INT8",  // default INT8
  "ridCompression": "NONE" | "VARINT_DELTA",       // default VARINT_DELTA
  "memtableMaxPostings": <int>,         // flush threshold, default 1_000_000
  "compactionFanout": <int>,            // size-tiered fanout, default 4
  "compactionBasePostings": <long>      // tier 0 ceiling, default 1_000_000
}
```

Global configuration:

```
SparseSegmentComponent.DEFAULT_PAGE_SIZE   (default 65536)  // page size for new segment files
```

Two operator-facing knobs landed alongside the per-bucket fan-out wiring in
#4085: `SPARSE_VECTOR_SCORING_POOL_THREADS` (default `0` - auto-size to
`max(2, cores)`) and `SPARSE_VECTOR_SCORING_QUEUE_SIZE` (default `1024`).
Saturation of either is observable via the Studio "Executor Pools" card -
the `caller_run_fallbacks` counter on the `sparse_vector` row.

---

## Phasing and PR breakdown (historical)

This section captures the original four-PR plan. **What actually shipped:** a single squash-merged PR ([#4078](https://github.com/ArcadeData/arcadedb/pull/4078)) carrying every phase listed in the Progress table above, followed by the Bolt protocol patch ([#4079](https://github.com/ArcadeData/arcadedb/pull/4079)) and the partition-pruning patch ([#4119](https://github.com/ArcadeData/arcadedb/pull/4119)). The plan is preserved here for context on how the work was originally scoped.

Land in four PRs, each independently reviewable. Each PR keeps the existing MVP working until the new path replaces it in the final PR.

### PR 1: New on-disk format (writer + reader, no integration)

- New package: `com.arcadedb.index.sparsevector`
- File header, dim header, block format, skip list serialization.
- A `SealedSegmentBuilder` that takes an in-memory posting iterator and writes a `.sparseseg` file.
- A `SealedSegmentReader` that opens a file, exposes `iteratePostings(dim)`, `seekToRid(dim, rid)`, `blockMax(dim, blockIndex)`, etc.
- Roundtrip tests: build → read → verify identical posting set.
- Compression tests: int8 quantization stays within tolerance, VarInt-delta is reversible.
- Tagged `@Tag("slow")` tests for large segments (10M postings).
- No integration with `LSMSparseVectorIndex` yet; the new code lives alongside the MVP.

**Effort:** ~1 week.

### PR 2: BMW DAAT cursor + per-segment scoring

- `MemtableSourceCursor`, `SegmentSourceCursor`, `MergedDimCursor`.
- BMW DAAT main loop, reused from the MVP's `wandTopK` but rewritten against the new cursors.
- Per-segment parallel scoring with a heuristic gate.
- Correctness tests vs brute-force `vector.sparseDot` baseline (existing test).
- Benchmarks: 100k, 1M, 10M corpora. Compare new BMW vs brute force vs MVP WAND.

Still no integration; the new path is exercised through a separate test entry point.

**Effort:** ~1.5 weeks.

### PR 3: Memtable + compaction + WAL

- `Memtable` class with concurrent skip-list backing.
- Flush worker.
- Compaction worker with size-tiered policy.
- WAL record types and replay handlers.
- Recovery tests: kill -9 mid-write, kill -9 mid-flush, kill -9 mid-compact, verify the index is consistent on restart.
- HA tests: leader/follower segment replication, leader failover during a flush, compaction on a follower after promotion.

**Effort:** ~1.5 weeks.

### PR 4: Wire `LSM_SPARSE_VECTOR` to the new storage, delete the MVP

- `LSMSparseVectorIndex` no longer wraps `LSMTreeIndex`. Instead it owns a `SparseVectorEngine` with `Memtable` + `SegmentSet` + `CompactionWorker`.
- All public API stays identical (`put`, `remove`, `topK`, `vector.sparseNeighbors` SQL function).
- Existing tests in `LSMSparseVectorIndexTest`, `LSMSparseVectorIndexConcurrencyTest`, `LSMSparseVectorIndexSeekTest`, `Discussion4044HybridSearchE2ETest` all run against the new storage. No test changes expected.
- Benchmark updated: `LSMSparseVectorIndexBenchmark` measures the new storage at 100k, 1M, 10M; comparison-doc footnote about MVP being slow at 100k gets removed.
- The MVP code (composite-key approach) is deleted in this PR. Branch has the new storage from the start of the PR; rollback is reverting the PR.

**Effort:** ~1 week including test/benchmark sweep.

**Total:** roughly 4-5 calendar weeks from start to merged PR 4.

---

## Test plan

### Correctness (every PR)

- Roundtrip: build a segment from a known posting set, read it back, verify.
- BMW vs brute force: results on a 10k-doc corpus must match `vector.sparseDot` baseline within tolerance for every K from 1 to 100.
- Persistence: insert N docs, close, reopen, query, results unchanged.
- Tombstones: insert, delete, query - deleted RIDs never appear; insert again, query - reappears.
- Compaction: build several segments, force compact, run all queries again, results unchanged.
- Concurrency: 4 writers + 4 readers + background compaction, 5-minute soak test, verify no corruption (existing concurrency test extended).

### Recovery

- Crash mid-put (after WAL append, before memtable insert): replay restores.
- Crash mid-flush (segment file partially written): on restart, partial file is detected (CRC mismatch) and discarded; WAL replay rebuilds the memtable.
- Crash mid-compact: input segments still on disk, output segment partial; on restart, output is discarded, inputs remain live, compaction reschedules.

### HA

- Replicate segments to two followers, kill leader, follower takes over, query results match.
- Compaction on leader during a follower disconnect: follower catches up via segment file shipping.
- Memtable replay across cluster: 100k inserts, kill leader before flush, follower takes over, all 100k inserts queryable.

### Performance

- 1M corpus benchmark: BMW must beat brute force by ≥5x at K=10 with selective queries (head dims contributing >50% of score mass).
- 10M corpus benchmark: BMW QPS measured, compared to Qdrant 1.16 reference (informally; not a CI gate).
- First-query latency post-reopen: O(1) segment open vs O(N) MVP scan. Measured.
- Storage size: `du -sh database/` for a 1M corpus must be ≤ 25% of the MVP layout.

---

## Open questions

1. **Block size default.** 128 is the textbook value. Lucene uses 128 too. Worth empirically validating against a SPLADE-style corpus before committing the default.
2. **`fp16` mode urgency.** Some embedding providers (Cohere v4, OpenAI 3-large) emit non-uniform weight distributions where int8 quantization may be visibly lossy. fp16 is cheap to add but not strictly needed for v1. Including the option but defaulting to int8.
3. **Per-segment parallel scoring threshold.** The estimated-postings heuristic is a guess; revisit after the 1M benchmark.
4. ~~**Compaction triggering on deletes.**~~ **Resolved (Tier 2).** `compactSizeTiered` now has a secondary trigger after the size-tier overflow path: when `active.length >= tierFanout` and any segment carries `tombstoneCount / totalPostings >= TOMBSTONE_RATIO_TRIGGER` (default 0.30), the offender is paired with the oldest neighbors so the merge collapses file count. Tombstone counts are persisted in the manifest's previously-reserved 8-byte slot (no format version bump - older segments read 0L, a safe under-report). Runs with `dropAllTombstones=false` to preserve user-visible state when matching inserts are in older segments outside the input set; the win is purely in segment count.
5. **`REBUILD INDEX` semantics.** Should rebuild force a full compaction (all segments → one), or just trigger one round? Defaulting to "full compaction" since that's what users typically expect after a bulk delete or model change.

---

## Risks

- **Engineering scope.** 4-5 weeks of focused work is the optimistic estimate. Realistic range is 5-8 weeks if recovery, HA, and benchmarking surface issues. Surface a check-in at end of PR 1 to recalibrate.
- **WAL replay correctness.** Memtable + flush + WAL is the trickiest piece. Plan to write the recovery test suite before the implementation, not after.
- **Compaction stalls.** If the compaction worker can't keep up with ingest, segments accumulate, queries slow down (more cursors to merge). Monitor segment count metric and add throttling on insert if needed. Not a v1 blocker but should ship with metrics.
- **Memtable pressure under high write rate.** ~~Default 1M postings / 64 MiB is conservative. If memtables grow faster than flush, OOM.~~ **Resolved (Tier 2).** `PaginatedSparseVectorEngine.put` / `remove` soft-block on `mutatorLock` once memtable size crosses `MEMTABLE_HARD_LIMIT_FACTOR * memtableFlushThreshold` (default 2x). The lock take/release is free when no flush is running; under contention the put waits exactly for the in-progress flush to swap the memtable. Backpressure is deliberately a wait-on-lock and not a self-triggered flush, since the commit-replay path runs inside an outer `database.transaction(...)` and nesting a transaction in the flush body would deadlock.
- **Per-segment parallelism overhead.** Forking work to a thread pool can hurt small queries. The threshold heuristic mitigates, but verify on the benchmark.

---

## Open follow-ups (out of v1 PR scope, identified during Phase 4)

- ~~WAL durability for mid-transaction crashes~~ - **closed in Phase 4.1.** A post-commit callback now fsyncs the engine memtable to a sealed segment immediately after the transaction's commit-2nd-phase completes. The window between "ArcadeDB considers the transaction durable" and "segment file fsynced" is microsecond-scale, matching the durability profile of ArcadeDB's normal page-level commits with `asyncFlush=true`. A future enhancement would be inline `WAL_SPARSE_PUT/REMOVE/FLUSH` records that fold into the page WAL itself, eliminating even that microsecond window; tracked as a low-priority polish item.
- ~~**Per-segment parallel scoring.**~~ Partially closed: per-bucket fan-out shipped via #4085 (see Phase 5 step 6 row above). The `SparseVectorScoringPool` now actually serves work - `SQLFunctionVectorSparseNeighbors.executeWithIndexes` submits each per-bucket `LSMSparseVectorIndex.topK(...)` call onto it when the type has more than one bucket. The per-bucket dispatch is correctness-trivial because per-bucket sub-indexes are independent (disjoint RIDs, no tombstone or cross-segment-dim coordination across buckets). The `SPARSE_VECTOR_SCORING_POOL_THREADS` and `SPARSE_VECTOR_SCORING_QUEUE_SIZE` operator knobs are in `GlobalConfiguration` alongside the wiring. **Per-segment within-index parallelism (RID-range partitioning to handle cross-segment dim contributions and tombstone newest-wins inside one sub-index) is still open** - kept on this list as a future enhancement if profiling shows single-bucket workloads bottlenecked on the serial DAAT path. With per-bucket fan-out covering partitioned types and types with multiple buckets, the practical bar for the within-index work is now higher than the design doc originally framed.
- **Backward-compatible MVP rebuild.** The v2 backend keeps the legacy LSM-Tree shell readable (so prior schemas open) but does not auto-rebuild #4065 MVP-era postings into the new segment format. Pre-v2 datasets need re-insertion. Release notes call this out; if the field reports it, file a separate migration issue.
- **Segment proliferation under high commit rate.** ~~Flush-on-commit creates one sealed segment per transaction.~~ Resolved in Phase 5 step 9: an auto-flush threshold (≥ 1M postings) means most transactions don't seal a segment at all, and the size-tiered compaction trigger keeps the merged segment count at `O(log_fanout(N/base))`. For batch loaders this is a non-issue; for OLTP-style one-record-per-transaction workloads, segment count is bounded regardless. **Studio surface added** (Tier 1 follow-up): Server tab now shows a "Sparse Vector Indexes" card with one row per logical `LSMSparseVectorIndex` aggregating `memtablePostings`, `segmentCount`, and `totalPostings` across per-bucket sub-indexes. Backed by `LSMSparseVectorIndexMetrics.buildJSON` (engine) + `GetServerHandler.buildSparseVectorIndexesJSON` (server). Hidden by default when no database has any sparse-vector indexes.

## Companion follow-ups (out of v1 scope)

- **#4071** traversal-integrated grouping: rewrite of `groupBy` against the new cursor.
- ~~**#4073** `isOriginalCall` shape-sniffing cleanup~~ - **closed.** Replaced with the typed `SparsePostingReplayKey` record: `queueOrApply` wraps the scalar `(dim, rid, weight)` tuple in the marker before handing it to `addIndexOperation`, and the `put` / `remove` overrides dispatch via `instanceof SparsePostingReplayKey`. The dedup-map semantics carried by `TransactionIndexContext` are preserved (record `equals`/`hashCode` are field-wise; `Comparable` ordering routes through `BinaryComparator`'s fallback path). Verified by `SparsePostingReplayKeyTest` (3 tests) plus the full sparse-vector + LSM-Tree + HA-replication regression set staying green.
- **Persistent `dimMaxWeight` snapshot for fast cold-cache queries**: subsumed by per-block max in the new format.
- **BlockMax-MaxScore companion algorithm**: a future option if BMW underperforms on certain query shapes.
- **GPU-accelerated WAND** (CUDA / Metal): foundation is in place once #4068 lands, but separate ticket.
