# Issue #5664: GraphBatch retains per-vertex state for the whole batch lifetime

https://github.com/ArcadeData/arcadedb/issues/5664

## Root cause

`GraphBatch` (`engine/src/main/java/com/arcadedb/graph/GraphBatch.java`) buffers state that scaled with the
**lifetime of the batch** (i.e. the whole stream), not with `batchSize` as the API's only tuning knob implied:

1. `outChunkRIDCache` / `inChunkRIDCache` (`Map<Long, RID>`, head-chunk lookup accelerators) were plain
   `ConcurrentHashMap`s, never cleared. Every distinct vertex an edge touched added ~80-90 bytes across the two
   maps and it was never evicted, so a 100M-distinct-vertex stream held 16-18 GB in these two maps alone.
2. The deferred incoming-edge buffer (`inEdgeBucketIds`/`inEdgePositions`/`inVertexBucketIds`/
   `inVertexPositions`/`inDstBucketIds`/`inDstPositions`, six parallel primitive arrays, doubled on overflow)
   was only drained by `connectDeferredIncomingEdges()`, called exclusively from `close()`. 100M edges in one
   batch held 3.6 GB steady, up to ~2.5x that during a doubling copy.

`batchSize` only sizes the *outgoing* edge buffer, reset every `flush()` - the knob a user reaches for does not
touch either leak, which is exactly the "big batches vs. many streams" false dilemma reported against the
663M-vertex / 14B-edge gRPC `GraphBatchLoad` stream in discussion #5597.

## Fix

Both call sites for the two RID caches already fall back to reading the vertex's head chunk from disk on a
miss (`getOrCreate{Out,In}{Segment,EdgeChunk}{Deferred,Exact}`), so bounding them is a safe drop-in:

1. **Bounded head-chunk RID caches.** `outChunkRIDCache`/`inChunkRIDCache` are now
   `Collections.synchronizedMap(new LRUCache<>(chunkCacheCapacity))` - the same bounded-LRU pattern already
   used by `CypherStatementCache`. Configurable via `GraphBatch.Builder.withChunkCacheCapacity(int)`, default
   1,000,000 entries per cache (~100-150 MB total). `LRUCache` is not thread-safe on its own; wrapping in
   `Collections.synchronizedMap` is required because `getOrCreateOutEdgeChunk`/`getOrCreateInEdgeChunk` and the
   parallel-flush range handlers hit these maps from multiple async slots.

2. **Periodic draining of the deferred incoming-edge buffer.** `flush()` now checks, after accumulating a
   flush's edges into the deferred IN buffer, whether `inEdgeCount` has crossed a configurable cap
   (`GraphBatch.Builder.withMaxDeferredIncomingEdges(int)`, default 5,000,000) and if so calls
   `connectDeferredIncomingEdges()` early instead of waiting for `close()`. This amortizes the incoming-edge
   connection pass over the load rather than landing as one multi-minute pass at the end (issue #5470's
   86-second close-time wait is the shape of cost this removes). Setting the cap to `0` opts back into the
   pre-#5664 behavior of deferring everything to `close()`.
   - `connectDeferredIncomingEdges()` already null'd out the six deferred-buffer arrays when it finished (an
     existing optimization to let them be GC'd at `close()`); calling it mid-batch meant a later
     `accumulateIncomingEdges()` would NPE on the null arrays. Fixed by lazily re-allocating them (at the same
     initial `batchSize` capacity the constructor uses) the first time `accumulateIncomingEdges()` sees them
     null.

`deferredOutHead`/`deferredInHead`/`knownNewVertexKeys` were intentionally left alone: they already use the
zero-boxed `LongObjectHashMap`/`LongHashSet` (noted in the issue as ~5-7x lighter than the boxed alternative),
they are already cleared on every `batchUpdateVertexHeadChunks()` pass, and their size is bounded by the number
of distinct vertices that got a *new or overflowed* segment - proportional to the dataset the caller chose to
import, not to an unrelated leak. Widening the periodic-drain mechanism to also flush these early was
considered but out of scope for this fix: it would call `batchUpdateVertexHeadChunks()` (which persists vertex
records and clears `knownNewVertexKeys`) mid-batch, a materially bigger behavioral change than the two
call-site-safe caches this issue's evidence and proposal focus on.

## Changes

- `engine/src/main/java/com/arcadedb/graph/GraphBatch.java`
  - `outChunkRIDCache`/`inChunkRIDCache`: `ConcurrentHashMap` → bounded `LRUCache` wrapped in
    `Collections.synchronizedMap`, capacity configurable via the builder.
  - New builder methods: `withChunkCacheCapacity(int)`, `withMaxDeferredIncomingEdges(int)`.
  - New constants: `DEFAULT_CHUNK_CACHE_CAPACITY` (1,000,000), `DEFAULT_MAX_DEFERRED_INCOMING_EDGES`
    (5,000,000).
  - `flush()`: drains the deferred incoming-edge buffer early once it crosses the configured cap.
  - `accumulateIncomingEdges()`: lazily re-allocates the deferred buffer arrays if an earlier in-flush drain
    freed them.
  - New package-private test accessors `getOutChunkRIDCacheSize()` / `getInChunkRIDCacheSize()`.
  - Class-level javadoc updated to document both bounds (issue's ask to "document them").
- `engine/src/test/java/com/arcadedb/graph/GraphBatchBoundedStateTest.java` (new): regression coverage per the
  issue's own "Verification" section - runs a `GraphBatch` over many more distinct vertices than the configured
  cap and asserts the retained cache/buffer size stays bounded, for both the sequential and parallel flush
  paths, plus correctness (every edge still traversable in both directions despite evictions/early draining),
  plus builder validation and the `0`-disables-early-drain opt-out.

## Test results

- `GraphBatchBoundedStateTest` (new): 5/5 passed, 0.824s.
- `GraphBatchTest` (existing correctness + benchmark suite): 8/8 passed, 1.369s - no regression.
- `GraphBatchWALRestoreTest`: 3/3 passed.
- `GraphBatchUniqueIndexTest`: 3/3 passed.
- `GraphBatchCommitRetryTest`: 3/3 passed.
- `mvn -pl engine -am compile`: success.
- `mvn -pl engine,grpcw,server,network,integration -am compile`: success (confirms no downstream API break for
  the gRPC `GraphBatchLoad` stream this issue was reported against, the HTTP batch handler, `RemoteGraphBatch`,
  and the Neo4j importer - none of which needed changes, since all new builder methods are additive).

## Impact / follow-up for monitoring

- Both new caps are conservative defaults chosen to bound memory to low-hundreds-of-MB even on an extreme
  (100M+ vertex/edge) stream, while being large enough that typical/bounded imports never reach them - no
  behavior or performance change for the common case.
- Operators running very large streams should watch for the new INFO log lines from
  `connectDeferredIncomingEdges()` firing more often (once per drain instead of once at `close()`) - this is
  expected and is the amortization the fix is for, not a symptom of trouble.
- `deferredOutHead`/`deferredInHead`/`knownNewVertexKeys` remain unbounded for the batch's lifetime (bounded
  only by distinct-vertex count, not edge count). If a future report shows this scaling to be a problem in
  practice, extending the periodic-drain mechanism to call `batchUpdateVertexHeadChunks()` early is the
  natural next step, flagged here for future reference rather than acted on now.
