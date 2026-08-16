/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.LRUCache;
import com.arcadedb.utility.LongHashSet;
import com.arcadedb.utility.LongObjectHashMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.logging.Level;

/**
 * High-performance batch graph importer that buffers edges in memory and flushes them
 * sorted by vertex, converting random I/O into sequential I/O.
 * <p>
 * Key optimizations over the standard edge creation path:
 * <ul>
 *   <li>Edges are buffered in flat primitive arrays (no object overhead)</li>
 *   <li>On flush, outgoing edges are sorted by source vertex and connected in one pass</li>
 *   <li>Incoming edges are deferred to close() and connected in a single sorted pass</li>
 *   <li>Edge segments use O(1) append instead of O(n) insert-at-head</li>
 *   <li>Configurable initial segment size (default 2048 vs standard 64 bytes)</li>
 *   <li>Light edges by default when no properties are needed</li>
 *   <li>WAL and read-your-writes disabled during import</li>
 *   <li>Vertex creation pre-allocates edge segments to avoid lazy allocation during flush</li>
 *   <li>Edge type bucket ID is cached to avoid repeated schema lookups</li>
 *   <li>O(n) counting sort partitions edges by bucket before per-bucket position sort</li>
 *   <li>Optional parallel flush: edge connection dispatched to async threads by bucket</li>
 *   <li>Head chunk RID cache: skips vertex record loads when segment RID is already known, bounded by a
 *       configurable LRU cap so a long-lived stream's cache memory does not grow with distinct vertex count
 *       (issue #5664)</li>
 *   <li>Lazy vertex loading: vertex only loaded from disk on segment overflow</li>
 *   <li>Deferred incoming edges drain early once buffered past a configurable cap, instead of only at
 *       {@link #close()}, so the buffer and the close-time connection pass stay bounded on a long stream
 *       (issue #5664)</li>
 * </ul>
 * <p>
 * <b>Memory bounding is partial, by design (issue #5664).</b> The two head-chunk RID caches and the deferred
 * incoming-edge buffer above are bounded, but {@code deferredOutHead}, {@code deferredInHead} and
 * {@code knownNewVertexKeys} still grow with the number of DISTINCT VERTICES the batch touches (not with the
 * edge count, and not with {@code batchSize}); they are only cleared by {@code batchUpdateVertexHeadChunks()}
 * at {@link #close()}. They are also load-bearing rather than merely an optimization: since #5664 they are the
 * authoritative fallback consulted when a bounded RID cache evicts an entry, which is what keeps an eviction
 * from silently orphaning an earlier segment. So a stream touching hundreds of millions of distinct vertices
 * still carries per-vertex state proportional to that count. Bounding those too means draining vertex head
 * pointers mid-batch, a materially larger change; it is deliberately not attempted here.
 * <p>
 * <b>Super-node promotion (#5667):</b> unlike the standard {@code Vertex.newEdge()} path, a bulk load through
 * this class never promotes a hot vertex to the striped super-node layout (see
 * {@link GlobalConfiguration#GRAPH_SUPERNODE_THRESHOLD}) - every vertex loaded here keeps the classic chained
 * edge-list layout no matter how many edges land on it, so a very high-degree vertex ends up as a long chain of
 * capped-size segments instead of being striped across a per-type bucket pool. If the graph already has
 * promoted vertices (created through the standard API before this bulk load ran, or by disabling promotion
 * only partway through a long-running import), this class resumes over them correctly - edges for an
 * already-promoted vertex are routed through the same {@link StripedEdgeList} write path {@code Vertex.newEdge()}
 * uses, at ordinary (non-bulk) speed - it just does not perform new promotions itself. Set
 * {@code arcadedb.graph.supernodeThreshold=0} database-wide to disable promotion entirely if a bulk-loaded
 * super-node's degraded traversal performance is a concern.
 * <p>
 * Usage:
 * <pre>
 * try (final GraphBatch batch = database.batch()
 *     .withBatchSize(100_000)
 *     .withEdgeListInitialSize(2048)
 *     .withLightEdges(true)
 *     .withWAL(false)
 *     .build()) {
 *
 *   // Phase 1: create vertices (edge segments pre-allocated)
 *   MutableVertex v1 = batch.newVertex("Person").set("name", "Alice").save();
 *   MutableVertex v2 = batch.newVertex("Person").set("name", "Bob").save();
 *
 *   // Phase 2: buffer edges (outgoing flushed periodically, incoming deferred)
 *   batch.newEdge(v1.getIdentity(), "KNOWS", v2.getIdentity());
 *
 *   // Edges are flushed automatically on close (incoming edges connected here)
 * }
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphBatch implements AutoCloseable {

  private final DatabaseInternal database;

  /**
   * Database instance holding the single-batch guard this batch reserved, or {@code null} when the batch was
   * built outside {@link com.arcadedb.database.Database#batch()}. This is deliberately the concrete instance
   * and not {@link #database}, which may be an HA wrapper that only delegates {@code batch()} (issue #5666).
   */
  private final LocalDatabase guardOwner;
  private final AtomicBoolean guardReleased = new AtomicBoolean(false);

  // --- Configuration ---
  private final int     batchSize;
  private final int     edgeListInitialSize;
  private final boolean lightEdges;
  private final boolean bidirectional;
  private final int     commitEvery;
  private final boolean useWAL;
  private final WALFile.FlushType walFlush;
  private final boolean preAllocateEdgeChunks;
  private final boolean parallelFlush;
  private final int     commitRetries;
  private final long    commitRetryDelayMs;
  private final int     chunkCacheCapacity;
  private final int     maxDeferredIncomingEdges;

  /** Upper bound for the exponential back-off between vertex-commit retries. */
  private static final long MAX_COMMIT_RETRY_DELAY_MS = 10_000L;

  /**
   * Default cap on the {@link #outChunkRIDCache} / {@link #inChunkRIDCache} entries (issue #5664). Both caches
   * are pure lookup accelerators - every call site falls back to reading the vertex's head chunk from disk on a
   * miss - so bounding each with an LRU keeps its memory flat on a long-lived stream instead of growing with the
   * number of distinct vertices touched. ~1M entries is roughly 100-150 MB per cache.
   */
  private static final int DEFAULT_CHUNK_CACHE_CAPACITY = 1_000_000;

  /**
   * Default cap on the deferred incoming-edge buffer (issue #5664). Once {@link #inEdgeCount} reaches this many
   * buffered entries, {@link #connectDeferredIncomingEdges()} runs early from {@link #flush()} instead of
   * waiting for {@link #close()}, amortizing the connection pass over the load and keeping the buffer's
   * primitive arrays (and the transient doubling copy when they grow) from scaling with the full stream length.
   */
  private static final int DEFAULT_MAX_DEFERRED_INCOMING_EDGES = 5_000_000;

  /**
   * Test-only fault-injection hook. Invoked just before each {@link #createVertices} commit with
   * the 1-based attempt number; a test can throw a {@link NeedRetryException} to simulate a
   * transient replication failure (e.g. a Raft leader re-election) and verify the bounded
   * commit-retry recovers. Always {@code null} in production.
   */
  public static volatile IntConsumer TEST_BEFORE_VERTEX_COMMIT_HOOK = null;

  /**
   * Test-only fault-injection hook. Invoked with a 1-based call number immediately before each
   * {@code database.commit()} that durably persists a slice of deferred incoming edges - both the
   * sequential path's {@code commitEvery}-driven commits ({@link #connectIncomingEdgesSequential})
   * and the parallel path's one commit per destination-bucket task (fired at the end of
   * {@link #connectIncomingEdgesRangeLocal}, right before the surrounding {@code async.transaction}
   * commits). A test can throw to simulate a commit failing partway through
   * {@link #connectDeferredIncomingEdges()} and verify that a subsequent retry (e.g. from
   * {@link #close()}) does not reprocess the slices that already committed durably (issue #5950
   * review cycle 3). Always {@code null} in production.
   */
  public static volatile IntConsumer TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = null;

  /**
   * Test-only fault-injection hook. Invoked with a 1-based call number at the end of each
   * {@link #connectOutEdgesRangeLocal} bucket task, right before the surrounding
   * {@code async.transaction} commits it. A test throwing a {@link ConcurrentModificationException} here
   * drives the async executor's own CME retry (which re-invokes the same bucket lambda after a rollback,
   * see {@code DatabaseAsyncTransaction.execute()}) and so verifies that a rolled-back attempt left no
   * non-durable RID behind in the shared head-chunk caches (issue #5950 review cycle 4). Always
   * {@code null} in production.
   */
  public static volatile IntConsumer TEST_BEFORE_OUTGOING_EDGE_COMMIT_HOOK = null;

  /**
   * Test-only fault-injection hook. Invoked with a 1-based call number immediately before each
   * {@code database.commit()} inside {@link #reclaimOrphanEdgeRecords} - both the {@code commitEvery}-driven
   * commits and the final one. A test can throw on the SECOND call to simulate the cleanup after a failed flush
   * itself failing partway, and verify that the reclaims an earlier commit already made durable stay counted as
   * reclaimed rather than being rewritten as leaked (issue #6083 review cycle 1). Always {@code null} in
   * production.
   */
  public static volatile IntConsumer TEST_BEFORE_ORPHAN_RECLAIM_COMMIT_HOOK = null;

  /** Monotonic call counter feeding {@link #TEST_BEFORE_OUTGOING_EDGE_COMMIT_HOOK}'s 1-based argument. */
  private final AtomicInteger outgoingEdgeCommitAttempt = new AtomicInteger(0);

  /** Monotonic call counter feeding {@link #TEST_BEFORE_ORPHAN_RECLAIM_COMMIT_HOOK}'s 1-based argument. */
  private final AtomicInteger orphanReclaimCommitAttempt = new AtomicInteger(0);

  /** Monotonic call counter feeding {@link #TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK}'s 1-based argument. */
  private final AtomicInteger incomingEdgeCommitAttempt = new AtomicInteger(0);

  // --- Edge buffer: flat arrays for minimal GC pressure ---
  // Each edge occupies one slot across these parallel arrays.
  private int[]      edgeSrcBucketIds;
  private long[]     edgeSrcPositions;
  private int[]      edgeDstBucketIds;
  private long[]     edgeDstPositions;
  private int[]      edgeTypeBucketIds;   // first bucket id of the edge type (for light edge RID)
  private boolean[]  edgeHasProperties;
  private boolean[]  edgeIsLightweight;
  private long       duplicateLightEdges;
  private Object[][] edgeProperties;      // null for light edges
  private int        edgeCount;

  // --- Deferred incoming edges: accumulated across flushes, connected at close() ---
  // Uses growing arrays (doubled on overflow) to avoid ArrayList<RID> boxing.
  private int[]  inEdgeBucketIds;     // edge RID bucket
  private long[] inEdgePositions;     // edge RID position
  private int[]  inVertexBucketIds;   // source vertex RID bucket (the "from" vertex for incoming)
  private long[] inVertexPositions;   // source vertex RID position
  private int[]  inDstBucketIds;      // destination vertex RID bucket (the vertex that receives the incoming edge)
  private long[] inDstPositions;      // destination vertex RID position
  private int    inEdgeCount;

  /**
   * How far into the sort index built at the top of {@link #connectDeferredIncomingEdges()} the
   * sequential path ({@link #connectIncomingEdgesSequential}) has durably committed a prefix of
   * groups. Lets a retry after a partial failure resume after the last committed group instead of
   * recommitting it (issue #5950 review cycle 3): {@link #flush()}'s early drain can fail partway
   * through a large {@code connectDeferredIncomingEdges()} pass, after which {@link #close()}
   * unconditionally re-runs the drain over the same deferred buffer.
   * <p>
   * Sound because the retry rebuilds its sort index over the SAME pinned row count
   * ({@link #inEdgesDrainPrefix}) the failed attempt used, so the index has an identical shape and this
   * raw offset still means the same thing. Reset to 0 only once a pinned prefix completes in full.
   */
  private int inEdgesResumeSortIndex = 0;

  /**
   * Number of buffered rows the in-progress (or last failed) incoming-edge drain pass covers, or 0 when
   * no pass is pending. Pinning this decouples the resume state from later appends to the buffer (issue
   * #5950 review cycle 4): {@link #partitionIncomingByDestBucket} is a counting sort, so a buffer that
   * grew between a failed attempt and its retry would produce a differently-shaped sort index, against
   * which {@link #inEdgesResumeSortIndex} (a raw index) and {@link #completedIncomingBuckets} (bucket
   * ranges) would silently skip or reprocess groups. Rows appended past the pin are drained separately.
   */
  private int inEdgesDrainPrefix = 0;

  /**
   * Destination-bucket ids whose incoming-edge slice the parallel path
   * ({@link #connectIncomingEdgesParallel}) has already durably committed, across possibly several
   * attempts at {@link #connectDeferredIncomingEdges()} (issue #5950 review cycle 3). Each bucket
   * runs as its own independent {@code async.transaction} unit, so one bucket's commit failing does
   * not roll back another's; a retry skips scheduling any bucket already in this set instead of
   * recommitting it. Marked from the transaction's post-commit {@code OkCallback} - never from
   * inside the bucket-range lambda itself, which runs BEFORE the surrounding commit and could still
   * fail there - so membership always implies a durable commit. Cleared alongside the buffer arrays
   * only once {@code connectDeferredIncomingEdges()} completes ALL remaining work.
   */
  private final Set<Integer> completedIncomingBuckets = ConcurrentHashMap.newKeySet();

  // --- Temp arrays for vectorized segment writes (reused across flushes) ---
  private int[]  tmpEdgeBucketIds;
  private long[] tmpEdgePositions;
  private int[]  tmpVertexBucketIds;
  private long[] tmpVertexPositions;

  // --- Sort index: avoids moving the large property arrays during sort ---
  private int[] sortIndex;

  // --- Merge sort temp buffer: allocated once, reused across sorts ---
  private int[] mergeTmp;

  // --- Counting sort state (reusable across flushes) ---
  private int[] bucketCounts;
  private int[] bucketOffsets;
  private int[] countingSortCursor;

  // --- Edge type cache: avoids repeated schema lookups ---
  private final Map<String, Integer> edgeTypeFirstBucketCache = new ConcurrentHashMap<>();
  private final Map<String, Boolean> lightweightTypeCache     = new ConcurrentHashMap<>();

  // --- Head chunk RID cache: avoids vertex loads when chunk is already known ---
  // Bounded LRU wrapped in synchronizedMap (issue #5664): getOrCreate*EdgeChunk() is called from parallel async
  // slots, so the map must stay thread-safe, and an unbounded cache grows with the number of distinct vertices
  // touched over the WHOLE lifetime of the batch rather than with batchSize. Every call site falls back to
  // reading the vertex's head chunk from disk on a miss, so eviction is always safe, just slower on a miss.
  private final Map<Long, RID> outChunkRIDCache;
  private final Map<Long, RID> inChunkRIDCache;

  // --- Deferred vertex head chunk updates: persisted in one batch pass at close() ---
  // vertexKey → latest segment RID that needs to be set on the vertex record.
  //
  // Lifecycle (matters for the choice of non-concurrent collection):
  //   1. Sequential put() calls happen from connectOutgoingEdgesSorted /
  //      connectIncomingEdgesSequential (single-threaded paths).
  //   2. The PARALLEL paths (connectOutEdgesRangeLocal, connectIncomingEdgesRangeLocal) do NOT
  //      WRITE to these fields directly - they write to per-flush local ConcurrentHashMap
  //      parameters (parallelDeferredOutHead / parallelDeferredInHead) which are merged into
  //      these fields via putAll() AFTER async.waitCompletion(), establishing a happens-before
  //      barrier. They DO read these fields directly (issue #5950 review: the LRU-eviction fix),
  //      via LongObjectHashMap.get(), a pure read with no internal mutation - safe because no
  //      concurrent WRITER touches these fields during a parallel round; the read-only access from
  //      multiple async slots is fine.
  //   3. batchUpdateVertexHeadChunks() reads these single-threaded at flush end.
  // LongObjectHashMap (zero-boxing, ~5x less memory than ConcurrentHashMap<Long, RID>) is safe
  // under this lifecycle because writes are never concurrent and concurrent reads never race a write.
  private final LongObjectHashMap<RID> deferredOutHead = new LongObjectHashMap<>();
  private final LongObjectHashMap<RID> deferredInHead = new LongObjectHashMap<>();

  // --- Known-new vertices: created by createVertices(), guaranteed no existing segments ---
  // Allows skipping vertex record loads when creating first segment.
  //
  // Lifecycle (matters for the choice of non-concurrent collection):
  //   1. add() is only called from createVertices(), single-threaded, terminated by database.commit()
  //      which establishes happens-before with subsequent reads.
  //   2. contains() is called during edge-connect phases that may run with parallelFlush=true,
  //      but at that point the set is fully populated and never mutated (read-only).
  //   3. clear() runs at end of flush, single-threaded after another database.commit().
  // Plain LongHashSet (zero-boxing, ~7x less memory than ConcurrentHashMap.newKeySet()) is safe
  // because the only concurrent access is read-only after a publishing barrier.
  private final LongHashSet knownNewVertexKeys = new LongHashSet();

  // --- Statistics ---
  private long totalVerticesCreated;
  private long totalEdgesCreated;
  private long totalFlushes;
  private long totalFlushTimeNs;
  private long totalOrphanEdgeRecordsReclaimed;
  private long totalOrphanEdgeRecordsLeaked;

  /**
   * Edges of the flush in progress whose connection to their SOURCE vertex has durably committed (issue #6083
   * item 4). {@link #flush()} folds exactly this into {@link #totalEdgesCreated}, on the failure path as well as
   * the success one, so the running count never claims an edge the graph does not hold - and, since #6083, never
   * under-reports one it does. Reset at the top of every flush.
   */
  private int flushDurableOutEdges;

  /**
   * When the PARALLEL connect pass of the flush in progress failed, the {@code [from,to)} pairs into
   * {@link #sortIndex} naming the edges it nevertheless connected durably; {@code null} otherwise. The failed
   * buckets are wherever they are in the partition, so unlike the sequential path's durable prefix this cannot be
   * expressed as a single bound. Read once by {@link #flush()} right after the pass returns.
   */
  private int[] flushDurableOutRanges;

  // --- Saved state for restore after close ---
  private final boolean savedReadYourWrites;
  private final boolean savedUseWAL;
  private final WALFile.FlushType savedWALFlush;
  // Async executor WAL policy, relaxed/restored ONCE for the whole batch instead of once per flush
  // (issue #5665): DatabaseAsyncExecutorImpl tears down and respawns its entire worker pool on every
  // setTransactionUseWAL()/setTransactionSync() call, so applying the relaxed policy around every
  // flush's parallel connect phase recreated every async worker thread on every flush - killing any
  // concurrent async work on this database (an unrelated caller's in-flight/queued tasks force-exited
  // with "Async executor has been shut down") and, on a large multi-flush bulk load, doing so tens of
  // thousands of times. Meaningful only when parallelFlush is true; unused (false/null) otherwise.
  private final boolean            savedAsyncUseWAL;
  private final WALFile.FlushType  savedAsyncWALFlush;

  private GraphBatch(final DatabaseInternal database, final LocalDatabase guardOwner, final int batchSize,
      final int edgeListInitialSize,
      final boolean lightEdges, final boolean bidirectional, final int commitEvery,
      final boolean useWAL, final WALFile.FlushType walFlush, final boolean preAllocateEdgeChunks,
      final boolean parallelFlush, final int commitRetries, final long commitRetryDelayMs,
      final int chunkCacheCapacity, final int maxDeferredIncomingEdges) {
    this.database = database;
    this.guardOwner = guardOwner;
    this.batchSize = batchSize;
    this.edgeListInitialSize = edgeListInitialSize;
    this.lightEdges = lightEdges;
    this.bidirectional = bidirectional;
    this.commitEvery = commitEvery;
    this.commitRetries = commitRetries;
    this.commitRetryDelayMs = commitRetryDelayMs;
    this.chunkCacheCapacity = chunkCacheCapacity;
    this.maxDeferredIncomingEdges = maxDeferredIncomingEdges;
    this.outChunkRIDCache = Collections.synchronizedMap(new LRUCache<>(chunkCacheCapacity));
    this.inChunkRIDCache = Collections.synchronizedMap(new LRUCache<>(chunkCacheCapacity));
    // Already the effective value: Builder.build() forces the WAL on for replicated databases (issue #4076)
    // before it derives commitEvery from it (issue #5470).
    this.useWAL = useWAL;
    this.walFlush = walFlush;
    this.preAllocateEdgeChunks = preAllocateEdgeChunks;
    this.parallelFlush = parallelFlush;

    // Allocate edge buffers
    edgeSrcBucketIds = new int[batchSize];
    edgeSrcPositions = new long[batchSize];
    edgeDstBucketIds = new int[batchSize];
    edgeDstPositions = new long[batchSize];
    edgeTypeBucketIds = new int[batchSize];
    edgeHasProperties = new boolean[batchSize];
    edgeIsLightweight = new boolean[batchSize];
    edgeProperties = new Object[batchSize][];
    sortIndex = new int[batchSize];
    mergeTmp = new int[batchSize / 2 + 1];
    edgeCount = 0;

    // Allocate deferred incoming edge buffers (start with batchSize, grows dynamically)
    if (bidirectional) {
      final int initialInCapacity = batchSize;
      inEdgeBucketIds = new int[initialInCapacity];
      inEdgePositions = new long[initialInCapacity];
      inVertexBucketIds = new int[initialInCapacity];
      inVertexPositions = new long[initialInCapacity];
      inDstBucketIds = new int[initialInCapacity];
      inDstPositions = new long[initialInCapacity];
    }
    inEdgeCount = 0;

    // Allocate temp arrays for vectorized segment writes
    final int tmpSize = Math.min(batchSize, 4096);
    tmpEdgeBucketIds = new int[tmpSize];
    tmpEdgePositions = new long[tmpSize];
    tmpVertexBucketIds = new int[tmpSize];
    tmpVertexPositions = new long[tmpSize];

    // Save and optimize database settings for bulk import
    savedReadYourWrites = database.isReadYourWrites();
    database.setReadYourWrites(false);

    // Save WAL settings (per-transaction, so we apply them in beginTx()).
    // Issue #5378: the TransactionContext is reused across transactions on the same thread, so the relaxed
    // policy applied during the import would otherwise stick forever and silently downgrade the durability
    // contract of every later transaction. Capture the values actually in effect (which the context
    // initialized from arcadedb.txWAL / arcadedb.txWalFlush, unless the application overrode them) and put
    // exactly those back on close(). The current thread may have never run a transaction (e.g. an HTTP
    // worker thread in PostBatchHandler), in which case no context exists yet and the configured defaults
    // are exactly what a fresh context would initialize from.
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx != null) {
      savedUseWAL = tx.isUseWAL();
      savedWALFlush = tx.getWALFlush();
    } else {
      savedUseWAL = database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL);
      savedWALFlush = WALFile.getWALFlushType(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH));
    }

    if (savedUseWAL != this.useWAL || savedWALFlush != this.walFlush)
      LogManager.instance().log(this, Level.INFO,
          "GraphBatch: relaxing durability for the bulk load (useWAL %s->%s, walFlush %s->%s); the previous settings are "
              + "restored on close()", savedUseWAL, this.useWAL, savedWALFlush, this.walFlush);

    // Relax the shared async executor's WAL policy once, here, instead of once per flush (issue
    // #5665; see the field javadoc on savedAsyncUseWAL). Guarded on an actual change because the
    // setters below are not - each unconditionally tears down and respawns the worker pool.
    if (parallelFlush) {
      final DatabaseAsyncExecutor asyncExecutor = database.async();
      final boolean           priorAsyncUseWAL   = asyncExecutor.isTransactionUseWAL();
      final WALFile.FlushType priorAsyncWALFlush = asyncExecutor.getTransactionSync();
      savedAsyncUseWAL = priorAsyncUseWAL;
      savedAsyncWALFlush = priorAsyncWALFlush;

      final boolean asyncWALChanged   = priorAsyncUseWAL != this.useWAL;
      final boolean asyncFlushChanged = priorAsyncWALFlush != this.walFlush;

      if (asyncWALChanged)
        asyncExecutor.setTransactionUseWAL(this.useWAL);
      try {
        if (asyncFlushChanged)
          asyncExecutor.setTransactionSync(this.walFlush);
      } catch (final RuntimeException e) {
        // The constructor never completes on this path, so no instance exists afterwards to call
        // restoreAsyncSettings() - undo the first setter here or the async executor's WAL policy
        // stays relaxed forever for every other caller on this database.
        if (asyncWALChanged)
          asyncExecutor.setTransactionUseWAL(priorAsyncUseWAL);
        throw e;
      }

      if (asyncWALChanged || asyncFlushChanged)
        LogManager.instance().log(this, Level.INFO,
            "GraphBatch: relaxing async executor durability for the bulk load (useWAL %s->%s, walFlush %s->%s); the "
                + "previous settings are restored on close()/abandon()", priorAsyncUseWAL, this.useWAL, priorAsyncWALFlush,
            this.walFlush);
    } else {
      savedAsyncUseWAL = false;
      savedAsyncWALFlush = null;
    }
  }

  /**
   * Creates a new vertex of the given type. The vertex must be saved by the caller.
   * If preAllocateEdgeChunks is enabled (default), edge segments are pre-created
   * after the vertex is saved, eliminating lazy allocation during edge flush.
   * This matches the standard {@link Database#newVertex(String)} API.
   */
  public MutableVertex newVertex(final String typeName) {
    totalVerticesCreated++;
    return database.newVertex(typeName);
  }

  /**
   * Creates a new vertex, saves it, and pre-allocates edge segments if enabled.
   * Convenience method that handles save + pre-allocation in one call.
   * Must be called inside a transaction.
   *
   * @param typeName         vertex type name
   * @param vertexProperties optional key-value pairs
   * @return the saved vertex with pre-allocated edge segments
   */
  public MutableVertex createVertex(final String typeName, final Object... vertexProperties) {
    final MutableVertex vertex = database.newVertex(typeName);
    if (vertexProperties != null && vertexProperties.length > 0) {
      if (vertexProperties.length == 1 && vertexProperties[0] instanceof Map) {
        final Map<String, Object> map = (Map<String, Object>) vertexProperties[0];
        for (final Map.Entry<String, Object> entry : map.entrySet())
          vertex.set(entry.getKey(), entry.getValue());
      } else {
        if (vertexProperties.length % 2 != 0)
          throw new IllegalArgumentException("Properties must be an even number as pairs of name, value");
        for (int i = 0; i < vertexProperties.length; i += 2)
          vertex.set((String) vertexProperties[i], vertexProperties[i + 1]);
      }
    }

    vertex.save();

    if (preAllocateEdgeChunks) {
      getOrCreateOutEdgeChunk(vertex);
      if (bidirectional)
        getOrCreateInEdgeChunk(vertex);
    }

    totalVerticesCreated++;
    return vertex;
  }

  /**
   * Creates multiple vertices in a single transaction. Edge segments are NOT pre-allocated;
   * they will be created on-demand at flush time with exactly the right size based on the
   * actual edges buffered for each vertex.
   * Handles transaction begin/commit internally.
   *
   * @param typeName vertex type name
   * @param count    number of vertices to create
   * @return array of RIDs for the created vertices
   */
  public RID[] createVertices(final String typeName, final int count) {
    return createVerticesWithRetry(count, rids -> {
      for (int i = 0; i < count; i++) {
        final MutableVertex vertex = database.newVertex(typeName);
        vertex.save();
        rids[i] = vertex.getIdentity();
      }
    });
  }

  /**
   * Creates multiple vertices with properties in a single transaction. Edge segments are NOT
   * pre-allocated; they will be created on-demand at flush time with the exact size needed.
   *
   * @param typeName   vertex type name
   * @param properties per-vertex properties, may contain nulls for vertices with no properties
   * @return array of RIDs for the created vertices
   */
  public RID[] createVertices(final String typeName, final Object[][] properties) {
    final int count = properties.length;
    return createVerticesWithRetry(count, rids -> {
      for (int i = 0; i < count; i++) {
        final MutableVertex vertex = database.newVertex(typeName);
        final Object[] props = properties[i];
        if (props != null && props.length > 0) {
          for (int p = 0; p < props.length; p += 2)
            vertex.set((String) props[p], props[p + 1]);
        }
        vertex.save();
        rids[i] = vertex.getIdentity();
      }
    });
  }

  /**
   * Creates a batch of vertices inside a single transaction, retrying the whole begin/save/commit
   * unit on a transient {@link NeedRetryException} (e.g. a Raft {@code QuorumNotReachedException}
   * raised when a leader re-election interrupts replication mid-load).
   * <p>
   * Without this, a single transient cluster hiccup during a multi-million-row bulk import aborts
   * the entire streaming request with HTTP 503, forcing a full restart (issue #4724).
   * <p>
   * <b>At-least-once semantics on replicated databases:</b> when the commit fails after the Raft
   * entry was already dispatched, that entry may still be committed late by the cluster. The retry
   * re-creates the vertices with <i>fresh</i> RIDs, so in that narrow window the load may leave a
   * few orphan duplicate vertices behind. This is safe for the graph as a whole: the caller maps
   * temporary IDs to the RIDs returned here, and edges are connected only to those, so a late
   * duplicate is never referenced. {@code knownNewVertexKeys} is populated only after the commit
   * is durable so a rolled-back attempt never pollutes the edge-connect fast path.
   *
   * @param count  number of vertices to create
   * @param filler populates the supplied RID array by creating and saving {@code count} vertices
   *               inside the active transaction
   * @return RIDs of the durably-committed vertices
   */
  private RID[] createVerticesWithRetry(final int count, final Consumer<RID[]> filler) {
    int attempt = 0;
    while (true) {
      final RID[] rids = new RID[count];
      try {
        beginTx();
        filler.accept(rids);

        final IntConsumer hook = TEST_BEFORE_VERTEX_COMMIT_HOOK;
        if (hook != null)
          hook.accept(attempt + 1);

        database.commit();

        // Record known-new keys only after a durable commit so a rolled-back attempt never leaves
        // phantom keys in the edge-connect fast path.
        for (int i = 0; i < count; i++)
          knownNewVertexKeys.add(packVertexKey(rids[i].getBucketId(), rids[i].getPosition()));

        totalVerticesCreated += count;
        return rids;
      } catch (final NeedRetryException e) {
        if (database.isTransactionActive())
          database.rollback();

        if (++attempt > commitRetries)
          throw e;

        LogManager.instance().log(this, Level.WARNING,
            "GraphBatch.createVertices commit failed with a retryable error (attempt %d/%d): %s. Retrying after back-off...",
            null, attempt, commitRetries, e.getMessage());

        backoffBeforeRetry(attempt);
      }
    }
  }

  /**
   * Sleeps before the next vertex-commit retry using exponential back-off capped at
   * {@link #MAX_COMMIT_RETRY_DELAY_MS}, giving the cluster time to elect a new leader and the
   * Raft client time to refresh before the next attempt.
   */
  private void backoffBeforeRetry(final int attempt) {
    long delay = commitRetryDelayMs;
    for (int i = 1; i < attempt && delay < MAX_COMMIT_RETRY_DELAY_MS; i++)
      delay = Math.min(delay * 2, MAX_COMMIT_RETRY_DELAY_MS);

    if (delay <= 0)
      return;

    try {
      Thread.sleep(delay);
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new NeedRetryException("Interrupted while waiting to retry GraphBatch vertex commit");
    }
  }

  /**
   * Buffers an edge for batch insertion. The edge is NOT created immediately; it will be
   * materialized when the buffer is full or when {@link #flush()} / {@link #close()} is called.
   * <p>
   * Outgoing edges are connected during {@link #flush()}.
   * Incoming edges are deferred and connected during {@link #close()} for maximum batching.
   *
   * @param sourceVertexRID source vertex RID (must be already persisted)
   * @param edgeTypeName    edge type name (must exist in schema)
   * @param destVertexRID   destination vertex RID (must be already persisted)
   * @param edgeProperties  optional key-value pairs (if empty and lightEdges=true, a light edge is created)
   */
  public void newEdge(final RID sourceVertexRID, final String edgeTypeName, final RID destVertexRID,
      final Object... edgeProperties) {
    if (sourceVertexRID == null)
      throw new IllegalArgumentException("Source vertex RID is null");
    if (destVertexRID == null)
      throw new IllegalArgumentException("Destination vertex RID is null");

    // Cached edge type bucket ID lookup
    final int typeBucketId = edgeTypeFirstBucketCache.computeIfAbsent(edgeTypeName,
        name -> ((EdgeType) database.getSchema().getType(name)).getFirstBucketId());

    final boolean typeIsLightweight = lightweightTypeCache.computeIfAbsent(edgeTypeName,
        name -> ((EdgeType) database.getSchema().getType(name)).isLightweight());

    final int idx = edgeCount;
    edgeSrcBucketIds[idx] = sourceVertexRID.getBucketId();
    edgeSrcPositions[idx] = sourceVertexRID.getPosition();
    edgeDstBucketIds[idx] = destVertexRID.getBucketId();
    edgeDstPositions[idx] = destVertexRID.getPosition();
    edgeTypeBucketIds[idx] = typeBucketId;

    final boolean hasProps = edgeProperties != null && edgeProperties.length > 0;
    if (typeIsLightweight && hasProps)
      throw new IllegalArgumentException("Edge type '" + edgeTypeName
          + "' is declared LIGHTWEIGHT, so its edges cannot have properties. Use a regular edge type if the edge "
          + "needs to carry data");

    edgeHasProperties[idx] = hasProps;
    this.edgeProperties[idx] = hasProps ? edgeProperties : null;
    // A LIGHTWEIGHT type is stored lightweight whatever the builder was told: the storage shape belongs to the
    // schema, and withLightEdges() is only the legacy per-batch override for types that do not declare one.
    edgeIsLightweight[idx] = typeIsLightweight || (lightEdges && !hasProps);

    edgeCount++;

    if (edgeCount >= batchSize)
      flush();
  }

  /**
   * Flushes all buffered edges to disk. Outgoing edges are sorted by source vertex
   * and connected immediately. Incoming edges are accumulated in the deferred buffer
   * and will be connected at {@link #close()}.
   */
  public void flush() {
    if (edgeCount == 0)
      return;

    final long startNs = System.nanoTime();

    // Track whether this flush started the transaction so a failure (e.g. DuplicatedKeyException
    // surfaced by the bulk-edge index update introduced for issue #4113) doesn't leave the
    // database with the half-written batch visible to the next caller.
    final boolean startedTx = !database.isTransactionActive();

    // Number of buffered edges this flush is about to write. Read after the buffer is reset below, so it cannot
    // be replaced by edgeCount there.
    final int bufferedEdges = edgeCount;

    // How many of them are durably connected to their source vertex. Advanced by the connect pass itself, one
    // durable commit at a time, so a flush that dies part-way reports what the graph actually holds rather than
    // the whole flush or nothing (issue #6083 item 4).
    flushDurableOutEdges = 0;
    flushDurableOutRanges = null;

    try {
      // --- PHASE 1: Create edge records for edges that need them (non-light) ---
      // Hoisted out of PHASE 1 so the failure paths in PHASE 3 can reclaim the records this flush created but
      // never linked (issue #6083 item 2).
      final RID[] edgeRIDs = new RID[edgeCount];
      beginTx();

      // Count non-light edges and group by bucket for bulk creation
      int nonLightCount = 0;
      for (int i = 0; i < edgeCount; i++) {
        if (edgeIsLightweight[i])
          edgeRIDs[i] = new RID(edgeTypeBucketIds[i], -1L);
        else
          nonLightCount++;
      }

      if (nonLightCount > 0)
        createEdgeRecordsBulk(edgeRIDs, nonLightCount);


      // --- PHASE 2: Partition by source bucket (O(n) counting sort + per-bucket position sort) ---
      final int maxBucket = partitionBySourceBucket();

      // --- PHASE 3: Connect outgoing edges ---
      //
      // A failure here is CAPTURED rather than thrown (issue #6083 item 2), because part of the flush may already
      // be durable and the rest of this method is what makes that part a whole edge. Everything PHASE 3 did not
      // durably connect has had its edge record reclaimed by the time the capture happens; what remains is
      // OUT-connected and still owes its IN back-pointer, which PHASE 4 below queues for it exactly as it does on
      // the success path. Without that, a partially-failed flush left half-edges the integrity checker flags -
      // the same reason close() drains the deferred IN buffer on the way out of a failed batch.
      //
      // durableOutRanges describes what survived, as [from,to) pairs into sortIndex; null means "all of it", the
      // normal case, and keeps the success path free of the extra indirection.
      RuntimeException connectFailure = null;
      int[] durableOutRanges = null;

      if (parallelFlush) {
        // Commit edge records to release page locks before parallel work. From here on the records exist
        // durably whatever happens to the connect pass, which is why that pass reclaims its own failures.
        database.commit();
        try {
          connectOutgoingEdgesParallel(edgeRIDs, maxBucket);
        } catch (final RuntimeException e) {
          connectFailure = e;
          durableOutRanges = flushDurableOutRanges;
        }
      } else {
        try {
          // Commits internally, including the final one, and sets flushDurableOutEdges as it goes.
          connectOutgoingEdgesSorted(edgeRIDs);
        } catch (final RuntimeException e) {
          // With commitEvery > 0 this pass commits several times inside one flush, and the FIRST of those
          // commits also makes PHASE 1's edge records durable. Everything the pass had not linked by its last
          // durable commit is therefore a record no vertex points at (issue #6083 item 2). With commitEvery == 0
          // nothing committed, flushDurableOutEdges is still 0, and the rollback already undid the records -
          // which is exactly what the guard inside reclaimOrphanEdgeRecords reads.
          reclaimOrphanEdgeRecords(edgeRIDs, flushDurableOutEdges, bufferedEdges, flushDurableOutEdges > 0, e);
          connectFailure = e;
          durableOutRanges = new int[] { 0, flushDurableOutEdges };
        }
      }

      // --- PHASE 4: Accumulate incoming edges in deferred buffer (array-only, no DB) ---
      if (bidirectional) {
        accumulateIncomingEdges(edgeRIDs, durableOutRanges);

        // Drain early once the buffer crosses the configured cap (issue #5664): left alone, the deferred
        // incoming-edge arrays (and the in-chunk RID cache they populate) grow with the FULL stream length,
        // not with batchSize, and connectDeferredIncomingEdges() would land as one unbounded pass at close().
        // Draining here amortizes that pass over the load instead. maxDeferredIncomingEdges=0 opts back into
        // the pre-#5664 behavior of deferring everything to close().
        //
        // Skipped when PHASE 3 failed: close() drains the buffer on its way out of a failed batch anyway, and
        // draining here would replace the failure the caller has to see with whatever the drain hits.
        if (connectFailure == null && maxDeferredIncomingEdges > 0 && inEdgeCount >= maxDeferredIncomingEdges)
          connectDeferredIncomingEdges();
      }

      if (connectFailure != null)
        // Statistics and buffer reset are the catch block's job from here; it folds flushDurableOutEdges too.
        throw connectFailure;

      totalEdgesCreated += flushDurableOutEdges;
      totalFlushes++;
      totalFlushTimeNs += System.nanoTime() - startNs;

      // Reset buffer
      edgeCount = 0;
      // Clear property references to allow GC
      Arrays.fill(edgeProperties, 0, edgeProperties.length, null);
    } catch (final RuntimeException e) {
      if (startedTx && database.isTransactionActive())
        database.rollback();

      // A flush that died after PHASE 3 (in the deferred-incoming drain) still created every edge it counted:
      // those are connected to their source vertex and reachable, they are only missing a back-pointer that
      // close() drains separately. Counting them is right; the connect pass has already excluded whatever it
      // failed to make durable (issue #6083 item 4).
      totalEdgesCreated += flushDurableOutEdges;

      // Discard the buffered edges so a subsequent close() / flush() doesn't replay them.
      Arrays.fill(edgeProperties, 0, edgeProperties.length, null);
      edgeCount = 0;
      throw e;
    }
  }

  /**
   * Deletes the edge records this flush created but never linked to their source vertex (issue #6083 item 2).
   * <p>
   * An edge is written in two steps that do not share a transaction: PHASE 1 creates the record, PHASE 3 links it
   * into the source vertex's edge list. The parallel path has to commit between them (the edge-record pages must
   * be released before the per-bucket tasks touch them) and the sequential path does so too whenever
   * {@code commitEvery > 0}. So a connect pass that dies part-way leaves records that are durable and reachable
   * from nothing: {@code countType()} counts them and no traversal finds them.
   * <p>
   * CHECK DATABASE does not rescue them on its own. Since #6090 its {@code checkEdges} pass does REPORT them - it
   * names each one and counts them under {@code unreachableEdgeRecords} - but removing them is an explicit opt-in
   * ({@code CHECK DATABASE FIX DELETE ORPHANS}), never part of a plain {@code FIX}, because the same shape is what
   * a vertex that merely lost its head-chunk pointer looks like. Before that the finding was discarded entirely
   * into the aggregate {@code missingReferenceBack} counter, which a perfectly healthy UNIDIRECTIONAL edge also
   * raises. ({@code FIX} does reclaim orphaned edge SEGMENTS, but those are the internal linked-list chunks, not
   * the edge records at issue here.) So nothing cleans these up unattended, and the flush cleans up after itself,
   * before the failure propagates.
   * <p>
   * Deletion goes through {@code database.deleteRecord} rather than a direct bucket delete: an edge record may
   * carry index entries (issue #4113) and EXTERNAL property values, and this flush creates them by two different
   * routes - {@code createRecordsBulk} plus {@link #indexEdgeProperties} for a single-bucket flush, an ordinary
   * {@code save()} for a multi-type one. The database's own delete path is the one inverse that is correct for
   * both, for every index kind, without a second copy of the key-derivation logic to keep in step. It also walks
   * the endpoints' edge lists looking for a back-reference that is not there, which is provably wasted work here;
   * that cost is accepted for the correctness, and it is bounded because it only ever runs on a failed flush.
   * <p>
   * Best-effort by construction: whatever it cannot reclaim is logged and counted
   * ({@link #getOrphanEdgeRecordsLeaked()}), never allowed to replace the failure the caller is already handling.
   *
   * @param edgeRIDs          RIDs assigned in PHASE 1, indexed by edge-buffer slot
   * @param fromSortPos       first {@link #sortIndex} position to reclaim, inclusive
   * @param toSortPos         last {@link #sortIndex} position to reclaim, exclusive
   * @param recordsAreDurable whether PHASE 1's records outlived the failure. False when no commit intervened, in
   *                          which case the rollback already removed them and deleting again would be an error
   * @param cause             the failure being handled, for the log line only
   */
  private void reclaimOrphanEdgeRecords(final RID[] edgeRIDs, final int fromSortPos, final int toSortPos,
      final boolean recordsAreDurable, final Throwable cause) {
    if (!recordsAreDurable || fromSortPos >= toSortPos)
      return;

    // First position of the slice the current transaction covers. Everything before it has been through a
    // commit that returned, so its outcome is durable and already folded into the totals; everything from here
    // to toSortPos is still undecided. The catch block below needs it, so it lives outside the try.
    int uncommittedFrom = fromSortPos;

    try {
      // The failed attempt's transaction, if one is still open, holds writes that were never committed and must
      // not ride along with the deletions.
      if (database.isTransactionActive())
        database.rollback();

      // Counted for the current transaction only and folded into the instance totals as each commit returns
      // (issue #6083 review cycle 1). Deferring the fold to the end of the loop instead would discard the
      // reclaims an earlier commit already made durable if a LATER commit in the same pass threw, and the catch
      // block would then blame the whole range as leaked - under-reporting reclaims and double-counting records
      // that are in fact gone, on the one path whose entire purpose is to leave an accurate count behind.
      int pendingReclaimed = 0;
      int pendingLeaked = 0;
      int passReclaimed = 0;
      int passLeaked = 0;
      int inTx = 0;
      beginTx();

      for (int k = fromSortPos; k < toSortPos; k++) {
        final int idx = sortIndex[k];
        final RID edgeRID = edgeRIDs[idx];
        // A lightweight edge never allocated a record (its RID carries the placeholder position -1), so an
        // unconnected one leaves nothing behind to reclaim.
        if (edgeIsLightweight[idx] || edgeRID == null || edgeRID.getPosition() < 0)
          continue;

        try {
          database.deleteRecord(database.lookupByRID(edgeRID, true));
          pendingReclaimed++;
        } catch (final RecordNotFoundException e) {
          // Already gone - the failure took it with it. Nothing to reclaim and nothing wrong.
        } catch (final RuntimeException e) {
          pendingLeaked++;
          LogManager.instance().log(this, Level.WARNING,
              "GraphBatch: cannot reclaim orphan edge record %s left by a failed flush: %s", null, edgeRID,
              e.toString());
        }

        if (commitEvery > 0 && ++inTx >= commitEvery) {
          fireBeforeOrphanReclaimCommitHook();
          database.commit();
          totalOrphanEdgeRecordsReclaimed += pendingReclaimed;
          totalOrphanEdgeRecordsLeaked += pendingLeaked;
          passReclaimed += pendingReclaimed;
          passLeaked += pendingLeaked;
          pendingReclaimed = 0;
          pendingLeaked = 0;
          uncommittedFrom = k + 1;
          beginTx();
          inTx = 0;
        }
      }

      fireBeforeOrphanReclaimCommitHook();
      database.commit();
      totalOrphanEdgeRecordsReclaimed += pendingReclaimed;
      totalOrphanEdgeRecordsLeaked += pendingLeaked;
      passReclaimed += pendingReclaimed;
      passLeaked += pendingLeaked;

      if (passReclaimed > 0 || passLeaked > 0)
        LogManager.instance().log(this, Level.WARNING,
            "GraphBatch: an edge flush failed (%s) after its edge records were committed; reclaimed %d record(s) that "
                + "were left connected to no vertex, %d could not be removed", null,
            cause != null ? cause.toString() : "unknown", passReclaimed, passLeaked);
    } catch (final RuntimeException e) {
      // The cleanup is a courtesy on a path that is already failing; the caller's exception is the one that
      // describes what went wrong and it must be the one that propagates.
      //
      // Only the slice this transaction was covering is leaked. Anything before uncommittedFrom went through a
      // commit that returned and has already been counted, so blaming the whole range here would both lose those
      // reclaims and count records that are gone.
      final int leakedNow = countOrphanCandidates(edgeRIDs, uncommittedFrom, toSortPos);
      totalOrphanEdgeRecordsLeaked += leakedNow;
      LogManager.instance().log(this, Level.WARNING,
          "GraphBatch: could not reclaim the orphan edge records left by a failed flush; run CHECK DATABASE and "
              + "expect up to %d unreachable edge record(s)", e, leakedNow);
      if (database.isTransactionActive())
        try {
          database.rollback();
        } catch (final RuntimeException ignored) {
          // Nothing further to attempt; the original failure is still the one that propagates.
        }
    }
  }

  /**
   * Edges in {@code [fromSortPos, toSortPos)} that own a real edge record, and so leave one behind when they are
   * not connected. Lightweight edges are excluded: their RID carries the placeholder position -1 and no record
   * was ever allocated for them.
   */
  private int countOrphanCandidates(final RID[] edgeRIDs, final int fromSortPos, final int toSortPos) {
    int candidates = 0;
    for (int k = fromSortPos; k < toSortPos; k++) {
      final int idx = sortIndex[k];
      final RID edgeRID = edgeRIDs[idx];
      if (!edgeIsLightweight[idx] && edgeRID != null && edgeRID.getPosition() >= 0)
        candidates++;
    }
    return candidates;
  }

  /** Fires {@link #TEST_BEFORE_ORPHAN_RECLAIM_COMMIT_HOOK}, if set, right before an orphan-reclaim commit. */
  private void fireBeforeOrphanReclaimCommitHook() {
    final IntConsumer hook = TEST_BEFORE_ORPHAN_RECLAIM_COMMIT_HOOK;
    if (hook != null)
      hook.accept(orphanReclaimCommitAttempt.incrementAndGet());
  }

  /**
   * Pre-resolved metadata for serializing edges with the same set of property names.
   * Eliminates per-edge dictionary lookups, type resolution, MutableEdge allocation,
   * and HashMap operations.
   */
  private static class EdgeSerializationTemplate {
    final String[] propertyNames;
    final int[]    dictionaryIds;
    final byte[]   typeFlags;        // pre-resolved from schema, or -1 if schema-less
    final boolean  allTypesKnown;    // true if all types resolved from schema

    EdgeSerializationTemplate(final String[] propertyNames, final int[] dictionaryIds,
        final byte[] typeFlags, final boolean allTypesKnown) {
      this.propertyNames = propertyNames;
      this.dictionaryIds = dictionaryIds;
      this.typeFlags = typeFlags;
      this.allTypesKnown = allTypesKnown;
    }
  }

  // Cache for edge serialization templates, keyed by property signature
  private final Map<String, EdgeSerializationTemplate> templateCache = new HashMap<>();

  /**
   * Gets or creates a serialization template for edges with the given properties.
   */
  private EdgeSerializationTemplate getOrCreateTemplate(final Object[] props, final int edgeTypeBucketId) {
    // Build signature from property names
    final int propCount = props.length / 2;
    final StringBuilder sig = new StringBuilder(edgeTypeBucketId);
    for (int p = 0; p < propCount; p++) {
      if (p > 0)
        sig.append(',');
      sig.append(props[p * 2]);
    }
    final String key = sig.toString();

    EdgeSerializationTemplate template = templateCache.get(key);
    if (template != null)
      return template;

    // Create new template
    final Dictionary dictionary = database.getSchema().getDictionary();
    final DocumentType edgeType = database.getSchema().getTypeByBucketId(edgeTypeBucketId);
    final String[] names = new String[propCount];
    final int[] dictIds = new int[propCount];
    final byte[] types = new byte[propCount];
    boolean allKnown = true;

    for (int p = 0; p < propCount; p++) {
      names[p] = (String) props[p * 2];
      dictIds[p] = dictionary.getIdByName(names[p], true);
      final Property schemaProp = edgeType.getPropertyIfExists(names[p]);
      if (schemaProp != null)
        types[p] = schemaProp.getType().getBinaryType();
      else {
        types[p] = -1;
        allKnown = false;
      }
    }

    template = new EdgeSerializationTemplate(names, dictIds, types, allKnown);
    templateCache.put(key, template);
    return template;
  }

  /**
   * Serializes an edge directly into a Binary buffer using a pre-resolved template.
   * Avoids MutableEdge allocation, HashMap operations, dictionary lookups, and type resolution.
   */
  private Binary serializeEdgeDirect(final int outBucket, final long outPos,
      final int inBucket, final long inPos, final Object[] props,
      final EdgeSerializationTemplate template) {
    // Estimate buffer size: 1 (type) + ~10 (2 RIDs) + 4 (header size) + ~20 (header) + ~20 (content)
    final int propCount = template.dictionaryIds.length;
    final Binary buffer = new Binary(64 + propCount * 12);

    // Record type
    buffer.putByte(Edge.RECORD_TYPE);

    // OUT and IN compressed RIDs
    buffer.putNumber(outBucket);
    buffer.putNumber(outPos);
    buffer.putNumber(inBucket);
    buffer.putNumber(inPos);

    if (propCount == 0) {
      // No properties — write empty header
      buffer.putInt(buffer.position() + Binary.INT_SERIALIZED_SIZE);
      buffer.putUnsignedNumber(0);
      buffer.flip();
      return buffer;
    }

    // --- Properties header + content (two-pass within the same buffer) ---
    // Header: [4 bytes: headerEnd offset] [VLQ: propCount] [for each: VLQ dictId, VLQ contentOffset]
    // Content: [for each: 1 byte type, serialized value]

    // Write header placeholder for headerEnd
    final int headerSizePos = buffer.position();
    buffer.putInt(0);

    // Property count
    buffer.putUnsignedNumber(propCount);

    // Write dictionary IDs and reserve space for content offsets
    // We store the positions where content offsets need to be patched
    final int[] offsetPatchPositions = new int[propCount];
    for (int p = 0; p < propCount; p++) {
      buffer.putUnsignedNumber(template.dictionaryIds[p]);
      offsetPatchPositions[p] = buffer.position();
      // Reserve max 3 bytes for content offset (supports offsets up to ~2M)
      buffer.putByte((byte) 0);
      buffer.putByte((byte) 0);
      buffer.putByte((byte) 0);
    }

    // Update header end position
    final int headerEnd = buffer.position();
    buffer.putInt(headerSizePos, headerEnd);

    // Write content and patch offsets
    final int contentStart = headerEnd;
    for (int p = 0; p < propCount; p++) {
      final int contentOffset = buffer.position() - contentStart;

      // Patch the content offset in the header (overwrite the 3 reserved bytes)
      patchUnsignedNumber3(buffer, offsetPatchPositions[p], contentOffset);

      // Resolve type
      final Object value = props[p * 2 + 1];
      byte type = template.typeFlags[p];
      if (type == -1)
        type = BinaryTypes.getTypeFromValue(value, null);

      // Write type tag + value
      buffer.putByte(type);
      serializeValueDirect(buffer, type, value);
    }

    buffer.flip();
    return buffer;
  }

  /**
   * Writes a VLQ unsigned number into exactly 3 pre-reserved bytes.
   * The number must fit in 21 bits (values up to 2,097,151).
   */
  private static void patchUnsignedNumber3(final Binary buffer, final int position, final int value) {
    buffer.putByte(position, (byte) ((value & 0x7F) | 0x80));
    buffer.putByte(position + 1, (byte) (((value >>> 7) & 0x7F) | 0x80));
    buffer.putByte(position + 2, (byte) ((value >>> 14) & 0x7F));
  }

  /**
   * Serializes a property value directly into a Binary buffer.
   * Handles the most common types used in edge properties.
   */
  private void serializeValueDirect(final Binary buffer, final byte type, final Object value) {
    if (value == null)
      return;
    switch (type) {
    case BinaryTypes.TYPE_INT:
      buffer.putNumber(((Number) value).intValue());
      break;
    case BinaryTypes.TYPE_LONG:
      buffer.putNumber(((Number) value).longValue());
      break;
    case BinaryTypes.TYPE_SHORT:
      buffer.putNumber(((Number) value).shortValue());
      break;
    case BinaryTypes.TYPE_FLOAT:
      buffer.putNumber(Float.floatToIntBits(((Number) value).floatValue()));
      break;
    case BinaryTypes.TYPE_DOUBLE:
      buffer.putNumber(Double.doubleToLongBits(((Number) value).doubleValue()));
      break;
    case BinaryTypes.TYPE_BYTE:
      buffer.putByte((Byte) value);
      break;
    case BinaryTypes.TYPE_BOOLEAN:
      buffer.putByte((byte) ((Boolean) value ? 1 : 0));
      break;
    case BinaryTypes.TYPE_STRING:
      buffer.putString(value.toString());
      break;
    case BinaryTypes.TYPE_COMPRESSED_STRING:
      buffer.putUnsignedNumber((Integer) value);
      break;
    case BinaryTypes.TYPE_COMPRESSED_RID:
      final RID rid = ((Identifiable) value).getIdentity();
      buffer.putNumber(rid.getBucketId());
      buffer.putNumber(rid.getPosition());
      break;
    default:
      // Fall back to the full serializer for complex types
      database.getSerializer().serializeValue(database, buffer, type, value);
      break;
    }
  }

  /**
   * Bulk-creates edge records using template-based serialization and sequential page writes.
   * For each unique set of property names, a template is created once that pre-resolves
   * dictionary IDs and type tags. Edges are then serialized directly into Binary buffers
   * without MutableEdge allocation or HashMap operations.
   */
  private void createEdgeRecordsBulk(final RID[] edgeRIDs, final int nonLightCount) {
    // Collect indices of non-light edges
    final int[] nonLightIndices = new int[nonLightCount];
    int nlIdx = 0;
    for (int i = 0; i < edgeCount; i++)
      if (edgeRIDs[i] == null)
        nonLightIndices[nlIdx++] = i;

    // Serialize all edge records using templates
    final Binary[] serializedBuffers = new Binary[nonLightCount];

    for (int k = 0; k < nonLightCount; k++) {
      final int i = nonLightIndices[k];
      final Object[] props = edgeProperties[i];

      if (props != null && props.length > 0) {
        final EdgeSerializationTemplate template = getOrCreateTemplate(props, edgeTypeBucketIds[i]);
        serializedBuffers[k] = serializeEdgeDirect(
            edgeSrcBucketIds[i], edgeSrcPositions[i],
            edgeDstBucketIds[i], edgeDstPositions[i],
            props, template);
      } else {
        // No properties — serialize minimal edge
        serializedBuffers[k] = serializeEdgeDirect(
            edgeSrcBucketIds[i], edgeSrcPositions[i],
            edgeDstBucketIds[i], edgeDstPositions[i],
            null, new EdgeSerializationTemplate(new String[0], new int[0], new byte[0], true));
      }
    }

    // Group by bucket and bulk-write
    final int firstBucket = edgeTypeBucketIds[nonLightIndices[0]];
    boolean singleBucket = true;
    for (int k = 1; k < nonLightCount; k++) {
      if (edgeTypeBucketIds[nonLightIndices[k]] != firstBucket) {
        singleBucket = false;
        break;
      }
    }

    if (singleBucket) {
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(firstBucket);
      final RID[] bulkRIDs = new RID[nonLightCount];
      bucket.createRecordsBulk(serializedBuffers, 0, nonLightCount, bulkRIDs);

      // Resolve indexes on this edge bucket once per flush (issue #4113):
      // the bulk write skips LocalDatabase.createRecord, so unique/non-unique indexes are not
      // updated unless we re-apply them here. Index entries scheduled in the transaction
      // surface DuplicatedKeyException eagerly when a unique constraint is violated.
      final List<IndexInternal> bucketIndexes = database.getSchema()
          .getTypeByBucketId(firstBucket).getPolymorphicBucketIndexByBucketId(firstBucket, null);

      for (int k = 0; k < nonLightCount; k++) {
        final int i = nonLightIndices[k];
        edgeRIDs[i] = bulkRIDs[k];
        database.getTransaction().updateBucketRecordDelta(firstBucket, +1);

        if (!bucketIndexes.isEmpty())
          indexEdgeProperties(bucketIndexes, bulkRIDs[k],
              edgeSrcBucketIds[i], edgeSrcPositions[i],
              edgeDstBucketIds[i], edgeDstPositions[i],
              edgeProperties[i]);
      }
    } else {
      // Multiple buckets — fall back to per-edge standard creation
      for (int k = 0; k < nonLightCount; k++) {
        final int i = nonLightIndices[k];
        final EdgeType edgeType = (EdgeType) database.getSchema().getTypeByBucketId(edgeTypeBucketIds[i]);
        final RID srcRID = new RID(edgeSrcBucketIds[i], edgeSrcPositions[i]);
        final RID dstRID = new RID(edgeDstBucketIds[i], edgeDstPositions[i]);

        final MutableEdge edge = new MutableEdge(database, edgeType, srcRID, dstRID);
        if (edgeHasProperties[i])
          GraphEngine.setProperties(edge, edgeProperties[i]);
        edge.save();
        edgeRIDs[i] = edge.getIdentity();
      }
    }
  }

  /**
   * Registers a freshly bulk-created edge into the indexes defined on its bucket.
   * Mirrors the relevant portion of {@link com.arcadedb.database.DocumentIndexer#createDocument}
   * but reads property values straight from the flat edge buffer instead of materializing
   * a {@link MutableEdge}. The pseudo-properties {@code @out} and {@code @in} resolve to
   * the source / destination vertex RIDs (issue #4113).
   */
  private void indexEdgeProperties(final List<IndexInternal> indexes, final RID rid,
      final int srcBucket, final long srcPosition, final int dstBucket, final long dstPosition,
      final Object[] properties) {
    final RID[] ridArr = { rid };
    for (final Index index : indexes) {
      final List<String> keyNames = index.getPropertyNames();
      final int keyCount = keyNames.size();
      final Object[] keyValues = new Object[keyCount];
      for (int p = 0; p < keyCount; p++) {
        final String name = keyNames.get(p);
        if ("@out".equals(name))
          keyValues[p] = new RID(srcBucket, srcPosition);
        else if ("@in".equals(name))
          keyValues[p] = new RID(dstBucket, dstPosition);
        else
          keyValues[p] = lookupEdgeProperty(properties, name);
      }
      index.put(keyValues, ridArr);
    }
  }

  /**
   * Looks up a property value from the flat {@code [key, value, key, value, ...]} edge
   * buffer used by {@link #newEdge}. Returns {@code null} when the property is absent.
   */
  private static Object lookupEdgeProperty(final Object[] properties, final String name) {
    if (properties == null)
      return null;
    for (int i = 0; i < properties.length; i += 2) {
      if (name.equals(properties[i]))
        return properties[i + 1];
    }
    return null;
  }

  @Override
  public void close() {
    // The guard release sits in the outermost finally on purpose: every other exit of this method throws,
    // and a guard left behind locks the database out of batching until the process restarts (issue #5666).
    try {
      RuntimeException flushFailure = null;

      // Flush any remaining outgoing edges. Capture rather than rethrow so we can still drain the
      // deferred IN buffer for previously-flushed edges; otherwise a unique-constraint violation
      // on the trailing buffer (issue #4113) would leave already-persisted edges with no
      // back-pointer and trip the database integrity checker.
      try {
        flush();
      } catch (final RuntimeException e) {
        flushFailure = e;
      }

      try {
        // Connect all deferred incoming edges in one sorted pass. On a large load this is minutes of work and it
        // runs on the way out of a FAILED batch too - it has to, or the edges already persisted keep no back-pointer
        // and the integrity checker trips (see #4113 above). That is why a rejected batch can take a while to answer
        // (86 seconds of the timeline on issue #5470); connectDeferredIncomingEdges logs the pass and its duration
        // itself, so the wait is already accounted for.
        if (bidirectional && inEdgeCount > 0)
          connectDeferredIncomingEdges();

        // Batch-update all vertex head chunk pointers in one pass
        if (!deferredOutHead.isEmpty() || !deferredInHead.isEmpty())
          batchUpdateVertexHeadChunks();
      } finally {
        // Restore database settings, even on an exceptional exit (issue #5378)
        restoreDatabaseSettings();
      }

      LogManager.instance().log(this, Level.INFO,
          "GraphBatch closed: vertices=%d edges=%d flushes=%d avgFlushMs=%.1f",
          null, totalVerticesCreated, totalEdgesCreated, totalFlushes,
          totalFlushes > 0 ? (totalFlushTimeNs / totalFlushes) / 1_000_000.0 : 0.0);

      if (flushFailure != null)
        throw flushFailure;
    } finally {
      releaseBatchGuard();
    }
  }

  /**
   * Gives up the single-batch slot of the database without flushing anything. For callers that abandon a
   * failed batch on a thread that cannot afford the cost of a full {@link #close()}: whatever is still
   * buffered is dropped. Calling {@link #close()} afterwards remains safe and releases nothing twice.
   * <p>
   * The read-your-writes policy is database wide and was relaxed for the load, so it is put back here: a
   * batch that ends this way is never closed and would otherwise leave every later reader on that database
   * unable to see its own writes. The per-thread transaction WAL policy is not, because it lives on the
   * {@code TransactionContext} of the thread that opened the batch and this method is expected to run on
   * another one. The async executor's WAL policy (issue #5665) IS restored here: unlike the per-thread
   * one, it is database-wide and, when {@code parallelFlush} is true, was already relaxed at construction
   * regardless of whether any flush ever ran - leaving it behind would permanently downgrade durability
   * for every other async caller on this database.
   */
  public void abandon() {
    database.setReadYourWrites(savedReadYourWrites);
    restoreAsyncSettings();
    releaseBatchGuard();
  }

  /**
   * Hands the single-batch slot back to the database that granted it. Idempotent: a batch releases at most
   * once, so a double close cannot free a slot that a later batch has meanwhile taken.
   */
  private void releaseBatchGuard() {
    if (guardOwner != null && guardReleased.compareAndSet(false, true))
      guardOwner.batchFinished();
  }

  /**
   * Puts back the database/transaction settings relaxed for the bulk load. The WAL policy is per-transaction
   * but the {@code TransactionContext} is reused across transactions on the same thread, so failing to restore
   * it here would silently downgrade the durability of every later transaction on this thread (issue #5378).
   */
  private void restoreDatabaseSettings() {
    database.setReadYourWrites(savedReadYourWrites);
    restoreAsyncSettings();

    // If the thread still has no TransactionContext (the batch never began a transaction), nothing leaked.
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx != null) {
      tx.setUseWAL(savedUseWAL);
      tx.setWALFlush(savedWALFlush);
    }

    LogManager.instance().log(this, Level.FINE, "GraphBatch: restored WAL settings useWAL=%s walFlush=%s", savedUseWAL,
        savedWALFlush);
  }

  /**
   * Puts back the async executor's WAL policy relaxed once at construction for {@code parallelFlush}
   * (issue #5665). Guarded on an actual change for the same reason as the constructor: the setters are
   * not, so calling them unconditionally would tear down and respawn the worker pool for nothing. A
   * no-op when {@code parallelFlush} is false (the policy was never touched) or when called twice
   * (idempotent: {@link #abandon()} and {@link #close()} can both reach this).
   */
  private void restoreAsyncSettings() {
    if (!parallelFlush)
      return;

    final DatabaseAsyncExecutor asyncExecutor = database.async();
    final boolean           currentAsyncUseWAL   = asyncExecutor.isTransactionUseWAL();
    final WALFile.FlushType currentAsyncWALFlush = asyncExecutor.getTransactionSync();

    final boolean walChanged = currentAsyncUseWAL != savedAsyncUseWAL;
    final boolean flushChanged = currentAsyncWALFlush != savedAsyncWALFlush;

    if (walChanged)
      asyncExecutor.setTransactionUseWAL(savedAsyncUseWAL);
    if (flushChanged)
      asyncExecutor.setTransactionSync(savedAsyncWALFlush);

    if (walChanged || flushChanged)
      LogManager.instance().log(this, Level.INFO,
          "GraphBatch: restored async executor durability (useWAL %s->%s, walFlush %s->%s)", currentAsyncUseWAL,
          savedAsyncUseWAL, currentAsyncWALFlush, savedAsyncWALFlush);
  }

  /**
   * Number of edges durably connected to their source vertex so far.
   * <p>
   * EXACT since issue #6083 item 4, on a failed load as well as a successful one: it advances one durable commit
   * at a time inside the connect pass rather than a whole flush at a time, so a load dying mid-flush reports the
   * edges the graph actually holds instead of rounding the part-written flush down to zero. A caller reconciling
   * a failed bulk load against this figure re-sends exactly what is missing - it no longer has to over-send a
   * flush's worth of edges and rely on the duplicates being harmless.
   */
  public long getTotalEdgesCreated() {
    return totalEdgesCreated;
  }

  /**
   * Edge records this batch created, failed to connect to any vertex, and deleted again (issue #6083 item 2).
   * Non-zero only after a flush failed part-way.
   */
  public long getOrphanEdgeRecordsReclaimed() {
    return totalOrphanEdgeRecordsReclaimed;
  }

  /**
   * Edge records left connected to no vertex because the cleanup after a failed flush could not remove them
   * (issue #6083 item 2). Non-zero means unreachable records are still in the database.
   */
  public long getOrphanEdgeRecordsLeaked() {
    return totalOrphanEdgeRecordsLeaked;
  }

  /**
   * Returns the number of edges currently buffered (not yet flushed).
   */
  public int getBufferedEdgeCount() {
    return edgeCount;
  }

  /**
   * Returns the number of deferred incoming edges waiting to be connected at close().
   */
  public int getDeferredIncomingEdgeCount() {
    return inEdgeCount;
  }

  /**
   * Test-only accessor: current size of the OUT head-chunk RID cache. Used to verify the bound from issue #5664
   * holds regardless of how many distinct vertices the stream has touched.
   */
  int getOutChunkRIDCacheSize() {
    return outChunkRIDCache.size();
  }

  /**
   * Test-only accessor: current size of the IN head-chunk RID cache. Used to verify the bound from issue #5664
   * holds regardless of how many distinct vertices the stream has touched.
   */
  int getInChunkRIDCacheSize() {
    return inChunkRIDCache.size();
  }

  /**
   * Number of records written inside one transaction during an edge flush, or 0 when the whole flush is
   * committed at once. On a replicated database this is also the size of a single Raft entry, so it bounds
   * how large a replicated entry can get (issue #5470).
   */
  public int getCommitEvery() {
    return commitEvery;
  }

  /**
   * Whether the bulk load writes the WAL. Always true on a replicated database, whatever the builder was
   * asked for, because replication ships the WAL bytes to the followers (issue #4076).
   */
  public boolean isUseWAL() {
    return useWAL;
  }

  // ---------------------------------------------------------------------------
  // Batch vertex head chunk update
  // ---------------------------------------------------------------------------

  /**
   * Updates all vertex records with their final OUT and IN head chunk pointers in a single pass.
   * Each vertex is loaded once, both pointers are set if needed, and the vertex is saved once.
   * This eliminates per-vertex I/O during flush() and connectDeferredIncomingEdges().
   */
  private void batchUpdateVertexHeadChunks() {
    LogManager.instance().log(this, Level.INFO,
        "Batch updating %d OUT + %d IN vertex head chunk pointers...",
        null, deferredOutHead.size(), deferredInHead.size());

    final long startNs = System.nanoTime();

    // Collect all vertex keys that need updating. Using LongHashSet (zero-boxing,
    // open-addressing) instead of HashSet<Long> avoids ~70 bytes/entry of overhead
    // and Long boxing on every addAll, which matters during 100K+ entry bulk loads.
    final LongHashSet allKeys = new LongHashSet(deferredOutHead.size() + deferredInHead.size());
    deferredOutHead.forEach((k, v) -> allKeys.add(k));
    deferredInHead.forEach((k, v) -> allKeys.add(k));

    // Sort by vertex key for page locality.
    // NOTE (concurrency): Arrays.parallelSort forks to the JDK common ForkJoinPool, which is
    // discouraged elsewhere in the engine (see QueryEngineManager class javadoc, "No JDK common
    // ForkJoinPool" rule). Tolerated here because GraphBatch runs during bulk import - a
    // foreground operational task, not a hot per-query path - and the sort dominates its own
    // critical section. Migrate to QueryEngineManager.getExecutorService() if profiling later
    // shows common-pool contention with user code during overlapping bulk loads.
    final long[] sortedKeys = allKeys.toArray();
    Arrays.parallelSort(sortedKeys);

    beginTx();
    int updated = 0;

    for (final long vertexKey : sortedKeys) {
      final int bucketId = (int) (vertexKey >>> 40);
      final long position = vertexKey & 0xFFFFFFFFFFL;

      MutableVertex vertex = ((Vertex) database.lookupByRID(new RID(bucketId, position), true)).modify();

      final RID outHead = deferredOutHead.get(vertexKey);
      if (outHead != null)
        vertex.setOutEdgesHeadChunk(outHead);

      final RID inHead = deferredInHead.get(vertexKey);
      if (inHead != null)
        vertex.setInEdgesHeadChunk(inHead);

      database.updateRecord(vertex);
      updated++;

      if (commitEvery > 0 && updated % commitEvery == 0) {
        database.commit();
        beginTx();
      }
    }

    database.commit();
    deferredOutHead.clear();
    deferredInHead.clear();
    knownNewVertexKeys.clear();

    final double ms = (System.nanoTime() - startNs) / 1_000_000.0;
    LogManager.instance().log(this, Level.INFO,
        "Batch vertex update: %d vertices in %.1f ms", null, updated, ms);
  }

  // ---------------------------------------------------------------------------
  // Transaction helpers
  // ---------------------------------------------------------------------------

  private void beginTx() {
    if (!database.isTransactionActive())
      database.begin();
    // Apply WAL settings to the current transaction
    database.getTransaction().setUseWAL(useWAL);
    database.getTransaction().setWALFlush(walFlush);
  }

  /**
   * Packs a vertex (bucketId, position) into a single long for use as a HashMap key.
   * Supports up to 24-bit bucket IDs and 40-bit positions (1 trillion records per bucket).
   */
  private static long packVertexKey(final int bucketId, final long position) {
    return ((long) bucketId << 40) | (position & 0xFFFFFFFFFFL);
  }

  // ---------------------------------------------------------------------------
  // Sorted outgoing edge connection
  // ---------------------------------------------------------------------------

  private void connectOutgoingEdgesSorted(final RID[] edgeRIDs) {
    int i = 0;
    int edgesInBatch = 0;

    // getOrCreateOutSegmentDeferred/persistNewSegment/addEdgesToSegmentBulkLazy write deferredOutHead and
    // outChunkRIDCache as they process a group, BEFORE the transaction holding that group's writes commits. If
    // that commit never happens, the maps are left naming segment records the rollback undid - and
    // batchUpdateVertexHeadChunks() at close() then stamps one of those dead RIDs onto the vertex, which CHECK
    // DATABASE reports as "out edges record is not valid" and which no later read can follow.
    //
    // Exactly the hazard #5950 cycle 3 fixed for the IN direction (see connectIncomingEdgesSequential's
    // inHeadUndoLog) and cycle 4 for the parallel OUT direction (the per-bucket local maps); this path was the
    // one left. Same remedy: snapshot each touched vertex's PRE-slice deferredOutHead value once per slice, and
    // on failure restore it exactly - "was absent" via remove(), "had segment X" back to X (issue #6083).
    //
    // LongObjectHashMap, not HashMap<Long, RID>: this is allocated on EVERY sequential flush and takes an entry
    // per distinct source vertex in the slice, so it is exactly the shape the zero-boxing map exists for (~16
    // bytes/entry against 72-90). A null VALUE is what "the vertex had no deferred head" is recorded as, which
    // this map represents faithfully - occupancy lives in its key array, so containsKey() still answers true for
    // a key put with a null value (issue #6083 review cycle 1).
    final LongObjectHashMap<RID> outHeadUndoLog = new LongObjectHashMap<>();

    try {
      while (i < edgeCount) {
        final int idx = sortIndex[i];
        final int srcBucket = edgeSrcBucketIds[idx];
        final long srcPos = edgeSrcPositions[idx];

        // Collect all edges from the same source vertex
        int j = i;
        while (j < edgeCount) {
          final int jIdx = sortIndex[j];
          if (edgeSrcBucketIds[jIdx] != srcBucket || edgeSrcPositions[jIdx] != srcPos)
            break;
          j++;
        }

        // Pre-compute exact bytes needed for this group
        final int groupSize = j - i;
        ensureTmpArrays(groupSize);
        int totalBytesNeeded = 0;
        for (int k = 0; k < groupSize; k++) {
          final int kIdx = sortIndex[i + k];
          tmpEdgeBucketIds[k] = edgeRIDs[kIdx].getBucketId();
          tmpEdgePositions[k] = edgeRIDs[kIdx].getPosition();
          tmpVertexBucketIds[k] = edgeDstBucketIds[kIdx];
          tmpVertexPositions[k] = edgeDstPositions[kIdx];
          totalBytesNeeded += Binary.getNumberSpace(tmpEdgeBucketIds[k]) + Binary.getNumberSpace(tmpEdgePositions[k])
              + Binary.getNumberSpace(tmpVertexBucketIds[k]) + Binary.getNumberSpace(tmpVertexPositions[k]);
        }

        // Get or create segment — deferred vertex update, no vertex load for known-new vertices
        final long vertexKey = packVertexKey(srcBucket, srcPos);
        // Snapshot BEFORE getOrCreateOutSegmentDeferred can mutate deferredOutHead for this vertex. A promoted
        // vertex (below) touches neither map, so the snapshot is harmlessly unused in that case.
        if (!outHeadUndoLog.containsKey(vertexKey))
          outHeadUndoLog.put(vertexKey, deferredOutHead.get(vertexKey));
        final EdgeSegment outChunk = getOrCreateOutSegmentDeferred(srcBucket, srcPos, vertexKey, totalBytesNeeded);

        // NOTE (edge-append merge): this bulk path intentionally neither tracks (trackEdgeAppend) nor poisons its
        // chunk pages. Since #5596 that needs no side condition: the merge accepts a page only when EVERY byte this
        // transaction wrote to it was declared replayable, and these bulk writes declare nothing - so a page this
        // path touched can never be rebased, even if the same transaction also drove EdgeLinkedList.add on it.
        // See docs/supernode.md §3.
        if (lastSegmentPromoted) {
          // #5667: the vertex is already a promoted super-node - route this group through the standard,
          // MVCC-safe StripedEdgeList write path instead of the bulk segment path.
          addGroupThroughStripedEdgeList(lastPromotedVertex, Vertex.DIRECTION.OUT, lastPromotedDirectory,
              tmpEdgeBucketIds, tmpEdgePositions, tmpVertexBucketIds, tmpVertexPositions, groupSize);
        } else if (lastSegmentIsNew) {
          // New segment: fill FIRST, then persist ONCE (no updateRecord needed)
          outChunk.addManyAtEndDirect(tmpEdgeBucketIds, tmpEdgePositions,
              tmpVertexBucketIds, tmpVertexPositions, 0, groupSize);
          persistNewSegment(outChunk, srcBucket, Vertex.DIRECTION.OUT, vertexKey);
        } else {
          // Existing segment: check if all edges fit
          final int available = outChunk.getRecordSize() - outChunk.getUsed();
          if (totalBytesNeeded <= available) {
            outChunk.addManyAtEndDirect(tmpEdgeBucketIds, tmpEdgePositions,
                tmpVertexBucketIds, tmpVertexPositions, 0, groupSize);
            database.updateRecord(outChunk);
          } else {
            // Slow path: existing segment overflows
            addEdgesToSegmentBulkLazy(srcBucket, srcPos, Vertex.DIRECTION.OUT, outChunk, edgeRIDs, i, j, true);
          }
        }

        edgesInBatch += groupSize;

        // Periodic commit for very large flushes
        if (commitEvery > 0 && edgesInBatch >= commitEvery) {
          database.commit();
          // Only AFTER the commit returns: sortIndex positions [0, j) are now durably linked, and this is what
          // tells a failed flush which records it still has to reclaim and how many edges it may claim (issue
          // #6083 items 2 and 4). The slice's head pointers are durable too, so its undo entries can go.
          flushDurableOutEdges = j;
          outHeadUndoLog.clear();
          beginTx();
          edgesInBatch = 0;
        }

        i = j;
      }

      // Inside the try, unlike before issue #6083: a final commit that fails rolls back the last slice's
      // segments too, and the undo log is the only thing that can take their RIDs back out of deferredOutHead.
      database.commit();
      flushDurableOutEdges = edgeCount;
    } catch (final RuntimeException e) {
      outHeadUndoLog.forEach((key, previous) -> {
        if (previous == null)
          deferredOutHead.remove(key);
        else
          deferredOutHead.put(key, previous);
        // Just an accelerator cache - evicting is always safe, forces a deferredOutHead/disk fallback.
        outChunkRIDCache.remove(key);
      });
      throw e;
    }
  }

  // ---------------------------------------------------------------------------
  // Deferred incoming edge accumulation and connection
  // ---------------------------------------------------------------------------

  /**
   * Accumulates incoming edge info from the current flush buffer into the deferred arrays.
   *
   * @param durableOutRanges {@code [from,to)} pairs into {@link #sortIndex} selecting the edges PHASE 3 durably
   *                         connected to their source vertex, or {@code null} for the whole buffer - the normal
   *                         case, where every edge was connected. A partially-failed flush passes only what
   *                         survived, so the IN pass at {@link #close()} completes exactly those edges and never
   *                         writes a back-pointer to an edge record the failure took away (issue #6083 item 2).
   */
  private void accumulateIncomingEdges(final RID[] edgeRIDs, final int[] durableOutRanges) {
    if (durableOutRanges == null) {
      accumulateIncomingEdgeRange(edgeRIDs, 0, edgeCount, false);
      return;
    }
    for (int r = 0; r < durableOutRanges.length; r += 2)
      accumulateIncomingEdgeRange(edgeRIDs, durableOutRanges[r], durableOutRanges[r + 1], true);
  }

  /**
   * Appends one contiguous run of the flush buffer to the deferred incoming-edge arrays. Positions are read
   * straight out of the buffer when {@code throughSortIndex} is false, and through {@link #sortIndex} when it is
   * true - the two orders differ, but the drain re-sorts by destination bucket anyway, so only membership matters.
   */
  private void accumulateIncomingEdgeRange(final RID[] edgeRIDs, final int from, final int to,
      final boolean throughSortIndex) {
    if (from >= to)
      return;
    // An earlier in-flush drain (issue #5664) frees these arrays at the end of connectDeferredIncomingEdges();
    // re-allocate at the same initial size the constructor would have used.
    if (inEdgeBucketIds == null) {
      final int initialInCapacity = batchSize;
      inEdgeBucketIds = new int[initialInCapacity];
      inEdgePositions = new long[initialInCapacity];
      inVertexBucketIds = new int[initialInCapacity];
      inVertexPositions = new long[initialInCapacity];
      inDstBucketIds = new int[initialInCapacity];
      inDstPositions = new long[initialInCapacity];
    }

    final int needed = inEdgeCount + (to - from);
    if (needed > inEdgeBucketIds.length)
      growIncomingBuffers(needed);

    for (int k = from; k < to; k++) {
      final int i = throughSortIndex ? sortIndex[k] : k;
      final int pos = inEdgeCount;
      inEdgeBucketIds[pos] = edgeRIDs[i].getBucketId();
      inEdgePositions[pos] = edgeRIDs[i].getPosition();
      inVertexBucketIds[pos] = edgeSrcBucketIds[i];   // source vertex = the "from" for incoming
      inVertexPositions[pos] = edgeSrcPositions[i];
      inDstBucketIds[pos] = edgeDstBucketIds[i];      // destination = the vertex receiving the incoming edge
      inDstPositions[pos] = edgeDstPositions[i];
      inEdgeCount++;
    }
  }

  private void growIncomingBuffers(final int minCapacity) {
    int newCap = inEdgeBucketIds.length;
    while (newCap < minCapacity)
      newCap = newCap * 2;

    inEdgeBucketIds = Arrays.copyOf(inEdgeBucketIds, newCap);
    inEdgePositions = Arrays.copyOf(inEdgePositions, newCap);
    inVertexBucketIds = Arrays.copyOf(inVertexBucketIds, newCap);
    inVertexPositions = Arrays.copyOf(inVertexPositions, newCap);
    inDstBucketIds = Arrays.copyOf(inDstBucketIds, newCap);
    inDstPositions = Arrays.copyOf(inDstPositions, newCap);
  }

  /**
   * Connects all deferred incoming edges in a single sorted pass. Called once at close().
   * This is the "Phase 3" from Neo4j's batch importer: sort by destination vertex
   * so each vertex's segment is loaded, filled, and written exactly once.
   */
  private void connectDeferredIncomingEdges() {
    // Drains in PINNED PREFIXES rather than straight over inEdgeCount (issue #5950 review cycle 4).
    //
    // The resume state (inEdgesResumeSortIndex, completedIncomingBuckets) is expressed against the sort
    // index built at the top of a pass. That index is a counting sort over the buffer, so appending rows
    // changes bucketCounts/bucketOffsets and therefore what a given raw index - and a given bucket's
    // [from,to) range - refers to. A caller that catches the exception from a failed early drain and keeps
    // calling newEdge() (nothing forbids it, and this class supports exactly that "absorb a transient
    // failure and keep streaming" shape elsewhere - see createVerticesWithRetry for #4724) would grow the
    // buffer between the failed attempt and the retry, and the resume state would then be applied to a
    // differently-shaped index: silently skipping groups (edge loss) or reprocessing them (duplicates).
    //
    // Pinning the row count a failed pass covered, and reusing that exact pin on the retry, keeps the
    // index shape identical across attempts. Rows appended after the pin are drained by a later iteration
    // of the loop below, against their own freshly-built index.
    while (inEdgeCount > 0) {
      final int prefix = inEdgesDrainPrefix > 0 ? inEdgesDrainPrefix : inEdgeCount;
      inEdgesDrainPrefix = prefix;

      LogManager.instance().log(this, Level.INFO,
          "Connecting %d deferred incoming edges...", null, prefix);

      final long startNs = System.nanoTime();

      // Build sort index over the pinned prefix only, partitioned by destination bucket (O(n) counting sort)
      final int[] inSortIndex = new int[prefix];
      final int maxDstBucket = partitionIncomingByDestBucket(inSortIndex, prefix);

      if (parallelFlush)
        connectIncomingEdgesParallel(inSortIndex, maxDstBucket);
      else
        connectIncomingEdgesSequential(inSortIndex, prefix);

      final double ms = (System.nanoTime() - startNs) / 1_000_000.0;
      LogManager.instance().log(this, Level.INFO,
          "Incoming edges connected: %d edges in %.1f ms (%.0f edges/sec)",
          null, prefix, ms, prefix / (ms / 1000.0));

      // This prefix is fully durable (an exception from either connect* method above skips this point
      // entirely, leaving the pin and the resume state in place for the retry). Drop it from the buffer and
      // clear the resume state so the next iteration - or the next drain - starts clean.
      discardDrainedIncomingPrefix(prefix);
      inEdgesDrainPrefix = 0;
      inEdgesResumeSortIndex = 0;
      completedIncomingBuckets.clear();
    }

    // Fully drained - release the buffers so a long-lived batch does not hold them between drains.
    inEdgeBucketIds = null;
    inEdgePositions = null;
    inVertexBucketIds = null;
    inVertexPositions = null;
    inDstBucketIds = null;
    inDstPositions = null;
  }

  /**
   * Removes the first {@code prefix} rows from the deferred incoming buffer, shifting anything appended
   * after the pin down to the front. On the normal path {@code prefix == inEdgeCount}, so this is just a
   * counter reset with no copying (issue #5950 review cycle 4).
   */
  private void discardDrainedIncomingPrefix(final int prefix) {
    final int remaining = inEdgeCount - prefix;
    if (remaining > 0) {
      System.arraycopy(inEdgeBucketIds, prefix, inEdgeBucketIds, 0, remaining);
      System.arraycopy(inEdgePositions, prefix, inEdgePositions, 0, remaining);
      System.arraycopy(inVertexBucketIds, prefix, inVertexBucketIds, 0, remaining);
      System.arraycopy(inVertexPositions, prefix, inVertexPositions, 0, remaining);
      System.arraycopy(inDstBucketIds, prefix, inDstBucketIds, 0, remaining);
      System.arraycopy(inDstPositions, prefix, inDstPositions, 0, remaining);
    }
    inEdgeCount = remaining;
  }

  /** Fires {@link #TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK}, if set, right before a deferred incoming-edge commit. */
  private void fireBeforeIncomingEdgeCommitHook() {
    final IntConsumer hook = TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK;
    if (hook != null)
      hook.accept(incomingEdgeCommitAttempt.incrementAndGet());
  }

  private void connectIncomingEdgesSequential(final int[] inSortIndex, final int count) {
    beginTx();
    // Resume after the last durably committed group instead of recommitting it, if a previous attempt
    // at this drain partially succeeded before failing (issue #5950 review cycle 3).
    int i = inEdgesResumeSortIndex;
    int edgesInBatch = 0;

    // getOrCreateInSegmentDeferred/persistNewSegment/addIncomingEdgesToSegmentBulkLazy mutate
    // deferredInHead/inChunkRIDCache as a side effect of processing a group, BEFORE the transaction
    // containing that group's writes commits. If the commit that would make those writes durable never
    // happens (an exception anywhere in this slice), the maps are left pointing at segment records from
    // a transaction the caller is about to roll back. Snapshot each touched vertex's PRE-slice
    // deferredInHead value (only once per vertex per slice) so a failure can restore it exactly -
    // "was absent" restores via remove(), "had segment X" restores X - before rethrowing (issue #5950
    // review cycle 3).
    //
    // LongObjectHashMap for the same reason as the OUT side's outHeadUndoLog, and to keep the two identical
    // structures from drifting apart: allocated per drain, one entry per distinct destination vertex in the
    // slice, and a null value faithfully records "no deferred head" because occupancy lives in the key array
    // (issue #6083 review cycle 1).
    final LongObjectHashMap<RID> inHeadUndoLog = new LongObjectHashMap<>();

    try {
      while (i < count) {
        final int idx = inSortIndex[i];
        final int dstBucket = inDstBucketIds[idx];
        final long dstPos = inDstPositions[idx];

        int j = i;
        while (j < count) {
          final int jIdx = inSortIndex[j];
          if (inDstBucketIds[jIdx] != dstBucket || inDstPositions[jIdx] != dstPos)
            break;
          j++;
        }

        // Pre-compute exact bytes needed for this group
        final int groupSize = j - i;
        ensureTmpArrays(groupSize);
        int totalBytesNeeded = 0;
        for (int k = 0; k < groupSize; k++) {
          final int kIdx = inSortIndex[i + k];
          tmpEdgeBucketIds[k] = inEdgeBucketIds[kIdx];
          tmpEdgePositions[k] = inEdgePositions[kIdx];
          tmpVertexBucketIds[k] = inVertexBucketIds[kIdx];
          tmpVertexPositions[k] = inVertexPositions[kIdx];
          totalBytesNeeded += Binary.getNumberSpace(tmpEdgeBucketIds[k]) + Binary.getNumberSpace(tmpEdgePositions[k])
              + Binary.getNumberSpace(tmpVertexBucketIds[k]) + Binary.getNumberSpace(tmpVertexPositions[k]);
        }

        final long vertexKey = packVertexKey(dstBucket, dstPos);
        // Snapshot BEFORE getOrCreateInSegmentDeferred can mutate deferredInHead for this vertex (issue
        // #5950 review cycle 3) - see the undo-log comment above. A promoted vertex (below) never touches
        // deferredInHead/inChunkRIDCache, so this snapshot is harmlessly unused in that case.
        if (!inHeadUndoLog.containsKey(vertexKey))
          inHeadUndoLog.put(vertexKey, deferredInHead.get(vertexKey));
        final EdgeSegment inChunk = getOrCreateInSegmentDeferred(dstBucket, dstPos, vertexKey, totalBytesNeeded);

        if (lastSegmentPromoted) {
          // #5667: the vertex is already a promoted super-node - route through StripedEdgeList.
          addGroupThroughStripedEdgeList(lastPromotedVertex, Vertex.DIRECTION.IN, lastPromotedDirectory,
              tmpEdgeBucketIds, tmpEdgePositions, tmpVertexBucketIds, tmpVertexPositions, groupSize);
        } else if (lastSegmentIsNew) {
          // New segment: fill FIRST, then persist ONCE
          inChunk.addManyAtEndDirect(tmpEdgeBucketIds, tmpEdgePositions,
              tmpVertexBucketIds, tmpVertexPositions, 0, groupSize);
          persistNewSegment(inChunk, dstBucket, Vertex.DIRECTION.IN, vertexKey);
        } else {
          final int available = inChunk.getRecordSize() - inChunk.getUsed();
          if (totalBytesNeeded <= available) {
            inChunk.addManyAtEndDirect(tmpEdgeBucketIds, tmpEdgePositions,
                tmpVertexBucketIds, tmpVertexPositions, 0, groupSize);
            database.updateRecord(inChunk);
          } else {
            addIncomingEdgesToSegmentBulkLazy(dstBucket, dstPos, inChunk, inSortIndex, i, j);
          }
        }

        edgesInBatch += groupSize;
        if (commitEvery > 0 && edgesInBatch >= commitEvery) {
          fireBeforeIncomingEdgeCommitHook();
          database.commit();
          // Record the committed prefix only AFTER the commit succeeds (issue #5950 review cycle 3).
          inEdgesResumeSortIndex = j;
          inHeadUndoLog.clear();
          beginTx();
          edgesInBatch = 0;
        }

        i = j;
      }

      fireBeforeIncomingEdgeCommitHook();
      database.commit();
      inEdgesResumeSortIndex = count;
    } catch (final RuntimeException e) {
      inHeadUndoLog.forEach((key, previous) -> {
        if (previous == null)
          deferredInHead.remove(key);
        else
          deferredInHead.put(key, previous);
        // Just an accelerator cache - evicting is always safe, forces a deferredInHead/disk fallback.
        inChunkRIDCache.remove(key);
      });
      throw e;
    }
  }

  private void connectIncomingEdgesParallel(final int[] inSortIndex, final int maxBucket) {
    // The async executor's WAL policy is relaxed once for the whole batch in the constructor and
    // restored once in restoreAsyncSettings(), not around every parallel phase (issue #5665).
    final DatabaseAsyncExecutor async = database.async();

    final AtomicReference<Throwable> error = new AtomicReference<>();
    final int parallelLevel = async.getParallelLevel();

    // One local (non-concurrent) head-pointer map pair per scheduled bucket task, keyed by bucket id.
    // Each bucket's vertexKeys are disjoint from every other bucket's (vertexKey encodes the
    // destination bucket id), and a bucket runs on exactly one thread at a time - including across its
    // own internal CME retries, which rerun connectIncomingEdgesRangeLocal from scratch into the SAME
    // local maps - so no synchronization is needed for these. They are merged into the class-level,
    // NOT-thread-safe deferredInHead/inChunkRIDCache only in the single-threaded pass below, and only
    // for buckets that are actually in completedIncomingBuckets: a bucket that exhausts its retries
    // must not poison the authoritative state with RIDs from its rolled-back transaction, which a
    // subsequent retry of connectDeferredIncomingEdges() would then dereference and crash on (issue
    // #5950 review cycle 3).
    final Map<Integer, Map<Long, RID>> bucketDeferredInHead = new HashMap<>();
    final Map<Integer, Map<Long, RID>> bucketInChunkCache = new HashMap<>();

    // Schedule each bucket as a transaction on its assigned async slot (bucket % parallelLevel).
    // Buckets assigned to the same slot run sequentially within that slot's thread, so
    // no two tasks on the same slot ever run concurrently — thread-local state is safe.
    for (int b = 0; b <= maxBucket; b++) {
      if (bucketCounts[b] == 0)
        continue;

      // Already durably committed by a prior, partially-failed attempt at this drain - skip
      // rescheduling it so a retry doesn't recommit it (issue #5950 review cycle 3).
      if (completedIncomingBuckets.contains(b))
        continue;

      final int from = bucketOffsets[b];
      final int to = bucketOffsets[b + 1];
      final int slot = b % parallelLevel;
      final int bucketId = b;

      final Map<Long, RID> localDeferredInHead = new HashMap<>();
      final Map<Long, RID> localInChunkCache = new HashMap<>();
      bucketDeferredInHead.put(bucketId, localDeferredInHead);
      bucketInChunkCache.put(bucketId, localInChunkCache);

      // Mark the bucket complete from the transaction's OWN post-commit success callback, never from
      // inside connectIncomingEdgesRangeLocal itself: that method runs BEFORE the surrounding commit
      // and could still fail there, so only the OkCallback (fired strictly after database.commit()
      // succeeds, see DatabaseAsyncTransaction.execute()) guarantees membership implies a durable
      // commit (issue #5950 review cycle 3).
      //
      // The clear() makes each attempt start from an empty local map: async.transaction retries the same
      // lambda on a ConcurrentModificationException, and a rolled-back attempt's entries point at records
      // rollback undid (issue #5950 review cycle 4 - the retry does recompute and overwrite every entry
      // for this fixed range, so this is belt-and-braces, but it removes the dependency on that argument).
      async.transaction(() -> {
            localDeferredInHead.clear();
            localInChunkCache.clear();
            connectIncomingEdgesRangeLocal(inSortIndex, from, to, localDeferredInHead, localInChunkCache);
          },
          3, () -> completedIncomingBuckets.add(bucketId), e -> error.compareAndSet(null, e), slot);
    }

    async.waitCompletion();

    // Merge deferred head pointers back into the (non-concurrent) shared maps, single-threaded here,
    // and ONLY for buckets that durably committed (issue #5950 review cycle 3) - putAll is unavailable
    // on LongObjectHashMap by design; iterate and put.
    for (final Map.Entry<Integer, Map<Long, RID>> entry : bucketDeferredInHead.entrySet()) {
      if (!completedIncomingBuckets.contains(entry.getKey()))
        continue;
      entry.getValue().forEach(deferredInHead::put);
      bucketInChunkCache.get(entry.getKey()).forEach(inChunkRIDCache::put);
    }

    final Throwable t = error.get();
    if (t != null)
      throw t instanceof RuntimeException ? (RuntimeException) t : new RuntimeException(t);
  }

  /**
   * Connects incoming edges for a bucket range using local tmp arrays.
   * Uses the same fill-then-persist optimization as the sequential path.
   * Each invocation runs within a single async slot's transaction.
   */
  private void connectIncomingEdgesRangeLocal(final int[] inSortIndex, final int from, final int to,
      final Map<Long, RID> sharedDeferredInHead,
      final Map<Long, RID> sharedInChunkCache) {

    // Local tmp arrays (not shared across threads)
    int tmpSize = 64;
    int[] localTmpEdgeBucketIds = new int[tmpSize];
    long[] localTmpEdgePositions = new long[tmpSize];
    int[] localTmpVertexBucketIds = new int[tmpSize];
    long[] localTmpVertexPositions = new long[tmpSize];

    int i = from;
    while (i < to) {
      final int idx = inSortIndex[i];
      final int dstBucket = inDstBucketIds[idx];
      final long dstPos = inDstPositions[idx];

      // Group edges for the same destination vertex
      int j = i;
      while (j < to) {
        final int jIdx = inSortIndex[j];
        if (inDstBucketIds[jIdx] != dstBucket || inDstPositions[jIdx] != dstPos)
          break;
        j++;
      }

      final int groupSize = j - i;

      // Ensure local tmp arrays are big enough
      if (groupSize > tmpSize) {
        tmpSize = groupSize;
        localTmpEdgeBucketIds = new int[tmpSize];
        localTmpEdgePositions = new long[tmpSize];
        localTmpVertexBucketIds = new int[tmpSize];
        localTmpVertexPositions = new long[tmpSize];
      }

      // Fill local tmp arrays and compute total bytes
      int totalBytesNeeded = 0;
      for (int k = 0; k < groupSize; k++) {
        final int kIdx = inSortIndex[i + k];
        localTmpEdgeBucketIds[k] = inEdgeBucketIds[kIdx];
        localTmpEdgePositions[k] = inEdgePositions[kIdx];
        localTmpVertexBucketIds[k] = inVertexBucketIds[kIdx];
        localTmpVertexPositions[k] = inVertexPositions[kIdx];
        totalBytesNeeded += Binary.getNumberSpace(localTmpEdgeBucketIds[k]) + Binary.getNumberSpace(localTmpEdgePositions[k])
            + Binary.getNumberSpace(localTmpVertexBucketIds[k]) + Binary.getNumberSpace(localTmpVertexPositions[k]);
      }

      final long vertexKey = packVertexKey(dstBucket, dstPos);

      // Get or create IN segment
      EdgeSegment inChunk;
      boolean isNew;
      final RID cachedChunkRID = inChunkRIDCache.get(vertexKey);
      // Checked unconditionally (not just for known-new vertices): a pre-existing vertex touched
      // earlier in this batch is equally vulnerable to inChunkRIDCache eviction, and its on-disk head
      // is stale mid-batch too (issue #5950 review, second sub-case).
      final RID deferredChunkRID = cachedChunkRID == null ? deferredInHead.get(vertexKey) : null;
      if (cachedChunkRID != null) {
        inChunk = (EdgeSegment) database.lookupByRID(cachedChunkRID, true);
        isNew = false;
      } else if (deferredChunkRID != null) {
        // inChunkRIDCache entry was LRU-evicted: deferredInHead is unbounded and always authoritative
        // for the current head, so reuse it instead of creating an unlinked duplicate segment (issue
        // #5950 review: silent edge loss otherwise). Repopulate the cache so a vertex touched again
        // later in this parallel round doesn't keep paying for the fallback lookup (issue #5950
        // review cycle 3: minor cache-refresh consistency with the sequential path).
        inChunkRIDCache.put(vertexKey, deferredChunkRID);
        inChunk = (EdgeSegment) database.lookupByRID(deferredChunkRID, true);
        isNew = false;
      } else if (knownNewVertexKeys.contains(vertexKey)) {
        final int segmentSize = MutableEdgeSegment.CONTENT_START_POSITION + totalBytesNeeded;
        inChunk = new MutableEdgeSegment(database, segmentSize);
        isNew = true;
      } else {
        final MutableVertex dstVertex = ((Vertex) database.lookupByRID(new RID(dstBucket, dstPos), true)).modify();
        // #5667: check for an already-promoted super-node BEFORE assuming a plain segment, with purely
        // local variables - this method runs concurrently across async-executor threads (one per bucket slot),
        // so signalling through a shared instance field (as the single-threaded sequential path does) would race.
        final RID dstHeadChunk = dstVertex.getInEdgesHeadChunk();
        if (dstHeadChunk != null) {
          final Record head = database.lookupByRID(dstHeadChunk, true);
          if (head instanceof StripeDirectory directory) {
            // Route through StripedEdgeList and skip the bulk segment write below entirely.
            addGroupThroughStripedEdgeList(dstVertex, Vertex.DIRECTION.IN, directory,
                localTmpEdgeBucketIds, localTmpEdgePositions, localTmpVertexBucketIds, localTmpVertexPositions, groupSize);
            i = j;
            continue;
          }
          // Existing, durable head chunk - safe to cache immediately, unlike a freshly created segment:
          // this RID is already committed on disk, not pending in the current (possibly still-failing)
          // transaction (issue #5950 review cycle 3).
          inChunkRIDCache.put(vertexKey, dstHeadChunk);
          inChunk = (EdgeSegment) head;
          isNew = false;
        } else {
          // No segment yet: build in-memory only here, same as the knownNewVertexKeys branch above -
          // NOT via getOrCreateInEdgeChunk(), which persists AND caches into the class-level
          // inChunkRIDCache immediately. That eager cache write survives this bucket's transaction being
          // rolled back on failure, so a retry that skips completed buckets but reschedules this one
          // would still see the phantom RID and crash with RecordNotFoundException. Deferring to the
          // isNew branch below persists through - and caches via - the per-bucket LOCAL map, merged into
          // the shared caches only once this bucket's commit actually succeeds (issue #5950 review cycle
          // 3), exactly mirroring the sequential path's getOrCreateInSegmentDeferred/persistNewSegment.
          final int segmentSize = MutableEdgeSegment.CONTENT_START_POSITION + totalBytesNeeded;
          inChunk = new MutableEdgeSegment(database, segmentSize);
          isNew = true;
        }
      }

      final String edgeBucketName = database.getGraphEngine().getEdgesBucketName(dstBucket, Vertex.DIRECTION.IN);

      if (isNew) {
        // Fill first, then persist once
        inChunk.addManyAtEndDirect(localTmpEdgeBucketIds, localTmpEdgePositions,
            localTmpVertexBucketIds, localTmpVertexPositions, 0, groupSize);
        database.createRecord(inChunk, edgeBucketName);
        sharedInChunkCache.put(vertexKey, inChunk.getIdentity());
        sharedDeferredInHead.put(vertexKey, inChunk.getIdentity());
      } else {
        final int available = inChunk.getRecordSize() - inChunk.getUsed();
        if (totalBytesNeeded <= available) {
          inChunk.addManyAtEndDirect(localTmpEdgeBucketIds, localTmpEdgePositions,
              localTmpVertexBucketIds, localTmpVertexPositions, 0, groupSize);
          database.updateRecord(inChunk);
        } else {
          // Overflow: create exact-sized new segment, fill, persist
          final int newSize = MutableEdgeSegment.CONTENT_START_POSITION + totalBytesNeeded;
          final MutableEdgeSegment newChunk = new MutableEdgeSegment(database, newSize);
          newChunk.setPrevious(inChunk);
          newChunk.addManyAtEndDirect(localTmpEdgeBucketIds, localTmpEdgePositions,
              localTmpVertexBucketIds, localTmpVertexPositions, 0, groupSize);
          database.createRecord(newChunk, edgeBucketName);
          sharedInChunkCache.put(vertexKey, newChunk.getIdentity());
          sharedDeferredInHead.put(vertexKey, newChunk.getIdentity());
        }
      }

      i = j;
    }

    // Fired right before the surrounding async.transaction commits this bucket's work (see
    // DatabaseAsyncTransaction.execute()), so a test hook throwing here simulates that commit failing.
    fireBeforeIncomingEdgeCommitHook();
  }

  /**
   * Adds incoming edges to a segment with lazy vertex loading.
   * Vertex is only loaded on segment overflow.
   */
  private void addIncomingEdgesToSegmentBulkLazy(final int dstBucket, final long dstPos,
      EdgeSegment currentSegment, final int[] inSortIdx, final int fromIdx, final int toIdx) {

    final int groupSize = toIdx - fromIdx;

    // --- Fast path: estimate total bytes and try vectorized write if all edges fit ---
    if (groupSize > 1) {
      final int available = currentSegment.getRecordSize() - currentSegment.getUsed();
      int totalBytes = 0;

      ensureTmpArrays(groupSize);
      for (int k = 0; k < groupSize; k++) {
        final int idx = inSortIdx[fromIdx + k];
        tmpEdgeBucketIds[k] = inEdgeBucketIds[idx];
        tmpEdgePositions[k] = inEdgePositions[idx];
        tmpVertexBucketIds[k] = inVertexBucketIds[idx];
        tmpVertexPositions[k] = inVertexPositions[idx];

        totalBytes += Binary.getNumberSpace(tmpEdgeBucketIds[k]) + Binary.getNumberSpace(tmpEdgePositions[k])
            + Binary.getNumberSpace(tmpVertexBucketIds[k]) + Binary.getNumberSpace(tmpVertexPositions[k]);
      }

      if (totalBytes <= available) {
        currentSegment.addManyAtEndDirect(tmpEdgeBucketIds, tmpEdgePositions,
            tmpVertexBucketIds, tmpVertexPositions, 0, groupSize);
        database.updateRecord(currentSegment);
        return;
      }
    }

    // --- Slow path: per-edge with overflow handling + exact-sized new segments ---
    boolean segmentModified = false;
    final long vertexKey = packVertexKey(dstBucket, dstPos);

    int k = fromIdx;
    while (k < toIdx) {
      final int idx = inSortIdx[k];

      final int eBucket = inEdgeBucketIds[idx];
      final long ePos = inEdgePositions[idx];
      final int vBucket = inVertexBucketIds[idx];
      final long vPos = inVertexPositions[idx];

      if (currentSegment.addAtEndDirect(eBucket, ePos, vBucket, vPos)) {
        segmentModified = true;
        k++;
      } else {
        if (segmentModified) {
          database.updateRecord(currentSegment);
          segmentModified = false;
        }

        // Compute exact bytes for remaining edges
        final int remaining = toIdx - k;
        int remainingBytes = 0;
        for (int r = k; r < toIdx; r++) {
          final int rIdx = inSortIdx[r];
          remainingBytes += Binary.getNumberSpace(inEdgeBucketIds[rIdx]) + Binary.getNumberSpace(inEdgePositions[rIdx])
              + Binary.getNumberSpace(inVertexBucketIds[rIdx]) + Binary.getNumberSpace(inVertexPositions[rIdx]);
        }

        final int newSize = MutableEdgeSegment.CONTENT_START_POSITION + remainingBytes;
        final MutableEdgeSegment newChunk = new MutableEdgeSegment(database, newSize);
        newChunk.setPrevious(currentSegment);

        // Fill tmp arrays and vectorized write all remaining edges BEFORE persisting
        ensureTmpArrays(remaining);
        for (int r = 0; r < remaining; r++) {
          final int rIdx = inSortIdx[k + r];
          tmpEdgeBucketIds[r] = inEdgeBucketIds[rIdx];
          tmpEdgePositions[r] = inEdgePositions[rIdx];
          tmpVertexBucketIds[r] = inVertexBucketIds[rIdx];
          tmpVertexPositions[r] = inVertexPositions[rIdx];
        }
        newChunk.addManyAtEndDirect(tmpEdgeBucketIds, tmpEdgePositions,
            tmpVertexBucketIds, tmpVertexPositions, 0, remaining);

        // Persist once (already filled)
        final String bucketName = database.getSchema().getBucketById(currentSegment.getIdentity().getBucketId()).getName();
        database.createRecord(newChunk, bucketName);

        // Defer vertex head chunk update
        inChunkRIDCache.put(vertexKey, newChunk.getIdentity());
        deferredInHead.put(vertexKey, newChunk.getIdentity());
        return;
      }
    }

    if (segmentModified)
      database.updateRecord(currentSegment);
  }

  // ---------------------------------------------------------------------------
  // Deferred segment creation: creates segments without loading/saving vertices
  // ---------------------------------------------------------------------------

  // Result holder for segment lookup: existing (already persisted) or new (not yet persisted)
  private EdgeSegment lastSegmentResult;
  private boolean     lastSegmentIsNew;

  // #5667: set by getOrCreateOutSegmentDeferred/getOrCreateInSegmentDeferred when the vertex is already
  // promoted to the super-node striped layout (a StripeDirectory head). GraphBatch's bulk path writes segments
  // and flips the vertex head pointer directly, which is only safe for the classic (non-promoted) layout - see
  // addGroupThroughStripedEdgeList. ONLY safe as an instance field because those two methods are called
  // exclusively from the single-threaded SEQUENTIAL connect path (connectOutgoingEdgesSorted /
  // connectIncomingEdgesSequential); the PARALLEL range-local connectors check locally instead - see
  // getOrCreateOutEdgeChunk's javadoc.
  private boolean         lastSegmentPromoted;
  private Vertex          lastPromotedVertex;
  private StripeDirectory lastPromotedDirectory;

  /**
   * Gets or creates an OUT segment for a vertex without loading the vertex record.
   * For known-new vertices (from createVertices()), skips the vertex load entirely.
   * Sets lastSegmentIsNew=true if a new segment was created (not yet persisted — caller
   * must fill it and call createRecord). Sets false if an existing segment was found.
   */
  private EdgeSegment getOrCreateOutSegmentDeferred(final int bucketId, final long position,
      final long vertexKey, final int dataBytesNeeded) {

    lastSegmentPromoted = false;

    // Check cache first
    final RID cachedRID = outChunkRIDCache.get(vertexKey);
    if (cachedRID != null) {
      lastSegmentIsNew = false;
      return (EdgeSegment) database.lookupByRID(cachedRID, true);
    }

    // outChunkRIDCache is LRU-bounded and may have already evicted this vertex's entry even though
    // the vertex was already given a segment earlier in this batch (large batch, many other vertices
    // touched in between) - both for a known-new vertex (whose on-disk head stays null until close())
    // and for a pre-existing vertex (whose on-disk head, if any, predates this batch and is stale once
    // this batch has overflowed it). deferredOutHead is unbounded for the whole batch and always
    // reflects the vertex's true current head once touched, so it must be checked BEFORE falling back
    // to knownNewVertexKeys / an on-disk read for either case (issue #5950 review, both sub-cases:
    // a second, unlinked segment would otherwise silently orphan the earlier one's committed edges).
    final RID deferredRID = deferredOutHead.get(vertexKey);
    if (deferredRID != null) {
      outChunkRIDCache.put(vertexKey, deferredRID);
      lastSegmentIsNew = false;
      return (EdgeSegment) database.lookupByRID(deferredRID, true);
    }

    // For known-new vertices with no deferred head yet, skip loading — we know there's no existing segment
    if (!knownNewVertexKeys.contains(vertexKey)) {
      final VertexInternal vertex = (VertexInternal) database.lookupByRID(new RID(bucketId, position), true);
      final RID headChunk = vertex.getOutEdgesHeadChunk();
      if (headChunk != null) {
        final Record head = database.lookupByRID(headChunk, true);
        if (head instanceof StripeDirectory directory) {
          // #5667: the bulk path manipulates the vertex head pointer directly and cannot safely touch a
          // promoted vertex's striped layout. Route this group through the standard, MVCC-safe StripedEdgeList
          // write path instead of failing the whole batch (see the GraphBatch class javadoc).
          lastSegmentPromoted = true;
          lastPromotedVertex = vertex;
          lastPromotedDirectory = directory;
          return null;
        }
        outChunkRIDCache.put(vertexKey, headChunk);
        lastSegmentIsNew = false;
        return (EdgeSegment) head;
      }
    }

    // Create exact-sized segment in memory — NOT persisted yet (caller will fill then persist)
    final int segmentSize = MutableEdgeSegment.CONTENT_START_POSITION + dataBytesNeeded;
    lastSegmentIsNew = true;
    return new MutableEdgeSegment(database, segmentSize);
  }

  /**
   * Gets or creates an IN segment for a vertex without loading the vertex record.
   * Same contract as getOrCreateOutSegmentDeferred regarding lastSegmentIsNew.
   */
  private EdgeSegment getOrCreateInSegmentDeferred(final int bucketId, final long position,
      final long vertexKey, final int dataBytesNeeded) {

    lastSegmentPromoted = false;

    final RID cachedRID = inChunkRIDCache.get(vertexKey);
    if (cachedRID != null) {
      lastSegmentIsNew = false;
      return (EdgeSegment) database.lookupByRID(cachedRID, true);
    }

    // See getOrCreateOutSegmentDeferred: deferredInHead must be checked unconditionally, before
    // knownNewVertexKeys / an on-disk read, for both the known-new and pre-existing vertex cases.
    final RID deferredRID = deferredInHead.get(vertexKey);
    if (deferredRID != null) {
      inChunkRIDCache.put(vertexKey, deferredRID);
      lastSegmentIsNew = false;
      return (EdgeSegment) database.lookupByRID(deferredRID, true);
    }

    if (!knownNewVertexKeys.contains(vertexKey)) {
      final VertexInternal vertex = (VertexInternal) database.lookupByRID(new RID(bucketId, position), true);
      final RID headChunk = vertex.getInEdgesHeadChunk();
      if (headChunk != null) {
        final Record head = database.lookupByRID(headChunk, true);
        if (head instanceof StripeDirectory directory) {
          // #5667: see getOrCreateOutSegmentDeferred - route through StripedEdgeList instead of failing.
          lastSegmentPromoted = true;
          lastPromotedVertex = vertex;
          lastPromotedDirectory = directory;
          return null;
        }
        inChunkRIDCache.put(vertexKey, headChunk);
        lastSegmentIsNew = false;
        return (EdgeSegment) head;
      }
    }

    final int segmentSize = MutableEdgeSegment.CONTENT_START_POSITION + dataBytesNeeded;
    lastSegmentIsNew = true;
    return new MutableEdgeSegment(database, segmentSize);
  }

  /**
   * Persists a newly created segment (fill-then-persist pattern) and updates caches.
   */
  private void persistNewSegment(final EdgeSegment segment, final int bucketId,
      final Vertex.DIRECTION direction, final long vertexKey) {
    final String bucketName = database.getGraphEngine().getEdgesBucketName(bucketId, direction);
    database.createRecord(segment, bucketName);

    if (direction == Vertex.DIRECTION.OUT) {
      outChunkRIDCache.put(vertexKey, segment.getIdentity());
      deferredOutHead.put(vertexKey, segment.getIdentity());
    } else {
      inChunkRIDCache.put(vertexKey, segment.getIdentity());
      deferredInHead.put(vertexKey, segment.getIdentity());
    }
  }

  /**
   * #5667: writes a batch group's edges for an already-promoted (super-node) vertex through the standard
   * {@link StripedEdgeList} write path, one edge at a time. GraphBatch's bulk segment-manipulation code writes
   * segments directly and flips the vertex head pointer without going through the MVCC-anchored, directory-aware
   * writes {@link StripedEdgeList} depends on for its stripe slot updates - safe only for the classic
   * (non-promoted) layout. This lets a bulk load resume over a graph that was promoted through the standard API
   * (or by a previous, non-bulk write) instead of hard-failing.
   * <p>
   * GraphBatch itself never promotes a vertex during bulk load - see the class javadoc.
   */
  private void addGroupThroughStripedEdgeList(final Vertex vertex, final Vertex.DIRECTION direction,
      final StripeDirectory directory, final int[] edgeBucketIds, final long[] edgePositions,
      final int[] vertexBucketIds, final long[] vertexPositions, final int count) {
    final StripedEdgeList striped = new StripedEdgeList(vertex, direction, directory);
    for (int k = 0; k < count; k++)
      striped.add(new RID(edgeBucketIds[k], edgePositions[k]), new RID(vertexBucketIds[k], vertexPositions[k]));
  }

  // ---------------------------------------------------------------------------
  // Edge segment helpers (with edgeListInitialSize override)
  // ---------------------------------------------------------------------------

  /**
   * #5667: unlike {@code getOrCreateOutSegmentDeferred}/{@code getOrCreateInSegmentDeferred}, this method (and
   * its IN counterpart) is also called from the PARALLEL range-local connectors, which run concurrently across
   * multiple async-executor threads (see {@code connectOutEdgesRangeLocal}). It therefore does NOT signal a
   * promoted vertex through the shared {@code lastSegmentPromoted}/{@code lastPromotedVertex} instance fields -
   * those are safe only for the single-threaded sequential path. Callers on the parallel path check for a
   * {@link StripeDirectory} head themselves, with purely local variables, before ever calling this method.
   */
  private EdgeSegment getOrCreateOutEdgeChunk(final MutableVertex vertex) {
    final RID vertexRID = vertex.getIdentity();
    final long key = packVertexKey(vertexRID.getBucketId(), vertexRID.getPosition());

    // Check cache first (avoids reading vertex's head chunk field)
    final RID cachedRID = outChunkRIDCache.get(key);
    if (cachedRID != null)
      return (EdgeSegment) database.lookupByRID(cachedRID, true);

    final RID headChunk = vertex.getOutEdgesHeadChunk();
    if (headChunk != null) {
      outChunkRIDCache.put(key, headChunk);
      return (EdgeSegment) database.lookupByRID(headChunk, true);
    }

    // Create with our custom initial size
    final MutableEdgeSegment chunk = new MutableEdgeSegment(database, edgeListInitialSize);
    final String bucketName = database.getGraphEngine().getEdgesBucketName(vertexRID.getBucketId(), Vertex.DIRECTION.OUT);
    database.createRecord(chunk, bucketName);
    vertex.setOutEdgesHeadChunk(chunk.getIdentity());
    database.updateRecord(vertex);
    outChunkRIDCache.put(key, chunk.getIdentity());
    return chunk;
  }

  /** #5667: see {@link #getOrCreateOutEdgeChunk} - same thread-safety contract for the IN direction. */
  private EdgeSegment getOrCreateInEdgeChunk(final MutableVertex vertex) {
    final RID vertexRID = vertex.getIdentity();
    final long key = packVertexKey(vertexRID.getBucketId(), vertexRID.getPosition());

    final RID cachedRID = inChunkRIDCache.get(key);
    if (cachedRID != null)
      return (EdgeSegment) database.lookupByRID(cachedRID, true);

    final RID headChunk = vertex.getInEdgesHeadChunk();
    if (headChunk != null) {
      inChunkRIDCache.put(key, headChunk);
      return (EdgeSegment) database.lookupByRID(headChunk, true);
    }

    final MutableEdgeSegment chunk = new MutableEdgeSegment(database, edgeListInitialSize);
    final String bucketName = database.getGraphEngine().getEdgesBucketName(vertexRID.getBucketId(), Vertex.DIRECTION.IN);
    database.createRecord(chunk, bucketName);
    vertex.setInEdgesHeadChunk(chunk.getIdentity());
    database.updateRecord(vertex);
    inChunkRIDCache.put(key, chunk.getIdentity());
    return chunk;
  }

  /**
   * Like getOrCreateOutEdgeChunk but creates segments with exactly the right size
   * for the known edge data bytes. Returns existing segment if already present.
   */
  private EdgeSegment getOrCreateOutEdgeChunkExact(final MutableVertex vertex, final int dataBytesNeeded) {
    final RID vertexRID = vertex.getIdentity();
    final long key = packVertexKey(vertexRID.getBucketId(), vertexRID.getPosition());

    final RID cachedRID = outChunkRIDCache.get(key);
    if (cachedRID != null)
      return (EdgeSegment) database.lookupByRID(cachedRID, true);

    final RID headChunk = vertex.getOutEdgesHeadChunk();
    if (headChunk != null) {
      outChunkRIDCache.put(key, headChunk);
      return (EdgeSegment) database.lookupByRID(headChunk, true);
    }

    final int segmentSize = MutableEdgeSegment.CONTENT_START_POSITION + dataBytesNeeded;
    final MutableEdgeSegment chunk = new MutableEdgeSegment(database, segmentSize);
    final String bucketName = database.getGraphEngine().getEdgesBucketName(vertexRID.getBucketId(), Vertex.DIRECTION.OUT);
    database.createRecord(chunk, bucketName);
    vertex.setOutEdgesHeadChunk(chunk.getIdentity());
    database.updateRecord(vertex);
    outChunkRIDCache.put(key, chunk.getIdentity());
    return chunk;
  }

  /**
   * Like getOrCreateInEdgeChunk but creates segments with exactly the right size.
   */
  private EdgeSegment getOrCreateInEdgeChunkExact(final MutableVertex vertex, final int dataBytesNeeded) {
    final RID vertexRID = vertex.getIdentity();
    final long key = packVertexKey(vertexRID.getBucketId(), vertexRID.getPosition());

    final RID cachedRID = inChunkRIDCache.get(key);
    if (cachedRID != null)
      return (EdgeSegment) database.lookupByRID(cachedRID, true);

    final RID headChunk = vertex.getInEdgesHeadChunk();
    if (headChunk != null) {
      inChunkRIDCache.put(key, headChunk);
      return (EdgeSegment) database.lookupByRID(headChunk, true);
    }

    final int segmentSize = MutableEdgeSegment.CONTENT_START_POSITION + dataBytesNeeded;
    final MutableEdgeSegment chunk = new MutableEdgeSegment(database, segmentSize);
    final String bucketName = database.getGraphEngine().getEdgesBucketName(vertexRID.getBucketId(), Vertex.DIRECTION.IN);
    database.createRecord(chunk, bucketName);
    vertex.setInEdgesHeadChunk(chunk.getIdentity());
    database.updateRecord(vertex);
    inChunkRIDCache.put(key, chunk.getIdentity());
    return chunk;
  }

  // ---------------------------------------------------------------------------
  // Bulk segment append with lazy vertex loading
  // ---------------------------------------------------------------------------

  /**
   * Adds edges to a segment in bulk with lazy vertex loading. The vertex is only
   * loaded from disk when the segment overflows and a new segment must be created
   * (which requires updating the vertex's head chunk pointer). For vertices with
   * pre-allocated segments large enough to hold all their edges, the vertex is
   * never loaded — saving one record read per vertex group.
   */
  private void addEdgesToSegmentBulkLazy(final int srcBucket, final long srcPos,
      final Vertex.DIRECTION direction, EdgeSegment currentSegment,
      final RID[] edgeRIDs, final int fromIdx, final int toIdx, final boolean outgoing) {

    final int groupSize = toIdx - fromIdx;

    // --- Fast path: estimate total bytes and try vectorized write if all edges fit ---
    if (groupSize > 1) {
      final int available = currentSegment.getRecordSize() - currentSegment.getUsed();
      int totalBytes = 0;
      boolean fits = true;

      // Fill temp arrays and compute total bytes
      ensureTmpArrays(groupSize);
      for (int k = 0; k < groupSize; k++) {
        final int idx = sortIndex[fromIdx + k];
        final int eBucket = edgeRIDs[idx].getBucketId();
        final long ePos = edgeRIDs[idx].getPosition();
        final int vBucket = outgoing ? edgeDstBucketIds[idx] : edgeSrcBucketIds[idx];
        final long vPos = outgoing ? edgeDstPositions[idx] : edgeSrcPositions[idx];

        tmpEdgeBucketIds[k] = eBucket;
        tmpEdgePositions[k] = ePos;
        tmpVertexBucketIds[k] = vBucket;
        tmpVertexPositions[k] = vPos;

        totalBytes += Binary.getNumberSpace(eBucket) + Binary.getNumberSpace(ePos)
            + Binary.getNumberSpace(vBucket) + Binary.getNumberSpace(vPos);
      }

      if (totalBytes <= available) {
        // All edges fit — vectorized write, no overflow check per edge
        currentSegment.addManyAtEndDirect(tmpEdgeBucketIds, tmpEdgePositions,
            tmpVertexBucketIds, tmpVertexPositions, 0, groupSize);
        database.updateRecord(currentSegment);
        return;
      }
    }

    // --- Slow path: per-edge with overflow handling + exact-sized new segments ---
    // No vertex loading needed — head chunk updates are deferred to close()
    boolean segmentModified = false;
    final long vertexKey = packVertexKey(srcBucket, srcPos);

    int k = fromIdx;
    while (k < toIdx) {
      final int idx = sortIndex[k];

      final int eBucket = edgeRIDs[idx].getBucketId();
      final long ePos = edgeRIDs[idx].getPosition();
      final int vBucket = outgoing ? edgeDstBucketIds[idx] : edgeSrcBucketIds[idx];
      final long vPos = outgoing ? edgeDstPositions[idx] : edgeSrcPositions[idx];

      if (currentSegment.addAtEndDirect(eBucket, ePos, vBucket, vPos)) {
        segmentModified = true;
        k++;
      } else {
        if (segmentModified) {
          database.updateRecord(currentSegment);
          segmentModified = false;
        }

        // Compute exact bytes for remaining edges (from k to toIdx)
        final int remaining = toIdx - k;
        int remainingBytes = 0;
        for (int r = k; r < toIdx; r++) {
          final int rIdx = sortIndex[r];
          final int rEB = edgeRIDs[rIdx].getBucketId();
          final long rEP = edgeRIDs[rIdx].getPosition();
          final int rVB = outgoing ? edgeDstBucketIds[rIdx] : edgeSrcBucketIds[rIdx];
          final long rVP = outgoing ? edgeDstPositions[rIdx] : edgeSrcPositions[rIdx];
          remainingBytes += Binary.getNumberSpace(rEB) + Binary.getNumberSpace(rEP)
              + Binary.getNumberSpace(rVB) + Binary.getNumberSpace(rVP);
        }

        final int newSize = MutableEdgeSegment.CONTENT_START_POSITION + remainingBytes;
        final MutableEdgeSegment newChunk = new MutableEdgeSegment(database, newSize);
        newChunk.setPrevious(currentSegment);

        // Fill tmp arrays and vectorized write BEFORE persisting
        ensureTmpArrays(remaining);
        for (int r = 0; r < remaining; r++) {
          final int rIdx = sortIndex[k + r];
          tmpEdgeBucketIds[r] = edgeRIDs[rIdx].getBucketId();
          tmpEdgePositions[r] = edgeRIDs[rIdx].getPosition();
          tmpVertexBucketIds[r] = outgoing ? edgeDstBucketIds[rIdx] : edgeSrcBucketIds[rIdx];
          tmpVertexPositions[r] = outgoing ? edgeDstPositions[rIdx] : edgeSrcPositions[rIdx];
        }
        newChunk.addManyAtEndDirect(tmpEdgeBucketIds, tmpEdgePositions,
            tmpVertexBucketIds, tmpVertexPositions, 0, remaining);

        // Persist once (already filled)
        final String bucketName = database.getSchema().getBucketById(currentSegment.getIdentity().getBucketId()).getName();
        database.createRecord(newChunk, bucketName);

        // Defer vertex head chunk update — just update caches
        if (direction == Vertex.DIRECTION.OUT) {
          outChunkRIDCache.put(vertexKey, newChunk.getIdentity());
          deferredOutHead.put(vertexKey, newChunk.getIdentity());
        } else {
          inChunkRIDCache.put(vertexKey, newChunk.getIdentity());
          deferredInHead.put(vertexKey, newChunk.getIdentity());
        }

        // All remaining edges written at once, done
        return;
      }
    }

    if (segmentModified)
      database.updateRecord(currentSegment);
  }

  private void ensureTmpArrays(final int size) {
    if (tmpEdgeBucketIds.length < size) {
      tmpEdgeBucketIds = new int[size];
      tmpEdgePositions = new long[size];
      tmpVertexBucketIds = new int[size];
      tmpVertexPositions = new long[size];
    }
  }

  private int computeSegmentSize(final int previousSize) {
    if (previousSize == 0)
      return edgeListInitialSize;
    int newSize = previousSize * 2;
    if (newSize > LocalDatabase.MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE)
      newSize = LocalDatabase.MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE;
    return newSize;
  }

  // ---------------------------------------------------------------------------
  // Counting sort: O(n) bucket partitioning + per-bucket position sort
  // ---------------------------------------------------------------------------

  /**
   * Partitions edges by source bucket using O(n) counting sort, then sorts within
   * each bucket by position using merge sort. Produces bucket boundary info
   * in {@link #bucketCounts} and {@link #bucketOffsets} for parallel dispatch.
   *
   * @return the maximum bucket ID found
   */
  private int partitionBySourceBucket() {
    int maxBucket = 0;
    for (int i = 0; i < edgeCount; i++)
      if (edgeSrcBucketIds[i] > maxBucket)
        maxBucket = edgeSrcBucketIds[i];

    ensureCountingSortArrays(maxBucket);

    // Count edges per bucket
    Arrays.fill(bucketCounts, 0, maxBucket + 1, 0);
    for (int i = 0; i < edgeCount; i++)
      bucketCounts[edgeSrcBucketIds[i]]++;

    // Prefix sum → offsets
    bucketOffsets[0] = 0;
    for (int b = 0; b <= maxBucket; b++)
      bucketOffsets[b + 1] = bucketOffsets[b] + bucketCounts[b];

    // Distribute indices into sortIndex by bucket (stable)
    System.arraycopy(bucketOffsets, 0, countingSortCursor, 0, maxBucket + 1);
    for (int i = 0; i < edgeCount; i++) {
      final int bucket = edgeSrcBucketIds[i];
      sortIndex[countingSortCursor[bucket]++] = i;
    }

    // Sort within each bucket by position
    for (int b = 0; b <= maxBucket; b++)
      if (bucketCounts[b] > 1)
        mergeSort(sortIndex, bucketOffsets[b], bucketOffsets[b + 1], true);

    findDuplicateLightEdges(maxBucket);

    return maxBucket;
  }

  /**
   * Reports lightweight edges buffered twice in this flush.
   * <p>
   * A lightweight edge is the triple (type, out, in), so two entries carrying the same three values are one edge
   * stored twice. The sort above already places them adjacently, so finding them is one comparison per edge in a
   * pass that has to happen anyway - which is why this is on by default rather than opt-in.
   * <p>
   * <b>Scope: this flush only.</b> It cannot see a duplicate against an edge already in the database, nor one split
   * across two flushes; catching those needs the O(degree) edge-list scan that
   * {@link com.arcadedb.schema.EdgeType#isUnique()} performs on the single-edge path. What it does catch is the
   * common case of a re-run import. On a type that declares UNIQUE the duplicate is an error; otherwise it is
   * counted and reported through {@link #getDuplicateLightEdges()}.
   */
  private void findDuplicateLightEdges(final int maxBucket) {
    for (int b = 0; b <= maxBucket; b++) {
      for (int i = bucketOffsets[b] + 1; i < bucketOffsets[b + 1]; i++) {
        final int prev = sortIndex[i - 1];
        final int curr = sortIndex[i];

        if (!edgeIsLightweight[prev] || !edgeIsLightweight[curr])
          continue;
        if (edgeTypeBucketIds[prev] != edgeTypeBucketIds[curr]
            || edgeDstBucketIds[prev] != edgeDstBucketIds[curr]
            || edgeDstPositions[prev] != edgeDstPositions[curr]
            || edgeSrcBucketIds[prev] != edgeSrcBucketIds[curr]
            || edgeSrcPositions[prev] != edgeSrcPositions[curr])
          continue;

        ++duplicateLightEdges;

        final EdgeType edgeType = (EdgeType) database.getSchema().getTypeByBucketId(edgeTypeBucketIds[curr]);
        if (edgeType.isUnique())
          throw new DuplicatedKeyException(edgeType.getName() + "[@out,@in]",
              "[#" + edgeSrcBucketIds[curr] + ":" + edgeSrcPositions[curr] + ", #" + edgeDstBucketIds[curr] + ":"
                  + edgeDstPositions[curr] + "]", null);
      }
    }
  }

  /**
   * Number of lightweight edges this batch found buffered twice within a single flush. See
   * {@code findDuplicateLightEdges} for what this does and does not cover.
   */
  public long getDuplicateLightEdges() {
    return duplicateLightEdges;
  }

  /**
   * Partitions deferred incoming edges by destination bucket using counting sort.
   */
  /**
   * @param count number of leading buffer rows to partition - the pinned drain prefix, not necessarily
   *              {@link #inEdgeCount} (issue #5950 review cycle 4).
   */
  private int partitionIncomingByDestBucket(final int[] inSortIndex, final int count) {
    int maxBucket = 0;
    for (int i = 0; i < count; i++)
      if (inDstBucketIds[i] > maxBucket)
        maxBucket = inDstBucketIds[i];

    ensureCountingSortArrays(maxBucket);

    Arrays.fill(bucketCounts, 0, maxBucket + 1, 0);
    for (int i = 0; i < count; i++)
      bucketCounts[inDstBucketIds[i]]++;

    bucketOffsets[0] = 0;
    for (int b = 0; b <= maxBucket; b++)
      bucketOffsets[b + 1] = bucketOffsets[b] + bucketCounts[b];

    System.arraycopy(bucketOffsets, 0, countingSortCursor, 0, maxBucket + 1);
    for (int i = 0; i < count; i++) {
      final int bucket = inDstBucketIds[i];
      inSortIndex[countingSortCursor[bucket]++] = i;
    }

    // Ensure merge temp buffer is large enough
    if (mergeTmp.length < count / 2 + 1)
      mergeTmp = new int[count / 2 + 1];

    // Sort within each bucket by position
    for (int b = 0; b <= maxBucket; b++)
      if (bucketCounts[b] > 1)
        mergeSortIncoming(inSortIndex, bucketOffsets[b], bucketOffsets[b + 1]);

    return maxBucket;
  }

  private void ensureCountingSortArrays(final int maxBucket) {
    if (bucketCounts == null || bucketCounts.length <= maxBucket) {
      final int size = maxBucket + 2;
      bucketCounts = new int[size];
      bucketOffsets = new int[size];
      countingSortCursor = new int[size];
    }
  }

  // ---------------------------------------------------------------------------
  // Parallel outgoing edge connection via async executor
  // ---------------------------------------------------------------------------

  private void connectOutgoingEdgesParallel(final RID[] edgeRIDs, final int maxBucket) {
    // The async executor's WAL policy is relaxed once for the whole batch in the constructor and
    // restored once in restoreAsyncSettings(), not around every parallel phase (issue #5665).
    final DatabaseAsyncExecutor async = database.async();

    final AtomicReference<Throwable> error = new AtomicReference<>();
    final int parallelLevel = async.getParallelLevel();

    // One local head-pointer map pair per scheduled bucket, merged into the class-level maps only for
    // buckets that durably committed - structurally identical to connectIncomingEdgesParallel (issue
    // #5950 review cycle 4). Two distinct pre-commit-write hazards make this necessary here too:
    //   1. async.transaction(..., 3, ...) retries the SAME lambda on a ConcurrentModificationException,
    //      re-invoking connectOutEdgesRangeLocal over the same range after a rollback. Anything the
    //      failed attempt wrote into a class-level map still points at records that rollback undid, and
    //      the retry reads those maps FIRST - dereferencing a RID that was never made durable.
    //   2. A bucket that exhausts its 3 retries would otherwise still have its entries merged below,
    //      poisoning deferredOutHead - the authoritative head-chunk fallback the review-cycle-1/2 fix
    //      depends on - for the whole rest of the batch.
    // Unlike the IN side this set is method-local, not an instance field: a failed flush() drops the
    // outgoing buffer outright (see flush()'s catch block), so there is no cross-call resume to support.
    final Map<Integer, Map<Long, RID>> bucketDeferredOutHead = new HashMap<>();
    final Map<Integer, Map<Long, RID>> bucketOutChunkCache = new HashMap<>();
    final Set<Integer> completedOutgoingBuckets = ConcurrentHashMap.newKeySet();

    for (int b = 0; b <= maxBucket; b++) {
      if (bucketCounts[b] == 0)
        continue;

      final int from = bucketOffsets[b];
      final int to = bucketOffsets[b + 1];
      final int slot = b % parallelLevel;
      final int bucketId = b;

      // Re-created per attempt inside the lambda, not captured from here: a CME retry must not inherit
      // the rolled-back attempt's entries (issue #5950 review cycle 4, hazard 1 above).
      final Map<Long, RID> localDeferredOutHead = new HashMap<>();
      final Map<Long, RID> localOutChunkCache = new HashMap<>();
      bucketDeferredOutHead.put(bucketId, localDeferredOutHead);
      bucketOutChunkCache.put(bucketId, localOutChunkCache);

      async.transaction(() -> {
            localDeferredOutHead.clear();
            localOutChunkCache.clear();
            connectOutEdgesRangeLocal(edgeRIDs, from, to, localDeferredOutHead, localOutChunkCache);
          },
          3, () -> completedOutgoingBuckets.add(bucketId), e -> error.compareAndSet(null, e), slot);
    }

    async.waitCompletion();

    // Merge deferred head pointers back into the (non-concurrent) shared maps, single-threaded here, and
    // ONLY for buckets that durably committed - putAll is unavailable on LongObjectHashMap by design.
    for (final Map.Entry<Integer, Map<Long, RID>> entry : bucketDeferredOutHead.entrySet()) {
      if (!completedOutgoingBuckets.contains(entry.getKey()))
        continue;
      entry.getValue().forEach(deferredOutHead::put);
      bucketOutChunkCache.get(entry.getKey()).forEach(outChunkRIDCache::put);
    }

    // Exactly the edges a completed bucket connected, counted whether or not a sibling bucket failed (issue
    // #6083 item 4). A bucket is in this set only once its own transaction committed, so this never claims an
    // edge a rollback took back.
    int durable = 0;
    for (final int bucketId : completedOutgoingBuckets)
      durable += bucketCounts[bucketId];
    flushDurableOutEdges = durable;
    flushDurableOutRanges = null;

    final Throwable t = error.get();
    if (t == null)
      return;

    // Every edge record was made durable by the commit that preceded this dispatch, so a bucket that never
    // committed leaves its records behind connected to nothing (issue #6083 item 2). Unlike the sequential
    // path's failure these are not a suffix of the sort index - the buckets that failed are wherever they are -
    // so walk the partition once, reclaiming the failed buckets and recording the surviving ones for the IN
    // pass that flush() still owes them.
    final int[] survivors = new int[completedOutgoingBuckets.size() * 2];
    int survivorCount = 0;
    for (int b = 0; b <= maxBucket; b++) {
      if (bucketCounts[b] == 0)
        continue;
      if (completedOutgoingBuckets.contains(b)) {
        survivors[survivorCount++] = bucketOffsets[b];
        survivors[survivorCount++] = bucketOffsets[b + 1];
      } else
        reclaimOrphanEdgeRecords(edgeRIDs, bucketOffsets[b], bucketOffsets[b + 1], true, t);
    }
    flushDurableOutRanges = survivorCount == survivors.length ? survivors : Arrays.copyOf(survivors, survivorCount);

    throw t instanceof RuntimeException ? (RuntimeException) t : new RuntimeException(t);
  }

  /**
   * Connects outgoing edges for a bucket range using local tmp arrays.
   * Uses fill-then-persist for new segments and vectorized writes for existing ones.
   */
  private void connectOutEdgesRangeLocal(final RID[] edgeRIDs, final int from, final int to,
      final Map<Long, RID> sharedDeferredOutHead,
      final Map<Long, RID> sharedOutChunkCache) {

    int tmpSize = 64;
    int[] localTmpEdgeBucketIds = new int[tmpSize];
    long[] localTmpEdgePositions = new long[tmpSize];
    int[] localTmpVertexBucketIds = new int[tmpSize];
    long[] localTmpVertexPositions = new long[tmpSize];

    int i = from;
    while (i < to) {
      final int idx = sortIndex[i];
      final int srcBucket = edgeSrcBucketIds[idx];
      final long srcPos = edgeSrcPositions[idx];

      int j = i;
      while (j < to) {
        final int jIdx = sortIndex[j];
        if (edgeSrcBucketIds[jIdx] != srcBucket || edgeSrcPositions[jIdx] != srcPos)
          break;
        j++;
      }

      final int groupSize = j - i;

      // Ensure local tmp arrays are big enough
      if (groupSize > tmpSize) {
        tmpSize = groupSize;
        localTmpEdgeBucketIds = new int[tmpSize];
        localTmpEdgePositions = new long[tmpSize];
        localTmpVertexBucketIds = new int[tmpSize];
        localTmpVertexPositions = new long[tmpSize];
      }

      // Fill local tmp arrays and compute total bytes
      int totalBytesNeeded = 0;
      for (int k = 0; k < groupSize; k++) {
        final int kIdx = sortIndex[i + k];
        final RID edgeRID = edgeRIDs[kIdx];
        localTmpEdgeBucketIds[k] = edgeRID.getBucketId();
        localTmpEdgePositions[k] = edgeRID.getPosition();
        localTmpVertexBucketIds[k] = edgeDstBucketIds[kIdx];
        localTmpVertexPositions[k] = edgeDstPositions[kIdx];
        totalBytesNeeded += Binary.getNumberSpace(localTmpEdgeBucketIds[k]) + Binary.getNumberSpace(localTmpEdgePositions[k])
            + Binary.getNumberSpace(localTmpVertexBucketIds[k]) + Binary.getNumberSpace(localTmpVertexPositions[k]);
      }

      final long vertexKey = packVertexKey(srcBucket, srcPos);

      // Get or create OUT segment
      EdgeSegment outChunk;
      boolean isNew;
      final RID cachedChunkRID = outChunkRIDCache.get(vertexKey);
      // Checked unconditionally (not just for known-new vertices): a pre-existing vertex touched
      // earlier in this batch is equally vulnerable to outChunkRIDCache eviction, and its on-disk head
      // is stale mid-batch too (issue #5950 review, second sub-case).
      final RID deferredChunkRID = cachedChunkRID == null ? deferredOutHead.get(vertexKey) : null;
      if (cachedChunkRID != null) {
        outChunk = (EdgeSegment) database.lookupByRID(cachedChunkRID, true);
        isNew = false;
      } else if (deferredChunkRID != null) {
        // outChunkRIDCache entry was LRU-evicted: deferredOutHead is unbounded and always authoritative
        // for the current head, so reuse it instead of creating an unlinked duplicate segment (issue
        // #5950 review: silent edge loss otherwise). Repopulate the cache so a vertex touched again
        // later in this parallel round doesn't keep paying for the fallback lookup (issue #5950
        // review cycle 3: minor cache-refresh consistency with the sequential path).
        outChunkRIDCache.put(vertexKey, deferredChunkRID);
        outChunk = (EdgeSegment) database.lookupByRID(deferredChunkRID, true);
        isNew = false;
      } else if (knownNewVertexKeys.contains(vertexKey)) {
        final int segmentSize = MutableEdgeSegment.CONTENT_START_POSITION + totalBytesNeeded;
        outChunk = new MutableEdgeSegment(database, segmentSize);
        isNew = true;
      } else {
        final MutableVertex srcVertex = ((Vertex) database.lookupByRID(new RID(srcBucket, srcPos), true)).modify();
        // #5667: check locally for an already-promoted super-node BEFORE assuming a plain segment; this
        // method runs concurrently across async-executor threads, so a shared instance field would race.
        final RID srcHeadChunk = srcVertex.getOutEdgesHeadChunk();
        if (srcHeadChunk != null) {
          final Record head = database.lookupByRID(srcHeadChunk, true);
          if (head instanceof StripeDirectory directory) {
            addGroupThroughStripedEdgeList(srcVertex, Vertex.DIRECTION.OUT, directory,
                localTmpEdgeBucketIds, localTmpEdgePositions, localTmpVertexBucketIds, localTmpVertexPositions, groupSize);
            i = j;
            continue;
          }
          // Existing, durable head chunk - already committed on disk, so caching it immediately is safe
          // even if this bucket's transaction later rolls back (issue #5950 review cycle 4).
          outChunkRIDCache.put(vertexKey, srcHeadChunk);
          outChunk = (EdgeSegment) head;
          isNew = false;
        } else {
          // No segment yet: build in-memory only, same as the knownNewVertexKeys branch above - NOT via
          // getOrCreateOutEdgeChunk(), which persists the segment AND writes it into the class-level
          // outChunkRIDCache before this bucket's transaction commits. That eager write survives a
          // rollback, so the async CME retry (or any later flush) would read a RID belonging to a
          // rolled-back transaction and fail with RecordNotFoundException. Falling through to the isNew
          // branch below routes the write through the per-bucket LOCAL maps, merged into the shared state
          // only once this bucket's commit actually succeeds (issue #5950 review cycle 4), mirroring what
          // connectIncomingEdgesRangeLocal already does for the IN direction.
          final int segmentSize = MutableEdgeSegment.CONTENT_START_POSITION + totalBytesNeeded;
          outChunk = new MutableEdgeSegment(database, segmentSize);
          isNew = true;
        }
      }

      final String edgeBucketName = database.getGraphEngine().getEdgesBucketName(srcBucket, Vertex.DIRECTION.OUT);

      if (isNew) {
        outChunk.addManyAtEndDirect(localTmpEdgeBucketIds, localTmpEdgePositions,
            localTmpVertexBucketIds, localTmpVertexPositions, 0, groupSize);
        database.createRecord(outChunk, edgeBucketName);
        sharedOutChunkCache.put(vertexKey, outChunk.getIdentity());
        sharedDeferredOutHead.put(vertexKey, outChunk.getIdentity());
      } else {
        final int available = outChunk.getRecordSize() - outChunk.getUsed();
        if (totalBytesNeeded <= available) {
          outChunk.addManyAtEndDirect(localTmpEdgeBucketIds, localTmpEdgePositions,
              localTmpVertexBucketIds, localTmpVertexPositions, 0, groupSize);
          database.updateRecord(outChunk);
        } else {
          // Overflow: create exact-sized new segment, fill, persist
          final int newSize = MutableEdgeSegment.CONTENT_START_POSITION + totalBytesNeeded;
          final MutableEdgeSegment newChunk = new MutableEdgeSegment(database, newSize);
          newChunk.setPrevious(outChunk);
          newChunk.addManyAtEndDirect(localTmpEdgeBucketIds, localTmpEdgePositions,
              localTmpVertexBucketIds, localTmpVertexPositions, 0, groupSize);
          database.createRecord(newChunk, edgeBucketName);
          sharedOutChunkCache.put(vertexKey, newChunk.getIdentity());
          sharedDeferredOutHead.put(vertexKey, newChunk.getIdentity());
        }
      }

      i = j;
    }

    // Fired right before the surrounding async.transaction commits this bucket's work, so a test hook
    // throwing here simulates that commit failing (issue #5950 review cycle 4).
    final IntConsumer hook = TEST_BEFORE_OUTGOING_EDGE_COMMIT_HOOK;
    if (hook != null)
      hook.accept(outgoingEdgeCommitAttempt.incrementAndGet());
  }

  // ---------------------------------------------------------------------------
  // Sorting (merge sort for within-bucket position ordering)
  // ---------------------------------------------------------------------------

  private void mergeSort(final int[] index, final int from, final int to, final boolean bySource) {
    if (to - from <= 16) {
      // Insertion sort for small ranges
      for (int i = from + 1; i < to; i++) {
        final int key = index[i];
        int j = i - 1;
        while (j >= from && compare(index[j], key, bySource) > 0) {
          index[j + 1] = index[j];
          j--;
        }
        index[j + 1] = key;
      }
      return;
    }

    final int mid = (from + to) >>> 1;
    mergeSort(index, from, mid, bySource);
    mergeSort(index, mid, to, bySource);

    // Merge using pre-allocated temp buffer
    final int leftLen = mid - from;
    System.arraycopy(index, from, mergeTmp, 0, leftLen);

    int li = 0, ri = mid, wi = from;
    while (li < leftLen && ri < to) {
      if (compare(mergeTmp[li], index[ri], bySource) <= 0)
        index[wi++] = mergeTmp[li++];
      else
        index[wi++] = index[ri++];
    }
    while (li < leftLen)
      index[wi++] = mergeTmp[li++];
  }

  private int compare(final int a, final int b, final boolean bySource) {
    if (bySource) {
      int cmp = Integer.compare(edgeSrcBucketIds[a], edgeSrcBucketIds[b]);
      if (cmp != 0)
        return cmp;
      cmp = Long.compare(edgeSrcPositions[a], edgeSrcPositions[b]);
      if (cmp != 0)
        return cmp;
      // Secondary keys: grouping by source is all the write pass needs, but ordering within a source group by
      // (edge type, destination) makes two identical lightweight edges land next to each other, which turns
      // duplicate detection into one comparison inside a pass that already exists. See findDuplicateLightEdges.
      cmp = Integer.compare(edgeTypeBucketIds[a], edgeTypeBucketIds[b]);
      if (cmp != 0)
        return cmp;
      cmp = Integer.compare(edgeDstBucketIds[a], edgeDstBucketIds[b]);
      return cmp != 0 ? cmp : Long.compare(edgeDstPositions[a], edgeDstPositions[b]);
    }
    final int cmp = Integer.compare(edgeDstBucketIds[a], edgeDstBucketIds[b]);
    return cmp != 0 ? cmp : Long.compare(edgeDstPositions[a], edgeDstPositions[b]);
  }

  /**
   * Merge sort for the deferred incoming edges, sorted by (dstBucket, dstPosition).
   */
  private void mergeSortIncoming(final int[] index, final int from, final int to) {
    if (to - from <= 16) {
      for (int i = from + 1; i < to; i++) {
        final int key = index[i];
        int j = i - 1;
        while (j >= from && compareIncoming(index[j], key) > 0) {
          index[j + 1] = index[j];
          j--;
        }
        index[j + 1] = key;
      }
      return;
    }

    final int mid = (from + to) >>> 1;
    mergeSortIncoming(index, from, mid);
    mergeSortIncoming(index, mid, to);

    final int leftLen = mid - from;
    System.arraycopy(index, from, mergeTmp, 0, leftLen);

    int li = 0, ri = mid, wi = from;
    while (li < leftLen && ri < to) {
      if (compareIncoming(mergeTmp[li], index[ri]) <= 0)
        index[wi++] = mergeTmp[li++];
      else
        index[wi++] = index[ri++];
    }
    while (li < leftLen)
      index[wi++] = mergeTmp[li++];
  }

  private int compareIncoming(final int a, final int b) {
    final int cmp = Integer.compare(inDstBucketIds[a], inDstBucketIds[b]);
    return cmp != 0 ? cmp : Long.compare(inDstPositions[a], inDstPositions[b]);
  }

  // ---------------------------------------------------------------------------
  // Builder
  // ---------------------------------------------------------------------------

  public static Builder builder(final Database database) {
    return new Builder((DatabaseInternal) database, null);
  }

  /**
   * Builds a batch that reserves the single-batch slot of {@code guardOwner} on {@link Builder#build()} and
   * hands it back on {@link GraphBatch#close()}. {@code database} is the instance the batch writes through
   * and may be an HA wrapper of {@code guardOwner}.
   */
  public static Builder builder(final Database database, final LocalDatabase guardOwner) {
    return new Builder((DatabaseInternal) database, guardOwner);
  }

  public static class Builder {
    private static final int MIN_BATCH_SIZE = 100_000;
    private static final int MAX_BATCH_SIZE = 5_000_000;

    private final DatabaseInternal database;
    private final LocalDatabase    guardOwner;
    private int                batchSize            = MIN_BATCH_SIZE;
    private boolean            batchSizeExplicit    = false;
    private int                expectedEdgeCount    = 0;
    private int                edgeListInitialSize  = 2048;
    private boolean            lightEdges           = false;
    private boolean            bidirectional        = true;
    private int                commitEvery          = 50_000;
    private boolean            commitEveryExplicit  = false;
    private boolean            useWAL               = false;
    private WALFile.FlushType  walFlush             = WALFile.FlushType.NO;
    private boolean            preAllocateEdgeChunks = true;
    private boolean            parallelFlush         = true;
    private int                commitRetries         = 10;
    private long               commitRetryDelayMs    = 1000;
    private int                chunkCacheCapacity       = DEFAULT_CHUNK_CACHE_CAPACITY;
    private int                maxDeferredIncomingEdges = DEFAULT_MAX_DEFERRED_INCOMING_EDGES;

    Builder(final DatabaseInternal database, final LocalDatabase guardOwner) {
      this.database = database;
      this.guardOwner = guardOwner;
    }

    /**
     * Maximum number of edges buffered before an automatic flush. Default: 100,000.
     * Overrides any auto-tuning from {@link #withExpectedEdgeCount(int)}.
     */
    public Builder withBatchSize(final int batchSize) {
      if (batchSize <= 0)
        throw new IllegalArgumentException("Batch size must be > 0");
      this.batchSize = batchSize;
      this.batchSizeExplicit = true;
      return this;
    }

    /**
     * Hint for the expected total number of edges to import. When set and no explicit
     * batch size is provided, the batch size is auto-tuned to {@code expectedEdgeCount},
     * clamped between 100K and 5M. Benchmarks show that a single flush (batch >= edgeCount)
     * is optimal, with diminishing returns beyond the total edge count.
     */
    public Builder withExpectedEdgeCount(final int expectedEdgeCount) {
      if (expectedEdgeCount < 0)
        throw new IllegalArgumentException("Expected edge count must be >= 0");
      this.expectedEdgeCount = expectedEdgeCount;
      return this;
    }

    /**
     * Initial size in bytes for new edge segments. Larger values reduce segment splits.
     * Default: 2048 (vs standard 64). Max: 8192.
     */
    public Builder withEdgeListInitialSize(final int size) {
      if (size < 64 || size > LocalDatabase.MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE)
        throw new IllegalArgumentException(
            "Edge list initial size must be between 64 and " + LocalDatabase.MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE);
      this.edgeListInitialSize = size;
      return this;
    }

    /**
     * If true (default), edges without properties are created as light edges
     * (no record stored, only connectivity pointers). Saves ~33% I/O for property-less edges.
     *
     * @deprecated Declare {@code LIGHTWEIGHT} on the edge type instead. An edge type that declares it is stored
     * lightweight whatever this flag says; this flag remains only for types that declare nothing.
     */
    @Deprecated
    public Builder withLightEdges(final boolean lightEdges) {
      this.lightEdges = lightEdges;
      return this;
    }

    /**
     * If true (default), incoming edges are also connected. Set to false for
     * unidirectional graphs or when incoming edges will be connected later.
     */
    public Builder withBidirectional(final boolean bidirectional) {
      this.bidirectional = bidirectional;
      return this;
    }

    /**
     * Number of edges to process before committing and starting a new transaction within a flush.
     * Default: 50,000 (or 0 when WAL is off, for single-commit-per-flush). Set to 0 to commit once per flush.
     */
    public Builder withCommitEvery(final int commitEvery) {
      this.commitEvery = commitEvery;
      this.commitEveryExplicit = true;
      return this;
    }

    /**
     * If false (default), Write-Ahead Logging is disabled during import for maximum speed.
     * Set to true if crash recovery is needed during import.
     */
    public Builder withWAL(final boolean useWAL) {
      this.useWAL = useWAL;
      return this;
    }

    /**
     * WAL flush type. Default: NO (no flushing). Ignored if WAL is disabled.
     * Options: NO, YES_NOMETADATA, YES_FULL.
     */
    public Builder withWALFlush(final WALFile.FlushType walFlush) {
      this.walFlush = walFlush;
      return this;
    }

    /**
     * If true (default), {@link #createVertex} pre-allocates empty OUT and IN edge
     * segments at vertex creation time. This eliminates the lazy allocation cost
     * when the first edge is connected to the vertex.
     */
    public Builder withPreAllocateEdgeChunks(final boolean preAllocate) {
      this.preAllocateEdgeChunks = preAllocate;
      return this;
    }

    /**
     * If true, edge connection during flush() and close() is parallelized across
     * multiple async threads, partitioned by bucket ID. Each bucket's edges are
     * routed to a consistent thread slot, so there is no page contention.
     * Default: true.
     */
    public Builder withParallelFlush(final boolean parallel) {
      this.parallelFlush = parallel;
      return this;
    }

    /**
     * Number of times a vertex-creation commit is retried when it fails with a transient
     * {@link NeedRetryException} (e.g. a Raft {@code QuorumNotReachedException} during a leader
     * re-election). Default: 10. Set to 0 to disable retries and fail fast on the first error.
     */
    public Builder withCommitRetries(final int commitRetries) {
      if (commitRetries < 0)
        throw new IllegalArgumentException("Commit retries must be >= 0");
      this.commitRetries = commitRetries;
      return this;
    }

    /**
     * Initial back-off in milliseconds before the first vertex-commit retry. Subsequent retries use
     * exponential back-off capped at 10000 ms. Default: 1000.
     */
    public Builder withCommitRetryDelay(final long commitRetryDelayMs) {
      if (commitRetryDelayMs < 0)
        throw new IllegalArgumentException("Commit retry delay must be >= 0");
      this.commitRetryDelayMs = commitRetryDelayMs;
      return this;
    }

    /**
     * Maximum number of entries retained in each of the OUT/IN head-chunk RID lookup caches (issue #5664).
     * Both caches are pure accelerators - a miss falls back to loading the vertex's head chunk from disk - so
     * bounding them with an LRU keeps memory flat on a long-lived stream instead of growing with the number of
     * distinct vertices touched. Default: 1,000,000 (roughly 100-150 MB per cache).
     */
    public Builder withChunkCacheCapacity(final int chunkCacheCapacity) {
      if (chunkCacheCapacity <= 0)
        throw new IllegalArgumentException("Chunk cache capacity must be > 0");
      this.chunkCacheCapacity = chunkCacheCapacity;
      return this;
    }

    /**
     * Maximum number of buffered deferred incoming edges before the incoming-edge connection pass that
     * {@link #close()} would otherwise run alone runs early, from {@link #flush()} (issue #5664). This
     * amortizes the cost over the load instead of paying it in one unbounded pass at close time. Default:
     * 5,000,000. Set to 0 to defer everything to close(), matching pre-#5664 behavior (unbounded buffer growth).
     */
    public Builder withMaxDeferredIncomingEdges(final int maxDeferredIncomingEdges) {
      if (maxDeferredIncomingEdges < 0)
        throw new IllegalArgumentException("Max deferred incoming edges must be >= 0");
      this.maxDeferredIncomingEdges = maxDeferredIncomingEdges;
      return this;
    }

    public GraphBatch build() {
      int effectiveBatchSize = batchSize;
      if (!batchSizeExplicit && expectedEdgeCount > 0)
        effectiveBatchSize = Math.max(MIN_BATCH_SIZE, Math.min(MAX_BATCH_SIZE, expectedEdgeCount));

      // Replication layers (e.g. Raft) need the WAL bytes captured during commit phase 1 to ship to the
      // followers; if we skipped the WAL the leader would write pages locally but the replicas would
      // silently miss the changes. Force WAL on for replicated databases (issue #4076).
      final boolean effectiveUseWAL = useWAL || database.isReplicated();
      if (effectiveUseWAL && !useWAL)
        LogManager.instance().log(GraphBatch.class, Level.INFO,
            "GraphBatch: WAL was disabled but the database is replicated, forcing WAL on so changes can be replicated");

      // When WAL is off and no explicit commitEvery, use 0 (single commit per flush)
      // to eliminate unnecessary transaction begin/commit overhead.
      // This must be decided on the EFFECTIVE WAL setting, not on the requested one: on a replicated
      // database the shortcut would otherwise commit a whole flush (up to batchSize edges) in one
      // transaction, which replication ships as a single Raft entry - past the maximum replicated entry
      // size the load then dies with ReplicatedEntryTooLargeException (issue #5470).
      final int effectiveCommitEvery = !commitEveryExplicit && !effectiveUseWAL ? 0 : commitEvery;

      // Reserve the slot here and not in Database.batch(): a builder that is configured with a bad value, or
      // simply dropped, must not leave the database unable to batch ever again (issue #5666). Everything from
      // here on either returns a batch that owns the slot or gives the slot straight back.
      if (guardOwner != null)
        guardOwner.batchStarted();

      try {
        return new GraphBatch(database, guardOwner, effectiveBatchSize, edgeListInitialSize, lightEdges,
            bidirectional, effectiveCommitEvery, effectiveUseWAL, walFlush, preAllocateEdgeChunks, parallelFlush,
            commitRetries, commitRetryDelayMs, chunkCacheCapacity, maxDeferredIncomingEdges);
      } catch (final RuntimeException | Error e) {
        if (guardOwner != null)
          guardOwner.batchFinished();
        throw e;
      }
    }
  }
}
