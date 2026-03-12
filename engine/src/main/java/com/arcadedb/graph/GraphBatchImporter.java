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

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.serializer.BinarySerializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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
 *   <li>Head chunk RID cache: skips vertex record loads when segment RID is already known</li>
 *   <li>Lazy vertex loading: vertex only loaded from disk on segment overflow</li>
 * </ul>
 * <p>
 * Usage:
 * <pre>
 * try (final GraphBatchImporter importer = GraphBatchImporter.builder(database)
 *     .withBatchSize(100_000)
 *     .withEdgeListInitialSize(2048)
 *     .withLightEdges(true)
 *     .withWAL(false)
 *     .build()) {
 *
 *   // Phase 1: create vertices (edge segments pre-allocated)
 *   MutableVertex v1 = importer.newVertex("Person").set("name", "Alice").save();
 *   MutableVertex v2 = importer.newVertex("Person").set("name", "Bob").save();
 *
 *   // Phase 2: buffer edges (outgoing flushed periodically, incoming deferred)
 *   importer.newEdge(v1.getIdentity(), "KNOWS", v2.getIdentity());
 *
 *   // Edges are flushed automatically on close (incoming edges connected here)
 * }
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphBatchImporter implements AutoCloseable {

  private final DatabaseInternal database;

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

  // --- Edge buffer: flat arrays for minimal GC pressure ---
  // Each edge occupies one slot across these parallel arrays.
  private int[]      edgeSrcBucketIds;
  private long[]     edgeSrcPositions;
  private int[]      edgeDstBucketIds;
  private long[]     edgeDstPositions;
  private int[]      edgeTypeBucketIds;   // first bucket id of the edge type (for light edge RID)
  private boolean[]  edgeHasProperties;
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
  private final Map<String, Integer> edgeTypeFirstBucketCache = new HashMap<>();

  // --- Head chunk RID cache: avoids vertex loads when chunk is already known ---
  private final Map<Long, RID> outChunkRIDCache = new HashMap<>();
  private final Map<Long, RID> inChunkRIDCache = new HashMap<>();

  // --- Deferred vertex head chunk updates: persisted in one batch pass at close() ---
  // vertexKey → latest segment RID that needs to be set on the vertex record
  private final Map<Long, RID> deferredOutHead = new HashMap<>();
  private final Map<Long, RID> deferredInHead = new HashMap<>();

  // --- Known-new vertices: created by createVertices(), guaranteed no existing segments ---
  // Allows skipping vertex record loads when creating first segment
  private final java.util.HashSet<Long> knownNewVertexKeys = new java.util.HashSet<>();

  // --- Statistics ---
  private long totalVerticesCreated;
  private long totalEdgesCreated;
  private long totalFlushes;
  private long totalFlushTimeNs;

  // --- Saved state for restore after close ---
  private final boolean savedReadYourWrites;
  private final boolean savedUseWAL;
  private final WALFile.FlushType savedWALFlush;

  private GraphBatchImporter(final DatabaseInternal database, final int batchSize, final int edgeListInitialSize,
      final boolean lightEdges, final boolean bidirectional, final int commitEvery,
      final boolean useWAL, final WALFile.FlushType walFlush, final boolean preAllocateEdgeChunks,
      final boolean parallelFlush) {
    this.database = database;
    this.batchSize = batchSize;
    this.edgeListInitialSize = edgeListInitialSize;
    this.lightEdges = lightEdges;
    this.bidirectional = bidirectional;
    this.commitEvery = commitEvery;
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

    // Save WAL settings (per-transaction, so we apply them in beginTx())
    savedUseWAL = true; // default
    savedWALFlush = WALFile.FlushType.YES_NOMETADATA; // default
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
    final RID[] rids = new RID[count];
    beginTx();

    for (int i = 0; i < count; i++) {
      final MutableVertex vertex = database.newVertex(typeName);
      vertex.save();
      rids[i] = vertex.getIdentity();
      knownNewVertexKeys.add(packVertexKey(rids[i].getBucketId(), rids[i].getPosition()));
    }

    database.commit();
    totalVerticesCreated += count;
    return rids;
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
    final RID[] rids = new RID[count];
    beginTx();

    for (int i = 0; i < count; i++) {
      final MutableVertex vertex = database.newVertex(typeName);
      final Object[] props = properties[i];
      if (props != null && props.length > 0) {
        for (int p = 0; p < props.length; p += 2)
          vertex.set((String) props[p], props[p + 1]);
      }
      vertex.save();
      rids[i] = vertex.getIdentity();
      knownNewVertexKeys.add(packVertexKey(rids[i].getBucketId(), rids[i].getPosition()));
    }

    database.commit();
    totalVerticesCreated += count;
    return rids;
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

    final int idx = edgeCount;
    edgeSrcBucketIds[idx] = sourceVertexRID.getBucketId();
    edgeSrcPositions[idx] = sourceVertexRID.getPosition();
    edgeDstBucketIds[idx] = destVertexRID.getBucketId();
    edgeDstPositions[idx] = destVertexRID.getPosition();
    edgeTypeBucketIds[idx] = typeBucketId;

    final boolean hasProps = edgeProperties != null && edgeProperties.length > 0;
    edgeHasProperties[idx] = hasProps;
    this.edgeProperties[idx] = hasProps ? edgeProperties : null;

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

    // --- PHASE 1: Create edge records for edges that need them (non-light) ---
    final RID[] edgeRIDs = new RID[edgeCount];
    beginTx();

    // Count non-light edges and group by bucket for bulk creation
    int nonLightCount = 0;
    for (int i = 0; i < edgeCount; i++) {
      if (lightEdges && !edgeHasProperties[i])
        edgeRIDs[i] = new RID(database, edgeTypeBucketIds[i], -1L);
      else
        nonLightCount++;
    }

    if (nonLightCount > 0)
      createEdgeRecordsBulk(edgeRIDs, nonLightCount);


    // --- PHASE 2: Partition by source bucket (O(n) counting sort + per-bucket position sort) ---
    final int maxBucket = partitionBySourceBucket();

    // --- PHASE 3: Connect outgoing edges ---
    if (parallelFlush) {
      // Commit edge records to release page locks before parallel work
      database.commit();
      connectOutgoingEdgesParallel(edgeRIDs, maxBucket);
    } else {
      connectOutgoingEdgesSorted(edgeRIDs);
      database.commit();
    }

    // --- PHASE 4: Accumulate incoming edges in deferred buffer (array-only, no DB) ---
    if (bidirectional)
      accumulateIncomingEdges(edgeRIDs);

    totalEdgesCreated += edgeCount;
    totalFlushes++;
    totalFlushTimeNs += System.nanoTime() - startNs;

    // Reset buffer
    edgeCount = 0;
    // Clear property references to allow GC
    Arrays.fill(edgeProperties, 0, edgeProperties.length, null);
  }

  /**
   * Bulk-creates edge records by pre-serializing all edges, then writing them to the bucket
   * in a single sequential pass. Bypasses per-record overhead (lock acquisition, validation,
   * event callbacks, findAvailableSpace) that the standard createRecord path imposes.
   */
  private void createEdgeRecordsBulk(final RID[] edgeRIDs, final int nonLightCount) {
    // Group edges by their target bucket for sequential page writes
    // For simplicity, we handle one bucket at a time (most common: all edges go to the same bucket)

    // Collect indices of non-light edges
    final int[] nonLightIndices = new int[nonLightCount];
    int nlIdx = 0;
    for (int i = 0; i < edgeCount; i++)
      if (edgeRIDs[i] == null) // null means not yet assigned = non-light
        nonLightIndices[nlIdx++] = i;

    // Pre-serialize all edge records
    final BinarySerializer serializer = database.getSerializer();
    final Binary[] serializedBuffers = new Binary[nonLightCount];

    for (int k = 0; k < nonLightCount; k++) {
      final int i = nonLightIndices[k];
      final EdgeType edgeType = (EdgeType) database.getSchema().getTypeByBucketId(edgeTypeBucketIds[i]);
      final RID srcRID = new RID(database, edgeSrcBucketIds[i], edgeSrcPositions[i]);
      final RID dstRID = new RID(database, edgeDstBucketIds[i], edgeDstPositions[i]);

      final MutableEdge edge = new MutableEdge(database, edgeType, srcRID, dstRID);
      if (edgeHasProperties[i])
        GraphEngine.setProperties(edge, edgeProperties[i]);

      serializedBuffers[k] = serializer.serializeEdge(database, edge).copyOfContent();
    }

    // Group by bucket and bulk-write
    // Find the bucket for edge records (all edges of the same type go to the same bucket)
    // Sort by bucket ID for sequential writes
    final int firstBucket = edgeTypeBucketIds[nonLightIndices[0]];
    boolean singleBucket = true;
    for (int k = 1; k < nonLightCount; k++) {
      if (edgeTypeBucketIds[nonLightIndices[k]] != firstBucket) {
        singleBucket = false;
        break;
      }
    }

    if (singleBucket) {
      // Fast path: all edges go to the same bucket
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(firstBucket);
      final RID[] bulkRIDs = new RID[nonLightCount];
      bucket.createRecordsBulk(serializedBuffers, 0, nonLightCount, bulkRIDs);

      // Map back to edgeRIDs
      for (int k = 0; k < nonLightCount; k++) {
        edgeRIDs[nonLightIndices[k]] = bulkRIDs[k];
        // Update transaction cache
        database.getTransaction().updateBucketRecordDelta(firstBucket, +1);
      }
    } else {
      // Slow path: multiple buckets — fall back to per-edge creation
      for (int k = 0; k < nonLightCount; k++) {
        final int i = nonLightIndices[k];
        final EdgeType edgeType = (EdgeType) database.getSchema().getTypeByBucketId(edgeTypeBucketIds[i]);
        final RID srcRID = new RID(database, edgeSrcBucketIds[i], edgeSrcPositions[i]);
        final RID dstRID = new RID(database, edgeDstBucketIds[i], edgeDstPositions[i]);

        final MutableEdge edge = new MutableEdge(database, edgeType, srcRID, dstRID);
        if (edgeHasProperties[i])
          GraphEngine.setProperties(edge, edgeProperties[i]);
        edge.save();
        edgeRIDs[i] = edge.getIdentity();
      }
    }
  }

  @Override
  public void close() {
    // Flush any remaining outgoing edges
    flush();

    // Connect all deferred incoming edges in one sorted pass
    if (bidirectional && inEdgeCount > 0)
      connectDeferredIncomingEdges();

    // Batch-update all vertex head chunk pointers in one pass
    if (!deferredOutHead.isEmpty() || !deferredInHead.isEmpty())
      batchUpdateVertexHeadChunks();

    // Restore database settings
    database.setReadYourWrites(savedReadYourWrites);

    LogManager.instance().log(this, Level.INFO,
        "GraphBatchImporter closed: vertices=%d edges=%d flushes=%d avgFlushMs=%.1f",
        null, totalVerticesCreated, totalEdgesCreated, totalFlushes,
        totalFlushes > 0 ? (totalFlushTimeNs / totalFlushes) / 1_000_000.0 : 0.0);
  }

  /**
   * Returns the total number of edges created so far (including flushed).
   */
  public long getTotalEdgesCreated() {
    return totalEdgesCreated;
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

    // Collect all vertex keys that need updating
    final java.util.Set<Long> allKeys = new java.util.HashSet<>(deferredOutHead.keySet());
    allKeys.addAll(deferredInHead.keySet());

    // Sort by vertex key for page locality
    final long[] sortedKeys = new long[allKeys.size()];
    int ki = 0;
    for (final long key : allKeys)
      sortedKeys[ki++] = key;
    Arrays.sort(sortedKeys);

    beginTx();
    int updated = 0;

    for (final long vertexKey : sortedKeys) {
      final int bucketId = (int) (vertexKey >>> 40);
      final long position = vertexKey & 0xFFFFFFFFFFL;

      MutableVertex vertex = new RID(database, bucketId, position).asVertex().modify();

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
      final EdgeSegment outChunk = getOrCreateOutSegmentDeferred(srcBucket, srcPos, vertexKey, totalBytesNeeded);

      if (lastSegmentIsNew) {
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
        beginTx();
        edgesInBatch = 0;
      }

      i = j;
    }
  }

  // ---------------------------------------------------------------------------
  // Deferred incoming edge accumulation and connection
  // ---------------------------------------------------------------------------

  /**
   * Accumulates incoming edge info from the current flush buffer into the deferred arrays.
   */
  private void accumulateIncomingEdges(final RID[] edgeRIDs) {
    final int needed = inEdgeCount + edgeCount;
    if (needed > inEdgeBucketIds.length)
      growIncomingBuffers(needed);

    for (int i = 0; i < edgeCount; i++) {
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
    LogManager.instance().log(this, Level.INFO,
        "Connecting %d deferred incoming edges...", null, inEdgeCount);

    final long startNs = System.nanoTime();

    // Build sort index, partitioned by destination bucket (O(n) counting sort)
    final int[] inSortIndex = new int[inEdgeCount];
    final int maxDstBucket = partitionIncomingByDestBucket(inSortIndex);

    if (parallelFlush)
      connectIncomingEdgesParallel(inSortIndex, maxDstBucket);
    else
      connectIncomingEdgesSequential(inSortIndex);

    final double ms = (System.nanoTime() - startNs) / 1_000_000.0;
    LogManager.instance().log(this, Level.INFO,
        "Incoming edges connected: %d edges in %.1f ms (%.0f edges/sec)",
        null, inEdgeCount, ms, inEdgeCount / (ms / 1000.0));

    // Free deferred buffers
    inEdgeCount = 0;
    inEdgeBucketIds = null;
    inEdgePositions = null;
    inVertexBucketIds = null;
    inVertexPositions = null;
    inDstBucketIds = null;
    inDstPositions = null;
  }

  private void connectIncomingEdgesSequential(final int[] inSortIndex) {
    beginTx();
    int i = 0;
    int edgesInBatch = 0;

    while (i < inEdgeCount) {
      final int idx = inSortIndex[i];
      final int dstBucket = inDstBucketIds[idx];
      final long dstPos = inDstPositions[idx];

      int j = i;
      while (j < inEdgeCount) {
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
      final EdgeSegment inChunk = getOrCreateInSegmentDeferred(dstBucket, dstPos, vertexKey, totalBytesNeeded);

      if (lastSegmentIsNew) {
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
        database.commit();
        beginTx();
        edgesInBatch = 0;
      }

      i = j;
    }

    database.commit();
  }

  private void connectIncomingEdgesParallel(final int[] inSortIndex, final int maxBucket) {
    final DatabaseAsyncExecutor async = database.async();
    final boolean savedAsyncWAL = async.isTransactionUseWAL();
    final WALFile.FlushType savedAsyncSync = async.getTransactionSync();
    async.setTransactionUseWAL(useWAL);
    async.setTransactionSync(walFlush);

    final AtomicReference<Throwable> error = new AtomicReference<>();
    final int parallelLevel = async.getParallelLevel();

    for (int b = 0; b <= maxBucket; b++) {
      if (bucketCounts[b] == 0)
        continue;

      final int from = bucketOffsets[b];
      final int to = bucketOffsets[b + 1];
      final int slot = b % parallelLevel;

      async.transaction(() -> connectIncomingEdgesRange(inSortIndex, from, to),
          3, null, e -> error.compareAndSet(null, e), slot);
    }

    async.waitCompletion();
    async.setTransactionUseWAL(savedAsyncWAL);
    async.setTransactionSync(savedAsyncSync);

    final Throwable t = error.get();
    if (t != null)
      throw (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t);
  }

  private void connectIncomingEdgesRange(final int[] inSortIndex, final int from, final int to) {
    int i = from;
    while (i < to) {
      final int idx = inSortIndex[i];
      final int dstBucket = inDstBucketIds[idx];
      final long dstPos = inDstPositions[idx];

      int j = i;
      while (j < to) {
        final int jIdx = inSortIndex[j];
        if (inDstBucketIds[jIdx] != dstBucket || inDstPositions[jIdx] != dstPos)
          break;
        j++;
      }

      final long vertexKey = packVertexKey(dstBucket, dstPos);
      final RID cachedChunkRID = inChunkRIDCache.get(vertexKey);

      EdgeSegment inChunk;
      if (cachedChunkRID != null) {
        inChunk = (EdgeSegment) database.lookupByRID(cachedChunkRID, true);
      } else {
        final MutableVertex dstVertex = new RID(database, dstBucket, dstPos).asVertex().modify();
        inChunk = getOrCreateInEdgeChunk(dstVertex);
      }

      addIncomingEdgesToSegmentBulkLazy(dstBucket, dstPos, inChunk, inSortIndex, i, j);

      i = j;
    }
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

  /**
   * Gets or creates an OUT segment for a vertex without loading the vertex record.
   * For known-new vertices (from createVertices()), skips the vertex load entirely.
   * Sets lastSegmentIsNew=true if a new segment was created (not yet persisted — caller
   * must fill it and call createRecord). Sets false if an existing segment was found.
   */
  private EdgeSegment getOrCreateOutSegmentDeferred(final int bucketId, final long position,
      final long vertexKey, final int dataBytesNeeded) {

    // Check cache first
    final RID cachedRID = outChunkRIDCache.get(vertexKey);
    if (cachedRID != null) {
      lastSegmentIsNew = false;
      return (EdgeSegment) database.lookupByRID(cachedRID, true);
    }

    // For known-new vertices, skip loading — we know there's no existing segment
    if (!knownNewVertexKeys.contains(vertexKey)) {
      final VertexInternal vertex = (VertexInternal) new RID(database, bucketId, position).asVertex();
      final RID headChunk = vertex.getOutEdgesHeadChunk();
      if (headChunk != null) {
        outChunkRIDCache.put(vertexKey, headChunk);
        lastSegmentIsNew = false;
        return (EdgeSegment) database.lookupByRID(headChunk, true);
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

    final RID cachedRID = inChunkRIDCache.get(vertexKey);
    if (cachedRID != null) {
      lastSegmentIsNew = false;
      return (EdgeSegment) database.lookupByRID(cachedRID, true);
    }

    if (!knownNewVertexKeys.contains(vertexKey)) {
      final VertexInternal vertex = (VertexInternal) new RID(database, bucketId, position).asVertex();
      final RID headChunk = vertex.getInEdgesHeadChunk();
      if (headChunk != null) {
        inChunkRIDCache.put(vertexKey, headChunk);
        lastSegmentIsNew = false;
        return (EdgeSegment) database.lookupByRID(headChunk, true);
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

  // ---------------------------------------------------------------------------
  // Edge segment helpers (with edgeListInitialSize override)
  // ---------------------------------------------------------------------------

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

    return maxBucket;
  }

  /**
   * Partitions deferred incoming edges by destination bucket using counting sort.
   */
  private int partitionIncomingByDestBucket(final int[] inSortIndex) {
    int maxBucket = 0;
    for (int i = 0; i < inEdgeCount; i++)
      if (inDstBucketIds[i] > maxBucket)
        maxBucket = inDstBucketIds[i];

    ensureCountingSortArrays(maxBucket);

    Arrays.fill(bucketCounts, 0, maxBucket + 1, 0);
    for (int i = 0; i < inEdgeCount; i++)
      bucketCounts[inDstBucketIds[i]]++;

    bucketOffsets[0] = 0;
    for (int b = 0; b <= maxBucket; b++)
      bucketOffsets[b + 1] = bucketOffsets[b] + bucketCounts[b];

    System.arraycopy(bucketOffsets, 0, countingSortCursor, 0, maxBucket + 1);
    for (int i = 0; i < inEdgeCount; i++) {
      final int bucket = inDstBucketIds[i];
      inSortIndex[countingSortCursor[bucket]++] = i;
    }

    // Ensure merge temp buffer is large enough
    if (mergeTmp.length < inEdgeCount / 2 + 1)
      mergeTmp = new int[inEdgeCount / 2 + 1];

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
    final DatabaseAsyncExecutor async = database.async();
    final boolean savedAsyncWAL = async.isTransactionUseWAL();
    final WALFile.FlushType savedAsyncSync = async.getTransactionSync();
    async.setTransactionUseWAL(useWAL);
    async.setTransactionSync(walFlush);

    final AtomicReference<Throwable> error = new AtomicReference<>();
    final int parallelLevel = async.getParallelLevel();

    for (int b = 0; b <= maxBucket; b++) {
      if (bucketCounts[b] == 0)
        continue;

      final int from = bucketOffsets[b];
      final int to = bucketOffsets[b + 1];
      final int slot = b % parallelLevel;

      async.transaction(() -> connectEdgesRange(edgeRIDs, from, to),
          3, null, e -> error.compareAndSet(null, e), slot);
    }

    async.waitCompletion();
    async.setTransactionUseWAL(savedAsyncWAL);
    async.setTransactionSync(savedAsyncSync);

    final Throwable t = error.get();
    if (t != null)
      throw (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t);
  }

  /**
   * Connects outgoing edges for a range of the sorted index. Used by the parallel path.
   */
  private void connectEdgesRange(final RID[] edgeRIDs, final int from, final int to) {
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

      final long vertexKey = packVertexKey(srcBucket, srcPos);
      final RID cachedChunkRID = outChunkRIDCache.get(vertexKey);

      EdgeSegment outChunk;
      if (cachedChunkRID != null) {
        outChunk = (EdgeSegment) database.lookupByRID(cachedChunkRID, true);
      } else {
        final MutableVertex srcVertex = new RID(database, srcBucket, srcPos).asVertex().modify();
        outChunk = getOrCreateOutEdgeChunk(srcVertex);
      }

      addEdgesToSegmentBulkLazy(srcBucket, srcPos, Vertex.DIRECTION.OUT, outChunk, edgeRIDs, i, j, true);

      i = j;
    }
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
      final int cmp = Integer.compare(edgeSrcBucketIds[a], edgeSrcBucketIds[b]);
      return cmp != 0 ? cmp : Long.compare(edgeSrcPositions[a], edgeSrcPositions[b]);
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
    return new Builder((DatabaseInternal) database);
  }

  public static class Builder {
    private static final int MIN_BATCH_SIZE = 100_000;
    private static final int MAX_BATCH_SIZE = 5_000_000;

    private final DatabaseInternal database;
    private int                batchSize            = MIN_BATCH_SIZE;
    private boolean            batchSizeExplicit    = false;
    private int                expectedEdgeCount    = 0;
    private int                edgeListInitialSize  = 2048;
    private boolean            lightEdges           = true;
    private boolean            bidirectional        = true;
    private int                commitEvery          = 50_000;
    private boolean            useWAL               = false;
    private WALFile.FlushType  walFlush             = WALFile.FlushType.NO;
    private boolean            preAllocateEdgeChunks = true;
    private boolean            parallelFlush         = false;

    Builder(final DatabaseInternal database) {
      this.database = database;
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
     * batch size is provided, the batch size is auto-tuned to {@code expectedEdgeCount / 5},
     * clamped between 100K and 5M. This typically yields ~5 flush cycles, which benchmarks
     * show is the optimal trade-off between sort overhead and flush frequency.
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
     */
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
     * Default: 50,000. Set to 0 to commit once per flush.
     */
    public Builder withCommitEvery(final int commitEvery) {
      this.commitEvery = commitEvery;
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
     * Default: false.
     */
    public Builder withParallelFlush(final boolean parallel) {
      this.parallelFlush = parallel;
      return this;
    }

    public GraphBatchImporter build() {
      int effectiveBatchSize = batchSize;
      if (!batchSizeExplicit && expectedEdgeCount > 0)
        effectiveBatchSize = Math.max(MIN_BATCH_SIZE, Math.min(MAX_BATCH_SIZE, expectedEdgeCount / 5));

      return new GraphBatchImporter(database, effectiveBatchSize, edgeListInitialSize, lightEdges,
          bidirectional, commitEvery, useWAL, walFlush, preAllocateEdgeChunks, parallelFlush);
    }
  }
}
