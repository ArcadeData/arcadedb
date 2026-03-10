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
package com.arcadedb.grapholap;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Graph Analytical View (GAV) — a synchronized, read-optimized CSR representation of the OLTP graph.
 * <p>
 * Stores one {@link CSRAdjacencyIndex} per edge type, with a per-bucket
 * {@link NodeIdMapping} across all vertex types and per-bucket {@link ColumnStore}s
 * for property access. This enables:
 * <ul>
 *   <li>Type-filtered traversal and counting via edge type selection</li>
 *   <li>Per-bucket parallelism aligned with ArcadeDB's storage architecture</li>
 *   <li>Columnar property access (dictionary-encoded strings, null bitmaps)</li>
 *   <li>Optional auto-update on transaction commit</li>
 * </ul>
 * <p>
 * Usage:
 * <pre>
 *   // Via builder (recommended)
 *   GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
 *       .withVertexTypes("Person", "Company")
 *       .withEdgeTypes("FOLLOWS", "WORKS_AT")
 *       .withProperties("name", "age")
 *       .withAutoUpdate(true)
 *       .build();
 *
 *   int nodeId = gav.getNodeId(vertexRID);
 *   long followsOut = gav.countEdges(nodeId, Vertex.DIRECTION.OUT, "FOLLOWS");
 *   int[] neighbors = gav.getVertices(nodeId, Vertex.DIRECTION.OUT, "FOLLOWS");
 *   Object name = gav.getProperty(nodeId, "name");
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphAnalyticalView implements GraphTraversalProvider {

  public enum Status {
    NOT_BUILT, BUILDING, READY
  }

  private final Database                       database;
  private final String                         name;
  private final String[]                       vertexTypes;
  private final String[]                       edgeTypes;
  private final String[]                       propertyFilter;
  private final boolean                        autoUpdate;

  private volatile Map<String, CSRAdjacencyIndex> csrPerType;
  private volatile NodeIdMapping                  nodeMapping;
  private volatile ColumnStore[]                  bucketColumns;
  private volatile long                           buildTimestamp;
  private volatile Status                         status = Status.NOT_BUILT;
  private volatile CountDownLatch                 readyLatch = new CountDownLatch(1);

  // Auto-update listeners
  private AfterRecordCreateListener createListener;
  private AfterRecordUpdateListener updateListener;
  private AfterRecordDeleteListener deleteListener;

  /**
   * Creates a builder for configuring the analytical view.
   */
  public static GraphAnalyticalViewBuilder builder(final Database database) {
    return new GraphAnalyticalViewBuilder(database);
  }

  /**
   * Simple constructor for backward compatibility. Use {@link #builder(Database)} for full control.
   */
  public GraphAnalyticalView(final Database database) {
    this(database, null, null, null, null, false);
  }

  GraphAnalyticalView(final Database database, final String name, final String[] vertexTypes, final String[] edgeTypes,
      final String[] propertyFilter, final boolean autoUpdate) {
    this.database = database;
    this.name = name;
    this.vertexTypes = vertexTypes;
    this.edgeTypes = edgeTypes;
    this.propertyFilter = propertyFilter;
    this.autoUpdate = autoUpdate;
  }

  /**
   * Registers this view as a {@link GraphTraversalProvider} so the query planner can discover it.
   * Called by the builder after construction.
   */
  void registerAsTraversalProvider() {
    GraphTraversalProviderRegistry.register(database, this);
  }

  /**
   * Builds (or rebuilds) the analytical view synchronously.
   * Status transitions: NOT_BUILT/READY → BUILDING → READY.
   */
  public void build() {
    build(vertexTypes, edgeTypes);
  }

  /**
   * Builds (or rebuilds) the analytical view synchronously.
   *
   * @param vertexTypes vertex type names to include (null = all)
   * @param edgeTypes   edge type names to include (null = all)
   */
  public void build(final String[] vertexTypes, final String[] edgeTypes) {
    status = Status.BUILDING;
    try {
      final CSRBuilder builder = new CSRBuilder(database, propertyFilter);
      final CSRBuilder.CSRResult result = builder.build(vertexTypes, edgeTypes);

      this.csrPerType = result.getCsrPerType();
      this.nodeMapping = result.getMapping();
      this.bucketColumns = result.getBucketColumns();
      this.buildTimestamp = System.currentTimeMillis();
      this.status = Status.READY;
      this.readyLatch.countDown();

      if (autoUpdate && createListener == null)
        registerAutoUpdateListeners();
    } catch (final Exception e) {
      this.status = csrPerType != null ? Status.READY : Status.NOT_BUILT;
      throw e;
    }
  }

  /**
   * Builds the analytical view asynchronously in a background thread.
   * Returns immediately. Use {@link #awaitReady(long, TimeUnit)} or
   * {@link #getStatus()} to check completion.
   */
  public void buildAsync() {
    status = Status.BUILDING;
    readyLatch = new CountDownLatch(1);
    final Thread buildThread = new Thread(() -> {
      try {
        final CSRBuilder builder = new CSRBuilder(database, propertyFilter);
        final CSRBuilder.CSRResult result = builder.build(vertexTypes, edgeTypes);

        this.csrPerType = result.getCsrPerType();
        this.nodeMapping = result.getMapping();
        this.bucketColumns = result.getBucketColumns();
        this.buildTimestamp = System.currentTimeMillis();
        this.status = Status.READY;
        this.readyLatch.countDown();

        if (autoUpdate && createListener == null)
          registerAutoUpdateListeners();
      } catch (final Exception e) {
        this.status = csrPerType != null ? Status.READY : Status.NOT_BUILT;
        this.readyLatch.countDown();
        LogManager.instance().log(this, Level.SEVERE, "Async build of GraphAnalyticalView '%s' failed", e, name);
      }
    }, "gav-build" + (name != null ? "-" + name : ""));
    buildThread.setDaemon(true);
    buildThread.start();
  }

  /**
   * Waits until the view reaches READY status or the timeout expires.
   *
   * @return true if the view is READY, false if the timeout elapsed
   */
  public boolean awaitReady(final long timeout, final TimeUnit unit) {
    if (status == Status.READY)
      return true;
    try {
      return readyLatch.await(timeout, unit);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * Unregisters auto-update listeners, removes from registries, and releases resources.
   * Call this when the view is no longer needed.
   */
  public void close() {
    unregisterAutoUpdateListeners();
    GraphTraversalProviderRegistry.unregister(database, this);
    if (name != null)
      GraphAnalyticalViewRegistry.unregister(database, name);
  }

  // --- GraphTraversalProvider SPI ---

  @Override
  public boolean coversVertexType(final String typeName) {
    if (typeName == null)
      return vertexTypes == null; // null = all types
    if (vertexTypes == null)
      return true; // we include all vertex types
    for (final String vt : vertexTypes)
      if (vt.equals(typeName))
        return true;
    return false;
  }

  @Override
  public boolean coversEdgeType(final String edgeTypeName) {
    if (edgeTypeName == null)
      return edgeTypes == null; // null = all types
    if (edgeTypes == null)
      return true; // we include all edge types
    for (final String et : edgeTypes)
      if (et.equals(edgeTypeName))
        return true;
    return false;
  }

  @Override
  public int[] getNeighborIds(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
    checkBuilt();
    return getVertices(nodeId, direction, edgeTypes);
  }

  // --- Node ID / RID mapping ---

  /**
   * Returns the dense node ID for a given RID, or -1 if not in the view.
   */
  public int getNodeId(final RID rid) {
    checkBuilt();
    return nodeMapping.getGlobalId(rid);
  }

  /**
   * Returns the RID for a given dense node ID.
   */
  public RID getRID(final int nodeId) {
    checkBuilt();
    return nodeMapping.getRID(nodeId);
  }

  // --- Node type queries ---

  /**
   * Returns the vertex type name for the given node.
   */
  public String getNodeTypeName(final int nodeId) {
    checkBuilt();
    return nodeMapping.getTypeName(nodeId);
  }

  /**
   * Returns the bucket index for the given node.
   */
  public int getNodeBucketIdx(final int nodeId) {
    checkBuilt();
    return nodeMapping.getBucketIdx(nodeId);
  }

  // --- Edge counting (mirrors Vertex.countEdges) ---

  /**
   * Returns the edge count for a node in the given direction, optionally filtered by edge types.
   * Mirrors {@code Vertex.countEdges(DIRECTION, String...)}.
   */
  public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
    checkBuilt();
    if (edgeTypes != null && edgeTypes.length > 0) {
      long total = 0;
      for (final String edgeType : edgeTypes) {
        final CSRAdjacencyIndex csr = csrPerType.get(edgeType);
        if (csr != null)
          total += countDirectional(csr, nodeId, direction);
      }
      return total;
    }
    long total = 0;
    for (final CSRAdjacencyIndex csr : csrPerType.values())
      total += countDirectional(csr, nodeId, direction);
    return total;
  }

  // --- Neighbor access (mirrors Vertex.getVertices) ---

  /**
   * Returns dense node IDs of connected vertices in the given direction, optionally filtered by edge types.
   * Mirrors {@code Vertex.getVertices(DIRECTION, String...)}.
   */
  public int[] getVertices(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
    checkBuilt();

    if (edgeTypes != null && edgeTypes.length == 1) {
      final CSRAdjacencyIndex csr = csrPerType.get(edgeTypes[0]);
      if (csr == null)
        return new int[0];
      return getNeighborsFromCSR(csr, nodeId, direction);
    }

    final Iterable<CSRAdjacencyIndex> csrs;
    if (edgeTypes != null && edgeTypes.length > 0) {
      final java.util.List<CSRAdjacencyIndex> list = new java.util.ArrayList<>(edgeTypes.length);
      for (final String et : edgeTypes) {
        final CSRAdjacencyIndex csr = csrPerType.get(et);
        if (csr != null)
          list.add(csr);
      }
      csrs = list;
    } else {
      csrs = csrPerType.values();
    }

    final int totalDeg = (int) countEdges(nodeId, direction, edgeTypes);
    if (totalDeg == 0)
      return new int[0];

    final int[] result = new int[totalDeg];
    int pos = 0;
    for (final CSRAdjacencyIndex csr : csrs) {
      if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
        final int fStart = csr.outOffset(nodeId);
        final int fLen = csr.outOffsetEnd(nodeId) - fStart;
        if (fLen > 0) {
          System.arraycopy(csr.getForwardNeighbors(), fStart, result, pos, fLen);
          pos += fLen;
        }
      }
      if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
        final int bStart = csr.inOffset(nodeId);
        final int bLen = csr.inOffsetEnd(nodeId) - bStart;
        if (bLen > 0) {
          System.arraycopy(csr.getBackwardNeighbors(), bStart, result, pos, bLen);
          pos += bLen;
        }
      }
    }
    if (pos < result.length) {
      final int[] trimmed = new int[pos];
      System.arraycopy(result, 0, trimmed, 0, pos);
      Arrays.sort(trimmed);
      return trimmed;
    }
    Arrays.sort(result);
    return result;
  }

  // --- Vectorized scan ---

  /**
   * Creates a vectorized scan operator for the neighbors of a node, for a specific edge type.
   */
  public CSRScanOperator scanNeighbors(final int nodeId, final Vertex.DIRECTION direction, final String edgeType) {
    checkBuilt();
    final CSRAdjacencyIndex csr = csrPerType.get(edgeType);
    if (csr == null)
      throw new IllegalArgumentException("Edge type not in view: " + edgeType);
    return new CSRScanOperator(csr, nodeId, direction);
  }

  // --- Connectivity (mirrors Vertex.isConnectedTo) ---

  /**
   * Checks if nodeA has an edge to nodeB, optionally filtered by edge type.
   * O(log(degree)) using binary search on sorted CSR.
   */
  public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    checkBuilt();
    if (edgeTypes != null && edgeTypes.length > 0) {
      for (final String edgeType : edgeTypes)
        if (isConnectedForType(nodeA, nodeB, direction, edgeType))
          return true;
      return false;
    }
    for (final String edgeType : csrPerType.keySet())
      if (isConnectedForType(nodeA, nodeB, direction, edgeType))
        return true;
    return false;
  }

  /**
   * Counts common neighbors between two nodes, optionally filtered by edge types.
   */
  public int countCommonNeighbors(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    checkBuilt();
    int total = 0;
    if (edgeTypes != null && edgeTypes.length > 0) {
      for (final String edgeType : edgeTypes)
        total += countCommonForType(nodeA, nodeB, direction, edgeType);
    } else {
      for (final String edgeType : csrPerType.keySet())
        total += countCommonForType(nodeA, nodeB, direction, edgeType);
    }
    return total;
  }

  // --- Property access (per-bucket columnar) ---

  /**
   * Returns the property value for a given node, or null if not set.
   * Mirrors {@code Document.get(String)}.
   */
  public Object getProperty(final int nodeId, final String propertyName) {
    checkBuilt();
    if (bucketColumns == null)
      return null;
    final int bucketIdx = nodeMapping.getBucketIdx(nodeId);
    final int localId = nodeMapping.getLocalId(nodeId);
    return bucketColumns[bucketIdx].getValue(localId, propertyName);
  }

  /**
   * Returns the per-bucket column store for direct vectorized access.
   */
  public ColumnStore getBucketColumnStore(final int bucketIdx) {
    checkBuilt();
    return bucketColumns != null ? bucketColumns[bucketIdx] : null;
  }

  /**
   * Returns the column store for a given node's bucket.
   */
  public ColumnStore getColumnStore() {
    checkBuilt();
    // Return first non-empty bucket column store for backward compatibility
    if (bucketColumns != null)
      for (final ColumnStore cs : bucketColumns)
        if (cs.getColumnCount() > 0)
          return cs;
    return null;
  }

  // --- Metadata ---

  /**
   * Returns the CSR index for a specific edge type, or null if not present.
   */
  public CSRAdjacencyIndex getCSRIndex(final String edgeType) {
    checkBuilt();
    return csrPerType.get(edgeType);
  }

  public Set<String> getEdgeTypes() {
    checkBuilt();
    return Collections.unmodifiableSet(csrPerType.keySet());
  }

  public NodeIdMapping getNodeMapping() {
    checkBuilt();
    return nodeMapping;
  }

  public int getNodeCount() {
    checkBuilt();
    return nodeMapping.size();
  }

  public int getEdgeCount() {
    checkBuilt();
    int total = 0;
    for (final CSRAdjacencyIndex csr : csrPerType.values())
      total += csr.getEdgeCount();
    return total;
  }

  public int getEdgeCount(final String edgeType) {
    checkBuilt();
    final CSRAdjacencyIndex csr = csrPerType.get(edgeType);
    return csr != null ? csr.getEdgeCount() : 0;
  }

  public String getName() {
    return name;
  }

  public Status getStatus() {
    return status;
  }

  public long getBuildTimestamp() {
    return buildTimestamp;
  }

  public boolean isBuilt() {
    return csrPerType != null;
  }

  public boolean isReady() {
    return status == Status.READY;
  }

  public boolean isAutoUpdate() {
    return autoUpdate;
  }

  public String[] getVertexTypes() {
    return vertexTypes;
  }

  public String[] getEdgeTypeFilter() {
    return edgeTypes;
  }

  public String[] getPropertyFilter() {
    return propertyFilter;
  }

  public long getMemoryUsageBytes() {
    if (csrPerType == null)
      return 0;
    long total = 0;
    for (final CSRAdjacencyIndex csr : csrPerType.values())
      total += csr.getMemoryUsageBytes();
    if (bucketColumns != null)
      for (final ColumnStore cs : bucketColumns)
        total += cs.getMemoryUsageBytes();
    return total;
  }

  // --- Private helpers ---

  private void registerAutoUpdateListeners() {
    createListener = record -> scheduleRebuild();
    updateListener = record -> scheduleRebuild();
    deleteListener = record -> scheduleRebuild();

    database.getEvents().registerListener(createListener);
    database.getEvents().registerListener(updateListener);
    database.getEvents().registerListener(deleteListener);
  }

  private void unregisterAutoUpdateListeners() {
    if (createListener != null) {
      database.getEvents().unregisterListener(createListener);
      database.getEvents().unregisterListener(updateListener);
      database.getEvents().unregisterListener(deleteListener);
      createListener = null;
      updateListener = null;
      deleteListener = null;
    }
  }

  private void scheduleRebuild() {
    try {
      final DatabaseInternal dbInternal = (DatabaseInternal) database;
      if (dbInternal.isTransactionActive()) {
        final String callbackId = "gav-rebuild" + (name != null ? "-" + name : "");
        dbInternal.getTransaction().addAfterCommitCallbackIfAbsent(callbackId, () -> {
          try {
            buildAsync();
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.WARNING, "Failed to auto-rebuild GraphAnalyticalView '%s'", e, name);
          }
        });
      }
    } catch (final Exception e) {
      // Not in a transaction context or database is closing — skip
    }
  }

  private boolean isConnectedForType(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String edgeType) {
    final CSRAdjacencyIndex csr = csrPerType.get(edgeType);
    if (csr == null)
      return false;
    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
      final int[] neighbors = csr.getForwardNeighbors();
      final int[] offsets = csr.getForwardOffsets();
      if (Arrays.binarySearch(neighbors, offsets[nodeA], offsets[nodeA + 1], nodeB) >= 0)
        return true;
    }
    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
      final int[] neighbors = csr.getBackwardNeighbors();
      final int[] offsets = csr.getBackwardOffsets();
      if (Arrays.binarySearch(neighbors, offsets[nodeA], offsets[nodeA + 1], nodeB) >= 0)
        return true;
    }
    return false;
  }

  private int countCommonForType(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String edgeType) {
    final CSRAdjacencyIndex csr = csrPerType.get(edgeType);
    if (csr == null)
      return 0;
    int count = 0;
    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
      final int[] neighbors = csr.getForwardNeighbors();
      final int[] offsets = csr.getForwardOffsets();
      count += sortedIntersectionCount(neighbors, offsets[nodeA], offsets[nodeA + 1],
          neighbors, offsets[nodeB], offsets[nodeB + 1]);
    }
    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
      final int[] neighbors = csr.getBackwardNeighbors();
      final int[] offsets = csr.getBackwardOffsets();
      count += sortedIntersectionCount(neighbors, offsets[nodeA], offsets[nodeA + 1],
          neighbors, offsets[nodeB], offsets[nodeB + 1]);
    }
    return count;
  }

  private static long countDirectional(final CSRAdjacencyIndex csr, final int nodeId,
      final Vertex.DIRECTION direction) {
    long count = 0;
    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH)
      count += csr.outDegree(nodeId);
    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH)
      count += csr.inDegree(nodeId);
    return count;
  }

  private static int[] getNeighborsFromCSR(final CSRAdjacencyIndex csr, final int nodeId,
      final Vertex.DIRECTION direction) {
    if (direction == Vertex.DIRECTION.OUT)
      return Arrays.copyOfRange(csr.getForwardNeighbors(), csr.outOffset(nodeId), csr.outOffsetEnd(nodeId));
    if (direction == Vertex.DIRECTION.IN)
      return Arrays.copyOfRange(csr.getBackwardNeighbors(), csr.inOffset(nodeId), csr.inOffsetEnd(nodeId));

    final int outLen = csr.outDegree(nodeId);
    final int inLen = csr.inDegree(nodeId);
    if (outLen == 0 && inLen == 0)
      return new int[0];
    final int[] result = new int[outLen + inLen];
    if (outLen > 0)
      System.arraycopy(csr.getForwardNeighbors(), csr.outOffset(nodeId), result, 0, outLen);
    if (inLen > 0)
      System.arraycopy(csr.getBackwardNeighbors(), csr.inOffset(nodeId), result, outLen, inLen);
    Arrays.sort(result);
    return result;
  }

  private void checkBuilt() {
    if (csrPerType == null)
      throw new IllegalStateException("GraphAnalyticalView has not been built yet. Call build() first.");
  }

  private static int sortedIntersectionCount(final int[] a, int startA, final int endA,
      final int[] b, int startB, final int endB) {
    int count = 0;
    while (startA < endA && startB < endB) {
      final int va = a[startA];
      final int vb = b[startB];
      if (va == vb) {
        count++;
        startA++;
        startB++;
      } else if (va < vb)
        startA++;
      else
        startB++;
    }
    return count;
  }
}
