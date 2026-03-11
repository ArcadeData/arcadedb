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
package com.arcadedb.graph.olap;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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

  // Incremental auto-update: delta overlay + collector
  private volatile DeltaOverlay overlay;
  private DeltaCollector         deltaCollector;
  private int                    compactionThreshold = 10000;

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

      this.overlay = null; // clear overlay on full rebuild

      if (autoUpdate && deltaCollector == null)
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
        this.overlay = null; // clear overlay on full rebuild
        this.status = Status.READY;
        this.readyLatch.countDown();

        if (autoUpdate && deltaCollector == null)
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
   * Unregisters auto-update listeners, removes from registries and schema, and releases resources.
   * Call this when the view is no longer needed.
   */
  public void close() {
    unregisterAutoUpdateListeners();
    GraphTraversalProviderRegistry.unregister(database, this);
    if (name != null) {
      GraphAnalyticalViewRegistry.unregister(database, name);
      GraphAnalyticalViewPersistence.remove(database, name);
    }
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
    final DeltaOverlay ov = this.overlay;
    if (ov != null)
      return ov.resolveNodeId(rid, nodeMapping);
    return nodeMapping.getGlobalId(rid);
  }

  /**
   * Returns the RID for a given dense node ID.
   */
  public RID getRID(final int nodeId) {
    checkBuilt();
    if (nodeId >= nodeMapping.size()) {
      final DeltaOverlay ov = this.overlay;
      if (ov != null) {
        final RID rid = ov.getOverflowRID(nodeId);
        if (rid != null)
          return rid;
      }
    }
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
          total += countDirectional(csr, nodeId, direction, edgeType);
        else {
          // No base CSR but overlay may have edges for this type
          final DeltaOverlay ov = this.overlay;
          if (ov != null) {
            if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH)
              total += ov.getAddedOutNeighbors(nodeId, edgeType).length;
            if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH)
              total += ov.getAddedInNeighbors(nodeId, edgeType).length;
          }
        }
      }
      return total;
    }
    long total = 0;
    for (final var entry : csrPerType.entrySet())
      total += countDirectional(entry.getValue(), nodeId, direction, entry.getKey());
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
      if (csr != null)
        return getNeighborsFromCSR(csr, nodeId, direction, edgeTypes[0]);
      // No base CSR — check overlay only
      final DeltaOverlay ov = this.overlay;
      if (ov == null)
        return new int[0];
      return getNeighborsFromCSR(null, nodeId, direction, edgeTypes[0]);
    }

    // Multiple edge types: collect from each
    final java.util.List<int[]> segments = new java.util.ArrayList<>();
    int totalLen = 0;
    if (edgeTypes != null && edgeTypes.length > 0) {
      for (final String et : edgeTypes) {
        final CSRAdjacencyIndex csr = csrPerType.get(et);
        final int[] neighbors = csr != null
            ? getNeighborsFromCSR(csr, nodeId, direction, et)
            : getNeighborsFromCSR(null, nodeId, direction, et);
        if (neighbors.length > 0) {
          segments.add(neighbors);
          totalLen += neighbors.length;
        }
      }
    } else {
      for (final var entry : csrPerType.entrySet()) {
        final int[] neighbors = getNeighborsFromCSR(entry.getValue(), nodeId, direction, entry.getKey());
        if (neighbors.length > 0) {
          segments.add(neighbors);
          totalLen += neighbors.length;
        }
      }
    }

    if (totalLen == 0)
      return new int[0];
    if (segments.size() == 1)
      return segments.get(0);

    final int[] result = new int[totalLen];
    int pos = 0;
    for (final int[] seg : segments) {
      System.arraycopy(seg, 0, result, pos, seg.length);
      pos += seg.length;
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
    // Check overlay first (overrides and overflow nodes)
    final DeltaOverlay ov = this.overlay;
    if (ov != null) {
      final Object override = ov.getPropertyOverride(nodeId, propertyName);
      if (override != DeltaOverlay.UNSET)
        return override;
    }
    // Fall back to base column store
    if (bucketColumns == null || nodeId >= nodeMapping.size())
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
    final DeltaOverlay ov = this.overlay;
    if (ov != null)
      return ov.getTotalNodeCount();
    return nodeMapping.size();
  }

  public int getEdgeCount() {
    checkBuilt();
    int total = 0;
    for (final CSRAdjacencyIndex csr : csrPerType.values())
      total += csr.getEdgeCount();
    final DeltaOverlay ov = this.overlay;
    if (ov != null)
      total += ov.getDeltaEdgeCount();
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

  Database getDatabase() {
    return database;
  }

  public int getCompactionThreshold() {
    return compactionThreshold;
  }

  void setCompactionThreshold(final int compactionThreshold) {
    this.compactionThreshold = compactionThreshold;
  }

  public long getMemoryUsageBytes() {
    if (csrPerType == null)
      return 0;
    long total = 0;
    // CSR arrays (forward + backward offsets and neighbors per edge type)
    for (final CSRAdjacencyIndex csr : csrPerType.values())
      total += csr.getMemoryUsageBytes();
    // Column stores (per-bucket property data)
    if (bucketColumns != null)
      for (final ColumnStore cs : bucketColumns)
        total += cs.getMemoryUsageBytes();
    // Node ID mapping (bucketId→idx maps, position arrays, base offsets)
    if (nodeMapping != null)
      total += nodeMapping.getMemoryUsageBytes();
    return total;
  }

  /**
   * Returns detailed statistics about this view as a map.
   * Includes vertex/edge counts, memory breakdown, edge types, and overlay state.
   */
  public Map<String, Object> getStats() {
    final Map<String, Object> stats = new HashMap<>();
    stats.put("name", name);
    stats.put("status", status.name());
    stats.put("autoUpdate", autoUpdate);

    if (csrPerType == null) {
      stats.put("nodeCount", 0);
      stats.put("edgeCount", 0);
      stats.put("memoryUsageBytes", 0L);
      return stats;
    }

    stats.put("nodeCount", getNodeCount());
    stats.put("edgeCount", getEdgeCount());
    stats.put("buildTimestamp", buildTimestamp);

    // Per-edge-type breakdown
    final Map<String, Object> edgeTypeStats = new HashMap<>();
    for (final var entry : csrPerType.entrySet()) {
      final CSRAdjacencyIndex csr = entry.getValue();
      final Map<String, Object> etStat = new HashMap<>();
      etStat.put("edgeCount", csr.getEdgeCount());
      etStat.put("nodeCount", csr.getNodeCount());
      etStat.put("memoryBytes", csr.getMemoryUsageBytes());
      edgeTypeStats.put(entry.getKey(), etStat);
    }
    stats.put("edgeTypes", edgeTypeStats);

    // Memory breakdown
    long csrMemory = 0;
    for (final CSRAdjacencyIndex csr : csrPerType.values())
      csrMemory += csr.getMemoryUsageBytes();
    long columnMemory = 0;
    int propertyCount = 0;
    if (bucketColumns != null) {
      for (final ColumnStore cs : bucketColumns) {
        columnMemory += cs.getMemoryUsageBytes();
        propertyCount = Math.max(propertyCount, cs.getColumnCount());
      }
    }
    long mappingMemory = nodeMapping != null ? nodeMapping.getMemoryUsageBytes() : 0;

    stats.put("memoryUsageBytes", csrMemory + columnMemory + mappingMemory);
    stats.put("csrMemoryBytes", csrMemory);
    stats.put("columnMemoryBytes", columnMemory);
    stats.put("mappingMemoryBytes", mappingMemory);
    stats.put("propertyCount", propertyCount);

    if (vertexTypes != null)
      stats.put("vertexTypes", java.util.Arrays.asList(vertexTypes));
    if (edgeTypes != null)
      stats.put("edgeTypeFilter", java.util.Arrays.asList(edgeTypes));
    if (propertyFilter != null)
      stats.put("propertyFilter", java.util.Arrays.asList(propertyFilter));

    // Overlay state
    final DeltaOverlay ov = this.overlay;
    if (ov != null) {
      stats.put("overlayActive", true);
      stats.put("overlayOverflowNodes", ov.getOverflowCount());
      stats.put("overlayDeltaEdges", ov.getDeltaEdgeCount());
    } else {
      stats.put("overlayActive", false);
    }

    stats.put("compactionThreshold", compactionThreshold);
    return stats;
  }

  // --- Private helpers ---

  private void registerAutoUpdateListeners() {
    deltaCollector = new DeltaCollector(this);
    database.getEvents().registerListener((AfterRecordCreateListener) deltaCollector);
    database.getEvents().registerListener((AfterRecordUpdateListener) deltaCollector);
    database.getEvents().registerListener((AfterRecordDeleteListener) deltaCollector);
  }

  private void unregisterAutoUpdateListeners() {
    if (deltaCollector != null) {
      database.getEvents().unregisterListener((AfterRecordCreateListener) deltaCollector);
      database.getEvents().unregisterListener((AfterRecordUpdateListener) deltaCollector);
      database.getEvents().unregisterListener((AfterRecordDeleteListener) deltaCollector);
      deltaCollector = null;
    }
  }

  /**
   * Applies a transaction delta to the overlay. Called from the post-commit callback.
   * If the overlay grows beyond the compaction threshold, triggers a full rebuild.
   */
  void applyDelta(final TxDelta delta) {
    final DeltaOverlay current = this.overlay;
    final DeltaOverlay base = current != null ? current : new DeltaOverlay(nodeMapping.size());
    final DeltaOverlay merged = base.merge(delta, nodeMapping);
    this.overlay = merged;

    if (Math.abs(merged.getDeltaEdgeCount()) > compactionThreshold) {
      // Trigger full compaction in background
      final Thread compactThread = new Thread(() -> {
        try {
          final CSRBuilder builder = new CSRBuilder(database, propertyFilter);
          final CSRBuilder.CSRResult result = builder.build(vertexTypes, edgeTypes);
          this.csrPerType = result.getCsrPerType();
          this.nodeMapping = result.getMapping();
          this.bucketColumns = result.getBucketColumns();
          this.overlay = null; // clear overlay after successful compaction
          this.buildTimestamp = System.currentTimeMillis();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING, "Failed to compact GraphAnalyticalView '%s'", e, name);
        }
      }, "gav-compact" + (name != null ? "-" + name : ""));
      compactThread.setDaemon(true);
      compactThread.start();
    }
  }

  private boolean isConnectedForType(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String edgeType) {
    final CSRAdjacencyIndex csr = csrPerType.get(edgeType);
    final DeltaOverlay ov = this.overlay;
    final boolean nodeAInBase = nodeA < nodeMapping.size();

    // Check base CSR
    if (csr != null && nodeAInBase) {
      if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
        if (ov != null && ov.isEdgeDeleted(edgeType, nodeA, nodeB)) { /* deleted */ }
        else {
          final int[] neighbors = csr.getForwardNeighbors();
          final int[] offsets = csr.getForwardOffsets();
          if (Arrays.binarySearch(neighbors, offsets[nodeA], offsets[nodeA + 1], nodeB) >= 0)
            return true;
        }
      }
      if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
        if (ov != null && ov.isEdgeDeleted(edgeType, nodeB, nodeA)) { /* deleted */ }
        else {
          final int[] neighbors = csr.getBackwardNeighbors();
          final int[] offsets = csr.getBackwardOffsets();
          if (Arrays.binarySearch(neighbors, offsets[nodeA], offsets[nodeA + 1], nodeB) >= 0)
            return true;
        }
      }
    }

    // Check overlay added edges
    if (ov != null) {
      if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
        for (final int neighbor : ov.getAddedOutNeighbors(nodeA, edgeType))
          if (neighbor == nodeB)
            return true;
      }
      if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
        for (final int neighbor : ov.getAddedInNeighbors(nodeA, edgeType))
          if (neighbor == nodeB)
            return true;
      }
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

  private long countDirectional(final CSRAdjacencyIndex csr, final int nodeId,
      final Vertex.DIRECTION direction, final String edgeType) {
    long count = 0;
    final DeltaOverlay ov = this.overlay;
    final boolean nodeInBase = nodeId < nodeMapping.size();

    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
      if (nodeInBase)
        count += csr.outDegree(nodeId);
      if (ov != null)
        count += ov.getAddedOutNeighbors(nodeId, edgeType).length;
    }
    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
      if (nodeInBase)
        count += csr.inDegree(nodeId);
      if (ov != null)
        count += ov.getAddedInNeighbors(nodeId, edgeType).length;
    }
    return count;
  }

  private int[] getNeighborsFromCSR(final CSRAdjacencyIndex csr, final int nodeId,
      final Vertex.DIRECTION direction, final String edgeType) {
    final DeltaOverlay ov = this.overlay;
    final boolean nodeInBase = nodeId < nodeMapping.size();

    // Collect base CSR neighbors (only if node is in base mapping and CSR exists)
    int[] baseOut = EMPTY_INT;
    int[] baseIn = EMPTY_INT;
    if (nodeInBase && csr != null) {
      if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH)
        baseOut = Arrays.copyOfRange(csr.getForwardNeighbors(), csr.outOffset(nodeId), csr.outOffsetEnd(nodeId));
      if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH)
        baseIn = Arrays.copyOfRange(csr.getBackwardNeighbors(), csr.inOffset(nodeId), csr.inOffsetEnd(nodeId));
    }

    // Collect overlay neighbors
    int[] ovOut = EMPTY_INT;
    int[] ovIn = EMPTY_INT;
    if (ov != null) {
      if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH)
        ovOut = ov.getAddedOutNeighbors(nodeId, edgeType);
      if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH)
        ovIn = ov.getAddedInNeighbors(nodeId, edgeType);
    }

    final int totalLen = baseOut.length + baseIn.length + ovOut.length + ovIn.length;
    if (totalLen == 0)
      return EMPTY_INT;

    final int[] result = new int[totalLen];
    int pos = 0;
    if (baseOut.length > 0) { System.arraycopy(baseOut, 0, result, pos, baseOut.length); pos += baseOut.length; }
    if (baseIn.length > 0) { System.arraycopy(baseIn, 0, result, pos, baseIn.length); pos += baseIn.length; }
    if (ovOut.length > 0) { System.arraycopy(ovOut, 0, result, pos, ovOut.length); pos += ovOut.length; }
    if (ovIn.length > 0) { System.arraycopy(ovIn, 0, result, pos, ovIn.length); pos += ovIn.length; }
    Arrays.sort(result);
    return result;
  }

  private static final int[] EMPTY_INT = new int[0];

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
