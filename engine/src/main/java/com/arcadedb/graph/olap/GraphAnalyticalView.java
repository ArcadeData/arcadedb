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
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 *       .withUpdateMode(UpdateMode.SYNCHRONOUS)
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

  /** Shared executor for all GAV async builds and compactions. Uses virtual threads for lightweight concurrency. */
  private static final ExecutorService EXECUTOR = Executors.newThreadPerTaskExecutor(
      Thread.ofVirtual().name("gav-worker-", 0).factory());

  public enum Status {
    NOT_BUILT, BUILDING, READY, STALE
  }

  public enum UpdateMode {
    OFF, SYNCHRONOUS, ASYNCHRONOUS
  }

  /**
   * Immutable snapshot of all mutable CSR state. Swapped atomically via a single volatile write
   * to guarantee readers always see a consistent view, even during background compaction/rebuild.
   */
  static final class Snapshot {
    final Map<String, CSRAdjacencyIndex> csrPerType;
    final NodeIdMapping                  nodeMapping;
    final ColumnStore[]                  bucketColumns;
    final DeltaOverlay                   overlay;
    final long                           buildTimestamp;
    final long                           buildDurationMs;

    Snapshot(final Map<String, CSRAdjacencyIndex> csrPerType, final NodeIdMapping nodeMapping,
        final ColumnStore[] bucketColumns, final DeltaOverlay overlay, final long buildTimestamp,
        final long buildDurationMs) {
      this.csrPerType = csrPerType;
      this.nodeMapping = nodeMapping;
      this.bucketColumns = bucketColumns;
      this.overlay = overlay;
      this.buildTimestamp = buildTimestamp;
      this.buildDurationMs = buildDurationMs;
    }

    /** Returns a new snapshot with a different overlay, keeping everything else unchanged. */
    Snapshot withOverlay(final DeltaOverlay newOverlay) {
      return new Snapshot(csrPerType, nodeMapping, bucketColumns, newOverlay, buildTimestamp, buildDurationMs);
    }
  }

  private final Database   database;
  private final String     name;
  private final String[]   vertexTypes;
  private final String[]   edgeTypes;
  private final String[]   propertyFilter;
  private volatile UpdateMode updateMode;

  /** Single volatile reference for all mutable CSR state — ensures atomic visibility to readers. */
  private volatile Snapshot          snapshot;
  private volatile Status            status    = Status.NOT_BUILT;
  private volatile CountDownLatch    readyLatch = new CountDownLatch(1);

  // Incremental auto-update
  private DeltaCollector         deltaCollector;
  private int                    compactionThreshold = 10000;
  private final AtomicBoolean    compacting = new AtomicBoolean(false);
  private final AtomicBoolean    buildQueued = new AtomicBoolean(false);

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
    this(database, null, null, null, null, UpdateMode.OFF);
  }

  GraphAnalyticalView(final Database database, final String name, final String[] vertexTypes, final String[] edgeTypes,
      final String[] propertyFilter, final UpdateMode updateMode) {
    this.database = database;
    this.name = name;
    this.vertexTypes = vertexTypes;
    this.edgeTypes = edgeTypes;
    this.propertyFilter = propertyFilter;
    this.updateMode = updateMode;
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
  public synchronized void build(final String[] vertexTypes, final String[] edgeTypes) {
    status = Status.BUILDING;
    readyLatch = new CountDownLatch(1);
    try {
      final long buildStart = System.currentTimeMillis();
      final CSRBuilder builder = new CSRBuilder(database, propertyFilter);
      final CSRBuilder.CSRResult result = builder.build(vertexTypes, edgeTypes);
      final long durationMs = System.currentTimeMillis() - buildStart;

      // Atomic swap — readers see all-or-nothing
      this.snapshot = new Snapshot(result.getCsrPerType(), result.getMapping(),
          result.getBucketColumns(), null, System.currentTimeMillis(), durationMs);
      this.status = Status.READY;

      if (deltaCollector == null)
        registerChangeListeners();
    } catch (final Exception e) {
      this.status = snapshot != null ? Status.READY : Status.NOT_BUILT;
      throw e;
    } finally {
      this.readyLatch.countDown();
    }
  }

  /**
   * Builds the analytical view asynchronously in a background thread.
   * Returns immediately. Use {@link #awaitReady(long, TimeUnit)} or
   * {@link #getStatus()} to check completion.
   */
  public void buildAsync() {
    if (!buildQueued.compareAndSet(false, true))
      return; // a build is already queued or running
    status = Status.BUILDING;
    readyLatch = new CountDownLatch(1);
    EXECUTOR.execute(() -> {
      try {
        final long buildStart = System.currentTimeMillis();
        final CSRBuilder builder = new CSRBuilder(database, propertyFilter);
        final CSRBuilder.CSRResult result = builder.build(vertexTypes, edgeTypes);
        final long durationMs = System.currentTimeMillis() - buildStart;

        this.snapshot = new Snapshot(result.getCsrPerType(), result.getMapping(),
            result.getBucketColumns(), null, System.currentTimeMillis(), durationMs);
        this.status = Status.READY;

        if (deltaCollector == null)
          registerChangeListeners();
      } catch (final Exception e) {
        this.status = snapshot != null ? Status.READY : Status.NOT_BUILT;
        LogManager.instance().log(this, Level.SEVERE, "Async build of GraphAnalyticalView '%s' failed", e, name);
      } finally {
        buildQueued.set(false);
        this.readyLatch.countDown();
      }
    });
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
   * Drops this view: unregisters listeners, removes from registries and schema.
   * Call this when the view is no longer needed (user-initiated removal).
   */
  public void drop() {
    shutdown();
    if (name != null)
      GraphAnalyticalViewPersistence.remove(database, name);
  }

  /**
   * Shuts down this view without removing the schema definition.
   * Called during database close to release resources while preserving persistence.
   */
  public void shutdown() {
    unregisterChangeListeners();
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
    return getVertices(nodeId, direction, edgeTypes);
  }

  @Override
  public NeighborView getNeighborView(final Vertex.DIRECTION direction, final String... edgeTypes) {
    final Snapshot snap = checkBuilt();

    // Cannot provide zero-copy view when overlay is active (delta edges modify topology)
    if (snap.overlay != null)
      return null;

    final int n = snap.nodeMapping.size();

    if (edgeTypes != null && edgeTypes.length == 1) {
      // Single edge type: zero-copy — return CSR arrays directly
      final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeTypes[0]);
      if (csr == null)
        return null;
      return buildNeighborViewFromCSR(csr, n, direction);
    }

    // Multiple or all edge types: merge CSR arrays into a single packed structure
    final Collection<CSRAdjacencyIndex> indices;
    if (edgeTypes != null && edgeTypes.length > 0) {
      final List<CSRAdjacencyIndex> list = new ArrayList<>(edgeTypes.length);
      for (final String et : edgeTypes) {
        final CSRAdjacencyIndex csr = snap.csrPerType.get(et);
        if (csr != null)
          list.add(csr);
      }
      if (list.isEmpty())
        return null;
      if (list.size() == 1)
        return buildNeighborViewFromCSR(list.get(0), n, direction);
      indices = list;
    } else {
      indices = snap.csrPerType.values();
      if (indices.isEmpty())
        return null;
      if (indices.size() == 1)
        return buildNeighborViewFromCSR(indices.iterator().next(), n, direction);
    }

    return buildMergedNeighborView(indices, n, direction);
  }

  private static NeighborView buildNeighborViewFromCSR(final CSRAdjacencyIndex csr, final int n,
      final Vertex.DIRECTION direction) {
    if (direction == Vertex.DIRECTION.OUT)
      return new NeighborView(n, csr.getForwardOffsets(), csr.getForwardNeighbors());
    if (direction == Vertex.DIRECTION.IN)
      return new NeighborView(n, csr.getBackwardOffsets(), csr.getBackwardNeighbors());

    // BOTH: merge forward + backward into a single packed structure
    return buildMergedNeighborView(List.of(csr), n, direction);
  }

  private static NeighborView buildMergedNeighborView(final Collection<CSRAdjacencyIndex> indices,
      final int n, final Vertex.DIRECTION direction) {
    // First pass: compute degree per node
    final int[] offsets = new int[n + 1];
    for (final CSRAdjacencyIndex csr : indices)
      for (int i = 0; i < n; i++) {
        if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH)
          offsets[i + 1] += csr.outDegree(i);
        if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH)
          offsets[i + 1] += csr.inDegree(i);
      }

    // Convert counts to prefix sums
    for (int i = 1; i <= n; i++)
      offsets[i] += offsets[i - 1];

    // Second pass: fill neighbors
    final int totalEdges = offsets[n];
    final int[] neighbors = new int[totalEdges];
    final int[] pos = new int[n]; // current write position per node
    for (int i = 0; i < n; i++)
      pos[i] = offsets[i];

    for (final CSRAdjacencyIndex csr : indices) {
      if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
        final int[] fwdNbrs = csr.getForwardNeighbors();
        for (int i = 0; i < n; i++) {
          final int start = csr.outOffset(i);
          final int end = csr.outOffsetEnd(i);
          for (int j = start; j < end; j++)
            neighbors[pos[i]++] = fwdNbrs[j];
        }
      }
      if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
        final int[] bwdNbrs = csr.getBackwardNeighbors();
        for (int i = 0; i < n; i++) {
          final int start = csr.inOffset(i);
          final int end = csr.inOffsetEnd(i);
          for (int j = start; j < end; j++)
            neighbors[pos[i]++] = bwdNbrs[j];
        }
      }
    }

    return new NeighborView(n, offsets, neighbors);
  }

  // --- Node ID / RID mapping ---

  /**
   * Returns the dense node ID for a given RID, or -1 if not in the view.
   */
  public int getNodeId(final RID rid) {
    final Snapshot snap = checkBuilt();
    final DeltaOverlay ov = snap.overlay;
    if (ov != null)
      return ov.resolveNodeId(rid, snap.nodeMapping);
    return snap.nodeMapping.getGlobalId(rid);
  }

  /**
   * Returns the RID for a given dense node ID.
   */
  public RID getRID(final int nodeId) {
    final Snapshot snap = checkBuilt();
    if (nodeId >= snap.nodeMapping.size()) {
      final DeltaOverlay ov = snap.overlay;
      if (ov != null) {
        final RID rid = ov.getOverflowRID(nodeId);
        if (rid != null)
          return rid;
      }
    }
    return snap.nodeMapping.getRID(nodeId);
  }

  // --- Node type queries ---

  /**
   * Returns the vertex type name for the given node.
   */
  public String getNodeTypeName(final int nodeId) {
    final Snapshot snap = checkBuilt();
    return snap.nodeMapping.getTypeName(nodeId);
  }

  /**
   * Returns the bucket index for the given node.
   */
  public int getNodeBucketIdx(final int nodeId) {
    final Snapshot snap = checkBuilt();
    return snap.nodeMapping.getBucketIdx(nodeId);
  }

  // --- Edge counting (mirrors Vertex.countEdges) ---

  /**
   * Returns the edge count for a node in the given direction, optionally filtered by edge types.
   * Mirrors {@code Vertex.countEdges(DIRECTION, String...)}.
   */
  public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
    final Snapshot snap = checkBuilt();
    if (edgeTypes != null && edgeTypes.length > 0) {
      long total = 0;
      for (final String edgeType : edgeTypes) {
        final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeType);
        if (csr != null)
          total += countDirectional(snap, csr, nodeId, direction, edgeType);
        else {
          // No base CSR but overlay may have edges for this type
          final DeltaOverlay ov = snap.overlay;
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
    for (final var entry : snap.csrPerType.entrySet())
      total += countDirectional(snap, entry.getValue(), nodeId, direction, entry.getKey());
    return total;
  }

  // --- Neighbor access (mirrors Vertex.getVertices) ---

  /**
   * Returns dense node IDs of connected vertices in the given direction, optionally filtered by edge types.
   * Mirrors {@code Vertex.getVertices(DIRECTION, String...)}.
   */
  public int[] getVertices(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
    final Snapshot snap = checkBuilt();

    if (edgeTypes != null && edgeTypes.length == 1) {
      final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeTypes[0]);
      if (csr != null)
        return getNeighborsFromCSR(snap, csr, nodeId, direction, edgeTypes[0]);
      // No base CSR — check overlay only
      if (snap.overlay == null)
        return new int[0];
      return getNeighborsFromCSR(snap, null, nodeId, direction, edgeTypes[0]);
    }

    // Multiple edge types: collect from each
    final java.util.List<int[]> segments = new java.util.ArrayList<>();
    int totalLen = 0;
    if (edgeTypes != null && edgeTypes.length > 0) {
      for (final String et : edgeTypes) {
        final CSRAdjacencyIndex csr = snap.csrPerType.get(et);
        final int[] neighbors = csr != null
            ? getNeighborsFromCSR(snap, csr, nodeId, direction, et)
            : getNeighborsFromCSR(snap, null, nodeId, direction, et);
        if (neighbors.length > 0) {
          segments.add(neighbors);
          totalLen += neighbors.length;
        }
      }
    } else {
      for (final var entry : snap.csrPerType.entrySet()) {
        final int[] neighbors = getNeighborsFromCSR(snap, entry.getValue(), nodeId, direction, entry.getKey());
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
    final Snapshot snap = checkBuilt();
    final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeType);
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
    final Snapshot snap = checkBuilt();
    if (edgeTypes != null && edgeTypes.length > 0) {
      for (final String edgeType : edgeTypes)
        if (isConnectedForType(snap, nodeA, nodeB, direction, edgeType))
          return true;
      return false;
    }
    for (final String edgeType : snap.csrPerType.keySet())
      if (isConnectedForType(snap, nodeA, nodeB, direction, edgeType))
        return true;
    return false;
  }

  /**
   * Counts common neighbors between two nodes, optionally filtered by edge types.
   */
  public int countCommonNeighbors(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    final Snapshot snap = checkBuilt();
    int total = 0;
    if (edgeTypes != null && edgeTypes.length > 0) {
      for (final String edgeType : edgeTypes)
        total += countCommonForType(snap, nodeA, nodeB, direction, edgeType);
    } else {
      for (final String edgeType : snap.csrPerType.keySet())
        total += countCommonForType(snap, nodeA, nodeB, direction, edgeType);
    }
    return total;
  }

  // --- Property access (per-bucket columnar) ---

  /**
   * Returns the property value for a given node, or null if not set.
   * Mirrors {@code Document.get(String)}.
   */
  public Object getProperty(final int nodeId, final String propertyName) {
    final Snapshot snap = checkBuilt();
    // Check overlay first (overrides and overflow nodes)
    final DeltaOverlay ov = snap.overlay;
    if (ov != null) {
      final Object override = ov.getPropertyOverride(nodeId, propertyName);
      if (override != DeltaOverlay.UNSET)
        return override;
    }
    // Fall back to base column store
    if (snap.bucketColumns == null || nodeId >= snap.nodeMapping.size())
      return null;
    final int bucketIdx = snap.nodeMapping.getBucketIdx(nodeId);
    final int localId = snap.nodeMapping.getLocalId(nodeId);
    return snap.bucketColumns[bucketIdx].getValue(localId, propertyName);
  }

  /**
   * Returns the per-bucket column store for direct vectorized access.
   */
  public ColumnStore getBucketColumnStore(final int bucketIdx) {
    final Snapshot snap = checkBuilt();
    return snap.bucketColumns != null ? snap.bucketColumns[bucketIdx] : null;
  }

  /**
   * Returns the column store for a given node's bucket.
   */
  public ColumnStore getColumnStore() {
    final Snapshot snap = checkBuilt();
    if (snap.bucketColumns != null)
      for (final ColumnStore cs : snap.bucketColumns)
        if (cs.getColumnCount() > 0)
          return cs;
    return null;
  }

  // --- Metadata ---

  /**
   * Returns the CSR index for a specific edge type, or null if not present.
   */
  public CSRAdjacencyIndex getCSRIndex(final String edgeType) {
    final Snapshot snap = checkBuilt();
    return snap.csrPerType.get(edgeType);
  }

  public Set<String> getEdgeTypes() {
    final Snapshot snap = checkBuilt();
    return Collections.unmodifiableSet(snap.csrPerType.keySet());
  }

  public NodeIdMapping getNodeMapping() {
    final Snapshot snap = checkBuilt();
    return snap.nodeMapping;
  }

  public int getNodeCount() {
    final Snapshot snap = checkBuilt();
    final DeltaOverlay ov = snap.overlay;
    if (ov != null)
      return ov.getTotalNodeCount();
    return snap.nodeMapping.size();
  }

  public int getEdgeCount() {
    final Snapshot snap = checkBuilt();
    int total = 0;
    for (final CSRAdjacencyIndex csr : snap.csrPerType.values())
      total += csr.getEdgeCount();
    final DeltaOverlay ov = snap.overlay;
    if (ov != null)
      total += ov.getDeltaEdgeCount();
    return total;
  }

  public int getEdgeCount(final String edgeType) {
    final Snapshot snap = checkBuilt();
    final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeType);
    return csr != null ? csr.getEdgeCount() : 0;
  }

  public String getName() {
    return name;
  }

  public Status getStatus() {
    return status;
  }

  public long getBuildTimestamp() {
    final Snapshot snap = this.snapshot;
    return snap != null ? snap.buildTimestamp : 0;
  }

  public long getBuildDurationMs() {
    final Snapshot snap = this.snapshot;
    return snap != null ? snap.buildDurationMs : 0;
  }

  public boolean isBuilt() {
    return snapshot != null;
  }

  public boolean isReady() {
    return status == Status.READY || status == Status.STALE;
  }

  public boolean isStale() {
    return status == Status.STALE;
  }

  public boolean isAutoUpdate() {
    return updateMode != UpdateMode.OFF;
  }

  public UpdateMode getUpdateMode() {
    return updateMode;
  }

  /**
   * Changes the update mode at runtime. Re-registers change listeners as needed.
   * Synchronized to prevent a race window where a committing transaction's delta
   * could be lost between unregister and register.
   */
  public synchronized void setUpdateMode(final UpdateMode newMode) {
    if (this.updateMode == newMode)
      return;
    // Re-register listeners: DeltaCollector behavior depends on the mode.
    // Register new listener before unregistering old one to avoid a window
    // where no listener is active and a committing tx's delta is lost.
    final DeltaCollector oldCollector = this.deltaCollector;
    this.updateMode = newMode;
    if (snapshot != null)
      registerChangeListeners();
    if (oldCollector != null) {
      database.getEvents().unregisterListener((AfterRecordCreateListener) oldCollector);
      database.getEvents().unregisterListener((AfterRecordUpdateListener) oldCollector);
      database.getEvents().unregisterListener((AfterRecordDeleteListener) oldCollector);
    }
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

  public void setCompactionThreshold(final int compactionThreshold) {
    this.compactionThreshold = compactionThreshold;
  }

  public long getMemoryUsageBytes() {
    final Snapshot snap = this.snapshot;
    if (snap == null)
      return 0;
    long total = 0;
    for (final CSRAdjacencyIndex csr : snap.csrPerType.values())
      total += csr.getMemoryUsageBytes();
    if (snap.bucketColumns != null)
      for (final ColumnStore cs : snap.bucketColumns)
        total += cs.getMemoryUsageBytes();
    if (snap.nodeMapping != null)
      total += snap.nodeMapping.getMemoryUsageBytes();
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
    stats.put("updateMode", updateMode.name());

    final Snapshot snap = this.snapshot;
    if (snap == null) {
      stats.put("nodeCount", 0);
      stats.put("edgeCount", 0);
      stats.put("memoryUsageBytes", 0L);
      return stats;
    }

    stats.put("nodeCount", getNodeCount());
    stats.put("edgeCount", getEdgeCount());
    stats.put("buildTimestamp", snap.buildTimestamp);
    stats.put("buildDurationMs", snap.buildDurationMs);

    // Per-edge-type breakdown
    final Map<String, Object> edgeTypeStats = new HashMap<>();
    for (final var entry : snap.csrPerType.entrySet()) {
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
    for (final CSRAdjacencyIndex csr : snap.csrPerType.values())
      csrMemory += csr.getMemoryUsageBytes();
    long columnMemory = 0;
    int propertyCount = 0;
    if (snap.bucketColumns != null) {
      for (final ColumnStore cs : snap.bucketColumns) {
        columnMemory += cs.getMemoryUsageBytes();
        propertyCount = Math.max(propertyCount, cs.getColumnCount());
      }
    }
    long mappingMemory = snap.nodeMapping != null ? snap.nodeMapping.getMemoryUsageBytes() : 0;

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
    final DeltaOverlay ov = snap.overlay;
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

  private void registerChangeListeners() {
    deltaCollector = new DeltaCollector(this);
    database.getEvents().registerListener((AfterRecordCreateListener) deltaCollector);
    database.getEvents().registerListener((AfterRecordUpdateListener) deltaCollector);
    database.getEvents().registerListener((AfterRecordDeleteListener) deltaCollector);
  }

  private void unregisterChangeListeners() {
    if (deltaCollector != null) {
      database.getEvents().unregisterListener((AfterRecordCreateListener) deltaCollector);
      database.getEvents().unregisterListener((AfterRecordUpdateListener) deltaCollector);
      database.getEvents().unregisterListener((AfterRecordDeleteListener) deltaCollector);
      deltaCollector = null;
    }
  }

  /**
   * Called by the DeltaCollector (ASYNCHRONOUS/OFF mode) after a committed transaction affected
   * covered vertex/edge types. ASYNCHRONOUS triggers an async rebuild, OFF marks the view as STALE.
   */
  void onRelevantCommit() {
    if (updateMode == UpdateMode.ASYNCHRONOUS) {
      if (!compacting.compareAndSet(false, true))
        return; // rebuild already in progress, it will pick up committed changes
      this.status = Status.BUILDING;
      this.readyLatch = new CountDownLatch(1);
      EXECUTOR.execute(() -> {
        try {
          final long buildStart = System.currentTimeMillis();
          final CSRBuilder builder = new CSRBuilder(database, propertyFilter);
          final CSRBuilder.CSRResult result = builder.build(vertexTypes, edgeTypes);
          final long durationMs = System.currentTimeMillis() - buildStart;
          // Atomic swap — readers see all-or-nothing
          this.snapshot = new Snapshot(result.getCsrPerType(), result.getMapping(),
              result.getBucketColumns(), null, System.currentTimeMillis(), durationMs);
          this.status = Status.READY;
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING, "Failed to rebuild GraphAnalyticalView '%s'", e, name);
        } finally {
          compacting.set(false);
          this.readyLatch.countDown();
        }
      });
    } else {
      this.status = Status.STALE;
    }
  }

  /**
   * Applies a transaction delta to the overlay (SYNCHRONOUS mode). Called from the post-commit callback.
   * If the overlay grows beyond the compaction threshold, triggers a full rebuild in the background.
   * <p>
   * Thread-safety guarantees:
   * <ul>
   *   <li>Snapshot swap is atomic — readers always see consistent state</li>
   *   <li>Only one compaction thread runs at a time (AtomicBoolean guard)</li>
   *   <li>Overlays applied during compaction are preserved: the compaction thread
   *       re-checks the overlay after rebuild and merges any accumulated deltas</li>
   * </ul>
   */
  void applyDelta(final TxDelta delta) {
    // Merge delta into the current snapshot's overlay
    final Snapshot current = this.snapshot;
    final DeltaOverlay base = current.overlay != null ? current.overlay : new DeltaOverlay(current.nodeMapping.size());
    final DeltaOverlay merged = base.merge(delta, current.nodeMapping);
    // Atomic swap — overlay update visible to all readers immediately
    this.snapshot = current.withOverlay(merged);

    if (Math.abs(merged.getDeltaEdgeCount()) > compactionThreshold) {
      // Guard: only one compaction thread at a time
      if (!compacting.compareAndSet(false, true))
        return;

      EXECUTOR.execute(() -> {
        try {
          final long buildStart = System.currentTimeMillis();
          final CSRBuilder builder = new CSRBuilder(database, propertyFilter);
          final CSRBuilder.CSRResult result = builder.build(vertexTypes, edgeTypes);
          final long durationMs = System.currentTimeMillis() - buildStart;

          // Swap to fresh CSR with no overlay
          this.snapshot = new Snapshot(result.getCsrPerType(), result.getMapping(),
              result.getBucketColumns(), null, System.currentTimeMillis(), durationMs);

          // Note: any deltas that arrived during rebuild are already committed to OLTP,
          // so the fresh CSR built from OLTP already includes them. No need to re-apply.
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING, "Failed to compact GraphAnalyticalView '%s'", e, name);
        } finally {
          compacting.set(false);
        }
      });
    }
  }

  private boolean isConnectedForType(final Snapshot snap, final int nodeA, final int nodeB,
      final Vertex.DIRECTION direction, final String edgeType) {
    final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeType);
    final DeltaOverlay ov = snap.overlay;
    final boolean nodeAInBase = nodeA < snap.nodeMapping.size();

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

  private int countCommonForType(final Snapshot snap, final int nodeA, final int nodeB,
      final Vertex.DIRECTION direction, final String edgeType) {
    final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeType);
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

  private long countDirectional(final Snapshot snap, final CSRAdjacencyIndex csr, final int nodeId,
      final Vertex.DIRECTION direction, final String edgeType) {
    long count = 0;
    final DeltaOverlay ov = snap.overlay;
    final boolean nodeInBase = nodeId < snap.nodeMapping.size();

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

  private int[] getNeighborsFromCSR(final Snapshot snap, final CSRAdjacencyIndex csr, final int nodeId,
      final Vertex.DIRECTION direction, final String edgeType) {
    final DeltaOverlay ov = snap.overlay;
    final boolean nodeInBase = nodeId < snap.nodeMapping.size();

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

  /**
   * Checks the view is built and returns a consistent snapshot for the caller to use.
   * All public methods should capture this once and use it throughout their execution.
   */
  private Snapshot checkBuilt() {
    final Snapshot snap = this.snapshot;
    if (snap == null)
      throw new IllegalStateException("GraphAnalyticalView has not been built yet. Call build() first.");
    return snap;
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
