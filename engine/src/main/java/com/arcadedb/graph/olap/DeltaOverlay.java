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

import com.arcadedb.database.RID;

import java.util.*;

/**
 * Immutable delta overlay on top of a base CSR snapshot. Stores new vertices, deleted vertices,
 * new edges, deleted edges, and property overrides that haven't been compacted into the base yet.
 * <p>
 * Thread-safe for reads (immutable after construction). A new overlay is created for each
 * committed transaction by merging the previous overlay with the new {@link TxDelta}.
 * <p>
 * Query methods in {@link GraphAnalyticalView} merge base CSR results with this overlay.
 */
class DeltaOverlay {
  // New vertices not yet in the base NodeIdMapping.
  // Overflow IDs start at baseNodeCount.
  private final Map<RID, Integer>           overflowNodeIds;
  private final RID[]                       overflowIdToRID;
  private final Map<String, Object>[]       overflowProperties;

  // Deleted base-mapped nodes
  private final BitSet                      deletedBaseNodes;

  // Deleted overflow nodes (indexed by overflowIdx = globalId - baseNodeCount)
  private final BitSet                      deletedOverflowNodes;

  // Added edges per type: edgeType -> list of (srcGlobalId, tgtGlobalId) pairs
  private final Map<String, List<long[]>>   addedEdgesPerType;

  // Secondary indexes for O(1) neighbor lookup: edgeType -> nodeId -> neighbor list
  private final Map<String, Map<Integer, int[]>> outNeighborIndex;
  private final Map<String, Map<Integer, int[]>> inNeighborIndex;

  // Deleted edges per type: edgeType -> set of packed (src << 32 | tgt)
  private final Map<String, Set<Long>>      deletedEdgesPerType;

  // Per-node deleted edge counts for O(1) lookup: edgeType -> nodeId -> count
  private final Map<String, Map<Integer, Integer>> deletedOutEdgeCounts;
  private final Map<String, Map<Integer, Integer>> deletedInEdgeCounts;

  // Property overrides for base nodes: globalId -> (propName -> value)
  private final Map<Integer, Map<String, Object>> propertyOverrides;

  private final int baseNodeCount;
  private final int overflowCount;
  private final int deltaEdgeCount;

  @SuppressWarnings("unchecked")
  DeltaOverlay(final int baseNodeCount) {
    this.baseNodeCount = baseNodeCount;
    this.overflowNodeIds = Collections.emptyMap();
    this.overflowIdToRID = new RID[0];
    this.overflowProperties = new Map[0];
    this.deletedBaseNodes = new BitSet();
    this.deletedOverflowNodes = new BitSet();
    this.addedEdgesPerType = Collections.emptyMap();
    this.deletedEdgesPerType = Collections.emptyMap();
    this.deletedOutEdgeCounts = Collections.emptyMap();
    this.deletedInEdgeCounts = Collections.emptyMap();
    this.propertyOverrides = Collections.emptyMap();
    this.outNeighborIndex = Collections.emptyMap();
    this.inNeighborIndex = Collections.emptyMap();
    this.overflowCount = 0;
    this.deltaEdgeCount = 0;
  }

  @SuppressWarnings("unchecked")
  private DeltaOverlay(final int baseNodeCount,
      final Map<RID, Integer> overflowNodeIds, final RID[] overflowIdToRID,
      final Map<String, Object>[] overflowProperties,
      final BitSet deletedBaseNodes, final BitSet deletedOverflowNodes,
      final Map<String, List<long[]>> addedEdgesPerType,
      final Map<String, Set<Long>> deletedEdgesPerType,
      final Map<String, Map<Integer, Integer>> deletedOutEdgeCounts,
      final Map<String, Map<Integer, Integer>> deletedInEdgeCounts,
      final Map<Integer, Map<String, Object>> propertyOverrides,
      final Map<String, Map<Integer, int[]>> outNeighborIndex,
      final Map<String, Map<Integer, int[]>> inNeighborIndex,
      final int overflowCount, final int deltaEdgeCount) {
    this.baseNodeCount = baseNodeCount;
    this.overflowNodeIds = overflowNodeIds;
    this.overflowIdToRID = overflowIdToRID;
    this.overflowProperties = overflowProperties;
    this.deletedBaseNodes = deletedBaseNodes;
    this.deletedOverflowNodes = deletedOverflowNodes;
    this.addedEdgesPerType = addedEdgesPerType;
    this.deletedEdgesPerType = deletedEdgesPerType;
    this.deletedOutEdgeCounts = deletedOutEdgeCounts;
    this.deletedInEdgeCounts = deletedInEdgeCounts;
    this.propertyOverrides = propertyOverrides;
    this.outNeighborIndex = outNeighborIndex;
    this.inNeighborIndex = inNeighborIndex;
    this.overflowCount = overflowCount;
    this.deltaEdgeCount = deltaEdgeCount;
  }

  /**
   * Creates a new overlay by merging this overlay with a transaction delta.
   * The previous overlay is not modified.
   */
  @SuppressWarnings("unchecked")
  DeltaOverlay merge(final TxDelta delta, final NodeIdMapping baseMapping) {
    // Copy mutable structures from previous overlay
    final Map<RID, Integer> newOverflowIds = new HashMap<>(overflowNodeIds);
    final List<RID> overflowRIDsList = new ArrayList<>(Arrays.asList(overflowIdToRID));
    final List<Map<String, Object>> overflowPropsList = new ArrayList<>(Arrays.asList(overflowProperties));
    final BitSet newDeleted = (BitSet) deletedBaseNodes.clone();
    final BitSet newDeletedOverflow = (BitSet) deletedOverflowNodes.clone();
    final Map<String, List<long[]>> newAddedEdges = new HashMap<>();
    for (final var entry : addedEdgesPerType.entrySet())
      newAddedEdges.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    final Map<String, Set<Long>> newDeletedEdges = new HashMap<>();
    for (final var entry : deletedEdgesPerType.entrySet())
      newDeletedEdges.put(entry.getKey(), new HashSet<>(entry.getValue()));
    final Map<Integer, Map<String, Object>> newPropOverrides = new HashMap<>(propertyOverrides);

    int newOverflowCount = overflowCount;
    int newDeltaEdgeCount = deltaEdgeCount;

    // Process added vertices
    for (final TxDelta.VertexDelta vd : delta.addedVertices) {
      if (baseMapping.getGlobalId(vd.rid) >= 0)
        continue; // already in base
      if (newOverflowIds.containsKey(vd.rid))
        continue; // already in overflow
      final int overflowId = baseNodeCount + newOverflowCount;
      newOverflowIds.put(vd.rid, overflowId);
      overflowRIDsList.add(vd.rid);
      overflowPropsList.add(vd.properties != null ? new HashMap<>(vd.properties) : Collections.emptyMap());
      newOverflowCount++;
    }

    // Process deleted vertices
    for (final RID rid : delta.deletedVertices) {
      final int baseId = baseMapping.getGlobalId(rid);
      if (baseId >= 0)
        newDeleted.set(baseId);
      else {
        final Integer overflowId = newOverflowIds.remove(rid);
        if (overflowId != null)
          newDeletedOverflow.set(overflowId - baseNodeCount);
      }
    }

    // Process added edges
    for (final TxDelta.EdgeDelta ed : delta.addedEdges) {
      final int srcId = resolveNodeId(ed.source, baseMapping, newOverflowIds);
      final int tgtId = resolveNodeId(ed.target, baseMapping, newOverflowIds);
      if (srcId < 0 || tgtId < 0)
        continue; // endpoint not in view
      newAddedEdges.computeIfAbsent(ed.edgeType, k -> new ArrayList<>())
          .add(new long[] { srcId, tgtId });
      newDeltaEdgeCount++;
    }

    // Process deleted edges
    for (final TxDelta.EdgeDelta ed : delta.deletedEdges) {
      final int srcId = resolveNodeId(ed.source, baseMapping, newOverflowIds);
      final int tgtId = resolveNodeId(ed.target, baseMapping, newOverflowIds);
      if (srcId < 0 || tgtId < 0)
        continue;
      newDeletedEdges.computeIfAbsent(ed.edgeType, k -> new HashSet<>())
          .add(packEdge(srcId, tgtId));
      newDeltaEdgeCount--;
    }

    // Process property updates
    for (final var entry : delta.updatedProperties.entrySet()) {
      final int baseId = baseMapping.getGlobalId(entry.getKey());
      if (baseId >= 0)
        newPropOverrides.merge(baseId, entry.getValue(), (old, nw) -> { old.putAll(nw); return old; });
    }

    // Build secondary neighbor indexes for O(1) lookup
    final Map<String, Map<Integer, int[]>> newOutIndex = buildNeighborIndex(newAddedEdges, true);
    final Map<String, Map<Integer, int[]>> newInIndex = buildNeighborIndex(newAddedEdges, false);

    // Build per-node deleted edge count indexes for O(1) lookup
    final Map<String, Map<Integer, Integer>> newDelOutCounts = buildDeletedEdgeCounts(newDeletedEdges, true);
    final Map<String, Map<Integer, Integer>> newDelInCounts = buildDeletedEdgeCounts(newDeletedEdges, false);

    return new DeltaOverlay(baseNodeCount,
        Collections.unmodifiableMap(newOverflowIds),
        overflowRIDsList.toArray(new RID[0]),
        overflowPropsList.toArray(new Map[0]),
        newDeleted, newDeletedOverflow, newAddedEdges, newDeletedEdges,
        newDelOutCounts, newDelInCounts, newPropOverrides,
        newOutIndex, newInIndex,
        newOverflowCount, newDeltaEdgeCount);
  }

  // --- Query helpers ---

  int resolveNodeId(final RID rid, final NodeIdMapping baseMapping) {
    final int baseId = baseMapping.getGlobalId(rid);
    if (baseId >= 0)
      return baseId;
    final Integer overflowId = overflowNodeIds.get(rid);
    return overflowId != null ? overflowId : -1;
  }

  boolean isDeleted(final int globalId) {
    if (globalId < baseNodeCount)
      return deletedBaseNodes.get(globalId);
    final int overflowIdx = globalId - baseNodeCount;
    return overflowIdx < overflowCount && deletedOverflowNodes.get(overflowIdx);
  }

  /**
   * Returns added out-neighbors for a given node and edge type.
   */
  int[] getAddedOutNeighbors(final int nodeId, final String edgeType) {
    return getAddedNeighbors(nodeId, edgeType, true);
  }

  /**
   * Returns added in-neighbors for a given node and edge type.
   */
  int[] getAddedInNeighbors(final int nodeId, final String edgeType) {
    return getAddedNeighbors(nodeId, edgeType, false);
  }

  private int[] getAddedNeighbors(final int nodeId, final String edgeType, final boolean outgoing) {
    final Map<String, Map<Integer, int[]>> index = outgoing ? outNeighborIndex : inNeighborIndex;
    final Map<Integer, int[]> typeIndex = index.get(edgeType);
    if (typeIndex == null)
      return EMPTY_INT;
    final int[] result = typeIndex.get(nodeId);
    return result != null ? result : EMPTY_INT;
  }

  boolean isEdgeDeleted(final String edgeType, final int srcId, final int tgtId) {
    final Set<Long> deleted = deletedEdgesPerType.get(edgeType);
    return deleted != null && deleted.contains(packEdge(srcId, tgtId));
  }

  /**
   * Counts the number of deleted outgoing edges from {@code nodeId} for the given edge type. O(1).
   */
  int countDeletedOutEdges(final int nodeId, final String edgeType) {
    final Map<Integer, Integer> counts = deletedOutEdgeCounts.get(edgeType);
    if (counts == null)
      return 0;
    return counts.getOrDefault(nodeId, 0);
  }

  /**
   * Counts the number of deleted incoming edges to {@code nodeId} for the given edge type. O(1).
   */
  int countDeletedInEdges(final int nodeId, final String edgeType) {
    final Map<Integer, Integer> counts = deletedInEdgeCounts.get(edgeType);
    if (counts == null)
      return 0;
    return counts.getOrDefault(nodeId, 0);
  }

  /**
   * Returns a property value for a node, or null if no override exists.
   * Returns UNSET if the property is not overridden.
   */
  Object getPropertyOverride(final int globalId, final String propertyName) {
    if (globalId >= baseNodeCount) {
      // Overflow node
      final int overflowIdx = globalId - baseNodeCount;
      if (overflowIdx < overflowProperties.length && !deletedOverflowNodes.get(overflowIdx)) {
        final Map<String, Object> props = overflowProperties[overflowIdx];
        if (props != null && props.containsKey(propertyName))
          return props.get(propertyName);
      }
      return UNSET;
    }
    // Base node override
    final Map<String, Object> overrides = propertyOverrides.get(globalId);
    if (overrides != null && overrides.containsKey(propertyName))
      return overrides.get(propertyName);
    return UNSET;
  }

  RID getOverflowRID(final int globalId) {
    final int idx = globalId - baseNodeCount;
    if (idx < 0 || idx >= overflowIdToRID.length || deletedOverflowNodes.get(idx))
      return null;
    return overflowIdToRID[idx];
  }

  int getTotalNodeCount() {
    return baseNodeCount - deletedBaseNodes.cardinality() + overflowCount - deletedOverflowNodes.cardinality();
  }

  int getOverflowCount() {
    return overflowCount;
  }

  int getDeltaEdgeCount() {
    return deltaEdgeCount;
  }

  boolean hasChanges() {
    return overflowCount > 0 || !deletedBaseNodes.isEmpty()
        || !addedEdgesPerType.isEmpty() || !deletedEdgesPerType.isEmpty()
        || !propertyOverrides.isEmpty();
  }

  // --- Internals ---

  private static int resolveNodeId(final RID rid, final NodeIdMapping baseMapping,
      final Map<RID, Integer> overflowIds) {
    final int baseId = baseMapping.getGlobalId(rid);
    if (baseId >= 0)
      return baseId;
    final Integer overflowId = overflowIds.get(rid);
    return overflowId != null ? overflowId : -1;
  }

  /**
   * Builds a secondary index: edgeType -> nodeId -> int[] neighbors, for O(1) lookup.
   * The fill count is stored in {@code arr[0]} of each growable array to avoid a separate
   * autoboxing-heavy {@code Map<Integer, Integer>} for size tracking. Data occupies
   * {@code arr[1..size]}, then is trimmed into a clean {@code int[size]} at the end.
   */
  private static Map<String, Map<Integer, int[]>> buildNeighborIndex(
      final Map<String, List<long[]>> addedEdges, final boolean outgoing) {
    if (addedEdges.isEmpty())
      return Collections.emptyMap();
    final Map<String, Map<Integer, int[]>> result = new HashMap<>();
    for (final var entry : addedEdges.entrySet()) {
      final Map<Integer, int[]> perNode = new HashMap<>();
      for (final long[] pair : entry.getValue()) {
        final int key = (int) (outgoing ? pair[0] : pair[1]);
        final int neighbor = (int) (outgoing ? pair[1] : pair[0]);
        int[] arr = perNode.get(key);
        if (arr == null) {
          arr = new int[5]; // arr[0] = fill count, arr[1..4] = data slots
          perNode.put(key, arr);
        }
        final int size = arr[0];
        if (size + 1 == arr.length) {
          final int[] grown = new int[arr.length * 2];
          System.arraycopy(arr, 0, grown, 0, size + 1);
          arr = grown;
          perNode.put(key, arr);
        }
        arr[size + 1] = neighbor;
        arr[0] = size + 1;
      }
      // Trim: extract arr[1..size] into a clean int[size]
      final Map<Integer, int[]> frozen = new HashMap<>(perNode.size());
      for (final var e : perNode.entrySet()) {
        final int[] arr = e.getValue();
        final int size = arr[0];
        frozen.put(e.getKey(), Arrays.copyOfRange(arr, 1, size + 1));
      }
      result.put(entry.getKey(), frozen);
    }
    return result;
  }

  /**
   * Builds per-node deleted edge count index: edgeType -> nodeId -> count, for O(1) lookup.
   *
   * @param outgoing true for outgoing counts (keyed by source), false for incoming (keyed by target)
   */
  private static Map<String, Map<Integer, Integer>> buildDeletedEdgeCounts(
      final Map<String, Set<Long>> deletedEdges, final boolean outgoing) {
    if (deletedEdges.isEmpty())
      return Collections.emptyMap();
    final Map<String, Map<Integer, Integer>> result = new HashMap<>();
    for (final var entry : deletedEdges.entrySet()) {
      final Map<Integer, Integer> counts = new HashMap<>();
      for (final long packed : entry.getValue()) {
        final int nodeId = outgoing ? (int) (packed >>> 32) : (int) packed;
        counts.merge(nodeId, 1, Integer::sum);
      }
      result.put(entry.getKey(), counts);
    }
    return result;
  }

  private static long packEdge(final int src, final int tgt) {
    return ((long) src << 32) | (tgt & 0xFFFFFFFFL);
  }

  static final Object UNSET = new Object();
  private static final int[] EMPTY_INT = new int[0];
}
