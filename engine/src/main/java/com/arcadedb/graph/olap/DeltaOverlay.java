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

  // Added edges per type: edgeType -> list of (srcGlobalId, tgtGlobalId) pairs
  private final Map<String, List<long[]>>   addedEdgesPerType;

  // Secondary indexes for O(1) neighbor lookup: edgeType -> nodeId -> neighbor list
  private final Map<String, Map<Integer, int[]>> outNeighborIndex;
  private final Map<String, Map<Integer, int[]>> inNeighborIndex;

  // Deleted edges per type: edgeType -> set of packed (src << 32 | tgt)
  private final Map<String, Set<Long>>      deletedEdgesPerType;

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
    this.addedEdgesPerType = Collections.emptyMap();
    this.deletedEdgesPerType = Collections.emptyMap();
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
      final BitSet deletedBaseNodes,
      final Map<String, List<long[]>> addedEdgesPerType,
      final Map<String, Set<Long>> deletedEdgesPerType,
      final Map<Integer, Map<String, Object>> propertyOverrides,
      final Map<String, Map<Integer, int[]>> outNeighborIndex,
      final Map<String, Map<Integer, int[]>> inNeighborIndex,
      final int overflowCount, final int deltaEdgeCount) {
    this.baseNodeCount = baseNodeCount;
    this.overflowNodeIds = overflowNodeIds;
    this.overflowIdToRID = overflowIdToRID;
    this.overflowProperties = overflowProperties;
    this.deletedBaseNodes = deletedBaseNodes;
    this.addedEdgesPerType = addedEdgesPerType;
    this.deletedEdgesPerType = deletedEdgesPerType;
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
      else
        newOverflowIds.remove(rid); // remove from overflow if present
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

    return new DeltaOverlay(baseNodeCount,
        Collections.unmodifiableMap(newOverflowIds),
        overflowRIDsList.toArray(new RID[0]),
        overflowPropsList.toArray(new Map[0]),
        newDeleted, newAddedEdges, newDeletedEdges, newPropOverrides,
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
    return globalId < baseNodeCount && deletedBaseNodes.get(globalId);
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
   * Counts the number of deleted outgoing edges from {@code nodeId} for the given edge type.
   */
  int countDeletedOutEdges(final int nodeId, final String edgeType) {
    final Set<Long> deleted = deletedEdgesPerType.get(edgeType);
    if (deleted == null || deleted.isEmpty())
      return 0;
    int count = 0;
    final long prefix = (long) nodeId << 32;
    for (final long packed : deleted)
      if ((packed & 0xFFFFFFFF00000000L) == prefix)
        count++;
    return count;
  }

  /**
   * Counts the number of deleted incoming edges to {@code nodeId} for the given edge type.
   */
  int countDeletedInEdges(final int nodeId, final String edgeType) {
    final Set<Long> deleted = deletedEdgesPerType.get(edgeType);
    if (deleted == null || deleted.isEmpty())
      return 0;
    int count = 0;
    final int masked = nodeId;
    for (final long packed : deleted)
      if ((int) packed == masked)
        count++;
    return count;
  }

  /**
   * Returns a property value for a node, or null if no override exists.
   * Returns UNSET if the property is not overridden.
   */
  Object getPropertyOverride(final int globalId, final String propertyName) {
    if (globalId >= baseNodeCount) {
      // Overflow node
      final int overflowIdx = globalId - baseNodeCount;
      if (overflowIdx < overflowProperties.length) {
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
    return idx >= 0 && idx < overflowIdToRID.length ? overflowIdToRID[idx] : null;
  }

  int getTotalNodeCount() {
    return baseNodeCount - deletedBaseNodes.cardinality() + overflowCount;
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
   * Uses primitive int arrays throughout to avoid Integer autoboxing.
   */
  private static Map<String, Map<Integer, int[]>> buildNeighborIndex(
      final Map<String, List<long[]>> addedEdges, final boolean outgoing) {
    if (addedEdges.isEmpty())
      return Collections.emptyMap();
    final Map<String, Map<Integer, int[]>> result = new HashMap<>();
    for (final var entry : addedEdges.entrySet()) {
      final Map<Integer, int[]> perNode = new HashMap<>();
      final Map<Integer, Integer> sizes = new HashMap<>();
      for (final long[] pair : entry.getValue()) {
        final int key = (int) (outgoing ? pair[0] : pair[1]);
        final int neighbor = (int) (outgoing ? pair[1] : pair[0]);
        final int size = sizes.getOrDefault(key, 0);
        int[] arr = perNode.get(key);
        if (arr == null) {
          arr = new int[4];
          perNode.put(key, arr);
        } else if (size == arr.length) {
          final int[] grown = new int[arr.length * 2];
          System.arraycopy(arr, 0, grown, 0, size);
          arr = grown;
          perNode.put(key, arr);
        }
        arr[size] = neighbor;
        sizes.put(key, size + 1);
      }
      // Trim to exact size
      final Map<Integer, int[]> frozen = new HashMap<>(perNode.size());
      for (final var e : perNode.entrySet()) {
        final int size = sizes.get(e.getKey());
        final int[] arr = e.getValue();
        frozen.put(e.getKey(), size == arr.length ? arr : Arrays.copyOf(arr, size));
      }
      result.put(entry.getKey(), frozen);
    }
    return result;
  }

  private static long packEdge(final int src, final int tgt) {
    return ((long) src << 32) | (tgt & 0xFFFFFFFFL);
  }

  static final Object UNSET = new Object();
  private static final int[] EMPTY_INT = new int[0];
}
