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
import com.arcadedb.utility.IntIntHashMap;

import java.util.*;

/**
 * Immutable delta overlay on top of a base CSR snapshot. Stores new vertices, deleted vertices,
 * new edges, deleted edges, and property overrides that haven't been compacted into the base yet.
 * <p>
 * Thread-safe for reads (immutable after construction). A new overlay is created for each
 * committed transaction by merging the previous overlay with the new {@link TxDelta}.
 * The {@code merge()} method is called from {@code GraphAnalyticalView.applyDelta()}, which
 * synchronizes on the view instance to serialize concurrent commits. The resulting overlay is
 * then published via a volatile write, so readers never see a partially constructed instance.
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

  // Added edges per type: edgeType -> the added edge's own identity -> its endpoints and materialised
  // property values.
  // Keyed by RID rather than held in a plain list so that an edge added and later deleted within the SAME
  // not-yet-compacted overlay window can be withdrawn from here by identity, instead of being masked with a
  // pair-keyed deletion it has no right to spend (issue #6775 - see merge()). Insertion-ordered, because
  // the neighbour index below is built from it and parallel edges must keep their addition order.
  private final Map<String, Map<RID, AddedEdge>> addedEdgesPerType;

  // Secondary indexes for O(1) neighbor lookup: edgeType -> nodeId -> neighbor list (with the edges' own
  // property values beside it, when the view materialises any)
  private final Map<String, Map<Integer, AddedNeighbors>> outNeighborIndex;
  private final Map<String, Map<Integer, AddedNeighbors>> inNeighborIndex;

  // Deleted edges per type: edgeType -> packed (src << 32 | tgt) -> number of distinct edges deleted for
  // that pair. A count rather than a presence flag: two parallel edges between the same pair, only one of
  // them deleted, must leave the other one discoverable (see #copyBaseExcludingDeleted and issue #6769).
  // Holds ONLY deletions of edges the overlay never added itself, so the count is exactly the exclusion
  // budget to spend against the BASE CSR's run for the pair - never against the overlay's own additions.
  private final Map<String, Map<Long, Integer>> deletedEdgesPerType;

  // The deleted edges' own identities, per type. Exists purely to tell a genuinely new deletion apart from
  // the same edge being reported deleted twice (replayed across merges, or emitted twice within one
  // TxDelta) when deriving deletedEdgesPerType above - the pair alone cannot make that distinction once a
  // pair carries more than one parallel edge.
  private final Map<String, Set<RID>>       deletedEdgeRIDsPerType;

  // Per-node deleted edge counts for O(1) lookup: edgeType -> nodeId -> count
  private final Map<String, IntIntHashMap> deletedOutEdgeCounts;
  private final Map<String, IntIntHashMap> deletedInEdgeCounts;

  // Property overrides for base nodes: globalId -> (propName -> value)
  private final Map<Integer, Map<String, Object>> propertyOverrides;

  private final int baseNodeCount;
  private final int overflowCount;
  private final int deltaEdgeCount;

  // The edge types whose property columns a committed transaction has left out of date. Such a change has no
  // overlay representation - an edge already in the base CSR is addressed by its column slot, and nothing maps
  // that slot back from the edge's RID - so those columns are stale until the rebuild
  // GraphAnalyticalView.applyDelta() forces for them lands. Until then the view answers "no edge properties"
  // for those types rather than a stale weight: the added edges below carry their own values and could be
  // served exactly, but a base edge whose weight was just updated could not, and there is no honest way to
  // serve one without the other (issues #4513 and #6315).
  //
  // Per type rather than one flag for the overlay, so that an update to one materialised edge type does not
  // cost the columnar path for every other type the view holds.
  private final Set<String> dirtyEdgeTypes;

  // True when the columns are out of date for types unknown - the bulk case, where DeltaCollector gave up on
  // naming the edges individually. Every type is then out of date.
  private final boolean     allEdgeTypesDirty;

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
    this.deletedEdgeRIDsPerType = Collections.emptyMap();
    this.deletedOutEdgeCounts = Collections.emptyMap();
    this.deletedInEdgeCounts = Collections.emptyMap();
    this.propertyOverrides = Collections.emptyMap();
    this.outNeighborIndex = Collections.emptyMap();
    this.inNeighborIndex = Collections.emptyMap();
    this.overflowCount = 0;
    this.deltaEdgeCount = 0;
    this.dirtyEdgeTypes = Collections.emptySet();
    this.allEdgeTypesDirty = false;
  }

  // The private constructor takes ownership of all passed collections — callers MUST NOT
  // retain or mutate them after construction. The merge() method satisfies this contract
  // by building fresh collections that are not referenced elsewhere.
  @SuppressWarnings("unchecked")
  private DeltaOverlay(final int baseNodeCount,
      final Map<RID, Integer> overflowNodeIds, final RID[] overflowIdToRID,
      final Map<String, Object>[] overflowProperties,
      final BitSet deletedBaseNodes, final BitSet deletedOverflowNodes,
      final Map<String, Map<RID, AddedEdge>> addedEdgesPerType,
      final Map<String, Map<Long, Integer>> deletedEdgesPerType,
      final Map<String, Set<RID>> deletedEdgeRIDsPerType,
      final Map<String, IntIntHashMap> deletedOutEdgeCounts,
      final Map<String, IntIntHashMap> deletedInEdgeCounts,
      final Map<Integer, Map<String, Object>> propertyOverrides,
      final Map<String, Map<Integer, AddedNeighbors>> outNeighborIndex,
      final Map<String, Map<Integer, AddedNeighbors>> inNeighborIndex,
      final int overflowCount, final int deltaEdgeCount, final Set<String> dirtyEdgeTypes,
      final boolean allEdgeTypesDirty) {
    this.baseNodeCount = baseNodeCount;
    this.overflowNodeIds = overflowNodeIds;
    this.overflowIdToRID = overflowIdToRID;
    this.overflowProperties = overflowProperties;
    this.deletedBaseNodes = deletedBaseNodes;
    this.deletedOverflowNodes = deletedOverflowNodes;
    this.addedEdgesPerType = addedEdgesPerType;
    this.deletedEdgesPerType = deletedEdgesPerType;
    this.deletedEdgeRIDsPerType = deletedEdgeRIDsPerType;
    this.deletedOutEdgeCounts = deletedOutEdgeCounts;
    this.deletedInEdgeCounts = deletedInEdgeCounts;
    this.propertyOverrides = propertyOverrides;
    this.outNeighborIndex = outNeighborIndex;
    this.inNeighborIndex = inNeighborIndex;
    this.overflowCount = overflowCount;
    this.deltaEdgeCount = deltaEdgeCount;
    this.dirtyEdgeTypes = dirtyEdgeTypes;
    this.allEdgeTypesDirty = allEdgeTypesDirty;
  }

  /**
   * Creates a new overlay by merging this overlay with a transaction delta.
   * The previous overlay is not modified.
   */
  DeltaOverlay merge(final TxDelta delta, final NodeIdMapping baseMapping) {
    return merge(delta, baseMapping, null);
  }

  /**
   * Creates a new overlay by merging this overlay with a transaction delta.
   * The previous overlay is not modified.
   * <p>
   * When {@code baseCsrPerType} is non-null (post-compaction delta re-application, see issue #4588),
   * an added edge whose both endpoints are base nodes and that the freshly built base CSR already
   * contains is skipped, unless the same edge is masked as deleted (either by an earlier buffered
   * delta or within this delta). The compaction scan is read-committed and non-atomic, so a buffered
   * delta may already be reflected in the new base CSR; re-adding it to the overlay would surface a
   * duplicate neighbour and inflate the delta edge counter.
   */
  @SuppressWarnings("unchecked")
  DeltaOverlay merge(final TxDelta delta, final NodeIdMapping baseMapping,
      final Map<String, CSRAdjacencyIndex> baseCsrPerType) {
    // Copy mutable structures from previous overlay
    final Map<RID, Integer> newOverflowIds = new HashMap<>(overflowNodeIds);
    final List<RID> overflowRIDsList = new ArrayList<>(Arrays.asList(overflowIdToRID));
    final List<Map<String, Object>> overflowPropsList = new ArrayList<>(Arrays.asList(overflowProperties));
    final BitSet newDeleted = (BitSet) deletedBaseNodes.clone();
    final BitSet newDeletedOverflow = (BitSet) deletedOverflowNodes.clone();
    final Map<String, Map<RID, AddedEdge>> newAddedEdges = new HashMap<>();
    for (final var entry : addedEdgesPerType.entrySet())
      newAddedEdges.put(entry.getKey(), new LinkedHashMap<>(entry.getValue()));
    final Map<String, Map<Long, Integer>> newDeletedEdges = new HashMap<>();
    for (final var entry : deletedEdgesPerType.entrySet())
      newDeletedEdges.put(entry.getKey(), new HashMap<>(entry.getValue()));
    final Map<String, Set<RID>> newDeletedEdgeRIDs = new HashMap<>();
    for (final var entry : deletedEdgeRIDsPerType.entrySet())
      newDeletedEdgeRIDs.put(entry.getKey(), new HashSet<>(entry.getValue()));
    final Map<Integer, Map<String, Object>> newPropOverrides = new HashMap<>(propertyOverrides.size());
    for (final var propEntry : propertyOverrides.entrySet())
      newPropOverrides.put(propEntry.getKey(), new HashMap<>(propEntry.getValue()));

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

    // Pre-resolve edges this same delta deletes, so a delete + re-add of the same edge within one
    // transaction is not mistakenly skipped by the base-CSR dedup below (#4588). Only needed on the
    // re-application path (baseCsrPerType != null).
    Set<Long> sameDeltaDeleted = null;
    if (baseCsrPerType != null && !delta.deletedEdges.isEmpty()) {
      sameDeltaDeleted = new HashSet<>();
      for (final TxDelta.EdgeDelta ed : delta.deletedEdges) {
        final int s = resolveNodeId(ed.source, baseMapping, newOverflowIds);
        final int t = resolveNodeId(ed.target, baseMapping, newOverflowIds);
        if (s >= 0 && t >= 0)
          sameDeltaDeleted.add(packEdge(s, t));
      }
    }

    // Process added edges
    for (final TxDelta.EdgeDelta ed : delta.addedEdges) {
      final int srcId = resolveNodeId(ed.source, baseMapping, newOverflowIds);
      final int tgtId = resolveNodeId(ed.target, baseMapping, newOverflowIds);
      if (srcId < 0 || tgtId < 0)
        continue; // endpoint not in view
      // Post-compaction re-application: if the freshly built base CSR already contains this edge (the
      // read-committed scan crossed its bucket after it committed), re-adding it would duplicate the
      // neighbour and inflate the counter. Skip it unless it is masked as deleted, in which case the
      // overlay must keep the explicit add to reinstate it. See issue #4588.
      if (baseCsrPerType != null && srcId < baseNodeCount && tgtId < baseNodeCount) {
        final CSRAdjacencyIndex csr = baseCsrPerType.get(ed.edgeType);
        if (csr != null && csr.hasForwardEdge(srcId, tgtId)) {
          final long packed = packEdge(srcId, tgtId);
          final Map<Long, Integer> prevDel = newDeletedEdges.get(ed.edgeType);
          final boolean masked = (prevDel != null && prevDel.containsKey(packed))
              || (sameDeltaDeleted != null && sameDeltaDeleted.contains(packed));
          if (!masked) {
            // The fresh base CSR already represents this edge, so it is not tracked by identity here
            // (issue #4588) - which means a LATER deletion of this exact RID cannot be withdrawn by
            // identity either. Drop any stale reservation of this RID from the identity-dedup Set: RIDs
            // are reused for a later, unrelated insert once their original record is deleted ("hole
            // reuse", #5279), so an earlier deletion recorded under this same RID belongs to whatever
            // edge previously occupied the slot, not to this one. Leaving it in place would make this
            // edge's own eventual deletion look like a replay of that unrelated one and get silently
            // absorbed by Set.add() returning false (issue #6777). The earlier deletion's own exclusion
            // budget (deletedEdgesPerType, keyed by pair, not RID) is untouched by this.
            final Set<RID> ridsForType = newDeletedEdgeRIDs.get(ed.edgeType);
            if (ridsForType != null)
              ridsForType.remove(ed.rid);
            continue; // already represented by the fresh base CSR
          }
        }
      }
      // Keyed by the edge's own identity, so a replayed add of an edge the overlay already holds is
      // absorbed instead of appending a second, phantom, occurrence of the same edge.
      if (newAddedEdges.computeIfAbsent(ed.edgeType, k -> new LinkedHashMap<>())
          .put(ed.rid, new AddedEdge(srcId, tgtId, ed.properties)) == null)
        newDeltaEdgeCount++;
    }

    // Process edge property updates. An edge this overlay window holds as an addition takes the new values
    // straight into its own entry - it has no column slot to be out of step with, and the ordinary
    // `newEdge(...).save()` reports exactly this, one create and one update of the same edge. Anything else is
    // an edge the base CSR holds, whose column slot cannot be found from its RID, so the columns are now out of
    // date and only the rebuild GraphAnalyticalView.applyDelta() forces can repair them (issues #4513, #6315).
    // Runs after the additions above so that an edge added and updated within the same transaction is found.
    final boolean newAllDirty = allEdgeTypesDirty || delta.forceEdgePropertyRebuild;
    // Copied on the first type this delta actually dirties and not before, so an overlay whose updates all
    // resolved against its own additions - the ordinary insert - keeps sharing the previous set.
    Set<String> newDirtyTypes = dirtyEdgeTypes;
    boolean dirtyTypesCopied = false;
    for (final TxDelta.EdgeDelta ed : delta.updatedEdges.values()) {
      final Map<RID, AddedEdge> addedForType = newAddedEdges.get(ed.edgeType);
      final AddedEdge added = addedForType != null ? addedForType.get(ed.rid) : null;
      if (added != null)
        addedForType.put(ed.rid, new AddedEdge(added.src(), added.tgt(), ed.properties));
      else if (!newDirtyTypes.contains(ed.edgeType)) {
        if (!dirtyTypesCopied) {
          newDirtyTypes = new HashSet<>(dirtyEdgeTypes);
          dirtyTypesCopied = true;
        }
        newDirtyTypes.add(ed.edgeType);
      }
    }

    // Process deleted edges
    for (final TxDelta.EdgeDelta ed : delta.deletedEdges) {
      final int srcId = resolveNodeId(ed.source, baseMapping, newOverflowIds);
      final int tgtId = resolveNodeId(ed.target, baseMapping, newOverflowIds);
      if (srcId < 0 || tgtId < 0)
        continue;
      // The edge was added by THIS overlay window and is now gone: the add and the delete are one no-op, so
      // withdraw the add instead of recording a deletion (issue #6775). The distinction matters because the
      // deleted counts below are a budget spent against the BASE CSR's run for the pair: recording one here
      // would mask a base edge nobody deleted, while leaving the phantom add in place - which is exactly how
      // the append-only added index used to surface an added-then-deleted edge as a live neighbour. Done by
      // identity, before the dedup guard below, so a RID recycled onto a new edge (see #6777) still cancels
      // its own add rather than having the whole deletion dropped.
      final Map<RID, AddedEdge> addedForType = newAddedEdges.get(ed.edgeType);
      if (addedForType != null && addedForType.remove(ed.rid) != null) {
        newDeltaEdgeCount--; // undo the +1 the withdrawn add contributed
        if (addedForType.isEmpty())
          newAddedEdges.remove(ed.edgeType); // keep hasChanges() honest: no adds left for this type
        // Remembered anyway, so a replay of this same deletion cannot fall through to the branch below and
        // spend a budget against the base CSR now that the add it belongs to is gone.
        newDeletedEdgeRIDs.computeIfAbsent(ed.edgeType, k -> new HashSet<>()).add(ed.rid);
        continue;
      }
      // Only count when the deletion is new: newDeletedEdgeRIDs is keyed by the edge's own identity, so a
      // duplicate deletion (replayed across merges or emitted twice within one TxDelta) is absorbed by
      // add() returning false. Counting it again would drift newDeltaEdgeCount negative and corrupt the
      // compaction trigger (Math.abs(deltaEdgeCount) > threshold, issue #4587) - and, since the per-pair
      // count below feeds copyBaseExcludingDeleted's exclusion budget, it would also mask a second, still
      // live, parallel edge that was never actually deleted (issue #6769).
      // This relies on a reused RID ("hole reuse", #5279) always being visible to THIS overlay window by
      // identity before its own deletion is processed - which holds for every edge added through the
      // normal (non-compaction) merge path, since an add always lands in newAddedEdges above and is caught
      // by the withdraw branch first. The one path where an add is deliberately NOT tracked by identity is
      // the post-compaction re-application skip a few lines up (issue #4588): there, a reused RID's stale
      // entry in this set is explicitly cleared at the point the add is skipped, so it cannot be mistaken
      // for a replay of the unrelated edge that originally freed the slot (issue #6777).
      if (newDeletedEdgeRIDs.computeIfAbsent(ed.edgeType, k -> new HashSet<>()).add(ed.rid)) {
        newDeletedEdges.computeIfAbsent(ed.edgeType, k -> new HashMap<>())
            .merge(packEdge(srcId, tgtId), 1, Integer::sum);
        newDeltaEdgeCount--;
      }
    }

    // Process property updates
    for (final var entry : delta.updatedProperties.entrySet()) {
      final int baseId = baseMapping.getGlobalId(entry.getKey());
      if (baseId >= 0)
        newPropOverrides.merge(baseId, entry.getValue(), (old, nw) -> { old.putAll(nw); return old; });
    }

    // Build secondary neighbor indexes for O(1) lookup
    final Map<String, Map<Integer, AddedNeighbors>> newOutIndex = buildNeighborIndex(newAddedEdges, true);
    final Map<String, Map<Integer, AddedNeighbors>> newInIndex = buildNeighborIndex(newAddedEdges, false);

    // Build per-node deleted edge count indexes for O(1) lookup
    final Map<String, IntIntHashMap> newDelOutCounts = buildDeletedEdgeCounts(newDeletedEdges, true);
    final Map<String, IntIntHashMap> newDelInCounts = buildDeletedEdgeCounts(newDeletedEdges, false);

    return new DeltaOverlay(baseNodeCount,
        Collections.unmodifiableMap(newOverflowIds),
        overflowRIDsList.toArray(new RID[0]),
        overflowPropsList.toArray(new Map[0]),
        newDeleted, newDeletedOverflow, newAddedEdges, newDeletedEdges, newDeletedEdgeRIDs,
        newDelOutCounts, newDelInCounts, newPropOverrides,
        newOutIndex, newInIndex,
        newOverflowCount, newDeltaEdgeCount, newDirtyTypes, newAllDirty);
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
    final AddedNeighbors added = getAdded(nodeId, edgeType, outgoing);
    return added != null ? added.nodeIds() : EMPTY_INT;
  }

  /**
   * Returns one node's added edges of a type and direction - the neighbours and, beside them, each edge's own
   * property values - or {@code null} when it has none.
   * <p>
   * The two together rather than one accessor each, because a caller that needs both (
   * {@link GraphAnalyticalView#edgeWeightsForSlice} does) would otherwise walk the same index twice for one
   * slice.
   *
   * @param outgoing true for the added edges leaving {@code nodeId}, false for those reaching it
   */
  AddedNeighbors getAdded(final int nodeId, final String edgeType, final boolean outgoing) {
    final Map<String, Map<Integer, AddedNeighbors>> index = outgoing ? outNeighborIndex : inNeighborIndex;
    final Map<Integer, AddedNeighbors> typeIndex = index.get(edgeType);
    return typeIndex == null ? null : typeIndex.get(nodeId);
  }

  /**
   * True when a committed transaction left {@code edgeType}'s property columns holding a value the database no
   * longer has. See the field's own comment.
   */
  boolean isEdgePropertiesDirty(final String edgeType) {
    return allEdgeTypesDirty || dirtyEdgeTypes.contains(edgeType);
  }

  /** True when any edge type's property columns are out of date. */
  boolean isEdgePropertiesDirty() {
    return allEdgeTypesDirty || !dirtyEdgeTypes.isEmpty();
  }

  boolean isEdgeDeleted(final String edgeType, final int srcId, final int tgtId) {
    return countDeletedEdges(edgeType, srcId, tgtId) > 0;
  }

  /**
   * Returns how many distinct edges of {@code edgeType} between {@code srcId} and {@code tgtId} the
   * overlay has recorded as deleted - the exclusion budget {@link GraphAnalyticalView#copyBaseExcludingDeleted}
   * spends against the base CSR's parallel-edge run for that pair, so that deleting one of several
   * parallel edges leaves the others discoverable instead of masking the whole pair (issue #6769).
   */
  int countDeletedEdges(final String edgeType, final int srcId, final int tgtId) {
    final Map<Long, Integer> deleted = deletedEdgesPerType.get(edgeType);
    return deleted == null ? 0 : deleted.getOrDefault(packEdge(srcId, tgtId), 0);
  }

  /**
   * Counts the number of deleted outgoing edges from {@code nodeId} for the given edge type. O(1).
   */
  int countDeletedOutEdges(final int nodeId, final String edgeType) {
    final IntIntHashMap counts = deletedOutEdgeCounts.get(edgeType);
    if (counts == null)
      return 0;
    return counts.get(nodeId, 0);
  }

  /**
   * Counts the number of deleted incoming edges to {@code nodeId} for the given edge type. O(1).
   */
  int countDeletedInEdges(final int nodeId, final String edgeType) {
    final IntIntHashMap counts = deletedInEdgeCounts.get(edgeType);
    if (counts == null)
      return 0;
    return counts.get(nodeId, 0);
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

  /**
   * Returns the number of live overflow vertices, i.e. the allocated overflow slots minus the
   * ones that have been deleted. The internal {@code overflowCount} field is a monotonic
   * slot-allocation counter (it drives overflow id assignment and the {@link #isDeleted} bounds
   * check), so deleted slots are never reclaimed; subtracting the deleted cardinality - exactly
   * as {@link #getTotalNodeCount()} does - keeps this count consistent. See issue #4720.
   */
  int getOverflowCount() {
    return overflowCount - deletedOverflowNodes.cardinality();
  }

  int getDeltaEdgeCount() {
    return deltaEdgeCount;
  }

  boolean hasChanges() {
    return overflowCount > 0 || !deletedBaseNodes.isEmpty()
        || !addedEdgesPerType.isEmpty() || !deletedEdgesPerType.isEmpty()
        || !propertyOverrides.isEmpty() || isEdgePropertiesDirty();
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
   * Builds a secondary index: edgeType -> nodeId -> its added neighbours, for O(1) lookup.
   * Uses a two-pass approach: first counts the degree per node to allocate exact-size arrays,
   * then fills them in a second pass. This avoids repeated doubling and trimming allocations
   * that would occur with a growable-array approach for high-degree nodes.
   * <p>
   * The edges' own property values are filled in the same pass as the neighbour ids and into an array of the
   * same length, so an added edge's weight is found at the index its neighbour is found at - the pairing
   * {@link GraphAnalyticalView#edgeWeightsForSlice} hands on to its caller, and the reason it can serve an
   * overlay-added edge exactly rather than at a default weight (issue #6315). The property array is left
   * {@code null} for a type no added edge of which carries any value, which is every type of a view that
   * materialises no edge property columns at all - the common case, and it costs nothing there.
   */
  private static Map<String, Map<Integer, AddedNeighbors>> buildNeighborIndex(
      final Map<String, Map<RID, AddedEdge>> addedEdges, final boolean outgoing) {
    if (addedEdges.isEmpty())
      return Collections.emptyMap();
    final Map<String, Map<Integer, AddedNeighbors>> result = new HashMap<>();
    for (final var entry : addedEdges.entrySet()) {
      final Collection<AddedEdge> edges = entry.getValue().values();

      // Pass 1: count neighbors per node
      final IntIntHashMap counts = new IntIntHashMap();
      boolean anyProperties = false;
      for (final AddedEdge edge : edges) {
        counts.increment(outgoing ? edge.src() : edge.tgt());
        if (edge.properties() != null)
          anyProperties = true;
      }

      // Allocate exact-size arrays
      final boolean withProperties = anyProperties;
      final Map<Integer, AddedNeighbors> perNode = new HashMap<>(counts.size());
      counts.forEach((key, count) -> perNode.put(key,
          new AddedNeighbors(new int[count], withProperties ? new Object[count][] : null)));

      // Pass 2: fill arrays (use a fresh map as fill-position tracker)
      final IntIntHashMap positions = new IntIntHashMap(counts.size());
      for (final AddedEdge edge : edges) {
        final int key = outgoing ? edge.src() : edge.tgt();
        final int neighbor = outgoing ? edge.tgt() : edge.src();
        final AddedNeighbors added = perNode.get(key);
        final int pos = positions.get(key, 0);
        added.nodeIds()[pos] = neighbor;
        if (added.properties() != null)
          added.properties()[pos] = edge.properties();
        positions.put(key, pos + 1);
      }

      result.put(entry.getKey(), perNode);
    }
    return result;
  }

  /**
   * Builds per-node deleted edge count index: edgeType -> nodeId -> count, for O(1) lookup.
   *
   * @param outgoing true for outgoing counts (keyed by source), false for incoming (keyed by target)
   */
  private static Map<String, IntIntHashMap> buildDeletedEdgeCounts(
      final Map<String, Map<Long, Integer>> deletedEdges, final boolean outgoing) {
    if (deletedEdges.isEmpty())
      return Collections.emptyMap();
    final Map<String, IntIntHashMap> result = new HashMap<>();
    for (final var entry : deletedEdges.entrySet()) {
      final IntIntHashMap counts = new IntIntHashMap();
      for (final var pairEntry : entry.getValue().entrySet()) {
        final long packed = pairEntry.getKey();
        final int nodeId = outgoing ? (int) (packed >>> 32) : (int) packed;
        counts.add(nodeId, pairEntry.getValue());
      }
      result.put(entry.getKey(), counts);
    }
    return result;
  }

  // Mask to zero-extend a signed int to 64 bits, preventing sign-extension from corrupting the upper 32 bits
  private static final long UNSIGNED_INT_MASK = 0xFFFFFFFFL;

  private static long packEdge(final int src, final int tgt) {
    return ((long) src << 32) | (tgt & UNSIGNED_INT_MASK);
  }

  /**
   * One edge the overlay holds that the base CSR does not: its endpoints as dense ids, and the values of the
   * edge properties the view materialises columns for, captured at commit time by {@link DeltaCollector}.
   * <p>
   * The values are carried rather than re-read from the record later because the overlay outlives the
   * transaction that produced it, and by the time an algorithm asks for the weight the edge may be gone. They
   * are also what makes an overlay-added edge answerable at all: it has no column slot to be addressed by.
   *
   * @param properties the materialised edge properties' values, one slot per name of the view's edge property
   *                   filter in its order, or {@code null} when the view materialises none
   */
  record AddedEdge(int src, int tgt, Object[] properties) {
  }

  /**
   * One node's added neighbours of one edge type and direction, each beside its own edge's property values.
   *
   * @param nodeIds    the added neighbours' dense ids
   * @param properties the property values of the edge reaching the neighbour at the same index, or {@code null}
   *                   when no added edge of this type carries any
   */
  record AddedNeighbors(int[] nodeIds, Object[][] properties) {
  }

  static final Object UNSET = new Object();
  private static final int[] EMPTY_INT = new int[0];
}
