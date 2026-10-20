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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Captures vertex/edge changes from a single transaction for incremental GAV update.
 * Populated by {@link DeltaCollector} during the transaction, then frozen and applied
 * to the {@link DeltaOverlay} after commit.
 * <p>
 * Not thread-safe — accessed only by the owning transaction thread.
 */
class TxDelta {
  // Numbers every lightweight edge change, see lightEdgeKey()
  private static final AtomicLong LIGHT_EDGE_CHANGES = new AtomicLong();

  // Stands, in addedEdges, for a creation the transaction took back by deleting the edge (see cancelAddedEdge). Never reaches a frozen delta
  private static final EdgeDelta  CANCELLED          = new EdgeDelta(null, null, null, null);

  final List<VertexDelta>          addedVertices    = new ArrayList<>();
  final Set<RID>                   deletedVertices  = new HashSet<>();
  final List<EdgeDelta>            addedEdges       = new ArrayList<>();
  final List<EdgeDelta>            deletedEdges     = new ArrayList<>();
  // The updated vertices' properties, per vertex. Each map is the vertex's COMPLETE state after the update, whatever the view's
  // property filter: DeltaOverlay.merge() replaces what it holds with it, so a property the vertex no longer has reads as missing
  // (issue #10335). A producer must never report only the properties it changed. One entry per vertex: the last update wins.
  final Map<RID, Map<String, Object>> updatedProperties = new HashMap<>();

  // Covered edges whose properties changed, with the values they changed to. An edge THIS overlay window
  // added carries its values in its own overlay entry, so such an update is applied there and costs nothing
  // (which is the ordinary `newEdge(...).save()`, an insert that reports one create and one update). An edge
  // already in the base CSR is addressed by a column slot instead, and nothing maps that slot back from its
  // RID, so an update to one leaves the columns holding a value the database no longer has and only a rebuild
  // can repair them - which DeltaOverlay.merge() decides, being the one place that knows which of the two this
  // is. See issues #4513 and #6315.
  // Keyed by the edge's own identity so that a transaction updating one edge's weight repeatedly - an
  // accumulator, say - holds one entry rather than one per call. Last write wins, which is what
  // DeltaOverlay.merge() would have arrived at anyway by overwriting as it walked the list, and it keeps the
  // cap below a count of the edges actually touched instead of of the calls made. Insertion-ordered, so the
  // merge walks them in the order the transaction made them.
  final Map<RID, EdgeDelta>        updatedEdges     = new LinkedHashMap<>();

  // Set only by the synthetic delta GraphAnalyticalView uses to schedule the follow-up rebuild an edge
  // property update buffered during a compaction needs (#4513). It stands for no edge in particular, so it
  // marks the columns out of date outright rather than being resolved against the overlay's own additions.
  boolean                          forceEdgePropertyRebuild = false;

  // The position in addedEdges of each record-backed edge the transaction created, built by cancelAddedEdge() on the first deletion
  // of a transaction that created edges, and kept up to date by addEdge() from then on. Null until then
  private Map<RID, Integer>        addedEdgeSlots;
  // How many entries of addedEdges are CANCELLED tombstones
  private int                      cancelledEdges;

  /**
   * True when this transaction changed a covered edge's properties, however the change is spelled: the
   * individual edges while there were few enough of them to track, or the bare flag {@link DeltaCollector}
   * falls back to past its cap. Asked through one method rather than open-coded, because a caller that
   * remembers only the first spelling silently skips exactly the deltas that need the rebuild most - a bulk
   * rewrite - and the view is then left unable to serve edge properties until some later commit happens to
   * re-trigger it.
   */
  boolean hasEdgePropertyChanges() {
    return !updatedEdges.isEmpty() || forceEdgePropertyRebuild;
  }

  boolean isEmpty() {
    return addedVertices.isEmpty() && deletedVertices.isEmpty()
        && addedEdges.size() == cancelledEdges && deletedEdges.isEmpty()
        && updatedProperties.isEmpty() && !hasEdgePropertyChanges();
  }

  void clear() {
    addedVertices.clear();
    deletedVertices.clear();
    addedEdges.clear();
    deletedEdges.clear();
    updatedProperties.clear();
    updatedEdges.clear();
    forceEdgePropertyRebuild = false;
    addedEdgeSlots = null;
    cancelledEdges = 0;
  }

  /** Reports an edge the transaction created. Edges are reported through here, so a later deletion can find its creation. */
  void addEdge(final EdgeDelta edge) {
    addedEdges.add(edge);
    if (addedEdgeSlots != null && !edge.isLightweight())
      addedEdgeSlots.put(edge.rid, addedEdges.size() - 1);
  }

  /**
   * Takes back the creation of the record-backed edge {@code rid} when this transaction made it, because the transaction now
   * deletes it: an edge created and deleted by one transaction is no change to the committed state, and a deletion reported for its
   * RID would be applied to whichever edge another transaction committed into the same slot meanwhile (this one holds its own copy
   * of the page, so it was handed the slot out again), losing that edge from the view (issue #10305). Another transaction's edge
   * cannot be mistaken for it: the slot is the transaction's own for as long as it holds the edge.
   * <p>
   * The index of the created edges is built on the first deletion of a transaction that created edges, so a transaction that only
   * inserts, a bulk load by the million, pays nothing for it. A cancelled entry stays in the list as a tombstone, so the creation
   * order of the others is kept and nothing shifts.
   *
   * @return true when the transaction created the edge, whose deletion must then not be reported
   */
  boolean cancelAddedEdge(final RID rid) {
    if (addedEdges.size() == cancelledEdges)
      return false;
    if (addedEdgeSlots == null) {
      addedEdgeSlots = new HashMap<>();
      for (int i = 0; i < addedEdges.size(); i++) {
        final EdgeDelta edge = addedEdges.get(i);
        if (edge != CANCELLED && !edge.isLightweight())
          addedEdgeSlots.put(edge.rid, i);
      }
    }
    final Integer slot = addedEdgeSlots.remove(rid);
    if (slot == null)
      return false;
    addedEdges.set(slot, CANCELLED);
    ++cancelledEdges;
    // the properties it was given after its creation went with it
    updatedEdges.remove(rid);
    return true;
  }

  /** Adds the edges the transaction created and kept, in the order it created them, to {@code target}. */
  void copyAddedEdgesTo(final List<EdgeDelta> target) {
    if (cancelledEdges == 0) {
      target.addAll(addedEdges);
      return;
    }
    for (final EdgeDelta edge : addedEdges)
      if (edge != CANCELLED)
        target.add(edge);
  }

  static class VertexDelta {
    final RID                rid;
    final Map<String, Object> properties;

    VertexDelta(final RID rid, final Map<String, Object> properties) {
      this.rid = rid;
      this.properties = properties;
    }
  }

  static class EdgeDelta {
    final String edgeType;
    final RID    source;
    final RID    target;
    // The deleted/added edge's own identity. For deletedEdges this is what lets DeltaOverlay.merge()
    // tell "the same edge reported twice" (replayed across merges, or emitted twice within one TxDelta -
    // must not double-count) apart from "two distinct parallel edges between the same pair" (must each
    // count, see issue #6769).
    final RID    rid;
    // For addedEdges and updatedEdges, the values of the edge properties the view materialises columns for -
    // one slot per name of the view's edge property filter, in its order - or null when it materialises none.
    // An added edge has no column slot of its own, the columns having been built with the base CSR, so this is
    // the only place its weight can be read from while the overlay is the view's representation of it (issue
    // #6315). Null for deletedEdges, which are never asked for a value.
    //
    // An array rather than a map because there is one of these per added edge and a bulk insert makes as many
    // of them as it inserts edges: the names are a small fixed list known before the first edge arrives, so
    // carrying them again per edge would be paying for a hash table per edge to look up a position the caller
    // can compute once for a whole slice.
    final Object[] properties;

    EdgeDelta(final String edgeType, final RID source, final RID target, final RID rid) {
      this(edgeType, source, target, rid, null);
    }

    EdgeDelta(final String edgeType, final RID source, final RID target, final RID rid,
        final Object[] properties) {
      this.edgeType = edgeType;
      this.source = source;
      this.target = target;
      this.rid = rid;
      this.properties = properties;
    }

    /**
     * The change of a lightweight edge, which {@link #rid} then names by a key of the change's own (see
     * {@link #lightEdgeKey}): its bucket is the edge type's, its position a negative number no record has.
     */
    boolean isLightweight() {
      return rid.getPosition() < 0;
    }
  }

  /**
   * An identity for one change of a lightweight edge (issue #9572). The edge's own identity is the triple (type, out,
   * in), which every copy of a duplicated lightweight edge shares, so keying the overlay by it would merge two copies
   * into one, and drop the second of two deletions as a replay of the first. Each creation and each deletion gets a
   * key no other change has instead, kept for as long as the delta is: a delta replayed on another base still
   * carries the same keys, so the replay is recognised. Which copy a deletion took does not matter, the copies of a
   * triple being indistinguishable, so {@link DeltaOverlay#merge} matches a deletion to an addition by the pair.
   * <p>
   * The bucket stays the edge type's, the one the edge-list entry carries, and the position is below the -1 every
   * record-less RID uses, so the key equals no edge identity the database hands out. In memory only: a key is never
   * persisted (the persisted CSR holds no overlay) nor shown to a query.
   */
  static RID lightEdgeKey(final int edgeTypeBucketId) {
    return new RID(edgeTypeBucketId, -2L - LIGHT_EDGE_CHANGES.getAndIncrement());
  }

  /**
   * The sequence number of the lightweight change a {@link #lightEdgeKey} names: changes are numbered as they are
   * reported, inside their transaction and so before it commits.
   */
  static long lightEdgeChangeOf(final RID key) {
    return -2L - key.getPosition();
  }

  /** How many lightweight changes were reported so far: every later one is numbered from here. */
  static long lightEdgeChangesSoFar() {
    return LIGHT_EDGE_CHANGES.get();
  }
}
