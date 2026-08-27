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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for {@link DeltaOverlay} delta edge counter accounting.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DeltaOverlayTest {
  private static final String EDGE_TYPE = "Knows";

  /**
   * Builds a minimal compacted mapping with the given number of base nodes in bucket 1,
   * positions 0..count-1. The global ids match the local positions because there is a
   * single bucket based at 0.
   */
  private NodeIdMapping baseMappingWith(final int count) {
    final NodeIdMapping mapping = new NodeIdMapping(1);
    final int bucketIdx = mapping.registerBucket(1, "V", count);
    for (int i = 0; i < count; i++)
      mapping.addNode(bucketIdx, i);
    mapping.compact();
    return mapping;
  }

  private RID rid(final int position) {
    return new RID(1, position);
  }

  /**
   * Issue #4587: a deletion of the same edge replayed across two merges must not drive
   * deltaEdgeCount below the single decrement that the unique deletion warrants.
   */
  @Test
  void duplicateEdgeDeletionAcrossMergesDecrementsOnce() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final DeltaOverlay empty = new DeltaOverlay(mapping.size());
    final RID edgeRid = rid(10);

    final TxDelta firstDelete = new TxDelta();
    firstDelete.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));
    final DeltaOverlay afterFirst = empty.merge(firstDelete, mapping);
    assertThat(afterFirst.getDeltaEdgeCount()).isEqualTo(-1);

    // Replay the exact same deletion (same edge identity) in a subsequent transaction delta.
    final TxDelta secondDelete = new TxDelta();
    secondDelete.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));
    final DeltaOverlay afterSecond = afterFirst.merge(secondDelete, mapping);

    // Before the fix this drifted to -2.
    assertThat(afterSecond.getDeltaEdgeCount()).isEqualTo(-1);
    assertThat(afterSecond.isEdgeDeleted(EDGE_TYPE, 0, 1)).isTrue();
    assertThat(afterSecond.countDeletedEdges(EDGE_TYPE, 0, 1)).isEqualTo(1);
  }

  /**
   * Issue #4587: the same deletion emitted twice within a single TxDelta must decrement only once.
   */
  @Test
  void duplicateEdgeDeletionWithinSingleDeltaDecrementsOnce() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final DeltaOverlay empty = new DeltaOverlay(mapping.size());
    final RID edgeRid = rid(10);

    final TxDelta delta = new TxDelta();
    delta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));
    delta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));

    final DeltaOverlay merged = empty.merge(delta, mapping);
    assertThat(merged.getDeltaEdgeCount()).isEqualTo(-1);
    assertThat(merged.countDeletedEdges(EDGE_TYPE, 0, 1)).isEqualTo(1);
  }

  /**
   * Distinct edge deletions must each decrement the counter, ensuring the fix does not
   * suppress legitimate accounting.
   */
  @Test
  void distinctEdgeDeletionsEachDecrement() {
    final NodeIdMapping mapping = baseMappingWith(3);
    final DeltaOverlay empty = new DeltaOverlay(mapping.size());

    final TxDelta delta = new TxDelta();
    delta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), rid(10)));
    delta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(2), rid(11)));

    final DeltaOverlay merged = empty.merge(delta, mapping);
    assertThat(merged.getDeltaEdgeCount()).isEqualTo(-2);
  }

  /**
   * Issue #6769: two DISTINCT parallel edges between the SAME pair, each deleted, must each be
   * counted - the pair alone cannot distinguish "one of two parallel edges deleted" from "the
   * same edge reported twice". A boolean per-pair flag (the pre-fix representation) collapses
   * both to the same answer, which then made {@code GraphAnalyticalView#copyBaseExcludingDeleted}
   * exclude every parallel edge between the pair instead of just the ones actually deleted.
   */
  @Test
  void distinctParallelEdgesBetweenSamePairAreCountedSeparately() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final DeltaOverlay empty = new DeltaOverlay(mapping.size());

    final TxDelta delta = new TxDelta();
    delta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), rid(10)));
    delta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), rid(11)));

    final DeltaOverlay merged = empty.merge(delta, mapping);
    assertThat(merged.getDeltaEdgeCount()).isEqualTo(-2);
    assertThat(merged.countDeletedEdges(EDGE_TYPE, 0, 1)).isEqualTo(2);

    // Replaying just one of the two deletions must not inflate the count further.
    final TxDelta replayOne = new TxDelta();
    replayOne.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), rid(10)));
    final DeltaOverlay afterReplay = merged.merge(replayOne, mapping);
    assertThat(afterReplay.getDeltaEdgeCount()).isEqualTo(-2);
    assertThat(afterReplay.countDeletedEdges(EDGE_TYPE, 0, 1)).isEqualTo(2);
  }

  /**
   * An added edge followed by its deletion across merges nets the counter back to zero.
   */
  @Test
  void addThenDeleteNetsToZero() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final DeltaOverlay empty = new DeltaOverlay(mapping.size());
    final RID edgeRid = rid(10);

    final TxDelta addDelta = new TxDelta();
    addDelta.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));
    final DeltaOverlay afterAdd = empty.merge(addDelta, mapping);
    assertThat(afterAdd.getDeltaEdgeCount()).isEqualTo(1);

    final TxDelta delDelta = new TxDelta();
    delDelta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));
    final DeltaOverlay afterDelete = afterAdd.merge(delDelta, mapping);
    assertThat(afterDelete.getDeltaEdgeCount()).isEqualTo(0);

    // Replaying the deletion must not push it negative.
    final TxDelta replay = new TxDelta();
    replay.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));
    final DeltaOverlay afterReplay = afterDelete.merge(replay, mapping);
    assertThat(afterReplay.getDeltaEdgeCount()).isEqualTo(0);
  }

  /**
   * Issue #6775: an edge created and deleted inside ONE transaction must be withdrawn from the added-edge
   * index, not masked with a pair-keyed deletion. Both facts reach {@code merge()} in the same
   * {@link TxDelta} (adds are processed first), and the pair alone cannot say which of the pair's edges
   * the deletion belongs to - recording one would spend an exclusion budget against the base CSR's run for
   * the pair, masking an edge nobody deleted, while leaving the phantom add on record as a live neighbour.
   */
  @Test
  void edgeAddedAndDeletedWithinOneDeltaIsWithdrawnRatherThanMasked() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final DeltaOverlay empty = new DeltaOverlay(mapping.size());
    final RID edgeRid = rid(10);

    final TxDelta delta = new TxDelta();
    delta.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));
    delta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));

    final DeltaOverlay merged = empty.merge(delta, mapping);

    assertThat(merged.getAddedOutNeighbors(0, EDGE_TYPE)).isEmpty();
    assertThat(merged.getAddedInNeighbors(1, EDGE_TYPE)).isEmpty();
    // No budget on record: there is no base edge for it to be spent against.
    assertThat(merged.countDeletedEdges(EDGE_TYPE, 0, 1)).isZero();
    assertThat(merged.countDeletedOutEdges(0, EDGE_TYPE)).isZero();
    assertThat(merged.countDeletedInEdges(1, EDGE_TYPE)).isZero();
    assertThat(merged.getDeltaEdgeCount()).isZero();
    assertThat(merged.hasChanges()).isFalse();
  }

  /**
   * Issue #6775, the across-transactions shape: the add lands in one overlay window and the deletion in a
   * later one, before any compaction. The withdrawal must reach back into the added index the earlier
   * merge built.
   */
  @Test
  void edgeAddedAndDeletedAcrossDeltasIsWithdrawnRatherThanMasked() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final RID edgeRid = rid(10);

    final TxDelta addDelta = new TxDelta();
    addDelta.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));
    final DeltaOverlay afterAdd = new DeltaOverlay(mapping.size()).merge(addDelta, mapping);
    assertThat(afterAdd.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1);

    final TxDelta delDelta = new TxDelta();
    delDelta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));
    final DeltaOverlay afterDelete = afterAdd.merge(delDelta, mapping);

    assertThat(afterDelete.getAddedOutNeighbors(0, EDGE_TYPE)).isEmpty();
    assertThat(afterDelete.getAddedInNeighbors(1, EDGE_TYPE)).isEmpty();
    assertThat(afterDelete.countDeletedEdges(EDGE_TYPE, 0, 1)).isZero();
    assertThat(afterDelete.getDeltaEdgeCount()).isZero();

    // The earlier overlay is immutable and must not have been reached into.
    assertThat(afterAdd.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1);
  }

  /**
   * Issue #6775 meets #6769: deleting ONE of two parallel edges the overlay itself added must withdraw
   * exactly that edge and leave its sibling live, rather than withdrawing both or masking the pair.
   */
  @Test
  void deletingOneOfTwoParallelAddedEdgesWithdrawsOnlyThatOne() {
    final NodeIdMapping mapping = baseMappingWith(2);

    final TxDelta addDelta = new TxDelta();
    addDelta.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), rid(10)));
    addDelta.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), rid(11)));
    final DeltaOverlay afterAdd = new DeltaOverlay(mapping.size()).merge(addDelta, mapping);
    assertThat(afterAdd.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1, 1);
    assertThat(afterAdd.getDeltaEdgeCount()).isEqualTo(2);

    final TxDelta delDelta = new TxDelta();
    delDelta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), rid(10)));
    final DeltaOverlay afterDelete = afterAdd.merge(delDelta, mapping);

    assertThat(afterDelete.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1);
    assertThat(afterDelete.getAddedInNeighbors(1, EDGE_TYPE)).containsExactly(0);
    assertThat(afterDelete.countDeletedEdges(EDGE_TYPE, 0, 1)).isZero();
    assertThat(afterDelete.getDeltaEdgeCount()).isEqualTo(1);
  }

  /**
   * The other half of the #6775 rule: a deletion of an edge the overlay never added is a BASE edge, and it
   * must still leave the per-pair budget {@code GraphAnalyticalView#copyBaseExcludingDeleted} spends
   * against the base CSR run. Withdrawing adds must not have made every deletion vanish.
   */
  @Test
  void deletionOfAnEdgeTheOverlayNeverAddedStillRecordsTheBaseBudget() {
    final NodeIdMapping mapping = baseMappingWith(2);

    // One edge the overlay added, and one it did not (it lives in the base CSR).
    final TxDelta addDelta = new TxDelta();
    addDelta.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), rid(11)));
    final DeltaOverlay afterAdd = new DeltaOverlay(mapping.size()).merge(addDelta, mapping);

    final TxDelta delDelta = new TxDelta();
    delDelta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), rid(10)));
    final DeltaOverlay afterDelete = afterAdd.merge(delDelta, mapping);

    assertThat(afterDelete.countDeletedEdges(EDGE_TYPE, 0, 1)).isEqualTo(1);
    assertThat(afterDelete.countDeletedOutEdges(0, EDGE_TYPE)).isEqualTo(1);
    // ...and the unrelated added edge is untouched
    assertThat(afterDelete.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1);
    assertThat(afterDelete.getDeltaEdgeCount()).isZero();
  }

  /**
   * Keying the added edges by identity also absorbs a replayed add of an edge the overlay already holds,
   * which the previous append-only list turned into a second, phantom, occurrence of one edge.
   */
  @Test
  void replayedAddOfTheSameEdgeIsAbsorbed() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final RID edgeRid = rid(10);

    final TxDelta addDelta = new TxDelta();
    addDelta.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));
    final DeltaOverlay afterAdd = new DeltaOverlay(mapping.size()).merge(addDelta, mapping);

    final TxDelta replay = new TxDelta();
    replay.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));
    final DeltaOverlay afterReplay = afterAdd.merge(replay, mapping);

    assertThat(afterReplay.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1);
    assertThat(afterReplay.getDeltaEdgeCount()).isEqualTo(1);
  }

  /**
   * Issue #4720: {@code getOverflowCount()} must report the number of live overflow vertices,
   * subtracting the ones that have been deleted, consistently with {@code getTotalNodeCount()}.
   * Before the fix the counter kept counting deleted overflow slots, inflating the reported count.
   */
  @Test
  void overflowCountExcludesDeletedOverflowVertices() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final DeltaOverlay empty = new DeltaOverlay(mapping.size());

    // Add three overflow vertices (RIDs not present in the base mapping).
    final TxDelta addDelta = new TxDelta();
    addDelta.addedVertices.add(new TxDelta.VertexDelta(rid(2), null));
    addDelta.addedVertices.add(new TxDelta.VertexDelta(rid(3), null));
    addDelta.addedVertices.add(new TxDelta.VertexDelta(rid(4), null));
    final DeltaOverlay afterAdd = empty.merge(addDelta, mapping);

    assertThat(afterAdd.getOverflowCount()).isEqualTo(3);
    assertThat(afterAdd.getTotalNodeCount()).isEqualTo(5); // 2 base + 3 overflow

    // Delete one of the overflow vertices.
    final TxDelta delDelta = new TxDelta();
    delDelta.deletedVertices.add(rid(3));
    final DeltaOverlay afterDelete = afterAdd.merge(delDelta, mapping);

    // Before the fix this stayed at 3 because the deleted slot kept being counted.
    assertThat(afterDelete.getOverflowCount()).isEqualTo(2);
    assertThat(afterDelete.getTotalNodeCount()).isEqualTo(4); // 2 base + 2 live overflow

    // Deleting the remaining overflow vertices drives the live count to zero.
    final TxDelta delRest = new TxDelta();
    delRest.deletedVertices.add(rid(2));
    delRest.deletedVertices.add(rid(4));
    final DeltaOverlay afterDeleteAll = afterDelete.merge(delRest, mapping);

    assertThat(afterDeleteAll.getOverflowCount()).isEqualTo(0);
    assertThat(afterDeleteAll.getTotalNodeCount()).isEqualTo(2); // only base nodes remain
  }

  /**
   * Issue #6315: a change to one edge type's properties leaves that type's columns out of date and no other
   * type's. Serving the columnar path is decided per type, so disqualifying every type for an update to one of
   * them would cost a view that materialises several of them the fast path it could still honestly offer.
   */
  @Test
  void anEdgePropertyUpdateDirtiesOnlyItsOwnEdgeType() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final TxDelta delta = new TxDelta();
    delta.updatedEdges.put(rid(10), new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), rid(10), null));

    final DeltaOverlay overlay = new DeltaOverlay(mapping.size()).merge(delta, mapping);

    assertThat(overlay.isEdgePropertiesDirty(EDGE_TYPE)).isTrue();
    assertThat(overlay.isEdgePropertiesDirty("SomeOtherType")).isFalse();
    assertThat(overlay.isEdgePropertiesDirty()).as("some type is out of date").isTrue();
  }

  /**
   * Issue #6315: the bulk case, where the collector gave up on naming the edges individually, has to leave
   * every type out of date - it no longer knows which ones it would otherwise have named.
   */
  @Test
  void aBulkEdgePropertyRewriteDirtiesEveryEdgeType() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final TxDelta delta = new TxDelta();
    delta.forceEdgePropertyRebuild = true;

    final DeltaOverlay overlay = new DeltaOverlay(mapping.size()).merge(delta, mapping);

    assertThat(overlay.isEdgePropertiesDirty(EDGE_TYPE)).isTrue();
    assertThat(overlay.isEdgePropertiesDirty("SomeOtherType")).isTrue();
  }

  /**
   * Issue #6315: an update to an edge the overlay itself holds is applied to that edge's own entry, so nothing
   * about the base columns went out of date - which is what keeps the ordinary insert, reported as one create
   * and one update of the same edge, from forcing a rebuild.
   */
  @Test
  void updatingAnEdgeTheOverlayHoldsDirtiesNothing() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final RID edgeRid = rid(10);

    final TxDelta delta = new TxDelta();
    delta.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid, new Object[] { 1.0 }));
    delta.updatedEdges.put(edgeRid, new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid, new Object[] { 2.0 }));

    final DeltaOverlay overlay = new DeltaOverlay(mapping.size()).merge(delta, mapping);

    assertThat(overlay.isEdgePropertiesDirty(EDGE_TYPE)).isFalse();
    assertThat(overlay.getAdded(0, EDGE_TYPE, true).properties()[0])
        .as("the added edge's own entry carries the value the update set")
        .isEqualTo(new Object[] { 2.0 });
  }
}
