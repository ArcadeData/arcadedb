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

    final TxDelta firstDelete = new TxDelta();
    firstDelete.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1)));
    final DeltaOverlay afterFirst = empty.merge(firstDelete, mapping);
    assertThat(afterFirst.getDeltaEdgeCount()).isEqualTo(-1);

    // Replay the exact same deletion in a subsequent transaction delta.
    final TxDelta secondDelete = new TxDelta();
    secondDelete.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1)));
    final DeltaOverlay afterSecond = afterFirst.merge(secondDelete, mapping);

    // Before the fix this drifted to -2.
    assertThat(afterSecond.getDeltaEdgeCount()).isEqualTo(-1);
    assertThat(afterSecond.isEdgeDeleted(EDGE_TYPE, 0, 1)).isTrue();
  }

  /**
   * Issue #4587: the same deletion emitted twice within a single TxDelta must decrement only once.
   */
  @Test
  void duplicateEdgeDeletionWithinSingleDeltaDecrementsOnce() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final DeltaOverlay empty = new DeltaOverlay(mapping.size());

    final TxDelta delta = new TxDelta();
    delta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1)));
    delta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1)));

    final DeltaOverlay merged = empty.merge(delta, mapping);
    assertThat(merged.getDeltaEdgeCount()).isEqualTo(-1);
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
    delta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1)));
    delta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(2)));

    final DeltaOverlay merged = empty.merge(delta, mapping);
    assertThat(merged.getDeltaEdgeCount()).isEqualTo(-2);
  }

  /**
   * An added edge followed by its deletion across merges nets the counter back to zero.
   */
  @Test
  void addThenDeleteNetsToZero() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final DeltaOverlay empty = new DeltaOverlay(mapping.size());

    final TxDelta addDelta = new TxDelta();
    addDelta.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1)));
    final DeltaOverlay afterAdd = empty.merge(addDelta, mapping);
    assertThat(afterAdd.getDeltaEdgeCount()).isEqualTo(1);

    final TxDelta delDelta = new TxDelta();
    delDelta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1)));
    final DeltaOverlay afterDelete = afterAdd.merge(delDelta, mapping);
    assertThat(afterDelete.getDeltaEdgeCount()).isEqualTo(0);

    // Replaying the deletion must not push it negative.
    final TxDelta replay = new TxDelta();
    replay.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1)));
    final DeltaOverlay afterReplay = afterDelete.merge(replay, mapping);
    assertThat(afterReplay.getDeltaEdgeCount()).isEqualTo(0);
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
}
