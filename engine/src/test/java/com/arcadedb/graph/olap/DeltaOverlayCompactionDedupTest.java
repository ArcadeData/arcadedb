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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4588: after a compaction, the buffered transaction deltas are re-applied
 * against the freshly built base CSR. A delta that committed before the read-committed CSR scan crossed
 * its bucket is already reflected in the new base, so re-merging it blindly would surface duplicate
 * neighbours and inflate the delta edge counter. The CSR-aware {@code merge} overload must skip such
 * already-present edges while still reinstating edges that the overlay masks as deleted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DeltaOverlayCompactionDedupTest {
  private static final String EDGE_TYPE = "Knows";

  /**
   * Builds a single-bucket mapping where global ids match positions 0..count-1.
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
   * Builds a base CSR over {@code nodeCount} nodes containing the single forward edge {@code src -> tgt}.
   */
  private Map<String, CSRAdjacencyIndex> csrWithEdge(final int nodeCount, final int src, final int tgt) {
    final int[] fwdOffsets = new int[nodeCount + 1];
    final int[] bwdOffsets = new int[nodeCount + 1];
    for (int i = 0; i <= nodeCount; i++) {
      fwdOffsets[i] = i > src ? 1 : 0;
      bwdOffsets[i] = i > tgt ? 1 : 0;
    }
    final CSRAdjacencyIndex csr = new CSRAdjacencyIndex(fwdOffsets, new int[] { tgt }, bwdOffsets, new int[] { src }, nodeCount, 1);
    return Map.of(EDGE_TYPE, csr);
  }

  private Map<String, CSRAdjacencyIndex> emptyCsr(final int nodeCount) {
    final CSRAdjacencyIndex csr = new CSRAdjacencyIndex(new int[nodeCount + 1], new int[0], new int[nodeCount + 1], new int[0],
        nodeCount, 0);
    return Map.of(EDGE_TYPE, csr);
  }

  /**
   * The core bug: a buffered add of an edge already present in the freshly built base CSR must not be
   * re-added to the overlay, otherwise reads concatenate it twice.
   */
  @Test
  void bufferedAddAlreadyInBaseCsrIsSkipped() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final DeltaOverlay empty = new DeltaOverlay(mapping.size());

    final TxDelta replay = new TxDelta();
    replay.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1)));

    // Without CSR awareness (legacy path) the edge would be appended to the overlay → duplicate.
    final DeltaOverlay legacy = empty.merge(replay, mapping);
    assertThat(legacy.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1);
    assertThat(legacy.getDeltaEdgeCount()).isEqualTo(1);

    // With CSR awareness the already-present edge is skipped.
    final DeltaOverlay deduped = empty.merge(replay, mapping, csrWithEdge(2, 0, 1));
    assertThat(deduped.getAddedOutNeighbors(0, EDGE_TYPE)).isEmpty();
    assertThat(deduped.getAddedInNeighbors(1, EDGE_TYPE)).isEmpty();
    assertThat(deduped.getDeltaEdgeCount()).isEqualTo(0);
  }

  /**
   * A buffered add of an edge that the scan did NOT capture (not in the fresh base CSR) must still be
   * applied, otherwise the update would be lost.
   */
  @Test
  void bufferedAddNotInBaseCsrIsApplied() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final DeltaOverlay empty = new DeltaOverlay(mapping.size());

    final TxDelta delta = new TxDelta();
    delta.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1)));

    final DeltaOverlay merged = empty.merge(delta, mapping, emptyCsr(2));
    assertThat(merged.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1);
    assertThat(merged.getDeltaEdgeCount()).isEqualTo(1);
  }

  /**
   * Delete then re-add of the same edge across two buffered deltas: the re-add must be reinstated even
   * though the edge exists in the base CSR, because an earlier delta masks it as deleted.
   */
  @Test
  void deleteThenReAddAcrossDeltasReinstatesEdge() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final Map<String, CSRAdjacencyIndex> csr = csrWithEdge(2, 0, 1);
    final DeltaOverlay empty = new DeltaOverlay(mapping.size());

    final TxDelta del = new TxDelta();
    del.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1)));
    final DeltaOverlay afterDelete = empty.merge(del, mapping, csr);
    assertThat(afterDelete.isEdgeDeleted(EDGE_TYPE, 0, 1)).isTrue();

    final TxDelta readd = new TxDelta();
    readd.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1)));
    final DeltaOverlay afterReadd = afterDelete.merge(readd, mapping, csr);

    // The explicit add is kept so the base deletion is offset → edge present once on read.
    assertThat(afterReadd.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1);
  }

  /**
   * Delete then re-add of the same edge within a single buffered delta must keep the add for the same
   * reason: the deletion would otherwise erase the base edge.
   */
  @Test
  void deleteThenReAddWithinSingleDeltaReinstatesEdge() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final DeltaOverlay empty = new DeltaOverlay(mapping.size());

    final TxDelta delta = new TxDelta();
    delta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1)));
    delta.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1)));

    final DeltaOverlay merged = empty.merge(delta, mapping, csrWithEdge(2, 0, 1));
    assertThat(merged.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1);
    assertThat(merged.isEdgeDeleted(EDGE_TYPE, 0, 1)).isTrue();
  }
}
