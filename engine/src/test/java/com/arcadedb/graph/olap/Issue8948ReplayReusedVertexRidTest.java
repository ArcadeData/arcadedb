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
 * Issue #8948, post-compaction replay: the fresh base already holds the new vertex sitting on a reused slot (and its
 * edge), then the buffered "delete old vertex" and "add new vertex + edge" deltas are replayed over it. The edge must
 * be visible exactly once, and deleting the new vertex afterwards must leave nothing behind.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8948ReplayReusedVertexRidTest {
  private static final String EDGE_TYPE = "K";
  private static final RID    A          = new RID(1, 0);
  private static final RID    REUSED     = new RID(1, 1);
  private static final RID    EDGE       = new RID(2, 20);

  @Test
  void replayedReuseOverAFreshBaseThatAlreadySawTheNewVertexCountsTheEdgeOnce() {
    final NodeIdMapping mapping = new NodeIdMapping(1);
    final int bucket = mapping.registerBucket(1, "V", 2);
    mapping.addNode(bucket, 0);
    mapping.addNode(bucket, 1);
    mapping.compact();
    // fresh base: a(0) -> new vertex(1), captured by the scan
    final CSRAdjacencyIndex csr = new CSRAdjacencyIndex(new int[] { 0, 1, 1 }, new int[] { 1 }, new int[] { 0, 0, 1 }, new int[] { 0 }, 2, 1);
    final Map<String, CSRAdjacencyIndex> fresh = Map.of(EDGE_TYPE, csr);
    final DeltaOverlay.PreCompactionPairCount preCount = (type, src, tgt) -> 0;

    final TxDelta deleteOld = new TxDelta();
    deleteOld.deletedVertices.add(REUSED);
    final TxDelta addNew = new TxDelta();
    addNew.addedVertices.add(new TxDelta.VertexDelta(REUSED, Map.of()));
    addNew.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, A, REUSED, EDGE));

    DeltaOverlay overlay = new DeltaOverlay(mapping.size());
    overlay = overlay.merge(deleteOld, mapping, fresh, preCount);
    overlay = overlay.merge(addNew, mapping, fresh, preCount);

    final int newId = overlay.resolveNodeId(REUSED, mapping);
    assertThat(newId).as("the new vertex is an overflow node").isGreaterThanOrEqualTo(mapping.size());
    assertThat(overlay.isDeleted(1)).as("the base node stays masked, hiding the scan's copy of the edge").isTrue();
    assertThat(overlay.getAddedOutNeighbors(0, EDGE_TYPE)).as("the edge is re-created once, on the overflow node").containsExactly(newId);

    final TxDelta deleteNew = new TxDelta();
    deleteNew.deletedVertices.add(REUSED);
    deleteNew.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, A, REUSED, EDGE));
    final DeltaOverlay afterDelete = overlay.merge(deleteNew, mapping, fresh, preCount);

    assertThat(afterDelete.getAddedOutNeighbors(0, EDGE_TYPE)).isEmpty();
    assertThat(afterDelete.getDeltaEdgeCount()).isZero();
  }
}
