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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4395: {@code EdgeLinkedList.removeVertex} must remove <b>every</b> reference to a vertex,
 * even when the vertex has many parallel edges to the same neighbour spread across multiple edge segments. The single
 * edge deletion path ({@code removeEdge}) must keep removing exactly one entry, so parallel (lightweight) edges are
 * preserved.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class EdgeLinkedListRemoveVertexTest extends TestHelper {

  // The initial edge segment is 64 bytes, so a few dozen entries span many segments.
  private static final int PARALLEL_EDGES = 80;

  @Test
  void bulkRemoveVertexClearsAllSegments() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");

    final RID[] rids = new RID[2];

    // Use lightweight edges (no edge records) and clean BOTH directions, so the one-sided low-level
    // removeVertex() primitive leaves the graph consistent for the integrity checker.
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").save();
      final MutableVertex b = database.newVertex("V").save();
      for (int i = 0; i < PARALLEL_EDGES; i++)
        a.newLightEdge("E", b);

      rids[0] = a.getIdentity();
      rids[1] = b.getIdentity();

      assertThat(countEdges(a, Vertex.DIRECTION.OUT)).isEqualTo(PARALLEL_EDGES);
      assertThat(countEdges(b, Vertex.DIRECTION.IN)).isEqualTo(PARALLEL_EDGES);

      final GraphEngine ge = ((DatabaseInternal) database).getGraphEngine();
      // BULK REMOVAL: must wipe every reference, across all segments (issue #4395)
      ge.getEdgeHeadChunk((VertexInternal) a, Vertex.DIRECTION.OUT).removeVertex(b.getIdentity());
      ge.getEdgeHeadChunk((VertexInternal) b, Vertex.DIRECTION.IN).removeVertex(a.getIdentity());
    });

    // Verify after commit in a fresh transaction
    database.transaction(() -> {
      final Vertex a = database.lookupByRID(rids[0], true).asVertex();
      final Vertex b = database.lookupByRID(rids[1], true).asVertex();
      assertThat(countEdges(a, Vertex.DIRECTION.OUT)).isEqualTo(0);
      assertThat(countEdges(b, Vertex.DIRECTION.IN)).isEqualTo(0);
    });
  }

  @Test
  void deleteSingleRegularEdgeRemovesOnlyOne() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");

    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").save();
      final MutableVertex b = database.newVertex("V").save();
      RID firstEdge = null;
      for (int i = 0; i < PARALLEL_EDGES; i++) {
        final RID e = a.newEdge("E", b).save().getIdentity();
        if (i == 0)
          firstEdge = e; // oldest edge, lives in the deepest segment
      }

      database.lookupByRID(firstEdge, true).asEdge().delete();

      assertThat(countEdges(a, Vertex.DIRECTION.OUT)).isEqualTo(PARALLEL_EDGES - 1);
      assertThat(countEdges(b, Vertex.DIRECTION.IN)).isEqualTo(PARALLEL_EDGES - 1);
    });
  }

  @Test
  void deleteSingleLightEdgeRemovesOnlyOne() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");

    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").save();
      final MutableVertex b = database.newVertex("V").save();
      for (int i = 0; i < PARALLEL_EDGES; i++)
        a.newLightEdge("E", b);

      // delete a single light edge: parallel light edges to B must be preserved
      a.getEdges(Vertex.DIRECTION.OUT, "E").iterator().next().delete();

      assertThat(countEdges(a, Vertex.DIRECTION.OUT)).isEqualTo(PARALLEL_EDGES - 1);
      assertThat(countEdges(b, Vertex.DIRECTION.IN)).isEqualTo(PARALLEL_EDGES - 1);
    });
  }

  @Test
  void deleteNeighbourVertexLeavesNoOrphanReferences() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");

    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").save();
      final MutableVertex b = database.newVertex("V").save();
      for (int i = 0; i < PARALLEL_EDGES; i++)
        a.newEdge("E", b).save();

      final RID aRid = a.getIdentity();
      b.delete();

      assertThat(countEdges(database.lookupByRID(aRid, true).asVertex(), Vertex.DIRECTION.OUT)).isEqualTo(0);
    });
  }

  private long countEdges(final Vertex v, final Vertex.DIRECTION dir) {
    long c = 0;
    for (final Edge ignored : v.getEdges(dir, "E"))
      c++;
    return c;
  }
}
