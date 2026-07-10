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
import com.arcadedb.engine.DatabaseChecker;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4395:
 * EdgeLinkedList.removeVertex stopped after the first matching segment,
 * leaving orphaned edge references in older segments.
 */
class EdgeLinkedListRemoveAllSegmentsTest extends TestHelper {

  private static final String VERTEX_TYPE = "Node";
  private static final String EDGE_TYPE   = "Link";

  /**
   * Directly tests that EdgeLinkedList.removeVertex(RID) removes ALL entries
   * referencing the given vertex across ALL segments, not just the first
   * matching segment.
   *
   * The initial edge-list segment is 64 bytes with a 17-byte header, holding
   * approximately 11 entries. Creating 25 bidirectional light edges forces
   * at least three segments. A single call to removeVertex must clean all of
   * them; before the fix it stopped at the first matching segment.
   */
  @Test
  void removeVertexCleansAllSegmentsInSingleCall() {
    database.getSchema().buildVertexType().withName(VERTEX_TYPE).create();
    database.getSchema().buildEdgeType().withName(EDGE_TYPE).create();

    final RID[] srcRID = new RID[1];
    final RID[] dstRID = new RID[1];
    final int edgeCount = 25;

    database.transaction(() -> {
      final MutableVertex src = database.newVertex(VERTEX_TYPE).set("name", "src").save();
      final MutableVertex dst = database.newVertex(VERTEX_TYPE).set("name", "dst").save();
      srcRID[0] = src.getIdentity();
      dstRID[0] = dst.getIdentity();
      for (int i = 0; i < edgeCount; i++)
        src.newLightEdge(EDGE_TYPE, dst);
    });

    // Verify precondition: all edges are counted on the source OUT list
    database.transaction(() -> {
      final Vertex src = srcRID[0].asVertex(true);
      final long countBefore = src.countEdges(Vertex.DIRECTION.OUT, null);
      assertThat(countBefore).as("OUT edge count before removal").isEqualTo(edgeCount);
    });

    // Directly call removeVertex ONCE and verify ALL entries are removed.
    // Before the fix, only the first matching segment was cleaned, leaving
    // count > 0 after a single call.
    database.transaction(() -> {
      final EdgeLinkedList outEdges = ((DatabaseInternal) database)
          .getGraphEngine()
          .getEdgeHeadChunk((VertexInternal) srcRID[0].asVertex(true), Vertex.DIRECTION.OUT);
      assertThat(outEdges).as("OUT edge list must not be null").isNotNull();
      outEdges.removeVertex(dstRID[0]);
    });

    database.transaction(() -> {
      final Vertex src = srcRID[0].asVertex(true);
      final long countAfter = src.countEdges(Vertex.DIRECTION.OUT, null);
      assertThat(countAfter)
          .as("A single removeVertex call must clean ALL segments - orphaned entries remain without the fix")
          .isEqualTo(0);
    });
  }

  /**
   * Regression test: deleting a vertex with many bidirectional light edges
   * that span multiple edge-list segments must leave the neighbor with zero
   * edge references. This exercises the full deleteVertex path.
   */
  @Test
  void deleteVertexCleansNeighborEdgeListAcrossSegments() {
    database.getSchema().buildVertexType().withName(VERTEX_TYPE).create();
    database.getSchema().buildEdgeType().withName(EDGE_TYPE).create();

    final RID[] srcRID = new RID[1];
    final RID[] dstRID = new RID[1];
    final int edgeCount = 25;

    database.transaction(() -> {
      final MutableVertex src = database.newVertex(VERTEX_TYPE).set("name", "src").save();
      final MutableVertex dst = database.newVertex(VERTEX_TYPE).set("name", "dst").save();
      srcRID[0] = src.getIdentity();
      dstRID[0] = dst.getIdentity();
      for (int i = 0; i < edgeCount; i++)
        src.newLightEdge(EDGE_TYPE, dst);
    });

    // Delete the destination vertex
    database.transaction(() -> database.deleteRecord(dstRID[0].asVertex(true)));

    database.transaction(() -> {
      final Vertex src = srcRID[0].asVertex(true);
      final long countAfter = src.countEdges(Vertex.DIRECTION.OUT, null);
      assertThat(countAfter)
          .as("OUT edge count after deleting destination vertex must be 0")
          .isEqualTo(0);
    });

    new DatabaseChecker(database).setVerboseLevel(0).check();
  }
}
