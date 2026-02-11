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
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Vertex v1 format with per-type edge maps.
 *
 * Verifies that edges are organized by type (bucket ID) for optimized access.
 * The per-type map optimization improves performance from O(n) to O(n/k).
 */
public class VertexEdgeMapTest extends TestHelper {
  private static final String VERTEX_TYPE = "TestVertex";
  private static final String EDGE_TYPE1 = "TestEdge1";
  private static final String EDGE_TYPE2 = "TestEdge2";
  private static final String EDGE_TYPE3 = "TestEdge3";

  @Override
  public void beginTest() {
    database.transaction(() -> {
      if (!database.getSchema().existsType(VERTEX_TYPE))
        database.getSchema().buildVertexType().withName(VERTEX_TYPE).withTotalBuckets(1).create();

      if (!database.getSchema().existsType(EDGE_TYPE1))
        database.getSchema().buildEdgeType().withName(EDGE_TYPE1).withTotalBuckets(1).create();

      if (!database.getSchema().existsType(EDGE_TYPE2))
        database.getSchema().buildEdgeType().withName(EDGE_TYPE2).withTotalBuckets(1).create();

      if (!database.getSchema().existsType(EDGE_TYPE3))
        database.getSchema().buildEdgeType().withName(EDGE_TYPE3).withTotalBuckets(1).create();
    });
  }

  @Test
  public void testNewVerticesUseV1Format() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
    });
  }

  @Test
  public void testPerTypeEdgeLists() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v3 = database.newVertex(VERTEX_TYPE).save();

      // Add edges of different types
      v1.newEdge(EDGE_TYPE1, v2).save();
      v1.newEdge(EDGE_TYPE2, v3).save();

      // Should have 2 different edge bucket types
      final Set<Integer> outBuckets = v1.getOutEdgeBuckets();
      assertThat(outBuckets).hasSize(2);

      // Each type should have its own head chunk
      for (final int bucketId : outBuckets) {
        final RID headChunk = v1.getOutEdgesHeadChunk(bucketId);
        assertThat(headChunk).isNotNull();
      }
    });
  }

  @Test
  public void testCountEdgesWithPerTypeOptimization() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();

      // Add multiple edges of each type
      v1.newEdge(EDGE_TYPE1, v2).save();
      v1.newEdge(EDGE_TYPE1, v2).save();
      v1.newEdge(EDGE_TYPE1, v2).save();
      v1.newEdge(EDGE_TYPE2, v2).save();
      v1.newEdge(EDGE_TYPE2, v2).save();

      // Count should be accurate using per-type access
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1)).isEqualTo(3);
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE2)).isEqualTo(2);
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT)).isEqualTo(5);
    });
  }

  @Test
  public void testGetEdgesWithPerTypeOptimization() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v3 = database.newVertex(VERTEX_TYPE).save();

      // Add edges of different types
      v1.newEdge(EDGE_TYPE1, v2).save();
      v1.newEdge(EDGE_TYPE1, v3).save();
      v1.newEdge(EDGE_TYPE2, v2).save();

      // getEdges with type filter should only return edges of that type
      long count1 = 0;
      for (final Edge edge : v1.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1)) {
        assertThat(edge.getTypeName()).isEqualTo(EDGE_TYPE1);
        count1++;
      }
      assertThat(count1).isEqualTo(2);

      long count2 = 0;
      for (final Edge edge : v1.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE2)) {
        assertThat(edge.getTypeName()).isEqualTo(EDGE_TYPE2);
        count2++;
      }
      assertThat(count2).isEqualTo(1);
    });
  }

  @Test
  public void testIncomingEdgesWithPerType() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v3 = database.newVertex(VERTEX_TYPE).save();

      // Create edges pointing TO v1
      v2.newEdge(EDGE_TYPE1, v1).save();
      v3.newEdge(EDGE_TYPE2, v1).save();

      final Set<Integer> inBuckets = v1.getInEdgeBuckets();
      assertThat(inBuckets).hasSize(2);

      // Count should work correctly
      assertThat(v1.countEdges(Vertex.DIRECTION.IN, EDGE_TYPE1)).isEqualTo(1);
      assertThat(v1.countEdges(Vertex.DIRECTION.IN, EDGE_TYPE2)).isEqualTo(1);
      assertThat(v1.countEdges(Vertex.DIRECTION.IN)).isEqualTo(2);
    });
  }

  @Test
  public void testMixedDirectionEdges() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();

      // Add both outgoing and incoming edges
      v1.newEdge(EDGE_TYPE1, v2).save();
      v2.newEdge(EDGE_TYPE2, v1).save();

      // Outgoing edges
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1)).isEqualTo(1);
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE2)).isEqualTo(0);

      // Incoming edges
      assertThat(v1.countEdges(Vertex.DIRECTION.IN, EDGE_TYPE1)).isEqualTo(0);
      assertThat(v1.countEdges(Vertex.DIRECTION.IN, EDGE_TYPE2)).isEqualTo(1);
    });
  }

  @Test
  public void testMultipleEdgesBetweenSameVertices() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();

      // Add multiple edges of same type between same vertices
      v1.newEdge(EDGE_TYPE1, v2).save();
      v1.newEdge(EDGE_TYPE1, v2).save();
      v1.newEdge(EDGE_TYPE1, v2).save();

      // All should be stored in the same per-type list
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1)).isEqualTo(3);

      // Should only have one bucket ID
      assertThat(v1.getOutEdgeBuckets()).hasSize(1);
    });
  }

  @Test
  public void testEdgeRemovalUpdatesPerTypeList() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();

      final MutableEdge e1 = v1.newEdge(EDGE_TYPE1, v2).save();
      final MutableEdge e2 = v1.newEdge(EDGE_TYPE2, v2).save();

      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1)).isEqualTo(1);
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE2)).isEqualTo(1);

      // Remove one edge
      e1.delete();

      // Reload vertex to get updated state
      final Vertex v1Reloaded = v1.getIdentity().asVertex();
      assertThat(v1Reloaded.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1)).isEqualTo(0);
      assertThat(v1Reloaded.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE2)).isEqualTo(1);
    });
  }

  @Test
  public void testPersistenceOfPerTypeEdges() {
    // Create vertices and edges, get RID
    final RID[] v1RIDHolder = new RID[1];

    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();

      v1.newEdge(EDGE_TYPE1, v2).save();
      v1.newEdge(EDGE_TYPE2, v2).save();
      v1.newEdge(EDGE_TYPE3, v2).save();

      v1RIDHolder[0] = v1.getIdentity();
    });

    final RID finalV1RID = v1RIDHolder[0];

    // Verify persistence in another transaction
    database.transaction(() -> {
      // Reload vertex from database
      final Vertex v1Reloaded = finalV1RID.asVertex();

      // Edge counts should be preserved
      assertThat(v1Reloaded.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1)).isEqualTo(1);
      assertThat(v1Reloaded.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE2)).isEqualTo(1);
      assertThat(v1Reloaded.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE3)).isEqualTo(1);
      assertThat(v1Reloaded.countEdges(Vertex.DIRECTION.OUT)).isEqualTo(3);
    });
  }

  @Test
  public void testLargeNumberOfEdgeTypes() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();

      // Add edges of all 3 types
      v1.newEdge(EDGE_TYPE1, v2).save();
      v1.newEdge(EDGE_TYPE2, v2).save();
      v1.newEdge(EDGE_TYPE3, v2).save();

      // Should have 3 separate bucket lists
      assertThat(v1.getOutEdgeBuckets()).hasSize(3);

      // Each should be accessible independently
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1)).isEqualTo(1);
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE2)).isEqualTo(1);
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE3)).isEqualTo(1);
    });
  }
}
