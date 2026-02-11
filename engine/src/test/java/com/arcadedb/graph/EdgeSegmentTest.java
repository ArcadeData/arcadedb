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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for EdgeSegment v1 format with count caching optimization.
 *
 * Verifies the optimization works correctly through the public Vertex API.
 * The count caching is transparent to users but improves performance.
 */
public class EdgeSegmentTest extends TestHelper {
  private static final String VERTEX_TYPE = "TestVertex";
  private static final String EDGE_TYPE1 = "TestEdge1";
  private static final String EDGE_TYPE2 = "TestEdge2";

  @Override
  public void beginTest() {
    database.transaction(() -> {
      if (!database.getSchema().existsType(VERTEX_TYPE))
        database.getSchema().buildVertexType().withName(VERTEX_TYPE).withTotalBuckets(1).create();

      if (!database.getSchema().existsType(EDGE_TYPE1))
        database.getSchema().buildEdgeType().withName(EDGE_TYPE1).withTotalBuckets(1).create();

      if (!database.getSchema().existsType(EDGE_TYPE2))
        database.getSchema().buildEdgeType().withName(EDGE_TYPE2).withTotalBuckets(1).create();
    });
  }

  @Test
  public void testCountCachingWithMultipleEdges() {
    database.transaction(() -> {
      // Create vertices
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v3 = database.newVertex(VERTEX_TYPE).save();

      // Add some edges
      v1.newEdge(EDGE_TYPE1, v2).save();
      v1.newEdge(EDGE_TYPE1, v3).save();

      // Count should be 2
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, null)).isEqualTo(2);
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1)).isEqualTo(2);

      // Add more edges of different type
      v1.newEdge(EDGE_TYPE2, v2).save();

      // Count should be 3 total, 2 for TYPE1, 1 for TYPE2
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, null)).isEqualTo(3);
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1)).isEqualTo(2);
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE2)).isEqualTo(1);
    });
  }

  @Test
  public void testCountUpdatesOnEdgeRemoval() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v3 = database.newVertex(VERTEX_TYPE).save();

      final MutableEdge e1 = v1.newEdge(EDGE_TYPE1, v2).save();
      final MutableEdge e2 = v1.newEdge(EDGE_TYPE1, v3).save();

      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, null)).isEqualTo(2);

      // Remove one edge
      e1.delete();

      // Reload vertex to get updated count
      final Vertex v1Reloaded = v1.getIdentity().asVertex();
      assertThat(v1Reloaded.countEdges(Vertex.DIRECTION.OUT, null)).isEqualTo(1);

      // Remove another edge
      e2.delete();

      final Vertex v1Reloaded2 = v1.getIdentity().asVertex();
      assertThat(v1Reloaded2.countEdges(Vertex.DIRECTION.OUT, null)).isEqualTo(0);
    });
  }

  @Test
  public void testCountWithMultipleEdges() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();

      // Create many vertices to connect to (this may create multiple segments)
      final int edgeCount = 50;
      for (int i = 0; i < edgeCount; i++) {
        final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();
        v1.newEdge(EDGE_TYPE1, v2).save();
      }

      // Verify count is accurate even across multiple segments
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, null)).isEqualTo(edgeCount);
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1)).isEqualTo(edgeCount);
    });
  }

  @Test
  public void testIncomingEdgeCounts() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v3 = database.newVertex(VERTEX_TYPE).save();

      // Create edges pointing TO v1
      v2.newEdge(EDGE_TYPE1, v1).save();
      v3.newEdge(EDGE_TYPE1, v1).save();

      // v1 should have 2 incoming edges
      assertThat(v1.countEdges(Vertex.DIRECTION.IN, null)).isEqualTo(2);
      assertThat(v1.countEdges(Vertex.DIRECTION.IN, EDGE_TYPE1)).isEqualTo(2);

      // v1 should have 0 outgoing edges
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, null)).isEqualTo(0);
    });
  }

  @Test
  public void testMixedEdgeTypesCounting() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();

      // Create 3 edges of type1 and 2 edges of type2
      v1.newEdge(EDGE_TYPE1, v2).save();
      v1.newEdge(EDGE_TYPE1, v2).save();
      v1.newEdge(EDGE_TYPE1, v2).save();
      v1.newEdge(EDGE_TYPE2, v2).save();
      v1.newEdge(EDGE_TYPE2, v2).save();

      // Total should be 5
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, null)).isEqualTo(5);

      // Type-specific counts
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1)).isEqualTo(3);
      assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE2)).isEqualTo(2);
    });
  }

  @Test
  public void testCachedCountAfterReload() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();

      v1.newEdge(EDGE_TYPE1, v2).save();
      v1.newEdge(EDGE_TYPE1, v2).save();

      // Save the RID
      final RID v1RID = v1.getIdentity();

      // Commit to persist
      database.commit();
      database.begin();

      // Reload the vertex from database
      final Vertex v1Reloaded = v1RID.asVertex();

      // Count should still be 2 after reload
      assertThat(v1Reloaded.countEdges(Vertex.DIRECTION.OUT)).isEqualTo(2);
      assertThat(v1Reloaded.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1)).isEqualTo(2);
    });
  }
}
