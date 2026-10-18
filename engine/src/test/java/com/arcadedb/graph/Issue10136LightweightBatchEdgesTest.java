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
import com.arcadedb.exception.DuplicatedKeyException;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10136: {@link GraphEngine#newEdges} built a record-backed {@link MutableEdge} for every connection, so on a
 * LIGHTWEIGHT edge type it stored a record in the type's bucket and accepted properties, where the single-edge path
 * creates a light edge and refuses them. The storage shape of an edge is a property of its type, not of the call.
 */
class Issue10136LightweightBatchEdgesTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("V").create();
      database.getSchema().buildEdgeType().withName("L").withLightweight(true).create();
      database.getSchema().buildEdgeType().withName("UL").withBidirectional(false).withLightweight(true).create();
      database.getSchema().buildEdgeType().withName("E").create();
      database.getSchema().buildEdgeType().withName("U").withLightweight(true).withUnique(true).create();
    });
  }

  @Test
  void aBatchOfLightweightEdgesStoresNoRecord() {
    final RID[] v = newVertices(3);

    final List<Edge> created = newEdges(v[0],
        new GraphEngine.CreateEdgeOperation("L", v[1], new Object[0]),
        new GraphEngine.CreateEdgeOperation("L", v[2], new Object[0]));

    assertThat(created).hasSize(2);
    assertThat(created).allMatch(edge -> edge instanceof LightEdge);
    assertThat(database.countType("L", true)).isZero();
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "L")).isEqualTo(2L);
    assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "L")).isEqualTo(1L);
    assertThat(v[2].asVertex().countEdges(Vertex.DIRECTION.IN, "L")).isEqualTo(1L);
  }

  /** The incoming side follows the type, not the {@code bidirectional} argument of the batch, as on the single-edge path. */
  @Test
  void aBatchOfUnidirectionalLightweightEdgesDoesNotWireIncoming() {
    final RID[] v = newVertices(2);

    final List<Edge> created = newEdges(v[0], new GraphEngine.CreateEdgeOperation("UL", v[1], new Object[0]));

    assertThat(created).hasSize(1);
    assertThat(created).allMatch(edge -> edge instanceof LightEdge);
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "UL")).isEqualTo(1L);
    assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "UL")).isZero();
  }

  @Test
  void aBatchAgreesWithTheSingleEdgePath() {
    final RID[] v = newVertices(3);
    database.transaction(() -> v[0].asVertex().newEdge("L", v[1]));

    newEdges(v[0], new GraphEngine.CreateEdgeOperation("L", v[2], new Object[0]));

    assertThat(database.countType("L", true)).isZero();
    final List<Edge> edges = new ArrayList<>();
    v[0].asVertex().getEdges(Vertex.DIRECTION.OUT, "L").forEach(edges::add);
    assertThat(edges).hasSize(2);
    assertThat(edges).allMatch(edge -> edge instanceof LightEdge);
  }

  @Test
  void aLightweightEdgeInABatchRefusesProperties() {
    final RID[] v = newVertices(2);

    assertThatThrownBy(() -> newEdges(v[0], new GraphEngine.CreateEdgeOperation("L", v[1], new Object[] { "weight", 1 })))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("LIGHTWEIGHT");

    assertThat(database.countType("L", true)).isZero();
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "L")).isZero();
  }

  @Test
  void aMixedBatchKeepsTheShapeOfEachType() {
    final RID[] v = newVertices(3);

    final List<Edge> created = newEdges(v[0],
        new GraphEngine.CreateEdgeOperation("E", v[1], new Object[] { "weight", 1 }),
        new GraphEngine.CreateEdgeOperation("L", v[2], new Object[0]));

    assertThat(created).hasSize(2);
    assertThat(database.countType("E", true)).isEqualTo(1L);
    assertThat(database.countType("L", true)).isZero();
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1L);
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "L")).isEqualTo(1L);
    assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1L);
    assertThat(v[2].asVertex().countEdges(Vertex.DIRECTION.IN, "L")).isEqualTo(1L);
  }

  /**
   * The first light edge of a batch creates the source's edge list on the transaction's copy of the vertex: the uniqueness
   * check of the next connection has to see it, or a unique type accepts the same pair twice in one batch.
   */
  @Test
  void aUniqueLightweightTypeRefusesTheSamePairTwiceInOneBatch() {
    final RID[] v = newVertices(2);

    assertThatThrownBy(() -> newEdges(v[0],
        new GraphEngine.CreateEdgeOperation("U", v[1], new Object[0]),
        new GraphEngine.CreateEdgeOperation("U", v[1], new Object[0])))
        .isInstanceOf(DuplicatedKeyException.class);

    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "U")).isZero();
    assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "U")).isZero();
  }

  private RID[] newVertices(final int count) {
    final RID[] rids = new RID[count];
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        rids[i] = database.newVertex("V").save().getIdentity();
    });
    return rids;
  }

  private List<Edge> newEdges(final RID source, final GraphEngine.CreateEdgeOperation... operations) {
    final List<Edge> created = new ArrayList<>();
    database.transaction(() -> created.addAll(((DatabaseInternal) database).getGraphEngine()
        .newEdges((VertexInternal) source.asVertex(), List.of(operations), true)));
    return created;
  }
}
