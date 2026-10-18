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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10268: {@link GraphEngine#newEdges} wired the incoming half of every record-backed edge from its {@code bidirectional}
 * argument instead of from the edge type. A unidirectional type got an IN entry the type says cannot exist, and a bidirectional
 * type got a half-wired edge. The direction belongs to the type, as on {@link GraphEngine#newEdge} and the other flag-taking APIs:
 * a unidirectional type ignores {@code bidirectional=true}, and a bidirectional type refuses {@code bidirectional=false}.
 */
class Issue10268NewEdgesDirectionFromTypeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("V").create();
      database.getSchema().buildEdgeType().withName("B").create();
      database.getSchema().buildEdgeType().withName("U").withBidirectional(false).create();
      database.getSchema().buildEdgeType().withName("UL").withBidirectional(false).withLightweight(true).create();
      database.getSchema().buildEdgeType().withName("L").withLightweight(true).create();
    });
  }

  /** The first row of the reproduction in #10136: an IN entry on a unidirectional type. */
  @Test
  void aUnidirectionalTypeGetsNoIncomingSideFromABidirectionalBatch() {
    final RID[] v = newVertices(2);

    final List<Edge> created = newEdges(v[0], true, new GraphEngine.CreateEdgeOperation("U", v[1], new Object[0]));

    assertThat(created).hasSize(1);
    assertThat(database.countType("U", true)).isEqualTo(1L);
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "U")).isEqualTo(1L);
    assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "U")).isZero();
  }

  /** The second row of the reproduction in #10136: a half-wired edge on a bidirectional type is refused, not stored. */
  @Test
  void aBidirectionalTypeRefusesAUnidirectionalBatch() {
    final RID[] v = newVertices(2);

    assertThatThrownBy(() -> newEdges(v[0], false, new GraphEngine.CreateEdgeOperation("B", v[1], new Object[0])))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(GraphEngine.unidirectionalEdgeOnBidirectionalTypeMessage("B"));

    assertThat(database.countType("B", true)).isZero();
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "B")).isZero();
    assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "B")).isZero();
  }

  /**
   * The refusal comes before anything is stored: a connection on a unidirectional type earlier in the batch is not created
   * either, even when the caller's own transaction catches the exception and commits.
   */
  @Test
  void theRefusalStoresNothingFromTheRestOfTheBatch() {
    final RID[] v = newVertices(3);

    database.transaction(() -> assertThatThrownBy(() -> graphEngine().newEdges((VertexInternal) v[0].asVertex(),
        List.of(new GraphEngine.CreateEdgeOperation("U", v[1], new Object[0]),
            new GraphEngine.CreateEdgeOperation("B", v[2], new Object[0])), false))
        .isInstanceOf(IllegalArgumentException.class));

    assertThat(database.countType("U", true)).isZero();
    assertThat(database.countType("B", true)).isZero();
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT)).isZero();
  }

  @Test
  void aMixedBatchWiresTheIncomingSideOfEachTypeAsTheTypeSays() {
    final RID[] v = newVertices(4);

    final List<Edge> created = newEdges(v[0], true,
        new GraphEngine.CreateEdgeOperation("B", v[1], new Object[] { "weight", 1 }),
        new GraphEngine.CreateEdgeOperation("U", v[2], new Object[] { "weight", 2 }),
        new GraphEngine.CreateEdgeOperation("B", v[3], new Object[0]));

    assertThat(created).hasSize(3);
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT)).isEqualTo(3L);
    assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "B")).isEqualTo(1L);
    assertThat(v[2].asVertex().countEdges(Vertex.DIRECTION.IN, "U")).isZero();
    assertThat(v[3].asVertex().countEdges(Vertex.DIRECTION.IN, "B")).isEqualTo(1L);

    // THE IN ENTRY NAMES THE EDGE AND THE SOURCE VERTEX, IN THAT ORDER
    final Edge incoming = v[1].asVertex().getEdges(Vertex.DIRECTION.IN, "B").iterator().next();
    assertThat(incoming.getIdentity()).isEqualTo(created.get(0).getIdentity());
    assertThat(incoming.getOut()).isEqualTo(v[0]);
  }

  /** A unidirectional batch is what a unidirectional type is: accepted, for record-backed and lightweight types alike. */
  @Test
  void aUnidirectionalBatchOnUnidirectionalTypesIsAccepted() {
    final RID[] v = newVertices(3);

    final List<Edge> created = newEdges(v[0], false,
        new GraphEngine.CreateEdgeOperation("U", v[1], new Object[0]),
        new GraphEngine.CreateEdgeOperation("UL", v[2], new Object[0]));

    assertThat(created).hasSize(2);
    assertThat(created.get(1)).isInstanceOf(LightEdge.class);
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT)).isEqualTo(2L);
    assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "U")).isZero();
    assertThat(v[2].asVertex().countEdges(Vertex.DIRECTION.IN, "UL")).isZero();
  }

  /** The type check runs on a lightweight type too, where the single-edge path would otherwise ignore the flag. */
  @Test
  void aBidirectionalLightweightTypeRefusesAUnidirectionalBatch() {
    final RID[] v = newVertices(2);

    assertThatThrownBy(() -> newEdges(v[0], false, new GraphEngine.CreateEdgeOperation("L", v[1], new Object[0])))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(GraphEngine.unidirectionalEdgeOnBidirectionalTypeMessage("L"));

    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "L")).isZero();
  }

  private GraphEngine graphEngine() {
    return ((DatabaseInternal) database).getGraphEngine();
  }

  private RID[] newVertices(final int count) {
    final RID[] rids = new RID[count];
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        rids[i] = database.newVertex("V").save().getIdentity();
    });
    return rids;
  }

  private List<Edge> newEdges(final RID source, final boolean bidirectional, final GraphEngine.CreateEdgeOperation... operations) {
    final List<Edge> created = new ArrayList<>();
    database.transaction(
        () -> created.addAll(graphEngine().newEdges((VertexInternal) source.asVertex(), List.of(operations), bidirectional)));
    return created;
  }
}
