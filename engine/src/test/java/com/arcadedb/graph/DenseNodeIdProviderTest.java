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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit coverage for {@link DenseNodeIdProvider}, the renumbering wrapper issue #6792 puts in front of a provider
 * whose node ID space has holes in it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DenseNodeIdProviderTest {
  private Database            database;
  private GraphAnalyticalView view;
  private RID                 a;
  private RID                 b;
  private RID                 fresh;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-dense-node-id-provider");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("E");

    final MutableVertex[] created = new MutableVertex[3];
    database.transaction(() -> {
      created[0] = database.newVertex("N").set("name", "SPARE").save();
      created[1] = database.newVertex("N").set("name", "A").save();
      created[2] = database.newVertex("N").set("name", "B").save();
      created[1].newEdge("E", created[2]).save();
    });
    a = created[1].getIdentity();
    b = created[2].getIdentity();

    view = GraphAnalyticalView.builder(database)
        .withName("dense-node-id-provider")
        .withVertexTypes("N")
        .withEdgeTypes("E")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .withCompactionThreshold(Integer.MAX_VALUE)
        .build();
  }

  @AfterEach
  void teardown() {
    if (view != null)
      view.drop();
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  @Test
  void aCompactProviderIsHandedBackUnwrapped() {
    // Nothing pending, so the view's IDs are already 0..getNodeCount(): wrapping would only cost a translation.
    assertThat(DenseNodeIdProvider.wrap(view)).isSameAs(view);
    assertThat(DenseNodeIdProvider.wrap(null)).isNull();
  }

  @Test
  void aHoleInTheIdSpaceIsRenumberedAway() {
    deleteSpareAndAddFresh();

    assertThat(view.getNodeCount()).isNotEqualTo(view.getNodeIdUpperBound());
    final GraphTraversalProvider dense = DenseNodeIdProvider.wrap(view);
    assertThat(dense).isNotSameAs(view);

    assertThat(dense.getNodeCount()).isEqualTo(3);
    assertThat(dense.getNodeIdUpperBound()).isEqualTo(3);

    final Set<RID> resolved = new HashSet<>();
    for (int id = 0; id < dense.getNodeCount(); id++) {
      assertThat(dense.isNodeLive(id)).isTrue();
      final RID rid = dense.getRID(id);
      assertThat(rid).isNotNull();
      resolved.add(rid);
      // Round trip: the dense id of the RID this id resolves to is this id.
      assertThat(dense.getNodeId(rid)).isEqualTo(id);
    }
    assertThat(resolved).containsExactlyInAnyOrder(a, b, fresh);
    assertThat(dense.isNodeLive(3)).isFalse();
    assertThat(dense.isNodeLive(-1)).isFalse();
  }

  @Test
  void neighbourIdsAreTranslatedIntoTheDenseSpace() {
    deleteSpareAndAddFresh();
    final GraphTraversalProvider dense = DenseNodeIdProvider.wrap(view);

    final int denseA = dense.getNodeId(a);
    final int denseB = dense.getNodeId(b);
    final int denseFresh = dense.getNodeId(fresh);

    assertThat(dense.getNeighborIds(denseA, Vertex.DIRECTION.OUT, "E")).containsExactly(denseB);
    assertThat(dense.getNeighborIds(denseB, Vertex.DIRECTION.OUT, "E")).containsExactly(denseFresh);
    assertThat(dense.getNeighborIds(denseFresh, Vertex.DIRECTION.OUT, "E")).isEmpty();
    assertThat(dense.getNeighborIds(denseFresh, Vertex.DIRECTION.IN, "E")).containsExactly(denseB);

    assertThat(dense.isConnectedTo(denseB, denseFresh, Vertex.DIRECTION.OUT, "E")).isTrue();
    assertThat(dense.isConnectedTo(denseA, denseFresh, Vertex.DIRECTION.OUT, "E")).isFalse();
    assertThat(dense.countEdges(denseB, Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
    assertThat(dense.countEdgesBetween(denseB, denseFresh, Vertex.DIRECTION.OUT, "E")).isEqualTo(1);

    final int[] degrees = new int[dense.getNodeCount()];
    dense.getDegrees(degrees, Vertex.DIRECTION.OUT, "E");
    assertThat(degrees[denseA]).isEqualTo(1);
    assertThat(degrees[denseB]).isEqualTo(1);
    assertThat(degrees[denseFresh]).isZero();
  }

  @Test
  void theWrapperRefusesThePackedViewAndKeepsTheDelegatesOwnAnswers() {
    deleteSpareAndAddFresh();
    final GraphTraversalProvider dense = DenseNodeIdProvider.wrap(view);

    // A packed NeighborView is the delegate's own CSR arrays, addressed by the delegate's own ids: it cannot be
    // renumbered without copying the graph, so it is refused and the caller uses the per-node lookups.
    assertThat(dense.getNeighborView(Vertex.DIRECTION.OUT, "E")).isNull();

    // Renumbering says nothing about the freshness of those arrays, so the delegate's answer is passed through.
    assertThat(dense.hasPendingChanges()).isTrue();
    assertThat(dense.isReady()).isEqualTo(view.isReady());
    assertThat(dense.getName()).isEqualTo(view.getName());
    assertThat(dense.coversVertexType("N")).isTrue();
    assertThat(dense.coversEdgeType("E")).isTrue();
  }

  private void deleteSpareAndAddFresh() {
    final MutableVertex[] added = new MutableVertex[1];
    database.transaction(() -> {
      database.query("sql", "SELECT FROM N WHERE name = 'SPARE'").next().getRecord().get().asVertex().delete();
      added[0] = database.newVertex("N").set("name", "FRESH").save();
      b.asVertex().modify().newEdge("E", added[0]).save();
    });
    fresh = added[0].getIdentity();
  }
}
