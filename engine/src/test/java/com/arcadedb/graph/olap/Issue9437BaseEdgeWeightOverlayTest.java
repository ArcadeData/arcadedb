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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.NodeEdgeWeights;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A committed change to the weight of an edge already in a view's base CSR (issue #9437). When the edge is the only one
 * of its type between its two vertices, its pair names its column slot, so the new value is served from the overlay
 * at once and the view does not rebuild its columns - which is what lets a contraction hierarchy follow a traffic update
 * in milliseconds. A pair joined by parallel edges is still ambiguous and still rebuilds, as before.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9437BaseEdgeWeightOverlayTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-9437-base-edge-weight-overlay");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("ROAD").createProperty("w", Type.DOUBLE);
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
      database = null;
    }
  }

  @Test
  void aSoleEdgeWeightIsServedFromTheOverlayWithoutARebuild() {
    graph(false);
    final GraphAnalyticalView view = syncView();
    try {
      final long built = view.getBuildTimestamp();
      setWeight("A", "B", 77.0);
      setWeight("A", "B", 78.0); // a second update of the same edge replaces the first

      assertThat(view.hasEdgeProperty("ROAD", "w")).as("the columns are not out of date").isTrue();
      assertThat(view.hasPendingChanges()).isTrue();
      final int a = view.getNodeId(vertex("A").getIdentity());
      final int b = view.getNodeId(vertex("B").getIdentity());
      assertThat(weightsByName(view, a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrderEntriesOf(
          Map.of("B", 78.0, "C", 20.0));
      assertThat(weightsByName(view, b, Vertex.DIRECTION.IN)).containsExactlyInAnyOrderEntriesOf(Map.of("A", 78.0));

      // the weighted algorithms reading the view see it too
      assertThat(costTo("B")).isEqualTo(78.0);
      assertThat(view.getBuildTimestamp()).as("no rebuild was forced").isEqualTo(built);

      // a later deletion of the same edge still removes it, value and all
      deleteEdge("A", "B");
      assertThat(weightsByName(view, a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrderEntriesOf(Map.of("C", 20.0));
    } finally {
      view.drop();
    }
  }

  @Test
  void parallelEdgesStillRebuildTheColumns() {
    graph(true);
    final GraphAnalyticalView view = syncView();
    try {
      final double replaced = setWeight("A", "B", 77.0);
      // the pair A -> B has two edges: which slot changed cannot be told, so the columns are rebuilt and then serve it
      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      final long deadline = System.currentTimeMillis() + 30_000;
      while (!view.hasEdgeProperty("ROAD", "w") && System.currentTimeMillis() < deadline)
        Thread.yield();
      final int a = view.getNodeId(vertex("A").getIdentity());
      final NodeEdgeWeights edges = view.edgeWeightsForSlice(a, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null);
      assertThat(edges).isNotNull();
      assertThat(edges.weights()).contains(77.0);
      // the edge that kept its weight is the other one of the two
      assertThat(costTo("B")).isEqualTo(Math.min(77.0, 10.0 + 15.0 - replaced));
    } finally {
      view.drop();
    }
  }

  // ---------------------------------------------------------------------------------------------------------------

  /** A -> B at 10, A -> C at 20, plus a second A -> B at 15 when {@code parallel}. */
  private void graph(final boolean parallel) {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("name", "A").save();
      final MutableVertex b = database.newVertex("N").set("name", "B").save();
      final MutableVertex c = database.newVertex("N").set("name", "C").save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 10.0 }).save();
      a.newEdge("ROAD", c, true, new Object[] { "w", 20.0 }).save();
      if (parallel)
        a.newEdge("ROAD", b, true, new Object[] { "w", 15.0 }).save();
    });
  }

  private GraphAnalyticalView syncView() {
    return GraphAnalyticalView.builder(database).withName("roads").withVertexTypes("N").withEdgeTypes("ROAD")
        .withEdgeProperties("w").withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).build();
  }

  /** Sets the weight of the first edge found from {@code from} to {@code to}; returns the weight it replaced. */
  private double setWeight(final String from, final String to, final double weight) {
    final double[] replaced = new double[1];
    database.transaction(() -> {
      for (final Edge edge : vertex(from).getEdges(Vertex.DIRECTION.OUT, "ROAD"))
        if (to.equals(edge.getInVertex().get("name"))) {
          replaced[0] = ((Number) edge.get("w")).doubleValue();
          edge.modify().set("w", weight).save();
          return;
        }
    });
    return replaced[0];
  }

  private void deleteEdge(final String from, final String to) {
    database.transaction(() -> {
      for (final Edge edge : vertex(from).getEdges(Vertex.DIRECTION.OUT, "ROAD"))
        if (to.equals(edge.getInVertex().get("name"))) {
          edge.delete();
          return;
        }
    });
  }

  private double costTo(final String name) {
    try (final var rows = database.command("opencypher",
        "MATCH (a:N {name: 'A'}) CALL algo.dijkstra.singleSource(a, 'ROAD', 'w', 'OUT') YIELD node, cost "
            + "WITH node, cost WHERE node.name = $name RETURN cost", Map.of("name", name))) {
      return ((Number) rows.next().getProperty("cost")).doubleValue();
    }
  }

  private Map<String, Double> weightsByName(final GraphAnalyticalView view, final int nodeId, final Vertex.DIRECTION direction) {
    final NodeEdgeWeights edges = view.edgeWeightsForSlice(nodeId, direction, "ROAD", "w", -1.0, null);
    assertThat(edges).isNotNull();
    final Map<String, Double> byName = new HashMap<>();
    for (int i = 0; i < edges.neighbors().length; i++)
      byName.put((String) view.getRID(edges.neighbors()[i]).asVertex().get("name"), edges.weights()[i]);
    return byName;
  }

  private Vertex vertex(final String name) {
    return database.query("sql", "select from N where name = ?", name).next().getRecord().get().asVertex();
  }
}
