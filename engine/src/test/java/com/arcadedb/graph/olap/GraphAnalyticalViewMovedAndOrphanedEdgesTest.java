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

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two ways a {@code SYNCHRONOUS} Graph Analytical View's overlay kept an edge the graph no longer had, found while working
 * on issue #9572:
 * <ul>
 *   <li>re-pointing an edge ({@code SET @in} / {@code @out}) replaces its record: the replacement was reported as created,
 *   the record it replaced was never reported as gone, so the view held both;</li>
 *   <li>the edges of a vertex the overlay itself added, deleted with that vertex, were dropped from the delta because the
 *   vertex was already gone when they were resolved: they stayed in the added index and in the degree of the vertex at
 *   their other end.</li>
 * </ul>
 * Every count is checked against the edge lists, read through the vertices and never through the view.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphAnalyticalViewMovedAndOrphanedEdgesTest extends TestHelper {

  private RID a;
  private RID b;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
    database.transaction(() -> {
      final MutableVertex va = database.newVertex("P").save();
      final MutableVertex vb = database.newVertex("P").save();
      va.newEdge("K", vb);
      a = va.getIdentity();
      b = vb.getIdentity();
    });
  }

  @Test
  void anEdgeMovedToAnotherVertexLeavesTheOldOneBehind() {
    final GraphAnalyticalView view = syncView("move");
    try {
      final RID c = newVertex();
      database.transaction(() -> {
        for (final Edge e : a.asVertex().getEdges(Vertex.DIRECTION.OUT, "K"))
          e.modify().set("@in", c).save();
      });
      assertThat(c.asVertex().countEdges(Vertex.DIRECTION.IN, "K")).isEqualTo(1);
      assertCounts(view);

      database.transaction(() -> {
        for (final Edge e : c.asVertex().getEdges(Vertex.DIRECTION.IN, "K"))
          e.modify().set("@out", b).save();
      });
      assertCounts(view);
    } finally {
      view.drop();
    }
  }

  @Test
  void theEdgesOfAnAddedVertexGoWithIt() {
    theEdgesOfAnAddedVertexGoWithIt(false);
  }

  /** The same with lightweight edges, which reach the overlay since #9572. */
  @Test
  void theLightweightEdgesOfAnAddedVertexGoWithIt() {
    theEdgesOfAnAddedVertexGoWithIt(true);
  }

  private void theEdgesOfAnAddedVertexGoWithIt(final boolean lightweight) {
    final GraphAnalyticalView view = syncView("vertex-delete");
    try {
      final RID[] c = new RID[1];
      database.transaction(() -> {
        final MutableVertex vc = database.newVertex("P").save();
        if (lightweight) {
          vc.newLightEdge("K", a);
          a.asVertex().newLightEdge("K", vc);
        } else {
          vc.newEdge("K", a);
          a.asVertex().newEdge("K", vc);
        }
        c[0] = vc.getIdentity();
      });
      assertCounts(view);

      // An overflow vertex: the overlay added it, and with it both its edges
      database.transaction(() -> c[0].asVertex().delete());
      assertCounts(view);

      // A base vertex, for contrast: its node is masked and its edges' deletions are budgeted against the base
      database.transaction(() -> b.asVertex().delete());
      assertCounts(view);
    } finally {
      view.drop();
    }
  }

  private GraphAnalyticalView syncView(final String name) {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName(name).withVertexTypes("P")
        .withEdgeTypes("K").withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).build();
    assertThat(view.isReady()).isTrue();
    return view;
  }

  private RID newVertex() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newVertex("P").save().getIdentity());
    return rid[0];
  }

  /** The counts the view and the queries it serves answer, against the edge lists read without it. */
  private void assertCounts(final GraphAnalyticalView view) {
    long expected = 0;
    for (final Vertex v : vertices()) {
      final int nodeId = view.getNodeId(v.getIdentity());
      assertThat(nodeId).as("node of %s", v.getIdentity()).isGreaterThanOrEqualTo(0);
      final long out = v.countEdges(Vertex.DIRECTION.OUT, "K");
      assertThat(view.countEdges(nodeId, Vertex.DIRECTION.OUT, "K")).as("OUT degree of %s", v.getIdentity()).isEqualTo(out);
      assertThat(view.countEdges(nodeId, Vertex.DIRECTION.IN, "K")).as("IN degree of %s", v.getIdentity())
          .isEqualTo(v.countEdges(Vertex.DIRECTION.IN, "K"));
      expected += out;
    }
    assertThat(cypherCount("MATCH (x:P)-[:K]->(y:P) RETURN count(*) AS n")).as("count push-down").isEqualTo(expected);
    assertThat(cypherCount("MATCH (x:P)-[:K]->(y:P) WITH * RETURN count(*) AS n")).as("row pipeline").isEqualTo(expected);
  }

  private List<Vertex> vertices() {
    final List<Vertex> result = new ArrayList<>();
    database.iterateType("P", true).forEachRemaining(r -> result.add(r.asVertex()));
    return result;
  }

  private long cypherCount(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
