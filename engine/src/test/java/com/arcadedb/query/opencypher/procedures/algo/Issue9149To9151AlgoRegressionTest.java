/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression tests for #9149 (algo.astar weight and path), #9150 (modularity, conductance and louvain modularity) and #9151
 * (algo.graphSummary degrees). The expected numbers are the textbook formulas (networkx gives the same values).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9149To9151AlgoRegressionTest extends TestHelper {
  // two K4 cliques {0..3} and {4..7}, a triangle {8,9,10}, and the bridges 3-4 and 7-8
  private static final int[]   COMM    = { 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2 };
  private static final int[][] CLIQUES = { { 0, 1, 2, 3 }, { 4, 5, 6, 7 }, { 8, 9, 10 } };

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("ROAD");
  }

  private void buildCommunityGraph(final boolean reversed) {
    database.transaction(() -> {
      final MutableVertex[] v = new MutableVertex[COMM.length];
      for (int i = 0; i < v.length; i++)
        v[i] = database.newVertex("Person").set("id", i).set("community", COMM[i]).save();
      final List<int[]> edges = new ArrayList<>();
      for (final int[] g : CLIQUES)
        for (int i = 0; i < g.length; i++)
          for (int j = i + 1; j < g.length; j++)
            edges.add(new int[] { g[i], g[j] });
      edges.add(new int[] { 3, 4 });
      edges.add(new int[] { 7, 8 });
      for (final int[] e : edges) {
        if (reversed)
          v[e[1]].newEdge("KNOWS", v[e[0]]).save();
        else
          v[e[0]].newEdge("KNOWS", v[e[1]]).save();
      }
    });
  }

  @Test
  void graphSummaryCountsBothDirections() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Person").set("name", "A").save();
      final MutableVertex b = database.newVertex("Person").set("name", "B").save();
      final MutableVertex c = database.newVertex("Person").set("name", "C").save();
      database.newVertex("Person").set("name", "D").save();
      a.newEdge("KNOWS", b).save();
      b.newEdge("KNOWS", c).save();
    });
    try (final ResultSet rs = database.query("opencypher", "CALL algo.graphSummary('KNOWS', 'Person') YIELD nodeCount, edgeCount, "
        + "avgDegree, maxDegree, minDegree, density, isolatedNodes, selfLoops RETURN *")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("nodeCount").longValue()).isEqualTo(4L);
      assertThat(r.<Number>getProperty("edgeCount").longValue()).isEqualTo(2L);
      assertThat(r.<Number>getProperty("isolatedNodes").longValue()).isEqualTo(1L);
      assertThat(r.<Number>getProperty("maxDegree").longValue()).isEqualTo(2L);
      assertThat(r.<Number>getProperty("minDegree").longValue()).isEqualTo(0L);
      assertThat(r.<Number>getProperty("avgDegree").doubleValue()).isCloseTo(1.0, within(1e-9));
      assertThat(r.<Number>getProperty("density").doubleValue()).isCloseTo(1.0 / 3, within(1e-9));
    }
  }

  @Test
  void modularityScoreMatchesFormulaAndIgnoresEdgeDirection() {
    buildCommunityGraph(false);
    assertThat(modularityScore()).isCloseTo(0.524221, within(1e-5));
    database.transaction(() -> database.command("sql", "DELETE FROM KNOWS"));
    database.transaction(() -> database.command("sql", "DELETE FROM Person"));
    buildCommunityGraph(true);
    assertThat(modularityScore()).isCloseTo(0.524221, within(1e-5));
  }

  private double modularityScore() {
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.modularityScore('community', 'KNOWS') YIELD modularity RETURN modularity")) {
      return rs.next().<Number>getProperty("modularity").doubleValue();
    }
  }

  @Test
  void conductanceCountsEachBoundaryEdgeOnce() {
    assertConductance(false);
  }

  @Test
  void conductanceIgnoresEdgeDirection() {
    assertConductance(true);
  }

  private void assertConductance(final boolean reversed) {
    buildCommunityGraph(reversed);
    final Map<Number, double[]> byCommunity = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.conductance('community', 'KNOWS') YIELD community, conductance, boundaryEdges RETURN *")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        byCommunity.put(r.<Number>getProperty("community"),
            new double[] { r.<Number>getProperty("conductance").doubleValue(), r.<Number>getProperty("boundaryEdges").doubleValue() });
      }
    }
    assertThat(byCommunity).hasSize(3);
    assertThat(byCommunity.get(0)[1]).isEqualTo(1.0);
    assertThat(byCommunity.get(1)[1]).isEqualTo(2.0);
    assertThat(byCommunity.get(2)[1]).isEqualTo(1.0);
    assertThat(byCommunity.get(0)[0]).isCloseTo(1.0 / 13, within(1e-9));
    assertThat(byCommunity.get(1)[0]).isCloseTo(1.0 / 7, within(1e-9));
    assertThat(byCommunity.get(2)[0]).isCloseTo(1.0 / 7, within(1e-9));
  }

  @Test
  void louvainModularityColumnMatchesModularityScoreOfItsPartition() {
    buildCommunityGraph(false);
    final Map<RID, Integer> partition = new HashMap<>();
    double reported = Double.NaN;
    try (final ResultSet rs = database.query("opencypher", "CALL algo.louvain() YIELD node, communityId, modularity RETURN *")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        partition.put(r.<RID>getProperty("node"), r.<Number>getProperty("communityId").intValue());
        reported = r.<Number>getProperty("modularity").doubleValue();
      }
    }
    database.transaction(() -> {
      for (final Map.Entry<RID, Integer> e : partition.entrySet())
        database.lookupByRID(e.getKey(), true).asVertex().modify().set("community", e.getValue()).save();
    });
    assertThat(reported).isCloseTo(modularityScore(), within(1e-9));
  }

  @Test
  void astarReturnsWeightRelationshipsAndDefaultsToBothDirections() {
    database.transaction(() -> {
      final MutableVertex rome = database.newVertex("City").set("name", "Rome", "lat", 41.9, "lon", 12.5).save();
      final MutableVertex milan = database.newVertex("City").set("name", "Milan", "lat", 45.5, "lon", 9.2).save();
      final MutableVertex paris = database.newVertex("City").set("name", "Paris", "lat", 48.9, "lon", 2.4).save();
      rome.newEdge("ROAD", milan, "km", 480.0).save();
      milan.newEdge("ROAD", paris, "km", 850.0).save();
    });
    final String m = "MATCH (src:City {name:'Rome'}), (dst:City {name:'Paris'}) ";
    for (final String call : new String[] { "algo.astar(src, dst, 'ROAD', 'km')", "algo.astar(src, dst, 'ROAD', 'km', 'lat', 'lon')",
        "algo.bellmanford(src, dst, 'ROAD', 'km')", "algo.dijkstra(src, dst, 'ROAD', 'km', 'OUT')" }) {
      try (final ResultSet rs = database.query("opencypher",
          m + "CALL " + call + " YIELD path, weight RETURN weight, size(path.nodes) AS nodes, size(path.relationships) AS rels")) {
        final Result r = rs.next();
        assertThat(r.<Number>getProperty("weight").doubleValue()).as(call).isEqualTo(1330.0);
        assertThat(r.<Number>getProperty("nodes").intValue()).as(call).isEqualTo(3);
        assertThat(r.<Number>getProperty("rels").intValue()).as(call).isEqualTo(2);
      }
    }
    // reverse direction: A* now follows both directions by default, like algo.dijkstra
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (src:City {name:'Paris'}), (dst:City {name:'Rome'}) CALL algo.astar(src, dst, 'ROAD', 'km') YIELD weight RETURN weight")) {
      assertThat(rs.next().<Number>getProperty("weight").doubleValue()).isEqualTo(1330.0);
    }
  }

  @Test
  void graphSummaryCountsSelfLoopTwiceInDegree() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Person").set("name", "A").save();
      a.newEdge("KNOWS", a).save();
      database.newVertex("Person").set("name", "B").save();
    });
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.graphSummary('KNOWS', 'Person') YIELD maxDegree, isolatedNodes, selfLoops, edgeCount RETURN *")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("maxDegree").longValue()).isEqualTo(2L);
      assertThat(r.<Number>getProperty("isolatedNodes").longValue()).isEqualTo(1L);
      assertThat(r.<Number>getProperty("selfLoops").longValue()).isEqualTo(1L);
      assertThat(r.<Number>getProperty("edgeCount").longValue()).isEqualTo(1L);
    }
  }

  @Test
  void pathWeightPicksTheLightestParallelEdge() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("City").set("name", "A").save();
      final MutableVertex b = database.newVertex("City").set("name", "B").save();
      a.newEdge("ROAD", b, "km", 90.0).save();
      a.newEdge("ROAD", b, "km", 10.0).save();
    });
    for (final String call : new String[] { "algo.astar(src, dst, 'ROAD', 'km')", "algo.bellmanford(src, dst, 'ROAD', 'km')",
        "algo.dijkstra(src, dst, 'ROAD', 'km')" }) {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (src:City {name:'A'}), (dst:City {name:'B'}) CALL " + call + " YIELD weight RETURN weight")) {
        assertThat(rs.next().<Number>getProperty("weight").doubleValue()).as(call).isEqualTo(10.0);
      }
    }
  }

  @Test
  void weightedModularityScoreAndLouvainAgree() {
    buildCommunityGraph(false);
    database.transaction(() -> database.command("sql", "UPDATE KNOWS SET w = 2"));
    final Map<RID, Integer> partition = new HashMap<>();
    double reported = Double.NaN;
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.louvain({weightProperty: 'w'}) YIELD node, communityId, modularity RETURN *")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        partition.put(r.<RID>getProperty("node"), r.<Number>getProperty("communityId").intValue());
        reported = r.<Number>getProperty("modularity").doubleValue();
      }
    }
    // a uniform weight leaves the modularity of any partition unchanged: compare with the unweighted score of it
    database.transaction(() -> {
      for (final Map.Entry<RID, Integer> e : partition.entrySet())
        database.lookupByRID(e.getKey(), true).asVertex().modify().set("community", e.getValue()).save();
    });
    assertThat(reported).isCloseTo(modularityScore(), within(1e-9));
  }
}
