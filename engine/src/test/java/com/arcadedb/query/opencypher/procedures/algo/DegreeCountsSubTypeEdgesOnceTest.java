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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewBuilder;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12075: a degree asked over several edge types counts each edge once, whichever of the types it matches. With
 * {@code E1 EXTENDS E0}, a type's degree includes its sub-types' edges, so a sum taken one type at a time over {@code E0} and
 * {@code E1} counted every {@code E1} edge twice: {@code algo.degree()} over a Graph Analytical View (every "all types" request
 * lists both), {@code algo.degree(['E0', 'E1'])} over a view, and the {@code node.degree*} functions with or without one.
 */
class DegreeCountsSubTypeEdgesOnceTest extends TestHelper {
  // E0: i -> i + 1 and 0 -> 2; E1: i + 1 -> i and 3 -> 1
  private static final int[][] E0_EDGES = { { 0, 1 }, { 1, 2 }, { 2, 3 }, { 0, 2 } };
  private static final int[][] E1_EDGES = { { 1, 0 }, { 2, 1 }, { 3, 2 }, { 3, 1 } };

  private final MutableVertex[] vertices = new MutableVertex[4];

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE E0");
    database.command("sql", "CREATE EDGE TYPE E1 EXTENDS E0");
    database.transaction(() -> {
      for (int i = 0; i < vertices.length; i++)
        vertices[i] = database.newVertex("V").set("i", i).save();
      for (final int[] e : E0_EDGES)
        vertices[e[0]].newEdge("E0", vertices[e[1]]);
      for (final int[] e : E1_EDGES)
        vertices[e[0]].newEdge("E1", vertices[e[1]]);
    });
  }

  @Test
  void algoDegreeOverAViewOfEveryEdgeTypeCountsSubTypeEdgesOnce() {
    final Map<Integer, String> withoutView = degrees("");
    assertThat(withoutView).isEqualTo(expected());

    buildView(null);
    assertThat(degrees("")).isEqualTo(withoutView);
    assertThat(degrees("['E0', 'E1']")).isEqualTo(withoutView);
    assertThat(degrees("'E0'")).isEqualTo(withoutView);
  }

  @Test
  void algoDegreeOverAViewOfTheSuperTypeCountsSubTypeEdgesOnce() {
    buildView("E0");
    assertThat(degrees("['E0', 'E1']")).isEqualTo(expected());
    assertThat(degrees("['E1', 'E0', 'E1']")).isEqualTo(expected());
    assertThat(degrees("'E0'")).isEqualTo(expected());
  }

  @Test
  void algoDegreeCountsSubTypeEdgesTheViewHeardAfterItsBuildOnce() {
    buildView(null);
    database.transaction(() -> vertices[0].newEdge("E1", vertices[3]));

    final Map<Integer, String> degrees = degrees("");
    assertThat(degrees.get(0)).isEqualTo("in 1, out 3");
    assertThat(degrees.get(3)).isEqualTo("in 2, out 2");
    assertThat(degrees.values().stream().mapToInt(DegreeCountsSubTypeEdgesOnceTest::outOf).sum())
        .isEqualTo(E0_EDGES.length + E1_EDGES.length + 1);
  }

  @Test
  void nodeDegreeFunctionsCountSubTypeEdgesOnce() {
    // Vertex 3: out E1 3 -> 2 and 3 -> 1; in E0 2 -> 3
    assertThat(nodeFunction("node.degree(a, ['E0', 'E1'], 'OUT')")).isEqualTo(2L);
    assertThat(nodeFunction("node.degree(a, ['E0', 'E1'], 'IN')")).isEqualTo(1L);
    assertThat(nodeFunction("node.degree(a, ['E0', 'E1'])")).isEqualTo(3L);
    assertThat(nodeFunction("node.degree.out(a, ['E0', 'E1'])")).isEqualTo(2L);
    assertThat(nodeFunction("node.degree.in(a, ['E0', 'E1'])")).isEqualTo(1L);
    assertThat(nodeFunction("node.degree.out(a, ['E1', 'E1'])")).isEqualTo(2L);
    assertThat(nodeFunction("node.degree.out(a, 'E0')")).isEqualTo(2L);
    assertThat(nodeFunction("node.degree.in(a)")).isEqualTo(1L);

    buildView(null);
    assertThat(nodeFunction("node.degree(a, ['E0', 'E1'])")).isEqualTo(3L);
    assertThat(nodeFunction("node.degree.in(a, ['E0', 'E1'])")).isEqualTo(1L);
  }

  private void buildView(final String edgeType) {
    final GraphAnalyticalViewBuilder builder = GraphAnalyticalView.builder(database).withName("G").withVertexTypes("V")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS);
    if (edgeType != null)
      builder.withEdgeTypes(edgeType);
    builder.build();
  }

  /** The in and out degree of every vertex, from the edge lists above. */
  private static Map<Integer, String> expected() {
    final int[] in = new int[4];
    final int[] out = new int[4];
    for (final int[][] edges : new int[][][] { E0_EDGES, E1_EDGES })
      for (final int[] e : edges) {
        out[e[0]]++;
        in[e[1]]++;
      }
    final Map<Integer, String> degrees = new TreeMap<>();
    for (int i = 0; i < 4; i++)
      degrees.put(i, "in " + in[i] + ", out " + out[i]);
    return degrees;
  }

  /** {@code algo.degree(args)} per vertex, as "in N, out M". */
  private Map<Integer, String> degrees(final String args) {
    final Map<Integer, String> degrees = new TreeMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.degree(" + args + ") YIELD node, inDegree, outDegree RETURN node.i AS i, inDegree, outDegree")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        degrees.put(row.<Number>getProperty("i").intValue(),
            "in " + row.<Number>getProperty("inDegree") + ", out " + row.<Number>getProperty("outDegree"));
      }
    }
    return degrees;
  }

  private static int outOf(final String degrees) {
    return Integer.parseInt(degrees.substring(degrees.indexOf("out ") + 4));
  }

  private Object nodeFunction(final String call) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:V {i: 3}) RETURN " + call + " AS c")) {
      return rs.next().getProperty("c");
    }
  }
}
