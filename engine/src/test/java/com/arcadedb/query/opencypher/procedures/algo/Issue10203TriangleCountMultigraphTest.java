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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #10203: {@code algo.triangleCount} counted a self-loop, a parallel edge and an edge stored in both directions
 * as extra triangles, and {@code algo.localClusteringCoefficient}'s OLTP path did the same (its CSR path already
 * reduced the graph to a simple one, so the two paths disagreed). Triangles and clustering are defined on the simple
 * undirected graph, as NetworkX {@code triangles}/{@code clustering} compute them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10203TriangleCountMultigraphTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-10203-triangles");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E1");
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void selfLoopIsNoTriangle() {
    build(1, new int[][] { { 0, 0 } });
    assertThat(triangles(1)).containsExactly(0L);
  }

  @Test
  void simpleTriangleControl() {
    build(3, new int[][] { { 0, 1 }, { 1, 2 }, { 2, 0 } });
    assertThat(triangles(3)).containsExactly(1L, 1L, 1L);
  }

  @Test
  void parallelEdgeIsNotAnotherTriangle() {
    build(3, new int[][] { { 0, 1 }, { 0, 1 }, { 1, 2 }, { 2, 0 } });
    assertThat(triangles(3)).containsExactly(1L, 1L, 1L);
    assertThat(triangleCountCoefficients(3)).containsExactly(1.0, 1.0, 1.0);
  }

  @Test
  void reciprocalEdgeIsNotAnotherTriangle() {
    build(3, new int[][] { { 0, 1 }, { 1, 0 }, { 1, 2 }, { 2, 0 } });
    assertThat(triangles(3)).containsExactly(1L, 1L, 1L);
  }

  @Test
  void selfLoopOnATriangleVertexChangesNothing() {
    build(3, new int[][] { { 0, 1 }, { 1, 2 }, { 2, 0 }, { 0, 0 } });
    assertThat(triangles(3)).containsExactly(1L, 1L, 1L);
    assertThat(triangleCountCoefficients(3)).containsExactly(1.0, 1.0, 1.0);
    assertThat(localClusteringCoefficients(3)).containsExactly(1.0, 1.0, 1.0);
  }

  @Test
  void completeGraphOnFourVertices() {
    build(4, new int[][] { { 0, 1 }, { 0, 2 }, { 0, 3 }, { 1, 2 }, { 1, 3 }, { 2, 3 } });
    assertThat(triangles(4)).containsExactly(3L, 3L, 3L, 3L);
  }

  /**
   * A differential against a brute force over every vertex triple, on seeded random multigraphs with self-loops,
   * parallel and reciprocal edges: the shape of the report, kept as a guard for both procedures.
   */
  @Test
  void randomMultigraphsMatchBruteForce() {
    final Random random = new Random(10203);
    for (int round = 0; round < 6; round++) {
      final int n = 6 + random.nextInt(10);
      final int edgeCount = n * (2 + random.nextInt(3));
      final int[][] edges = new int[edgeCount][];
      for (int i = 0; i < edgeCount; i++)
        edges[i] = new int[] { random.nextInt(n), random.nextInt(n) };

      teardown();
      setup();
      build(n, edges);

      final boolean[][] adjacent = new boolean[n][n];
      for (final int[] e : edges)
        if (e[0] != e[1])
          adjacent[e[0]][e[1]] = adjacent[e[1]][e[0]] = true;

      final long[] expectedTriangles = new long[n];
      for (int a = 0; a < n; a++)
        for (int b = a + 1; b < n; b++)
          for (int c = b + 1; c < n; c++)
            if (adjacent[a][b] && adjacent[b][c] && adjacent[a][c]) {
              expectedTriangles[a]++;
              expectedTriangles[b]++;
              expectedTriangles[c]++;
            }

      final double[] expectedCoefficients = new double[n];
      for (int u = 0; u < n; u++) {
        int degree = 0;
        for (int v = 0; v < n; v++)
          if (adjacent[u][v])
            degree++;
        expectedCoefficients[u] = degree < 2 ? 0.0 : 2.0 * expectedTriangles[u] / (degree * (degree - 1.0));
      }

      assertThat(triangles(n)).as("round %d", round).containsExactly(expectedTriangles);
      final double[] fromTriangleCount = triangleCountCoefficients(n);
      final double[] fromLocalClustering = localClusteringCoefficients(n);
      for (int u = 0; u < n; u++) {
        assertThat(fromTriangleCount[u]).as("round %d, uid %d", round, u).isCloseTo(expectedCoefficients[u], within(1e-12));
        assertThat(fromLocalClustering[u]).as("round %d, uid %d", round, u).isCloseTo(expectedCoefficients[u], within(1e-12));
      }
    }
  }

  private void build(final int n, final int[][] edges) {
    database.transaction(() -> {
      final MutableVertex[] v = new MutableVertex[n];
      for (int i = 0; i < n; i++)
        v[i] = database.newVertex("V").set("uid", i).save();
      for (final int[] e : edges)
        v[e[0]].newEdge("E1", v[e[1]]);
    });
  }

  private long[] triangles(final int n) {
    final long[] out = new long[n];
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.triangleCount('E1') YIELD node, triangles RETURN node.uid AS u, triangles")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out[r.<Integer>getProperty("u")] = r.<Number>getProperty("triangles").longValue();
      }
    }
    return out;
  }

  private double[] triangleCountCoefficients(final int n) {
    return doubles(n, "CALL algo.triangleCount('E1') YIELD node, clusteringCoefficient RETURN node.uid AS u, clusteringCoefficient AS c");
  }

  private double[] localClusteringCoefficients(final int n) {
    return doubles(n, "CALL algo.localClusteringCoefficient('E1') YIELD node, localClusteringCoefficient RETURN node.uid AS u, localClusteringCoefficient AS c");
  }

  private double[] doubles(final int n, final String query) {
    final double[] out = new double[n];
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out[r.<Integer>getProperty("u")] = r.<Number>getProperty("c").doubleValue();
      }
    }
    return out;
  }
}
