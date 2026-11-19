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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the algo.maxFlow Cypher procedure.
 */
class AlgoMaxFlowTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-maxflow");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("PIPE");

    // Classic max-flow example:
    // S -> A (cap 10), S -> B (cap 10)
    // A -> T (cap 10), B -> T (cap 10)
    // Max flow S->T = 20
    database.transaction(() -> {
      final MutableVertex s = database.newVertex("Node").set("name", "S").save();
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex t = database.newVertex("Node").set("name", "T").save();
      s.newEdge("PIPE", a, true, new Object[] { "capacity", 10.0 }).save();
      s.newEdge("PIPE", b, true, new Object[] { "capacity", 10.0 }).save();
      a.newEdge("PIPE", t, true, new Object[] { "capacity", 10.0 }).save();
      b.newEdge("PIPE", t, true, new Object[] { "capacity", 10.0 }).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void maxFlowReturnsOneRow() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (s:Node {name:'S'}), (t:Node {name:'T'}) \
        CALL algo.maxFlow(s, t, 'PIPE', 'capacity') YIELD maxFlow, sourceId, sinkId \
        RETURN maxFlow, sourceId, sinkId""");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(1);
  }

  @Test
  void maxFlowValue() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (s:Node {name:'S'}), (t:Node {name:'T'}) \
        CALL algo.maxFlow(s, t, 'PIPE', 'capacity') YIELD maxFlow \
        RETURN maxFlow""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("maxFlow");
    // S->A->T and S->B->T carry 10 each.
    assertThat(((Number) val).doubleValue()).isEqualTo(20.0);
  }

  @Test
  void maxFlowSourceAndSinkIds() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (s:Node {name:'S'}), (t:Node {name:'T'}) \
        CALL algo.maxFlow(s, t, 'PIPE', 'capacity') YIELD maxFlow, sourceId, sinkId \
        RETURN maxFlow, sourceId, sinkId""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object sourceId = result.getProperty("sourceId");
    final Object sinkId = result.getProperty("sinkId");
    assertThat(sourceId).isNotNull();
    assertThat(sinkId).isNotNull();
  }

  @Test
  void maxFlowNoCapacityProperty() {
    // Without capacity property: all edges have capacity 1
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (s:Node {name:'S'}), (t:Node {name:'T'}) \
        CALL algo.maxFlow(s, t) YIELD maxFlow \
        RETURN maxFlow""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("maxFlow");
    // Every pipe has capacity 1: one unit along each of the two disjoint paths.
    assertThat(((Number) val).doubleValue()).isEqualTo(2.0);
  }

  /**
   * Issue #11798: a relationship is a pipe from its source to its target. The flow used to be computed on the undirected
   * network, so it ran against the relationships and an opposite relationship added its capacity to the forward one.
   */
  @Test
  void relationshipsCarryFlowOnlyInTheirOwnDirection() {
    pipes3();
    assertThat(flow("1", "2", null)).isEqualTo(3.0);
    assertThat(flow("2", "1", null)).isEqualTo(2.0);
    assertThat(flow("1", "3", null)).isEqualTo(1.0);
    // Node 3 has no outgoing pipe: nothing can leave it.
    assertThat(flow("3", "1", null)).isEqualTo(0.0);
    assertThat(flow("3", "2", null)).isEqualTo(0.0);
  }

  @Test
  void directionOutIsTheDefault() {
    pipes3();
    assertThat(flow("1", "2", "OUT")).isEqualTo(flow("1", "2", null));
    assertThat(flow("3", "1", "OUT")).isEqualTo(0.0);
  }

  @Test
  void directionInRunsTheFlowAgainstTheRelationships() {
    pipes3();
    // Reversed network: (2)-[3]->(1), (1)-[2]->(2), (3)-[1]->(2).
    assertThat(flow("1", "2", "IN")).isEqualTo(2.0);
    assertThat(flow("2", "1", "IN")).isEqualTo(3.0);
    assertThat(flow("3", "1", "IN")).isEqualTo(1.0);
    assertThat(flow("1", "3", "IN")).isEqualTo(0.0);
  }

  @Test
  void directionBothTreatsEveryRelationshipAsAnUndirectedPipe() {
    pipes3();
    // Undirected: between 1 and 2 the two pipes give 3 + 2, and 3 hangs off 2 through a pipe of capacity 1.
    assertThat(flow("1", "2", "BOTH")).isEqualTo(5.0);
    assertThat(flow("2", "1", "BOTH")).isEqualTo(5.0);
    assertThat(flow("1", "3", "BOTH")).isEqualTo(1.0);
    assertThat(flow("3", "1", "BOTH")).isEqualTo(1.0);
  }

  @Test
  void anInvalidDirectionIsRefused() {
    pipes3();
    assertThatThrownBy(() -> flow("1", "2", "INCOMING"))
        .hasStackTraceContaining("algo.maxFlow(): ");
  }

  @Test
  void parallelRelationshipsAddTheirCapacities() {
    database.transaction(() -> {
      final MutableVertex x = database.newVertex("Node").set("name", "X").save();
      final MutableVertex y = database.newVertex("Node").set("name", "Y").save();
      x.newEdge("PIPE", y, true, new Object[] { "capacity", 1.5 }).save();
      x.newEdge("PIPE", y, true, new Object[] { "capacity", 2.5 }).save();
      // A self loop carries nothing anywhere, and neither does a capacity that is not positive.
      x.newEdge("PIPE", x, true, new Object[] { "capacity", 100.0 }).save();
      x.newEdge("PIPE", y, true, new Object[] { "capacity", -5.0 }).save();
      x.newEdge("PIPE", y, true, new Object[] { "capacity", 0.0 }).save();
      x.newEdge("PIPE", y, true, new Object[] { "capacity", Double.NaN }).save();
    });
    assertThat(flow("X", "Y", null)).isEqualTo(4.0);
    assertThat(flow("Y", "X", null)).isEqualTo(0.0);
  }

  @Test
  void aPathOfInfiniteCapacitiesMakesTheFlowInfinite() {
    database.transaction(() -> {
      final MutableVertex x = database.newVertex("Node").set("name", "IX").save();
      final MutableVertex y = database.newVertex("Node").set("name", "IY").save();
      final MutableVertex z = database.newVertex("Node").set("name", "IZ").save();
      x.newEdge("PIPE", y, true, new Object[] { "capacity", Double.POSITIVE_INFINITY }).save();
      y.newEdge("PIPE", z, true, new Object[] { "capacity", 4.0 }).save();
    });
    // used to read path[-2] once the augmentation saturated nothing
    assertThat(flow("IX", "IY", null)).isEqualTo(Double.POSITIVE_INFINITY);
    // a finite arc on the path bounds it
    assertThat(flow("IX", "IZ", null)).isEqualTo(4.0);
  }

  @Test
  void theResidualArcsAreNeededToReachTheMaximum() {
    // whichever of A->C and A->D the search tries first, the maximum of 2 needs S->A->C->T to be partly undone when the first
    // path through C was taken: S->A, S->B, A->C, A->D, B->C, C->T, D->T, every capacity 1
    database.transaction(() -> {
      final MutableVertex s = database.newVertex("Node").set("name", "RS").save();
      final MutableVertex a = database.newVertex("Node").set("name", "RA").save();
      final MutableVertex b = database.newVertex("Node").set("name", "RB").save();
      final MutableVertex c = database.newVertex("Node").set("name", "RC").save();
      final MutableVertex d = database.newVertex("Node").set("name", "RD").save();
      final MutableVertex t = database.newVertex("Node").set("name", "RT").save();
      s.newEdge("PIPE", a, true, new Object[] { "capacity", 1.0 }).save();
      s.newEdge("PIPE", b, true, new Object[] { "capacity", 1.0 }).save();
      a.newEdge("PIPE", c, true, new Object[] { "capacity", 1.0 }).save();
      a.newEdge("PIPE", d, true, new Object[] { "capacity", 1.0 }).save();
      b.newEdge("PIPE", c, true, new Object[] { "capacity", 1.0 }).save();
      c.newEdge("PIPE", t, true, new Object[] { "capacity", 1.0 }).save();
      d.newEdge("PIPE", t, true, new Object[] { "capacity", 1.0 }).save();
    });
    assertThat(flow("RS", "RT", null)).isEqualTo(2.0);
  }

  @Test
  void theFlowFromANodeToItselfIsZero() {
    // Used to augment the empty path forever, by Double.MAX_VALUE each round, until the command timed out.
    assertThat(flow("S", "S", null)).isEqualTo(0.0);
  }

  @Test
  void theResidualArcsReroutePreviousFlow() {
    // The textbook case where a greedy first path (S->A->B->T) must be undone through the residual arc B->A to reach 2:
    // S->A(1), S->B(1), A->B(1), A->T(1), B->T(1).
    database.transaction(() -> {
      final MutableVertex s = database.newVertex("Node").set("name", "s").save();
      final MutableVertex a = database.newVertex("Node").set("name", "a").save();
      final MutableVertex b = database.newVertex("Node").set("name", "b").save();
      final MutableVertex t = database.newVertex("Node").set("name", "t").save();
      s.newEdge("PIPE", a, true, new Object[] { "capacity", 1.0 }).save();
      s.newEdge("PIPE", b, true, new Object[] { "capacity", 1.0 }).save();
      a.newEdge("PIPE", b, true, new Object[] { "capacity", 1.0 }).save();
      a.newEdge("PIPE", t, true, new Object[] { "capacity", 1.0 }).save();
      b.newEdge("PIPE", t, true, new Object[] { "capacity", 1.0 }).save();
    });
    assertThat(flow("s", "t", null)).isEqualTo(2.0);
  }

  @Test
  void aLongChainIsSolvedWithoutRecursion() {
    // The augmenting path is 5 000 relationships long: a recursive depth-first search would need a frame per hop.
    final int length = 5_000;
    database.transaction(() -> {
      MutableVertex previous = database.newVertex("Node").set("name", "c0").save();
      for (int i = 1; i <= length; i++) {
        final MutableVertex next = database.newVertex("Node").set("name", "c" + i).save();
        previous.newEdge("PIPE", next, true, new Object[] { "capacity", (double) (1 + i % 7) }).save();
        previous = next;
      }
    });
    assertThat(flow("c0", "c" + length, null)).isEqualTo(1.0);
  }

  /**
   * Dinic over the sparse residual network against a textbook Edmonds-Karp over a dense matrix, on random multigraphs with
   * parallel relationships, opposite relationships and self loops, in both the directed and the undirected reading.
   */
  @Test
  void agreesWithAReferenceEdmondsKarpOnRandomNetworks() {
    final Random random = new Random(11798);
    final int n = 9;
    for (int round = 0; round < 25; round++) {
      final String prefix = "r" + round + "_";
      final int[][] edges = new int[5 + random.nextInt(25)][];
      for (int e = 0; e < edges.length; e++)
        edges[e] = new int[] { random.nextInt(n), random.nextInt(n), 1 + random.nextInt(9) };

      database.transaction(() -> {
        final MutableVertex[] nodes = new MutableVertex[n];
        for (int i = 0; i < n; i++)
          nodes[i] = database.newVertex("Node").set("name", prefix + i).save();
        for (final int[] edge : edges)
          nodes[edge[0]].newEdge("PIPE", nodes[edge[1]], true, new Object[] { "capacity", (double) edge[2] }).save();
      });

      for (int pair = 0; pair < 4; pair++) {
        final int s = random.nextInt(n);
        final int t = random.nextInt(n);
        assertThat(flow(prefix + s, prefix + t, null)).as("round %d, %d -> %d, OUT", round, s, t)
            .isEqualTo(referenceMaxFlow(n, edges, s, t, false));
        assertThat(flow(prefix + s, prefix + t, "BOTH")).as("round %d, %d -> %d, BOTH", round, s, t)
            .isEqualTo(referenceMaxFlow(n, edges, s, t, true));
      }
    }
  }

  private static double referenceMaxFlow(final int n, final int[][] edges, final int s, final int t, final boolean undirected) {
    if (s == t)
      return 0;
    final double[][] residual = new double[n][n];
    for (final int[] edge : edges)
      if (edge[0] != edge[1]) {
        residual[edge[0]][edge[1]] += edge[2];
        if (undirected)
          residual[edge[1]][edge[0]] += edge[2];
      }
    double flow = 0;
    while (true) {
      final int[] parent = new int[n];
      Arrays.fill(parent, -1);
      parent[s] = s;
      final ArrayDeque<Integer> queue = new ArrayDeque<>(List.of(s));
      while (!queue.isEmpty() && parent[t] < 0) {
        final int u = queue.poll();
        for (int v = 0; v < n; v++)
          if (parent[v] < 0 && residual[u][v] > 0) {
            parent[v] = u;
            queue.add(v);
          }
      }
      if (parent[t] < 0)
        return flow;
      double path = Double.MAX_VALUE;
      for (int v = t; v != s; v = parent[v])
        path = Math.min(path, residual[parent[v]][v]);
      for (int v = t; v != s; v = parent[v]) {
        residual[parent[v]][v] -= path;
        residual[v][parent[v]] += path;
      }
      flow += path;
    }
  }

  /** Issue #11798's network: (1)-[cap 3]->(2), (2)-[cap 2]->(1), (2)-[cap 1]->(3). */
  private void pipes3() {
    database.transaction(() -> {
      final MutableVertex n1 = database.newVertex("Node").set("name", "1").save();
      final MutableVertex n2 = database.newVertex("Node").set("name", "2").save();
      final MutableVertex n3 = database.newVertex("Node").set("name", "3").save();
      n1.newEdge("PIPE", n2, true, new Object[] { "capacity", 3.0 }).save();
      n2.newEdge("PIPE", n1, true, new Object[] { "capacity", 2.0 }).save();
      n2.newEdge("PIPE", n3, true, new Object[] { "capacity", 1.0 }).save();
    });
  }

  private double flow(final String source, final String sink, final String direction) {
    final String call = direction == null ?
        "CALL algo.maxFlow(s, t, 'PIPE', 'capacity')" :
        "CALL algo.maxFlow(s, t, 'PIPE', 'capacity', '" + direction + "')";
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:Node {name: $s}), (t:Node {name: $t}) " + call + " YIELD maxFlow RETURN maxFlow",
        Map.of("s", source, "t", sink))) {
      return rs.next().<Number>getProperty("maxFlow").doubleValue();
    }
  }
}
