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
package com.arcadedb.function.sql.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10527: {@code maxDepth} on {@code dijkstra()} and {@code astar()} bounded the nodes the search POLLED, not the edges of the
 * path. Dijkstra answered "no path" for a 2-hop path at any {@code maxDepth} from 2 to 9 (the goal was refused when it was polled at
 * the bound), and astar answered a path that never reached the destination.
 * <p>
 * The rule, shared with {@code shortestPath()} since #10439: {@code maxDepth: n} is the number of edges a returned path may have, and
 * a returned path always runs from the source to the destination. Beyond that, the answer is the CHEAPEST path of at most n edges,
 * which is not the cheapest path cut short: a dearer path with fewer hops can be the only one that fits.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10527WeightedPathMaxDepthTest extends TestHelper {
  private static final String[] FUNCTIONS = { "dijkstra", "astar" };

  private RID source;
  private RID target;

  // ------------------------------------------------------------------------------------------------------------ the branching graph

  /** S -> a1 (50) -> T, and S -> a2..a8 (1) which are dead ends: the only path to T is S, a1, T. */
  private void buildBranching() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX V SET nm = 'S'");
      database.command("sql", "CREATE VERTEX V SET nm = 'T'");
      for (int i = 1; i <= 8; i++)
        database.command("sql", "CREATE VERTEX V SET nm = ?", "a" + i);
      edge("S", "a1", 50.0);
      for (int i = 2; i <= 8; i++)
        edge("S", "a" + i, 1.0);
      edge("a1", "T", 1.0);
    });
    source = rid("S");
    target = rid("T");
  }

  @ParameterizedTest
  @ValueSource(strings = { "dijkstra", "astar" })
  void aTwoHopPathIsFoundWheneverTheBoundAllowsIt(final String function) {
    buildBranching();

    for (int maxDepth = 2; maxDepth <= 12; maxDepth++)
      assertThat(names(path(function, maxDepth, null))).as("%s maxDepth %d", function, maxDepth).containsExactly("S", "a1", "T");

    assertThat(path(function, 1, null)).as("%s maxDepth 1", function).isEmpty();
    assertThat(path(function, 0, null)).as("%s maxDepth 0", function).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = { "dijkstra", "astar" })
  void aBoundTooSmallIsNeverAnsweredWithAPathThatMissesTheDestination(final String function) {
    buildBranching();

    for (final Boolean emptyIfMaxDepth : new Boolean[] { null, Boolean.TRUE, Boolean.FALSE })
      for (int maxDepth = 0; maxDepth < 2; maxDepth++)
        assertThat(path(function, maxDepth, emptyIfMaxDepth)).as("%s maxDepth %d emptyIfMaxDepth %s", function, maxDepth, emptyIfMaxDepth)
            .isEmpty();
  }

  // ------------------------------------------------------------------------------------------------------------------ the chain

  @ParameterizedTest
  @ValueSource(strings = { "dijkstra", "astar" })
  void maxDepthIsTheNumberOfEdgesOfThePath(final String function) {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      for (int i = 0; i <= 5; i++)
        database.command("sql", "CREATE VERTEX V SET nm = ?", "v" + i);
      for (int i = 0; i < 5; i++)
        edge("v" + i, "v" + (i + 1), 1.0);
    });
    source = rid("v0");
    target = rid("v5");

    // five edges: the bound that answers is five, as for shortestPath(), and not six
    assertThat(path(function, 5, null)).as("%s maxDepth 5", function).hasSize(6);
    assertThat(path(function, 4, null)).as("%s maxDepth 4", function).isEmpty();
    assertThat(path(function, 6, null)).as("%s maxDepth 6", function).hasSize(6);
  }

  @ParameterizedTest
  @ValueSource(strings = { "dijkstra", "astar" })
  void aSourceThatIsTheDestinationIsAZeroEdgePath(final String function) {
    buildBranching();
    source = rid("S");
    target = rid("S");

    assertThat(names(path(function, 0, null))).containsExactly("S");
    assertThat(names(path(function, 3, null))).containsExactly("S");
  }

  // --------------------------------------------------------------------------------------- the cheapest path that fits, not the cut one

  /**
   * S -> T costs 10 in one hop; S -> b1 -> b2 -> b3 -> T costs 4 in four. The cheapest path of at most three edges is the direct
   * one, though the cheapest path overall is the long one.
   */
  @ParameterizedTest
  @ValueSource(strings = { "dijkstra", "astar" })
  void theCheapestPathThatFitsTheBoundWinsOverTheCheapestPathOverall(final String function) {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      for (final String name : new String[] { "S", "T", "b1", "b2", "b3" })
        database.command("sql", "CREATE VERTEX V SET nm = ?", name);
      edge("S", "T", 10.0);
      edge("S", "b1", 1.0);
      edge("b1", "b2", 1.0);
      edge("b2", "b3", 1.0);
      edge("b3", "T", 1.0);
    });
    source = rid("S");
    target = rid("T");

    assertThat(names(path(function, null, null))).containsExactly("S", "b1", "b2", "b3", "T");
    assertThat(names(path(function, 4, null))).containsExactly("S", "b1", "b2", "b3", "T");
    assertThat(names(path(function, 3, null))).containsExactly("S", "T");
    assertThat(names(path(function, 1, null))).containsExactly("S", "T");
  }

  /**
   * The vertex M is reached cheaply in two hops (S, c, M) and dearly in one (S, M). The cheap arrival is settled first, and the dear
   * one has to stay a candidate: it is the only way to M, and on to T, within three edges.
   */
  @ParameterizedTest
  @ValueSource(strings = { "dijkstra", "astar" })
  void aVertexReachedCheaplyInManyHopsStaysReachableInFewer(final String function) {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      for (final String name : new String[] { "S", "T", "c", "M", "x", "y" })
        database.command("sql", "CREATE VERTEX V SET nm = ?", name);
      edge("S", "c", 1.0);
      edge("c", "M", 1.0);
      edge("S", "M", 9.0);
      edge("M", "x", 1.0);
      edge("x", "y", 1.0);
      edge("y", "T", 1.0);
    });
    source = rid("S");
    target = rid("T");

    // S c M x y T is five edges
    assertThat(names(path(function, 5, null))).containsExactly("S", "c", "M", "x", "y", "T");
    // S M x y T is four
    assertThat(names(path(function, 4, null))).containsExactly("S", "M", "x", "y", "T");
    assertThat(path(function, 3, null)).isEmpty();
  }

  // ----------------------------------------------------------------------------------------------------------- against a reference

  /**
   * The property, with no function named: over random weighted digraphs and every bound, each function answers exactly when a path
   * of at most n edges exists, the answer runs from the source to the destination, has at most n edges, and costs what the best such
   * path costs - which a table of the cheapest cost per (vertex, number of edges) gives independently of any search.
   */
  @Test
  void theAnswerIsTheCheapestPathOfAtMostMaxDepthEdges() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    final Random random = new Random(10527L);

    for (int round = 0; round < 12; round++) {
      final int vertices = 6 + random.nextInt(7);
      final double[][] weight = new double[vertices][vertices];
      for (final double[] row : weight)
        Arrays.fill(row, Double.POSITIVE_INFINITY);

      final RID[] rids = new RID[vertices];
      database.transaction(() -> {
        for (int i = 0; i < vertices; i++)
          rids[i] = database.newVertex("V").set("nm", "r" + i).save().getIdentity();
      });
      final int edges = vertices * 2 + random.nextInt(vertices);
      database.transaction(() -> {
        for (int e = 0; e < edges; e++) {
          final int from = random.nextInt(vertices);
          final int to = random.nextInt(vertices);
          if (from == to || weight[from][to] != Double.POSITIVE_INFINITY)
            continue;
          // integral weights, so a total is exact and two paths of one cost are not told apart by rounding
          weight[from][to] = 1 + random.nextInt(9);
          rids[from].asVertex().newEdge("E", rids[to].asVertex()).set("w", weight[from][to]).save();
        }
      });

      for (int s = 0; s < vertices; s++)
        for (int t = 0; t < vertices; t++)
          for (final int maxDepth : new int[] { 0, 1, 2, 3, 5, vertices }) {
            final double expected = cheapestWithin(weight, s, t, maxDepth);
            for (final String function : FUNCTIONS) {
              source = rids[s];
              target = rids[t];
              final List<RID> answer = path(function, maxDepth, null);
              final String what = function + " " + s + "->" + t + " maxDepth " + maxDepth + " round " + round;
              if (expected == Double.POSITIVE_INFINITY) {
                assertThat(answer).as(what).isEmpty();
              } else {
                assertThat(answer).as(what).isNotEmpty();
                assertThat(answer.getFirst()).as(what).isEqualTo(rids[s]);
                assertThat(answer.getLast()).as(what).isEqualTo(rids[t]);
                assertThat(answer.size() - 1).as(what + " edges").isLessThanOrEqualTo(maxDepth);
                assertThat(cost(answer)).as(what + " cost").isEqualTo(expected);
              }
            }
          }

      database.transaction(() -> {
        database.command("sql", "DELETE FROM E");
        database.command("sql", "DELETE FROM V");
      });
    }
  }

  /**
   * A grid of 625 vertices with a road in each of the four directions and random weights, where the cheap routes wind and the short
   * ones are dear: the search under a bound that cuts the cheap routes keeps several labels per vertex. It must still finish, and
   * answer what the layered reference answers. The timeout is a hang detector, not a latency bound.
   */
  @Test
  @Timeout(120)
  void aBoundedSearchOverALargerGraphFinishesAndAnswersTheReference() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    final int side = 25;
    final int vertices = side * side;
    final double[][] weight = new double[vertices][vertices];
    for (final double[] row : weight)
      Arrays.fill(row, Double.POSITIVE_INFINITY);
    final Random random = new Random(1052725L);

    final RID[] rids = new RID[vertices];
    database.transaction(() -> {
      for (int i = 0; i < vertices; i++)
        rids[i] = database.newVertex("V").set("nm", "g" + i).save().getIdentity();
      for (int row = 0; row < side; row++)
        for (int column = 0; column < side; column++) {
          final int from = row * side + column;
          final int[] neighbours = { column + 1 < side ? from + 1 : -1, column > 0 ? from - 1 : -1, row + 1 < side ? from + side : -1,
              row > 0 ? from - side : -1 };
          for (final int to : neighbours)
            if (to >= 0) {
              weight[from][to] = 1 + random.nextInt(9);
              rids[from].asVertex().newEdge("E", rids[to].asVertex()).set("w", weight[from][to]).save();
            }
        }
    });

    final int s = 0;
    final int t = vertices - 1;
    // the corner to the opposite corner is 48 edges at the least: below that nothing fits, past it the bound starts to bind on cost
    for (final int maxDepth : new int[] { 47, 48, 50, 56, 70 }) {
      final double expected = cheapestWithin(weight, s, t, maxDepth);
      for (final String function : FUNCTIONS) {
        source = rids[s];
        target = rids[t];
        final List<RID> answer = path(function, maxDepth, null);
        final String what = function + " maxDepth " + maxDepth;
        if (expected == Double.POSITIVE_INFINITY) {
          assertThat(answer).as(what).isEmpty();
        } else {
          assertThat(answer).as(what).isNotEmpty();
          assertThat(answer.size() - 1).as(what + " edges").isLessThanOrEqualTo(maxDepth);
          assertThat(cost(answer)).as(what + " cost").isEqualTo(expected);
        }
      }
    }
  }

  /** Bellman-Ford by layers: the cheapest cost from {@code s} to {@code t} over at most {@code maxDepth} edges. */
  private static double cheapestWithin(final double[][] weight, final int s, final int t, final int maxDepth) {
    final int n = weight.length;
    double[] best = new double[n];
    Arrays.fill(best, Double.POSITIVE_INFINITY);
    best[s] = 0;
    for (int layer = 0; layer < maxDepth; layer++) {
      final double[] next = best.clone();
      for (int u = 0; u < n; u++)
        if (best[u] != Double.POSITIVE_INFINITY)
          for (int v = 0; v < n; v++)
            if (weight[u][v] != Double.POSITIVE_INFINITY && best[u] + weight[u][v] < next[v])
              next[v] = best[u] + weight[u][v];
      best = next;
    }
    return best[t];
  }

  /** The cost of a path of vertices: the cheapest edge between each consecutive pair, which is the one a search can use. */
  private double cost(final List<RID> path) {
    double total = 0;
    for (int i = 0; i + 1 < path.size(); i++) {
      double cheapest = Double.POSITIVE_INFINITY;
      final Vertex from = path.get(i).asVertex();
      for (final Edge edge : from.getEdges(Vertex.DIRECTION.OUT, "E"))
        if (edge.getIn().equals(path.get(i + 1)))
          cheapest = Math.min(cheapest, ((Number) edge.get("w")).doubleValue());
      total += cheapest;
    }
    return total;
  }

  // ---------------------------------------------------------------------------------------------------------------------- helpers

  private void edge(final String from, final String to, final double weight) {
    database.command("sql", "CREATE EDGE E FROM (SELECT FROM V WHERE nm = ?) TO (SELECT FROM V WHERE nm = ?) SET w = ?", from, to, weight);
  }

  private RID rid(final String name) {
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM V WHERE nm = ?", name)) {
      return resultSet.next().getIdentity().orElseThrow();
    }
  }

  private List<String> names(final List<RID> rids) {
    final List<String> names = new ArrayList<>();
    for (final RID rid : rids)
      names.add(rid.asVertex().getString("nm"));
    return names;
  }

  private List<RID> path(final String function, final Integer maxDepth, final Boolean emptyIfMaxDepth) {
    final StringBuilder options = new StringBuilder("{direction: 'OUT', edgeTypeNames: ['E']");
    if (function.equals("astar"))
      options.append(", vertexAxisNames: []");
    if (maxDepth != null)
      options.append(", maxDepth: ").append(maxDepth);
    if (emptyIfMaxDepth != null)
      options.append(", emptyIfMaxDepth: ").append(emptyIfMaxDepth);
    options.append('}');

    try (final ResultSet resultSet = database.query("sql", "SELECT " + function + "(?, ?, 'w', " + options + ") AS p", source, target)) {
      final List<?> answer = resultSet.next().getProperty("p");
      final List<RID> rids = new ArrayList<>();
      if (answer != null)
        for (final Object element : answer)
          rids.add((RID) element);
      return rids;
    }
  }
}
