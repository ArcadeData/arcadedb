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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * {@code shortestPath()}, {@code dijkstra()}, {@code astar()} and {@code bellmanFord()} against textbook references on random
 * multigraphs (parallel edges, self loops, two edge types, every direction, a depth bound, negative edge weights without a
 * negative cycle for Bellman-Ford): a path has to exist exactly when the reference finds one, to walk existing edges and to cost
 * what the reference's cheapest path costs (or have as many hops as the fewest).
 */
@Tag("slow")
class GraphPathFunctionsOracleTest extends TestHelper {
  private static final double INFINITY = Double.POSITIVE_INFINITY;

  private record Edge(int from, int to, double weight, String type) {
  }

  private static boolean forward(final String direction) {
    return !"IN".equals(direction);
  }

  private static boolean backward(final String direction) {
    return !"OUT".equals(direction);
  }

  @Test
  void pathFunctionsAgreeWithReferencesOnRandomGraphs() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E1");
    database.getSchema().createEdgeType("E2");

    final Random random = new Random(12_345);
    int checks = 0;
    for (int round = 0; round < 40; round++) {
      database.transaction(() -> database.command("sql", "DELETE FROM V"));
      final int vertexCount = 3 + random.nextInt(12);
      final boolean negative = round % 3 == 0;
      // a weight is c + p(from) - p(to) with c >= 0: negative edges, and no negative cycle whatever the edges are
      final double[] potential = new double[vertexCount];
      for (int i = 0; i < vertexCount; i++)
        potential[i] = random.nextInt(8) * 0.5;

      final List<Edge> edges = new ArrayList<>();
      final RID[] rids = new RID[vertexCount];
      final Map<RID, Integer> indexOf = new HashMap<>();
      database.begin();
      final MutableVertex[] vertices = new MutableVertex[vertexCount];
      for (int i = 0; i < vertexCount; i++) {
        vertices[i] = database.newVertex("V").set("i", i);
        vertices[i].save();
        rids[i] = vertices[i].getIdentity();
        indexOf.put(rids[i], i);
      }
      final int edgeCount = random.nextInt(vertexCount * 3);
      for (int k = 0; k < edgeCount; k++) {
        final int from = random.nextInt(vertexCount);
        final int to = random.nextInt(vertexCount);
        final double c = new double[] { 0, 0.5, 1, 1, 2, 3.5, 0.25 }[random.nextInt(7)];
        final double weight = negative ? c + potential[from] - potential[to] : c == 0 ? 0.5 : c;
        final String type = random.nextBoolean() ? "E1" : "E2";
        vertices[from].newEdge(type, vertices[to], "weight", weight).save();
        edges.add(new Edge(from, to, weight, type));
      }
      database.commit();

      for (int q = 0; q < 10; q++) {
        final int source = random.nextInt(vertexCount);
        final int target = random.nextInt(vertexCount);
        if (source == target)
          continue;
        final String direction = new String[] { "OUT", "IN", "BOTH" }[random.nextInt(3)];
        final String type = random.nextInt(3) == 0 ? (random.nextBoolean() ? "E1" : "E2") : null;

        // shortestPath: the fewest hops, over the edges of the type
        final int hops = bfs(vertexCount, edges, source, target, direction, type);
        final List<RID> hopPath = path("shortestPath(" + rids[source] + ", " + rids[target] + ", '" + direction + "'"
            + (type == null ? "" : ", '" + type + "'") + ")");
        checks++;
        if (hops < 0)
          assertThat(hopPath).as("shortestPath %s -> %s %s %s: unreachable", source, target, direction, type).isEmpty();
        else {
          assertThat(hopPath).as("shortestPath %s -> %s %s %s edges %s", source, target, direction, type, edges)
              .hasSize(hops + 1);
          assertConnects("shortestPath", hopPath, rids[source], rids[target]);
          assertThat(walksEdges(hopPath, indexOf, edges, direction, type)).as("shortestPath %s walks edges that exist", hopPath).isTrue();
        }
        final int bound = random.nextInt(6);
        final List<RID> boundedPath = path("shortestPath(" + rids[source] + ", " + rids[target] + ", '" + direction + "', "
            + (type == null ? "null" : "'" + type + "'") + ", {maxDepth: " + bound + "})");
        checks++;
        if (hops < 0 || hops > bound)
          assertThat(boundedPath).as("shortestPath maxDepth %s over %s hops", bound, hops).isEmpty();
        else {
          assertThat(boundedPath).as("shortestPath maxDepth %s over %s hops", bound, hops).hasSize(hops + 1);
          assertConnects("bounded shortestPath", boundedPath, rids[source], rids[target]);
          assertThat(walksEdges(boundedPath, indexOf, edges, direction, type)).as("bounded shortestPath %s walks edges that exist", boundedPath)
              .isTrue();
        }

        // the weighted functions
        final double[] cheapest = bellmanFordReference(vertexCount, edges, source, direction, Integer.MAX_VALUE);
        if (!negative) {
          for (final String function : new String[] { "dijkstra", "astar" }) {
            final List<RID> weighted = path(
                function + "(" + rids[source] + ", " + rids[target] + ", 'weight', {direction: '" + direction + "'})");
            checks++;
            assertWeightedPath(function, weighted, rids[source], rids[target], indexOf, edges, direction, cheapest[target]);

            final int depth = random.nextInt(6);
            final double[] bounded = bellmanFordReference(vertexCount, edges, source, direction, depth);
            final List<RID> weightedBounded = path(function + "(" + rids[source] + ", " + rids[target]
                + ", 'weight', {direction: '" + direction + "', maxDepth: " + depth + "})");
            checks++;
            assertWeightedPath(function + " maxDepth " + depth, weightedBounded, rids[source], rids[target], indexOf, edges, direction,
                bounded[target]);
            assertThat(weightedBounded.size() - 1).isLessThanOrEqualTo(Math.max(depth, 0));
          }
        }
        // negative weights are fine for Bellman-Ford as long as no edge is walked both ways
        if (!negative || !"BOTH".equals(direction)) {
          final List<RID> weighted = path(
              "bellmanFord(" + rids[source] + ", " + rids[target] + ", 'weight', '" + direction + "')");
          checks++;
          assertWeightedPath("bellmanFord", weighted, rids[source], rids[target], indexOf, edges, direction, cheapest[target]);
        }
      }
    }
    assertThat(checks).as("the oracle ran its checks").isGreaterThan(1_000);
  }

  private void assertWeightedPath(final String what, final List<RID> path, final RID from, final RID to, final Map<RID, Integer> indexOf,
      final List<Edge> edges, final String direction, final double expectedCost) {
    if (expectedCost == INFINITY) {
      assertThat(path).as("%s: no path expected", what).isEmpty();
      return;
    }
    assertThat(path).as("%s: a path of cost %s expected, edges %s", what, expectedCost, edges).isNotEmpty();
    assertConnects(what, path, from, to);
    final double cost = cost(path, indexOf, edges, direction);
    assertThat(cost).as("%s walks edges that exist: %s", what, path).isNotNaN();
    assertThat(cost).as("%s cost, path %s edges %s", what, path, edges).isCloseTo(expectedCost, within(1e-9));
  }

  /** A path runs from the requested source to the requested target: an edge-valid path of the right length can still join other vertices. */
  private static void assertConnects(final String what, final List<RID> path, final RID from, final RID to) {
    assertThat(path).as("%s starts at the source", what).first().isEqualTo(from);
    assertThat(path).as("%s ends at the target", what).last().isEqualTo(to);
  }

  private List<RID> path(final String function) {
    final List<RID> path = new ArrayList<>();
    try (final ResultSet resultSet = database.query("sql", "SELECT " + function + " AS p")) {
      final Object value = resultSet.next().getProperty("p");
      assertThat(value).as("%s answers a list of vertices (empty when unreachable)", function).isInstanceOf(Collection.class);
      for (final Object element : (Collection<?>) value)
        path.add(((Identifiable) element).getIdentity());
    }
    return path;
  }

  private static int bfs(final int vertexCount, final List<Edge> edges, final int source, final int target, final String direction,
      final String type) {
    final int[] distance = new int[vertexCount];
    Arrays.fill(distance, -1);
    distance[source] = 0;
    final ArrayDeque<Integer> queue = new ArrayDeque<>();
    queue.add(source);
    while (!queue.isEmpty()) {
      final int node = queue.poll();
      for (final Edge edge : edges) {
        if (type != null && !edge.type.equals(type))
          continue;
        if (forward(direction) && edge.from == node && distance[edge.to] < 0) {
          distance[edge.to] = distance[node] + 1;
          queue.add(edge.to);
        }
        if (backward(direction) && edge.to == node && distance[edge.from] < 0) {
          distance[edge.from] = distance[node] + 1;
          queue.add(edge.from);
        }
      }
    }
    return distance[target];
  }

  private static boolean walksEdges(final List<RID> path, final Map<RID, Integer> indexOf, final List<Edge> edges,
      final String direction, final String type) {
    for (int i = 0; i + 1 < path.size(); i++) {
      final Integer from = indexOf.get(path.get(i));
      final Integer to = indexOf.get(path.get(i + 1));
      if (from == null || to == null)
        return false;
      boolean found = false;
      for (final Edge edge : edges) {
        if (type != null && !edge.type.equals(type))
          continue;
        if (forward(direction) && edge.from == from && edge.to == to || backward(direction) && edge.to == from && edge.from == to)
          found = true;
      }
      if (!found)
        return false;
    }
    return true;
  }

  /** The cost of the path, taking the cheapest of the parallel edges between two consecutive vertices; NaN if one is missing. */
  private static double cost(final List<RID> path, final Map<RID, Integer> indexOf, final List<Edge> edges, final String direction) {
    double cost = 0;
    for (int i = 0; i + 1 < path.size(); i++) {
      final Integer from = indexOf.get(path.get(i));
      final Integer to = indexOf.get(path.get(i + 1));
      if (from == null || to == null)
        return Double.NaN;
      double best = INFINITY;
      for (final Edge edge : edges) {
        if (forward(direction) && edge.from == from && edge.to == to)
          best = Math.min(best, edge.weight);
        if (backward(direction) && edge.to == from && edge.from == to)
          best = Math.min(best, edge.weight);
      }
      if (best == INFINITY)
        return Double.NaN;
      cost += best;
    }
    return cost;
  }

  /** The cheapest cost to every vertex using at most {@code maxHops} edges (all of them for Integer.MAX_VALUE). */
  private static double[] bellmanFordReference(final int vertexCount, final List<Edge> edges, final int source,
      final String direction, final int maxHops) {
    double[] distance = new double[vertexCount];
    Arrays.fill(distance, INFINITY);
    distance[source] = 0;
    final int rounds = maxHops == Integer.MAX_VALUE ? vertexCount + 1 : maxHops;
    for (int round = 0; round < rounds; round++) {
      final double[] next = distance.clone();
      for (final Edge edge : edges) {
        if (forward(direction) && distance[edge.from] + edge.weight < next[edge.to])
          next[edge.to] = distance[edge.from] + edge.weight;
        if (backward(direction) && distance[edge.to] + edge.weight < next[edge.from])
          next[edge.from] = distance[edge.to] + edge.weight;
      }
      distance = next;
    }
    return distance;
  }
}
