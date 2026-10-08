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

import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Customizable Contraction Hierarchies against plain Dijkstra on a road-like grid (issue #9437): preparation cost
 * (ordering + contraction, customization) and point-to-point query latency, short and long routes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class CCHBenchmark {

  @Test
  void gridRoutes() {
    for (final int side : new int[] { 100, 300, 600 })
      run(side);
  }

  private static void run(final int side) {
    final Random random = new Random(side);
    final int n = side * side;
    final int[] tails = new int[4 * n];
    final int[] heads = new int[4 * n];
    final double[] weights = new double[4 * n];
    int m = 0;
    for (int r = 0; r < side; r++)
      for (int c = 0; c < side; c++) {
        final int u = r * side + c;
        for (final int v : new int[] { c + 1 < side ? u + 1 : -1, r + 1 < side ? u + side : -1 }) {
          if (v < 0)
            continue;
          tails[m] = u;
          heads[m] = v;
          weights[m++] = 1 + random.nextInt(100);
          tails[m] = v;
          heads[m] = u;
          weights[m++] = 1 + random.nextInt(100);
        }
      }

    long begin = System.nanoTime();
    final CCHTopology topology = CCHTopology.build(n, tails, heads, m, Long.MAX_VALUE);
    final long topologyMs = (System.nanoTime() - begin) / 1_000_000;

    begin = System.nanoTime();
    final CCHMetric metric = CCHMetric.customize(topology, tails, heads, weights, m, false);
    final long customizeMs = (System.nanoTime() - begin) / 1_000_000;

    // adjacency for the plain Dijkstra baseline
    final int[] offsets = new int[n + 1];
    for (int i = 0; i < m; i++)
      offsets[tails[i] + 1]++;
    for (int i = 0; i < n; i++)
      offsets[i + 1] += offsets[i];
    final int[] adjacency = new int[m];
    final double[] adjacencyWeights = new double[m];
    final int[] fill = Arrays.copyOf(offsets, n);
    for (int i = 0; i < m; i++) {
      adjacency[fill[tails[i]]] = heads[i];
      adjacencyWeights[fill[tails[i]]++] = weights[i];
    }

    final int queries = 200;
    final int[][] longRoutes = new int[2][queries];
    final int[][] shortRoutes = new int[2][queries];
    for (int q = 0; q < queries; q++) {
      // long routes: opposite corners of the grid, give or take
      longRoutes[0][q] = random.nextInt(side / 10) * side + random.nextInt(side / 10);
      longRoutes[1][q] = n - 1 - (random.nextInt(side / 10) * side + random.nextInt(side / 10));
      // short routes: a few blocks away
      final int r = 5 + random.nextInt(side - 10);
      final int c = 5 + random.nextInt(side - 10);
      shortRoutes[0][q] = r * side + c;
      shortRoutes[1][q] = (r + random.nextInt(5) - 2) * side + c + random.nextInt(5) - 2;
    }

    final String routes = measure(metric, n, offsets, adjacency, adjacencyWeights, longRoutes, "long") + " | "
        + measure(metric, n, offsets, adjacency, adjacencyWeights, shortRoutes, "short");

    LogManager.instance().log(CCHBenchmark.class, Level.INFO,
        "grid %dx%d: %d nodes, %d arcs -> %d supergraph arcs (%.1fx), search space %d | order+contract %d ms, customize %d ms | "
            + "query scratch %d KB (graph-sized would be %d KB) | %s",
        side, side, n, m, topology.arcCount(), topology.arcCount() / (m / 2.0), topology.maxSearchSpace(), topologyMs,
        customizeMs, 24L * topology.maxSearchSpace() / 1024, 24L * n / 1024, routes);
  }

  private static String measure(final CCHMetric metric, final int n, final int[] offsets, final int[] adjacency,
      final double[] adjacencyWeights, final int[][] routes, final String label) {
    final int queries = routes[0].length;
    for (int q = 0; q < 20; q++) {
      metric.shortestPath(routes[0][q], routes[1][q]);
      dijkstra(n, offsets, adjacency, adjacencyWeights, routes[0][q], routes[1][q]);
    }

    long begin = System.nanoTime();
    for (int q = 0; q < queries; q++)
      metric.shortestPathWithDistance(routes[0][q], routes[1][q]);
    final double cchMicros = (System.nanoTime() - begin) / 1_000.0 / queries;

    begin = System.nanoTime();
    double expected = 0;
    final int dijkstraQueries = Math.min(queries, 40);
    for (int q = 0; q < dijkstraQueries; q++)
      expected += dijkstra(n, offsets, adjacency, adjacencyWeights, routes[0][q], routes[1][q]);
    final double dijkstraMicros = (System.nanoTime() - begin) / 1_000.0 / dijkstraQueries;

    double actual = 0;
    for (int q = 0; q < dijkstraQueries; q++)
      actual += metric.distance(routes[0][q], routes[1][q]);
    assertThat(actual).isEqualTo(expected);
    return String.format("%s: CCH %.1f us vs Dijkstra %.1f us (%.0fx)", label, cchMicros, dijkstraMicros,
        dijkstraMicros / cchMicros);
  }

  private static double dijkstra(final int n, final int[] offsets, final int[] adjacency, final double[] weights,
      final int source, final int target) {
    final double[] dist = new double[n];
    Arrays.fill(dist, Double.POSITIVE_INFINITY);
    dist[source] = 0;
    final PriorityQueue<double[]> heap = new PriorityQueue<>((a, b) -> Double.compare(a[0], b[0]));
    heap.add(new double[] { 0, source });
    while (!heap.isEmpty()) {
      final double[] top = heap.poll();
      final int u = (int) top[1];
      if (top[0] > dist[u])
        continue;
      if (u == target)
        return top[0];
      for (int e = offsets[u]; e < offsets[u + 1]; e++) {
        final double nd = top[0] + weights[e];
        if (nd < dist[adjacency[e]]) {
          dist[adjacency[e]] = nd;
          heap.add(new double[] { nd, adjacency[e] });
        }
      }
    }
    return Double.POSITIVE_INFINITY;
  }
}
