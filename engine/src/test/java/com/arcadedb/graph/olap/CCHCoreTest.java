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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Customizable Contraction Hierarchies on plain arrays (issue #9437): every distance the hierarchy answers must be the
 * one a plain Dijkstra on the same arcs answers, and every path it unpacks must be made of real arcs adding up to that
 * distance.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CCHCoreTest {
  private static final double EPS = 1e-9;

  /** Arcs of a directed weighted multigraph, in parallel arrays. */
  private record Arcs(int nodeCount, int[] tails, int[] heads, double[] weights) {
    int count() {
      return tails.length;
    }
  }

  @Test
  void randomSparseDirectedGraphsMatchDijkstra() {
    final Random random = new Random(42);
    for (int round = 0; round < 20; round++) {
      final Arcs arcs = randomGraph(random, 50 + random.nextInt(250), 2.5, false);
      assertMatchesDijkstra(arcs, false, random, 300);
      assertMatchesDijkstra(arcs, true, random, 300);
    }
  }

  @Test
  void gridGraphsMatchDijkstra() {
    final Random random = new Random(7);
    for (final int side : new int[] { 2, 5, 17, 40 }) {
      final Arcs arcs = grid(side, random, true);
      assertMatchesDijkstra(arcs, false, random, 400);
      assertMatchesDijkstra(arcs, true, random, 400);
    }
  }

  @Test
  void oneWayStreetsAreHonoured() {
    // 0 -> 1 -> 2 is cheap one way only; going back has to take the expensive detour through 3
    final Arcs arcs = new Arcs(4, new int[] { 0, 1, 2, 3 }, new int[] { 1, 2, 3, 0 }, new double[] { 1, 1, 10, 10 });
    final CCHTopology topology = CCHTopology.build(arcs.nodeCount(), arcs.tails(), arcs.heads(), arcs.count(), Long.MAX_VALUE);
    final CCHMetric metric = CCHMetric.customize(topology, arcs.tails(), arcs.heads(), arcs.weights(), arcs.count(), false);

    assertThat(metric.distance(0, 2)).isEqualTo(2.0);
    assertThat(metric.distance(2, 0)).isEqualTo(20.0);
    assertThat(metric.shortestPath(2, 0)).containsExactly(2, 3, 0);

    final CCHMetric undirected = CCHMetric.customize(topology, arcs.tails(), arcs.heads(), arcs.weights(), arcs.count(), true);
    assertThat(undirected.distance(2, 0)).isEqualTo(2.0);
    assertThat(undirected.shortestPath(2, 0)).containsExactly(2, 1, 0);
  }

  @Test
  void disconnectedComponentsIsolatedNodesSelfLoopsAndParallelArcs() {
    // component {0,1,2}, component {3,4}, isolated 5, self loop on 1, parallel arcs 0->1 (5 and 2)
    final Arcs arcs = new Arcs(6, new int[] { 0, 0, 1, 1, 3 }, new int[] { 1, 1, 2, 1, 4 }, new double[] { 5, 2, 3, 0.5, 1 });
    final CCHTopology topology = CCHTopology.build(arcs.nodeCount(), arcs.tails(), arcs.heads(), arcs.count(), Long.MAX_VALUE);
    final CCHMetric metric = CCHMetric.customize(topology, arcs.tails(), arcs.heads(), arcs.weights(), arcs.count(), false);

    assertThat(metric.distance(0, 2)).isEqualTo(5.0);
    assertThat(metric.shortestPath(0, 2)).containsExactly(0, 1, 2);
    assertThat(metric.distance(0, 4)).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(metric.shortestPath(0, 4)).isNull();
    assertThat(metric.distance(5, 5)).isEqualTo(0.0);
    assertThat(metric.shortestPath(5, 5)).containsExactly(5);
    assertThat(metric.shortestPath(5, 0)).isNull();
  }

  @Test
  void zeroWeightArcsAndTies() {
    final Random random = new Random(99);
    for (int round = 0; round < 10; round++) {
      final Arcs arcs = randomGraph(random, 80, 3.0, true);
      assertMatchesDijkstra(arcs, false, random, 300);
    }
  }

  @Test
  void customizationFollowsWeightChangesWithoutANewTopology() {
    final Random random = new Random(3);
    final Arcs arcs = grid(20, random, true);
    final CCHTopology topology = CCHTopology.build(arcs.nodeCount(), arcs.tails(), arcs.heads(), arcs.count(), Long.MAX_VALUE);
    for (int round = 0; round < 5; round++) {
      for (int i = 0; i < arcs.weights().length; i++)
        if (random.nextInt(4) == 0)
          arcs.weights()[i] = random.nextInt(100);
      final CCHMetric metric = CCHMetric.customize(topology, arcs.tails(), arcs.heads(), arcs.weights(), arcs.count(), false);
      assertThat(metric).isNotNull();
      assertPairs(arcs, metric, false, random, 200);
    }
  }

  @Test
  void anArcTheTopologyDoesNotKnowIsRefused() {
    final Arcs path = new Arcs(4, new int[] { 0, 1, 2 }, new int[] { 1, 2, 3 }, new double[] { 1, 1, 1 });
    final CCHTopology topology = CCHTopology.build(path.nodeCount(), path.tails(), path.heads(), path.count(), Long.MAX_VALUE);

    // 0 -> 3 is not an arc of the chordal supergraph of a path (whatever the order, a path stays a tree)
    final CCHMetric metric = CCHMetric.customize(topology, new int[] { 0, 1, 2, 0 }, new int[] { 1, 2, 3, 3 },
        new double[] { 1, 1, 1, 1 }, 4, false);
    assertThat(metric).isNull();

    // a node past the topology's node space is refused too
    assertThat(CCHMetric.customize(topology, new int[] { 0, 4 }, new int[] { 1, 0 }, new double[] { 1, 1 }, 2, false)).isNull();
  }

  @Test
  void aDenseGraphPastTheArcBudgetIsRefused() {
    // a complete graph on 60 nodes has 1770 undirected pairs: the supergraph cannot be smaller, so a budget of 100 refuses it
    final int n = 60;
    final int m = n * (n - 1);
    final int[] tails = new int[m];
    final int[] heads = new int[m];
    int k = 0;
    for (int i = 0; i < n; i++)
      for (int j = 0; j < n; j++)
        if (i != j) {
          tails[k] = i;
          heads[k++] = j;
        }
    assertThat(CCHTopology.build(n, tails, heads, m, 100)).isNull();
    assertThat(CCHTopology.build(n, tails, heads, m, Long.MAX_VALUE)).isNotNull();
  }

  @Test
  void nestedDissectionKeepsGridSupergraphSmall() {
    // on a 100x100 grid a good order keeps the chordal supergraph within a small multiple of the input edges (nested
    // dissection measures ~9x here, the n log n fill a grid cannot avoid); a row-major order produces ~50x and climbs ~2
    // side vertices per query, so this guards the quality of the separators
    final Random random = new Random(1);
    final int side = 100;
    final Arcs arcs = grid(side, random, false);
    final CCHTopology topology = CCHTopology.build(arcs.nodeCount(), arcs.tails(), arcs.heads(), arcs.count(), Long.MAX_VALUE);
    final int undirectedEdges = 2 * side * (side - 1);
    final String shape = "arcs " + topology.arcCount() + ", search space " + topology.maxSearchSpace();
    assertThat(topology.arcCount()).as(shape).isLessThan(12 * undirectedEdges);
    assertThat(topology.maxSearchSpace()).as(shape).isLessThan(side * 4);
  }

  // ---------------------------------------------------------------------------------------------------------------

  private static void assertMatchesDijkstra(final Arcs arcs, final boolean undirected, final Random random, final int pairs) {
    final CCHTopology topology = CCHTopology.build(arcs.nodeCount(), arcs.tails(), arcs.heads(), arcs.count(), Long.MAX_VALUE);
    assertThat(topology).isNotNull();
    final CCHMetric metric = CCHMetric.customize(topology, arcs.tails(), arcs.heads(), arcs.weights(), arcs.count(), undirected);
    assertThat(metric).isNotNull();
    assertPairs(arcs, metric, undirected, random, pairs);
  }

  private static void assertPairs(final Arcs arcs, final CCHMetric metric, final boolean undirected, final Random random,
      final int pairs) {
    final int n = arcs.nodeCount();
    for (int p = 0; p < pairs; p++) {
      final int s = random.nextInt(n);
      final double[] expected = dijkstra(arcs, s, undirected);
      final int t = random.nextInt(n);
      final double actual = metric.distance(s, t);
      assertThat(actual).as("distance %d -> %d", s, t).isCloseTo(expected[t], within(EPS));

      final int[] path = metric.shortestPath(s, t);
      if (expected[t] == Double.POSITIVE_INFINITY) {
        assertThat(path).isNull();
        continue;
      }
      assertThat(path).isNotNull();
      assertThat(path[0]).isEqualTo(s);
      assertThat(path[path.length - 1]).isEqualTo(t);
      double sum = 0;
      for (int i = 0; i + 1 < path.length; i++)
        sum += cheapestArc(arcs, path[i], path[i + 1], undirected);
      assertThat(sum).as("path %s weighs its distance", Arrays.toString(path)).isCloseTo(expected[t], within(EPS));
    }
  }

  private static double cheapestArc(final Arcs arcs, final int from, final int to, final boolean undirected) {
    double best = Double.POSITIVE_INFINITY;
    for (int i = 0; i < arcs.count(); i++)
      if ((arcs.tails()[i] == from && arcs.heads()[i] == to) || (undirected && arcs.tails()[i] == to && arcs.heads()[i] == from))
        best = Math.min(best, arcs.weights()[i]);
    assertThat(best).as("arc %d -> %d exists", from, to).isLessThan(Double.POSITIVE_INFINITY);
    return best;
  }

  private static double[] dijkstra(final Arcs arcs, final int source, final boolean undirected) {
    final int n = arcs.nodeCount();
    final int[] offsets = new int[n + 1];
    for (int i = 0; i < arcs.count(); i++) {
      offsets[arcs.tails()[i] + 1]++;
      if (undirected)
        offsets[arcs.heads()[i] + 1]++;
    }
    for (int i = 0; i < n; i++)
      offsets[i + 1] += offsets[i];
    final int[] targets = new int[offsets[n]];
    final double[] costs = new double[offsets[n]];
    final int[] fill = Arrays.copyOf(offsets, n);
    for (int i = 0; i < arcs.count(); i++) {
      targets[fill[arcs.tails()[i]]] = arcs.heads()[i];
      costs[fill[arcs.tails()[i]]++] = arcs.weights()[i];
      if (undirected) {
        targets[fill[arcs.heads()[i]]] = arcs.tails()[i];
        costs[fill[arcs.heads()[i]]++] = arcs.weights()[i];
      }
    }

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
      for (int e = offsets[u]; e < offsets[u + 1]; e++) {
        final double nd = top[0] + costs[e];
        if (nd < dist[targets[e]]) {
          dist[targets[e]] = nd;
          heap.add(new double[] { nd, targets[e] });
        }
      }
    }
    return dist;
  }

  private static Arcs randomGraph(final Random random, final int n, final double avgOutDegree, final boolean zeroWeights) {
    final int m = (int) (n * avgOutDegree);
    final int[] tails = new int[m];
    final int[] heads = new int[m];
    final double[] weights = new double[m];
    for (int i = 0; i < m; i++) {
      tails[i] = random.nextInt(n);
      heads[i] = random.nextInt(n);
      weights[i] = zeroWeights ? random.nextInt(3) : 1 + random.nextInt(50) + random.nextDouble();
    }
    return new Arcs(n, tails, heads, weights);
  }

  /** A side x side grid with arcs both ways between neighbours; one-way streets when {@code oneWays} is set. */
  private static Arcs grid(final int side, final Random random, final boolean oneWays) {
    final int n = side * side;
    final int[] tails = new int[4 * n];
    final int[] heads = new int[4 * n];
    final double[] weights = new double[4 * n];
    int k = 0;
    for (int r = 0; r < side; r++)
      for (int c = 0; c < side; c++) {
        final int u = r * side + c;
        final int[] neighbours = { c + 1 < side ? u + 1 : -1, r + 1 < side ? u + side : -1 };
        for (final int v : neighbours) {
          if (v < 0)
            continue;
          final boolean forward = !oneWays || random.nextInt(5) != 0;
          final boolean backward = !oneWays || !forward || random.nextInt(5) != 0;
          if (forward) {
            tails[k] = u;
            heads[k] = v;
            weights[k++] = 1 + random.nextInt(20);
          }
          if (backward) {
            tails[k] = v;
            heads[k] = u;
            weights[k++] = 1 + random.nextInt(20);
          }
        }
      }
    return new Arcs(n, Arrays.copyOf(tails, k), Arrays.copyOf(heads, k), Arrays.copyOf(weights, k));
  }
}
