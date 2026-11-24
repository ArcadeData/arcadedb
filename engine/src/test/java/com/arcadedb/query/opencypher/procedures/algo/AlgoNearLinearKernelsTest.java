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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression tests for issues #12168 ({@code algo.densestSubgraph}), #12169 ({@code algo.louvain}), #12170
 * ({@code algo.voteRank}) and #12171 ({@code algo.hierarchicalClustering}): four procedures whose answer needed a
 * whole-graph scan per step, now near-linear.
 * <p>
 * Each kernel is checked against what it replaced on random multigraphs (self-loops and parallel edges included): the
 * former round-by-round algorithm, kept here as the reference, for the three whose answer the change must not move
 * (hierarchical clustering, vote rank, Louvain), and the definition of a greedy peeling for the densest subgraph, whose
 * tie-break among equal degrees changed on purpose. Then each runs on a graph the former code needed minutes or more for.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoNearLinearKernelsTest {
  private static final WorkGuard NO_DEADLINE = WorkGuard.forDeadline("the test", Long.MAX_VALUE);

  /** A directed multigraph: its edges, and the OUT and BOTH adjacency an algorithm reads, BOTH listing OUT then IN. */
  private record Graph(int n, int[] src, int[] dst, double[] weight, int[][] out, int[][] both, double[][] bothWeights) {
    static Graph of(final int n, final int[] src, final int[] dst, final double[] weight) {
      final int[] outDegree = new int[n];
      final int[] bothDegree = new int[n];
      for (int e = 0; e < src.length; e++) {
        outDegree[src[e]]++;
        bothDegree[src[e]]++;
        bothDegree[dst[e]]++;
      }
      final int[][] out = new int[n][];
      final int[][] both = new int[n][];
      final double[][] bothWeights = new double[n][];
      for (int i = 0; i < n; i++) {
        out[i] = new int[outDegree[i]];
        both[i] = new int[bothDegree[i]];
        bothWeights[i] = new double[bothDegree[i]];
      }
      final int[] outFill = new int[n];
      final int[] bothFill = new int[n];
      for (int e = 0; e < src.length; e++) {
        out[src[e]][outFill[src[e]]++] = dst[e];
        bothWeights[src[e]][bothFill[src[e]]] = weight[e];
        both[src[e]][bothFill[src[e]]++] = dst[e];
      }
      for (int e = 0; e < src.length; e++) {
        bothWeights[dst[e]][bothFill[dst[e]]] = weight[e];
        both[dst[e]][bothFill[dst[e]]++] = src[e];
      }
      return new Graph(n, src, dst, weight, out, both, bothWeights);
    }
  }

  /**
   * Random edges, a fraction of them inside small planted groups so triangles (and ties between them) are common, plus
   * the odd self-loop and parallel edge. Weights are small integers, so every sum is exact whatever its order.
   */
  private static Graph randomGraph(final Random random, final int n, final int edges) {
    final int[] src = new int[edges];
    final int[] dst = new int[edges];
    final double[] weight = new double[edges];
    final int group = 2 + random.nextInt(6);
    for (int e = 0; e < edges; e++) {
      final int u = random.nextInt(n);
      final int roll = random.nextInt(20);
      final int v;
      if (roll == 0)
        v = u; // self-loop
      else if (roll == 1 && e > 0) {
        v = dst[e - 1]; // parallel to the previous edge
        src[e] = src[e - 1];
        dst[e] = v;
        weight[e] = 1 + random.nextInt(4);
        continue;
      } else if (roll < 12)
        v = Math.min(n - 1, (u / group) * group + random.nextInt(group)); // inside u's group
      else
        v = random.nextInt(n);
      src[e] = u;
      dst[e] = v;
      weight[e] = 1 + random.nextInt(4);
    }
    return Graph.of(n, src, dst, weight);
  }

  /** Barabasi-Albert, {@code m} edges from each new node to older ones picked in proportion to their degree. */
  private static Graph powerLawGraph(final Random random, final int n, final int m) {
    final int edges = (n - m) * m;
    final int[] src = new int[edges];
    final int[] dst = new int[edges];
    final double[] weight = new double[edges];
    final int[] endpoints = new int[2 * edges + m];
    int endpointCount = 0;
    for (int i = 0; i < m; i++)
      endpoints[endpointCount++] = i;
    int e = 0;
    for (int u = m; u < n; u++) {
      final int before = endpointCount;
      for (int k = 0; k < m; k++) {
        final int v = endpoints[random.nextInt(before)];
        src[e] = u;
        dst[e] = v;
        weight[e++] = 1.0;
        endpoints[endpointCount++] = u;
        endpoints[endpointCount++] = v;
      }
    }
    return Graph.of(n, src, dst, weight);
  }

  private static Graph uniformGraph(final Random random, final int n, final int edges) {
    final int[] src = new int[edges];
    final int[] dst = new int[edges];
    final double[] weight = new double[edges];
    for (int e = 0; e < edges; e++) {
      src[e] = random.nextInt(n);
      dst[e] = random.nextInt(n);
      weight[e] = 1.0;
    }
    return Graph.of(n, src, dst, weight);
  }

  // ── algo.hierarchicalClustering (#12171) ──────────────────────────────────────────────────────────────────────────

  @Test
  void hierarchicalClusteringMergesExactlyAsTheRoundByRoundScanDid() {
    final Random random = new Random(12171);
    for (int round = 0; round < 80; round++) {
      final int n = 1 + random.nextInt(150);
      final Graph graph = randomGraph(random, n, random.nextInt(5 * n + 1));
      for (final int target : new int[] { 1, 2, 3, n / 4 + 1, n }) {
        final int clamped = Math.max(1, Math.min(target, n));
        assertThat(AlgoHierarchicalClustering.cluster(graph.both(), clamped, NO_DEADLINE))
            .as("round %d, %d nodes, %d edges, %d clusters", round, n, graph.src().length, clamped)
            .containsExactly(referenceHierarchicalClustering(graph.both(), clamped));
      }
    }
  }

  /**
   * The adjacency is read one row at a time, so an edge deleted between two row reads can be left in one row only. The
   * forward rows are filled from whichever row names the edge, so such an edge is still one edge, where looking for its
   * mirror entry failed with an {@code ArrayIndexOutOfBoundsException}.
   */
  @Test
  void anEdgeListedByOneRowOnlyStillMergesWhicheverRowListsIt() {
    // listed by the lower-index end only, then by the higher-index end only
    assertThat(AlgoHierarchicalClustering.cluster(new int[][] { { 1 }, {} }, 1, NO_DEADLINE)).containsExactly(0, 0);
    assertThat(AlgoHierarchicalClustering.cluster(new int[][] { {}, { 0 } }, 1, NO_DEADLINE)).containsExactly(0, 0);
    // a triangle whose edges are each listed by one end only, in both orientations, still closes: the chain 0-1-2 merges
    assertThat(AlgoHierarchicalClustering.cluster(new int[][] { { 1 }, { 2 }, { 0 } }, 1, NO_DEADLINE)).containsExactly(0, 0, 0);
    assertThat(AlgoHierarchicalClustering.cluster(new int[][] { {}, { 0 }, { 1, 0 } }, 1, NO_DEADLINE)).containsExactly(0, 0, 0);
  }

  @Test
  void hierarchicalClusteringToleratesAnEdgeListedByOneRowOnly() {
    final Random random = new Random(121710);
    for (int round = 0; round < 80; round++) {
      final int n = 2 + random.nextInt(120);
      final int[][] adj = randomGraph(random, n, random.nextInt(5 * n + 1)).both();
      for (int u = 0; u < n; u++)
        if (adj[u].length > 0 && random.nextInt(4) == 0)
          adj[u] = Arrays.copyOf(adj[u], random.nextInt(adj[u].length)); // drops the tail of the row, mirrors stay
      final int target = 1 + random.nextInt(n);

      final int[] cluster = AlgoHierarchicalClustering.cluster(adj, target, NO_DEADLINE);

      assertThat(cluster).as("round %d", round).hasSize(n);
      final int clusters = Arrays.stream(cluster).max().orElse(-1) + 1;
      assertThat(clusters).as("round %d", round).isGreaterThanOrEqualTo(target);
      for (int i = 0; i < n; i++)
        assertThat(cluster[i]).as("round %d: ids are numbered in order of first appearance", round)
            .isLessThanOrEqualTo(i == 0 ? 0 : Arrays.stream(cluster, 0, i).max().orElse(-1) + 1);
    }
  }

  /** The former algorithm: one merge per round, along the first edge of highest similarity in scan order. */
  private static int[] referenceHierarchicalClustering(final int[][] adj, final int targetClusters) {
    final int n = adj.length;
    final BitSet[] neighborSets = new BitSet[n];
    for (int i = 0; i < n; i++) {
      neighborSets[i] = new BitSet(n);
      neighborSets[i].set(i);
      for (final int j : adj[i])
        neighborSets[i].set(j);
    }
    final int[] parent = new int[n];
    for (int i = 0; i < n; i++)
      parent[i] = i;
    int clusters = n;
    while (clusters > targetClusters) {
      int bestU = -1;
      int bestV = -1;
      int bestSim = -1;
      for (int u = 0; u < n; u++) {
        final int rootU = find(parent, u);
        for (final int v : adj[u]) {
          if (v <= u)
            continue;
          final int rootV = find(parent, v);
          if (rootU == rootV)
            continue;
          final BitSet intersection = (BitSet) neighborSets[u].clone();
          intersection.and(neighborSets[v]);
          if (intersection.cardinality() > bestSim) {
            bestSim = intersection.cardinality();
            bestU = rootU;
            bestV = rootV;
          }
        }
      }
      if (bestU < 0)
        break;
      parent[bestV] = bestU;
      clusters--;
    }
    final Map<Integer, Integer> remap = new HashMap<>();
    final int[] cluster = new int[n];
    for (int i = 0; i < n; i++)
      cluster[i] = remap.computeIfAbsent(find(parent, i), r -> remap.size());
    return cluster;
  }

  private static int find(final int[] parent, int x) {
    while (parent[x] != x)
      x = parent[x];
    return x;
  }

  // ── algo.voteRank (#12170) ────────────────────────────────────────────────────────────────────────────────────────

  @Test
  void voteRankElectsExactlyAsTheFullRecomputeDid() {
    final Random random = new Random(12170);
    for (int round = 0; round < 120; round++) {
      final int n = 1 + random.nextInt(200);
      final Graph graph = randomGraph(random, n, random.nextInt(4 * n + 1));
      for (final int topK : new int[] { 1, 3, n / 2 + 1, Integer.MAX_VALUE }) {
        final int limit = Math.min(topK, n);
        assertThat(AlgoVoteRank.elect(graph.out(), limit, NO_DEADLINE))
            .as("round %d, %d nodes, %d edges, topK %d", round, n, graph.src().length, limit)
            .containsExactly(referenceVoteRank(graph.out(), limit));
      }
    }
  }

  /**
   * Sources point at every center and every center at every sink: each election of a center lowers every sink score, so
   * the lazy heap receives a fresh entry per sink per election and its stale entries pile up far past the node count.
   * The heap is rebuilt from the live scores when it fills its fixed capacity (twice the node count), so it never grows
   * past what the call reserved, and the elections are still those of the full recompute.
   */
  @Test
  void voteRankRebuildsItsHeapWhenStaleEntriesFillIt() {
    final int sources = 61, centers = 60, sinks = 60;
    final int n = sources + centers + sinks;
    final int edges = sources * centers + centers * sinks;
    final int[] src = new int[edges];
    final int[] dst = new int[edges];
    int e = 0;
    for (int s = 0; s < sources; s++)
      for (int c = 0; c < centers; c++) {
        src[e] = s;
        dst[e++] = sources + c;
      }
    for (int c = 0; c < centers; c++)
      for (int k = 0; k < sinks; k++) {
        src[e] = sources + c;
        dst[e++] = sources + centers + k;
      }
    final Graph graph = Graph.of(n, src, dst, new double[edges]);
    assertThat(centers * sinks).as("pushes the centers' elections cause, against the heap capacity")
        .isGreaterThan(2 * n + AlgoVoteRank.ScoreHeap.SLACK);

    assertThat(AlgoVoteRank.elect(graph.out(), n, NO_DEADLINE)).containsExactly(referenceVoteRank(graph.out(), n));
  }

  /** The former algorithm: a scan of every node for the winner and a full recompute of every score per round. */
  private static int[] referenceVoteRank(final int[][] adjOut, final int limit) {
    final int n = adjOut.length;
    final double[] voteScore = new double[n];
    final double[] votingAbility = new double[n];
    for (int i = 0; i < n; i++) {
      votingAbility[i] = 1.0;
      for (final int j : adjOut[i])
        voteScore[j]++;
    }
    final boolean[] elected = new boolean[n];
    final List<Integer> order = new ArrayList<>();
    while (order.size() < limit) {
      int best = -1;
      double bestScore = -1.0;
      for (int i = 0; i < n; i++)
        if (!elected[i] && voteScore[i] > bestScore) {
          bestScore = voteScore[i];
          best = i;
        }
      if (best == -1 || bestScore <= 0.0)
        break;
      elected[best] = true;
      order.add(best);
      final int deg = adjOut[best].length;
      if (deg > 0)
        for (final int neighbor : adjOut[best])
          if (!elected[neighbor])
            votingAbility[neighbor] = Math.max(0.0, votingAbility[neighbor] - 1.0 / deg);
      for (int v = 0; v < n; v++)
        voteScore[v] = 0.0;
      for (int u = 0; u < n; u++)
        if (!elected[u])
          for (final int v : adjOut[u])
            if (!elected[v])
              voteScore[v] += votingAbility[u];
    }
    return order.stream().mapToInt(Integer::intValue).toArray();
  }

  // ── algo.louvain (#12169) ─────────────────────────────────────────────────────────────────────────────────────────

  @Test
  void louvainMovesExactlyAsTheCommunityDegreeScanDid() {
    final Random random = new Random(12169);
    for (int round = 0; round < 80; round++) {
      final int n = 1 + random.nextInt(150);
      final Graph graph = randomGraph(random, n, random.nextInt(5 * n + 1));
      final int maxIterations = 1 + random.nextInt(10);
      final AlgoLouvain.Communities actual = AlgoLouvain.detect(graph.both(), graph.bothWeights(), maxIterations, 0.0001,
          NO_DEADLINE);
      final AlgoLouvain.Communities expected = referenceLouvain(graph, maxIterations, 0.0001);
      assertThat(actual.community()).as("round %d, %d nodes, %d edges", round, n, graph.src().length)
          .containsExactly(expected.community());
      assertThat(actual.modularity()).as("round %d", round).isEqualTo(expected.modularity());
    }
  }

  /**
   * The scan-based algorithm: each candidate community's total degree summed over every node, the node taken out of its
   * community first so that staying competes with the candidates (a move needs a strictly higher gain), candidates met in
   * ascending id (the lowest id wins equal gains), and the modularity computed from the edge list.
   */
  private static AlgoLouvain.Communities referenceLouvain(final Graph graph, final int maxIterations, final double tolerance) {
    final int n = graph.n();
    final double[] nodeDegree = new double[n];
    double totalWeight = 0.0;
    for (int i = 0; i < n; i++)
      for (final double w : graph.bothWeights()[i]) {
        nodeDegree[i] += w;
        totalWeight += w;
      }
    totalWeight /= 2.0;
    final int[] community = new int[n];
    for (int i = 0; i < n; i++)
      community[i] = i;

    double prevModularity = referenceModularity(graph, community, nodeDegree, totalWeight);
    for (int iter = 0; iter < maxIterations; iter++) {
      boolean changed = false;
      for (int i = 0; i < n; i++) {
        final TreeMap<Integer, Double> weightTo = new TreeMap<>();
        for (int k = 0; k < graph.both()[i].length; k++)
          if (graph.both()[i][k] != i)
            weightTo.merge(community[graph.both()[i][k]], graph.bothWeights()[i][k], Double::sum);
        int bestCommunity = community[i];
        double ownDegree = 0.0;
        for (int j = 0; j < n; j++)
          if (community[j] == community[i])
            ownDegree += nodeDegree[j];
        double bestGain = weightTo.getOrDefault(community[i], 0.0) - nodeDegree[i] * (ownDegree - nodeDegree[i]) / (2.0 * totalWeight);
        for (final Map.Entry<Integer, Double> entry : weightTo.entrySet()) {
          if (entry.getKey() == community[i])
            continue;
          double communityDegree = 0.0;
          for (int j = 0; j < n; j++)
            if (community[j] == entry.getKey())
              communityDegree += nodeDegree[j];
          final double gain = entry.getValue() - nodeDegree[i] * communityDegree / (2.0 * totalWeight);
          if (gain > bestGain) {
            bestGain = gain;
            bestCommunity = entry.getKey();
          }
        }
        if (bestCommunity != community[i]) {
          community[i] = bestCommunity;
          changed = true;
        }
      }
      if (!changed)
        break;
      final double newModularity = referenceModularity(graph, community, nodeDegree, totalWeight);
      if (Math.abs(newModularity - prevModularity) < tolerance)
        break;
      prevModularity = newModularity;
    }
    final double modularity = referenceModularity(graph, community, nodeDegree, totalWeight);
    final Map<Integer, Integer> remap = new HashMap<>();
    final int[] remapped = new int[n];
    for (int i = 0; i < n; i++)
      remapped[i] = remap.computeIfAbsent(community[i], c -> remap.size());
    return new AlgoLouvain.Communities(remapped, modularity);
  }

  private static double referenceModularity(final Graph graph, final int[] community, final double[] nodeDegree,
      final double totalWeight) {
    if (totalWeight == 0.0)
      return 0.0;
    final int n = graph.n();
    final double[] internal = new double[n];
    final double[] degree = new double[n];
    for (int i = 0; i < n; i++)
      degree[community[i]] += nodeDegree[i];
    for (int e = 0; e < graph.src().length; e++)
      if (community[graph.src()[e]] == community[graph.dst()[e]])
        internal[community[graph.src()[e]]] += graph.weight()[e];
    double modularity = 0.0;
    for (int c = 0; c < n; c++) {
      final double share = degree[c] / (2.0 * totalWeight);
      modularity += internal[c] / totalWeight - share * share;
    }
    return modularity;
  }

  // ── algo.densestSubgraph (#12168) ─────────────────────────────────────────────────────────────────────────────────

  /**
   * The peeling is checked against its definition rather than against the former scan, whose lowest-index tie-break
   * the bucket queue does not keep: every peeled node had the minimum degree among the nodes left, the subgraph reported
   * is the densest of the nested ones the peeling went through (the first of equals), and its density is the one counted
   * from scratch on the adjacency, self-loops included.
   */
  @Test
  void densestSubgraphIsAGreedyPeelingAndReportsTheDensityOfItsSubgraph() {
    final Random random = new Random(12168);
    for (int round = 0; round < 200; round++) {
      final int n = 1 + random.nextInt(120);
      final Graph graph = randomGraph(random, n, random.nextInt(5 * n + 1));
      final int[][] adj = graph.both();
      final AlgoDensestSubgraph.Peeling peeling = AlgoDensestSubgraph.peel(adj, NO_DEADLINE);
      final String as = "round " + round + ", " + n + " nodes, " + graph.src().length + " edges";

      final int[] order = peeling.order();
      assertThat(order).as(as).hasSize(n);
      final boolean[] seen = new boolean[n];
      for (final int v : order) {
        assertThat(seen[v]).as(as + ": node %d peeled twice", v).isFalse();
        seen[v] = true;
      }

      final boolean[] removed = new boolean[n];
      double bestDensity = densityOf(adj, removed, n);
      int bestPeeled = 0;
      for (int k = 0; k < n; k++) {
        final double density = densityOf(adj, removed, n - k);
        if (density > bestDensity) {
          bestDensity = density;
          bestPeeled = k;
        }
        if (k == n - 1)
          break; // the node left alone is never peeled
        int minDegree = Integer.MAX_VALUE;
        for (int v = 0; v < n; v++)
          if (!removed[v])
            minDegree = Math.min(minDegree, degreeOf(adj, removed, v));
        assertThat(degreeOf(adj, removed, order[k])).as(as + ": peel %d is not of minimum degree", k).isEqualTo(minDegree);
        removed[order[k]] = true;
      }
      assertThat(peeling.bestPeeled()).as(as).isEqualTo(bestPeeled);
      assertThat(peeling.density()).as(as).isEqualTo(bestDensity);

      final boolean[] outside = new boolean[n];
      for (int k = 0; k < peeling.bestPeeled(); k++)
        outside[order[k]] = true;
      assertThat(peeling.density()).as(as + ": density of the subgraph reported")
          .isEqualTo(densityOf(adj, outside, n - peeling.bestPeeled()));
    }
  }

  /** Entries of v's row towards the nodes not removed, v itself included (a self-loop counts twice, as in a degree). */
  private static int degreeOf(final int[][] adj, final boolean[] removed, final int v) {
    int degree = 0;
    for (final int u : adj[v])
      if (!removed[u])
        degree++;
    return degree;
  }

  /** Edges among the nodes not removed, over their count, counted from scratch. */
  private static double densityOf(final int[][] adj, final boolean[] removed, final int remaining) {
    long entries = 0;
    for (int v = 0; v < adj.length; v++)
      if (!removed[v])
        entries += degreeOf(adj, removed, v);
    return entries / (2.0 * remaining);
  }

  // ── Scale: graphs the former code needed minutes or more for ─────────────────────────────────────────────────────

  /**
   * 200,000 nodes and 600,000 edges, uniform and power-law. Before: densest subgraph about 90 s and vote rank and Louvain
   * well past 90 s by the measurements of the issues (they took 60 s, 90 s and 90 s at 160,000 nodes), hierarchical
   * clustering about 43 s at 10,000 nodes and cubic. Each kernel now takes well under a second on such a graph; the bound
   * leaves an order of magnitude of headroom and still separates near-linear from what they were.
   */
  @Test
  void theFourKernelsAreNearLinear() {
    final Random random = new Random(42);
    for (final Graph graph : new Graph[] { uniformGraph(random, 200_000, 600_000), powerLawGraph(random, 200_000, 3) }) {
      StallAwareStopwatch watch = StallAwareStopwatch.start();
      AlgoDensestSubgraph.peel(graph.both(), NO_DEADLINE);
      watch.assertStayedUnder(15_000, "algo.densestSubgraph peels in O(V + E), not with a scan of every node per peel");

      watch = StallAwareStopwatch.start();
      AlgoVoteRank.elect(graph.out(), graph.n(), NO_DEADLINE);
      watch.assertStayedUnder(15_000, "algo.voteRank updates the scores within two hops of the elected node only");

      watch = StallAwareStopwatch.start();
      AlgoLouvain.detect(graph.both(), graph.bothWeights(), 10, 0.0001, NO_DEADLINE);
      watch.assertStayedUnder(15_000, "algo.louvain keeps the total degree of each community instead of summing it");

      watch = StallAwareStopwatch.start();
      AlgoHierarchicalClustering.cluster(graph.both(), 2, NO_DEADLINE);
      watch.assertStayedUnder(15_000, "algo.hierarchicalClustering is Kruskal over similarities computed once");
    }
  }

  // ── Through the procedures ────────────────────────────────────────────────────────────────────────────────────────

  @Nested
  class ThroughTheProcedures {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-near-linear-kernels");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("N");
      database.getSchema().createEdgeType("E");
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }

    private MutableVertex[] nodes(final int count) {
      final MutableVertex[] nodes = new MutableVertex[count];
      for (int i = 0; i < count; i++)
        nodes[i] = database.newVertex("N").set("name", "n" + i).save();
      return nodes;
    }

    private void clique(final MutableVertex[] nodes, final int from, final int to) {
      for (int i = from; i < to; i++)
        for (int j = i + 1; j < to; j++)
          nodes[i].newEdge("E", nodes[j], true, (Object[]) null).save();
    }

    /**
     * A K4 and a fifth node whose only edge is a self-loop: its degree (2) is the lowest, so it goes first, and the four
     * left have density 6 / 4. The self-loop used to stay in the edge count after its node was peeled: 7 / 4 reported.
     */
    @Test
    void aSelfLoopLeavesWithItsNode() {
      database.transaction(() -> {
        final MutableVertex[] n = nodes(5);
        clique(n, 0, 4);
        n[4].newEdge("E", n[4], true, (Object[]) null).save();
      });

      final Set<String> dense = new HashSet<>();
      double density = -1;
      try (final ResultSet rs = database.query("opencypher",
          "CALL algo.densestSubgraph() YIELD node, inDenseSubgraph, density RETURN node.name AS name, inDenseSubgraph, density")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          if (Boolean.TRUE.equals(row.getProperty("inDenseSubgraph")))
            dense.add(row.getProperty("name"));
          density = ((Number) row.getProperty("density")).doubleValue();
        }
      }
      assertThat(dense).containsExactlyInAnyOrder("n0", "n1", "n2", "n3");
      assertThat(density).isEqualTo(1.5);
    }

    /**
     * Louvain now reads the graph through a view when one covers it. The local moving visits the nodes in index order,
     * and a view numbers them its own way, so the partition can differ from the record path's, as for every
     * order-dependent procedure of the package; what must hold on both paths is that every vertex has one row and that
     * the modularity reported is the one of the partition returned, counted here from the edges.
     */
    @Test
    void louvainReadsTheGraphThroughAView() {
      final List<RID[]> edges = new ArrayList<>();
      database.transaction(() -> {
        final MutableVertex[] n = nodes(8);
        clique(n, 0, 4);
        clique(n, 4, 8);
        n[3].newEdge("E", n[4], true, (Object[]) null).save();
        n[7].newEdge("E", n[7], true, (Object[]) null).save();
      });
      try (final ResultSet rs = database.query("sql", "SELECT @out AS src, @in AS dst FROM E")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          edges.add(new RID[] { row.getProperty("src"), row.getProperty("dst") });
        }
      }

      final BasicCommandContext records = newContext();
      assertLouvainIsConsistent(records, edges);
      assertThat(records.getVariable(CommandContext.CSR_ACCELERATED_VAR)).isNotEqualTo(true);

      final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("louvain-12169").withVertexTypes("N")
          .withEdgeTypes("E").build();
      try {
        final BasicCommandContext csr = newContext();
        assertLouvainIsConsistent(csr, edges);
        assertThat(csr.getVariable(CommandContext.CSR_ACCELERATED_VAR))
            .as("the view must back the call, or this checks the record path twice").isEqualTo(true);
      } finally {
        view.drop();
      }
    }

    private BasicCommandContext newContext() {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      return context;
    }

    private void assertLouvainIsConsistent(final CommandContext context, final List<RID[]> edges) {
      final Map<RID, Integer> community = new HashMap<>();
      double modularity = Double.NaN;
      final Stream<Result> rows = new AlgoLouvain().execute(new Object[0], null, context);
      for (final Iterator<Result> it = rows.iterator(); it.hasNext(); ) {
        final Result row = it.next();
        assertThat(community.put(row.getProperty("node"), ((Number) row.getProperty("communityId")).intValue())).isNull();
        modularity = ((Number) row.getProperty("modularity")).doubleValue();
      }
      assertThat(community).hasSize(8);

      final Map<Integer, Double> internal = new HashMap<>();
      final Map<Integer, Double> degree = new HashMap<>();
      for (final RID[] edge : edges) {
        final int a = community.get(edge[0]);
        final int b = community.get(edge[1]);
        degree.merge(a, 1.0, Double::sum);
        degree.merge(b, 1.0, Double::sum);
        if (a == b)
          internal.merge(a, 1.0, Double::sum);
      }
      final double m = edges.size();
      double expected = 0.0;
      for (final Map.Entry<Integer, Double> entry : degree.entrySet()) {
        final double share = entry.getValue() / (2.0 * m);
        expected += internal.getOrDefault(entry.getKey(), 0.0) / m - share * share;
      }
      assertThat(modularity).isCloseTo(expected, within(1e-12));
    }
  }
}
