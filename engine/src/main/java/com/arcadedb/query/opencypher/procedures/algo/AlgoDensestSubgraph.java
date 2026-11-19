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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.densestSubgraph(relTypes?)
 * <p>
 * Finds the densest subgraph using Charikar's greedy peeling algorithm (2-approximation),
 * with O(V+E) edge processing. Density is defined as edges/nodes ratio for the subgraph.
 * Returns each node with a flag indicating whether it belongs to the densest subgraph found,
 * and the density value of that subgraph.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.densestSubgraph()
 * YIELD node, inDenseSubgraph, density
 * RETURN node.name, inDenseSubgraph, density ORDER BY inDenseSubgraph DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoDensestSubgraph extends AbstractAlgoProcedure {
  public static final String NAME = "algo.densestSubgraph";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 0;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Find the densest subgraph using Charikar's greedy peeling algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "inDenseSubgraph", "density");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = extractRelTypes(argOrDefault(args, 0, null), context);

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    final GraphData graph = loadGraph(db, null, relTypes, context);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH, relTypes);

    int maxDegree = 0;
    for (final int[] row : adj)
      maxDegree = Math.max(maxDegree, row.length);
    // degree, position and peel order per node, one bucket start per degree, and the removed flags
    graph.memory().reserve(saturatingSum(saturatingProduct(3L * n + maxDegree + 1, INT_BYTES), saturatingProduct(n, BOOLEAN_BYTES)),
        "the peeling buckets", n + " nodes, maximum degree " + maxDegree);

    final Peeling peeling = peel(adj, guard);

    final boolean[] inDenseSubgraph = new boolean[n];
    for (int k = peeling.bestPeeled(); k < n; k++)
      inDenseSubgraph[peeling.order()[k]] = true;
    final double finalDensity = peeling.density();

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(i));
      r.setProperty("inDenseSubgraph", inDenseSubgraph[i]);
      r.setProperty("density", finalDensity);
      return (Result) r;
    });
  }

  /**
   * The outcome of a greedy peeling: the nodes in the order they were peeled (the last one is the node left alone at the
   * end), and the densest of the nested subgraphs the peeling went through, which is {@code order[bestPeeled..]}.
   */
  record Peeling(int[] order, int bestPeeled, double density) {
  }

  /**
   * Charikar's greedy peeling over an undirected adjacency ({@code BOTH} rows, so every edge is listed once in the row
   * of each of its ends and a self-loop twice in its node's own row): peel a node of minimum degree in what is left,
   * one at a time, and keep the densest subgraph met on the way, density being edges over nodes.
   * <p>
   * O(V + E) (issue #12168): the nodes not peeled yet sit in one array, bucketed by their current degree in ascending
   * order (the bucket arrays of Batagelj and Zaversnik, the layout {@code algo.kcore} peels with), so the node to peel
   * next is always the first one after the peeled prefix and the prefix itself is the peel order. Removing an edge
   * moves its other end to the bucket one degree lower with a single swap. Unlike the k-core peeling a degree can drop
   * below the one being peeled, so the bucket under the lowest one in use is opened on demand ({@code floor}). The best
   * subgraph is a suffix of the peel order, so it is recorded as a position instead of copied on every improvement.
   * <p>
   * Among nodes of equal degree the bucket order decides, which is deterministic for a given adjacency. Any choice
   * keeps Charikar's guarantee (half the optimal density); the lowest-index rule of the former linear scan was never
   * stable across backings anyway, since a Graph Analytical View numbers the nodes differently from the records.
   * <p>
   * The edge count is kept as twice itself (adjacency entries) so a self-loop leaves with its node: the former count
   * never subtracted it, and every subgraph left after peeling a node with a self-loop was reported denser than it was.
   */
  static Peeling peel(final int[][] adj, final WorkGuard guard) {
    final int n = adj.length;
    final int[] degree = new int[n];
    long entries = 0; // twice the edges among the nodes not peeled yet
    int maxDegree = 0;
    for (int i = 0; i < n; i++) {
      degree[i] = adj[i].length;
      entries += degree[i];
      maxDegree = Math.max(maxDegree, degree[i]);
    }

    // order[0 .. peeled) is the peel order; order[peeled .. n) the nodes left, bucketed by ascending degree, bucket d
    // starting at bucketStart[d]
    final int[] bucketStart = new int[maxDegree + 1];
    for (int i = 0; i < n; i++)
      bucketStart[degree[i]]++;
    for (int d = 0, start = 0; d <= maxDegree; d++) {
      final int count = bucketStart[d];
      bucketStart[d] = start;
      start += count;
    }
    final int[] order = new int[n];
    final int[] position = new int[n];
    for (int i = 0; i < n; i++) {
      position[i] = bucketStart[degree[i]]++;
      order[position[i]] = i;
    }
    for (int d = maxDegree; d > 0; d--)
      bucketStart[d] = bucketStart[d - 1];
    if (n > 0)
      bucketStart[0] = 0;

    final boolean[] removed = new boolean[n];
    double bestDensity = n > 0 ? entries / (2.0 * n) : 0.0;
    int bestPeeled = 0;
    // The lowest degree whose bucket start is current: the buckets below it are empty and their starts stale
    int floor = 0;

    for (int peeled = 0; peeled < n - 1; peeled++) {
      guard.checkPeriodically(peeled);
      final double density = entries / (2.0 * (n - peeled));
      if (density > bestDensity) {
        bestDensity = density;
        bestPeeled = peeled;
      }

      // The first node left has the minimum degree, and is the first of its bucket
      final int v = order[peeled];
      final int d = degree[v];
      bucketStart[d]++;
      floor = d;
      removed[v] = true;

      int selfEntries = 0;
      for (final int u : adj[v]) {
        if (u == v) {
          selfEntries++;
          continue;
        }
        if (removed[u])
          continue;
        final int du = degree[u];
        if (du == floor) {
          bucketStart[du - 1] = bucketStart[du];
          floor = du - 1;
        }
        // Swap u with the first node of its bucket, then shrink the bucket from the front: u is now the last of du - 1
        final int first = bucketStart[du];
        final int pu = position[u];
        final int w = order[first];
        order[pu] = w;
        position[w] = pu;
        order[first] = u;
        position[u] = first;
        bucketStart[du]++;
        degree[u] = du - 1;
      }
      // v's own entries towards the nodes left (itself included) and their mirrors in the other rows
      entries -= 2L * d - selfEntries;
    }

    // The node left alone counts only through its self-loops
    if (n > 1 && entries / 2.0 > bestDensity) {
      bestDensity = entries / 2.0;
      bestPeeled = n - 1;
    }
    return new Peeling(order, bestPeeled, bestDensity);
  }
}
