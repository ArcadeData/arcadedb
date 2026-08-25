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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Stream;

/**
 * Procedure: algo.kShortestPaths(startNode, endNode, k, relTypes?, weightProperty?)
 * <p>
 * Finds the k shortest paths between two nodes using Yen's algorithm. Returns up to k
 * paths in ascending order of total weight.
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (a:City {name:'A'}), (b:City {name:'Z'})
 * CALL algo.kShortestPaths(a, b, 3, 'ROAD', 'distance')
 * YIELD path, weight, rank
 * RETURN rank, weight
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoKShortestPaths extends AbstractAlgoProcedure {
  public static final String NAME = "algo.kShortestPaths";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 5;
  }

  @Override
  public String getDescription() {
    return "Finds the k shortest paths between two nodes using Yen's algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("path", "weight", "rank");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode        = extractVertex(args[0], "startNode");
    final Vertex endNode          = extractVertex(args[1], "endNode");
    final int k                   = extractCount((Number) args[2], "k");
    final String[] relTypes       = args.length > 3 ? extractRelTypes(args[3]) : null;
    final String weightProperty   = args.length > 4 ? extractString(args[4], "weightProperty") : null;

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);
    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    final int startIdx = graph.indexOf(startNode.getIdentity());
    final int endIdx   = graph.indexOf(endNode.getIdentity());
    if (startIdx < 0 || endIdx < 0)
      return Stream.empty();

    // Yen's algorithm is dense here: a nodeCount x nodeCount weight matrix for the whole run - 800 MB at
    // 10 000 nodes, with no knob involved. Reserved before it is allocated so that a graph too large for the
    // dense formulation is a client error naming the node count and the budget, rather than an
    // OutOfMemoryError. The two spur masks beside it are nodeCount-sized and allocated once for the whole call
    // (see below), so they are a rounding error next to the matrix and are priced with it rather than apart.
    final MemoryBudget memory = graph.memory();
    memory.reserve(saturatingSum(matrixBytes(n, n, DOUBLE_BYTES), matrixBytes(2, n, BOOLEAN_BYTES)),
        "the weight matrix and the spur masks",
        "a double matrix of " + n + " x " + n + " nodes and two boolean masks of " + n + " nodes");

    // Build weighted adjacency matrix (OUT direction). Edge collection and weight extraction go through
    // GraphData.weightedAdjacency (issue #6316), the same shared helper algo.steinerTree and algo.mst read
    // weights through, rather than a hand-rolled getEdges() walk. Parallel edges collapse to the cheapest one,
    // same as before.
    final double[][] weightMatrix = new double[n][n];
    for (double[] row : weightMatrix)
      Arrays.fill(row, Double.MAX_VALUE);

    final String weightProp = weightProperty != null && !weightProperty.isEmpty() ? weightProperty : null;
    final GraphData.WeightedAdjacency weighted = graph.weightedAdjacency(guard, Vertex.DIRECTION.OUT, weightProp, relTypes);
    final int[][] neighbors = weighted.neighbors();
    final double[][] edgeWeights = weighted.weights();

    for (int i = 0; i < n; i++) {
      // One row of the matrix is O(n) and one vertex's edges are O(degree); either is more than a flag test.
      guard.check();
      weightMatrix[i][i] = 0.0;
      final int[] row = neighbors[i];
      final double[] rowWeights = edgeWeights[i];
      for (int e = 0; e < row.length; e++) {
        final int j = row[e];
        final double w = rowWeights[e];
        if (w < weightMatrix[i][j])
          weightMatrix[i][j] = w;
      }
    }

    // Yen's k-shortest paths. The loop below (`for (int ki = 1; ki < k; ki++)`) terminates as soon
    // as no more candidate paths exist, so k itself is safe to leave unbounded (saturating a huge k
    // to "as many k-shortest-paths as exist" is the right reading here) - but it must not be used
    // directly as an eager ArrayList capacity, since that risks a multi-GB allocation attempt for a
    // huge k (e.g. algo.kShortestPaths(a, b, 2147483648)) that will never be filled.
    final List<int[]> kPaths     = new ArrayList<>(Math.min(k, n));
    final List<Double> kWeights  = new ArrayList<>(Math.min(k, n));

    // Find first shortest path with Dijkstra
    final int[] firstPath = dijkstra(weightMatrix, n, startIdx, endIdx, null, guard);
    if (firstPath == null)
      return Stream.empty();

    kPaths.add(firstPath);
    kWeights.add(pathWeight(firstPath, weightMatrix));

    // Candidate paths, as parallel lists of the path and its cost.
    final List<int[]> candidatePaths = new ArrayList<>();
    final List<Double> candidateCosts = new ArrayList<>();

    // The two masks Yen's spur loop needs, allocated once for the whole call and cleared entry by entry
    // between spur nodes (issue #6289).
    //
    // `removedSpurTargets` used to be a nodeCount x nodeCount boolean matrix allocated fresh per spur node -
    // ~1 MB per allocation at 1000 nodes, ~200 MB through the young generation for a single k=10 call over a
    // 20-hop path. It never needed a row per source: every edge it removes leaves the SAME node. The removal
    // loop below keeps an edge (p[i], p[i+1]) of a previously-found path only when that path shares the root
    // p[0..i] with prevPath, and prevPath[i] IS the spur node - so p[i] == spurNode for every entry it ever
    // set, and the other n-1 rows were allocated, zeroed and read only to prove they were empty. One row
    // indexed by target says exactly as much, in O(nodeCount) rather than O(nodeCount²).
    //
    // Cleared rather than reallocated because the entries set are the few the two loops below can name: at
    // most one per previously-found path, and one per root-path node. Clearing exactly those is O(paths + i),
    // where an Arrays.fill would be O(nodeCount) and a fresh allocation O(nodeCount) plus the zeroing.
    final boolean[] removedSpurTargets = new boolean[n];
    final boolean[] removedNodes = new boolean[n];

    for (int ki = 1; ki < k; ki++) {
      // Yen's is O(k x pathLength x V²) over a dense matrix, and `k` is deliberately left unbounded above
      // because the loop stops as soon as no candidate remains. "Unbounded but self-terminating" still needs a
      // way out: nothing here could observe a thread interrupt or arcadedb.command.timeout before issue #6302,
      // so a k that ran for minutes ran for minutes. The two checkpoints below cost one flag test per spur node
      // and per matrix row respectively, both already O(V) bodies.
      guard.check();
      final int[] prevPath = kPaths.get(ki - 1);

      for (int i = 0; i < prevPath.length - 1; i++) {
        // One spur node is a whole Dijkstra over the dense matrix, i.e. O(V²).
        guard.check();
        final int spurNode = prevPath[i];
        final int[] rootPath = Arrays.copyOf(prevPath, i + 1);

        // Remove the edges leaving the spur node that are part of previously-found paths sharing this root
        markRemovedSpurTargets(kPaths, prevPath, i, removedSpurTargets, true);

        // Remove root path nodes (except spurNode) from the graph
        for (int r = 0; r < i; r++)
          removedNodes[rootPath[r]] = true;

        // Find spur path from spurNode to endIdx
        final int[] spurPath = dijkstra(weightMatrix, n, spurNode, endIdx,
            (u, v) -> (u == spurNode && removedSpurTargets[v]) || removedNodes[u] || removedNodes[v], guard);

        // Both masks are shared across spur nodes, so they are handed back empty for the next one.
        markRemovedSpurTargets(kPaths, prevPath, i, removedSpurTargets, false);
        for (int r = 0; r < i; r++)
          removedNodes[rootPath[r]] = false;

        if (spurPath != null) {
          // Build total path = rootPath + spurPath (skip duplicate spurNode)
          final int[] totalPath = new int[rootPath.length - 1 + spurPath.length];
          System.arraycopy(rootPath, 0, totalPath, 0, rootPath.length - 1);
          System.arraycopy(spurPath, 0, totalPath, rootPath.length - 1, spurPath.length);

          // The cost of the concatenated path, walked once. The incremental form this replaced - root cost
          // plus spur cost, corrected for the prefixes counted twice - computed the same number four ways and
          // then threw it away in favour of this line, at the price of two Arrays.copyOf and four pathWeight
          // walks per spur node.
          final double fullCost = pathWeight(totalPath, weightMatrix);

          // Check if already in candidates or kPaths
          boolean duplicate = false;
          for (final int[] cp : candidatePaths) {
            if (Arrays.equals(cp, totalPath)) {
              duplicate = true;
              break;
            }
          }
          for (final int[] kp : kPaths) {
            if (Arrays.equals(kp, totalPath)) {
              duplicate = true;
              break;
            }
          }
          if (!duplicate) {
            candidatePaths.add(totalPath);
            candidateCosts.add(fullCost);
          }
        }
      }

      if (candidatePaths.isEmpty())
        break;

      // Find best candidate
      int bestIdx = 0;
      double bestCost = candidateCosts.get(0);
      for (int ci = 1; ci < candidateCosts.size(); ci++) {
        if (candidateCosts.get(ci) < bestCost) {
          bestCost = candidateCosts.get(ci);
          bestIdx = ci;
        }
      }

      kPaths.add(candidatePaths.remove(bestIdx));
      kWeights.add(candidateCosts.remove(bestIdx));
    }

    final List<Result> results = new ArrayList<>(kPaths.size());
    for (int i = 0; i < kPaths.size(); i++) {
      final int[] path = kPaths.get(i);
      final List<RID> pathRids = new ArrayList<>(path.length);
      for (final int idx : path)
        pathRids.add(graph.getRID(idx));

      final ResultInternal r = new ResultInternal();
      r.setProperty("path", buildPath(pathRids, db));
      r.setProperty("weight", kWeights.get(i));
      r.setProperty("rank", i + 1);
      results.add(r);
    }
    return results.stream();
  }

  @FunctionalInterface
  private interface EdgeFilter {
    boolean isRemoved(int u, int v);
  }

  /**
   * Marks (or unmarks) the targets of the edges Yen's removes for the spur node {@code prevPath[spurIndex]}:
   * the successor of that node in every already-found path whose first {@code spurIndex + 1} nodes match
   * {@code prevPath}.
   * <p>
   * The same traversal both sets and clears, so the mask handed to the next spur node is empty without an
   * {@code Arrays.fill} over the node count or a fresh allocation - it touches only the entries it can name.
   */
  private static void markRemovedSpurTargets(final List<int[]> kPaths, final int[] prevPath, final int spurIndex,
      final boolean[] removedSpurTargets, final boolean marked) {
    for (final int[] p : kPaths) {
      if (p.length <= spurIndex + 1)
        continue;
      boolean sameRoot = true;
      for (int r = 0; r <= spurIndex; r++) {
        if (p[r] != prevPath[r]) {
          sameRoot = false;
          break;
        }
      }
      if (sameRoot)
        // p[spurIndex] == prevPath[spurIndex] == the spur node, which is why only the target needs indexing.
        removedSpurTargets[p[spurIndex + 1]] = marked;
    }
  }

  private static int[] dijkstra(final double[][] w, final int n, final int src, final int dst,
      final EdgeFilter removed, final WorkGuard guard) {
    final double[] dist = new double[n];
    final int[] prev    = new int[n];
    Arrays.fill(dist, Double.MAX_VALUE);
    Arrays.fill(prev, -1);
    dist[src] = 0.0;

    // PriorityQueue of {cost, nodeIdx}
    final PriorityQueue<double[]> pq = new PriorityQueue<>(Comparator.comparingDouble(a -> a[0]));
    pq.offer(new double[] { 0.0, src });

    while (!pq.isEmpty()) {
      // Relaxing one settled node scans the whole matrix row, so a single Dijkstra here is O(V²) and has to be
      // abortable from inside rather than only between spur nodes.
      guard.check();
      final double[] top  = pq.poll();
      final double cost   = top[0];
      final int u         = (int) top[1];
      if (cost > dist[u])
        continue;
      if (u == dst)
        break;
      for (int v = 0; v < n; v++) {
        if (w[u][v] == Double.MAX_VALUE)
          continue;
        if (removed != null && removed.isRemoved(u, v))
          continue;
        final double newDist = dist[u] + w[u][v];
        if (newDist < dist[v]) {
          dist[v] = newDist;
          prev[v] = u;
          pq.offer(new double[] { newDist, v });
        }
      }
    }

    if (dist[dst] == Double.MAX_VALUE)
      return null;

    // Reconstruct path
    final int[] tmp = new int[n];
    int len = 0;
    int cur = dst;
    while (cur != -1) {
      tmp[len++] = cur;
      if (cur == src)
        break;
      cur = prev[cur];
      if (len > n)
        return null; // cycle guard
    }
    final int[] path = new int[len];
    for (int i = 0; i < len; i++)
      path[i] = tmp[len - 1 - i];
    return path;
  }

  private static double pathWeight(final int[] path, final double[][] w) {
    double total = 0.0;
    for (int i = 0; i < path.length - 1; i++) {
      final double edge = w[path[i]][path[i + 1]];
      if (edge == Double.MAX_VALUE)
        return Double.MAX_VALUE;
      total += edge;
    }
    return total;
  }
}
