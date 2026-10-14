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
import com.arcadedb.graph.EdgeWeight;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Procedure: algo.kShortestPaths(startNode, endNode, k, relTypes?, weightProperty?, direction?)
 * <p>
 * Finds the k shortest loopless paths between two nodes using Yen's algorithm. Returns up to k
 * paths in ascending order of total weight, each with the relationships it walks.
 * </p>
 * <p>
 * {@code direction} is {@code 'BOTH'} (the default), {@code 'OUT'} or {@code 'IN'}, as for {@code algo.dijkstra},
 * so that the first path is the one {@code algo.dijkstra} returns for the same arguments (issue #9914).
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
    return 6;
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

    final Vertex startNode = extractVertex(args[0], "startNode");
    final Vertex endNode = extractVertex(args[1], "endNode");
    final int k = extractCount((Number) args[2], "k");
    final String[] relTypes = extractRelTypes(argOrDefault(args, 3, null), context);
    final String weightProperty = extractWeightProperty(argOrDefault(args, 4, null), context);
    final String directionArg = extractString(argOrDefault(args, 5, null), "direction");
    final Vertex.DIRECTION direction = directionArg != null && !directionArg.isEmpty() ? parseDirection(directionArg) : Vertex.DIRECTION.BOTH;

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);
    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    final int startIdx = graph.indexOf(startNode.getIdentity());
    final int endIdx = graph.indexOf(endNode.getIdentity());
    if (startIdx < 0 || endIdx < 0)
      return Stream.empty();

    // Yen's runs on the adjacency lists, not on a nodeCount x nodeCount weight matrix: the matrix this replaced was
    // 800 MB at 10 000 nodes and made the procedure unusable on a road network, whose degree is a handful. What Yen's
    // needs beside the lists is node-sized: the distances, predecessors and visit stack of the spur Dijkstra, allocated
    // once and cleared entry by entry between runs, and the two spur masks (issue #6289). The search queue is not priced:
    // it grows with the entries pushed by one search, at most one per edge it relaxes, which the adjacency already holds.
    final MemoryBudget memory = graph.memory();
    memory.reserve(saturatingProduct(n, DOUBLE_BYTES + 2 * INT_BYTES + 2 * BOOLEAN_BYTES), "the path search state",
        "distances, predecessors, a visit stack and two boolean masks of " + n + " nodes");

    final String weightProp = weightProperty != null && !weightProperty.isEmpty() ? weightProperty : null;
    final GraphData.WeightedAdjacency weighted = graph.weightedAdjacency(guard, direction, weightProp, relTypes);
    final int[][] neighbors = weighted.neighbors();
    final double[][] weights = weighted.weights();

    final Search search = new Search(n, neighbors, weights, guard);

    // Yen's k-shortest paths. The loop below (`for (int ki = 1; ki < k; ki++)`) terminates as soon
    // as no more candidate paths exist, so k itself is safe to leave unbounded (saturating a huge k
    // to "as many k-shortest-paths as exist" is the right reading here) - but it must not be used
    // directly as an eager ArrayList capacity, since that risks a multi-GB allocation attempt for a
    // huge k (e.g. algo.kShortestPaths(a, b, 2147483648)) that will never be filled.
    final List<int[]> kPaths = new ArrayList<>(Math.min(k, n));
    final List<Double> kWeights = new ArrayList<>(Math.min(k, n));

    final int[] firstPath = search.shortestPath(startIdx, endIdx, -1);
    if (firstPath == null)
      return Stream.empty();

    kPaths.add(firstPath);
    kWeights.add(search.pathWeight(firstPath));

    // Candidate paths, as parallel lists of the path and its cost, and every path already found or proposed: a spur that
    // rebuilds one of them is not proposed again (a hash lookup, where scanning both lists per spur node made the
    // duplicate check the dominant cost once the dense matrix was gone)
    final List<int[]> candidatePaths = new ArrayList<>();
    final List<Double> candidateCosts = new ArrayList<>();
    final Set<PathKey> known = new HashSet<>();
    known.add(new PathKey(firstPath));

    for (int ki = 1; ki < k; ki++) {
      // Yen's is O(k x pathLength) Dijkstra runs, and `k` is deliberately left unbounded above because the loop
      // stops as soon as no candidate remains. "Unbounded but self-terminating" still needs a way out (#6302): one
      // flag test per spur node, and one per settled node inside each run.
      guard.check();
      final int[] prevPath = kPaths.get(ki - 1);

      for (int i = 0; i < prevPath.length - 1; i++) {
        guard.check();
        final int spurNode = prevPath[i];
        final int[] rootPath = Arrays.copyOf(prevPath, i + 1);

        // Remove the edges leaving the spur node that are part of previously-found paths sharing this root
        markRemovedSpurTargets(kPaths, prevPath, i, search.removedSpurTargets, true);

        // Remove root path nodes (except spurNode) from the graph
        for (int r = 0; r < i; r++)
          search.removedNodes[rootPath[r]] = true;

        // Find spur path from spurNode to endIdx
        final int[] spurPath = search.shortestPath(spurNode, endIdx, spurNode);

        // Both masks are shared across spur nodes, so they are handed back empty for the next one.
        markRemovedSpurTargets(kPaths, prevPath, i, search.removedSpurTargets, false);
        for (int r = 0; r < i; r++)
          search.removedNodes[rootPath[r]] = false;

        if (spurPath != null) {
          // Build total path = rootPath + spurPath (skip duplicate spurNode)
          final int[] totalPath = new int[rootPath.length - 1 + spurPath.length];
          System.arraycopy(rootPath, 0, totalPath, 0, rootPath.length - 1);
          System.arraycopy(spurPath, 0, totalPath, rootPath.length - 1, spurPath.length);

          if (known.add(new PathKey(totalPath))) {
            candidatePaths.add(totalPath);
            candidateCosts.add(search.pathWeight(totalPath));
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

      // Yen's works on node sequences; the relationships are the lightest walkable edge between consecutive nodes in
      // the walked direction, the one the weight was computed from (parallel edges collapse to the cheapest)
      final WeightedPath walked = attachEdges(pathRids, relTypes, direction, weightProp);

      final ResultInternal r = new ResultInternal();
      r.setProperty("path", buildPath(walked.ridsWithEdges(), db));
      r.setProperty("weight", kWeights.get(i));
      r.setProperty("rank", i + 1);
      results.add(r);
    }
    return results.stream();
  }

  /** A node sequence as a hash key. */
  private record PathKey(int[] nodes) {
    @Override
    public boolean equals(final Object obj) {
      return obj instanceof PathKey other && Arrays.equals(nodes, other.nodes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(nodes);
    }
  }

  /**
   * Marks (or unmarks) the targets of the edges Yen's removes for the spur node {@code prevPath[spurIndex]}:
   * the successor of that node in every already-found path whose first {@code spurIndex + 1} nodes match
   * {@code prevPath}.
   * <p>
   * The same traversal both sets and clears, so the mask handed to the next spur node is empty without an
   * {@code Arrays.fill} over the node count or a fresh allocation - it touches only the entries it can name.
   * Every edge it removes leaves the spur node, which is why one row indexed by target is enough (issue #6289).
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

  /**
   * Dijkstra over the adjacency lists, with its state allocated once for the whole call and reset entry by entry: a
   * spur search settles a few nodes around the spur, and clearing node-sized arrays per search would make every one of
   * them O(nodeCount) however little it explored. The queue is a binary heap of primitive (distance, node) pairs with
   * lazy deletion. An edge whose weight is negative, NaN or infinite is not walked (issue #9443).
   */
  private static final class Search {
    private final int[][]    neighbors;
    private final double[][] weights;
    private final WorkGuard  guard;
    private final double[]   dist;
    private final int[]      prev;
    private final boolean[]  removedSpurTargets;
    private final boolean[]  removedNodes;
    // NODES WHOSE dist/prev THE LAST SEARCH WROTE, SO THE NEXT ONE STARTS FROM A CLEAN STATE WITHOUT AN O(N) FILL
    private       int[]      touched     = new int[64];
    private       int        touchedSize;
    private       double[]   heapKeys    = new double[64];
    private       int[]      heapNodes   = new int[64];
    private       int        heapSize;

    private Search(final int n, final int[][] neighbors, final double[][] weights, final WorkGuard guard) {
      this.neighbors = neighbors;
      this.weights = weights;
      this.guard = guard;
      this.dist = new double[n];
      this.prev = new int[n];
      this.removedSpurTargets = new boolean[n];
      this.removedNodes = new boolean[n];
      Arrays.fill(dist, Double.POSITIVE_INFINITY);
      Arrays.fill(prev, -1);
    }

    /**
     * @param spurNode the node whose edges towards {@link #removedSpurTargets} are not walked, or -1
     *
     * @return the node sequence of the shortest path from {@code src} to {@code dst}, or null when there is none
     */
    private int[] shortestPath(final int src, final int dst, final int spurNode) {
      try {
        touch(src);
        dist[src] = 0.0;
        push(0.0, src);
        while (heapSize > 0) {
          final double cost = heapKeys[0];
          final int u = pop();
          if (cost > dist[u])
            continue;
          if (u == dst)
            break;
          // ONE SETTLED NODE RELAXES ITS WHOLE ROW: THE CHECKPOINT KEEPS A LARGE SEARCH ABORTABLE FROM INSIDE (#6302)
          guard.check();
          final int[] row = neighbors[u];
          final double[] rowWeights = weights[u];
          for (int e = 0; e < row.length; e++) {
            final int v = row[e];
            final double w = rowWeights[e];
            if (!EdgeWeight.isWalkable(w) || removedNodes[v] || (u == spurNode && removedSpurTargets[v]))
              continue;
            final double candidate = cost + w;
            if (candidate < dist[v]) {
              if (dist[v] == Double.POSITIVE_INFINITY)
                touch(v);
              dist[v] = candidate;
              prev[v] = u;
              push(candidate, v);
            }
          }
        }

        if (dist[dst] == Double.POSITIVE_INFINITY)
          return null;

        int length = 1;
        for (int cur = dst; cur != src; cur = prev[cur])
          ++length;
        final int[] path = new int[length];
        for (int cur = dst, i = length - 1; i >= 0; cur = prev[cur], --i)
          path[i] = cur;
        return path;
      } finally {
        reset();
      }
    }

    /** The weight of a node sequence: the lightest walkable edge between each consecutive pair. */
    private double pathWeight(final int[] path) {
      double total = 0.0;
      for (int i = 0; i < path.length - 1; i++) {
        final int[] row = neighbors[path[i]];
        final double[] rowWeights = weights[path[i]];
        double best = Double.POSITIVE_INFINITY;
        for (int e = 0; e < row.length; e++)
          if (row[e] == path[i + 1] && EdgeWeight.isWalkable(rowWeights[e]) && rowWeights[e] < best)
            best = rowWeights[e];
        total += best;
      }
      return total;
    }

    private void touch(final int node) {
      if (touchedSize == touched.length)
        touched = Arrays.copyOf(touched, touchedSize * 2);
      touched[touchedSize++] = node;
    }

    private void reset() {
      for (int i = 0; i < touchedSize; i++) {
        dist[touched[i]] = Double.POSITIVE_INFINITY;
        prev[touched[i]] = -1;
      }
      touchedSize = 0;
      heapSize = 0;
    }

    private void push(final double key, final int node) {
      if (heapSize == heapKeys.length) {
        heapKeys = Arrays.copyOf(heapKeys, heapSize * 2);
        heapNodes = Arrays.copyOf(heapNodes, heapSize * 2);
      }
      int i = heapSize++;
      while (i > 0) {
        final int parent = (i - 1) >>> 1;
        if (heapKeys[parent] <= key)
          break;
        heapKeys[i] = heapKeys[parent];
        heapNodes[i] = heapNodes[parent];
        i = parent;
      }
      heapKeys[i] = key;
      heapNodes[i] = node;
    }

    private int pop() {
      final int top = heapNodes[0];
      final int last = --heapSize;
      if (last > 0) {
        final double key = heapKeys[last];
        final int node = heapNodes[last];
        int i = 0;
        while (true) {
          int child = 2 * i + 1;
          if (child >= last)
            break;
          if (child + 1 < last && heapKeys[child + 1] < heapKeys[child])
            ++child;
          if (heapKeys[child] >= key)
            break;
          heapKeys[i] = heapKeys[child];
          heapNodes[i] = heapNodes[child];
          i = child;
        }
        heapKeys[i] = key;
        heapNodes[i] = node;
      }
      return top;
    }
  }
}
