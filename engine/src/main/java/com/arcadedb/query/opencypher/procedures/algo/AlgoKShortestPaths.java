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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
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
    final int k                   = ((Number) args[2]).intValue();
    final String[] relTypes       = args.length > 3 ? extractRelTypes(args[3]) : null;
    final String weightProperty   = args.length > 4 ? extractString(args[4], "weightProperty") : null;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);

    final Integer startIdx = ridToIdx.get(startNode.getIdentity());
    final Integer endIdx   = ridToIdx.get(endNode.getIdentity());
    if (startIdx == null || endIdx == null)
      return Stream.empty();

    // Build weighted adjacency matrix (OUT direction)
    final double[][] weightMatrix = new double[n][n];
    for (double[] row : weightMatrix)
      Arrays.fill(row, Double.MAX_VALUE);

    for (int i = 0; i < n; i++) {
      weightMatrix[i][i] = 0.0;
      final Iterable<Edge> edges = relTypes != null && relTypes.length > 0 ?
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT, relTypes) :
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT);
      for (final Edge e : edges) {
        final Integer j = ridToIdx.get(e.getIn());
        if (j == null)
          continue;
        double w = 1.0;
        if (weightProperty != null && !weightProperty.isEmpty()) {
          final Object wObj = e.get(weightProperty);
          if (wObj instanceof Number num)
            w = num.doubleValue();
        }
        if (w < weightMatrix[i][j])
          weightMatrix[i][j] = w;
      }
    }

    // Yen's k-shortest paths
    final List<int[]> kPaths     = new ArrayList<>(k);
    final List<Double> kWeights  = new ArrayList<>(k);

    // Find first shortest path with Dijkstra
    final int[] firstPath = dijkstra(weightMatrix, n, startIdx, endIdx, null);
    if (firstPath == null)
      return Stream.empty();

    kPaths.add(firstPath);
    kWeights.add(pathWeight(firstPath, weightMatrix));

    // Candidate paths: PriorityQueue of {weight, path}
    final PriorityQueue<double[]> candidates = new PriorityQueue<>(Comparator.comparingDouble(a2 -> a2[0]));
    // We'll encode paths as serialized int arrays stored separately
    final List<int[]> candidatePaths = new ArrayList<>();
    final List<Double> candidateCosts = new ArrayList<>();

    for (int ki = 1; ki < k; ki++) {
      final int[] prevPath = kPaths.get(ki - 1);

      for (int i = 0; i < prevPath.length - 1; i++) {
        final int spurNode = prevPath[i];
        final int[] rootPath = Arrays.copyOf(prevPath, i + 1);
        final double rootCost = pathWeight(rootPath, weightMatrix);

        // Remove edges that are part of previous k-shortest paths sharing the same root
        // We track removed edges as pairs via a HashSet-like structure (boolean matrix)
        final boolean[][] removedEdges = new boolean[n][n];
        for (final int[] p : kPaths) {
          if (p.length > i + 1) {
            boolean sameRoot = true;
            for (int r = 0; r <= i; r++) {
              if (p[r] != prevPath[r]) {
                sameRoot = false;
                break;
              }
            }
            if (sameRoot)
              removedEdges[p[i]][p[i + 1]] = true;
          }
        }

        // Remove root path nodes (except spurNode) from the graph
        final boolean[] removedNodes = new boolean[n];
        for (int r = 0; r < i; r++)
          removedNodes[rootPath[r]] = true;

        // Find spur path from spurNode to endIdx
        final int[] spurPath = dijkstra(weightMatrix, n, spurNode, endIdx, (u, v) -> removedEdges[u][v] || removedNodes[u] || removedNodes[v]);

        if (spurPath != null) {
          // Build total path = rootPath + spurPath (skip duplicate spurNode)
          final int[] totalPath = new int[rootPath.length - 1 + spurPath.length];
          System.arraycopy(rootPath, 0, totalPath, 0, rootPath.length - 1);
          System.arraycopy(spurPath, 0, totalPath, rootPath.length - 1, spurPath.length);

          final double totalCost = rootCost - (i > 0 ? pathWeight(Arrays.copyOf(rootPath, i), weightMatrix) : 0)
              + pathWeight(rootPath, weightMatrix) - (i > 0 ? pathWeight(Arrays.copyOf(rootPath, rootPath.length - 1), weightMatrix) : 0)
              + pathWeight(spurPath, weightMatrix);
          // Simpler: just compute full path weight
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
        pathRids.add(vertices.get(idx).getIdentity());

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

  private static int[] dijkstra(final double[][] w, final int n, final int src, final int dst,
      final EdgeFilter removed) {
    final double[] dist = new double[n];
    final int[] prev    = new int[n];
    Arrays.fill(dist, Double.MAX_VALUE);
    Arrays.fill(prev, -1);
    dist[src] = 0.0;

    // PriorityQueue of {cost, nodeIdx}
    final PriorityQueue<double[]> pq = new PriorityQueue<>(Comparator.comparingDouble(a -> a[0]));
    pq.offer(new double[] { 0.0, src });

    while (!pq.isEmpty()) {
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
