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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.knn(k?, relTypes?, direction?)
 * <p>
 * Constructs a K-Nearest Neighbours graph using Jaccard similarity of node neighbourhoods.
 * For each node, the {@code k} most-similar other nodes (by Jaccard index of their adjacency
 * sets) are returned.
 * </p>
 * <p>
 * Jaccard similarity: {@code |N(u) ∩ N(v)| / |N(u) ∪ N(v)|}
 * </p>
 * <p>
 * Only two nodes that share a neighbour have a similarity above zero, so each node is compared with the
 * nodes two hops away, found by walking its neighbours' rows, rather than with every other node of the
 * graph: the work is the sum of the two-hop walks and the memory linear in the graph (issue #12172).
 * </p>
 * <p>
 * Parameters:
 * <ul>
 *   <li>k (optional, int, default 10): number of nearest neighbours per node</li>
 *   <li>relTypes (optional): relationship types to consider for neighbourhoods</li>
 *   <li>direction (optional): "OUT", "IN", or "BOTH" (default "BOTH")</li>
 * </ul>
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.knn(5, 'KNOWS', 'BOTH')
 * YIELD node1, node2, similarity
 * RETURN node1.name, node2.name, similarity ORDER BY similarity DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoKNN extends AbstractAlgoProcedure {
  public static final String NAME = "algo.knn";

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
    return 3;
  }

  @Override
  public String getDescription() {
    return "K-Nearest Neighbours graph construction based on Jaccard similarity of neighbourhoods";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node1", "node2", "similarity");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final int rawK             = extractCount(optionalNumberArg(args, 0, "k", 10), "k");
    final String[] relTypes    = extractRelTypes(argOrDefault(args, 1, null), context);
    final Vertex.DIRECTION dir = parseDirection(extractString(argOrDefault(args, 2, "BOTH"), "direction"));

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    final GraphData graph = loadGraph(db, null, relTypes, context);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    // Clamp to the graph size: there can never be more than n-1 neighbours, and sizing the
    // per-node top-k arrays directly off an unclamped rawK risks a multi-GB allocation attempt
    // for a huge k (e.g. algo.knn(2147483648)).
    final int k = Math.min(rawK, n);
    // No neighbour asked for: nothing to list (and no top-k slot to compare a candidate against)
    if (k == 0)
      return Stream.empty();

    // |N(u)| and |N(v)| are the sizes of the neighbour sets: a parallel or a reciprocal relationship is one neighbour (issue #12934)
    final int[][] adj = graph.adjacency(dir, relTypes);
    toDistinctNeighbours(adj, guard);
    final int[] degree = new int[n];
    for (int i = 0; i < n; i++)
      degree[i] = adj[i].length;
    // v lists w in its row exactly as often as w lists v in its row of the opposite direction: the walk from u reaches every v
    // that shares a neighbour with it through that row (issue #12172)
    final int[][] back;
    if (dir == Vertex.DIRECTION.BOTH)
      back = adj;
    else {
      back = graph.adjacency(dir == Vertex.DIRECTION.OUT ? Vertex.DIRECTION.IN : Vertex.DIRECTION.OUT, relTypes);
      toDistinctNeighbours(back, guard);
    }

    // The degrees, the shared-neighbour count of every node u reaches, the nodes it reached, and the top-k of u: reused from node to
    // node
    graph.memory().reserve(saturatingSum(saturatingProduct(3L * n, INT_BYTES), saturatingProduct(k, INT_BYTES + DOUBLE_BYTES)),
        "the shared-neighbour counters", n + " nodes, k=" + k);
    final int[] shared = new int[n];
    final int[] reached = new int[n];
    final double[] topSim = new double[k];
    final int[] topIdx = new int[k];

    final List<Result> results = new ArrayList<>();

    for (int u = 0; u < n; u++) {
      // One u walks its two-hop neighbourhood, which a supernode makes as long as the graph (issue #6302): checked per node
      guard.check();
      if (degree[u] == 0)
        continue;

      // Only a node two hops away shares a neighbour with u, so only those are candidates: the walk counts, for each, how many
      // distinct neighbours of u it lists
      int reachedCount = 0;
      for (final int w : adj[u])
        for (final int v : back[w]) {
          if (v == u)
            continue;
          if (shared[v]++ == 0)
            reached[reachedCount++] = v;
        }
      // Met in ascending order, as a scan of every node meets them: equal similarities keep the same node
      Arrays.sort(reached, 0, reachedCount);

      // Find top-k neighbours by Jaccard similarity
      int topCount = 0;
      double minTop = 0.0;

      for (int c = 0; c < reachedCount; c++) {
        final int v = reached[c];
        final int inter = shared[v];
        shared[v] = 0;
        final int union = degree[u] + degree[v] - inter;
        final double jaccard = union == 0 ? 0.0 : (double) inter / union;

        if (jaccard <= 0.0)
          continue;

        if (topCount < k) {
          topSim[topCount] = jaccard;
          topIdx[topCount] = v;
          topCount++;
          if (topCount == k) {
            // Find minimum
            minTop = topSim[0];
            for (int x = 1; x < k; x++)
              if (topSim[x] < minTop) minTop = topSim[x];
          }
        } else if (jaccard > minTop) {
          // Replace minimum
          int minPos = 0;
          for (int x = 1; x < k; x++)
            if (topSim[x] < topSim[minPos]) minPos = x;
          topSim[minPos] = jaccard;
          topIdx[minPos] = v;
          minTop = topSim[0];
          for (int x = 1; x < k; x++)
            if (topSim[x] < minTop) minTop = topSim[x];
        }
      }

      for (int i = 0; i < topCount; i++) {
        final ResultInternal r = new ResultInternal();
        r.setProperty("node1", graph.getRID(u));
        r.setProperty("node2", graph.getRID(topIdx[i]));
        r.setProperty("similarity", topSim[i]);
        results.add(r);
      }
    }
    return results.stream();
  }
}
