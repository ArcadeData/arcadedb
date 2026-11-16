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
    final int[][] adj = graph.adjacency(dir, relTypes);

    // The Jaccard union is taken over the neighbour lists as read, repeated neighbours included, and the intersection
    // over the distinct neighbours: the lengths are kept before the rows are reduced to sorted distinct sets.
    final int[] degree = new int[n];
    long distinctEntries = 0;
    for (int i = 0; i < n; i++) {
      guard.checkPeriodically(i);
      degree[i] = adj[i].length;
      adj[i] = distinctSorted(adj[i]);
      distinctEntries += adj[i].length;
    }

    // Only nodes with a neighbour in common can score above zero, and those are the ones two hops away through it: who
    // lists w as a neighbour is the reverse of the neighbour sets, listed in ascending node order (issue #12172).
    graph.memory().reserve(saturatingSum(saturatingProduct(n, MATRIX_ROW_OVERHEAD_BYTES + 3 * INT_BYTES),
            saturatingProduct(distinctEntries, INT_BYTES)), "the reverse neighbour sets and common-neighbour counters",
        n + " nodes, " + distinctEntries + " edge entries");
    final int[] listedBy = new int[n];
    for (int i = 0; i < n; i++)
      for (final int w : adj[i])
        listedBy[w]++;
    final int[][] reverse = new int[n][];
    for (int w = 0; w < n; w++)
      reverse[w] = new int[listedBy[w]];
    Arrays.fill(listedBy, 0);
    for (int v = 0; v < n; v++)
      for (final int w : adj[v])
        reverse[w][listedBy[w]++] = v;

    // common[v] = |N(u) ∩ N(v)| for the current u, and the nodes it was raised for; both are reset after each u
    final int[] common = listedBy;
    Arrays.fill(common, 0);
    int[] touched = new int[16];

    final List<Result> results = new ArrayList<>();

    for (int u = 0; u < n; u++) {
      guard.checkPeriodically(u);
      if (degree[u] == 0)
        continue;

      int touchedCount = 0;
      for (final int w : adj[u]) {
        for (final int v : reverse[w]) {
          if (v == u)
            continue;
          if (common[v]++ == 0) {
            if (touchedCount == touched.length)
              touched = Arrays.copyOf(touched, touchedCount * 2);
            touched[touchedCount++] = v;
          }
        }
      }
      // The candidates in ascending order, the order the scan over every node met them in: among equal similarities
      // the top-k keeps the same nodes
      Arrays.sort(touched, 0, touchedCount);

      // Find top-k neighbours by Jaccard similarity (avoid duplicate pairs by requiring v > u)
      // We collect all candidates and pick top-k
      double[] topSim = new double[k];
      int[] topIdx = new int[k];
      int topCount = 0;
      double minTop = 0.0;

      for (int c = 0; c < touchedCount; c++) {
        final int v = touched[c];

        // Jaccard = |intersection| / |union|
        final int inter = common[v];
        common[v] = 0;
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

  /** The distinct ids of {@code row} in ascending order; the row itself when it has no repeated id. */
  private static int[] distinctSorted(final int[] row) {
    Arrays.sort(row);
    int write = 0;
    for (int i = 0; i < row.length; i++)
      if (i == 0 || row[i] != row[i - 1])
        row[write++] = row[i];
    return write == row.length ? row : Arrays.copyOf(row, write);
  }
}
