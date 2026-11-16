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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.hierarchicalClustering(relTypes?, numClusters?)
 * <p>
 * Performs agglomerative hierarchical clustering using graph-based single-linkage.
 * Starts with each node in its own cluster and repeatedly merges the two clusters
 * with the highest inter-cluster similarity (number of common neighbors / Jaccard).
 * Uses Union-Find for efficient cluster management.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.hierarchicalClustering('KNOWS', 3)
 * YIELD nodeId, cluster
 * RETURN nodeId, cluster
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoHierarchicalClustering extends AbstractAlgoProcedure {
  public static final String NAME = "algo.hierarchicalClustering";

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
    return 2;
  }

  @Override
  public String getDescription() {
    return "Performs agglomerative hierarchical clustering using graph-based single-linkage";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodeId", "cluster");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = extractRelTypes(argOrDefault(args, 0, null), context);
    final int numClusters = extractInt(optionalNumberArg(args, 1, "numClusters", 2), "numClusters");

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    final GraphData graph = loadGraph(db, null, relTypes, context);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH, relTypes);

    // The candidate merges are the edges (u, v) with u < v, in the order a scan of the adjacency meets them: by u, then
    // by position in u's row
    long entries = 0;
    for (int u = 0; u < n; u++)
      for (final int v : adj[u])
        if (v > u)
          entries++;
    final int edgeCount = checkedEdgeCount(NAME, n, entries);
    graph.memory().reserve(saturatingSum(saturatingProduct(edgeCount, 2 * INT_BYTES + LONG_BYTES),
            saturatingProduct(n, MATRIX_ROW_OVERHEAD_BYTES + INT_BYTES)), "the candidate merges and the neighbour sets",
        n + " nodes, " + edgeCount + " edges");
    final int[] edgeU = new int[edgeCount];
    final int[] edgeV = new int[edgeCount];
    int e = 0;
    for (int u = 0; u < n; u++) {
      for (final int v : adj[u]) {
        if (v > u) {
          edgeU[e] = u;
          edgeV[e] = v;
          e++;
        }
      }
    }

    // The neighbour set of every node, the node itself included, as a sorted array of distinct ids
    for (int i = 0; i < n; i++) {
      guard.checkPeriodically(i);
      adj[i] = closedNeighbourSet(adj[i], i);
    }

    // Similarity = common neighbours (Jaccard numerator), by a sorted merge of the two sets. It depends on the edge
    // alone, never on the clusters, so it is computed once: each merge used to rescan every edge and intersect two
    // n-bit sets per edge (issue #12171). Sorted by similarity descending, then by scan order.
    final long[] order = new long[edgeCount];
    for (int k = 0; k < edgeCount; k++) {
      guard.checkPeriodically(k);
      final int sim = commonCount(adj[edgeU[k]], adj[edgeV[k]]);
      order[k] = ((long) (Integer.MAX_VALUE - sim) << 32) | k;
    }
    Arrays.sort(order);

    // Union-Find
    final int[] parent = new int[n];
    final int[] rank = new int[n];
    for (int i = 0; i < n; i++)
      parent[i] = i;

    int currentClusters = n;
    final int targetClusters = Math.max(1, Math.min(numClusters, n));

    // Agglomerative: each merge joins the two clusters of the most similar edge between two clusters, the first in scan
    // order among equals. Clusters only grow, so an edge passed over, being inside a cluster, stays inside one: the
    // next merge is always the next edge in the sorted order that still joins two clusters. That is Kruskal's
    // algorithm, O(E log E) for the same merges in the same order.
    for (int k = 0; k < edgeCount && currentClusters > targetClusters; k++) {
      guard.checkPeriodically(k);
      final int edge = (int) order[k];
      final int rootU = find(parent, edgeU[edge]);
      final int rootV = find(parent, edgeV[edge]);
      if (rootU == rootV)
        continue;
      union(parent, rank, rootU, rootV);
      currentClusters--;
    }

    // Remap cluster IDs (find roots) to sequential IDs
    final Map<Integer, Integer> clusterRemap = new HashMap<>();
    int nextId = 0;
    for (int i = 0; i < n; i++) {
      final int root = find(parent, i);
      if (!clusterRemap.containsKey(root))
        clusterRemap.put(root, nextId++);
    }

    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", graph.getRID(i));
      r.setProperty("cluster", clusterRemap.get(find(parent, i)));
      results.add(r);
    }
    return results.stream();
  }

  private int find(final int[] parent, int x) {
    while (parent[x] != x) {
      parent[x] = parent[parent[x]]; // path compression (halving)
      x = parent[x];
    }
    return x;
  }

  private void union(final int[] parent, final int[] rank, final int a, final int b) {
    final int ra = find(parent, a);
    final int rb = find(parent, b);
    if (ra == rb)
      return;
    if (rank[ra] < rank[rb])
      parent[ra] = rb;
    else if (rank[ra] > rank[rb])
      parent[rb] = ra;
    else {
      parent[rb] = ra;
      rank[ra]++;
    }
  }

  /** {@code row} and {@code self} as a sorted array of distinct ids. */
  private static int[] closedNeighbourSet(final int[] row, final int self) {
    final int[] set = Arrays.copyOf(row, row.length + 1);
    set[row.length] = self;
    Arrays.sort(set);
    int write = 0;
    for (int i = 0; i < set.length; i++)
      if (i == 0 || set[i] != set[i - 1])
        set[write++] = set[i];
    return write == set.length ? set : Arrays.copyOf(set, write);
  }

  /** The number of ids two sorted arrays of distinct ids have in common. */
  private static int commonCount(final int[] a, final int[] b) {
    int count = 0;
    int i = 0;
    int j = 0;
    while (i < a.length && j < b.length) {
      if (a[i] < b[j])
        i++;
      else if (a[i] > b[j])
        j++;
      else {
        count++;
        i++;
        j++;
      }
    }
    return count;
  }
}
