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
 * Procedure: algo.hierarchicalClustering(relTypes?, numClusters?)
 * <p>
 * Performs agglomerative hierarchical clustering using graph-based single-linkage.
 * Starts with each node in its own cluster and repeatedly merges the two clusters
 * linked by the edge of highest similarity (the number of neighbours its two ends share, each end counted in its own
 * neighbourhood). Uses Union-Find for efficient cluster management.
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

    long entries = 0;
    for (final int[] row : adj)
      entries += row.length;
    // ints: the forward adjacency (offsets and fill cursors per node, ids: one per entry before the duplicates go,
    // triangle counts: half the entries at most), the edge list (two ends, a similarity and a position in the merge order
    // per edge: half the entries), the union-find and the result; the similarity histogram is at most one int per node
    graph.memory().reserve(saturatingProduct(saturatingSum(saturatingProduct(entries, 4L), 7L * n + 2), INT_BYTES),
        "the edge similarities", n + " nodes, " + entries + " edge entries");

    final int[] cluster = cluster(adj, Math.max(1, Math.min(numClusters, n)), guard);

    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", graph.getRID(i));
      r.setProperty("cluster", cluster[i]);
      results.add(r);
    }
    return results.stream();
  }

  /**
   * Clusters the nodes of an undirected adjacency ({@code BOTH} rows) down to {@code targetClusters} clusters, or to the
   * connected components when there are more of them, and returns the cluster of each node, numbered in order of first
   * appearance. Each merge joins the two clusters linked by the edge of highest similarity, the similarity of an edge
   * {@code (u, v)} being {@code |N(u) ∩ N(v)|} with each node counted in its own neighbourhood, so {@code 2 + } the
   * triangles the edge closes. Among edges of equal similarity the first met by the scan "node by ascending index, then
   * its row in order" wins.
   * <p>
   * The neighbourhoods never change while the clusters merge, so neither does any similarity, and merging along the
   * best edge between two clusters one round at a time is Kruskal's algorithm over the edges in decreasing similarity,
   * stopped at the target (issue #12171): an edge passed over because both its ends were in one cluster is never
   * between two clusters again. Each similarity is therefore computed once, the edges are ordered by a stable counting
   * sort (similarities are bounded by the node count), and the merges cost a union-find each, instead of a whole-graph
   * scan per merge with an {@code n}-bit set cloned per edge and an {@code n x n} bit matrix behind it.
   * <p>
   * The triangles of every edge come from one pass of the forward algorithm over the simple graph (self-loops and
   * repeated neighbours dropped), each node listing only its neighbours of higher (degree, index) rank: every triangle is
   * met once, from its lowest-ranked node, and credited to its three edges. That is O(E^1.5) whatever the degrees, where
   * a merge of the two full rows per edge is O(sum of degree²), which a supernode turns quadratic.
   */
  static int[] cluster(final int[][] adj, final int targetClusters, final WorkGuard guard) {
    final int n = adj.length;

    // Forward adjacency: the neighbours of higher rank, sorted, without duplicates. Every edge {u, v} with u != v sits in
    // the forward row of exactly one of its ends, its lower-ranked one, whichever row listed it: a row that names a
    // neighbour whose own row does not name it back (an edge deleted between the two row reads) still yields one edge
    final int[] forwardStart = new int[n + 1];
    for (int u = 0; u < n; u++)
      for (final int v : adj[u])
        if (v != u)
          forwardStart[(ranksAbove(adj, v, u) ? u : v) + 1]++;
    for (int u = 0; u < n; u++)
      forwardStart[u + 1] += forwardStart[u];
    final int[] forward = new int[forwardStart[n]];
    final int[] fill = Arrays.copyOf(forwardStart, n);
    for (int u = 0; u < n; u++) {
      guard.checkPeriodically(u);
      for (final int v : adj[u])
        if (v != u) {
          if (ranksAbove(adj, v, u))
            forward[fill[u]++] = v;
          else
            forward[fill[v]++] = u;
        }
    }
    // Sort each row and drop its repeated neighbours, compacting the rows towards the front (a row never moves right)
    int write = 0;
    for (int u = 0; u < n; u++) {
      guard.checkPeriodically(u);
      final int from = forwardStart[u];
      final int to = forwardStart[u + 1];
      forwardStart[u] = write;
      Arrays.sort(forward, from, to);
      int last = -1;
      for (int p = from; p < to; p++)
        if (forward[p] != last)
          forward[write++] = last = forward[p];
    }
    forwardStart[n] = write;

    // Triangles per forward edge: u lowest-ranked, v and w in its forward row, w also in v's
    final int[] triangles = new int[write];
    int work = 0; // merge steps: one node can cost many, so the checkpoint counts these rather than nodes
    for (int u = 0; u < n; u++) {
      guard.check();
      final int uEnd = forwardStart[u + 1];
      for (int a = forwardStart[u]; a < uEnd; a++) {
        final int v = forward[a];
        int iu = forwardStart[u];
        int iv = forwardStart[v];
        final int vEnd = forwardStart[v + 1];
        while (iu < uEnd && iv < vEnd) {
          guard.checkPeriodically(++work);
          final int x = forward[iu];
          final int y = forward[iv];
          if (x < y)
            iu++;
          else if (x > y)
            iv++;
          else {
            triangles[a]++;
            triangles[iu++]++;
            triangles[iv++]++;
          }
        }
      }
    }

    // The edges in the order the scan meets them, with their similarities: an edge is met from the row of its lower-index
    // end, and from the other end's row only when the lower one does not list it (an edge left in one row only), which
    // the scan knows because the lower row was walked first and marked the edge
    final boolean[] met = new boolean[write];
    int edgeCount = 0;
    for (int u = 0; u < n; u++) {
      guard.checkPeriodically(u);
      for (final int v : adj[u])
        if (v != u) {
          final int f = forwardIndex(adj, forward, forwardStart, u, v);
          if (v > u || !met[f]) {
            met[f] = true;
            edgeCount++;
          }
        }
    }
    Arrays.fill(met, false);
    final int[] edgeU = new int[edgeCount];
    final int[] edgeV = new int[edgeCount];
    final int[] similarity = new int[edgeCount];
    int maxSimilarity = 0;
    for (int u = 0, e = 0; u < n; u++) {
      guard.checkPeriodically(u);
      for (final int v : adj[u]) {
        if (v == u)
          continue;
        final int f = forwardIndex(adj, forward, forwardStart, u, v);
        if (v < u && met[f])
          continue;
        met[f] = true;
        edgeU[e] = u;
        edgeV[e] = v;
        similarity[e++] = triangles[f];
        maxSimilarity = Math.max(maxSimilarity, triangles[f]);
      }
    }

    // Stable counting sort by decreasing similarity, so equal similarities keep the scan order
    final int[] bucket = new int[maxSimilarity + 2];
    for (int e = 0; e < edgeCount; e++)
      bucket[maxSimilarity - similarity[e] + 1]++;
    for (int b = 1; b < bucket.length; b++)
      bucket[b] += bucket[b - 1];
    final int[] mergeOrder = new int[edgeCount];
    for (int e = 0; e < edgeCount; e++)
      mergeOrder[bucket[maxSimilarity - similarity[e]]++] = e;

    // Kruskal, stopped at the target
    final int[] parent = new int[n];
    final int[] rank = new int[n];
    for (int i = 0; i < n; i++)
      parent[i] = i;
    int clusters = n;
    for (int k = 0; k < edgeCount && clusters > targetClusters; k++) {
      guard.checkPeriodically(k);
      final int e = mergeOrder[k];
      if (union(parent, rank, edgeU[e], edgeV[e]))
        clusters--;
    }

    // Number the clusters in order of first appearance
    final int[] clusterOfRoot = new int[n];
    Arrays.fill(clusterOfRoot, -1);
    final int[] cluster = new int[n];
    int nextId = 0;
    for (int i = 0; i < n; i++) {
      final int root = find(parent, i);
      if (clusterOfRoot[root] < 0)
        clusterOfRoot[root] = nextId++;
      cluster[i] = clusterOfRoot[root];
    }
    return cluster;
  }

  /** Position of the edge {u, v} in the forward rows: in the row of its lower-ranked end, which always lists it. */
  private static int forwardIndex(final int[][] adj, final int[] forward, final int[] forwardStart, final int u, final int v) {
    final int lower = ranksAbove(adj, v, u) ? u : v;
    final int upper = lower == u ? v : u;
    return Arrays.binarySearch(forward, forwardStart[lower], forwardStart[lower + 1], upper);
  }

  /** Whether {@code v} ranks above {@code u}: higher degree, or the same degree and a higher index. */
  private static boolean ranksAbove(final int[][] adj, final int v, final int u) {
    final int dv = adj[v].length;
    final int du = adj[u].length;
    return dv > du || (dv == du && v > u);
  }

  private static int find(final int[] parent, int x) {
    while (parent[x] != x) {
      parent[x] = parent[parent[x]]; // path compression (halving)
      x = parent[x];
    }
    return x;
  }

  /** Joins the clusters of {@code a} and {@code b}, answering false when they were one already. */
  private static boolean union(final int[] parent, final int[] rank, final int a, final int b) {
    final int ra = find(parent, a);
    final int rb = find(parent, b);
    if (ra == rb)
      return false;
    if (rank[ra] < rank[rb])
      parent[ra] = rb;
    else if (rank[ra] > rank[rb])
      parent[rb] = ra;
    else {
      parent[rb] = ra;
      rank[ra]++;
    }
    return true;
  }
}
