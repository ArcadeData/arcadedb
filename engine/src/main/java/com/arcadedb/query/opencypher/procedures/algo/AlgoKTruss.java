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
 * Procedure: algo.kTruss(relTypes?, k?)
 * <p>
 * Computes the k-truss decomposition of the graph. Each node is assigned the maximum k for
 * which it participates in a k-truss subgraph (a subgraph where every edge is supported by
 * at least k-2 triangles), from sorted neighbour sets and a bucket queue of edge supports.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.kTruss('KNOWS', 3)
 * YIELD nodeId, trussNumber
 * RETURN nodeId, trussNumber ORDER BY trussNumber DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoKTruss extends AbstractAlgoProcedure {
  public static final String NAME = "algo.kTruss";

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
    return "Computes k-truss decomposition assigning each vertex its maximum truss number";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodeId", "trussNumber");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = extractRelTypes(argOrDefault(args, 0, null), context);
    // Validated as before. The result is the full decomposition, which no k changes: the k-bounded peeling pass that ran
    // ahead of it never reached the output, and is gone (issue #12173).
    extractInt(optionalNumberArg(args, 1, "k", 3), "k");

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    final GraphData graph = loadGraph(db, null, relTypes, context);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH, relTypes);

    // The neighbour set of every node as a sorted array of distinct ids. A self-loop keeps the node in its own set, as
    // the bit sets this replaces did, so it adds one to the support of every edge at that node (issue #12173: the bit
    // sets were an n x n matrix, built twice, and every intersection cost n / 64 words whatever the degrees).
    long entries = 0;
    for (int i = 0; i < n; i++) {
      guard.checkPeriodically(i);
      adj[i] = distinctSorted(adj[i]);
      entries += adj[i].length;
    }

    // Canonical edges (u < v) and, beside every neighbour entry, the id of its edge (-1 for a self-loop)
    long edges = 0;
    for (int u = 0; u < n; u++)
      for (final int v : adj[u])
        if (u < v)
          edges++;
    final int edgeCount = checkedEdgeCount(NAME, n, edges);
    graph.memory().reserve(saturatingSum(saturatingSum(saturatingProduct(n, MATRIX_ROW_OVERHEAD_BYTES + INT_BYTES),
            saturatingProduct(entries, INT_BYTES)), saturatingProduct(edgeCount, 6 * INT_BYTES)),
        "the edge ids and the truss peeling state", n + " nodes, " + edgeCount + " edges");
    final int[][] edgeIds = new int[n][];
    for (int u = 0; u < n; u++)
      edgeIds[u] = new int[adj[u].length];
    final int[] edgeU = new int[edgeCount];
    final int[] edgeV = new int[edgeCount];
    int e = 0;
    for (int u = 0; u < n; u++) {
      guard.checkPeriodically(u);
      final int[] row = adj[u];
      for (int p = 0; p < row.length; p++) {
        final int v = row[p];
        if (v == u)
          edgeIds[u][p] = -1;
        else if (u < v) {
          edgeU[e] = u;
          edgeV[e] = v;
          edgeIds[u][p] = e;
          edgeIds[v][Arrays.binarySearch(adj[v], u)] = e;
          e++;
        }
      }
    }

    // support[e] = |N(u) ∩ N(v)|: the triangles on the edge, plus one per self-loop at either end
    final int[] support = new int[edgeCount];
    int maxSupport = 0;
    for (int k = 0; k < edgeCount; k++) {
      guard.checkPeriodically(k);
      support[k] = commonNeighbours(adj[edgeU[k]], adj[edgeV[k]]);
      maxSupport = Math.max(maxSupport, support[k]);
    }

    final int[] edgeTruss = computeTrussDecomposition(guard, adj, edgeIds, edgeU, edgeV, support, maxSupport);

    // Node truss = max truss of any incident edge
    final int[] nodeTruss = new int[n];
    for (int k = 0; k < edgeCount; k++) {
      if (edgeTruss[k] > nodeTruss[edgeU[k]])
        nodeTruss[edgeU[k]] = edgeTruss[k];
      if (edgeTruss[k] > nodeTruss[edgeV[k]])
        nodeTruss[edgeV[k]] = edgeTruss[k];
    }

    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", graph.getRID(i));
      r.setProperty("trussNumber", nodeTruss[i]);
      results.add(r);
    }
    return results.stream();
  }

  /**
   * The truss number of every edge: the largest k such that the edge belongs to the maximal subgraph whose edges all
   * have a support of at least k - 2. That subgraph does not depend on the order edges are peeled in, so the edges are
   * peeled in order of current support from a bucket queue (Wang and Cheng), O(E) moves plus one walk of the common
   * neighbours per edge, instead of a sweep over every edge per round and per k.
   */
  private static int[] computeTrussDecomposition(final WorkGuard guard, final int[][] adj, final int[][] edgeIds,
      final int[] edgeU, final int[] edgeV, final int[] support, final int maxSupport) {
    final int edgeCount = support.length;
    // Edges sorted by support: order[] lists them, position[] is where each one is, bucketStart[s] is where support s
    // begins
    final int[] bucketStart = new int[maxSupport + 2];
    for (final int s : support)
      bucketStart[s + 1]++;
    for (int s = 1; s < bucketStart.length; s++)
      bucketStart[s] += bucketStart[s - 1];
    final int[] order = new int[edgeCount];
    final int[] position = new int[edgeCount];
    final int[] fill = Arrays.copyOf(bucketStart, bucketStart.length);
    for (int k = 0; k < edgeCount; k++) {
      position[k] = fill[support[k]]++;
      order[position[k]] = k;
    }

    final boolean[] removed = new boolean[edgeCount];
    final int[] edgeTruss = new int[edgeCount];
    final int[] commonUW = new int[2];
    for (int i = 0; i < edgeCount; i++) {
      guard.checkPeriodically(i);
      final int edge = order[i];
      final int s = support[edge];
      edgeTruss[edge] = s + 2;
      removed[edge] = true;

      // Every triangle (u, v, w) still standing loses this edge: the other two edges lose one support each, down to s
      final int u = edgeU[edge];
      final int v = edgeV[edge];
      final int[] rowU = adj[u];
      final int[] rowV = adj[v];
      final boolean uSmaller = rowU.length <= rowV.length;
      final int[] small = uSmaller ? rowU : rowV;
      final int[] large = uSmaller ? rowV : rowU;
      final int[] smallIds = edgeIds[uSmaller ? u : v];
      final int[] largeIds = edgeIds[uSmaller ? v : u];
      int from = 0;
      for (int p = 0; p < small.length && from < large.length; p++) {
        final int w = small[p];
        final int q = Arrays.binarySearch(large, from, large.length, w);
        if (q < 0) {
          from = -q - 1;
          continue;
        }
        from = q + 1;
        if (w == u || w == v)
          continue;
        commonUW[0] = smallIds[p];
        commonUW[1] = largeIds[q];
        if (removed[commonUW[0]] || removed[commonUW[1]])
          continue;
        for (final int other : commonUW) {
          final int so = support[other];
          if (so > s) {
            // Swap it with the first edge of its bucket, then move the bucket's start past it
            final int first = bucketStart[so];
            final int firstEdge = order[first];
            order[first] = other;
            order[position[other]] = firstEdge;
            position[firstEdge] = position[other];
            position[other] = first;
            bucketStart[so]++;
            support[other] = so - 1;
          }
        }
      }
    }
    return edgeTruss;
  }

  /** The number of ids two sorted arrays of distinct ids have in common, walking the smaller one. */
  private static int commonNeighbours(final int[] a, final int[] b) {
    final int[] small = a.length <= b.length ? a : b;
    final int[] large = a.length <= b.length ? b : a;
    int count = 0;
    int from = 0;
    for (int p = 0; p < small.length && from < large.length; p++) {
      final int q = Arrays.binarySearch(large, from, large.length, small[p]);
      if (q >= 0) {
        count++;
        from = q + 1;
      } else
        from = -q - 1;
    }
    return count;
  }

  /** The distinct ids of {@code row} in ascending order. */
  private static int[] distinctSorted(final int[] row) {
    Arrays.sort(row);
    int write = 0;
    for (int i = 0; i < row.length; i++)
      if (i == 0 || row[i] != row[i - 1])
        row[write++] = row[i];
    return write == row.length ? row : Arrays.copyOf(row, write);
  }
}
