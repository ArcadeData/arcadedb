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
 * at least k-2 triangles), over the simple undirected graph: the direction, the multiplicity of
 * the edges and self-loops do not count. The decomposition peels the edges in support order over
 * sorted neighbour sets, O(E^1.5) time and O(V + E) memory (issue #12173). The optional
 * {@code k} is validated but selects nothing: every node is listed with its truss number.
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
    // Validated as before, but it selects nothing: every node gets its truss number from the full decomposition, which is what the
    // documented yield (the maximum k of a k-truss the node belongs to) asks for
    extractInt(optionalNumberArg(args, 1, "k", 3), "k");

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    // Trusses are defined on the simple undirected graph: sorted neighbour sets with no self-loop and no repeated neighbour, the
    // representation algo.triangleCount reads (issue #10203). A self-loop no longer counts its own node as a common neighbour.
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH, relTypes);
    toSimpleNeighbourSets(adj, guard);

    final int[] nodeTruss = trussNumbers(graph.memory(), guard, adj);

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
   * The truss number of every node: the largest truss number of its edges, 0 for a node with none (issue #12173).
   * <p>
   * The edge decomposition is the peeling of Wang and Cheng, the edge counterpart of the bin-sorted core decomposition: the edges are
   * kept ordered by support (the triangles they close) in bins, the edge of least support is removed at a truss number of its support
   * plus 2, and each triangle it closed lowers the support of its two other edges, down to the support being removed and no further.
   * Supports come from one listing of the triangles over the sorted rows. O(E^1.5) time and O(V + E) memory, where the bit matrix
   * this replaced cost O(V^2) memory and O(V / 64) per set operation whatever the degree.
   */
  private static int[] trussNumbers(final MemoryBudget memory, final WorkGuard guard, final int[][] adj) {
    final int n = adj.length;
    long totalArcs = 0;
    for (final int[] row : adj)
      totalArcs += row.length;
    final int arcs = checkedEdgeCount(NAME, n, totalArcs);
    final int[] offsets = new int[n + 1];
    for (int u = 0; u < n; u++)
      offsets[u + 1] = offsets[u] + adj[u].length;
    final int m = arcs / 2;

    // The edge of every arc; the two ends of every edge, its support, its slot and position in the bins (at most one bin per edge)
    // and whether it is gone; the row offsets and the truss number of every node
    memory.reserve(saturatingSum(saturatingSum(saturatingProduct(arcs, INT_BYTES), saturatingProduct(m, 6 * INT_BYTES + BOOLEAN_BYTES)),
        saturatingProduct(n + 1L, 2 * INT_BYTES)), "the truss decomposition", n + " nodes, " + m + " edges");
    final int[] arcEdge = new int[arcs];
    final int[] edgeU = new int[m];
    final int[] edgeV = new int[m];
    int edges = 0;
    for (int u = 0; u < n; u++) {
      guard.checkPeriodically(u);
      final int[] row = adj[u];
      for (int k = -Arrays.binarySearch(row, u) - 1; k < row.length; k++) {
        final int v = row[k];
        edgeU[edges] = u;
        edgeV[edges] = v;
        arcEdge[offsets[u] + k] = edges;
        arcEdge[offsets[v] + Arrays.binarySearch(adj[v], u)] = edges;
        edges++;
      }
    }

    // Support: every triangle u < v < w found once from its lowest edge, by a sorted merge of the two rows past v
    final int[] support = new int[m];
    for (int u = 0; u < n; u++) {
      guard.check();
      final int[] nu = adj[u];
      for (int k = -Arrays.binarySearch(nu, u) - 1; k < nu.length; k++) {
        final int v = nu[k];
        final int[] nv = adj[v];
        final int uv = arcEdge[offsets[u] + k];
        int iu = k + 1;
        int iv = -Arrays.binarySearch(nv, v) - 1;
        while (iu < nu.length && iv < nv.length) {
          final int a = nu[iu];
          final int b = nv[iv];
          if (a < b)
            iu++;
          else if (a > b)
            iv++;
          else {
            support[uv]++;
            support[arcEdge[offsets[u] + iu]]++;
            support[arcEdge[offsets[v] + iv]]++;
            iu++;
            iv++;
          }
        }
      }
    }

    // The edges ordered by support in bins: binStart[s] is the first position of support s
    int maxSupport = 0;
    for (int e = 0; e < m; e++)
      if (support[e] > maxSupport)
        maxSupport = support[e];
    final int[] binStart = new int[maxSupport + 2];
    for (int e = 0; e < m; e++)
      binStart[support[e] + 1]++;
    for (int s = 0; s <= maxSupport; s++)
      binStart[s + 1] += binStart[s];
    final int[] order = new int[m];
    final int[] position = new int[m];
    final int[] fill = Arrays.copyOf(binStart, maxSupport + 1);
    for (int e = 0; e < m; e++) {
      position[e] = fill[support[e]]++;
      order[position[e]] = e;
    }

    final boolean[] removed = new boolean[m];
    final int[] nodeTruss = new int[n];
    for (int i = 0; i < m; i++) {
      guard.checkPeriodically(i);
      final int e = order[i];
      final int s = support[e];
      final int truss = s + 2;
      removed[e] = true;
      final int u = edgeU[e];
      final int v = edgeV[e];
      if (truss > nodeTruss[u])
        nodeTruss[u] = truss;
      if (truss > nodeTruss[v])
        nodeTruss[v] = truss;

      // Every triangle (u, v, w) still standing loses this edge: its other two edges lose one of support, down to s. A support of
      // zero means no triangle is left (a support is never below the triangles still standing), so there is nothing to walk
      if (s > 0)
        lowerSupportOfTriangles(u, v, s, adj, offsets, arcEdge, removed, support, binStart, order, position);
    }
    return nodeTruss;
  }

  /**
   * Lowers the support of the two other edges of every triangle (u, v, w) whose edges are all still standing. The common neighbours
   * are found by a binary search per entry of the shorter row when the other is much longer (a hub), and by a merge otherwise, so the
   * work is bounded by the shorter row whatever the node numbering.
   */
  private static void lowerSupportOfTriangles(final int u, final int v, final int floor, final int[][] adj, final int[] offsets,
      final int[] arcEdge, final boolean[] removed, final int[] support, final int[] binStart, final int[] order, final int[] position) {
    final int[] nu = adj[u];
    final int[] nv = adj[v];
    if ((long) nu.length * 8 < nv.length || (long) nv.length * 8 < nu.length) {
      final boolean uShorter = nu.length <= nv.length;
      final int[] shortRow = uShorter ? nu : nv;
      final int[] longRow = uShorter ? nv : nu;
      final int shortNode = uShorter ? u : v;
      final int longNode = uShorter ? v : u;
      for (int is = 0; is < shortRow.length; is++) {
        final int il = Arrays.binarySearch(longRow, shortRow[is]);
        if (il >= 0)
          lowerSupportOfTriangle(arcEdge[offsets[shortNode] + is], arcEdge[offsets[longNode] + il], floor, removed, support, binStart,
              order, position);
      }
      return;
    }
    int iu = 0;
    int iv = 0;
    while (iu < nu.length && iv < nv.length) {
      final int a = nu[iu];
      final int b = nv[iv];
      if (a < b)
        iu++;
      else if (a > b)
        iv++;
      else {
        lowerSupportOfTriangle(arcEdge[offsets[u] + iu], arcEdge[offsets[v] + iv], floor, removed, support, binStart, order,
            position);
        iu++;
        iv++;
      }
    }
  }

  private static void lowerSupportOfTriangle(final int first, final int second, final int floor, final boolean[] removed,
      final int[] support, final int[] binStart, final int[] order, final int[] position) {
    if (!removed[first] && !removed[second]) {
      lowerSupport(first, floor, support, binStart, order, position);
      lowerSupport(second, floor, support, binStart, order, position);
    }
  }

  /**
   * Lowers the support of edge {@code f} by one when it is above {@code floor}, moving it to the front of its bin and the bin's start
   * past it, so the bins stay ordered without a re-sort.
   */
  private static void lowerSupport(final int f, final int floor, final int[] support, final int[] binStart, final int[] order,
      final int[] position) {
    final int s = support[f];
    if (s <= floor)
      return;
    final int first = binStart[s];
    final int g = order[first];
    if (g != f) {
      final int pf = position[f];
      order[first] = f;
      position[f] = first;
      order[pf] = g;
      position[g] = pf;
    }
    binStart[s]++;
    support[f] = s - 1;
  }
}
