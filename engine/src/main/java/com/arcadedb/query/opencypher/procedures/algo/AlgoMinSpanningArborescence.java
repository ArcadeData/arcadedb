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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.msa(rootNode, relTypes?, weightProperty?)
 *
 * <p>Minimum Spanning Arborescence (directed MST) rooted at a given vertex, computed
 * with the Chu-Liu/Edmonds algorithm (Edmonds 1967, Chu &amp; Liu 1965).
 * Unlike the undirected {@code algo.mst}, this procedure returns a <em>directed</em>
 * spanning tree where every non-root vertex has exactly one incoming edge and all
 * vertices are reachable from the root along directed paths.</p>
 *
 * <p>Returns one row per MSA edge. If no spanning arborescence exists (the graph is
 * not strongly connected from the root), the procedure returns an empty result.</p>
 *
 * <p>Example:
 * <pre>
 * MATCH (r:City {name:'Rome'})
 * CALL algo.msa(r, 'ROAD', 'distance')
 * YIELD source, target, weight, totalWeight
 * RETURN source.name, target.name, weight
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoMinSpanningArborescence extends AbstractAlgoProcedure {

  @Override
  public String getName() {
    return "algo.msa";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }


  @Override
  public String getDescription() {
    return "Computes the minimum spanning arborescence (directed MST) rooted at a given vertex using the Chu-Liu/Edmonds algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("source", "target", "weight", "totalWeight");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex root = extractVertex(args[0], "rootNode");
    final String[] relTypes = args.length > 1 ? extractRelTypes(args[1]) : null;
    final String weightProperty = args.length > 2 ? extractString(args[2], "weightProperty") : null;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> it = getAllVertices(db, null);
    while (it.hasNext())
      vertices.add(it.next());
    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final Integer rootIdxObj = ridToIdx.get(root.getIdentity());
    if (rootIdxObj == null)
      return Stream.empty();
    final int rootIdx = rootIdxObj;

    // Collect all directed edges as primitive arrays (two-pass, OUT direction = directed edges)
    // Pass 1: count
    int edgeCount = 0;
    for (int i = 0; i < n; i++) {
      final Iterable<Edge> edges = relTypes != null && relTypes.length > 0 ?
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT, relTypes) :
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT);
      for (final Edge e : edges)
        if (ridToIdx.containsKey(e.getIn()))
          edgeCount++;
    }

    // Pass 2: fill
    final int[] eFrom = new int[edgeCount];
    final int[] eTo = new int[edgeCount];
    final double[] eW = new double[edgeCount];
    int ec = 0;
    for (int i = 0; i < n; i++) {
      final Iterable<Edge> edges = relTypes != null && relTypes.length > 0 ?
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT, relTypes) :
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT);
      for (final Edge e : edges) {
        final Integer j = ridToIdx.get(e.getIn());
        if (j == null)
          continue;
        eFrom[ec] = i;
        eTo[ec] = j;
        if (weightProperty != null) {
          final Object w = e.get(weightProperty);
          eW[ec] = w instanceof Number num ? num.doubleValue() : 1.0;
        } else {
          eW[ec] = 1.0;
        }
        ec++;
      }
    }

    // Run Chu-Liu/Edmonds on node IDs 0..n-1, root = rootIdx
    final int[] msaEdges = edmonds(n, rootIdx, eFrom, eTo, eW, ec);
    if (msaEdges == null)
      return Stream.empty();

    double total = 0.0;
    for (final int ei : msaEdges)
      total += eW[ei];
    final double totalWeight = total;

    final List<Result> results = new ArrayList<>(msaEdges.length);
    for (final int ei : msaEdges) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("source", vertices.get(eFrom[ei]));
      r.setProperty("target", vertices.get(eTo[ei]));
      r.setProperty("weight", eW[ei]);
      r.setProperty("totalWeight", totalWeight);
      results.add(r);
    }
    return results.stream();
  }

  // ── Chu-Liu/Edmonds algorithm ────────────────────────────────────────────

  /**
   * Returns the indices (into the original eFrom/eTo/eW arrays) of the n-1 edges
   * that form the minimum spanning arborescence, or {@code null} if none exists.
   *
   * @param n          number of nodes (IDs 0..n-1)
   * @param root       root node ID
   * @param eFrom      edge source array (length ≥ m)
   * @param eTo        edge destination array
   * @param eW         edge weight array
   * @param m          number of edges
   */
  private static int[] edmonds(final int n, final int root,
      final int[] eFrom, final int[] eTo, final double[] eW, final int m) {
    if (n == 1)
      return new int[0];

    // ── Step 1: find cheapest incoming edge for each non-root vertex ────────
    final int[] bestEdge = new int[n];   // edge index of cheapest incoming
    final double[] bestW = new double[n];
    Arrays.fill(bestEdge, -1);
    Arrays.fill(bestW, Double.MAX_VALUE);

    for (int i = 0; i < m; i++) {
      final int v = eTo[i];
      if (v == root)
        continue;
      if (eW[i] < bestW[v]) {
        bestW[v] = eW[i];
        bestEdge[v] = i;
      }
    }

    // All non-root vertices must have an incoming edge
    for (int v = 0; v < n; v++) {
      if (v != root && bestEdge[v] == -1)
        return null; // No arborescence possible
    }

    // ── Step 2: detect cycles in selected edges (functional graph) ──────────
    // Follow par[v] = eFrom[bestEdge[v]] until root or revisit
    final int[] color = new int[n]; // 0=white, 1=grey, 2=black
    final int[] cycleId = new int[n];
    Arrays.fill(cycleId, -1);
    int numCycles = 0;
    final int[][] cycleNodes = new int[n][]; // cycleNodes[c] = nodes in cycle c

    for (int start = 0; start < n; start++) {
      if (start == root || color[start] == 2)
        continue;

      // Trace path from start
      final int[] path = new int[n];
      int pathLen = 0;
      int cur = start;
      while (cur != root && color[cur] == 0) {
        color[cur] = 1; // grey
        path[pathLen++] = cur;
        cur = eFrom[bestEdge[cur]]; // follow to source of best incoming edge
      }

      if (color[cur] == 1) {
        // cur is grey → it's in the current path → cycle found
        // Find where cur appears in path
        int cycleStart = pathLen - 1;
        while (path[cycleStart] != cur)
          cycleStart--;

        final int cLen = pathLen - cycleStart;
        final int[] cyc = new int[cLen];
        System.arraycopy(path, cycleStart, cyc, 0, cLen);
        for (final int node : cyc) {
          cycleId[node] = numCycles;
          color[node] = 2;
        }
        cycleNodes[numCycles++] = cyc;
        // Non-cycle prefix
        for (int i = 0; i < cycleStart; i++)
          color[path[i]] = 2;
      } else {
        for (int i = 0; i < pathLen; i++)
          color[path[i]] = 2;
      }
    }

    // ── Step 3: no cycles → return bestEdge for all non-root vertices ───────
    if (numCycles == 0) {
      final int[] result = new int[n - 1];
      int ri = 0;
      for (int v = 0; v < n; v++)
        if (v != root)
          result[ri++] = bestEdge[v];
      return result;
    }

    // ── Step 4: contract cycles ─────────────────────────────────────────────
    // Map each original node to its "super" node ID
    final int[] superNode = new int[n];
    int newN = 0;
    // Regular (non-cycle) nodes get IDs first
    for (int v = 0; v < n; v++)
      if (cycleId[v] == -1)
        superNode[v] = newN++;
    // Supernodes for cycles
    final int[] cycleSuper = new int[numCycles];
    for (int c = 0; c < numCycles; c++)
      cycleSuper[c] = newN++;
    // Map cycle nodes to their supernode
    for (int v = 0; v < n; v++)
      if (cycleId[v] >= 0)
        superNode[v] = cycleSuper[cycleId[v]];

    final int newRoot = superNode[root];

    // Build contracted edge list; skip internal cycle edges
    // Count new edges first
    int newM = 0;
    for (int i = 0; i < m; i++) {
      if (superNode[eFrom[i]] != superNode[eTo[i]])
        newM++;
    }

    final int[] nFrom = new int[newM];
    final int[] nTo = new int[newM];
    final double[] nW = new double[newM];
    final int[] origIdx = new int[newM]; // back-reference to original edge
    int ni = 0;
    for (int i = 0; i < m; i++) {
      final int su = superNode[eFrom[i]];
      final int sv = superNode[eTo[i]];
      if (su == sv)
        continue;
      nFrom[ni] = su;
      nTo[ni] = sv;
      // Adjust weight for edges entering a cycle: subtract the cycle edge it would replace
      nW[ni] = cycleId[eTo[i]] >= 0 ? eW[i] - bestW[eTo[i]] : eW[i];
      origIdx[ni] = i;
      ni++;
    }

    // ── Step 5: recurse on contracted graph ─────────────────────────────────
    final int[] subResult = edmonds(newN, newRoot, nFrom, nTo, nW, newM);
    if (subResult == null)
      return null;

    // ── Step 6: expand ───────────────────────────────────────────────────────
    // Start with all cycle edges selected
    final boolean[] inMSA = new boolean[m];
    for (int c = 0; c < numCycles; c++)
      for (final int v : cycleNodes[c])
        inMSA[bestEdge[v]] = true;

    // For each edge in the sub-result, add it to MSA and remove the replaced cycle edge
    for (final int sei : subResult) {
      final int origEdge = origIdx[sei];
      inMSA[origEdge] = true;
      final int dest = eTo[origEdge];
      if (cycleId[dest] >= 0)
        inMSA[bestEdge[dest]] = false; // remove the cycle edge that this replaces
    }

    // Collect selected original edge indices
    int cnt = 0;
    for (int i = 0; i < m; i++)
      if (inMSA[i])
        cnt++;
    final int[] result = new int[cnt];
    int ri = 0;
    for (int i = 0; i < m; i++)
      if (inMSA[i])
        result[ri++] = i;
    return result;
  }
}
