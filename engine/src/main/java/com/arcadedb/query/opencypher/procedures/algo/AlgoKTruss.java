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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.kTruss(relTypes?, k?)
 * <p>
 * Computes the k-truss decomposition of the graph. Each node is assigned the maximum k for
 * which it participates in a k-truss subgraph (a subgraph where every edge is supported by
 * at least k-2 triangles). Uses BitSet-based triangle counting for efficiency.
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

    final String[] relTypes = args.length > 0 ? extractRelTypes(args[0]) : null;
    final int kParam = args.length > 1 && args[1] instanceof Number n ? n.intValue() : 3;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.BOTH, relTypes);

    // Build neighbor BitSets for O(1) membership test
    final BitSet[] neighborSets = new BitSet[n];
    for (int i = 0; i < n; i++) {
      neighborSets[i] = new BitSet(n);
      for (final int j : adj[i])
        neighborSets[i].set(j);
    }

    // Build canonical edge list (u < v only for undirected)
    // Map each edge to an index
    final Map<Long, Integer> edgeMap = new HashMap<>();
    final List<int[]> edges = new ArrayList<>();
    for (int u = 0; u < n; u++) {
      for (final int v : adj[u]) {
        if (u < v) {
          final long key = ((long) u << 32) | (v & 0xFFFFFFFFL);
          if (!edgeMap.containsKey(key)) {
            edgeMap.put(key, edges.size());
            edges.add(new int[]{u, v});
          }
        }
      }
    }

    final int edgeCount = edges.size();
    // support[e] = number of triangles edge e is part of
    final int[] support = new int[edgeCount];
    final boolean[] removed = new boolean[edgeCount];

    // Compute initial support for each edge
    for (int e = 0; e < edgeCount; e++) {
      final int u = edges.get(e)[0];
      final int v = edges.get(e)[1];
      // |N(u) ∩ N(v)| — use BitSet AND
      final BitSet intersection = (BitSet) neighborSets[u].clone();
      intersection.and(neighborSets[v]);
      support[e] = intersection.cardinality();
    }

    // Track node truss numbers
    final int[] trussNumber = new int[n];
    // Initialize to max possible k (will be refined)

    // Iterative edge removal for k-truss decomposition
    // For each k from 3 up, remove edges with support < k-2
    int currentK = 2;
    boolean anyRemoved = true;
    while (anyRemoved) {
      anyRemoved = false;
      for (int e = 0; e < edgeCount; e++) {
        if (!removed[e] && support[e] < currentK - 2) {
          removed[e] = true;
          anyRemoved = true;
          final int u = edges.get(e)[0];
          final int v = edges.get(e)[1];

          // Update supports of edges sharing a triangle with (u,v)
          // Common neighbors of u and v
          final BitSet commonNeighbors = (BitSet) neighborSets[u].clone();
          commonNeighbors.and(neighborSets[v]);

          for (int w = commonNeighbors.nextSetBit(0); w >= 0; w = commonNeighbors.nextSetBit(w + 1)) {
            // Decrement support of edge (u,w) and (v,w)
            final int uw = u < w ? edgeIndex(edgeMap, u, w) : edgeIndex(edgeMap, w, u);
            final int vw = v < w ? edgeIndex(edgeMap, v, w) : edgeIndex(edgeMap, w, v);
            if (uw >= 0 && !removed[uw])
              support[uw]--;
            if (vw >= 0 && !removed[vw])
              support[vw]--;
          }

          // Remove edge from adjacency BitSets
          neighborSets[u].clear(v);
          neighborSets[v].clear(u);
        }
      }

      if (!anyRemoved && currentK <= kParam) {
        currentK++;
        anyRemoved = true; // force next iteration to check
      }
    }

    // Assign truss numbers: nodes in remaining edges get at least kParam truss number
    // For full decomposition, track max k where each node is still connected
    // Re-run the full decomposition
    final int[] nodeTruss = computeFullTrussDecomposition(n, adj, neighborSets, edgeMap, edges, edgeCount);

    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", vertices.get(i).getIdentity());
      r.setProperty("trussNumber", nodeTruss[i]);
      results.add(r);
    }
    return results.stream();
  }

  private int edgeIndex(final Map<Long, Integer> edgeMap, final int u, final int v) {
    final long key = ((long) u << 32) | (v & 0xFFFFFFFFL);
    final Integer idx = edgeMap.get(key);
    return idx == null ? -1 : idx;
  }

  private int[] computeFullTrussDecomposition(final int n, final int[][] adj,
      final BitSet[] neighborSetsOrig, final Map<Long, Integer> edgeMap,
      final List<int[]> edges, final int edgeCount) {

    // Rebuild neighbor sets (fresh copy)
    final BitSet[] neighborSets = new BitSet[n];
    for (int i = 0; i < n; i++) {
      neighborSets[i] = new BitSet(n);
      for (final int j : adj[i])
        neighborSets[i].set(j);
    }

    final int[] support = new int[edgeCount];
    final boolean[] removed = new boolean[edgeCount];
    // edgeTruss[e] = the truss number assigned to edge e
    final int[] edgeTruss = new int[edgeCount];

    // Compute initial support
    for (int e = 0; e < edgeCount; e++) {
      final int u = edges.get(e)[0];
      final int v = edges.get(e)[1];
      final BitSet inter = (BitSet) neighborSets[u].clone();
      inter.and(neighborSets[v]);
      support[e] = inter.cardinality();
    }

    int k = 2;
    int removedCount = 0;
    while (removedCount < edgeCount) {
      boolean anyRemoved = true;
      while (anyRemoved) {
        anyRemoved = false;
        for (int e = 0; e < edgeCount; e++) {
          if (!removed[e] && support[e] < k - 2) {
            removed[e] = true;
            edgeTruss[e] = k - 1;
            removedCount++;
            anyRemoved = true;
            final int u = edges.get(e)[0];
            final int v = edges.get(e)[1];

            final BitSet common = (BitSet) neighborSets[u].clone();
            common.and(neighborSets[v]);
            for (int w = common.nextSetBit(0); w >= 0; w = common.nextSetBit(w + 1)) {
              final int uw = u < w ? edgeIndex(edgeMap, u, w) : edgeIndex(edgeMap, w, u);
              final int vw = v < w ? edgeIndex(edgeMap, v, w) : edgeIndex(edgeMap, w, v);
              if (uw >= 0 && !removed[uw])
                support[uw]--;
              if (vw >= 0 && !removed[vw])
                support[vw]--;
            }
            neighborSets[u].clear(v);
            neighborSets[v].clear(u);
          }
        }
      }
      if (removedCount < edgeCount)
        k++;
    }

    // Assign each remaining edge (never removed) truss = k
    for (int e = 0; e < edgeCount; e++) {
      if (!removed[e])
        edgeTruss[e] = k;
    }

    // Node truss = max truss of any incident edge
    final int[] nodeTruss = new int[n];
    for (int e = 0; e < edgeCount; e++) {
      final int u = edges.get(e)[0];
      final int v = edges.get(e)[1];
      if (edgeTruss[e] > nodeTruss[u])
        nodeTruss[u] = edgeTruss[e];
      if (edgeTruss[e] > nodeTruss[v])
        nodeTruss[v] = edgeTruss[e];
    }
    return nodeTruss;
  }
}
