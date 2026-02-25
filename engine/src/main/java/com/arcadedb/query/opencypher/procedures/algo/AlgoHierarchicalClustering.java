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

    final String[] relTypes = args.length > 0 ? extractRelTypes(args[0]) : null;
    final int numClusters = args.length > 1 && args[1] instanceof Number n ? n.intValue() : 2;

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

    // Build neighbor BitSets for Jaccard similarity
    final BitSet[] neighborSets = new BitSet[n];
    for (int i = 0; i < n; i++) {
      neighborSets[i] = new BitSet(n);
      neighborSets[i].set(i); // include self for union calculation
      for (final int j : adj[i])
        neighborSets[i].set(j);
    }

    // Union-Find
    final int[] parent = new int[n];
    final int[] rank = new int[n];
    for (int i = 0; i < n; i++)
      parent[i] = i;

    int currentClusters = n;
    final int targetClusters = Math.max(1, Math.min(numClusters, n));

    // Agglomerative: repeatedly merge until we have targetClusters
    while (currentClusters > targetClusters) {
      // Find the two nodes (representing different clusters) with highest similarity
      // Similarity = common neighbors (Jaccard numerator) — use BitSet AND cardinality
      int bestU = -1;
      int bestV = -1;
      int bestSim = -1;

      for (int u = 0; u < n; u++) {
        final int rootU = find(parent, u);
        for (final int v : adj[u]) {
          if (v <= u)
            continue;
          final int rootV = find(parent, v);
          if (rootU == rootV)
            continue;

          // Jaccard similarity: |N(u) ∩ N(v)| / |N(u) ∪ N(v)|
          final BitSet intersection = (BitSet) neighborSets[u].clone();
          intersection.and(neighborSets[v]);
          final int sim = intersection.cardinality();

          if (sim > bestSim) {
            bestSim = sim;
            bestU = rootU;
            bestV = rootV;
          }
        }
      }

      if (bestU < 0 || bestV < 0)
        break; // no connected clusters to merge

      union(parent, rank, bestU, bestV);
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
      r.setProperty("nodeId", vertices.get(i).getIdentity());
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
}
