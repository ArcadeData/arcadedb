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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    final int k                = args.length > 0 && args[0] instanceof Number num ? num.intValue() : 10;
    final String[] relTypes    = args.length > 1 ? extractRelTypes(args[1]) : null;
    final Vertex.DIRECTION dir = args.length > 2 ? parseDirection(extractString(args[2], "direction")) : Vertex.DIRECTION.BOTH;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, dir, relTypes);

    // Build BitSets for O(1) intersection
    final BitSet[] neighborSets = new BitSet[n];
    for (int i = 0; i < n; i++) {
      neighborSets[i] = new BitSet(n);
      for (final int j : adj[i])
        neighborSets[i].set(j);
    }

    final List<Result> results = new ArrayList<>();

    for (int u = 0; u < n; u++) {
      if (adj[u].length == 0)
        continue;

      // Find top-k neighbours by Jaccard similarity (avoid duplicate pairs by requiring v > u)
      // We collect all candidates and pick top-k
      double[] topSim = new double[k];
      int[] topIdx = new int[k];
      int topCount = 0;
      double minTop = 0.0;

      for (int v = 0; v < n; v++) {
        if (v == u || adj[v].length == 0)
          continue;

        // Jaccard = |intersection| / |union|
        final BitSet intersection = (BitSet) neighborSets[u].clone();
        intersection.and(neighborSets[v]);
        final int inter = intersection.cardinality();
        if (inter == 0)
          continue;
        final int union = adj[u].length + adj[v].length - inter;
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
        r.setProperty("node1", vertices.get(u));
        r.setProperty("node2", vertices.get(topIdx[i]));
        r.setProperty("similarity", topSim[i]);
        results.add(r);
      }
    }
    return results.stream();
  }
}
