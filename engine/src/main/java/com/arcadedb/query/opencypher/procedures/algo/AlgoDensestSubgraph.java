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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.densestSubgraph(relTypes?)
 * <p>
 * Finds the densest subgraph using Charikar's greedy peeling algorithm (2-approximation),
 * with O(V+E) edge processing. Density is defined as edges/nodes ratio for the subgraph.
 * Returns each node with a flag indicating whether it belongs to the densest subgraph found,
 * and the density value of that subgraph.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.densestSubgraph()
 * YIELD node, inDenseSubgraph, density
 * RETURN node.name, inDenseSubgraph, density ORDER BY inDenseSubgraph DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoDensestSubgraph extends AbstractAlgoProcedure {
  public static final String NAME = "algo.densestSubgraph";

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
    return 1;
  }

  @Override
  public String getDescription() {
    return "Find the densest subgraph using Charikar's greedy peeling algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "inDenseSubgraph", "density");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = args.length > 0 ? extractRelTypes(args[0]) : null;

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

    // Charikar's greedy peeling
    final int[] deg = new int[n];
    int totalEdges = 0;
    for (int i = 0; i < n; i++) {
      deg[i] = adj[i].length;
      totalEdges += deg[i];
    }
    totalEdges /= 2;

    final boolean[] removed = new boolean[n];
    // bestSubgraph: snapshot of non-removed nodes at best density
    final boolean[] bestSubgraph = new boolean[n];
    Arrays.fill(bestSubgraph, true); // initially all nodes are in
    double bestDensity = n > 0 ? (double) totalEdges / n : 0.0;
    int remaining = n;

    while (remaining > 1) {
      final double density = (double) totalEdges / remaining;
      if (density > bestDensity) {
        bestDensity = density;
        // Snapshot current non-removed set
        for (int i = 0; i < n; i++)
          bestSubgraph[i] = !removed[i];
      }

      // Find minimum-degree non-removed vertex (linear scan)
      int minDeg = Integer.MAX_VALUE;
      int minV = -1;
      for (int i = 0; i < n; i++) {
        if (!removed[i] && deg[i] < minDeg) {
          minDeg = deg[i];
          minV = i;
        }
      }
      if (minV == -1)
        break;

      removed[minV] = true;
      remaining--;
      for (final int u : adj[minV]) {
        if (!removed[u]) {
          deg[u]--;
          totalEdges--;
        }
      }
    }

    // Check the last remaining singleton
    if (remaining == 1) {
      final double density = totalEdges > 0 ? (double) totalEdges / remaining : 0.0;
      if (density > bestDensity) {
        bestDensity = density;
        for (int i = 0; i < n; i++)
          bestSubgraph[i] = !removed[i];
      }
    }

    final double finalDensity = bestDensity;

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("inDenseSubgraph", bestSubgraph[i]);
      r.setProperty("density", finalDensity);
      return (Result) r;
    });
  }
}
