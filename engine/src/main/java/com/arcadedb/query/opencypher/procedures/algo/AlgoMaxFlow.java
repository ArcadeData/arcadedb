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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.maxFlow(sourceNode, sinkNode, relTypes?, capacityProperty?)
 * <p>
 * Computes the maximum flow between two nodes using the Edmonds-Karp algorithm
 * (BFS-based Ford-Fulkerson). Returns the maximum flow value along with source
 * and sink node identities.
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (s:Station {name:'A'}), (t:Station {name:'Z'})
 * CALL algo.maxFlow(s, t, 'PIPE', 'capacity')
 * YIELD maxFlow, sourceId, sinkId
 * RETURN maxFlow
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoMaxFlow extends AbstractAlgoProcedure {
  public static final String NAME = "algo.maxFlow";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Computes maximum flow between two nodes using the Edmonds-Karp algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("maxFlow", "sourceId", "sinkId");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex sourceNode       = extractVertex(args[0], "sourceNode");
    final Vertex sinkNode         = extractVertex(args[1], "sinkNode");
    final String[] relTypes       = args.length > 2 ? extractRelTypes(args[2]) : null;
    final String capacityProperty = args.length > 3 ? extractString(args[3], "capacityProperty") : null;

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);
    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    final int srcIdx = graph.indexOf(sourceNode.getIdentity());
    final int snkIdx = graph.indexOf(sinkNode.getIdentity());
    if (srcIdx < 0 || snkIdx < 0)
      return Stream.empty();

    // Edmonds-Karp keeps a dense capacity matrix and a dense residual matrix, both nodeCount x nodeCount and
    // both alive to the end: 1.6 GB at 10 000 nodes, with no knob involved. Reserved before the first is
    // allocated so that a graph too large for the dense formulation is a client error naming the node count and
    // the budget, rather than an OutOfMemoryError.
    final MemoryBudget memory = graph.memory();
    memory.reserve(saturatingProduct(2L, matrixBytes(n, n, DOUBLE_BYTES)),
        "the capacity and residual matrices", "2 matrices of " + n + " x " + n + " nodes");

    // Build capacity matrix as n×n array. Capacity extraction goes through GraphData.weightedAdjacency (issue
    // #6316), the same shared helper the other weighted algo.* procedures read edge weights through, rather
    // than a hand-rolled getEdges() walk. A blank capacityProperty is normalised to null first so both mean
    // "no property, every edge has capacity 1.0" exactly as before.
    final String capacityProp = capacityProperty != null && !capacityProperty.isEmpty() ? capacityProperty : null;
    final GraphData.WeightedAdjacency weighted = graph.weightedAdjacency(guard, Vertex.DIRECTION.OUT, capacityProp, relTypes);
    final int[][] neighbors = weighted.neighbors();
    final double[][] weights = weighted.weights();

    final double[][] capacity = new double[n][n];
    for (int i = 0; i < n; i++) {
      final int[] row = neighbors[i];
      final double[] rowWeights = weights[i];
      for (int k = 0; k < row.length; k++) {
        final int j = row[k];
        final double cap = rowWeights[k];
        capacity[i][j] += cap;
        // For undirected: also add reverse capacity
        capacity[j][i] += cap;
      }
    }

    // Edmonds-Karp: BFS to find augmenting paths
    final double[][] residual = new double[n][n];
    for (int i = 0; i < n; i++)
      residual[i] = Arrays.copyOf(capacity[i], n);

    final int[] queue  = new int[n];
    final int[] parent = new int[n];
    double maxFlow = 0.0;

    while (true) {
      // Edmonds-Karp augments once per iteration and each augmentation is a BFS over the dense residual
      // matrix, so the loop is O(V x E²) with the graph alone sizing it - no knob, and until issue #6302 no
      // way for a thread interrupt or arcadedb.command.timeout to end it.
      guard.check();
      // BFS to find augmenting path from src to snk
      Arrays.fill(parent, -1);
      parent[srcIdx] = srcIdx;
      int head = 0, tail = 0;
      queue[tail++] = srcIdx;

      bfs:
      while (head < tail) {
        // Scanning one settled node's residual row is O(V), so a single BFS is O(V²) and needs a checkpoint of
        // its own rather than one per augmentation.
        guard.check();
        final int u = queue[head++];
        for (int v = 0; v < n; v++) {
          if (parent[v] == -1 && residual[u][v] > 0) {
            parent[v] = u;
            if (v == snkIdx)
              break bfs;
            queue[tail++] = v;
          }
        }
      }

      if (parent[snkIdx] == -1)
        break; // No augmenting path found

      // Find minimum residual capacity along the path
      double pathFlow = Double.MAX_VALUE;
      int v = snkIdx;
      while (v != srcIdx) {
        final int u = parent[v];
        if (residual[u][v] < pathFlow)
          pathFlow = residual[u][v];
        v = u;
      }

      // Update residual capacities
      v = snkIdx;
      while (v != srcIdx) {
        final int u = parent[v];
        residual[u][v] -= pathFlow;
        residual[v][u] += pathFlow;
        v = u;
      }

      maxFlow += pathFlow;
    }

    final ResultInternal result = new ResultInternal();
    result.setProperty("maxFlow", maxFlow);
    result.setProperty("sourceId", sourceNode.getIdentity());
    result.setProperty("sinkId", sinkNode.getIdentity());
    return Stream.of(result);
  }
}
