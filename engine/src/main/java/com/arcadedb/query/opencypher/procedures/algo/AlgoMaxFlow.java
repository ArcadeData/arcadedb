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
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);

    final Integer srcIdx = ridToIdx.get(sourceNode.getIdentity());
    final Integer snkIdx = ridToIdx.get(sinkNode.getIdentity());
    if (srcIdx == null || snkIdx == null)
      return Stream.empty();

    // Build capacity matrix as n×n array
    final double[][] capacity = new double[n][n];

    for (int i = 0; i < n; i++) {
      final Iterable<Edge> edges = relTypes != null && relTypes.length > 0 ?
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT, relTypes) :
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT);
      for (final Edge e : edges) {
        final Integer j = ridToIdx.get(e.getIn());
        if (j == null)
          continue;
        double cap = 1.0;
        if (capacityProperty != null && !capacityProperty.isEmpty()) {
          final Object w = e.get(capacityProperty);
          if (w instanceof Number num)
            cap = num.doubleValue();
        }
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
      // BFS to find augmenting path from src to snk
      Arrays.fill(parent, -1);
      parent[srcIdx] = srcIdx;
      int head = 0, tail = 0;
      queue[tail++] = srcIdx;

      bfs:
      while (head < tail) {
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
