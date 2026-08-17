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
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Procedure: algo.bellmanford(startNode, endNode, relTypes, weightProperty)
 * <p>
 * Finds the shortest path between two nodes using the Bellman-Ford algorithm, which
 * supports graphs with negative edge weights (unlike Dijkstra). It also detects
 * negative-weight cycles.
 * </p>
 * <p>
 * When a Graph Analytical View with edge properties is available, the edge list is built
 * directly from CSR arrays, avoiding OLTP edge deserialization.
 * </p>
 * <p>
 * Parameters:
 * <ul>
 *   <li>startNode: source vertex</li>
 *   <li>endNode: destination vertex</li>
 *   <li>relTypes (string): relationship type filter (empty string for all types)</li>
 *   <li>weightProperty (string): edge property to use as weight</li>
 * </ul>
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:City {name: 'A'}), (b:City {name: 'B'})
 * CALL algo.bellmanford(a, b, 'ROAD', 'distance')
 * YIELD path, weight, negativeCycle
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoBellmanFord extends AbstractAlgoProcedure {
  public static final String NAME = "algo.bellmanford";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 4;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Finds the shortest path between two nodes using the Bellman-Ford algorithm (supports negative weights)";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("path", "weight", "negativeCycle");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode = extractVertex(args[0], "startNode");
    final Vertex endNode = extractVertex(args[1], "endNode");
    final String relType = extractString(args[2], "relTypes");
    final String weightProperty = extractString(args[3], "weightProperty");

    final Database db = context.getDatabase();
    final String[] relTypes = relType != null && !relType.isEmpty() ? new String[] { relType } : null;
    final WorkGuard guard = newWorkGuard(context);

    final GraphData graph = loadGraph(db, null, relTypes, context);
    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    final int startIdx = graph.indexOf(startNode.getIdentity());
    final int endIdx = graph.indexOf(endNode.getIdentity());
    if (startIdx < 0 || endIdx < 0)
      return Stream.empty();

    // Build the edge list. Neighbours and weights come out of one walk of the same edges - columnar when the
    // view materialises the property, the edge records otherwise - so a weight always belongs to the edge it is
    // written against. The form this replaced read the weights through a second, independently ordered
    // traversal (issue #6301) and, worse, fell back to a unit weight for EVERY edge whenever the graph was
    // CSR-backed without the property column: the same query answered `weightProperty` on OLTP and ignored it
    // entirely once a Graph Analytical View existed.
    final GraphData.WeightedAdjacency weighted = graph.weightedAdjacency(guard, Vertex.DIRECTION.OUT,
        weightProperty != null && !weightProperty.isEmpty() ? weightProperty : null, relTypes);
    final int[][] adj = weighted.neighbors();
    final double[][] adjW = weighted.weights();

    int edgeCount = 0;
    for (int i = 0; i < n; i++)
      edgeCount += adj[i].length;

    // Parallel primitive arrays rather than a List<int[]> and a List<Double>: the relaxation below reads every
    // edge up to n - 1 times, so this is the hottest loop in the procedure and the one place boxing is paid for
    // over and over.
    final int[] edgeFrom = new int[edgeCount];
    final int[] edgeTo = new int[edgeCount];
    final double[] edgeWeight = new double[edgeCount];
    int e = 0;
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < adj[i].length; j++) {
        edgeFrom[e] = i;
        edgeTo[e] = adj[i][j];
        edgeWeight[e] = adjW[i][j];
        e++;
      }
    }

    final double[] dist = new double[n];
    final int[] prev = new int[n];
    for (int i = 0; i < n; i++) {
      dist[i] = Double.MAX_VALUE;
      prev[i] = -1;
    }
    dist[startIdx] = 0.0;

    // Bellman-Ford relaxation: V-1 iterations
    for (int iter = 0; iter < n - 1; iter++) {
      // n - 1 passes over every edge is O(V x E), and nothing about the call bounds either factor - the graph
      // alone sizes both. The early exit below ends a converged run, not a long one.
      guard.check();
      boolean anyRelaxed = false;
      for (int k = 0; k < edgeCount; k++) {
        final int u = edgeFrom[k];
        final int v = edgeTo[k];
        final double w = edgeWeight[k];
        if (dist[u] != Double.MAX_VALUE && dist[u] + w < dist[v]) {
          dist[v] = dist[u] + w;
          prev[v] = u;
          anyRelaxed = true;
        }
      }
      if (!anyRelaxed)
        break;
    }

    // Check for negative cycles reachable from start: one more pass over every edge, guarded like the
    // relaxation passes above rather than left as the one unabortable loop in the method.
    boolean negativeCycle = false;
    guard.check();
    for (int k = 0; k < edgeCount; k++) {
      final int u = edgeFrom[k];
      final int v = edgeTo[k];
      final double w = edgeWeight[k];
      if (dist[u] != Double.MAX_VALUE && dist[u] + w < dist[v]) {
        negativeCycle = true;
        break;
      }
    }

    // Reconstruct path if destination is reachable
    if (dist[endIdx] == Double.MAX_VALUE) {
      if (negativeCycle) {
        final ResultInternal result = new ResultInternal();
        result.setProperty("path", null);
        result.setProperty("weight", null);
        result.setProperty("negativeCycle", true);
        return Stream.of(result);
      }
      return Stream.empty();
    }

    // Build path RIDs with cycle guard (negative cycles may corrupt prev[])
    final LinkedList<RID> pathRids = new LinkedList<>();
    int current = endIdx;
    final Set<Integer> visited = new HashSet<>();
    while (current != -1) {
      if (!visited.add(current)) {
        final ResultInternal cycleResult = new ResultInternal();
        cycleResult.setProperty("path", null);
        cycleResult.setProperty("weight", null);
        cycleResult.setProperty("negativeCycle", true);
        return Stream.of(cycleResult);
      }
      pathRids.addFirst(graph.getRID(current));
      current = prev[current];
      if (current == startIdx) {
        pathRids.addFirst(graph.getRID(current));
        break;
      }
    }

    final Map<String, Object> path = buildPath(new ArrayList<>(pathRids), db);

    final ResultInternal result = new ResultInternal();
    result.setProperty("path", path);
    result.setProperty("weight", dist[endIdx]);
    result.setProperty("negativeCycle", negativeCycle);
    return Stream.of(result);
  }

}
