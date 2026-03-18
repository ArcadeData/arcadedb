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
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.*;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.dijkstra.singleSource(startNode, relTypes, weightProperty, direction?)
 * <p>
 * Computes the single-source shortest path (SSSP) from a given start node to all reachable
 * nodes using Dijkstra's algorithm with a binary min-heap. This extends the existing
 * source-target {@code algo.dijkstra} to return results for all reachable targets at once.
 * </p>
 * <p>
 * When a Graph Analytical View (GAV) with edge properties is available, the algorithm runs
 * entirely on CSR arrays with zero OLTP access, providing massive speedups on large graphs.
 * </p>
 * <p>
 * Parameters:
 * <ul>
 *   <li>startNode (required): source vertex</li>
 *   <li>relTypes (required): relationship type(s) to traverse</li>
 *   <li>weightProperty (required): edge property to use as weight (must be numeric)</li>
 *   <li>direction (optional): "OUT", "IN", or "BOTH" (default "OUT")</li>
 * </ul>
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (start:City {name: 'London'})
 * CALL algo.dijkstra.singleSource(start, 'ROAD', 'distance')
 * YIELD node, cost
 * RETURN node.name, cost ORDER BY cost
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoDijkstraSingleSource extends AbstractAlgoProcedure {
  public static final String NAME = "algo.dijkstra.singleSource";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Single-source shortest path (Dijkstra) from a start node to all reachable nodes";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "cost");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode      = extractVertex(args[0], "startNode");
    final String[] relTypes     = extractRelTypes(args[1]);
    final String weightProperty = extractString(args[2], "weightProperty");
    final Vertex.DIRECTION dir  = args.length > 3 ? parseDirection(extractString(args[3], "direction")) : Vertex.DIRECTION.OUT;

    final Database db = context.getDatabase();

    // Try CSR-accelerated path: requires a provider with edge properties
    final GraphTraversalProvider provider = findProvider(db, relTypes);
    if (provider instanceof GraphAnalyticalView gav && gav.hasEdgeProperties()) {
      context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
      return executeWithCSR(context, gav, startNode.getIdentity(), relTypes, weightProperty, dir);
    }

    // Fall back to OLTP path
    return executeWithOLTP(db, startNode, relTypes, weightProperty, dir);
  }

  private Stream<Result> executeWithCSR(final CommandContext context, final GraphAnalyticalView gav, final RID startRid,
      final String[] relTypes, final String weightProperty, final Vertex.DIRECTION dir) {
    final int n = gav.getNodeCount();
    if (n == 0)
      return Stream.empty();

    final int src = gav.getNodeId(startRid);
    if (src < 0)
      return Stream.empty();

    final double[] dist = GraphAlgorithms.dijkstraSingleSource(
        gav, src, weightProperty, dir, relTypes);
    long reachable = 0;
    for (int i = 0; i < n; i++)
      if (i != src && dist[i] < Double.POSITIVE_INFINITY) reachable++;
    context.setVariable(CommandContext.RESULT_COUNT_HINT_VAR, reachable);

    return IntStream.range(0, n).filter(i -> i != src && dist[i] < Double.POSITIVE_INFINITY).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", gav.getRID(i));
      r.setProperty("cost", dist[i]);
      return (Result) r;
    });
  }

  private Stream<Result> executeWithOLTP(final Database db, final Vertex startNode,
      final String[] relTypes, final String weightProperty, final Vertex.DIRECTION dir) {
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final Integer startIdxObj = ridToIdx.get(startNode.getIdentity());
    if (startIdxObj == null)
      return Stream.empty();
    final int src = startIdxObj;

    // Build weighted adjacency once to avoid OLTP per-vertex during Dijkstra
    final Set<String> relTypeSet = relTypes != null ? new HashSet<>(Arrays.asList(relTypes)) : null;
    final int[][] adjNeighbors = new int[n][];
    final double[][] adjWeights = new double[n][];
    for (int i = 0; i < n; i++) {
      final Vertex v = vertices.get(i);
      final List<int[]> nbrs = new ArrayList<>();
      final List<Double> wts = new ArrayList<>();
      for (final Edge edge : v.getEdges(dir)) {
        if (relTypeSet != null && !relTypeSet.contains(edge.getTypeName()))
          continue;
        final RID neighborRid = neighborRid(edge, v.getIdentity(), dir);
        final Integer nbrIdx = ridToIdx.get(neighborRid);
        if (nbrIdx == null)
          continue;
        double weight = 1.0;
        if (weightProperty != null) {
          final Object w = edge.get(weightProperty);
          if (w instanceof Number num)
            weight = num.doubleValue();
        }
        if (weight < 0)
          continue;
        nbrs.add(new int[]{ nbrIdx });
        wts.add(weight);
      }
      adjNeighbors[i] = new int[nbrs.size()];
      adjWeights[i] = new double[wts.size()];
      for (int j = 0; j < nbrs.size(); j++) {
        adjNeighbors[i][j] = nbrs.get(j)[0];
        adjWeights[i][j] = wts.get(j);
      }
    }

    // Dijkstra purely in-memory
    final double[] dist = new double[n];
    Arrays.fill(dist, Double.POSITIVE_INFINITY);
    dist[src] = 0.0;

    final PriorityQueue<double[]> heap = new PriorityQueue<>((a, b) -> Double.compare(a[0], b[0]));
    heap.offer(new double[]{ 0.0, src });

    while (!heap.isEmpty()) {
      final double[] entry = heap.poll();
      final double d = entry[0];
      final int u = (int) entry[1];
      if (d > dist[u])
        continue;

      for (int j = 0; j < adjNeighbors[u].length; j++) {
        final int v = adjNeighbors[u][j];
        final double newDist = d + adjWeights[u][j];
        if (newDist < dist[v]) {
          dist[v] = newDist;
          heap.offer(new double[]{ newDist, v });
        }
      }
    }

    return IntStream.range(0, n).filter(i -> i != src && dist[i] < Double.POSITIVE_INFINITY).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i).getIdentity());
      r.setProperty("cost", dist[i]);
      return (Result) r;
    });
  }
}
