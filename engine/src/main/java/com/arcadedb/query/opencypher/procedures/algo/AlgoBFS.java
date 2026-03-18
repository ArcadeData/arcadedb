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
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.bfs(startNode, relTypes?, direction?, maxDepth?)
 * <p>
 * Performs a Breadth-First Search (BFS) from the given start node and returns all reachable
 * nodes in BFS order together with their depth from the start node.
 * </p>
 * <p>
 * When a Graph Analytical View (GAV) is available, the algorithm uses zero-allocation
 * NeighborView iteration over CSR arrays, avoiding per-node array materialization.
 * </p>
 * <p>
 * Parameters:
 * <ul>
 *   <li>startNode (required): the node from which to start the traversal</li>
 *   <li>relTypes (optional): relationship types to traverse (null = all)</li>
 *   <li>direction (optional): "OUT", "IN", or "BOTH" (default "BOTH")</li>
 *   <li>maxDepth (optional): maximum depth to traverse (default unbounded)</li>
 * </ul>
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (start:Person {name: 'Alice'})
 * CALL algo.bfs(start, 'KNOWS', 'BOTH', 3)
 * YIELD node, depth
 * RETURN node.name, depth ORDER BY depth
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoBFS extends AbstractAlgoProcedure {
  public static final String NAME = "algo.bfs";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Breadth-First Search traversal from a start node";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "depth");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode     = extractVertex(args[0], "startNode");
    final String[] relTypes    = args.length > 1 ? extractRelTypes(args[1]) : null;
    final Vertex.DIRECTION dir = args.length > 2 ? parseDirection(extractString(args[2], "direction")) : Vertex.DIRECTION.BOTH;
    final int maxDepth         = args.length > 3 ? ((Number) args[3]).intValue() : Integer.MAX_VALUE;

    final Database db = context.getDatabase();

    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    final int startIdx = graph.indexOf(startNode.getIdentity());
    if (startIdx < 0)
      return Stream.empty();

    // Try zero-allocation NeighborView path (CSR-backed)
    final NeighborView nv = graph.neighborView(dir, relTypes);
    if (nv != null)
      return bfsWithNeighborView(nv, graph, n, startIdx, maxDepth);

    // Fallback: materialize adjacency arrays
    final int[][] adj = graph.adjacency(dir, relTypes);
    return bfsWithAdjacency(adj, graph, n, startIdx, maxDepth);
  }

  private Stream<Result> bfsWithNeighborView(final NeighborView nv, final GraphData graph,
      final int n, final int startIdx, final int maxDepth) {
    final int[] nbrs = nv.neighbors();
    final int[] queue = new int[n];
    final int[] depth = new int[n];
    final boolean[] visited = new boolean[n];

    int head = 0, tail = 0;
    queue[tail++] = startIdx;
    visited[startIdx] = true;
    depth[startIdx] = 0;

    // BFS traversal: record visited nodes and their depths
    final int[] visited_nodes = new int[n];
    int visitedCount = 0;

    while (head < tail) {
      final int v = queue[head++];
      if (depth[v] >= maxDepth)
        continue;
      for (int j = nv.offset(v), end = nv.offsetEnd(v); j < end; j++) {
        final int u = nbrs[j];
        if (!visited[u]) {
          visited[u] = true;
          depth[u] = depth[v] + 1;
          queue[tail++] = u;
          visited_nodes[visitedCount++] = u;
        }
      }
    }

    final int count = visitedCount;
    return IntStream.range(0, count).mapToObj(idx -> {
      final int u = visited_nodes[idx];
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(u));
      r.setProperty("depth", depth[u]);
      return (Result) r;
    });
  }

  private Stream<Result> bfsWithAdjacency(final int[][] adj, final GraphData graph,
      final int n, final int startIdx, final int maxDepth) {
    final int[] queue = new int[n];
    final int[] depth = new int[n];
    final boolean[] visited = new boolean[n];

    int head = 0, tail = 0;
    queue[tail++] = startIdx;
    visited[startIdx] = true;
    depth[startIdx] = 0;

    // BFS traversal: record visited nodes and their depths
    final int[] visited_nodes = new int[n];
    int visitedCount = 0;

    while (head < tail) {
      final int v = queue[head++];
      if (depth[v] >= maxDepth)
        continue;
      for (final int u : adj[v]) {
        if (!visited[u]) {
          visited[u] = true;
          depth[u] = depth[v] + 1;
          queue[tail++] = u;
          visited_nodes[visitedCount++] = u;
        }
      }
    }

    final int count = visitedCount;
    return IntStream.range(0, count).mapToObj(idx -> {
      final int u = visited_nodes[idx];
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(u));
      r.setProperty("depth", depth[u]);
      return (Result) r;
    });
  }
}
