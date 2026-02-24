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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.biconnectedComponents(relTypes?)
 * <p>
 * Finds all biconnected components (2-edge-connected subgraphs) in the graph using a
 * non-recursive DFS with an explicit edge stack. A biconnected component is a maximal
 * subgraph that has no articulation point (i.e. it cannot be disconnected by removing a
 * single vertex).
 * </p>
 * <p>
 * Each node is yielded once per biconnected component it belongs to; nodes belonging to more
 * than one component (articulation points) appear multiple times with different componentIds.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.biconnectedComponents()
 * YIELD node, componentId
 * RETURN node.name, componentId ORDER BY componentId
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoBiconnectedComponents extends AbstractAlgoProcedure {
  public static final String NAME = "algo.biconnectedComponents";

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
    return "Finds all biconnected components using DFS with edge stack";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "componentId");
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

    final int[] disc   = new int[n];
    final int[] low    = new int[n];
    final int[] parent = new int[n];
    Arrays.fill(disc, -1);
    Arrays.fill(parent, -1);

    // node → list of component IDs it belongs to
    final List<List<Integer>> nodeComponents = new ArrayList<>(n);
    for (int i = 0; i < n; i++)
      nodeComponents.add(new ArrayList<>());

    final int[] timer   = { 0 };
    final int[] compId  = { 0 };

    // Edge stack for collecting edges of current component
    // Each entry: [u, v]
    final Deque<int[]> edgeStack = new ArrayDeque<>();

    for (int start = 0; start < n; start++) {
      if (disc[start] != -1)
        continue;
      dfs(start, adj, disc, low, parent, timer, compId, edgeStack, nodeComponents);
    }

    final List<Result> results = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      for (final int cid : nodeComponents.get(i)) {
        final ResultInternal r = new ResultInternal();
        r.setProperty("node", vertices.get(i));
        r.setProperty("componentId", cid);
        results.add(r);
      }
    }
    return results.stream();
  }

  private void dfs(final int root, final int[][] adj,
      final int[] disc, final int[] low, final int[] parent,
      final int[] timer, final int[] compId,
      final Deque<int[]> edgeStack, final List<List<Integer>> nodeComponents) {

    // Iterative DFS using an explicit call stack: [node, adjacencyPointer]
    final Deque<int[]> callStack = new ArrayDeque<>();
    disc[root] = low[root] = timer[0]++;
    callStack.push(new int[]{ root, 0 });

    while (!callStack.isEmpty()) {
      final int[] frame = callStack.peek();
      final int u = frame[0];
      final int ki = frame[1];

      if (ki < adj[u].length) {
        frame[1]++; // advance pointer
        final int v = adj[u][ki];

        if (disc[v] == -1) {
          // Tree edge
          parent[v] = u;
          disc[v] = low[v] = timer[0]++;
          edgeStack.push(new int[]{ u, v });
          callStack.push(new int[]{ v, 0 });
        } else if (v != parent[u] && disc[v] < disc[u]) {
          // Back edge (avoid going up to direct parent)
          low[u] = Math.min(low[u], disc[v]);
          edgeStack.push(new int[]{ u, v });
        }
      } else {
        // Finished processing u — backtrack
        callStack.pop();
        if (!callStack.isEmpty()) {
          final int p = parent[u];
          low[p] = Math.min(low[p], low[u]);

          // If p is an articulation point w.r.t. edge (p, u), pop a biconnected component
          if (low[u] >= disc[p]) {
            final int cid = compId[0]++;
            final java.util.Set<Integer> componentNodes = new java.util.HashSet<>();
            while (true) {
              final int[] edge = edgeStack.pop();
              componentNodes.add(edge[0]);
              componentNodes.add(edge[1]);
              if (edge[0] == p && edge[1] == u)
                break;
            }
            for (final int node : componentNodes)
              nodeComponents.get(node).add(cid);
          }
        } else {
          // Root — pop remaining edges as one component
          if (!edgeStack.isEmpty()) {
            final int cid = compId[0]++;
            final java.util.Set<Integer> componentNodes = new java.util.HashSet<>();
            while (!edgeStack.isEmpty()) {
              final int[] edge = edgeStack.pop();
              componentNodes.add(edge[0]);
              componentNodes.add(edge[1]);
            }
            for (final int node : componentNodes)
              nodeComponents.get(node).add(cid);
          }
        }
      }
    }
  }
}
