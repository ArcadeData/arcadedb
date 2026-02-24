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
 * Procedure: algo.cycleDetection(relTypes?)
 * <p>
 * Detects cycles in the graph using Kosaraju's SCC algorithm on the OUT-direction adjacency.
 * A node is considered to be in a cycle if it belongs to a non-trivial SCC (size &gt; 1)
 * or has a self-loop. The global {@code hasCycle} flag is true if any node is in a cycle.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.cycleDetection()
 * YIELD node, inCycle, hasCycle
 * RETURN node.name, inCycle ORDER BY inCycle DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoCycleDetection extends AbstractAlgoProcedure {
  public static final String NAME = "algo.cycleDetection";

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
    return "Detect cycles in the graph and identify nodes that participate in cycles";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "inCycle", "hasCycle");
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
    final int[][] adj  = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.OUT, relTypes);
    final int[][] radj = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.IN,  relTypes);

    // Kosaraju Pass 1: iterative DFS on original graph — record post-order finish
    final int[] order   = new int[n];
    int orderSize = 0;
    final boolean[] visited = new boolean[n];
    final int[] stack    = new int[n];
    final int[] edgeIdx  = new int[n];
    int stackTop;

    for (int s = 0; s < n; s++) {
      if (visited[s])
        continue;
      stackTop = 0;
      stack[0]   = s;
      edgeIdx[0] = 0;
      visited[s] = true;

      while (stackTop >= 0) {
        final int v = stack[stackTop];
        final int[] neighbors = adj[v];
        boolean pushed = false;
        while (edgeIdx[stackTop] < neighbors.length) {
          final int u = neighbors[edgeIdx[stackTop]++];
          if (!visited[u]) {
            visited[u] = true;
            stackTop++;
            stack[stackTop]   = u;
            edgeIdx[stackTop] = 0;
            pushed = true;
            break;
          }
        }
        if (!pushed) {
          order[orderSize++] = v;
          stackTop--;
        }
      }
    }

    // Kosaraju Pass 2: BFS on reversed graph in reverse finish order
    final int[] comp = new int[n];
    Arrays.fill(comp, -1);
    final int[] queue = new int[n];
    int numComponents = 0;

    for (int i = orderSize - 1; i >= 0; i--) {
      final int s = order[i];
      if (comp[s] != -1)
        continue;
      int head = 0, tail = 0;
      queue[tail++] = s;
      comp[s] = numComponents;
      while (head < tail) {
        final int v = queue[head++];
        for (final int u : radj[v]) {
          if (comp[u] == -1) {
            comp[u] = numComponents;
            queue[tail++] = u;
          }
        }
      }
      numComponents++;
    }

    // Count component sizes
    final int[] compSize = new int[numComponents];
    for (int i = 0; i < n; i++)
      compSize[comp[i]]++;

    // Determine inCycle: SCC size > 1 OR self-loop
    final boolean[] inCycle = new boolean[n];
    boolean hasCycle = false;
    for (int v = 0; v < n; v++) {
      boolean selfLoop = false;
      for (final int u : adj[v])
        if (u == v) {
          selfLoop = true;
          break;
        }
      inCycle[v] = compSize[comp[v]] > 1 || selfLoop;
      if (inCycle[v])
        hasCycle = true;
    }

    final boolean finalHasCycle = hasCycle;

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("inCycle", inCycle[i]);
      r.setProperty("hasCycle", finalHasCycle);
      return (Result) r;
    });
  }
}
