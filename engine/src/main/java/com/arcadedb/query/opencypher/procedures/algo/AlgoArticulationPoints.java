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
import java.util.stream.Stream;

/**
 * Procedure: algo.articulationPoints(relTypes?)
 * <p>
 * Finds all articulation points (cut vertices) in the graph using Tarjan's iterative DFS
 * algorithm. An articulation point is a vertex whose removal increases the number of connected
 * components. Only the articulation point vertices are returned.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.articulationPoints()
 * YIELD node
 * RETURN node.name
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoArticulationPoints extends AbstractAlgoProcedure {
  public static final String NAME = "algo.articulationPoints";

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
    return "Find all articulation points (cut vertices) in the graph using Tarjan's iterative DFS";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node");
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

    final int[] disc       = new int[n];
    final int[] low        = new int[n];
    final int[] parent     = new int[n];
    final int[] childCount = new int[n];
    final boolean[] isAP   = new boolean[n];

    Arrays.fill(disc, -1);
    Arrays.fill(parent, -1);

    // Iterative DFS stack
    final int[] stack   = new int[n];
    final int[] edgeIdx = new int[n];
    int timer = 0;

    for (int root = 0; root < n; root++) {
      if (disc[root] != -1)
        continue;

      int top = 0;
      stack[top]   = root;
      edgeIdx[top] = 0;
      disc[root]   = low[root] = timer++;

      while (top >= 0) {
        final int v = stack[top];
        if (edgeIdx[top] < adj[v].length) {
          final int u = adj[v][edgeIdx[top]++];
          if (disc[u] == -1) {
            // Tree edge: visit u
            parent[u] = v;
            childCount[v]++;
            disc[u] = low[u] = timer++;
            top++;
            stack[top]   = u;
            edgeIdx[top] = 0;
          } else if (u != parent[v])
            // Back edge: update low
            low[v] = Math.min(low[v], disc[u]);
        } else {
          // Done with v, backtrack
          top--;
          if (top >= 0) {
            final int p = stack[top];
            low[p] = Math.min(low[p], low[v]);
            // Check AP condition for p
            if (parent[p] == -1 && childCount[p] >= 2)
              isAP[p] = true;
            if (parent[p] != -1 && low[v] >= disc[p])
              isAP[p] = true;
          }
        }
      }
    }

    final List<Result> results = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      if (!isAP[i])
        continue;
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      results.add(r);
    }
    return results.stream();
  }
}
