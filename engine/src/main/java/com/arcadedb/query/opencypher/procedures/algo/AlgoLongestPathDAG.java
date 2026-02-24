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
 * Procedure: algo.longestPath(relTypes?, weightProperty?)
 * <p>
 * Finds the longest path in a Directed Acyclic Graph (DAG) using dynamic programming on the
 * topological order. If the graph contains a cycle the procedure returns an empty result.
 * </p>
 * <p>
 * Returns one row per node with the maximum distance from the furthest source and the node that
 * achieves it. Optionally uses an edge weight property; defaults to unit weight (hop count).
 * </p>
 * <p>
 * Parameters:
 * <ul>
 *   <li>relTypes (optional): relationship types to follow</li>
 *   <li>weightProperty (optional): edge property to use as weight (default 1.0 per hop)</li>
 * </ul>
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.longestPath('DEPENDS_ON')
 * YIELD node, distance, source
 * RETURN node.name, distance, source.name ORDER BY distance DESC LIMIT 1
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoLongestPathDAG extends AbstractAlgoProcedure {
  public static final String NAME = "algo.longestPath";

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
    return 2;
  }

  @Override
  public String getDescription() {
    return "Longest path in a Directed Acyclic Graph (DAG) via topological-order DP";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "distance", "source");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes     = args.length > 0 ? extractRelTypes(args[0]) : null;
    final String weightProperty = args.length > 1 ? extractString(args[1], "weightProperty") : null;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] outAdj = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.OUT, relTypes);

    // Compute in-degrees for topological sort (Kahn's algorithm)
    final int[] inDegree = new int[n];
    for (int u = 0; u < n; u++)
      for (final int v : outAdj[u])
        inDegree[v]++;

    // Topological sort
    final int[] topoOrder = new int[n];
    int topoLen = 0;
    final int[] queue = new int[n];
    int head = 0, tail = 0;
    for (int i = 0; i < n; i++)
      if (inDegree[i] == 0)
        queue[tail++] = i;

    while (head < tail) {
      final int u = queue[head++];
      topoOrder[topoLen++] = u;
      for (final int v : outAdj[u]) {
        if (--inDegree[v] == 0)
          queue[tail++] = v;
      }
    }

    // Cycle detected
    if (topoLen < n)
      return Stream.empty();

    // DP: dp[v] = longest distance to reach v; source[v] = source of that path
    final double[] dp     = new double[n];
    final int[]    source = new int[n];
    Arrays.fill(dp, 0.0);
    for (int i = 0; i < n; i++)
      source[i] = i; // each node is its own source initially

    for (int ti = 0; ti < n; ti++) {
      final int u = topoOrder[ti];
      final Vertex vu = vertices.get(u);
      for (final Edge edge : vu.getEdges(Vertex.DIRECTION.OUT)) {
        if (relTypes != null) {
          final String type = edge.getTypeName();
          boolean found = false;
          for (final String rt : relTypes)
            if (rt.equals(type)) { found = true; break; }
          if (!found) continue;
        }
        final Integer vObj = ridToIdx.get(edge.getIn());
        if (vObj == null) continue;
        final int v = vObj;
        double weight = 1.0;
        if (weightProperty != null) {
          final Object w = edge.get(weightProperty);
          if (w instanceof Number num) weight = num.doubleValue();
        }
        final double newDist = dp[u] + weight;
        if (newDist > dp[v]) {
          dp[v] = newDist;
          source[v] = source[u];
        }
      }
    }

    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("distance", dp[i]);
      r.setProperty("source", vertices.get(source[i]));
      results.add(r);
    }
    return results.stream();
  }
}
