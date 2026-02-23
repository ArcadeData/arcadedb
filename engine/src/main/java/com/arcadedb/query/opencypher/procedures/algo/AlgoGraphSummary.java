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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.graphSummary(relTypes?, nodeLabels?)
 * <p>
 * Computes a summary of the graph structure in a single pass over the adjacency list.
 * Returns basic graph metrics: node count, edge count, average/max/min degree, density,
 * isolated nodes, and self-loop count.
 * </p>
 * <p>
 * Density is computed as: 2E / (V * (V-1)) for undirected graphs.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.graphSummary()
 * YIELD nodeCount, edgeCount, avgDegree, maxDegree, minDegree, density, isolatedNodes, selfLoops
 * RETURN nodeCount, edgeCount, avgDegree
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoGraphSummary extends AbstractAlgoProcedure {
  public static final String NAME = "algo.graphSummary";

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
    return "Computes a structural summary of the graph including node/edge counts, degree statistics, and density";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodeCount", "edgeCount", "avgDegree", "maxDegree", "minDegree", "density", "isolatedNodes", "selfLoops");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes   = args.length > 0 ? extractRelTypes(args[0]) : null;
    final String[] nodeLabels = args.length > 1 ? extractRelTypes(args[1]) : null;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, nodeLabels);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeCount", 0L);
      r.setProperty("edgeCount", 0L);
      r.setProperty("avgDegree", 0.0);
      r.setProperty("maxDegree", 0L);
      r.setProperty("minDegree", 0L);
      r.setProperty("density", 0.0);
      r.setProperty("isolatedNodes", 0L);
      r.setProperty("selfLoops", 0L);
      return Stream.of(r);
    }

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adjOut = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.OUT, relTypes);

    long edgeCount    = 0L;
    long selfLoops    = 0L;
    long isolatedNodes = 0L;
    long maxDegree    = 0L;
    long minDegree    = Long.MAX_VALUE;

    for (int i = 0; i < n; i++) {
      final int deg = adjOut[i].length;
      long selfLoopCount = 0L;
      for (final int j : adjOut[i]) {
        if (j == i)
          selfLoopCount++;
      }
      selfLoops += selfLoopCount;
      edgeCount += deg;
      if (deg > maxDegree)
        maxDegree = deg;
      if (deg < minDegree)
        minDegree = deg;
      if (deg == 0)
        isolatedNodes++;
    }

    if (minDegree == Long.MAX_VALUE)
      minDegree = 0L;

    final double avgDegree = n > 0 ? (double) edgeCount / n : 0.0;
    final double density   = n > 1 ? (2.0 * edgeCount) / ((long) n * (n - 1)) : 0.0;

    final ResultInternal result = new ResultInternal();
    result.setProperty("nodeCount", (long) n);
    result.setProperty("edgeCount", edgeCount);
    result.setProperty("avgDegree", avgDegree);
    result.setProperty("maxDegree", maxDegree);
    result.setProperty("minDegree", minDegree);
    result.setProperty("density", density);
    result.setProperty("isolatedNodes", isolatedNodes);
    result.setProperty("selfLoops", selfLoops);
    return Stream.of(result);
  }
}
