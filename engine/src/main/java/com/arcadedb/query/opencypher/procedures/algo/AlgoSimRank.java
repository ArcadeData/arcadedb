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
 * Procedure: algo.simRank(nodeA, nodeB, relTypes?, decayFactor?, maxIterations?)
 * <p>
 * Computes the SimRank similarity between two nodes. SimRank is based on the idea
 * that two nodes are similar if they are pointed to by similar nodes.
 * </p>
 * <p>
 * Formula: sim[u][v] = C / (|N(u)| * |N(v)|) * sum_{i,j} sim[N_i(u)][N_j(v)]
 * where C is the decay factor and N(u) are the in-neighbors of u.
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (a:Item {id:1}), (b:Item {id:2})
 * CALL algo.simRank(a, b, 'SIMILAR', 0.8, 5)
 * YIELD similarity, nodeAId, nodeBId
 * RETURN similarity
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoSimRank extends AbstractAlgoProcedure {
  public static final String NAME = "algo.simRank";

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
    return 5;
  }

  @Override
  public String getDescription() {
    return "Computes SimRank similarity between two nodes based on their structural graph similarity";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("similarity", "nodeAId", "nodeBId");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex nodeA       = extractVertex(args[0], "nodeA");
    final Vertex nodeB       = extractVertex(args[1], "nodeB");
    final String[] relTypes  = args.length > 2 ? extractRelTypes(args[2]) : null;
    final double decayFactor = args.length > 3 ? ((Number) args[3]).doubleValue() : 0.8;
    final int maxIterations  = args.length > 4 ? ((Number) args[4]).intValue() : 5;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final Integer idxA = ridToIdx.get(nodeA.getIdentity());
    final Integer idxB = ridToIdx.get(nodeB.getIdentity());

    if (idxA == null || idxB == null)
      return Stream.empty();

    if (idxA.equals(idxB)) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("similarity", 1.0);
      r.setProperty("nodeAId", nodeA.getIdentity());
      r.setProperty("nodeBId", nodeB.getIdentity());
      return Stream.of(r);
    }

    // Build IN adjacency list (who points to whom)
    final int[][] adjIn = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.IN, relTypes);

    // Initialize sim matrix: sim[i][i] = 1.0, all others = 0.0
    double[][] sim    = new double[n][n];
    double[][] newSim = new double[n][n];
    for (int i = 0; i < n; i++)
      sim[i][i] = 1.0;

    for (int iter2 = 0; iter2 < maxIterations; iter2++) {
      for (int i = 0; i < n; i++)
        for (int j = 0; j < n; j++)
          newSim[i][j] = i == j ? 1.0 : 0.0;

      for (int u = 0; u < n; u++) {
        for (int v = u + 1; v < n; v++) {
          final int[] inU = adjIn[u];
          final int[] inV = adjIn[v];
          if (inU.length == 0 || inV.length == 0) {
            newSim[u][v] = 0.0;
            newSim[v][u] = 0.0;
            continue;
          }
          double sumSim = 0.0;
          for (final int a : inU)
            for (final int b : inV)
              sumSim += sim[a][b];
          final double val = (decayFactor / (inU.length * inV.length)) * sumSim;
          newSim[u][v] = val;
          newSim[v][u] = val;
        }
      }

      // Swap
      final double[][] tmp = sim;
      sim = newSim;
      newSim = tmp;
    }

    final double similarity = sim[idxA][idxB];

    final ResultInternal result = new ResultInternal();
    result.setProperty("similarity", similarity);
    result.setProperty("nodeAId", nodeA.getIdentity());
    result.setProperty("nodeBId", nodeB.getIdentity());
    return Stream.of(result);
  }
}
