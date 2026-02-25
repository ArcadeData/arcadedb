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
 * Procedure: algo.katz(relTypes?, alpha?, maxIterations?, tolerance?)
 * <p>
 * Computes Katz centrality for all nodes in the graph. Katz centrality accounts for all
 * paths between nodes with a penalty factor (alpha) applied per hop, so longer paths
 * contribute less to the centrality score.
 * </p>
 * <p>
 * The formula is: k[i] = alpha * sum_j(A[j][i] * k[j]) + 1, using power iteration with
 * primitive arrays (no boxing) for efficiency.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.katz('KNOWS', 0.005, 100, 1e-6)
 * YIELD nodeId, score
 * RETURN nodeId, score ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoKatz extends AbstractAlgoProcedure {
  public static final String NAME = "algo.katz";

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
    return 4;
  }

  @Override
  public String getDescription() {
    return "Computes Katz centrality scores for all nodes in the graph using power iteration";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodeId", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes    = args.length > 0 ? extractRelTypes(args[0]) : null;
    final double alpha         = args.length > 1 ? ((Number) args[1]).doubleValue() : 0.005;
    final int maxIterations    = args.length > 2 ? ((Number) args[2]).intValue() : 100;
    final double tolerance     = args.length > 3 ? ((Number) args[3]).doubleValue() : 1e-6;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    // Use IN adjacency to compute: k[i] += alpha * k[j] for each j that points to i
    final int[][] adjIn = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.IN, relTypes);

    double[] scores    = new double[n];
    double[] newScores = new double[n];
    Arrays.fill(scores, 1.0);

    for (int iter2 = 0; iter2 < maxIterations; iter2++) {
      // newScores[i] = alpha * sum_{j in inNeighbors(i)} scores[j] + 1
      for (int i = 0; i < n; i++) {
        double sum = 0.0;
        for (final int j : adjIn[i])
          sum += scores[j];
        newScores[i] = alpha * sum + 1.0;
      }

      double maxChange = 0.0;
      for (int i = 0; i < n; i++) {
        final double d = Math.abs(newScores[i] - scores[i]);
        if (d > maxChange)
          maxChange = d;
      }

      // Swap arrays
      final double[] tmp = scores;
      scores = newScores;
      newScores = tmp;

      if (maxChange < tolerance)
        break;
    }

    // Normalize by max score
    double maxScore = 0.0;
    for (int i = 0; i < n; i++)
      if (scores[i] > maxScore)
        maxScore = scores[i];

    final double[] finalScores = scores;
    final double finalMax = maxScore > 0 ? maxScore : 1.0;

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", vertices.get(i).getIdentity());
      r.setProperty("score", finalScores[i] / finalMax);
      return (Result) r;
    });
  }
}
