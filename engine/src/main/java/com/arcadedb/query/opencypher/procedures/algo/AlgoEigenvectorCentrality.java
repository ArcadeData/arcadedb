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
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.eigenvector(relTypes?, direction?, maxIterations?, tolerance?)
 * <p>
 * Computes eigenvector centrality for every vertex using the power iteration method.
 * A vertex's score is proportional to the sum of the scores of its neighbors.
 * Scores are normalized by the L∞ (max) norm keeping values in [0, 1].
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.eigenvector('KNOWS', 'BOTH', 50, 1e-8)
 * YIELD node, score
 * RETURN node.name, score ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoEigenvectorCentrality extends AbstractAlgoProcedure {
  public static final String NAME = "algo.eigenvector";

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
    return "Compute eigenvector centrality for all vertices using power iteration";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes    = args.length > 0 ? extractRelTypes(args[0]) : null;
    final Vertex.DIRECTION dir = args.length > 1 ? parseDirection(extractString(args[1], "direction")) : Vertex.DIRECTION.BOTH;
    final int maxIterations    = args.length > 2 ? ((Number) args[2]).intValue() : 20;
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
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, dir, relTypes);

    double[] scores    = new double[n];
    double[] newScores = new double[n];
    for (int i = 0; i < n; i++)
      scores[i] = 1.0;

    for (int iteration = 0; iteration < maxIterations; iteration++) {
      // newScores[v] = sum of scores[u] for all neighbors u of v
      for (int v = 0; v < n; v++) {
        double sum = 0.0;
        for (final int u : adj[v])
          sum += scores[u];
        newScores[v] = sum;
      }

      // Normalize by L-infinity (max) norm to keep values in [0, 1]
      double maxVal = 0.0;
      for (int i = 0; i < n; i++)
        if (newScores[i] > maxVal) maxVal = newScores[i];
      if (maxVal > 0)
        for (int i = 0; i < n; i++) newScores[i] /= maxVal;

      // Check convergence: max change across all vertices
      double maxChange = 0.0;
      for (int i = 0; i < n; i++) {
        final double d = Math.abs(newScores[i] - scores[i]);
        if (d > maxChange) maxChange = d;
      }

      // Swap arrays
      final double[] tmp = scores;
      scores = newScores;
      newScores = tmp;

      if (maxChange < tolerance)
        break;
    }

    final double[] finalScores = scores;
    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("score", finalScores[i]);
      return (Result) r;
    });
  }
}
