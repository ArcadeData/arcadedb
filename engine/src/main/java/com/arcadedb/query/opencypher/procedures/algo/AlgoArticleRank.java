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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.articlerank([config])
 * <p>
 * Computes ArticleRank scores for all nodes in the graph. ArticleRank is a variant of PageRank
 * (Yan &amp; Ding, 2007) that down-weights contributions from high-degree nodes by normalising
 * each contribution by {@code outDeg(v) + avgOutDeg}, where {@code avgOutDeg} is the mean
 * out-degree of all nodes. This makes the measure more robust to highly-connected hub nodes.
 * </p>
 * <p>
 * Formula: {@code AR(u) = (1-d)/N + d * Σ_{v→u} AR(v) / (outDeg(v) + avgOutDeg)}
 * </p>
 * <p>
 * Config map parameters (all optional):
 * <ul>
 *   <li>dampingFactor (double, default 0.85)</li>
 *   <li>maxIterations (int, default 20)</li>
 *   <li>tolerance (double, default 0.0001)</li>
 * </ul>
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.articlerank({dampingFactor: 0.85, maxIterations: 20})
 * YIELD node, score
 * RETURN node.name, score ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoArticleRank extends AbstractAlgoProcedure {
  public static final String NAME = "algo.articlerank";

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
    return "Computes ArticleRank scores (PageRank variant down-weighting high-degree node contributions)";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<String, Object> config = args.length > 0 ? extractMap(args[0], "config") : null;

    final double dampingFactor = config != null && config.get("dampingFactor") instanceof Number num ?
        num.doubleValue() : 0.85;
    final int maxIterations = config != null && config.get("maxIterations") instanceof Number num ?
        num.intValue() : 20;
    final double tolerance = config != null && config.get("tolerance") instanceof Number num ?
        num.doubleValue() : 0.0001;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());
    if (vertices.isEmpty())
      return Stream.empty();

    final int n = vertices.size();
    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);

    // Compute out-degrees
    final long[] outDegrees = new long[n];
    long totalOutDeg = 0;
    for (int i = 0; i < n; i++) {
      outDegrees[i] = vertices.get(i).countEdges(Vertex.DIRECTION.OUT);
      totalOutDeg += outDegrees[i];
    }
    final double avgOutDeg = n > 0 ? (double) totalOutDeg / n : 1.0;

    // Initialize scores
    final double[] scores    = new double[n];
    final double[] newScores = new double[n];
    final double initialScore = 1.0 / n;
    for (int i = 0; i < n; i++)
      scores[i] = initialScore;

    // Iterative ArticleRank computation
    for (int iter2 = 0; iter2 < maxIterations; iter2++) {
      double dangling = 0.0;
      for (int i = 0; i < n; i++) {
        if (outDegrees[i] == 0)
          dangling += scores[i];
        newScores[i] = 0.0;
      }

      for (int i = 0; i < n; i++) {
        final Vertex v = vertices.get(i);
        // ArticleRank: divide by (outDeg + avgOutDeg) instead of outDeg
        final double denom = outDegrees[i] + avgOutDeg;
        for (final Edge edge : v.getEdges(Vertex.DIRECTION.OUT)) {
          final Integer neighborIdx = ridToIdx.get(edge.getIn());
          if (neighborIdx != null)
            newScores[neighborIdx] += scores[i] / denom;
        }
      }

      final double danglingContribution = dampingFactor * dangling / n;
      double maxChange = 0.0;
      for (int i = 0; i < n; i++) {
        newScores[i] = (1.0 - dampingFactor) / n + dampingFactor * newScores[i] + danglingContribution;
        maxChange = Math.max(maxChange, Math.abs(newScores[i] - scores[i]));
        scores[i] = newScores[i];
      }

      if (maxChange < tolerance)
        break;
    }

    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("score", scores[i]);
      results.add(r);
    }
    return results.stream();
  }
}
