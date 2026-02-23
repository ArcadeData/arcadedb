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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.pagerank([config])
 * <p>
 * Computes the PageRank score for all nodes in the graph. PageRank measures the importance of
 * nodes based on the link structure: a node is more important if it receives links from other
 * important nodes.
 * </p>
 * <p>
 * Config map parameters (all optional):
 * <ul>
 *   <li>dampingFactor (double, default 0.85): probability of following a link</li>
 *   <li>maxIterations (int, default 20): maximum number of iterations</li>
 *   <li>tolerance (double, default 0.0001): convergence threshold</li>
 *   <li>weightProperty (string, default null): edge property to use as weight</li>
 * </ul>
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 20})
 * YIELD node, score
 * RETURN node.name, score ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoPageRank extends AbstractAlgoProcedure {
  public static final String NAME = "algo.pagerank";

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
    return "Computes PageRank scores for all nodes in the graph";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<String, Object> config = args.length > 0 ? extractMap(args[0], "config") : null;

    final double dampingFactor = config != null && config.get("dampingFactor") instanceof Number n ?
        n.doubleValue() : 0.85;
    final int maxIterations = config != null && config.get("maxIterations") instanceof Number n ?
        n.intValue() : 20;
    final double tolerance = config != null && config.get("tolerance") instanceof Number n ?
        n.doubleValue() : 0.0001;
    final String weightProperty = config != null ? (String) config.get("weightProperty") : null;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> vertIter = getAllVertices(db, null);
    while (vertIter.hasNext())
      vertices.add(vertIter.next());
    if (vertices.isEmpty())
      return Stream.empty();

    final int n = vertices.size();
    final Map<Vertex, Integer> vertexIndex = new HashMap<>(n);
    for (int i = 0; i < n; i++)
      vertexIndex.put(vertices.get(i), i);

    // Initialize scores
    final double[] scores = new double[n];
    final double[] newScores = new double[n];
    final double initialScore = 1.0 / n;
    for (int i = 0; i < n; i++)
      scores[i] = initialScore;

    // Iterative PageRank computation
    for (int iter = 0; iter < maxIterations; iter++) {
      double dangling = 0.0;

      for (int i = 0; i < n; i++) {
        final Vertex v = vertices.get(i);
        final long outDegree = v.countEdges(Vertex.DIRECTION.OUT);
        if (outDegree == 0)
          dangling += scores[i];
        newScores[i] = 0.0;
      }

      for (int i = 0; i < n; i++) {
        final Vertex v = vertices.get(i);
        for (final Edge edge : v.getEdges(Vertex.DIRECTION.OUT)) {
          final Vertex neighbor = edge.getInVertex();
          final Integer neighborIdx = vertexIndex.get(neighbor);
          if (neighborIdx == null)
            continue;

          double weight = 1.0;
          if (weightProperty != null) {
            final Object w = edge.get(weightProperty);
            if (w instanceof Number num)
              weight = num.doubleValue();
          }

          final long outDegree = v.countEdges(Vertex.DIRECTION.OUT);
          if (outDegree > 0)
            newScores[neighborIdx] += scores[i] * weight / outDegree;
        }
      }

      // Apply damping factor and dangling node contribution
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

    // Build result stream
    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", vertices.get(i));
      result.setProperty("score", scores[i]);
      results.add(result);
    }
    return results.stream();
  }
}
