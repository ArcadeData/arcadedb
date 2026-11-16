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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.adamicAdar(node, relTypes?, direction?, cutoff?)
 * <p>
 * Computes the Adamic-Adar link prediction index between the given source node and every
 * other reachable vertex in the graph. The score between two vertices u and v is:
 * {@code sum over common neighbors w: 1 / log(degree(w))}.
 * Only pairs with score &gt; cutoff (default 0.0) are returned, sorted by score descending.
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (a:Person {name: 'Alice'})
 * CALL algo.adamicAdar(a, 'KNOWS', 'BOTH', 0.1)
 * YIELD node1, node2, score
 * RETURN node2.name, score ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoAdamicAdar extends AbstractAlgoProcedure {
  public static final String NAME = "algo.adamicAdar";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Compute Adamic-Adar link prediction score between a source node and all other nodes";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node1", "node2", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex sourceVertex  = extractVertex(args[0], "node");
    final String[] relTypes    = extractRelTypes(argOrDefault(args, 1, null), context);
    final Vertex.DIRECTION dir = parseDirection(extractString(argOrDefault(args, 2, "BOTH"), "direction"));
    final double cutoff        = optionalNumberArg(args, 3, "cutoff", 0.0).doubleValue();

    final Database db = context.getDatabase();
    // A score above a cutoff of zero or more comes from a common neighbour, so only the nodes two hops away are read, from the
    // records when no view covers the graph (issue #12176). A negative cutoff keeps every node
    final boolean wholeGraph = !(cutoff >= 0);
    final Neighborhood graph = openNeighborhood(db, relTypes, context, wholeGraph);
    final int srcIdx = graph.nodeOf(sourceVertex);
    if (srcIdx < 0)
      return Stream.empty();

    // Accumulated per node in the order the contributions arrive, so a score sums the same terms in the same order as before
    final Map<Integer, Double> aaScores = new HashMap<>();
    for (final int w : graph.neighbors(srcIdx, dir)) {
      final int[] row = graph.neighbors(w, dir);
      final int degree = row.length;
      if (degree <= 1)
        continue;  // log(1) = 0, skip to avoid division by zero / zero contribution
      final double contribution = 1.0 / Math.log(degree);
      for (final int v : row) {
        if (v != srcIdx)
          aaScores.merge(v, contribution, Double::sum);
      }
    }

    final int[] candidates;
    if (wholeGraph)
      candidates = graph.allNodes();
    else {
      candidates = aaScores.keySet().stream().mapToInt(Integer::intValue).toArray();
      graph.sortInGraphOrder(candidates);
    }

    // Collect results above cutoff, sorted by score descending
    final List<Result> results = new ArrayList<>();
    for (final int v : candidates) {
      final double score = aaScores.getOrDefault(v, 0.0);
      if (v == srcIdx || score <= cutoff)
        continue;
      final ResultInternal r = new ResultInternal();
      r.setProperty("node1", sourceVertex.getIdentity());
      r.setProperty("node2", graph.ridOf(v));
      r.setProperty("score", score);
      results.add(r);
    }
    graph.reserveVisited();
    results.sort(Comparator.comparingDouble((Result r) -> (double) r.getProperty("score")).reversed());
    return results.stream();
  }
}
