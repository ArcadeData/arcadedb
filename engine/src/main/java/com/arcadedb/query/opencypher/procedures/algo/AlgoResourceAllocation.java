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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.resourceAllocation(node, relTypes?, direction?, cutoff?)
 * <p>
 * Computes the Resource Allocation link prediction index between the given source node and
 * every other vertex. The score between two vertices u and v is:
 * {@code sum over common neighbors w: 1 / degree(w)}.
 * Only pairs with score &gt; cutoff (default 0.0) are returned, sorted by score descending.
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (a:Person {name: 'Alice'})
 * CALL algo.resourceAllocation(a, 'KNOWS', 'BOTH', 0.1)
 * YIELD node1, node2, score
 * RETURN node2.name, score ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoResourceAllocation extends AbstractAlgoProcedure {
  public static final String NAME = "algo.resourceAllocation";

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
    return "Compute Resource Allocation link prediction score between a source node and all other nodes";
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

    final int[] srcNeighbors = Neighborhood.distinctSorted(graph.neighbors(srcIdx, dir));
    // The degree of each neighbour of the source, read once: every candidate shares some of them
    final int[] srcNeighborDegrees = new int[srcNeighbors.length];
    for (int i = 0; i < srcNeighbors.length; i++)
      srcNeighborDegrees[i] = graph.neighbors(srcNeighbors[i], dir).length;

    final List<Result> results = new ArrayList<>();
    for (final int v : wholeGraph ? graph.allNodes() : graph.nodesReaching(srcNeighbors, dir)) {
      if (v == srcIdx)
        continue;
      double score = 0.0;
      for (final int w : graph.neighbors(v, dir)) {
        final int i = Arrays.binarySearch(srcNeighbors, w);
        if (i >= 0 && srcNeighborDegrees[i] > 0)
          score += 1.0 / srcNeighborDegrees[i];
      }
      if (score <= cutoff)
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
