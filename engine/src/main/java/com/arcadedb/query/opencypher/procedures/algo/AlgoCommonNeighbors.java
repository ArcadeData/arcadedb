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
import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.commonNeighbors(node, relTypes?, direction?, cutoff?)
 * <p>
 * Counts the number of common neighbors between the given source node and every other
 * vertex in the graph. Only pairs with count &gt;= cutoff (default 1) are returned,
 * sorted by common neighbor count descending.
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (a:Person {name: 'Alice'})
 * CALL algo.commonNeighbors(a, 'KNOWS', 'BOTH', 2)
 * YIELD node1, node2, commonNeighbors
 * RETURN node2.name, commonNeighbors ORDER BY commonNeighbors DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoCommonNeighbors extends AbstractAlgoProcedure {
  public static final String NAME = "algo.commonNeighbors";

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
    return "Count common neighbors between a source node and all other nodes";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node1", "node2", "commonNeighbors");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex sourceVertex  = extractVertex(args[0], "node");
    final String[] relTypes    = extractRelTypes(argOrDefault(args, 1, null), context);
    final Vertex.DIRECTION dir = parseDirection(extractString(argOrDefault(args, 2, "BOTH"), "direction"));
    final int cutoff           = extractInt(optionalNumberArg(args, 3, "cutoff", 1), "cutoff");

    final Database db = context.getDatabase();

    // A cutoff of one or more keeps only the nodes with a neighbour in common with the source, two hops away: only those are
    // read, from the records when no view covers the graph (issue #12176). A cutoff of zero or less keeps every node
    final boolean wholeGraph = cutoff <= 0;
    final Neighborhood graph = openNeighborhood(db, relTypes, context, wholeGraph);
    final int srcIdx = graph.nodeOf(sourceVertex);
    if (srcIdx < 0)
      return Stream.empty();

    final int[] srcNeighbors = Neighborhood.distinctSorted(graph.neighbors(srcIdx, dir));

    final List<Result> results = new ArrayList<>();
    for (final int v : wholeGraph ? graph.allNodes() : graph.nodesReaching(srcNeighbors, dir)) {
      if (v == srcIdx)
        continue;
      int count = 0;
      for (final int w : graph.neighbors(v, dir))
        if (Neighborhood.contains(srcNeighbors, w))
          count++;
      if (count >= cutoff) {
        final ResultInternal r = new ResultInternal();
        r.setProperty("node1", sourceVertex.getIdentity());
        r.setProperty("node2", graph.ridOf(v));
        r.setProperty("commonNeighbors", count);
        results.add(r);
      }
    }
    graph.reserveVisited();
    results.sort(Comparator.comparingInt((Result r) -> (int) r.getProperty("commonNeighbors")).reversed());
    return results.stream();
  }
}
