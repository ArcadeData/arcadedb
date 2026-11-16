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
import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.jaccard(node, relTypes?, direction?, cutoff?)
 * <p>
 * Computes the Jaccard similarity between the given source node and every other vertex in the
 * graph: {@code |N(src) ∩ N(v)| / |N(src) ∪ N(v)|}. Only pairs with similarity &gt; cutoff
 * (default 0.0) are returned. With a cutoff of zero or more only the vertices two hops from the source can qualify, and
 * only those are read (issue #12176).
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (a:Person {name: 'Alice'})
 * CALL algo.jaccard(a, 'KNOWS', 'BOTH', 0.1)
 * YIELD node1, node2, similarity
 * RETURN node2.name, similarity ORDER BY similarity DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoJaccardSimilarity extends AbstractAlgoProcedure {
  public static final String NAME = "algo.jaccard";

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
    return "Compute Jaccard similarity between a source node and all other nodes";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node1", "node2", "similarity");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex sourceVertex = extractVertex(args[0], "node");
    final String[] relTypes   = extractRelTypes(argOrDefault(args, 1, null), context);
    final Vertex.DIRECTION dir = parseDirection(extractString(argOrDefault(args, 2, "BOTH"), "direction"));
    final double cutoff       = optionalNumberArg(args, 3, "cutoff", 0.0).doubleValue();

    final Database db = context.getDatabase();

    // A similarity above a cutoff of zero or more needs a neighbour in common with the source, so only the nodes two hops away
    // are read, from the records when no view covers the graph (issue #12176). A negative cutoff keeps every node
    final boolean wholeGraph = !(cutoff >= 0);
    final Neighborhood graph = openNeighborhood(db, relTypes, context, wholeGraph);
    final int srcIdx = graph.nodeOf(sourceVertex);
    if (srcIdx < 0)
      return Stream.empty();

    final int[] srcRow = graph.neighbors(srcIdx, dir);
    final int[] srcNeighbors = Neighborhood.distinctSorted(srcRow);
    final int srcDeg = srcRow.length;

    final List<Result> results = new ArrayList<>();
    for (final int v : wholeGraph ? graph.allNodes() : graph.nodesReaching(srcNeighbors, dir)) {
      if (v == srcIdx)
        continue;
      final int[] row = graph.neighbors(v, dir);
      int intersection = 0;
      for (final int w : row)
        if (Neighborhood.contains(srcNeighbors, w))
          intersection++;
      final int union = srcDeg + row.length - intersection;
      if (union == 0)
        continue;
      final double similarity = (double) intersection / union;
      if (similarity <= cutoff)
        continue;
      final ResultInternal r = new ResultInternal();
      r.setProperty("node1", sourceVertex.getIdentity());
      r.setProperty("node2", graph.ridOf(v));
      r.setProperty("similarity", similarity);
      results.add(r);
    }
    graph.reserveVisited();
    return results.stream();
  }
}
