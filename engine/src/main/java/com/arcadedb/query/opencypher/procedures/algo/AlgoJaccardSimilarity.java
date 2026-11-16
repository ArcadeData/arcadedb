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
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.jaccard(node, relTypes?, direction?, cutoff?)
 * <p>
 * Computes the Jaccard similarity between the given source node and every other vertex in the
 * graph: {@code |N(src) ∩ N(v)| / |N(src) ∪ N(v)|}. Only pairs with similarity &gt; cutoff
 * (default 0.0) are returned. Only the nodes two hops away can share a neighbour with the source,
 * so only their rows are read (issue #12176). Results are listed in RID order.
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
    final WorkGuard guard = newWorkGuard(context);

    // Only a node two hops away shares a neighbour with the source, so only those are read: no view, no loading of the whole
    // graph (issue #12176)
    final Neighbourhood graph = openNeighbourhood(db, relTypes, context, sourceVertex.getIdentity());

    final int srcIdx = graph.nodeOf(sourceVertex.getIdentity());
    if (srcIdx < 0)
      return Stream.empty();
    final int srcDeg = graph.neighbors(srcIdx, dir).length;

    // count = |N(src) ∩ N(v)|: the entries of v's row that name a neighbour of the source
    final NodeScores shared = scoresThroughNeighbours(graph, srcIdx, dir, w -> 0.0, guard);

    // A node sharing no neighbour scores 0, and is listed only when the cutoff admits 0 (a negative one)
    final boolean everyNode = !(cutoff >= 0.0);
    final List<Result> results = new ArrayList<>();
    for (final int v : candidateNodes(db, graph, shared, srcIdx, everyNode)) {
      guard.checkPeriodically(results.size());
      final int slot = shared.slotOf(v);
      final int intersection = slot >= 0 ? shared.count(slot) : 0;
      final int union = srcDeg + graph.neighbors(v, dir).length - intersection;
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
    return results.stream();
  }
}
