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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.resourceAllocation(node, relTypes?, direction?, cutoff?)
 * <p>
 * Computes the Resource Allocation link prediction index between the given source node and
 * every other vertex. The score between two vertices u and v is:
 * {@code sum over common neighbors w: 1 / degree(w)}.
 * Only pairs with score &gt; cutoff (default 0.0) are returned, sorted by score descending, equal
 * scores in RID order. Only the nodes two hops away can share a neighbour with the source, so only
 * their rows are read (issue #12176).
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
    final WorkGuard guard = newWorkGuard(context);

    // Only a node two hops away shares a neighbour with the source, so only those are read: no view, no loading of the whole
    // graph (issue #12176)
    final Neighbourhood graph = openNeighbourhood(db, relTypes, context, sourceVertex.getIdentity());

    final int srcIdx = graph.nodeOf(sourceVertex.getIdentity());
    if (srcIdx < 0)
      return Stream.empty();

    // score = the sum of 1 / degree(w) over the common neighbours w of the source and v, in the direction asked
    final NodeScores shared = scoresThroughNeighbours(graph, srcIdx, dir, w -> {
      final int degree = graph.distinctDegree(w, dir);
      return degree > 0 ? 1.0 / degree : 0.0;
    }, guard);

    // A node sharing no neighbour scores 0, and is listed only when the cutoff admits 0 (a negative one)
    final List<Result> results = new ArrayList<>();
    int candidate = 0;
    for (final int v : candidateNodes(db, graph, shared, srcIdx, cutoff < 0.0 || Double.isNaN(cutoff))) {
      // Every candidate advances the checkpoint, not only the accepted ones: a cutoff that rejects most of them must not skip it
      guard.checkPeriodically(candidate++);
      final int slot = shared.slotOf(v);
      final double score = slot >= 0 ? shared.score(slot) : 0.0;
      if (score <= cutoff)
        continue;
      final ResultInternal r = new ResultInternal();
      r.setProperty("node1", sourceVertex.getIdentity());
      r.setProperty("node2", graph.ridOf(v));
      r.setProperty("score", score);
      results.add(r);
    }
    results.sort(Comparator.comparingDouble((Result r) -> (double) r.getProperty("score")).reversed());
    return results.stream();
  }
}
