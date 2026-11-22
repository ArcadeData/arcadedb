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

import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.totalNeighbors(node1, node2, relTypes?, direction?)
 * <p>
 * Link-prediction metric that returns the size of the union of the two nodes' neighbourhoods:
 * {@code |N(u) ∪ N(v)| = |N(u)| + |N(v)| − |N(u) ∩ N(v)|}. A higher value indicates that
 * both nodes have a large combined neighbourhood, which can serve as a normalisation factor
 * for other link-prediction scores.
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
 * CALL algo.totalNeighbors(a, b, 'KNOWS', 'BOTH')
 * YIELD node1, node2, coefficient
 * RETURN coefficient
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoTotalNeighbors extends AbstractAlgoProcedure {
  public static final String NAME = "algo.totalNeighbors";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Link prediction: size of the union of two nodes' neighbourhoods";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node1", "node2", "coefficient");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex node1         = extractVertex(args[0], "node1");
    final Vertex node2         = extractVertex(args[1], "node2");
    final String[] relTypes    = extractRelTypes(argOrDefault(args, 2, null), context);
    final Vertex.DIRECTION dir = parseDirection(extractString(argOrDefault(args, 3, "BOTH"), "direction"));

    final Database db = context.getDatabase();

    // Two rows are all the answer reads: no view, no loading of the whole graph (issue #12176)
    final Neighbourhood graph = openNeighbourhood(db, relTypes, context, node1.getIdentity(), node2.getIdentity());

    final int idx1 = graph.nodeOf(node1.getIdentity());
    final int idx2 = graph.nodeOf(node2.getIdentity());
    if (idx1 < 0 || idx2 < 0)
      return Stream.empty();

    // |N(u) ∪ N(v)| = |N(u)| + |N(v)| - |N(u) ∩ N(v)|, over the distinct neighbours: a parallel or a reciprocal relationship is one
    // neighbour (issue #12934)
    final int[] adj1 = graph.distinctNeighbors(idx1, dir);
    final int[] adj2 = graph.distinctNeighbors(idx2, dir);

    // Both sorted: one merge pass counts the intersection
    int common = 0;
    for (int i = 0, j = 0; i < adj1.length && j < adj2.length; )
      if (adj1[i] < adj2[j])
        i++;
      else if (adj1[i] > adj2[j])
        j++;
      else {
        common++;
        i++;
        j++;
      }
    final long total = (long) adj1.length + adj2.length - common;

    final ResultInternal r = new ResultInternal();
    r.setProperty("node1", node1);
    r.setProperty("node2", node2);
    r.setProperty("coefficient", total);
    return Stream.of(r);
  }
}
