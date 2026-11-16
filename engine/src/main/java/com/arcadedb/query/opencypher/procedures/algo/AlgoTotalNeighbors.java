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

    // The rows of the two nodes and nothing else: from their records when no view covers the graph (issue #12176)
    final Neighborhood graph = openNeighborhood(db, relTypes, context, false);
    final int idx1 = graph.nodeOf(node1);
    final int idx2 = graph.nodeOf(node2);
    if (idx1 < 0 || idx2 < 0)
      return Stream.empty();

    // |N(u) ∪ N(v)| = |N(u)| + |N(v)| - |N(u) ∩ N(v)|
    final int[] row1 = graph.neighbors(idx1, dir);
    final int[] row2 = graph.neighbors(idx2, dir);
    final int[] n1Set = Neighborhood.distinctSorted(row1);

    int common = 0;
    for (final int j : row2)
      if (Neighborhood.contains(n1Set, j))
        common++;
    final long total = (long) row1.length + row2.length - common;
    graph.reserveVisited();

    final ResultInternal r = new ResultInternal();
    r.setProperty("node1", node1);
    r.setProperty("node2", node2);
    r.setProperty("coefficient", total);
    return Stream.of(r);
  }
}
