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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    final String[] relTypes    = args.length > 2 ? extractRelTypes(args[2]) : null;
    final Vertex.DIRECTION dir = args.length > 3 ? parseDirection(extractString(args[3], "direction")) : Vertex.DIRECTION.BOTH;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, dir, relTypes);

    final Integer idx1obj = ridToIdx.get(node1.getIdentity());
    final Integer idx2obj = ridToIdx.get(node2.getIdentity());
    if (idx1obj == null || idx2obj == null)
      return Stream.empty();

    final int idx1 = idx1obj;
    final int idx2 = idx2obj;

    // |N(u) ∪ N(v)| = |N(u)| + |N(v)| - |N(u) ∩ N(v)|
    final BitSet n1Set = new BitSet(n);
    for (final int j : adj[idx1])
      n1Set.set(j);

    int common = 0;
    for (final int j : adj[idx2]) {
      if (n1Set.get(j))
        common++;
    }
    final long total = (long) adj[idx1].length + adj[idx2].length - common;

    final ResultInternal r = new ResultInternal();
    r.setProperty("node1", node1);
    r.setProperty("node2", node2);
    r.setProperty("coefficient", total);
    return Stream.of(r);
  }
}
