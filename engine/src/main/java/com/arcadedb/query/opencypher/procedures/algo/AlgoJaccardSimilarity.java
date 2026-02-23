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
 * Procedure: algo.jaccard(node, relTypes?, direction?, cutoff?)
 * <p>
 * Computes the Jaccard similarity between the given source node and every other vertex in the
 * graph: {@code |N(src) ∩ N(v)| / |N(src) ∪ N(v)|}. Only pairs with similarity &gt; cutoff
 * (default 0.0) are returned. Uses BitSet neighbor sets for O(degree) intersection with no
 * per-pair heap allocation.
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
    final String[] relTypes   = args.length > 1 ? extractRelTypes(args[1]) : null;
    final Vertex.DIRECTION dir = args.length > 2 ? parseDirection(extractString(args[2], "direction")) : Vertex.DIRECTION.BOTH;
    final double cutoff       = args.length > 3 ? ((Number) args[3]).doubleValue() : 0.0;

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

    final Integer srcIdxObj = ridToIdx.get(sourceVertex.getIdentity());
    if (srcIdxObj == null)
      return Stream.empty();
    final int srcIdx = srcIdxObj;

    // Build source neighbor BitSet
    final BitSet srcNeighbors = new BitSet(n);
    for (final int j : adj[srcIdx])
      srcNeighbors.set(j);
    final int srcDeg = adj[srcIdx].length;

    final List<Result> results = new ArrayList<>();
    for (int v = 0; v < n; v++) {
      if (v == srcIdx)
        continue;
      // Count intersection without allocation: iterate adj[v] and check BitSet
      int intersection = 0;
      for (final int w : adj[v]) {
        if (srcNeighbors.get(w))
          intersection++;
      }
      final int union = srcDeg + adj[v].length - intersection;
      if (union == 0)
        continue;
      final double similarity = (double) intersection / union;
      if (similarity <= cutoff)
        continue;
      final ResultInternal r = new ResultInternal();
      r.setProperty("node1", sourceVertex);
      r.setProperty("node2", vertices.get(v));
      r.setProperty("similarity", similarity);
      results.add(r);
    }
    return results.stream();
  }
}
