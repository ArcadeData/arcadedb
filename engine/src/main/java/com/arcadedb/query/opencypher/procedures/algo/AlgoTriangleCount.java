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
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.triangleCount(relTypes?)
 * <p>
 * Counts the number of triangles each vertex participates in and computes the local
 * clustering coefficient. Uses BOTH-direction adjacency and BitSet-based intersection for
 * minimal GC pressure.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.triangleCount()
 * YIELD node, triangles, clusteringCoefficient
 * RETURN node.name, triangles ORDER BY triangles DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoTriangleCount extends AbstractAlgoProcedure {
  public static final String NAME = "algo.triangleCount";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 0;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Count triangles per vertex and compute local clustering coefficient";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "triangles", "clusteringCoefficient");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = args.length > 0 ? extractRelTypes(args[0]) : null;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    // Always use BOTH for triangle counting (undirected notion)
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.BOTH, relTypes);

    // Build neighbor BitSets for fast intersection
    final BitSet[] neighborSets = new BitSet[n];
    for (int i = 0; i < n; i++) {
      neighborSets[i] = new BitSet(n);
      for (final int j : adj[i])
        neighborSets[i].set(j);
    }

    final long[] triangles = new long[n];

    // For each node u, for each neighbor v of u, count common neighbors
    // Each triangle (u,v,w) is counted at u via (v,w) AND via (w,v) → divide by 2
    for (int u = 0; u < n; u++) {
      long count = 0;
      final int[] nu = adj[u];
      for (int ki = 0; ki < nu.length; ki++) {
        final int v = nu[ki];
        // Count neighbors of v that are in N(u) — inner loop, no allocation
        final int[] nv = adj[v];
        for (int kj = 0; kj < nv.length; kj++) {
          if (neighborSets[u].get(nv[kj]))
            count++;
        }
      }
      triangles[u] = count / 2;
    }

    return IntStream.range(0, n).mapToObj(i -> {
      final long deg = adj[i].length;
      final double coeff = deg < 2 ? 0.0 : (2.0 * triangles[i]) / (double) (deg * (deg - 1));
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("triangles", triangles[i]);
      r.setProperty("clusteringCoefficient", coeff);
      return (Result) r;
    });
  }
}
