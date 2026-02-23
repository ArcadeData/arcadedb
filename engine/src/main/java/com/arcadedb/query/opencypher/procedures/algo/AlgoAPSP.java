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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.apsp(weightProperty?, relTypes?)
 * <p>
 * Computes All-Pairs Shortest Paths (APSP) using the Floyd-Warshall algorithm O(V³).
 * Returns one result per reachable (source, target) pair (i ≠ j). When no weight property
 * is specified, all edges have unit weight 1.0.
 * </p>
 * <p>
 * Note: this algorithm is O(V²) in memory and O(V³) in time. Only suitable for graphs
 * with up to a few thousand vertices.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.apsp('distance', 'ROAD')
 * YIELD source, target, distance
 * RETURN source.name, target.name, distance ORDER BY distance ASC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoAPSP extends AbstractAlgoProcedure {
  public static final String NAME = "algo.apsp";

  private static final double INF = Double.MAX_VALUE / 2.0;

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
    return 2;
  }

  @Override
  public String getDescription() {
    return "Compute all-pairs shortest paths using Floyd-Warshall algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("source", "target", "distance");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String weightProperty = args.length > 0 ? extractString(args[0], "weightProperty") : null;
    final String[] relTypes     = args.length > 1 ? extractRelTypes(args[1]) : null;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);

    // Allocate distance matrix: one large contiguous allocation is GC-friendly
    final double[][] dist = new double[n][n];
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++)
        dist[i][j] = (i == j) ? 0.0 : INF;
    }

    // Fill direct edges
    for (int i = 0; i < n; i++) {
      final Iterable<Edge> edges = relTypes != null && relTypes.length > 0 ?
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT, relTypes) :
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT);
      for (final Edge e : edges) {
        final Integer j = ridToIdx.get(e.getIn());
        if (j == null)
          continue;
        final double w;
        if (weightProperty != null) {
          final Object wObj = e.get(weightProperty);
          w = wObj instanceof Number num ? num.doubleValue() : 1.0;
        } else
          w = 1.0;
        if (w < dist[i][j])
          dist[i][j] = w;
      }
    }

    // Floyd-Warshall
    for (int k = 0; k < n; k++) {
      for (int i = 0; i < n; i++) {
        if (dist[i][k] >= INF)
          continue;  // Skip unreachable intermediate
        for (int j = 0; j < n; j++) {
          final double through = dist[i][k] + dist[k][j];
          if (through < dist[i][j])
            dist[i][j] = through;
        }
      }
    }

    // Collect results: only reachable pairs with i != j
    final List<Result> results = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        if (i == j || dist[i][j] >= INF)
          continue;
        final ResultInternal r = new ResultInternal();
        r.setProperty("source", vertices.get(i));
        r.setProperty("target", vertices.get(j));
        r.setProperty("distance", dist[i][j]);
        results.add(r);
      }
    }
    return results.stream();
  }
}
