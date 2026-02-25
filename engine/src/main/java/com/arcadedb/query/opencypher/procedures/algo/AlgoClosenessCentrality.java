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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.closeness(relTypes?, direction?, normalized?)
 * <p>
 * Computes closeness centrality for every vertex: the reciprocal of the average shortest-path
 * distance to all other reachable vertices (BFS, unweighted). If {@code normalized=true}
 * (default) the Wasserman-Faust formula is applied so that disconnected graphs are handled fairly.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.closeness('ROAD', 'BOTH', true)
 * YIELD node, score
 * RETURN node.name, score ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoClosenessCentrality extends AbstractAlgoProcedure {
  public static final String NAME = "algo.closeness";

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
    return 3;
  }

  @Override
  public String getDescription() {
    return "Compute closeness centrality for all vertices using BFS";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = args.length > 0 ? extractRelTypes(args[0]) : null;
    final Vertex.DIRECTION dir = args.length > 1 ? parseDirection(extractString(args[1], "direction")) : Vertex.DIRECTION.BOTH;
    final boolean normalized = args.length > 2 ? Boolean.parseBoolean(args[2].toString()) : true;

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

    // BFS from each vertex using primitive int[] queue (no boxing)
    final int[] queue = new int[n];
    final int[] dist = new int[n];
    final double[] scores = new double[n];

    for (int src = 0; src < n; src++) {
      Arrays.fill(dist, -1);
      dist[src] = 0;
      int head = 0, tail = 0;
      queue[tail++] = src;
      long sumDist = 0;
      int reachable = 0;

      while (head < tail) {
        final int u = queue[head++];
        final int[] neighbors = adj[u];
        for (int k = 0; k < neighbors.length; k++) {
          final int v = neighbors[k];
          if (dist[v] == -1) {
            dist[v] = dist[u] + 1;
            sumDist += dist[v];
            reachable++;
            queue[tail++] = v;
          }
        }
      }

      if (reachable == 0 || sumDist == 0) {
        scores[src] = 0.0;
      } else {
        scores[src] = (double) reachable / sumDist;
        if (normalized && n > 1)
          scores[src] *= (double) reachable / (n - 1);
      }
    }

    // Build result stream
    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("score", scores[i]);
      return (Result) r;
    });
  }
}
