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
 * Procedure: algo.bipartite(relTypes?)
 * <p>
 * Checks whether the graph is bipartite using BFS 2-coloring. Returns each node with its
 * partition (0 or 1) and a global {@code isBipartite} flag. Works on disconnected graphs
 * by independently coloring each connected component.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.bipartite()
 * YIELD node, partition, isBipartite
 * RETURN node.name, partition, isBipartite
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoBipartiteCheck extends AbstractAlgoProcedure {
  public static final String NAME = "algo.bipartite";

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
    return "Check whether the graph is bipartite using BFS 2-coloring";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "partition", "isBipartite");
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
    // Use BOTH direction for bipartite check (undirected interpretation)
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.BOTH, relTypes);

    final int[] color = new int[n];
    Arrays.fill(color, -1);
    boolean bipartite = true;
    final int[] queue = new int[n];

    for (int root = 0; root < n; root++) {
      if (color[root] != -1)
        continue;
      color[root] = 0;
      int head = 0, tail = 0;
      queue[tail++] = root;

      while (head < tail) {
        final int v = queue[head++];
        for (final int u : adj[v]) {
          if (color[u] == -1) {
            color[u] = 1 - color[v];
            queue[tail++] = u;
          } else if (color[u] == color[v])
            bipartite = false;
        }
      }
    }

    final boolean finalBipartite = bipartite;

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("partition", color[i] == -1 ? 0 : color[i]);
      r.setProperty("isBipartite", finalBipartite);
      return (Result) r;
    });
  }
}
