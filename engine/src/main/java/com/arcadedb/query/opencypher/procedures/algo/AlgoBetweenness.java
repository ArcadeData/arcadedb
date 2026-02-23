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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Stream;

/**
 * Procedure: algo.betweenness([config])
 * <p>
 * Computes betweenness centrality for all nodes using the Brandes algorithm. Betweenness
 * centrality measures how often a node lies on the shortest path between other nodes,
 * identifying critical bridge or broker nodes.
 * </p>
 * <p>
 * Config map parameters (all optional):
 * <ul>
 *   <li>normalized (boolean, default true): whether to normalize scores by 2/((n-1)(n-2))</li>
 * </ul>
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL algo.betweenness({normalized: true})
 * YIELD node, score
 * RETURN node.name, score ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoBetweenness extends AbstractAlgoProcedure {
  public static final String NAME = "algo.betweenness";

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
    return "Computes betweenness centrality scores for all nodes using the Brandes algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<String, Object> config = args.length > 0 ? extractMap(args[0], "config") : null;
    final boolean normalized = config == null || !Boolean.FALSE.equals(config.get("normalized"));

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> vertIter = getAllVertices(db, null);
    while (vertIter.hasNext())
      vertices.add(vertIter.next());
    if (vertices.isEmpty())
      return Stream.empty();

    final int n = vertices.size();
    final Map<Vertex, Integer> vertexIndex = new HashMap<>(n);
    for (int i = 0; i < n; i++)
      vertexIndex.put(vertices.get(i), i);

    final double[] betweenness = new double[n];

    // Brandes algorithm
    for (int s = 0; s < n; s++) {
      final Vertex source = vertices.get(s);

      final Deque<Integer> stack = new ArrayDeque<>();
      final List<List<Integer>> predecessors = new ArrayList<>(n);
      for (int i = 0; i < n; i++)
        predecessors.add(new ArrayList<>());

      final double[] sigma = new double[n];
      final int[] dist = new int[n];
      sigma[s] = 1.0;
      for (int i = 0; i < n; i++)
        dist[i] = -1;
      dist[s] = 0;

      final Queue<Integer> queue = new LinkedList<>();
      queue.add(s);

      while (!queue.isEmpty()) {
        final int v = queue.poll();
        stack.push(v);

        final Vertex vVertex = vertices.get(v);
        for (final Edge edge : vVertex.getEdges(Vertex.DIRECTION.OUT)) {
          final Vertex neighbor = edge.getInVertex();
          final Integer w = vertexIndex.get(neighbor);
          if (w == null)
            continue;

          // First time visiting w?
          if (dist[w] < 0) {
            queue.add(w);
            dist[w] = dist[v] + 1;
          }
          // Shortest path to w via v?
          if (dist[w] == dist[v] + 1) {
            sigma[w] += sigma[v];
            predecessors.get(w).add(v);
          }
        }
      }

      // Back-propagation
      final double[] delta = new double[n];
      while (!stack.isEmpty()) {
        final int w = stack.pop();
        for (final int v : predecessors.get(w)) {
          delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w]);
        }
        if (w != s)
          betweenness[w] += delta[w];
      }
    }

    // Normalize
    if (normalized && n > 2) {
      final double normFactor = 2.0 / ((double) (n - 1) * (n - 2));
      for (int i = 0; i < n; i++)
        betweenness[i] *= normFactor;
    }

    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", vertices.get(i));
      result.setProperty("score", betweenness[i]);
      results.add(result);
    }
    return results.stream();
  }
}
