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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
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
    final WorkGuard guard = newWorkGuard(context);
    final GraphData graph = loadGraph(db, null, null, context);
    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.OUT);

    final double[] betweenness = new double[n];

    // Brandes algorithm.
    //
    // A forward BFS and a backward accumulation per source node, so O(V x E) sized by nothing but the graph
    // (issue #6302). The inner checkpoints keep the abort latency below one whole source's pass.
    //
    // Every per-source working structure below is allocated once, outside the loop, and reused across all n
    // sources - issue #6718: the previous version allocated n fresh ArrayList<Integer> instances (predecessors)
    // per source, n times over, an O(n^2) allocation-and-boxing cost cutting against the project's "prefer arrays
    // of primitives" guidance. stack/queue/dist/sigma/delta/predCount are reset only over the nodes a source's
    // BFS actually touches - the same "reset only what you touched" pattern issue #6265 used for
    // AlgoInfluenceMaximization's activated/queue buffers - rather than a full O(n) clear per source, which would
    // have only traded the ArrayList allocations for an equally O(n^2) fill.
    final int[]    stack         = new int[n];
    final int[]    queue         = new int[n];
    final int[][]  predNeighbors = new int[n][];
    final int[]    predCount     = new int[n];
    final double[] sigma         = new double[n];
    final int[]    dist          = new int[n];
    final double[] delta         = new double[n];
    Arrays.fill(dist, -1);

    for (int s = 0; s < n; s++) {
      guard.check();

      int stackSize = 0;
      int qHead = 0, qTail = 0;

      sigma[s] = 1.0;
      dist[s] = 0;
      queue[qTail++] = s;

      int visited = 0;
      while (qHead < qTail) {
        guard.checkPeriodically(visited++);
        final int v = queue[qHead++];
        stack[stackSize++] = v;

        for (final int w : adj[v]) {
          // First time visiting w?
          if (dist[w] < 0) {
            queue[qTail++] = w;
            dist[w] = dist[v] + 1;
          }
          // Shortest path to w via v?
          if (dist[w] == dist[v] + 1) {
            sigma[w] += sigma[v];
            if (predNeighbors[w] == null)
              predNeighbors[w] = new int[4];
            else if (predCount[w] == predNeighbors[w].length)
              predNeighbors[w] = Arrays.copyOf(predNeighbors[w], predNeighbors[w].length * 2);
            predNeighbors[w][predCount[w]++] = v;
          }
        }
      }

      // Back-propagation, in the same reverse-BFS-order that draining a LIFO stack would give - here just a
      // descending walk of the array the forward BFS above already filled in that order.
      int accumulated = 0;
      for (int idx = stackSize - 1; idx >= 0; idx--) {
        guard.checkPeriodically(accumulated++);
        final int w = stack[idx];
        final int[] preds = predNeighbors[w];
        final int predN = predCount[w];
        for (int k = 0; k < predN; k++) {
          final int v = preds[k];
          delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w]);
        }
        if (w != s)
          betweenness[w] += delta[w];
      }

      // Reset only the nodes this source's BFS touched, not the full n-sized buffers.
      for (int idx = 0; idx < stackSize; idx++) {
        final int node = stack[idx];
        dist[node] = -1;
        sigma[node] = 0.0;
        delta[node] = 0.0;
        predCount[node] = 0;
      }
    }

    // Normalize
    if (normalized && n > 2) {
      final double normFactor = 2.0 / ((double) (n - 1) * (n - 2));
      for (int i = 0; i < n; i++)
        betweenness[i] *= normFactor;
    }

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", graph.getRID(i));
      result.setProperty("score", betweenness[i]);
      return (Result) result;
    });
  }
}
