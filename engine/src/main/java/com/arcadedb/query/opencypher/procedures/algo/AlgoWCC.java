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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.wcc([config])
 * <p>
 * Finds Weakly Connected Components (WCC) in the graph. Two nodes belong to the same component
 * if there exists a path between them when all edges are treated as undirected. This is
 * fundamental for determining if a network is fully connected and for isolating disconnected
 * subgraphs.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL algo.wcc()
 * YIELD node, componentId
 * RETURN componentId, count(*) AS size ORDER BY size DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoWCC extends AbstractAlgoProcedure {
  public static final String NAME = "algo.wcc";

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
    return "Finds weakly connected components in the graph";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "componentId");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Database db = context.getDatabase();

    final GraphData graph = loadGraph(db, null, null, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH);

    final int[] componentId = new int[n];
    for (int i = 0; i < n; i++)
      componentId[i] = -1;

    int nextComponentId = 0;

    // BFS treating all edges as undirected, using int[] queue (zero allocation)
    final int[] queue = new int[n];
    for (int i = 0; i < n; i++) {
      if (componentId[i] != -1)
        continue;

      int head = 0, tail = 0;
      queue[tail++] = i;
      componentId[i] = nextComponentId;

      while (head < tail) {
        final int v = queue[head++];
        for (final int w : adj[v]) {
          if (componentId[w] != -1)
            continue;
          componentId[w] = nextComponentId;
          queue[tail++] = w;
        }
      }
      nextComponentId++;
    }

    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", graph.getVertex(i));
      result.setProperty("componentId", componentId[i]);
      results.add(result);
    }
    return results.stream();
  }
}
