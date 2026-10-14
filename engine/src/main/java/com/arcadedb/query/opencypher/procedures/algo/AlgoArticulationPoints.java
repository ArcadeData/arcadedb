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
 * Procedure: algo.articulationPoints(relTypes?)
 * <p>
 * Finds all articulation points (cut vertices) in the graph using Tarjan's iterative DFS
 * algorithm. An articulation point is a vertex whose removal increases the number of connected
 * components. Only the articulation point vertices are returned.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.articulationPoints()
 * YIELD node
 * RETURN node.name
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoArticulationPoints extends AbstractAlgoProcedure {
  public static final String NAME = "algo.articulationPoints";

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
    return "Find all articulation points (cut vertices) in the graph using Tarjan's iterative DFS";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = extractRelTypes(argOrDefault(args, 0, null), context);

    final Database db = context.getDatabase();
    final GraphData graph = loadGraph(db, null, relTypes, context);
    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH, relTypes);
    final UndirectedLowPointDfs dfs = new UndirectedLowPointDfs(adj);
    final int[] childCount = new int[n];
    final boolean[] isAP = new boolean[n];

    dfs.walk(new UndirectedLowPointDfs.Visitor() {
      @Override
      public void treeEdge(final int vertex, final int child) {
        childCount[vertex]++;
      }

      @Override
      public void childFinished(final int vertex, final int child) {
        // A ROOT CUTS THE GRAPH WHEN IT HAS TWO DFS CHILDREN; ANY OTHER VERTEX WHEN A CHILD'S SUBTREE CANNOT CLIMB ABOVE IT
        if (dfs.parent[vertex] == -1 ? childCount[vertex] >= 2 : dfs.low[child] >= dfs.disc[vertex])
          isAP[vertex] = true;
      }
    });

    final List<Result> results = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      if (!isAP[i])
        continue;
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(i));
      results.add(r);
    }
    return results.stream();
  }
}
