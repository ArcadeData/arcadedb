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
import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.bridges(relTypes?)
 * <p>
 * Finds all bridge edges in the graph using Tarjan's iterative DFS algorithm.
 * A bridge is an edge whose removal increases the number of connected components of the UNDIRECTED graph, so the
 * stored direction of an edge plays no part in whether it is one (issue #9766). Two parallel edges between the same
 * pair are never bridges, since removing one leaves the other. Returns one result per bridge edge, oriented as the
 * edge is stored (source → target).
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.bridges()
 * YIELD source, target
 * RETURN source.name, target.name
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoBridges extends AbstractAlgoProcedure {
  public static final String NAME = "algo.bridges";

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
    return "Find all bridge edges in the graph using Tarjan's iterative DFS";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("source", "target");
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

    // A BRIDGE IS A PROPERTY OF THE UNDIRECTED GRAPH: EVERY EDGE MUST BE SEEN FROM BOTH OF ITS ENDPOINTS (ISSUE #9766)
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH, relTypes);
    final UndirectedLowPointDfs dfs = new UndirectedLowPointDfs(adj);

    final List<Result> results = new ArrayList<>();
    dfs.walk(new UndirectedLowPointDfs.Visitor() {
      @Override
      public void childFinished(final int parent, final int child) {
        // NO BACK EDGE FROM THE CHILD'S SUBTREE REACHES THE PARENT OR ABOVE: THE TREE EDGE IS THE ONLY WAY ACROSS
        if (dfs.low[child] > dfs.disc[parent])
          results.add(bridge(graph, adj, parent, child, relTypes));
      }
    });

    return results.stream();
  }

  /**
   * The bridge between {@code a} and {@code b}, source first as the edge is stored. The walk is undirected and does not
   * know which way the single edge between the two points, so the endpoint with the shorter row is asked whether it
   * holds an outgoing one to the other: a scan of edge-list pointers that loads no edge record, bounded by the smaller
   * degree. When that vertex can no longer be loaded (deleted since a Graph Analytical View was built) the other endpoint
   * is asked instead; only when neither can be loaded is the walk's own order kept.
   */
  private static Result bridge(final GraphData graph, final int[][] adj, final int a, final int b, final String[] relTypes) {
    int from = adj[a].length <= adj[b].length ? a : b;
    int to = from == a ? b : a;
    final Vertex fromVertex = graph.getVertex(from);
    final boolean swap;
    if (fromVertex != null)
      swap = !isConnectedOut(fromVertex, graph.getRID(to), relTypes);
    else {
      final Vertex toVertex = graph.getVertex(to);
      swap = toVertex != null && isConnectedOut(toVertex, graph.getRID(from), relTypes);
    }
    if (swap) {
      final int tmp = from;
      from = to;
      to = tmp;
    }
    final ResultInternal r = new ResultInternal();
    r.setProperty("source", graph.getRID(from));
    r.setProperty("target", graph.getRID(to));
    return r;
  }

  private static boolean isConnectedOut(final Vertex vertex, final RID other, final String[] relTypes) {
    if (relTypes == null || relTypes.length == 0)
      return vertex.isConnectedTo(other, Vertex.DIRECTION.OUT);
    for (final String relType : relTypes)
      if (vertex.isConnectedTo(other, Vertex.DIRECTION.OUT, relType))
        return true;
    return false;
  }
}
