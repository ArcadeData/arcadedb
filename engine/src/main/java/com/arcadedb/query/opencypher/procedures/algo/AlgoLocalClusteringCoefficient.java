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
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.localClusteringCoefficient(relTypes?)
 * <p>
 * Computes the local clustering coefficient for every node in the graph. The local clustering
 * coefficient of a node {@code u} measures the density of connections among its neighbours:
 * {@code LCC(u) = 2 * t(u) / (deg(u) * (deg(u) - 1))}, where {@code t(u)} is the number of
 * triangles that {@code u} participates in.
 * </p>
 * <p>
 * When a Graph Analytical View (GAV) is available, delegates to the CSR-native implementation
 * in {@link GraphAlgorithms#localClusteringCoefficient} for maximum performance.
 * </p>
 * <p>
 * Nodes with degree &lt; 2 receive a coefficient of 0.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.localClusteringCoefficient()
 * YIELD node, localClusteringCoefficient
 * RETURN node.name, localClusteringCoefficient ORDER BY localClusteringCoefficient DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoLocalClusteringCoefficient extends AbstractAlgoProcedure {
  public static final String NAME = "algo.localClusteringCoefficient";

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
    return "Computes the local clustering coefficient for each node";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "localClusteringCoefficient");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = extractRelTypes(argOrDefault(args, 0, null), context);

    final Database db = context.getDatabase();

    // Try CSR-accelerated path
    final GraphTraversalProvider provider = findProvider(db, relTypes, context);
    // Only while the view is not serving pending changes: the GraphAlgorithms kernel below reads its base CSR
    // arrays directly and sizes its result from the base node mapping, which is neither the current graph nor
    // as wide as the id space the view now reports - see GraphTraversalProvider#hasPendingChanges (issue #6792).
    if (provider instanceof GraphAnalyticalView.Pinned gav && !gav.hasPendingChanges()) {
      context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
      return executeWithCSR(context, gav, relTypes);
    }

    // Fall back to OLTP path
    return executeWithOLTP(db, relTypes, context);
  }

  private Stream<Result> executeWithCSR(final CommandContext context, final GraphAnalyticalView.Pinned gav, final String[] relTypes) {
    final int n = gav.getNodeCount();
    if (n == 0)
      return Stream.empty();

    // The CSR path is now abortable the same way the OLTP path is (issue #6318): the kernel's two parallel
    // phases and its sequential prep passes are all checkpointed via this guard.
    final WorkGuard guard = newWorkGuard(context);
    final double[] lcc = relTypes != null ?
        GraphAlgorithms.localClusteringCoefficient(gav, guard::check, relTypes) :
        GraphAlgorithms.localClusteringCoefficient(gav, guard::check);

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", gav.getRID(i));
      r.setProperty("localClusteringCoefficient", lcc[i]);
      return (Result) r;
    });
  }

  private Stream<Result> executeWithOLTP(final Database db, final String[] relTypes, final CommandContext context) {
    final WorkGuard guard = newWorkGuard(context);
    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    // The simple undirected graph, as the CSR kernel builds it: self-loops, parallel and reciprocal edges would
    // otherwise inflate both the triangle count and the degree, so the two paths answered differently (issue #10203).
    // Both loops are checkpointed per node by the same guard the CSR path threads into its kernel (issues #6302, #6318).
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH, relTypes);
    toSimpleNeighbourSets(adj, guard);
    final long[] triangles = countTriangles(adj, guard);

    return IntStream.range(0, n).mapToObj(i -> {
      final long deg = adj[i].length;
      final double coeff = deg < 2 ? 0.0 : (2.0 * triangles[i]) / (double) (deg * (deg - 1));
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(i));
      r.setProperty("localClusteringCoefficient", coeff);
      return (Result) r;
    });
  }
}
