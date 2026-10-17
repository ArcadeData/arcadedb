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

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.triangleCount(relTypes?)
 * <p>
 * Counts the number of triangles each vertex participates in and computes the local
 * clustering coefficient over the simple undirected graph: edge direction, parallel edges and
 * self-loops do not change the count. Uses sorted-merge intersection, with no memory beyond the
 * adjacency and the result.
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

    final String[] relTypes = extractRelTypes(argOrDefault(args, 0, null), context);

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    final GraphData graph = loadGraph(db, null, relTypes, context);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    // Triangles are an undirected, simple-graph notion: BOTH directions, each distinct neighbour once and no
    // self-loop (issue #10203). The degree of the clustering coefficient is then the distinct-neighbour count too.
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH, relTypes);
    toSimpleNeighbourSets(adj, guard);
    final long[] triangles = countTriangles(adj, guard);

    return IntStream.range(0, n).mapToObj(i -> {
      final long deg = adj[i].length;
      final double coeff = deg < 2 ? 0.0 : (2.0 * triangles[i]) / (double) (deg * (deg - 1));
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(i));
      r.setProperty("triangles", triangles[i]);
      r.setProperty("clusteringCoefficient", coeff);
      return (Result) r;
    });
  }
}
