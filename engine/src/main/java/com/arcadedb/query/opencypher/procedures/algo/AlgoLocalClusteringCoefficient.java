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

import java.util.Arrays;
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

    final String[] relTypes = args.length > 0 ? extractRelTypes(args[0]) : null;

    final Database db = context.getDatabase();

    final GraphData graph = loadGraph(db, null, relTypes, context);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH, relTypes);

    // Sort adjacency lists for merge-based intersection (O(m log m) total)
    for (int i = 0; i < n; i++)
      Arrays.sort(adj[i]);

    // Count triangles using sorted-merge intersection — O(m * sqrt(m)) time, O(m) memory
    final long[] triangles = new long[n];
    for (int u = 0; u < n; u++) {
      final int[] neighborsU = adj[u];
      for (final int v : neighborsU) {
        // Count common neighbors of u and v via sorted merge
        final int[] neighborsV = adj[v];
        int iu = 0, iv = 0;
        while (iu < neighborsU.length && iv < neighborsV.length) {
          if (neighborsU[iu] < neighborsV[iv])
            iu++;
          else if (neighborsU[iu] > neighborsV[iv])
            iv++;
          else {
            triangles[u]++;
            iu++;
            iv++;
          }
        }
      }
    }
    // Each triangle counted twice per node (once per neighbor direction)
    for (int u = 0; u < n; u++)
      triangles[u] /= 2;

    return IntStream.range(0, n).mapToObj(i -> {
      final long deg = adj[i].length;
      final double coeff = deg < 2 ? 0.0 : (2.0 * triangles[i]) / (double) (deg * (deg - 1));
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getVertex(i));
      r.setProperty("localClusteringCoefficient", coeff);
      return (Result) r;
    });
  }
}
