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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.assortativity(relTypes?)
 * <p>
 * Computes Newman's degree assortativity coefficient for the graph. A positive value indicates
 * that high-degree nodes tend to connect to other high-degree nodes (assortative mixing),
 * while a negative value indicates disassortative mixing. Returns a single row.
 * </p>
 * <p>
 * Formula: r = [M⁻¹ Σ j_i·k_i − (M⁻¹ Σ ½(j_i+k_i))²] /
 *              [M⁻¹ Σ ½(j_i²+k_i²) − (M⁻¹ Σ ½(j_i+k_i))²]
 * where j_i, k_i are degrees of endpoints of undirected edge i, M = number of undirected edges.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.assortativity('KNOWS')
 * YIELD assortativity, edgeCount
 * RETURN assortativity, edgeCount
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoAssortativity extends AbstractAlgoProcedure {
  public static final String NAME = "algo.assortativity";

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
    return "Computes Newman's degree assortativity coefficient for the graph";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("assortativity", "edgeCount");
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
    if (n == 0) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("assortativity", 0.0);
      r.setProperty("edgeCount", 0L);
      return Stream.of(r);
    }

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] outAdj = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.OUT, relTypes);

    // Compute undirected degrees: degree[i] = number of unique neighbors (BOTH direction)
    final int[][] bothAdj = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.BOTH, relTypes);
    final int[] degree = new int[n];
    for (int i = 0; i < n; i++)
      degree[i] = bothAdj[i].length;

    // Iterate over undirected edges (OUT adjacency, treat as u->v and skip if u>v to avoid dups
    // But since directed graphs have edges u->v only, we iterate all OUT edges)
    // Newman's formula uses directed edges (each directed edge is one pair (j_i, k_i))
    // For undirected treatment: iterate OUT edges only, use BOTH degrees for both endpoints

    double sumJK = 0.0;         // Σ j_i * k_i
    double sumHalfJplusK = 0.0; // Σ 1/2 * (j_i + k_i)
    double sumHalfJ2K2 = 0.0;   // Σ 1/2 * (j_i² + k_i²)
    long edgeCount = 0;

    for (int u = 0; u < n; u++) {
      final int ju = degree[u];
      for (final int v : outAdj[u]) {
        final int kv = degree[v];
        sumJK += ju * kv;
        sumHalfJplusK += 0.5 * (ju + kv);
        sumHalfJ2K2 += 0.5 * (ju * ju + kv * kv);
        edgeCount++;
      }
    }

    final ResultInternal result = new ResultInternal();
    if (edgeCount == 0) {
      result.setProperty("assortativity", 0.0);
      result.setProperty("edgeCount", 0L);
      return Stream.of(result);
    }

    final double M = edgeCount;
    final double meanJK = sumJK / M;
    final double meanHalf = sumHalfJplusK / M;
    final double meanHalfSq = sumHalfJ2K2 / M;

    final double numerator = meanJK - meanHalf * meanHalf;
    final double denominator = meanHalfSq - meanHalf * meanHalf;

    final double assortativity = denominator == 0.0 ? 0.0 : numerator / denominator;

    result.setProperty("assortativity", assortativity);
    result.setProperty("edgeCount", edgeCount);
    return Stream.of(result);
  }
}
