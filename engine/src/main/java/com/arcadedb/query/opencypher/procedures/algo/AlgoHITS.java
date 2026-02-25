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
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.hits(relTypes?, maxIterations?, tolerance?)
 * <p>
 * Computes the HITS (Hyperlink-Induced Topic Search) algorithm, assigning each vertex a
 * hub score and an authority score via power iteration. Hub score reflects how well a
 * vertex points to authoritative vertices; authority score reflects how many good hubs
 * point to it.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.hits()
 * YIELD node, hubScore, authorityScore
 * RETURN node.name, hubScore, authorityScore ORDER BY authorityScore DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoHITS extends AbstractAlgoProcedure {
  public static final String NAME = "algo.hits";

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
    return 3;
  }

  @Override
  public String getDescription() {
    return "Compute HITS hub and authority scores for all vertices using power iteration";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "hubScore", "authorityScore");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes    = args.length > 0 ? extractRelTypes(args[0]) : null;
    final int maxIterations    = args.length > 1 ? ((Number) args[1]).intValue() : 20;
    final double tolerance     = args.length > 2 ? ((Number) args[2]).doubleValue() : 1e-6;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adjOut = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.OUT, relTypes);
    final int[][] adjIn  = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.IN, relTypes);

    double[] hub     = new double[n];
    double[] auth    = new double[n];
    double[] newHub  = new double[n];
    double[] newAuth = new double[n];

    for (int i = 0; i < n; i++) {
      hub[i]  = 1.0;
      auth[i] = 1.0;
    }

    for (int iter2 = 0; iter2 < maxIterations; iter2++) {
      // Update auth[v] = sum of hub[u] for all in-neighbors u of v
      for (int v = 0; v < n; v++) {
        double sum = 0.0;
        for (final int u : adjIn[v])
          sum += hub[u];
        newAuth[v] = sum;
      }

      // Update hub[v] = sum of auth[w] for all out-neighbors w of v
      for (int v = 0; v < n; v++) {
        double sum = 0.0;
        for (final int w : adjOut[v])
          sum += newAuth[w];
        newHub[v] = sum;
      }

      // Normalize by L2 norm
      double normHub = 0.0;
      double normAuth = 0.0;
      for (int i = 0; i < n; i++) {
        normHub  += newHub[i]  * newHub[i];
        normAuth += newAuth[i] * newAuth[i];
      }
      normHub  = Math.sqrt(normHub);
      normAuth = Math.sqrt(normAuth);

      if (normHub  > 0) for (int i = 0; i < n; i++) newHub[i]  /= normHub;
      if (normAuth > 0) for (int i = 0; i < n; i++) newAuth[i] /= normAuth;

      // Check convergence
      double maxChange = 0.0;
      for (int i = 0; i < n; i++) {
        final double dh = Math.abs(newHub[i]  - hub[i]);
        final double da = Math.abs(newAuth[i] - auth[i]);
        if (dh > maxChange) maxChange = dh;
        if (da > maxChange) maxChange = da;
      }

      // Swap arrays
      final double[] tmpH = hub;
      hub = newHub;
      newHub = tmpH;
      final double[] tmpA = auth;
      auth = newAuth;
      newAuth = tmpA;

      if (maxChange < tolerance)
        break;
    }

    final double[] finalHub  = hub;
    final double[] finalAuth = auth;
    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("hubScore", finalHub[i]);
      r.setProperty("authorityScore", finalAuth[i]);
      return (Result) r;
    });
  }
}
