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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.modularityScore(communityProperty, relTypes?)
 * <p>
 * Computes the modularity score Q of a community partition stored as a vertex property.
 * Modularity measures the quality of a community partition:
 * Q = sum_c [ L_c/m - (d_c/(2m))^2 ]
 * where L_c = edges within community c, d_c = sum of degrees in community c,
 * and m = total number of edges.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.modularityScore('community', 'KNOWS')
 * YIELD modularity, communities, edgeCount
 * RETURN modularity, communities
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoModularityScore extends AbstractAlgoProcedure {
  public static final String NAME = "algo.modularityScore";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Computes the modularity score Q of a community partition stored as a vertex property";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("modularity", "communities", "edgeCount");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String communityProperty = extractString(args[0], "communityProperty");
    final String[] relTypes        = args.length > 1 ? extractRelTypes(args[1]) : null;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("modularity", 0.0);
      r.setProperty("communities", 0);
      r.setProperty("edgeCount", 0L);
      return Stream.of(r);
    }

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adjOut = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.OUT, relTypes);

    // Map community label (Object) to a compact integer index
    final Map<Object, Integer> communityToIdx = new HashMap<>();
    final int[] vertexCommunity = new int[n];
    int numCommunities = 0;

    for (int i = 0; i < n; i++) {
      final Object communityLabel = vertices.get(i).get(communityProperty);
      final Object key = communityLabel != null ? communityLabel : "__null__";
      Integer idx = communityToIdx.get(key);
      if (idx == null) {
        idx = numCommunities++;
        communityToIdx.put(key, idx);
      }
      vertexCommunity[i] = idx;
    }

    // L_c[c] = number of edges within community c (directed: both endpoints in c)
    // d_c[c] = sum of out-degrees of nodes in community c
    final long[] internalEdges = new long[numCommunities];
    final long[] totalDegree   = new long[numCommunities];
    long totalEdges = 0L;

    for (int i = 0; i < n; i++) {
      final int ci = vertexCommunity[i];
      final int deg = adjOut[i].length;
      totalDegree[ci] += deg;
      totalEdges += deg;
      for (final int j : adjOut[i]) {
        if (vertexCommunity[j] == ci)
          internalEdges[ci]++;
      }
    }

    // m = total edges (directed count)
    final long m = totalEdges;
    if (m == 0) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("modularity", 0.0);
      r.setProperty("communities", numCommunities);
      r.setProperty("edgeCount", 0L);
      return Stream.of(r);
    }

    // Q = sum_c [ L_c/m - (d_c/(2m))^2 ]
    double modularity = 0.0;
    for (int c = 0; c < numCommunities; c++) {
      final double lc = internalEdges[c];
      final double dc = totalDegree[c];
      modularity += (lc / m) - (dc / (2.0 * m)) * (dc / (2.0 * m));
    }

    final ResultInternal result = new ResultInternal();
    result.setProperty("modularity", modularity);
    result.setProperty("communities", numCommunities);
    result.setProperty("edgeCount", m);
    return Stream.of(result);
  }
}
