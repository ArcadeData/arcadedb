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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.kcore(relTypes?)
 * <p>
 * Computes the k-core decomposition of the graph using the O(V+E) algorithm by
 * Batagelj &amp; Zaversnik (2003). Each vertex is assigned its <em>core number</em>: the
 * largest k such that it belongs to a subgraph where every vertex has degree ≥ k.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.kcore()
 * YIELD node, coreNumber
 * RETURN node.name, coreNumber ORDER BY coreNumber DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoKCore extends AbstractAlgoProcedure {
  public static final String NAME = "algo.kcore";

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
    return "Compute k-core decomposition assigning each vertex its core number";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "coreNumber");
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
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.BOTH, relTypes);

    // Batagelj-Zaversnik O(V+E) k-core algorithm with bucket sort
    final int[] deg = new int[n];
    int maxDeg = 0;
    for (int i = 0; i < n; i++) {
      deg[i] = adj[i].length;
      if (deg[i] > maxDeg)
        maxDeg = deg[i];
    }

    // Bucket sort: bin[d] = number of vertices with degree d (then start offset)
    final int[] bin = new int[maxDeg + 1];
    for (int i = 0; i < n; i++)
      bin[deg[i]]++;
    int start = 0;
    for (int d = 0; d <= maxDeg; d++) {
      final int cnt = bin[d];
      bin[d] = start;
      start += cnt;
    }

    // vert[p] = vertex at sorted position p; pos[v] = position of vertex v
    final int[] vert = new int[n];
    final int[] pos = new int[n];
    final int[] binTmp = Arrays.copyOf(bin, bin.length);
    for (int i = 0; i < n; i++) {
      pos[i] = binTmp[deg[i]];
      vert[pos[i]] = i;
      binTmp[deg[i]]++;
    }

    // Core decomposition pass
    final int[] core = new int[n];
    for (int i = 0; i < n; i++) {
      final int v = vert[i];
      core[v] = deg[v];
      for (final int u : adj[v]) {
        if (deg[u] > core[v]) {
          final int du = deg[u];
          final int pw = bin[du]; // first vertex in bucket du
          final int pu = pos[u];
          if (pu != pw) {
            final int w = vert[pw];
            vert[pu] = w;
            vert[pw] = u;
            pos[w] = pu;
            pos[u] = pw;
          }
          bin[du]++;
          deg[u]--;
        }
      }
    }

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("coreNumber", core[i]);
      return (Result) r;
    });
  }
}
