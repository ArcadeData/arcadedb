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
 * Procedure: algo.richClub(relTypes?, minDegree?)
 * <p>
 * Computes the rich-club coefficient φ(k) for each degree threshold k from minDegree to maxDegree.
 * φ(k) = 2·E_k / (N_k·(N_k−1)) where N_k = number of nodes with degree &gt; k and
 * E_k = number of edges between those nodes.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.richClub('KNOWS', 2)
 * YIELD degree, richClubCoefficient, nodeCount, edgeCount
 * RETURN degree, richClubCoefficient ORDER BY degree ASC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoRichClub extends AbstractAlgoProcedure {
  public static final String NAME = "algo.richClub";

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
    return 2;
  }

  @Override
  public String getDescription() {
    return "Computes rich-club coefficients for each degree threshold";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("degree", "richClubCoefficient", "nodeCount", "edgeCount");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = args.length > 0 ? extractRelTypes(args[0]) : null;
    final int minDegree = args.length > 1 && args[1] instanceof Number n ? n.intValue() : 2;

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

    // Compute degrees
    final int[] degree = new int[n];
    int maxDegree = 0;
    for (int i = 0; i < n; i++) {
      degree[i] = adj[i].length;
      if (degree[i] > maxDegree)
        maxDegree = degree[i];
    }

    if (maxDegree < minDegree)
      return Stream.empty();

    // Build OUT adjacency for edge counting (directed, use BOTH to avoid double-counting)
    // We'll use the undirected adjacency and count edges only for u < v
    final List<Result> results = new ArrayList<>(maxDegree - minDegree + 1);

    for (int k = minDegree; k <= maxDegree; k++) {
      // Build mask: nodes with degree > k
      final boolean[] inRichClub = new boolean[n];
      int nodeCount = 0;
      for (int i = 0; i < n; i++) {
        if (degree[i] > k) {
          inRichClub[i] = true;
          nodeCount++;
        }
      }

      if (nodeCount < 2) {
        final ResultInternal r = new ResultInternal();
        r.setProperty("degree", k);
        r.setProperty("richClubCoefficient", 0.0);
        r.setProperty("nodeCount", nodeCount);
        r.setProperty("edgeCount", 0);
        results.add(r);
        continue;
      }

      // Count edges between rich-club members (undirected: count each edge once)
      int edgeCount = 0;
      for (int u = 0; u < n; u++) {
        if (!inRichClub[u])
          continue;
        for (final int v : adj[u]) {
          if (v > u && inRichClub[v])
            edgeCount++;
        }
      }

      final long maxPossibleEdges = (long) nodeCount * (nodeCount - 1);
      final double phi = maxPossibleEdges == 0 ? 0.0 : (2.0 * edgeCount) / maxPossibleEdges;

      final ResultInternal r = new ResultInternal();
      r.setProperty("degree", k);
      r.setProperty("richClubCoefficient", phi);
      r.setProperty("nodeCount", nodeCount);
      r.setProperty("edgeCount", edgeCount);
      results.add(r);
    }

    return results.stream();
  }
}
