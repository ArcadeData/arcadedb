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
 * Procedure: algo.eccentricity(relTypes?, direction?)
 * <p>
 * Computes the eccentricity of each vertex: the maximum BFS distance to any reachable vertex
 * (unreachable vertices are ignored — partial eccentricity). Also identifies center vertices
 * (eccentricity == radius) and peripheral vertices (eccentricity == diameter).
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.eccentricity()
 * YIELD node, eccentricity, isCenter, isPeripheral
 * RETURN node.name, eccentricity, isCenter ORDER BY eccentricity
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoEccentricity extends AbstractAlgoProcedure {
  public static final String NAME = "algo.eccentricity";

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
    return "Compute eccentricity for all vertices and identify graph center and periphery";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "eccentricity", "isCenter", "isPeripheral");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = args.length > 0 ? extractRelTypes(args[0]) : null;
    final Vertex.DIRECTION dir = args.length > 1 ? parseDirection(extractString(args[1], "direction")) : Vertex.DIRECTION.BOTH;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, dir, relTypes);

    final int[] ecc = new int[n];
    final int[] queue = new int[n];
    final int[] dist = new int[n];

    for (int src = 0; src < n; src++) {
      Arrays.fill(dist, -1);
      dist[src] = 0;
      int head = 0, tail = 0;
      queue[tail++] = src;
      int maxDist = 0;

      while (head < tail) {
        final int u = queue[head++];
        for (final int v : adj[u]) {
          if (dist[v] == -1) {
            dist[v] = dist[u] + 1;
            if (dist[v] > maxDist)
              maxDist = dist[v];
            queue[tail++] = v;
          }
        }
      }
      ecc[src] = maxDist;
    }

    // Compute diameter and radius in a single pass
    int diameter = 0;
    int radius = Integer.MAX_VALUE;
    for (int i = 0; i < n; i++) {
      if (ecc[i] > diameter)
        diameter = ecc[i];
      if (ecc[i] > 0 && ecc[i] < radius)
        radius = ecc[i];
    }
    if (radius == Integer.MAX_VALUE)
      radius = 0;

    final int finalDiameter = diameter;
    final int finalRadius = radius;

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("eccentricity", ecc[i]);
      r.setProperty("isCenter", ecc[i] > 0 && ecc[i] == finalRadius);
      r.setProperty("isPeripheral", ecc[i] == finalDiameter);
      return (Result) r;
    });
  }
}
