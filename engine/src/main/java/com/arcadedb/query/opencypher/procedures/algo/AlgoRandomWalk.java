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
import java.util.Random;
import java.util.stream.Stream;

/**
 * Procedure: algo.randomWalk(startNode, steps, relTypes?, direction?, seed?)
 * <p>
 * Performs a uniform random walk of exactly {@code steps} hops from {@code startNode},
 * stopping early if a dead-end is reached. The full graph adjacency is materialised once
 * using primitive {@code int[][]} arrays; the walk itself uses only a single {@code int[]}
 * (no heap allocation in the inner loop).
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (a:Page {title: 'Java'})
 * CALL algo.randomWalk(a, 10)
 * YIELD path, steps
 * RETURN path
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoRandomWalk extends AbstractAlgoProcedure {
  public static final String NAME = "algo.randomWalk";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 5;
  }

  @Override
  public String getDescription() {
    return "Perform a uniform random walk from a start node for a given number of steps";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("path", "steps");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startVertex = extractVertex(args[0], "startNode");
    final int steps = ((Number) args[1]).intValue();
    final String[] relTypes   = args.length > 2 ? extractRelTypes(args[2]) : null;
    final Vertex.DIRECTION dir = args.length > 3 ? parseDirection(extractString(args[3], "direction")) : Vertex.DIRECTION.BOTH;
    final long seed = args.length > 4 ? ((Number) args[4]).longValue() : System.currentTimeMillis();

    if (steps <= 0)
      return Stream.empty();

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

    final Integer srcIdxObj = ridToIdx.get(startVertex.getIdentity());
    if (srcIdxObj == null)
      return Stream.empty();

    // Walk — use int[] array (all primitives, zero boxing)
    final int[] walk = new int[steps + 1];
    walk[0] = srcIdxObj;
    int walkLen = 1;

    final Random rnd = new Random(seed);
    int cur = srcIdxObj;
    for (int step = 0; step < steps; step++) {
      final int[] neighbors = adj[cur];
      if (neighbors.length == 0)
        break;
      cur = neighbors[rnd.nextInt(neighbors.length)];
      walk[walkLen++] = cur;
    }

    // Build RID list from walk (one ArrayList allocation)
    final List<RID> rids = new ArrayList<>(walkLen);
    for (int i = 0; i < walkLen; i++)
      rids.add(vertices.get(walk[i]).getIdentity());

    final Map<String, Object> path = buildPath(rids, db);
    final ResultInternal result = new ResultInternal();
    result.setProperty("path", path);
    result.setProperty("steps", walkLen - 1);
    return Stream.of(result);
  }
}
