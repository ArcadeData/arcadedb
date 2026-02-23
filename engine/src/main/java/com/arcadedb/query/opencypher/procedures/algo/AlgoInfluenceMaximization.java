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
import java.util.Random;
import java.util.stream.Stream;

/**
 * Procedure: algo.influenceMaximization(k, relTypes?, simulations?, propagationProbability?)
 * <p>
 * Finds k seed nodes that maximize influence spread using a greedy algorithm with the
 * Independent Cascade (IC) model. In the IC model, each activated node u tries to activate
 * each inactive neighbor v with probability p. Greedy selection with Monte Carlo simulation
 * is used to estimate expected spread.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.influenceMaximization(3, 'KNOWS', 100, 0.1)
 * YIELD nodeId, rank, marginalGain
 * RETURN nodeId, rank, marginalGain ORDER BY rank ASC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoInfluenceMaximization extends AbstractAlgoProcedure {
  public static final String NAME = "algo.influenceMaximization";

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
    return 4;
  }

  @Override
  public String getDescription() {
    return "Finds k seed nodes maximizing influence spread via Independent Cascade model";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodeId", "rank", "marginalGain");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final int k = args[0] instanceof Number n ? n.intValue() : 1;
    final String[] relTypes = args.length > 1 ? extractRelTypes(args[1]) : null;
    final int simulations = args.length > 2 && args[2] instanceof Number n ? n.intValue() : 100;
    final double propagationProbability = args.length > 3 && args[3] instanceof Number n ? n.doubleValue() : 0.1;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.OUT, relTypes);

    final int seedCount = Math.min(k, n);
    final boolean[] isSeed = new boolean[n];
    final int[] seeds = new int[seedCount];
    final Random rng = new Random(42L);

    final List<Result> results = new ArrayList<>(seedCount);
    double prevSpread = 0.0;

    for (int round = 0; round < seedCount; round++) {
      int bestNode = -1;
      double bestSpread = -1.0;

      for (int candidate = 0; candidate < n; candidate++) {
        if (isSeed[candidate])
          continue;

        // Simulate IC spread from seeds ∪ {candidate}
        double totalSpread = 0.0;
        for (int sim = 0; sim < simulations; sim++)
          totalSpread += simulateIC(adj, seeds, round, candidate, n, propagationProbability, rng);
        final double avgSpread = totalSpread / simulations;

        if (avgSpread > bestSpread) {
          bestSpread = avgSpread;
          bestNode = candidate;
        }
      }

      if (bestNode < 0)
        break;

      isSeed[bestNode] = true;
      seeds[round] = bestNode;
      final double marginalGain = bestSpread - prevSpread;
      prevSpread = bestSpread;

      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", vertices.get(bestNode).getIdentity());
      r.setProperty("rank", round + 1);
      r.setProperty("marginalGain", marginalGain);
      results.add(r);
    }

    return results.stream();
  }

  /**
   * Simulates one IC cascade from the current seed set plus a candidate node.
   * Returns the number of activated nodes.
   */
  private int simulateIC(final int[][] adj, final int[] seeds, final int seedCount,
      final int candidate, final int n, final double p, final Random rng) {
    final boolean[] activated = new boolean[n];
    // BFS queue (int[] with head/tail pointers — zero boxing)
    final int[] queue = new int[n];
    int head = 0;
    int tail = 0;

    // Activate seeds
    for (int i = 0; i < seedCount; i++) {
      if (!activated[seeds[i]]) {
        activated[seeds[i]] = true;
        queue[tail++] = seeds[i];
      }
    }
    // Activate candidate
    if (!activated[candidate]) {
      activated[candidate] = true;
      queue[tail++] = candidate;
    }

    int count = tail;

    while (head < tail) {
      final int u = queue[head++];
      for (final int v : adj[u]) {
        if (!activated[v] && rng.nextDouble() < p) {
          activated[v] = true;
          queue[tail++] = v;
          count++;
        }
      }
    }

    return count;
  }
}
