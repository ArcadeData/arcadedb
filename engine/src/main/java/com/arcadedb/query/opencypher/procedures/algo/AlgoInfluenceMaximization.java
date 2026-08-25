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
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.List;
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
 * <p>
 * {@code k} is clamped to the node count and rejected when negative. {@code simulations} must be at least 1
 * (at 0 the Monte Carlo average divides by zero and the procedure silently returns nothing); above that it
 * multiplies CPU work with no graph-derived ceiling, so instead of a guessed cap the cascade loop honours
 * thread interruption and the {@code arcadedb.command.timeout} deadline.
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

    // k saturates on purpose ("more seeds than nodes" reads as "as many as exist" and is clamped to n below),
    // but a NEGATIVE k has no such reading: unclamped it reaches `new int[seedCount]` as a bare
    // NegativeArraySizeException that never names the parameter. extractCount() is exactly those two halves,
    // added for algo.knn / algo.kShortestPaths by #6065.
    final int k = args[0] instanceof Number n ? extractCount(n, "k") : 1;
    final String[] relTypes = args.length > 1 ? extractRelTypes(args[1]) : null;
    final int simulations = args.length > 2 && args[2] instanceof Number n ? extractInt(n, "simulations", 1) : 100;
    final double propagationProbability = args.length > 3 && args[3] instanceof Number n ? n.doubleValue() : 0.1;

    final Database db = context.getDatabase();

    final GraphData graph = loadGraph(db, null, relTypes, context);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.OUT, relTypes);

    final int seedCount = Math.min(k, n);
    final boolean[] isSeed = new boolean[n];
    final int[] seeds = new int[seedCount];
    final Random rng = new Random(42L);
    final WorkGuard guard = newWorkGuard(context);

    // Hoisted out of simulateIC() and reused across every Monte Carlo call (k x n x simulations of them).
    // simulateIC() resets only the entries it touched, turning what used to be an O(n) allocate-and-zero
    // per call into an O(activated) reset (issue #6265).
    final boolean[] activated = new boolean[n];
    final int[]     queue     = new int[n];

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
        for (int sim = 0; sim < simulations; sim++) {
          // Deliberately the unthrottled check(), not checkPeriodically(), unlike node2vec's and maxKCut's
          // inner loops. guard.check() costs one flag test when no timeout is configured, so paying it once
          // per simulation is cheap even now that simulateIC's buffers are hoisted to the caller and a
          // simulation on a sparse graph with a low propagationProbability can be close to O(1) (issue
          // #6265). Throttling to checkPeriodically(1024) would instead make abort latency
          // 1024 x O(n + m) cascades, worst on exactly the large graphs where the deadline matters most.
          guard.check();
          totalSpread += simulateIC(adj, seeds, round, candidate, n, propagationProbability, rng, activated, queue);
        }
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
      r.setProperty("nodeId", graph.getRID(bestNode));
      r.setProperty("rank", round + 1);
      r.setProperty("marginalGain", marginalGain);
      results.add(r);
    }

    return results.stream();
  }

  /**
   * Simulates one IC cascade from the current seed set plus a candidate node.
   * Returns the number of activated nodes.
   * <p>
   * {@code activated} and {@code queue} are owned by the caller and reused across every simulation
   * (issue #6265): both are guaranteed all-{@code false}/scratch on entry and are restored to that state
   * before returning, by resetting only the entries this call touched (recorded in {@code queue[0..tail)})
   * rather than reallocating and zero-filling {@code n}-sized arrays on every call.
   */
  private int simulateIC(final int[][] adj, final int[] seeds, final int seedCount,
      final int candidate, final int n, final double p, final Random rng,
      final boolean[] activated, final int[] queue) {
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

    // Reset only the touched entries so the shared buffers are clean for the next call (issue #6265).
    for (int i = 0; i < tail; i++)
      activated[queue[i]] = false;

    return count;
  }
}
