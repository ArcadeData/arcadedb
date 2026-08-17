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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.slpa([config])
 * <p>
 * Speaker-Listener Label Propagation Algorithm (SLPA) for overlapping community detection
 * (Xie et al., 2011). Unlike plain Label Propagation, each node maintains a memory of all
 * labels it has heard over time; at the end, communities are derived by thresholding the
 * label-frequency distribution.
 * </p>
 * <p>
 * Config map parameters (all optional):
 * <ul>
 *   <li>iterations (int, default 20): number of propagation rounds</li>
 *   <li>threshold (double, default 0.1): minimum relative frequency for a label to be kept</li>
 *   <li>seed (long, default -1): random seed for reproducibility (-1 = random)</li>
 * </ul>
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.slpa({iterations: 20, threshold: 0.1})
 * YIELD node, communities
 * RETURN node.name, communities
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoSLPA extends AbstractAlgoProcedure {
  public static final String NAME = "algo.slpa";

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
    return "Speaker-Listener Label Propagation Algorithm (SLPA) for overlapping community detection";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "communities");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<String, Object> config = args.length > 0 ? extractMap(args[0], "config") : null;

    final int iterations = config != null && config.get("iterations") instanceof Number num ?
        extractInt(num, "iterations", 1) : 20;
    final double threshold = config != null && config.get("threshold") instanceof Number num ?
        num.doubleValue() : 0.1;
    final long seedVal = config != null && config.get("seed") instanceof Number num ?
        num.longValue() : -1L;
    final Random rng = seedVal < 0 ? new Random() : new Random(seedVal);

    final String[] relTypes = config != null ? extractRelTypes(config.get("relTypes")) : null;

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    final GraphData graph = loadGraph(db, null, relTypes, context);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    // Memory: memory[v] is a list of labels heard by v (including its initial label)
    // Using int[] lists backed by arrays for performance.
    //
    // Unlike the other iteration knobs, SLPA's `iterations` buys heap as well as time: every node keeps one row of
    // `iterations + 1` ints, so the matrix is nodeCount x (iterations + 1) and a value that merely looks large -
    // {iterations: 1000000} on a 10k-node graph is 40 GB - reaches the allocator with nothing between it and the
    // heap. The footprint is reserved against the call's working-memory budget, in saturating long arithmetic,
    // BEFORE the first row is allocated; `iterations + 1` is computed in long because at
    // Integer.MAX_VALUE the int form wraps to Integer.MIN_VALUE and died as a bare NegativeArraySizeException.
    final long rowCapacity = iterations + 1L;
    newMemoryBudget(db).reserve(matrixBytes(n, rowCapacity, INT_BYTES), "the label memory",
        "iterations=" + iterations + " over " + n + " nodes");
    if (rowCapacity > Integer.MAX_VALUE)
      throw new IllegalArgumentException(getName() + "(): iterations=" + iterations + " needs " + rowCapacity
          + " label entries per node, more than the " + Integer.MAX_VALUE + " a Java array can hold");

    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH);

    final int[][] memory     = new int[n][];
    final int[]   memorySize = new int[n];

    // Each node starts with a unique label equal to its index
    for (int i = 0; i < n; i++) {
      memory[i] = new int[(int) rowCapacity];
      memory[i][0] = i;
      memorySize[i] = 1;
    }

    // Propagation rounds
    final int[] order = new int[n];
    for (int i = 0; i < n; i++)
      order[i] = i;

    for (int t = 0; t < iterations; t++) {
      // iterations is a caller-supplied knob and this kernel has no convergence test at all, so it always runs the
      // full count: the guard is the only thing that can end a run the caller no longer wants.
      guard.check();
      // Shuffle node order each round
      for (int i = n - 1; i > 0; i--) {
        guard.checkPeriodically(i);
        final int j = rng.nextInt(i + 1);
        final int tmp = order[i]; order[i] = order[j]; order[j] = tmp;
      }

      for (int idx = 0; idx < n; idx++) {
        // A single round walks the whole graph, so on a large one the checkpoint belongs inside the round too.
        guard.checkPeriodically(idx);
        final int listener = order[idx];
        if (adj[listener].length == 0)
          continue;

        // Each neighbour speaks its most-frequent label (with ties broken randomly)
        final int[] heard = new int[adj[listener].length];
        int heardCount = 0;
        for (final int speaker : adj[listener]) {
          // Speaker picks a random label from its memory (uniform)
          final int pick = memory[speaker][rng.nextInt(memorySize[speaker])];
          heard[heardCount++] = pick;
        }

        // Listener adds the most-frequent heard label to its memory
        final int mostFreq = mostFrequent(heard, heardCount, rng);
        memory[listener][memorySize[listener]++] = mostFreq;
      }
    }

    // Post-processing: keep only labels with relative frequency >= threshold
    // Pre-compute communities for all nodes (pure int/map work, no vertex loading)
    @SuppressWarnings("unchecked")
    final List<Long>[] allCommunities = new List[n];
    for (int i = 0; i < n; i++) {
      final Map<Integer, Integer> freq = new HashMap<>();
      for (int j = 0; j < memorySize[i]; j++) {
        final int label = memory[i][j];
        freq.merge(label, 1, Integer::sum);
      }

      final List<Long> communities = new ArrayList<>();
      for (final Map.Entry<Integer, Integer> e : freq.entrySet()) {
        if ((double) e.getValue() / memorySize[i] >= threshold)
          communities.add((long) e.getKey());
      }
      if (communities.isEmpty())
        communities.add((long) i); // keep at least the initial label
      allCommunities[i] = communities;
    }

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(i));
      r.setProperty("communities", allCommunities[i]);
      return (Result) r;
    });
  }

  /** Returns the most-frequent element in arr[0..len), breaking ties randomly. */
  private int mostFrequent(final int[] arr, final int len, final Random rng) {
    // Frequency map using arrays (avoid HashMap allocation for small len)
    int bestLabel = arr[0], bestCount = 0;
    // Simple O(len^2) scan — len = number of neighbours, typically small
    for (int i = 0; i < len; i++) {
      int count = 0;
      for (int j = 0; j < len; j++)
        if (arr[j] == arr[i]) count++;
      if (count > bestCount || (count == bestCount && rng.nextBoolean())) {
        bestCount = count;
        bestLabel = arr[i];
      }
    }
    return bestLabel;
  }
}
