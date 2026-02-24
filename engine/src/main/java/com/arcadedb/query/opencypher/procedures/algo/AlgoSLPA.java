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
import java.util.Random;
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
        num.intValue() : 20;
    final double threshold = config != null && config.get("threshold") instanceof Number num ?
        num.doubleValue() : 0.1;
    final long seedVal = config != null && config.get("seed") instanceof Number num ?
        num.longValue() : -1L;
    final Random rng = seedVal < 0 ? new Random() : new Random(seedVal);

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.BOTH, null);

    // Memory: memory[v] is a list of labels heard by v (including its initial label)
    // Using int[] lists backed by arrays for performance
    final int[][] memory     = new int[n][];
    final int[]   memorySize = new int[n];

    // Each node starts with a unique label equal to its index
    for (int i = 0; i < n; i++) {
      memory[i] = new int[iterations + 1];
      memory[i][0] = i;
      memorySize[i] = 1;
    }

    // Propagation rounds
    final int[] order = new int[n];
    for (int i = 0; i < n; i++)
      order[i] = i;

    for (int t = 0; t < iterations; t++) {
      // Shuffle node order each round
      for (int i = n - 1; i > 0; i--) {
        final int j = rng.nextInt(i + 1);
        final int tmp = order[i]; order[i] = order[j]; order[j] = tmp;
      }

      for (final int listener : order) {
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
    final List<Result> results = new ArrayList<>(n);
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

      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("communities", communities);
      results.add(r);
    }
    return results.stream();
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
