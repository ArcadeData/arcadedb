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
 * Procedure: algo.node2vec([config])
 *
 * <p>Node2Vec node embedding algorithm (Grover &amp; Leskovec, 2016).
 * Generates dense embeddings by combining biased second-order random walks
 * with a Skip-gram model trained via negative sampling.</p>
 *
 * <p>Config map parameters (all optional):
 * <ul>
 *   <li>{@code embeddingDimension} (int, default 128) – embedding size</li>
 *   <li>{@code walkLength} (int, default 80) – steps per random walk</li>
 *   <li>{@code walksPerNode} (int, default 10) – walks generated per node</li>
 *   <li>{@code iterations} (int, default 1) – training epochs over all walks</li>
 *   <li>{@code windowSize} (int, default 10) – Skip-gram context window radius</li>
 *   <li>{@code negSamples} (int, default 5) – negative samples per positive pair</li>
 *   <li>{@code learningRate} (double, default 0.025) – initial SGD learning rate</li>
 *   <li>{@code p} (double, default 1.0) – return parameter: high p → less return</li>
 *   <li>{@code q} (double, default 1.0) – in-out parameter: low q → DFS-like,
 *       high q → BFS-like</li>
 *   <li>{@code relTypes} (String, default all) – comma-separated edge type names</li>
 *   <li>{@code direction} (String, default BOTH)</li>
 *   <li>{@code seed} (long, default -1) – random seed; -1 = random</li>
 * </ul>
 * </p>
 *
 * <p>Example:
 * <pre>
 * CALL algo.node2vec({embeddingDimension: 64, walkLength: 20, walksPerNode: 5})
 * YIELD node, embedding
 * RETURN node.name, embedding
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoNode2Vec extends AbstractAlgoProcedure {

  @Override
  public String getName() {
    return "algo.node2vec";
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
    return "Generates node embeddings using Node2Vec biased random walks combined with a Skip-gram model trained via negative sampling";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "embedding");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<String, Object> config = args.length > 0 ? extractMap(args[0], "config") : null;
    final int dim = config != null && config.get("embeddingDimension") instanceof Number n ? n.intValue() : 128;
    final int walkLen = config != null && config.get("walkLength") instanceof Number n ? n.intValue() : 80;
    final int walksPerNode = config != null && config.get("walksPerNode") instanceof Number n ? n.intValue() : 10;
    final int epochs = config != null && config.get("iterations") instanceof Number n ? n.intValue() : 1;
    final int window = config != null && config.get("windowSize") instanceof Number n ? n.intValue() : 10;
    final int negSamples = config != null && config.get("negSamples") instanceof Number n ? n.intValue() : 5;
    final double lr0 = config != null && config.get("learningRate") instanceof Number n ? n.doubleValue() : 0.025;
    final double p = config != null && config.get("p") instanceof Number n ? n.doubleValue() : 1.0;
    final double q = config != null && config.get("q") instanceof Number n ? n.doubleValue() : 1.0;
    final long seed = config != null && config.get("seed") instanceof Number n ? n.longValue() : -1L;
    final String[] relTypes = config != null ? extractRelTypes(config.get("relTypes")) : null;
    final Vertex.DIRECTION dir = parseDirection(config != null ? (String) config.get("direction") : null);

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> it = getAllVertices(db, null);
    while (it.hasNext())
      vertices.add(it.next());
    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, dir, relTypes);

    final Random rng = seed >= 0 ? new Random(seed) : new Random();

    // ── Phase 1: Generate biased random walks ──────────────────────────────
    final int totalWalks = n * walksPerNode;
    final int[][] walks = new int[totalWalks][walkLen];
    int wi = 0;
    for (int v = 0; v < n; v++) {
      for (int w = 0; w < walksPerNode; w++) {
        final int[] walk = walks[wi++];
        walk[0] = v;
        if (walkLen == 1 || adj[v].length == 0) {
          Arrays.fill(walk, v);
          continue;
        }
        // First step: uniform random neighbour
        walk[1] = adj[v][rng.nextInt(adj[v].length)];
        for (int step = 2; step < walkLen; step++) {
          final int prev = walk[step - 2];
          final int curr = walk[step - 1];
          if (adj[curr].length == 0) {
            walk[step] = curr;
          } else {
            walk[step] = nextBiasedStep(prev, curr, adj, p, q, rng);
          }
        }
      }
    }

    // ── Phase 2: Skip-gram with negative sampling ──────────────────────────
    // W: input embeddings, WCtx: context embeddings
    final double[][] W = new double[n][dim];
    final double[][] WCtx = new double[n][dim];
    // Xavier initialisation for W
    final double scale = 1.0 / Math.sqrt(dim);
    for (int i = 0; i < n; i++)
      for (int d = 0; d < dim; d++)
        W[i][d] = (rng.nextDouble() * 2.0 - 1.0) * scale;
    // WCtx initialised to zero (standard word2vec)

    // Walk index array for shuffling
    final int[] walkOrder = new int[totalWalks];
    for (int i = 0; i < totalWalks; i++)
      walkOrder[i] = i;

    final double[] grad = new double[dim]; // accumulated gradient for center node
    for (int epoch = 0; epoch < epochs; epoch++) {
      // Shuffle walk order
      for (int i = totalWalks - 1; i > 0; i--) {
        final int j = rng.nextInt(i + 1);
        final int tmp = walkOrder[i];
        walkOrder[i] = walkOrder[j];
        walkOrder[j] = tmp;
      }
      // Linearly decaying learning rate
      final double lr = lr0 * (1.0 - (double) epoch / epochs);

      for (final int walkIdx : walkOrder) {
        final int[] walk = walks[walkIdx];
        for (int pos = 0; pos < walkLen; pos++) {
          final int center = walk[pos];
          Arrays.fill(grad, 0.0);

          final int winStart = Math.max(0, pos - window);
          final int winEnd = Math.min(walkLen - 1, pos + window);
          for (int ctx = winStart; ctx <= winEnd; ctx++) {
            if (ctx == pos)
              continue;
            final int ctxNode = walk[ctx];

            // Positive sample: target = 1
            final double posScore = dot(W[center], WCtx[ctxNode]);
            final double posGrad = lr * (1.0 - sigmoid(posScore));
            for (int d = 0; d < dim; d++) {
              grad[d] += posGrad * WCtx[ctxNode][d];
              WCtx[ctxNode][d] += posGrad * W[center][d];
            }

            // Negative samples: target = 0
            for (int ns = 0; ns < negSamples; ns++) {
              int neg = rng.nextInt(n);
              if (neg == center || neg == ctxNode)
                neg = (neg + 1) % n;
              final double negScore = dot(W[center], WCtx[neg]);
              final double negGrad = -lr * sigmoid(negScore);
              for (int d = 0; d < dim; d++) {
                grad[d] += negGrad * WCtx[neg][d];
                WCtx[neg][d] += negGrad * W[center][d];
              }
            }
          }
          // Apply accumulated gradient to center
          for (int d = 0; d < dim; d++)
            W[center][d] += grad[d];
        }
      }
    }

    // Normalise and return input embeddings
    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      normalizeL2(W[i]);
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("embedding", toEmbeddingList(W[i]));
      results.add(r);
    }
    return results.stream();
  }

  /**
   * Samples the next node in the biased random walk from {@code curr}
   * (given we came from {@code prev}).
   */
  private static int nextBiasedStep(final int prev, final int curr, final int[][] adj,
      final double p, final double q, final Random rng) {
    final int[] neighbors = adj[curr];
    final int deg = neighbors.length;

    // Compute unnormalised weights
    final double[] weights = new double[deg];
    double total = 0.0;
    for (int i = 0; i < deg; i++) {
      final int x = neighbors[i];
      double w;
      if (x == prev)
        w = 1.0 / p;               // return to previous node
      else if (isNeighbor(adj[prev], x))
        w = 1.0;                    // triangle edge (BFS-like)
      else
        w = 1.0 / q;               // move away (DFS-like)
      weights[i] = w;
      total += w;
    }

    // Cumulative sampling
    double r = rng.nextDouble() * total;
    for (int i = 0; i < deg - 1; i++) {
      r -= weights[i];
      if (r <= 0)
        return neighbors[i];
    }
    return neighbors[deg - 1];
  }

  /** Linear scan to check neighbour membership (acceptable for typical graph degrees). */
  private static boolean isNeighbor(final int[] adjRow, final int target) {
    for (final int x : adjRow)
      if (x == target)
        return true;
    return false;
  }
}
