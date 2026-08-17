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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;
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
 *   <li>{@code embeddingDimension} (int, default 128, max {@value AbstractAlgoProcedure#MAX_EMBEDDING_DIMENSION}) – embedding size</li>
 *   <li>{@code walkLength} (int, default 80, minimum 1) – steps per random walk</li>
 *   <li>{@code walksPerNode} (int, default 10, minimum 1) – walks generated per node</li>
 *   <li>{@code iterations} (int, default 1, minimum 1) – training epochs over all walks</li>
 *   <li>{@code windowSize} (int, default 10, minimum 1) – Skip-gram context window radius, clamped to
 *       {@code walkLength} since a wider window already spans the whole walk</li>
 *   <li>{@code negSamples} (int, default 5, minimum 0) – negative samples per positive pair</li>
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
 * <p>Working set: a walk matrix of {@code walksPerNode x nodeCount} walks of {@code walkLength} steps AND the
 * two {@code nodeCount x embeddingDimension} matrices of the Skip-gram model, which are alive at the same time
 * and so reserved together through {@link AbstractAlgoProcedure.MemoryBudget} before anything is allocated. The
 * training loops honour thread interruption and {@code arcadedb.command.timeout}.</p>
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
    final int dim =
        config != null && config.get("embeddingDimension") instanceof Number n ? extractEmbeddingDimension(n, "embeddingDimension") : 128;
    final int walkLen = config != null && config.get("walkLength") instanceof Number n ? extractInt(n, "walkLength", 1) : 80;
    final int walksPerNode =
        config != null && config.get("walksPerNode") instanceof Number n ? extractInt(n, "walksPerNode", 1) : 10;
    final int epochs = config != null && config.get("iterations") instanceof Number n ? extractInt(n, "iterations", 1) : 1;
    final int rawWindow = config != null && config.get("windowSize") instanceof Number n ? extractInt(n, "windowSize", 1) : 10;
    final int negSamples = config != null && config.get("negSamples") instanceof Number n ? extractInt(n, "negSamples", 0) : 5;
    final double lr0 = config != null && config.get("learningRate") instanceof Number n ? n.doubleValue() : 0.025;
    final double p = config != null && config.get("p") instanceof Number n ? n.doubleValue() : 1.0;
    final double q = config != null && config.get("q") instanceof Number n ? n.doubleValue() : 1.0;
    final long seed = config != null && config.get("seed") instanceof Number n ? n.longValue() : -1L;
    final String[] relTypes = config != null ? extractRelTypes(config.get("relTypes")) : null;
    final Vertex.DIRECTION dir = parseDirection(config != null ? (String) config.get("direction") : null);

    final Database db = context.getDatabase();
    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    // Both reservations are made off the node count and the knobs alone, so they come before the adjacency
    // lists are materialised: a call that cannot afford its buffers should not first pay the O(edges) build.
    final MemoryBudget memory = newMemoryBudget(db);
    // Sized in long arithmetic: `n * walksPerNode` wraps int for a large walksPerNode, which used to size the
    // walk matrix with a negative or (for an exact multiple of 2^32) far too small a value.
    final long totalWalksAsLong = saturatingProduct(n, walksPerNode);
    // Per walk: one matrix row of walkLength ints, plus one entry of the walkOrder shuffle array. Saturating
    // throughout, like the estimate itself: a footprint that mixes a saturated product with a plain addition
    // wraps to a negative number, and a negative estimate passes the budget check unconditionally. walkLen is
    // int-bounded so this one cannot reach that today - it is written this way so the shape is the same at
    // every call site.
    final long bytesPerWalk = saturatingSum(MATRIX_ROW_OVERHEAD_BYTES + INT_BYTES,
        saturatingProduct(INT_BYTES, walkLen));
    memory.reserve(saturatingProduct(totalWalksAsLong, bytesPerWalk), "the random walk buffer",
        "walksPerNode=" + walksPerNode + " x walkLength=" + walkLen + " over " + n + " nodes");
    // The two nodeCount x embeddingDimension matrices of phase 2 are reserved here too, because both stay alive
    // to the end of the call alongside the walk matrix and because a run that cannot afford them should not
    // first spend minutes generating walks. The dimension cap bounds one embedding ROW at 32 KB and says
    // nothing about the matrix, which at the default dimension of 128 costs about 2 KB per node - the same
    // order as the walk buffer priced above.
    memory.reserve(saturatingProduct(2L, matrixBytes(n, dim, DOUBLE_BYTES)), "the embedding matrices",
        "2 matrices of " + n + " nodes x embeddingDimension=" + dim);
    if (totalWalksAsLong > Integer.MAX_VALUE)
      throw new IllegalArgumentException(getName() + "(): walksPerNode=" + walksPerNode + " over " + n + " nodes needs "
          + totalWalksAsLong + " walks, more than the " + Integer.MAX_VALUE + " entries a Java array can hold");

    final int[][] adj = graph.adjacency(dir, relTypes);

    final Random rng = seed >= 0 ? new Random(seed) : new Random();
    final WorkGuard guard = newWorkGuard(context);

    // A context window wider than the walk already spans the whole walk, so clamping it changes no result.
    // Without the clamp `pos + window` wraps int for a large windowSize, leaving winEnd below winStart: the
    // Skip-gram inner loop then never runs and the procedure quietly returns untrained embeddings.
    // The ceiling is walkLen and not walkLen - 1 on purpose: winStart and winEnd are clamped to
    // [0, walkLen - 1] anyway, so the tighter bound buys nothing and would send windowSize to 0 at
    // walkLength 1, below the minimum of 1 the extraction above enforces.
    final int window = Math.min(rawWindow, walkLen);

    // ── Phase 1: Generate biased random walks ──────────────────────────────
    final int totalWalks = (int) totalWalksAsLong;
    final int[][] walks = new int[totalWalks][walkLen];
    int wi = 0;
    for (int v = 0; v < n; v++) {
      for (int w = 0; w < walksPerNode; w++) {
        guard.check();
        final int[] walk = walks[wi++];
        walk[0] = v;
        if (walkLen == 1 || adj[v].length == 0) {
          Arrays.fill(walk, v);
          continue;
        }
        // First step: uniform random neighbour
        walk[1] = adj[v][rng.nextInt(adj[v].length)];
        for (int step = 2; step < walkLen; step++) {
          // One walk is walkLength steps and walkLength has no ceiling of its own, so the checkpoint belongs
          // inside the walk too, not only between walks.
          guard.checkPeriodically(step);
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
    // W: input embeddings, WCtx: context embeddings. Both were reserved before phase 1 started.
    final double[][] W = new double[n][dim];
    final double[][] WCtx = new double[n][dim];
    // Xavier initialisation for W.
    // Every remaining O(nodeCount) / O(totalWalks) loop below carries a checkpoint too. "Bounded by the walk
    // budget" is not a time bound for any of them: the budget is tunable and accepts a negative value meaning
    // no limit, in which case totalWalks is capped only by Integer.MAX_VALUE - the same reasoning that put a
    // checkpoint in algo.randomWalk's step loop.
    final double scale = 1.0 / Math.sqrt(dim);
    for (int i = 0; i < n; i++) {
      guard.checkPeriodically(i);
      for (int d = 0; d < dim; d++)
        W[i][d] = (rng.nextDouble() * 2.0 - 1.0) * scale;
    }
    // WCtx initialised to zero (standard word2vec)

    // Walk index array for shuffling
    final int[] walkOrder = new int[totalWalks];
    for (int i = 0; i < totalWalks; i++) {
      guard.checkPeriodically(i);
      walkOrder[i] = i;
    }

    final double[] grad = new double[dim]; // accumulated gradient for center node
    for (int epoch = 0; epoch < epochs; epoch++) {
      // Shuffle walk order
      for (int i = totalWalks - 1; i > 0; i--) {
        guard.checkPeriodically(i);
        final int j = rng.nextInt(i + 1);
        final int tmp = walkOrder[i];
        walkOrder[i] = walkOrder[j];
        walkOrder[j] = tmp;
      }
      // Linearly decaying learning rate
      final double lr = lr0 * (1.0 - (double) epoch / epochs);

      for (final int walkIdx : walkOrder) {
        guard.check();
        final int[] walk = walks[walkIdx];
        for (int pos = 0; pos < walkLen; pos++) {
          final int center = walk[pos];
          Arrays.fill(grad, 0.0);

          final int winStart = Math.max(0, pos - window);
          // In long arithmetic: `window` is clamped to `walkLen`, but `walkLen` itself is bounded only by the
          // walk-memory budget, so an operator who raises that far enough could still make `pos + window` wrap.
          final int winEnd = (int) Math.min(walkLen - 1L, pos + (long) window);
          for (int ctx = winStart; ctx <= winEnd; ctx++) {
            // The context loop, not the walk loop, is what bounds abort latency here: a window as wide as the
            // walk makes a single walk O(walkLength x walkLength), so a checkpoint between walks could be
            // hours apart.
            guard.checkPeriodically(ctx);
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
              // negSamples is the innermost knob of all, and the only one with neither a heap ceiling nor a
              // maximum: one (position, context) pair costs negSamples x dim. The enclosing checkpoint runs
              // before the sampling starts, so without one here a single pair is unabortable however long it
              // takes - the same "checkpoint outside the unbounded loop" shape closed above for windowSize.
              // Note ns restarts at 0 per pair, so this also tests once per pair and not only every 1024
              // samples. Deliberate: one flag test per pair is what keeps the default negSamples of 5
              // responsive, since the ctx checkpoint above fires only about once every 1024 positions when
              // the window is narrow.
              guard.checkPeriodically(ns);
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
    for (int i = 0; i < n; i++)
      normalizeL2(W[i]);

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(i));
      r.setProperty("embedding", toEmbeddingList(W[i]));
      return (Result) r;
    });
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
