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

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.hashgnn([config])
 *
 * <p>HashGNN: a training-free graph neural network that uses locality-sensitive hashing
 * to aggregate neighbourhood information (Hamilton 2020, extended). Each node is initialised
 * with a sparse random binary feature vector derived from its structural identity. For each
 * propagation round the node's feature set is OR-combined with those of its neighbours, then
 * reduced to a fixed-size MinHash sketch. The final embedding is the L2-normalised MinHash
 * signature concatenated across all propagation rounds.</p>
 *
 * <p>Config map parameters (all optional):
 * <ul>
 *   <li>{@code embeddingDimension} (int, default 128, max {@value AbstractAlgoProcedure#MAX_EMBEDDING_DIMENSION}) – output embedding size</li>
 *   <li>{@code iterations} (int, default 4) – number of message-passing rounds</li>
 *   <li>{@code relTypes} (String, default all) – comma-separated edge type names</li>
 *   <li>{@code direction} (String, default BOTH)</li>
 *   <li>{@code seed} (long, default -1) – random seed; -1 = random</li>
 * </ul>
 * </p>
 *
 * <p>Example:
 * <pre>
 * CALL algo.hashgnn({embeddingDimension: 64, iterations: 3})
 * YIELD node, embedding
 * RETURN node.name, embedding
 * </pre>
 * </p>
 *
 * <p>Working set: two {@code nodeCount x (4 x embeddingDimension)} feature matrices plus one
 * {@code nodeCount x embeddingDimension} embedding matrix - the feature pair being the larger of the two -
 * reserved through {@link AbstractAlgoProcedure.MemoryBudget} before the first is allocated.</p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoHashGNN extends AbstractAlgoProcedure {

  @Override
  public String getName() {
    return "algo.hashgnn";
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
    return "Generates training-free node embeddings using locality-sensitive hashing with MinHash sketches aggregated over neighbourhood feature sets";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "embedding");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<String, Object> config = args.length > 0 ? extractMap(args[0], "config") : null;
    final int embDim = config != null && config.get("embeddingDimension") instanceof Number n ? extractEmbeddingDimension(n, "embeddingDimension") : 128;
    final int iterations = config != null && config.get("iterations") instanceof Number n ? extractInt(n, "iterations", 1) : 4;
    final long seed = config != null && config.get("seed") instanceof Number n ? n.longValue() : -1L;
    final String[] relTypes = config != null ? extractRelTypes(config.get("relTypes")) : null;
    final Vertex.DIRECTION dir = parseDirection(config != null ? (String) config.get("direction") : null);

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);
    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    // Use 4× over-parameterised bit space for good hash quality
    final int numFeatures = Math.max(embDim * 4, 64);

    // Three nodeCount-scaled matrices live at once here, and the two feature matrices - not the embedding one -
    // are the larger pair: they are four times as wide as the embedding, so even as booleans they cost half a
    // byte per embedding dimension per node against the embedding's eight. The embeddingDimension cap bounds a
    // row, not a matrix; at the default of 128 the three together cost about 2 KB per node. The node count and
    // the knob are all the estimates need, so they are reserved before the adjacency lists are materialised: a
    // call that cannot afford its matrices should not first pay the O(edges) build.
    final MemoryBudget memory = graph.memory();
    memory.reserve(saturatingProduct(2L, matrixBytes(n, numFeatures, BOOLEAN_BYTES)), "the feature matrices",
        "2 matrices of " + n + " nodes x " + numFeatures + " features (embeddingDimension=" + embDim + " x 4)");
    memory.reserve(matrixBytes(n, embDim, DOUBLE_BYTES), "the embedding matrix",
        n + " nodes x embeddingDimension=" + embDim);

    final int[][] adj = graph.adjacency(dir, relTypes);
    final Random rng = seed >= 0 ? new Random(seed) : new Random();

    // Initialise: each node gets ~12.5% sparse random binary feature vector
    final boolean[][] features = new boolean[n][numFeatures];
    for (int i = 0; i < n; i++) {
      // Rejection sampling over numFeatures bits, once per node. Bounded per node, unbounded over the graph -
      // and it runs BEFORE the message-passing loop, so the checkpoint inside that loop never sees it.
      guard.checkPeriodically(i);
      // Use per-node deterministic seed for reproducible initialisation
      final Random nodeRng = seed >= 0 ? new Random(seed + i * 1_000_003L) :
          new Random(graph.getRID(i).hashCode() * 1_000_003L + i);
      int set = 0;
      final int target = numFeatures / 8; // 12.5% density
      while (set < target) {
        final int bit = nodeRng.nextInt(numFeatures);
        if (!features[i][bit]) {
          features[i][bit] = true;
          set++;
        }
      }
    }

    // Pre-compute MinHash hash function parameters: h_d(x) = (a*x + b) mod numFeatures
    // Using two arrays of coprime pairs (a[d], b[d])
    final int[] hashA = new int[embDim];
    final int[] hashB = new int[embDim];
    for (int d = 0; d < embDim; d++) {
      // Ensure a is odd (to guarantee full-period LCG-style coverage)
      hashA[d] = rng.nextInt(numFeatures / 2) * 2 + 1;
      hashB[d] = rng.nextInt(numFeatures);
    }

    // Iterative message passing: OR-combine neighbour features, then MinHash-reduce
    final boolean[][] newFeatures = new boolean[n][numFeatures];
    for (int iter = 0; iter < iterations; iter++) {
      // iterations is a caller-supplied knob and this kernel has no convergence test at all, so it always runs the
      // full count: the guard is the only thing that can end a run the caller no longer wants.
      guard.check();
      for (int i = 0; i < n; i++) {
        // A single message-passing round walks the whole graph, so on a large one the checkpoint belongs inside it.
        guard.checkPeriodically(i);
        System.arraycopy(features[i], 0, newFeatures[i], 0, numFeatures);
        for (final int j : adj[i]) {
          for (int f = 0; f < numFeatures; f++)
            newFeatures[i][f] |= features[j][f];
        }
      }
      // Swap
      for (int i = 0; i < n; i++) {
        guard.checkPeriodically(i);
        System.arraycopy(newFeatures[i], 0, features[i], 0, numFeatures);
      }
    }

    // Compute MinHash signature → float embedding.
    //
    // This phase, not the loop `iterations` drives, is where an algo.hashgnn call actually spends its time, and
    // until issue #6295 it was the one phase no checkpoint reached. Per node it costs embeddingDimension x
    // numFeatures = 4 x embeddingDimension² operations - 6.7e7 at the 4096 cap - and that bounded per-node figure
    // is then multiplied by an unbounded node count. Measured: 113 seconds against a 1000 ms
    // arcadedb.command.timeout on 2000 nodes, and the call RETURNED rather than aborting.
    //
    // The lens that missed it looked for knobs with no ceiling; embeddingDimension has one (#6065), which is
    // exactly why it looked handled. The question that finds it is "which loop over the node count has no
    // checkpoint", and the phases after the knob-driven loop are where to ask it.
    //
    // The counter spans both loops rather than sitting on either: on a wide embedding one node is already 6.7e7
    // operations, so a per-node checkpoint would be too coarse, and on a narrow one (embeddingDimension 1, 64
    // features) a per-dimension clock read would cost more than the work it guards. One check per 1024
    // (node, dimension) pairs is bounded by numFeatures x 1024 operations either way.
    final double[][] embeddings = new double[n][embDim];
    int reductionStep = 0;
    for (int i = 0; i < n; i++) {
      for (int d = 0; d < embDim; d++) {
        guard.checkPeriodically(reductionStep++);
        int minHash = Integer.MAX_VALUE;
        final int a = hashA[d], b = hashB[d];
        for (int f = 0; f < numFeatures; f++) {
          if (features[i][f]) {
            final int h = Math.floorMod(a * f + b, numFeatures);
            if (h < minHash)
              minHash = h;
          }
        }
        embeddings[i][d] = minHash == Integer.MAX_VALUE ? 0.0 : (double) minHash / numFeatures;
      }
      normalizeL2(embeddings[i]);
    }

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(i));
      r.setProperty("embedding", toEmbeddingList(embeddings[i]));
      return (Result) r;
    });
  }
}
