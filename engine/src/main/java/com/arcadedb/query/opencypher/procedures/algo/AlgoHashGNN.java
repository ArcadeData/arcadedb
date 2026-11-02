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
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.hashgnn([config])
 *
 * <p>HashGNN: a training-free graph neural network that replaces the learned aggregation of a message-passing
 * network with MinHash sampling (Wu et al., "Hashing-Accelerated Graph Neural Networks for Link Prediction", 2021).
 * Each node draws {@code embeddingDensity} samples from the nodes around it, and nodes whose neighbourhoods overlap draw
 * the same samples: a node and its neighbours share many, two nodes far apart share none.</p>
 *
 * <p>Each sample slot {@code k} is a consistent weighted sample (an exponential race). Every node {@code s} draws one
 * race time {@code E_k(s) ~ Exp(1)} from a hash shared by the whole graph, and node {@code u} keeps, in slot
 * {@code k}, the node with the smallest race time scaled by its distance from {@code u}: one round of message passing
 * lets a node take its neighbours' best times multiplied by {@code degree(u) / neighborInfluence}, its own time staying
 * as it is. So the node itself weighs 1, its neighbours together weigh {@code neighborInfluence}, at any degree, and
 * every further hop weighs {@code neighborInfluence} again of the one before. Because the hash is shared, two nodes
 * keep the same sample in a slot with a probability that grows with how much of their weighted neighbourhoods they
 * share. The embedding hashes every sample into one of {@code embeddingDimension} positions and is the L2-normalised
 * binary vector of the positions hit: each is {@code 1 / sqrt(positionCount)}, the others 0.</p>
 *
 * <p>Three alternatives fail, and the shape above is what is left once they are ruled out (issue #11144):
 * <ul>
 *   <li>OR-combining feature sets every round and MinHash-reducing once at the end: nothing bounds the density, a few
 *   rounds over a few dozen nodes set every feature, and every node of the region gets the same vector - all zeros
 *   once every feature is set and {@code (a * f + b) mod numFeatures} runs over its whole period.</li>
 *   <li>Random features of the embedding space as the sampled items: a neighbourhood's union covers most of that
 *   space, so the features that hash lowest are picked everywhere and unrelated nodes look alike. Node identities are
 *   as many as the nodes, so disjoint neighbourhoods cannot pick the same sample.</li>
 *   <li>Unweighted MinHash per round, with fresh hash functions each round: the samples coalesce like a voter model,
 *   and on a small dense graph a few rounds leave every node with the same vector again. With the same hash functions
 *   each round it becomes the MinHash of the {@code iterations}-hop ball, identical for every node once the ball
 *   covers the graph. The per-hop weight is what keeps a node's own neighbourhood ahead of the rest of the graph.</li>
 * </ul>
 * </p>
 *
 * <p>Config map parameters (all optional):
 * <ul>
 *   <li>{@code embeddingDimension} (int, default 128, max {@value AbstractAlgoProcedure#MAX_EMBEDDING_DIMENSION}) – output embedding size</li>
 *   <li>{@code embeddingDensity} (int, default {@code max(1, embeddingDimension / 8)}, at most {@code embeddingDimension}) –
 *   samples per node, so the most non-zero entries an embedding can have</li>
 *   <li>{@code iterations} (int, default 4) – number of message-passing rounds, the farthest hop a sample can come from</li>
 *   <li>{@code neighborInfluence} (double, default 1.0, at least 0) – weight of a node's neighbours, all together,
 *   against the node itself; lower keeps the embedding local, higher reaches farther</li>
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
 * <p>Working set: two {@code nodeCount x embeddingDensity} sample matrices and two race-time matrices of the same
 * shape, reserved through {@link AbstractAlgoProcedure.MemoryBudget} before the first is allocated. The embedding rows
 * are built one at a time as the result is consumed, so no {@code nodeCount x embeddingDimension} matrix exists.</p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoHashGNN extends AbstractAlgoProcedure {
  /** The keys the config map may hold: any other is refused, so a misspelt one cannot fall back to its default (issue #11145). */
  private static final Set<String> CONFIG_KEYS = Set.of("direction", "embeddingDensity", "embeddingDimension", "iterations",
      "neighborInfluence", "relTypes", "seed");
  private static final long        GOLDEN_GAMMA = 0x9E3779B97F4A7C15L;

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
    return "Generates training-free node embeddings from consistent weighted MinHash samples of each node's neighbourhood";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "embedding");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<String, Object> config = extractConfig(argOrDefault(args, 0, null), "config", CONFIG_KEYS);
    final int embDim = config != null && config.get("embeddingDimension") instanceof Number n ? extractEmbeddingDimension(n, "embeddingDimension") : 128;
    final int density = config != null && config.get("embeddingDensity") instanceof Number n ? extractInt(n, "embeddingDensity", 1) : Math.max(1, embDim / 8);
    if (density > embDim)
      throw new IllegalArgumentException(
          getName() + "(): embeddingDensity must not exceed embeddingDimension (" + embDim + "), got " + density);
    final int iterations = config != null && config.get("iterations") instanceof Number n ? extractInt(n, "iterations", 1) : 4;
    final double neighborInfluence = config != null && config.get("neighborInfluence") instanceof Number n ? n.doubleValue() : 1.0;
    if (!Double.isFinite(neighborInfluence) || neighborInfluence < 0.0)
      throw new IllegalArgumentException(
          getName() + "(): neighborInfluence must be a finite number of at least 0, got " + neighborInfluence);
    final long seed = config != null && config.get("seed") instanceof Number n ? n.longValue() : -1L;
    final String[] relTypes = config != null ? extractRelTypes(config.get("relTypes"), context) : null;
    final Vertex.DIRECTION dir = parseDirection(config != null ? (String) config.get("direction") : null);

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);
    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    // The node count and the knobs are all the estimates need, so they are reserved before the adjacency lists are
    // materialised: a call that cannot afford its matrices should not first pay the O(edges) build.
    final MemoryBudget memory = graph.memory();
    memory.reserve(saturatingProduct(2L, matrixBytes(n, density, INT_BYTES)), "the sample matrices",
        "2 matrices of " + n + " nodes x embeddingDensity=" + density);
    memory.reserve(saturatingProduct(2L, matrixBytes(n, density, DOUBLE_BYTES)), "the race-time matrices",
        "2 matrices of " + n + " nodes x embeddingDensity=" + density);

    final int[][] adj = graph.adjacency(dir, relTypes);
    final Random rng = seed >= 0 ? new Random(seed) : new Random();
    final long positionKey = rng.nextLong();
    final long[] slotKeys = new long[density];
    for (int k = 0; k < density; k++)
      slotKeys[k] = rng.nextLong();

    // Every node starts as its own sample in every slot, with its race time for that slot: -ln(1 - U), U uniform in
    // [0, 1) from 53 bits of the slot's hash, so the time is Exp(1) and the minimum over a set falls on any member
    // with probability proportional to its weight. StrictMath rather than Math: Math.log1p may be an intrinsic whose
    // last bit differs between JVMs and CPUs, and a seeded run has to answer the same embeddings everywhere. Bounded
    // per node, unbounded over the graph, and it runs before the message-passing loop, so the checkpoint inside that
    // loop never sees it.
    int[][] samples = new int[n][density];
    double[][] races = new double[n][density];
    int step = 0;
    for (int i = 0; i < n; i++) {
      final int[] sampleRow = samples[i];
      final double[] raceRow = races[i];
      for (int k = 0; k < density; k++) {
        guard.checkPeriodically(step++);
        sampleRow[k] = i;
        raceRow[k] = -StrictMath.log1p(-((mix(slotKeys[k] + i * GOLDEN_GAMMA) >>> 11) * 0x1.0p-53));
      }
    }

    // Round r answers, per slot, the best race over the r-hop ball: min(own best, neighbours' best x hop factor). It
    // reads round r-1 and writes a second pair of matrices, so a round moves a sample exactly one hop whatever order
    // the nodes are visited in: in place, a sample could cross several hops in one round, depending on the load order.
    int[][] nextSamples = new int[n][density];
    double[][] nextRaces = new double[n][density];
    for (int iter = 0; iter < iterations; iter++) {
      // iterations is a caller-supplied knob and this kernel has no convergence test at all, so it always runs the
      // full count: the guard is the only thing that can end a run the caller no longer wants.
      guard.check();
      step = 0;
      for (int i = 0; i < n; i++) {
        // The counter runs per neighbour visited rather than per node: a supernode walks its whole adjacency at once.
        guard.checkPeriodically(step++);
        final int[] bestSample = nextSamples[i];
        final double[] bestRace = nextRaces[i];
        System.arraycopy(samples[i], 0, bestSample, 0, density);
        System.arraycopy(races[i], 0, bestRace, 0, density);
        // Dividing the neighbours' weight by the degree is what makes neighborInfluence their weight ALL TOGETHER: on
        // a hub the neighbours would otherwise drown the node itself, and every hop multiplies that over again.
        final double hopFactor = adj[i].length / neighborInfluence;
        for (final int j : adj[i]) {
          guard.checkPeriodically(step++);
          final int[] neighbourSamples = samples[j];
          final double[] neighbourRaces = races[j];
          for (int k = 0; k < density; k++) {
            final double race = neighbourRaces[k] * hopFactor;
            if (race < bestRace[k]) {
              bestRace[k] = race;
              bestSample[k] = neighbourSamples[k];
            }
          }
        }
      }
      final int[][] swapSamples = samples;
      samples = nextSamples;
      nextSamples = swapSamples;
      final double[][] swapRaces = races;
      races = nextRaces;
      nextRaces = swapRaces;
    }

    final int[][] finalSamples = samples;
    return IntStream.range(0, n).mapToObj(i -> {
      final double[] embedding = new double[embDim];
      // A sample drawn in two slots, or two samples hashed to one position, hit one position: between 1 and `density`.
      int hit = 0;
      for (final int s : finalSamples[i]) {
        final int position = (int) (((mix(positionKey + s * GOLDEN_GAMMA) >>> 32) * embDim) >>> 32);
        if (embedding[position] == 0.0) {
          embedding[position] = 1.0;
          hit++;
        }
      }
      final double value = 1.0 / Math.sqrt(hit);
      for (int d = 0; d < embDim; d++)
        if (embedding[d] != 0.0)
          embedding[d] = value;

      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(i));
      r.setProperty("embedding", toEmbeddingList(embedding));
      return (Result) r;
    });
  }

  /** The SplitMix64 finaliser: a bijective 64-bit mixer, so distinct inputs never collide. */
  private static long mix(final long value) {
    final long first = (value ^ (value >>> 30)) * 0xBF58476D1CE4E5B9L;
    final long second = (first ^ (first >>> 27)) * 0x94D049BB133111EBL;
    return second ^ (second >>> 31);
  }
}
