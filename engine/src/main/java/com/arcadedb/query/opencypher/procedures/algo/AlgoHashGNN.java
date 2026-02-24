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
 *   <li>{@code embeddingDimension} (int, default 128) – output embedding size</li>
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
    final int embDim = config != null && config.get("embeddingDimension") instanceof Number n ? n.intValue() : 128;
    final int iterations = config != null && config.get("iterations") instanceof Number n ? n.intValue() : 4;
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

    // Use 4× over-parameterised bit space for good hash quality
    final int numFeatures = Math.max(embDim * 4, 64);
    final Random rng = seed >= 0 ? new Random(seed) : new Random();

    // Initialise: each node gets ~12.5% sparse random binary feature vector
    final boolean[][] features = new boolean[n][numFeatures];
    for (int i = 0; i < n; i++) {
      // Use per-node deterministic seed for reproducible initialisation
      final Random nodeRng = seed >= 0 ? new Random(seed + i * 1_000_003L) :
          new Random(vertices.get(i).getIdentity().hashCode() * 1_000_003L + i);
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
      hashA[d] = (rng.nextInt(numFeatures / 2) * 2 + 1);
      hashB[d] = rng.nextInt(numFeatures);
    }

    // Iterative message passing: OR-combine neighbour features, then MinHash-reduce
    final boolean[][] newFeatures = new boolean[n][numFeatures];
    for (int iter = 0; iter < iterations; iter++) {
      for (int i = 0; i < n; i++) {
        System.arraycopy(features[i], 0, newFeatures[i], 0, numFeatures);
        for (final int j : adj[i]) {
          for (int f = 0; f < numFeatures; f++)
            newFeatures[i][f] |= features[j][f];
        }
      }
      // Swap
      for (int i = 0; i < n; i++)
        System.arraycopy(newFeatures[i], 0, features[i], 0, numFeatures);
    }

    // Compute MinHash signature → float embedding
    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final double[] embed = new double[embDim];
      for (int d = 0; d < embDim; d++) {
        int minHash = Integer.MAX_VALUE;
        final int a = hashA[d], b = hashB[d];
        for (int f = 0; f < numFeatures; f++) {
          if (features[i][f]) {
            final int h = Math.floorMod(a * f + b, numFeatures);
            if (h < minHash)
              minHash = h;
          }
        }
        embed[d] = minHash == Integer.MAX_VALUE ? 0.0 : (double) minHash / numFeatures;
      }
      normalizeL2(embed);

      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("embedding", toEmbeddingList(embed));
      results.add(r);
    }
    return results.stream();
  }
}
