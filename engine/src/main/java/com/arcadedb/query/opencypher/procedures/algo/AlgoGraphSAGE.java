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
 * Procedure: algo.graphsage([config])
 *
 * <p>GraphSAGE (Hamilton et al., 2017) – inductive neighbourhood aggregation embedding.
 * This implementation is <em>unsupervised</em>: no training labels are required.
 * Node features are initialised from structural properties (log-degree + Gaussian noise)
 * and propagated through {@code layers} mean-aggregation rounds, each followed by a random
 * linear projection and ReLU activation. The resulting embeddings capture multi-hop
 * structural similarity and can be used directly for downstream tasks.</p>
 *
 * <p>Config map parameters (all optional):
 * <ul>
 *   <li>{@code embeddingDimension} (int, default 64) – output embedding size per layer</li>
 *   <li>{@code layers} (int, default 2) – number of aggregation layers</li>
 *   <li>{@code relTypes} (String, default all) – comma-separated edge type names</li>
 *   <li>{@code direction} (String, default BOTH)</li>
 *   <li>{@code seed} (long, default -1) – random seed; -1 = random</li>
 * </ul>
 * </p>
 *
 * <p>Example:
 * <pre>
 * CALL algo.graphsage({embeddingDimension: 32, layers: 2})
 * YIELD node, embedding
 * RETURN node.name, embedding
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoGraphSAGE extends AbstractAlgoProcedure {

  @Override
  public String getName() {
    return "algo.graphsage";
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
    return "Computes inductive node embeddings using unsupervised GraphSAGE mean-aggregation with random linear projection and ReLU activation";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "embedding");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<String, Object> config = args.length > 0 ? extractMap(args[0], "config") : null;
    final int outDim = config != null && config.get("embeddingDimension") instanceof Number n ? n.intValue() : 64;
    final int layers = config != null && config.get("layers") instanceof Number n ? n.intValue() : 2;
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

    final int[] degree = new int[n];
    int maxDeg = 0;
    for (int i = 0; i < n; i++) {
      degree[i] = adj[i].length;
      if (degree[i] > maxDeg)
        maxDeg = degree[i];
    }
    final double logMaxDeg = maxDeg > 0 ? Math.log1p(maxDeg) : 1.0;

    // Initial node features: [log-normalised degree, Gaussian noise × (inDim-1)]
    final int initDim = Math.max(outDim, 16);
    final Random rng = seed >= 0 ? new Random(seed) : new Random();
    double[][] embed = new double[n][initDim];
    for (int i = 0; i < n; i++) {
      embed[i][0] = Math.log1p(degree[i]) / logMaxDeg; // structural feature
      for (int d = 1; d < initDim; d++)
        embed[i][d] = rng.nextGaussian() * 0.1;
      normalizeL2(embed[i]);
    }

    int curDim = initDim;

    // Layer-wise aggregation
    for (int layer = 0; layer < layers; layer++) {
      // Random projection: (2*curDim) → outDim with Xavier initialisation
      final int concatDim = curDim * 2;
      final double projScale = Math.sqrt(2.0 / (concatDim + outDim));
      final double[][] W = new double[outDim][concatDim];
      for (int o = 0; o < outDim; o++)
        for (int j = 0; j < concatDim; j++)
          W[o][j] = rng.nextGaussian() * projScale;

      final double[][] nextEmbed = new double[n][outDim];
      final double[] agg = new double[curDim];
      final double[] concat = new double[concatDim];

      for (int i = 0; i < n; i++) {
        // Mean aggregation over neighbours
        final int deg = degree[i];
        Arrays.fill(agg, 0.0);
        if (deg > 0) {
          for (final int j : adj[i])
            for (int d = 0; d < curDim; d++)
              agg[d] += embed[j][d];
          for (int d = 0; d < curDim; d++)
            agg[d] /= deg;
        } else {
          System.arraycopy(embed[i], 0, agg, 0, curDim);
        }

        // Concatenate self + aggregated
        System.arraycopy(embed[i], 0, concat, 0, curDim);
        System.arraycopy(agg, 0, concat, curDim, curDim);

        // Linear projection + ReLU
        for (int o = 0; o < outDim; o++) {
          double sum = 0.0;
          final double[] wo = W[o];
          for (int j = 0; j < concatDim; j++)
            sum += wo[j] * concat[j];
          nextEmbed[i][o] = Math.max(0.0, sum);
        }
        normalizeL2(nextEmbed[i]);
      }

      embed = nextEmbed;
      curDim = outDim;
    }

    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("embedding", toEmbeddingList(embed[i]));
      results.add(r);
    }
    return results.stream();
  }
}
