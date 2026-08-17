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
 * Procedure: algo.fastrp([config])
 *
 * <p>Fast Random Projection (FastRP) node embedding algorithm. Generates dense node
 * embeddings by propagating sparse random projections through the graph topology.
 * Each iteration mixes the current node embedding with a weighted average of neighbour
 * embeddings, followed by L2 normalisation. This produces embeddings that capture
 * multi-hop structural proximity without any training phase.</p>
 *
 * <p>Config map parameters (all optional):
 * <ul>
 *   <li>{@code dimensions} (int, default 128, max {@value AbstractAlgoProcedure#MAX_EMBEDDING_DIMENSION}) – embedding vector size</li>
 *   <li>{@code iterations} (int, default 4) – propagation depth</li>
 *   <li>{@code normalization} (double, default 0.0) – degree-normalization exponent α:
 *       weight of neighbour j contributing to node i is proportional to
 *       deg(i)^{-α} * deg(j)^{-α}; 0 = no normalisation, 1 = GCN-style</li>
 *   <li>{@code selfInfluence} (double, default 0.0) – weight [0,1] given to the
 *       node's own previous embedding vs. the aggregated neighbour embedding</li>
 *   <li>{@code relTypes} (String, default all) – comma-separated edge type names</li>
 *   <li>{@code direction} (String, default BOTH) – {@code IN}, {@code OUT} or {@code BOTH}</li>
 *   <li>{@code seed} (long, default -1) – random seed; -1 = random</li>
 * </ul>
 * </p>
 *
 * <p>Example:
 * <pre>
 * CALL algo.fastrp({dimensions: 64, iterations: 3})
 * YIELD node, embedding
 * RETURN node.name, embedding
 * </pre>
 * </p>
 *
 * <p>Working set: two {@code nodeCount x dimensions} matrices, reserved through
 * {@link AbstractAlgoProcedure.MemoryBudget} before either is allocated.</p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoFastRP extends AbstractAlgoProcedure {

  @Override
  public String getName() {
    return "algo.fastrp";
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
    return "Generates dense node embeddings using Fast Random Projection by propagating sparse random projections through the graph topology";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "embedding");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<String, Object> config = args.length > 0 ? extractMap(args[0], "config") : null;
    final int dimensions = config != null && config.get("dimensions") instanceof Number n ? extractEmbeddingDimension(n, "dimensions") : 128;
    final int iterations = config != null && config.get("iterations") instanceof Number n ? extractInt(n, "iterations", 1) : 4;
    final double normStrength = config != null && config.get("normalization") instanceof Number n ? n.doubleValue() : 0.0;
    final double selfInfluence = config != null && config.get("selfInfluence") instanceof Number n ? n.doubleValue() : 0.0;
    final long seed = config != null && config.get("seed") instanceof Number n ? n.longValue() : -1L;
    final String[] relTypes = config != null ? extractRelTypes(config.get("relTypes")) : null;
    final Vertex.DIRECTION dir = parseDirection(config != null ? (String) config.get("direction") : null);

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);
    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    // The two nodeCount x dimensions matrices below are the whole working set of this procedure, and the only
    // allocation of any size it makes: it has no walk buffer, so nothing else would ever price them. `dimensions`
    // is capped at MAX_EMBEDDING_DIMENSION, which bounds one embedding ROW at 32 KB and says nothing about the
    // matrix - at the default of 128 the pair costs about 2 KB per node, 2 GB at a million nodes. The node count
    // and the knob are all the estimate needs, so the reservation comes before the adjacency lists are
    // materialised: a call that cannot afford its matrices should not first pay the O(edges) build.
    newMemoryBudget(db).reserve(saturatingProduct(2L, matrixBytes(n, dimensions, DOUBLE_BYTES)),
        "the embedding matrices", "2 matrices of " + n + " nodes x dimensions=" + dimensions);

    final int[][] adj = graph.adjacency(dir, relTypes);

    final int[] degree = new int[n];
    for (int i = 0; i < n; i++)
      degree[i] = adj[i].length;

    // Sparse ternary initialisation: +√3 with p=1/6, -√3 with p=1/6, 0 with p=2/3
    // (Achlioptas 2003 — optimal sparse random projection)
    final Random rng = seed >= 0 ? new Random(seed) : new Random();
    final double val = Math.sqrt(3.0);
    final double[][] embed = new double[n][dimensions];
    for (int i = 0; i < n; i++) {
      for (int d = 0; d < dimensions; d++) {
        final int r = rng.nextInt(6);
        if (r == 0)
          embed[i][d] = val;
        else if (r == 1)
          embed[i][d] = -val;
        // else 0 (4/6 probability)
      }
      normalizeL2(embed[i]);
    }

    // Iterative neighbourhood propagation
    final double[][] newEmbed = new double[n][dimensions];
    for (int iter = 0; iter < iterations; iter++) {
      // iterations is a caller-supplied knob and this kernel has no convergence test at all, so it always runs the
      // full count: the guard is the only thing that can end a run the caller no longer wants.
      guard.check();
      for (int i = 0; i < n; i++) {
        // A single propagation walks the whole graph, so on a large one the checkpoint belongs inside it too.
        guard.checkPeriodically(i);
        final int deg = degree[i];
        // Self contribution
        for (int d = 0; d < dimensions; d++)
          newEmbed[i][d] = selfInfluence * embed[i][d];

        if (deg > 0) {
          final double degFactor = normStrength == 0.0 ? 1.0 : Math.pow(deg, normStrength);
          final double nw = (1.0 - selfInfluence) / degFactor;
          for (final int j : adj[i]) {
            final double jFactor = normStrength == 0.0 ? 1.0 : (degree[j] > 0 ? Math.pow(degree[j], normStrength) : 1.0);
            final double w = nw / jFactor;
            for (int d = 0; d < dimensions; d++)
              newEmbed[i][d] += w * embed[j][d];
          }
        } else {
          // Isolated node: keep self embedding
          System.arraycopy(embed[i], 0, newEmbed[i], 0, dimensions);
        }
        normalizeL2(newEmbed[i]);
      }
      // Swap buffers
      for (int i = 0; i < n; i++) {
        guard.checkPeriodically(i);
        System.arraycopy(newEmbed[i], 0, embed[i], 0, dimensions);
      }
    }

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(i));
      r.setProperty("embedding", toEmbeddingList(embed[i]));
      return (Result) r;
    });
  }
}
