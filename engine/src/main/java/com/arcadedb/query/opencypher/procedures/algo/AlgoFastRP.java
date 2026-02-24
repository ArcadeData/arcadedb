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
 *   <li>{@code dimensions} (int, default 128) – embedding vector size</li>
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
    final int dimensions = config != null && config.get("dimensions") instanceof Number n ? n.intValue() : 128;
    final int iterations = config != null && config.get("iterations") instanceof Number n ? n.intValue() : 4;
    final double normStrength = config != null && config.get("normalization") instanceof Number n ? n.doubleValue() : 0.0;
    final double selfInfluence = config != null && config.get("selfInfluence") instanceof Number n ? n.doubleValue() : 0.0;
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
      for (int i = 0; i < n; i++) {
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
      for (int i = 0; i < n; i++)
        System.arraycopy(newEmbed[i], 0, embed[i], 0, dimensions);
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
