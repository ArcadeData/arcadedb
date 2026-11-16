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
import com.arcadedb.query.sql.executor.WorkGuard;

import com.arcadedb.utility.IntIntHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Procedure: algo.louvain([config])
 * <p>
 * Detects communities using the Louvain method, a modularity-based hierarchical community
 * detection algorithm. It iteratively optimizes local modularity to find communities at
 * multiple scales. Widely used for social network analysis, biological network clustering,
 * and customer segmentation.
 * </p>
 * <p>
 * Config map parameters (all optional):
 * <ul>
 *   <li>maxIterations (int, default 10): maximum number of optimization iterations</li>
 *   <li>tolerance (double, default 0.0001): minimum modularity improvement to continue</li>
 *   <li>weightProperty (string, default null): edge property to use as weight</li>
 * </ul>
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL algo.louvain({maxIterations: 10})
 * YIELD node, communityId, modularity
 * RETURN communityId, count(*) AS size ORDER BY size DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoLouvain extends AbstractAlgoProcedure {
  public static final String NAME = "algo.louvain";

  /** The keys the config map may hold: any other is refused, so a misspelt one cannot fall back to its default (issue #11145). */
  private static final Set<String> CONFIG_KEYS = Set.of("maxIterations", "tolerance", "weightProperty");

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
    return "Detects communities using the Louvain modularity optimization algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "communityId", "modularity");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<String, Object> config = extractConfig(argOrDefault(args, 0, null), "config", CONFIG_KEYS);
    final int maxIterations = config != null && config.get("maxIterations") instanceof Number n ?
        extractInt(n, "maxIterations", 1) : 10;
    final double tolerance = config != null && config.get("tolerance") instanceof Number n ?
        n.doubleValue() : 0.0001;
    final String weightProperty = config != null ? (String) config.get("weightProperty") : null;

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);
    // Every vertex and every edge type, read once: through a Graph Analytical View when one covers the graph, from the
    // records otherwise (issue #12169).
    final GraphData graph = loadGraph(db, null, null, context);
    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    final GraphData.WeightedAdjacency weighted = graph.weightedAdjacency(guard, Vertex.DIRECTION.BOTH, weightProperty);
    final int[][] adj = weighted.neighbors();
    final double[][] adjWeights = weighted.weights();

    // Initialize: each node in its own community
    final int[] community = new int[n];
    for (int i = 0; i < n; i++)
      community[i] = i;

    // Precompute total edge weight for modularity calculation
    double totalWeight = 0.0;
    final double[] nodeDegree = new double[n];
    for (int i = 0; i < n; i++) {
      for (final double w : adjWeights[i]) {
        nodeDegree[i] += w;
        totalWeight += w;
      }
    }
    totalWeight /= 2.0; // Each edge counted twice

    // The weighted degree of every community, kept up to date as vertices move. Reading it here instead of summing
    // the degrees of the community's members over every vertex is what makes a pass O(E) rather than O(E x V)
    // (issue #12169). Community ids are vertex indexes, so community i starts with vertex i alone.
    graph.memory().reserve(saturatingProduct(3L * n, DOUBLE_BYTES), "the community degrees and modularity buffers",
        n + " nodes");
    final double[] communityDegree = nodeDegree.clone();
    final double[] scratchDegree = new double[n];
    final double[] scratchInternal = new double[n];

    double prevModularity = computeModularity(adj, adjWeights, community, nodeDegree, totalWeight, scratchDegree,
        scratchInternal);

    // Phase 1: Modularity optimization
    for (int iter = 0; iter < maxIterations; iter++) {
      // maxIterations is a caller-supplied knob and the "nothing moved" break only fires if the graph settles, so
      // the outer loop carries the checkpoint. One iteration walks every edge, plus a modularity pass.
      guard.check();
      boolean changed = false;

      for (int i = 0; i < n; i++) {
        guard.checkPeriodically(i);
        final int currentCommunity = community[i];

        // Compute neighbor community weights. A HashMap, as when they were read off the edge records: the order the
        // candidates are tried in decides which of two equal gains wins.
        final int[] neighbors = adj[i];
        final double[] weights = adjWeights[i];
        final Map<Integer, Double> neighborCommunityWeight = new HashMap<>();
        for (int k = 0; k < neighbors.length; k++)
          neighborCommunityWeight.merge(community[neighbors[k]], weights[k], Double::sum);

        // Find best community to move to
        int bestCommunity = currentCommunity;
        double bestGain = 0.0;

        for (final Map.Entry<Integer, Double> entry : neighborCommunityWeight.entrySet()) {
          final int candidateCommunity = entry.getKey();
          if (candidateCommunity == currentCommunity)
            continue;

          // Modularity gain = 2 * w_ic - k_i * sum_c / m
          final double gain = entry.getValue() - nodeDegree[i] * communityDegree[candidateCommunity] / (2.0 * totalWeight);

          if (gain > bestGain) {
            bestGain = gain;
            bestCommunity = candidateCommunity;
          }
        }

        if (bestCommunity != currentCommunity) {
          communityDegree[currentCommunity] -= nodeDegree[i];
          communityDegree[bestCommunity] += nodeDegree[i];
          community[i] = bestCommunity;
          changed = true;
        }
      }

      if (!changed)
        break;

      final double newModularity = computeModularity(adj, adjWeights, community, nodeDegree, totalWeight, scratchDegree,
          scratchInternal);
      if (Math.abs(newModularity - prevModularity) < tolerance)
        break;
      prevModularity = newModularity;
    }

    final double finalModularity = computeModularity(adj, adjWeights, community, nodeDegree, totalWeight, scratchDegree,
        scratchInternal);

    // Remap community IDs to be sequential starting from 0. A view keeps the slot of a vertex deleted since it was
    // built as a hole with no RID and no edges: it is neither reported nor numbered.
    final IntIntHashMap communityRemap = new IntIntHashMap();
    final List<Result> results = new ArrayList<>(n);
    int nextId = 0;
    for (int i = 0; i < n; i++) {
      final RID rid = graph.getRID(i);
      if (rid == null)
        continue;
      if (!communityRemap.containsKey(community[i]))
        communityRemap.put(community[i], nextId++);
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", rid);
      result.setProperty("communityId", communityRemap.get(community[i], -1));
      result.setProperty("modularity", finalModularity);
      results.add(result);
    }
    return results.stream();
  }

  /**
   * Q = sum_c [ L_c/m - (d_c/(2m))^2 ] with L_c the weight of the edges inside community c and d_c the sum of the
   * weighted degrees of its vertices. Community ids are vertex indexes (0..n-1). Every edge is listed at both of its
   * ends, so the weight met inside community c is 2 x L_c. d_c is summed again here, vertex by vertex, rather than
   * taken from the running totals of the moves, so the reported modularity does not carry their rounding.
   */
  private static double computeModularity(final int[][] adj, final double[][] adjWeights, final int[] community,
      final double[] nodeDegree, final double totalWeight, final double[] communityDegree, final double[] internalWeight) {
    if (totalWeight == 0.0)
      return 0.0;

    final int n = adj.length;
    Arrays.fill(communityDegree, 0.0);
    Arrays.fill(internalWeight, 0.0);
    for (int i = 0; i < n; i++) {
      final int c = community[i];
      communityDegree[c] += nodeDegree[i];
      final int[] neighbors = adj[i];
      final double[] weights = adjWeights[i];
      for (int k = 0; k < neighbors.length; k++)
        if (community[neighbors[k]] == c)
          internalWeight[c] += weights[k];
    }

    double modularity = 0.0;
    for (int c = 0; c < n; c++) {
      final double share = communityDegree[c] / (2.0 * totalWeight);
      modularity += internalWeight[c] / (2.0 * totalWeight) - share * share;
    }
    return modularity;
  }
}
