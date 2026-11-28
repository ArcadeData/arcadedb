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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;
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
 *   <li>weightProperty (string, default null): edge property to use as weight; every weight must be finite and
 *       non-negative, or the call is refused</li>
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
    final String weightProperty = configString(config, "weightProperty");

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    // Read once, through a Graph Analytical View when one covers the graph (issue #12169): the edge records used to be
    // read again by every pass and by every modularity computation
    final GraphData graph = loadGraph(db, null, null, context);
    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    final GraphData.WeightedAdjacency adjacency = graph.weightedAdjacency(guard, Vertex.DIRECTION.BOTH, weightProperty);
    // one NaN or infinite weight made the degrees, the total weight and so the modularity NaN (issue #12957)
    if (weightProperty != null)
      requireFiniteNonNegativeWeights(adjacency, weightProperty, guard);

    // doubles: weighted degree, community total degree, candidate weights and the two modularity accumulators; ints:
    // the community and the candidate list (reused for the renumbering); the touched flags
    graph.memory().reserve(saturatingProduct(n, 5 * DOUBLE_BYTES + 2 * INT_BYTES + BOOLEAN_BYTES), "the community working set",
        n + " nodes");

    final Communities communities = detect(adjacency.neighbors(), adjacency.weights(), maxIterations, tolerance, guard);
    final int[] communityId = communities.community();
    final double finalModularity = communities.modularity();

    return IntStream.range(0, n).mapToObj(i -> {
      // a node id a view keeps for a deleted vertex is no vertex: it is isolated, and it has no row
      final RID rid = graph.getRID(i);
      if (rid == null)
        return null;
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", rid);
      result.setProperty("communityId", communityId[i]);
      result.setProperty("modularity", finalModularity);
      return (Result) result;
    }).filter(Objects::nonNull);
  }

  /**
   * The community of each node, numbered from 0 in order of first appearance, and the modularity of the partition at the
   * resolution the passes optimised (the standard modularity at resolution 1).
   */
  record Communities(int[] community, double modularity) {
  }

  /** {@link #detect(int[][], double[][], int, double, double, WorkGuard)} at the standard resolution of 1. */
  static Communities detect(final int[][] neighbors, final double[][] weights, final int maxIterations, final double tolerance,
      final WorkGuard guard) {
    return detect(neighbors, weights, maxIterations, tolerance, 1.0, guard);
  }

  /**
   * Runs the local-moving passes over an undirected weighted adjacency ({@code BOTH} rows, every edge listed once in the
   * row of each of its ends, a self-loop twice in its node's own row). In each pass every node, in index order, is taken out
   * of its community and put into the community of highest gain {@code w(i, c) - resolution * k(i) * Σtot(c) / 2m}, over its
   * neighbouring communities and the one it left (whose {@code Σtot} no longer holds the node). It stays unless another
   * community gains strictly more, and among equal gains the lowest community id wins. The passes run until one moves
   * nothing, the modularity moves by less than {@code tolerance}, or {@code maxIterations} passes ran.
   * <p>
   * The gain of staying is part of the comparison: a node used to move to any neighbouring community whose gain was positive,
   * which is true of almost every candidate, so nodes left communities they were as well off in and two 4-cliques joined by
   * one edge ended as four communities.
   * <p>
   * O(E) per pass (issue #12169): the total degree of each community is kept in an array, updated as a node leaves one
   * community for another, where it used to be summed over every node for every candidate community of every node, and
   * the weights towards the candidate communities are gathered in arrays indexed by community and cleared through the
   * list of those touched, where a map was allocated per node.
   */
  static Communities detect(final int[][] neighbors, final double[][] weights, final int maxIterations, final double tolerance,
      final double resolution, final WorkGuard guard) {
    final int n = neighbors.length;
    final double[] nodeDegree = new double[n];
    double totalWeight = 0.0;
    int work = 0; // adjacency entries walked: a vertex of high degree is many of them, so the checkpoint counts these
    for (int i = 0; i < n; i++) {
      for (final double w : weights[i]) {
        guard.checkPeriodically(work++);
        nodeDegree[i] += w;
      }
      totalWeight += nodeDegree[i];
    }
    totalWeight /= 2.0; // every edge is listed from both of its ends

    final int[] community = new int[n];
    final double[] communityDegree = new double[n];
    for (int i = 0; i < n; i++) {
      community[i] = i;
      communityDegree[i] = nodeDegree[i];
    }

    final double[] scratchA = new double[n];
    final double[] scratchB = new double[n];
    double prevModularity = modularity(neighbors, weights, community, nodeDegree, totalWeight, resolution, scratchA, scratchB, guard);

    final double[] candidateWeight = new double[n]; // all zero between two nodes
    final boolean[] touched = new boolean[n];
    final int[] candidates = new int[n];

    for (int iter = 0; iter < maxIterations; iter++) {
      // maxIterations is a caller-supplied knob and the "nothing moved" break only fires if the graph settles, so
      // the outer loop carries the checkpoint. One iteration walks every edge, plus a modularity pass.
      guard.check();
      boolean changed = false;

      for (int i = 0; i < n; i++) {
        guard.checkPeriodically(i);
        final int currentCommunity = community[i];
        final int[] row = neighbors[i];
        final double[] rowWeights = weights[i];

        int candidateCount = 0;
        for (int k = 0; k < row.length; k++) {
          guard.checkPeriodically(++work);
          if (row[k] == i)
            continue; // a self-loop travels with the node: it is inside every community the node can be in
          final int c = community[row[k]];
          if (!touched[c]) {
            touched[c] = true;
            candidates[candidateCount++] = c;
          }
          candidateWeight[c] += rowWeights[k];
        }

        // the node is taken out of its community first: staying is a candidate like the others, with the community's degree
        // sum as it is without the node
        final double twoM = 2.0 * totalWeight;
        int bestCommunity = currentCommunity;
        double bestGain = candidateWeight[currentCommunity]
            - resolution * nodeDegree[i] * (communityDegree[currentCommunity] - nodeDegree[i]) / twoM;
        for (int k = 0; k < candidateCount; k++) {
          guard.checkPeriodically(++work);
          final int c = candidates[k];
          if (c != currentCommunity) {
            final double gain = candidateWeight[c] - resolution * nodeDegree[i] * communityDegree[c] / twoM;
            if (gain > bestGain || (gain == bestGain && bestCommunity != currentCommunity && c < bestCommunity)) {
              bestGain = gain;
              bestCommunity = c;
            }
          }
          candidateWeight[c] = 0.0;
          touched[c] = false;
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

      final double newModularity = modularity(neighbors, weights, community, nodeDegree, totalWeight, resolution, scratchA, scratchB, guard);
      if (Math.abs(newModularity - prevModularity) < tolerance)
        break;
      prevModularity = newModularity;
    }

    final double finalModularity = modularity(neighbors, weights, community, nodeDegree, totalWeight, resolution, scratchA, scratchB, guard);

    // Number the communities from 0 in order of first appearance (community ids are node indexes)
    final int[] remap = candidates; // reused
    Arrays.fill(remap, -1);
    int nextId = 0;
    for (int i = 0; i < n; i++) {
      final int c = community[i];
      if (remap[c] < 0)
        remap[c] = nextId++;
      community[i] = remap[c];
    }
    return new Communities(community, finalModularity);
  }

  /**
   * Q = sum_c [ L_c/m - resolution * (d_c/(2m))^2 ] with L_c the weight of the edges inside community c and d_c the sum of
   * the weighted degrees of its nodes. Community ids are node indexes (0..n-1). Every edge is listed from both of its ends,
   * so the internal weights summed over the rows are twice L_c. The two scratch arrays are cleared first. The resolution is
   * the one the local moves optimise, so that the tolerance check measures the objective the passes improve: at any other
   * resolution a pass can change it by less than the tolerance while it still has moves to make.
   */
  private static double modularity(final int[][] neighbors, final double[][] weights, final int[] community,
      final double[] nodeDegree, final double totalWeight, final double resolution, final double[] internalWeight,
      final double[] communityDegree, final WorkGuard guard) {
    if (totalWeight == 0.0)
      return 0.0;

    final int n = neighbors.length;
    Arrays.fill(internalWeight, 0.0);
    Arrays.fill(communityDegree, 0.0);
    int work = 0;
    for (int i = 0; i < n; i++) {
      final int c = community[i];
      communityDegree[c] += nodeDegree[i];
      final int[] row = neighbors[i];
      final double[] rowWeights = weights[i];
      for (int k = 0; k < row.length; k++) {
        guard.checkPeriodically(++work);
        if (community[row[k]] == c)
          internalWeight[c] += rowWeights[k];
      }
    }

    double modularity = 0.0;
    for (int c = 0; c < n; c++) {
      final double share = communityDegree[c] / (2.0 * totalWeight);
      modularity += internalWeight[c] / 2.0 / totalWeight - resolution * share * share;
    }
    return modularity;
  }
}
