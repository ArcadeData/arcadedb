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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    final Map<String, Object> config = args.length > 0 ? extractMap(args[0], "config") : null;
    final int maxIterations = config != null && config.get("maxIterations") instanceof Number n ?
        n.intValue() : 10;
    final double tolerance = config != null && config.get("tolerance") instanceof Number n ?
        n.doubleValue() : 0.0001;
    final String weightProperty = config != null ? (String) config.get("weightProperty") : null;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> vertIter = getAllVertices(db, null);
    while (vertIter.hasNext())
      vertices.add(vertIter.next());
    if (vertices.isEmpty())
      return Stream.empty();

    final int n = vertices.size();
    final Map<Vertex, Integer> vertexIndex = new HashMap<>(n);
    for (int i = 0; i < n; i++)
      vertexIndex.put(vertices.get(i), i);

    // Initialize: each node in its own community
    final int[] community = new int[n];
    for (int i = 0; i < n; i++)
      community[i] = i;

    // Precompute total edge weight for modularity calculation
    double totalWeight = 0.0;
    final double[] nodeDegree = new double[n];
    for (int i = 0; i < n; i++) {
      final Vertex v = vertices.get(i);
      for (final Edge edge : v.getEdges(Vertex.DIRECTION.BOTH)) {
        double w = 1.0;
        if (weightProperty != null) {
          final Object wObj = edge.get(weightProperty);
          if (wObj instanceof Number num)
            w = num.doubleValue();
        }
        nodeDegree[i] += w;
        totalWeight += w;
      }
    }
    totalWeight /= 2.0; // Each edge counted twice

    double prevModularity = computeModularity(vertices, community, vertexIndex, nodeDegree, totalWeight, weightProperty);

    // Phase 1: Modularity optimization
    for (int iter = 0; iter < maxIterations; iter++) {
      boolean changed = false;

      for (int i = 0; i < n; i++) {
        final Vertex v = vertices.get(i);
        final int currentCommunity = community[i];

        // Compute neighbor community weights
        final Map<Integer, Double> neighborCommunityWeight = new HashMap<>();
        for (final Edge edge : v.getEdges(Vertex.DIRECTION.BOTH)) {
          final Vertex neighbor = edge.getOut().equals(v.getIdentity()) ?
              edge.getInVertex() : edge.getOutVertex();
          final Integer neighborIdx = vertexIndex.get(neighbor);
          if (neighborIdx == null)
            continue;

          double w = 1.0;
          if (weightProperty != null) {
            final Object wObj = edge.get(weightProperty);
            if (wObj instanceof Number num)
              w = num.doubleValue();
          }
          final int neighborComm = community[neighborIdx];
          neighborCommunityWeight.merge(neighborComm, w, Double::sum);
        }

        // Find best community to move to
        int bestCommunity = currentCommunity;
        double bestGain = 0.0;

        for (final Map.Entry<Integer, Double> entry : neighborCommunityWeight.entrySet()) {
          final int candidateCommunity = entry.getKey();
          if (candidateCommunity == currentCommunity)
            continue;

          // Modularity gain = 2 * w_ic - k_i * sum_c / m
          final double communityTotalDegree = getCommunityDegree(community, nodeDegree, candidateCommunity, n);
          final double gain = entry.getValue() - nodeDegree[i] * communityTotalDegree / (2.0 * totalWeight);

          if (gain > bestGain) {
            bestGain = gain;
            bestCommunity = candidateCommunity;
          }
        }

        if (bestCommunity != currentCommunity) {
          community[i] = bestCommunity;
          changed = true;
        }
      }

      if (!changed)
        break;

      final double newModularity = computeModularity(vertices, community, vertexIndex, nodeDegree, totalWeight, weightProperty);
      if (Math.abs(newModularity - prevModularity) < tolerance)
        break;
      prevModularity = newModularity;
    }

    final double finalModularity = computeModularity(vertices, community, vertexIndex, nodeDegree, totalWeight, weightProperty);

    // Remap community IDs to be sequential starting from 0
    final Map<Integer, Integer> communityRemap = new HashMap<>();
    int nextId = 0;
    for (int i = 0; i < n; i++) {
      if (!communityRemap.containsKey(community[i]))
        communityRemap.put(community[i], nextId++);
    }

    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", vertices.get(i));
      result.setProperty("communityId", communityRemap.get(community[i]));
      result.setProperty("modularity", finalModularity);
      results.add(result);
    }
    return results.stream();
  }

  private double getCommunityDegree(final int[] community, final double[] nodeDegree, final int targetCommunity, final int n) {
    double total = 0.0;
    for (int i = 0; i < n; i++) {
      if (community[i] == targetCommunity)
        total += nodeDegree[i];
    }
    return total;
  }

  private double computeModularity(final List<Vertex> vertices, final int[] community,
      final Map<Vertex, Integer> vertexIndex, final double[] nodeDegree,
      final double totalWeight, final String weightProperty) {
    if (totalWeight == 0.0)
      return 0.0;

    double modularity = 0.0;
    final int n = vertices.size();

    for (int i = 0; i < n; i++) {
      final Vertex v = vertices.get(i);
      for (final Edge edge : v.getEdges(Vertex.DIRECTION.OUT)) {
        final Vertex neighbor = edge.getInVertex();
        final Integer j = vertexIndex.get(neighbor);
        if (j == null || community[i] != community[j])
          continue;

        double w = 1.0;
        if (weightProperty != null) {
          final Object wObj = edge.get(weightProperty);
          if (wObj instanceof Number num)
            w = num.doubleValue();
        }
        modularity += w - (nodeDegree[i] * nodeDegree[j]) / (2.0 * totalWeight);
      }
    }
    return modularity / (2.0 * totalWeight);
  }
}
