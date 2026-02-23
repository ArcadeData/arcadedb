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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.leiden(relTypes?, maxIterations?, resolution?)
 * <p>
 * Detects communities using the Leiden algorithm, an improved version of Louvain with a
 * refinement phase that guarantees well-connected communities. The resolution parameter γ
 * controls community granularity.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.leiden('KNOWS', 10, 1.0)
 * YIELD nodeId, community
 * RETURN nodeId, community
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoLeiden extends AbstractAlgoProcedure {
  public static final String NAME = "algo.leiden";

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
    return 3;
  }

  @Override
  public String getDescription() {
    return "Detects communities using the Leiden algorithm with refinement phase";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodeId", "community");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = args.length > 0 ? extractRelTypes(args[0]) : null;
    final int maxIterations = args.length > 1 && args[1] instanceof Number n ? n.intValue() : 10;
    final double resolution = args.length > 2 && args[2] instanceof Number n ? n.doubleValue() : 1.0;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.BOTH, relTypes);

    // Compute total edges (m) and node degrees
    final int[] degree = new int[n];
    long totalEdges = 0;
    for (int i = 0; i < n; i++) {
      degree[i] = adj[i].length;
      totalEdges += degree[i];
    }
    // Each undirected edge counted twice
    final double m = totalEdges / 2.0;
    if (m == 0.0) {
      // Isolated nodes: each in its own community
      return buildResults(vertices, initCommunities(n));
    }

    // Phase 1: Initialize each node in its own community
    final int[] community = initCommunities(n);
    final double[] communityWeight = new double[n]; // sum of internal edge weights
    final long[] communityDegree = new long[n];     // sum of degrees of nodes in community
    for (int i = 0; i < n; i++)
      communityDegree[i] = degree[i];

    for (int iter2 = 0; iter2 < maxIterations; iter2++) {
      boolean changed = false;

      // Phase 1: Local moves (greedy modularity optimization)
      for (int i = 0; i < n; i++) {
        final int currentComm = community[i];
        final long ki = degree[i];

        // Count weights to each neighboring community
        final Map<Integer, Integer> commWeights = new HashMap<>();
        for (final int j : adj[i]) {
          final int jComm = community[j];
          commWeights.merge(jComm, 1, Integer::sum);
        }

        int bestComm = currentComm;
        double bestGain = 0.0;

        for (final Map.Entry<Integer, Integer> entry : commWeights.entrySet()) {
          final int candidateComm = entry.getKey();
          if (candidateComm == currentComm)
            continue;

          final double eIc = entry.getValue();
          final double dc = communityDegree[candidateComm];
          // Modularity gain = [e_ic / m] - γ * [k_i * d_c / (2m²)]
          final double gain = eIc / m - resolution * ki * dc / (2.0 * m * m);

          if (gain > bestGain) {
            bestGain = gain;
            bestComm = candidateComm;
          }
        }

        if (bestComm != currentComm) {
          communityDegree[currentComm] -= ki;
          communityDegree[bestComm] += ki;
          community[i] = bestComm;
          changed = true;
        }
      }

      // Phase 2: Refinement — within each community, try to split into sub-communities
      // by re-running local moves restricted to same community
      if (changed) {
        boolean refined = false;
        for (int i = 0; i < n; i++) {
          final int currentComm = community[i];
          final long ki = degree[i];

          final Map<Integer, Integer> sameCommWeights = new HashMap<>();
          for (final int j : adj[i]) {
            if (community[j] == currentComm)
              sameCommWeights.merge(community[j], 1, Integer::sum);
          }

          // Try removing from current community
          final int internalEdges = sameCommWeights.getOrDefault(currentComm, 0);
          final double removeGain = -(internalEdges / m - resolution * ki * communityDegree[currentComm] / (2.0 * m * m));

          if (removeGain > 0.0) {
            // Move to its own community (use index as community ID)
            final int newComm = i;
            if (newComm != currentComm) {
              communityDegree[currentComm] -= ki;
              communityDegree[newComm] = ki;
              community[i] = newComm;
              refined = true;
            }
          }
        }

        if (!refined)
          break;
      } else {
        break;
      }
    }

    // Remap communities to sequential IDs
    final Map<Integer, Integer> remap = new HashMap<>();
    int nextId = 0;
    for (int i = 0; i < n; i++) {
      if (!remap.containsKey(community[i]))
        remap.put(community[i], nextId++);
    }

    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", vertices.get(i).getIdentity());
      r.setProperty("community", remap.get(community[i]));
      results.add(r);
    }
    return results.stream();
  }

  private int[] initCommunities(final int n) {
    final int[] c = new int[n];
    for (int i = 0; i < n; i++)
      c[i] = i;
    return c;
  }

  private Stream<Result> buildResults(final List<Vertex> vertices, final int[] community) {
    final int n = vertices.size();
    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", vertices.get(i).getIdentity());
      r.setProperty("community", community[i]);
      results.add(r);
    }
    return results.stream();
  }
}
