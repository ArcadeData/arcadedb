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
import com.arcadedb.graph.Edge;
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
 * Procedure: algo.maxKCut(k, config?)
 *
 * <p>Approximate Maximum k-Cut: partition the graph into k vertex-disjoint subsets to
 * maximise the total weight of edges crossing between partitions. Uses a greedy local-search
 * heuristic with multiple random restarts: each restart randomly assigns all nodes to k
 * partitions and then iteratively moves nodes to the partition that maximises the local
 * cut contribution, until convergence. The best partition found across all restarts is
 * returned.</p>
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@code k} (int, required) – number of partitions (k ≥ 2)</li>
 *   <li>{@code config} (map, optional):
 *     <ul>
 *       <li>{@code maxIterations} (int, default 100) – local-search passes per restart</li>
 *       <li>{@code restarts} (int, default 3) – number of random restarts</li>
 *       <li>{@code weightProperty} (String, default null) – edge weight property name</li>
 *       <li>{@code relTypes} (String, default all)</li>
 *       <li>{@code direction} (String, default BOTH)</li>
 *       <li>{@code seed} (long, default -1)</li>
 *     </ul>
 *   </li>
 * </ul>
 * </p>
 *
 * <p>Example:
 * <pre>
 * CALL algo.maxKCut(3, {restarts: 5})
 * YIELD node, community, cutWeight
 * RETURN node.name, community
 * ORDER BY community
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoMaxKCut extends AbstractAlgoProcedure {

  @Override
  public String getName() {
    return "algo.maxKCut";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Approximate Maximum k-Cut: partitions the graph into k vertex-disjoint subsets to maximise inter-partition edge weight via greedy local search with random restarts";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "community", "cutWeight");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final int k = args[0] instanceof Number kn ? kn.intValue() : 2;
    if (k < 2)
      throw new IllegalArgumentException(getName() + "(): k must be ≥ 2, got " + k);

    final Map<String, Object> config = args.length > 1 ? extractMap(args[1], "config") : null;
    final int maxIter = config != null && config.get("maxIterations") instanceof Number n ? n.intValue() : 100;
    final int restarts = config != null && config.get("restarts") instanceof Number n ? n.intValue() : 3;
    final String weightProperty = config != null ? (String) config.get("weightProperty") : null;
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

    // Build weighted adjacency (null weightProperty → all weights = 1.0)
    final double[][] adjW = buildWeightedAdj(vertices, ridToIdx, adj, dir, relTypes, weightProperty);

    final Random rng = seed >= 0 ? new Random(seed) : new Random();
    int[] bestAssign = new int[n];
    double bestCut = -1.0;

    for (int restart = 0; restart < restarts; restart++) {
      // Random initial assignment
      final int[] assign = new int[n];
      for (int i = 0; i < n; i++)
        assign[i] = rng.nextInt(k);

      // Local search: for each node try all k partitions
      for (int pass = 0; pass < maxIter; pass++) {
        boolean improved = false;
        for (int i = 0; i < n; i++) {
          final int curPart = assign[i];
          // Compute contribution of each partition choice for node i
          final double[] gain = new double[k];
          for (int j = 0; j < adj[i].length; j++) {
            final int nb = adj[i][j];
            final double w = adjW[i][j];
            final int nbPart = assign[nb];
            for (int p = 0; p < k; p++)
              if (p != nbPart)
                gain[p] += w;
          }
          // Find best partition
          int bestPart = curPart;
          double bestGain = gain[curPart];
          for (int p = 0; p < k; p++) {
            if (gain[p] > bestGain) {
              bestGain = gain[p];
              bestPart = p;
            }
          }
          if (bestPart != curPart) {
            assign[i] = bestPart;
            improved = true;
          }
        }
        if (!improved)
          break;
      }

      // Compute total cut weight
      final double cutWeight = computeCutWeight(n, adj, adjW, assign);
      if (cutWeight > bestCut) {
        bestCut = cutWeight;
        bestAssign = Arrays.copyOf(assign, n);
      }
    }

    final double finalCut = bestCut;
    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("community", bestAssign[i]);
      r.setProperty("cutWeight", finalCut);
      results.add(r);
    }
    return results.stream();
  }

  /** Builds a parallel weight array for each adjacency list entry. */
  private double[][] buildWeightedAdj(final List<Vertex> vertices, final Map<RID, Integer> ridToIdx,
      final int[][] adj, final Vertex.DIRECTION dir, final String[] relTypes, final String weightProp) {
    final int n = vertices.size();
    final double[][] adjW = new double[n][];
    for (int i = 0; i < n; i++) {
      adjW[i] = new double[adj[i].length];
      Arrays.fill(adjW[i], 1.0);
    }
    if (weightProp == null)
      return adjW;

    // Fill weights from edge properties
    for (int i = 0; i < n; i++) {
      final Iterable<Edge> edges = relTypes != null && relTypes.length > 0 ?
          vertices.get(i).getEdges(dir, relTypes) :
          vertices.get(i).getEdges(dir);
      int pos = 0;
      for (final Edge e : edges) {
        final RID nbRid = neighborRid(e, vertices.get(i).getIdentity(), dir);
        if (nbRid == null || !ridToIdx.containsKey(nbRid))
          continue;
        if (pos < adjW[i].length) {
          final Object w = e.get(weightProp);
          adjW[i][pos] = w instanceof Number num ? num.doubleValue() : 1.0;
          pos++;
        }
      }
    }
    return adjW;
  }

  private static double computeCutWeight(final int n, final int[][] adj, final double[][] adjW,
      final int[] assign) {
    double cut = 0.0;
    for (int i = 0; i < n; i++)
      for (int j = 0; j < adj[i].length; j++)
        if (assign[i] != assign[adj[i][j]])
          cut += adjW[i][j];
    return cut / 2.0; // each edge counted twice in undirected adjacency
  }
}
