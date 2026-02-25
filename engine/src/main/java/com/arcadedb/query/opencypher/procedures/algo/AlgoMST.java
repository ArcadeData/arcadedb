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
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.mst(weightProperty?, relTypes?)
 * <p>
 * Computes the Minimum Spanning Tree (or Forest) of the graph using Kruskal's algorithm.
 * Returns one result row per MST edge. The {@code totalWeight} field repeats on each row
 * for convenience.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.mst('distance', 'ROAD')
 * YIELD source, target, weight, totalWeight
 * RETURN source.name, target.name, weight
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoMST extends AbstractAlgoProcedure {
  public static final String NAME = "algo.mst";

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
    return 2;
  }

  @Override
  public String getDescription() {
    return "Compute Minimum Spanning Tree using Kruskal's algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("source", "target", "weight", "totalWeight");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String weightProperty = args.length > 0 ? extractString(args[0], "weightProperty") : null;
    final String[] relTypes     = args.length > 1 ? extractRelTypes(args[1]) : null;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);

    // Collect edges — two passes to allocate primitive arrays without reallocation
    // Pass 1: count
    int edgeCount = 0;
    for (int i = 0; i < n; i++) {
      final Iterable<Edge> edges = relTypes != null && relTypes.length > 0 ?
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT, relTypes) :
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT);
      for (final Edge e : edges) {
        if (ridToIdx.containsKey(e.getIn()))
          edgeCount++;
      }
    }

    // Pass 2: fill primitive arrays
    final int[]    eu = new int[edgeCount];
    final int[]    ev = new int[edgeCount];
    final double[] ew = new double[edgeCount];
    int ec = 0;
    for (int i = 0; i < n; i++) {
      final Iterable<Edge> edges = relTypes != null && relTypes.length > 0 ?
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT, relTypes) :
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT);
      for (final Edge e : edges) {
        final Integer j = ridToIdx.get(e.getIn());
        if (j == null)
          continue;
        eu[ec] = i;
        ev[ec] = j;
        if (weightProperty != null) {
          final Object w = e.get(weightProperty);
          ew[ec] = w instanceof Number num ? num.doubleValue() : 1.0;
        } else {
          ew[ec] = 1.0;
        }
        ec++;
      }
    }

    // Sort edge indices by weight (Integer[] boxing, one allocation O(E))
    final Integer[] sortIdx = new Integer[ec];
    for (int i = 0; i < ec; i++)
      sortIdx[i] = i;
    Arrays.sort(sortIdx, (a, b) -> Double.compare(ew[a], ew[b]));

    // Union-Find with path compression + union by rank
    final int[] parent = new int[n];
    final int[] rank   = new int[n];
    for (int i = 0; i < n; i++)
      parent[i] = i;

    // Kruskal's
    final int[]    mstU = new int[n - 1];
    final int[]    mstV = new int[n - 1];
    final double[] mstW = new double[n - 1];
    int mstSize = 0;
    double totalWeight = 0.0;

    for (int k = 0; k < ec && mstSize < n - 1; k++) {
      final int idx = sortIdx[k];
      final int ru  = find(parent, eu[idx]);
      final int rv  = find(parent, ev[idx]);
      if (ru != rv) {
        union(parent, rank, ru, rv);
        mstU[mstSize] = eu[idx];
        mstV[mstSize] = ev[idx];
        mstW[mstSize] = ew[idx];
        totalWeight  += ew[idx];
        mstSize++;
      }
    }

    final double finalTotal = totalWeight;
    final int finalSize = mstSize;
    return IntStream.range(0, finalSize).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("source", vertices.get(mstU[i]));
      r.setProperty("target", vertices.get(mstV[i]));
      r.setProperty("weight", mstW[i]);
      r.setProperty("totalWeight", finalTotal);
      return (Result) r;
    });
  }

  // Union-Find helpers
  private static int find(final int[] parent, int x) {
    while (parent[x] != x) {
      parent[x] = parent[parent[x]]; // path halving
      x = parent[x];
    }
    return x;
  }

  private static void union(final int[] parent, final int[] rank, final int a, final int b) {
    if (rank[a] < rank[b])
      parent[a] = b;
    else if (rank[a] > rank[b])
      parent[b] = a;
    else {
      parent[b] = a;
      rank[a]++;
    }
  }
}
