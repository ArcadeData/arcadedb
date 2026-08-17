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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
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
 * <p>
 * Working set: three edge-sized arrays plus the index sort's two, 24 bytes per edge, reserved through
 * {@link AbstractAlgoProcedure.MemoryBudget} as the counting pass runs so that a graph too large to serve is
 * refused mid-walk rather than after one (issue #6300). The passes themselves and Kruskal's loop carry a
 * {@link WorkGuard} checkpoint, since O(E log E) over a graph nothing bounds has to be abortable (issue #6302).
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoMST extends AbstractAlgoProcedure {
  public static final String NAME = "algo.mst";

  /**
   * Heap this procedure spends per edge: the two endpoint arrays and the weight array it fills, plus the index
   * array and merge scratch {@link #sortedIndexesByWeight(double[], int)} allocates over them.
   */
  private static final long EDGE_BYTES = 2 * INT_BYTES + DOUBLE_BYTES + 2 * INT_BYTES;

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
    final WorkGuard guard = newWorkGuard(context);
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);

    // The working set here is sized by the EDGE count, which is the one dense shape #6263 did not price: its
    // criterion was "knob-sized or quadratic in the node count", and edge-linear reads like the graph paying for
    // itself. It is not small - the edge count is the largest linear dimension a graph has, usually an order of
    // magnitude above the node count - and linear was never the criterion. What matters is whether the caller can
    // predict a ceiling, and here there is none: 100M edges ask for ~2.4 GB with nothing between them and the
    // allocator (issue #6300).
    //
    // 24 bytes per edge: the endpoints eu/ev and the weight ew, plus the two int arrays sortedIndexesByWeight
    // works over (the order and the merge scratch).
    //
    // The budget is consulted as pass 1 counts rather than once it is done. Both refuse the same calls, but a
    // check afterwards first pays in full for a traversal it will throw away - the same argument that puts
    // algo.steinerTree's reservation ahead of its adjacency build.
    final MemoryBudget memory = newMemoryBudget(db);
    final long maxEdges = memory.capacityFor(EDGE_BYTES);

    // Collect edges — two passes to allocate primitive arrays without reallocation. Ghost edges are
    // skipped identically in both passes (same getEdges() order), so the pass-2 fill never exceeds the
    // pass-1 count and the arrays are always sized correctly. This is safe because an edge record is
    // only ever deleted, never resurrected, during a read query: a ghost in pass 1 is still a ghost in
    // pass 2.
    // Pass 1: count
    int edgeCount = 0;
    int edgeStep = 0;
    for (int i = 0; i < n; i++) {
      final Iterable<Edge> edges = relTypes != null && relTypes.length > 0 ?
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT, relTypes) :
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT);
      for (final Edge e : edges) {
        // Both passes deserialise every edge record of the graph, so each is O(V + E) of real work with nothing
        // but the graph to bound it (issue #6302). Throttled by EDGE rather than by vertex: a near-star graph
        // puts millions of edges inside a single vertex iteration, and a per-vertex checkpoint would leave that
        // whole node unabortable - which is exactly the case the budget cannot catch when it is disabled.
        guard.checkPeriodically(edgeStep++);
        try {
          if (ridToIdx.containsKey(e.getIn())) {
            edgeCount++;
            if (edgeCount > maxEdges)
              // Over budget: reserve() is what refuses, so the message names the same component, figure and
              // setting it would have named had the count finished first.
              memory.reserve(saturatingProduct(edgeCount, EDGE_BYTES), "the edge arrays",
                  "more than " + maxEdges + " edges");
          }
        } catch (final RecordNotFoundException rnf) {  // 'rnf' not 'e' here: 'e' is the Edge loop variable in this scope
          GhostEdgeReporter.reportSkipped(rnf);
        }
      }
    }
    // Succeeds by construction - the loop above stopped the walk the moment the count passed what the budget
    // allows - and is made anyway so the reservation is recorded against the call rather than only tested. What
    // it costs is one comparison; what it buys is that `reserved` describes the heap this call actually holds,
    // which is the invariant every other component priced here relies on.
    memory.reserve(saturatingProduct(edgeCount, EDGE_BYTES), "the edge arrays", edgeCount + " edges");

    // Pass 2: fill primitive arrays
    final int[]    eu = new int[edgeCount];
    final int[]    ev = new int[edgeCount];
    final double[] ew = new double[edgeCount];
    int ec = 0;
    edgeStep = 0;
    for (int i = 0; i < n; i++) {
      final Iterable<Edge> edges = relTypes != null && relTypes.length > 0 ?
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT, relTypes) :
          vertices.get(i).getEdges(Vertex.DIRECTION.OUT);
      for (final Edge e : edges) {
        guard.checkPeriodically(edgeStep++);
        try {
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
        } catch (final RecordNotFoundException rnf) {  // 'rnf' not 'e' here: 'e' is the Edge loop variable in this scope
          GhostEdgeReporter.reportSkipped(rnf);
        }
      }
    }

    // Sort edge indices by weight, over primitive indices: one int[] of order plus one of merge scratch,
    // against the 24 bytes per edge an Integer[] cost (issue #6289).
    final int[] sortIdx = sortedIndexesByWeight(ew, ec);

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
      // O(E) iterations of two union-find finds each - small enough per iteration to throttle the check.
      guard.checkPeriodically(k);
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
      r.setProperty("source", vertices.get(mstU[i]).getIdentity());
      r.setProperty("target", vertices.get(mstV[i]).getIdentity());
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
