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

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.densestSubgraph(relTypes?)
 * <p>
 * Finds the densest subgraph using Charikar's greedy peeling algorithm (2-approximation),
 * with O(V+E) edge processing. Density is defined as edges/nodes ratio for the subgraph.
 * Returns each node with a flag indicating whether it belongs to the densest subgraph found,
 * and the density value of that subgraph.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.densestSubgraph()
 * YIELD node, inDenseSubgraph, density
 * RETURN node.name, inDenseSubgraph, density ORDER BY inDenseSubgraph DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoDensestSubgraph extends AbstractAlgoProcedure {
  public static final String NAME = "algo.densestSubgraph";

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
    return "Find the densest subgraph using Charikar's greedy peeling algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "inDenseSubgraph", "density");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = extractRelTypes(argOrDefault(args, 0, null), context);

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    final GraphData graph = loadGraph(db, null, relTypes, context);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH, relTypes);

    // Charikar's greedy peeling
    final int[] deg = new int[n];
    int totalEdges = 0;
    for (int i = 0; i < n; i++) {
      deg[i] = adj[i].length;
      totalEdges += deg[i];
    }
    totalEdges /= 2;

    final boolean[] removed = new boolean[n];
    // The peel order, and how many of its first vertices were peeled when the density peaked: the best set is rebuilt from
    // them once at the end instead of being copied on every improvement (issue #12168)
    final int[] peelOrder = new int[n];
    int peeled = 0;
    int bestPeeled = 0; // initially all nodes are in
    double bestDensity = n > 0 ? (double) totalEdges / n : 0.0;
    int remaining = n;

    // The vertex to peel is the remaining one of lowest degree, the lowest index among equals, the vertex the linear scan
    // this replaces picked: a heap of (degree, index) keys with lazy deletion, one entry per degree change, so the loop
    // is O((V + E) log V) rather than a scan of every vertex per peeled vertex (issue #12168)
    final LongMinHeap heap = new LongMinHeap(n);
    for (int i = 0; i < n; i++)
      heap.push(key(deg[i], i));

    while (remaining > 1) {
      guard.check();
      final double density = (double) totalEdges / remaining;
      if (density > bestDensity) {
        bestDensity = density;
        bestPeeled = peeled;
      }

      int minV = -1;
      while (heap.size() > 0) {
        final long top = heap.pop();
        final int v = (int) top;
        if (!removed[v] && deg[v] == (int) (top >>> 32)) {
          minV = v;
          break;
        }
      }
      if (minV == -1)
        break;

      removed[minV] = true;
      peelOrder[peeled++] = minV;
      remaining--;
      for (final int u : adj[minV]) {
        if (!removed[u]) {
          deg[u]--;
          totalEdges--;
          heap.push(key(deg[u], u));
        }
      }
    }

    // Check the last remaining singleton
    if (remaining == 1) {
      final double density = totalEdges > 0 ? (double) totalEdges / remaining : 0.0;
      if (density > bestDensity) {
        bestDensity = density;
        bestPeeled = peeled;
      }
    }

    final boolean[] bestSubgraph = new boolean[n];
    Arrays.fill(bestSubgraph, true);
    for (int k = 0; k < bestPeeled; k++)
      bestSubgraph[peelOrder[k]] = false;

    final double finalDensity = bestDensity;

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(i));
      r.setProperty("inDenseSubgraph", bestSubgraph[i]);
      r.setProperty("density", finalDensity);
      return (Result) r;
    });
  }

  /** Orders by degree, then by vertex index: a degree is never negative, so the unsigned shift keeps the order. */
  private static long key(final int degree, final int vertex) {
    return ((long) degree << 32) | vertex;
  }

  /** A binary min-heap of primitive longs, so the peel loop boxes nothing. */
  private static final class LongMinHeap {
    private long[] items;
    private int    size;

    LongMinHeap(final int capacity) {
      items = new long[Math.max(16, capacity)];
    }

    int size() {
      return size;
    }

    void push(final long value) {
      if (size == items.length)
        items = Arrays.copyOf(items, items.length * 2);
      int i = size++;
      while (i > 0) {
        final int parent = (i - 1) >>> 1;
        if (items[parent] <= value)
          break;
        items[i] = items[parent];
        i = parent;
      }
      items[i] = value;
    }

    long pop() {
      final long top = items[0];
      final long last = items[--size];
      int i = 0;
      while (true) {
        int child = 2 * i + 1;
        if (child >= size)
          break;
        if (child + 1 < size && items[child + 1] < items[child])
          child++;
        if (items[child] >= last)
          break;
        items[i] = items[child];
        i = child;
      }
      if (size > 0)
        items[i] = last;
      return top;
    }
  }
}
