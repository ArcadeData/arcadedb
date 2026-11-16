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
import java.util.stream.Stream;

/**
 * Procedure: algo.maxFlow(sourceNode, sinkNode, relTypes?, capacityProperty?)
 * <p>
 * Computes the maximum flow between two nodes using the Edmonds-Karp algorithm
 * (BFS-based Ford-Fulkerson). Returns the maximum flow value along with source
 * and sink node identities. Every edge carries its capacity in both directions. The network is
 * held as a residual graph of arcs, O(V + E) memory and O(E) per augmenting path (issue #12175).
 * The source and the sink must be two different vertices.
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (s:Station {name:'A'}), (t:Station {name:'Z'})
 * CALL algo.maxFlow(s, t, 'PIPE', 'capacity')
 * YIELD maxFlow, sourceId, sinkId
 * RETURN maxFlow
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoMaxFlow extends AbstractAlgoProcedure {
  public static final String NAME = "algo.maxFlow";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Computes maximum flow between two nodes using the Edmonds-Karp algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("maxFlow", "sourceId", "sinkId");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex sourceNode       = extractVertex(args[0], "sourceNode");
    final Vertex sinkNode         = extractVertex(args[1], "sinkNode");
    final String[] relTypes       = extractRelTypes(argOrDefault(args, 2, null), context);
    final String capacityProperty = extractString(argOrDefault(args, 3, null), "capacityProperty");

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);
    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    final int srcIdx = graph.indexOf(sourceNode.getIdentity());
    final int snkIdx = graph.indexOf(sinkNode.getIdentity());
    if (srcIdx < 0 || snkIdx < 0)
      return Stream.empty();
    // A flow from a node to itself has no bound: every round would find the empty path again and add an infinite amount
    if (srcIdx == snkIdx)
      throw new IllegalArgumentException(getName() + "(): sourceNode and sinkNode must be two different vertices, both are "
          + sourceNode.getIdentity());

    // Capacity extraction goes through GraphData.weightedAdjacency (issue #6316), the same shared helper the other weighted
    // algo.* procedures read edge weights through. A blank capacityProperty is normalised to null first so both mean "no
    // property, every edge has capacity 1.0".
    final String capacityProp = capacityProperty != null && !capacityProperty.isEmpty() ? capacityProperty : null;
    final GraphData.WeightedAdjacency weighted = graph.weightedAdjacency(guard, Vertex.DIRECTION.OUT, capacityProp, relTypes);

    final ResidualGraph residual = new ResidualGraph(graph.memory(), guard, n, weighted);
    final double maxFlow = residual.maxFlow(srcIdx, snkIdx);

    final ResultInternal result = new ResultInternal();
    result.setProperty("maxFlow", maxFlow);
    result.setProperty("sourceId", sourceNode.getIdentity());
    result.setProperty("sinkId", sinkNode.getIdentity());
    return Stream.of(result);
  }

  /**
   * The network as a residual graph of arcs (issue #12175): an edge carries its capacity both ways, so every pair of adjacent nodes
   * is one arc each way, holding the sum of the capacities of the edges between them in either direction, and each arc knows its
   * twin. That is what the dense formulation kept in two nodeCount x nodeCount matrices, in O(V + E) memory, and a BFS step reads
   * the arcs of the node it settles instead of a whole matrix row.
   * <p>
   * A row lists its arcs by ascending neighbour, and a pair's capacity is summed in the order the edges are read, as the matrix
   * summed it: the BFS meets the neighbours in the order a scan of the matrix row met them, so it finds the same augmenting paths
   * and adds up the same flow, to the last bit.
   */
  private static final class ResidualGraph {
    private final WorkGuard guard;
    private final int       nodeCount;
    // ARCS OF NODE u: [offsets[u], offsets[u + 1]), TO targets[a] WITH RESIDUAL CAPACITY residual[a], REVERSED BY twin[a]
    private final int[]     offsets;
    private final int[]     targets;
    private final double[]  residual;
    private final int[]     twin;

    private ResidualGraph(final MemoryBudget memory, final WorkGuard guard, final int n, final GraphData.WeightedAdjacency weighted) {
      this.guard = guard;
      this.nodeCount = n;
      final int[][] neighbors = weighted.neighbors();
      final double[][] weights = weighted.weights();

      // One arc each way per edge, a self-loop none: it carries nothing from one node to another
      long arcCount = 0;
      for (int i = 0; i < n; i++)
        for (final int j : neighbors[i])
          if (j != i)
            arcCount += 2;
      if (arcCount > Integer.MAX_VALUE - 8)
        throw new IllegalArgumentException(
            NAME + "(): the graph has " + arcCount + " arcs, more than the " + (Integer.MAX_VALUE - 8) + " a Java array can hold");
      final int arcs = (int) arcCount;
      // The arcs as read (source, target, capacity), the two stable passes that order them by (source, target), and the merged
      // residual graph: all alive at the peak, priced before the first is allocated
      memory.reserve(saturatingSum(saturatingProduct(arcs, 2 * INT_BYTES + DOUBLE_BYTES + 2 * INT_BYTES),
              saturatingSum(saturatingProduct(arcs, 2 * INT_BYTES + DOUBLE_BYTES), saturatingProduct(n + 2L, 3 * INT_BYTES))),
          "the residual graph", n + " nodes, " + arcs + " arcs");

      final int[] from = new int[arcs];
      final int[] to = new int[arcs];
      final double[] cap = new double[arcs];
      int a = 0;
      for (int i = 0; i < n; i++) {
        guard.checkPeriodically(i);
        final int[] row = neighbors[i];
        final double[] rowWeights = weights[i];
        for (int k = 0; k < row.length; k++) {
          final int j = row[k];
          if (j == i)
            continue;
          from[a] = i;
          to[a] = j;
          cap[a++] = rowWeights[k];
          from[a] = j;
          to[a] = i;
          cap[a++] = rowWeights[k];
        }
      }

      // Ordered by (source, target), arcs of one pair in the order they were read: two stable counting sorts
      final int[] byTarget = countingSort(null, to, arcs, n);
      final int[] order = countingSort(byTarget, from, arcs, n);

      // Each run of one (source, target) pair becomes one arc, its capacities summed in reading order
      final int[] offs = new int[n + 1];
      final int[] tgt = new int[arcs];
      final double[] res = new double[arcs];
      int merged = 0;
      int previousFrom = -1;
      int previousTo = -1;
      for (int o = 0; o < arcs; o++) {
        final int arc = order[o];
        final int u = from[arc];
        final int v = to[arc];
        if (u != previousFrom || v != previousTo) {
          offs[u + 1]++;
          tgt[merged] = v;
          res[merged++] = 0.0;
          previousFrom = u;
          previousTo = v;
        }
        res[merged - 1] += cap[arc];
      }
      for (int u = 0; u < n; u++)
        offs[u + 1] += offs[u];

      this.offsets = offs;
      this.targets = merged == arcs ? tgt : Arrays.copyOf(tgt, merged);
      this.residual = merged == arcs ? res : Arrays.copyOf(res, merged);

      // Every pair is listed from both ends, so the twin of (u, v) is v's arc to u, found in v's sorted row
      this.twin = new int[merged];
      for (int u = 0; u < n; u++) {
        guard.checkPeriodically(u);
        for (int arc = offsets[u]; arc < offsets[u + 1]; arc++) {
          final int v = targets[arc];
          twin[arc] = Arrays.binarySearch(targets, offsets[v], offsets[v + 1], u);
        }
      }
    }

    /**
     * The positions of the {@code count} arcs ordered by {@code key} (a node id below {@code n}), stably: in the order of
     * {@code input} when it is given, of the arcs otherwise.
     */
    private static int[] countingSort(final int[] input, final int[] key, final int count, final int n) {
      final int[] start = new int[n + 1];
      for (int i = 0; i < count; i++)
        start[key[i] + 1]++;
      for (int k = 0; k < n; k++)
        start[k + 1] += start[k];
      final int[] output = new int[count];
      for (int i = 0; i < count; i++) {
        final int arc = input != null ? input[i] : i;
        output[start[key[arc]]++] = arc;
      }
      return output;
    }

    /** Edmonds-Karp: augments along a shortest path of positive residual capacity until none is left. */
    private double maxFlow(final int source, final int sink) {
      final int[] queue = new int[nodeCount];
      // THE ARC EACH NODE WAS REACHED THROUGH, -1 WHEN NOT REACHED YET; THE SOURCE IS MARKED REACHED WITH AN ARC OF ITS OWN
      final int[] parentArc = new int[nodeCount];
      double maxFlow = 0.0;

      while (true) {
        // Edmonds-Karp augments once per iteration, O(V x E²) in all with the graph alone sizing it: checked per augmentation,
        // and per settled node inside the BFS (issue #6302)
        guard.check();
        Arrays.fill(parentArc, -1);
        parentArc[source] = Integer.MAX_VALUE;
        int head = 0, tail = 0;
        queue[tail++] = source;

        bfs:
        while (head < tail) {
          final int u = queue[head++];
          guard.checkPeriodically(head);
          for (int arc = offsets[u]; arc < offsets[u + 1]; arc++) {
            final int v = targets[arc];
            if (parentArc[v] == -1 && residual[arc] > 0) {
              parentArc[v] = arc;
              if (v == sink)
                break bfs;
              queue[tail++] = v;
            }
          }
        }

        if (parentArc[sink] == -1)
          break; // No augmenting path found

        // Find minimum residual capacity along the path
        double pathFlow = Double.MAX_VALUE;
        for (int v = sink; v != source; v = targets[twin[parentArc[v]]]) {
          final int arc = parentArc[v];
          if (residual[arc] < pathFlow)
            pathFlow = residual[arc];
        }

        // Update residual capacities
        for (int v = sink; v != source; v = targets[twin[parentArc[v]]]) {
          final int arc = parentArc[v];
          residual[arc] -= pathFlow;
          residual[twin[arc]] += pathFlow;
        }

        maxFlow += pathFlow;
      }
      return maxFlow;
    }
  }
}
