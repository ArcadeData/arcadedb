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
 * and sink node identities.
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

    // Capacity extraction goes through GraphData.weightedAdjacency (issue #6316), the same shared helper the other
    // weighted algo.* procedures read edge weights through, rather than a hand-rolled getEdges() walk. A blank
    // capacityProperty is normalised to null first so both mean "no property, every edge has capacity 1.0" exactly as
    // before.
    final String capacityProp = capacityProperty != null && !capacityProperty.isEmpty() ? capacityProperty : null;
    final GraphData.WeightedAdjacency weighted = graph.weightedAdjacency(guard, Vertex.DIRECTION.OUT, capacityProp, relTypes);
    final int[][] neighbors = weighted.neighbors();
    final double[][] weights = weighted.weights();

    // The residual graph as arcs, one per direction of every pair of nodes an edge joins (issue #12175: it was two dense
    // nodeCount x nodeCount matrices, 1.6 GB at 10 000 nodes, and the search scanned a whole row per node). The graph is
    // read as undirected: an edge i -> j gives both i -> j and j -> i its capacity, summed over the edges of the pair in
    // the order they are read, so the capacity of a pair is the one the matrix cell held. A self-loop is never on a path.
    final int[] contributions = new int[n];
    for (int i = 0; i < n; i++) {
      for (final int j : neighbors[i]) {
        if (j != i) {
          contributions[i]++;
          contributions[j]++;
        }
      }
    }
    long total = 0;
    for (final int c : contributions)
      total += c;
    final int contributionCount = checkedEdgeCount(NAME, n, total);
    final MemoryBudget memory = graph.memory();
    memory.reserve(saturatingSum(saturatingProduct(contributionCount, 2 * INT_BYTES + 2 * DOUBLE_BYTES + LONG_BYTES),
            saturatingProduct(n + 1L, 5 * INT_BYTES)), "the residual graph", n + " nodes, " + contributionCount + " arcs");

    final int[] arcStart = new int[n + 1];
    for (int i = 0; i < n; i++)
      arcStart[i + 1] = arcStart[i] + contributions[i];
    final int[] contributionHead = new int[contributionCount];
    final double[] contributionCapacity = new double[contributionCount];
    final int[] fill = Arrays.copyOf(arcStart, n);
    for (int i = 0; i < n; i++) {
      guard.checkPeriodically(i);
      final int[] row = neighbors[i];
      final double[] rowWeights = weights[i];
      for (int k = 0; k < row.length; k++) {
        final int j = row[k];
        if (j == i)
          continue;
        contributionHead[fill[i]] = j;
        contributionCapacity[fill[i]++] = rowWeights[k];
        contributionHead[fill[j]] = i;
        contributionCapacity[fill[j]++] = rowWeights[k];
      }
    }

    // Each node's arcs sorted by head, the contributions to one head summed in reading order: the breadth-first search
    // then meets the heads in the ascending order the row scan met them in, and finds the same augmenting paths
    final long[] sortKeys = new long[contributionCount];
    final int[] arcHead = new int[contributionCount];
    final double[] residual = new double[contributionCount];
    int arcs = 0;
    for (int u = 0; u < n; u++) {
      guard.checkPeriodically(u);
      final int from = arcStart[u];
      final int to = arcStart[u + 1];
      for (int c = from; c < to; c++)
        sortKeys[c] = ((long) contributionHead[c] << 32) | (c - from);
      Arrays.sort(sortKeys, from, to);
      arcStart[u] = arcs;
      for (int c = from; c < to; c++) {
        final int head = (int) (sortKeys[c] >>> 32);
        final double capacity = contributionCapacity[from + (int) sortKeys[c]];
        if (arcs > arcStart[u] && arcHead[arcs - 1] == head)
          residual[arcs - 1] += capacity;
        else {
          arcHead[arcs] = head;
          residual[arcs++] = 0.0 + capacity;
        }
      }
    }
    arcStart[n] = arcs;
    // The reverse of every arc: the pairs are symmetric, so v's sorted heads hold u
    final int[] reverseArc = new int[arcs];
    for (int u = 0; u < n; u++)
      for (int a = arcStart[u]; a < arcStart[u + 1]; a++)
        reverseArc[a] = Arrays.binarySearch(arcHead, arcStart[arcHead[a]], arcStart[arcHead[a] + 1], u);

    final int[] queue = new int[n];
    final int[] parent = new int[n];
    final int[] parentArc = new int[n];
    double maxFlow = 0.0;

    while (true) {
      // Edmonds-Karp augments once per iteration and each augmentation is a BFS over the residual arcs, O(V + E), so
      // the loop is O(V x E²) with the graph alone sizing it - no knob, and until issue #6302 no way for a thread
      // interrupt or arcadedb.command.timeout to end it.
      guard.check();
      // BFS to find augmenting path from src to snk
      Arrays.fill(parent, -1);
      parent[srcIdx] = srcIdx;
      int head = 0, tail = 0;
      queue[tail++] = srcIdx;

      bfs:
      while (head < tail) {
        guard.checkPeriodically(head);
        final int u = queue[head++];
        for (int a = arcStart[u]; a < arcStart[u + 1]; a++) {
          final int v = arcHead[a];
          if (parent[v] == -1 && residual[a] > 0) {
            parent[v] = u;
            parentArc[v] = a;
            if (v == snkIdx)
              break bfs;
            queue[tail++] = v;
          }
        }
      }

      if (parent[snkIdx] == -1)
        break; // No augmenting path found

      // Find minimum residual capacity along the path
      double pathFlow = Double.MAX_VALUE;
      int v = snkIdx;
      while (v != srcIdx) {
        if (residual[parentArc[v]] < pathFlow)
          pathFlow = residual[parentArc[v]];
        v = parent[v];
      }

      // Update residual capacities
      v = snkIdx;
      while (v != srcIdx) {
        final int a = parentArc[v];
        residual[a] -= pathFlow;
        residual[reverseArc[a]] += pathFlow;
        v = parent[v];
      }

      maxFlow += pathFlow;
    }

    final ResultInternal result = new ResultInternal();
    result.setProperty("maxFlow", maxFlow);
    result.setProperty("sourceId", sourceNode.getIdentity());
    result.setProperty("sinkId", sinkNode.getIdentity());
    return Stream.of(result);
  }
}
