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
 * Procedure: algo.maxFlow(sourceNode, sinkNode, relTypes?, capacityProperty?, direction?)
 * <p>
 * Computes the maximum flow between two nodes. Every relationship is a pipe from its source to its target, whose
 * capacity is the {@code capacityProperty} (1.0 when absent); relationships between the same pair in the same direction
 * add up, and a self loop, a non-positive or a NaN capacity carries nothing. {@code direction} (default {@code OUT})
 * chooses how a relationship is read: {@code OUT} along it, {@code IN} against it, {@code BOTH} as an undirected pipe
 * usable either way. The flow from a node to itself is 0, and a path made of infinite capacities only makes it infinite.
 * </p>
 * <p>
 * The residual network is held sparse, one arc per relationship plus its reverse arc, and solved with Dinic's
 * algorithm: O(V + E) memory and O(V² x E) time. The former Edmonds-Karp over two dense V x V matrices needed 1.6 GB at
 * 10 000 nodes whatever the number of relationships.
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
    return 5;
  }

  @Override
  public String getDescription() {
    return "Computes the maximum flow between two nodes along the relationships (direction OUT by default, IN or BOTH)";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("maxFlow", "sourceId", "sinkId");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex sourceNode = extractVertex(args[0], "sourceNode");
    final Vertex sinkNode = extractVertex(args[1], "sinkNode");
    final String[] relTypes = extractRelTypes(argOrDefault(args, 2, null), context);
    final String capacityProperty = extractString(argOrDefault(args, 3, null), "capacityProperty");
    final Vertex.DIRECTION direction = extractDirection(argOrDefault(args, 4, null), Vertex.DIRECTION.OUT);

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

    // A node to itself has no path to augment: the old loop augmented the empty path by Double.MAX_VALUE forever.
    final double maxFlow = srcIdx == snkIdx ? 0.0 : solve(graph, guard, direction, capacityProperty, relTypes, srcIdx, snkIdx);

    final ResultInternal result = new ResultInternal();
    result.setProperty("maxFlow", maxFlow);
    result.setProperty("sourceId", sourceNode.getIdentity());
    result.setProperty("sinkId", sinkNode.getIdentity());
    return Stream.of(result);
  }

  /**
   * Builds the residual network and returns its maximum flow.
   * <p>
   * Capacity extraction goes through {@link GraphData#weightedAdjacency} (issue #6316), the same shared helper the other
   * weighted {@code algo.*} procedures read edge weights through. A blank capacityProperty is normalised to null first so
   * both mean "no property, every edge has capacity 1.0". The row of node i lists one entry per relationship read in
   * {@code direction}, so the entry (i, j, c) is an arc i -> j of capacity c. Issue #11798: the arc used to be added in
   * both directions too, which computed the flow of the undirected network whatever the relationships said; {@code BOTH}
   * lists a relationship from both of its ends, so it yields the two arcs of an undirected pipe on its own.
   */
  private static double solve(final GraphData graph, final WorkGuard guard, final Vertex.DIRECTION direction,
      final String capacityProperty, final String[] relTypes, final int source, final int sink) {
    final int n = graph.nodeCount;
    final String capacityProp = capacityProperty != null && !capacityProperty.isEmpty() ? capacityProperty : null;
    final GraphData.WeightedAdjacency weighted = graph.weightedAdjacency(guard, direction, capacityProp, relTypes);
    final int[][] neighbors = weighted.neighbors();
    final double[][] weights = weighted.weights();

    long arcs = 0;
    for (int i = 0; i < n; i++) {
      guard.checkPeriodically(i);
      final int[] row = neighbors[i];
      final double[] rowWeights = weights[i];
      for (int k = 0; k < row.length; k++)
        if (carries(i, row[k], rowWeights[k]))
          arcs++;
    }
    if (arcs == 0)
      return 0.0;

    // Every arc travels with its reverse arc, which starts at capacity 0 and is where the residual network gives back
    // flow already sent. While building, an arc and its reverse sit at the pair positions 2k and 2k + 1, so the reverse
    // of the pair position p is p ^ 1; the arcs are then grouped by tail (CSR) and the pairing carried over in reverse[].
    // Per arc: pairTail (reused as slotOf), pairHead, pairCapacity, to, capacity, reverse. Per node: start, fill and
    // the four arrays of the solver.
    final long residualArcs = 2 * arcs;
    graph.memory().reserve(saturatingSum(saturatingProduct(residualArcs, 4 * INT_BYTES + 2 * DOUBLE_BYTES),
            saturatingProduct(n + 1L, 6 * INT_BYTES)), "the residual network",
        n + " nodes, " + residualArcs + " residual arcs");
    if (residualArcs > Integer.MAX_VALUE - 8)
      throw new IllegalArgumentException(NAME + "(): the residual network of " + residualArcs + " arcs exceeds the largest array");

    final int m = (int) residualArcs;
    final int[] pairTail = new int[m];
    final int[] pairHead = new int[m];
    final double[] pairCapacity = new double[m];
    int next = 0;
    for (int i = 0; i < n; i++) {
      guard.checkPeriodically(i);
      final int[] row = neighbors[i];
      final double[] rowWeights = weights[i];
      for (int k = 0; k < row.length; k++) {
        final int j = row[k];
        final double cap = rowWeights[k];
        if (!carries(i, j, cap))
          continue;
        pairTail[next] = i;
        pairHead[next] = j;
        pairCapacity[next++] = cap;
        pairTail[next] = j;
        pairHead[next] = i;
        pairCapacity[next++] = 0.0;
      }
    }

    final int[] start = new int[n + 1];
    for (int p = 0; p < m; p++)
      start[pairTail[p] + 1]++;
    for (int i = 0; i < n; i++)
      start[i + 1] += start[i];

    final int[] fill = Arrays.copyOf(start, n);
    final int[] to = new int[m];
    final double[] capacity = new double[m];
    final int[] reverse = new int[m];
    // pairTail is not read again once a pair position has its slot, so it holds the slot from here on.
    final int[] slotOf = pairTail;
    for (int p = 0; p < m; p++) {
      guard.checkPeriodically(p);
      final int slot = fill[pairTail[p]]++;
      to[slot] = pairHead[p];
      capacity[slot] = pairCapacity[p];
      slotOf[p] = slot;
    }
    for (int p = 0; p < m; p++)
      reverse[slotOf[p]] = slotOf[p ^ 1];

    return dinic(n, start, to, capacity, reverse, source, sink, guard);
  }

  /** A self loop never helps a flow, and a capacity that is not strictly positive (NaN included) carries nothing. */
  private static boolean carries(final int tail, final int head, final double capacity) {
    return tail != head && capacity > 0;
  }

  /**
   * Dinic's algorithm: a breadth-first search layers the residual network by distance from the source, then a blocking
   * flow is pushed along the arcs that go one layer down, until the sink is no longer reachable. The depth-first search is
   * iterative, since an augmenting path can be as long as the graph, and keeps a current-arc pointer per node so that an
   * arc is discarded at most once per phase.
   */
  private static double dinic(final int n, final int[] start, final int[] to, final double[] capacity, final int[] reverse,
      final int source, final int sink, final WorkGuard guard) {
    final int[] level = new int[n];
    final int[] queue = new int[n];
    final int[] current = new int[n];
    // The arcs of the path being built: it only ever descends one level per arc, so it has at most n - 1 of them.
    final int[] path = new int[n];
    double maxFlow = 0.0;
    int work = 0;

    while (true) {
      guard.check();
      Arrays.fill(level, -1);
      level[source] = 0;
      int head = 0, tail = 0;
      queue[tail++] = source;
      while (head < tail && level[sink] < 0) {
        final int u = queue[head++];
        for (int a = start[u], end = start[u + 1]; a < end; a++) {
          guard.checkPeriodically(++work);
          final int v = to[a];
          if (level[v] < 0 && capacity[a] > 0) {
            level[v] = level[u] + 1;
            queue[tail++] = v;
          }
        }
      }
      if (level[sink] < 0)
        return maxFlow;

      System.arraycopy(start, 0, current, 0, n);
      int depth = 0;
      int u = source;
      while (true) {
        guard.checkPeriodically(++work);
        if (u == sink) {
          double pathFlow = Double.POSITIVE_INFINITY;
          for (int k = 0; k < depth; k++)
            if (capacity[path[k]] < pathFlow)
              pathFlow = capacity[path[k]];
          // a path of infinite capacities only: nothing bounds the flow, and no arc of the path would ever be saturated
          if (pathFlow == Double.POSITIVE_INFINITY)
            return Double.POSITIVE_INFINITY;

          int firstSaturated = -1;
          for (int k = 0; k < depth; k++) {
            final int a = path[k];
            capacity[a] -= pathFlow;
            capacity[reverse[a]] += pathFlow;
            if (firstSaturated < 0 && capacity[a] <= 0)
              firstSaturated = k;
          }
          maxFlow += pathFlow;

          // Resume from the tail of the first arc the augmentation saturated: the prefix before it still has room.
          depth = firstSaturated;
          u = depth == 0 ? source : to[path[depth - 1]];
          continue;
        }

        final int end = start[u + 1];
        final int nextLevel = level[u] + 1;
        int a = current[u];
        while (a < end && !(capacity[a] > 0 && level[to[a]] == nextLevel))
          a++;
        current[u] = a;

        if (a < end) {
          path[depth++] = a;
          u = to[a];
        } else {
          // Dead end: no path of this phase goes through u any more.
          level[u] = -1;
          if (depth == 0)
            break;
          --depth;
          u = depth == 0 ? source : to[path[depth - 1]];
          current[u]++;
        }
      }
    }
  }
}
