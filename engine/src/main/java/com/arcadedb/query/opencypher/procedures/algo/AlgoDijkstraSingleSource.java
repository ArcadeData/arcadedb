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
import com.arcadedb.graph.EdgeWeight;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Procedure: algo.dijkstra.singleSource(startNode, relTypes, weightProperty, direction?)
 * <p>
 * Computes the single-source shortest path (SSSP) from a given start node to all reachable
 * nodes using Dijkstra's algorithm with a binary min-heap. This extends the existing
 * source-target {@code algo.dijkstra} to return results for all reachable targets at once.
 * </p>
 * <p>
 * When a Graph Analytical View (GAV) with edge properties is available, the algorithm runs
 * entirely on CSR arrays with zero OLTP access, providing massive speedups on large graphs.
 * </p>
 * <p>
 * Parameters:
 * <ul>
 *   <li>startNode (required): source vertex</li>
 *   <li>relTypes (required): relationship type(s) to traverse</li>
 *   <li>weightProperty (required): edge property to use as weight (must be numeric)</li>
 *   <li>direction (optional): "OUT", "IN", or "BOTH" (default "OUT")</li>
 * </ul>
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (start:City {name: 'London'})
 * CALL algo.dijkstra.singleSource(start, 'ROAD', 'distance')
 * YIELD node, cost
 * RETURN node.name, cost ORDER BY cost
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoDijkstraSingleSource extends AbstractAlgoProcedure {
  public static final String NAME = "algo.dijkstra.singleSource";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Single-source shortest path (Dijkstra) from a start node to all reachable nodes";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "cost");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode      = extractVertex(args[0], "startNode");
    final String[] relTypes     = extractRelTypes(args[1], context);
    final String weightProperty = extractString(args[2], "weightProperty");
    final Vertex.DIRECTION dir  = parseDirection(extractString(argOrDefault(args, 3, "OUT"), "direction"));

    final Database db = context.getDatabase();

    // Try CSR-accelerated path: requires a provider with edge properties
    final GraphTraversalProvider provider = findProvider(db, relTypes, context);
    // Not the coarser hasEdgeProperties(): the CSR kernel below reads the weight column directly and falls back
    // to a unit weight when it is missing, so a view materialising some OTHER property would silently answer an
    // unweighted shortest path to a weighted question (issue #6301).
    // Unlike the topology-only kernels (algo.bfs, algo.pagerank, ...), which stay overlay-blind and refuse
    // outright via hasPendingChanges(), GraphAlgorithms.dijkstraSingleSource resolves an active overlay itself -
    // through GraphTraversalProvider#edgeWeightsOf per popped node, sized against the overlay's own id-space
    // upper bound rather than the base node mapping (issue #6792) - so this procedure no longer has to refuse
    // the whole call just because a commit landed since the view was last built (issue #6791).
    if (provider instanceof GraphAnalyticalView.Pinned gav && gav.servesEdgeProperty(weightProperty, relTypes)) {
      final Stream<Result> accelerated = executeWithCSR(gav, startNode.getIdentity(), relTypes, weightProperty, dir);
      // Null means the kernel refused outright: a node it popped could not be answered exactly even through
      // the overlay-aware fallback GraphAlgorithms.dijkstraSingleSource takes while an overlay is active
      // (issue #6791) - an ambiguous parallel-edge deletion is the one case that can still happen.
      if (accelerated != null) {
        context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
        return accelerated;
      }
    }

    // Fall back to OLTP path
    return executeWithOLTP(db, startNode, relTypes, weightProperty, dir, context);
  }

  private Stream<Result> executeWithCSR(final GraphAnalyticalView.Pinned gav, final RID startRid,
      final String[] relTypes, final String weightProperty, final Vertex.DIRECTION dir) {
    // The exclusive bound of the dense id space, not the live node count: while an overlay is active the two
    // diverge (issue #6792) - an added node's id sits above the base mapping regardless of how many nodes are
    // currently live, and enumerating only up to the live count would silently drop it from the results below.
    final int n = gav.getNodeIdUpperBound();
    if (n == 0)
      return Stream.empty();

    final int src = gav.getNodeId(startRid);
    if (src < 0)
      return Stream.empty();

    final double[] dist = GraphAlgorithms.dijkstraSingleSource(
        gav, src, weightProperty, dir, relTypes);
    if (dist == null)
      return null; // a popped node could not be answered exactly; the caller reads the edges instead
    // The reached ids up front rather than a filter() on the stream: a filtered stream no longer knows its size, and
    // the exact size is what lets a count-only CALL answer without building a row per node (issue #9453)
    final int[] reached = new int[n];
    int reachable = 0;
    for (int i = 0; i < n; i++)
      if (i != src && dist[i] < Double.POSITIVE_INFINITY)
        reached[reachable++] = i;

    return Arrays.stream(reached, 0, reachable).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", gav.getRID(i));
      r.setProperty("cost", dist[i]);
      return (Result) r;
    });
  }

  /** Entries the row buffers start with: a row of that many or fewer is priced by the batch of entries reserved with the rows. */
  private static final int INITIAL_ROW_BUFFER = 16;

  private Stream<Result> executeWithOLTP(final Database db, final Vertex startNode,
      final String[] relTypes, final String weightProperty, final Vertex.DIRECTION dir, final CommandContext context) {
    final GraphData graph = loadGraphFromRecords(db, null, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    final int src = graph.indexOf(startNode.getIdentity());
    if (src < 0)
      return Stream.empty();

    // Build weighted adjacency once to avoid OLTP per-vertex during Dijkstra. Priced against the call's budget as the rows are built,
    // with the distances, the reached ids and the frontier beside them (issue #12554)
    final MemoryBudget memory = graph.memory();
    memory.reserve(saturatingSum(saturatingProduct(n, 2 * MATRIX_ROW_OVERHEAD_BYTES), saturatingProduct(n, DOUBLE_BYTES + 3 * INT_BYTES)),
        "the weighted adjacency rows and the search state", n + " nodes");
    final Set<String> relTypeSet = relTypes != null ? new HashSet<>(Arrays.asList(relTypes)) : null;
    final int[][] adjNeighbors = new int[n][];
    final double[][] adjWeights = new double[n][];
    // One growable primitive buffer per column, reused across every node and copied out at its exact size
    int[] nbrBuf = new int[INITIAL_ROW_BUFFER];
    double[] wtBuf = new double[INITIAL_ROW_BUFFER];
    long unpriced = 0;
    for (int i = 0; i < n; i++) {
      // A vertex deleted since the graph was loaded has no edges
      final Vertex v = graph.getVertex(i);
      int count = 0;
      if (v != null) {
        // the incoming side of a unidirectional edge type is on the other end only (issue #8629)
        for (final Edge edge : edgesOf(context, v, dir, relTypes)) {
          if (relTypeSet != null && !relTypeSet.contains(edge.getTypeName()))
            continue;
          final RID neighborRid = neighborRid(edge, v.getIdentity(), dir);
          final int nbrIdx = neighborRid != null ? graph.indexOf(neighborRid) : -1;
          if (nbrIdx < 0)
            continue;
          // the rule every weighted path finder shares (issue #9443): a NaN weight used to slip past `weight < 0`
          final double weight = weightProperty != null ? EdgeWeight.of(edge.get(weightProperty)) : EdgeWeight.MISSING;
          if (!EdgeWeight.isWalkable(weight))
            continue;
          if (count == nbrBuf.length) {
            // Priced before the buffers are allocated: a supernode doubles them again and again. They stay for the rows that follow, so
            // each growth is charged once, by the entries it adds, and the copy of a row is charged beside it with the batch below
            final int newLength = (int) Math.min(2L * count, Integer.MAX_VALUE - 8L);
            memory.reserve(saturatingProduct(newLength - count, INT_BYTES + DOUBLE_BYTES), "the neighbour buffers of one node",
                "a row of more than " + count + " edge entries");
            nbrBuf = Arrays.copyOf(nbrBuf, newLength);
            wtBuf = Arrays.copyOf(wtBuf, newLength);
          }
          nbrBuf[count] = nbrIdx;
          wtBuf[count] = weight;
          count++;
        }
      }
      // The copy of a long row is refused before it is made when the budget cannot hold it; the batch below reserves it
      if (count > INITIAL_ROW_BUFFER && count > memory.capacityFor(INT_BYTES + DOUBLE_BYTES))
        memory.extendCapacity(count, INT_BYTES + DOUBLE_BYTES, "the weighted adjacency list", "a row of " + count + " edge entries");
      adjNeighbors[i] = Arrays.copyOf(nbrBuf, count);
      adjWeights[i] = Arrays.copyOf(wtBuf, count);
      unpriced += count;
      // The batch is capped by what the budget has left, recomputed after every reservation, as weightedAdjacency does: a fixed batch
      // could be allocated past a budget that is nearly spent before it is refused
      final long checkpoint = Math.min(ADJACENCY_CHECKPOINT_ENTRIES / 3, memory.capacityFor(INT_BYTES + DOUBLE_BYTES));
      if (unpriced >= checkpoint || (i & 1023) == 1023) {
        memory.reserve(saturatingProduct(unpriced, INT_BYTES + DOUBLE_BYTES), "the weighted adjacency list", unpriced + " edge entries");
        unpriced = 0;
      }
    }
    memory.reserve(saturatingProduct(unpriced, INT_BYTES + DOUBLE_BYTES), "the weighted adjacency list", unpriced + " edge entries");

    // Dijkstra purely in-memory
    final double[] dist = new double[n];
    Arrays.fill(dist, Double.POSITIVE_INFINITY);
    dist[src] = 0.0;

    // The frontier is an indexed binary heap over primitive arrays: a node is in it at most once, and a shorter way to it moves it up
    // instead of adding an entry beside the stale one. A heap of one boxed entry per relaxation held up to one array per edge, which no
    // reservation could see (issue #12554); this one is the two int arrays of the node count priced above
    final int[] heap = new int[n];
    final int[] where = new int[n];
    Arrays.fill(where, -1);
    int size = 1;
    heap[0] = src;
    where[src] = 0;

    while (size > 0) {
      final int u = heap[0];
      where[u] = -1;
      if (--size > 0) {
        heap[0] = heap[size];
        where[heap[0]] = 0;
        siftDown(heap, where, dist, size, 0);
      }
      final double d = dist[u];

      for (int j = 0; j < adjNeighbors[u].length; j++) {
        final int v = adjNeighbors[u][j];
        final double newDist = d + adjWeights[u][j];
        if (newDist < dist[v]) {
          dist[v] = newDist;
          if (where[v] < 0) {
            heap[size] = v;
            where[v] = size++;
          }
          siftUp(heap, where, dist, where[v]);
        }
      }
    }

    // Sized, like the CSR path: see the comment there (issue #9453)
    final int[] reached = new int[n];
    int reachable = 0;
    for (int i = 0; i < n; i++)
      if (i != src && dist[i] < Double.POSITIVE_INFINITY)
        reached[reachable++] = i;

    return Arrays.stream(reached, 0, reachable).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(i));
      r.setProperty("cost", dist[i]);
      return (Result) r;
    });
  }

  /** Moves the node at {@code i} up until its parent is not further away; {@code where[node]} is the position of a node in the heap. */
  private static void siftUp(final int[] heap, final int[] where, final double[] key, final int start) {
    int i = start;
    final int node = heap[i];
    while (i > 0) {
      final int parent = (i - 1) >>> 1;
      if (key[heap[parent]] <= key[node])
        break;
      heap[i] = heap[parent];
      where[heap[i]] = i;
      i = parent;
    }
    heap[i] = node;
    where[node] = i;
  }

  /** Moves the node at {@code i} down until none of its children is nearer. */
  private static void siftDown(final int[] heap, final int[] where, final double[] key, final int size, final int start) {
    int i = start;
    final int node = heap[i];
    final int half = size >>> 1;
    while (i < half) {
      int child = 2 * i + 1;
      if (child + 1 < size && key[heap[child + 1]] < key[heap[child]])
        child++;
      if (key[node] <= key[heap[child]])
        break;
      heap[i] = heap[child];
      where[heap[i]] = i;
      i = child;
    }
    heap[i] = node;
    where[node] = i;
  }
}
