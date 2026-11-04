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
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.utility.NumberUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.dfs(startNode, relTypes?, direction?, maxDepth?)
 * <p>
 * Performs a Depth-First Search (DFS) from the given start node and returns all reachable
 * nodes in DFS discovery order together with their depth from the start node.
 * </p>
 * <p>
 * A bounded call reads the edges of the nodes it reaches and allocates by them, not by the graph (issue #10580, the
 * defect #10539 fixed for {@code algo.bfs}): over a Graph Analytical View the walk reads the rows of the nodes it reaches
 * from the CSR (through its packed neighbour view when the walk is expected to pay for it), and without a view it loads the
 * records it reaches from the start node rather than every vertex of the database. The visited set is a bitmap of the node
 * count, and the stack and the result grow by doubling with the nodes reached, both priced to the call's memory budget.
 * </p>
 * <p>
 * Parameters:
 * <ul>
 *   <li>startNode (required): the node from which to start the traversal</li>
 *   <li>relTypes (optional): relationship types to traverse (null = all)</li>
 *   <li>direction (optional): "OUT", "IN", or "BOTH" (default "BOTH")</li>
 *   <li>maxDepth (optional): maximum depth to traverse (default unbounded)</li>
 * </ul>
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (start:Person {name: 'Alice'})
 * CALL algo.dfs(start, 'KNOWS', 'BOTH', 3)
 * YIELD node, depth
 * RETURN node.name, depth
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoDFS extends AbstractAlgoProcedure {
  public static final String NAME = "algo.dfs";

  // First capacity of a walk's stack and result, grown by doubling, so that a bounded walk allocates by what it reaches (issue #10580)
  private static final int  INITIAL_CAPACITY     = 16;
  // What one reached node costs the walk: its entry on the stack (node and depth) and in the result (node and depth)
  private static final long TRAVERSAL_ENTRY_BYTES = 4 * INT_BYTES;
  // What one neighbour held while the walk reads a vertex costs: the RID the edge answered and the slot that holds it
  private static final long OLTP_ROW_ENTRY_BYTES  = 40L;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Depth-First Search traversal from a start node";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "depth");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode     = extractVertex(args[0], "startNode");
    final String[] relTypes    = extractRelTypes(argOrDefault(args, 1, null), context);
    final Vertex.DIRECTION dir = parseDirection(extractString(argOrDefault(args, 2, "BOTH"), "direction"));
    final int maxDepth         = NumberUtils.saturateToInt(optionalNumberArg(args, 3, "maxDepth", Integer.MAX_VALUE));

    final Database db = context.getDatabase();

    // No view at all: walk the records from the start node, rather than loading every vertex of the database to reach the few
    // a bounded call asks for (issue #10580)
    final GraphTraversalProvider provider = findProvider(db, relTypes, context);
    if (provider == null)
      return walkRecords(db, startNode, relTypes, dir, maxDepth, context);

    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    final int startIdx = graph.indexOf(startNode.getIdentity());
    if (startIdx < 0)
      return Stream.empty();

    if (!graph.isCSRBacked()) {
      // The view went away between the two lookups: the vertices are loaded already, so walk their adjacency
      return walkIds(graph, null, graph.adjacency(dir, relTypes), n, startIdx, maxDepth, dir, relTypes);
    }

    // The packed neighbour view of a view serving pending changes is built for the whole graph: taken only when the walk is
    // expected to read as many entries as building it writes, and otherwise the walk reads the nodes it reaches one by one
    final NeighborView nv = graph.neighborViewForVisits(expectedVisits(graph.meanDegree(dir, relTypes), maxDepth), dir, relTypes);
    return walkIds(graph, nv, null, n, startIdx, maxDepth, dir, relTypes);
  }

  /**
   * DFS over dense node ids, from a packed neighbour view when {@code nv} is not null, from the materialised {@code adj} when
   * it is not null, and otherwise one node at a time through {@link GraphData#neighborIds}. Iterative, with an explicit stack
   * to avoid stack overflow on large graphs. A node is marked when it is pushed, the neighbours are pushed in reverse so the
   * first is popped first, and a node is reported when it is popped, at the depth of the node that pushed it plus one.
   * Allocates a visited bitmap of the node count, and a stack and a result that grow with the nodes reached.
   */
  private Stream<Result> walkIds(final GraphData graph, final NeighborView nv, final int[][] adj, final int n, final int startIdx,
      final int maxDepth, final Vertex.DIRECTION dir, final String[] relTypes) {
    final MemoryBudget memory = graph.memory();
    memory.reserve(saturatingProduct((n + 63L) >>> 6, LONG_BYTES), "the visited bitmap", n + " nodes");
    final long[] visited = new long[(n + 63) >>> 6];
    final int[] nbrs = nv != null ? nv.neighbors() : null;

    // Neither the stack nor the result can hold more than the node count: a node is pushed once, and the start is not reported
    final int firstCapacity = Math.min(n, INITIAL_CAPACITY);
    memory.reserve(saturatingProduct(firstCapacity, TRAVERSAL_ENTRY_BYTES), "the traversal state", firstCapacity + " nodes");
    int[] stack = new int[firstCapacity];
    int[] stackDepth = new int[firstCapacity];
    int[] order = new int[firstCapacity];
    int[] depths = new int[firstCapacity];
    int size = 0;

    int top = 0;
    stack[top] = startIdx;
    stackDepth[top] = 0;
    top++;
    visited[startIdx >>> 6] |= 1L << (startIdx & 63);

    while (top > 0) {
      top--;
      final int v = stack[top];
      final int d = stackDepth[top];

      if (d > 0) {
        // Don't emit the start node itself
        if (size == order.length) {
          final int grown = (int) Math.min(n, (long) order.length * 2);
          memory.reserve(saturatingProduct(grown - order.length, 2 * INT_BYTES), "the traversal state", "at least " + grown + " nodes");
          order = Arrays.copyOf(order, grown);
          depths = Arrays.copyOf(depths, grown);
        }
        order[size] = v;
        depths[size] = d;
        size++;
      }

      if (d < maxDepth) {
        final int[] row;
        final int from;
        final int to;
        if (nv != null) {
          row = nbrs;
          from = nv.offset(v);
          to = nv.offsetEnd(v);
        } else {
          row = adj != null ? adj[v] : graph.neighborIds(v, dir, relTypes);
          from = 0;
          to = row.length;
        }
        // Push neighbors in reverse order so that forward order is preserved
        for (int j = to - 1; j >= from; j--) {
          final int u = row[j];
          final int word = u >>> 6;
          final long bit = 1L << (u & 63);
          if ((visited[word] & bit) == 0) {
            visited[word] |= bit;
            if (top == stack.length) {
              final int grown = (int) Math.min(n, (long) stack.length * 2);
              memory.reserve(saturatingProduct(grown - stack.length, 2 * INT_BYTES), "the traversal state", "at least " + grown + " nodes");
              stack = Arrays.copyOf(stack, grown);
              stackDepth = Arrays.copyOf(stackDepth, grown);
            }
            stack[top] = u;
            stackDepth[top] = d + 1;
            top++;
          }
        }
      }
    }

    final int[] reached = order;
    final int[] reachedDepths = depths;
    return rows(size, i -> graph.getRID(reached[i]), i -> reachedDepths[i]);
  }

  /**
   * DFS over the records, from the start vertex: loads the vertices it reaches and nothing else, and charges them to the
   * call's memory budget as {@code loadGraph} charges every vertex of the database (issue #10580). The edges are read as an
   * algorithm reads them, the incoming side of a unidirectional type included (issue #8629), and an entry whose edge or far
   * vertex is gone is skipped and reported, as the adjacency build skips it.
   */
  private Stream<Result> walkRecords(final Database db, final Vertex start, final String[] relTypes, final Vertex.DIRECTION dir,
      final int maxDepth, final CommandContext context) {
    final MemoryBudget memory = newMemoryBudget(db);
    long capacity = memory.capacityFor(OLTP_VERTEX_BYTES);
    final CommandContext edgeContext = incomingSideContext(db, context);

    final Set<RID> visited = new HashSet<>();
    visited.add(start.getIdentity());
    // The vertices loaded so far, the start included: what the budget is charged for
    long loaded = 1;
    final List<RID> order = new ArrayList<>();
    int[] depths = new int[INITIAL_CAPACITY];

    Vertex[] stack = new Vertex[INITIAL_CAPACITY];
    int[] stackDepth = new int[INITIAL_CAPACITY];
    RID[] row = new RID[INITIAL_CAPACITY];
    int top = 0;
    stack[top] = start;
    stackDepth[top] = 0;
    top++;

    while (top > 0) {
      top--;
      final Vertex v = stack[top];
      final int d = stackDepth[top];
      stack[top] = null;

      if (d > 0) {
        // Don't emit the start node itself
        if (order.size() == depths.length)
          depths = Arrays.copyOf(depths, depths.length * 2);
        depths[order.size()] = d;
        order.add(v.getIdentity());
      }

      if (d < maxDepth) {
        // The row of this vertex, as the edges are read, without the vertices met already: they are skipped whatever their
        // place in the row, and so are never worth a slot. What is left is walked backwards below, as the rows of the view are
        final RID vid = v.getIdentity();
        int rowSize = 0;
        for (final Edge e : edgesOf(edgeContext, v, dir, relTypes)) {
          final RID nid;
          try {
            nid = neighborRid(e, vid, dir);
          } catch (final RecordNotFoundException rnf) {
            GhostEdgeReporter.reportSkipped(rnf);
            continue;
          }
          // A damaged edge can carry no endpoint: there is no vertex to reach through it
          if (nid == null || visited.contains(nid))
            continue;
          if (rowSize == row.length) {
            // One buffer for the whole walk, charged by what it grows by (the old length, as it doubles), so for the most it held
            memory.reserve(saturatingProduct(row.length, OLTP_ROW_ENTRY_BYTES), "the neighbours of a vertex",
                "at least " + row.length * 2 + " entries");
            row = Arrays.copyOf(row, row.length * 2);
          }
          row[rowSize++] = nid;
        }

        // Push neighbors in reverse order so that forward order is preserved
        for (int j = rowSize - 1; j >= 0; j--) {
          final RID nid = row[j];
          row[j] = null;
          if (!visited.add(nid))
            continue;
          final Vertex u;
          try {
            u = nid.asVertex();
          } catch (final RecordNotFoundException rnf) {
            // A far vertex that is gone stays marked as met, so another entry naming it is not read again, and it is not
            // loaded, so it is not charged
            GhostEdgeReporter.reportSkipped(rnf);
            continue;
          }
          if (loaded >= capacity)
            capacity = memory.extendCapacity(loaded + 1, OLTP_VERTEX_BYTES, "the visited vertices",
                "at least " + (loaded + 1) + " nodes");
          loaded++;
          if (top == stack.length) {
            stack = Arrays.copyOf(stack, stack.length * 2);
            stackDepth = Arrays.copyOf(stackDepth, stackDepth.length * 2);
          }
          stack[top] = u;
          stackDepth[top] = d + 1;
          top++;
        }
      }
    }
    memory.reserve(saturatingProduct(loaded, OLTP_VERTEX_BYTES), "the visited vertices", loaded + " nodes");

    final int[] reachedDepths = depths;
    return rows(order.size(), order::get, i -> reachedDepths[i]);
  }

  /**
   * One row per reached node but the start, in DFS order. A sized stream: the exact size is what lets a count-only CALL answer
   * without building a row per node (issue #9453).
   */
  private static Stream<Result> rows(final int size, final IntFunction<RID> rid, final IntUnaryOperator depth) {
    return IntStream.range(0, size).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", rid.apply(i));
      r.setProperty("depth", depth.applyAsInt(i));
      return (Result) r;
    });
  }
}
