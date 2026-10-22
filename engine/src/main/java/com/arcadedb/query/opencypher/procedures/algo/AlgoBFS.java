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
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.graph.olap.GraphAnalyticalView;
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
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.bfs(startNode, relTypes?, direction?, maxDepth?)
 * <p>
 * Performs a Breadth-First Search (BFS) from the given start node and returns all reachable
 * nodes in BFS order together with their depth from the start node.
 * </p>
 * <p>
 * Every path stops expanding at {@code maxDepth}, so a bounded call reads the edges of the hops it walks and allocates
 * by the nodes it reaches, not by the graph (issue #10539): a ready Graph Analytical View runs the depth-bounded
 * {@link GraphAlgorithms#breadthFirst} kernel over its CSR arrays, a view serving pending changes is walked node by
 * node (or through its packed neighbour view when the walk is expected to pay for it), and without a view the walk
 * loads the records it reaches from the start node rather than every vertex of the database.
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
 * CALL algo.bfs(start, 'KNOWS', 'BOTH', 3)
 * YIELD node, depth
 * RETURN node.name, depth ORDER BY depth
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoBFS extends AbstractAlgoProcedure {
  public static final String NAME = "algo.bfs";

  // First capacity of a walk's result, grown by doubling, so that a bounded walk allocates by what it reaches (issue #10539)
  private static final int INITIAL_CAPACITY = 1024;

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
    return "Breadth-First Search traversal from a start node";
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

    // Try CSR-accelerated path: delegate to parallel BFS on CSR arrays
    final GraphTraversalProvider provider = findProvider(db, relTypes, context);
    // Only while the view is not serving pending changes: the GraphAlgorithms kernel below reads its base CSR
    // arrays directly and sizes its result from the base node mapping, which is neither the current graph nor
    // as wide as the id space the view now reports - see GraphTraversalProvider#hasPendingChanges (issue #6792).
    if (provider instanceof GraphAnalyticalView.Pinned gav && !gav.hasPendingChanges()) {
      context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
      final int startIdx = gav.getNodeId(startNode.getIdentity());
      if (startIdx < 0)
        return Stream.empty();
      // The kernel stops at maxDepth and returns only the nodes it reached, in BFS order (issue #10539). A sized stream:
      // the exact size is what lets a count-only CALL answer without building a row per node (issue #9453)
      final GraphAlgorithms.BFSLevels bfs = GraphAlgorithms.breadthFirst(gav, startIdx, maxDepth, dir, relTypes);
      final int[] order = bfs.order();
      return rows(bfs.size(), i -> gav.getRID(order[i]), bfs.levelStarts(), bfs.levels());
    }

    // No view at all: walk the records from the start node, rather than loading every vertex of the database to reach
    // the few a bounded call asks for (issue #10539)
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
      final int[][] adj = graph.adjacency(dir, relTypes);
      return walkIds(graph, null, adj, n, startIdx, maxDepth, dir, relTypes);
    }

    // The packed neighbour view of a view serving pending changes is built for the whole graph: taken only when the walk is
    // expected to read as many entries as building it writes, and otherwise the walk reads the nodes it reaches one by one
    final NeighborView nv = graph.neighborViewForVisits(expectedVisits(graph.meanDegree(dir, relTypes), maxDepth), dir, relTypes);
    return walkIds(graph, nv, null, n, startIdx, maxDepth, dir, relTypes);
  }

  /**
   * BFS over dense node ids, from a packed neighbour view when {@code nv} is not null, from the materialised {@code adj}
   * when it is not null, and otherwise one node at a time through {@link GraphData#neighborIds}. Allocates a visited
   * bitmap of the node count and a result that grows with the nodes reached.
   */
  private static Stream<Result> walkIds(final GraphData graph, final NeighborView nv, final int[][] adj, final int n,
      final int startIdx, final int maxDepth, final Vertex.DIRECTION dir, final String[] relTypes) {
    final int[] nbrs = nv != null ? nv.neighbors() : null;
    final long[] visited = new long[(n + 63) >>> 6];
    visited[startIdx >>> 6] |= 1L << (startIdx & 63);

    int[] order = new int[Math.min(n, INITIAL_CAPACITY)];
    order[0] = startIdx;
    int size = 1;
    int[] levelStarts = new int[16];
    levelStarts[1] = 1;
    int levels = 1;
    int frontierStart = 0;
    int frontierEnd = 1;
    int depth = 0;

    while (frontierEnd > frontierStart && depth < maxDepth) {
      depth++;
      for (int f = frontierStart; f < frontierEnd; f++) {
        final int v = order[f];
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
        for (int j = from; j < to; j++) {
          final int u = row[j];
          final int word = u >>> 6;
          final long bit = 1L << (u & 63);
          if ((visited[word] & bit) == 0) {
            visited[word] |= bit;
            if (size == order.length)
              order = Arrays.copyOf(order, (int) Math.min(n, (long) order.length * 2));
            order[size++] = u;
          }
        }
      }
      frontierStart = frontierEnd;
      frontierEnd = size;
      if (frontierEnd > frontierStart) {
        if (++levels >= levelStarts.length)
          levelStarts = Arrays.copyOf(levelStarts, levelStarts.length * 2);
        levelStarts[levels] = frontierEnd;
      }
    }

    final int[] reached = order;
    return rows(size, i -> graph.getRID(reached[i]), levelStarts, levels);
  }

  /**
   * BFS over the records, from the start vertex: loads the vertices it reaches and nothing else, and charges them to the
   * call's memory budget as {@code loadGraph} charges every vertex of the database (issue #10539). The edges are read as an
   * algorithm reads them, the incoming side of a unidirectional type included (issue #8629), and an entry whose edge or far
   * vertex is gone is skipped and reported, as the adjacency build skips it.
   */
  private Stream<Result> walkRecords(final Database db, final Vertex start, final String[] relTypes, final Vertex.DIRECTION dir,
      final int maxDepth, final CommandContext context) {
    final MemoryBudget memory = newMemoryBudget(db);
    final long capacity = memory.capacityFor(OLTP_VERTEX_BYTES);
    final CommandContext edgeContext = incomingSideContext(db, context);

    final Set<RID> visited = new HashSet<>();
    visited.add(start.getIdentity());
    final List<Vertex> order = new ArrayList<>();
    order.add(start);
    int[] levelStarts = new int[16];
    levelStarts[1] = 1;
    int levels = 1;
    int frontierStart = 0;
    int frontierEnd = 1;
    int depth = 0;

    while (frontierEnd > frontierStart && depth < maxDepth) {
      depth++;
      for (int f = frontierStart; f < frontierEnd; f++) {
        final Vertex v = order.get(f);
        final RID vid = v.getIdentity();
        for (final Edge e : edgesOf(edgeContext, v, dir, relTypes)) {
          final RID nid;
          try {
            nid = neighborRid(e, vid, dir);
          } catch (final RecordNotFoundException rnf) {
            GhostEdgeReporter.reportSkipped(rnf);
            continue;
          }
          // A damaged edge can carry no endpoint: there is no vertex to reach through it
          if (nid == null || !visited.add(nid))
            continue;
          final Vertex u;
          try {
            u = nid.asVertex();
          } catch (final RecordNotFoundException rnf) {
            GhostEdgeReporter.reportSkipped(rnf);
            continue;
          }
          if (order.size() >= capacity)
            memory.reserve(saturatingProduct(order.size() + 1L, OLTP_VERTEX_BYTES), "the visited vertices",
                "at least " + (order.size() + 1) + " nodes");
          order.add(u);
        }
      }
      frontierStart = frontierEnd;
      frontierEnd = order.size();
      if (frontierEnd > frontierStart) {
        if (++levels >= levelStarts.length)
          levelStarts = Arrays.copyOf(levelStarts, levelStarts.length * 2);
        levelStarts[levels] = frontierEnd;
      }
    }
    memory.reserve(saturatingProduct(order.size(), OLTP_VERTEX_BYTES), "the visited vertices", order.size() + " nodes");

    return rows(order.size(), i -> order.get(i).getIdentity(), levelStarts, levels);
  }

  /**
   * One row per reached node but the start (index 0), in BFS order. A sized stream: the exact size is what lets a
   * count-only CALL answer without building a row per node (issue #9453).
   */
  private static Stream<Result> rows(final int size, final IntFunction<RID> rid, final int[] levelStarts, final int levels) {
    return IntStream.range(1, size).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", rid.apply(i));
      r.setProperty("depth", GraphAlgorithms.BFSLevels.depthAt(levelStarts, levels, i));
      return (Result) r;
    });
  }
}
