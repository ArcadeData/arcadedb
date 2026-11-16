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

import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * The neighbour rows a procedure anchored on one or two vertices reads (random walk, link prediction): the rows of the nodes
 * it reaches from them, one node at a time, rather than the adjacency of the whole graph (issue #12176). A row holds what
 * {@link AbstractAlgoProcedure.GraphData#adjacency} holds for the node, in the same order and with the same multiplicity, so a
 * procedure answers the same whichever way the rows are read.
 * <p>
 * Without a view the rows come from the records of the vertices reached ({@link OfRecords}), as {@code algo.bfs} walks them
 * (issue #10539); with one, from the view, row by row ({@link OfGraph}).
 */
abstract class Neighborhood {
  private static final int[] NO_NEIGHBORS = new int[0];

  /** The node of {@code vertex}, or -1 when the graph does not hold it. */
  abstract int nodeOf(Vertex vertex);

  /** The neighbours of {@code node} in {@code dir}, as the row of {@link AbstractAlgoProcedure.GraphData#adjacency}. */
  abstract int[] neighbors(int node, Vertex.DIRECTION dir);

  abstract RID ridOf(int node);

  /** The budget of the call, which a procedure charges its own working set to. */
  abstract AbstractAlgoProcedure.MemoryBudget memory();

  /** Every node of the graph, in the order the graph numbers them: only a graph loaded whole knows them. */
  abstract int[] allNodes();

  /** Sorts {@code nodes} in the order the graph loaded whole numbers them, which is the order a procedure reports its rows in. */
  abstract void sortInGraphOrder(int[] nodes);

  /** Charges what was read to the call's budget, once the procedure has read everything it needs. */
  abstract void reserveVisited();

  /**
   * The nodes whose row in {@code dir} holds one of {@code targets} (a sorted set), in graph order: the rows that reach a node in
   * {@code dir} are its row in the opposite direction.
   */
  int[] nodesReaching(final int[] targets, final Vertex.DIRECTION dir) {
    final Vertex.DIRECTION reverse = switch (dir) {
      case OUT -> Vertex.DIRECTION.IN;
      case IN -> Vertex.DIRECTION.OUT;
      default -> Vertex.DIRECTION.BOTH;
    };
    final Set<Integer> reaching = new HashSet<>();
    for (final int target : targets)
      for (final int node : neighbors(target, reverse))
        reaching.add(node);
    final int[] nodes = new int[reaching.size()];
    int i = 0;
    for (final int node : reaching)
      nodes[i++] = node;
    sortInGraphOrder(nodes);
    return nodes;
  }

  /** The distinct entries of a row, sorted for {@link #contains}. */
  static int[] distinctSorted(final int[] row) {
    return Arrays.stream(row).sorted().distinct().toArray();
  }

  static boolean contains(final int[] sortedSet, final int node) {
    return Arrays.binarySearch(sortedSet, node) >= 0;
  }

  /**
   * The rows of a loaded graph: read from the view node by node for a procedure that reads a few nodes, or from the adjacency of
   * every node, built once per direction, for one that reads them all or when there is no view to read a row from.
   */
  static final class OfGraph extends Neighborhood {
    private final AbstractAlgoProcedure.GraphData graph;
    private final String[]                        relTypes;
    private final boolean                         rowByRow;
    private final int[][][]                       adjacency = new int[Vertex.DIRECTION.values().length][][];

    OfGraph(final AbstractAlgoProcedure.GraphData graph, final String[] relTypes, final boolean wholeGraph) {
      this.graph = graph;
      this.relTypes = relTypes;
      this.rowByRow = !wholeGraph && graph.isCSRBacked();
    }

    @Override
    int nodeOf(final Vertex vertex) {
      return graph.indexOf(vertex.getIdentity());
    }

    @Override
    int[] neighbors(final int node, final Vertex.DIRECTION dir) {
      if (rowByRow)
        return graph.neighborIds(node, dir, relTypes);
      if (adjacency[dir.ordinal()] == null)
        adjacency[dir.ordinal()] = graph.adjacency(dir, relTypes);
      return adjacency[dir.ordinal()][node];
    }

    @Override
    RID ridOf(final int node) {
      return graph.getRID(node);
    }

    @Override
    AbstractAlgoProcedure.MemoryBudget memory() {
      return graph.memory();
    }

    @Override
    int[] allNodes() {
      return IntStream.range(0, graph.nodeCount).toArray();
    }

    @Override
    void sortInGraphOrder(final int[] nodes) {
      Arrays.sort(nodes);
    }

    @Override
    void reserveVisited() {
      // THE GRAPH WAS CHARGED WHEN IT WAS LOADED, ITS ADJACENCY WHEN IT WAS BUILT
    }
  }

  /**
   * The rows of the vertices reached from the records, loading each vertex the first time an edge reaches it. The edges are read
   * as an algorithm reads them, the incoming side of a unidirectional type included (issue #8629), and an entry whose edge or far
   * vertex is gone is skipped and reported, as {@code algo.bfs} skips it. Each vertex loaded is charged to the call's budget as
   * loading the whole graph charges every vertex of it.
   */
  static final class OfRecords extends Neighborhood {
    private final CommandContext                     edgeContext;
    private final String[]                           relTypes;
    private final AbstractAlgoProcedure.MemoryBudget memory;
    private       long                               capacity;
    // A VERTEX REACHED TO ITS NODE, -1 FOR ONE THAT IS GONE
    private final Map<RID, Integer>                  nodes    = new HashMap<>();
    private final List<Vertex>                       vertices = new ArrayList<>();
    // A ROW READ ONCE, BY NODE << 2 | DIRECTION
    private final Map<Long, int[]>                   rows     = new HashMap<>();

    OfRecords(final CommandContext edgeContext, final String[] relTypes, final AbstractAlgoProcedure.MemoryBudget memory) {
      this.edgeContext = edgeContext;
      this.relTypes = relTypes;
      this.memory = memory;
      this.capacity = memory.capacityFor(AbstractAlgoProcedure.OLTP_VERTEX_BYTES);
    }

    @Override
    int nodeOf(final Vertex vertex) {
      final Integer node = nodes.get(vertex.getIdentity());
      return node != null ? node : add(vertex);
    }

    @Override
    int[] neighbors(final int node, final Vertex.DIRECTION dir) {
      final long key = ((long) node << 2) | dir.ordinal();
      final int[] cached = rows.get(key);
      if (cached != null)
        return cached;

      final Vertex vertex = vertices.get(node);
      final RID vid = vertex.getIdentity();
      int[] row = NO_NEIGHBORS;
      int size = 0;
      for (final Edge e : AbstractAlgoProcedure.edgesOf(edgeContext, vertex, dir, relTypes)) {
        final RID nid;
        try {
          nid = GraphEngine.neighborRid(e, vid, dir);
        } catch (final RecordNotFoundException rnf) {
          GhostEdgeReporter.reportSkipped(rnf);
          continue;
        }
        // A damaged edge can carry no endpoint: there is no vertex to reach through it
        if (nid == null)
          continue;
        final int neighbor = nodeOf(nid);
        if (neighbor < 0)
          continue;
        if (size == row.length)
          row = Arrays.copyOf(row, Math.max(8, size * 2));
        row[size++] = neighbor;
      }
      final int[] result = size == row.length ? row : Arrays.copyOf(row, size);
      rows.put(key, result);
      return result;
    }

    private int nodeOf(final RID rid) {
      final Integer known = nodes.get(rid);
      if (known != null)
        return known;
      final Vertex vertex;
      try {
        vertex = rid.asVertex();
      } catch (final RecordNotFoundException rnf) {
        GhostEdgeReporter.reportSkipped(rnf);
        nodes.put(rid, -1);
        return -1;
      }
      return add(vertex);
    }

    private int add(final Vertex vertex) {
      if (vertices.size() >= capacity)
        capacity = memory.extendCapacity(vertices.size() + 1L, AbstractAlgoProcedure.OLTP_VERTEX_BYTES, "the visited vertices",
            "at least " + (vertices.size() + 1) + " nodes");
      final int node = vertices.size();
      vertices.add(vertex);
      nodes.put(vertex.getIdentity(), node);
      return node;
    }

    @Override
    RID ridOf(final int node) {
      return vertices.get(node).getIdentity();
    }

    @Override
    AbstractAlgoProcedure.MemoryBudget memory() {
      return memory;
    }

    @Override
    int[] allNodes() {
      throw new IllegalStateException("a walk of the records does not know every node of the graph");
    }

    /** By RID: the order the whole graph is loaded in, type by type, bucket by bucket, for the vertices of one type. */
    @Override
    void sortInGraphOrder(final int[] nodes) {
      final Integer[] boxed = Arrays.stream(nodes).boxed().toArray(Integer[]::new);
      Arrays.sort(boxed, (a, b) -> ridOf(a).compareTo(ridOf(b)));
      for (int i = 0; i < nodes.length; i++)
        nodes[i] = boxed[i];
    }

    @Override
    void reserveVisited() {
      memory.reserve(AbstractAlgoProcedure.saturatingProduct(vertices.size(), AbstractAlgoProcedure.OLTP_VERTEX_BYTES),
          "the visited vertices", vertices.size() + " nodes");
    }
  }
}
