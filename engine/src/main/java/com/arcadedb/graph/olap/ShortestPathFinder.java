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
package com.arcadedb.graph.olap;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.NodeEdgeWeights;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.IntIntHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Point-to-point weighted shortest paths for the {@code algo.cch.shortestPath} procedure and the {@code cchShortestPath}
 * SQL function (issue #9437), answered by the fastest exact means available:
 * <ol>
 *   <li>a {@link ContractionHierarchy} prepared for the snapshot a ready view is serving;</li>
 *   <li>otherwise bidirectional Dijkstra on a ready view that materializes the weight;</li>
 *   <li>otherwise bidirectional Dijkstra on the vertex and edge records, which is also what a transaction holding
 *       uncommitted changes always gets, since no view can see them.</li>
 * </ol>
 * All three share one definition of the weight: the edge property's numeric value, 1 when the edge has none, and an edge
 * whose value is negative or NaN is not walked. Among parallel edges only the cheapest counts.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ShortestPathFinder {
  /** What answered a query. */
  public enum Engine {
    CONTRACTION_HIERARCHY, VIEW, RECORDS
  }

  /** A shortest path: its vertices in walking order, its total weight, and what computed it. */
  public record Result(List<RID> vertices, double weight, Engine engine) {
  }

  private static final double MISSING_WEIGHT = 1.0;

  private ShortestPathFinder() {
  }

  /**
   * The shortest path from {@code source} to {@code target}.
   *
   * @param direction  OUT follows edges forward, IN backward, BOTH ignores their direction
   * @param edgeTypes  the edge types to walk; null or empty for every type
   * @param guard      checked while a search runs, so the command's timeout and interrupt stop it; null for none
   *
   * @return the path, or null when {@code target} cannot be reached
   */
  public static Result find(final Database database, final RID source, final RID target, final String weightProperty,
      final Vertex.DIRECTION direction, final String[] edgeTypes, final WorkGuard guard) {
    final String[] types = edgeTypes == null || edgeTypes.length == 0 ? null : edgeTypes;
    if (source.equals(target))
      return new Result(List.of(source), 0, Engine.RECORDS);

    final ContractionHierarchy hierarchy = ContractionHierarchy.find(database, weightProperty, types);
    if (hierarchy != null) {
      final ContractionHierarchy.Path path = hierarchy.shortestPath(source, target, direction);
      if (path != null)
        return path == ContractionHierarchy.NO_PATH ? null :
            new Result(path.vertices(), path.weight(), Engine.CONTRACTION_HIERARCHY);
    }

    final WorkGuard searchGuard = guard != null ? guard : WorkGuard.forCommand(null, "shortest path");
    // The view's accessors each read whichever snapshot the view serves at that moment, and a compaction renumbers
    // its dense ids: one snapshot is captured and every read of the search - endpoints, adjacency, RIDs - goes
    // through it, so a path can never be stitched together from two numberings.
    final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(database, types);
    if (provider instanceof GraphAnalyticalView view && provider.servesEdgeProperty(weightProperty, types)) {
      final GraphAnalyticalView.Snapshot snap = view.currentSnapshot();
      final int s = snap != null ? view.nodeIdOf(snap, source) : -1;
      final int t = snap != null ? view.nodeIdOf(snap, target) : -1;
      if (s >= 0 && t >= 0) {
        final ViewGraph graph = new ViewGraph(view, snap, weightProperty, direction, types);
        final Search search = new Search(graph, searchGuard);
        if (search.run(s, t)) {
          if (search.meet < 0)
            return null;
          final int[] nodes = search.path(s, t);
          final List<RID> vertices = new ArrayList<>(nodes.length);
          boolean complete = true;
          for (final int node : nodes) {
            final RID rid = view.ridOf(snap, node);
            if (rid == null) {
              complete = false;
              break;
            }
            vertices.add(rid);
          }
          if (complete)
            return new Result(vertices, search.best, Engine.VIEW);
        }
        // a node the view could not answer for exactly: the records can
      }
    }

    final RecordGraph graph = new RecordGraph(weightProperty, direction, types);
    final Search search = new Search(graph, searchGuard);
    final int s = graph.intern(source);
    final int t = graph.intern(target);
    search.run(s, t);
    if (search.meet < 0)
      return null;
    final int[] nodes = search.path(s, t);
    final List<RID> vertices = new ArrayList<>(nodes.length);
    for (final int node : nodes)
      vertices.add(graph.rids.get(node));
    return new Result(vertices, search.best, Engine.RECORDS);
  }

  /** The weight an edge contributes, or a negative value when it must not be walked. */
  static double weightOf(final Object value) {
    if (!(value instanceof Number number))
      return MISSING_WEIGHT;
    final double weight = number.doubleValue();
    return weight >= 0 ? weight : -1; // NaN fails the comparison as well
  }

  // ---------------------------------------------------------------------------------------------------------------

  /** A graph seen through int ids: the arcs leaving a node forward, or entering it backward. */
  private interface Graph {
    /**
     * Hands every usable arc to {@code sink}. Returns false when this graph cannot answer for {@code node} exactly.
     */
    boolean expand(int node, boolean forward, ArcSink sink);
  }

  @FunctionalInterface
  private interface ArcSink {
    void accept(int neighbor, double weight);
  }

  private static Vertex.DIRECTION reverse(final Vertex.DIRECTION direction) {
    return switch (direction) {
      case OUT -> Vertex.DIRECTION.IN;
      case IN -> Vertex.DIRECTION.OUT;
      default -> Vertex.DIRECTION.BOTH;
    };
  }

  /** Arcs read from one snapshot of a view's columns, overlay included. */
  private static final class ViewGraph implements Graph {
    private final GraphAnalyticalView          view;
    private final GraphAnalyticalView.Snapshot snapshot;
    private final String                       weightProperty;
    private final Vertex.DIRECTION             forward;
    private final Vertex.DIRECTION             backward;
    private final String[]                     edgeTypes;

    ViewGraph(final GraphAnalyticalView view, final GraphAnalyticalView.Snapshot snapshot, final String weightProperty,
        final Vertex.DIRECTION direction, final String[] edgeTypes) {
      this.view = view;
      this.snapshot = snapshot;
      this.weightProperty = weightProperty;
      this.forward = direction;
      this.backward = reverse(direction);
      this.edgeTypes = edgeTypes;
    }

    @Override
    public boolean expand(final int node, final boolean isForward, final ArcSink sink) {
      final NodeEdgeWeights edges = view.edgeWeightsOf(snapshot, node, isForward ? forward : backward, weightProperty,
          MISSING_WEIGHT, edgeTypes);
      if (edges == null)
        return false;
      final int[] neighbors = edges.neighbors();
      final double[] weights = edges.weights();
      for (int i = 0; i < neighbors.length; i++) {
        final double w = weights[i];
        if (w >= 0 && neighbors[i] != node)
          sink.accept(neighbors[i], w);
      }
      return true;
    }
  }

  /** Arcs read from the edge records; vertices get ids as the search discovers them. */
  private static final class RecordGraph implements Graph {
    private final String           weightProperty;
    private final Vertex.DIRECTION forward;
    private final Vertex.DIRECTION backward;
    private final String[]         edgeTypes;
    private final Map<RID, Integer> ids  = new HashMap<>();
    final         List<RID>        rids = new ArrayList<>();

    RecordGraph(final String weightProperty, final Vertex.DIRECTION direction, final String[] edgeTypes) {
      this.weightProperty = weightProperty;
      this.forward = direction;
      this.backward = reverse(direction);
      this.edgeTypes = edgeTypes;
    }

    int intern(final RID rid) {
      final Integer id = ids.get(rid);
      if (id != null)
        return id;
      final int next = rids.size();
      ids.put(rid, next);
      rids.add(rid);
      return next;
    }

    @Override
    public boolean expand(final int node, final boolean isForward, final ArcSink sink) {
      final RID rid = rids.get(node);
      final Vertex vertex;
      try {
        vertex = rid.asVertex();
      } catch (final RecordNotFoundException e) {
        return true; // a vertex deleted under the search has no arcs
      }
      final Vertex.DIRECTION direction = isForward ? forward : backward;
      for (final Edge edge : edgeTypes != null ? vertex.getEdges(direction, edgeTypes) : vertex.getEdges(direction)) {
        try {
          final RID out = edge.getOut();
          final RID other = out.equals(rid) ? edge.getIn() : out;
          if (other.equals(rid))
            continue;
          final double w = weightOf(edge.get(weightProperty));
          if (w >= 0)
            sink.accept(intern(other), w);
        } catch (final RecordNotFoundException e) {
          GhostEdgeReporter.reportSkipped(e);
        }
      }
      return true;
    }
  }

  /**
   * Bidirectional Dijkstra: a forward search from the source and a backward one from the target, alternating by the
   * smaller frontier key, stopped once the two keys together reach the best meeting cost found. State lives in
   * hash-indexed primitive arrays, so its size follows the part of the graph the search touches, not the graph.
   */
  private static final class Search implements ArcSink {
    private final Graph     graph;
    private final WorkGuard guard;
    private final Side      forward  = new Side();
    private final Side      backward = new Side();
    private       Side      expanding;
    private       Side      opposite;
    private       int       expandingSlot;
    double best = Double.POSITIVE_INFINITY;
    int    meet = -1;

    Search(final Graph graph, final WorkGuard guard) {
      this.graph = graph;
      this.guard = guard;
    }

    /** Returns false when the graph could not answer for a node the search needed. */
    boolean run(final int source, final int target) {
      forward.reach(source, 0, -1);
      backward.reach(target, 0, -1);
      int iterations = 0;
      while (!forward.isEmpty() || !backward.isEmpty()) {
        guard.checkPeriodically(iterations++);
        final double forwardKey = forward.peekKey();
        final double backwardKey = backward.peekKey();
        if (forwardKey + backwardKey >= best)
          break;
        final boolean goForward = forwardKey <= backwardKey;
        expanding = goForward ? forward : backward;
        opposite = goForward ? backward : forward;
        final int slot = expanding.pop();
        if (slot < 0)
          continue;
        expandingSlot = slot;
        final int node = expanding.nodes[slot];
        final int other = opposite.slotOf(node);
        if (other >= 0 && expanding.dist[slot] + opposite.dist[other] < best) {
          best = expanding.dist[slot] + opposite.dist[other];
          meet = node;
        }
        if (!graph.expand(node, goForward, this))
          return false;
      }
      return true;
    }

    @Override
    public void accept(final int neighbor, final double weight) {
      final double candidate = expanding.dist[expandingSlot] + weight;
      final int slot = expanding.reach(neighbor, candidate, expanding.nodes[expandingSlot]);
      if (slot < 0)
        return;
      final int other = opposite.slotOf(neighbor);
      if (other >= 0 && candidate + opposite.dist[other] < best) {
        best = candidate + opposite.dist[other];
        meet = neighbor;
      }
    }

    /** The nodes from source to target through the meeting node. */
    int[] path(final int source, final int target) {
      final int[] head = forward.chain(meet, source);
      final int[] tail = backward.chain(meet, target);
      final int[] result = new int[head.length + tail.length - 1];
      for (int i = 0; i < head.length; i++)
        result[i] = head[head.length - 1 - i];
      System.arraycopy(tail, 1, result, head.length, tail.length - 1);
      return result;
    }
  }

  /** One direction of the search: discovered nodes, their tentative distances and parents, and a lazy min-heap. */
  private static final class Side {
    private final IntIntHashMap index    = new IntIntHashMap(64);
    int[]                       nodes    = new int[64];
    double[]                    dist     = new double[64];
    int[]                       parent   = new int[64];
    boolean[]                   settled  = new boolean[64];
    private int                 size;
    // heap of (key, slot), keys duplicated so stale entries can be skipped on pop
    private double[]            heapKeys  = new double[64];
    private int[]               heapSlots = new int[64];
    private int                 heapSize;

    boolean isEmpty() {
      return heapSize == 0;
    }

    double peekKey() {
      return heapSize == 0 ? Double.POSITIVE_INFINITY : heapKeys[0];
    }

    int slotOf(final int node) {
      return index.get(node, -1);
    }

    /** Records {@code node} at {@code distance} if that improves it; returns its slot, or -1 when it does not. */
    int reach(final int node, final double distance, final int from) {
      int slot = index.get(node, -1);
      if (slot < 0) {
        if (size == nodes.length) {
          final int grown = size * 2;
          nodes = Arrays.copyOf(nodes, grown);
          dist = Arrays.copyOf(dist, grown);
          parent = Arrays.copyOf(parent, grown);
          settled = Arrays.copyOf(settled, grown);
        }
        slot = size++;
        index.put(node, slot);
        nodes[slot] = node;
      } else if (settled[slot] || distance >= dist[slot])
        return -1;
      dist[slot] = distance;
      parent[slot] = from;
      push(distance, slot);
      return slot;
    }

    /** The closest unsettled slot, settled now; -1 for a stale heap entry. */
    int pop() {
      final double key = heapKeys[0];
      final int slot = heapSlots[0];
      heapSize--;
      if (heapSize > 0) {
        heapKeys[0] = heapKeys[heapSize];
        heapSlots[0] = heapSlots[heapSize];
        siftDown(0);
      }
      if (settled[slot] || key > dist[slot])
        return -1;
      settled[slot] = true;
      return slot;
    }

    /** The parent chain from {@code node} back to {@code root}, both included. */
    int[] chain(final int node, final int root) {
      int length = 1;
      for (int n = node; n != root; n = parent[index.get(n, -1)])
        length++;
      final int[] result = new int[length];
      int i = 0;
      for (int n = node; ; n = parent[index.get(n, -1)]) {
        result[i++] = n;
        if (n == root)
          break;
      }
      return result;
    }

    private void push(final double key, final int slot) {
      if (heapSize == heapKeys.length) {
        heapKeys = Arrays.copyOf(heapKeys, heapSize * 2);
        heapSlots = Arrays.copyOf(heapSlots, heapSize * 2);
      }
      int i = heapSize++;
      while (i > 0) {
        final int up = (i - 1) >>> 1;
        if (heapKeys[up] <= key)
          break;
        heapKeys[i] = heapKeys[up];
        heapSlots[i] = heapSlots[up];
        i = up;
      }
      heapKeys[i] = key;
      heapSlots[i] = slot;
    }

    private void siftDown(int i) {
      final double key = heapKeys[i];
      final int slot = heapSlots[i];
      while (true) {
        int child = 2 * i + 1;
        if (child >= heapSize)
          break;
        if (child + 1 < heapSize && heapKeys[child + 1] < heapKeys[child])
          child++;
        if (heapKeys[child] >= key)
          break;
        heapKeys[i] = heapKeys[child];
        heapSlots[i] = heapSlots[child];
        i = child;
      }
      heapKeys[i] = key;
      heapSlots[i] = slot;
    }
  }
}
