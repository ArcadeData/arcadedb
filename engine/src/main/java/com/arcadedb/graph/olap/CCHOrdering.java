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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.function.BooleanSupplier;

/**
 * Nested-dissection vertex order for a {@link CCHTopology} (issue #9437), computed from the topology alone.
 * <p>
 * The order decides everything a contraction hierarchy costs: how many shortcuts the contraction adds, how long the
 * elimination-tree chains a query climbs are, and how many triangles a customization relaxes. Nested dissection keeps
 * all three small on graphs with small separators (road and utility networks, grids): a piece of the graph is cut in
 * two by a small set of vertices, those vertices are ranked above everything in the piece, and the two halves are
 * ordered the same way, recursively.
 * <p>
 * There are no coordinates to cut along, so a piece is projected on a pseudo-axis instead: the BFS distance from one
 * end of a pseudo-diameter minus the distance from the other end. The lowest quarter of that axis and the highest
 * quarter are then separated by a minimum vertex cut (unit vertex capacities, Dinic's blocking flows), which is the
 * cut an inertial-flow partitioner would find with real coordinates. When the cut is larger than a piece of that size
 * should need - the graph does not have small separators there - the cheaper BFS level separator is used instead. Pieces
 * of at most 64 vertices are ordered by minimum-degree elimination on bit masks.
 * <p>
 * All the scratch state lives in arrays sized to the graph and is reset piece by piece; the recursion is an explicit
 * work stack, so a deep dissection cannot overflow the thread's stack.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CCHOrdering {
  /** Pieces up to this size are ordered by minimum degree on one 64-bit adjacency mask per vertex. */
  static final  int    SMALL_PIECE      = 64;
  /** The fraction of a piece each side of a flow cut is anchored to: the cut is balanced at least this well. */
  private static final double TERMINAL_FRACTION = 0.25;

  private final int             n;
  private final int[]           offsets;
  private final int[]           adjacency;
  private final int[]           reverse;     // adjacency entry u->v -> index of its twin v->u
  private final long            maxArcs;
  private final BooleanSupplier cancelled;

  private final int[] nodeAt;     // the result: rank -> node
  private final int[] stamp;      // piece membership marker
  private       int   epoch;
  private final int[] distA;
  private final int[] distB;
  private final int[] queue;
  private final byte[] role;      // 0 = inner, 1 = source side, 2 = sink side (flow terminals)
  private final byte[] nodeFlow;  // 1 when the vertex carries a unit of flow
  private final byte[] edgeFlow;  // per adjacency entry u->v: 1 when u_out -> v_in carries a unit of flow
  private final int[]  level;     // per flow state (2 * node + side), -1 = unreached
  private final int[]  cursor;    // per flow state, next arc to try in the blocking-flow search
  private final int[]  pathStates;
  private final int[]  pathArcs;
  private       int    flowEpoch;   // the piece the flow runs in: neighbours stamped otherwise are outside it

  private CCHOrdering(final int n, final int[] offsets, final int[] adjacency, final long maxArcs,
      final BooleanSupplier cancelled) {
    this.n = n;
    this.offsets = offsets;
    this.adjacency = adjacency;
    this.maxArcs = maxArcs;
    this.cancelled = cancelled;
    this.reverse = buildReverse(n, offsets, adjacency);
    this.nodeAt = new int[n];
    this.stamp = new int[n];
    this.distA = new int[n];
    this.distB = new int[n];
    this.queue = new int[2 * n + 2];
    this.role = new byte[n];
    this.nodeFlow = new byte[n];
    this.edgeFlow = new byte[adjacency.length];
    this.level = new int[2 * n];
    this.cursor = new int[2 * n];
    this.pathStates = new int[2 * n + 1];
    this.pathArcs = new int[2 * n + 1];
    Arrays.fill(level, -1);
  }

  /**
   * Computes the order of an undirected simple graph given as CSR (every edge listed from both endpoints, no self loops,
   * no duplicates, each slice sorted ascending).
   *
   * @param maxArcs   the supergraph arc budget: a separator whose clique alone would exceed it proves the graph unsuitable
   * @param cancelled polled between pieces; may be null
   *
   * @return rank -> node, or null when the graph cannot be ordered within {@code maxArcs} or the work was cancelled
   */
  static int[] order(final int n, final int[] offsets, final int[] adjacency, final long maxArcs,
      final BooleanSupplier cancelled) {
    return new CCHOrdering(n, offsets, adjacency, maxArcs, cancelled).run();
  }

  private int[] run() {
    final ArrayDeque<Piece> stack = new ArrayDeque<>();
    final int[] all = new int[n];
    for (int i = 0; i < n; i++)
      all[i] = i;
    stack.push(new Piece(all, 0));

    while (!stack.isEmpty()) {
      if (cancelled != null && cancelled.getAsBoolean())
        return null;
      final Piece piece = stack.pop();
      final int[] nodes = piece.nodes;
      final int size = nodes.length;
      if (size == 0)
        continue;
      if (size == 1) {
        nodeAt[piece.lo] = nodes[0];
        continue;
      }

      markPiece(nodes);
      final int[][] components = components(nodes);
      if (components.length > 1) {
        int lo = piece.lo;
        for (final int[] component : components) {
          stack.push(new Piece(component, lo));
          lo += component.length;
        }
        continue;
      }

      if (size <= SMALL_PIECE) {
        minimumDegree(nodes, piece.lo);
        continue;
      }

      final int[] separator = separator(nodes);
      if (separator == null)
        return null;
      // every separator vertex ends up adjacent to every other one in the worst case: if even that clique cannot fit the
      // budget the graph has no small separators here, and the rest of the dissection is wasted work
      if ((long) separator.length * (separator.length - 1) / 2 > maxArcs)
        return null;

      // the separator takes the top ranks of the piece, the rest is ordered below it
      final int top = piece.lo + size - separator.length;
      markPiece(separator);
      final int separatorEpoch = epoch;
      for (int i = 0; i < separator.length; i++)
        nodeAt[top + i] = separator[i];
      final int[] rest = new int[size - separator.length];
      int k = 0;
      for (final int v : nodes)
        if (stamp[v] != separatorEpoch)
          rest[k++] = v;
      stack.push(new Piece(rest, piece.lo));
    }
    return nodeAt;
  }

  private record Piece(int[] nodes, int lo) {
  }

  private void markPiece(final int[] nodes) {
    if (++epoch == Integer.MAX_VALUE) {
      Arrays.fill(stamp, 0);
      epoch = 1;
    }
    for (final int v : nodes)
      stamp[v] = epoch;
  }

  /** The connected components of the marked piece; the piece stays marked with the current epoch afterwards. */
  private int[][] components(final int[] nodes) {
    final int pieceEpoch = epoch;
    // a second epoch marks the visited vertices; membership is "stamp is either of the two"
    final int visitedEpoch = ++epoch;
    int count = 0;
    int[][] result = new int[4][];
    for (final int start : nodes) {
      if (stamp[start] != pieceEpoch)
        continue;
      int head = 0;
      int tail = 0;
      queue[tail++] = start;
      stamp[start] = visitedEpoch;
      while (head < tail) {
        final int u = queue[head++];
        for (int e = offsets[u], end = offsets[u + 1]; e < end; e++) {
          final int v = adjacency[e];
          if (stamp[v] == pieceEpoch) {
            stamp[v] = visitedEpoch;
            queue[tail++] = v;
          }
        }
      }
      if (count == 0 && tail == nodes.length) {
        // connected: the common case, no copy
        for (final int v : nodes)
          stamp[v] = pieceEpoch;
        epoch = pieceEpoch;
        return new int[][] { nodes };
      }
      if (count == result.length)
        result = Arrays.copyOf(result, count * 2);
      result[count++] = Arrays.copyOf(queue, tail);
    }
    return Arrays.copyOf(result, count);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Small pieces: minimum-degree elimination

  private void minimumDegree(final int[] nodes, final int lo) {
    final int size = nodes.length;
    final long[] masks = new long[size];
    // local index by binary search: the piece array is not sorted, so sort a copy with its positions
    final long[] keyed = new long[size];
    for (int i = 0; i < size; i++)
      keyed[i] = ((long) nodes[i] << 32) | i;
    Arrays.sort(keyed);
    for (int i = 0; i < size; i++) {
      final int u = nodes[i];
      for (int e = offsets[u], end = offsets[u + 1]; e < end; e++) {
        final int j = localIndex(keyed, adjacency[e]);
        if (j >= 0)
          masks[i] |= 1L << j;
      }
    }
    long remaining = size == 64 ? -1L : (1L << size) - 1;
    for (int r = 0; r < size; r++) {
      int best = -1;
      int bestDegree = Integer.MAX_VALUE;
      for (long rest = remaining; rest != 0; rest &= rest - 1) {
        final int i = Long.numberOfTrailingZeros(rest);
        final int degree = Long.bitCount(masks[i] & remaining);
        if (degree < bestDegree) {
          bestDegree = degree;
          best = i;
        }
      }
      remaining &= ~(1L << best);
      final long neighbours = masks[best] & remaining;
      for (long rest = neighbours; rest != 0; rest &= rest - 1) {
        final int j = Long.numberOfTrailingZeros(rest);
        masks[j] |= neighbours & ~(1L << j);
      }
      nodeAt[lo + r] = nodes[best];
    }
  }

  private static int localIndex(final long[] keyed, final int node) {
    int low = 0;
    int high = keyed.length - 1;
    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final int value = (int) (keyed[mid] >>> 32);
      if (value < node)
        low = mid + 1;
      else if (value > node)
        high = mid - 1;
      else
        return (int) keyed[mid];
    }
    return -1;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Separators

  /** A vertex separator of the marked, connected piece: never empty, never the whole piece. */
  private int[] separator(final int[] nodes) {
    final int pieceEpoch = epoch;

    // pseudo-diameter: the farthest vertex from an arbitrary one, then the farthest from that
    final int a = farthest(bfs(nodes[0], distA, pieceEpoch));
    final int b = farthest(bfs(a, distA, pieceEpoch));
    bfs(b, distB, pieceEpoch);

    final int[] flowCut = flowSeparator(nodes);
    if (flowCut != null)
      return flowCut;
    return levelSeparator(nodes);
  }

  /** BFS inside the piece; returns the number of vertices reached, leaving them in {@link #queue} in BFS order. */
  private int bfs(final int source, final int[] dist, final int pieceEpoch) {
    // the visited marker is the distance itself, reset for the piece first
    int head = 0;
    int tail = 0;
    queue[tail++] = source;
    dist[source] = 0;
    final int visitedEpoch = -pieceEpoch - 1; // negative so it can never collide with a live epoch
    stamp[source] = visitedEpoch;
    while (head < tail) {
      final int u = queue[head++];
      final int du = dist[u] + 1;
      for (int e = offsets[u], end = offsets[u + 1]; e < end; e++) {
        final int v = adjacency[e];
        if (stamp[v] == pieceEpoch) {
          stamp[v] = visitedEpoch;
          dist[v] = du;
          queue[tail++] = v;
        }
      }
    }
    for (int i = 0; i < tail; i++)
      stamp[queue[i]] = pieceEpoch;
    return tail;
  }

  /** The last vertex the previous {@link #bfs} reached, which is one of the farthest from its source. */
  private int farthest(final int reached) {
    return queue[reached - 1];
  }

  /**
   * The minimum vertex cut between the lowest and the highest quarter of the piece on the {@code distA - distB} axis, or
   * null when the two quarters cannot be kept apart (a piece of small diameter) or the cut is larger than a piece of this
   * size should need.
   */
  private int[] flowSeparator(final int[] nodes) {
    final int size = nodes.length;
    // the key of an edge's endpoints differs by at most 2, so terminals whose keys are 3 apart are never adjacent: the
    // flow then always passes through at least one unit-capacity vertex and stays finite
    int minKey = Integer.MAX_VALUE;
    int maxKey = Integer.MIN_VALUE;
    for (final int v : nodes) {
      final int key = distA[v] - distB[v];
      if (key < minKey)
        minKey = key;
      if (key > maxKey)
        maxKey = key;
    }
    final int range = maxKey - minKey + 1;
    final int[] histogram = new int[range];
    for (final int v : nodes)
      histogram[distA[v] - distB[v] - minKey]++;

    final int quota = Math.max(1, (int) (size * TERMINAL_FRACTION));
    int sourceKey = 0;
    for (int acc = 0; sourceKey < range; sourceKey++) {
      acc += histogram[sourceKey];
      if (acc >= quota)
        break;
    }
    int sinkKey = range - 1;
    for (int acc = 0; sinkKey >= 0; sinkKey--) {
      acc += histogram[sinkKey];
      if (acc >= quota)
        break;
    }
    if (sinkKey - sourceKey < 3)
      return null;

    for (final int v : nodes) {
      final int key = distA[v] - distB[v] - minKey;
      role[v] = key <= sourceKey ? (byte) 1 : key >= sinkKey ? (byte) 2 : (byte) 0;
    }

    flowEpoch = epoch;
    final int maxCut = Math.max(16, (int) (4 * Math.sqrt(size)));
    final int flow = maxFlow(nodes, maxCut);
    final int[] cut = flow > 0 ? minCut(nodes, flow) : null;
    // reset the flow state of the piece
    for (final int v : nodes) {
      role[v] = 0;
      nodeFlow[v] = 0;
      for (int e = offsets[v], end = offsets[v + 1]; e < end; e++)
        edgeFlow[e] = 0;
    }
    if (cut == null || cut.length == 0 || cut.length >= size)
      return null;
    return cut;
  }

  // Flow states: 2v = v_in, 2v + 1 = v_out. Arcs: v_in -> v_out (capacity 1, unbounded for terminals), u_out -> v_in for
  // every edge (unbounded). Sources are the out states of the source terminals, sinks the in states of the sink ones.

  /** Dinic's max flow with unit pushes; -1 when it grows past {@code maxCut}. */
  private int maxFlow(final int[] nodes, final int maxCut) {
    int flow = 0;
    while (buildLevels(nodes)) {
      for (final int v : nodes) {
        cursor[2 * v] = 0;
        cursor[2 * v + 1] = 0;
      }
      for (final int v : nodes) {
        if (role[v] != 1)
          continue;
        while (augment(2 * v + 1)) {
          if (++flow > maxCut) {
            resetLevels(nodes);
            return -1;
          }
        }
      }
      resetLevels(nodes);
      if (cancelled != null && cancelled.getAsBoolean())
        return -1;
    }
    // the last levelling reached no sink and left its marks behind
    resetLevels(nodes);
    return flow;
  }

  private void resetLevels(final int[] nodes) {
    for (final int v : nodes) {
      level[2 * v] = -1;
      level[2 * v + 1] = -1;
    }
  }

  /** Levels the residual graph from the sources; true when a sink is reachable. */
  private boolean buildLevels(final int[] nodes) {
    int head = 0;
    int tail = 0;
    for (final int v : nodes)
      if (role[v] == 1) {
        level[2 * v + 1] = 0;
        queue[tail++] = 2 * v + 1;
      }
    boolean sinkReached = false;
    while (head < tail) {
      final int state = queue[head++];
      final int next = level[state] + 1;
      final int v = state >>> 1;
      if ((state & 1) == 0) {
        // v_in: a sink stops here
        if (role[v] == 2) {
          sinkReached = true;
          continue;
        }
        if (nodeFlow[v] == 0 && level[state + 1] < 0) {
          level[state + 1] = next;
          queue[tail++] = state + 1;
        }
        for (int e = offsets[v], end = offsets[v + 1]; e < end; e++) {
          final int u = adjacency[e];
          if (stamp[u] != flowEpoch || role[u] == 1 || edgeFlow[reverse[e]] == 0)
            continue;
          final int target = 2 * u + 1;
          if (level[target] < 0) {
            level[target] = next;
            queue[tail++] = target;
          }
        }
      } else {
        if (role[v] == 0 && nodeFlow[v] == 1 && level[state - 1] < 0) {
          level[state - 1] = next;
          queue[tail++] = state - 1;
        }
        for (int e = offsets[v], end = offsets[v + 1]; e < end; e++) {
          final int u = adjacency[e];
          if (stamp[u] != flowEpoch || role[u] == 1)
            continue;
          final int target = 2 * u;
          if (level[target] < 0) {
            level[target] = next;
            queue[tail++] = target;
          }
        }
      }
    }
    return sinkReached;
  }

  /**
   * One unit-flow augmenting path along the level graph from {@code source}, found depth first with per-state cursors so
   * that dead ends are never revisited within a phase. Arc index 0 of a state is its vertex arc (forward from in, backward
   * from out); index {@code 1 + i} is its i-th adjacency entry.
   */
  private boolean augment(final int source) {
    int depth = 0;
    pathStates[0] = source;
    while (depth >= 0) {
      final int state = pathStates[depth];
      final int v = state >>> 1;
      if ((state & 1) == 0 && role[v] == 2) {
        applyPath(depth);
        return true;
      }
      final int degree = offsets[v + 1] - offsets[v];
      boolean advanced = false;
      while (cursor[state] <= degree) {
        final int arc = cursor[state];
        final int target = admissible(state, v, arc);
        if (target >= 0 && level[target] == level[state] + 1) {
          pathArcs[depth] = arc;
          pathStates[++depth] = target;
          advanced = true;
          break;
        }
        cursor[state]++;
      }
      if (!advanced) {
        // dead end: retire the state for this phase and step back
        level[state] = -1;
        depth--;
        if (depth >= 0)
          cursor[pathStates[depth]]++;
      }
    }
    return false;
  }

  /** The state arc {@code arc} of {@code state} leads to in the residual graph, or -1 when it has no residual capacity. */
  private int admissible(final int state, final int v, final int arc) {
    if ((state & 1) == 0) {
      if (arc == 0)
        return nodeFlow[v] == 0 ? state + 1 : -1;
      final int e = offsets[v] + arc - 1;
      final int u = adjacency[e];
      return stamp[u] == flowEpoch && role[u] != 1 && edgeFlow[reverse[e]] == 1 ? 2 * u + 1 : -1;
    }
    if (arc == 0)
      return role[v] == 0 && nodeFlow[v] == 1 ? state - 1 : -1;
    final int u = adjacency[offsets[v] + arc - 1];
    return stamp[u] == flowEpoch && role[u] != 1 ? 2 * u : -1;
  }

  private void applyPath(final int length) {
    for (int i = 0; i < length; i++) {
      final int state = pathStates[i];
      final int v = state >>> 1;
      final int arc = pathArcs[i];
      if ((state & 1) == 0) {
        if (arc == 0)
          nodeFlow[v] = 1;
        else
          edgeFlow[reverse[offsets[v] + arc - 1]] = 0; // cancels the unit on u_out -> v_in
      } else {
        if (arc == 0)
          nodeFlow[v] = 0;
        else
          edgeFlow[offsets[v] + arc - 1] = 1;
      }
    }
    // every arc the path used is saturated now except an edge arc out of an out state, whose capacity is unbounded: only
    // those keep their cursor, so the next search of the phase can take them again
    for (int i = 0; i < length; i++)
      if ((pathStates[i] & 1) == 0 || pathArcs[i] == 0)
        cursor[pathStates[i]]++;
  }

  /** The vertices whose in state the source side reaches in the final residual graph and whose out state it does not. */
  private int[] minCut(final int[] nodes, final int flow) {
    int head = 0;
    int tail = 0;
    for (final int v : nodes)
      if (role[v] == 1) {
        level[2 * v + 1] = 0;
        queue[tail++] = 2 * v + 1;
      }
    while (head < tail) {
      final int state = queue[head++];
      final int v = state >>> 1;
      final int degree = offsets[v + 1] - offsets[v];
      for (int arc = 0; arc <= degree; arc++) {
        final int target = admissible(state, v, arc);
        if (target >= 0 && level[target] < 0) {
          level[target] = 0;
          queue[tail++] = target;
        }
      }
    }
    final int[] cut = new int[flow];
    int k = 0;
    for (final int v : nodes)
      if (role[v] == 0 && level[2 * v] == 0 && level[2 * v + 1] < 0 && k < flow)
        cut[k++] = v;
    resetLevels(nodes);
    return k == flow ? cut : null;
  }

  /**
   * The BFS level separator: of the levels around the middle of the piece, the smallest, reduced to the vertices that have
   * a neighbour on the next level (the others can join the lower side without connecting it to the upper one).
   */
  private int[] levelSeparator(final int[] nodes) {
    final int size = nodes.length;
    int depth = 0;
    for (final int v : nodes)
      if (distA[v] > depth)
        depth = distA[v];
    final int[] perLevel = new int[depth + 1];
    for (final int v : nodes)
      perLevel[distA[v]]++;

    int chosen = -1;
    int below = 0;
    int best = Integer.MAX_VALUE;
    for (int l = 1; l < depth; l++) {
      below += perLevel[l - 1];
      final int above = size - below - perLevel[l];
      if (below >= size / 5 && above >= size / 5 && perLevel[l] < best) {
        best = perLevel[l];
        chosen = l;
      }
    }
    if (chosen < 0)
      // no balanced level: the middle one
      chosen = Math.max(1, depth / 2);

    int count = 0;
    final int[] separator = new int[perLevel[chosen]];
    for (final int v : nodes) {
      if (distA[v] != chosen)
        continue;
      boolean touchesNext = false;
      for (int e = offsets[v], end = offsets[v + 1]; e < end && !touchesNext; e++) {
        final int u = adjacency[e];
        touchesNext = stamp[u] == epoch && distA[u] == chosen + 1;
      }
      if (touchesNext)
        separator[count++] = v;
    }
    if (count == 0) {
      // a piece of diameter one or two (a clique, a star seen from its hub): the level itself
      count = 0;
      for (final int v : nodes)
        if (distA[v] == chosen)
          separator[count++] = v;
    }
    if (count >= size)
      count = size - 1;
    return Arrays.copyOf(separator, count);
  }

  private static int[] buildReverse(final int n, final int[] offsets, final int[] adjacency) {
    final int[] reverse = new int[adjacency.length];
    for (int u = 0; u < n; u++)
      for (int e = offsets[u], end = offsets[u + 1]; e < end; e++) {
        final int v = adjacency[e];
        reverse[e] = Arrays.binarySearch(adjacency, offsets[v], offsets[v + 1], u);
      }
    return reverse;
  }
}
