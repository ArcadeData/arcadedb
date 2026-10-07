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

import java.util.Arrays;
import java.util.function.BooleanSupplier;

/**
 * The weight-dependent half of a Customizable Contraction Hierarchy (issue #9437): the cost of every arc of a
 * {@link CCHTopology}'s supergraph, in both directions, after customization.
 * <p>
 * <b>Customization.</b> Each arc starts with the cheapest input arc joining its endpoints in that direction (infinite
 * when there is none, which is how a shortcut and a deleted edge both start). Then, for every vertex {@code x} in rank
 * order and every lower triangle {@code z < x < y} below it, the arc {@code x -> y} is relaxed through {@code z}. When
 * {@code x} is reached its own lower arcs are final already - they were relaxed when their lower endpoint was - so one
 * pass is exact. The middle vertex that won is remembered per arc and direction, which is what path unpacking follows
 * instead of re-deriving it from floating-point sums.
 * <p>
 * <b>Query.</b> Both endpoints climb their elimination-tree chain, relaxing up arcs (with the forward cost from the
 * source, the backward cost from the target), and the answer is the best meeting vertex on the shared part of the two
 * chains. No priority queue is needed: a chain visits its vertices in rank order and every up arc leads further up the
 * same chain. The cost depends on the depth of the elimination tree rather than on how far apart the two endpoints are.
 * <p>
 * Directed weights use the two directions of each arc separately; {@code undirected} metrics fold both directions of
 * every input arc into one cost and share a single array for the two.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CCHMetric {
  static final double INFINITY = Double.POSITIVE_INFINITY;

  final CCHTopology topology;
  final boolean     undirected;
  // Pre-customization arc costs: up = lower -> higher rank, down = higher -> lower. Kept so a later customization
  // can tell that nothing it would start from has changed and reuse this one.
  final double[]    inputUp;
  final double[]    inputDown;
  final double[]    up;
  final double[]    down;
  final int[]       upMiddle;   // the vertex the customized up cost goes through, -1 for an input arc
  final int[]       downMiddle;

  private CCHMetric(final CCHTopology topology, final boolean undirected, final double[] inputUp, final double[] inputDown,
      final double[] up, final double[] down, final int[] upMiddle, final int[] downMiddle) {
    this.topology = topology;
    this.undirected = undirected;
    this.inputUp = inputUp;
    this.inputDown = inputDown;
    this.up = up;
    this.down = down;
    this.upMiddle = upMiddle;
    this.downMiddle = downMiddle;
  }

  /** Same as {@link #inputCosts}, then {@link #customize(CCHTopology, double[], double[], boolean, BooleanSupplier)}. */
  static CCHMetric customize(final CCHTopology topology, final int[] tails, final int[] heads, final double[] weights,
      final int arcCount, final boolean undirected) {
    final double[][] input = inputCosts(topology, tails, heads, weights, arcCount, undirected);
    return input == null ? null : customize(topology, input[0], input[1], undirected, null);
  }

  /**
   * The cost each supergraph arc starts customization from: the cheapest input arc joining its endpoints, per direction.
   * A negative or NaN weight makes its arc unusable, the same as a missing one: a shortest path is not defined over it.
   *
   * @return {up, down} (the same array twice when undirected), or null when an input arc joins two vertices the
   * supergraph has no arc between, or names a vertex outside it: the topology does not describe this graph
   */
  static double[][] inputCosts(final CCHTopology topology, final int[] tails, final int[] heads, final double[] weights,
      final int arcCount, final boolean undirected) {
    final int arcs = topology.arcCount();
    final double[] up = new double[arcs];
    Arrays.fill(up, INFINITY);
    final double[] down = undirected ? up : new double[arcs];
    if (!undirected)
      Arrays.fill(down, INFINITY);

    final int n = topology.nodeCount;
    final int[] rankOf = topology.rankOf;
    for (int i = 0; i < arcCount; i++) {
      final int u = tails[i];
      final int v = heads[i];
      if (u < 0 || v < 0 || u >= n || v >= n)
        return null;
      if (u == v)
        continue; // a self loop is never part of a shortest path
      final double w = weights[i];
      final int ru = rankOf[u];
      final int rv = rankOf[v];
      final int arc = ru < rv ? topology.findArc(ru, rv) : topology.findArc(rv, ru);
      if (arc < 0)
        return null;
      if (!(w >= 0))
        continue;
      if (undirected || ru < rv) {
        if (w < up[arc])
          up[arc] = w;
      } else if (w < down[arc])
        down[arc] = w;
    }
    return new double[][] { up, down };
  }

  /**
   * Customizes {@code topology} from the per-arc input costs {@link #inputCosts} produced.
   *
   * @return the metric, or null when cancelled
   */
  static CCHMetric customize(final CCHTopology topology, final double[] inputUp, final double[] inputDown,
      final boolean undirected, final BooleanSupplier cancelled) {
    final int n = topology.nodeCount;
    final int arcs = topology.arcCount();
    final double[] up = Arrays.copyOf(inputUp, arcs);
    final double[] down = undirected ? up : Arrays.copyOf(inputDown, arcs);
    final int[] upMiddle = new int[arcs];
    Arrays.fill(upMiddle, -1);
    final int[] downMiddle = undirected ? upMiddle : new int[arcs];
    if (!undirected)
      Arrays.fill(downMiddle, -1);

    final int[] upOffsets = topology.upOffsets;
    final int[] upHeads = topology.upHeads;
    final int[] arcTails = topology.arcTails;
    final int[] downOffsets = topology.downOffsets;
    final int[] downArcs = topology.downArcs;
    // position of each up-neighbour of the current vertex, so a triangle's third arc is found by lookup
    final int[] arcTo = new int[n];
    Arrays.fill(arcTo, -1);

    for (int x = 0; x < n; x++) {
      if ((x & 0xFFFF) == 0 && cancelled != null && cancelled.getAsBoolean())
        return null;
      final int upStart = upOffsets[x];
      final int upEnd = upOffsets[x + 1];
      if (upStart == upEnd)
        continue;
      for (int a = upStart; a < upEnd; a++)
        arcTo[upHeads[a]] = a;

      for (int d = downOffsets[x], dEnd = downOffsets[x + 1]; d < dEnd; d++) {
        final int zx = downArcs[d];               // arc z -> x, z below x
        final int z = arcTails[zx];
        final double zxUp = up[zx];               // z -> x
        final double xzDown = down[zx];           // x -> z
        if (zxUp == INFINITY && xzDown == INFINITY)
          continue;
        // every up-neighbour of z above x is an up-neighbour of x (chordality), listed after x in z's sorted slice
        for (int zy = zx + 1, zEnd = upOffsets[z + 1]; zy < zEnd; zy++) {
          final int xy = arcTo[upHeads[zy]];
          final double viaUp = xzDown + up[zy];   // x -> z -> y
          if (viaUp < up[xy]) {
            up[xy] = viaUp;
            upMiddle[xy] = z;
          }
          if (!undirected) {
            final double viaDown = down[zy] + zxUp; // y -> z -> x
            if (viaDown < down[xy]) {
              down[xy] = viaDown;
              downMiddle[xy] = z;
            }
          }
        }
      }

      for (int a = upStart; a < upEnd; a++)
        arcTo[upHeads[a]] = -1;
    }
    return new CCHMetric(topology, undirected, inputUp, undirected ? inputUp : inputDown, up, down, upMiddle, downMiddle);
  }

  /** True when this metric was customized from exactly these input costs, so customizing them again would rebuild it. */
  boolean hasInput(final double[] otherUp, final double[] otherDown, final boolean otherUndirected) {
    return undirected == otherUndirected && Arrays.equals(inputUp, otherUp) && (undirected || Arrays.equals(inputDown,
        otherDown));
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Query

  /** Scratch for one query: per-rank distances and the arc each was reached by, reset after use along the two chains. */
  static final class QueryState {
    final double[] forward;
    final double[] backward;
    final int[]    forwardArc;
    final int[]    backwardArc;

    QueryState(final int n) {
      forward = new double[n];
      backward = new double[n];
      forwardArc = new int[n];
      backwardArc = new int[n];
      Arrays.fill(forward, INFINITY);
      Arrays.fill(backward, INFINITY);
    }
  }

  /** The shortest-path distance from node {@code source} to node {@code target}, infinite when there is no path. */
  double distance(final int source, final int target) {
    if (source == target)
      return 0;
    final QueryState state = topology.borrowState();
    try {
      final int meet = search(state, topology.rankOf[source], topology.rankOf[target]);
      final double result = meet < 0 ? INFINITY : state.forward[meet] + state.backward[meet];
      reset(state, topology.rankOf[source], topology.rankOf[target]);
      return result;
    } finally {
      topology.returnState(state);
    }
  }

  /** The nodes of a shortest path from {@code source} to {@code target}, both included; null when there is no path. */
  int[] shortestPath(final int source, final int target) {
    final PathResult result = shortestPathWithDistance(source, target);
    return result == null ? null : result.nodes();
  }

  record PathResult(int[] nodes, double distance) {
  }

  PathResult shortestPathWithDistance(final int source, final int target) {
    if (source == target)
      return new PathResult(new int[] { source }, 0);
    final int rs = topology.rankOf[source];
    final int rt = topology.rankOf[target];
    final QueryState state = topology.borrowState();
    try {
      final int meet = search(state, rs, rt);
      final PathResult result = meet < 0 ? null :
          new PathResult(unpack(state, rs, rt, meet), state.forward[meet] + state.backward[meet]);
      reset(state, rs, rt);
      return result;
    } finally {
      topology.returnState(state);
    }
  }

  /**
   * Climbs both chains together in rank order; returns the best meeting rank, or -1 when the two never meet at a finite
   * cost. A rank is final once reached (everything below it on its chain has been relaxed), so the best meeting cost is
   * known as soon as the chains join, and from there on a rank whose own distance already reaches it is not expanded:
   * every path through its up arcs costs at least as much. That cuts the top of the tree - where the separators with the
   * most up arcs sit - out of every query whose endpoints are close in the tree.
   */
  private int search(final QueryState state, final int rs, final int rt) {
    final int[] parent = topology.parent;
    final double[] forward = state.forward;
    final double[] backward = state.backward;

    forward[rs] = 0;
    backward[rt] = 0;
    int meet = -1;
    double best = INFINITY;
    int fx = rs;
    int bx = rt;
    while (fx >= 0 || bx >= 0) {
      if (fx >= 0 && (bx < 0 || fx < bx)) {
        if (forward[fx] < best)
          relax(fx, forward[fx], up, forward, state.forwardArc);
        fx = parent[fx];
      } else if (bx >= 0 && (fx < 0 || bx < fx)) {
        if (backward[bx] < best)
          relax(bx, backward[bx], down, backward, state.backwardArc);
        bx = parent[bx];
      } else {
        // a rank on both chains: both of its distances are final here
        final int x = fx;
        final double total = forward[x] + backward[x];
        if (total < best) {
          best = total;
          meet = x;
        }
        if (forward[x] < best)
          relax(x, forward[x], up, forward, state.forwardArc);
        if (backward[x] < best)
          relax(x, backward[x], down, backward, state.backwardArc);
        fx = parent[x];
        bx = fx;
      }
    }
    return meet;
  }

  private void relax(final int x, final double dx, final double[] cost, final double[] dist, final int[] reachedBy) {
    final int[] upHeads = topology.upHeads;
    for (int a = topology.upOffsets[x], end = topology.upOffsets[x + 1]; a < end; a++) {
      final double candidate = dx + cost[a];
      final int y = upHeads[a];
      if (candidate < dist[y]) {
        dist[y] = candidate;
        reachedBy[y] = a;
      }
    }
  }

  private void reset(final QueryState state, final int rs, final int rt) {
    final int[] parent = topology.parent;
    for (int x = rs; x >= 0; x = parent[x])
      state.forward[x] = INFINITY;
    for (int x = rt; x >= 0; x = parent[x])
      state.backward[x] = INFINITY;
  }

  /** Expands the search's two half paths through the meeting rank into the original nodes, shortcuts unpacked. */
  private int[] unpack(final QueryState state, final int rs, final int rt, final int meet) {
    final int[] arcTails = topology.arcTails;
    // the source half, collected from the meeting rank back down and then walked forward
    int forwardCount = 0;
    for (int y = meet; y != rs; y = arcTails[state.forwardArc[y]])
      forwardCount++;
    final int[] forwardArcs = new int[forwardCount];
    for (int y = meet, i = forwardCount - 1; y != rs; y = arcTails[state.forwardArc[y]])
      forwardArcs[i--] = state.forwardArc[y];

    final IntList path = new IntList(16);
    path.add(rs);
    final IntList stack = new IntList(16);
    for (final int arc : forwardArcs)
      expand(arc, true, path, stack);
    for (int y = meet; y != rt; ) {
      final int arc = state.backwardArc[y];
      expand(arc, false, path, stack);
      y = arcTails[arc];
    }

    final int[] nodes = new int[path.size];
    for (int i = 0; i < path.size; i++)
      nodes[i] = topology.nodeAt[path.items[i]];
    return nodes;
  }

  /**
   * Appends the ranks an arc stands for, excluding the one the walk is already on. Upward means lower -> higher rank,
   * downward the reverse. A stack entry is {@code arc << 1 | upward}.
   */
  private void expand(final int arc, final boolean upward, final IntList path, final IntList stack) {
    final int[] arcTails = topology.arcTails;
    final int[] upHeads = topology.upHeads;
    stack.size = 0;
    stack.add(arc << 1 | (upward ? 1 : 0));
    while (stack.size > 0) {
      final int entry = stack.items[--stack.size];
      final int a = entry >>> 1;
      final boolean goingUp = (entry & 1) == 1;
      final int low = arcTails[a];
      final int high = upHeads[a];
      final int middle = goingUp ? upMiddle[a] : downMiddle[a];
      if (middle < 0) {
        path.add(goingUp ? high : low);
        continue;
      }
      // low -> middle -> high goes down {middle, low} then up {middle, high}; the reverse for a downward arc. Pushed in
      // reverse order of walking.
      final int toLow = topology.findArc(middle, low);
      final int toHigh = topology.findArc(middle, high);
      if (goingUp) {
        stack.add(toHigh << 1 | 1);
        stack.add(toLow << 1);
      } else {
        stack.add(toLow << 1 | 1);
        stack.add(toHigh << 1);
      }
    }
  }

  /** Heap held by the cost arrays. */
  long getMemoryUsageBytes() {
    final long perDirection = 8L * up.length + 4L * upMiddle.length + 8L * inputUp.length;
    return undirected ? perDirection : 2 * perDirection;
  }

  private static final class IntList {
    int[] items;
    int   size;

    IntList(final int capacity) {
      items = new int[capacity];
    }

    void add(final int value) {
      if (size == items.length)
        items = Arrays.copyOf(items, size * 2);
      items[size++] = value;
    }
  }
}
