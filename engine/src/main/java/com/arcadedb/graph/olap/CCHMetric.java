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

import com.arcadedb.graph.EdgeWeight;
import com.arcadedb.utility.IntIntHashMap;

import java.util.Arrays;
import java.util.concurrent.locks.StampedLock;
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
  // Guards the arrays above against {@link #update}, which rewrites them in place: queries read optimistically (see
  // read()), the single writer - the hierarchy's preparation, one at a time - takes the write lock.
  private final StampedLock lock = new StampedLock();

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
   * A negative, NaN or infinite weight makes its arc unusable ({@link EdgeWeight#isWalkable}), the same as an absent arc:
   * a shortest path is not defined over it.
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
      if (!EdgeWeight.isWalkable(w))
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

  /**
   * Partial customization: sets the input cost of {@code count} arcs and recomputes, in place, exactly the arcs whose
   * customized cost can depend on them - the changed arcs themselves and, transitively, every arc they close a lower
   * triangle of. An arc {@code x -> y} is the lower arc of the triangles {@code x < y, w} for the other up-neighbours
   * {@code w} of {@code x}, so a change to it can only reach the arc joining {@code y} and {@code w}, whose lower endpoint
   * ranks above {@code x}. Processing arcs in order of lower endpoint therefore finds each one's lower triangles already
   * final, exactly as a full customization would, and a single changed weight on a road network touches a few hundred
   * arcs instead of all of them.
   * <p>
   * An arc is recomputed from its input cost and all its lower triangles, not merely relaxed, because a cost can go UP
   * (heavier traffic, a closed road) as well as down.
   *
   * @param arcs       the arcs whose input cost changes
   * @param inputUps   their new lower -> higher cost
   * @param inputDowns their new higher -> lower cost (ignored when undirected)
   * @param count      how many entries of the three arrays to apply
   *
   * @return how many arcs were recomputed
   */
  int update(final int[] arcs, final double[] inputUps, final double[] inputDowns, final int count) {
    final long stamp = lock.writeLock();
    try {
      final IntIntHashMap queued = new IntIntHashMap(Math.max(16, count * 4));
      final ArcQueue queue = new ArcQueue(Math.max(16, count * 2));
      for (int i = 0; i < count; i++) {
        final int arc = arcs[i];
        inputUp[arc] = inputUps[i];
        if (!undirected)
          inputDown[arc] = inputDowns[i];
        if (queued.put(arc, 1) == Integer.MIN_VALUE)
          queue.push(topology.arcTails[arc], arc);
      }

      final int[] upOffsets = topology.upOffsets;
      final int[] upHeads = topology.upHeads;
      final int[] arcTails = topology.arcTails;
      final int[] downOffsets = topology.downOffsets;
      final int[] downArcs = topology.downArcs;
      int recomputed = 0;
      while (!queue.isEmpty()) {
        final int arc = queue.pop();
        recomputed++;
        final int x = arcTails[arc];
        final int y = upHeads[arc];

        double newUp = inputUp[arc];
        int newUpMiddle = -1;
        double newDown = undirected ? newUp : inputDown[arc];
        int newDownMiddle = -1;
        for (int d = downOffsets[x], dEnd = downOffsets[x + 1]; d < dEnd; d++) {
          final int zx = downArcs[d];
          final int z = arcTails[zx];
          final int zy = findArcAfter(z, zx, y);
          if (zy < 0)
            continue;
          final double viaUp = down[zx] + up[zy];   // x -> z -> y
          if (viaUp < newUp) {
            newUp = viaUp;
            newUpMiddle = z;
          }
          if (!undirected) {
            final double viaDown = down[zy] + up[zx]; // y -> z -> x
            if (viaDown < newDown) {
              newDown = viaDown;
              newDownMiddle = z;
            }
          }
        }

        upMiddle[arc] = newUpMiddle;
        if (!undirected)
          downMiddle[arc] = newDownMiddle;
        final boolean changed = newUp != up[arc] || (!undirected && newDown != down[arc]);
        if (!changed)
          continue;
        up[arc] = newUp;
        if (!undirected)
          down[arc] = newDown;

        // the triangles x < y, w this arc is the lower arc of: their third arc joins y and w
        for (int xw = upOffsets[x], end = upOffsets[x + 1]; xw < end; xw++) {
          final int w = upHeads[xw];
          if (w == y)
            continue;
          final int dependent = w < y ? topology.findArc(w, y) : topology.findArc(y, w);
          if (dependent >= 0 && queued.put(dependent, 1) == Integer.MIN_VALUE)
            queue.push(arcTails[dependent], dependent);
        }
      }
      return recomputed;
    } finally {
      lock.unlockWrite(stamp);
    }
  }

  /** The arc {@code z -> y} among z's up arcs listed after {@code zx} (whose head ranks below y), or -1. */
  private int findArcAfter(final int z, final int zx, final int y) {
    final int found = Arrays.binarySearch(topology.upHeads, zx + 1, topology.upOffsets[z + 1], y);
    return found >= 0 ? found : -1;
  }

  /** A binary min-heap of arcs keyed by their lower endpoint. */
  private static final class ArcQueue {
    private long[] items;
    private int    size;

    ArcQueue(final int capacity) {
      items = new long[capacity];
    }

    boolean isEmpty() {
      return size == 0;
    }

    void push(final int tail, final int arc) {
      if (size == items.length)
        items = Arrays.copyOf(items, size * 2);
      final long item = (long) tail << 32 | (arc & 0xFFFFFFFFL);
      int i = size++;
      while (i > 0) {
        final int parent = (i - 1) >>> 1;
        if (items[parent] <= item)
          break;
        items[i] = items[parent];
        i = parent;
      }
      items[i] = item;
    }

    int pop() {
      final long top = items[0];
      final long last = items[--size];
      int i = 0;
      while (true) {
        int child = 2 * i + 1;
        if (child >= size)
          break;
        if (child + 1 < size && items[child + 1] < items[child])
          child++;
        if (items[child] >= last)
          break;
        items[i] = items[child];
        i = child;
      }
      if (size > 0)
        items[i] = last;
      return (int) top;
    }
  }

  /** True when this metric was customized from exactly these input costs, so customizing them again would rebuild it. */
  boolean hasInput(final double[] otherUp, final double[] otherDown, final boolean otherUndirected) {
    return undirected == otherUndirected && Arrays.equals(inputUp, otherUp) && (undirected || Arrays.equals(inputDown,
        otherDown));
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Query

  /**
   * Scratch for one query: the distance of each rank on the two chains and the arc that reached it, reset after use.
   * Indexed by the rank's DEPTH, not by the rank: a chain holds one rank per depth, so the arrays need the depth of the
   * deepest chain (a few thousand entries on a large road network) rather than one entry per node of the graph.
   */
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
    final PathResult result = read(topology.rankOf[source], topology.rankOf[target], false);
    return result == null ? INFINITY : result.distance();
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
    return read(topology.rankOf[source], topology.rankOf[target], true);
  }

  /**
   * One query, read consistently against {@link #update}: optimistically first, which costs readers nothing and never
   * blocks them, then - only when an update overlapped it - again under the read lock. An overlapped optimistic attempt
   * may have read half-updated costs, so its answer, or the exception those costs led to, is discarded rather than
   * trusted; the scratch is reset either way.
   */
  private PathResult read(final int rs, final int rt, final boolean withPath) {
    final QueryState state = topology.borrowState();
    try {
      final long stamp = lock.tryOptimisticRead();
      if (stamp != 0) {
        try {
          final PathResult result = query(state, rs, rt, withPath);
          if (lock.validate(stamp))
            return result;
        } catch (final RuntimeException e) {
          if (lock.validate(stamp))
            throw e;
        } finally {
          reset(state, rs, rt);
        }
      }
      final long readStamp = lock.readLock();
      try {
        return query(state, rs, rt, withPath);
      } finally {
        reset(state, rs, rt);
        lock.unlockRead(readStamp);
      }
    } finally {
      topology.returnState(state);
    }
  }

  private PathResult query(final QueryState state, final int rs, final int rt, final boolean withPath) {
    final int meet = search(state, rs, rt);
    if (meet < 0)
      return null;
    final double distance = state.forward[topology.depth[meet]] + state.backward[topology.depth[meet]];
    return new PathResult(withPath ? unpack(state, rs, rt, meet) : null, distance);
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
    final int[] depth = topology.depth;
    final double[] forward = state.forward;
    final double[] backward = state.backward;

    forward[depth[rs]] = 0;
    backward[depth[rt]] = 0;
    int meet = -1;
    double best = INFINITY;
    int fx = rs;
    int bx = rt;
    while (fx >= 0 || bx >= 0) {
      if (fx >= 0 && (bx < 0 || fx < bx)) {
        final double dx = forward[depth[fx]];
        if (dx < best)
          relax(fx, dx, up, forward, state.forwardArc);
        fx = parent[fx];
      } else if (bx >= 0 && (fx < 0 || bx < fx)) {
        final double dx = backward[depth[bx]];
        if (dx < best)
          relax(bx, dx, down, backward, state.backwardArc);
        bx = parent[bx];
      } else {
        // a rank on both chains: both of its distances are final here
        final int x = fx;
        final double fd = forward[depth[x]];
        final double bd = backward[depth[x]];
        final double total = fd + bd;
        if (total < best) {
          best = total;
          meet = x;
        }
        if (fd < best)
          relax(x, fd, up, forward, state.forwardArc);
        if (bd < best)
          relax(x, bd, down, backward, state.backwardArc);
        fx = parent[x];
        bx = fx;
      }
    }
    return meet;
  }

  /** Relaxes the up arcs of {@code x}; every head is an ancestor on the same chain, addressed by its depth. */
  private void relax(final int x, final double dx, final double[] cost, final double[] dist, final int[] reachedBy) {
    final int[] upHeads = topology.upHeads;
    final int[] depth = topology.depth;
    for (int a = topology.upOffsets[x], end = topology.upOffsets[x + 1]; a < end; a++) {
      final double candidate = dx + cost[a];
      final int at = depth[upHeads[a]];
      if (candidate < dist[at]) {
        dist[at] = candidate;
        reachedBy[at] = a;
      }
    }
  }

  private void reset(final QueryState state, final int rs, final int rt) {
    // a chain spans every depth from its start up to 0
    Arrays.fill(state.forward, 0, topology.depth[rs] + 1, INFINITY);
    Arrays.fill(state.backward, 0, topology.depth[rt] + 1, INFINITY);
  }

  /** Expands the search's two half paths through the meeting rank into the original nodes, shortcuts unpacked. */
  private int[] unpack(final QueryState state, final int rs, final int rt, final int meet) {
    final int[] arcTails = topology.arcTails;
    final int[] depth = topology.depth;
    // the source half, collected from the meeting rank back down and then walked forward
    int forwardCount = 0;
    for (int y = meet; y != rs; y = arcTails[state.forwardArc[depth[y]]])
      forwardCount++;
    final int[] forwardArcs = new int[forwardCount];
    for (int y = meet, i = forwardCount - 1; y != rs; y = arcTails[state.forwardArc[depth[y]]])
      forwardArcs[i--] = state.forwardArc[depth[y]];

    final IntList path = new IntList(16);
    path.add(rs);
    final IntList stack = new IntList(16);
    for (final int arc : forwardArcs)
      expand(arc, true, path, stack);
    for (int y = meet; y != rt; ) {
      final int arc = state.backwardArc[depth[y]];
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
      // a shortest path visits every node at most once: only middles read while an update rewrote them can go past that
      if (path.size > topology.nodeCount)
        throw new IllegalStateException("Shortcut unpacking did not converge");
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
