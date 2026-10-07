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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BooleanSupplier;

/**
 * The metric-independent half of a Customizable Contraction Hierarchy (issue #9437): a vertex order and the chordal
 * supergraph that eliminating the vertices in that order produces.
 * <p>
 * Everything is in RANK space (rank 0 is eliminated first). Every arc of the supergraph joins a lower rank to a higher
 * one and is stored once, in the "up" CSR of its lower endpoint, sorted by the higher one. The chordal property is what
 * every other part relies on:
 * <ul>
 *   <li>the up-neighbours of a vertex are all ancestors of it in the elimination tree, whose parent link is the lowest
 *       of them - so the whole upward search space of a vertex is its chain of ancestors, which is what lets a query
 *       climb it without a priority queue;</li>
 *   <li>the up-neighbours of a vertex above some up-neighbour {@code x} are up-neighbours of {@code x} too - so every
 *       pair of a vertex's up-neighbours is itself an arc, and the lower triangles a customization relaxes can be found
 *       by position rather than by search.</li>
 * </ul>
 * Building it costs the ordering (see {@link CCHOrdering}) plus one pass of elimination; it depends on which vertices
 * are adjacent and on nothing else, so any change of weights - and any deletion, which is an infinite weight - is
 * absorbed by a new {@link CCHMetric} over the same topology.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CCHTopology {
  final int   nodeCount;
  final int[] rankOf;      // node -> rank
  final int[] nodeAt;      // rank -> node
  final int[] upOffsets;   // rank -> first arc, nodeCount + 1 entries
  final int[] upHeads;     // arc -> higher rank, ascending within a rank
  final int[] arcTails;    // arc -> lower rank
  final int[] parent;      // rank -> elimination-tree parent rank, -1 for a root
  final int[] downOffsets; // rank -> first entry of downArcs, nodeCount + 1 entries
  final int[] downArcs;    // the arcs whose higher endpoint is the rank, ordered by lower endpoint

  // Query scratch, sized to the node count and handed out one per concurrent query; see CCHMetric.
  private final ConcurrentLinkedQueue<CCHMetric.QueryState> statePool = new ConcurrentLinkedQueue<>();
  private static final int MAX_POOLED_STATES = Math.max(2, Runtime.getRuntime().availableProcessors());

  private CCHTopology(final int nodeCount, final int[] rankOf, final int[] nodeAt, final int[] upOffsets,
      final int[] upHeads, final int[] arcTails, final int[] parent) {
    this.nodeCount = nodeCount;
    this.rankOf = rankOf;
    this.nodeAt = nodeAt;
    this.upOffsets = upOffsets;
    this.upHeads = upHeads;
    this.arcTails = arcTails;
    this.parent = parent;

    final int arcs = upHeads.length;
    this.downOffsets = new int[nodeCount + 1];
    for (int a = 0; a < arcs; a++)
      downOffsets[upHeads[a] + 1]++;
    for (int r = 0; r < nodeCount; r++)
      downOffsets[r + 1] += downOffsets[r];
    this.downArcs = new int[arcs];
    final int[] fill = Arrays.copyOf(downOffsets, nodeCount);
    // arcs are numbered by lower endpoint, so walking them in order lists each rank's down arcs by lower endpoint
    for (int a = 0; a < arcs; a++)
      downArcs[fill[upHeads[a]]++] = a;
  }

  /** Same as {@link #build(int, int[], int[], int, long, BooleanSupplier)}, never cancelled. */
  static CCHTopology build(final int nodeCount, final int[] tails, final int[] heads, final int arcCount, final long maxArcs) {
    return build(nodeCount, tails, heads, arcCount, maxArcs, null);
  }

  /**
   * Orders and contracts the graph whose arcs are {@code tails[i] -> heads[i]} for {@code i < arcCount}. Direction,
   * duplicates and self loops do not matter here: the topology is that of the underlying undirected simple graph.
   *
   * @param maxArcs   the most supergraph arcs the caller is willing to hold; a graph without small separators needs far
   *                  more than its own edge count, and is refused rather than allowed to exhaust the heap
   * @param cancelled polled between units of work; may be null
   *
   * @return the topology, or null when it would need more than {@code maxArcs} arcs or the work was cancelled
   */
  static CCHTopology build(final int nodeCount, final int[] tails, final int[] heads, final int arcCount, final long maxArcs,
      final BooleanSupplier cancelled) {
    return build(nodeCount, tails, heads, arcCount, maxArcs, null, cancelled);
  }

  /**
   * Same as {@link #build(int, int[], int[], int, long, BooleanSupplier)}, contracting in {@code order} (rank -> node)
   * when one is given instead of computing it: the order is the expensive part, and one persisted with the graph it was
   * computed for is still a valid order for it. An order that is not a permutation of the nodes is ignored.
   */
  static CCHTopology build(final int nodeCount, final int[] tails, final int[] heads, final int arcCount, final long maxArcs,
      final int[] order, final BooleanSupplier cancelled) {
    // 1. the undirected simple graph, as CSR with sorted, deduplicated slices
    final int[] degree = new int[nodeCount + 1];
    for (int i = 0; i < arcCount; i++) {
      final int u = tails[i];
      final int v = heads[i];
      if (u == v)
        continue;
      degree[u + 1]++;
      degree[v + 1]++;
    }
    for (int u = 0; u < nodeCount; u++)
      degree[u + 1] += degree[u];
    final int[] raw = new int[degree[nodeCount]];
    final int[] fill = Arrays.copyOf(degree, nodeCount);
    for (int i = 0; i < arcCount; i++) {
      final int u = tails[i];
      final int v = heads[i];
      if (u == v)
        continue;
      raw[fill[u]++] = v;
      raw[fill[v]++] = u;
    }
    final int[] offsets = new int[nodeCount + 1];
    int size = 0;
    for (int u = 0; u < nodeCount; u++) {
      final int start = degree[u];
      final int end = degree[u + 1];
      Arrays.sort(raw, start, end);
      offsets[u] = size;
      int last = -1;
      for (int e = start; e < end; e++)
        if (raw[e] != last) {
          last = raw[e];
          raw[size++] = last;
        }
    }
    offsets[nodeCount] = size;
    final int[] adjacency = Arrays.copyOf(raw, size);

    if (cancelled != null && cancelled.getAsBoolean())
      return null;

    // 2. the order
    final int[] nodeAt = isPermutation(order, nodeCount) ? order : CCHOrdering.order(nodeCount, offsets, adjacency, maxArcs,
        cancelled);
    if (nodeAt == null)
      return null;
    final int[] rankOf = new int[nodeCount];
    for (int r = 0; r < nodeCount; r++)
      rankOf[nodeAt[r]] = r;

    // 3. elimination: each rank's up-neighbours, minus the lowest of them, become up-neighbours of that lowest one
    final int[][] up = new int[nodeCount][];
    final int[] upSize = new int[nodeCount];
    for (int u = 0; u < nodeCount; u++) {
      final int ru = rankOf[u];
      for (int e = offsets[u], end = offsets[u + 1]; e < end; e++) {
        final int rv = rankOf[adjacency[e]];
        if (ru < rv)
          append(up, upSize, ru, rv);
      }
    }

    final int[] parent = new int[nodeCount];
    long total = 0;
    for (int r = 0; r < nodeCount; r++) {
      if ((r & 0xFFFF) == 0 && cancelled != null && cancelled.getAsBoolean())
        return null;
      final int[] list = up[r];
      final int count = upSize[r];
      if (count == 0) {
        parent[r] = -1;
        up[r] = null;
        continue;
      }
      Arrays.sort(list, 0, count);
      int unique = 0;
      int last = -1;
      for (int i = 0; i < count; i++)
        if (list[i] != last) {
          last = list[i];
          list[unique++] = last;
        }
      upSize[r] = unique;
      total += unique;
      if (total > maxArcs)
        return null;
      final int p = list[0];
      parent[r] = p;
      for (int i = 1; i < unique; i++)
        append(up, upSize, p, list[i]);
    }

    // 4. the up CSR
    final int[] upOffsets = new int[nodeCount + 1];
    for (int r = 0; r < nodeCount; r++)
      upOffsets[r + 1] = upOffsets[r] + upSize[r];
    final int arcs = upOffsets[nodeCount];
    final int[] upHeads = new int[arcs];
    final int[] arcTails = new int[arcs];
    for (int r = 0; r < nodeCount; r++) {
      final int count = upSize[r];
      if (count > 0) {
        System.arraycopy(up[r], 0, upHeads, upOffsets[r], count);
        Arrays.fill(arcTails, upOffsets[r], upOffsets[r] + count, r);
      }
      up[r] = null;
    }
    return new CCHTopology(nodeCount, rankOf, nodeAt, upOffsets, upHeads, arcTails, parent);
  }

  private static boolean isPermutation(final int[] order, final int nodeCount) {
    if (order == null || order.length != nodeCount)
      return false;
    final boolean[] seen = new boolean[nodeCount];
    for (final int node : order) {
      if (node < 0 || node >= nodeCount || seen[node])
        return false;
      seen[node] = true;
    }
    return true;
  }

  private static void append(final int[][] lists, final int[] sizes, final int index, final int value) {
    int[] list = lists[index];
    final int size = sizes[index];
    if (list == null)
      lists[index] = list = new int[4];
    else if (size == list.length)
      lists[index] = list = Arrays.copyOf(list, size * 2);
    list[size] = value;
    sizes[index] = size + 1;
  }

  int arcCount() {
    return upHeads.length;
  }

  /** The arc joining {@code lowRank} to {@code highRank}, or -1 when the supergraph has none. */
  int findArc(final int lowRank, final int highRank) {
    final int found = Arrays.binarySearch(upHeads, upOffsets[lowRank], upOffsets[lowRank + 1], highRank);
    return found >= 0 ? found : -1;
  }

  /** The longest elimination-tree chain, which bounds the vertices any one direction of a query visits. */
  int maxSearchSpace() {
    final int[] chain = new int[nodeCount];
    int max = 0;
    for (int r = nodeCount - 1; r >= 0; r--) {
      chain[r] = parent[r] < 0 ? 1 : chain[parent[r]] + 1;
      if (chain[r] > max)
        max = chain[r];
    }
    return max;
  }

  /** Heap held by the arrays of this topology, excluding the pooled query scratch. */
  long getMemoryUsageBytes() {
    return 4L * (rankOf.length + nodeAt.length + upOffsets.length + upHeads.length + arcTails.length + parent.length
        + downOffsets.length + downArcs.length);
  }

  CCHMetric.QueryState borrowState() {
    final CCHMetric.QueryState state = statePool.poll();
    return state != null ? state : new CCHMetric.QueryState(nodeCount);
  }

  void returnState(final CCHMetric.QueryState state) {
    if (statePool.size() < MAX_POOLED_STATES)
      statePool.offer(state);
  }
}
