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
package com.arcadedb.graph;

/**
 * Zero-allocation view over a packed CSR adjacency structure.
 * <p>
 * Provides offset-based access to neighbor arrays without materializing per-node {@code int[]}
 * arrays. Algorithms iterate neighbors of node {@code v} as:
 * <pre>
 *   final int[] nbrs = view.neighbors();
 *   for (int j = view.offset(v), end = view.offsetEnd(v); j < end; j++) {
 *     final int neighbor = nbrs[j];
 *     // ...
 *   }
 * </pre>
 * <p>
 * Each node's neighbour range is in ascending id order (parallel edges repeat the id), which the sorted-intersection and
 * binary-search consumers rely on.
 * <p>
 * For single-edge-type CSR, this is a zero-copy wrapper over the internal arrays.
 * For multi-edge-type or overlay scenarios, a merged structure is built once (O(E) total,
 * not O(N) individual arrays).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NeighborView {
  private final int   nodeCount;
  private final int[] offsets;   // length = nodeCount + 1
  private final int[] neighbors; // packed neighbor IDs
  // THE VIEW WITH EACH SELF LOOP LISTED ONCE, BUILT ON FIRST USE: A CACHED VIEW IS SHARED BY EVERY QUERY ON ITS SNAPSHOT.
  // TWO THREADS MAY BOTH BUILD IT, AND EITHER COPY IS THE SAME VIEW
  private volatile NeighborView selfLoopsOnce;

  public NeighborView(final int nodeCount, final int[] offsets, final int[] neighbors) {
    this.nodeCount = nodeCount;
    this.offsets = offsets;
    this.neighbors = neighbors;
  }

  /** Returns the number of nodes in this view. */
  public int nodeCount() {
    return nodeCount;
  }

  /** Returns the degree (number of neighbors) of the given node. */
  public int degree(final int nodeId) {
    return offsets[nodeId + 1] - offsets[nodeId];
  }

  /** Returns the start offset (inclusive) into {@link #neighbors()} for the given node. */
  public int offset(final int nodeId) {
    return offsets[nodeId];
  }

  /** Returns the end offset (exclusive) into {@link #neighbors()} for the given node. */
  public int offsetEnd(final int nodeId) {
    return offsets[nodeId + 1];
  }

  /** Returns a specific neighbor by index within the node's adjacency list. */
  public int neighbor(final int nodeId, final int index) {
    return neighbors[offsets[nodeId] + index];
  }

  /**
   * Returns the packed neighbor array. Do NOT modify — this may be the CSR's internal buffer.
   */
  public int[] neighbors() {
    return neighbors;
  }

  /** Returns the total number of edges in this view. */
  public int edgeCount() {
    return neighbors.length;
  }

  /**
   * This view with each self loop listed once, for a view of {@link Vertex.DIRECTION#BOTH} (issues #8750, #9283). An
   * undirected view merges the outgoing and the incoming adjacency of every node, and a self loop sits in both, so the
   * node is listed among its own neighbors twice; an undirected relationship pattern matches it once. Meaningless for a
   * view of one direction, where a self loop is listed once already.
   * <p>
   * Built once per view and kept with it, so the merged views a provider caches pay for it once per snapshot rather than
   * once per query. A view without self loops - the common case - answers itself after one scan of its entries; one
   * with self loops keeps a second CSR alive for as long as the view itself.
   */
  public NeighborView withSelfLoopsOnce() {
    NeighborView once = selfLoopsOnce;
    if (once == null) {
      once = buildWithSelfLoopsOnce();
      selfLoopsOnce = once;
    }
    return once;
  }

  /**
   * Copies the neighbors of {@code node} into {@code target} from {@code pos}, keeping one entry of the two each self
   * loop has in the merged range of an undirected view: of {@code n} entries of the node itself, {@code n - n / 2} are
   * kept, which is every one of them less a copy per pair.
   *
   * @return the position after the last entry copied
   */
  public int copyNeighborsWithSelfLoopsOnce(final int node, final int[] target, int pos) {
    boolean skip = false;
    for (int j = offsets[node], end = offsets[node + 1]; j < end; j++) {
      final int neighbor = neighbors[j];
      if (neighbor == node) {
        skip = !skip;
        if (!skip)
          continue;
      }
      target[pos++] = neighbor;
    }
    return pos;
  }

  private NeighborView buildWithSelfLoopsOnce() {
    // THE COPIES DROPPED ARE COUNTED PER NODE: AN ODD COUNT ON TWO NODES DROPS NONE, WHERE HALF THEIR SUM WOULD DROP ONE
    int dropped = 0;
    int entries = 0;
    for (int v = 0; v < nodeCount; v++) {
      final int end = offsets[v + 1];
      entries += end - offsets[v];
      int selfEntries = 0;
      for (int j = offsets[v]; j < end; j++)
        if (neighbors[j] == v)
          ++selfEntries;
      dropped += selfEntries / 2;
    }
    if (dropped == 0)
      return this;

    // SIZED ON THE RANGES, NOT ON THE ARRAY: A ZERO-COPY VIEW MAY BE BACKED BY A LARGER BUFFER
    final int[] onceOffsets = new int[nodeCount + 1];
    final int[] onceNeighbors = new int[entries - dropped];
    int pos = 0;
    for (int v = 0; v < nodeCount; v++) {
      onceOffsets[v] = pos;
      pos = copyNeighborsWithSelfLoopsOnce(v, onceNeighbors, pos);
    }
    onceOffsets[nodeCount] = pos;
    final NeighborView once = new NeighborView(nodeCount, onceOffsets, onceNeighbors);
    once.selfLoopsOnce = once;
    return once;
  }
}
