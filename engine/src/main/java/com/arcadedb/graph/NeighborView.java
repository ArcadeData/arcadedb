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
}
