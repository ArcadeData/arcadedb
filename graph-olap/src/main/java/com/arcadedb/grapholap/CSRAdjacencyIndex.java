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
package com.arcadedb.grapholap;

/**
 * Compressed Sparse Row (CSR) adjacency index for high-performance graph traversal.
 * <p>
 * Stores graph topology in two flat arrays per direction:
 * <ul>
 *   <li><b>offsets[N+1]</b>: offset into neighbors array for each node's adjacency list</li>
 *   <li><b>neighbors[E]</b>: dense IDs of neighbor nodes, contiguous per source node</li>
 * </ul>
 * <p>
 * Neighbors of node v = neighbors[offsets[v] .. offsets[v+1]).
 * <p>
 * All arrays use {@code int[]} (not {@code long[]}) because Java arrays are capped
 * at {@code Integer.MAX_VALUE} entries (~2.1B). This saves 50% memory on offset arrays
 * compared to {@code long[]} with no loss of capacity — the neighbors array itself
 * cannot exceed 2.1B entries regardless of offset type. For graphs exceeding this limit,
 * a segmented off-heap storage approach would be needed (future phase).
 * <p>
 * This layout is optimal for:
 * <ul>
 *   <li>Sequential memory access (cache-line friendly)</li>
 *   <li>SIMD-friendly batch processing (contiguous int arrays)</li>
 *   <li>Zero GC pressure (no objects, only primitives)</li>
 *   <li>Parallel processing (each node's range is independent)</li>
 * </ul>
 * <p>
 * Double-indexed: both forward (OUT) and backward (IN) CSR are maintained,
 * following the Kuzu/LadybugDB pattern for bidirectional traversal at equal speed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CSRAdjacencyIndex {
  // Forward CSR (outgoing edges: source → targets)
  private final int[] fwdOffsets;
  private final int[] fwdNeighbors;

  // Backward CSR (incoming edges: target → sources)
  private final int[] bwdOffsets;
  private final int[] bwdNeighbors;

  private final int nodeCount;
  private final int edgeCount;

  CSRAdjacencyIndex(final int[] fwdOffsets, final int[] fwdNeighbors,
      final int[] bwdOffsets, final int[] bwdNeighbors,
      final int nodeCount, final int edgeCount) {
    this.fwdOffsets = fwdOffsets;
    this.fwdNeighbors = fwdNeighbors;
    this.bwdOffsets = bwdOffsets;
    this.bwdNeighbors = bwdNeighbors;
    this.nodeCount = nodeCount;
    this.edgeCount = edgeCount;
  }

  /**
   * Returns the outgoing degree for the given node.
   */
  public int outDegree(final int nodeId) {
    return fwdOffsets[nodeId + 1] - fwdOffsets[nodeId];
  }

  /**
   * Returns the incoming degree for the given node.
   */
  public int inDegree(final int nodeId) {
    return bwdOffsets[nodeId + 1] - bwdOffsets[nodeId];
  }

  /**
   * Returns the start offset into the forward neighbors array for a node.
   */
  public int outOffset(final int nodeId) {
    return fwdOffsets[nodeId];
  }

  /**
   * Returns the end offset (exclusive) into the forward neighbors array for a node.
   */
  public int outOffsetEnd(final int nodeId) {
    return fwdOffsets[nodeId + 1];
  }

  /**
   * Returns the start offset into the backward neighbors array for a node.
   */
  public int inOffset(final int nodeId) {
    return bwdOffsets[nodeId];
  }

  /**
   * Returns the end offset (exclusive) into the backward neighbors array for a node.
   */
  public int inOffsetEnd(final int nodeId) {
    return bwdOffsets[nodeId + 1];
  }

  /**
   * Returns a specific outgoing neighbor by index within the node's adjacency list.
   */
  public int outNeighbor(final int nodeId, final int index) {
    return fwdNeighbors[fwdOffsets[nodeId] + index];
  }

  /**
   * Returns a specific incoming neighbor by index within the node's adjacency list.
   */
  public int inNeighbor(final int nodeId, final int index) {
    return bwdNeighbors[bwdOffsets[nodeId] + index];
  }

  /**
   * Direct access to the forward neighbors array for vectorized batch processing.
   * The array is the internal buffer — do NOT modify it.
   */
  public int[] getForwardNeighbors() {
    return fwdNeighbors;
  }

  /**
   * Direct access to the backward neighbors array for vectorized batch processing.
   */
  public int[] getBackwardNeighbors() {
    return bwdNeighbors;
  }

  /**
   * Direct access to the forward offsets array.
   */
  public int[] getForwardOffsets() {
    return fwdOffsets;
  }

  /**
   * Direct access to the backward offsets array.
   */
  public int[] getBackwardOffsets() {
    return bwdOffsets;
  }

  public int getNodeCount() {
    return nodeCount;
  }

  public int getEdgeCount() {
    return edgeCount;
  }

  /**
   * Returns the total memory footprint in bytes (approximate).
   */
  public long getMemoryUsageBytes() {
    return (long) fwdOffsets.length * Integer.BYTES
        + (long) fwdNeighbors.length * Integer.BYTES
        + (long) bwdOffsets.length * Integer.BYTES
        + (long) bwdNeighbors.length * Integer.BYTES;
  }
}
