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
package com.arcadedb.query.facet;

import java.util.Arrays;

/**
 * Computes the ancestor closure of a set of hierarchy nodes over a CSR parents array. Each node of the closure appears once, so a
 * document that carries two tags in the same subtree counts the shared ancestors once. Cycles and shared ancestors (DAG) terminate.
 * <p>
 * The visited set is a reusable bitset and {@link #nodes()} doubles as the DFS stack, so the per-document cost is proportional to the
 * size of the closure: only the bits of the previous closure are cleared. One instance per collector task, not thread-safe.
 */
public final class AncestorExpander {
  private long[] visited;
  private int[]  nodes;
  private int    size;

  public AncestorExpander(final int nodeCapacity) {
    final int capacity = Math.max(nodeCapacity, 1);
    visited = new long[(capacity + 63) >>> 6];
    nodes = new int[capacity];
  }

  /**
   * Expands {@code startNodes} to its closure over parents. Negative entries of {@code startNodes} (orphans) and -1 parents are
   * ignored; any other value outside {@code [0, nodeCount)} is a builder bug and fails an assertion.
   * The parents of node {@code n} are {@code parents[parentOffsets[n]..parentOffsets[n + 1])}.
   *
   * @return the number of nodes in the closure, readable from {@link #nodes()}
   */
  public int expand(final int[] parentOffsets, final int[] parents, final int nodeCount, final int[] startNodes, final int count) {
    reset(nodeCount);

    // size is advanced with every node marked, so reset() clears exactly the marked bits even if this method throws midway
    for (int i = 0; i < count; i++) {
      final int start = startNodes[i];
      if (start < 0)
        continue;
      assert start < nodeCount : "Start node " + start + " out of range, node count " + nodeCount;
      if (markVisited(start))
        nodes[size++] = start;
    }

    for (int i = 0; i < size; i++) {
      final int node = nodes[i];
      for (int p = parentOffsets[node], end = parentOffsets[node + 1]; p < end; p++) {
        final int parent = parents[p];
        if (parent < 0) {
          assert parent == -1 : "Parent " + parent + " of node " + node + " is invalid";
          continue;
        }
        assert parent < nodeCount : "Parent " + parent + " of node " + node + " out of range, node count " + nodeCount;
        if (markVisited(parent))
          nodes[size++] = parent;
      }
    }

    return size;
  }

  /**
   * @return the closure computed by the last {@link #expand}: only the first {@code expand} return value entries are meaningful,
   * and the array is valid until the next {@code expand}
   */
  public int[] nodes() {
    return nodes;
  }

  private void reset(final int nodeCount) {
    for (int i = 0; i < size; i++) {
      final int node = nodes[i];
      visited[node >>> 6] = 0L;
    }
    size = 0;

    final int words = (nodeCount + 63) >>> 6;
    if (words > visited.length)
      visited = Arrays.copyOf(visited, words);
    if (nodeCount > nodes.length)
      nodes = new int[nodeCount];
  }

  /** @return true when the node was not visited before */
  private boolean markVisited(final int node) {
    final int word = node >>> 6;
    final long bit = 1L << node;
    if ((visited[word] & bit) != 0L)
      return false;
    visited[word] |= bit;
    return true;
  }
}
