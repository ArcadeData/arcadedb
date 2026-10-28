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
package com.arcadedb.index.columnar;

import com.arcadedb.query.facet.FacetValueComparator;

import java.util.Map;

/**
 * The node space of one hierarchy, as built from the tag graph at one graph generation: a node per distinct key value, the parents
 * of each node in CSR form (the parents of node {@code n} are {@code parents[parentOffsets[n]..parentOffsets[n + 1])}, deduplicated,
 * sorted by value, each in {@code [0, nodeCount)}), the depth of each node and its orphan flag. It holds no dictionary ordinal: the
 * ordinal to node mapping is the collector's, through {@link #nodeOfValue}.
 * <p>
 * Immutable once built and safe to share between threads; the arrays returned are the view's own and must not be modified.
 */
public final class HierarchyView {
  private static final boolean[] NO_ORPHANS = new boolean[0];

  private final long                 generation;
  private final int[]                parentOffsets;
  private final int[]                parents;
  private final Object[]             values;
  private final int[]                depth;
  private final boolean[]            orphan;
  private final Map<Object, Integer> keyToNode;
  private final int                  cycleNodes;
  private final long                 cycleSignature;
  private final int                  duplicateKeys;
  private final int                  nullKeys;

  /** {@code orphan} null means no node is an orphan. */
  HierarchyView(final long generation, final int[] parentOffsets, final int[] parents, final Object[] values, final int[] depth,
      final boolean[] orphan, final Map<Object, Integer> keyToNode, final int cycleNodes, final long cycleSignature, final int duplicateKeys,
      final int nullKeys) {
    this.generation = generation;
    this.parentOffsets = parentOffsets;
    this.parents = parents;
    this.values = values;
    this.depth = depth;
    this.orphan = orphan != null ? orphan : NO_ORPHANS;
    this.keyToNode = keyToNode;
    this.cycleNodes = cycleNodes;
    this.cycleSignature = cycleSignature;
    this.duplicateKeys = duplicateKeys;
    this.nullKeys = nullKeys;
  }

  public int nodeCount() {
    return values.length;
  }

  /** CSR offsets into {@link #parents()}, {@code nodeCount() + 1} entries. */
  public int[] parentOffsets() {
    return parentOffsets;
  }

  public int[] parents() {
    return parents;
  }

  /** The key value of the node (normalized as {@link FacetValueComparator#normalize} does). */
  public Object value(final int node) {
    return values[node];
  }

  /** Shortest distance from a root (a node without parents), 0 for a root; -1 when no root reaches the node (a cycle above it). */
  public int depth(final int node) {
    return depth[node];
  }

  /**
   * True for a node that stands for a value with no vertex in the hierarchy. A view built from the graph has a vertex behind every node,
   * so this is false for all of them (no flag array is kept): orphan values are resolved by the collector, which finds no node for
   * them.
   */
  public boolean orphan(final int node) {
    return node < orphan.length && orphan[node];
  }

  /** The node of a key value, -1 when no vertex of the hierarchy has that key. */
  public int nodeOfValue(final Object value) {
    if (value == null)
      return -1;
    final Integer node = keyToNode.get(FacetValueComparator.normalize(value));
    return node != null ? node : -1;
  }

  /** The graph generation the view was built from. */
  public long generation() {
    return generation;
  }

  /** The number of nodes that lie on a cycle of parent links (a node that is its own parent included). */
  public int cycleNodes() {
    return cycleNodes;
  }

  /**
   * Whether this view has the same set of nodes on a cycle as described: the same count and the same order-independent signature of
   * their values (a 64-bit hash: a collision only skips one log line).
   */
  boolean sameCycles(final int otherCycleNodes, final long otherCycleSignature) {
    return cycleNodes == otherCycleNodes && cycleSignature == otherCycleSignature;
  }

  /** The number of vertices whose key another vertex already had: they share its node, their parents unioned. */
  public int duplicateKeys() {
    return duplicateKeys;
  }

  /** The number of vertices skipped because their key is null. */
  public int nullKeys() {
    return nullKeys;
  }
}
