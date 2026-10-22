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

import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;

import java.util.Arrays;

/**
 * A breadth-first search over a {@link GraphTraversalProvider}'s adjacency that is expanded one level at a time, so the caller
 * stops it at the depth it needs instead of paying for the whole reachable graph (issue #10548).
 * <p>
 * Every node is found once, at its shallowest depth. The nodes found so far are kept in discovery order, the start first, and
 * the visited set is a bitmap sized once for the provider's node id space and cleared afterwards only at the bits the search
 * set: one instance runs any number of searches, each costing what it reaches and not what the graph holds. The neighbours are
 * read from the provider's packed {@link NeighborView} when it has one ready (or the reads are expected to pay for it), and per
 * node otherwise.
 * <p>
 * Not thread-safe: one instance serves one caller at a time. The provider is expected to be pinned at one snapshot for as long as
 * the instance is used, since the node ids of two snapshots do not name the same nodes.
 */
public final class BoundedBreadthFirstSearch {
  /** How often, in nodes expanded, the search asks its {@link WorkCheckpoint} whether to go on. */
  private static final int CHECK_EVERY = 1024;

  /**
   * Told about every adjacency entry a level expansion reads, for a caller that needs more than the set of nodes found: the
   * branch a node was found in, or whether an entry leads back to the start. Both methods are called while the level of
   * {@code from} is expanded, so {@link #depth()} is the depth of {@code from}.
   */
  public interface EntryListener {
    /** {@code to} was found for the first time, from {@code from}. */
    void discovered(int from, int to);

    /** {@code to} was found already, the start included. */
    void revisited(int from, int to);
  }

  private final GraphTraversalProvider provider;
  private final Vertex.DIRECTION       direction;
  private final String[]               edgeTypes;
  private final WorkCheckpoint         checkpoint;
  private       NeighborView           view;
  private       boolean                viewAsked;
  private       long[]                 visited;
  private       int[]                  order = new int[16];
  private       int                    size;
  private       int                    levelStart;
  private       int                    levelEnd;
  private       int                    depth;
  private       int                    expanded;

  /**
   * @param provider   the adjacency, pinned at the snapshot the search reads
   * @param direction  the direction the relationships are followed in
   * @param edgeTypes  the relationship types followed, empty or null for every type
   * @param checkpoint asked every {@value #CHECK_EVERY} nodes expanded whether to go on, {@link WorkCheckpoint#NONE} for never
   */
  public BoundedBreadthFirstSearch(final GraphTraversalProvider provider, final Vertex.DIRECTION direction, final String[] edgeTypes,
      final WorkCheckpoint checkpoint) {
    this.provider = provider;
    this.direction = direction;
    this.edgeTypes = edgeTypes != null ? edgeTypes : new String[0];
    this.checkpoint = checkpoint != null ? checkpoint : WorkCheckpoint.NONE;
  }

  /**
   * Starts a new search from {@code source}, forgetting the previous one.
   *
   * @param source         the dense node id to start from
   * @param expectedVisits about how many adjacency entries the search is expected to read, which decides whether the packed
   *                       view is worth building (see {@link GraphTraversalProvider#getNeighborViewForVisits}); asked on the first
   *                       search only
   */
  public void start(final int source, final long expectedVisits) {
    if (!viewAsked) {
      viewAsked = true;
      view = provider.getNeighborViewForVisits(expectedVisits, direction, edgeTypes);
    }
    if (visited == null)
      visited = new long[(Math.max(provider.getNodeIdUpperBound(), source + 1) + 63) >>> 6];
    else
      for (int i = 0; i < size; i++)
        clear(order[i]);

    size = 0;
    depth = 0;
    expanded = 0;
    append(source);
    mark(source);
    levelStart = 0;
    levelEnd = 1;
  }

  /** Whether the last level found anything left to expand. */
  public boolean hasFrontier() {
    return levelStart < levelEnd;
  }

  /** The depth of the level the next {@link #expandLevel} expands: 0 before the first expansion. */
  public int depth() {
    return depth;
  }

  /** How many nodes the search has found, the start included. */
  public int size() {
    return size;
  }

  /** The {@code index}-th node found, in discovery order: 0 is the start, then the nodes at depth 1, at depth 2 and so on. */
  public int node(final int index) {
    return order[index];
  }

  /** Whether the search has found {@code nodeId}. */
  public boolean isVisited(final int nodeId) {
    final int word = nodeId >>> 6;
    return word < visited.length && (visited[word] & (1L << nodeId)) != 0;
  }

  /**
   * Expands the current level: finds the nodes one hop further, appended after the ones found so far, and makes them the next
   * level. Does nothing once a level finds nothing new.
   *
   * @param listener told about every entry read, or null
   *
   * @return true if the level found at least one new node
   */
  public boolean expandLevel(final EntryListener listener) {
    if (levelStart >= levelEnd)
      return false;
    final int end = levelEnd;
    for (int i = levelStart; i < end; i++) {
      if ((++expanded % CHECK_EVERY) == 0)
        checkpoint.check();
      final int from = order[i];
      if (view != null && from < view.nodeCount()) {
        final int[] neighbors = view.neighbors();
        for (int j = view.offset(from), last = view.offsetEnd(from); j < last; j++)
          reach(from, neighbors[j], listener);
      } else {
        final int[] neighbors = provider.getNeighborIds(from, direction, edgeTypes);
        if (neighbors != null)
          for (final int to : neighbors)
            reach(from, to, listener);
      }
    }
    levelStart = end;
    levelEnd = size;
    depth++;
    return levelStart < levelEnd;
  }

  private void reach(final int from, final int to, final EntryListener listener) {
    if (to < 0)
      return;
    if (isVisited(to)) {
      if (listener != null)
        listener.revisited(from, to);
      return;
    }
    // A hole left behind by a deleted node is no node at all: it is not found, and leads nowhere
    if (!provider.isNodeLive(to))
      return;
    mark(to);
    append(to);
    if (listener != null)
      listener.discovered(from, to);
  }

  private void append(final int nodeId) {
    if (size == order.length)
      order = Arrays.copyOf(order, order.length << 1);
    order[size++] = nodeId;
  }

  private void mark(final int nodeId) {
    final int word = nodeId >>> 6;
    if (word >= visited.length)
      visited = Arrays.copyOf(visited, Math.max(word + 1, visited.length << 1));
    visited[word] |= 1L << nodeId;
  }

  private void clear(final int nodeId) {
    visited[nodeId >>> 6] &= ~(1L << nodeId);
  }
}
