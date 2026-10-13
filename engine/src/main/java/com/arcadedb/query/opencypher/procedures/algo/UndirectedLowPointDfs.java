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
package com.arcadedb.query.opencypher.procedures.algo;

import java.util.Arrays;

/**
 * Tarjan's low-point depth-first search of the UNDIRECTED graph, the one walk {@code algo.bridges},
 * {@code algo.articulationPoints} and {@code algo.biconnectedComponents} read their answers off (issue #9766).
 * <p>
 * The three used to carry a copy each, and the copies drifted: {@code algo.bridges} walked the outgoing adjacency only,
 * so an edge was seen from its source alone, and it reported bridges a triangle does not have and missed the only bridge
 * of a barbell whose connector was stored the other way round. Walking one shared DFS is what keeps the direction from
 * differing between them again.
 * <p>
 * The adjacency must be the {@code BOTH} one, where every edge appears once from each of its endpoints (a self-loop twice
 * in its own row). The tree edge back to the parent is skipped by OCCURRENCE, not by endpoint: only the first entry of
 * the parent in a vertex's row is the tree edge, and any further one is a parallel edge, hence a back edge. Skipping by
 * endpoint would discard the parallel edge too and declare both edges of a doubled pair to be bridges, although removing
 * one leaves the other.
 * <p>
 * Iterative, so a path-shaped graph of any length cannot overflow the thread stack. Every non-tree edge of an undirected
 * DFS joins a vertex to one of its ancestors; it is reported once, from the descendant's side.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class UndirectedLowPointDfs {

  /** Callbacks in DFS order. {@link #disc} and {@link #low} may be read from inside them. */
  interface Visitor {
    /** {@code child} is discovered through the tree edge {@code parent - child}. */
    default void treeEdge(final int parent, final int child) {
    }

    /** The non-tree edge {@code descendant - ancestor}, reported once, from the descendant. */
    default void backEdge(final int descendant, final int ancestor) {
    }

    /** The subtree of {@code child} is complete: {@code low[child]} is final and already folded into {@code parent}. */
    default void childFinished(final int parent, final int child) {
    }

    /** Every vertex reachable from {@code root} has been visited. */
    default void rootFinished(final int root) {
    }
  }

  /** Discovery time of each vertex. */
  final int[] disc;
  /** The lowest discovery time reachable from the vertex's subtree through at most one back edge. */
  final int[] low;
  /** The DFS parent of each vertex, -1 for a root. */
  final int[] parent;

  private final int[][] adj;

  /**
   * @param adj the {@code Vertex.DIRECTION.BOTH} adjacency
   */
  UndirectedLowPointDfs(final int[][] adj) {
    this.adj = adj;
    final int n = adj.length;
    disc = new int[n];
    low = new int[n];
    parent = new int[n];
  }

  /**
   * Walks every connected component of the graph, roots taken in index order. The visitor may read {@link #disc},
   * {@link #low} and {@link #parent} while it is notified.
   */
  void walk(final Visitor visitor) {
    final int n = adj.length;
    Arrays.fill(disc, -1);
    Arrays.fill(parent, -1);

    // EXPLICIT CALL STACK: THE VERTEX AND HOW FAR ITS ROW HAS BEEN READ
    final int[] stack = new int[n];
    final int[] edgeIdx = new int[n];
    // WHETHER THE ENTRY OF THE TREE EDGE BACK TO THE PARENT HAS ALREADY BEEN SKIPPED IN THE VERTEX'S ROW
    final boolean[] parentEntrySkipped = new boolean[n];
    int timer = 0;

    for (int root = 0; root < n; root++) {
      if (disc[root] != -1)
        continue;

      int top = 0;
      stack[top] = root;
      edgeIdx[top] = 0;
      disc[root] = low[root] = timer++;

      while (top >= 0) {
        final int v = stack[top];
        final int[] row = adj[v];
        if (edgeIdx[top] < row.length) {
          final int u = row[edgeIdx[top]++];
          if (disc[u] == -1) {
            parent[u] = v;
            disc[u] = low[u] = timer++;
            visitor.treeEdge(v, u);
            top++;
            stack[top] = u;
            edgeIdx[top] = 0;
          } else if (u == parent[v] && !parentEntrySkipped[v]) {
            // THE TREE EDGE ITSELF, SEEN FROM THE CHILD: ONLY ITS FIRST OCCURRENCE, A SECOND ONE IS A PARALLEL EDGE
            parentEntrySkipped[v] = true;
          } else if (disc[u] < disc[v]) {
            // TO AN ANCESTOR (OR A PARALLEL EDGE TO THE PARENT). THE ANCESTOR'S OWN ENTRY OF THE SAME EDGE HAS
            // disc[u] > disc[v] FROM ITS SIDE AND IS IGNORED, AS IS A SELF-LOOP
            low[v] = Math.min(low[v], disc[u]);
            visitor.backEdge(v, u);
          }
        } else {
          top--;
          if (top >= 0) {
            final int p = stack[top];
            low[p] = Math.min(low[p], low[v]);
            visitor.childFinished(p, v);
          } else
            visitor.rootFinished(v);
        }
      }
    }
  }
}
