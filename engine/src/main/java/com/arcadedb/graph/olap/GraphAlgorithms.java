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

import com.arcadedb.graph.Vertex;

import java.util.Arrays;
import java.util.Random;

/**
 * Graph algorithms operating directly on the CSR arrays of a {@link GraphAnalyticalView}.
 * All algorithms run in-memory on the packed int[] arrays with zero GC pressure.
 * <p>
 * Algorithms:
 * <ul>
 *   <li>{@link #pageRank} — iterative PageRank with configurable damping and iterations</li>
 *   <li>{@link #connectedComponents} — union-find based weakly connected components</li>
 *   <li>{@link #shortestPath} — BFS-based unweighted shortest path (returns hop count)</li>
 *   <li>{@link #labelPropagation} — community detection via label propagation</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GraphAlgorithms {

  private GraphAlgorithms() {
  }

  // --- PageRank ---

  /**
   * Computes PageRank over the given view for the specified edge types.
   *
   * @param view       the analytical view (must be built)
   * @param damping    damping factor, typically 0.85
   * @param iterations number of power-iteration steps
   * @param edgeTypes  edge types to traverse (null or empty = all)
   * @return double[] of ranks indexed by dense node ID
   */
  public static double[] pageRank(final GraphAnalyticalView view, final double damping,
      final int iterations, final String... edgeTypes) {
    final int n = view.getNodeMapping().size();
    if (n == 0)
      return new double[0];

    double[] rank = new double[n];
    double[] next = new double[n];
    final double initial = 1.0 / n;
    Arrays.fill(rank, initial);

    final String[] types = resolveEdgeTypes(view, edgeTypes);

    // Precompute dangling nodes (no outgoing edges across all relevant edge types)
    final boolean[] isDangling = new boolean[n];
    for (int u = 0; u < n; u++) {
      boolean hasDeg = false;
      for (final String edgeType : types) {
        final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
        if (csr != null && csr.outDegree(u) > 0) {
          hasDeg = true;
          break;
        }
      }
      isDangling[u] = !hasDeg;
    }

    for (int iter = 0; iter < iterations; iter++) {
      final double teleport = (1.0 - damping) / n;
      Arrays.fill(next, teleport);

      for (final String edgeType : types) {
        final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
        if (csr == null)
          continue;
        final int[] fwdOffsets = csr.getForwardOffsets();
        final int[] fwdNeighbors = csr.getForwardNeighbors();

        for (int u = 0; u < n; u++) {
          final int outDeg = fwdOffsets[u + 1] - fwdOffsets[u];
          if (outDeg == 0)
            continue;
          final double contrib = damping * rank[u] / outDeg;
          final int start = fwdOffsets[u];
          final int end = fwdOffsets[u + 1];
          for (int j = start; j < end; j++)
            next[fwdNeighbors[j]] += contrib;
        }
      }

      // Handle dangling nodes: distribute their rank evenly
      double danglingSum = 0.0;
      for (int u = 0; u < n; u++)
        if (isDangling[u])
          danglingSum += rank[u];
      if (danglingSum > 0.0) {
        final double danglingContrib = damping * danglingSum / n;
        for (int u = 0; u < n; u++)
          next[u] += danglingContrib;
      }

      // Swap
      final double[] tmp = rank;
      rank = next;
      next = tmp;
    }
    return rank;
  }

  /**
   * Computes PageRank with default parameters: damping=0.85, iterations=20.
   */
  public static double[] pageRank(final GraphAnalyticalView view, final String... edgeTypes) {
    return pageRank(view, 0.85, 20, edgeTypes);
  }

  // --- Connected Components (Union-Find) ---

  /**
   * Computes weakly connected components using union-find with path compression and union by rank.
   * Treats all edges as undirected.
   *
   * @param view      the analytical view (must be built)
   * @param edgeTypes edge types to consider (null or empty = all)
   * @return int[] of component IDs indexed by dense node ID (component ID = root node ID)
   */
  public static int[] connectedComponents(final GraphAnalyticalView view, final String... edgeTypes) {
    final int n = view.getNodeMapping().size();
    if (n == 0)
      return new int[0];

    final int[] parent = new int[n];
    final int[] rnk = new int[n];
    for (int i = 0; i < n; i++)
      parent[i] = i;

    final String[] types = resolveEdgeTypes(view, edgeTypes);

    for (final String edgeType : types) {
      final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
      if (csr == null)
        continue;
      final int[] fwdOffsets = csr.getForwardOffsets();
      final int[] fwdNeighbors = csr.getForwardNeighbors();

      for (int u = 0; u < n; u++) {
        final int start = fwdOffsets[u];
        final int end = fwdOffsets[u + 1];
        for (int j = start; j < end; j++)
          union(parent, rnk, u, fwdNeighbors[j]);
      }
    }

    // Flatten: ensure every node points directly to its root
    for (int i = 0; i < n; i++)
      parent[i] = find(parent, i);

    return parent;
  }

  /**
   * Returns the number of distinct connected components.
   */
  public static int countComponents(final int[] components) {
    if (components.length == 0)
      return 0;
    final boolean[] seen = new boolean[components.length];
    int count = 0;
    for (final int c : components) {
      if (!seen[c]) {
        seen[c] = true;
        count++;
      }
    }
    return count;
  }

  // --- Shortest Path (BFS, unweighted) ---

  /**
   * Computes the shortest path (hop count) between two nodes using BFS.
   * Uses the specified direction for traversal.
   *
   * @param view      the analytical view
   * @param source    source dense node ID
   * @param target    target dense node ID
   * @param direction traversal direction (OUT, IN, or BOTH)
   * @param edgeTypes edge types to traverse (null or empty = all)
   * @return hop count, or -1 if no path exists
   */
  public static int shortestPath(final GraphAnalyticalView view, final int source, final int target,
      final Vertex.DIRECTION direction, final String... edgeTypes) {
    if (source == target)
      return 0;

    final int n = view.getNodeMapping().size();
    if (source < 0 || source >= n || target < 0 || target >= n)
      return -1;

    final String[] types = resolveEdgeTypes(view, edgeTypes);

    // BFS using two int[] arrays as queue (no object allocation)
    final int[] dist = new int[n];
    Arrays.fill(dist, -1);
    dist[source] = 0;

    int[] frontier = new int[Math.min(n, 1024)];
    frontier[0] = source;
    int frontierSize = 1;

    while (frontierSize > 0) {
      int nextSize = 0;
      int[] nextFrontier = new int[Math.min(n, frontierSize * 4)];

      for (int f = 0; f < frontierSize; f++) {
        final int u = frontier[f];
        final int nextDist = dist[u] + 1;

        for (final String edgeType : types) {
          final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
          if (csr == null)
            continue;

          if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
            final int[] fwdOffsets = csr.getForwardOffsets();
            final int[] fwdNeighbors = csr.getForwardNeighbors();
            for (int j = fwdOffsets[u]; j < fwdOffsets[u + 1]; j++) {
              final int v = fwdNeighbors[j];
              if (dist[v] < 0) {
                dist[v] = nextDist;
                if (v == target)
                  return nextDist;
                if (nextSize >= nextFrontier.length)
                  nextFrontier = Arrays.copyOf(nextFrontier, Math.min(n, nextFrontier.length * 2));
                nextFrontier[nextSize++] = v;
              }
            }
          }
          if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
            final int[] bwdOffsets = csr.getBackwardOffsets();
            final int[] bwdNeighbors = csr.getBackwardNeighbors();
            for (int j = bwdOffsets[u]; j < bwdOffsets[u + 1]; j++) {
              final int v = bwdNeighbors[j];
              if (dist[v] < 0) {
                dist[v] = nextDist;
                if (v == target)
                  return nextDist;
                if (nextSize >= nextFrontier.length)
                  nextFrontier = Arrays.copyOf(nextFrontier, Math.min(n, nextFrontier.length * 2));
                nextFrontier[nextSize++] = v;
              }
            }
          }
        }
      }

      frontier = nextFrontier;
      frontierSize = nextSize;
    }
    return -1;
  }

  /**
   * Returns the full distance array from a source node to all reachable nodes.
   * Unreachable nodes have distance -1.
   */
  public static int[] shortestPathAll(final GraphAnalyticalView view, final int source,
      final Vertex.DIRECTION direction, final String... edgeTypes) {
    final int n = view.getNodeMapping().size();
    final int[] dist = new int[n];
    Arrays.fill(dist, -1);

    if (source < 0 || source >= n)
      return dist;

    dist[source] = 0;
    final String[] types = resolveEdgeTypes(view, edgeTypes);

    int[] frontier = new int[Math.min(n, 1024)];
    frontier[0] = source;
    int frontierSize = 1;

    while (frontierSize > 0) {
      int nextSize = 0;
      int[] nextFrontier = new int[Math.min(n, frontierSize * 4)];

      for (int f = 0; f < frontierSize; f++) {
        final int u = frontier[f];
        final int nextDist = dist[u] + 1;

        for (final String edgeType : types) {
          final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
          if (csr == null)
            continue;

          if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
            final int[] fwdOffsets = csr.getForwardOffsets();
            final int[] fwdNeighbors = csr.getForwardNeighbors();
            for (int j = fwdOffsets[u]; j < fwdOffsets[u + 1]; j++) {
              final int v = fwdNeighbors[j];
              if (dist[v] < 0) {
                dist[v] = nextDist;
                if (nextSize >= nextFrontier.length)
                  nextFrontier = Arrays.copyOf(nextFrontier, Math.min(n, nextFrontier.length * 2));
                nextFrontier[nextSize++] = v;
              }
            }
          }
          if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
            final int[] bwdOffsets = csr.getBackwardOffsets();
            final int[] bwdNeighbors = csr.getBackwardNeighbors();
            for (int j = bwdOffsets[u]; j < bwdOffsets[u + 1]; j++) {
              final int v = bwdNeighbors[j];
              if (dist[v] < 0) {
                dist[v] = nextDist;
                if (nextSize >= nextFrontier.length)
                  nextFrontier = Arrays.copyOf(nextFrontier, Math.min(n, nextFrontier.length * 2));
                nextFrontier[nextSize++] = v;
              }
            }
          }
        }
      }

      frontier = nextFrontier;
      frontierSize = nextSize;
    }
    return dist;
  }

  // --- Label Propagation (Community Detection) ---

  /**
   * Detects communities using the label propagation algorithm.
   * Each node starts with its own label; in each iteration, nodes adopt the most frequent
   * label among their neighbors. Converges when labels stabilize.
   *
   * @param view       the analytical view
   * @param maxIters   maximum number of iterations
   * @param edgeTypes  edge types to consider (null or empty = all)
   * @return int[] of community labels indexed by dense node ID
   */
  public static int[] labelPropagation(final GraphAnalyticalView view, final int maxIters,
      final String... edgeTypes) {
    final int n = view.getNodeMapping().size();
    if (n == 0)
      return new int[0];

    final int[] labels = new int[n];
    for (int i = 0; i < n; i++)
      labels[i] = i;

    final String[] types = resolveEdgeTypes(view, edgeTypes);
    final Random rng = new Random(42);

    // Pre-compute max degree to pre-allocate a reusable neighbor labels buffer
    int maxDegree = 0;
    for (int u = 0; u < n; u++) {
      int deg = 0;
      for (final String edgeType : types) {
        final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
        if (csr != null)
          deg += csr.outDegree(u) + csr.inDegree(u);
      }
      if (deg > maxDegree)
        maxDegree = deg;
    }

    final int[] neighborLabels = new int[maxDegree];

    // Shuffle order for each iteration to avoid bias
    final int[] order = new int[n];
    for (int i = 0; i < n; i++)
      order[i] = i;

    for (int iter = 0; iter < maxIters; iter++) {
      // Fisher-Yates shuffle
      for (int i = n - 1; i > 0; i--) {
        final int j = rng.nextInt(i + 1);
        final int tmp = order[i];
        order[i] = order[j];
        order[j] = tmp;
      }

      boolean changed = false;

      for (int idx = 0; idx < n; idx++) {
        final int u = order[idx];

        // Count neighbor labels using a simple frequency approach
        int bestLabel = labels[u];
        int bestCount = 0;

        // Collect all neighbor labels and find the most frequent
        int totalNeighbors = 0;
        for (final String edgeType : types) {
          final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
          if (csr == null)
            continue;
          totalNeighbors += csr.outDegree(u) + csr.inDegree(u);
        }

        if (totalNeighbors == 0)
          continue;

        // Fill reusable buffer with neighbor labels
        int pos = 0;
        for (final String edgeType : types) {
          final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
          if (csr == null)
            continue;
          final int[] fwdOffsets = csr.getForwardOffsets();
          final int[] fwdNeighbors = csr.getForwardNeighbors();
          for (int j = fwdOffsets[u]; j < fwdOffsets[u + 1]; j++)
            neighborLabels[pos++] = labels[fwdNeighbors[j]];
          final int[] bwdOffsets = csr.getBackwardOffsets();
          final int[] bwdNeighbors = csr.getBackwardNeighbors();
          for (int j = bwdOffsets[u]; j < bwdOffsets[u + 1]; j++)
            neighborLabels[pos++] = labels[bwdNeighbors[j]];
        }

        // Sort and count runs to find mode (O(d log d) but avoids HashMap allocation)
        Arrays.sort(neighborLabels, 0, pos);
        int currentLabel = neighborLabels[0];
        int currentCount = 1;
        bestLabel = currentLabel;
        bestCount = 1;
        for (int i = 1; i < pos; i++) {
          if (neighborLabels[i] == currentLabel) {
            currentCount++;
          } else {
            if (currentCount > bestCount) {
              bestCount = currentCount;
              bestLabel = currentLabel;
            }
            currentLabel = neighborLabels[i];
            currentCount = 1;
          }
        }
        if (currentCount > bestCount)
          bestLabel = currentLabel;

        if (labels[u] != bestLabel) {
          labels[u] = bestLabel;
          changed = true;
        }
      }

      if (!changed)
        break;
    }
    return labels;
  }

  /**
   * Label propagation with default max iterations of 100.
   */
  public static int[] labelPropagation(final GraphAnalyticalView view, final String... edgeTypes) {
    return labelPropagation(view, 100, edgeTypes);
  }

  // --- Helpers ---

  private static String[] resolveEdgeTypes(final GraphAnalyticalView view, final String... edgeTypes) {
    if (edgeTypes != null && edgeTypes.length > 0)
      return edgeTypes;
    return view.getEdgeTypes().toArray(new String[0]);
  }

  private static int find(final int[] parent, int x) {
    while (parent[x] != x) {
      parent[x] = parent[parent[x]]; // path halving
      x = parent[x];
    }
    return x;
  }

  private static void union(final int[] parent, final int[] rank, final int a, final int b) {
    int ra = find(parent, a);
    int rb = find(parent, b);
    if (ra == rb)
      return;
    if (rank[ra] < rank[rb]) {
      final int tmp = ra;
      ra = rb;
      rb = tmp;
    }
    parent[rb] = ra;
    if (rank[ra] == rank[rb])
      rank[ra]++;
  }
}
