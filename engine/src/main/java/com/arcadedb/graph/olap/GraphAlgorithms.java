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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * Graph algorithms operating directly on the CSR arrays of a {@link GraphAnalyticalView}.
 * All algorithms run in-memory on the packed int[] arrays with zero GC pressure.
 * <p>
 * Most algorithms are parallelized using plain Thread[] with range partitioning
 * (no ForkJoinPool overhead). Parallelism kicks in only above configurable thresholds
 * to avoid overhead on small graphs.
 * <p>
 * Algorithms:
 * <ul>
 *   <li>{@link #pageRank} — pull-based parallel PageRank with configurable damping and iterations</li>
 *   <li>{@link #connectedComponents} — parallel min-label propagation for weakly connected components</li>
 *   <li>{@link #shortestPath} — BFS-based unweighted shortest path (returns hop count)</li>
 *   <li>{@link #shortestPathAll} — parallel BFS for single-source shortest paths to all nodes</li>
 *   <li>{@link #labelPropagation} — synchronous parallel community detection via label propagation</li>
 *   <li>{@link #localClusteringCoefficient} — parallel triangle counting for LCC</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GraphAlgorithms {

  private static final int PARALLELISM           = Runtime.getRuntime().availableProcessors();
  private static final int PARALLEL_THRESHOLD     = 8192;
  private static final int PARALLEL_BFS_THRESHOLD = 4096;
  private static final int PULL_DIRECTION_DIVISOR = 20; // switch to pull when frontier > n/20

  private GraphAlgorithms() {
  }

  // --- Parallel Infrastructure ---

  /**
   * Partitions range [0, n) into chunks and runs each on a dedicated thread.
   * Falls back to single-threaded execution when n is below threshold.
   * Propagates any exception thrown by a worker to the calling thread.
   */
  static void parallelForRange(final int n, final BiConsumer<Integer, Integer> work) {
    if (n < PARALLEL_THRESHOLD) {
      work.accept(0, n);
      return;
    }
    final int chunkSize = (n + PARALLELISM - 1) / PARALLELISM;
    final Thread[] threads = new Thread[PARALLELISM];
    final AtomicReference<Throwable> firstError = new AtomicReference<>();
    int launched = 0;
    for (int t = 0; t < PARALLELISM; t++) {
      final int start = t * chunkSize;
      final int end = Math.min(start + chunkSize, n);
      if (start >= n)
        break;
      final Thread thread = new Thread(() -> {
        try {
          work.accept(start, end);
        } catch (final Throwable e) {
          firstError.compareAndSet(null, e);
        }
      });
      thread.setDaemon(true);
      thread.setName("gav-algo-" + t);
      threads[t] = thread;
      thread.start();
      launched++;
    }
    joinThreads(threads, launched);
    rethrowIfFailed(firstError);
  }

  /**
   * Creates a named daemon thread for algorithm parallelism.
   */
  static Thread newAlgoThread(final int index, final AtomicReference<Throwable> firstError, final Runnable task) {
    final Thread thread = new Thread(() -> {
      try {
        task.run();
      } catch (final Throwable e) {
        firstError.compareAndSet(null, e);
      }
    });
    thread.setDaemon(true);
    thread.setName("gav-algo-" + index);
    return thread;
  }

  private static void joinThreads(final Thread[] threads, final int count) {
    for (int i = 0; i < count; i++)
      if (threads[i] != null)
        try {
          threads[i].join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
  }

  private static void rethrowIfFailed(final AtomicReference<Throwable> firstError) {
    final Throwable t = firstError.get();
    if (t != null) {
      if (t instanceof RuntimeException)
        throw (RuntimeException) t;
      if (t instanceof Error)
        throw (Error) t;
      throw new RuntimeException(t);
    }
  }

  // --- PageRank (Pull-based, Parallel) ---

  /**
   * Computes PageRank over the given view for the specified edge types.
   * Uses pull-based iteration: each node reads contributions FROM its in-neighbors
   * via backward CSR. Each thread writes to a disjoint range of the next[] array,
   * requiring zero synchronization.
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

    // Precompute outDegree array across all edge types (once, outside iteration loop)
    final int[] outDeg = new int[n];
    for (final String edgeType : types) {
      final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
      if (csr == null)
        continue;
      final int[] fwdOffsets = csr.getForwardOffsets();
      for (int u = 0; u < n; u++)
        outDeg[u] += fwdOffsets[u + 1] - fwdOffsets[u];
    }

    for (int iter = 0; iter < iterations; iter++) {
      final double base = (1.0 - damping) / n;
      final double[] currentRank = rank;
      final double[] nextRank = next;

      // PULL: each node sums contributions from backward neighbors — parallel, zero sync
      parallelForRange(n, (start, end) -> {
        for (int u = start; u < end; u++) {
          double sum = 0;
          for (final String edgeType : types) {
            final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
            if (csr == null)
              continue;
            final int[] bwdOffsets = csr.getBackwardOffsets();
            final int[] bwdNeighbors = csr.getBackwardNeighbors();
            for (int j = bwdOffsets[u]; j < bwdOffsets[u + 1]; j++) {
              final int v = bwdNeighbors[j];
              if (outDeg[v] > 0)
                sum += currentRank[v] / outDeg[v];
            }
          }
          nextRank[u] = base + damping * sum;
        }
      });

      // Handle dangling nodes: distribute their rank evenly (sequential, O(n))
      double danglingSum = 0.0;
      for (int u = 0; u < n; u++)
        if (outDeg[u] == 0)
          danglingSum += currentRank[u];
      if (danglingSum > 0.0) {
        final double danglingContrib = damping * danglingSum / n;
        for (int u = 0; u < n; u++)
          nextRank[u] += danglingContrib;
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

  // --- Connected Components (Parallel Min-Label Propagation) ---

  /**
   * Computes weakly connected components using synchronous min-label propagation.
   * Each node starts with its own ID as label; in each iteration, each node takes the
   * minimum label among itself and all its neighbors (both directions).
   * Converges in O(diameter) iterations. Each iteration is fully parallelizable since
   * threads write to disjoint ranges of newLabel[].
   *
   * @param view      the analytical view (must be built)
   * @param edgeTypes edge types to consider (null or empty = all)
   * @return int[] of component IDs indexed by dense node ID (component ID = min node ID in component)
   */
  public static int[] connectedComponents(final GraphAnalyticalView view, final String... edgeTypes) {
    final int n = view.getNodeMapping().size();
    if (n == 0)
      return new int[0];

    final int[] label = new int[n];
    final int[] newLabel = new int[n];
    for (int i = 0; i < n; i++)
      label[i] = i;

    final String[] types = resolveEdgeTypes(view, edgeTypes);

    boolean changed = true;
    while (changed) {
      System.arraycopy(label, 0, newLabel, 0, n);

      final AtomicBoolean anyChanged = new AtomicBoolean(false);
      parallelForRange(n, (start, end) -> {
        boolean localChanged = false;
        for (int u = start; u < end; u++) {
          int minLabel = label[u];
          for (final String edgeType : types) {
            final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
            if (csr == null)
              continue;
            final int[] fwdOffsets = csr.getForwardOffsets();
            final int[] fwdNeighbors = csr.getForwardNeighbors();
            for (int j = fwdOffsets[u]; j < fwdOffsets[u + 1]; j++) {
              final int nl = label[fwdNeighbors[j]];
              if (nl < minLabel)
                minLabel = nl;
            }
            final int[] bwdOffsets = csr.getBackwardOffsets();
            final int[] bwdNeighbors = csr.getBackwardNeighbors();
            for (int j = bwdOffsets[u]; j < bwdOffsets[u + 1]; j++) {
              final int nl = label[bwdNeighbors[j]];
              if (nl < minLabel)
                minLabel = nl;
            }
          }
          newLabel[u] = minLabel;
          if (minLabel != label[u])
            localChanged = true;
        }
        if (localChanged)
          anyChanged.set(true);
      });

      System.arraycopy(newLabel, 0, label, 0, n);
      changed = anyChanged.get();
    }

    return label;
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
   * Single-threaded with early termination on target found.
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

    // Pre-hoist CSR arrays and direction flags outside the BFS loop
    final boolean useFwd = direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH;
    final boolean useBwd = direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH;
    final int typeCount = types.length;
    final int[][] allFwdOffsets = new int[typeCount][];
    final int[][] allFwdNeighbors = new int[typeCount][];
    final int[][] allBwdOffsets = new int[typeCount][];
    final int[][] allBwdNeighbors = new int[typeCount][];
    for (int t = 0; t < typeCount; t++) {
      final CSRAdjacencyIndex csr = view.getCSRIndex(types[t]);
      if (csr == null)
        continue;
      if (useFwd) {
        allFwdOffsets[t] = csr.getForwardOffsets();
        allFwdNeighbors[t] = csr.getForwardNeighbors();
      }
      if (useBwd) {
        allBwdOffsets[t] = csr.getBackwardOffsets();
        allBwdNeighbors[t] = csr.getBackwardNeighbors();
      }
    }

    // Bitmap visited set: n/64 longs (~80KB for 633K nodes) fits in L2 cache
    final long[] visited = new long[(n + 63) >>> 6];
    visited[source >>> 6] |= 1L << (source & 63);

    // Pre-allocate frontiers at full capacity, swap references between levels
    int[] frontier = new int[n];
    int[] nextFrontier = new int[n];
    frontier[0] = source;
    int frontierSize = 1;
    int depth = 0;

    while (frontierSize > 0) {
      depth++;
      int nextSize = 0;

      for (int f = 0; f < frontierSize; f++) {
        final int u = frontier[f];

        for (int t = 0; t < typeCount; t++) {
          if (useFwd && allFwdOffsets[t] != null) {
            final int[] offsets = allFwdOffsets[t];
            final int[] neighbors = allFwdNeighbors[t];
            for (int j = offsets[u], end = offsets[u + 1]; j < end; j++) {
              final int v = neighbors[j];
              final int word = v >>> 6;
              final long bit = 1L << (v & 63);
              if ((visited[word] & bit) == 0) {
                visited[word] |= bit;
                if (v == target)
                  return depth;
                nextFrontier[nextSize++] = v;
              }
            }
          }
          if (useBwd && allBwdOffsets[t] != null) {
            final int[] offsets = allBwdOffsets[t];
            final int[] neighbors = allBwdNeighbors[t];
            for (int j = offsets[u], end = offsets[u + 1]; j < end; j++) {
              final int v = neighbors[j];
              final int word = v >>> 6;
              final long bit = 1L << (v & 63);
              if ((visited[word] & bit) == 0) {
                visited[word] |= bit;
                if (v == target)
                  return depth;
                nextFrontier[nextSize++] = v;
              }
            }
          }
        }
      }

      // Swap frontier references (zero-cost)
      final int[] tmp = frontier;
      frontier = nextFrontier;
      nextFrontier = tmp;
      frontierSize = nextSize;
    }
    return -1;
  }

  /**
   * Returns the full distance array from a source node to all reachable nodes.
   * Unreachable nodes have distance -1.
   * <p>
   * Parallelizes frontier expansion when frontier exceeds threshold using
   * AtomicLongArray bitmap CAS for thread-safe discovery and thread-local next-frontier buffers.
   */
  public static int[] shortestPathAll(final GraphAnalyticalView view, final int source,
      final Vertex.DIRECTION direction, final String... edgeTypes) {
    final int n = view.getNodeMapping().size();

    if (source < 0 || source >= n) {
      final int[] result = new int[n];
      Arrays.fill(result, -1);
      return result;
    }

    final String[] types = resolveEdgeTypes(view, edgeTypes);

    // Pre-hoist CSR arrays and direction flags outside the BFS loop.
    // Both forward and backward arrays are always loaded because pull mode
    // needs the reverse-direction arrays for early-break scanning.
    final boolean useFwd = direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH;
    final boolean useBwd = direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH;
    final int typeCount = types.length;
    final int[][] allFwdOffsets = new int[typeCount][];
    final int[][] allFwdNeighbors = new int[typeCount][];
    final int[][] allBwdOffsets = new int[typeCount][];
    final int[][] allBwdNeighbors = new int[typeCount][];
    for (int t = 0; t < typeCount; t++) {
      final CSRAdjacencyIndex csr = view.getCSRIndex(types[t]);
      if (csr == null)
        continue;
      allFwdOffsets[t] = csr.getForwardOffsets();
      allFwdNeighbors[t] = csr.getForwardNeighbors();
      allBwdOffsets[t] = csr.getBackwardOffsets();
      allBwdNeighbors[t] = csr.getBackwardNeighbors();
    }

    // dist[] for output, bitmap for fast visited check in hot loop
    final int[] dist = new int[n];
    Arrays.fill(dist, -1);
    dist[source] = 0;

    // Bitmap visited set: n/64 longs (~80KB for 633K nodes) fits in L2 cache
    final long[] visited = new long[(n + 63) >>> 6];
    visited[source >>> 6] |= 1L << (source & 63);

    // Pre-allocate frontiers at full capacity, swap references between levels
    int[] frontier = new int[n];
    int[] nextFrontier = new int[n];
    frontier[0] = source;
    int frontierSize = 1;
    int depth = 0;

    // Frontier bitmap for pull mode: tracks which nodes are in the CURRENT frontier
    // (separate from visited bitmap which tracks all ever-seen nodes)
    final long[] frontierBitmap = new long[(n + 63) >>> 6];
    frontierBitmap[source >>> 6] |= 1L << (source & 63);

    // Pre-hoist pull-mode CSR arrays: pull checks reverse direction neighbors
    // OUT direction pull: check backward neighbors (who points to me?)
    // IN direction pull: check forward neighbors (who do I point to?)
    final int[][] pullOffsets1 = new int[typeCount][];
    final int[][] pullNeighbors1 = new int[typeCount][];
    final int[][] pullOffsets2 = new int[typeCount][]; // for BOTH direction, second set
    final int[][] pullNeighbors2 = new int[typeCount][];
    for (int t = 0; t < typeCount; t++) {
      if (useFwd && allBwdOffsets[t] != null) {
        pullOffsets1[t] = allBwdOffsets[t];
        pullNeighbors1[t] = allBwdNeighbors[t];
      }
      if (useBwd && allFwdOffsets[t] != null) {
        if (pullOffsets1[t] == null) {
          pullOffsets1[t] = allFwdOffsets[t];
          pullNeighbors1[t] = allFwdNeighbors[t];
        } else {
          pullOffsets2[t] = allFwdOffsets[t];
          pullNeighbors2[t] = allFwdNeighbors[t];
        }
      }
    }

    // Pull mode only beneficial for large enough graphs and frontiers
    final int pullThreshold = n >= PULL_DIRECTION_DIVISOR * 2 ? n / PULL_DIRECTION_DIVISOR : Integer.MAX_VALUE;

    while (frontierSize > 0) {
      depth++;

      if (frontierSize > pullThreshold) {
        // PULL mode: scan ALL unvisited nodes, check if any reverse-neighbor is in frontier.
        // Breaks early after finding one parent — skips rest of adjacency list.
        // Advantage: for large frontiers, most neighbors are in the frontier, so early break
        // avoids scanning thousands of already-visited neighbors per high-degree node.
        int nextSize = 0;
        for (int v = 0; v < n; v++) {
          final int vWord = v >>> 6;
          final long vBit = 1L << (v & 63);
          if ((visited[vWord] & vBit) != 0)
            continue; // already visited

          boolean found = false;
          for (int t = 0; t < typeCount && !found; t++) {
            if (pullOffsets1[t] != null) {
              final int[] offsets = pullOffsets1[t];
              final int[] neighbors = pullNeighbors1[t];
              for (int j = offsets[v], end = offsets[v + 1]; j < end; j++) {
                final int u = neighbors[j];
                if ((frontierBitmap[u >>> 6] & (1L << (u & 63))) != 0) {
                  found = true;
                  break;
                }
              }
            }
            if (!found && pullOffsets2[t] != null) {
              final int[] offsets = pullOffsets2[t];
              final int[] neighbors = pullNeighbors2[t];
              for (int j = offsets[v], end = offsets[v + 1]; j < end; j++) {
                final int u = neighbors[j];
                if ((frontierBitmap[u >>> 6] & (1L << (u & 63))) != 0) {
                  found = true;
                  break;
                }
              }
            }
          }
          if (found) {
            visited[vWord] |= vBit;
            dist[v] = depth;
            nextFrontier[nextSize++] = v;
          }
        }

        // Update frontier bitmap: clear old frontier, set new frontier
        Arrays.fill(frontierBitmap, 0L);
        for (int i = 0; i < nextSize; i++) {
          final int v = nextFrontier[i];
          frontierBitmap[v >>> 6] |= 1L << (v & 63);
        }

        // Swap frontier references
        final int[] tmp = frontier;
        frontier = nextFrontier;
        nextFrontier = tmp;
        frontierSize = nextSize;

      } else if (frontierSize > PARALLEL_BFS_THRESHOLD) {
        // PUSH mode (parallel): expand frontier outward using threads
        final AtomicLongArray visitedAtomic = new AtomicLongArray(visited.length);
        for (int i = 0; i < visited.length; i++)
          visitedAtomic.set(i, visited[i]);

        final int fSize = frontierSize;
        final int[] currentFrontier = frontier;
        final int currentDepth = depth;
        final int numThreads = Math.min(PARALLELISM, (fSize + PARALLEL_BFS_THRESHOLD - 1) / PARALLEL_BFS_THRESHOLD);
        final int chunkSize = (fSize + numThreads - 1) / numThreads;
        final int[][] localNexts = new int[numThreads][];
        final int[] localSizes = new int[numThreads];

        final Thread[] threads = new Thread[numThreads];
        final AtomicReference<Throwable> bfsError = new AtomicReference<>();
        int launched = 0;
        for (int t = 0; t < numThreads; t++) {
          final int tIdx = t;
          final int tStart = t * chunkSize;
          final int tEnd = Math.min(tStart + chunkSize, fSize);
          if (tStart >= fSize)
            break;
          threads[t] = newAlgoThread(t, bfsError, () -> {
            int[] localNext = new int[Math.min(n, (tEnd - tStart) * 8)];
            int localSize = 0;
            for (int f = tStart; f < tEnd; f++) {
              final int u = currentFrontier[f];
              for (int ti = 0; ti < typeCount; ti++) {
                if (useFwd && allFwdOffsets[ti] != null) {
                  final int[] offsets = allFwdOffsets[ti];
                  final int[] neighbors = allFwdNeighbors[ti];
                  for (int j = offsets[u], end = offsets[u + 1]; j < end; j++) {
                    final int v = neighbors[j];
                    final int word = v >>> 6;
                    final long bit = 1L << (v & 63);
                    long oldVal;
                    do {
                      oldVal = visitedAtomic.get(word);
                      if ((oldVal & bit) != 0)
                        break;
                    } while (!visitedAtomic.compareAndSet(word, oldVal, oldVal | bit));
                    if ((oldVal & bit) == 0) {
                      dist[v] = currentDepth;
                      if (localSize >= localNext.length)
                        localNext = Arrays.copyOf(localNext, Math.min(n, localNext.length * 2));
                      localNext[localSize++] = v;
                    }
                  }
                }
                if (useBwd && allBwdOffsets[ti] != null) {
                  final int[] offsets = allBwdOffsets[ti];
                  final int[] neighbors = allBwdNeighbors[ti];
                  for (int j = offsets[u], end = offsets[u + 1]; j < end; j++) {
                    final int v = neighbors[j];
                    final int word = v >>> 6;
                    final long bit = 1L << (v & 63);
                    long oldVal;
                    do {
                      oldVal = visitedAtomic.get(word);
                      if ((oldVal & bit) != 0)
                        break;
                    } while (!visitedAtomic.compareAndSet(word, oldVal, oldVal | bit));
                    if ((oldVal & bit) == 0) {
                      dist[v] = currentDepth;
                      if (localSize >= localNext.length)
                        localNext = Arrays.copyOf(localNext, Math.min(n, localNext.length * 2));
                      localNext[localSize++] = v;
                    }
                  }
                }
              }
            }
            localNexts[tIdx] = localNext;
            localSizes[tIdx] = localSize;
          });
          threads[t].start();
          launched++;
        }
        joinThreads(threads, launched);
        rethrowIfFailed(bfsError);

        // Copy atomic bitmap back to plain bitmap for next iteration
        for (int i = 0; i < visited.length; i++)
          visited[i] = visitedAtomic.get(i);

        // Merge thread-local frontiers into pre-allocated nextFrontier
        int totalNext = 0;
        for (int t = 0; t < launched; t++)
          if (localNexts[t] != null)
            totalNext += localSizes[t];
        int pos = 0;
        for (int t = 0; t < launched; t++)
          if (localNexts[t] != null && localSizes[t] > 0) {
            System.arraycopy(localNexts[t], 0, nextFrontier, pos, localSizes[t]);
            pos += localSizes[t];
          }

        // Update frontier bitmap for potential pull mode next iteration
        Arrays.fill(frontierBitmap, 0L);
        for (int i = 0; i < totalNext; i++) {
          final int v = nextFrontier[i];
          frontierBitmap[v >>> 6] |= 1L << (v & 63);
        }

        // Swap frontier references
        final int[] tmp = frontier;
        frontier = nextFrontier;
        nextFrontier = tmp;
        frontierSize = totalNext;

      } else {
        // PUSH mode (sequential): expand frontier outward — uses plain long[] bitmap
        int nextSize = 0;
        for (int f = 0; f < frontierSize; f++) {
          final int u = frontier[f];
          for (int t = 0; t < typeCount; t++) {
            if (useFwd && allFwdOffsets[t] != null) {
              final int[] offsets = allFwdOffsets[t];
              final int[] neighbors = allFwdNeighbors[t];
              for (int j = offsets[u], end = offsets[u + 1]; j < end; j++) {
                final int v = neighbors[j];
                final int word = v >>> 6;
                final long bit = 1L << (v & 63);
                if ((visited[word] & bit) == 0) {
                  visited[word] |= bit;
                  dist[v] = depth;
                  nextFrontier[nextSize++] = v;
                }
              }
            }
            if (useBwd && allBwdOffsets[t] != null) {
              final int[] offsets = allBwdOffsets[t];
              final int[] neighbors = allBwdNeighbors[t];
              for (int j = offsets[u], end = offsets[u + 1]; j < end; j++) {
                final int v = neighbors[j];
                final int word = v >>> 6;
                final long bit = 1L << (v & 63);
                if ((visited[word] & bit) == 0) {
                  visited[word] |= bit;
                  dist[v] = depth;
                  nextFrontier[nextSize++] = v;
                }
              }
            }
          }
        }

        // Update frontier bitmap for potential pull mode next iteration
        Arrays.fill(frontierBitmap, 0L);
        for (int i = 0; i < nextSize; i++) {
          final int v = nextFrontier[i];
          frontierBitmap[v >>> 6] |= 1L << (v & 63);
        }

        // Swap frontier references
        final int[] tmp = frontier;
        frontier = nextFrontier;
        nextFrontier = tmp;
        frontierSize = nextSize;
      }
    }

    return dist;
  }

  // --- Dijkstra Single-Source Shortest Path (weighted) ---

  /**
   * Computes single-source shortest paths using Dijkstra's algorithm directly on CSR arrays
   * with edge weights from columnar storage. Zero OLTP access.
   *
   * @param view           the analytical view (must be built with edge properties)
   * @param source         source dense node ID
   * @param weightProperty edge property name for weights (must be numeric)
   * @param direction      traversal direction (OUT, IN, or BOTH)
   * @param edgeTypes      edge types to traverse (null or empty = all)
   * @return double[] of distances indexed by dense node ID (POSITIVE_INFINITY = unreachable)
   */
  public static double[] dijkstraSingleSource(final GraphAnalyticalView view, final int source,
      final String weightProperty, final Vertex.DIRECTION direction, final String... edgeTypes) {
    final int n = view.getNodeMapping().size();
    final double[] dist = new double[n];
    Arrays.fill(dist, Double.POSITIVE_INFINITY);

    if (source < 0 || source >= n)
      return dist;

    dist[source] = 0.0;
    final String[] types = resolveEdgeTypes(view, edgeTypes);

    // Pre-load CSR arrays and weight columns for each edge type (avoid map lookups in hot loop)
    final int typeCount = types.length;
    final CSRAdjacencyIndex[] csrs = new CSRAdjacencyIndex[typeCount];
    final double[][] weightDoubleArrays = new double[typeCount][];
    final int[][] weightIntArrays = new int[typeCount][];
    final long[][] weightLongArrays = new long[typeCount][];
    final long[][] weightNullBitsets = new long[typeCount][];
    final int[][] bwdToFwds = new int[typeCount][];

    for (int t = 0; t < typeCount; t++) {
      csrs[t] = view.getCSRIndex(types[t]);
      if (csrs[t] == null)
        continue;
      final ColumnStore edgeStore = view.getEdgeColumnStore(types[t]);
      if (edgeStore != null) {
        final Column wCol = edgeStore.getColumn(weightProperty);
        if (wCol != null) {
          weightNullBitsets[t] = wCol.getNullBitset();
          switch (wCol.getType()) {
          case DOUBLE:
            weightDoubleArrays[t] = wCol.getDoubleData();
            break;
          case INT:
            weightIntArrays[t] = wCol.getIntData();
            break;
          case LONG:
            weightLongArrays[t] = wCol.getLongData();
            break;
          default:
            break;
          }
        }
      }
      if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH)
        bwdToFwds[t] = view.getBwdToFwdMapping(types[t]);
    }

    // Dijkstra with binary min-heap (PriorityQueue)
    final java.util.PriorityQueue<double[]> heap = new java.util.PriorityQueue<>((a, b) -> Double.compare(a[0], b[0]));
    heap.offer(new double[]{ 0.0, source });

    while (!heap.isEmpty()) {
      final double[] entry = heap.poll();
      final double d = entry[0];
      final int u = (int) entry[1];
      if (d > dist[u])
        continue;

      for (int t = 0; t < typeCount; t++) {
        final CSRAdjacencyIndex csr = csrs[t];
        if (csr == null)
          continue;

        if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
          final int[] fwdOffsets = csr.getForwardOffsets();
          final int[] fwdNeighbors = csr.getForwardNeighbors();
          final int start = fwdOffsets[u];
          final int end = fwdOffsets[u + 1];
          for (int j = start; j < end; j++) {
            final double w = getWeight(j, weightDoubleArrays[t], weightIntArrays[t],
                weightLongArrays[t], weightNullBitsets[t]);
            if (w < 0)
              continue;
            final double newDist = d + w;
            final int v = fwdNeighbors[j];
            if (newDist < dist[v]) {
              dist[v] = newDist;
              heap.offer(new double[]{ newDist, v });
            }
          }
        }

        if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
          final int[] bwdOffsets = csr.getBackwardOffsets();
          final int[] bwdNeighbors = csr.getBackwardNeighbors();
          final int start = bwdOffsets[u];
          final int end = bwdOffsets[u + 1];
          for (int j = start; j < end; j++) {
            final int fwdIdx = bwdToFwds[t] != null ? bwdToFwds[t][j] : j;
            final double w = getWeight(fwdIdx, weightDoubleArrays[t], weightIntArrays[t],
                weightLongArrays[t], weightNullBitsets[t]);
            if (w < 0)
              continue;
            final double newDist = d + w;
            final int v = bwdNeighbors[j];
            if (newDist < dist[v]) {
              dist[v] = newDist;
              heap.offer(new double[]{ newDist, v });
            }
          }
        }
      }
    }

    return dist;
  }

  /** Extracts edge weight from the appropriate typed array. Returns 1.0 if no weight column. */
  private static double getWeight(final int fwdIdx, final double[] doubleData, final int[] intData,
      final long[] longData, final long[] nullBitset) {
    if (nullBitset != null && (nullBitset[fwdIdx >>> 6] & (1L << (fwdIdx & 63))) != 0)
      return 1.0;
    if (doubleData != null)
      return doubleData[fwdIdx];
    if (intData != null)
      return intData[fwdIdx];
    if (longData != null)
      return longData[fwdIdx];
    return 1.0;
  }

  // --- Label Propagation (Synchronous, Parallel) ---

  /**
   * Detects communities using synchronous label propagation.
   * Each node starts with its own label; in each iteration, all nodes simultaneously
   * adopt the most frequent label among their neighbors (reading from previous iteration,
   * writing to new array). Fully parallelizable since each thread writes to a disjoint
   * range of newLabels[].
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
    final int[] newLabels = new int[n];
    for (int i = 0; i < n; i++)
      labels[i] = i;

    final String[] types = resolveEdgeTypes(view, edgeTypes);

    // Pre-compute max degree to size thread-local buffers
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

    final int maxDeg = maxDegree;

    for (int iter = 0; iter < maxIters; iter++) {
      System.arraycopy(labels, 0, newLabels, 0, n);

      final AtomicBoolean anyChanged = new AtomicBoolean(false);

      parallelForRange(n, (start, end) -> {
        final int[] neighborBuf = new int[maxDeg];
        boolean localChanged = false;

        for (int u = start; u < end; u++) {
          // Collect neighbor labels into thread-local buffer
          int pos = 0;
          for (final String edgeType : types) {
            final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
            if (csr == null)
              continue;
            final int[] fwdOffsets = csr.getForwardOffsets();
            final int[] fwdNeighbors = csr.getForwardNeighbors();
            for (int j = fwdOffsets[u]; j < fwdOffsets[u + 1]; j++)
              neighborBuf[pos++] = labels[fwdNeighbors[j]];
            final int[] bwdOffsets = csr.getBackwardOffsets();
            final int[] bwdNeighbors = csr.getBackwardNeighbors();
            for (int j = bwdOffsets[u]; j < bwdOffsets[u + 1]; j++)
              neighborBuf[pos++] = labels[bwdNeighbors[j]];
          }

          if (pos == 0)
            continue;

          // Sort and find mode — smallest label wins ties
          Arrays.sort(neighborBuf, 0, pos);
          int bestLabel = neighborBuf[0];
          int bestCount = 1;
          int currentLabel = neighborBuf[0];
          int currentCount = 1;
          for (int i = 1; i < pos; i++) {
            if (neighborBuf[i] == currentLabel) {
              currentCount++;
            } else {
              if (currentCount > bestCount) {
                bestCount = currentCount;
                bestLabel = currentLabel;
              }
              currentLabel = neighborBuf[i];
              currentCount = 1;
            }
          }
          if (currentCount > bestCount)
            bestLabel = currentLabel;

          newLabels[u] = bestLabel;
          if (labels[u] != bestLabel)
            localChanged = true;
        }

        if (localChanged)
          anyChanged.set(true);
      });

      System.arraycopy(newLabels, 0, labels, 0, n);
      if (!anyChanged.get())
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

  // --- Local Clustering Coefficient ---

  /**
   * Computes the local clustering coefficient for every node in the graph.
   * LCC(u) = 2 * triangles(u) / (deg(u) * (deg(u) - 1)), where deg is the undirected degree.
   * Nodes with degree &lt; 2 receive a coefficient of 0.
   * <p>
   * Uses sorted-merge intersection on CSR arrays for efficient triangle counting.
   * O(m * sqrt(m)) time, O(m) memory, zero object allocation in the hot loop.
   * Parallelized using plain Thread[] with range partitioning.
   *
   * @param view      the analytical view (must be built)
   * @param edgeTypes edge types to consider (null or empty = all)
   * @return double[] of LCC values indexed by dense node ID
   */
  public static double[] localClusteringCoefficient(final GraphAnalyticalView view, final String... edgeTypes) {
    final int n = view.getNodeMapping().size();
    if (n == 0)
      return new double[0];

    final String[] types = resolveEdgeTypes(view, edgeTypes);

    return lccBuildAndIntersect(view, types, n);
  }

  /**
   * Builds a flat merged undirected adjacency from CSR arrays, then counts triangles via
   * sorted-merge intersection. For single edge type, uses merge (O(d)) instead of sort (O(d log d))
   * since CSR forward and backward arrays are already individually sorted.
   * Multi-type sort phase and triangle counting are both parallelized.
   */
  private static double[] lccBuildAndIntersect(final GraphAnalyticalView view, final String[] types, final int n) {
    final boolean singleType = types.length == 1;
    // First pass: compute degree per node
    final int[] degree = new int[n];
    for (final String edgeType : types) {
      final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
      if (csr == null)
        continue;
      for (int u = 0; u < n; u++)
        degree[u] += csr.outDegree(u) + csr.inDegree(u);
    }

    // Build offsets from degrees
    final int[] offsets = new int[n + 1];
    for (int i = 0; i < n; i++)
      offsets[i + 1] = offsets[i] + degree[i];

    final int totalEdges = offsets[n];
    final int[] neighbors = new int[totalEdges];
    final int[] pos = new int[n];
    for (int i = 0; i < n; i++)
      pos[i] = offsets[i];

    if (singleType) {
      // Single edge type: merge forward + backward (both already sorted) -> O(d) per node
      final CSRAdjacencyIndex csr = view.getCSRIndex(types[0]);
      final int[] fwdOffsets = csr.getForwardOffsets();
      final int[] fwdNeighbors = csr.getForwardNeighbors();
      final int[] bwdOffsets = csr.getBackwardOffsets();
      final int[] bwdNeighbors = csr.getBackwardNeighbors();
      for (int u = 0; u < n; u++) {
        int ia = fwdOffsets[u], aEnd = fwdOffsets[u + 1];
        int ib = bwdOffsets[u], bEnd = bwdOffsets[u + 1];
        int p = offsets[u];
        while (ia < aEnd && ib < bEnd) {
          if (fwdNeighbors[ia] <= bwdNeighbors[ib])
            neighbors[p++] = fwdNeighbors[ia++];
          else
            neighbors[p++] = bwdNeighbors[ib++];
        }
        while (ia < aEnd)
          neighbors[p++] = fwdNeighbors[ia++];
        while (ib < bEnd)
          neighbors[p++] = bwdNeighbors[ib++];
      }
    } else {
      // Multiple edge types: copy all, then sort in parallel
      for (final String edgeType : types) {
        final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
        if (csr == null)
          continue;
        final int[] fwdOffsets = csr.getForwardOffsets();
        final int[] fwdNeighbors = csr.getForwardNeighbors();
        for (int u = 0; u < n; u++)
          for (int j = fwdOffsets[u]; j < fwdOffsets[u + 1]; j++)
            neighbors[pos[u]++] = fwdNeighbors[j];
        final int[] bwdOffsets = csr.getBackwardOffsets();
        final int[] bwdNeighbors = csr.getBackwardNeighbors();
        for (int u = 0; u < n; u++)
          for (int j = bwdOffsets[u]; j < bwdOffsets[u + 1]; j++)
            neighbors[pos[u]++] = bwdNeighbors[j];
      }
      // Sort each adjacency list — parallel for large graphs
      parallelForRange(n, (start, end) -> {
        for (int u = start; u < end; u++)
          Arrays.sort(neighbors, offsets[u], offsets[u + 1]);
      });
    }

    // Count triangles — parallel per vertex (each triangles[u] is independent)
    final long[] triangles = new long[n];
    parallelForRange(n, (start, end) -> {
      for (int u = start; u < end; u++) {
        final int uStart = offsets[u];
        final int uEnd = offsets[u + 1];
        long count = 0;
        for (int k = uStart; k < uEnd; k++) {
          final int v = neighbors[k];
          final int vStart = offsets[v];
          final int vEnd = offsets[v + 1];
          int iu = uStart, iv = vStart;
          while (iu < uEnd && iv < vEnd) {
            if (neighbors[iu] < neighbors[iv])
              iu++;
            else if (neighbors[iu] > neighbors[iv])
              iv++;
            else {
              count++;
              iu++;
              iv++;
            }
          }
        }
        triangles[u] = count;
      }
    });

    // Each triangle counted twice per node
    final double[] lcc = new double[n];
    for (int u = 0; u < n; u++) {
      final long deg = offsets[u + 1] - offsets[u];
      if (deg >= 2)
        lcc[u] = (double) triangles[u] / (double) (deg * (deg - 1));
    }
    return lcc;
  }

  // --- Helpers ---

  private static String[] resolveEdgeTypes(final GraphAnalyticalView view, final String... edgeTypes) {
    if (edgeTypes != null && edgeTypes.length > 0)
      return edgeTypes;
    return view.getEdgeTypes().toArray(new String[0]);
  }
}
