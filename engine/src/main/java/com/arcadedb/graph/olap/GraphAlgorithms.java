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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.Vertex.DIRECTION;

import com.arcadedb.query.QueryEngineManager;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
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
  /**
   * How many checkpointed batches {@link #parallelForRangeCheckpointed} aims for on a range small enough that
   * dividing it by this count still keeps every batch at least {@link #PARALLEL_THRESHOLD} large (below that,
   * fewer, larger-than-this-count batches are used instead, down to a single one). Above
   * {@link #MAX_CHECKPOINT_BATCH_SIZE} nodes this count is no longer the driver: batch size is capped there
   * instead, so batch count keeps growing with the range and abort latency per batch stays bounded - without
   * the cap, batch size grew unboundedly with the range for any n above
   * {@code CHECKPOINT_BATCHES x PARALLEL_THRESHOLD}, since batch count was pinned at exactly this many.
   */
  private static final int CHECKPOINT_BATCHES     = 16;
  /** Upper bound on a single checkpointed batch's size, so abort latency stays bounded as the range grows past
   *  it instead of scaling with the range - see {@link #CHECKPOINT_BATCHES}. */
  private static final int MAX_CHECKPOINT_BATCH_SIZE = CHECKPOINT_BATCHES * PARALLEL_THRESHOLD;
  /** Bitmask for how often {@link #lccBuildAndIntersect}'s sequential prep passes check in between nodes -
   *  {@code (u & MASK) == MASK} is true every 1024th node, not every {@code MASK}th one. Matches the 1024-node
   *  stride {@code WorkGuard.checkPeriodically} and {@code GraphData.adjacency} both use elsewhere in the
   *  codebase. */
  private static final int LCC_PREP_CHECKPOINT_MASK = 1023;
  /** Entry-count threshold at which {@link #lccBuildAndIntersect}'s per-node prep passes checkpoint mid-row,
   *  inside a single node's own edge walk rather than only between nodes - otherwise one supernode row is an
   *  unabortable unit regardless of its own size, the same class of gap issue #6715 names for
   *  {@code weightedAdjacencyFromColumns}. Same magnitude as
   *  {@code AbstractAlgoProcedure.ADJACENCY_CHECKPOINT_ENTRIES}, duplicated here rather than shared because
   *  that constant is {@code protected} on a class in a different package with no public accessor. */
  private static final int LCC_ROW_CHECKPOINT_ENTRIES = 1_048_576;
  private static final int PARALLEL_BFS_THRESHOLD = 4096;
  private static final double ALPHA              = 8.0;  // edge ratio for push->pull switch
  private static final int PULL_ENTER_DIVISOR    = 8;    // push->pull when frontier > n/8
  private static final int PULL_EXIT_DIVISOR     = 512;  // pull->push when frontier < n/512

  // VarHandle for lock-free CAS on long[] bitmap in parallel push mode
  private static final VarHandle LONG_ARRAY_VH = MethodHandles.arrayElementVarHandle(long[].class);

  private GraphAlgorithms() {
  }

  // --- Parallel Infrastructure ---

  /**
   * Partitions range [0, n) into chunks and submits each to the shared query-engine pool.
   * The calling thread always runs chunk 0 itself to avoid pool-starvation deadlock
   * when invoked from within a pool thread (e.g., during query execution).
   * Falls back to single-threaded execution when n is below threshold.
   */
  static void parallelForRange(final int n, final BiConsumer<Integer, Integer> work) {
    if (n < PARALLEL_THRESHOLD) {
      work.accept(0, n);
      return;
    }
    final ExecutorService executor = QueryEngineManager.getInstance().getExecutorService();
    final int chunkSize = (n + PARALLELISM - 1) / PARALLELISM;
    final Future<?>[] futures = new Future<?>[PARALLELISM - 1];
    int launched = 0;
    for (int t = 1; t < PARALLELISM; t++) {
      final int start = t * chunkSize;
      final int end = Math.min(start + chunkSize, n);
      if (start >= n)
        break;
      futures[launched++] = executor.submit(() -> work.accept(start, end));
    }
    // Calling thread runs chunk 0 — prevents deadlock when caller is a pool thread
    work.accept(0, Math.min(chunkSize, n));
    awaitFutures(futures, launched);
  }

  /**
   * Waits for all submitted futures to complete and rethrows the first exception if any.
   * <p>
   * #4951: on interrupt this must ABORT, never return normally. The previous code set the interrupt
   * flag and kept looping (every remaining {@code get()} threw immediately), then returned as if all
   * chunks had completed - so a query killed by a timeout or cancel merged partial per-chunk results
   * and reported them as a successful, complete answer. Now the outstanding futures are cancelled and
   * a {@link CommandExecutionException} is thrown, with the interrupt flag preserved for the caller.
   * Public because the parallel Cypher operators (GAV fused chain, partitioned triangle count) reuse
   * it for the same guarantee.
   */
  public static void awaitFutures(final Future<?>[] futures, final int count) {
    Throwable firstError = null;
    for (int i = 0; i < count; i++) {
      if (futures[i] == null)
        // Defensive: a caller that partially populated the array must not NPE the whole await.
        continue;
      try {
        futures[i].get();
      } catch (final ExecutionException e) {
        if (firstError == null)
          firstError = e.getCause();
      } catch (final InterruptedException e) {
        for (int j = i; j < count; j++)
          if (futures[j] != null)
            futures[j].cancel(true);
        Thread.currentThread().interrupt();
        final CommandExecutionException interrupted = new CommandExecutionException(
            "Parallel graph computation interrupted, partial results discarded");
        if (firstError != null)
          // A chunk had already failed before the interrupt: keep its cause for diagnostics.
          interrupted.addSuppressed(firstError);
        throw interrupted;
      }
    }
    if (firstError != null) {
      if (firstError instanceof RuntimeException re)
        throw re;
      if (firstError instanceof Error er)
        throw er;
      throw new RuntimeException(firstError);
    }
  }

  /**
   * As {@link #parallelForRange}, but for a kernel with no natural per-iteration checkpoint of its own: the range
   * is split into up to {@link #CHECKPOINT_BATCHES} batches, each run through {@link #parallelForRange} in turn,
   * with {@code checkpoint} called on the calling thread between batches - never from inside a worker chunk, same
   * contract as {@link WorkCheckpoint#check()} documents.
   * <p>
   * A batch is never smaller than {@link #PARALLEL_THRESHOLD}, so a small-but-nonzero range still runs as one
   * batch with one checkpoint call, exactly like a plain {@link #parallelForRange} call would have. A batch is
   * also never larger than {@link #MAX_CHECKPOINT_BATCH_SIZE}, so a range large enough to need more than
   * {@link #CHECKPOINT_BATCHES} of them gets more, smaller batches instead of {@link #CHECKPOINT_BATCHES} ever-
   * larger ones - otherwise abort latency would grow with the range without bound. {@code n == 0}
   * is the one exception: the batch loop never runs at all, so {@code checkpoint} is never called - harmless
   * today because every current caller ({@link #pageRank}, {@link #labelPropagation}, the LCC kernel) already
   * returns before reaching a checkpointed loop on an empty graph, but a future caller without that guarantee
   * would not get even one check-in.
   * <p>
   * On a large graph this is what gives {@link #localClusteringCoefficient} intra-pass abortability at all (issue
   * #6318: its one-shot triangle count had no iteration boundary to hang a checkpoint on the way {@link #pageRank}
   * and {@link #labelPropagation} do), and it narrows their own between-iterations latency to within a single pass.
   */
  static void parallelForRangeCheckpointed(final int n, final WorkCheckpoint checkpoint, final BiConsumer<Integer, Integer> work) {
    final int batchSize = Math.min(
        Math.max(PARALLEL_THRESHOLD, (n + CHECKPOINT_BATCHES - 1) / CHECKPOINT_BATCHES),
        MAX_CHECKPOINT_BATCH_SIZE);
    for (int start = 0; start < n; start += batchSize) {
      checkpoint.check();
      final int batchStart = start;
      final int batchEnd = Math.min(start + batchSize, n);
      parallelForRange(batchEnd - batchStart, (s, e) -> work.accept(batchStart + s, batchStart + e));
    }
  }

  // --- PageRank (Pull-based, Parallel) ---

  /**
   * Computes PageRank over the given view for the specified edge types.
   * Uses pull-based iteration: each node reads contributions FROM its neighbors
   * via CSR arrays. Each thread writes to a disjoint range of the next[] array,
   * requiring zero synchronization.
   * <p>
   * When direction is OUT (directed), out-degree uses forward CSR and pull reads backward CSR.
   * When direction is BOTH (undirected), out-degree uses forward+backward CSR and pull reads both.
   *
   * @param view       the analytical view (must be built)
   * @param damping    damping factor, typically 0.85
   * @param iterations number of power-iteration steps
   * @param direction  edge direction: OUT for directed, BOTH for undirected
   * @param edgeTypes  edge types to traverse (null or empty = all)
   * @return double[] of ranks indexed by dense node ID
   */
  public static double[] pageRank(final GraphAnalyticalView view, final double damping,
      final int iterations, final DIRECTION direction, final String... edgeTypes) {
    return pageRank(view, damping, iterations, direction, WorkCheckpoint.NONE, edgeTypes);
  }

  /**
   * {@link #pageRank(GraphAnalyticalView, double, int, DIRECTION, String...)} with a cooperative abort hook.
   * <p>
   * This kernel has no convergence test - it always runs the full {@code iterations} count - and that count comes
   * straight from a caller-supplied knob, so without a checkpoint {@code algo.pageRank({maxIterations: 2000000000})}
   * spins with nothing able to stop it. The hook is called once per power iteration at minimum, which bounds abort
   * latency by one sweep of the graph and costs one virtual call per O(n + m) of work.
   * <p>
   * The per-node work here runs inside {@link #parallelForRange}, on worker threads that would not observe an
   * interrupt aimed at the calling thread, and throwing out of a chunk closure would leave its siblings running.
   * So the checkpoint stays on the calling thread, between parallel phases - via {@link #parallelForRangeCheckpointed}
   * rather than {@link #parallelForRange} directly, which narrows abort latency to within a single large phase
   * rather than only between iterations (issue #6318).
   *
   * @param checkpoint called between iterations; throws to abort. {@link WorkCheckpoint#NONE} to run unbounded
   */
  public static double[] pageRank(final GraphAnalyticalView view, final double damping,
      final int iterations, final DIRECTION direction, final WorkCheckpoint checkpoint, final String... edgeTypes) {
    final int n = view.getNodeMapping().size();
    if (n == 0)
      return new double[0];

    final boolean undirected = direction == DIRECTION.BOTH;

    double[] rank = new double[n];
    double[] next = new double[n];
    final double initial = 1.0 / n;
    Arrays.fill(rank, initial);

    final String[] types = resolveEdgeTypes(view, edgeTypes);

    // Pre-hoist CSR arrays outside the iteration loop to avoid repeated HashMap lookups
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

    // Precompute outDegree array across all edge types (once, outside iteration loop)
    final int[] outDeg = new int[n];
    for (int t = 0; t < typeCount; t++) {
      if (allFwdOffsets[t] != null) {
        final int[] fwdOffsets = allFwdOffsets[t];
        for (int u = 0; u < n; u++)
          outDeg[u] += fwdOffsets[u + 1] - fwdOffsets[u];
      }
      if (undirected && allBwdOffsets[t] != null) {
        final int[] bwdOffsets = allBwdOffsets[t];
        for (int u = 0; u < n; u++)
          outDeg[u] += bwdOffsets[u + 1] - bwdOffsets[u];
      }
    }

    // Pre-compute 1/outDeg to replace division with multiplication in the hot loop
    final double[] invDeg = new double[n];
    for (int u = 0; u < n; u++)
      invDeg[u] = outDeg[u] > 0 ? 1.0 / outDeg[u] : 0.0;

    // Pre-collect dangling node IDs (zero out-degree) for fast iteration
    int danglingCount = 0;
    for (int u = 0; u < n; u++)
      if (outDeg[u] == 0)
        danglingCount++;
    final int[] danglingNodes = new int[danglingCount];
    int dIdx = 0;
    for (int u = 0; u < n; u++)
      if (outDeg[u] == 0)
        danglingNodes[dIdx++] = u;

    for (int iter = 0; iter < iterations; iter++) {
      checkpoint.check();
      final double base = (1.0 - damping) / n;
      final double[] currentRank = rank;
      final double[] nextRank = next;

      // Pre-compute contribution per node: rank[v] / outDeg[v].
      // This turns the inner loop into a single gather+accumulate with no arithmetic.
      final double[] contrib = new double[n];
      parallelForRangeCheckpointed(n, checkpoint, (s, e) -> {
        for (int u = s; u < e; u++)
          contrib[u] = currentRank[u] * invDeg[u];
      });

      // PULL: each node sums contributions from neighbors — parallel, zero sync
      parallelForRangeCheckpointed(n, checkpoint, (start, end) -> {
        for (int u = start; u < end; u++) {
          double sum = 0;
          for (int t = 0; t < typeCount; t++) {
            if (allBwdOffsets[t] != null) {
              final int[] bwdOffsets = allBwdOffsets[t];
              final int[] bwdNeighbors = allBwdNeighbors[t];
              for (int j = bwdOffsets[u]; j < bwdOffsets[u + 1]; j++)
                sum += contrib[bwdNeighbors[j]];
            }
            if (undirected && allFwdOffsets[t] != null) {
              final int[] fwdOffsets = allFwdOffsets[t];
              final int[] fwdNeighbors = allFwdNeighbors[t];
              for (int j = fwdOffsets[u]; j < fwdOffsets[u + 1]; j++)
                sum += contrib[fwdNeighbors[j]];
            }
          }
          nextRank[u] = base + damping * sum;
        }
      });

      // Handle dangling nodes: distribute their rank evenly
      double danglingSum = 0.0;
      for (int i = 0; i < danglingNodes.length; i++) {
        if ((i & 1023) == 1023)
          checkpoint.check();
        danglingSum += currentRank[danglingNodes[i]];
      }
      if (danglingSum > 0.0) {
        final double danglingContrib = damping * danglingSum / n;
        parallelForRangeCheckpointed(n, checkpoint, (s, e) -> {
          for (int u = s; u < e; u++)
            nextRank[u] += danglingContrib;
        });
      }

      // Swap
      final double[] tmp = rank;
      rank = next;
      next = tmp;
    }
    return rank;
  }

  /**
   * Computes PageRank over the given view for the specified edge types using directed (OUT) semantics.
   */
  public static double[] pageRank(final GraphAnalyticalView view, final double damping,
      final int iterations, final String... edgeTypes) {
    return pageRank(view, damping, iterations, DIRECTION.OUT, edgeTypes);
  }

  /**
   * Computes PageRank with default parameters: damping=0.85, iterations=20, direction=OUT.
   */
  public static double[] pageRank(final GraphAnalyticalView view, final String... edgeTypes) {
    return pageRank(view, 0.85, 20, DIRECTION.OUT, edgeTypes);
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

    // Pre-hoist CSR arrays outside the convergence loop to avoid repeated HashMap lookups
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

    boolean changed = true;
    while (changed) {
      System.arraycopy(label, 0, newLabel, 0, n);

      final AtomicBoolean anyChanged = new AtomicBoolean(false);
      parallelForRange(n, (start, end) -> {
        boolean localChanged = false;
        for (int u = start; u < end; u++) {
          int minLabel = label[u];
          for (int t = 0; t < typeCount; t++) {
            if (allFwdOffsets[t] != null) {
              final int[] fwdOffsets = allFwdOffsets[t];
              final int[] fwdNeighbors = allFwdNeighbors[t];
              for (int j = fwdOffsets[u]; j < fwdOffsets[u + 1]; j++) {
                final int nl = label[fwdNeighbors[j]];
                if (nl < minLabel)
                  minLabel = nl;
              }
            }
            if (allBwdOffsets[t] != null) {
              final int[] bwdOffsets = allBwdOffsets[t];
              final int[] bwdNeighbors = allBwdNeighbors[t];
              for (int j = bwdOffsets[u]; j < bwdOffsets[u + 1]; j++) {
                final int nl = label[bwdNeighbors[j]];
                if (nl < minLabel)
                  minLabel = nl;
              }
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
   * Uses Beamer's direction-optimizing push/pull BFS with edge-count heuristic,
   * hysteresis thresholds (separate enter/exit), and parallel pull mode.
   * Push mode uses VarHandle CAS on the visited bitmap for lock-free thread safety.
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

    // Precompute push-direction degree per node for edge-count heuristic
    final int[] degree = new int[n];
    long totalEdges = 0;
    for (int t = 0; t < typeCount; t++) {
      if (useFwd && allFwdOffsets[t] != null) {
        final int[] off = allFwdOffsets[t];
        for (int i = 0; i < n; i++)
          degree[i] += off[i + 1] - off[i];
      }
      if (useBwd && allBwdOffsets[t] != null) {
        final int[] off = allBwdOffsets[t];
        for (int i = 0; i < n; i++)
          degree[i] += off[i + 1] - off[i];
      }
    }
    for (int i = 0; i < n; i++)
      totalEdges += degree[i];

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

    // Edge-count heuristic state (Beamer's direction-optimizing BFS)
    final int pullEnterThreshold = n / PULL_ENTER_DIVISOR;
    final int pullExitThreshold = n / PULL_EXIT_DIVISOR;
    boolean inPullMode = false;
    long visitedEdges = degree[source];
    long edgesInFrontier = degree[source];
    int prevFrontierSize = 1;

    while (frontierSize > 0) {
      depth++;

      // Direction-optimizing switch using edge-count heuristic with hysteresis
      final long edgesUnexplored = totalEdges - visitedEdges;
      final boolean frontierGrowing = frontierSize >= prevFrontierSize;
      if (inPullMode) {
        // Stay in pull unless frontier is small and shrinking
        if (!frontierGrowing && frontierSize <= pullExitThreshold)
          inPullMode = false;
      } else {
        // Switch to pull when frontier is large and edge-dense
        if (frontierSize > pullEnterThreshold && edgesInFrontier > edgesUnexplored / ALPHA)
          inPullMode = true;
      }

      prevFrontierSize = frontierSize;
      long nextEdgesInFrontier = 0;

      if (inPullMode) {
        // PULL mode: scan ALL unvisited nodes, check if any reverse-neighbor is in frontier.
        // Parallelized: each thread handles a disjoint node range — no synchronization needed.
        // Breaks early after finding one parent per node.
        final int currentDepth = depth;
        final int numThreads = Math.min(PARALLELISM, Math.max(1, (n + PARALLEL_THRESHOLD - 1) / PARALLEL_THRESHOLD));

        if (numThreads <= 1) {
          // Sequential pull (small graph)
          int nextSize = 0;
          for (int v = 0; v < n; v++) {
            final int vWord = v >>> 6;
            final long vBit = 1L << (v & 63);
            if ((visited[vWord] & vBit) != 0)
              continue;

            boolean found = false;
            for (int t = 0; t < typeCount && !found; t++) {
              if (pullOffsets1[t] != null) {
                final int[] offsets = pullOffsets1[t];
                final int[] neighbors = pullNeighbors1[t];
                for (int j = offsets[v], end = offsets[v + 1]; j < end; j++) {
                  if ((frontierBitmap[neighbors[j] >>> 6] & (1L << (neighbors[j] & 63))) != 0) {
                    found = true;
                    break;
                  }
                }
              }
              if (!found && pullOffsets2[t] != null) {
                final int[] offsets = pullOffsets2[t];
                final int[] neighbors = pullNeighbors2[t];
                for (int j = offsets[v], end = offsets[v + 1]; j < end; j++) {
                  if ((frontierBitmap[neighbors[j] >>> 6] & (1L << (neighbors[j] & 63))) != 0) {
                    found = true;
                    break;
                  }
                }
              }
            }
            if (found) {
              visited[vWord] |= vBit;
              dist[v] = currentDepth;
              nextFrontier[nextSize++] = v;
              nextEdgesInFrontier += degree[v];
            }
          }

          Arrays.fill(frontierBitmap, 0L);
          for (int i = 0; i < nextSize; i++) {
            final int v = nextFrontier[i];
            frontierBitmap[v >>> 6] |= 1L << (v & 63);
          }
          final int[] tmp = frontier;
          frontier = nextFrontier;
          nextFrontier = tmp;
          frontierSize = nextSize;

        } else {
          // Parallel pull: each thread scans a disjoint range of unvisited nodes
          final int chunkSize = (n + numThreads - 1) / numThreads;
          final int[][] localNexts = new int[numThreads][];
          final int[] localSizes = new int[numThreads];
          final long[] localEdges = new long[numThreads];

          final ExecutorService executor = QueryEngineManager.getInstance().getExecutorService();
          final Future<?>[] futures = new Future<?>[numThreads - 1];
          int launched = 0;
          Runnable callerTask = null;
          for (int thr = 0; thr < numThreads; thr++) {
            final int tIdx = thr;
            final int tStart = thr * chunkSize;
            final int tEnd = Math.min(tStart + chunkSize, n);
            if (tStart >= n)
              break;
            final Runnable task = () -> {
              int[] localNext = new int[Math.min(n, (tEnd - tStart) / 4 + 64)];
              int localSize = 0;
              long localEdgeSum = 0;
              for (int v = tStart; v < tEnd; v++) {
                final int vWord = v >>> 6;
                final long vBit = 1L << (v & 63);
                if ((visited[vWord] & vBit) != 0)
                  continue;
                boolean found = false;
                for (int t = 0; t < typeCount && !found; t++) {
                  if (pullOffsets1[t] != null) {
                    final int[] offsets = pullOffsets1[t];
                    final int[] neighbors = pullNeighbors1[t];
                    for (int j = offsets[v], end = offsets[v + 1]; j < end; j++) {
                      if ((frontierBitmap[neighbors[j] >>> 6] & (1L << (neighbors[j] & 63))) != 0) {
                        found = true;
                        break;
                      }
                    }
                  }
                  if (!found && pullOffsets2[t] != null) {
                    final int[] offsets = pullOffsets2[t];
                    final int[] neighbors = pullNeighbors2[t];
                    for (int j = offsets[v], end = offsets[v + 1]; j < end; j++) {
                      if ((frontierBitmap[neighbors[j] >>> 6] & (1L << (neighbors[j] & 63))) != 0) {
                        found = true;
                        break;
                      }
                    }
                  }
                }
                if (found) {
                  dist[v] = currentDepth;
                  if (localSize >= localNext.length)
                    localNext = Arrays.copyOf(localNext, Math.min(n, localNext.length * 2));
                  localNext[localSize++] = v;
                  localEdgeSum += degree[v];
                }
              }
              localNexts[tIdx] = localNext;
              localSizes[tIdx] = localSize;
              localEdges[tIdx] = localEdgeSum;
            };
            if (callerTask == null)
              callerTask = task;
            else
              futures[launched++] = executor.submit(task);
          }
          // Calling thread runs chunk 0 — prevents pool-starvation deadlock
          if (callerTask != null)
            callerTask.run();
          awaitFutures(futures, launched);

          // Merge thread-local results
          int totalNext = 0;
          for (int t = 0; t < numThreads; t++) {
            if (localNexts[t] == null)
              continue;
            totalNext += localSizes[t];
            nextEdgesInFrontier += localEdges[t];
          }
          int pos = 0;
          for (int t = 0; t < numThreads; t++)
            if (localNexts[t] != null && localSizes[t] > 0) {
              System.arraycopy(localNexts[t], 0, nextFrontier, pos, localSizes[t]);
              pos += localSizes[t];
            }

          // Update visited and frontier bitmaps after merge (sequential, no atomics)
          for (int i = 0; i < totalNext; i++) {
            final int v = nextFrontier[i];
            visited[v >>> 6] |= 1L << (v & 63);
          }
          Arrays.fill(frontierBitmap, 0L);
          for (int i = 0; i < totalNext; i++) {
            final int v = nextFrontier[i];
            frontierBitmap[v >>> 6] |= 1L << (v & 63);
          }

          final int[] tmp = frontier;
          frontier = nextFrontier;
          nextFrontier = tmp;
          frontierSize = totalNext;
        }

      } else if (frontierSize > PARALLEL_BFS_THRESHOLD) {
        // PUSH mode (parallel): expand frontier using VarHandle CAS on visited bitmap
        final int fSize = frontierSize;
        final int[] currentFrontier = frontier;
        final int currentDepth = depth;
        final int numThreads = Math.min(PARALLELISM, (fSize + PARALLEL_BFS_THRESHOLD - 1) / PARALLEL_BFS_THRESHOLD);
        final int chunkSize = (fSize + numThreads - 1) / numThreads;
        final int[][] localNexts = new int[numThreads][];
        final int[] localSizes = new int[numThreads];
        final long[] localEdges = new long[numThreads];

        final ExecutorService executor = QueryEngineManager.getInstance().getExecutorService();
        final Future<?>[] futures = new Future<?>[numThreads - 1];
        int launched = 0;
        Runnable callerTask = null;
        for (int t = 0; t < numThreads; t++) {
          final int tIdx = t;
          final int tStart = t * chunkSize;
          final int tEnd = Math.min(tStart + chunkSize, fSize);
          if (tStart >= fSize)
            break;
          final Runnable task = () -> {
            int[] localNext = new int[Math.min(n, (tEnd - tStart) * 8)];
            int localSize = 0;
            long localEdgeSum = 0;
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
                      oldVal = (long) LONG_ARRAY_VH.getVolatile(visited, word);
                      if ((oldVal & bit) != 0)
                        break;
                    } while (!LONG_ARRAY_VH.compareAndSet(visited, word, oldVal, oldVal | bit));
                    if ((oldVal & bit) == 0) {
                      dist[v] = currentDepth;
                      if (localSize >= localNext.length)
                        localNext = Arrays.copyOf(localNext, Math.min(n, localNext.length * 2));
                      localNext[localSize++] = v;
                      localEdgeSum += degree[v];
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
                      oldVal = (long) LONG_ARRAY_VH.getVolatile(visited, word);
                      if ((oldVal & bit) != 0)
                        break;
                    } while (!LONG_ARRAY_VH.compareAndSet(visited, word, oldVal, oldVal | bit));
                    if ((oldVal & bit) == 0) {
                      dist[v] = currentDepth;
                      if (localSize >= localNext.length)
                        localNext = Arrays.copyOf(localNext, Math.min(n, localNext.length * 2));
                      localNext[localSize++] = v;
                      localEdgeSum += degree[v];
                    }
                  }
                }
              }
            }
            localNexts[tIdx] = localNext;
            localSizes[tIdx] = localSize;
            localEdges[tIdx] = localEdgeSum;
          };
          if (callerTask == null)
            callerTask = task;
          else
            futures[launched++] = executor.submit(task);
        }
        // Calling thread runs chunk 0 — prevents pool-starvation deadlock
        if (callerTask != null)
          callerTask.run();
        awaitFutures(futures, launched);

        // Merge thread-local frontiers
        int totalNext = 0;
        for (int t = 0; t < numThreads; t++)
          if (localNexts[t] != null) {
            totalNext += localSizes[t];
            nextEdgesInFrontier += localEdges[t];
          }
        int pos = 0;
        for (int t = 0; t < numThreads; t++)
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
                  nextEdgesInFrontier += degree[v];
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
                  nextEdgesInFrontier += degree[v];
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

        final int[] tmp = frontier;
        frontier = nextFrontier;
        nextFrontier = tmp;
        frontierSize = nextSize;
      }

      // Update edge-count tracking for next iteration
      visitedEdges += nextEdgesInFrontier;
      edgesInFrontier = nextEdgesInFrontier;
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
    final PriorityQueue<double[]> heap = new PriorityQueue<>((a, b) -> Double.compare(a[0], b[0]));
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
    return labelPropagation(view, maxIters, WorkCheckpoint.NONE, edgeTypes);
  }

  /**
   * {@link #labelPropagation(GraphAnalyticalView, int, String...)} with a cooperative abort hook.
   * <p>
   * The convergence test ({@code break} once no label moved) is not a bound on {@code maxIters}: a graph that
   * oscillates between two labellings never converges, so the caller-supplied knob is what decides when the run
   * ends. The hook is called once per iteration at minimum, which bounds abort latency by one sweep of the graph;
   * the per-node work inside that sweep runs through {@link #parallelForRangeCheckpointed} for the same
   * intra-phase latency {@link #pageRank(GraphAnalyticalView, double, int, DIRECTION, WorkCheckpoint, String...)}
   * gets from it.
   *
   * @param checkpoint called between iterations; throws to abort. {@link WorkCheckpoint#NONE} to run unbounded
   */
  public static int[] labelPropagation(final GraphAnalyticalView view, final int maxIters,
      final WorkCheckpoint checkpoint, final String... edgeTypes) {
    final int n = view.getNodeMapping().size();
    if (n == 0)
      return new int[0];

    final int[] labels = new int[n];
    final int[] newLabels = new int[n];
    for (int i = 0; i < n; i++)
      labels[i] = i;

    final String[] types = resolveEdgeTypes(view, edgeTypes);

    // Pre-hoist CSR arrays outside the iteration loop to avoid repeated HashMap lookups
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

    // Pre-compute max degree using pre-hoisted arrays (avoid method calls in loop)
    int maxDegree = 0;
    for (int u = 0; u < n; u++) {
      int deg = 0;
      for (int t = 0; t < typeCount; t++) {
        if (allFwdOffsets[t] != null)
          deg += allFwdOffsets[t][u + 1] - allFwdOffsets[t][u];
        if (allBwdOffsets[t] != null)
          deg += allBwdOffsets[t][u + 1] - allBwdOffsets[t][u];
      }
      if (deg > maxDegree)
        maxDegree = deg;
    }

    final int maxDeg = maxDegree;

    for (int iter = 0; iter < maxIters; iter++) {
      checkpoint.check();
      System.arraycopy(labels, 0, newLabels, 0, n);

      final AtomicBoolean anyChanged = new AtomicBoolean(false);

      // A fresh neighborBuf per chunk invocation rather than a shared/pooled one: parallelForRangeCheckpointed's
      // batching (issue #6318) multiplies how often this closure runs per iteration - up to CHECKPOINT_BATCHES
      // x, versus once per parallelForRange chunk before. A ThreadLocal to reuse the buffer across batches/
      // iterations was tried and rejected: it trades a small, bounded per-call allocation for unbounded
      // retention on the engine's long-lived shared thread pool - a ThreadLocal set on a pool thread outlives
      // this call, so a supernode-sized maxDeg (e.g. a 10M-degree hub, ~40 MB) stays retained on every thread
      // that ever ran a chunk, indefinitely, until that thread happens to touch an unrelated ThreadLocal and
      // expunges the stale entry. A retained-indefinitely large buffer is worse than a reallocated-but-GC'd
      // small one, so this keeps the simpler shape.
      parallelForRangeCheckpointed(n, checkpoint, (start, end) -> {
        final int[] neighborBuf = new int[maxDeg];
        boolean localChanged = false;

        for (int u = start; u < end; u++) {
          // Collect neighbor labels into thread-local buffer
          int pos = 0;
          for (int t = 0; t < typeCount; t++) {
            if (allFwdOffsets[t] != null) {
              final int[] fwdOffsets = allFwdOffsets[t];
              final int[] fwdNeighbors = allFwdNeighbors[t];
              for (int j = fwdOffsets[u]; j < fwdOffsets[u + 1]; j++)
                neighborBuf[pos++] = labels[fwdNeighbors[j]];
            }
            if (allBwdOffsets[t] != null) {
              final int[] bwdOffsets = allBwdOffsets[t];
              final int[] bwdNeighbors = allBwdNeighbors[t];
              for (int j = bwdOffsets[u]; j < bwdOffsets[u + 1]; j++)
                neighborBuf[pos++] = labels[bwdNeighbors[j]];
            }
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
    return localClusteringCoefficient(view, WorkCheckpoint.NONE, edgeTypes);
  }

  /**
   * {@link #localClusteringCoefficient(GraphAnalyticalView, String...)} with a cooperative abort hook.
   * <p>
   * Unlike {@link #pageRank} and {@link #labelPropagation}, this kernel has no iteration loop to hang a
   * once-per-iteration checkpoint on: it is one O(m x sqrt(m)) pass with nothing but the graph sizing it (issue
   * #6318). {@code checkpoint} is called periodically through the sequential prep passes (degree count, adjacency
   * build, compaction) and, via {@link #parallelForRangeCheckpointed}, between the batches its two parallel
   * phases - the multi-type sort and the triangle count - are now split into, so the CSR path gets the same
   * abortability the OLTP path (whole node per {@code guard.check()}) already had.
   *
   * @param checkpoint called periodically; throws to abort. {@link WorkCheckpoint#NONE} to run unbounded
   */
  public static double[] localClusteringCoefficient(final GraphAnalyticalView view, final WorkCheckpoint checkpoint,
      final String... edgeTypes) {
    final int n = view.getNodeMapping().size();
    if (n == 0)
      return new double[0];

    final String[] types = resolveEdgeTypes(view, edgeTypes);

    return lccBuildAndIntersect(view, types, n, checkpoint);
  }

  /**
   * Builds a flat merged undirected adjacency from CSR arrays, then counts triangles via
   * sorted-merge intersection. For single edge type, uses merge (O(d)) instead of sort (O(d log d))
   * since CSR forward and backward arrays are already individually sorted.
   * Multi-type sort phase and triangle counting are both parallelized.
   */
  private static double[] lccBuildAndIntersect(final GraphAnalyticalView view, final String[] types, final int n,
      final WorkCheckpoint checkpoint) {
    final boolean singleType = types.length == 1;
    // First pass: compute degree per node
    final int[] degree = new int[n];
    for (final String edgeType : types) {
      final CSRAdjacencyIndex csr = view.getCSRIndex(edgeType);
      if (csr == null)
        continue;
      for (int u = 0; u < n; u++) {
        if ((u & LCC_PREP_CHECKPOINT_MASK) == LCC_PREP_CHECKPOINT_MASK)
          checkpoint.check();
        degree[u] += csr.outDegree(u) + csr.inDegree(u);
      }
    }

    // Build offsets from degrees
    final int[] offsets = new int[n + 1];
    for (int i = 0; i < n; i++) {
      if ((i & LCC_PREP_CHECKPOINT_MASK) == LCC_PREP_CHECKPOINT_MASK)
        checkpoint.check();
      offsets[i + 1] = offsets[i] + degree[i];
    }

    final int totalEdges = offsets[n];
    final int[] neighbors = new int[totalEdges];
    final int[] pos = new int[n];
    for (int i = 0; i < n; i++) {
      if ((i & LCC_PREP_CHECKPOINT_MASK) == LCC_PREP_CHECKPOINT_MASK)
        checkpoint.check();
      pos[i] = offsets[i];
    }

    if (singleType) {
      // Single edge type: merge forward + backward (both already sorted) -> O(d) per node
      final CSRAdjacencyIndex csr = view.getCSRIndex(types[0]);
      final int[] fwdOffsets = csr.getForwardOffsets();
      final int[] fwdNeighbors = csr.getForwardNeighbors();
      final int[] bwdOffsets = csr.getBackwardOffsets();
      final int[] bwdNeighbors = csr.getBackwardNeighbors();
      for (int u = 0; u < n; u++) {
        if ((u & LCC_PREP_CHECKPOINT_MASK) == LCC_PREP_CHECKPOINT_MASK)
          checkpoint.check();
        int ia = fwdOffsets[u], aEnd = fwdOffsets[u + 1];
        int ib = bwdOffsets[u], bEnd = bwdOffsets[u + 1];
        int p = offsets[u];
        while (ia < aEnd && ib < bEnd) {
          // Checkpointed on entries written for THIS node, not just between nodes: a single supernode row
          // (millions of entries) is otherwise one unabortable unit regardless of its own size, the same class
          // of gap issue #6715 names for weightedAdjacencyFromColumns.
          if (((p - offsets[u]) & (LCC_ROW_CHECKPOINT_ENTRIES - 1)) == 0)
            checkpoint.check();
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
        for (int u = 0; u < n; u++) {
          if ((u & LCC_PREP_CHECKPOINT_MASK) == LCC_PREP_CHECKPOINT_MASK)
            checkpoint.check();
          for (int j = fwdOffsets[u]; j < fwdOffsets[u + 1]; j++) {
            if (((j - fwdOffsets[u]) & (LCC_ROW_CHECKPOINT_ENTRIES - 1)) == 0)
              checkpoint.check();
            neighbors[pos[u]++] = fwdNeighbors[j];
          }
        }
        final int[] bwdOffsets = csr.getBackwardOffsets();
        final int[] bwdNeighbors = csr.getBackwardNeighbors();
        for (int u = 0; u < n; u++) {
          if ((u & LCC_PREP_CHECKPOINT_MASK) == LCC_PREP_CHECKPOINT_MASK)
            checkpoint.check();
          for (int j = bwdOffsets[u]; j < bwdOffsets[u + 1]; j++) {
            if (((j - bwdOffsets[u]) & (LCC_ROW_CHECKPOINT_ENTRIES - 1)) == 0)
              checkpoint.check();
            neighbors[pos[u]++] = bwdNeighbors[j];
          }
        }
      }
      // Sort each adjacency list — parallel for large graphs, checkpointed between batches (issue #6318)
      parallelForRangeCheckpointed(n, checkpoint, (start, end) -> {
        for (int u = start; u < end; u++)
          Arrays.sort(neighbors, offsets[u], offsets[u + 1]);
      });
    }

    // Both merge paths produce per-node sorted adjacency. Compact in place: drop self-loops and
    // duplicates from reciprocal edges (same neighbour in both the fwd and bwd CSR lists), so the
    // undirected neighbour set used for the simple-graph LCC holds each distinct neighbour once.
    // offsets[u] is captured before being overwritten, and the write cursor never overtakes the
    // read cursor, so the mutation is safe in place with no extra allocation. Sequential (the write
    // cursor carries across nodes), so it cannot go through parallelForRangeCheckpointed and is
    // checkpointed the same periodic way as the prep passes above.
    int write = 0;
    for (int u = 0; u < n; u++) {
      if ((u & LCC_PREP_CHECKPOINT_MASK) == LCC_PREP_CHECKPOINT_MASK)
        checkpoint.check();
      final int readStart = offsets[u];
      final int readEnd = offsets[u + 1];
      offsets[u] = write;
      int last = -1; // node IDs are non-negative sequential integers, so -1 never matches
      for (int j = readStart; j < readEnd; j++) {
        if (((j - readStart) & (LCC_ROW_CHECKPOINT_ENTRIES - 1)) == 0)
          checkpoint.check();
        final int neighbor = neighbors[j];
        if (neighbor != u && neighbor != last)
          neighbors[write++] = last = neighbor;
      }
    }
    offsets[n] = write;

    // Count triangles using the "forward" technique: for each edge (u, v) where v > u,
    // count common neighbors w > v via sorted-merge intersection.
    // Each triangle {u, v, w} is found exactly once (u < v < w), then credited to all 3 nodes.
    // This halves the intersection work compared to counting from both directions.
    // Uses AtomicLongArray for thread-safe increments on shared triangle counts.
    // Checkpointed between batches (issue #6318): this is the dominant O(m x sqrt(m)) phase.
    final AtomicLongArray triangles = new AtomicLongArray(n);
    parallelForRangeCheckpointed(n, checkpoint, (start, end) -> {
      for (int u = start; u < end; u++) {
        final int uStart = offsets[u];
        final int uEnd = offsets[u + 1];
        for (int k = uStart; k < uEnd; k++) {
          final int v = neighbors[k];
          if (v <= u)
            continue;  // only process edges where v > u
          // Intersect N(u) ∩ N(v) for neighbors w > v
          final int vStart = offsets[v];
          final int vEnd = offsets[v + 1];
          int iu = k + 1;  // start after v in u's sorted list (all entries > v)
          int iv = vStart;
          // Advance iv past entries <= v
          while (iv < vEnd && neighbors[iv] <= v)
            iv++;
          while (iu < uEnd && iv < vEnd) {
            final int nu = neighbors[iu];
            final int nv = neighbors[iv];
            if (nu < nv)
              iu++;
            else if (nu > nv)
              iv++;
            else {
              // Triangle {u, v, nu} found — credit all three nodes atomically
              triangles.incrementAndGet(u);
              triangles.incrementAndGet(v);
              triangles.incrementAndGet(nu);
              iu++;
              iv++;
            }
          }
        }
      }
    });

    // With forward counting, each triangle is found once and credited to all 3 nodes
    final double[] lcc = new double[n];
    for (int u = 0; u < n; u++) {
      if ((u & LCC_PREP_CHECKPOINT_MASK) == LCC_PREP_CHECKPOINT_MASK)
        checkpoint.check();
      final long deg = offsets[u + 1] - offsets[u];
      if (deg >= 2)
        lcc[u] = (2.0 * triangles.get(u)) / (double) (deg * (deg - 1));
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
