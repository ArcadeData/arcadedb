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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GAVVertex;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.LongLongHashMap;

import com.arcadedb.query.QueryEngineManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Fused multi-hop GAV traversal operator — zero intermediate object allocation.
 * <p>
 * Replaces a chain of GAVExpandAll operators with a single operator that traverses
 * the entire path using only {@code int} nodeIds from CSR arrays. No intermediate
 * {@link ResultInternal}, {@link Vertex}, or HashMap allocations.
 * <p>
 * Only variables referenced in downstream expressions (WHERE/WITH/RETURN) are
 * materialized as Vertex objects in the final output. Intermediate variables
 * that are only used for traversal are never loaded from OLTP.
 * <p>
 * Memory: O(max_fanout) per source vertex for the traversal stack.
 * GC pressure: near-zero (only int[] arrays reused from CSR slices).
 * <p>
 * Cypher binds a relationship at most once per MATCH clause. The hops that could collide are walked one adjacency
 * slice per edge type and orientation, so every entry of their stack names one relationship, and a candidate is refused
 * when an earlier hop of its clause stands on the same one (issue #8394). An undirected hop meets a self-loop in both
 * lists of its vertex and takes it once.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GAVFusedChainOperator extends AbstractPhysicalOperator {
  private final GraphTraversalProvider provider;

  // Source variable (bound by the child operator, e.g., NodeByLabelScan)
  private final String sourceVariable;

  // Chain hops: [0] = first expand, [n-1] = last expand
  private final Vertex.DIRECTION[] hopDirections;
  private final String[][] hopEdgeTypes;
  private final String[] hopTargetVariables;

  // Target label filter per hop: bucket IDs for type checking (null = no filter)
  private final int[][] hopTargetBucketIds;

  // Which variables need to be materialized as Vertex in the output
  private final boolean[] materializeVariable; // [0]=source, [1..n]=hop targets

  // Optional pushed-down filter predicate (evaluated via column store before emitting)
  private BooleanExpression pushedFilter;

  // Fused aggregation: when set, the parallel DFS counts per group internally
  // instead of producing individual rows. Bypasses GroupByAggregationStep entirely.
  // groupKeyVariables = variables to group by (e.g., ["asker", "answerer"])
  // groupKeyProperties = property to read from each variable for grouping (e.g., ["Id", "Id"]), null = use variable identity
  // countOutputName = output alias for count(*) (e.g., "interactions")
  private String[] groupKeyVariables;
  private String countOutputName;
  private String[] groupKeyOutputNames;

  // Relationship uniqueness (#8394): per hop, whether its relationship may collide with another one of its clause,
  // and the earlier hops (indices into the chain) whose relationship it must not take again. Null when no hop collides.
  private boolean[] hopTracked;
  private int[][]   hopConflictsWith;

  public GAVFusedChainOperator(final PhysicalOperator child,
      final GraphTraversalProvider provider,
      final String sourceVariable,
      final Vertex.DIRECTION[] hopDirections,
      final String[][] hopEdgeTypes,
      final String[] hopTargetVariables,
      final int[][] hopTargetBucketIds,
      final boolean[] materializeVariable,
      final double estimatedCost,
      final long estimatedCardinality) {
    super(child, estimatedCost, estimatedCardinality);
    this.provider = provider;
    this.sourceVariable = sourceVariable;
    this.hopDirections = hopDirections;
    this.hopEdgeTypes = hopEdgeTypes;
    this.hopTargetVariables = hopTargetVariables;
    this.hopTargetBucketIds = hopTargetBucketIds;
    this.materializeVariable = materializeVariable;
  }

  /**
   * Pushes a WHERE filter into the fused chain. The filter is evaluated via GAVVertex
   * (column store access) before creating output objects, avoiding ResultInternal
   * allocations for rows that will be immediately discarded.
   */
  public void setPushedFilter(final BooleanExpression filter) {
    this.pushedFilter = filter;
  }

  /**
   * Enables fused aggregation: the parallel DFS counts per group internally
   * instead of producing individual rows. The downstream GroupByAggregationStep is bypassed.
   *
   * @param groupKeyVariables  variables to group by (must be materialized)
   * @param groupKeyOutputNames output aliases for each grouping key
   * @param countOutputName    output alias for count(*)
   */
  public void setFusedAggregation(final String[] groupKeyVariables, final String[] groupKeyOutputNames,
      final String countOutputName) {
    this.groupKeyVariables = groupKeyVariables;
    this.groupKeyOutputNames = groupKeyOutputNames;
    this.countOutputName = countOutputName;
  }

  /**
   * Makes the chain enforce relationship uniqueness: hop {@code i} is walked per edge type and orientation when
   * {@code hopTracked[i]}, and refuses a relationship the hops listed in {@code hopConflictsWith[i]} stand on.
   */
  public void setEdgeTracking(final boolean[] hopTracked, final int[][] hopConflictsWith) {
    this.hopTracked = hopTracked;
    this.hopConflictsWith = hopConflictsWith;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    // Built once on the calling thread and handed to the workers: reading the deadline from the shared context
    // inside each chunk would have every worker race to publish the same lazily-computed value (issue #6266).
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    final ResultSet inputResults = child.execute(context, nRecords);
    final Database db = context.getDatabase();
    final int chainLength = hopDirections.length;

    // Pre-compute the output variable names array (shared across all rows — zero per-row allocation)
    final String[] outputNames = buildOutputNames();

    // Pre-acquire NeighborViews for zero-allocation traversal (one per hop, shared across all vertices)
    final NeighborView[] hopViews = new NeighborView[chainLength];
    for (int i = 0; i < chainLength; i++)
      if (hopTracked == null || !hopTracked[i])
        hopViews[i] = provider.getNeighborView(hopDirections[i], hopEdgeTypes[i]);
    final TrackedTypes trackedTypes = hopTracked != null ? resolveTrackedTypes() : null;

    // Collect all source nodeIds into a primitive int[] for parallel partitioning (zero boxing)
    int[] sourceNodeIdsBuf = new int[1024];
    int sourceCount = 0;
    while (inputResults.hasNext()) {
      guard.check();
      final Result inputResult = inputResults.next();
      final Object sourceObj = inputResult.getProperty(sourceVariable);
      final int nodeId;
      if (sourceObj instanceof GAVVertex)
        nodeId = ((GAVVertex) sourceObj).getNodeId();
      else if (sourceObj instanceof Vertex)
        nodeId = provider.getNodeId(((Vertex) sourceObj).getIdentity());
      else
        continue;
      if (nodeId >= 0) {
        if (sourceCount == sourceNodeIdsBuf.length)
          sourceNodeIdsBuf = Arrays.copyOf(sourceNodeIdsBuf, sourceNodeIdsBuf.length * 2);
        sourceNodeIdsBuf[sourceCount++] = nodeId;
      }
    }
    inputResults.close();

    final int[] sourceNodeIds = sourceNodeIdsBuf;
    final int totalSources = sourceCount; // effectively final for lambda capture
    final int parallelism = Runtime.getRuntime().availableProcessors();
    // Guard against an empty source set: chunkSize must never be 0 (issue #5306, it later divides by it).
    final int chunkSize = Math.max(1, (totalSources + parallelism - 1) / parallelism);

    // If fused aggregation is enabled, use the parallel aggregating path
    if (groupKeyVariables != null)
      return executeWithFusedAggregation(sourceNodeIds, totalSources, parallelism, chunkSize,
          hopViews, trackedTypes, chainLength, db, context, guard);

    // Parallel DFS: each thread processes a chunk of source vertices with its own stack
    @SuppressWarnings("unchecked")
    final List<Result>[] threadResults = new List[Math.min(parallelism, Math.max(1, (totalSources + chunkSize - 1) / chunkSize))];

    final int threadCount;
    if (totalSources < 8192) {
      // Below threshold: single-threaded
      threadResults[0] = new ArrayList<>();
      traverseChunk(sourceNodeIds, 0, totalSources, hopViews, trackedTypes, chainLength, outputNames, db, context, guard,
          threadResults[0]);
      threadCount = 1;
    } else {
      // Parallel execution using shared query worker pool
      final ExecutorService executor = QueryEngineManager.getInstance().getExecutorService();
      final Future<?>[] futures = new Future<?>[threadResults.length];
      int launched = 0;
      for (int t = 0; t < threadResults.length; t++) {
        final int start = t * chunkSize;
        final int end = Math.min(start + chunkSize, totalSources);
        if (start >= totalSources)
          break;
        threadResults[t] = new ArrayList<>();
        final int threadIdx = t;
        futures[t] = executor.submit(() ->
            traverseChunk(sourceNodeIds, start, end, hopViews, trackedTypes, chainLength, outputNames, db, context, guard,
                threadResults[threadIdx]));
        launched++;
      }
      // #4951: awaitFutures throws on interrupt (cancelling the outstanding chunks) instead of returning,
      // so a killed/timed-out query can never merge partial per-thread results as a complete answer.
      // #6568: it also RECLAIMS, which is what lets this fan-out submit EVERY chunk - unlike parallelForRange
      // and PartitionedTriangleOp, which keep chunk 0 on the caller for latency. A chunk still queued when the
      // wait begins is run here rather than waited for, so the caller can never park behind busy workers.
      GraphAlgorithms.awaitFutures(futures, launched);
      threadCount = launched;
    }

    // Merge thread-local results into a single iterator
    final List<Result> merged = new ArrayList<>();
    for (int t = 0; t < threadCount; t++)
      if (threadResults[t] != null)
        merged.addAll(threadResults[t]);

    final Iterator<Result> mergedIter = merged.iterator();
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return mergedIter.hasNext();
      }

      @Override
      public Result next() {
        return mergedIter.next();
      }

      @Override
      public void close() {
      }
    };
  }

  /**
   * Parallel fused aggregation: each thread traverses its chunk of sources via DFS
   * and accumulates counts per group using a thread-local HashMap&lt;long, long&gt;.
   * Zero GAVVertex/GAVResult allocation during traversal — pure int operations.
   * Thread-local maps are merged after all threads complete.
   */
  private ResultSet executeWithFusedAggregation(final int[] sourceNodeIds, final int totalSources,
      final int parallelism, final int chunkSize, final NeighborView[] hopViews, final TrackedTypes trackedTypes,
      final int chainLength, final Database db, final CommandContext context, final WorkGuard guard) {

    // Resolve which nodeId slot each group key variable maps to:
    // sourceVariable = slot 0, hopTargetVariables[i] = slot i+1
    final int[] groupKeySlots = new int[groupKeyVariables.length];
    for (int g = 0; g < groupKeyVariables.length; g++) {
      if (groupKeyVariables[g].equals(sourceVariable))
        groupKeySlots[g] = 0;
      else {
        groupKeySlots[g] = -1;
        for (int h = 0; h < hopTargetVariables.length; h++)
          if (groupKeyVariables[g].equals(hopTargetVariables[h])) {
            groupKeySlots[g] = h + 1;
            break;
          }
      }
    }

    final int numThreads = Math.min(parallelism, Math.max(1, (totalSources + chunkSize - 1) / chunkSize));
    @SuppressWarnings("unchecked")
    final LongLongHashMap[] threadMaps = new LongLongHashMap[numThreads];
    if (totalSources < 8192) {
      // Single-threaded
      threadMaps[0] = new LongLongHashMap();
      aggregateChunk(sourceNodeIds, 0, totalSources, hopViews, trackedTypes, chainLength, groupKeySlots, db, context, guard,
          threadMaps[0]);
    } else {
      // Parallel using shared query worker pool
      final ExecutorService executor = QueryEngineManager.getInstance().getExecutorService();
      final Future<?>[] futures = new Future<?>[numThreads];
      int launched = 0;
      for (int t = 0; t < numThreads; t++) {
        final int start = t * chunkSize;
        final int end = Math.min(start + chunkSize, totalSources);
        if (start >= totalSources)
          break;
        threadMaps[t] = new LongLongHashMap();
        final int threadIdx = t;
        futures[t] = executor.submit(() ->
            aggregateChunk(sourceNodeIds, start, end, hopViews, trackedTypes, chainLength, groupKeySlots, db, context, guard,
                threadMaps[threadIdx]));
        launched++;
      }
      // #4951: awaitFutures throws on interrupt (cancelling the outstanding chunks) instead of returning,
      // so a killed/timed-out query can never merge partial per-thread maps as a complete answer.
      // #6568: and it reclaims a still-queued chunk instead of parking on it - see the note on the DFS path.
      GraphAlgorithms.awaitFutures(futures, launched);
    }

    // Merge thread-local maps (zero boxing — primitive long operations)
    final LongLongHashMap merged = threadMaps[0] != null ? threadMaps[0] : new LongLongHashMap();
    for (int t = 1; t < numThreads; t++)
      if (threadMaps[t] != null)
        merged.mergeFrom(threadMaps[t]);

    // Build output results — one per group (only ~50K allocations, not 740K)
    final List<Result> results = new ArrayList<>(merged.size());
    merged.forEach((packedKey, count) -> {
      final ResultInternal result = new ResultInternal();
      if (groupKeySlots.length == 2) {
        final int nodeId0 = (int) (packedKey >>> 32);
        final int nodeId1 = (int) packedKey;
        result.setProperty(groupKeyOutputNames[0], new GAVVertex(provider.getRID(nodeId0), nodeId0, provider, db));
        result.setProperty(groupKeyOutputNames[1], new GAVVertex(provider.getRID(nodeId1), nodeId1, provider, db));
      } else if (groupKeySlots.length == 1) {
        final int nodeId0 = (int) packedKey;
        result.setProperty(groupKeyOutputNames[0], new GAVVertex(provider.getRID(nodeId0), nodeId0, provider, db));
      }
      result.setProperty(countOutputName, count);
      results.add(result);
    });

    final Iterator<Result> iter = results.iterator();
    return new ResultSet() {
      @Override public boolean hasNext() { return iter.hasNext(); }
      @Override public Result next() { return iter.next(); }
      @Override public void close() { }
    };
  }

  /**
   * Aggregating DFS for a chunk of sources. Pure int operations — zero object allocation
   * during traversal. Counts are accumulated in the thread-local map.
   */
  private void aggregateChunk(final int[] sourceNodeIds, final int start, final int end,
      final NeighborView[] hopViews, final TrackedTypes trackedTypes, final int chainLength, final int[] groupKeySlots,
      final Database db, final CommandContext context, final WorkGuard guard, final LongLongHashMap counts) {

    // Pre-allocate reusable filter objects ONCE per thread (zero per-path allocation)
    final Object[] filterValues;
    final GAVResult filterResult;
    if (pushedFilter != null) {
      final String[] filterNames = buildOutputNames();
      filterValues = new Object[filterNames.length];
      filterResult = new GAVResult(filterNames, filterValues);
    } else {
      filterValues = null;
      filterResult = null;
    }

    walkChunk(sourceNodeIds, start, end, hopViews, trackedTypes, chainLength, guard, stackNodeId -> {
      // Evaluate pushed filter using reusable GAVResult — only nodeId updated per path
      if (filterResult != null) {
        int slot = 0;
        if (materializeVariable[0])
          filterValues[slot++] = makeReference(stackNodeId[0], db);
        for (int i = 0; i < hopTargetVariables.length; i++)
          if (hopTargetVariables[i] != null && materializeVariable[i + 1])
            filterValues[slot++] = makeReference(stackNodeId[i + 1], db);
        if (!Boolean.TRUE.equals(pushedFilter.evaluate(filterResult, context)))
          return;
      }

      // Pack grouping key nodeIds into a single long (supports up to 2 int keys)
      long packedKey = 0;
      if (groupKeySlots.length == 2)
        packedKey = ((long) stackNodeId[groupKeySlots[0]] << 32) | (stackNodeId[groupKeySlots[1]] & 0xFFFFFFFFL);
      else if (groupKeySlots.length == 1)
        packedKey = stackNodeId[groupKeySlots[0]];

      // Increment count — zero boxing, zero allocation (primitive long → long)
      counts.increment(packedKey);
    });
  }

  /**
   * Traverses a chunk of source vertices through the multi-hop chain.
   * Each call has its own DFS stack — safe for parallel execution with no shared mutable state.
   */
  private void traverseChunk(final int[] sourceNodeIds, final int start, final int end,
      final NeighborView[] hopViews, final TrackedTypes trackedTypes, final int chainLength, final String[] outputNames,
      final Database db, final CommandContext context, final WorkGuard guard, final List<Result> output) {
    walkChunk(sourceNodeIds, start, end, hopViews, trackedTypes, chainLength, guard,
        stackNodeId -> emitResult(stackNodeId, outputNames, db, context, output));
  }

  /** Receives every complete path: {@code stackNodeId[0]} is the source, {@code stackNodeId[i + 1]} the target of hop i. */
  @FunctionalInterface
  private interface PathVisitor {
    void visit(int[] stackNodeId);
  }

  /**
   * The DFS both the row-producing and the aggregating paths run. Each call has its own stack, so chunks can be walked
   * in parallel with no shared mutable state.
   */
  private void walkChunk(final int[] sourceNodeIds, final int start, final int end, final NeighborView[] hopViews,
      final TrackedTypes trackedTypes, final int chainLength, final WorkGuard guard, final PathVisitor visitor) {
    // Per-thread DFS stack (allocated once, reused across all sources in this chunk)
    final int[] stackNodeId = new int[chainLength + 1];
    final int[] stackCursor = new int[chainLength];
    final int[] stackEnd = new int[chainLength];
    final int[][] fallbackNeighbors = new int[chainLength][];
    // Undirected untracked hops: how many entries equal to the hop's own vertex were met, to take each self-loop once
    final int[] selfLoopEntries = new int[chainLength];
    final TrackedAdjacency tracked = trackedTypes != null ? new TrackedAdjacency(trackedTypes, chainLength) : null;

    // One supernode's DFS can dwarf the whole chunk, so the per-source check alone would leave the abort
    // latency proportional to the largest fan-out. The counter climbs across sources on purpose: throttling
    // per DFS step is what bounds the latency by a fixed amount of work instead (issue #6266).
    int steps = 0;

    for (int s = start; s < end; s++) {
      guard.check();
      final int sourceNodeId = sourceNodeIds[s];
      stackNodeId[0] = sourceNodeId;
      initHop(hopViews, fallbackNeighbors, tracked, stackCursor, stackEnd, selfLoopEntries, 0, sourceNodeId);
      int depth = 0;

      while (depth >= 0) {
        guard.checkPeriodically(++steps);
        if (stackCursor[depth] >= stackEnd[depth]) {
          depth--;
          if (depth >= 0)
            stackCursor[depth]++;
          continue;
        }

        final int cursor = stackCursor[depth];
        final boolean trackedHop = tracked != null && hopTracked[depth];
        final int neighborId;
        if (trackedHop)
          neighborId = tracked.neighbors[depth][cursor];
        else {
          final NeighborView view = hopViews[depth];
          if (view != null)
            neighborId = view.neighbors()[cursor];
          else
            neighborId = fallbackNeighbors[depth][cursor];
        }

        if (trackedHop) {
          // A relationship an earlier hop of the clause stands on cannot be bound again
          if (tracked.conflicts(depth, cursor, stackNodeId, stackCursor)) {
            stackCursor[depth]++;
            continue;
          }
        } else if (hopDirections[depth] == Vertex.DIRECTION.BOTH && neighborId == stackNodeId[depth]
            && (selfLoopEntries[depth]++ & 1) == 1) {
          // A self-loop sits in both lists an undirected hop merges: every second entry is the same relationship
          stackCursor[depth]++;
          continue;
        }

        // Target label filter
        if (hopTargetBucketIds[depth] != null) {
          final RID rid = provider.getRID(neighborId);
          if (rid == null || !matchesBuckets(rid.getBucketId(), hopTargetBucketIds[depth])) {
            stackCursor[depth]++;
            continue;
          }
        }

        stackNodeId[depth + 1] = neighborId;

        if (depth == chainLength - 1) {
          visitor.visit(stackNodeId);
          stackCursor[depth]++;
        } else {
          depth++;
          initHop(hopViews, fallbackNeighbors, tracked, stackCursor, stackEnd, selfLoopEntries, depth, neighborId);
        }
      }
    }
  }

  private void initHop(final NeighborView[] hopViews, final int[][] fallbackNeighbors, final TrackedAdjacency tracked,
      final int[] stackCursor, final int[] stackEnd, final int[] selfLoopEntries, final int depth, final int nodeId) {
    stackCursor[depth] = 0;
    selfLoopEntries[depth] = 0;
    if (tracked != null && hopTracked[depth]) {
      stackEnd[depth] = tracked.fill(depth, nodeId);
      return;
    }
    final NeighborView view = hopViews[depth];
    if (view != null) {
      stackCursor[depth] = view.offset(nodeId);
      stackEnd[depth] = view.offsetEnd(nodeId);
    } else {
      final int[] nbrs = provider.getNeighborIds(nodeId, hopDirections[depth], hopEdgeTypes[depth]);
      fallbackNeighbors[depth] = nbrs;
      stackEnd[depth] = nbrs.length;
    }
  }

  /** The edge types the tracked hops walk, numbered: a hop's own types, or every type the view holds for an untyped hop. */
  private record TrackedTypes(String[] names, int[][] hopTypeIds) {
  }

  private TrackedTypes resolveTrackedTypes() {
    final Map<String, Integer> ids = new HashMap<>();
    final List<String> names = new ArrayList<>();
    final int[][] hopTypeIds = new int[hopDirections.length][];
    for (int i = 0; i < hopDirections.length; i++) {
      if (!hopTracked[i])
        continue;
      String[] types = hopEdgeTypes[i];
      if (types == null || types.length == 0) {
        types = provider.getMaterializedEdgeTypes();
        if (types == null)
          types = new String[0];
      }
      hopTypeIds[i] = new int[types.length];
      for (int t = 0; t < types.length; t++) {
        final String type = types[t];
        hopTypeIds[i][t] = ids.computeIfAbsent(type, k -> {
          names.add(k);
          return names.size() - 1;
        });
      }
    }
    return new TrackedTypes(names.toArray(new String[0]), hopTypeIds);
  }

  /**
   * The adjacency of the tracked hops on the DFS stack, one entry per relationship. Each entry carries its edge type
   * and whether the hop's vertex is the relationship's source, which together with the two vertices name the
   * relationship up to its parallel twins; the rank among those is taken only when two hops meet on the same ones.
   * Per-thread, reused across every source of a chunk.
   */
  private final class TrackedAdjacency {
    private final TrackedTypes types;
    private final int[][]      neighbors;
    // (type id << 1) | 1 when the hop's vertex is the relationship's source
    private final int[][]      meta;

    private TrackedAdjacency(final TrackedTypes types, final int chainLength) {
      this.types = types;
      this.neighbors = new int[chainLength][];
      this.meta = new int[chainLength][];
      for (int i = 0; i < chainLength; i++)
        if (hopTracked[i]) {
          neighbors[i] = new int[16];
          meta[i] = new int[16];
        }
    }

    /** Loads the relationships of {@code nodeId} for hop {@code depth} and returns how many there are. */
    private int fill(final int depth, final int nodeId) {
      int size = 0;
      final Vertex.DIRECTION direction = hopDirections[depth];
      for (final int typeId : types.hopTypeIds()[depth]) {
        final String type = types.names()[typeId];
        if (direction != Vertex.DIRECTION.IN)
          size = append(depth, size, provider.getNeighborIds(nodeId, Vertex.DIRECTION.OUT, type), (typeId << 1) | 1, -1);
        // Undirected: a self-loop is in the outgoing list already
        if (direction != Vertex.DIRECTION.OUT)
          size = append(depth, size, provider.getNeighborIds(nodeId, Vertex.DIRECTION.IN, type), typeId << 1,
              direction == Vertex.DIRECTION.BOTH ? nodeId : -1);
      }
      return size;
    }

    private int append(final int depth, int size, final int[] slice, final int entryMeta, final int skip) {
      if (slice == null || slice.length == 0)
        return size;
      if (size + slice.length > neighbors[depth].length) {
        final int capacity = Math.max(neighbors[depth].length * 2, size + slice.length);
        neighbors[depth] = Arrays.copyOf(neighbors[depth], capacity);
        meta[depth] = Arrays.copyOf(meta[depth], capacity);
      }
      final int[] n = neighbors[depth];
      final int[] m = meta[depth];
      for (final int neighbor : slice) {
        if (neighbor == skip)
          continue;
        n[size] = neighbor;
        m[size] = entryMeta;
        ++size;
      }
      return size;
    }

    /** True when an earlier hop of the clause stands on the relationship entry {@code index} of hop {@code depth} names. */
    private boolean conflicts(final int depth, final int index, final int[] stackNodeId, final int[] stackCursor) {
      final int[] against = hopConflictsWith[depth];
      if (against == null || against.length == 0)
        return false;
      final int entryMeta = meta[depth][index];
      final int vertex = stackNodeId[depth];
      final int neighbor = neighbors[depth][index];
      final int out = (entryMeta & 1) != 0 ? vertex : neighbor;
      final int in = (entryMeta & 1) != 0 ? neighbor : vertex;
      int rank = -1;
      for (final int j : against) {
        final int jIndex = stackCursor[j];
        final int jMeta = meta[j][jIndex];
        if ((jMeta >>> 1) != (entryMeta >>> 1))
          continue;
        final int jVertex = stackNodeId[j];
        final int jNeighbor = neighbors[j][jIndex];
        final int jOut = (jMeta & 1) != 0 ? jVertex : jNeighbor;
        final int jIn = (jMeta & 1) != 0 ? jNeighbor : jVertex;
        if (jOut != out || jIn != in)
          continue;
        if (rank < 0)
          rank = rank(depth, index);
        if (rank(j, jIndex) == rank)
          return true;
      }
      return false;
    }

    /** The rank of entry {@code index} among the equal entries (same neighbour, type and orientation) before it. */
    private int rank(final int depth, final int index) {
      final int[] n = neighbors[depth];
      final int[] m = meta[depth];
      final int neighbor = n[index];
      final int entryMeta = m[index];
      int rank = 0;
      for (int i = 0; i < index; i++)
        if (n[i] == neighbor && m[i] == entryMeta)
          ++rank;
      return rank;
    }
  }

  private void emitResult(final int[] stackNodeId, final String[] outputNames,
      final Database database, final CommandContext context, final List<Result> output) {
    final Object[] values = new Object[outputNames.length];
    int slot = 0;

    if (materializeVariable[0])
      values[slot++] = makeReference(stackNodeId[0], database);

    for (int i = 0; i < hopTargetVariables.length; i++)
      if (hopTargetVariables[i] != null && materializeVariable[i + 1])
        values[slot++] = makeReference(stackNodeId[i + 1], database);

    final GAVResult result = new GAVResult(outputNames, values);

    // Evaluate pushed filter (column store access) before adding to output
    if (pushedFilter != null)
      if (!Boolean.TRUE.equals(pushedFilter.evaluate(result, context)))
        return;

    output.add(result);
  }

  private GAVVertex makeReference(final int nodeId, final Database database) {
    final RID rid = provider.getRID(nodeId);
    return rid != null ? new GAVVertex(rid, nodeId, provider, database) : null;
  }

  /**
   * Pre-computes the output variable names array. Shared across all rows (interned).
   * <p>
   * The names must line up slot-for-slot with the values written by {@link #emitResult}, which
   * only writes a slot for a variable it actually materializes. Reserving a slot for a
   * non-materialized source shifts every subsequent variable by one, so each one reads back the
   * next one's vertex and the last one reads back null (#5746).
   */
  private String[] buildOutputNames() {
    final List<String> names = new ArrayList<>();
    if (materializeVariable[0])
      names.add(sourceVariable);

    for (int i = 0; i < hopTargetVariables.length; i++)
      if (hopTargetVariables[i] != null && materializeVariable[i + 1])
        names.add(hopTargetVariables[i]);

    return names.toArray(new String[0]);
  }

  private static boolean matchesBuckets(final int bucketId, final int[] targetBuckets) {
    for (final int tb : targetBuckets)
      if (tb == bucketId)
        return true;
    return false;
  }

  @Override
  public String getOperatorType() {
    return "GAVFusedChain";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);

    sb.append(indent).append("+ GAVFusedChain(").append(sourceVariable).append(")");
    for (int i = 0; i < hopDirections.length; i++) {
      sb.append("-[");
      if (hopEdgeTypes[i] != null && hopEdgeTypes[i].length > 0)
        sb.append(":").append(String.join("|", hopEdgeTypes[i]));
      sb.append("]-");
      sb.append(hopDirections[i] == Vertex.DIRECTION.OUT ? ">" : hopDirections[i] == Vertex.DIRECTION.IN ? "<" : "");
      sb.append("(").append(hopTargetVariables[i] != null ? hopTargetVariables[i] : "?");
      if (!materializeVariable[i + 1])
        sb.append("*"); // asterisk marks deferred variables
      sb.append(")");
    }
    sb.append(" [provider=").append(provider.getName());
    sb.append(", hops=").append(hopDirections.length);
    if (hopTracked != null)
      sb.append(", unique relationships");
    sb.append(", cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");

    if (child != null)
      sb.append(child.explain(depth + 1));

    return sb.toString();
  }

  public GraphTraversalProvider getProvider() {
    return provider;
  }

  public int getChainLength() {
    return hopDirections.length;
  }
}
