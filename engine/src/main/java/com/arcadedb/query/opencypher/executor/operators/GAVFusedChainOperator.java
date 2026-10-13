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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.graph.GAVVertex;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.LongLongHashMap;

import com.arcadedb.query.QueryEngineManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
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
 * The rows are handed out in bounded rounds rather than collected for the whole walk, and a count(*) fused into the
 * chain builds no row at all: neither holds heap in proportion to the number of paths (issue #9738).
 * <p>
 * Cypher binds a relationship at most once per MATCH clause. The hops that could collide are walked one adjacency
 * slice per edge type and orientation, so every entry of their stack names one relationship, and a candidate is refused
 * when an earlier hop of its clause stands on the same one (issue #8394). An undirected hop meets a self-loop in both
 * lists of its vertex and takes it once.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GAVFusedChainOperator extends AbstractPhysicalOperator {
  // Below this many sources the walk runs on the calling thread
  private static final int PARALLEL_THRESHOLD = 8192;
  // The paths each walker turns into rows per round of the row path: what bounds the rows held at once (issue #9738)
  private static final int ROWS_PER_ROUND     = 16_384;

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
  // The pushed filter split by the hop that binds what each conjunct reads (issue #9938): [d] is decided once the source
  // and the first d hop targets are bound, [n] on the complete path. Null when every conjunct waits for the complete path.
  private BooleanExpression[] filterAtDepth;
  // What a complete path is checked against: the last entry of filterAtDepth, or the whole pushed filter
  private BooleanExpression pathFilter;

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

  // The hops this chain replaced, in traversal order: a source the view does not map (a vertex created after the build
  // of a stale view still in use) is expanded through a fresh copy of them, which walks its edge records, rather than
  // dropped. Never executed themselves, so a cached plan shares no state between executions.
  private List<GAVExpandAll> unfusedHops;

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
    setPushedFilter(filter, null);
  }

  /**
   * Pushes a WHERE filter into the fused chain, split by the hop that binds the variables each conjunct reads: entry
   * {@code d} of {@code filterAtDepth} is evaluated as soon as the source and the targets of the first {@code d} hops are
   * bound, so a partial path that fails it is not walked any deeper (issue #9938), and the last entry on the complete path.
   * The conjuncts of all entries together must be {@code filter}, which the sources the view does not map are checked
   * against.
   *
   * @param filterAtDepth one entry per depth, {@code chainLength + 1} in all, null entries for the depths with nothing to
   *                      check, or null to check the whole filter on the complete path
   */
  public void setPushedFilter(final BooleanExpression filter, final BooleanExpression[] filterAtDepth) {
    if (filterAtDepth != null && filterAtDepth.length != hopDirections.length + 1)
      throw new IllegalArgumentException(
          "Expected " + (hopDirections.length + 1) + " filter depths for a chain of " + hopDirections.length + " hops, got "
              + filterAtDepth.length);
    this.pushedFilter = filter;
    this.filterAtDepth = filterAtDepth;
    this.pathFilter = filterAtDepth != null ? filterAtDepth[hopDirections.length] : filter;
  }

  /**
   * Enables fused aggregation: the parallel DFS counts per group internally
   * instead of producing individual rows. The downstream GroupByAggregationStep is bypassed.
   *
   * @param groupKeyVariables  variables to group by, bound by the chain (at most 2; none for one count over every path)
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

  /** The hops this chain replaced, in traversal order, used for the sources the view does not map. */
  public void setUnfusedHops(final List<GAVExpandAll> unfusedHops) {
    this.unfusedHops = unfusedHops;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    // Built once on the calling thread and handed to the workers: reading the deadline from the shared context
    // inside each chunk would have every worker race to publish the same lazily-computed value (issue #6266).
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    // Taken before any node ID is read: every ID of this execution must belong to this numbering
    final Object nodeIdSpace = provider.getNodeIdSpace();
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
    final TrackedTypes trackedTypes = hopTracked != null ? resolveTrackedTypes(db) : null;

    // Collect all source nodeIds into a primitive int[] for parallel partitioning (zero boxing)
    int[] sourceNodeIdsBuf = new int[1024];
    int sourceCount = 0;
    List<Result> unmappedSources = null;
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
      } else if (unfusedHops != null) {
        if (unmappedSources == null)
          unmappedSources = new ArrayList<>();
        unmappedSources.add(inputResult);
      }
    }
    inputResults.close();
    // Expanded whole, not streamed: these are the few vertices created after the build of a stale view still in use, and
    // their rows are bounded by their own fan-out rather than by the graph's
    final List<Result> unmappedRows = unmappedSources != null ? expandUnmappedSources(unmappedSources, context) : null;

    final int[] sourceNodeIds = sourceNodeIdsBuf;
    final int totalSources = sourceCount; // effectively final for lambda capture
    final int parallelism = Runtime.getRuntime().availableProcessors();
    // Guard against an empty source set: chunkSize must never be 0 (issue #5306, it later divides by it).
    final int chunkSize = Math.max(1, (totalSources + parallelism - 1) / parallelism);

    // If fused aggregation is enabled, use the parallel aggregating path
    if (groupKeyVariables != null)
      return executeWithFusedAggregation(sourceNodeIds, totalSources, parallelism, chunkSize,
          hopViews, trackedTypes, chainLength, db, context, guard, unmappedRows, nodeIdSpace);

    // One walker per thread, each over a contiguous slice of the sources, resumed round after round (issue #9738)
    final int walkerCount = totalSources < PARALLEL_THRESHOLD ? 1 :
        Math.min(parallelism, Math.max(1, (totalSources + chunkSize - 1) / chunkSize));
    final ChainWalker[] walkers = new ChainWalker[walkerCount];
    for (int t = 0; t < walkerCount; t++) {
      final int start = walkerCount == 1 ? 0 : Math.min(t * chunkSize, totalSources);
      final int end = walkerCount == 1 ? totalSources : Math.min(start + chunkSize, totalSources);
      walkers[t] = new ChainWalker(sourceNodeIds, start, end, hopViews, trackedTypes, chainLength, guard, db, context);
    }
    return new StreamingRows(walkers, outputNames, db, context, unmappedRows, nodeIdSpace);
  }

  /**
   * The rows of the walk, produced in rounds: each round resumes every unfinished walker for at most
   * {@link #ROWS_PER_ROUND} paths, in parallel when there is more than one, and the rows are handed out before the next
   * round starts. The heap the rows take is bounded by one round, whatever the number of paths, and a consumer that
   * stops early (a LIMIT) stops the walk with it (issue #9738). Between two rounds no task is in flight, so a result set
   * that is abandoned without being closed holds no worker.
   * <p>
   * The bound is per walker: a round holds at most {@code walkers x ROWS_PER_ROUND} rows. Every walker does the same
   * amount of work per round, whatever the degrees of its sources, so a skewed slice does not leave the others idle
   * until it ends; only its last round is shorter. With one walker the rows come in source order; with several they
   * interleave round by round, which no consumer relies on (Cypher rows have no order without ORDER BY, and the planner
   * never answers an ORDER BY from the index order through a fused chain).
   * <p>
   * A rebuild of the view between two rounds fails the next one with a {@link NeedRetryException} (see
   * {@link #checkNodeIdSpace(Object)}): unlike a fused count, which is all or nothing, a consumer may by then hold the rows
   * of the earlier rounds, which were read in one numbering and are right; only the rest of the walk is refused. Retrying
   * means running the query again from its first row: a caller that keeps the rows it already had and appends the
   * retry's would hold those rows twice.
   */
  private final class StreamingRows implements ResultSet {
    private final ChainWalker[]  walkers;
    private final List<Result>[] buffers;
    private final boolean[]      finished;
    private final String[]       outputNames;
    private final Database       database;
    private final CommandContext context;
    private final Object         nodeIdSpace;
    private       List<Result>   unmappedRows;
    private       int            unfinished;
    private       int            buffer;
    private       int            position;

    @SuppressWarnings("unchecked")
    private StreamingRows(final ChainWalker[] walkers, final String[] outputNames, final Database database,
        final CommandContext context, final List<Result> unmappedRows, final Object nodeIdSpace) {
      this.walkers = walkers;
      this.buffers = new List[walkers.length];
      this.finished = new boolean[walkers.length];
      this.outputNames = outputNames;
      this.database = database;
      this.context = context;
      this.nodeIdSpace = nodeIdSpace;
      this.unmappedRows = unmappedRows;
      this.unfinished = walkers.length;
      for (int t = 0; t < walkers.length; t++)
        buffers[t] = new ArrayList<>();
      this.buffer = walkers.length;
    }

    @Override
    public boolean hasNext() {
      while (!advanceToRow()) {
        if (unfinished > 0)
          walkRound();
        else if (unmappedRows != null && !unmappedRows.isEmpty()) {
          // The sources the view does not map come last, as one more buffer. Slot 0 is free to take it: every walker
          // has finished, so no round will write its buffer again, and every buffer was drained to get here
          buffers[0] = unmappedRows;
          unmappedRows = null;
          buffer = 0;
          position = 0;
        } else
          return false;
      }
      return true;
    }

    /** Skips the drained buffers of the round, releasing their rows: true when a row is ready. */
    private boolean advanceToRow() {
      while (buffer < buffers.length) {
        if (position < buffers[buffer].size())
          return true;
        buffers[buffer].clear();
        ++buffer;
        position = 0;
      }
      return false;
    }

    @Override
    public Result next() {
      if (!hasNext())
        throw new NoSuchElementException();
      return buffers[buffer].get(position++);
    }

    @Override
    public void close() {
      unfinished = 0;
      unmappedRows = null;
      // Never read again (buffer is past the last one), so the rows go now rather than with the result set
      Arrays.fill(buffers, List.of());
      buffer = buffers.length;
    }

    /**
     * A round that fails (the query timed out or was killed, a filter threw) ends the result set: the walkers are left
     * mid-path and a partial round must never be handed out as a complete one, so a later {@link #hasNext()} answers
     * false instead of resuming them.
     */
    private void walkRound() {
      try {
        walkPendingSlices();
      } catch (final RuntimeException e) {
        close();
        throw asRetryIfRenumbered(nodeIdSpace, e);
      } catch (final Error e) {
        close();
        throw e;
      }
    }

    private void walkPendingSlices() {
      int pending = 0;
      int last = -1;
      for (int t = 0; t < walkers.length; t++)
        if (!finished[t]) {
          ++pending;
          last = t;
        }
      if (pending == 1)
        walkSlice(last);
      else {
        final ExecutorService executor = QueryEngineManager.getInstance().getExecutorService();
        final Future<?>[] futures = new Future<?>[pending];
        int launched = 0;
        for (int t = 0; t < walkers.length; t++)
          if (!finished[t]) {
            final int walker = t;
            futures[launched++] = executor.submit(() -> walkSlice(walker));
          }
        // #4951: awaitFutures throws on interrupt (cancelling the outstanding slices) instead of returning, so a killed
        // or timed-out query never hands out a partial round as a complete one. #6568: it also RECLAIMS a slice still
        // queued, so the caller never parks behind busy workers.
        GraphAlgorithms.awaitFutures(futures, launched);
      }
      // finished[] and the buffers were written by the workers: Future.get() in awaitFutures, which also covers a slice
      // reclaimed and run on this thread, is what makes those writes visible here.
      // The walkers resume on the node IDs the rounds before read: a rebuild in between numbered the nodes afresh
      checkNodeIdSpace(nodeIdSpace);
      unfinished = 0;
      for (final boolean done : finished)
        if (!done)
          ++unfinished;
      buffer = 0;
      position = 0;
    }

    private void walkSlice(final int walker) {
      final List<Result> output = buffers[walker];
      finished[walker] = walkers[walker].walk(ROWS_PER_ROUND,
          stackNodeId -> emitResult(stackNodeId, outputNames, database, context, output));
    }
  }

  /**
   * Parallel fused aggregation: each thread traverses its chunk of sources via DFS
   * and accumulates counts per group using a thread-local HashMap&lt;long, long&gt;.
   * Zero GAVVertex/GAVResult allocation during traversal — pure int operations.
   * Thread-local maps are merged after all threads complete.
   * <p>
   * With no grouping key the count is a plain {@code long} per thread, and the answer is one row even when no path
   * matches (issue #9738).
   */
  private ResultSet executeWithFusedAggregation(final int[] sourceNodeIds, final int totalSources,
      final int parallelism, final int chunkSize, final NeighborView[] hopViews, final TrackedTypes trackedTypes,
      final int chainLength, final Database db, final CommandContext context, final WorkGuard guard,
      final List<Result> unmappedRows, final Object nodeIdSpace) {

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
    final boolean grouped = groupKeySlots.length > 0;
    final LongLongHashMap[] threadMaps = new LongLongHashMap[numThreads];
    final long[] threadTotals = new long[numThreads];
    try {
      if (totalSources < PARALLEL_THRESHOLD) {
        // Single-threaded
        if (grouped)
          threadMaps[0] = new LongLongHashMap();
        aggregateChunk(sourceNodeIds, 0, totalSources, hopViews, trackedTypes, chainLength, groupKeySlots, db, context, guard,
            threadMaps[0], threadTotals, 0);
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
          if (grouped)
            threadMaps[t] = new LongLongHashMap();
          final int threadIdx = t;
          futures[t] = executor.submit(() ->
              aggregateChunk(sourceNodeIds, start, end, hopViews, trackedTypes, chainLength, groupKeySlots, db, context, guard,
                  threadMaps[threadIdx], threadTotals, threadIdx));
          launched++;
        }
        // #4951: awaitFutures throws on interrupt (cancelling the outstanding chunks) instead of returning,
        // so a killed/timed-out query can never merge partial per-thread maps as a complete answer.
        // #6568: and it reclaims a still-queued chunk instead of parking on it - see the note on the row path.
        GraphAlgorithms.awaitFutures(futures, launched);
      }
    } catch (final RuntimeException e) {
      throw asRetryIfRenumbered(nodeIdSpace, e);
    }
    checkNodeIdSpace(nodeIdSpace);

    if (!grouped) {
      // The sources the view does not map were expanded and filtered already: each of their rows is one path
      long total = unmappedRows != null ? unmappedRows.size() : 0;
      for (final long threadTotal : threadTotals)
        total += threadTotal;
      final ResultInternal result = new ResultInternal();
      result.setProperty(countOutputName, total);
      return new IteratorResultSet(List.<Result>of(result).iterator());
    }

    // Merge thread-local maps (zero boxing — primitive long operations)
    final LongLongHashMap merged = threadMaps[0] != null ? threadMaps[0] : new LongLongHashMap();
    for (int t = 1; t < numThreads; t++)
      if (threadMaps[t] != null)
        merged.mergeFrom(threadMaps[t]);

    // The paths of the sources the view does not map: counted into the same groups when every key is a view node, into
    // groups of their own, keyed by the records, otherwise
    Map<List<RID>, Object[]> recordGroups = null;
    if (unmappedRows != null)
      for (final Result row : unmappedRows) {
        boolean mapped = true;
        final Object[] keys = new Object[groupKeyVariables.length];
        final int[] nodeIds = new int[groupKeyVariables.length];
        for (int g = 0; g < groupKeyVariables.length; g++) {
          keys[g] = row.getProperty(groupKeyVariables[g]);
          nodeIds[g] = keys[g] instanceof GAVVertex gav ? gav.getNodeId() :
              keys[g] instanceof Vertex v ? provider.getNodeId(v.getIdentity()) : -1;
          if (nodeIds[g] < 0)
            mapped = false;
        }
        if (mapped) {
          // Packed exactly as the DFS packs its keys
          long packedKey = 0;
          if (nodeIds.length == 2)
            packedKey = ((long) nodeIds[0] << 32) | (nodeIds[1] & 0xFFFFFFFFL);
          else if (nodeIds.length == 1)
            packedKey = nodeIds[0];
          merged.increment(packedKey);
          continue;
        }
        final List<RID> recordKey = new ArrayList<>(keys.length);
        for (final Object key : keys)
          recordKey.add(key instanceof Vertex v ? v.getIdentity() : null);
        if (recordGroups == null)
          recordGroups = new HashMap<>();
        final Object[] group = recordGroups.computeIfAbsent(recordKey, k -> {
          final Object[] entry = new Object[keys.length + 1];
          System.arraycopy(keys, 0, entry, 0, keys.length);
          entry[keys.length] = 0L;
          return entry;
        });
        group[keys.length] = (Long) group[keys.length] + 1;
      }

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
    if (recordGroups != null)
      for (final Object[] group : recordGroups.values()) {
        final ResultInternal result = new ResultInternal();
        for (int g = 0; g < groupKeyOutputNames.length; g++)
          result.setProperty(groupKeyOutputNames[g], group[g]);
        result.setProperty(countOutputName, group[groupKeyOutputNames.length]);
        results.add(result);
      }

    return new IteratorResultSet(results.iterator());
  }

  /**
   * Refuses what a walk read when the provider numbered its nodes afresh since the walk started: the walk's node IDs,
   * read in the old numbering, would be answered by the new one as other records, and its labels, RIDs and counts would
   * be wrong without any error. The query is safe to run again, on the new numbering.
   */
  private void checkNodeIdSpace(final Object nodeIdSpace) {
    if (provider.getNodeIdSpace() != nodeIdSpace)
      throw renumbered(null);
  }

  /**
   * A walk that failed while the provider numbered its nodes afresh failed on IDs of the old numbering read in the new one
   * (an ID past the new mapping, say): it is the rebuild the caller has to hear about, as the retry it calls for, not
   * whatever the stale ID broke.
   */
  private RuntimeException asRetryIfRenumbered(final Object nodeIdSpace, final RuntimeException failure) {
    return failure instanceof NeedRetryException || provider.getNodeIdSpace() == nodeIdSpace ? failure : renumbered(failure);
  }

  private NeedRetryException renumbered(final Throwable cause) {
    final String message = "Graph Analytical View '" + provider.getName() + "' was rebuilt while the query walked it: run the query again";
    return cause != null ? new NeedRetryException(message, cause) : new NeedRetryException(message);
  }

  /**
   * Expands the sources the view does not map through a fresh copy of the hops this chain replaced, which walk their
   * edge records, and applies the filter pushed into the chain. The copies enforce relationship uniqueness with the
   * same labels the fused walk compares (issue #8394).
   */
  private List<Result> expandUnmappedSources(final List<Result> sources, final CommandContext context) {
    PhysicalOperator chain = new RowsOperator(sources);
    for (final GAVExpandAll hop : unfusedHops) {
      final GAVExpandAll copy = new GAVExpandAll(chain, hop.getProvider(), hop.getSourceVariable(),
          hop.getTargetVariable(), hop.getDirection(), hop.getEdgeTypes(), hop.getEstimatedCost(),
          hop.getEstimatedCardinality());
      copy.setTargetLabel(hop.getTargetLabel());
      // Deferred target loading is left off on purpose: it only saves record loads on the fused path, and these rows
      // are the few sources the view does not map, whose targets are GAVVertex proxies loaded lazily anyway
      if (hop.getEdgeTrackingVar() != null)
        copy.setEdgeTracking(hop.getEdgeTrackingVar(), hop.getSameClausePrecedingRelVars());
      chain = copy;
    }
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet expanded = chain.execute(context, -1)) {
      while (expanded.hasNext()) {
        final Result row = expanded.next();
        if (pushedFilter == null || Boolean.TRUE.equals(pushedFilter.evaluate(row, context)))
          rows.add(row);
      }
    }
    return rows;
  }

  /**
   * Aggregating DFS for a chunk of sources. Pure int operations — zero object allocation
   * during traversal. Counts are accumulated in the thread-local map, or in {@code totals[slot]} with no grouping key.
   */
  private void aggregateChunk(final int[] sourceNodeIds, final int start, final int end,
      final NeighborView[] hopViews, final TrackedTypes trackedTypes, final int chainLength, final int[] groupKeySlots,
      final Database db, final CommandContext context, final WorkGuard guard, final LongLongHashMap counts,
      final long[] totals, final int slot) {

    // Pre-allocate reusable filter objects ONCE per thread (zero per-path allocation)
    final Object[] filterValues;
    final GAVResult filterResult;
    if (pathFilter != null) {
      final String[] filterNames = buildOutputNames();
      filterValues = new Object[filterNames.length];
      filterResult = new GAVResult(filterNames, filterValues);
    } else {
      filterValues = null;
      filterResult = null;
    }

    final long[] total = new long[1];
    new ChainWalker(sourceNodeIds, start, end, hopViews, trackedTypes, chainLength, guard, db, context)
        .walk(Long.MAX_VALUE, stackNodeId -> {
          // Evaluate pushed filter using reusable GAVResult — only nodeId updated per path
          if (filterResult != null) {
            int filterSlot = 0;
            if (materializeVariable[0])
              filterValues[filterSlot++] = makeReference(stackNodeId[0], db);
            for (int i = 0; i < hopTargetVariables.length; i++)
              if (hopTargetVariables[i] != null && materializeVariable[i + 1])
                filterValues[filterSlot++] = makeReference(stackNodeId[i + 1], db);
            if (!Boolean.TRUE.equals(pathFilter.evaluate(filterResult, context)))
              return;
          }

          if (counts == null) {
            ++total[0];
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
    totals[slot] = total[0];
  }

  /** Receives every complete path: {@code stackNodeId[0]} is the source, {@code stackNodeId[i + 1]} the target of hop i. */
  @FunctionalInterface
  private interface PathVisitor {
    void visit(int[] stackNodeId);
  }

  /**
   * The DFS both the row-producing and the aggregating paths run, over one contiguous slice of the sources. It can stop
   * after a number of paths and resume where it stopped, which is what lets the row path hand its rows out in bounded
   * rounds (issue #9738). Each walker has its own stack, so walkers can run in parallel with no shared mutable state; a
   * walker is only ever run by one thread at a time.
   */
  private final class ChainWalker {
    private final int[]            sourceNodeIds;
    private final int              end;
    private final NeighborView[]   hopViews;
    private final int              chainLength;
    private final WorkGuard        guard;
    // Per-walker DFS stack (allocated once, reused across all sources of the slice)
    private final int[]            stackNodeId;
    private final int[]            stackCursor;
    private final int[]            stackEnd;
    private final int[][]          fallbackNeighbors;
    // Undirected untracked hops: how many entries equal to the hop's own vertex were met, to take each self-loop once
    private final int[]            selfLoopEntries;
    private final TrackedAdjacency tracked;
    // The conjuncts decided on a partial path, null when there are none (issue #9938)
    private final PartialPathFilter partialFilter;
    private       int              next;
    private       int              depth = -1;
    // One supernode's DFS can dwarf the whole chunk, so the per-source check alone would leave the abort
    // latency proportional to the largest fan-out. The counter climbs across sources on purpose: throttling
    // per DFS step is what bounds the latency by a fixed amount of work instead (issue #6266).
    private       int              steps;

    private ChainWalker(final int[] sourceNodeIds, final int start, final int end, final NeighborView[] hopViews,
        final TrackedTypes trackedTypes, final int chainLength, final WorkGuard guard, final Database database,
        final CommandContext context) {
      this.sourceNodeIds = sourceNodeIds;
      this.next = start;
      this.end = end;
      this.hopViews = hopViews;
      this.chainLength = chainLength;
      this.guard = guard;
      this.stackNodeId = new int[chainLength + 1];
      this.stackCursor = new int[chainLength];
      this.stackEnd = new int[chainLength];
      this.fallbackNeighbors = new int[chainLength][];
      this.selfLoopEntries = new int[chainLength];
      this.tracked = trackedTypes != null ? new TrackedAdjacency(trackedTypes, chainLength) : null;
      this.partialFilter = filterAtDepth != null ? new PartialPathFilter(database, context) : null;
    }

    /**
     * Walks until {@code maxPaths} paths were visited or the slice is exhausted.
     *
     * @return true once every path of the slice was visited
     */
    private boolean walk(final long maxPaths, final PathVisitor visitor) {
      long paths = 0;
      while (true) {
        if (depth < 0) {
          if (next >= end)
            return true;
          if (paths >= maxPaths)
            return false;
          guard.check();
          final int sourceNodeId = sourceNodeIds[next++];
          stackNodeId[0] = sourceNodeId;
          if (partialFilter != null && !partialFilter.accepts(stackNodeId, 0))
            continue;
          depth = 0;
          initHop(hopViews, fallbackNeighbors, tracked, stackCursor, stackEnd, selfLoopEntries, 0, sourceNodeId);
          continue;
        }

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

        // Target label filter: read from the view's bucket table, building no RID per neighbour (issue #9738)
        if (hopTargetBucketIds[depth] != null && !matchesBuckets(provider.getBucketId(neighborId), hopTargetBucketIds[depth])) {
          stackCursor[depth]++;
          continue;
        }

        stackNodeId[depth + 1] = neighborId;

        if (depth == chainLength - 1) {
          visitor.visit(stackNodeId);
          stackCursor[depth]++;
          if (++paths >= maxPaths)
            return false;
        } else if (partialFilter != null && !partialFilter.accepts(stackNodeId, depth + 1)) {
          // The partial path already fails a conjunct: none of its extensions is walked
          stackCursor[depth]++;
        } else {
          depth++;
          initHop(hopViews, fallbackNeighbors, tracked, stackCursor, stackEnd, selfLoopEntries, depth, neighborId);
        }
      }
    }
  }

  /**
   * Evaluates the conjuncts of the pushed filter a partial path can decide (issue #9938). One per walker, so it reuses its
   * row: only the slots of the variables bound so far are written, and the conjuncts of a depth read no deeper one. A slot
   * keeps its vertex while the node under it stays the same, so the siblings of a partial path allocate one vertex each.
   */
  private final class PartialPathFilter {
    private final Database       database;
    private final CommandContext context;
    private final Object[]       values;
    private final GAVResult      row;
    // per stack position: its slot in the row (-1 when the variable is not materialized), and the node the slot holds
    private final int[]          slotOf;
    private final int[]          nodeInSlot;

    private PartialPathFilter(final Database database, final CommandContext context) {
      this.database = database;
      this.context = context;
      final String[] names = buildOutputNames();
      this.values = new Object[names.length];
      this.row = new GAVResult(names, values);
      this.slotOf = new int[hopTargetVariables.length + 1];
      int slot = 0;
      slotOf[0] = materializeVariable[0] ? slot++ : -1;
      for (int i = 0; i < hopTargetVariables.length; i++)
        slotOf[i + 1] = hopTargetVariables[i] != null && materializeVariable[i + 1] ? slot++ : -1;
      this.nodeInSlot = new int[slotOf.length];
      Arrays.fill(nodeInSlot, -1);
    }

    /** Whether the path {@code stackNodeId[0..depth]} passes the conjuncts decided at {@code depth}. */
    private boolean accepts(final int[] stackNodeId, final int depth) {
      final BooleanExpression filter = filterAtDepth[depth];
      if (filter == null)
        return true;
      for (int i = 0; i <= depth; i++) {
        final int slot = slotOf[i];
        if (slot >= 0 && nodeInSlot[i] != stackNodeId[i]) {
          values[slot] = makeReference(stackNodeId[i], database);
          nodeInSlot[i] = stackNodeId[i];
        }
      }
      return Boolean.TRUE.equals(filter.evaluate(row, context));
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

  /**
   * The edge types the tracked hops walk, numbered: see {@link GAVEdgeRef#trackedEdgeTypes}. {@code views} holds, per
   * type and orientation ({@code typeId * 2 + 1} for outgoing), the provider's zero-copy slice view, or null where the
   * provider has none (pending changes, a type without a slice): the walk then asks for the neighbour ids.
   */
  private record TrackedTypes(String[] names, int[][] hopTypeIds, NeighborView[] views) {
  }

  private TrackedTypes resolveTrackedTypes(final Database database) {
    final Map<String, Integer> ids = new HashMap<>();
    final List<String> names = new ArrayList<>();
    final int[][] hopTypeIds = new int[hopDirections.length][];
    for (int i = 0; i < hopDirections.length; i++) {
      if (!hopTracked[i])
        continue;
      final String[] types = GAVEdgeRef.trackedEdgeTypes(database, hopEdgeTypes[i]);
      hopTypeIds[i] = new int[types.length];
      for (int t = 0; t < types.length; t++) {
        final String type = types[t];
        hopTypeIds[i][t] = ids.computeIfAbsent(type, k -> {
          names.add(k);
          return names.size() - 1;
        });
      }
    }
    final NeighborView[] views = new NeighborView[names.size() * 2];
    for (int t = 0; t < names.size(); t++) {
      views[t * 2] = provider.getNeighborView(Vertex.DIRECTION.IN, names.get(t));
      views[t * 2 + 1] = provider.getNeighborView(Vertex.DIRECTION.OUT, names.get(t));
    }
    return new TrackedTypes(names.toArray(new String[0]), hopTypeIds, views);
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
    // Whether every (type, orientation) segment loaded for the hop is sorted, which bounds a rank by its run
    private final boolean[]    sorted;
    // The rank of the entry an earlier hop stands on, kept while it stands there: every later candidate with the same
    // endpoints compares against it, and it does not change until that hop's cursor moves
    private final int[]        rankedIndex;
    private final int[]        rankedValue;

    private TrackedAdjacency(final TrackedTypes types, final int chainLength) {
      this.types = types;
      this.neighbors = new int[chainLength][];
      this.meta = new int[chainLength][];
      this.sorted = new boolean[chainLength];
      this.rankedIndex = new int[chainLength];
      this.rankedValue = new int[chainLength];
      for (int i = 0; i < chainLength; i++)
        if (hopTracked[i]) {
          neighbors[i] = new int[16];
          meta[i] = new int[16];
        }
    }

    /** Loads the relationships of {@code nodeId} for hop {@code depth} and returns how many there are. */
    private int fill(final int depth, final int nodeId) {
      int size = 0;
      sorted[depth] = true;
      rankedIndex[depth] = -1;
      final Vertex.DIRECTION direction = hopDirections[depth];
      for (final int typeId : types.hopTypeIds()[depth]) {
        final String type = types.names()[typeId];
        if (direction != Vertex.DIRECTION.IN)
          size = appendSlice(depth, size, nodeId, typeId, Vertex.DIRECTION.OUT, type, -1);
        // Undirected: a self-loop is in the outgoing list already
        if (direction != Vertex.DIRECTION.OUT)
          size = appendSlice(depth, size, nodeId, typeId, Vertex.DIRECTION.IN, type,
              direction == Vertex.DIRECTION.BOTH ? nodeId : -1);
      }
      return size;
    }

    /**
     * Appends one (type, orientation) slice of {@code nodeId}: read in place from the provider's slice view when it
     * has one, as the untracked hops do, so a tracked hop allocates nothing per vertex; from its neighbour ids otherwise.
     */
    private int appendSlice(final int depth, final int size, final int nodeId, final int typeId,
        final Vertex.DIRECTION orientation, final String type, final int skip) {
      final int entryMeta = orientation == Vertex.DIRECTION.OUT ? (typeId << 1) | 1 : typeId << 1;
      final NeighborView view = types.views()[orientation == Vertex.DIRECTION.OUT ? typeId * 2 + 1 : typeId * 2];
      if (view != null && nodeId < view.nodeCount())
        return append(depth, size, view.neighbors(), view.offset(nodeId), view.offsetEnd(nodeId), entryMeta, skip);
      final int[] slice = provider.getNeighborIds(nodeId, orientation, type);
      return slice == null ? size : append(depth, size, slice, 0, slice.length, entryMeta, skip);
    }

    private int append(final int depth, int size, final int[] source, final int from, final int to,
        final int entryMeta, final int skip) {
      final int length = to - from;
      if (length <= 0)
        return size;
      if (size + length > neighbors[depth].length) {
        final int capacity = Math.max(neighbors[depth].length * 2, size + length);
        neighbors[depth] = Arrays.copyOf(neighbors[depth], capacity);
        meta[depth] = Arrays.copyOf(meta[depth], capacity);
      }
      final int[] n = neighbors[depth];
      final int[] m = meta[depth];
      int previous = Integer.MIN_VALUE;
      for (int i = from; i < to; i++) {
        final int neighbor = source[i];
        // Checked in the same pass: a provider does not promise sorted slices (see GAVEdgeRef.isSorted)
        if (neighbor < previous)
          sorted[depth] = false;
        previous = neighbor;
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
        if (rankedIndex[j] != jIndex) {
          rankedValue[j] = rank(j, jIndex);
          rankedIndex[j] = jIndex;
        }
        if (rankedValue[j] == rank)
          return true;
      }
      return false;
    }

    /**
     * The rank of entry {@code index} among the equal entries (same neighbour, type and orientation) before it. Each
     * (type, orientation) segment is appended whole, so in sorted segments those entries are the run ending at
     * {@code index}.
     */
    private int rank(final int depth, final int index) {
      final int[] n = neighbors[depth];
      final int[] m = meta[depth];
      final int neighbor = n[index];
      final int entryMeta = m[index];
      int rank = 0;
      if (sorted[depth]) {
        for (int i = index - 1; i >= 0 && n[i] == neighbor && m[i] == entryMeta; i--)
          ++rank;
        return rank;
      }
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
    if (pathFilter != null)
      if (!Boolean.TRUE.equals(pathFilter.evaluate(result, context)))
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
    if (filterAtDepth != null) {
      for (int d = 0; d < filterAtDepth.length; d++)
        if (filterAtDepth[d] != null)
          sb.append(", filter on (").append(d == 0 ? sourceVariable : hopTargetVariables[d - 1]).append(")=")
              .append(filterAtDepth[d].getText());
    } else if (pushedFilter != null)
      sb.append(", filter=").append(pushedFilter.getText());
    if (groupKeyVariables != null) {
      sb.append(", fused count(*) AS ").append(countOutputName);
      if (groupKeyVariables.length > 0)
        sb.append(" BY ").append(String.join(", ", groupKeyVariables));
    }
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

  /** True when {@code variable} is the chain's source or the target of one of its hops: bound on every path it walks. */
  public boolean bindsVariable(final String variable) {
    if (variable == null)
      return false;
    if (variable.equals(sourceVariable))
      return true;
    for (final String target : hopTargetVariables)
      if (variable.equals(target))
        return true;
    return false;
  }
}
