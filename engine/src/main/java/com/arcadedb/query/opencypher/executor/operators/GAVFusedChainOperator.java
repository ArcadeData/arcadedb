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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.NoSuchElementException;
import java.util.Set;

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

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final ResultSet inputResults = child.execute(context, nRecords);
    final Database db = context.getDatabase();
    final int chainLength = hopDirections.length;

    // Pre-compute the output variable names array (shared across all rows — zero per-row allocation)
    final String[] outputNames = buildOutputNames();

    // Pre-acquire NeighborViews for zero-allocation traversal (one per hop, shared across all vertices)
    final NeighborView[] hopViews = new NeighborView[chainLength];
    for (int i = 0; i < chainLength; i++)
      hopViews[i] = provider.getNeighborView(hopDirections[i], hopEdgeTypes[i]);

    // Collect all source nodeIds into a primitive int[] for parallel partitioning (zero boxing)
    int[] sourceNodeIdsBuf = new int[1024];
    int sourceCount = 0;
    while (inputResults.hasNext()) {
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
          sourceNodeIdsBuf = java.util.Arrays.copyOf(sourceNodeIdsBuf, sourceNodeIdsBuf.length * 2);
        sourceNodeIdsBuf[sourceCount++] = nodeId;
      }
    }
    inputResults.close();

    final int[] sourceNodeIds = sourceNodeIdsBuf;
    final int totalSources = sourceCount; // effectively final for lambda capture
    final int parallelism = Runtime.getRuntime().availableProcessors();
    final int chunkSize = (totalSources + parallelism - 1) / parallelism;

    // Parallel DFS: each thread processes a chunk of source vertices with its own stack
    @SuppressWarnings("unchecked")
    final List<Result>[] threadResults = new List[Math.min(parallelism, Math.max(1, (totalSources + chunkSize - 1) / chunkSize))];
    final AtomicReference<Throwable> firstError = new AtomicReference<>();

    final int threadCount;
    if (totalSources < 8192) {
      // Below threshold: single-threaded
      threadResults[0] = new ArrayList<>();
      traverseChunk(sourceNodeIds, 0, totalSources, hopViews, chainLength, outputNames, db, context, threadResults[0]);
      threadCount = 1;
    } else {
      // Parallel execution
      final Thread[] threads = new Thread[threadResults.length];
      int launched = 0;
      for (int t = 0; t < threadResults.length; t++) {
        final int start = t * chunkSize;
        final int end = Math.min(start + chunkSize, totalSources);
        if (start >= totalSources)
          break;
        threadResults[t] = new ArrayList<>();
        final int threadIdx = t;
        threads[t] = new Thread(() -> {
          try {
            traverseChunk(sourceNodeIds, start, end, hopViews, chainLength, outputNames, db, context, threadResults[threadIdx]);
          } catch (final Throwable e) {
            firstError.compareAndSet(null, e);
          }
        });
        threads[t].setDaemon(true);
        threads[t].setName("gav-chain-" + t);
        threads[t].start();
        launched++;
      }
      // Wait for all threads
      for (int t = 0; t < launched; t++) {
        try {
          threads[t].join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      if (firstError.get() != null)
        throw new RuntimeException("Parallel GAV traversal failed", (Exception) firstError.get());
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
   * Traverses a chunk of source vertices through the multi-hop chain.
   * Each call has its own DFS stack — safe for parallel execution with no shared mutable state.
   */
  private void traverseChunk(final int[] sourceNodeIds, final int start, final int end,
      final NeighborView[] hopViews, final int chainLength, final String[] outputNames,
      final Database db, final CommandContext context, final List<Result> output) {

    // Per-thread DFS stack (allocated once, reused across all sources in this chunk)
    final int[] stackNodeId = new int[chainLength + 1];
    final int[] stackCursor = new int[chainLength];
    final int[] stackEnd = new int[chainLength];
    final int[][] fallbackNeighbors = new int[chainLength][];

    for (int s = start; s < end; s++) {
      final int sourceNodeId = sourceNodeIds[s];
      stackNodeId[0] = sourceNodeId;
      initHop(hopViews, fallbackNeighbors, stackCursor, stackEnd, 0, sourceNodeId);
      int depth = 0;

      while (depth >= 0) {
        if (stackCursor[depth] >= stackEnd[depth]) {
          depth--;
          if (depth >= 0)
            stackCursor[depth]++;
          continue;
        }

        final int neighborId;
        final NeighborView view = hopViews[depth];
        if (view != null)
          neighborId = view.neighbors()[stackCursor[depth]];
        else
          neighborId = fallbackNeighbors[depth][stackCursor[depth]];

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
          // Emit result
          emitResult(stackNodeId, outputNames, db, context, output);
          stackCursor[depth]++;
        } else {
          depth++;
          initHop(hopViews, fallbackNeighbors, stackCursor, stackEnd, depth, neighborId);
        }
      }
    }
  }

  private void initHop(final NeighborView[] hopViews, final int[][] fallbackNeighbors,
      final int[] stackCursor, final int[] stackEnd, final int depth, final int nodeId) {
    final NeighborView view = hopViews[depth];
    if (view != null) {
      stackCursor[depth] = view.offset(nodeId);
      stackEnd[depth] = view.offsetEnd(nodeId);
    } else {
      final int[] nbrs = provider.getNeighborIds(nodeId, hopDirections[depth], hopEdgeTypes[depth]);
      fallbackNeighbors[depth] = nbrs;
      stackCursor[depth] = 0;
      stackEnd[depth] = nbrs.length;
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
   */
  private String[] buildOutputNames() {
    final List<String> names = new ArrayList<>();
    if (materializeVariable[0])
      names.add(sourceVariable);
    // When source is not materialized, we pass through input properties — those names come at runtime
    // For now, add source variable name as placeholder
    else
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
