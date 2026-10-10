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
package com.arcadedb.query.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.OrderBy;
import com.arcadedb.query.sql.parser.OrderByItem;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by luigidellaquila on 11/07/16.
 */
public class OrderByStep extends AbstractExecutionStep {
  private final OrderBy            orderBy;
  private       Integer            maxResults;
  private final long               timeoutMillis;
  // THE ROWS BUFFERED, UNDER THE PER-OPERATION CAP AND THE HEAP BUDGET OF ALL THE QUERIES (ISSUES #8585, #8591)
  private       OperationHeapLimit limit;

  List<Result> cachedResult = null;
  int          nextElement  = 0;

  // #8802: set when the top rows were kept in the workers of a parallel scan, for the plan printout
  private int                      parallelWorkers = 0;
  // THE PARTIAL TOP-K OF THE WORKERS OF A PARALLEL SCAN, SO A FAILURE CAN GIVE BACK WHAT THEY CHARGED
  private final Queue<PartialTopK> workerPartials  = new ConcurrentLinkedQueue<>();

  public OrderByStep(final OrderBy orderBy, final CommandContext context, final long timeoutMillis) {
    this(orderBy, null, context, timeoutMillis);
  }

  public OrderByStep(final OrderBy orderBy, final Integer maxResults, final CommandContext context, final long timeoutMillis) {
    super(context);
    this.orderBy = orderBy;
    this.maxResults = maxResults;
    if (this.maxResults != null && this.maxResults < 0) {
      this.maxResults = null;
    }
    this.timeoutMillis = timeoutMillis;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (cachedResult == null) {
      cachedResult = new ArrayList<>();
      limit = OperationHeapLimit.of(context, "ORDER BY");
      if (prev != null) {
        try {
          init(prev, context);
        } catch (final RuntimeException e) {
          releaseBuffer();
          throw e;
        }
      }
    }

    return new ResultSet() {
      private int currentBatchReturned = 0;
      private final int offset = nextElement;

      @Override
      public boolean hasNext() {
        if (currentBatchReturned >= nRecords) {
          return false;
        }
        return cachedResult.size() > nextElement;
      }

      @Override
      public Result next() {
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          if (currentBatchReturned >= nRecords) {
            throw new NoSuchElementException();
          }
          if (cachedResult.size() <= nextElement) {
            throw new NoSuchElementException();
          }
          final Result result = cachedResult.get(offset + currentBatchReturned);
          nextElement++;
          currentBatchReturned++;
          if (nextElement == cachedResult.size())
            // EVERY ROW WAS SERVED: THE BUFFER IS NOT NEEDED ANYMORE, EVEN IF THE CONSUMER KEEPS THE RESULT SET OPEN
            releaseBuffer();
          return result;
        } finally {
          if( context.isProfiling() ) {
            cost += System.nanoTime() - begin;
          }
        }
      }

      @Override
      public void close() {
        if (prev != null)
          prev.close();
      }
    };
  }

  private void init(final ExecutionStepInternal p, final CommandContext context) {
    if (maxResults != null) {
      initTopK(p, context);
      return;
    }
    final long timeoutBegin = System.currentTimeMillis();
    // The whole input is consumed before the first row is returned: see AggregateProjectionCalculationStep (issue #9680)
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    int consumed = 0;
    do {
      final ResultSet lastBatch = p.syncPull(context, DEFAULT_FETCH_RECORDS_PER_PULL);
      if (!lastBatch.hasNext())
        break;

      while (lastBatch.hasNext()) {
        guard.checkPeriodically(++consumed);
        if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis())
          sendTimeout();

        if (this.timedOut)
          break;

        final Result item = lastBatch.next();
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          cachedResult.add(item);
          limit.add(cachedResult.size(), item);
        } finally {
          if( context.isProfiling() ) {
            cost += System.nanoTime() - begin;
          }
        }
      }
      if (timedOut)
        break;
    } while (true);
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      cachedResult.sort((a, b) -> orderBy.compare(a, b, context));
    } finally {
      if( context.isProfiling() ) {
        cost += System.nanoTime() - begin;
      }
    }
  }

  /**
   * ORDER BY with a known bound (LIMIT, or SKIP + LIMIT): keeps the best {@code maxResults} rows seen in a heap, worst on
   * top. A row is compared once with the worst kept row and enters only if it sorts strictly before it, so among equal keys
   * the earlier row wins, as the stable sort-and-truncate did; the rows kept are sorted by key, then arrival, at the end.
   * Sorting the whole buffer every {@code maxResults} rows, and estimating the heap of every row kept each time, cost
   * several times the scan itself once SKIP made the bound large.
   */
  private void initTopK(final ExecutionStepInternal p, final CommandContext context) {
    parallelWorkers = 0;
    if (maxResults == 0)
      // nothing can be kept: no need to pull the input
      return;
    final long timeoutBegin = System.currentTimeMillis();
    // The whole input is consumed before the first row is returned: see AggregateProjectionCalculationStep (issue #9680)
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    int consumed = 0;
    if (topKInParallel(context, timeoutBegin))
      return;
    final Comparator<Kept> byKeyThenArrival = byKeyThenArrival(orderBy, context);
    final PriorityQueue<Kept> heap = new PriorityQueue<>(Math.min(maxResults, 1024) + 1, byKeyThenArrival.reversed());
    long arrival = 0;
    do {
      final ResultSet lastBatch = p.syncPull(context, DEFAULT_FETCH_RECORDS_PER_PULL);
      if (!lastBatch.hasNext())
        break;

      while (lastBatch.hasNext()) {
        guard.checkPeriodically(++consumed);
        if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis())
          sendTimeout();

        if (this.timedOut)
          break;

        final Result item = lastBatch.next();
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          if (heap.size() < maxResults) {
            heap.add(new Kept(item, arrival++));
            limit.add(heap.size(), item);
          } else if (orderBy.compare(item, heap.peek().row, context) < 0) {
            final Kept evicted = heap.poll();
            heap.add(new Kept(item, arrival++));
            limit.replace(evicted.row, item);
          } else
            arrival++;
        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }
      if (timedOut)
        break;
    } while (true);

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final List<Kept> kept = new ArrayList<>(heap);
      kept.sort(byKeyThenArrival);
      cachedResult = new ArrayList<>(kept.size());
      for (final Kept k : kept)
        cachedResult.add(k.row);
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /**
   * #8802: keeps the best rows in the workers of a parallel scan instead of on this thread, when this step reads a
   * {@link ParallelAggregationSource} directly or through the row filters and the one projection of {@link ParallelRowPipeline}.
   * Every worker runs those filters and that projection for its part of the rows on its own copy of them and keeps its own
   * top {@code maxResults} in a heap; the heaps are merged here. Every row remembers where in the sequential scan it was, so
   * among equal keys the row the sequential scan meets first wins, as in the sequential sort.
   *
   * @return true when it ran and {@code cachedResult} holds the rows, false when this execution sorts sequentially: the input
   * is not a parallel scan, a sort key is not a plain property, or an expression is not one the engine can evaluate on
   * several threads
   */
  private boolean topKInParallel(final CommandContext context, final long timeoutBegin) {
    final ParallelRowPipeline pipeline = parallelPipeline();
    if (pipeline == null)
      return false;

    final ParallelTypeScan firstScan = pipeline.source().planParallelAggregation(context);
    if (firstScan == null)
      return false;

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final boolean[] timedOutNotified = new boolean[1];
      final Runnable onWait = () -> {
        if (!timedOutNotified[0] && timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
          timedOutNotified[0] = true;
          sendTimeout();
        }
      };

      // A ROUND TAKES THE PARTIALS OF THE PREVIOUS ONES BACK BEFORE IT CREATES ANY, SO A SOURCE SERVED IN MANY ROUNDS STILL
      // ENDS WITH NO MORE PARTIALS THAN WORKERS
      final List<PartialTopK> partials = new ArrayList<>();
      final String[] types = pipeline.types().toArray(new String[0]);
      int workers = 0;
      long units = 0;
      ParallelTypeScan scan = firstScan;
      while (scan != null) {
        final ConcurrentLinkedQueue<PartialTopK> idle = new ConcurrentLinkedQueue<>(partials);
        // POSITIONS GO ON FROM ONE ROUND TO THE NEXT, AS THE SEQUENTIAL EXECUTION MEETS THE ROWS
        final long roundPosition = units << 32;
        final List<PartialTopK> used = scan.aggregate(context, workerContext -> {
              final PartialTopK reused = idle.poll();
              if (reused != null)
                return reused;
              final PartialTopK partial = new PartialTopK(types, pipeline.copyConditions(),
                  pipeline.projection() == null ? null : pipeline.projection().copy(), orderBy.copy(), workerContext, maxResults,
                  OperationHeapLimit.of(workerContext, "ORDER BY"));
              workerPartials.add(partial);
              return partial;
            }, (partial, row, position, ignored) -> partial.accept(row, roundPosition + position), onWait);
        for (final PartialTopK partial : used)
          if (!partials.contains(partial))
            partials.add(partial);

        workers = Math.max(workers, scan.getWorkerCount());
        units += scan.getUnitCount();
        scan = pipeline.source().nextParallelAggregationRound(context);
      }
      parallelWorkers = workers;

      final List<Kept> kept = new ArrayList<>();
      for (final PartialTopK partial : partials)
        kept.addAll(partial.heap);
      kept.sort(byKeyThenArrival(orderBy, context));
      final int size = Math.min(maxResults, kept.size());
      cachedResult = new ArrayList<>(size);
      for (int i = 0; i < size; i++)
        cachedResult.add(kept.get(i).row);

      // THE WORKERS CHARGED THEIR OWN ROWS WHILE THEY SCANNED; THIS STEP HOLDS THE ONES IT KEEPS FROM HERE ON. THE WORKERS'
      // CHARGES ARE TAKEN OVER WITHOUT GIVING THEM BACK TO THE BUDGET IN BETWEEN, WHERE ANOTHER QUERY COULD TAKE THEM WHILE THE
      // ROWS ARE STILL HELD, THEN ADJUSTED TO THE ROWS KEPT, EACH ESTIMATED FOR ITSELF
      for (final PartialTopK partial : partials)
        partial.handOverHeap(limit);
      limit.rechargeAll(cachedResult);
      return true;
    } finally {
      // A NO-OP ON SUCCESS; ON A FAILURE IT GIVES BACK WHAT THE WORKERS CHARGED, THE ONES STILL STOPPING INCLUDED
      for (final PartialTopK partial : workerPartials)
        partial.releaseHeap();
      workerPartials.clear();
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /** The pipeline this step can run in the workers of, or {@code null} when it cannot. */
  private ParallelRowPipeline parallelPipeline() {
    if (orderBy.getItems() == null || orderBy.getItems().isEmpty())
      return null;
    // A KEY THAT IS NOT A PLAIN PROPERTY OR RECORD ATTRIBUTE IS AN EXPRESSION THE PLANNER MOVES INTO THE PROJECTION; ANYTHING
    // STILL HERE IS LEFT TO THE SEQUENTIAL SORT
    for (final OrderByItem item : orderBy.getItems())
      if (item.expression != null || item.getModifier() != null || item.getDirectionParameter() != null)
        return null;

    final ParallelRowPipeline pipeline = ParallelRowPipeline.of(prev);
    if (pipeline == null || !SqlAstInspector.isParallelSafe(pipeline.projection(), pipeline.conditions()))
      return null;
    return pipeline;
  }

  /** Orders kept rows by the sort key, then by where the sequential scan met them. */
  private static Comparator<Kept> byKeyThenArrival(final OrderBy orderBy, final CommandContext context) {
    return (a, b) -> {
      final int c = orderBy.compare(a.row, b.row, context);
      return c != 0 ? c : Long.compare(a.arrival, b.arrival);
    };
  }

  /** A row kept by a top-K heap, with its position in the sequential scan: the tie-break that keeps the sort stable (#8802). */
  private record Kept(Result row, long arrival) {
  }

  /** One worker's top {@code maxResults}, on the worker's own copies of the expressions it evaluates. */
  private static final class PartialTopK {
    private final String[]            types;
    private final WhereClause[]       conditions;
    private final Projection          projection;
    private final OrderBy             orderBy;
    private final int                 maxResults;
    private final OperationHeapLimit  heapLimit;
    private final PriorityQueue<Kept> heap;
    private final CommandContext      context;
    // GUARDED BY this: SET BY THE CALLER, WHICH RELEASES THE HEAP OF A WORKER A FAILURE CANCELLED WHILE IT MAY STILL RUN
    private       boolean             heapReleased;

    PartialTopK(final String[] types, final WhereClause[] conditions, final Projection projection, final OrderBy orderBy,
        final CommandContext context, final int maxResults, final OperationHeapLimit heapLimit) {
      this.types = types;
      this.conditions = conditions;
      this.projection = projection;
      this.orderBy = orderBy;
      this.context = context;
      this.maxResults = maxResults;
      this.heapLimit = heapLimit;
      this.heap = new PriorityQueue<>(Math.min(maxResults, 1024) + 1, byKeyThenArrival(orderBy, context).reversed());
    }

    /**
     * A worker meets its rows in ascending position: units are claimed from a shared counter in order, and a round's
     * positions start above the previous one's. A row whose key ties the worst kept one therefore never replaces it, which
     * is what keeps the earlier row, as the sequential sort does. A scan that claimed units in another order would break the
     * ties here without the merge noticing.
     */
    void accept(final Result row, final long position) {
      if (!ParallelRowPipeline.matches(types, conditions, row, context))
        return;
      final Result next = projection != null ? projection.calculateSingle(context, row) : row;
      if (heap.size() < maxResults) {
        heap.add(new Kept(next, position));
        // THE MONITOR IS UNCONTENDED BUT FOR A CANCELLED WORKER, AND TAKEN ONLY FOR A ROW THAT ENTERS THE HEAP
        synchronized (this) {
          if (!heapReleased)
            heapLimit.add(heap.size(), next);
        }
      } else if (orderBy.compare(next, heap.peek().row, context) < 0) {
        final Kept evicted = heap.poll();
        heap.add(new Kept(next, position));
        synchronized (this) {
          if (!heapReleased)
            heapLimit.replace(evicted.row, next);
        }
      }
    }

    /** Gives back what this worker charged for its rows, and stops it charging more. */
    synchronized void releaseHeap() {
      heapReleased = true;
      heapLimit.release();
    }

    /** Hands what this worker charged over to {@code holder}, which holds the rows now, and stops charging. */
    synchronized void handOverHeap(final OperationHeapLimit holder) {
      heapReleased = true;
      if (!holder.transferFrom(heapLimit))
        heapLimit.release();
    }
  }

  private void releaseBuffer() {
    cachedResult = Collections.emptyList();
    if (limit != null)
      limit.release();
  }

  @Override
  public void close() {
    if (cachedResult != null)
      releaseBuffer();
    super.close();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ " + orderBy;
    if( context.isProfiling() ) {
      result += " (" + getCostFormatted() + ")";
    }
    result += maxResults != null ?
        "\n  (buffer size: " + maxResults + (parallelWorkers > 0 ? ", parallel: " + parallelWorkers + " workers" : "") + ")" :
        "";
    return result;
  }

}
