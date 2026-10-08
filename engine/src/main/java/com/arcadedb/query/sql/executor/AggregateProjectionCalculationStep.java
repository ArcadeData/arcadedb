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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.AggregateRowEvaluator.Group;
import com.arcadedb.query.sql.executor.AggregateRowEvaluator.GroupByKey;
import com.arcadedb.query.sql.parser.GroupBy;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by luigidellaquila on 12/07/16.
 */
public class AggregateProjectionCalculationStep extends ProjectionCalculationStep {
  // #8523: below this many groups across the partial aggregations, merging them on this thread is cheaper than handing
  // the merge to the pool
  private static final int PARALLEL_MERGE_MIN_GROUPS = 16_384;

  // #8523: set when the aggregation ran in the workers of a parallel scan, for the plan printout
  private int parallelWorkers = 0;
  private int parallelUnits   = 0;

  private final GroupBy                   groupBy;
  private final long                      timeoutMillis;
  private final long                      limit;
  // THE GROUPS HELD, UNDER THE PER-OPERATION CAP AND THE HEAP BUDGET OF ALL THE QUERIES (ISSUES #8585, #8591)
  private final OperationHeapLimit        groupsLimit;
  // #8591: THE PARTIAL AGGREGATIONS OF THE WORKERS OF A PARALLEL SCAN, SO A FAILURE CAN GIVE BACK WHAT THEY CHARGED
  private final Queue<PartialAggregation> workerPartials = new ConcurrentLinkedQueue<>();

  // #9402: A WORKER THAT HOLDS MORE THAN THIS MANY CHARGED BYTES FOLDS ITS GROUPS INTO sharedGroups AND STARTS OVER, SO THE
  // MEMORY OF A GROUP BY WITH MANY GROUPS IS ABOUT ONE COPY OF THE GROUPS PLUS ONE BOUNDED PARTIAL PER WORKER, NOT ONE
  // COPY PER WORKER (EVERY WORKER MEETS MOST KEYS WHEN THE ROWS OF A KEY ARE SPREAD ALL OVER THE TYPE). 0 = NEVER FLUSH
  private static final long MIN_WORKER_FLUSH_BYTES = 1024L * 1024;
  private volatile long                workerFlushBytes = 0L;
  private final    Object              sharedGroupsLock = new Object();
  private volatile PartialAggregation  sharedGroups;
  private final    AtomicInteger       sharedGroupCount = new AtomicInteger();

  //the key is the GROUP BY key, the value is the (partially) aggregated group
  private final Map<GroupByKey, Group> aggregateResults = new LinkedHashMap<>();
  private       List<ResultInternal>   finalResults     = null;

  private int nextItem = 0;


  public AggregateProjectionCalculationStep(final Projection projection, final GroupBy groupBy, final long limit,
      final CommandContext context,
      final long timeoutMillis) {
    super(projection, context);
    this.groupBy = groupBy;
    this.timeoutMillis = timeoutMillis;
    this.limit = limit;

    // Memory optimization: Enforce memory limits for GROUP BY operations
    this.groupsLimit = OperationHeapLimit.of(context, "groups", "GROUP BY");
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) {
    if (finalResults == null) {
      try {
        executeAggregation(context, nRecords);
      } catch (final RuntimeException e) {
        releaseGroups();
        throw e;
      }
    }

    return new ResultSet() {
      int localNext = 0;

      @Override
      public boolean hasNext() {
        return localNext < nRecords && nextItem < finalResults.size();
      }

      @Override
      public Result next() {
        if (localNext >= nRecords || nextItem >= finalResults.size()) {
          throw new NoSuchElementException();
        }
        final Result result = finalResults.get(nextItem);
        nextItem++;
        localNext++;
        if (nextItem == finalResults.size())
          // EVERY GROUP WAS SERVED: THEY ARE NOT NEEDED ANYMORE, EVEN IF THE CONSUMER KEEPS THE RESULT SET OPEN
          releaseGroups();
        return result;
      }
    };
  }

  private void executeAggregation(final CommandContext context, final int nRecords) {
    final long timeoutBegin = System.currentTimeMillis();

    final ExecutionStepInternal prevStep = checkForPrevious(
        "Cannot execute an aggregation or a GROUP BY without a previous result");

    finalResults = aggregateInParallel(context, timeoutBegin);
    if (finalResults == null) {
      // #9496: THE PROJECTION COMPUTING THE AGGREGATES' ARGUMENTS IS EVALUATED BY THE EVALUATOR, WHICH MAKES ITS ROW ONLY
      // WHEN AN EXPRESSION NEEDS IT, INSTEAD OF BY ITS OWN STEP, WHICH MADE ONE FOR EVERY ROW
      ExecutionStepInternal source = prevStep;
      Projection preProjection = null;
      // THE EXACT CLASS: A SUBCLASS OF THE PROJECTION STEP MAY DO MORE PER ROW, AND IS PULLED AS ITS OWN STEP
      if (prevStep.getClass() == ProjectionCalculationStep.class && ((ProjectionCalculationStep) prevStep).prev != null) {
        preProjection = ((ProjectionCalculationStep) prevStep).projection;
        source = ((ProjectionCalculationStep) prevStep).prev;
      }
      final AggregateRowEvaluator evaluator = new AggregateRowEvaluator(preProjection, projection, groupBy, true, null, context);
      final int groupOverhead = groupOverheadBytes(projection);

      ResultSet lastRs = source.syncPull(context, nRecords);
      while (lastRs.hasNext()) {
        if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
          sendTimeout();
        }
        aggregate(evaluator, lastRs.next(), context, groupOverhead);
        if (!lastRs.hasNext()) {
          lastRs = source.syncPull(context, nRecords);
        }
      }
      finalResults = new ArrayList<>(aggregateResults.size());
      for (final Group group : aggregateResults.values()) {
        if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
          sendTimeout();
        }
        finalResults.add(evaluator.toResult(group));
      }
      aggregateResults.clear();
    }
  }

  private void aggregate(final AggregateRowEvaluator evaluator, final Result next, final CommandContext context,
      final int groupOverhead) {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      evaluator.begin(next);
      final GroupByKey key = evaluator.evaluate(context);
      Group group = aggregateResults.get(key);
      if (group == null) {
        // Query LIMIT optimization: stop processing once we have enough groups
        if (limit > 0 && aggregateResults.size() >= limit)
          return;

        // Memory safety: enforce memory limit for GROUP BY operations
        groupsLimit.check(aggregateResults.size() + 1L, aggregateResults::clear);

        // AN AGGREGATE THAT KEEPS EVERY VALUE (list(), percentile()) CHARGES THEM TO THE OPERATION OF THE GROUPS
        group = evaluator.newGroup(key, 0L, context, groupsLimit);
        aggregateResults.put(group, group);
        groupsLimit.chargeElement(group.keyValues, groupOverhead);
      }

      evaluator.accumulate(group, context);

      // NOTE: we must NOT clear the element reference of the input Result here (issue #4590).
      // Doing so is a destructive side effect on a row we do not own exclusively: when the same
      // Result instance is referenced elsewhere (e.g. a materialized LET variable, a shared
      // ResultSet replayed via reset(), or a parallel sub-plan) later reads of getElement()
      // would silently return null. The local "next" reference is released at the end of each
      // loop iteration anyway, so the previous micro-optimization provided no real GC benefit.
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /** Refuses one group more when {@code groups} already reach the limit of groups one GROUP BY may hold in heap. */
  private void checkGroupCount(final long groups) {
    groupsLimit.check(groups + 1);
  }

  /**
   * What a group holds besides the values of its key, which are charged on their own: its key and its entry in the map
   * of the groups, the row of its non-aggregate projections - most often the key values again - and one state per
   * aggregate. What an aggregate that keeps every value gathers (list(), percentile()) is charged as it grows.
   */
  private static int groupOverheadBytes(final Projection projection) {
    return HeapEstimator.HASH_ENTRY_BYTES + 24 + HeapEstimator.RESULT_BYTES
        + 64 * projection.getItems().size();
  }

  private void releaseGroups() {
    aggregateResults.clear();
    if (finalResults != null)
      finalResults = Collections.emptyList();
    groupsLimit.release();
  }

  @Override
  public void close() {
    releaseGroups();
    super.close();
  }

  /**
   * Issue #8523: aggregates in the workers of a parallel scan instead of on this thread, when this step reads a
   * {@link ParallelAggregationSource} directly or through the row filters an index fetch leaves behind it (issue #8333)
   * and the projection computing the aggregates' arguments. Every worker runs those filters, that projection, the GROUP
   * BY and the aggregation for its part of the rows on its own copy of them, building a partial group state, and the
   * partials are merged here. The groups come out exactly as the sequential aggregation lists them - in the order the
   * scan first meets them, each carrying the non-aggregate values of that first row - because every group remembers
   * where in the sequential scan it was first seen; and a LIMIT keeps the same groups.
   * <p>
   * A source too large to hold at once - an index range loaded in physical order chunk by chunk - is aggregated round
   * after round, the partials of one round carrying on into the next.
   *
   * @return the groups, or {@code null} when this execution aggregates sequentially: the input is not a parallel
   * scan, an aggregate cannot merge partials (only count, sum, avg, min and max can), or an expression is not one the
   * engine can evaluate on several threads
   */
  private List<ResultInternal> aggregateInParallel(final CommandContext context, final long timeoutBegin) {
    final ParallelInput input = parallelInput(context);
    if (input == null)
      return null;

    final ParallelTypeScan firstScan = input.source().planParallelAggregation(context);
    if (firstScan == null)
      return null;

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final boolean[] timedOutNotified = new boolean[1];
      final Runnable onWait = () -> {
        if (!timedOutNotified[0] && timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
          timedOutNotified[0] = true;
          sendTimeout();
        }
      };
      // ONE PARTITION PER WORKER, SO THE MERGE CAN RUN IN PARALLEL TOO; WITHOUT A GROUP BY THERE IS ONE GROUP TO MERGE.
      // SET BY THE FIRST ROUND FOR ALL OF THEM: A PARTIAL KEEPS ITS PARTITIONS FROM ROUND TO ROUND
      final int partitions = groupBy == null ? 1 : firstScan.getWorkerCount();
      final int groupOverhead = groupOverheadBytes(projection);

      sharedGroups = null;
      sharedGroupCount.set(0);
      // A QUARTER OF THE BUDGET SPLIT BETWEEN THE WORKERS: THE REST IS LEFT TO THE MERGED GROUPS AND THE OTHER QUERIES
      workerFlushBytes = groupBy != null && groupsLimit.isCharging() ?
          Math.max(MIN_WORKER_FLUSH_BYTES, QueryHeapBudget.getLimitBytes() / (4L * Math.max(1, firstScan.getWorkerCount()))) :
          0L;

      // EVERY PARTIAL OF EVERY ROUND. A ROUND TAKES THE PARTIALS OF THE PREVIOUS ONES BACK BEFORE IT CREATES ANY, SO A
      // SOURCE SERVED IN MANY ROUNDS STILL ENDS WITH NO MORE PARTIALS THAN WORKERS TO MERGE
      final List<PartialAggregation> partials = new ArrayList<>();
      int workers = 0;
      long units = 0;
      ParallelTypeScan scan = firstScan;
      while (scan != null) {
        final ConcurrentLinkedQueue<PartialAggregation> idle = new ConcurrentLinkedQueue<>(partials);
        // POSITIONS GO ON FROM ONE ROUND TO THE NEXT, AS THE SEQUENTIAL EXECUTION MEETS THE ROWS
        final long roundPosition = units << 32;
        final List<PartialAggregation> used = scan.aggregate(context, workerContext -> {
              final PartialAggregation reused = idle.poll();
              if (reused != null)
                return reused;
              // A PARTIAL CHARGES ITS GROUPS TO ITS OWN OPERATION, KEPT FROM ROUND TO ROUND WITH THE GROUPS IT HOLDS
              final PartialAggregation partial = input.newPartial(this, partitions,
                  OperationHeapLimit.of(workerContext, "groups", "GROUP BY"), groupOverhead, workerContext);
              workerPartials.add(partial);
              return partial;
            }, (partial, row, position, workerContext) -> partial.accept(row, roundPosition + position, workerContext),
            onWait);
        for (final PartialAggregation partial : used)
          if (!containsSame(partials, partial))
            partials.add(partial);

        workers = Math.max(workers, scan.getWorkerCount());
        units += scan.getUnitCount();
        scan = input.source().nextParallelAggregationRound(context);
      }
      parallelWorkers = workers;
      parallelUnits = (int) Math.min(Integer.MAX_VALUE, units);

      // PARTITION p OF EVERY WORKER HOLDS THE SAME KEYS, AND NO OTHER PARTITION DOES: EACH ONE IS MERGED ON ITS OWN, IN
      // PARALLEL WHEN THERE ARE ENOUGH GROUPS FOR IT TO PAY
      // #9402: THE GROUPS THE WORKERS FLUSHED ARE MERGED ALREADY: THE REMAINING PARTIALS JOIN THEM. WITHOUT A FLUSH THE FIRST
      // PARTIAL IS THE TARGET, AS IT ALWAYS WAS
      final PartialAggregation shared = sharedGroups;
      final PartialAggregation merged = shared != null ? shared : partials.getFirst();
      long partialGroups = shared != null ? sharedGroupCount.get() : 0;
      for (final PartialAggregation partial : partials)
        partialGroups += partial.groupCount;
      final List<Runnable> merges = new ArrayList<>(partitions);
      for (int p = 0; p < partitions; p++) {
        final int partition = p;
        merges.add(() -> {
          for (final PartialAggregation partial : partials)
            if (partial != merged)
              merged.mergeFrom(partial, partition);
        });
      }
      firstScan.run(merges, partialGroups >= PARALLEL_MERGE_MIN_GROUPS);

      final List<Group> groups = new ArrayList<>();
      for (final HashMap<GroupByKey, Group> partition : merged.partitions)
        groups.addAll(partition.values());
      checkGroupCount(groups.size() - 1);

      groups.sort(Comparator.comparingLong(g -> g.firstSeen));
      final int size = limit > 0 ? (int) Math.min(limit, groups.size()) : groups.size();
      // EVERY WORKER'S EVALUATOR MAKES THE SAME ROWS OF THE GROUPS: THE FIRST ONE'S. partials HOLDS THE WORKERS' OWN, NEVER
      // THE SHARED GROUPS (WHICH HAVE NO EVALUATOR), AND THE CALLER IS ALWAYS ONE OF THE WORKERS
      AggregateRowEvaluator finisher = null;
      for (final PartialAggregation partial : partials)
        if (partial.evaluator != null) {
          finisher = partial.evaluator;
          break;
        }
      if (finisher == null)
        throw new IllegalStateException("Parallel aggregation ended without the partial aggregation of any worker");
      final List<ResultInternal> result = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        onWait.run();
        result.add(finisher.toResult(groups.get(i)));
      }

      // THE WORKERS CHARGED THEIR OWN GROUPS WHILE THEY SCANNED; THIS STEP HOLDS THE GROUPS IT RETURNS FROM HERE ON: A KEY
      // SEVERAL WORKERS MET IS ONE GROUP NOW, AND THE ONES PAST A LIMIT ARE GONE. THE STEP TAKES THE WORKERS' CHARGES OVER
      // WITHOUT GIVING THEM BACK TO THE BUDGET IN BETWEEN, WHERE ANOTHER QUERY COULD TAKE THEM WHILE THE GROUPS ARE STILL
      // HELD, THEN ADJUSTS THEM TO THE GROUPS IT KEEPS, EACH ESTIMATED FOR ITSELF: A PROPORTION OF THE WORKERS' CHARGES
      // WOULD MISS A LARGE KEY ONE WORKER HELD NEXT TO SMALL ONES THE OTHERS DID
      for (final PartialAggregation partial : partials)
        partial.handOverHeap(groupsLimit);
      if (shared != null)
        shared.handOverHeap(groupsLimit);
      long kept = 0L;
      for (int i = 0; i < size; i++)
        kept += HeapEstimator.estimate(groups.get(i).keyValues) + groupOverhead;
      if (kept < groupsLimit.getChargedBytes())
        groupsLimit.release(groupsLimit.getChargedBytes() - kept);
      else
        groupsLimit.charge(kept - groupsLimit.getChargedBytes());
      return result;

    } finally {
      // A NO-OP ON SUCCESS; ON A FAILURE IT GIVES BACK WHAT THE WORKERS CHARGED, THE ONES STILL STOPPING INCLUDED
      for (final PartialAggregation partial : workerPartials)
        partial.releaseHeap();
      workerPartials.clear();
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /**
   * The partition of a group whose key hashes to {@code hash}: the top bits of the hash multiplied by the golden ratio,
   * the same in every worker. Not {@code hash % partitions}, which it was: the map of a partition picks its buckets by the
   * low bits of the same hash, so that left each map only the buckets whose low bits matched its partition - one in four
   * with four workers - and four times the collisions on a GROUP BY of many groups (issue #9496).
   */
  static int partitionOf(final int hash, final int partitions) {
    return partitions == 1 ? 0 : (int) (((hash * 0x9E3779B9) & 0xFFFFFFFFL) * partitions >>> 32);
  }

  private static boolean containsSame(final List<PartialAggregation> partials, final PartialAggregation partial) {
    for (final PartialAggregation existing : partials)
      if (existing == partial)
        return true;
    return false;
  }

  /**
   * What a parallel aggregation reads: the source, the row filters between it and this step - the type check and the
   * conditions an index fetch leaves behind it (issue #8333) - and the projection computing the aggregates' arguments.
   */
  private record ParallelInput(ParallelAggregationSource source, List<String> types, List<WhereClause> conditions,
                               Projection preProjection) {
    /** A new partial aggregation, on its own copies of every expression it evaluates. */
    PartialAggregation newPartial(final AggregateProjectionCalculationStep step, final int partitions,
        final OperationHeapLimit heapLimit, final int groupOverhead, final CommandContext workerContext) {
      final WhereClause[] workerConditions = new WhereClause[conditions.size()];
      for (int i = 0; i < workerConditions.length; i++)
        workerConditions[i] = conditions.get(i).copy();
      // ONE EVALUATOR PER WORKER, ON ITS OWN COPIES: IT HOLDS THE STATE OF THE ROW IT EVALUATES. THE WORKER'S SCAN SETS
      // $current TO EVERY ROW ALREADY
      final AggregateRowEvaluator evaluator = new AggregateRowEvaluator(preProjection == null ? null : preProjection.copy(),
          step.projection.copy(), step.groupBy == null ? null : step.groupBy.copy(), false, workerConditions, workerContext);
      return step.new PartialAggregation(types.toArray(new String[0]), workerConditions, evaluator, partitions, heapLimit,
          groupOverhead);
    }
  }

  /**
   * The input this step can aggregate in the workers of, or {@code null} when it cannot: another input, an aggregate
   * that cannot merge partials, or an expression the engine cannot evaluate on several threads.
   */
  private ParallelInput parallelInput(final CommandContext context) {
    final ParallelRowPipeline pipeline = ParallelRowPipeline.of(prev);
    if (pipeline == null)
      return null;
    final ParallelAggregationSource source = pipeline.source();
    final List<WhereClause> conditions = pipeline.conditions();
    final Projection preProjection = pipeline.projection();
    final List<String> types = pipeline.types();

    for (final ProjectionItem proj : projection.getItems())
      if (proj.isAggregate(context) && !proj.getAggregationContext(context).canMerge())
        return null;
    if (!SqlAstInspector.isParallelSafe(preProjection, projection, groupBy, conditions))
      return null;
    return new ParallelInput(source, types, conditions, preProjection);
  }

  /** Whether an execution starting now would aggregate in parallel: what an EXPLAIN shows. */
  private boolean wouldAggregateInParallel() {
    final ParallelInput input = parallelInput(context);
    return input != null && input.source().wouldRunInParallel(context);
  }

  /**
   * One worker's partial aggregation, on the worker's own copies of the expressions. Its groups are spread over
   * partitions by the hash of their key, the same way in every worker, so partition {@code p} of every worker can be
   * merged independently of the others.
   */
  private final class PartialAggregation {
    private final String[]                     types;
    private final WhereClause[]                conditions;
    // NULL FOR THE GROUPS THE WORKERS FLUSH THEIRS INTO, WHICH EVALUATE NO ROW
    private final AggregateRowEvaluator        evaluator;
    private final HashMap<GroupByKey, Group>[] partitions;
    private final OperationHeapLimit           heapLimit;
    private final int                          groupOverhead;
    private       int                          groupCount;
    // GUARDED BY this: SET BY THE CALLER, WHICH RELEASES THE HEAP OF A WORKER A FAILURE CANCELLED WHILE IT MAY STILL RUN
    private       boolean                      heapReleased;

    @SuppressWarnings("unchecked")
    PartialAggregation(final String[] types, final WhereClause[] conditions, final AggregateRowEvaluator evaluator,
        final int partitionCount, final OperationHeapLimit heapLimit, final int groupOverhead) {
      this.types = types;
      this.conditions = conditions;
      this.evaluator = evaluator;
      this.heapLimit = heapLimit;
      this.groupOverhead = groupOverhead;
      this.partitions = new HashMap[partitionCount];
      for (int i = 0; i < partitionCount; i++)
        partitions[i] = new HashMap<>();
    }

    /** Gives back what this worker charged for its groups, and stops it charging more. */
    synchronized void releaseHeap() {
      heapReleased = true;
      heapLimit.release();
    }

    /** Hands what this worker charged for its groups over to {@code holder}, which holds them now, and stops charging. */
    synchronized void handOverHeap(final OperationHeapLimit holder) {
      heapReleased = true;
      if (!holder.transferFrom(heapLimit))
        heapLimit.release();
    }

    // THE GROUP LIMIT IS CHECKED ON THIS WORKER'S GROUPS HERE AND ON THE MERGED ONES AT THE END: AN EXACT GLOBAL COUNT
    // WHILE SCANNING WOULD NEED A KEY SET SHARED BY EVERY WORKER, SO THE PEAK CAN REACH THE LIMIT TIMES THE WORKERS
    // (DOCUMENTED ON QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP)
    void accept(final Result row, final long position, final CommandContext context) {
      // THE CONDITIONS READ THE RECORD THROUGH THE EVALUATOR'S CACHE WHEN THEY CAN: A PROPERTY THEY READ IS NOT READ AGAIN
      if (!matches(evaluator.begin(row), context))
        return;
      final GroupByKey key = evaluator.evaluate(context);
      final HashMap<GroupByKey, Group> partition = partitions[partitionOf(key.hashCode(), partitions.length)];
      Group group = partition.get(key);
      boolean flush = false;
      if (group == null) {
        checkGroupCount(groupCount);
        // NO OPERATION FOR THE AGGREGATES: ONLY THE ONES THAT MERGE PARTIALS RUN IN THE WORKERS (count, sum, avg, min, max),
        // AND NONE KEEPS THE VALUES IT AGGREGATES, SO NOTHING OUTSIDE THE MONITOR BELOW EVER CHARGES THIS WORKER'S OPERATION
        group = evaluator.newGroup(key, position, context, null);
        chargeNewGroup(group.keyValues, context);
        partition.put(group, group);
        ++groupCount;
        // CHECKED WHEN A GROUP IS CREATED ONLY: THE CHARGE GROWS ONLY THEN
        flush = workerFlushBytes > 0 && heapLimit.getChargedBytes() >= workerFlushBytes;
      }
      evaluator.accumulate(group, context);
      // AFTER THE ROW IS IN: THE FLUSH HANDS THE GROUP OVER, AND THE SHARED SIDE IS NOT TOUCHED WITHOUT ITS LOCK
      if (flush)
        flushToShared(context);
    }

    /**
     * Charges a group about to be added. A refusal may come only from the duplicates this worker holds of groups the shared
     * side has already (each charged here and there at once, until a flush gives one of them back): the worker then flushes
     * what it holds, which hands its charge over and releases the duplicates, and asks again. A refusal that persists is
     * the budget really being exceeded by the groups alone, and propagates.
     */
    private void chargeNewGroup(final Object[] keyValues, final CommandContext context) {
      try {
        synchronized (this) {
          // THE MONITOR IS UNCONTENDED BUT FOR A CANCELLED WORKER: TAKEN PER NEW GROUP, NOT PER ROW
          if (!heapReleased)
            heapLimit.chargeElement(keyValues, groupOverhead);
        }
      } catch (final CommandExecutionException e) {
        if (workerFlushBytes <= 0 || groupCount == 0)
          throw e;
        flushToShared(context);
        synchronized (this) {
          if (!heapReleased) {
            heapLimit.chargeElement(keyValues, groupOverhead);
            // ASKED NOW: A CHARGE BELOW THE FORWARDING THRESHOLD WOULD PASS WITHOUT THE BUDGET EVER ANSWERING, AND THE
            // REFUSAL THIS RETRY EXISTS TO CONFIRM WOULD NEVER COME
            heapLimit.settle();
          }
        }
      }
    }

    /** Whether the row passes the filters between the source and the aggregation, as their steps would decide. */
    private boolean matches(final Result row, final CommandContext context) {
      return ParallelRowPipeline.matches(types, conditions, row, context);
    }

    /** Folds one partition of another worker's groups into this one: the aggregations merge, the earliest first row wins. */
    void mergeFrom(final PartialAggregation other, final int partition) {
      mergePartition(partitions[partition], other.partitions[partition], null);
    }

    /**
     * Folds {@code source} into {@code groups}.
     *
     * @param duplicateBytes when not null, receives in [0] the estimated bytes of the groups of {@code source} whose key
     *                       was in {@code groups} already, which the caller had charged and now gives back
     *
     * @return the number of groups of {@code source} that {@code groups} adopted
     */
    private int mergePartition(final HashMap<GroupByKey, Group> groups, final HashMap<GroupByKey, Group> source,
        final long[] duplicateBytes) {
      int adopted = 0;
      for (final Map.Entry<GroupByKey, Group> entry : source.entrySet()) {
        final Group theirs = entry.getValue();
        final Group ours = groups.putIfAbsent(entry.getKey(), theirs);
        if (ours == null) {
          ++adopted;
          continue;
        }
        if (duplicateBytes != null)
          duplicateBytes[0] += HeapEstimator.estimate(theirs.keyValues) + groupOverhead;
        ours.merge(theirs);
      }
      return adopted;
    }

    /**
     * #9402: folds the groups of this worker into the shared ones and starts over, so what the worker holds stays bounded
     * while the others still scan. The worker's charge goes to the shared groups with its groups, then the bytes of the
     * ones that merged into groups the shared side held already are given back, so the query is never charged twice for
     * the same bytes nor free of them in between.
     */
    private void flushToShared(final CommandContext context) {
      PartialAggregation target = sharedGroups;
      if (target == null)
        synchronized (sharedGroupsLock) {
          target = sharedGroups;
          if (target == null) {
            target = new PartialAggregation(types, conditions, null, partitions.length,
                OperationHeapLimit.of(context, "groups", "GROUP BY"), groupOverhead);
            workerPartials.add(target);
            sharedGroups = target;
          }
        }

      final boolean transferred;
      synchronized (this) {
        if (heapReleased)
          // CANCELLED: THE GROUPS ARE DISCARDED WITH THE QUERY, AND THE CHARGE IS GIVEN BACK BY THE CALLER
          return;
        synchronized (target.heapLimit) {
          transferred = target.heapLimit.transferFrom(heapLimit);
        }
        // NOT TRANSFERRED ONLY WHEN THE BUDGET WAS SWITCHED ON OR OFF BETWEEN THE CREATIONS OF THE TWO: THE WORKER CHARGED
        // NOTHING THE SHARED SIDE COULD TAKE OVER, SO IT HAS NOTHING TO GIVE BACK FOR THE DUPLICATES EITHER
        if (!transferred)
          heapLimit.release();
      }

      final long[] duplicateBytes = new long[1];
      int adopted = 0;
      for (int p = 0; p < partitions.length; p++) {
        final HashMap<GroupByKey, Group> groups = target.partitions[p];
        synchronized (groups) {
          adopted += mergePartition(groups, partitions[p], duplicateBytes);
        }
        partitions[p] = new HashMap<>();
      }
      groupCount = 0;
      if (transferred)
        synchronized (target.heapLimit) {
          target.heapLimit.release(duplicateBytes[0]);
          // WHAT THE WORKER HAD NOT REPORTED YET CAME ALONG WITH ITS CHARGE: THE BUDGET ANSWERS FOR IT NOW
          target.heapLimit.settle();
        }
      // THE SAME CONVENTION AS THE FINAL CHECK: checkGroupCount(n) ASKS FOR n + 1, SO PASSING THE COUNT MINUS ONE CHECKS THE
      // COUNT. A FAILURE HERE LEAVES THE STEP TO RELEASE EVERY CHARGE: THE TARGET IS IN workerPartials, AND THE WORKER'S
      // GROUPS ARE DISCARDED WITH THE QUERY
      checkGroupCount(sharedGroupCount.addAndGet(adopted) - 1);
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ CALCULATE AGGREGATE PROJECTIONS";
    if (parallelWorkers > 0)
      result += " (parallel: " + parallelWorkers + " workers, " + parallelUnits + " units)";
    else if (finalResults == null && wouldAggregateInParallel())
      result += " (parallel)";
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";

    result += "\n" + spaces + "      " + projection.toString() + (groupBy == null ? "" : (spaces + "\n  " + groupBy));
    return result;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new AggregateProjectionCalculationStep(projection.copy(), groupBy == null ? null : groupBy.copy(), limit, context, timeoutMillis);
  }
}
