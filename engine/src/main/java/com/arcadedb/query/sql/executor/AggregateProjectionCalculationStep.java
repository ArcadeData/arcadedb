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

import com.arcadedb.function.HeapBufferingFunction;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.GroupBy;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.Type;

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
  // RESOLVED ONCE BEFORE THE SCAN: THE MERGES RUN CONCURRENTLY AND MUST NOT MEMOIZE ANYTHING ON THE SHARED PROJECTION
  private          String[]            mergeAliases;
  private          boolean[]           mergeAggregates;

  //the key is the GROUP BY key, the value is the (partially) aggregated value
  private final Map<GroupByKey, ResultInternal> aggregateResults = new LinkedHashMap<>();
  private       List<ResultInternal>            finalResults     = null;

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
      ResultSet lastRs = prevStep.syncPull(context, nRecords);
      while (lastRs.hasNext()) {
        if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
          sendTimeout();
        }
        aggregate(lastRs.next(), context);
        if (!lastRs.hasNext()) {
          lastRs = prevStep.syncPull(context, nRecords);
        }
      }
      finalResults = new ArrayList<>(aggregateResults.values());
      aggregateResults.clear();
    }
    for (final ResultInternal item : finalResults) {
      if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
        sendTimeout();
      }
      for (final String name : item.getTemporaryProperties()) {
        final Object prevVal = item.getTemporaryProperty(name);
        if (prevVal instanceof AggregationContext aggregationContext) {
          item.setTemporaryProperty(name, aggregationContext.getFinalValue());
        }
      }
    }
  }

  private void aggregate(final Result next, final CommandContext context) {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final GroupByKey key = groupKey(groupBy, next, context);
      ResultInternal preAggr = aggregateResults.get(key);
      if (preAggr == null) {
        // Query LIMIT optimization: stop processing once we have enough groups
        if (limit > 0 && aggregateResults.size() >= limit)
          return;

        // Memory safety: enforce memory limit for GROUP BY operations
        groupsLimit.check(aggregateResults.size() + 1L, aggregateResults::clear);

        preAggr = newGroup(projection, next, context);
        aggregateResults.put(key, preAggr);
        groupsLimit.chargeElement(key.values, groupOverheadBytes(projection));
      }

      applyAggregates(projection, preAggr, next, context, groupsLimit);

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

  private static GroupByKey groupKey(final GroupBy groupBy, final Result next, final CommandContext context) {
    if (groupBy == null)
      // No GROUP BY means single aggregation group
      return new GroupByKey(new Object[0]);

    // Memory optimization: Use Object[] instead of ArrayList to reduce object allocation overhead
    final Object[] keyValues = new Object[groupBy.getItems().size()];
    int idx = 0;
    for (final Expression item : groupBy.getItems())
      keyValues[idx++] = item.execute(next, context);
    return new GroupByKey(keyValues);
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

  /** A new group, holding the non-aggregate projections of the first row seen for it. */
  private static ResultInternal newGroup(final Projection projection, final Result next, final CommandContext context) {
    final ResultInternal group = new ResultInternal(context.getDatabase());
    for (final ProjectionItem proj : projection.getItems())
      if (!proj.isAggregate(context))
        group.setProperty(proj.getProjectionAlias().getStringValue(), proj.execute(next, context));
    return group;
  }

  /**
   * Feeds a row to the aggregates of a group. An aggregate that keeps every value (list(), percentile()) charges them to
   * {@code heapLimit}, the operation of the groups.
   */
  private static void applyAggregates(final Projection projection, final ResultInternal group, final Result next,
      final CommandContext context, final OperationHeapLimit heapLimit) {
    for (final ProjectionItem proj : projection.getItems()) {
      if (proj.isAggregate(context)) {
        final String alias = proj.getProjectionAlias().getStringValue();
        AggregationContext aggrCtx = (AggregationContext) group.getTemporaryProperty(alias);
        if (aggrCtx == null) {
          aggrCtx = HeapBufferingFunction.adopt(proj.getAggregationContext(context), heapLimit);
          group.setTemporaryProperty(alias, aggrCtx);
        }
        aggrCtx.apply(next, context);
      }
    }
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

      final List<ProjectionItem> items = projection.getItems();
      mergeAliases = new String[items.size()];
      mergeAggregates = new boolean[items.size()];
      for (int i = 0; i < mergeAliases.length; i++) {
        mergeAliases[i] = items.get(i).getProjectionAlias().getStringValue();
        mergeAggregates[i] = items.get(i).isAggregate(context);
      }
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
                  OperationHeapLimit.of(workerContext, "groups", "GROUP BY"), groupOverhead);
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
      final String[] aliases = mergeAliases;
      final boolean[] aggregates = mergeAggregates;
      final List<Runnable> merges = new ArrayList<>(partitions);
      for (int p = 0; p < partitions; p++) {
        final int partition = p;
        merges.add(() -> {
          for (final PartialAggregation partial : partials)
            if (partial != merged)
              merged.mergeFrom(partial, partition, aliases, aggregates);
        });
      }
      firstScan.run(merges, partialGroups >= PARALLEL_MERGE_MIN_GROUPS);

      final List<PartialGroup> groups = new ArrayList<>();
      for (final HashMap<GroupByKey, PartialGroup> partition : merged.partitions)
        groups.addAll(partition.values());
      checkGroupCount(groups.size() - 1);

      groups.sort(Comparator.comparingLong(g -> g.firstSeen));
      final int size = limit > 0 ? (int) Math.min(limit, groups.size()) : groups.size();
      final List<ResultInternal> result = new ArrayList<>(size);
      for (int i = 0; i < size; i++)
        result.add(groups.get(i).row);

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
        final OperationHeapLimit heapLimit, final int groupOverhead) {
      final WhereClause[] workerConditions = new WhereClause[conditions.size()];
      for (int i = 0; i < workerConditions.length; i++)
        workerConditions[i] = conditions.get(i).copy();
      return step.new PartialAggregation(types.toArray(new String[0]), workerConditions,
          preProjection == null ? null : preProjection.copy(), step.projection.copy(), step.groupBy == null ? null : step.groupBy.copy(),
          partitions, heapLimit, groupOverhead);
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

  /** A group of a partial aggregation, and where in the sequential scan its first row is. */
  private static final class PartialGroup {
    final ResultInternal row;
    /** The values of the group's key, which the group is charged for. */
    final Object[]       keyValues;
    long                 firstSeen;

    PartialGroup(final ResultInternal row, final Object[] keyValues, final long firstSeen) {
      this.row = row;
      this.keyValues = keyValues;
      this.firstSeen = firstSeen;
    }
  }

  /**
   * One worker's partial aggregation, on the worker's own copies of the expressions. Its groups are spread over
   * partitions by the hash of their key, the same way in every worker, so partition {@code p} of every worker can be
   * merged independently of the others.
   */
  private final class PartialAggregation {
    private final String[]                            types;
    private final WhereClause[]                       conditions;
    private final Projection                          preProjection;
    private final Projection                          workerProjection;
    private final GroupBy                             workerGroupBy;
    private final HashMap<GroupByKey, PartialGroup>[] partitions;
    private final OperationHeapLimit                  heapLimit;
    private final int                                 groupOverhead;
    private       int                                 groupCount;
    // GUARDED BY this: SET BY THE CALLER, WHICH RELEASES THE HEAP OF A WORKER A FAILURE CANCELLED WHILE IT MAY STILL RUN
    private       boolean                             heapReleased;

    @SuppressWarnings("unchecked")
    PartialAggregation(final String[] types, final WhereClause[] conditions, final Projection preProjection,
        final Projection workerProjection, final GroupBy workerGroupBy, final int partitionCount,
        final OperationHeapLimit heapLimit, final int groupOverhead) {
      this.types = types;
      this.conditions = conditions;
      this.preProjection = preProjection;
      this.workerProjection = workerProjection;
      this.workerGroupBy = workerGroupBy;
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
      if (!matches(row, context))
        return;
      final Result next = preProjection != null ? preProjection.calculateSingle(context, row) : row;
      final GroupByKey key = groupKey(workerGroupBy, next, context);
      final HashMap<GroupByKey, PartialGroup> groups = partitions[Math.floorMod(key.hashCode(), partitions.length)];
      PartialGroup group = groups.get(key);
      boolean flush = false;
      if (group == null) {
        checkGroupCount(groupCount);
        group = new PartialGroup(newGroup(workerProjection, next, context), key.values, position);
        groups.put(key, group);
        ++groupCount;
        // THE MONITOR IS UNCONTENDED BUT FOR A CANCELLED WORKER: TAKEN PER NEW GROUP, NOT PER ROW
        synchronized (this) {
          if (!heapReleased)
            heapLimit.chargeElement(key.values, groupOverhead);
        }
        flush = workerFlushBytes > 0 && heapLimit.getChargedBytes() >= workerFlushBytes;
      }
      // NO OPERATION FOR THE AGGREGATES: ONLY THE ONES THAT MERGE PARTIALS RUN IN THE WORKERS (count, sum, avg, min, max),
      // AND NONE KEEPS THE VALUES IT AGGREGATES, SO NOTHING OUTSIDE THE MONITOR ABOVE EVER CHARGES THIS WORKER'S OPERATION
      applyAggregates(workerProjection, group.row, next, context, null);
      // AFTER THE ROW IS IN: THE FLUSH HANDS THE GROUP OVER, AND THE SHARED SIDE IS NOT TOUCHED WITHOUT ITS LOCK
      if (flush)
        flushToShared(context);
    }

    /** Whether the row passes the filters between the source and the aggregation, as their steps would decide. */
    private boolean matches(final Result row, final CommandContext context) {
      return ParallelRowPipeline.matches(types, conditions, row, context);
    }

    /** Folds one partition of another worker's groups into this one: the aggregations merge, the earliest first row wins. */
    void mergeFrom(final PartialAggregation other, final int partition, final String[] aliases, final boolean[] aggregates) {
      mergePartition(partitions[partition], other.partitions[partition], aliases, aggregates, null);
    }

    /**
     * Folds {@code source} into {@code groups}.
     *
     * @param duplicateBytes when not null, receives in [0] the estimated bytes of the groups of {@code source} whose key
     *                       was in {@code groups} already, which the caller had charged and now gives back
     *
     * @return the number of groups of {@code source} that {@code groups} adopted
     */
    private int mergePartition(final HashMap<GroupByKey, PartialGroup> groups, final HashMap<GroupByKey, PartialGroup> source,
        final String[] aliases, final boolean[] aggregates, final long[] duplicateBytes) {
      int adopted = 0;
      for (final Map.Entry<GroupByKey, PartialGroup> entry : source.entrySet()) {
        final PartialGroup theirs = entry.getValue();
        final PartialGroup ours = groups.putIfAbsent(entry.getKey(), theirs);
        if (ours == null) {
          ++adopted;
          continue;
        }
        if (duplicateBytes != null)
          duplicateBytes[0] += HeapEstimator.estimate(theirs.keyValues) + groupOverhead;

        for (int i = 0; i < aliases.length; i++) {
          if (aggregates[i])
            ((AggregationContext) ours.row.getTemporaryProperty(aliases[i])).merge(
                (AggregationContext) theirs.row.getTemporaryProperty(aliases[i]));
          else if (theirs.firstSeen < ours.firstSeen)
            ours.row.setProperty(aliases[i], theirs.row.getProperty(aliases[i]));
        }
        if (theirs.firstSeen < ours.firstSeen)
          ours.firstSeen = theirs.firstSeen;
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
            target = new PartialAggregation(types, conditions, preProjection, workerProjection, workerGroupBy, partitions.length,
                OperationHeapLimit.of(context, "groups", "GROUP BY"), groupOverhead);
            workerPartials.add(target);
            sharedGroups = target;
          }
        }

      synchronized (this) {
        if (heapReleased)
          // CANCELLED: THE GROUPS ARE DISCARDED WITH THE QUERY, AND THE CHARGE IS GIVEN BACK BY THE CALLER
          return;
        synchronized (target.heapLimit) {
          if (!target.heapLimit.transferFrom(heapLimit))
            heapLimit.release();
        }
      }

      final long[] duplicateBytes = new long[1];
      int adopted = 0;
      for (int p = 0; p < partitions.length; p++) {
        final HashMap<GroupByKey, PartialGroup> groups = target.partitions[p];
        synchronized (groups) {
          adopted += mergePartition(groups, partitions[p], mergeAliases, mergeAggregates, duplicateBytes);
        }
        partitions[p] = new HashMap<>();
      }
      groupCount = 0;
      synchronized (target.heapLimit) {
        target.heapLimit.release(duplicateBytes[0]);
      }
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

  /**
   * Lightweight wrapper for GROUP BY keys using Object[] instead of ArrayList.
   * This reduces memory overhead by eliminating ArrayList wrapper objects for each key.
   */
  private static class GroupByKey {
    private final Object[] values;
    private final int hashCode;

    GroupByKey(final Object[] values) {
      // Normalise numeric values to a canonical form so that numerically-equal keys represented with different
      // numeric types (e.g. Integer(1) vs Long(1), or BigDecimal("1") vs BigDecimal("1.0")) end up in the same
      // group instead of being split (issue #4516).
      for (int i = 0; i < values.length; i++)
        values[i] = Type.normalizeForKey(values[i]);
      this.values = values;
      this.hashCode = Arrays.hashCode(values);
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj)
        return true;
      if (!(obj instanceof GroupByKey))
        return false;
      return Arrays.equals(this.values, ((GroupByKey) obj).values);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
