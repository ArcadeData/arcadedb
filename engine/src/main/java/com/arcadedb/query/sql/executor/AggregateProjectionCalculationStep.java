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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.HeapBufferingFunction;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.GroupBy;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;
import com.arcadedb.schema.Type;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

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

  // #8591: THE GROUPS HELD, UNDER THE HEAP BUDGET OF ALL THE QUERIES; THE CAP ON THEIR NUMBER IS maxGroupsAllowed
  private       OperationHeapLimit        heapLimit;
  // #8591: THE PARTIAL AGGREGATIONS OF THE WORKERS OF A PARALLEL SCAN, SO A FAILURE CAN GIVE BACK WHAT THEY CHARGED
  private final Queue<PartialAggregation> workerPartials = new ConcurrentLinkedQueue<>();

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
        values[i] = Type.normalizeNumberForKey(values[i]);
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

  private final GroupBy groupBy;
  private final long    timeoutMillis;
  private final long    limit;
  private final long    maxGroupsAllowed;

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
    final Database db = context == null ? null : context.getDatabase();
    this.maxGroupsAllowed = db == null ?
        GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong() :
        db.getConfiguration().getValueAsLong(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) {
    if (finalResults == null) {
      heapLimit = OperationHeapLimit.of(context, "GROUP BY");
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
        if (maxGroupsAllowed > 0 && aggregateResults.size() >= maxGroupsAllowed) {
          aggregateResults.clear();
          checkGroupCount(maxGroupsAllowed);
        }

        preAggr = newGroup(projection, next, context);
        aggregateResults.put(key, preAggr);
        heapLimit.chargeElement(key.values, groupOverheadBytes(projection));
      }

      applyAggregates(projection, preAggr, next, context, heapLimit);

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
    if (maxGroupsAllowed > 0 && groups >= maxGroupsAllowed) {
      throw new CommandExecutionException(
          "Limit of allowed groups for in-heap GROUP BY in a single query exceeded (" + maxGroupsAllowed + "). You can set "
              + GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey() + " to increase this limit");
    }
  }

  /**
   * What a group holds besides the values of its key, which are charged on their own: its key and its entry in the map
   * of the groups, the row of its non-aggregate projections - most often the key values again - and one state per
   * aggregate. The state of an aggregate that gathers values (list(), set(), percentile()) grows past that.
   */
  private static int groupOverheadBytes(final Projection projection) {
    return HeapEstimator.HASH_ENTRY_BYTES + 24 + HeapEstimator.RESULT_BYTES
        + 64 * projection.getItems().size();
  }

  private void releaseGroups() {
    aggregateResults.clear();
    if (finalResults != null)
      finalResults = Collections.emptyList();
    if (heapLimit != null)
      heapLimit.release();
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
   * Issue #8523: aggregates in the workers of a parallel scan instead of on this thread, when this step reads a type
   * scan directly or through the projection computing the aggregates' arguments. Every worker runs that projection,
   * the GROUP BY and the aggregation for its part of the rows on its own copy of them, building a partial group
   * state, and the partials are merged here. The groups come out exactly as the sequential aggregation lists them -
   * in the order the scan first meets them, each carrying the non-aggregate values of that first row - because every
   * group remembers where in the sequential scan it was first seen; and a LIMIT keeps the same groups.
   *
   * @return the groups, or {@code null} when this execution aggregates sequentially: the input is not a parallel
   * scan, an aggregate cannot merge partials (only count, sum, avg, min and max can), or an expression is not one the
   * engine can evaluate on several threads
   */
  private List<ResultInternal> aggregateInParallel(final CommandContext context, final long timeoutBegin) {
    final ExecutionStepInternal source = parallelSource(context);
    if (source == null)
      return null;
    final Projection preProjection = prev != source ? ((ProjectionCalculationStep) prev).projection : null;

    final ParallelTypeScan scan = source instanceof FetchFromTypeExecutionStep fetch ?
        fetch.planParallelAggregation(context) :
        ((FetchFromTypeWithFilterStep) source).planParallelAggregation(context);
    if (scan == null)
      return null;

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final Projection workerPreProjection = preProjection;
      final boolean[] timedOutNotified = new boolean[1];
      // ONE PARTITION PER WORKER, SO THE MERGE CAN RUN IN PARALLEL TOO; WITHOUT A GROUP BY THERE IS ONE GROUP TO MERGE
      final int partitions = groupBy == null ? 1 : scan.getWorkerCount();
      final int groupOverhead = groupOverheadBytes(projection);
      final List<PartialAggregation> partials = scan.aggregate(context,
          workerContext -> {
            final PartialAggregation partial = new PartialAggregation(
                workerPreProjection == null ? null : workerPreProjection.copy(), projection.copy(),
                groupBy == null ? null : groupBy.copy(), partitions, OperationHeapLimit.of(workerContext, "GROUP BY"), groupOverhead);
            workerPartials.add(partial);
            return partial;
          },
          PartialAggregation::accept,
          () -> {
            if (!timedOutNotified[0] && timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
              timedOutNotified[0] = true;
              sendTimeout();
            }
          });

      parallelWorkers = scan.getWorkerCount();
      parallelUnits = scan.getUnitCount();

      // PARTITION p OF EVERY WORKER HOLDS THE SAME KEYS, AND NO OTHER PARTITION DOES: EACH ONE IS MERGED ON ITS OWN, IN
      // PARALLEL WHEN THERE ARE ENOUGH GROUPS FOR IT TO PAY
      final PartialAggregation merged = partials.getFirst();
      long partialGroups = 0;
      for (final PartialAggregation partial : partials)
        partialGroups += partial.groupCount;
      // RESOLVED HERE, ONCE: THE MERGES RUN CONCURRENTLY AND MUST NOT MEMOIZE ANYTHING ON THE SHARED PROJECTION
      final List<ProjectionItem> items = projection.getItems();
      final String[] aliases = new String[items.size()];
      final boolean[] aggregates = new boolean[items.size()];
      for (int i = 0; i < aliases.length; i++) {
        aliases[i] = items.get(i).getProjectionAlias().getStringValue();
        aggregates[i] = items.get(i).isAggregate(context);
      }
      final List<Runnable> merges = new ArrayList<>(partitions);
      for (int p = 0; p < partitions; p++) {
        final int partition = p;
        merges.add(() -> {
          for (int i = 1; i < partials.size(); i++)
            merged.mergeFrom(partials.get(i), partition, aliases, aggregates);
        });
      }
      scan.run(merges, partialGroups >= PARALLEL_MERGE_MIN_GROUPS);

      final List<PartialGroup> groups = new ArrayList<>();
      for (final HashMap<GroupByKey, PartialGroup> partition : merged.partitions)
        groups.addAll(partition.values());
      checkGroupCount(groups.size() - 1);

      // THE WORKERS CHARGED THEIR OWN GROUPS WHILE THEY SCANNED; THIS STEP HOLDS THE MERGED ONES FROM HERE ON, A KEY
      // SEVERAL WORKERS MET BEING ONE GROUP NOW
      long partialBytes = 0L;
      for (final PartialAggregation partial : partials)
        partialBytes += partial.releaseHeap();
      if (partialGroups > 0)
        heapLimit.charge((long) ((double) partialBytes * groups.size() / partialGroups));
      groups.sort(Comparator.comparingLong(g -> g.firstSeen));
      final int size = limit > 0 ? (int) Math.min(limit, groups.size()) : groups.size();
      final List<ResultInternal> result = new ArrayList<>(size);
      for (int i = 0; i < size; i++)
        result.add(groups.get(i).row);
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
   * The type scan this step can aggregate in the workers of, reading it directly or through the plain projection that
   * computes the aggregates' arguments, or {@code null} when it cannot: another input, an aggregate that cannot merge
   * partials, or an expression the engine cannot evaluate on several threads.
   */
  private ExecutionStepInternal parallelSource(final CommandContext context) {
    ExecutionStepInternal source = prev;
    Projection preProjection = null;
    if (source != null && source.getClass() == ProjectionCalculationStep.class) {
      preProjection = ((ProjectionCalculationStep) source).projection;
      source = ((ProjectionCalculationStep) source).prev;
    }
    if (!(source instanceof FetchFromTypeExecutionStep) && !(source instanceof FetchFromTypeWithFilterStep))
      return null;

    for (final ProjectionItem proj : projection.getItems())
      if (proj.isAggregate(context) && !proj.getAggregationContext(context).canMerge())
        return null;
    if (!SqlAstInspector.isParallelSafe(preProjection, projection, groupBy))
      return null;
    return source;
  }

  /** Whether an execution starting now would aggregate in parallel: what an EXPLAIN shows. */
  private boolean wouldAggregateInParallel() {
    final ExecutionStepInternal source = parallelSource(context);
    if (source instanceof FetchFromTypeExecutionStep fetch)
      return fetch.wouldRunInParallel(context);
    return source instanceof FetchFromTypeWithFilterStep fetch && fetch.wouldRunInParallel(context);
  }

  /** A group of a partial aggregation, and where in the sequential scan its first row is. */
  private static final class PartialGroup {
    final ResultInternal row;
    long                 firstSeen;

    PartialGroup(final ResultInternal row, final long firstSeen) {
      this.row = row;
      this.firstSeen = firstSeen;
    }
  }

  /**
   * One worker's partial aggregation, on the worker's own copies of the expressions. Its groups are spread over
   * partitions by the hash of their key, the same way in every worker, so partition {@code p} of every worker can be
   * merged independently of the others.
   */
  private final class PartialAggregation {
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
    PartialAggregation(final Projection preProjection, final Projection workerProjection, final GroupBy workerGroupBy,
        final int partitionCount, final OperationHeapLimit heapLimit, final int groupOverhead) {
      this.preProjection = preProjection;
      this.workerProjection = workerProjection;
      this.workerGroupBy = workerGroupBy;
      this.heapLimit = heapLimit;
      this.groupOverhead = groupOverhead;
      this.partitions = new HashMap[partitionCount];
      for (int i = 0; i < partitionCount; i++)
        partitions[i] = new HashMap<>();
    }

    /** Gives back what this worker charged for its groups, and stops it charging more. Returns the bytes it held. */
    synchronized long releaseHeap() {
      heapReleased = true;
      final long bytes = heapLimit.getChargedBytes();
      heapLimit.release();
      return bytes;
    }

    // THE GROUP LIMIT IS CHECKED ON THIS WORKER'S GROUPS HERE AND ON THE MERGED ONES AT THE END: AN EXACT GLOBAL COUNT
    // WHILE SCANNING WOULD NEED A KEY SET SHARED BY EVERY WORKER, SO THE PEAK CAN REACH THE LIMIT TIMES THE WORKERS
    // (DOCUMENTED ON QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP)
    void accept(final Result row, final long position, final CommandContext context) {
      final Result next = preProjection != null ? preProjection.calculateSingle(context, row) : row;
      final GroupByKey key = groupKey(workerGroupBy, next, context);
      final HashMap<GroupByKey, PartialGroup> groups = partitions[Math.floorMod(key.hashCode(), partitions.length)];
      PartialGroup group = groups.get(key);
      if (group == null) {
        checkGroupCount(groupCount);
        group = new PartialGroup(newGroup(workerProjection, next, context), position);
        groups.put(key, group);
        ++groupCount;
        // THE MONITOR IS UNCONTENDED BUT FOR A CANCELLED WORKER: TAKEN PER NEW GROUP, NOT PER ROW
        synchronized (this) {
          if (!heapReleased)
            heapLimit.chargeElement(key.values, groupOverhead);
        }
      }
      applyAggregates(workerProjection, group.row, next, context, heapLimit);
    }

    /** Folds one partition of another worker's groups into this one: the aggregations merge, the earliest first row wins. */
    void mergeFrom(final PartialAggregation other, final int partition, final String[] aliases, final boolean[] aggregates) {
      final HashMap<GroupByKey, PartialGroup> groups = partitions[partition];
      for (final Map.Entry<GroupByKey, PartialGroup> entry : other.partitions[partition].entrySet()) {
        final PartialGroup theirs = entry.getValue();
        final PartialGroup ours = groups.putIfAbsent(entry.getKey(), theirs);
        if (ours == null)
          continue;

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
