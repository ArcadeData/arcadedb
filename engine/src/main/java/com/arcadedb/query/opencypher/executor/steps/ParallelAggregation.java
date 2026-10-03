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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.function.DistinctNumericKey;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.executor.operators.ParallelSafeExpressions;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ExecutionStepInternal;
import com.arcadedb.query.sql.executor.HeapEstimator;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.ParallelRecordScan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Partial aggregation of an openCypher aggregation or implicit GROUP BY in the workers of a parallel label scan (issue
 * #8797), the split SQL uses since issue #8523: every worker aggregates the rows of the units it scans into groups of its
 * own, and the groups of the workers are merged at the end, each partition of the key space on its own.
 * <p>
 * It applies when the rows come from a {@link ParallelRowSource} and everything the workers evaluate - the grouping keys
 * and the arguments of the aggregates - is built of nodes {@link ParallelSafeExpressions} accepts, and every aggregate is
 * one that can merge partial states ({@code count}, {@code sum}, {@code avg}, {@code min}, {@code max}, not DISTINCT).
 * Otherwise {@link #create} answers {@code null} and the step aggregates on the consuming thread, as before.
 * <p>
 * Accepted trade-off, as in SQL: the partials merge in worker order, not scan order, so a {@code sum} or {@code avg} of doubles is
 * not bit-for-bit reproducible (the addition order varies), and a {@code min} or {@code max} over values that compare equal but
 * differ in type (1 and 1.0) may answer either one.
 * <p>
 * PROFILE: the scan and filter steps feeding the aggregation are never pulled, so only this step reports time and rows.
 * <p>
 * The groups come out in the order of their first row in the sequential scan, each with the key values of that row, as the
 * sequential aggregation returns them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ParallelAggregation {
  /** Groups a merge task of one partition handles at least, below which the merge stays on the caller. */
  private static final int PARALLEL_MERGE_MIN_GROUPS = 16_384;

  /** One group: its key values, its aggregates, and where in the sequential scan its first row is. */
  static final class Group {
    Object[]                keyValues;
    final StatelessFunction[] aggregators;
    long                    firstSeen;

    Group(final Object[] keyValues, final StatelessFunction[] aggregators, final long firstSeen) {
      this.keyValues = keyValues;
      this.aggregators = aggregators;
      this.firstSeen = firstSeen;
    }
  }

  /** The merged groups, and how many workers produced them. */
  record Merged(List<Group> groups, int workers) {
  }

  private final ParallelRowSource            source;
  private final CypherFunctionFactory        functionFactory;
  private final ExpressionEvaluator          evaluator;
  private final Expression[]                 keys;
  private final FunctionCallExpression[]     aggregates;

  private ParallelAggregation(final ParallelRowSource source, final CypherFunctionFactory functionFactory,
      final ExpressionEvaluator evaluator, final Expression[] keys, final FunctionCallExpression[] aggregates) {
    this.source = source;
    this.functionFactory = functionFactory;
    this.evaluator = evaluator;
    this.keys = keys;
    this.aggregates = aggregates;
  }

  /**
   * @return the aggregation to run in the workers of the scan {@code prev} feeds, or {@code null} when it must run on the
   * consuming thread: the input is not a parallel scan, or an expression or an aggregate is not one the workers can run
   */
  static ParallelAggregation create(final ExecutionStepInternal prev, final Expression[] keys,
      final FunctionCallExpression[] aggregates, final CypherFunctionFactory functionFactory, final ExpressionEvaluator evaluator) {
    if (!(prev instanceof ParallelRowSource source) || functionFactory == null || aggregates.length == 0)
      return null;
    final String variable = source.parallelVariable();
    if (variable == null)
      return null;

    for (final Expression key : keys)
      if (!ParallelSafeExpressions.isParallelSafe(key, variable))
        return null;
    for (final FunctionCallExpression aggregate : aggregates) {
      if (aggregate.isDistinct())
        return null;
      for (final Expression argument : aggregate.getArguments())
        if (!ParallelSafeExpressions.isParallelSafe(argument, variable))
          return null;
      if (!functionFactory.getFunctionExecutor(aggregate.getFunctionName(), false).canMergePartials())
        return null;
    }
    return new ParallelAggregation(source, functionFactory, evaluator, keys, aggregates);
  }

  /**
   * Runs the aggregation in the workers and merges their groups.
   *
   * @param groupsLimit the step's budget for the groups it returns, which it is charged for from here on
   * @param overhead    what a group holds besides its key values, in bytes
   *
   * @return the groups, or {@code null} when this execution cannot run in parallel and the step aggregates as it would
   * have without the call
   */
  Merged run(final CommandContext context, final OperationHeapLimit groupsLimit, final int overhead) {
    final ParallelRecordScan scan = source.planParallelRows(context, List.of());
    if (scan == null)
      return null;

    // ONE PARTITION PER WORKER, SO THE MERGE CAN RUN IN PARALLEL TOO; WITHOUT A KEY THERE IS ONE GROUP TO MERGE
    final int partitions = keys.length == 0 ? 1 : scan.getWorkerCount();
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    final List<Partial> workerPartials = new CopyOnWriteArrayList<>();
    try {
      final List<Partial> partials = scan.aggregate(context, workerContext -> {
        final Partial partial = new Partial(partitions, OperationHeapLimit.of(workerContext, "groups", "GROUP BY"), overhead);
        workerPartials.add(partial);
        return partial;
      }, (partial, row, position, workerContext) -> partial.accept(row, position, workerContext), guard::check);

      final Partial merged = partials.getFirst();
      long partialGroups = 0;
      for (final Partial partial : partials)
        partialGroups += partial.groupCount;
      final List<Runnable> merges = new ArrayList<>(partitions);
      for (int p = 0; p < partitions; p++) {
        final int partition = p;
        merges.add(() -> {
          for (int i = 1; i < partials.size(); i++)
            merged.mergeFrom(partials.get(i), partition);
        });
      }
      scan.run(merges, partialGroups >= PARALLEL_MERGE_MIN_GROUPS);

      final List<Group> groups = new ArrayList<>();
      for (final Map<Object, Group> partition : merged.partitions)
        groups.addAll(partition.values());
      if (keys.length > 0)
        groupsLimit.check(groups.size());
      groups.sort(Comparator.comparingLong(g -> g.firstSeen));

      // THE WORKERS CHARGED THEIR OWN GROUPS WHILE THEY SCANNED; THE STEP HOLDS THE GROUPS IT RETURNS FROM HERE ON: A KEY SEVERAL
      // WORKERS MET IS ONE GROUP NOW. THE STEP TAKES THE WORKERS' CHARGES OVER WITHOUT GIVING THEM BACK TO THE BUDGET IN BETWEEN,
      // WHERE ANOTHER QUERY COULD TAKE THEM WHILE THE GROUPS ARE STILL HELD, THEN ADJUSTS THEM TO THE GROUPS IT KEEPS
      for (final Partial partial : partials)
        partial.handOverHeap(groupsLimit);
      if (keys.length > 0) {
        long kept = 0L;
        for (final Group group : groups)
          kept += HeapEstimator.estimate(group.keyValues) + overhead;
        if (kept < groupsLimit.getChargedBytes())
          groupsLimit.release(groupsLimit.getChargedBytes() - kept);
        else
          groupsLimit.charge(kept - groupsLimit.getChargedBytes());
      }
      return new Merged(groups, scan.getWorkerCount());
    } finally {
      // A NO-OP ON SUCCESS; ON A FAILURE IT GIVES BACK WHAT THE WORKERS CHARGED, THE ONES STILL STOPPING INCLUDED
      for (final Partial partial : workerPartials)
        partial.releaseHeap();
      workerPartials.clear();
    }
  }

  private StatelessFunction[] newAggregators() {
    final StatelessFunction[] aggregators = new StatelessFunction[aggregates.length];
    for (int i = 0; i < aggregators.length; i++)
      aggregators[i] = functionFactory.getFunctionExecutor(aggregates[i].getFunctionName(), false);
    return aggregators;
  }

  /**
   * One worker's partial aggregation. Its groups are spread over partitions by the hash of their key, the same way in every
   * worker, so partition {@code p} of every worker can be merged independently of the others. Not thread-safe: one worker
   * runs it, and the merge runs after the workers are over.
   */
  private final class Partial {
    private final Map<Object, Group>[] partitions;
    private final OperationHeapLimit   heapLimit;
    private final int                  overhead;
    private       int                  groupCount;
    // GUARDED BY this: SET BY THE CALLER, WHICH RELEASES THE HEAP OF A WORKER A FAILURE CANCELLED WHILE IT MAY STILL RUN
    private       boolean              heapReleased;

    @SuppressWarnings("unchecked")
    Partial(final int partitionCount, final OperationHeapLimit heapLimit, final int overhead) {
      this.partitions = new Map[partitionCount];
      for (int i = 0; i < partitionCount; i++)
        partitions[i] = new HashMap<>();
      this.heapLimit = heapLimit;
      this.overhead = overhead;
      // WITHOUT A KEY THE ONE GROUP EXISTS WHETHER OR NOT A ROW REACHES IT: AN AGGREGATION OF NO ROWS ANSWERS ONE ROW (count 0)
      if (keys.length == 0) {
        partitions[0].put(null, new Group(new Object[0], newAggregators(), Long.MAX_VALUE));
        groupCount = 1;
      }
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

    // THE GROUP LIMIT IS CHECKED ON THIS WORKER'S GROUPS HERE AND ON THE MERGED ONES AT THE END: AN EXACT GLOBAL COUNT WHILE
    // SCANNING WOULD NEED A KEY SET SHARED BY EVERY WORKER (DOCUMENTED ON QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP)
    void accept(final Result row, final long position, final CommandContext context) {
      final Group group;
      if (keys.length == 0) {
        group = partitions[0].get(null);
      } else {
        final Object[] values = new Object[keys.length];
        for (int i = 0; i < values.length; i++)
          values[i] = evaluator.evaluate(keys[i], row, context);
        // THE SAME KEY THE SEQUENTIAL STEPS GROUP BY: ONE LOGICAL NUMBER IS ONE GROUP WHATEVER ITS JAVA TYPE (#5789, #6676)
        final Object key = keys.length == 1 ? DistinctNumericKey.canonicalize(values[0]) : new GroupKeyValues(values);
        final Map<Object, Group> groups = partitions[Math.floorMod(key == null ? 0 : key.hashCode(), partitions.length)];
        Group existing = groups.get(key);
        if (existing == null) {
          existing = new Group(values, newAggregators(), position);
          groups.put(key, existing);
          ++groupCount;
          // THE MONITOR IS UNCONTENDED BUT FOR A CANCELLED WORKER: TAKEN PER NEW GROUP, NOT PER ROW
          synchronized (this) {
            if (!heapReleased)
              heapLimit.add(groupCount, values, overhead);
          }
        }
        group = existing;
      }

      for (int i = 0; i < aggregates.length; i++) {
        final List<Expression> arguments = aggregates[i].getArguments();
        final Object[] args = new Object[arguments.size()];
        for (int j = 0; j < args.length; j++)
          args[j] = evaluator.evaluate(arguments.get(j), row, context);
        group.aggregators[i].checkArity(args);
        group.aggregators[i].execute(args, context);
      }
    }

    /** Folds one partition of another worker's groups into this one: the aggregates merge, the earliest first row wins. */
    void mergeFrom(final Partial other, final int partition) {
      final Map<Object, Group> groups = partitions[partition];
      for (final Map.Entry<Object, Group> entry : other.partitions[partition].entrySet()) {
        final Group theirs = entry.getValue();
        final Group ours = groups.putIfAbsent(entry.getKey(), theirs);
        if (ours == null)
          continue;
        for (int i = 0; i < ours.aggregators.length; i++)
          ours.aggregators[i].mergePartial(theirs.aggregators[i]);
        if (theirs.firstSeen < ours.firstSeen) {
          ours.firstSeen = theirs.firstSeen;
          ours.keyValues = theirs.keyValues;
        }
      }
    }
  }
}
