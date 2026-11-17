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

import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.Type;
import com.arcadedb.database.Document;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.executor.operators.ParallelSafeExpressions;
import com.arcadedb.query.opencypher.rewriter.ProjectedExpressionSubstituter;
import com.arcadedb.query.opencypher.rewriter.ProjectedOrderByNormalizer;
import com.arcadedb.query.opencypher.temporal.*;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.HeapEstimator;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.ParallelRecordScan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.BinaryComparator;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Execution step for ORDER BY clause.
 * Sorts results according to specified order by items.
 * <p>
 * Example: ORDER BY n.name ASC, n.age DESC
 * <p>
 * **Memory Optimization**: When LIMIT is specified, uses Top-K algorithm with a
 * bounded priority queue (O(K) memory) instead of materializing all results (O(N) memory).
 * <p>
 * Without LIMIT: Materializes all N results in memory for sorting.
 * With LIMIT K: Uses priority queue of size K, keeping only top K results.
 */
public class OrderByStep extends AbstractExecutionStep {
  private final OrderByClause       orderByClause;
  private final ExpressionEvaluator evaluator;
  private final Integer             limit; // Downstream LIMIT value for Top-K optimization
  /** The {@link Keyed} holder itself: its header and its four fields, without the key array. */
  private static final long KEYED_BYTES = HeapEstimator.OBJECT_BYTES + 3L * HeapEstimator.REFERENCE_BYTES + Long.BYTES;
  // THE ROWS BUFFERED, UNDER THE PER-OPERATION CAP AND THE HEAP BUDGET OF ALL THE QUERIES (ISSUES #8585, #8591). ON THE
  // STEP, NOT ON ITS RESULT SET: THE CLOSE() OF A QUERY REACHES THE STEPS
  private       OperationHeapLimit  heapLimit;
  // SET WHEN THE TOP ROWS WERE KEPT IN THE WORKERS OF A PARALLEL SCAN, FOR THE PLAN PRINTOUT (ISSUE #11873)
  private       int                 parallelWorkers;
  // PER ORDER BY ITEM, THE COLUMN OF THE PROJECTION BEFORE THE SORT THAT ALREADY HOLDS ITS VALUE, AND THE EXPRESSION TO EVALUATE
  // WITH THE PROJECTED PARTS READ FROM THEIR COLUMNS (ISSUE #12368). NULL WHEN NO PROJECTION WAS GIVEN
  private       String[]            projectedColumns;
  private       Expression[]        keyExpressions;

  public OrderByStep(final OrderByClause orderByClause, final CommandContext context) {
    this(orderByClause, context, null, null);
  }

  public OrderByStep(final OrderByClause orderByClause, final CommandContext context,
                     final CypherFunctionFactory functionFactory) {
    this(orderByClause, context, functionFactory, null);
  }

  /**
   * Creates an OrderByStep with optional LIMIT for Top-K optimization.
   *
   * @param orderByClause    ORDER BY clause defining sort order
   * @param context          command context
   * @param functionFactory  function factory for expression evaluation
   * @param limit            downstream LIMIT value (null if no LIMIT present)
   */
  public OrderByStep(final OrderByClause orderByClause, final CommandContext context,
                     final CypherFunctionFactory functionFactory, final Integer limit) {
    super(context);
    this.orderByClause = orderByClause;
    this.evaluator = functionFactory != null ? new ExpressionEvaluator(functionFactory) : null;
    this.limit = limit;
  }

  /**
   * Reads the keys that repeat an expression of the projection in front of the sort from the columns it projected them as,
   * instead of evaluating them again (issue #12368). In
   * {@code RETURN n.external_id AS external_id ... ORDER BY n.external_id} the key used to be read from the record of {@code n}
   * once per row, although the row already held it under {@code external_id}; a key that only contains a projected expression
   * ({@code ORDER BY coalesce(rel.confidence, -1.0)} next to {@code rel.confidence AS confidence}) reads that part from its column.
   * <p>
   * The value is the same: the projection evaluated the same expression against the same input row. Where the two could differ
   * - a projected name that shadows a variable the expression reads, as in {@code RETURN n.y AS n, n.x AS x ORDER BY n.x}, where
   * the {@code n} of the ORDER BY is the column {@code n} - the key is evaluated as written, so the mapping never changes what a
   * key means. This is done here, at execution, and not in the parsed ORDER BY, which the planner reads to answer the sort from an
   * index and EXPLAIN prints as written.
   * <p>
   * A collapsing projection (DISTINCT, aggregation) had its ORDER BY rewritten at parse time already
   * ({@link ProjectedOrderByNormalizer}), so for it this finds nothing more to do.
   *
   * @param projected the items of the RETURN or WITH projection whose rows this step sorts, may be null
   *
   * @return this step
   */
  public OrderByStep withProjection(final List<ReturnClause.ReturnItem> projected) {
    if (projected == null || projected.isEmpty() || orderByClause.isEmpty())
      return this;

    final List<OrderByClause.OrderByItem> items = orderByClause.getItems();
    String[] columns = null;
    Expression[] expressions = null;
    ProjectedExpressionSubstituter substituter = null;
    for (int i = 0; i < items.size(); i++) {
      final OrderByClause.OrderByItem item = items.get(i);
      final String column = ProjectedOrderByNormalizer.findPlainProjectedOutputName(projected, item.getExpression());
      if (column != null) {
        if (columns == null)
          columns = new String[items.size()];
        columns[i] = column;
        continue;
      }

      final Expression ast = item.getExpressionAST();
      // an aggregation in a key is refused by the validator before this, and a key with none is evaluated per row as written
      if (ast == null || evaluator == null || ast.containsAggregation())
        continue;
      if (substituter == null)
        substituter = ProjectedOrderByNormalizer.newPlainProjectionSubstituter(projected);
      final Object substituted = substituter.rewrite(ast);
      if (substituted != ast && substituted instanceof Expression expression) {
        if (expressions == null)
          expressions = new Expression[items.size()];
        expressions[i] = expression;
      }
    }
    this.projectedColumns = columns;
    this.keyExpressions = expressions;
    return this;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("OrderByStep requires a previous step");

    return new ResultSet() {
      private List<Result> sortedResults = null;
      private int          currentIndex  = 0;

      @Override
      public boolean hasNext() {
        if (sortedResults == null) {
          heapLimit = OperationHeapLimit.of(context, "ORDER BY");
          try {
            materializeAndSort();
          } catch (final RuntimeException e) {
            releaseBuffer();
            throw e;
          }
        }
        return currentIndex < sortedResults.size();
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final Result result = sortedResults.get(currentIndex++);
        if (currentIndex == sortedResults.size())
          // EVERY ROW WAS SERVED: THE BUFFER IS NOT NEEDED ANYMORE, EVEN IF THE CONSUMER KEEPS THE RESULT SET OPEN. THE
          // STEP DOWNSTREAM MAY STILL HOLD THE LAST PULL BATCH OF THEM (UP TO nRecords ROWS), WHICH NO LIMIT COUNTS
          releaseBuffer();
        return result;
      }

      private void releaseBuffer() {
        if (sortedResults != null) {
          currentIndex = 0;
          sortedResults = Collections.emptyList();
        }
        releaseHeap();
      }

      /**
       * Materializes all results from previous step and sorts them.
       * Uses Top-K algorithm with bounded priority queue when LIMIT is present.
       */
      private void materializeAndSort() {
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          // Top-K optimization: Use bounded priority queue when LIMIT is present
          if (limit != null && limit > 0 && !orderByClause.isEmpty()) {
            sortedResults = materializeTopK(limit);
          } else {
            // Standard sorting: materialize all results. Pulled at the plan's batch size, as the top-K path below and
            // the aggregation steps do: Integer.MAX_VALUE made every streaming step upstream - the label scan, the
            // projection - hold the whole input in its own batch while this step copied it, doubling the heap of the
            // sort out of sight of both limits on it (issue #8591)
            final ResultSet prevResults = prev.syncPull(context, nRecords > 0 ? nRecords : 100);
            // The whole input is consumed inside this one pull: see AggregationStep (issue #9680)
            final WorkGuard guard = WorkGuard.forCommandDeadline(context);
            int consumed = 0;
            if (orderByClause.isEmpty()) {
              sortedResults = new ArrayList<>();
              while (prevResults.hasNext()) {
                guard.checkPeriodically(++consumed);
                final Result row = prevResults.next();
                sortedResults.add(row);
                heapLimit.add(sortedResults.size(), row);
              }
            } else {
              // Each row's keys are evaluated once, as it is read, and the sort compares them (issue #12341). The keys are
              // charged with their row while the sort holds them: an ORDER BY expression can build a value the row does
              // not hold, as large as it likes
              final List<Keyed> keyed = new ArrayList<>();
              long keyBytes = 0L;
              while (prevResults.hasNext()) {
                guard.checkPeriodically(++consumed);
                final Keyed entry = withKeys(prevResults.next(), context, consumed, heapLimit.isCharging());
                keyed.add(entry);
                heapLimit.add(keyed.size(), entry.row);
                heapLimit.charge(entry.keyBytes);
                keyBytes += entry.keyBytes;
              }
              keyed.sort(createComparator());
              sortedResults = new ArrayList<>(keyed.size());
              for (int i = 0; i < keyed.size(); i++)
                sortedResults.add(keyed.get(i).row);
              heapLimit.release(keyBytes);
            }
            // PROFILE reports the rows the sort read, not the one call that read them (issue #11702)
            if (context.isProfiling())
              rowCount += consumed;
          }
        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }

      /**
       * Top-K algorithm: Maintains a bounded priority queue of size K.
       * Memory: O(K) instead of O(N) where K << N
       *
       * CRITICAL: Pulls results in batches instead of Integer.MAX_VALUE to avoid
       * causing upstream steps (like OptionalMatchStep) to materialize everything.
       * <p>
       * Every row's sort keys are evaluated once (issue #12341), and the row remembers when it arrived: the heap orders the rows
       * by key, then arrival, with the worst on top, and a row enters a full heap only if it sorts strictly before the worst
       * one. Among equal keys the earlier row wins, so the rows kept, and their order, are those of a stable sort of every row
       * cut at K, which is also what the workers of a parallel scan answer ({@link #topKInParallel}, issue #11873).
       *
       * @param k the limit value (top K results to return)
       * @return list of top K results in correct sort order
       */
      private List<Result> materializeTopK(final int k) {
        final Comparator<Keyed> comparator = createComparator();
        final List<Result> inParallel = topKInParallel(k, comparator);
        if (inParallel != null)
          return inParallel;

        // Priority queue with reversed comparator: worst elements bubble to top. Each entry carries the keys of its row,
        // evaluated once when the row was read: re-evaluated at every offer, ORDER BY rand() LIMIT k drew new keys for the
        // rows already kept, so the later rows won far more often than the earlier ones (issue #12341)
        final PriorityQueue<Keyed> topK = new PriorityQueue<>(Math.min(k + 1, 1000), comparator.reversed());

        // Pull all results and maintain a top-K heap
        final int batchSize = Math.max(1000, k * 10);
        final ResultSet prevResults = prev.syncPull(context, batchSize);
        final WorkGuard guard = WorkGuard.forCommandDeadline(context);
        final boolean charging = heapLimit.isCharging();
        int consumed = 0;

        while (prevResults.hasNext()) {
          guard.checkPeriodically(++consumed);
          final Keyed entry = withKeys(prevResults.next(), context, consumed, charging);

          if (topK.size() < k) {
            // Haven't reached K elements yet, add unconditionally. Only a LIMIT larger than the cap can make the heap
            // outgrow it
            topK.offer(entry);
            heapLimit.add(topK.size(), entry.row);
            heapLimit.charge(entry.keyBytes);
          } else {
            // Heap is full: the row replaces the worst one only if it sorts before it
            final Keyed worst = topK.peek();
            if (comparator.compare(entry, worst) < 0) {
              topK.poll();
              topK.offer(entry);
              chargeReplacement(heapLimit, worst, entry);
            }
          }
        }
        if (context.isProfiling())
          rowCount += consumed;

        final List<Result> results = sortedRows(new ArrayList<>(topK), k, comparator);
        // The rows kept past the first ones were charged a sampled size and adjusted per replacement, and every kept row its
        // keys, which are gone now: charge exactly the rows held
        heapLimit.rechargeAll(results);
        return results;
      }

      /**
       * Keeps the top K rows in the workers of a parallel scan instead of on this thread (issue #11873), as SQL's ORDER BY
       * does since #8802, when this step reads the projection of a RETURN whose input is a {@link ParallelRowSource}, and the
       * projection and every sort key are expressions the workers can evaluate. Every worker projects its rows, evaluates
       * their sort keys and keeps its own top K; the heaps are merged here. A row remembers where in the sequential scan it
       * was, and a worker meets its rows in ascending position, so among equal keys the row the sequential scan meets first
       * wins, as in {@link #materializeTopK}: the answer, rows and order, is the sequential one.
       * <p>
       * Thread safety: the workers share this step's {@link ExpressionEvaluator}, the parsed ORDER BY items, the projection and
       * {@code projectedColumns} / {@code keyExpressions}, all read-only once the step runs. The expressions they evaluate are
       * the ones {@link ParallelSafeExpressions} admits, which keep no plain mutable state of their own
       * ({@code ParallelSafeExpressionsStateTest}), and each worker evaluates them against its own command context. Each worker
       * fills its own {@link PartialTopK} and charges its own heap limit; this thread reads the partials only after
       * {@link ParallelRecordScan#aggregate} has returned, and a worker a failure cancelled stops charging under the partial's
       * monitor.
       *
       * @return the rows, or {@code null} when this execution sorts on this thread
       */
      private List<Result> topKInParallel(final int k, final Comparator<Keyed> comparator) {
        parallelWorkers = 0;
        if (evaluator == null || !(prev instanceof ProjectReturnStep projection)
            || !(projection.getPrev() instanceof ParallelRowSource source))
          return null;
        final String variable = source.parallelVariable();
        if (variable == null || !projection.isParallelSafe(variable))
          return null;
        // A key read from a column of the projected row is a value; any other is evaluated on the row, in the workers
        final List<OrderByClause.OrderByItem> items = orderByClause.getItems();
        final Set<String> columns = projection.projectedColumns();
        columns.add(variable);
        for (final OrderByClause.OrderByItem item : items)
          if (!columns.contains(item.getExpression())
              && !ParallelSafeExpressions.isParallelSafe(item.getExpressionAST(), variable))
            return null;

        final ParallelRecordScan scan = source.planParallelRows(context, List.of());
        if (scan == null)
          return null;

        final WorkGuard guard = WorkGuard.forCommandDeadline(context);
        final List<PartialTopK> workerPartials = new CopyOnWriteArrayList<>();
        try {
          final List<PartialTopK> partials = scan.aggregate(context, workerContext -> {
            final PartialTopK partial = new PartialTopK(k, comparator, OperationHeapLimit.of(workerContext, "ORDER BY"));
            workerPartials.add(partial);
            return partial;
          }, (partial, row, position, workerContext) -> {
            final Result projected = projection.projectResult(row, workerContext);
            partial.accept(withKeys(projected, workerContext, position, partial.heapLimit.isCharging()));
          }, guard::check);

          final List<Keyed> kept = new ArrayList<>();
          long consumed = 0;
          for (final PartialTopK partial : partials) {
            kept.addAll(partial.heap);
            consumed += partial.rows;
          }
          if (context.isProfiling())
            rowCount += consumed;

          // THE WORKERS CHARGED THEIR OWN ROWS AND THEIR SORT KEYS WHILE THEY SCANNED; THIS STEP HOLDS THE ROWS IT KEEPS FROM HERE
          // ON. THE WORKERS' CHARGES ARE TAKEN OVER WITHOUT GIVING THEM BACK TO THE BUDGET IN BETWEEN, THEN ADJUSTED TO THE ROWS
          // KEPT, WHOSE KEYS ARE DROPPED
          for (final PartialTopK partial : partials)
            partial.handOverHeap(heapLimit);
          // A WORKER CHECKED THE ELEMENT CAP ON ITS OWN HEAP ONLY: THE MERGED ANSWER HOLDS AS MANY ROWS AS THE SEQUENTIAL HEAP
          // WOULD, SO IT IS CHECKED AGAINST THE SAME CAP
          heapLimit.check(Math.min(k, kept.size()));
          final List<Result> results = sortedRows(kept, k, comparator);
          heapLimit.rechargeAll(results);
          parallelWorkers = scan.getWorkerCount();
          return results;
        } finally {
          // A NO-OP ON SUCCESS; ON A FAILURE IT GIVES BACK WHAT THE WORKERS CHARGED, THE ONES STILL STOPPING INCLUDED
          for (final PartialTopK partial : workerPartials)
            partial.releaseHeap();
        }
      }

      /**
       * The ORDER BY keys of {@code row}, evaluated once, when the row enters the sort. Evaluated inside the comparator, every
       * key cost about 2 * log2(n) evaluations per row instead of one, a column key built the row's whole name set at each of
       * them, and a non-deterministic key ({@code rand()}) took a new value at every comparison: the sort failed with
       * "Comparison method violates its general contract!" and the top-k heap of {@code ORDER BY rand() LIMIT k} was biased
       * towards the last rows (issues #11661, #12340, #12341).
       * <p>
       * The top-k heap calls this for every row it reads, also the ones it then rejects: a row is rejected by comparing its
       * keys with the worst row kept, so they are needed before the decision, and they are evaluated exactly once either way.
       *
       * @param evaluationContext the context the keys are evaluated in: this step's, or a parallel scan worker's
       * @param arrival           where the row came in the sequential order, which breaks ties between equal keys
       * @param charging          whether the heap budget is on, so the key sizes are worth estimating
       */
      // The reference comparison is the point: a key that IS the column's value object is already charged with the row, an
      // equal but distinct one (a converted temporal, a copied list) is memory the sort holds on its own
      @SuppressWarnings("PMD.CompareObjectsWithEquals")
      private Keyed withKeys(final Result row, final CommandContext evaluationContext, final long arrival, final boolean charging) {
        final List<OrderByClause.OrderByItem> items = orderByClause.getItems();
        final Object[] keys = new Object[items.size()];
        long bytes = charging ? KEYED_BYTES + (long) keys.length * HeapEstimator.REFERENCE_BYTES : 0L;
        for (int i = 0; i < keys.length; i++) {
          final OrderByClause.OrderByItem item = items.get(i);
          final String expression;
          // the projection's column of a key that repeats one of its expressions (issue #12368), when the row has it
          if (projectedColumns != null && projectedColumns[i] != null && row.listsProperty(projectedColumns[i]))
            expression = projectedColumns[i];
          else
            expression = item.getExpression();
          // A projected column already holds what its Cypher expression evaluated to - a property read restored its
          // temporal there - so a String in it IS a string: sniffing it again turned a declared STRING "P10D" (or a
          // string literal) into a duration and sorted it as one (issue #8384). The column test does not build the
          // row's name set (issue #12340)
          if (expression != null && row.listsProperty(expression)) {
            final Object stored = row.getProperty(expression);
            keys[i] = convertFromStorage(stored);
            // the very value of the column is charged with the row; a converted one is the sort's own memory
            if (charging && keys[i] != stored)
              bytes += HeapEstimator.estimate(keys[i]);
          } else {
            keys[i] = evaluateKey(row, item, keyExpressions != null ? keyExpressions[i] : null, evaluationContext);
            if (charging && keys[i] != null)
              bytes += HeapEstimator.estimate(keys[i]);
          }
        }
        return new Keyed(row, keys, bytes, arrival);
      }

      /**
       * Compares two rows by the keys {@link #withKeys} evaluated, item by item, in each item's direction, then by when they
       * arrived: the order of a stable sort, which a top-K heap and the workers of a parallel scan reproduce (issue #11873).
       */
      private Comparator<Keyed> createComparator() {
        final List<OrderByClause.OrderByItem> items = orderByClause.getItems();
        final boolean[] ascending = new boolean[items.size()];
        for (int i = 0; i < ascending.length; i++)
          ascending[i] = items.get(i).isAscending();
        return (r1, r2) -> {
          final Object[] k1 = r1.keys;
          final Object[] k2 = r2.keys;
          for (int i = 0; i < ascending.length; i++) {
            final int comparison = compareValues(k1[i], k2[i]);
            if (comparison != 0)
              return ascending[i] ? comparison : -comparison;
          }
          return Long.compare(r1.arrival, r2.arrival);
        };
      }

      /** The value of an ORDER BY item that is not a column of the row: its expression, evaluated against the row. */
      private Object evaluateKey(final Result result, final OrderByClause.OrderByItem item, final Expression projectedAST,
          final CommandContext evaluationContext) {
        final String expression = item.getExpression();
        // If we have a parsed Expression AST, use ExpressionEvaluator for full expression support. The one with its projected
        // parts read from their columns first (issue #12368)
        final Expression exprAST = projectedAST != null ? projectedAST : item.getExpressionAST();
        if (exprAST != null && evaluator != null)
          return evaluator.evaluate(exprAST, result, evaluationContext);
        if (expression.contains(".")) {
          final String[] parts = expression.split("\\.", 2);
          final Object obj = result.getProperty(parts[0]);
          if (obj == null)
            return null;

          if (obj instanceof Vertex || obj instanceof Edge) {
            // The same read a property access makes: a temporal text a release before the native types wrote is
            // restored on a database that predates them (issue #9636)
            return convertFromStorage(TemporalUtil.convertFromStorage((Document) obj, parts[1]));
          }
        }

        return convertFromStorage(result.getProperty(expression));
      }

      /**
       * Convert ArcadeDB-stored values back to Cypher temporal types for proper comparison.
       * Duration, LocalTime, and Time are stored as Strings because ArcadeDB
       * doesn't have native binary types for them.
       */
      private static Object convertFromStorage(final Object value) {
        // Fast path: common non-temporal types don't need conversion
        if (value == null || value instanceof Number || value instanceof Boolean)
          return value;

        // Handle single values - check temporal types before collections
        if (value instanceof LocalDate ld)
          return new CypherDate(ld);
        if (value instanceof LocalDateTime ldt)
          return new CypherLocalDateTime(ldt);
        // The native types of the other Cypher temporals (issue #8572)
        if (value instanceof OffsetTime || value instanceof LocalTime || value instanceof ZonedDateTime || value instanceof CypherDuration)
          return TemporalUtil.convertFromStorage(value);

        // Handle collections (lists/arrays of temporal values)
        if (value instanceof Collection<?> collection) {
          final List<Object> converted = new ArrayList<>(collection.size());
          for (final Object item : collection) {
            converted.add(convertFromStorage(item));
          }
          return converted;
        }
        if (value instanceof Object[] array) {
          final Object[] converted = new Object[array.length];
          for (int i = 0; i < array.length; i++) {
            converted[i] = convertFromStorage(array[i]);
          }
          return converted;
        }

        return value;
      }

      /**
       * Compares two values for sorting.
       * In Cypher, null is considered the largest value: nulls sort last in ASC, first in DESC.
       * NaN sorts between null and all other values.
       * Type ordering: Map < Node < Relationship < List < Path < Temporal < String < Boolean < Number
       */
      @SuppressWarnings({"unchecked", "rawtypes"})
      private int compareValues(final Object v1, final Object v2) {
        if (v1 == null && v2 == null)
          return 0;
        if (v1 == null)
          return 1; // null is the largest value
        if (v2 == null)
          return -1;

        // Handle NaN: NaN sorts just before null
        final boolean nan1 = v1 instanceof Double && Double.isNaN((Double) v1) ||
            v1 instanceof Float && Float.isNaN((Float) v1);
        final boolean nan2 = v2 instanceof Double && Double.isNaN((Double) v2) ||
            v2 instanceof Float && Float.isNaN((Float) v2);
        if (nan1 && nan2) return 0;
        if (nan1) return 1;
        if (nan2) return -1;

        // Compare lists element-by-element. An array that is not a BINARY value is a sequence, ordered like the list of its elements
        // (issue #10380)
        if ((v1 instanceof List || MultiValue.isSequenceArray(v1)) && (v2 instanceof List || MultiValue.isSequenceArray(v2))) {
          final List<?> l1 = MultiValue.getMultiValueAsList(v1);
          final List<?> l2 = MultiValue.getMultiValueAsList(v2);
          final int minSize = Math.min(l1.size(), l2.size());
          for (int i = 0; i < minSize; i++) {
            final int cmp = compareValues(l1.get(i), l2.get(i));
            if (cmp != 0) return cmp;
          }
          return Integer.compare(l1.size(), l2.size());
        }

        // Maps by size, keys, then values, as min() and max() order them: their toString() put {a: 10} before {a: 9}
        if (v1 instanceof Map<?, ?> m1 && v2 instanceof Map<?, ?> m2)
          return CypherFunctionHelper.compareMaps(m1, m2, this::compareValues);

        // Compare booleans: false < true
        if (v1 instanceof Boolean && v2 instanceof Boolean)
          return Boolean.compare((Boolean) v1, (Boolean) v2);

        // Temporal values of one type compare by value, of two types by the rank of the type: DATETIME < LOCAL DATETIME < DATE
        // < TIME < LOCAL TIME < DURATION, never by content across types (issue #10200)
        if (v1 instanceof CypherTemporalValue t1 && v2 instanceof CypherTemporalValue t2) {
          final int rank1 = CypherFunctionHelper.temporalTypeRank(t1);
          final int rank2 = CypherFunctionHelper.temporalTypeRank(t2);
          return rank1 != rank2 ? Integer.compare(rank1, rank2) : t1.compareTo(t2);
        }

        // Strings sort by code point, the order an index holds them in, which lets a plan read them from one in place of
        // this sort (issue #8422)
        if (v1 instanceof String s1 && v2 instanceof String s2)
          return BinaryComparator.compareStrings(s1, s2);

        // BINARY values sort by their bytes, unsigned, as SQL and the index order them. A byte[] is not Comparable, and its
        // toString() is the identity of the array, which changes with every load of the record (issue #10195)
        if (v1 instanceof byte[] b1 && v2 instanceof byte[] b2)
          return BinaryComparator.compareBytes(b1, b2);

        // Same-type Comparable comparison
        if (v1.getClass().equals(v2.getClass()) && v1 instanceof Comparable) {
          try {
            return ((Comparable) v1).compareTo(v2);
          } catch (final ClassCastException e) {
            // Fall through
          }
        }

        // Cross-type number comparison. A Float goes through its decimal form so the ordering agrees with what
        // ComparisonExpression answers for the same pair, rather than ordering on the rounding error (issue #7609).
        if (v1 instanceof Number number1 && v2 instanceof Number number2)
          return Double.compare(toComparableDouble(number1), toComparableDouble(number2));

        // Different types: order by type rank
        final int rank1 = typeRank(v1);
        final int rank2 = typeRank(v2);
        if (rank1 != rank2)
          return Integer.compare(rank1, rank2);

        // One temporal type, one side a native java.time / java.util.Date value: compared as Cypher values, as min() and max()
        // compare them, rather than through a Comparable that cannot take the other class
        if (CypherFunctionHelper.isTemporalTypeRank(rank1))
          return ((CypherTemporalValue) TemporalUtil.fromCoreJavaType(v1)).compareTo((CypherTemporalValue) TemporalUtil.fromCoreJavaType(v2));

        // Same rank, try Comparable
        if (v1 instanceof Comparable && v2 instanceof Comparable) {
          try {
            return ((Comparable) v1).compareTo(v2);
          } catch (final ClassCastException e) {
            // Fall through
          }
        }

        return v1.toString().compareTo(v2.toString());
      }

      /**
       * The Cypher order of types, shared with min() and max() through {@link CypherFunctionHelper#cypherTypeRank(Object)} so
       * the two cannot drift apart. A value Cypher has no type for sorts after every other one.
       */
      private int typeRank(final Object v) {
        final int rank = CypherFunctionHelper.cypherTypeRank(v);
        return rank == CypherFunctionHelper.TYPE_RANK_PATH_OR_OTHER && !(v instanceof TraversalPath) ? Integer.MAX_VALUE : rank;
      }

      @Override
      public void close() {
        releaseBuffer();
        OrderByStep.this.close();
      }
    };
  }

  /**
   * A row and the values of its ORDER BY items, evaluated once when the row enters the sort (issue #12341), with the estimated
   * heap the keys add to the row, which the sort charges while it holds them, and where the row arrived in the sequential
   * order: the tie-break that keeps the sort stable (issue #11873).
   */
  private record Keyed(Result row, Object[] keys, long keyBytes, long arrival) {
  }

  /** The first {@code k} rows of {@code kept} in the order of {@code comparator}. */
  private static List<Result> sortedRows(final List<Keyed> kept, final int k, final Comparator<Keyed> comparator) {
    kept.sort(comparator);
    final int size = Math.min(k, kept.size());
    final List<Result> rows = new ArrayList<>(size);
    for (int i = 0; i < size; i++)
      rows.add(kept.get(i).row());
    return rows;
  }

  /**
   * Adjusts the charge of a top-K heap for {@code added} taking the place of {@code removed}, rows and keys in one step:
   * charging a larger row before giving back a larger key would hold both for a moment, which the budget may refuse although
   * the heap holds less after the replacement.
   */
  private static void chargeReplacement(final OperationHeapLimit heapLimit, final Keyed removed, final Keyed added) {
    if (!heapLimit.isCharging())
      return;
    final long delta = HeapEstimator.estimate(added.row()) + added.keyBytes() - HeapEstimator.estimate(removed.row())
        - removed.keyBytes();
    if (delta > 0)
      heapLimit.charge(delta);
    else
      heapLimit.release(-delta);
  }

  /** One worker's top K rows of a parallel scan (issue #11873). Not thread-safe: one worker fills it, the caller merges it after. */
  private static final class PartialTopK {
    private final int                 k;
    private final Comparator<Keyed>   comparator;
    private final OperationHeapLimit  heapLimit;
    private final PriorityQueue<Keyed> heap;
    private       long                rows;
    // GUARDED BY this: SET BY THE CALLER, WHICH RELEASES THE HEAP OF A WORKER A FAILURE CANCELLED WHILE IT MAY STILL RUN
    private       boolean             heapReleased;

    PartialTopK(final int k, final Comparator<Keyed> comparator, final OperationHeapLimit heapLimit) {
      this.k = k;
      this.comparator = comparator;
      this.heapLimit = heapLimit;
      this.heap = new PriorityQueue<>(Math.min(k + 1, 1000), comparator.reversed());
    }

    /**
     * A worker meets its rows in ascending position: a row whose keys tie the worst kept one sorts after it, so it never
     * replaces it, which keeps the earlier row as the sequential top K does.
     */
    void accept(final Keyed candidate) {
      ++rows;
      if (heap.size() < k) {
        heap.add(candidate);
        // THE MONITOR IS UNCONTENDED BUT FOR A CANCELLED WORKER, AND TAKEN ONLY FOR A ROW THAT ENTERS THE HEAP
        synchronized (this) {
          if (!heapReleased) {
            heapLimit.add(heap.size(), candidate.row());
            heapLimit.charge(candidate.keyBytes());
          }
        }
      } else if (comparator.compare(candidate, heap.peek()) < 0) {
        final Keyed evicted = heap.poll();
        heap.add(candidate);
        synchronized (this) {
          if (!heapReleased)
            chargeReplacement(heapLimit, evicted, candidate);
        }
      }
    }

    /** Gives back what this worker charged for its rows and their keys, and stops it charging more. */
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

  private void releaseHeap() {
    if (heapLimit != null)
      heapLimit.release();
  }

  @Override
  public void close() {
    releaseHeap();
    super.close();
  }

  /**
   * Widens a number for ordering, reading a {@link Float} through its decimal form. See {@link Type#widenFloat}.
   *
   * @param value the operand (never {@code null})
   *
   * @return the operand as a double
   */
  private static double toComparableDouble(final Number value) {
    return value instanceof Float float1 ? Type.widenFloat(float1) : value.doubleValue();
  }

  /**
   * The sort as a plan line: its keys with their directions, the top-k bound a downstream LIMIT gave it, and under PROFILE its
   * time and the rows it read. The default printed the bare class name, which showed neither what was sorted nor what it cost
   * (issue #11702).
   */
  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder("  ".repeat(Math.max(0, depth * indent)));
    builder.append("+ ORDER BY ");
    final List<OrderByClause.OrderByItem> items = orderByClause.getItems();
    for (int i = 0; i < items.size(); i++) {
      if (i > 0)
        builder.append(", ");
      builder.append(items.get(i).getExpression()).append(items.get(i).isAscending() ? " ASC" : " DESC");
    }
    // same guard as materializeAndSort: a LIMIT 0 (or an empty clause) sorts every row, so no bound to show
    if (limit != null && limit > 0 && !orderByClause.isEmpty()) {
      builder.append(" (top ").append(limit);
      if (parallelWorkers > 0)
        builder.append(", parallel: ").append(parallelWorkers).append(" workers");
      builder.append(")");
    }
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }
}
