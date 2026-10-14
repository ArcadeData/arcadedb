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

import com.arcadedb.database.Record;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.EqualsCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

@ExcludeFromJacocoGeneratedReport
public interface IndexableSQLFunction {
  boolean shouldExecuteAfterSearch(FromClause target, BinaryCompareOperator operator, Object right, CommandContext context, Expression[] oExpressions);

  boolean allowsIndexedExecution(FromClause target, BinaryCompareOperator operator, Object right, CommandContext context, Expression[] oExpressions);

  boolean canExecuteInline(FromClause target, BinaryCompareOperator operator, Object right, CommandContext context, Expression[] oExpressions);

  long estimate(FromClause target, BinaryCompareOperator operator, Object rightValue, CommandContext context, Expression[] oExpressions);

  /**
   * Answers the records an indexed function matches.
   * <p>
   * The result MAY BE LAZY, and its {@link java.util.Iterator} may then hold an open index scan - the geospatial
   * predicates chain one per bucket rather than loading every candidate up front (#5601). <b>A caller that stops
   * iterating before exhaustion must close an iterator that implements {@link AutoCloseable}</b>: a compacted-series
   * cursor registers with its file, and {@code LSMTreeIndex.dropRetiredCompactedIndexes} skips a retired file that
   * still has one, so an abandoned scan pins that file for the lifetime of the database. Draining the iterator fully
   * releases everything without a close. {@link FetchFromIndexedFunctionStep}, the only consumer today, does both.
   */
  Iterable<Record> searchFromTarget(FromClause target, BinaryCompareOperator operator, Object rightValue, CommandContext context, Expression[] oExpressions);

  /**
   * {@link #searchFromTarget(FromClause, BinaryCompareOperator, Object, CommandContext, Expression[])} for a plan that
   * consumes at most {@code limit} rows of the answer (issue #9895). The planner passes a limit only when the rows it consumes
   * are the first {@code limit} of the function's own best-first order - the statement has no ORDER BY, or orders by
   * {@code $score} descending, and nothing between the fetch and the {@code LIMIT} can drop or add a row - so a function
   * whose answer has such an order may compute only that many. The answer must still be correct past them: a record that
   * turns out to be gone is skipped, and the rows that make up for it have to come from somewhere.
   *
   * @param limit the rows the plan can consume, or -1 for all of them
   */
  default Iterable<Record> searchFromTarget(final FromClause target, final BinaryCompareOperator operator, final Object rightValue,
      final CommandContext context, final Expression[] oExpressions, final int limit) {
    return searchFromTarget(target, operator, rightValue, context, oExpressions);
  }

  /**
   * {@link #searchFromTarget(FromClause, BinaryCompareOperator, Object, CommandContext, Expression[], int)} for a plan that
   * also hands the function the {@code @rid} conditions of the WHERE block and the order it needs the rows in (issue
   * #9909). The plan no longer checks those conditions after the fetch: the answer has to satisfy them. A function that can
   * decide them on a candidate's RID skips the record before it loads it; this default, for one that cannot, filters the
   * records it answers, and does not hand on the limit, which would count rows the filter then drops.
   * <p>
   * A RID order is only ever asked of a function that {@link #supportsRIDOrder() supports it}.
   */
  default Iterable<Record> searchFromTarget(final FromClause target, final BinaryCompareOperator operator, final Object rightValue,
      final CommandContext context, final Expression[] oExpressions, final IndexedFunctionPushdown pushdown) {
    final RIDFilter ridFilter = pushdown.ridFilter();
    if (ridFilter == null)
      return searchFromTarget(target, operator, rightValue, context, oExpressions, pushdown.limit());
    return ridFilter.filter(searchFromTarget(target, operator, rightValue, context, oExpressions));
  }

  /**
   * Whether {@link #searchFromTarget(FromClause, BinaryCompareOperator, Object, CommandContext, Expression[],
   * IndexedFunctionPushdown)} answers in RID order when asked to, so that a statement ordered by {@code @rid} needs no sort
   * and its LIMIT can stop the loading (issue #9909).
   */
  default boolean supportsRIDOrder() {
    return false;
  }

  /**
   * Whether {@link #searchFromTarget(FromClause, BinaryCompareOperator, Object, CommandContext, Expression[],
   * IndexedFunctionPushdown)} answers best first - by descending score, equal scores by ascending RID - and the condition,
   * evaluated again on each record it answered, sets {@code $score} to that same score. The branches of an OR made of such
   * conditions are then merged by score rather than run one after the other and sorted (issue #10022).
   */
  default boolean answersByScore() {
    return false;
  }

  /**
   * Whether a condition {@code function(...) <operator> <right>} selects the records a boolean predicate function MATCHES,
   * which is all an index of matches can answer: {@code = true}. Any other comparison ({@code = false}, {@code <> true})
   * selects the records it does not match, and answering it with the matches returns the wrong rows, so a predicate
   * function refuses indexed execution for it and the plan scans instead.
   */
  static boolean selectsMatches(final BinaryCompareOperator operator, final Object right) {
    return operator instanceof EqualsCompareOperator && Boolean.TRUE.equals(right);
  }

  /**
   * Optional scoring metadata surfaced by {@code EXPLAIN}/{@code PROFILE} for this indexed function (e.g. the BM25 similarity,
   * parameters and per-term IDF for {@code SEARCH_INDEX}). Returns {@code null} when the function has nothing to explain.
   */
  default Object getScoringExplain(final FromClause target, final BinaryCompareOperator operator, final Object rightValue,
      final CommandContext context, final Expression[] oExpressions) {
    return null;
  }
}
