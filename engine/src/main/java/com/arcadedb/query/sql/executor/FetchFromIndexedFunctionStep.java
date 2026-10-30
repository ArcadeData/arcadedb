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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.facet.MatchSet;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.OrBlock;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.IntPredicate;
import java.util.logging.Level;

/**
 * Created by luigidellaquila on 06/08/16.
 */
public class FetchFromIndexedFunctionStep extends AbstractExecutionStep implements IndexedMatchSource {
  private final BinaryCondition functionCondition;
  private final FromClause      queryTarget;
  /**
   * How many rows of the function's answer the plan can consume, or -1 for all of them: set by the planner only when those
   * are the first rows of the function's own best-first order (issue #9895).
   */
  private final int                           limit;
  /**
   * The {@code @rid} conjuncts of the block, decided by the function on its candidates' RIDs before any record is loaded,
   * or null for none (issue #9909). Resolved at every execution, since they can read parameters and variables.
   */
  private final List<BooleanExpression>       ridConditions;
  /** The order the plan needs the rows in (issue #9909). */
  private final IndexedFunctionPushdown.Order order;

  //runtime0
  Iterator<Record> fullResult = null;
  /** The answer {@link #collectAllMatches} searched for, served as the rows too (FACETS only); null otherwise. */
  private Iterable<Record> collectedAnswer;

  public FetchFromIndexedFunctionStep(final BinaryCondition functionCondition, final FromClause queryTarget,
      final CommandContext context) {
    this(functionCondition, queryTarget, -1, null, IndexedFunctionPushdown.Order.FUNCTION, context);
  }

  /**
   * @param limit         how many rows of the function's answer the plan can consume, or -1 for all of them (see the
   *                      pushdown variant of {@link IndexableSQLFunction}{@code .searchFromTarget()})
   * @param ridConditions the {@code @rid} conjuncts the answer must satisfy, which the plan does not check after the fetch,
   *                      or null for none
   * @param order         the order the plan needs the rows in
   */
  public FetchFromIndexedFunctionStep(final BinaryCondition functionCondition, final FromClause queryTarget, final int limit,
      final List<BooleanExpression> ridConditions, final IndexedFunctionPushdown.Order order, final CommandContext context) {
    super(context);
    this.functionCondition = functionCondition;
    this.queryTarget = queryTarget;
    this.limit = limit;
    this.ridConditions = ridConditions == null || ridConditions.isEmpty() ? null : List.copyOf(ridConditions);
    this.order = order;
  }

  public int getLimit() {
    return limit;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);
    init(context);

    return new ResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        return fullResult.hasNext();
      }

      @Override
      public Result next() {
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          if (localCount >= nRecords) {
            throw new NoSuchElementException();
          }
          if (!fullResult.hasNext()) {
            throw new NoSuchElementException();
          }
          final ResultInternal result = new ResultInternal(fullResult.next().getRecord());
          localCount++;
          return result;
        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }

    };
  }

  /**
   * The function's complete answer by RID, when it is every match (no row limit was handed over) and the function can enumerate it
   * without loading a record. The search itself is the one the rows are served from: a function caches it for the statement.
   */
  @Override
  public boolean collectAllMatches(final CommandContext context, final MatchSet.Builder builder, final IntPredicate bucketFilter) {
    if (limit > -1)
      return false;
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final RIDFilter ridFilter = ridConditions != null ? RIDFilter.resolve(ridConditions, context) : null;
      if (ridFilter != null && ridFilter.acceptsNothing())
        return true;
      final Iterable<Record> answer = functionCondition.executeIndexedFunction(queryTarget, context,
          new IndexedFunctionPushdown(-1, ridFilter, order));
      // the rows are served from the same answer: a search with @rid conditions is not cached by the function
      collectedAnswer = answer;
      if (!(answer instanceof IndexedMatchSource.Rids rids))
        return false;
      rids.forEachRid(bucketFilter, rid -> builder.add(rid, 0));
      return true;
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /**
   * Whether {@code where} is this step's own function condition and nothing else: the filter the planner chains after the fetch when
   * the function is evaluated on each row again (for its {@code $score}), which every row of the complete answer passes.
   */
  @Override
  public boolean isOwnCondition(final WhereClause where) {
    BooleanExpression expression = where.getBaseExpression();
    while (true) {
      if (expression instanceof AndBlock and && and.getSubBlocks().size() == 1)
        expression = and.getSubBlocks().getFirst();
      else if (expression instanceof OrBlock or && or.getSubBlocks().size() == 1)
        expression = or.getSubBlocks().getFirst();
      else
        break;
    }
    return functionCondition.equals(expression);
  }

  private void init(final CommandContext context) {
    if (fullResult == null) {
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        final RIDFilter ridFilter = ridConditions != null ? RIDFilter.resolve(ridConditions, context) : null;
        if (collectedAnswer != null) {
          // the answer collectAllMatches() searched for, with the same pushdown
          fullResult = collectedAnswer.iterator();
          collectedAnswer = null;
        } else if (ridFilter != null && ridFilter.acceptsNothing())
          // the @rid conditions contradict each other or name no RID: there is nothing to search for
          fullResult = Collections.emptyIterator();
        else
          fullResult = functionCondition.executeIndexedFunction(queryTarget, context,
              new IndexedFunctionPushdown(limit, ridFilter, order)).iterator();
      } finally {
        if (context.isProfiling()) {
          cost += System.nanoTime() - begin;
        }
      }
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result =
        ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEXED FUNCTION " + functionCondition.toString();
    if (order != IndexedFunctionPushdown.Order.FUNCTION)
      result += " ORDER BY @rid " + (order == IndexedFunctionPushdown.Order.RID_ASC ? "ASC" : "DESC");
    if (limit > -1)
      result += order == IndexedFunctionPushdown.Order.FUNCTION ? " (best " + limit + " needed)" : " (first " + limit + " needed)";
    if (ridConditions != null)
      result += "\n" + ExecutionStepInternal.getIndent(depth, indent) + "  RID FILTER (" + conditionsToString() + ")";
    if (context.isProfiling()) {
      result += " (" + getCostFormatted() + ")";
    }
    // Surface BM25 scoring metadata (similarity, k1/b, N, avgdl, per-term df/idf) for full-text SEARCH_INDEX conditions so it can
    // be inspected via EXPLAIN/PROFILE. This delegates down the indexed-function chain
    // (BinaryCondition -> Expression -> ... -> FunctionCall -> IndexableSQLFunction.getScoringExplain), mirroring
    // executeIndexedFunction; non-indexable or non-full-text functions return null. Best-effort: never let an explain failure
    // break the plan rendering.
    try {
      final Object scoring = functionCondition.getIndexedFunctionScoringExplain(queryTarget, context);
      if (scoring != null)
        result += "\n" + ExecutionStepInternal.getIndent(depth, indent) + "  SCORING " + scoring;
    } catch (final Exception ex) {
      // Scoring metadata is informational only, so never let it break plan rendering; log at FINE so the failure is still
      // discoverable when debugging instead of being silently swallowed.
      LogManager.instance().log(this, Level.FINE, "Failed to collect full-text scoring metadata for EXPLAIN/PROFILE", ex);
    }
    return result;
  }

  private String conditionsToString() {
    final StringBuilder builder = new StringBuilder();
    for (final BooleanExpression condition : ridConditions) {
      if (!builder.isEmpty())
        builder.append(" AND ");
      builder.append(condition);
    }
    return builder.toString();
  }

  @Override
  public void close() {
    // Release the underlying index cursors even when the scan did not run to exhaustion (e.g. a LIMIT was reached or
    // the result set was closed early). The geospatial function now answers with a LAZY chain of index cursors
    // (#5601), and a compacted-series cursor registers with its file so a full compaction defers dropping it: an
    // unclosed cursor would keep the retired file alive. Mirrors FetchFromIndexStep.close().
    releaseResult();
    super.close();
  }

  @Override
  public void reset() {
    releaseResult();
  }

  private void releaseResult() {
    if (fullResult instanceof final AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.FINE, "Error on closing the indexed function result iterator", e);
      }
    }
    fullResult = null;
    collectedAnswer = null;
  }
}
