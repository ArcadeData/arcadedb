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

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.DistinctNumericKey;
import com.arcadedb.query.opencypher.executor.CypherExecutionPlan;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.HeapEstimator;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Execution step for UNION and UNION ALL queries.
 * Combines results from multiple subqueries.
 * <p>
 * - UNION: Removes duplicate rows from the combined result
 * - UNION ALL: Keeps all rows including duplicates
 * <p>
 * Example:
 * <pre>
 * MATCH (n:Person) RETURN n.name AS name
 * UNION
 * MATCH (n:Company) RETURN n.name AS name
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class UnionStep extends AbstractExecutionStep {
  private final List<CypherExecutionPlan> queryPlans;
  private final boolean removeDuplicates;
  private final QueryStatistics aggregatedStatistics = new QueryStatistics();

  // THE KEYS A DISTINCT REMEMBERS, UNDER THE PER-OPERATION CAP AND THE HEAP BUDGET OF ALL THE QUERIES (ISSUES #8585,
  // #8591). ON THE STEP: THE CLOSE() OF A QUERY REACHES THE STEPS, NOT THEIR RESULT SETS
  private OperationHeapLimit heapLimit;

  /**
   * Creates a UnionStep.
   *
   * @param queryPlans       execution plans for each subquery
   * @param removeDuplicates true for UNION (dedup), false for UNION ALL
   * @param context          command context
   */
  public UnionStep(final List<CypherExecutionPlan> queryPlans, final boolean removeDuplicates,
                   final CommandContext context) {
    super(context);
    this.queryPlans = queryPlans;
    this.removeDuplicates = removeDuplicates;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    return new ResultSet() {
      private int currentQueryIndex = 0;
      private ResultSet currentResultSet = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private final Set<List<Object>> seenResults = removeDuplicates ? new HashSet<>() : null;
      private final OperationHeapLimit distinctLimit = removeDuplicates ? distinctHeapLimit(context, "UNION") : null;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;

        if (finished)
          return false;

        fetchMore(nRecords);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        while (buffer.size() < n && !finished) {
          // Initialize or advance to next query's result set
          if (currentResultSet == null || !currentResultSet.hasNext()) {
            if (currentResultSet != null) {
              aggregatedStatistics.add(currentResultSet.getStatistics().orElse(null));
              currentResultSet.close();
            }

            // Move to next query
            if (currentQueryIndex >= queryPlans.size()) {
              finished = true;
              // Every branch is read: the UNION keys are not needed anymore, even if the consumer keeps the result set open
              if (seenResults != null) {
                seenResults.clear();
                distinctLimit.release();
              }
              break;
            }

            // Execute next query, on this step's context so the branch joins the enclosing statement's clock
            // instead of freezing one of its own (issue #7052).
            currentResultSet = queryPlans.get(currentQueryIndex).execute(context);
            currentQueryIndex++;
          }

          // Fetch results from current query
          while (buffer.size() < n && currentResultSet.hasNext()) {
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (context.isProfiling())
                rowCount++;

              final Result result = currentResultSet.next();

              // Apply deduplication for UNION (not UNION ALL)
              if (removeDuplicates) {
                final List<Object> resultKey = buildResultKey(result);
                if (!seenResults.add(resultKey))
                  continue; // Skip duplicate
                distinctLimit.add(seenResults.size(), resultKey, HeapEstimator.HASH_ENTRY_BYTES);
              }

              buffer.add(result);
            } finally {
              if (context.isProfiling())
                cost += System.nanoTime() - begin;
            }
          }
        }
      }

      /**
       * Builds a key for deduplication based on result properties. Values are canonicalized via
       * DistinctNumericKey so numerically-equal values (issue #5789) and graph-element references
       * sharing one RID (issue #6488) collapse together.
       */
      private List<Object> buildResultKey(final Result result) {
        return DistinctNumericKey.buildKey(result.getPropertyNames(), result::getProperty);
      }

      @Override
      public void close() {
        if (currentResultSet != null)
          currentResultSet.close();
        if (distinctLimit != null)
          distinctLimit.release();
      }
    };
  }

  /** The operation of the DISTINCT keys of the result set just handed out, which the step's close() releases. */
  private OperationHeapLimit distinctHeapLimit(final CommandContext context, final String operation) {
    heapLimit = OperationHeapLimit.of(context, operation);
    return heapLimit;
  }

  @Override
  public void close() {
    if (heapLimit != null)
      heapLimit.release();
    super.close();
  }

  /**
   * Returns the write statistics summed across every branch that has completed execution so far.
   * The accumulator fills in incrementally as branches are pulled, so the sum is only complete
   * once the caller has fully drained the ResultSet returned by {@link #syncPull}.
   */
  public QueryStatistics getAggregatedStatistics() {
    return aggregatedStatistics;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    builder.append(ind);
    builder.append("+ UNION");
    if (!removeDuplicates)
      builder.append(" ALL");
    builder.append(" (").append(queryPlans.size()).append(" queries)");

    if (context.isProfiling())
      builder.append(" (").append(getCostFormatted()).append(")");

    return builder.toString();
  }
}
