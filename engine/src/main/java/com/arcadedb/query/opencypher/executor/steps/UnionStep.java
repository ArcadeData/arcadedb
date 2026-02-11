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
import com.arcadedb.query.opencypher.executor.CypherExecutionPlan;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
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
      private final Set<String> seenResults = removeDuplicates ? new HashSet<>() : null;
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
              currentResultSet.close();
            }

            // Move to next query
            if (currentQueryIndex >= queryPlans.size()) {
              finished = true;
              break;
            }

            // Execute next query
            currentResultSet = queryPlans.get(currentQueryIndex).execute();
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
                final String resultKey = buildResultKey(result);
                if (seenResults.contains(resultKey))
                  continue; // Skip duplicate
                seenResults.add(resultKey);
              }

              buffer.add(result);
            } finally {
              if (context.isProfiling())
                cost += (System.nanoTime() - begin);
            }
          }
        }
      }

      /**
       * Builds a key for deduplication based on result properties.
       */
      private String buildResultKey(final Result result) {
        final StringBuilder sb = new StringBuilder();
        for (final String prop : result.getPropertyNames()) {
          sb.append(prop).append("=");
          final Object value = result.getProperty(prop);
          sb.append(value == null ? "null" : value.toString());
          sb.append("|");
        }
        return sb.toString();
      }

      @Override
      public void close() {
        if (currentResultSet != null)
          currentResultSet.close();
      }
    };
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
