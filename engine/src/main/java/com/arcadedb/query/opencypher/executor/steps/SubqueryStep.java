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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.SubqueryClause;
import com.arcadedb.query.opencypher.executor.CypherExecutionPlan;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Execution step for CALL subquery clause.
 * <p>
 * For each input row from the previous step, executes the inner query
 * with the outer row's variables available (imported via WITH inside the subquery).
 * The inner query's RETURN values are merged with the outer row.
 * <p>
 * When IN TRANSACTIONS is specified, the subquery commits changes in batches
 * rather than accumulating all changes in a single transaction.
 * <p>
 * Example:
 * <pre>
 * UNWIND [1, 2, 3] AS x
 * CALL { WITH x RETURN x * 10 AS y }
 * RETURN x, y
 * </pre>
 * Produces: {x:1, y:10}, {x:2, y:20}, {x:3, y:30}
 */
public class SubqueryStep extends AbstractExecutionStep {
  private final SubqueryClause subqueryClause;
  private final DatabaseInternal database;
  private final Map<String, Object> parameters;
  private final ExpressionEvaluator expressionEvaluator;

  public SubqueryStep(final SubqueryClause subqueryClause, final CommandContext context,
                       final DatabaseInternal database, final Map<String, Object> parameters,
                       final ExpressionEvaluator expressionEvaluator) {
    super(context);
    this.subqueryClause = subqueryClause;
    this.database = database;
    this.parameters = parameters;
    this.expressionEvaluator = expressionEvaluator;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (subqueryClause.isInTransactions())
      return syncPullInTransactions(context, nRecords);

    return syncPullNormal(context, nRecords);
  }

  private ResultSet syncPullNormal(final CommandContext context, final int nRecords) {
    final boolean hasPrevious = prev != null;

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      private Iterator<Result> currentInnerResults = null;
      private Result currentOuterRow = null;

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

        if (prevResults == null) {
          if (hasPrevious)
            prevResults = prev.syncPull(context, nRecords);
          else {
            prevResults = new ResultSet() {
              private boolean consumed = false;

              @Override
              public boolean hasNext() {
                return !consumed;
              }

              @Override
              public Result next() {
                if (consumed)
                  throw new NoSuchElementException();
                consumed = true;
                return new ResultInternal();
              }

              @Override
              public void close() {
              }
            };
          }
        }

        while (buffer.size() < n) {
          // If we have inner results from a previous outer row, continue consuming them
          if (currentInnerResults != null && currentInnerResults.hasNext()) {
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (context.isProfiling())
                rowCount++;

              final Result innerRow = currentInnerResults.next();
              buffer.add(mergeResults(currentOuterRow, innerRow));
            } finally {
              if (context.isProfiling())
                cost += (System.nanoTime() - begin);
            }
            continue;
          }

          // Need a new outer row
          if (!prevResults.hasNext()) {
            finished = true;
            break;
          }

          currentOuterRow = prevResults.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            // Execute the inner query seeded with the outer row
            final List<Result> innerResults = executeInnerQuery(currentOuterRow, context);

            if (innerResults.isEmpty()) {
              if (subqueryClause.isOptional()) {
                // OPTIONAL CALL - produce the outer row with nulls for inner columns
                buffer.add(currentOuterRow);
              }
              // Non-optional: no output rows for this outer row
              currentInnerResults = null;
            } else {
              currentInnerResults = innerResults.iterator();
            }
          } finally {
            if (context.isProfiling())
              cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {
        SubqueryStep.this.close();
      }
    };
  }

  /**
   * IN TRANSACTIONS mode: collects all input rows, executes the inner query in batches,
   * committing after each batch.
   */
  private ResultSet syncPullInTransactions(final CommandContext context, final int nRecords) {
    final boolean hasPrevious = prev != null;

    // Resolve batch size
    final int batchSize = resolveBatchSize(context);

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
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

        if (prevResults == null) {
          if (hasPrevious)
            prevResults = prev.syncPull(context, nRecords);
          else {
            prevResults = new ResultSet() {
              private boolean consumed = false;

              @Override
              public boolean hasNext() {
                return !consumed;
              }

              @Override
              public Result next() {
                if (consumed)
                  throw new NoSuchElementException();
                consumed = true;
                return new ResultInternal();
              }

              @Override
              public void close() {
              }
            };
          }
        }

        // Collect a batch of outer rows
        final List<Result> batch = new ArrayList<>();
        while (batch.size() < batchSize && prevResults.hasNext())
          batch.add(prevResults.next());

        if (batch.isEmpty()) {
          finished = true;
          return;
        }

        // Execute the batch in its own transaction
        database.begin();
        try {
          for (final Result outerRow : batch) {
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (context.isProfiling())
                rowCount++;

              final List<Result> innerResults = executeInnerQuery(outerRow, context);

              if (innerResults.isEmpty()) {
                if (subqueryClause.isOptional())
                  buffer.add(outerRow);
              } else {
                for (final Result innerRow : innerResults)
                  buffer.add(mergeResults(outerRow, innerRow));
              }
            } finally {
              if (context.isProfiling())
                cost += (System.nanoTime() - begin);
            }
          }
          database.commit();
        } catch (final Exception e) {
          database.rollbackAllNested();
          throw e;
        }

        // If no more input, mark finished
        if (!prevResults.hasNext())
          finished = true;
      }

      @Override
      public void close() {
        SubqueryStep.this.close();
      }
    };
  }

  private int resolveBatchSize(final CommandContext context) {
    final Expression batchSizeExpr = subqueryClause.getBatchSize();
    if (batchSizeExpr == null)
      return subqueryClause.getDefaultBatchSize();

    final Object value = expressionEvaluator.evaluate(batchSizeExpr, new ResultInternal(), context);
    if (value instanceof Number number)
      return number.intValue();

    throw new CommandExecutionException("IN TRANSACTIONS batch size must be a number, got: " + value);
  }

  /**
   * Executes the inner query with the outer row as the initial seed.
   * The seed row provides variables for the inner query's WITH clause.
   */
  private List<Result> executeInnerQuery(final Result outerRow, final CommandContext context) {
    final CypherStatement innerStatement = subqueryClause.getInnerStatement();

    final CypherExecutionPlan innerPlan = new CypherExecutionPlan(
        database, innerStatement, parameters, database.getConfiguration(), null, expressionEvaluator);

    final ResultSet resultSet = innerPlan.executeWithSeedRow(outerRow);

    final List<Result> results = new ArrayList<>();
    while (resultSet.hasNext())
      results.add(resultSet.next());

    return results;
  }

  /**
   * Merges the outer row and inner row into a single output row.
   * The outer row's properties are preserved, and inner row's properties are added.
   */
  private ResultInternal mergeResults(final Result outerRow, final Result innerRow) {
    final ResultInternal merged = new ResultInternal();

    // Copy outer row properties
    for (final String prop : outerRow.getPropertyNames())
      merged.setProperty(prop, outerRow.getProperty(prop));

    // Add inner row properties (may override outer if names clash, which is correct per Cypher semantics)
    for (final String prop : innerRow.getPropertyNames())
      merged.setProperty(prop, innerRow.getProperty(prop));

    return merged;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    builder.append(ind);
    if (subqueryClause.isOptional())
      builder.append("+ OPTIONAL ");
    else
      builder.append("+ ");
    builder.append("CALL SUBQUERY { ... }");
    if (subqueryClause.isInTransactions())
      builder.append(" IN TRANSACTIONS");
    if (context.isProfiling())
      builder.append(" (").append(getCostFormatted()).append(")");
    return builder.toString();
  }
}
