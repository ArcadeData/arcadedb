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
 *
 SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.DistinctNumericKey;
import com.arcadedb.query.opencypher.InternalVariables;
import com.arcadedb.query.opencypher.LoadCSVRowContext;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.HeapEstimator;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

/**
 * Execution step for WITH clause.
 * WITH allows query chaining by projecting, filtering, and transforming results
 * before passing them to the next part of the query.
 * <p>
 * Features:
 * - Projection (select and alias columns)
 * - DISTINCT (remove duplicates)
 * - WHERE filtering (using merged scope: both pre-projection and projected variables)
 * - ORDER BY, SKIP, LIMIT
 * - Aggregation support
 * <p>
 * Examples:
 * - MATCH (a:Person) WITH a.name AS name, a.age AS age WHERE age > 30 RETURN name
 * - MATCH (a:Person) WITH a ORDER BY a.name LIMIT 10 MATCH (a)-[:KNOWS]->(b) RETURN a, b
 * - MATCH (a:Person) WITH count(a) AS cnt WHERE cnt > 5 RETURN cnt
 */
public class WithStep extends AbstractExecutionStep {
  private final WithClause withClause;
  private final ExpressionEvaluator evaluator;
  private final boolean skipLimitDeferred;
  // SET BY THE PLANNER WHEN ANY WRITE PRECEDES THIS WITH (EVEN ONE AN EARLIER BARRIER COVERS, WHICH THEN DRAINS A SECOND TIME AT NO COST):
  // A LIMIT 0 THEN STILL DRAINS ITS INPUT, WHICH IS WHAT RUNS THE WRITES BEHIND IT
  private boolean drainOnZeroLimit;

  // THE KEYS A DISTINCT REMEMBERS, UNDER THE PER-OPERATION CAP AND THE HEAP BUDGET OF ALL THE QUERIES (ISSUES #8585,
  // #8591). ON THE STEP: THE CLOSE() OF A QUERY REACHES THE STEPS, NOT THEIR RESULT SETS
  private OperationHeapLimit heapLimit;

  public WithStep(final WithClause withClause, final CommandContext context,
                  final CypherFunctionFactory functionFactory) {
    super(context);
    this.withClause = withClause;
    this.evaluator = new ExpressionEvaluator(functionFactory);
    // Defer SKIP/LIMIT to downstream steps when ORDER BY is present,
    // so sorting happens before pagination
    this.skipLimitDeferred = withClause.getOrderByClause() != null;
  }

  /** Makes a {@code LIMIT 0} drain its input, like {@code LimitStep} does, so the writes behind it run. */
  public void setDrainOnZeroLimit(final boolean drainOnZeroLimit) {
    this.drainOnZeroLimit = drainOnZeroLimit;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final boolean hasPrevious = prev != null;

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      private final Set<List<Object>> seenResults = withClause.isDistinct() ? new HashSet<>() : null;
      private final OperationHeapLimit distinctLimit = withClause.isDistinct() ? distinctHeapLimit(context, "WITH DISTINCT") : null;
      private int skipped = 0;
      private int returned = 0;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size()) {
          return true;
        }

        if (finished) {
          return false;
        }

        // Fetch more results
        fetchMore(nRecords);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        // Initialize prevResults on first call
        if (prevResults == null) {
          if (hasPrevious) {
            prevResults = prev.syncPull(context, nRecords);
          } else {
            // No previous step - create a single empty input row
            // This allows standalone WITH at the start of a query (e.g. WITH 1 AS x ...)
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

        // Evaluate SKIP/LIMIT expressions (only when not deferred to downstream)
        final Integer limit = !skipLimitDeferred && withClause.getLimit() != null
            ? evaluator.evaluateSkipLimit(withClause.getLimit(), new ResultInternal(), context) : null;
        final Integer skipVal = !skipLimitDeferred && withClause.getSkip() != null
            ? evaluator.evaluateSkipLimit(withClause.getSkip(), new ResultInternal(), context) : null;

        // Check if LIMIT has been reached
        if (limit != null && returned >= limit) {
          // LIMIT 0 returns nothing, but like LimitStep it still drains the input so the writes behind it run
          if (limit == 0 && drainOnZeroLimit) {
            final WorkGuard guard = WorkGuard.forCommandDeadline(context);
            while (prevResults.hasNext()) {
              guard.check();
              prevResults.next();
            }
          }
          finish();
          return;
        }

        // Fetch up to n results from previous step
        while (buffer.size() < n && prevResults.hasNext()) {
          if (limit != null && returned >= limit)
            break;

          final Result inputResult = prevResults.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            // Project the result
            final ResultInternal projectedResult = projectResult(inputResult);

            // Apply WHERE clause filtering using a merged scope that contains
            // both the pre-projection variables AND the projected aliases.
            // In Cypher, WITH c WHERE r IS NULL can reference 'r' (pre-projection)
            // and WITH a.name2 AS name WHERE name = 'B' can reference 'name' (projected).
            if (withClause.getWhereClause() != null) {
              final ResultInternal mergedScope = new ResultInternal();
              for (final String prop : inputResult.getPropertyNames())
                mergedScope.setProperty(prop, inputResult.getProperty(prop));
              for (final String prop : projectedResult.getPropertyNames())
                mergedScope.setProperty(prop, projectedResult.getProperty(prop));
              if (!evaluateWhereClause(mergedScope))
                continue;
            }

            // Apply DISTINCT
            if (withClause.isDistinct()) {
              // Build the key from the projected properties' canonicalized values rather than
              // projectedResult.toString(): a document/vertex/edge's toString() decorates its RID
              // with the record's deserialized properties, and a not-yet-loaded property buffer
              // renders as a placeholder instead of the actual values, so two references to the
              // very same record can render two different strings depending on load state alone
              // (issue #6488).
              // WITH * forwards every property from the input row, including the executor's own
              // internal bindings for anonymous pattern elements; those must not affect distinctness,
              // the same reasoning ProjectReturnStep already applies to RETURN DISTINCT * (issue
              // #5444), or two rows that only differ by such a binding would wrongly stay distinct
              // (issue #6541).
              final List<String> names = new TreeSet<>(projectedResult.getPropertyNames()).stream()
                  .filter(name -> !InternalVariables.isInternal(name)).toList();
              final List<Object> resultKey = DistinctNumericKey.buildKey(names, projectedResult::getProperty);
              if (!seenResults.add(resultKey))
                continue;
              distinctLimit.add(seenResults.size(), resultKey, HeapEstimator.HASH_ENTRY_BYTES);
            }

            // Apply SKIP (only when not deferred to downstream)
            if (skipVal != null && skipped < skipVal) {
              skipped++;
              continue;
            }

            // When ORDER BY is present, output a merged scope so ORDER BY can reference
            // variables from the incoming scope that aren't in the projection.
            // E.g., WITH a, expr AS mod ORDER BY sum — 'sum' is from the incoming scope.
            // A downstream step will strip back to just the projected variables.
            if (skipLimitDeferred) {
              final ResultInternal merged = new ResultInternal();
              for (final String prop : inputResult.getPropertyNames())
                merged.setProperty(prop, inputResult.getProperty(prop));
              for (final String prop : projectedResult.getPropertyNames())
                merged.setProperty(prop, projectedResult.getProperty(prop));
              buffer.add(merged);
            } else {
              buffer.add(projectedResult);
            }
            returned++;
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }

        if (!prevResults.hasNext() || (limit != null && returned >= limit)) {
          finish();
        }
      }

      // No more input row: the DISTINCT keys are not needed anymore, even if the consumer keeps the result set open
      private void finish() {
        finished = true;
        if (seenResults != null) {
          seenResults.clear();
          distinctLimit.release();
        }
      }

      @Override
      public void close() {
        WithStep.this.close();
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
   * Projects a result according to the WITH clause items.
   * Handles WITH *, WITH *, extra AS alias, and WITH expr AS alias.
   */
  private ResultInternal projectResult(final Result inputResult) {
    final ResultInternal result = new ResultInternal();

    for (final ReturnClause.ReturnItem item : withClause.getItems()) {
      if (item.isStar()) {
        // WITH * — copy all properties from input
        for (final String prop : inputResult.getPropertyNames())
          result.setProperty(prop, inputResult.getProperty(prop));
      } else {
        // Evaluate and project the expression
        final Object value = evaluator.evaluate(item.getExpression(), inputResult, context);
        result.setProperty(item.getOutputName(), value);
      }
    }

    // The LOAD CSV file()/linenumber() pair describes the row, not the projection, so it survives one (issue #6402).
    LoadCSVRowContext.carryOver(inputResult, result);

    return result;
  }

  /**
   * Evaluates WHERE clause predicate on the input result (before projection).
   */
  private boolean evaluateWhereClause(final Result inputResult) {
    final BooleanExpression predicate =
        withClause.getWhereClause().getConditionExpression();
    return Boolean.TRUE.equals(predicate.evaluateTernary(inputResult, context));
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ WITH ");

    // Show projection
    final List<String> projectionStrings = new ArrayList<>();
    for (final ReturnClause.ReturnItem item : withClause.getItems()) {
      if (item.getAlias() != null) {
        projectionStrings.add(item.getExpression().getText() + " AS " + item.getAlias());
      } else {
        projectionStrings.add(item.getExpression().getText());
      }
    }
    builder.append(String.join(", ", projectionStrings));

    // Show DISTINCT
    if (withClause.isDistinct()) {
      builder.append(" DISTINCT");
    }

    // Show WHERE
    if (withClause.getWhereClause() != null) {
      builder.append(" WHERE ").append(withClause.getWhereClause().getConditionExpression().getText());
    }

    // Show ORDER BY
    if (withClause.getOrderByClause() != null) {
      builder.append(" ORDER BY ...");
    }

    // Show SKIP
    if (withClause.getSkip() != null) {
      builder.append(" SKIP ").append(withClause.getSkip().getText());
    }

    // Show LIMIT
    if (withClause.getLimit() != null) {
      builder.append(" LIMIT ").append(withClause.getLimit().getText());
    }

    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }

    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
