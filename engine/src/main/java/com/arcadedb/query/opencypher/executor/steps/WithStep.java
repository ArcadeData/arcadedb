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
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

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

  public WithStep(final WithClause withClause, final CommandContext context,
                  final CypherFunctionFactory functionFactory) {
    super(context);
    this.withClause = withClause;
    this.evaluator = new ExpressionEvaluator(functionFactory);
    // Defer SKIP/LIMIT to downstream steps when ORDER BY is present,
    // so sorting happens before pagination
    this.skipLimitDeferred = withClause.getOrderByClause() != null;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final boolean hasPrevious = prev != null;

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      private final Set<String> seenResults = withClause.isDistinct() ? new HashSet<>() : null;
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
        final Integer limit = (!skipLimitDeferred && withClause.getLimit() != null)
            ? evaluator.evaluateSkipLimit(withClause.getLimit(), new ResultInternal(), context) : null;
        final Integer skipVal = (!skipLimitDeferred && withClause.getSkip() != null)
            ? evaluator.evaluateSkipLimit(withClause.getSkip(), new ResultInternal(), context) : null;

        // Check if LIMIT has been reached
        if (limit != null && returned >= limit) {
          finished = true;
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
              final String resultKey = projectedResult.toString();
              if (seenResults.contains(resultKey))
                continue;
              seenResults.add(resultKey);
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
              cost += (System.nanoTime() - begin);
          }
        }

        if (!prevResults.hasNext() || (limit != null && returned >= limit)) {
          finished = true;
        }
      }

      @Override
      public void close() {
        WithStep.this.close();
      }
    };
  }

  /**
   * Projects a result according to the WITH clause items.
   */
  private ResultInternal projectResult(final Result inputResult) {
    final ResultInternal result = new ResultInternal();

    // WITH * - copy all properties
    if (withClause.getItems().size() == 1) {
      final ReturnClause.ReturnItem item = withClause.getItems().get(0);
      if ("*".equals(item.getOutputName())) {
        for (final String prop : inputResult.getPropertyNames()) {
          result.setProperty(prop, inputResult.getProperty(prop));
        }
        return result;
      }
    }

    // Project specified items
    for (final ReturnClause.ReturnItem item : withClause.getItems()) {
      final Expression expr = item.getExpression();
      final String outputName = item.getOutputName();

      // Evaluate expression
      final Object value = evaluator.evaluate(expr, inputResult, context);
      result.setProperty(outputName, value);
    }

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
