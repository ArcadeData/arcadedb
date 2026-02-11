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

import com.arcadedb.database.Document;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Execution step for filtering results based on WHERE clause conditions.
 * <p>
 * Phase 3: Simple property comparison (n.prop > value)
 * TODO: Full expression evaluation in later phases
 */
public class FilterPropertiesStep extends AbstractExecutionStep {
  private final WhereClause whereClause;

  // Simple pattern for basic property comparisons: variable.property operator value
  private static final Pattern COMPARISON_PATTERN = Pattern.compile("(\\w+)\\.(\\w+)\\s*([><=!]+)\\s*(\\w+|'[^']*'|\"[^\"]*\"|\\d+(?:\\.\\d+)?)");

  public FilterPropertiesStep(final WhereClause whereClause, final CommandContext context) {
    super(context);
    this.whereClause = whereClause;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("FilterPropertiesStep requires a previous step");

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;

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
          prevResults = prev.syncPull(context, nRecords);
        }

        // Fetch and filter up to n results from previous step
        while (buffer.size() < n && prevResults.hasNext()) {
          final Result result = prevResults.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            // Evaluate filter condition
            if (evaluateCondition(result)) {
              buffer.add(result);
            }
          } finally {
            if (context.isProfiling())
              cost += (System.nanoTime() - begin);
          }
        }

        if (!prevResults.hasNext()) {
          finished = true;
        }
      }

      @Override
      public void close() {
        FilterPropertiesStep.this.close();
      }
    };
  }

  /**
   * Evaluates the WHERE condition for a result.
   * Uses new BooleanExpression framework with fallback to legacy string parsing.
   */
  private boolean evaluateCondition(final Result result) {
    if (whereClause == null) {
      return true;
    }

    // Try new BooleanExpression approach first
    final BooleanExpression conditionExpr = whereClause.getConditionExpression();
    if (conditionExpr != null) {
      return conditionExpr.evaluate(result, context);
    }

    // Fall back to legacy string-based parsing for backward compatibility
    final String condition = whereClause.getCondition();
    if (condition == null || condition.trim().isEmpty()) {
      return true;
    }

    // Parse simple comparison: n.property > value
    final Matcher matcher = COMPARISON_PATTERN.matcher(condition);
    if (matcher.find()) {
      final String variable = matcher.group(1);
      final String property = matcher.group(2);
      final String operator = matcher.group(3);
      String value = matcher.group(4);

      // Remove quotes from string values
      if (value.startsWith("'") && value.endsWith("'")) {
        value = value.substring(1, value.length() - 1);
      } else if (value.startsWith("\"") && value.endsWith("\"")) {
        value = value.substring(1, value.length() - 1);
      }

      // Get the object from result
      final Object obj = result.getProperty(variable);
      if (obj instanceof Document) {
        final Document doc = (Document) obj;
        if (!doc.has(property)) {
          return false;
        }

        final Object propValue = doc.get(property);
        return compareValues(propValue, operator, value);
      }
    }

    // If we can't parse it, default to true (don't filter)
    return true;
  }

  /**
   * Compares two values based on the operator.
   */
  private boolean compareValues(final Object left, final String operator, final String right) {
    if (left == null) {
      return false;
    }

    try {
      // Try numeric comparison
      if (left instanceof Number) {
        final double leftNum = ((Number) left).doubleValue();
        final double rightNum = Double.parseDouble(right);

        return switch (operator) {
          case ">" -> leftNum > rightNum;
          case ">=" -> leftNum >= rightNum;
          case "<" -> leftNum < rightNum;
          case "<=" -> leftNum <= rightNum;
          case "=" -> leftNum == rightNum;
          case "!=" -> leftNum != rightNum;
          default -> false;
        };
      }

      // String comparison
      final String leftStr = left.toString();
      final int comparison = leftStr.compareTo(right);

      return switch (operator) {
        case ">" -> comparison > 0;
        case ">=" -> comparison >= 0;
        case "<" -> comparison < 0;
        case "<=" -> comparison <= 0;
        case "=" -> comparison == 0;
        case "!=" -> comparison != 0;
        default -> false;
      };
    } catch (final Exception e) {
      // If comparison fails, return false
      return false;
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FILTER WHERE ");
    if (whereClause != null) {
      builder.append(whereClause.getCondition());
    }
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
