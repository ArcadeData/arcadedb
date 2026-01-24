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
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionExecutor;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Execution step for GROUP BY aggregation.
 * Implements OpenCypher's implicit grouping: when a RETURN clause contains both
 * aggregation functions and non-aggregated expressions, the non-aggregated
 * expressions become grouping keys.
 *
 * Example:
 *   MATCH (n:Person) RETURN n.city, count(n), avg(n.age)
 *
 * This implicitly groups by n.city and computes count(n) and avg(n.age) for each city.
 */
public class GroupByAggregationStep extends AbstractExecutionStep {
  private final ReturnClause returnClause;
  private final CypherFunctionFactory functionFactory;
  private final ExpressionEvaluator evaluator;

  public GroupByAggregationStep(final ReturnClause returnClause, final CommandContext context,
      final CypherFunctionFactory functionFactory) {
    super(context);
    this.returnClause = returnClause;
    this.functionFactory = functionFactory;
    this.evaluator = new ExpressionEvaluator(functionFactory);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("GroupByAggregationStep requires a previous step");

    // Pull all results from previous step
    final ResultSet prevResults = prev.syncPull(context, Integer.MAX_VALUE);

    // Identify grouping keys (non-aggregated expressions) and aggregations
    final List<GroupingKey> groupingKeys = new ArrayList<>();
    final List<AggregationItem> aggregationItems = new ArrayList<>();

    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
      final Expression expr = item.getExpression();
      if (expr.isAggregation() && expr instanceof FunctionCallExpression) {
        aggregationItems.add(new AggregationItem(item.getOutputName(), (FunctionCallExpression) expr));
      } else {
        groupingKeys.add(new GroupingKey(item.getOutputName(), expr));
      }
    }

    // Group rows by grouping key values
    // Map: GroupKey -> List of Result rows in that group
    final Map<GroupKeyValues, List<Result>> groups = new LinkedHashMap<>();

    while (prevResults.hasNext()) {
      final Result inputRow = prevResults.next();

      // Evaluate grouping key for this row
      final GroupKeyValues keyValues = evaluateGroupingKey(groupingKeys, inputRow, context);

      // Add row to its group
      groups.computeIfAbsent(keyValues, k -> new ArrayList<>()).add(inputRow);
    }

    // Process each group and compute aggregations
    final List<Result> results = new ArrayList<>();

    for (final Map.Entry<GroupKeyValues, List<Result>> groupEntry : groups.entrySet()) {
      final GroupKeyValues keyValues = groupEntry.getKey();
      final List<Result> groupRows = groupEntry.getValue();

      // Create aggregators for this group
      final Map<String, CypherFunctionExecutor> aggregators = new HashMap<>();
      for (final AggregationItem aggItem : aggregationItems) {
        // Pass the DISTINCT flag to create the appropriate function executor
        final CypherFunctionExecutor executor = functionFactory.getFunctionExecutor(
            aggItem.funcExpr.getFunctionName(), aggItem.funcExpr.isDistinct());
        aggregators.put(aggItem.outputName, executor);
      }

      // Process all rows in this group
      for (final Result groupRow : groupRows) {
        for (final AggregationItem aggItem : aggregationItems) {
          final CypherFunctionExecutor executor = aggregators.get(aggItem.outputName);

          // Evaluate function arguments for this row
          final Object[] args = new Object[aggItem.funcExpr.getArguments().size()];
          for (int i = 0; i < args.length; i++) {
            args[i] = evaluator.evaluate(aggItem.funcExpr.getArguments().get(i), groupRow, context);
          }

          // Feed this row's data to the aggregator
          executor.execute(args, context);
        }
      }

      // Build result for this group
      final ResultInternal groupResult = new ResultInternal();

      // Add grouping key values
      for (int i = 0; i < groupingKeys.size(); i++) {
        groupResult.setProperty(groupingKeys.get(i).outputName, keyValues.values.get(i));
      }

      // Add aggregated values
      for (final Map.Entry<String, CypherFunctionExecutor> entry : aggregators.entrySet()) {
        final Object aggregatedValue = entry.getValue().getAggregatedResult();
        groupResult.setProperty(entry.getKey(), aggregatedValue);
      }

      results.add(groupResult);
    }

    return new ResultSet() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < results.size();
      }

      @Override
      public Result next() {
        return results.get(index++);
      }

      @Override
      public void close() {
        GroupByAggregationStep.this.close();
      }
    };
  }

  /**
   * Evaluate grouping key expressions for a given row.
   */
  private GroupKeyValues evaluateGroupingKey(final List<GroupingKey> keys, final Result row, final CommandContext context) {
    final List<Object> values = new ArrayList<>();
    for (final GroupingKey key : keys) {
      final Object value = evaluator.evaluate(key.expression, row, context);
      values.add(value);
    }
    return new GroupKeyValues(values);
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    builder.append(ind);
    builder.append("+ GROUP BY AGGREGATION ");

    // Show grouping keys
    final List<String> groupKeys = new ArrayList<>();
    final List<String> aggFuncs = new ArrayList<>();
    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
      if (item.getExpression().isAggregation()) {
        aggFuncs.add(item.getExpression().getText());
      } else {
        groupKeys.add(item.getExpression().getText());
      }
    }

    if (!groupKeys.isEmpty()) {
      builder.append("[").append(String.join(", ", groupKeys)).append("] ");
    }
    builder.append(String.join(", ", aggFuncs));

    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }

  /**
   * Represents a grouping key (non-aggregated expression).
   */
  private static class GroupingKey {
    final String outputName;
    final Expression expression;

    GroupingKey(final String outputName, final Expression expression) {
      this.outputName = outputName;
      this.expression = expression;
    }
  }

  /**
   * Represents an aggregation item.
   */
  private static class AggregationItem {
    final String outputName;
    final FunctionCallExpression funcExpr;

    AggregationItem(final String outputName, final FunctionCallExpression funcExpr) {
      this.outputName = outputName;
      this.funcExpr = funcExpr;
    }
  }

  /**
   * Represents the values of grouping keys for a specific group.
   * Used as a map key to identify groups.
   */
  private static class GroupKeyValues {
    final List<Object> values;

    GroupKeyValues(final List<Object> values) {
      this.values = values;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      final GroupKeyValues that = (GroupKeyValues) o;
      return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(values);
    }
  }
}
