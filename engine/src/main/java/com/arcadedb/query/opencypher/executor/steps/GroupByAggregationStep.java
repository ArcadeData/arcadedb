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
    // Wrapped aggregations: non-aggregation functions that wrap aggregations, e.g., HEAD(COLLECT(...))
    final List<WrappedAggregationItem> wrappedAggregationItems = new ArrayList<>();

    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
      final Expression expr = item.getExpression();
      if (expr.isAggregation() && expr instanceof FunctionCallExpression) {
        aggregationItems.add(new AggregationItem(item.getOutputName(), (FunctionCallExpression) expr));
      } else if (expr instanceof FunctionCallExpression) {
        // Check if this is a wrapper around an aggregation, e.g., HEAD(COLLECT(...))
        final FunctionCallExpression funcExpr = (FunctionCallExpression) expr;
        final FunctionCallExpression innerAgg = findInnerAggregation(funcExpr);
        if (innerAgg != null) {
          wrappedAggregationItems.add(new WrappedAggregationItem(item.getOutputName(), funcExpr, innerAgg));
        } else {
          groupingKeys.add(new GroupingKey(item.getOutputName(), expr));
        }
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

      // Create aggregators for wrapped aggregations (using the inner aggregation function)
      final Map<String, CypherFunctionExecutor> wrappedAggregators = new HashMap<>();
      for (final WrappedAggregationItem wrappedItem : wrappedAggregationItems) {
        final CypherFunctionExecutor executor = functionFactory.getFunctionExecutor(
            wrappedItem.innerAggregation.getFunctionName(), wrappedItem.innerAggregation.isDistinct());
        wrappedAggregators.put(wrappedItem.outputName, executor);
      }

      // Process all rows in this group
      for (final Result groupRow : groupRows) {
        // Process regular aggregations
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

        // Process inner aggregations for wrapped aggregation items
        for (final WrappedAggregationItem wrappedItem : wrappedAggregationItems) {
          final CypherFunctionExecutor executor = wrappedAggregators.get(wrappedItem.outputName);

          // Evaluate arguments for the inner aggregation
          final Object[] args = new Object[wrappedItem.innerAggregation.getArguments().size()];
          for (int i = 0; i < args.length; i++) {
            args[i] = evaluator.evaluate(wrappedItem.innerAggregation.getArguments().get(i), groupRow, context);
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

      // Add wrapped aggregated values (apply wrapper function to aggregated result)
      for (final WrappedAggregationItem wrappedItem : wrappedAggregationItems) {
        final Object innerAggregatedValue = wrappedAggregators.get(wrappedItem.outputName).getAggregatedResult();
        // Apply the wrapper function to the aggregated result
        final CypherFunctionExecutor wrapperExecutor = functionFactory.getFunctionExecutor(
            wrappedItem.wrapperFunction.getFunctionName());
        final Object wrappedValue = wrapperExecutor.execute(new Object[]{innerAggregatedValue}, context);
        groupResult.setProperty(wrappedItem.outputName, wrappedValue);
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

  /**
   * Represents a wrapped aggregation item - a non-aggregation function that wraps an aggregation.
   * Example: HEAD(COLLECT(...)) where HEAD is the wrapper and COLLECT is the inner aggregation.
   */
  private static class WrappedAggregationItem {
    final String outputName;
    final FunctionCallExpression wrapperFunction;
    final FunctionCallExpression innerAggregation;

    WrappedAggregationItem(final String outputName, final FunctionCallExpression wrapperFunction,
        final FunctionCallExpression innerAggregation) {
      this.outputName = outputName;
      this.wrapperFunction = wrapperFunction;
      this.innerAggregation = innerAggregation;
    }
  }

  /**
   * Find an inner aggregation function in a function call expression.
   * Returns the first aggregation function found in the arguments, or null if none.
   * This is used to detect patterns like HEAD(COLLECT(...)) where the outer function
   * is not an aggregation but wraps one.
   */
  private static FunctionCallExpression findInnerAggregation(final FunctionCallExpression funcExpr) {
    for (final Expression arg : funcExpr.getArguments()) {
      if (arg instanceof FunctionCallExpression) {
        final FunctionCallExpression argFunc = (FunctionCallExpression) arg;
        if (argFunc.isAggregation()) {
          return argFunc;
        }
        // Recursively check for nested function calls
        final FunctionCallExpression nested = findInnerAggregation(argFunc);
        if (nested != null) {
          return nested;
        }
      }
    }
    return null;
  }
}
