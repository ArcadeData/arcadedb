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
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Execution step for aggregating results.
 * Handles aggregation functions like COUNT, SUM, AVG, MIN, MAX.
 * Consumes all input rows and produces a single aggregated result row.
 */
public class AggregationStep extends AbstractExecutionStep {
  private final ReturnClause returnClause;
  private final CypherFunctionFactory functionFactory;
  private final ExpressionEvaluator evaluator;

  public AggregationStep(final ReturnClause returnClause, final CommandContext context,
      final CypherFunctionFactory functionFactory) {
    super(context);
    this.returnClause = returnClause;
    this.functionFactory = functionFactory;
    this.evaluator = new ExpressionEvaluator(functionFactory);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("AggregationStep requires a previous step");

    // Pull all results from previous step and aggregate
    final ResultSet prevResults = prev.syncPull(context, Integer.MAX_VALUE);

    // Map of aggregation functions (one per aggregation expression)
    final Map<String, StatelessFunction> aggregators = new HashMap<>();
    final Map<String, Expression> aggregationExpressions = new HashMap<>();

    // Wrapped aggregations: non-aggregation functions that wrap aggregations, e.g., HEAD(COLLECT(...))
    final Map<String, WrappedAggregationInfo> wrappedAggregators = new HashMap<>();

    final List<String> nonAggregationItems = new ArrayList<>();

    // Identify aggregation functions and create function instances
    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
      final Expression expr = item.getExpression();
      if (expr.isAggregation() && expr instanceof FunctionCallExpression) {
        final FunctionCallExpression funcExpr = (FunctionCallExpression) expr;
        // Pass the DISTINCT flag to create the appropriate function instance
        final StatelessFunction function = functionFactory.getFunctionExecutor(
            funcExpr.getFunctionName(), funcExpr.isDistinct());
        aggregators.put(item.getOutputName(), function);
        aggregationExpressions.put(item.getOutputName(), expr);
      } else if (expr instanceof FunctionCallExpression) {
        // Check if this is a wrapper around an aggregation, e.g., HEAD(COLLECT(...))
        final FunctionCallExpression funcExpr = (FunctionCallExpression) expr;
        final FunctionCallExpression innerAgg = findInnerAggregation(funcExpr);
        if (innerAgg != null) {
          // Create aggregator for the inner aggregation
          final StatelessFunction innerFunction = functionFactory.getFunctionExecutor(
              innerAgg.getFunctionName(), innerAgg.isDistinct());
          wrappedAggregators.put(item.getOutputName(),
              new WrappedAggregationInfo(funcExpr, innerAgg, innerFunction));
        } else {
          nonAggregationItems.add(item.getOutputName());
        }
      } else {
        nonAggregationItems.add(item.getOutputName());
      }
    }

    // Process all rows, feeding data to aggregators
    while (prevResults.hasNext()) {
      final Result inputRow = prevResults.next();

      // Process regular aggregations
      for (final Map.Entry<String, Expression> entry : aggregationExpressions.entrySet()) {
        final String outputName = entry.getKey();
        final Expression expr = entry.getValue();
        final StatelessFunction function = aggregators.get(outputName);

        // Evaluate the function arguments for this row
        if (expr instanceof FunctionCallExpression) {
          final FunctionCallExpression funcExpr = (FunctionCallExpression) expr;
          final Object[] args = new Object[funcExpr.getArguments().size()];
          for (int i = 0; i < args.length; i++) {
            args[i] = evaluator.evaluate(funcExpr.getArguments().get(i), inputRow, context);
          }

          // Feed this row's data to the aggregator
          function.execute(args, context);
        }
      }

      // Process wrapped aggregations - feed data to inner aggregation
      for (final WrappedAggregationInfo wrappedInfo : wrappedAggregators.values()) {
        final Object[] args = new Object[wrappedInfo.innerAggregation.getArguments().size()];
        for (int i = 0; i < args.length; i++) {
          args[i] = evaluator.evaluate(wrappedInfo.innerAggregation.getArguments().get(i), inputRow, context);
        }
        wrappedInfo.innerFunction.execute(args, context);
      }
    }

    // Build final aggregated result
    final ResultInternal aggregatedResult = new ResultInternal();

    // Get aggregated values
    for (final Map.Entry<String, StatelessFunction> entry : aggregators.entrySet()) {
      final String outputName = entry.getKey();
      final StatelessFunction function = entry.getValue();
      final Object aggregatedValue = function.getAggregatedResult();
      aggregatedResult.setProperty(outputName, aggregatedValue);
    }

    // Get wrapped aggregated values (apply wrapper function to aggregated result)
    for (final Map.Entry<String, WrappedAggregationInfo> entry : wrappedAggregators.entrySet()) {
      final String outputName = entry.getKey();
      final WrappedAggregationInfo wrappedInfo = entry.getValue();

      // Get the aggregated result from the inner function
      final Object innerAggregatedValue = wrappedInfo.innerFunction.getAggregatedResult();

      // Apply the wrapper function to the aggregated result
      final StatelessFunction wrapperFunction = functionFactory.getFunctionExecutor(
          wrappedInfo.wrapperFunction.getFunctionName());
      final Object wrappedValue = wrapperFunction.execute(new Object[]{innerAggregatedValue}, context);

      aggregatedResult.setProperty(outputName, wrappedValue);
    }

    // Return single aggregated row
    final List<Result> results = new ArrayList<>();
    results.add(aggregatedResult);

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
        AggregationStep.this.close();
      }
    };
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

  /**
   * Holds information about a wrapped aggregation.
   */
  private static class WrappedAggregationInfo {
    final FunctionCallExpression wrapperFunction;
    final FunctionCallExpression innerAggregation;
    final StatelessFunction innerFunction;

    WrappedAggregationInfo(final FunctionCallExpression wrapperFunction,
                           final FunctionCallExpression innerAggregation,
                           final StatelessFunction innerFunction) {
      this.wrapperFunction = wrapperFunction;
      this.innerAggregation = innerAggregation;
      this.innerFunction = innerFunction;
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    builder.append(ind);
    builder.append("+ AGGREGATION ");

    // Show aggregation functions (including wrapped aggregations)
    final List<String> aggFuncs = new ArrayList<>();
    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
      if (item.getExpression().containsAggregation()) {
        aggFuncs.add(item.getExpression().getText());
      }
    }
    builder.append(String.join(", ", aggFuncs));

    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }
}
