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
package com.arcadedb.opencypher.executor.steps;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.opencypher.ast.Expression;
import com.arcadedb.opencypher.ast.FunctionCallExpression;
import com.arcadedb.opencypher.ast.ReturnClause;
import com.arcadedb.opencypher.executor.CypherFunctionExecutor;
import com.arcadedb.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.opencypher.executor.ExpressionEvaluator;
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

    // Map of aggregation function executors (one per aggregation expression)
    final Map<String, CypherFunctionExecutor> aggregators = new HashMap<>();
    final Map<String, Expression> aggregationExpressions = new HashMap<>();
    final List<String> nonAggregationItems = new ArrayList<>();

    // Identify aggregation functions and create executors
    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
      final Expression expr = item.getExpression();
      if (expr.isAggregation() && expr instanceof FunctionCallExpression) {
        final FunctionCallExpression funcExpr = (FunctionCallExpression) expr;
        final CypherFunctionExecutor executor = functionFactory.getFunctionExecutor(funcExpr.getFunctionName());
        aggregators.put(item.getOutputName(), executor);
        aggregationExpressions.put(item.getOutputName(), expr);
      } else {
        nonAggregationItems.add(item.getOutputName());
      }
    }

    // Process all rows, feeding data to aggregators
    while (prevResults.hasNext()) {
      final Result inputRow = prevResults.next();

      for (final Map.Entry<String, Expression> entry : aggregationExpressions.entrySet()) {
        final String outputName = entry.getKey();
        final Expression expr = entry.getValue();
        final CypherFunctionExecutor executor = aggregators.get(outputName);

        // Evaluate the function arguments for this row
        if (expr instanceof FunctionCallExpression) {
          final FunctionCallExpression funcExpr = (FunctionCallExpression) expr;
          final Object[] args = new Object[funcExpr.getArguments().size()];
          for (int i = 0; i < args.length; i++) {
            args[i] = evaluator.evaluate(funcExpr.getArguments().get(i), inputRow, context);
          }

          // Feed this row's data to the aggregator
          executor.execute(args, context);
        }
      }
    }

    // Build final aggregated result
    final ResultInternal aggregatedResult = new ResultInternal();

    // Get aggregated values
    for (final Map.Entry<String, CypherFunctionExecutor> entry : aggregators.entrySet()) {
      final String outputName = entry.getKey();
      final CypherFunctionExecutor executor = entry.getValue();
      final Object aggregatedValue = executor.getAggregatedResult();
      aggregatedResult.setProperty(outputName, aggregatedValue);
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

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    builder.append(ind);
    builder.append("+ AGGREGATION ");

    // Show aggregation functions
    final List<String> aggFuncs = new ArrayList<>();
    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
      if (item.getExpression().isAggregation()) {
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
