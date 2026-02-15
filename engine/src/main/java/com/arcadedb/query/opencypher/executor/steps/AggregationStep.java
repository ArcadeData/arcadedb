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
import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.CaseExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpressionWrapper;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.ListComprehensionExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.ListPredicateExpression;
import com.arcadedb.query.opencypher.ast.MapExpression;
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

  // Configurable batch size for streaming aggregation (default: 10,000)
  private static final int DEFAULT_BATCH_SIZE = 10000;
  private static final int BATCH_SIZE = Integer.parseInt(
      System.getProperty("arcadedb.aggregation.batchSize", String.valueOf(DEFAULT_BATCH_SIZE)));

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

    // Map of aggregation functions (one per aggregation expression)
    final Map<String, StatelessFunction> aggregators = new HashMap<>();
    final Map<String, Expression> aggregationExpressions = new HashMap<>();

    // Complex aggregation expressions: arithmetic/other expressions containing aggregations
    // e.g., count(a) * 10 + count(b) * 5 AS x
    final Map<String, ComplexAggregationInfo> complexAggregations = new HashMap<>();

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
      } else if (expr.containsAggregation()) {
        // Complex expression containing aggregations (arithmetic, function wrapping, etc.)
        final Map<String, FunctionCallExpression> innerAggs = new HashMap<>();
        final Map<String, StatelessFunction> innerFunctions = new HashMap<>();
        collectAggregations(expr, innerAggs, innerFunctions);
        if (!innerAggs.isEmpty()) {
          complexAggregations.put(item.getOutputName(),
              new ComplexAggregationInfo(expr, innerAggs, innerFunctions));
        } else {
          nonAggregationItems.add(item.getOutputName());
        }
      } else {
        nonAggregationItems.add(item.getOutputName());
      }
    }

    // Process all rows, feeding data to aggregators
    final ResultSet prevResults = prev.syncPull(context, BATCH_SIZE);
    Result representativeRow = null;

    while (prevResults.hasNext()) {
      final Result inputRow = prevResults.next();
      if (representativeRow == null)
        representativeRow = inputRow;
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        if (context.isProfiling())
          rowCount++;

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

        // Process complex aggregation expressions - feed data to all inner aggregation functions
        for (final ComplexAggregationInfo complexInfo : complexAggregations.values()) {
          for (final Map.Entry<String, FunctionCallExpression> innerEntry : complexInfo.innerAggregations.entrySet()) {
            final FunctionCallExpression innerAgg = innerEntry.getValue();
            final StatelessFunction function = complexInfo.innerFunctions.get(innerEntry.getKey());
            final Object[] args = new Object[innerAgg.getArguments().size()];
            for (int i = 0; i < args.length; i++)
              args[i] = evaluator.evaluate(innerAgg.getArguments().get(i), inputRow, context);
            function.execute(args, context);
          }
        }
      } finally {
        if (context.isProfiling())
          cost += (System.nanoTime() - begin);
      }
    }

    // Build final aggregated result
    final long beginFinal = context.isProfiling() ? System.nanoTime() : 0;
    final ResultInternal aggregatedResult = new ResultInternal();
    try {
      // Get aggregated values
      for (final Map.Entry<String, StatelessFunction> entry : aggregators.entrySet()) {
        final String outputName = entry.getKey();
        final StatelessFunction function = entry.getValue();
        final Object aggregatedValue = function.getAggregatedResult();
        aggregatedResult.setProperty(outputName, aggregatedValue);
      }

      // Evaluate complex aggregation expressions using pre-computed aggregation values.
      // Use the representative input row so that property access (e.g. p.age) can resolve
      // the original variables, not just the aggregated result keys.
      for (final Map.Entry<String, ComplexAggregationInfo> entry : complexAggregations.entrySet()) {
        final String outputName = entry.getKey();
        final ComplexAggregationInfo complexInfo = entry.getValue();

        // Build overrides map: aggregation text -> aggregated result
        final Map<String, Object> overrides = new HashMap<>();
        for (final Map.Entry<String, StatelessFunction> funcEntry : complexInfo.innerFunctions.entrySet())
          overrides.put(funcEntry.getKey(), funcEntry.getValue().getAggregatedResult());

        // Evaluate the full expression with overrides, using representative row for variable resolution
        evaluator.setAggregationOverrides(overrides);
        try {
          final Result evalRow = representativeRow != null ? representativeRow : aggregatedResult;
          final Object value = evaluator.evaluate(complexInfo.expression, evalRow, context);
          aggregatedResult.setProperty(outputName, value);
        } finally {
          evaluator.clearAggregationOverrides();
        }
      }
    } finally {
      if (context.isProfiling())
        cost += (System.nanoTime() - beginFinal);
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
   * Recursively collects all aggregation FunctionCallExpressions from an expression tree.
   * Uses the expression text as key for deduplication.
   */
  private void collectAggregations(final Expression expr,
      final Map<String, FunctionCallExpression> innerAggs,
      final Map<String, StatelessFunction> innerFunctions) {
    if (expr == null)
      return;
    if (expr instanceof FunctionCallExpression funcExpr) {
      if (funcExpr.isAggregation()) {
        final String key = funcExpr.getText();
        if (!innerAggs.containsKey(key)) {
          innerAggs.put(key, funcExpr);
          innerFunctions.put(key, functionFactory.getFunctionExecutor(
              funcExpr.getFunctionName(), funcExpr.isDistinct()));
        }
        return; // Don't recurse into aggregation arguments
      }
      for (final Expression arg : funcExpr.getArguments())
        collectAggregations(arg, innerAggs, innerFunctions);
    } else if (expr instanceof ArithmeticExpression arith) {
      collectAggregations(arith.getLeft(), innerAggs, innerFunctions);
      collectAggregations(arith.getRight(), innerAggs, innerFunctions);
    } else if (expr instanceof ListComprehensionExpression lce) {
      collectAggregations(lce.getListExpression(), innerAggs, innerFunctions);
    } else if (expr instanceof ListPredicateExpression lpe) {
      collectAggregations(lpe.getListExpression(), innerAggs, innerFunctions);
    } else if (expr instanceof ListExpression le) {
      for (final Expression elem : le.getElements())
        collectAggregations(elem, innerAggs, innerFunctions);
    } else if (expr instanceof ComparisonExpressionWrapper cew) {
      collectAggregations(cew.getComparison().getLeft(), innerAggs, innerFunctions);
      collectAggregations(cew.getComparison().getRight(), innerAggs, innerFunctions);
    } else if (expr instanceof CaseExpression ce) {
      collectAggregations(ce.getCaseExpression(), innerAggs, innerFunctions);
      for (final com.arcadedb.query.opencypher.ast.CaseAlternative alt : ce.getAlternatives()) {
        collectAggregations(alt.getWhenExpression(), innerAggs, innerFunctions);
        collectAggregations(alt.getThenExpression(), innerAggs, innerFunctions);
      }
      collectAggregations(ce.getElseExpression(), innerAggs, innerFunctions);
    } else if (expr instanceof MapExpression me) {
      for (final Expression value : me.getEntries().values())
        collectAggregations(value, innerAggs, innerFunctions);
    } else if (expr instanceof BooleanWrapperExpression bwe) {
      collectBooleanAggregations(bwe.getBooleanExpression(), innerAggs, innerFunctions);
    }
  }

  private void collectBooleanAggregations(final BooleanExpression boolExpr,
      final Map<String, FunctionCallExpression> innerAggs,
      final Map<String, StatelessFunction> innerFunctions) {
    if (boolExpr instanceof ComparisonExpression comp) {
      collectAggregations(comp.getLeft(), innerAggs, innerFunctions);
      collectAggregations(comp.getRight(), innerAggs, innerFunctions);
    } else if (boolExpr instanceof LogicalExpression logical) {
      collectBooleanAggregations(logical.getLeft(), innerAggs, innerFunctions);
      if (logical.getRight() != null)
        collectBooleanAggregations(logical.getRight(), innerAggs, innerFunctions);
    }
  }

  /**
   * Holds information about a complex aggregation expression that contains
   * one or more aggregation sub-expressions within a larger expression.
   */
  private static class ComplexAggregationInfo {
    final Expression expression;
    final Map<String, FunctionCallExpression> innerAggregations;
    final Map<String, StatelessFunction> innerFunctions;

    ComplexAggregationInfo(final Expression expression,
                           final Map<String, FunctionCallExpression> innerAggregations,
                           final Map<String, StatelessFunction> innerFunctions) {
      this.expression = expression;
      this.innerAggregations = innerAggregations;
      this.innerFunctions = innerFunctions;
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
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }
}
