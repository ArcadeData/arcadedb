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
import com.arcadedb.query.opencypher.ast.*;
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

  // Configurable batch size for streaming aggregation (default: 10,000)
  private static final int DEFAULT_BATCH_SIZE = 10000;
  private static final int BATCH_SIZE = Integer.parseInt(
      System.getProperty("arcadedb.aggregation.batchSize", String.valueOf(DEFAULT_BATCH_SIZE)));

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

    // Identify grouping keys (non-aggregated expressions) and aggregations
    final List<GroupingKey> groupingKeys = new ArrayList<>();
    final List<AggregationItem> aggregationItems = new ArrayList<>();
    // Complex aggregation expressions: arithmetic/other expressions containing aggregations
    final List<ComplexAggregationItem> complexAggregationItems = new ArrayList<>();

    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
      final Expression expr = item.getExpression();
      if (expr.isAggregation() && expr instanceof FunctionCallExpression) {
        aggregationItems.add(new AggregationItem(item.getOutputName(), (FunctionCallExpression) expr));
      } else if (expr.containsAggregation()) {
        // Complex expression containing aggregations (arithmetic, function wrapping, etc.)
        final Map<String, FunctionCallExpression> innerAggs = new HashMap<>();
        final Map<String, StatelessFunction> dummyFuncs = new HashMap<>();
        collectAggregations(expr, innerAggs, dummyFuncs);
        if (!innerAggs.isEmpty())
          complexAggregationItems.add(new ComplexAggregationItem(item.getOutputName(), expr, innerAggs));
        else
          groupingKeys.add(new GroupingKey(item.getOutputName(), expr));
      } else {
        groupingKeys.add(new GroupingKey(item.getOutputName(), expr));
      }
    }

    // Convert aggregation items to arrays for fast indexed access (avoids per-row HashMap lookups)
    final int aggCount = aggregationItems.size();
    final FunctionCallExpression[] aggExpressions = new FunctionCallExpression[aggCount];
    final String[] aggOutputNames = new String[aggCount];
    for (int i = 0; i < aggCount; i++) {
      aggExpressions[i] = aggregationItems.get(i).funcExpr;
      aggOutputNames[i] = aggregationItems.get(i).outputName;
    }

    // Use single-key fast path when there's exactly 1 grouping key (most common case)
    // This avoids ArrayList + GroupKeyValues wrapper allocation per row
    final boolean singleKeyPath = groupingKeys.size() == 1 && complexAggregationItems.isEmpty();

    final List<Result> results;

    if (singleKeyPath) {
      results = aggregateSingleKey(groupingKeys.get(0), aggExpressions, aggOutputNames, aggCount, context);
    } else {
      results = aggregateMultiKey(groupingKeys, aggregationItems, complexAggregationItems,
          aggExpressions, aggOutputNames, aggCount, context);
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
   * Optimized aggregation for the common single-key case.
   * Uses the grouping value directly as HashMap key, avoiding ArrayList and GroupKeyValues
   * wrapper allocation per row.
   */
  private List<Result> aggregateSingleKey(final GroupingKey groupingKey,
      final FunctionCallExpression[] aggExpressions, final String[] aggOutputNames, final int aggCount,
      final CommandContext context) {

    // Map: raw grouping value -> array of aggregation functions (indexed, not HashMap)
    // LinkedHashMap preserves insertion order, needed because ORDER BY in WITH clauses
    // may fail to resolve aggregation expressions and fall back to iteration order
    final Map<Object, StatelessFunction[]> groups = new LinkedHashMap<>();

    final ResultSet prevResults = prev.syncPull(context, BATCH_SIZE);

    while (prevResults.hasNext()) {
      final Result inputRow = prevResults.next();

      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        if (context.isProfiling())
          rowCount++;

        // Evaluate grouping key directly (no wrapper)
        final Object keyValue = evaluator.evaluate(groupingKey.expression, inputRow, context);

        // Get or create aggregators for this group using array (not HashMap)
        StatelessFunction[] aggregators = groups.get(keyValue);
        if (aggregators == null) {
          aggregators = new StatelessFunction[aggCount];
          for (int i = 0; i < aggCount; i++)
            aggregators[i] = functionFactory.getFunctionExecutor(
                aggExpressions[i].getFunctionName(), aggExpressions[i].isDistinct());
          groups.put(keyValue, aggregators);
        }

        // Feed row to aggregators using direct array access
        for (int i = 0; i < aggCount; i++) {
          final List<Expression> funcArgs = aggExpressions[i].getArguments();
          final Object[] args = new Object[funcArgs.size()];
          for (int j = 0; j < args.length; j++)
            args[j] = evaluator.evaluate(funcArgs.get(j), inputRow, context);
          aggregators[i].execute(args, context);
        }
      } finally {
        if (context.isProfiling())
          cost += (System.nanoTime() - begin);
      }
    }

    // Build results
    final List<Result> results = new ArrayList<>(groups.size());
    final long beginBuild = context.isProfiling() ? System.nanoTime() : 0;
    try {
      for (final Map.Entry<Object, StatelessFunction[]> entry : groups.entrySet()) {
        final ResultInternal groupResult = new ResultInternal();
        groupResult.setProperty(groupingKey.outputName, entry.getKey());

        final StatelessFunction[] aggregators = entry.getValue();
        for (int i = 0; i < aggCount; i++)
          groupResult.setProperty(aggOutputNames[i], aggregators[i].getAggregatedResult());

        results.add(groupResult);
      }
    } finally {
      if (context.isProfiling())
        cost += (System.nanoTime() - beginBuild);
    }
    return results;
  }

  /**
   * General multi-key aggregation path. Used when there are multiple grouping keys
   * or complex aggregation expressions.
   */
  private List<Result> aggregateMultiKey(final List<GroupingKey> groupingKeys,
      final List<AggregationItem> aggregationItems, final List<ComplexAggregationItem> complexAggregationItems,
      final FunctionCallExpression[] aggExpressions, final String[] aggOutputNames, final int aggCount,
      final CommandContext context) {

    final Map<GroupKeyValues, GroupAggregators> groups = new LinkedHashMap<>();

    final ResultSet prevResults = prev.syncPull(context, BATCH_SIZE);

    while (prevResults.hasNext()) {
      final Result inputRow = prevResults.next();

      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        if (context.isProfiling())
          rowCount++;

        final GroupKeyValues keyValues = evaluateGroupingKey(groupingKeys, inputRow, context);

        GroupAggregators groupAgg = groups.get(keyValues);
        if (groupAgg == null) {
          groupAgg = createGroupAggregators(aggregationItems, complexAggregationItems);
          groupAgg.representativeRow = inputRow;
          groups.put(keyValues, groupAgg);
        }

        // Feed row using array-indexed access for regular aggregations
        for (int i = 0; i < aggCount; i++) {
          final List<Expression> funcArgs = aggExpressions[i].getArguments();
          final Object[] args = new Object[funcArgs.size()];
          for (int j = 0; j < args.length; j++)
            args[j] = evaluator.evaluate(funcArgs.get(j), inputRow, context);
          groupAgg.aggregatorArray[i].execute(args, context);
        }

        // Process inner aggregations for complex aggregation items
        for (final ComplexAggregationItem complexItem : complexAggregationItems) {
          final Map<String, StatelessFunction> innerFuncs = groupAgg.complexAggregators.get(complexItem.outputName);
          for (final Map.Entry<String, FunctionCallExpression> innerEntry : complexItem.innerAggregations.entrySet()) {
            final FunctionCallExpression innerAgg = innerEntry.getValue();
            final StatelessFunction function = innerFuncs.get(innerEntry.getKey());
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

    // Build results
    final List<Result> results = new ArrayList<>(groups.size());
    final long beginBuild = context.isProfiling() ? System.nanoTime() : 0;
    try {
      for (final Map.Entry<GroupKeyValues, GroupAggregators> groupEntry : groups.entrySet()) {
        final GroupKeyValues keyValues = groupEntry.getKey();
        final GroupAggregators groupAgg = groupEntry.getValue();
        final ResultInternal groupResult = new ResultInternal();

        for (int i = 0; i < groupingKeys.size(); i++)
          groupResult.setProperty(groupingKeys.get(i).outputName, keyValues.values[i]);

        for (int i = 0; i < aggCount; i++)
          groupResult.setProperty(aggOutputNames[i], groupAgg.aggregatorArray[i].getAggregatedResult());

        // Evaluate complex aggregation expressions
        for (final ComplexAggregationItem complexItem : complexAggregationItems) {
          final Map<String, StatelessFunction> innerFuncs = groupAgg.complexAggregators.get(complexItem.outputName);
          final Map<String, Object> overrides = new HashMap<>();
          for (final Map.Entry<String, StatelessFunction> funcEntry : innerFuncs.entrySet())
            overrides.put(funcEntry.getKey(), funcEntry.getValue().getAggregatedResult());
          evaluator.setAggregationOverrides(overrides);
          try {
            final Object value = evaluator.evaluate(complexItem.expression, groupAgg.representativeRow, context);
            groupResult.setProperty(complexItem.outputName, value);
          } finally {
            evaluator.clearAggregationOverrides();
          }
        }

        results.add(groupResult);
      }
    } finally {
      if (context.isProfiling())
        cost += (System.nanoTime() - beginBuild);
    }
    return results;
  }

  /**
   * Evaluate grouping key expressions for a given row.
   */
  private GroupKeyValues evaluateGroupingKey(final List<GroupingKey> keys, final Result row, final CommandContext context) {
    final int size = keys.size();
    final Object[] values = new Object[size];
    for (int i = 0; i < size; i++)
      values[i] = evaluator.evaluate(keys.get(i).expression, row, context);
    return new GroupKeyValues(values);
  }

  /**
   * Create fresh aggregators for a new group.
   */
  private GroupAggregators createGroupAggregators(final List<AggregationItem> aggregationItems,
      final List<ComplexAggregationItem> complexAggregationItems) {
    // Create array-indexed aggregators for fast per-row access
    final int aggCount = aggregationItems.size();
    final StatelessFunction[] aggregatorArray = new StatelessFunction[aggCount];
    for (int i = 0; i < aggCount; i++) {
      final AggregationItem aggItem = aggregationItems.get(i);
      aggregatorArray[i] = functionFactory.getFunctionExecutor(
          aggItem.funcExpr.getFunctionName(), aggItem.funcExpr.isDistinct());
    }

    final Map<String, Map<String, StatelessFunction>> complexAggregators = new HashMap<>();
    for (final ComplexAggregationItem complexItem : complexAggregationItems) {
      final Map<String, StatelessFunction> innerFuncs = new HashMap<>();
      for (final Map.Entry<String, FunctionCallExpression> innerEntry : complexItem.innerAggregations.entrySet()) {
        final FunctionCallExpression innerAgg = innerEntry.getValue();
        innerFuncs.put(innerEntry.getKey(), functionFactory.getFunctionExecutor(
            innerAgg.getFunctionName(), innerAgg.isDistinct()));
      }
      complexAggregators.put(complexItem.outputName, innerFuncs);
    }

    return new GroupAggregators(aggregatorArray, complexAggregators);
  }

  /**
   * Holds aggregation functions for a single group and a representative input row.
   * Uses array-indexed aggregators instead of HashMap for fast per-row access.
   */
  private static class GroupAggregators {
    final StatelessFunction[] aggregatorArray;
    final Map<String, Map<String, StatelessFunction>> complexAggregators;
    Result representativeRow;

    GroupAggregators(final StatelessFunction[] aggregatorArray,
        final Map<String, Map<String, StatelessFunction>> complexAggregators) {
      this.aggregatorArray = aggregatorArray;
      this.complexAggregators = complexAggregators;
    }
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
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
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
   * Uses an Object array with cached hashCode for fast HashMap operations.
   */
  private static class GroupKeyValues {
    final Object[] values;
    private final int hash;

    GroupKeyValues(final Object[] values) {
      this.values = values;
      // Pre-compute and cache hash to avoid recomputation on every HashMap lookup
      int h = 1;
      for (final Object v : values)
        h = 31 * h + (v == null ? 0 : v.hashCode());
      this.hash = h;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (!(o instanceof GroupKeyValues that)) return false;
      if (hash != that.hash || values.length != that.values.length) return false;
      for (int i = 0; i < values.length; i++)
        if (!Objects.equals(values[i], that.values[i])) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return hash;
    }
  }

  /**
   * Represents a complex aggregation item - an expression that contains one or more aggregation sub-expressions.
   */
  private static class ComplexAggregationItem {
    final String outputName;
    final Expression expression;
    final Map<String, FunctionCallExpression> innerAggregations;

    ComplexAggregationItem(final String outputName, final Expression expression,
        final Map<String, FunctionCallExpression> innerAggregations) {
      this.outputName = outputName;
      this.expression = expression;
      this.innerAggregations = innerAggregations;
    }
  }

  /**
   * Recursively collects all aggregation FunctionCallExpressions from an expression tree.
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
        return;
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
      for (final CaseAlternative alt : ce.getAlternatives()) {
        collectAggregations(alt.getWhenExpression(), innerAggs, innerFunctions);
        collectAggregations(alt.getThenExpression(), innerAggs, innerFunctions);
      }
      collectAggregations(ce.getElseExpression(), innerAggs, innerFunctions);
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
}
