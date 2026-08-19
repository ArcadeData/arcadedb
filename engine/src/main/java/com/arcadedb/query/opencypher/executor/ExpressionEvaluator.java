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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.CaseExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpressionWrapper;
import com.arcadedb.query.opencypher.ast.ListComprehensionExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.ListIndexExpression;
import com.arcadedb.query.opencypher.ast.ListPredicateExpression;
import com.arcadedb.query.opencypher.ast.ListSliceExpression;
import com.arcadedb.query.opencypher.ast.MapExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Evaluates Cypher expressions in the context of query results.
 * Handles variables, property access, and function calls.
 */
public class ExpressionEvaluator {
  /**
   * Aggregation overrides are stored thread-locally so that all
   * {@code ExpressionEvaluator} instances - including the shared static one
   * used by AST nodes like {@link com.arcadedb.query.opencypher.ast.ReduceExpression}
   * and {@link com.arcadedb.query.opencypher.ast.ListIndexExpression} - see the
   * same overrides during a single aggregation flush, while keeping concurrent
   * queries on different threads isolated.
   */
  private static final ThreadLocal<Map<String, Object>> AGGREGATION_OVERRIDES = new ThreadLocal<>();

  private final CypherFunctionFactory functionFactory;

  public ExpressionEvaluator(final CypherFunctionFactory functionFactory) {
    this.functionFactory = functionFactory;
  }

  public void setAggregationOverrides(final Map<String, Object> overrides) {
    AGGREGATION_OVERRIDES.set(overrides);
  }

  public void clearAggregationOverrides() {
    AGGREGATION_OVERRIDES.remove();
  }

  private Map<String, Object> aggregationOverrides() {
    return AGGREGATION_OVERRIDES.get();
  }

  /**
   * Evaluate an expression against a result row.
   */
  public Object evaluate(final Expression expression, final Result result, final CommandContext context) {
    if (expression instanceof VariableExpression) {
      return evaluateVariable((VariableExpression) expression, result);
    } else if (expression instanceof PropertyAccessExpression) {
      return evaluatePropertyAccess((PropertyAccessExpression) expression, result);
    } else if (expression instanceof ListIndexExpression) {
      return evaluateListIndex((ListIndexExpression) expression, result, context);
    } else if (expression instanceof FunctionCallExpression) {
      return evaluateFunction((FunctionCallExpression) expression, result, context);
    } else if (expression instanceof ArithmeticExpression) {
      return evaluateArithmetic((ArithmeticExpression) expression, result, context);
    } else if (expression instanceof ListExpression) {
      return evaluateList((ListExpression) expression, result, context);
    } else if (expression instanceof ComparisonExpressionWrapper) {
      return evaluateComparison((ComparisonExpressionWrapper) expression, result, context);
    } else if (expression instanceof BooleanWrapperExpression bwe) {
      return evaluateBooleanWrapper(bwe, result, context);
    } else if (aggregationOverrides() != null && expression instanceof MapExpression me) {
      return evaluateMap(me, result, context);
    } else if (aggregationOverrides() != null && expression instanceof ListComprehensionExpression) {
      return evaluateListComprehension((ListComprehensionExpression) expression, result, context);
    } else if (aggregationOverrides() != null && expression instanceof ListPredicateExpression) {
      return evaluateListPredicate((ListPredicateExpression) expression, result, context);
    } else if (aggregationOverrides() != null && expression instanceof CaseExpression ce) {
      // Route CASE branches through this evaluator so a pre-computed aggregation nested inside a
      // branch (e.g. CASE WHEN ... THEN sum(v) END) resolves to its accumulated value instead of
      // being re-evaluated against the single representative row (issue #5220).
      return ce.evaluateWith(sub -> evaluate(sub, result, context));
    } else if (expression instanceof ListSliceExpression lse) {
      return evaluateListSlice(lse, result, context);
    }

    // Fallback
    return expression.evaluate(result, context);
  }

  private Object evaluateVariable(final VariableExpression expression, final Result result) {
    return result.getProperty(expression.getVariableName());
  }

  private Object evaluatePropertyAccess(final PropertyAccessExpression expression, final Result result) {
    return expression.evaluate(result, null);
  }

  private Object evaluateListIndex(final ListIndexExpression expression, final Result result,
      final CommandContext context) {
    return expression.evaluate(result, context);
  }

  private Object evaluateFunction(final FunctionCallExpression expression, final Result result,
      final CommandContext context) {
    // Check for pre-computed aggregation override
    final Map<String, Object> overrides = aggregationOverrides();
    if (overrides != null && expression.isAggregation()) {
      final String key = expression.getText();
      if (overrides.containsKey(key))
        return overrides.get(key);
    }

    // Get function - use cache if available to avoid repeated lookups
    StatelessFunction function = expression.getCachedFunction();
    if (function == null) {
      function = functionFactory.getFunctionExecutor(expression.getFunctionName());
      expression.setCachedFunction(function);
    }

    // Evaluate arguments
    final Object[] args = new Object[expression.getArguments().size()];
    for (int i = 0; i < args.length; i++) {
      args[i] = evaluate(expression.getArguments().get(i), result, context);
    }

    // Execute function through the shared invocation point, which also publishes the row-scoped state a function
    // may read off the context - the LOAD CSV file()/linenumber() pair (issue #6402).
    return FunctionCallExpression.invoke(function, args, result, context);
  }

  /**
   * Evaluates an arithmetic expression, resolving both operands through this evaluator so that an inline
   * aggregator sees the pre-computed overrides, then handing the values to the operator semantics that live on
   * the AST node. This method used to carry a verbatim copy of those sixty-odd lines - null propagation, {@code ||}
   * strict typing, list and string concatenation, temporal arithmetic, numeric promotion - so a fix could land on
   * one path and not the other (issue #6354). Only the two operand evaluations below belong here.
   */
  private Object evaluateArithmetic(final ArithmeticExpression expression, final Result result,
      final CommandContext context) {
    return ArithmeticExpression.apply(expression.getOperator(), evaluate(expression.getLeft(), result, context),
        evaluate(expression.getRight(), result, context));
  }

  private Object evaluateList(final ListExpression expression, final Result result,
      final CommandContext context) {
    return expression.evaluateWith(element -> evaluate(element, result, context));
  }

  private Object evaluateMap(final MapExpression expression, final Result result,
      final CommandContext context) {
    return expression.evaluateWith(value -> evaluate(value, result, context));
  }

  /**
   * Evaluates a comparison expression using this evaluator (preserving aggregation overrides).
   */
  private Object evaluateComparison(final ComparisonExpressionWrapper expression,
      final Result result, final CommandContext context) {
    final ComparisonExpression comp = expression.getComparison();
    final Object leftValue = evaluate(comp.getLeft(), result, context);
    final Object rightValue = evaluate(comp.getRight(), result, context);
    return comp.evaluateWithValues(leftValue, rightValue);
  }

  private Object evaluateBooleanWrapper(final BooleanWrapperExpression expression,
      final Result result, final CommandContext context) {
    final BooleanExpression boolExpr = expression.getBooleanExpression();
    if (boolExpr instanceof ComparisonExpression comp) {
      final Object leftValue = evaluate(comp.getLeft(), result, context);
      final Object rightValue = evaluate(comp.getRight(), result, context);
      return comp.evaluateWithValues(leftValue, rightValue);
    }
    return expression.evaluate(result, context);
  }

  /**
   * Evaluates a list comprehension expression using this evaluator (preserving aggregation overrides).
   */
  private Object evaluateListComprehension(final ListComprehensionExpression expression,
      final Result result, final CommandContext context) {
    final Object listValue = evaluate(expression.getListExpression(), result, context);
    if (listValue == null)
      return null;

    final Iterable<?> iterable;
    if (listValue instanceof Iterable)
      iterable = (Iterable<?>) listValue;
    else
      return expression.evaluate(result, context); // fallback for arrays

    final List<Object> resultList = new ArrayList<>();
    for (final Object item : iterable) {
      final ResultInternal iterResult = new ResultInternal();
      if (result != null)
        for (final String prop : result.getPropertyNames())
          iterResult.setProperty(prop, result.getProperty(prop));
      iterResult.setProperty(expression.getVariable(), item);

      if (expression.getWhereExpression() != null) {
        final Object filterValue = evaluate(expression.getWhereExpression(), iterResult, context);
        if (filterValue == null || (filterValue instanceof Boolean b && !b))
          continue;
      }

      if (expression.getMapExpression() != null)
        resultList.add(evaluate(expression.getMapExpression(), iterResult, context));
      else
        resultList.add(item);
    }
    return resultList;
  }

  /**
   * Evaluates a list predicate expression (ALL/ANY/NONE/SINGLE) using this evaluator.
   */
  private Object evaluateListPredicate(final ListPredicateExpression expression,
      final Result result, final CommandContext context) {
    final Object listValue = evaluate(expression.getListExpression(), result, context);
    if (listValue == null)
      return null;

    final Iterable<?> iterable;
    if (listValue instanceof Iterable)
      iterable = (Iterable<?>) listValue;
    else
      return expression.evaluate(result, context); // fallback

    int matchCount = 0;
    int totalCount = 0;
    for (final Object item : iterable) {
      totalCount++;
      final ResultInternal iterResult = new ResultInternal();
      if (result != null)
        for (final String prop : result.getPropertyNames())
          iterResult.setProperty(prop, result.getProperty(prop));
      iterResult.setProperty(expression.getVariable(), item);

      if (expression.getWhereExpression() != null) {
        final Object filterValue = evaluate(expression.getWhereExpression(), iterResult, context);
        if (filterValue instanceof Boolean && (Boolean) filterValue)
          matchCount++;
      } else {
        matchCount++;
      }
    }

    return switch (expression.getPredicateType()) {
      case ALL -> matchCount == totalCount;
      case ANY -> matchCount > 0;
      case NONE -> matchCount == 0;
      case SINGLE -> matchCount == 1;
    };
  }

  private Object evaluateListSlice(final ListSliceExpression expression, final Result result,
      final CommandContext context) {
    // Evaluate the inner list expression through this evaluator so aggregation overrides apply
    final Object listValue = evaluate(expression.getListExpression(), result, context);
    if (listValue == null)
      return null;

    Integer from = null;
    if (expression.getFromExpression() != null) {
      final Object fromValue = evaluate(expression.getFromExpression(), result, context);
      if (fromValue == null)
        return null;
      from = ListSliceExpression.sliceBound(fromValue);
    }

    Integer to = null;
    if (expression.getToExpression() != null) {
      final Object toValue = evaluate(expression.getToExpression(), result, context);
      if (toValue == null)
        return null;
      to = ListSliceExpression.sliceBound(toValue);
    }

    // The slicing itself belongs to the AST node, so the two paths cannot answer differently (issue #6323).
    return ListSliceExpression.slice(listValue, from, to);
  }

  /**
   * Get the function factory used by this evaluator.
   * This is needed by execution steps that create function-dependent steps.
   *
   * @return the function factory
   */
  public CypherFunctionFactory getFunctionFactory() {
    return functionFactory;
  }

  /**
   * Evaluates a SKIP or LIMIT expression to an integer value.
   * Supports integer literals, parameters, and function calls like toInteger(ceil(1.7)).
   */
  public int evaluateSkipLimit(final Expression expr, final Result result, final CommandContext context) {
    final Object value = evaluate(expr, result, context);
    if (value instanceof Number) {
      final Number num = (Number) value;
      if (num instanceof Float || num instanceof Double) {
        final double d = num.doubleValue();
        if (d != Math.floor(d) || Double.isInfinite(d))
          throw new CommandParsingException("InvalidArgumentType: SKIP/LIMIT value must be an integer, got: Float(" + d + ")");
      }
      final int intVal = num.intValue();
      if (intVal < 0)
        throw new CommandParsingException("NegativeIntegerArgument: SKIP/LIMIT value must not be negative, got: " + intVal);
      return intVal;
    }
    if (value instanceof String)
      return Integer.parseInt((String) value);
    throw new CommandParsingException("InvalidArgumentType: SKIP/LIMIT value must be an integer, got: " + (value != null ? value.getClass().getSimpleName() : "null"));
  }
}
