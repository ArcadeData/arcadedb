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
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpressionWrapper;
import com.arcadedb.query.opencypher.ast.ListComprehensionExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.ListIndexExpression;
import com.arcadedb.query.opencypher.ast.ListPredicateExpression;
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
  private final CypherFunctionFactory functionFactory;
  private Map<String, Object> aggregationOverrides;

  public ExpressionEvaluator(final CypherFunctionFactory functionFactory) {
    this.functionFactory = functionFactory;
  }

  public void setAggregationOverrides(final Map<String, Object> overrides) {
    this.aggregationOverrides = overrides;
  }

  public void clearAggregationOverrides() {
    this.aggregationOverrides = null;
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
    } else if (aggregationOverrides != null && expression instanceof MapExpression me) {
      return evaluateMap(me, result, context);
    } else if (aggregationOverrides != null && expression instanceof ListComprehensionExpression) {
      return evaluateListComprehension((ListComprehensionExpression) expression, result, context);
    } else if (aggregationOverrides != null && expression instanceof ListPredicateExpression) {
      return evaluateListPredicate((ListPredicateExpression) expression, result, context);
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
    if (aggregationOverrides != null && expression.isAggregation()) {
      final String key = expression.getText();
      if (aggregationOverrides.containsKey(key))
        return aggregationOverrides.get(key);
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

    // Execute function
    return function.execute(args, context);
  }

  private Object evaluateArithmetic(final ArithmeticExpression expression, final Result result,
      final CommandContext context) {
    final Object leftValue = evaluate(expression.getLeft(), result, context);
    final Object rightValue = evaluate(expression.getRight(), result, context);

    if (leftValue == null || rightValue == null)
      return null;

    // String concatenation for + operator
    if (expression.getOperator() == ArithmeticExpression.Operator.ADD
        && (leftValue instanceof String || rightValue instanceof String))
      return leftValue.toString() + rightValue.toString();

    // List concatenation/append for + operator
    if (expression.getOperator() == ArithmeticExpression.Operator.ADD) {
      if (leftValue instanceof List && rightValue instanceof List) {
        final List<Object> combined = new ArrayList<>((List<?>) leftValue);
        combined.addAll((List<?>) rightValue);
        return combined;
      }
      if (leftValue instanceof List) {
        final List<Object> appended = new ArrayList<>((List<?>) leftValue);
        appended.add(rightValue);
        return appended;
      }
      if (rightValue instanceof List) {
        final List<Object> prepended = new ArrayList<>();
        prepended.add(leftValue);
        prepended.addAll((List<?>) rightValue);
        return prepended;
      }
    }

    // Temporal arithmetic with pre-evaluated values
    if (!(leftValue instanceof Number) || !(rightValue instanceof Number)) {
      final Object temporalResult = ArithmeticExpression.evaluateTemporalArithmetic(
          leftValue, rightValue, expression.getOperator());
      if (temporalResult != null)
        return temporalResult;
      throw new IllegalArgumentException(
          "Arithmetic operations require numeric operands, got: " + leftValue.getClass().getSimpleName()
              + " and " + rightValue.getClass().getSimpleName());
    }

    final Number leftNum = (Number) leftValue;
    final Number rightNum = (Number) rightValue;

    final boolean useInteger = isInteger(leftNum) && isInteger(rightNum)
        && expression.getOperator() != ArithmeticExpression.Operator.POWER;

    if (useInteger) {
      final long l = leftNum.longValue();
      final long r = rightNum.longValue();
      return switch (expression.getOperator()) {
        case ADD -> l + r;
        case SUBTRACT -> l - r;
        case MULTIPLY -> l * r;
        case DIVIDE -> r != 0 ? l / r : null;
        case MODULO -> r != 0 ? l % r : null;
        default -> null;
      };
    }

    final double l = leftNum.doubleValue();
    final double r = rightNum.doubleValue();
    return switch (expression.getOperator()) {
      case ADD -> l + r;
      case SUBTRACT -> l - r;
      case MULTIPLY -> l * r;
      case DIVIDE -> l / r; // IEEE 754: 0.0/0.0=NaN, x/0.0=±Infinity
      case MODULO -> r != 0 ? l % r : Double.NaN;
      case POWER -> Math.pow(l, r);
    };
  }

  private static boolean isInteger(final Number num) {
    return num instanceof Integer || num instanceof Long || num instanceof Short || num instanceof Byte;
  }

  private Object evaluateList(final ListExpression expression, final Result result,
      final CommandContext context) {
    final List<Object> values = new ArrayList<>();
    for (final Expression element : expression.getElements())
      values.add(evaluate(element, result, context));
    return values;
  }

  private Object evaluateMap(final MapExpression expression, final Result result,
      final CommandContext context) {
    final java.util.LinkedHashMap<String, Object> map = new java.util.LinkedHashMap<>();
    for (final Map.Entry<String, Expression> entry : expression.getEntries().entrySet())
      map.put(entry.getKey(), evaluate(entry.getValue(), result, context));
    return map;
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
        if (filterValue == null || (filterValue instanceof Boolean && !((Boolean) filterValue)))
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
