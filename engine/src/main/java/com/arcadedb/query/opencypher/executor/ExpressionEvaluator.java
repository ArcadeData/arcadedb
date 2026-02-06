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

import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.ListIndexExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.List;

/**
 * Evaluates Cypher expressions in the context of query results.
 * Handles variables, property access, and function calls.
 */
public class ExpressionEvaluator {
  private final CypherFunctionFactory functionFactory;

  public ExpressionEvaluator(final CypherFunctionFactory functionFactory) {
    this.functionFactory = functionFactory;
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
    // Get function
    final StatelessFunction function = functionFactory.getFunctionExecutor(expression.getFunctionName());

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

    if (!(leftValue instanceof Number) || !(rightValue instanceof Number))
      throw new IllegalArgumentException(
          "Arithmetic operations require numeric operands, got: " + leftValue.getClass().getSimpleName()
              + " and " + rightValue.getClass().getSimpleName());

    final Number leftNum = (Number) leftValue;
    final Number rightNum = (Number) rightValue;

    final boolean useInteger = isInteger(leftNum) && isInteger(rightNum)
        && expression.getOperator() != ArithmeticExpression.Operator.DIVIDE
        && expression.getOperator() != ArithmeticExpression.Operator.POWER;

    if (useInteger) {
      final long l = leftNum.longValue();
      final long r = rightNum.longValue();
      return switch (expression.getOperator()) {
        case ADD -> l + r;
        case SUBTRACT -> l - r;
        case MULTIPLY -> l * r;
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
      case DIVIDE -> r != 0 ? l / r : null;
      case MODULO -> r != 0 ? l % r : null;
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

  /**
   * Get the function factory used by this evaluator.
   * This is needed by execution steps that create function-dependent steps.
   *
   * @return the function factory
   */
  public CypherFunctionFactory getFunctionFactory() {
    return functionFactory;
  }
}
