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

import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Optimizes expressions by pre-computing constant sub-expressions.
 * <p>
 * This optimization identifies sub-expressions that don't depend on iteration variables
 * and evaluates them once before the loop, significantly improving performance for bulk operations.
 * <p>
 * Example:
 * - Input: r_lat * (lat_max - lat_min) + lat_min
 * - Variables: r_lat changes per iteration, lat_max and lat_min are constant
 * - Optimized: r_lat * 0.10 + 48.80  (pre-computed the constant parts)
 */
public class ExpressionOptimizer {

  /**
   * Optimizes an expression by pre-computing constant sub-expressions.
   *
   * @param expression         the expression to optimize
   * @param sampleContext      a sample result context containing constant values
   * @param context            the command context
   * @param iterationVariables set of variable names that change per iteration (not constant)
   * @return optimized expression with constants pre-computed, or original if no optimization possible
   */
  public static Expression optimize(final Expression expression, final Result sampleContext,
      final CommandContext context, final Set<String> iterationVariables) {
    if (expression == null)
      return null;

    // Check if entire expression is constant
    if (isConstant(expression, iterationVariables)) {
      // Evaluate once and replace with literal
      final Object value = expression.evaluate(sampleContext, context);
      return new LiteralExpression(value, String.valueOf(value));
    }

    // Recursively optimize sub-expressions
    if (expression instanceof ArithmeticExpression) {
      return optimizeArithmetic((ArithmeticExpression) expression, sampleContext, context, iterationVariables);
    }

    if (expression instanceof FunctionCallExpression) {
      return optimizeFunction((FunctionCallExpression) expression, sampleContext, context, iterationVariables);
    }

    // No optimization possible
    return expression;
  }

  /**
   * Optimizes an arithmetic expression by pre-computing constant operands.
   */
  private static Expression optimizeArithmetic(final ArithmeticExpression expression, final Result sampleContext,
      final CommandContext context, final Set<String> iterationVariables) {
    // Recursively optimize left and right
    final Expression optimizedLeft = optimize(expression.getLeft(), sampleContext, context, iterationVariables);
    final Expression optimizedRight = optimize(expression.getRight(), sampleContext, context, iterationVariables);

    // If both sides are now literals, compute the result
    if (optimizedLeft instanceof LiteralExpression && optimizedRight instanceof LiteralExpression) {
      final ArithmeticExpression temp = new ArithmeticExpression(optimizedLeft, expression.getOperator(), optimizedRight);
      final Object value = temp.evaluate(sampleContext, context);
      return new LiteralExpression(value, String.valueOf(value));
    }

    // If only one side changed, create new expression with optimized operands
    if (optimizedLeft != expression.getLeft() || optimizedRight != expression.getRight()) {
      return new ArithmeticExpression(optimizedLeft, expression.getOperator(), optimizedRight);
    }

    return expression;
  }

  /**
   * Optimizes a function call expression by pre-computing constant arguments.
   */
  private static Expression optimizeFunction(final FunctionCallExpression expression, final Result sampleContext,
      final CommandContext context, final Set<String> iterationVariables) {
    // Check if all arguments are constant
    final List<Expression> arguments = expression.getArguments();
    final List<Expression> optimizedArgs = new ArrayList<>(arguments.size());
    boolean anyChanged = false;

    for (final Expression arg : arguments) {
      final Expression optimizedArg = optimize(arg, sampleContext, context, iterationVariables);
      optimizedArgs.add(optimizedArg);
      if (optimizedArg != arg)
        anyChanged = true;
    }

    // If function is deterministic and all args are literals, evaluate it
    if (isDeterministicFunction(expression.getFunctionName())) {
      boolean allLiterals = true;
      for (final Expression arg : optimizedArgs) {
        if (!(arg instanceof LiteralExpression)) {
          allLiterals = false;
          break;
        }
      }

      if (allLiterals) {
        // Evaluate once and replace with literal
        final Object value = expression.evaluate(sampleContext, context);
        return new LiteralExpression(value, String.valueOf(value));
      }
    }

    // If arguments changed, create new function call
    if (anyChanged) {
      return new FunctionCallExpression(expression.getFunctionName(), optimizedArgs, expression.isDistinct());
    }

    return expression;
  }

  /**
   * Checks if an expression is constant (doesn't depend on iteration variables).
   */
  private static boolean isConstant(final Expression expression, final Set<String> iterationVariables) {
    if (expression instanceof LiteralExpression) {
      return true;
    }

    if (expression instanceof VariableExpression) {
      final String varName = ((VariableExpression) expression).getVariableName();
      return !iterationVariables.contains(varName);
    }

    if (expression instanceof ArithmeticExpression) {
      final ArithmeticExpression arith = (ArithmeticExpression) expression;
      return isConstant(arith.getLeft(), iterationVariables) && isConstant(arith.getRight(), iterationVariables);
    }

    if (expression instanceof FunctionCallExpression) {
      final FunctionCallExpression func = (FunctionCallExpression) expression;
      // Non-deterministic functions (rand, randomuuid) are never constant
      if (!isDeterministicFunction(func.getFunctionName())) {
        return false;
      }
      // Deterministic function is constant if all arguments are constant
      for (final Expression arg : func.getArguments()) {
        if (!isConstant(arg, iterationVariables)) {
          return false;
        }
      }
      return true;
    }

    // Unknown expression type - assume not constant to be safe
    return false;
  }

  /**
   * Checks if a function is deterministic (always returns the same result for the same inputs).
   */
  private static boolean isDeterministicFunction(final String functionName) {
    return switch (functionName.toLowerCase()) {
      // Non-deterministic functions
      case "rand", "randomuuid", "timestamp" -> false;
      // Date/time functions that return current time are non-deterministic
      case "date.realtime", "date.statement", "date.transaction" -> false;
      case "localtime.realtime", "localtime.statement", "localtime.transaction" -> false;
      case "time.realtime", "time.statement", "time.transaction" -> false;
      case "localdatetime.realtime", "localdatetime.statement", "localdatetime.transaction" -> false;
      case "datetime.realtime", "datetime.statement", "datetime.transaction" -> false;
      // All other functions are considered deterministic
      default -> true;
    };
  }
}
