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

import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

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
    } else if (expression instanceof FunctionCallExpression) {
      return evaluateFunction((FunctionCallExpression) expression, result, context);
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

  private Object evaluateFunction(final FunctionCallExpression expression, final Result result,
      final CommandContext context) {
    // Get function executor
    final CypherFunctionExecutor executor = functionFactory.getFunctionExecutor(expression.getFunctionName());

    // Evaluate arguments
    final Object[] args = new Object[expression.getArguments().size()];
    for (int i = 0; i < args.length; i++) {
      args[i] = evaluate(expression.getArguments().get(i), result, context);
    }

    // Execute function
    return executor.execute(args, context);
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
