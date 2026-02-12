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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.List;
import java.util.function.Function;

/**
 * Expression representing a function call.
 * Example: count(n), sum(n.age), toUpper(n.name)
 */
public class FunctionCallExpression implements Expression {
  private final String functionName;
  private final String originalFunctionName;
  private final List<Expression> arguments;
  private final boolean distinct;

  /**
   * Cached function executor to avoid repeated lookups.
   * This is lazily initialized on first use and significantly improves performance
   * for bulk operations where the same function is called many times.
   * Marked as transient to avoid serialization issues.
   */
  private transient volatile StatelessFunction cachedFunction;

  public FunctionCallExpression(final String functionName, final List<Expression> arguments, final boolean distinct) {
    this.originalFunctionName = functionName;
    this.functionName = functionName.toLowerCase(); // Cypher functions are case-insensitive
    this.arguments = arguments;
    this.distinct = distinct;
  }

  /**
   * Key used to store the function resolver in the CommandContext.
   * The resolver is a Function&lt;String, StatelessFunction&gt; that maps function names to executors.
   */
  public static final String FUNCTION_RESOLVER_KEY = "cypherFunctionResolver";

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    // Try to resolve the function through the context-stored resolver
    if (context != null) {
      @SuppressWarnings("unchecked")
      final Function<String, StatelessFunction> resolver =
          (Function<String, StatelessFunction>) context.getVariable(FUNCTION_RESOLVER_KEY);
      if (resolver != null) {
        final StatelessFunction function = resolver.apply(functionName);
        if (function != null) {
          final Object[] args = new Object[arguments.size()];
          for (int i = 0; i < args.length; i++)
            args[i] = arguments.get(i).evaluate(result, context);
          return function.execute(args, context);
        }
      }
    }
    throw new UnsupportedOperationException("Function evaluation requires StatelessFunction: " + functionName);
  }

  @Override
  public boolean isAggregation() {
    // Check if this is an aggregation function
    return isAggregationFunction(functionName);
  }

  @Override
  public boolean containsAggregation() {
    // If this function itself is an aggregation, return true
    if (isAggregation()) {
      return true;
    }
    // Otherwise, check if any argument contains an aggregation (wrapped aggregation)
    for (final Expression arg : arguments) {
      if (arg.containsAggregation()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getText() {
    final StringBuilder sb = new StringBuilder();
    sb.append(originalFunctionName).append("(");
    if (distinct) {
      sb.append("DISTINCT ");
    }
    for (int i = 0; i < arguments.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(arguments.get(i).getText());
    }
    sb.append(")");
    return sb.toString();
  }

  public String getFunctionName() {
    return functionName;
  }

  public List<Expression> getArguments() {
    return arguments;
  }

  public boolean isDistinct() {
    return distinct;
  }

  /**
   * Check if a function name represents an aggregation function.
   */
  private static boolean isAggregationFunction(final String functionName) {
    return switch (functionName) {
      case "count", "sum", "avg", "min", "max", "collect", "stdev", "stdevp", "percentilecont", "percentiledisc" -> true;
      default -> false;
    };
  }

  /**
   * Get the cached function executor, or null if not yet cached.
   * This is used by ExpressionEvaluator to optimize repeated function calls.
   *
   * @return the cached StatelessFunction, or null if not cached
   */
  public StatelessFunction getCachedFunction() {
    return cachedFunction;
  }

  /**
   * Set the cached function executor.
   * This is used by ExpressionEvaluator to cache the function on first lookup.
   *
   * @param function the StatelessFunction to cache
   */
  public void setCachedFunction(final StatelessFunction function) {
    this.cachedFunction = function;
  }
}
