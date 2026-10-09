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
import com.arcadedb.function.misc.CoalesceFunction;
import com.arcadedb.query.opencypher.LoadCSVRowContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.List;
import java.util.Locale;
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

  /** Set once {@link #finishCoalesce} has checked the argument count, which is a property of the call site. */
  private transient volatile boolean arityValidated;

  public FunctionCallExpression(final String functionName, final List<Expression> arguments, final boolean distinct) {
    this.originalFunctionName = functionName;
    // Cypher functions are case-insensitive. Locale.ROOT and not the default locale: under a Turkish default,
    // "ISNAN".toLowerCase() is "ısnan" with a dotless i, which matches no registry entry, so a valid query would be
    // rejected as calling an unknown function.
    this.functionName = functionName.toLowerCase(Locale.ROOT);
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
          if (function instanceof CoalesceFunction) {
            Object value = null;
            for (int i = 0; i < arguments.size() && value == null; i++)
              value = arguments.get(i).evaluate(result, context);
            return finishCoalesce(function, value, result, context);
          }
          final Object[] args = new Object[arguments.size()];
          for (int i = 0; i < args.length; i++)
            args[i] = arguments.get(i).evaluate(result, context);
          return invoke(function, args, result, context);
        }
      }
    }
    throw new UnsupportedOperationException("Function evaluation requires StatelessFunction: " + functionName);
  }

  /**
   * Evaluates {@code coalesce()} one argument at a time and stops at the first non-null one, as the openCypher
   * reference does: an argument after it is never evaluated, so {@code coalesce(null, 0, COUNT { ... })} does not run the
   * COUNT (issue #9580), and {@code coalesce(1, 1/0)} does not raise. Every other function takes all of its arguments
   * evaluated up front. The two evaluation paths - the AST's own and the {@code ExpressionEvaluator}'s, with its
   * aggregation overrides - each loop over {@link #getArguments()} resolving one argument at a time, and call this once
   * they have the value, so the arity check and the row-scoped state publication live in one place and the hot path
   * allocates nothing.
   * <p>
   * This bypasses {@code CoalesceFunction.execute()}, which stays the contract for callers that already hold evaluated
   * arguments; a change to what coalesce means has to be made in both.
   *
   * @param value the first non-null argument, or {@code null} when every argument was null
   */
  public Object finishCoalesce(final StatelessFunction function, final Object value, final Result result,
      final CommandContext context) {
    // The argument count is fixed by the call site, so it is checked on the first row only
    if (!arityValidated) {
      validateArity(function);
      arityValidated = true;
    }
    LoadCSVRowContext.bind(result, context);
    return value;
  }

  /**
   * Runs a resolved Cypher function against pre-evaluated arguments, after publishing whatever state the function
   * is entitled to read off the row it is being evaluated on.
   * <p>
   * A {@code StatelessFunction} is handed arguments and a {@link CommandContext}, never the row, so a function whose
   * answer is a property of the row - {@code file()} and {@code linenumber()} after {@code LOAD CSV} - can only be
   * served by the caller publishing it first. Both evaluation paths call this rather than each doing it themselves:
   * only one of them did, so the same call answered differently in a {@code RETURN} and in a {@code WHERE} of the
   * same query (issue #6402). Same arrangement as {@code ArithmeticExpression.apply} (issue #6354) - the semantics
   * live in one place and each path only resolves its inputs.
   *
   * @param function the resolved executor
   * @param args     the already-evaluated arguments
   * @param result   the row being evaluated, {@code null} when there is none
   * @param context  the command context handed to the function
   */
  public static Object invoke(final StatelessFunction function, final Object[] args, final Result result,
      final CommandContext context) {
    function.checkArity(args);
    LoadCSVRowContext.bind(result, context);
    return function.execute(args, context);
  }

  /**
   * Validates this call's argument count against the declared bounds of {@code function}. The count is a property of the call
   * site, so an aggregation step checks it once when it builds the aggregator, before any row (an empty input included),
   * instead of on every row.
   */
  public void validateArity(final StatelessFunction function) {
    function.checkArity(new Object[arguments.size()]);
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

  /**
   * Returns the function name as it was written in the query. Function names are case-insensitive, so
   * this differs from {@link #getFunctionName()} only in case, and matters when the call has to be
   * rendered back or rebuilt without losing the spelling the user chose.
   */
  public String getOriginalFunctionName() {
    return originalFunctionName;
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
      case "count", "sum", "avg", "min", "max", "collect", "collect_list", "stdev", "stdev_samp", "stdevp", "stdev_pop", "percentilecont", "percentile_cont", "percentiledisc", "percentile_disc" -> true;
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
