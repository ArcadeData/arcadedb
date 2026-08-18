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

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

/**
 * Base interface for all Cypher expressions.
 * Expressions can be evaluated in the context of a query result.
 */
public interface Expression {
  /**
   * Evaluate this expression in the context of a query result.
   *
   * @param result  The current result row containing variables
   * @param context The command execution context
   * @return The evaluated value
   */
  Object evaluate(Result result, CommandContext context);

  /**
   * Returns true if this expression is an aggregation function.
   * Aggregation functions require special handling in execution.
   */
  boolean isAggregation();

  /**
   * Returns true if this expression contains an aggregation function,
   * either directly or nested within other expressions.
   * This is used to detect wrapped aggregations like head(collect(...)).
   * Default implementation delegates to isAggregation().
   */
  default boolean containsAggregation() {
    return isAggregation();
  }

  /**
   * Get the string representation of this expression.
   */
  String getText();

  /**
   * How a composite expression evaluates one of its own sub-expressions.
   * <p>
   * An expression that is more than the sum of its operands - a CASE, a list or map literal, an arithmetic
   * operation - has two callers: {@link #evaluate(Result, CommandContext)}, which resolves operands directly, and
   * {@code ExpressionEvaluator}, which resolves them through itself so an inline aggregator sees the pre-computed
   * overrides {@code AggregationStep} installs (issue #4100). Only the operand resolution differs, so it is
   * passed in and everything else - the part that says what the expression MEANS - is written once. Writing it
   * twice is what let a fix land on one path and not the other (issues #6323, #6354).
   */
  @FunctionalInterface
  interface SubEvaluator {
    Object evaluate(Expression expression);
  }
}
