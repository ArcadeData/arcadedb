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

import java.util.function.Consumer;

/**
 * Wrapper that adapts a BooleanExpression to the Expression interface.
 * Used when boolean expressions need to be used in contexts that expect Expression
 * (e.g., CASE WHEN clauses).
 */
public class BooleanWrapperExpression implements Expression {
  private final BooleanExpression booleanExpression;

  public BooleanWrapperExpression(final BooleanExpression booleanExpression) {
    this.booleanExpression = booleanExpression;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    return booleanExpression.evaluateTernary(result, context);
  }

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public boolean containsAggregation() {
    return booleanContainsAggregation(booleanExpression);
  }

  static boolean booleanContainsAggregation(final BooleanExpression expr) {
    final boolean[] found = new boolean[1];
    forEachOperand(expr, operand -> found[0] |= operand.containsAggregation());
    return found[0];
  }

  /**
   * Hands {@code action} every operand expression of {@code expr}, descending through the boolean expressions an AND, OR
   * or NOT joins. {@link #booleanContainsAggregation} and the aggregation steps both walk a boolean expression with it, so
   * an aggregate under IN, IS NULL, STARTS WITH, =~ or a label check is seen by both (issue #11597).
   */
  public static void forEachOperand(final BooleanExpression expr, final Consumer<Expression> action) {
    switch (expr) {
    case null -> {
    }
    case ComparisonExpression comparison -> {
      accept(comparison.getLeft(), action);
      accept(comparison.getRight(), action);
    }
    case LogicalExpression logical -> {
      forEachOperand(logical.getLeft(), action);
      forEachOperand(logical.getRight(), action);
    }
    case InExpression in -> {
      accept(in.getExpression(), action);
      for (final Expression element : in.getList())
        accept(element, action);
    }
    case IsNullExpression isNull -> accept(isNull.getExpression(), action);
    case StringMatchExpression match -> {
      accept(match.getExpression(), action);
      accept(match.getPattern(), action);
    }
    case RegexExpression regex -> {
      accept(regex.getExpression(), action);
      accept(regex.getPattern(), action);
    }
    case LabelCheckExpression label -> accept(label.getVariableExpression(), action);
    case BooleanCoercionExpression coercion -> accept(coercion.getExpression(), action);
    case IsTypedExpression typed -> accept(typed.getValueExpression(), action);
    case IsNormalizedExpression normalized -> accept(normalized.getValueExpression(), action);
    default -> {
    }
    }
  }

  private static void accept(final Expression operand, final Consumer<Expression> action) {
    if (operand != null)
      action.accept(operand);
  }

  @Override
  public String getText() {
    return booleanExpression.getText();
  }

  public BooleanExpression getBooleanExpression() {
    return booleanExpression;
  }
}
