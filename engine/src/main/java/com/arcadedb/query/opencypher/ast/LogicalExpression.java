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
 * Logical expression for combining boolean conditions.
 * Supports: AND, OR, NOT
 * Example: n.age > 30 AND n.city = 'NYC', NOT n.active
 */
public class LogicalExpression implements BooleanExpression {
  public enum Operator {
    AND,
    OR,
    NOT,
    XOR
  }

  private final Operator operator;
  private final BooleanExpression left;
  private final BooleanExpression right; // null for NOT

  /**
   * Whether an operand runs a subquery or pattern match per row (see {@link ExpressionCost}). Fixed at construction: the
   * tree is immutable, and the answer decides the operand order on every evaluation of every row.
   */
  private final boolean leftCostly;
  private final boolean rightCostly;

  /**
   * AND and OR are commutative, nulls included, so the operand order is free: the one that cannot run a subquery goes
   * first and may make the other unnecessary (issue #9579).
   */
  private final BooleanExpression first;
  private final BooleanExpression second;

  public LogicalExpression(final Operator operator, final BooleanExpression left, final BooleanExpression right) {
    this.operator = operator;
    this.left = left;
    this.right = right;
    this.leftCostly = ExpressionCost.isCostly(left);
    this.rightCostly = ExpressionCost.isCostly(right);
    final boolean swap = leftCostly && !rightCostly;
    this.first = swap ? right : left;
    this.second = swap ? left : right;
  }

  public LogicalExpression(final Operator operator, final BooleanExpression operand) {
    this(operator, operand, null);
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    final Object ternary = evaluateTernary(result, context);
    return Boolean.TRUE.equals(ternary);
  }

  @Override
  public Object evaluateTernary(final Result result, final CommandContext context) {
    return switch (operator) {
      case AND -> evaluateAnd(result, context);
      case OR -> evaluateOr(result, context);
      case NOT -> evaluateNot(result, context);
      case XOR -> evaluateXor(result, context);
    };
  }

  /**
   * True when this node evaluates a traversal-running operand, so an enclosing {@code AND}/{@code OR} knows to put it last.
   */
  boolean isCostly() {
    return leftCostly || rightCostly;
  }

  private Object evaluateAnd(final Result result, final CommandContext context) {
    final Boolean leftBool = toBoolean(first.evaluateTernary(result, context));

    // false AND anything = false: the other operand is result-irrelevant, do not evaluate it.
    if (Boolean.FALSE.equals(leftBool))
      return false;

    final Boolean rightBool = toBoolean(second.evaluateTernary(result, context));
    if (Boolean.FALSE.equals(rightBool))
      return false;
    if (leftBool == null || rightBool == null)
      return null;
    return true;
  }

  private Object evaluateOr(final Result result, final CommandContext context) {
    final Boolean leftBool = toBoolean(first.evaluateTernary(result, context));

    // true OR anything = true: the other operand is result-irrelevant, do not evaluate it.
    if (Boolean.TRUE.equals(leftBool))
      return true;

    final Boolean rightBool = toBoolean(second.evaluateTernary(result, context));
    if (Boolean.TRUE.equals(rightBool))
      return true;
    if (leftBool == null || rightBool == null)
      return null;
    return false;
  }

  private Object evaluateNot(final Result result, final CommandContext context) {
    final Boolean leftBool = toBoolean(left.evaluateTernary(result, context));
    if (leftBool == null)
      return null;
    return !leftBool;
  }

  private Object evaluateXor(final Result result, final CommandContext context) {
    final Boolean leftBool = toBoolean(left.evaluateTernary(result, context));
    final Boolean rightBool = toBoolean(right.evaluateTernary(result, context));

    if (leftBool == null || rightBool == null)
      return null;
    return leftBool ^ rightBool;
  }

  private static Boolean toBoolean(final Object value) {
    if (value == null)
      return null;
    if (value instanceof Boolean)
      return (Boolean) value;
    return true;
  }

  @Override
  public String getText() {
    return switch (operator) {
      case NOT -> "NOT " + left.getText();
      case AND -> "(" + left.getText() + " AND " + right.getText() + ")";
      case OR -> "(" + left.getText() + " OR " + right.getText() + ")";
      case XOR -> "(" + left.getText() + " XOR " + right.getText() + ")";
    };
  }

  public Operator getOperator() {
    return operator;
  }

  public BooleanExpression getLeft() {
    return left;
  }

  public BooleanExpression getRight() {
    return right;
  }
}
