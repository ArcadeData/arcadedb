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
 * Logical expression that implements Cypher's three-valued (ternary) logic.
 * Used in Expression contexts (RETURN, WITH projections) where null must be preserved.
 * <p>
 * Three-valued logic truth tables:
 * <pre>
 * AND:  true AND null  = null    false AND null = false   null AND null = null
 * OR:   true OR null   = true    false OR null  = null    null OR null  = null
 * NOT:  NOT null       = null
 * </pre>
 */
public class TernaryLogicalExpression implements Expression {
  public enum Operator {
    AND, OR, NOT, XOR
  }

  private final Operator operator;
  private final Expression left;
  private final Expression right; // null for NOT

  public TernaryLogicalExpression(final Operator operator, final Expression left, final Expression right) {
    this.operator = operator;
    this.left = left;
    this.right = right;
  }

  public TernaryLogicalExpression(final Operator operator, final Expression operand) {
    this(operator, operand, null);
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    return switch (operator) {
      case AND -> evaluateAnd(result, context);
      case OR -> evaluateOr(result, context);
      case NOT -> evaluateNot(result, context);
      case XOR -> evaluateXor(result, context);
    };
  }

  private Object evaluateAnd(final Result result, final CommandContext context) {
    final Object leftVal = left.evaluate(result, context);
    final Object rightVal = right.evaluate(result, context);

    final Boolean leftBool = toBoolean(leftVal);
    final Boolean rightBool = toBoolean(rightVal);

    // false AND anything = false
    if (Boolean.FALSE.equals(leftBool) || Boolean.FALSE.equals(rightBool))
      return false;

    // If either is null (unknown), result is null
    if (leftBool == null || rightBool == null)
      return null;

    // Both are true
    return true;
  }

  private Object evaluateOr(final Result result, final CommandContext context) {
    final Object leftVal = left.evaluate(result, context);
    final Object rightVal = right.evaluate(result, context);

    final Boolean leftBool = toBoolean(leftVal);
    final Boolean rightBool = toBoolean(rightVal);

    // true OR anything = true
    if (Boolean.TRUE.equals(leftBool) || Boolean.TRUE.equals(rightBool))
      return true;

    // If either is null (unknown), result is null
    if (leftBool == null || rightBool == null)
      return null;

    // Both are false
    return false;
  }

  private Object evaluateNot(final Result result, final CommandContext context) {
    final Object leftVal = left.evaluate(result, context);
    final Boolean leftBool = toBoolean(leftVal);

    if (leftBool == null)
      return null;

    return !leftBool;
  }

  private Object evaluateXor(final Result result, final CommandContext context) {
    final Object leftVal = left.evaluate(result, context);
    final Object rightVal = right.evaluate(result, context);

    final Boolean leftBool = toBoolean(leftVal);
    final Boolean rightBool = toBoolean(rightVal);

    if (leftBool == null || rightBool == null)
      return null;

    return leftBool ^ rightBool;
  }

  private static Boolean toBoolean(final Object value) {
    if (value == null)
      return null;
    if (value instanceof Boolean)
      return (Boolean) value;
    // Non-null, non-boolean values are truthy in Cypher
    return true;
  }

  public Operator getOperator() {
    return operator;
  }

  public Expression getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }

  @Override
  public boolean isAggregation() {
    return false;
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
}
