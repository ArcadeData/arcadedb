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
 * Expression representing an arithmetic operation.
 * Supports: +, -, *, /, %, ^ (power)
 * Example: n.age * 2, n.value + 10, n.count / 2
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArithmeticExpression implements Expression {
  public enum Operator {
    ADD("+"),
    SUBTRACT("-"),
    MULTIPLY("*"),
    DIVIDE("/"),
    MODULO("%"),
    POWER("^");

    private final String symbol;

    Operator(final String symbol) {
      this.symbol = symbol;
    }

    public String getSymbol() {
      return symbol;
    }

    public static Operator fromString(final String str) {
      return switch (str) {
        case "+" -> ADD;
        case "-" -> SUBTRACT;
        case "*" -> MULTIPLY;
        case "/" -> DIVIDE;
        case "%" -> MODULO;
        case "^", "**" -> POWER;
        default -> throw new IllegalArgumentException("Unknown arithmetic operator: " + str);
      };
    }
  }

  private final Expression left;
  private final Operator operator;
  private final Expression right;
  private final String text;

  public ArithmeticExpression(final Expression left, final Operator operator, final Expression right) {
    this.left = left;
    this.operator = operator;
    this.right = right;
    this.text = left.getText() + " " + operator.getSymbol() + " " + right.getText();
  }

  public ArithmeticExpression(final Expression left, final Operator operator, final Expression right, final String text) {
    this.left = left;
    this.operator = operator;
    this.right = right;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    final Object leftValue = left.evaluate(result, context);
    final Object rightValue = right.evaluate(result, context);

    // Handle null values
    if (leftValue == null || rightValue == null)
      return null;

    // String concatenation for + operator
    if (operator == Operator.ADD && (leftValue instanceof String || rightValue instanceof String))
      return leftValue.toString() + rightValue.toString();

    // Numeric operations
    if (!(leftValue instanceof Number) || !(rightValue instanceof Number))
      throw new IllegalArgumentException(
          "Arithmetic operations require numeric operands, got: " + leftValue.getClass().getSimpleName() + " and " + rightValue.getClass().getSimpleName());

    final Number leftNum = (Number) leftValue;
    final Number rightNum = (Number) rightValue;

    // Determine result type (preserve integer if possible)
    final boolean useInteger = isInteger(leftNum) && isInteger(rightNum) && operator != Operator.DIVIDE && operator != Operator.POWER;

    if (useInteger) {
      final long l = leftNum.longValue();
      final long r = rightNum.longValue();

      return switch (operator) {
        case ADD -> l + r;
        case SUBTRACT -> l - r;
        case MULTIPLY -> l * r;
        case MODULO -> r != 0 ? l % r : null;
        default -> null; // DIVIDE and POWER handled below
      };
    }

    // Use double for division, power, and mixed types
    final double l = leftNum.doubleValue();
    final double r = rightNum.doubleValue();

    return switch (operator) {
      case ADD -> l + r;
      case SUBTRACT -> l - r;
      case MULTIPLY -> l * r;
      case DIVIDE -> r != 0 ? l / r : null;
      case MODULO -> r != 0 ? l % r : null;
      case POWER -> Math.pow(l, r);
    };
  }

  private boolean isInteger(final Number num) {
    return num instanceof Integer || num instanceof Long || num instanceof Short || num instanceof Byte;
  }

  @Override
  public boolean isAggregation() {
    return left.isAggregation() || right.isAggregation();
  }

  @Override
  public boolean containsAggregation() {
    return left.containsAggregation() || right.containsAggregation();
  }

  @Override
  public String getText() {
    return text;
  }

  public Expression getLeft() {
    return left;
  }

  public Operator getOperator() {
    return operator;
  }

  public Expression getRight() {
    return right;
  }
}
