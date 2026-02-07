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

import com.arcadedb.query.opencypher.temporal.*;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

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

    // List concatenation/append for + operator
    if (operator == Operator.ADD) {
      if (leftValue instanceof List && rightValue instanceof List) {
        final List<Object> combined = new ArrayList<>((List<?>) leftValue);
        combined.addAll((List<?>) rightValue);
        return combined;
      }
      if (leftValue instanceof List) {
        final List<Object> appended = new ArrayList<>((List<?>) leftValue);
        appended.add(rightValue);
        return appended;
      }
      if (rightValue instanceof List) {
        final List<Object> prepended = new ArrayList<>();
        prepended.add(leftValue);
        prepended.addAll((List<?>) rightValue);
        return prepended;
      }
    }

    // Temporal arithmetic: date/time ± duration, duration ± duration, duration * number
    final Object temporalResult = evaluateTemporalArithmetic(leftValue, rightValue, operator);
    if (temporalResult != null)
      return temporalResult;

    // Numeric operations
    if (!(leftValue instanceof Number) || !(rightValue instanceof Number))
      throw new IllegalArgumentException(
          "Arithmetic operations require numeric operands, got: " + leftValue.getClass().getSimpleName() + " and " + rightValue.getClass().getSimpleName());

    final Number leftNum = (Number) leftValue;
    final Number rightNum = (Number) rightValue;

    // Determine result type (preserve integer if possible)
    final boolean useInteger = isInteger(leftNum) && isInteger(rightNum) && operator != Operator.POWER;

    if (useInteger) {
      final long l = leftNum.longValue();
      final long r = rightNum.longValue();

      return switch (operator) {
        case ADD -> l + r;
        case SUBTRACT -> l - r;
        case MULTIPLY -> l * r;
        case DIVIDE -> r != 0 ? l / r : null; // Integer division (truncation)
        case MODULO -> r != 0 ? l % r : null;
        default -> null; // POWER handled below
      };
    }

    // Use double for division, power, and mixed types
    final double l = leftNum.doubleValue();
    final double r = rightNum.doubleValue();

    return switch (operator) {
      case ADD -> l + r;
      case SUBTRACT -> l - r;
      case MULTIPLY -> l * r;
      case DIVIDE -> l / r; // IEEE 754: 0.0/0.0=NaN, x/0.0=±Infinity
      case MODULO -> r != 0 ? l % r : Double.NaN;
      case POWER -> Math.pow(l, r);
    };
  }

  /**
   * Evaluate temporal arithmetic with pre-evaluated operands.
   * Can be called from ExpressionEvaluator with already-resolved values.
   *
   * @return the result, or null if not a temporal arithmetic operation
   */
  public static Object evaluateTemporalArithmetic(final Object leftValue, final Object rightValue, final Operator op) {
    // Duration + Duration or Duration - Duration
    if (leftValue instanceof CypherDuration ld && rightValue instanceof CypherDuration rd) {
      if (op == Operator.ADD)
        return ld.add(rd);
      if (op == Operator.SUBTRACT)
        return ld.subtract(rd);
    }

    // Duration * Number or Number * Duration
    if (op == Operator.MULTIPLY) {
      if (leftValue instanceof CypherDuration ld && rightValue instanceof Number)
        return ld.multiply(((Number) rightValue).doubleValue());
      if (leftValue instanceof Number && rightValue instanceof CypherDuration rd)
        return rd.multiply(((Number) leftValue).doubleValue());
    }

    // Duration / Number
    if (op == Operator.DIVIDE && leftValue instanceof CypherDuration ld && rightValue instanceof Number)
      return ld.divide(((Number) rightValue).doubleValue());

    // java.time.LocalDate (from ArcadeDB storage) ± Duration
    if (leftValue instanceof LocalDate ld && rightValue instanceof CypherDuration dur) {
      final LocalDate d = ld.plusMonths(op == Operator.ADD ? dur.getMonths() : -dur.getMonths())
          .plusDays(op == Operator.ADD ? dur.getDays() : -dur.getDays());
      return new CypherDate(d);
    }

    // java.time.LocalDateTime (from ArcadeDB storage) ± Duration
    if (leftValue instanceof LocalDateTime ldt && rightValue instanceof CypherDuration dur) {
      LocalDateTime dt = ldt
          .plusMonths(op == Operator.ADD ? dur.getMonths() : -dur.getMonths())
          .plusDays(op == Operator.ADD ? dur.getDays() : -dur.getDays())
          .plusSeconds(op == Operator.ADD ? dur.getSeconds() : -dur.getSeconds())
          .plusNanos(op == Operator.ADD ? dur.getNanosAdjustment() : -dur.getNanosAdjustment());
      return new CypherLocalDateTime(dt);
    }

    // Date ± Duration
    if (leftValue instanceof CypherDate cd && rightValue instanceof CypherDuration dur) {
      final LocalDate d = cd.getValue().plusMonths(op == Operator.ADD ? dur.getMonths() : -dur.getMonths())
          .plusDays(op == Operator.ADD ? dur.getDays() : -dur.getDays());
      return new CypherDate(d);
    }

    // LocalTime ± Duration
    if (leftValue instanceof CypherLocalTime clt && rightValue instanceof CypherDuration dur) {
      final long totalNanos = dur.getSeconds() * 1_000_000_000L + dur.getNanosAdjustment();
      final LocalTime t = op == Operator.ADD ? clt.getValue().plusNanos(totalNanos) : clt.getValue().minusNanos(totalNanos);
      return new CypherLocalTime(t);
    }

    // Time ± Duration
    if (leftValue instanceof CypherTime ct && rightValue instanceof CypherDuration dur) {
      final long totalNanos = dur.getSeconds() * 1_000_000_000L + dur.getNanosAdjustment();
      final OffsetTime t = op == Operator.ADD ? ct.getValue().plusNanos(totalNanos) : ct.getValue().minusNanos(totalNanos);
      return new CypherTime(t);
    }

    // LocalDateTime ± Duration
    if (leftValue instanceof CypherLocalDateTime cldt && rightValue instanceof CypherDuration dur) {
      LocalDateTime dt = cldt.getValue()
          .plusMonths(op == Operator.ADD ? dur.getMonths() : -dur.getMonths())
          .plusDays(op == Operator.ADD ? dur.getDays() : -dur.getDays())
          .plusSeconds(op == Operator.ADD ? dur.getSeconds() : -dur.getSeconds())
          .plusNanos(op == Operator.ADD ? dur.getNanosAdjustment() : -dur.getNanosAdjustment());
      return new CypherLocalDateTime(dt);
    }

    // DateTime ± Duration
    if (leftValue instanceof CypherDateTime cdt && rightValue instanceof CypherDuration dur) {
      ZonedDateTime dt = cdt.getValue()
          .plusMonths(op == Operator.ADD ? dur.getMonths() : -dur.getMonths())
          .plusDays(op == Operator.ADD ? dur.getDays() : -dur.getDays())
          .plusSeconds(op == Operator.ADD ? dur.getSeconds() : -dur.getSeconds())
          .plusNanos(op == Operator.ADD ? dur.getNanosAdjustment() : -dur.getNanosAdjustment());
      return new CypherDateTime(dt);
    }

    return null; // Not a temporal arithmetic operation
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
