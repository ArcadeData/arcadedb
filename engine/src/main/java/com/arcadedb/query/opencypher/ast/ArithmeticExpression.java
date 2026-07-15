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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.opencypher.temporal.*;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
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
    POWER("^"),
    CONCAT("||");

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

    // GQL / Cypher 25 concatenation operator || (strict typing, no implicit coercion, issue #5298).
    if (operator == Operator.CONCAT)
      return concatenate(leftValue, rightValue);

    // List concatenation/append for + operator (must be checked before string concatenation).
    // Coerce List/Collection/array (incl. primitive arrays from numeric-array parameters, issue #4284) to a List.
    if (operator == Operator.ADD) {
      final List<Object> leftList = MultiValue.getMultiValueAsList(leftValue);
      final List<Object> rightList = MultiValue.getMultiValueAsList(rightValue);
      if (leftList != null && rightList != null) {
        final List<Object> combined = new ArrayList<>(leftList);
        combined.addAll(rightList);
        return combined;
      }
      if (leftList != null) {
        final List<Object> appended = new ArrayList<>(leftList);
        appended.add(rightValue);
        return appended;
      }
      if (rightList != null) {
        final List<Object> prepended = new ArrayList<>();
        prepended.add(leftValue);
        prepended.addAll(rightList);
        return prepended;
      }
    }

    // String concatenation for + operator
    if (operator == Operator.ADD && (leftValue instanceof String || rightValue instanceof String))
      return leftValue.toString() + rightValue.toString();

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

    if (useInteger)
      return integerArithmetic(operator, leftNum.longValue(), rightNum.longValue());

    // Use double for division, power, and mixed types
    final double l = leftNum.doubleValue();
    final double r = rightNum.doubleValue();

    return switch (operator) {
      case ADD -> l + r;
      case SUBTRACT -> l - r;
      case MULTIPLY -> l * r;
      case DIVIDE -> l / r; // IEEE 754: 0.0/0.0=NaN, x/0.0=±Infinity (matches Neo4j / OpenCypher TCK)
      case MODULO -> r != 0 ? l % r : Double.NaN;
      case POWER -> Math.pow(l, r);
      case CONCAT -> throw new IllegalStateException("CONCAT is handled before numeric arithmetic");
    };
  }

  /**
   * Integer division/modulo by zero is an arithmetic error in Cypher (like Neo4j, and as required by the
   * OpenCypher TCK), not a silent {@code null}. Fail the query so callers can tell a real error from a
   * legitimate null (issue #5163). Floating-point division by zero is left to IEEE 754 semantics
   * ({@code Infinity}/{@code NaN}): the OpenCypher TCK requires {@code 0.0 / 0.0} to yield {@code NaN}.
   */
  public static void checkIntegerDivisorNotZero(final Operator op, final long divisor) {
    if (divisor == 0)
      throw new CommandExecutionException(op == Operator.MODULO ? "% by zero" : "/ by zero");
  }

  /**
   * Perform integer (64-bit) arithmetic that fails the query on overflow instead of silently wrapping around with
   * two's-complement semantics (issue #5164). Neo4j (the OpenCypher reference implementation) raises an arithmetic
   * error - {@code long overflow} - for {@code +}, {@code -} and {@code *} that exceed the {@code long} range, and
   * also for the single division overflow case {@code Long.MIN_VALUE / -1}. Silent wraparound produces
   * mathematically wrong results that look valid and can be persisted to storage, so we match Neo4j and throw.
   * <p>
   * Only pure-integer operations are checked here; mixed or floating-point arithmetic keeps IEEE 754 semantics
   * (overflow becomes {@code ±Infinity}), matching Neo4j.
   */
  public static long integerArithmetic(final Operator op, final long l, final long r) {
    try {
      return switch (op) {
        case ADD -> Math.addExact(l, r);
        case SUBTRACT -> Math.subtractExact(l, r);
        case MULTIPLY -> Math.multiplyExact(l, r);
        case DIVIDE -> { checkIntegerDivisorNotZero(op, r); yield Math.divideExact(l, r); } // truncates toward zero, guards Long.MIN_VALUE / -1
        case MODULO -> { checkIntegerDivisorNotZero(op, r); yield l % r; }
        case POWER -> throw new IllegalStateException("POWER is not an integer operation");
        case CONCAT -> throw new IllegalStateException("CONCAT is not an integer operation");
      };
    } catch (final ArithmeticException e) {
      throw new CommandExecutionException("long overflow", e);
    }
  }

  /**
   * GQL / Cypher 25 string-or-list concatenation operator {@code ||}. Unlike {@code +}, it does NOT implicitly
   * coerce operands: concatenating a STRING with a non-STRING (or a LIST with a non-LIST) is a type error in Neo4j
   * (the OpenCypher reference implementation), issue #5298. Both operands must be STRING, or both must be LIST;
   * {@code toString()} is required to concatenate non-STRING values. {@code null} operands propagate to {@code null}
   * and are handled by the callers before reaching here. Note that {@code +} can append a single element to a LIST,
   * while {@code ||} cannot - that too is a type error, matching Neo4j.
   */
  public static Object concatenate(final Object leftValue, final Object rightValue) {
    final List<Object> leftList = MultiValue.getMultiValueAsList(leftValue);
    final List<Object> rightList = MultiValue.getMultiValueAsList(rightValue);
    if (leftList != null && rightList != null) {
      final List<Object> combined = new ArrayList<>(leftList);
      combined.addAll(rightList);
      return combined;
    }

    if (leftValue instanceof String && rightValue instanceof String)
      return (String) leftValue + rightValue;

    throw new CommandSemanticException(
        "Type mismatch: both operands of the '||' concatenation operator must be STRING or both must be LIST, but got "
            + leftValue.getClass().getSimpleName() + " and " + rightValue.getClass().getSimpleName()
            + " (use toString() to convert non-STRING values)");
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
      // For dates, full days from the seconds component must be carried over
      final long extraDays = dur.getSeconds() / 86400;
      final long totalDays = dur.getDays() + extraDays;
      final LocalDate d = ld.plusMonths(op == Operator.ADD ? dur.getMonths() : -dur.getMonths())
          .plusDays(op == Operator.ADD ? totalDays : -totalDays);
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
      // For dates, full days from the seconds component must be carried over
      final long extraDays = dur.getSeconds() / 86400;
      final long totalDays = dur.getDays() + extraDays;
      final LocalDate d = cd.getValue().plusMonths(op == Operator.ADD ? dur.getMonths() : -dur.getMonths())
          .plusDays(op == Operator.ADD ? totalDays : -totalDays);
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
