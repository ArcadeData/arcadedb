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

import com.arcadedb.database.RID;
import com.arcadedb.function.graph.IdFunction;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.opencypher.temporal.CypherTemporalValue;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;

import java.time.temporal.Temporal;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Comparison expression for WHERE clauses.
 * Supports: =, !=, <, >, <=, >=
 * Example: n.age > 30, n.name = 'Alice'
 */
public class ComparisonExpression implements BooleanExpression {
  public enum Operator {
    EQUALS("="),
    NOT_EQUALS("!="),
    LESS_THAN("<"),
    GREATER_THAN(">"),
    LESS_THAN_OR_EQUAL("<="),
    GREATER_THAN_OR_EQUAL(">=");

    private final String symbol;

    Operator(final String symbol) {
      this.symbol = symbol;
    }

    public String getSymbol() {
      return symbol;
    }

    public static Operator fromString(final String str) {
      return switch (str) {
        case "=" -> EQUALS;
        case "!=" -> NOT_EQUALS;
        case "<" -> LESS_THAN;
        case ">" -> GREATER_THAN;
        case "<=" -> LESS_THAN_OR_EQUAL;
        case ">=" -> GREATER_THAN_OR_EQUAL;
        default -> throw new IllegalArgumentException("Unknown operator: " + str);
      };
    }
  }

  private final Expression left;
  private final Operator operator;
  private final Expression right;

  // Single-slot memo so an invariant temporal operand (e.g. a bound $parameter re-evaluated on every
  // scanned row) is wrapped into a CypherTemporalValue once instead of allocating a fresh wrapper per
  // row. volatile + an immutable {raw, coerced} pair keeps concurrent evaluators from seeing a torn pair.
  private volatile Object[] temporalCoercionMemo;

  public ComparisonExpression(final Expression left, final Operator operator, final Expression right) {
    this.left = left;
    this.operator = operator;
    this.right = right;
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    final Object ternary = evaluateTernary(result, context);
    return Boolean.TRUE.equals(ternary);
  }

  @Override
  public Object evaluateTernary(final Result result, final CommandContext context) {
    final Object leftValue;
    final Object rightValue;

    if (left instanceof FunctionCallExpression || right instanceof FunctionCallExpression) {
      leftValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(left, result, context);
      rightValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(right, result, context);
    } else {
      leftValue = left.evaluate(result, context);
      rightValue = right.evaluate(result, context);
    }

    return compareValuesTernary(leftValue, rightValue);
  }

  public Object evaluateWithValues(final Object leftValue, final Object rightValue) {
    return compareValuesTernary(leftValue, rightValue);
  }

  private Object compareValuesTernary(final Object left, final Object right) {
    // In OpenCypher, any comparison involving null returns null
    if (left == null || right == null)
      return null;

    // id()/elementId() interop: id() now returns a Long-encoded RID (Neo4j-compatible, issue #4183).
    // Legacy queries still pass an RID string parameter (e.g. {@code WHERE id(n) = "#1:0"}). Coerce
    // the Long-encoded side to its RID-string form and compare as RIDs, so the legacy pattern keeps
    // working without forcing callers to migrate. Both equality and ordering use the encoded long so
    // ordering stays consistent with id() / RID natural order. Only the RID-string form is coerced -
    // numeric strings are ambiguous and treating them as ids would break the Cypher TCK invariant
    // that 5 = "5" returns false.
    if (left instanceof Number leftNum && right instanceof String rightStr && RID.is(rightStr)) {
      final long leftEncoded = leftNum.longValue();
      final long rightEncoded = IdFunction.encodeRidAsLong(new RID(rightStr));
      return numericCompare(leftEncoded, rightEncoded);
    }
    if (left instanceof String leftStr && right instanceof Number rightNum && RID.is(leftStr)) {
      final long leftEncoded = IdFunction.encodeRidAsLong(new RID(leftStr));
      final long rightEncoded = rightNum.longValue();
      return numericCompare(leftEncoded, rightEncoded);
    }

    // Temporal comparison. Coerce native java.time / java.util.Date operands into Cypher temporal
    // values first, so a native temporal parameter (e.g. a datetime sent over Bolt, which resolves to
    // a raw java.time value) compares against a stored temporal instead of silently not matching.
    // Hot path: coerceTemporal short-circuits on the common numeric/string/boolean operand with a
    // single instanceof pair, and memoizes an invariant temporal operand to avoid per-row allocation.
    final Object leftTemporal = coerceTemporal(left);
    final Object rightTemporal = coerceTemporal(right);
    if (leftTemporal instanceof CypherTemporalValue value && rightTemporal instanceof CypherTemporalValue value1) {
      try {
        final int cmp = value.compareTo(value1);
        return switch (operator) {
          case EQUALS -> cmp == 0;
          case NOT_EQUALS -> cmp != 0;
          case LESS_THAN -> cmp < 0;
          case GREATER_THAN -> cmp > 0;
          case LESS_THAN_OR_EQUAL -> cmp <= 0;
          case GREATER_THAN_OR_EQUAL -> cmp >= 0;
        };
      } catch (final IllegalArgumentException e) {
        // Different temporal types: for equality, return false/true; for ordering, return null
        if (operator == Operator.EQUALS) return false;
        if (operator == Operator.NOT_EQUALS) return true;
        return null;
      }
    }

    // Numeric comparison
    if (left instanceof Number number && right instanceof Number number1) {
      // Use long comparison when both are integer types to avoid precision loss
      if ((left instanceof Long || left instanceof Integer) &&
          (right instanceof Long || right instanceof Integer)) {
        final long leftNum = number.longValue();
        final long rightNum = number1.longValue();
        return switch (operator) {
          case EQUALS -> leftNum == rightNum;
          case NOT_EQUALS -> leftNum != rightNum;
          case LESS_THAN -> leftNum < rightNum;
          case GREATER_THAN -> leftNum > rightNum;
          case LESS_THAN_OR_EQUAL -> leftNum <= rightNum;
          case GREATER_THAN_OR_EQUAL -> leftNum >= rightNum;
        };
      }
      final double leftNum = number.doubleValue();
      final double rightNum = number1.doubleValue();
      return switch (operator) {
        case EQUALS -> leftNum == rightNum;
        case NOT_EQUALS -> leftNum != rightNum;
        case LESS_THAN -> leftNum < rightNum;
        case GREATER_THAN -> leftNum > rightNum;
        case LESS_THAN_OR_EQUAL -> leftNum <= rightNum;
        case GREATER_THAN_OR_EQUAL -> leftNum >= rightNum;
      };
    }

    // Boolean comparison (booleans only compare with booleans)
    if (left instanceof Boolean boolean1 && right instanceof Boolean boolean2) {
      // false < true in Cypher
      final int cmp = Boolean.compare(boolean1, boolean2);
      return switch (operator) {
        case EQUALS -> cmp == 0;
        case NOT_EQUALS -> cmp != 0;
        case LESS_THAN -> cmp < 0;
        case GREATER_THAN -> cmp > 0;
        case LESS_THAN_OR_EQUAL -> cmp <= 0;
        case GREATER_THAN_OR_EQUAL -> cmp >= 0;
      };
    }

    // String comparison (strings only compare with strings)
    if (left instanceof String string && right instanceof String string1) {
      final int comparison = string.compareTo(string1);
      return switch (operator) {
        case EQUALS -> comparison == 0;
        case NOT_EQUALS -> comparison != 0;
        case LESS_THAN -> comparison < 0;
        case GREATER_THAN -> comparison > 0;
        case LESS_THAN_OR_EQUAL -> comparison <= 0;
        case GREATER_THAN_OR_EQUAL -> comparison >= 0;
      };
    }

    // List comparison (element-wise with null propagation).
    // Coerce List/Collection/array (incl. primitive arrays from numeric-array parameters, issue #4284) to a List.
    final List<Object> leftList = MultiValue.getMultiValueAsList(left);
    final List<Object> rightList = MultiValue.getMultiValueAsList(right);
    if (leftList != null && rightList != null) {
      if (operator == Operator.EQUALS || operator == Operator.NOT_EQUALS) {
        if (leftList.size() != rightList.size())
          return operator == Operator.NOT_EQUALS;
        boolean hasNull = false;
        for (int i = 0; i < leftList.size(); i++) {
          final Object elemResult = new ComparisonExpression(
              new LiteralExpression(leftList.get(i), ""), Operator.EQUALS,
              new LiteralExpression(rightList.get(i), ""))
              .evaluateTernary(null, null);
          if (elemResult == null)
            hasNull = true;
          else if (!Boolean.TRUE.equals(elemResult))
            return operator == Operator.NOT_EQUALS;
        }
        if (hasNull)
          return null;
        return operator == Operator.EQUALS;
      }
      // List ordering: lexicographic comparison with null propagation
      final int minSize = Math.min(leftList.size(), rightList.size());
      for (int i = 0; i < minSize; i++) {
        final Object elemCmp = new ComparisonExpression(
            new LiteralExpression(leftList.get(i), ""), Operator.EQUALS,
            new LiteralExpression(rightList.get(i), ""))
            .evaluateTernary(null, null);
        if (elemCmp == null)
          return null; // null element makes ordering undefined
        if (Boolean.TRUE.equals(elemCmp))
          continue; // elements are equal, compare next
        // Elements differ: check less than
        final Object ltResult = new ComparisonExpression(
            new LiteralExpression(leftList.get(i), ""), Operator.LESS_THAN,
            new LiteralExpression(rightList.get(i), ""))
            .evaluateTernary(null, null);
        if (ltResult == null)
          return null;
        final boolean isLess = Boolean.TRUE.equals(ltResult);
        return switch (operator) {
          case LESS_THAN -> isLess;
          case GREATER_THAN -> !isLess;
          case LESS_THAN_OR_EQUAL -> isLess;
          case GREATER_THAN_OR_EQUAL -> !isLess;
          default -> null;
        };
      }
      // All compared elements are equal; compare by length
      final int sizeCmp = Integer.compare(leftList.size(), rightList.size());
      return switch (operator) {
        case LESS_THAN -> sizeCmp < 0;
        case GREATER_THAN -> sizeCmp > 0;
        case LESS_THAN_OR_EQUAL -> sizeCmp <= 0;
        case GREATER_THAN_OR_EQUAL -> sizeCmp >= 0;
        default -> null;
      };
    }

    // Map comparison with 3VL null propagation
    if (left instanceof Map<?, ?> leftMap && right instanceof Map<?, ?> rightMap) {
      if (operator == Operator.EQUALS || operator == Operator.NOT_EQUALS) {
        // Different key sets means definitely not equal
        if (!leftMap.keySet().equals(rightMap.keySet()))
          return operator == Operator.NOT_EQUALS;
        // Same key set: compare values with 3VL
        boolean hasNull = false;
        for (final Object key : leftMap.keySet()) {
          final Object lv = leftMap.get(key);
          final Object rv = rightMap.get(key);
          final Object cmp = new ComparisonExpression(
              new LiteralExpression(lv, ""), Operator.EQUALS,
              new LiteralExpression(rv, "")).evaluateTernary(null, null);
          if (cmp == null)
            hasNull = true;
          else if (!Boolean.TRUE.equals(cmp))
            return operator == Operator.NOT_EQUALS;
        }
        if (hasNull)
          return null;
        return operator == Operator.EQUALS;
      }
      return null;
    }

    // Same-type objects: use equals() for = and <> (handles nodes, relationships, etc.)
    if (left.getClass().equals(right.getClass())) {
      if (operator == Operator.EQUALS)
        return left.equals(right);
      if (operator == Operator.NOT_EQUALS)
        return !left.equals(right);
    }

    // For = and <>, different types are simply not equal (return false/true)
    if (operator == Operator.EQUALS)
      return false;
    if (operator == Operator.NOT_EQUALS)
      return true;

    // For ordering operators (<, >, <=, >=), incompatible types return null
    return null;
  }

  @Override
  public String getText() {
    return left.getText() + " " + operator.getSymbol() + " " + right.getText();
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

  /**
   * Wrap a native {@code java.time} / {@code java.util.Date} operand into its Cypher temporal type,
   * returning any other value unchanged. Non-temporal operands (the common numeric/string/boolean case)
   * short-circuit on two instanceof checks; an invariant temporal operand re-evaluated across rows
   * (typically a bound parameter) is coerced once and served from a single-slot memo thereafter.
   */
  private Object coerceTemporal(final Object value) {
    if (!(value instanceof Temporal || value instanceof Date))
      return value;
    final Object[] memo = temporalCoercionMemo;
    if (memo != null && memo[0] == value)
      return memo[1];
    final Object coerced = TemporalUtil.fromCoreJavaType(value);
    temporalCoercionMemo = new Object[] { value, coerced };
    return coerced;
  }

  private Boolean numericCompare(final long leftNum, final long rightNum) {
    return switch (operator) {
      case EQUALS -> leftNum == rightNum;
      case NOT_EQUALS -> leftNum != rightNum;
      case LESS_THAN -> leftNum < rightNum;
      case GREATER_THAN -> leftNum > rightNum;
      case LESS_THAN_OR_EQUAL -> leftNum <= rightNum;
      case GREATER_THAN_OR_EQUAL -> leftNum >= rightNum;
    };
  }
}
