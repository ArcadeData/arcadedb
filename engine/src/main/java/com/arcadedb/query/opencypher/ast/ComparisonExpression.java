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

import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.opencypher.temporal.CypherTemporalValue;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.List;

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

  private Object compareValuesTernary(final Object left, final Object right) {
    // In OpenCypher, any comparison involving null returns null
    if (left == null || right == null)
      return null;

    // Temporal comparison
    if (left instanceof CypherTemporalValue && right instanceof CypherTemporalValue) {
      try {
        final int cmp = ((CypherTemporalValue) left).compareTo((CypherTemporalValue) right);
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
    if (left instanceof Number && right instanceof Number) {
      // Use long comparison when both are integer types to avoid precision loss
      if ((left instanceof Long || left instanceof Integer) &&
          (right instanceof Long || right instanceof Integer)) {
        final long leftNum = ((Number) left).longValue();
        final long rightNum = ((Number) right).longValue();
        return switch (operator) {
          case EQUALS -> leftNum == rightNum;
          case NOT_EQUALS -> leftNum != rightNum;
          case LESS_THAN -> leftNum < rightNum;
          case GREATER_THAN -> leftNum > rightNum;
          case LESS_THAN_OR_EQUAL -> leftNum <= rightNum;
          case GREATER_THAN_OR_EQUAL -> leftNum >= rightNum;
        };
      }
      final double leftNum = ((Number) left).doubleValue();
      final double rightNum = ((Number) right).doubleValue();
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
    if (left instanceof Boolean && right instanceof Boolean) {
      // false < true in Cypher
      final int cmp = Boolean.compare((Boolean) left, (Boolean) right);
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
    if (left instanceof String && right instanceof String) {
      final int comparison = ((String) left).compareTo((String) right);
      return switch (operator) {
        case EQUALS -> comparison == 0;
        case NOT_EQUALS -> comparison != 0;
        case LESS_THAN -> comparison < 0;
        case GREATER_THAN -> comparison > 0;
        case LESS_THAN_OR_EQUAL -> comparison <= 0;
        case GREATER_THAN_OR_EQUAL -> comparison >= 0;
      };
    }

    // List comparison (element-wise with null propagation)
    if (left instanceof List && right instanceof List) {
      final List<?> leftList = (List<?>) left;
      final List<?> rightList = (List<?>) right;
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
    if (left instanceof java.util.Map && right instanceof java.util.Map) {
      final java.util.Map<?, ?> leftMap = (java.util.Map<?, ?>) left;
      final java.util.Map<?, ?> rightMap = (java.util.Map<?, ?>) right;
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
}
