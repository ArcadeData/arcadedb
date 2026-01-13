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
    final Object leftValue = left.evaluate(result, context);
    final Object rightValue = right.evaluate(result, context);

    return compareValues(leftValue, rightValue);
  }

  private boolean compareValues(final Object left, final Object right) {
    // Handle null comparisons
    if (left == null || right == null) {
      return switch (operator) {
        case EQUALS -> left == right;
        case NOT_EQUALS -> left != right;
        default -> false;
      };
    }

    // Numeric comparison
    if (left instanceof Number && right instanceof Number) {
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

    // String comparison
    final String leftStr = left.toString();
    final String rightStr = right.toString();
    final int comparison = leftStr.compareTo(rightStr);

    return switch (operator) {
      case EQUALS -> comparison == 0;
      case NOT_EQUALS -> comparison != 0;
      case LESS_THAN -> comparison < 0;
      case GREATER_THAN -> comparison > 0;
      case LESS_THAN_OR_EQUAL -> comparison <= 0;
      case GREATER_THAN_OR_EQUAL -> comparison >= 0;
    };
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
