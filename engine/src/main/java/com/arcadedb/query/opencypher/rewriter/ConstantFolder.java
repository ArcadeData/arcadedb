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
package com.arcadedb.query.opencypher.rewriter;

import com.arcadedb.query.opencypher.ast.*;

/**
 * Folds constant expressions into literal values at rewrite time.
 * Reduces runtime computation for expressions with all-constant operands.
 *
 * Transformations:
 * - 1 + 2 → 3 (numeric arithmetic)
 * - 'hello' + ' world' → 'hello world' (string concatenation)
 * - 10 * 2 - 5 → 15 (nested arithmetic)
 * - 5 > 3 → true (constant comparisons)
 * - true AND false → false (boolean constants)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ConstantFolder extends ExpressionRewriter {

  @Override
  protected Expression visitArithmetic(final ArithmeticExpression expr) {
    // First, recursively rewrite children
    final Expression result = super.visitArithmetic(expr);

    if (!(result instanceof ArithmeticExpression))
      return result;

    final ArithmeticExpression arith = (ArithmeticExpression) result;

    // Check if both operands are literals
    if (!(arith.getLeft() instanceof LiteralExpression) || !(arith.getRight() instanceof LiteralExpression))
      return arith;

    final Object leftValue = ((LiteralExpression) arith.getLeft()).getValue();
    final Object rightValue = ((LiteralExpression) arith.getRight()).getValue();

    // Null propagation
    if (leftValue == null || rightValue == null)
      return new LiteralExpression(null, "null");

    try {
      final Object foldedValue = foldArithmetic(leftValue, rightValue, arith.getOperator());
      if (foldedValue != null)
        return new LiteralExpression(foldedValue, String.valueOf(foldedValue));
    } catch (final Exception e) {
      // If folding fails (e.g., division by zero, type mismatch), keep original expression
      // The error will be caught at execution time with proper context
    }

    return arith;
  }

  @Override
  protected BooleanExpression visitComparison(final ComparisonExpression expr) {
    // First, recursively rewrite children
    final BooleanExpression result = super.visitComparison(expr);

    if (!(result instanceof ComparisonExpression))
      return result;

    final ComparisonExpression comp = (ComparisonExpression) result;

    // Check if both operands are literals
    if (!(comp.getLeft() instanceof LiteralExpression) || !(comp.getRight() instanceof LiteralExpression))
      return comp;

    final Object leftValue = ((LiteralExpression) comp.getLeft()).getValue();
    final Object rightValue = ((LiteralExpression) comp.getRight()).getValue();

    try {
      final Object foldedValue = foldComparison(leftValue, rightValue, comp.getOperator());
      if (foldedValue != null) {
        // Note: LiteralExpression implements Expression, not BooleanExpression
        // We need to wrap it in something that implements BooleanExpression
        // For now, keep the original comparison expression
        // This will be handled when we have a proper wrapper
        return comp;
      }
    } catch (final Exception e) {
      // Keep original expression if folding fails
    }

    return comp;
  }

  /**
   * Fold a constant arithmetic operation.
   * <p>
   * The semantics are not decided here: they are the ones {@link ArithmeticExpression#apply} applies at execution
   * time, and folding an expression must not change what it means. This method used to carry its own copy of them
   * - null handling, list and string concatenation, numeric promotion, the double switch - which made three
   * places where a fix could land on two of them (issue #6354). What belongs here is only the folder's contract:
   * the caller keeps the expression unfolded when this returns {@code null} or throws, so an operation that fails
   * fails at execution time, where the query text is available to report it against.
   */
  private Object foldArithmetic(final Object left, final Object right, final ArithmeticExpression.Operator op) {
    return ArithmeticExpression.apply(op, left, right);
  }

  /**
   * Fold constant comparison operations.
   */
  private Object foldComparison(final Object left, final Object right, final ComparisonExpression.Operator op) {
    // Null comparison returns null
    if (left == null || right == null)
      return null;

    // Numeric comparison
    if (left instanceof Number && right instanceof Number) {
      final double l = ((Number) left).doubleValue();
      final double r = ((Number) right).doubleValue();

      return switch (op) {
        case EQUALS -> l == r;
        case NOT_EQUALS -> l != r;
        case LESS_THAN -> l < r;
        case GREATER_THAN -> l > r;
        case LESS_THAN_OR_EQUAL -> l <= r;
        case GREATER_THAN_OR_EQUAL -> l >= r;
      };
    }

    // Boolean comparison
    if (left instanceof Boolean && right instanceof Boolean) {
      final int cmp = Boolean.compare((Boolean) left, (Boolean) right);
      return switch (op) {
        case EQUALS -> cmp == 0;
        case NOT_EQUALS -> cmp != 0;
        case LESS_THAN -> cmp < 0;
        case GREATER_THAN -> cmp > 0;
        case LESS_THAN_OR_EQUAL -> cmp <= 0;
        case GREATER_THAN_OR_EQUAL -> cmp >= 0;
      };
    }

    // String comparison
    if (left instanceof String && right instanceof String) {
      final int cmp = ((String) left).compareTo((String) right);
      return switch (op) {
        case EQUALS -> cmp == 0;
        case NOT_EQUALS -> cmp != 0;
        case LESS_THAN -> cmp < 0;
        case GREATER_THAN -> cmp > 0;
        case LESS_THAN_OR_EQUAL -> cmp <= 0;
        case GREATER_THAN_OR_EQUAL -> cmp >= 0;
      };
    }

    // For equals/not equals, use Java equals
    if (op == ComparisonExpression.Operator.EQUALS)
      return left.equals(right);
    if (op == ComparisonExpression.Operator.NOT_EQUALS)
      return !left.equals(right);

    return null;
  }
}
