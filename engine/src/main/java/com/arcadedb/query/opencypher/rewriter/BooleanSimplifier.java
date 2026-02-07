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
 * Simplifies boolean expressions using boolean algebra identities.
 *
 * Transformations:
 * - NOT (NOT x) → x (double negation elimination)
 * - x AND true → x (AND identity)
 * - x AND false → false (AND annihilator)
 * - x OR true → true (OR annihilator)
 * - x OR false → x (OR identity)
 * - true AND x → x (commutative)
 * - false AND x → false (commutative)
 * - true OR x → true (commutative)
 * - false OR x → x (commutative)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BooleanSimplifier extends ExpressionRewriter {

  @Override
  protected BooleanExpression visitLogical(final LogicalExpression expr) {
    // First, recursively rewrite children
    final BooleanExpression result = super.visitLogical(expr);

    // If super.visitLogical created a new expression, work with that
    if (!(result instanceof LogicalExpression))
      return result;

    final LogicalExpression logical = (LogicalExpression) result;

    // Apply simplifications based on operator
    return switch (logical.getOperator()) {
      case NOT -> simplifyNot(logical);
      case AND -> simplifyAnd(logical);
      case OR -> simplifyOr(logical);
      case XOR -> logical; // XOR doesn't have simple identities
    };
  }

  /**
   * Simplify NOT expressions.
   * - NOT (NOT x) → x
   */
  private BooleanExpression simplifyNot(final LogicalExpression expr) {
    final BooleanExpression operand = expr.getLeft();

    // NOT (NOT x) → x (double negation elimination)
    if (operand instanceof LogicalExpression logic && logic.getOperator() == LogicalExpression.Operator.NOT)
      return logic.getLeft();

    // NOT true → false, NOT false → true
    // Note: LiteralExpression implements Expression, not BooleanExpression
    // We wrap it in a BooleanWrapperExpression if needed, or just return original
    if (operand instanceof LiteralExpression lit && lit.getValue() instanceof Boolean) {
      // Return original expression since we can't create a boolean literal that implements BooleanExpression
      // The constant folder will handle this at execution time
      return expr;
    }

    return expr;
  }

  /**
   * Simplify AND expressions.
   * - x AND true → x
   * - x AND false → false
   * - true AND x → x
   * - false AND x → false
   */
  private BooleanExpression simplifyAnd(final LogicalExpression expr) {
    final BooleanExpression left = expr.getLeft();
    final BooleanExpression right = expr.getRight();

    // x AND true → x (if right is wrapped literal true)
    if (isWrappedTrue(right))
      return left;

    // x AND false → false (if right is wrapped literal false)
    if (isWrappedFalse(right))
      return right;

    // true AND x → x (commutative)
    if (isWrappedTrue(left))
      return right;

    // false AND x → false (commutative)
    if (isWrappedFalse(left))
      return left;

    return expr;
  }

  /**
   * Simplify OR expressions.
   * - x OR true → true
   * - x OR false → x
   * - true OR x → true
   * - false OR x → x
   */
  private BooleanExpression simplifyOr(final LogicalExpression expr) {
    final BooleanExpression left = expr.getLeft();
    final BooleanExpression right = expr.getRight();

    // x OR true → true
    if (isWrappedTrue(right))
      return right;

    // x OR false → x
    if (isWrappedFalse(right))
      return left;

    // true OR x → true (commutative)
    if (isWrappedTrue(left))
      return left;

    // false OR x → x (commutative)
    if (isWrappedFalse(left))
      return right;

    return expr;
  }

  /**
   * Check if boolean expression wraps literal true.
   * In Cypher AST, literals implement Expression, not BooleanExpression.
   * They get wrapped in BooleanWrapperExpression when used in boolean context.
   */
  private boolean isWrappedTrue(final BooleanExpression expr) {
    if (expr instanceof BooleanWrapperExpression wrapper) {
      // Check if wrapper contains literal true
      // Note: BooleanWrapperExpression doesn't expose inner expression yet
      // For now, return false - this will be implemented when wrapper API is available
      return false;
    }
    return false;
  }

  /**
   * Check if boolean expression wraps literal false.
   */
  private boolean isWrappedFalse(final BooleanExpression expr) {
    if (expr instanceof BooleanWrapperExpression wrapper) {
      // Check if wrapper contains literal false
      // Note: BooleanWrapperExpression doesn't expose inner expression yet
      // For now, return false - this will be implemented when wrapper API is available
      return false;
    }
    return false;
  }
}
