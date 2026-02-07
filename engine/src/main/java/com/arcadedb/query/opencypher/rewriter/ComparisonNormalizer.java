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
 * Normalizes comparison expressions to canonical form for better optimizer matching.
 *
 * Goal: Put property/variable access on the left side of comparisons to help
 * the optimizer identify index-seekable conditions.
 *
 * Transformations:
 * - 5 < x.age → x.age > 5 (property on left)
 * - 10 > n.count → n.count < 10 (property on left)
 * - constant = variable → variable = constant (property on left)
 *
 * Benefits:
 * - Consistent form makes optimizer pattern matching simpler
 * - Index selection rules can assume "property op constant" pattern
 * - Easier to identify seekable conditions
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ComparisonNormalizer extends ExpressionRewriter {

  @Override
  protected BooleanExpression visitComparison(final ComparisonExpression expr) {
    // First, recursively rewrite children
    final BooleanExpression result = super.visitComparison(expr);

    if (!(result instanceof ComparisonExpression))
      return result;

    final ComparisonExpression comp = (ComparisonExpression) result;

    // Check if we should swap operands
    // Heuristic: if right side is "more interesting" than left, swap
    if (shouldSwap(comp.getLeft(), comp.getRight()))
      return swapComparison(comp);

    return comp;
  }

  /**
   * Determine if we should swap operands.
   * We want "interesting" expressions (variables, properties) on the left,
   * and "simple" expressions (literals, parameters) on the right.
   */
  private boolean shouldSwap(final Expression left, final Expression right) {
    final int leftScore = getInterestScore(left);
    final int rightScore = getInterestScore(right);

    // Swap if right is more interesting than left
    return rightScore > leftScore;
  }

  /**
   * Score expression "interestingness".
   * Higher score = more interesting (should be on left side).
   *
   * Scoring:
   * - PropertyAccessExpression: 3 (e.g., n.age)
   * - VariableExpression: 2 (e.g., n)
   * - FunctionCallExpression: 1 (e.g., id(n))
   * - LiteralExpression: 0 (e.g., 42, 'hello')
   * - ParameterExpression: 0 (e.g., $param)
   * - Other: 1 (default)
   */
  private int getInterestScore(final Expression expr) {
    if (expr instanceof PropertyAccessExpression)
      return 3; // Most interesting: property access (indexable)
    if (expr instanceof VariableExpression)
      return 2; // Interesting: variable reference
    if (expr instanceof FunctionCallExpression)
      return 1; // Less interesting: function call (not directly indexable)
    if (expr instanceof LiteralExpression || expr instanceof ParameterExpression)
      return 0; // Not interesting: constants
    return 1; // Default: somewhat interesting
  }

  /**
   * Swap comparison operands and flip operator.
   * Examples:
   * - 5 < x → x > 5
   * - 10 >= y → y <= 10
   */
  private ComparisonExpression swapComparison(final ComparisonExpression expr) {
    final ComparisonExpression.Operator flipped = flipOperator(expr.getOperator());
    return new ComparisonExpression(expr.getRight(), flipped, expr.getLeft());
  }

  /**
   * Flip comparison operator for swapped operands.
   */
  private ComparisonExpression.Operator flipOperator(final ComparisonExpression.Operator op) {
    return switch (op) {
      case LESS_THAN -> ComparisonExpression.Operator.GREATER_THAN;
      case GREATER_THAN -> ComparisonExpression.Operator.LESS_THAN;
      case LESS_THAN_OR_EQUAL -> ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
      case GREATER_THAN_OR_EQUAL -> ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
      case EQUALS, NOT_EQUALS -> op; // Symmetric operators don't change
    };
  }
}
