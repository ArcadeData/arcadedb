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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for CompositeRewriter that chains multiple rewrite rules.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CompositeRewriterTest {

  @Test
  public void testChainedRewrites() {
    // Test: 5 < (1 + 2)
    // Step 1 (ConstantFolder): 5 < (1 + 2) → 5 < 3
    // Step 2 (ComparisonNormalizer): 5 < 3 stays as-is (both are literals, equal interestingness)

    final ArithmeticExpression arithmetic = new ArithmeticExpression(
        new LiteralExpression(1, "1"),
        ArithmeticExpression.Operator.ADD,
        new LiteralExpression(2, "2")
    );

    final ComparisonExpression comparison = new ComparisonExpression(
        new LiteralExpression(5, "5"),
        ComparisonExpression.Operator.LESS_THAN,
        arithmetic
    );

    final ExpressionRewriter rewriter = new CompositeRewriter(
        new ConstantFolder(),         // Fold 1 + 2 → 3
        new ComparisonNormalizer()    // No change (5 < 3, both literals)
    );

    final BooleanExpression result = (BooleanExpression) rewriter.rewrite(comparison);

    assertThat(result).isInstanceOf(ComparisonExpression.class);
    final ComparisonExpression comp = (ComparisonExpression) result;

    // After constant folding (normalizer doesn't swap equal-interest literals):
    // 5 < 3 (both literals)
    assertThat(comp.getLeft()).isInstanceOf(LiteralExpression.class);
    assertThat(((LiteralExpression) comp.getLeft()).getValue()).isEqualTo(5);

    assertThat(comp.getOperator()).isEqualTo(ComparisonExpression.Operator.LESS_THAN);

    assertThat(comp.getRight()).isInstanceOf(LiteralExpression.class);
    assertThat(((LiteralExpression) comp.getRight()).getValue()).isEqualTo(3L);
  }

  @Test
  public void testEmptyRewriterList() {
    // Empty rewriter should return original expression
    final ExpressionRewriter rewriter = new CompositeRewriter();

    final LiteralExpression expr = new LiteralExpression(42, "42");
    final Expression result = (Expression) rewriter.rewrite(expr);

    assertThat(result).isSameAs(expr);
  }

  @Test
  public void testSingleRewriter() {
    // Single rewriter should work like standalone
    final ExpressionRewriter rewriter = new CompositeRewriter(new ConstantFolder());

    final ArithmeticExpression expr = new ArithmeticExpression(
        new LiteralExpression(1, "1"),
        ArithmeticExpression.Operator.ADD,
        new LiteralExpression(2, "2")
    );

    final Expression result = (Expression) rewriter.rewrite(expr);

    assertThat(result).isInstanceOf(LiteralExpression.class);
    assertThat(((LiteralExpression) result).getValue()).isEqualTo(3L);
  }

  @Test
  public void testOrderMatters() {
    // Test that order of rewrites matters
    // ComparisonNormalizer first, then ConstantFolder
    final PropertyAccessExpression property = new PropertyAccessExpression("n", "age");

    final ArithmeticExpression arithmetic = new ArithmeticExpression(
        new LiteralExpression(1, "1"),
        ArithmeticExpression.Operator.ADD,
        new LiteralExpression(2, "2")
    );

    final ComparisonExpression comparison = new ComparisonExpression(
        arithmetic,
        ComparisonExpression.Operator.LESS_THAN,
        property
    );

    // First normalize, then fold
    final ExpressionRewriter rewriter = new CompositeRewriter(
        new ComparisonNormalizer(),  // property > (1 + 2)
        new ConstantFolder()         // property > 3
    );

    final BooleanExpression result = (BooleanExpression) rewriter.rewrite(comparison);

    assertThat(result).isInstanceOf(ComparisonExpression.class);
    final ComparisonExpression comp = (ComparisonExpression) result;

    // After normalization and folding:
    // Left should be property, Right should be 3
    assertThat(comp.getLeft()).isInstanceOf(PropertyAccessExpression.class);
    assertThat(comp.getOperator()).isEqualTo(ComparisonExpression.Operator.GREATER_THAN);
    assertThat(comp.getRight()).isInstanceOf(LiteralExpression.class);
    assertThat(((LiteralExpression) comp.getRight()).getValue()).isEqualTo(3L);
  }
}
