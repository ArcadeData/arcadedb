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
 * Tests for ComparisonNormalizer rewrite rule.
 * Verifies that comparisons are normalized to canonical form with
 * properties/variables on the left and constants on the right.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ComparisonNormalizerTest {

  private final ComparisonNormalizer rewriter = new ComparisonNormalizer();

  @Test
  public void testSwapLiteralWithProperty() {
    // 5 < n.age → n.age > 5
    final PropertyAccessExpression property = new PropertyAccessExpression("n", "age");
    final LiteralExpression literal = new LiteralExpression(5, "5");

    final ComparisonExpression expr = new ComparisonExpression(
        literal,
        ComparisonExpression.Operator.LESS_THAN,
        property
    );

    final BooleanExpression result = (BooleanExpression) rewriter.rewrite(expr);

    assertThat(result).isInstanceOf(ComparisonExpression.class);
    final ComparisonExpression comp = (ComparisonExpression) result;
    assertThat(comp.getLeft()).isInstanceOf(PropertyAccessExpression.class);
    assertThat(comp.getOperator()).isEqualTo(ComparisonExpression.Operator.GREATER_THAN);
    assertThat(comp.getRight()).isInstanceOf(LiteralExpression.class);
  }

  @Test
  public void testSwapLiteralWithVariable() {
    // 10 > x → x < 10
    final VariableExpression variable = new VariableExpression("x");
    final LiteralExpression literal = new LiteralExpression(10, "10");

    final ComparisonExpression expr = new ComparisonExpression(
        literal,
        ComparisonExpression.Operator.GREATER_THAN,
        variable
    );

    final BooleanExpression result = (BooleanExpression) rewriter.rewrite(expr);

    assertThat(result).isInstanceOf(ComparisonExpression.class);
    final ComparisonExpression comp = (ComparisonExpression) result;
    assertThat(comp.getLeft()).isInstanceOf(VariableExpression.class);
    assertThat(comp.getOperator()).isEqualTo(ComparisonExpression.Operator.LESS_THAN);
    assertThat(comp.getRight()).isInstanceOf(LiteralExpression.class);
  }

  @Test
  public void testNoSwapWhenAlreadyNormalized() {
    // n.age > 5 → n.age > 5 (no change)
    final PropertyAccessExpression property = new PropertyAccessExpression("n", "age");
    final LiteralExpression literal = new LiteralExpression(5, "5");

    final ComparisonExpression expr = new ComparisonExpression(
        property,
        ComparisonExpression.Operator.GREATER_THAN,
        literal
    );

    final BooleanExpression result = (BooleanExpression) rewriter.rewrite(expr);

    assertThat(result).isSameAs(expr); // Should return original
  }

  @Test
  public void testOperatorFlipping() {
    // Test all operator flips
    final VariableExpression variable = new VariableExpression("x");
    final LiteralExpression literal = new LiteralExpression(5, "5");

    // < → >
    assertThat(flipAndGetOperator(literal, ComparisonExpression.Operator.LESS_THAN, variable))
        .isEqualTo(ComparisonExpression.Operator.GREATER_THAN);

    // > → <
    assertThat(flipAndGetOperator(literal, ComparisonExpression.Operator.GREATER_THAN, variable))
        .isEqualTo(ComparisonExpression.Operator.LESS_THAN);

    // <= → >=
    assertThat(flipAndGetOperator(literal, ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, variable))
        .isEqualTo(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL);

    // >= → <=
    assertThat(flipAndGetOperator(literal, ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, variable))
        .isEqualTo(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL);

    // = → = (symmetric)
    assertThat(flipAndGetOperator(literal, ComparisonExpression.Operator.EQUALS, variable))
        .isEqualTo(ComparisonExpression.Operator.EQUALS);

    // != → != (symmetric)
    assertThat(flipAndGetOperator(literal, ComparisonExpression.Operator.NOT_EQUALS, variable))
        .isEqualTo(ComparisonExpression.Operator.NOT_EQUALS);
  }

  @Test
  public void testInterestingnessPriority() {
    // Property > Variable > Literal
    final PropertyAccessExpression property = new PropertyAccessExpression("n", "age");
    final VariableExpression variable = new VariableExpression("x");
    final LiteralExpression literal = new LiteralExpression(5, "5");

    // literal = property → property = literal
    ComparisonExpression expr = new ComparisonExpression(
        literal, ComparisonExpression.Operator.EQUALS, property);
    BooleanExpression result = (BooleanExpression) rewriter.rewrite(expr);
    assertThat(((ComparisonExpression) result).getLeft()).isInstanceOf(PropertyAccessExpression.class);

    // literal = variable → variable = literal
    expr = new ComparisonExpression(
        literal, ComparisonExpression.Operator.EQUALS, variable);
    result = (BooleanExpression) rewriter.rewrite(expr);
    assertThat(((ComparisonExpression) result).getLeft()).isInstanceOf(VariableExpression.class);

    // variable = property → property = variable
    expr = new ComparisonExpression(
        variable, ComparisonExpression.Operator.EQUALS, property);
    result = (BooleanExpression) rewriter.rewrite(expr);
    assertThat(((ComparisonExpression) result).getLeft()).isInstanceOf(PropertyAccessExpression.class);
  }

  private ComparisonExpression.Operator flipAndGetOperator(final Expression left,
                                                            final ComparisonExpression.Operator op,
                                                            final Expression right) {
    final ComparisonExpression expr = new ComparisonExpression(left, op, right);
    final BooleanExpression result = (BooleanExpression) rewriter.rewrite(expr);
    return ((ComparisonExpression) result).getOperator();
  }
}
