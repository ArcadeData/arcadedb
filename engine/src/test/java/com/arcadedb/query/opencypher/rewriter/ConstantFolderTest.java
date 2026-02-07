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

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ConstantFolder rewrite rule.
 * Verifies that constant expressions are evaluated at parse time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ConstantFolderTest {

  private final ConstantFolder rewriter = new ConstantFolder();

  @Test
  public void testArithmeticAddIntegers() {
    // 1 + 2 → 3
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
  public void testArithmeticSubtract() {
    // 10 - 3 → 7
    final ArithmeticExpression expr = new ArithmeticExpression(
        new LiteralExpression(10, "10"),
        ArithmeticExpression.Operator.SUBTRACT,
        new LiteralExpression(3, "3")
    );

    final Expression result = (Expression) rewriter.rewrite(expr);

    assertThat(result).isInstanceOf(LiteralExpression.class);
    assertThat(((LiteralExpression) result).getValue()).isEqualTo(7L);
  }

  @Test
  public void testArithmeticMultiply() {
    // 5 * 3 → 15
    final ArithmeticExpression expr = new ArithmeticExpression(
        new LiteralExpression(5, "5"),
        ArithmeticExpression.Operator.MULTIPLY,
        new LiteralExpression(3, "3")
    );

    final Expression result = (Expression) rewriter.rewrite(expr);

    assertThat(result).isInstanceOf(LiteralExpression.class);
    assertThat(((LiteralExpression) result).getValue()).isEqualTo(15L);
  }

  @Test
  public void testArithmeticDivide() {
    // 10 / 2 → 5
    final ArithmeticExpression expr = new ArithmeticExpression(
        new LiteralExpression(10, "10"),
        ArithmeticExpression.Operator.DIVIDE,
        new LiteralExpression(2, "2")
    );

    final Expression result = (Expression) rewriter.rewrite(expr);

    assertThat(result).isInstanceOf(LiteralExpression.class);
    assertThat(((LiteralExpression) result).getValue()).isEqualTo(5L);
  }

  @Test
  public void testArithmeticPower() {
    // 2 ^ 3 → 8.0
    final ArithmeticExpression expr = new ArithmeticExpression(
        new LiteralExpression(2, "2"),
        ArithmeticExpression.Operator.POWER,
        new LiteralExpression(3, "3")
    );

    final Expression result = (Expression) rewriter.rewrite(expr);

    assertThat(result).isInstanceOf(LiteralExpression.class);
    assertThat(((LiteralExpression) result).getValue()).isEqualTo(8.0);
  }

  @Test
  public void testStringConcatenation() {
    // 'hello' + ' world' → 'hello world'
    final ArithmeticExpression expr = new ArithmeticExpression(
        new LiteralExpression("hello", "'hello'"),
        ArithmeticExpression.Operator.ADD,
        new LiteralExpression(" world", "' world'")
    );

    final Expression result = (Expression) rewriter.rewrite(expr);

    assertThat(result).isInstanceOf(LiteralExpression.class);
    assertThat(((LiteralExpression) result).getValue()).isEqualTo("hello world");
  }

  @Test
  public void testListConcatenation() {
    // [1, 2] + [3, 4] → [1, 2, 3, 4]
    final ArithmeticExpression expr = new ArithmeticExpression(
        new LiteralExpression(Arrays.asList(1, 2), "[1, 2]"),
        ArithmeticExpression.Operator.ADD,
        new LiteralExpression(Arrays.asList(3, 4), "[3, 4]")
    );

    final Expression result = (Expression) rewriter.rewrite(expr);

    assertThat(result).isInstanceOf(LiteralExpression.class);
    assertThat(((LiteralExpression) result).getValue()).isEqualTo(Arrays.asList(1, 2, 3, 4));
  }

  @Test
  public void testComparisonGreaterThan() {
    // 5 > 3 → true
    final ComparisonExpression expr = new ComparisonExpression(
        new LiteralExpression(5, "5"),
        ComparisonExpression.Operator.GREATER_THAN,
        new LiteralExpression(3, "3")
    );

    final BooleanExpression result = (BooleanExpression) rewriter.rewrite(expr);

    // Note: Can't fold to literal boolean because LiteralExpression doesn't implement BooleanExpression
    // So comparison should remain unchanged for now
    assertThat(result).isInstanceOf(ComparisonExpression.class);
  }

  @Test
  public void testComparisonEquals() {
    // 'a' = 'a' → true
    final ComparisonExpression expr = new ComparisonExpression(
        new LiteralExpression("a", "'a'"),
        ComparisonExpression.Operator.EQUALS,
        new LiteralExpression("a", "'a'")
    );

    final BooleanExpression result = (BooleanExpression) rewriter.rewrite(expr);

    // Note: Can't fold to literal boolean, remains as comparison
    assertThat(result).isInstanceOf(ComparisonExpression.class);
  }

  @Test
  public void testNullPropagation() {
    // null + 5 → null
    final ArithmeticExpression expr = new ArithmeticExpression(
        new LiteralExpression(null, "null"),
        ArithmeticExpression.Operator.ADD,
        new LiteralExpression(5, "5")
    );

    final Expression result = (Expression) rewriter.rewrite(expr);

    assertThat(result).isInstanceOf(LiteralExpression.class);
    assertThat(((LiteralExpression) result).getValue()).isNull();
  }

  @Test
  public void testNestedArithmetic() {
    // (1 + 2) * 3 → 3 * 3 → 9
    final ArithmeticExpression inner = new ArithmeticExpression(
        new LiteralExpression(1, "1"),
        ArithmeticExpression.Operator.ADD,
        new LiteralExpression(2, "2")
    );

    final ArithmeticExpression outer = new ArithmeticExpression(
        inner,
        ArithmeticExpression.Operator.MULTIPLY,
        new LiteralExpression(3, "3")
    );

    final Expression result = (Expression) rewriter.rewrite(outer);

    assertThat(result).isInstanceOf(LiteralExpression.class);
    assertThat(((LiteralExpression) result).getValue()).isEqualTo(9L);
  }

  @Test
  public void testDoNotFoldWithVariable() {
    // x + 5 → x + 5 (no folding, has variable)
    final ArithmeticExpression expr = new ArithmeticExpression(
        new VariableExpression("x"),
        ArithmeticExpression.Operator.ADD,
        new LiteralExpression(5, "5")
    );

    final Expression result = (Expression) rewriter.rewrite(expr);

    assertThat(result).isSameAs(expr); // Should return original
  }
}
