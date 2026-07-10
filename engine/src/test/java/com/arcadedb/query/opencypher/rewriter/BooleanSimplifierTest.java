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
 * Tests for BooleanSimplifier rewrite rule.
 * Verifies boolean algebra identities applied at parse time:
 * - x AND true  → x
 * - x AND false → false
 * - x OR true   → true
 * - x OR false  → x
 * - true AND x  → x (commutative forms)
 * - false AND x → false
 * - true OR x   → true
 * - false OR x  → x
 * - NOT (NOT x) → x
 *
 * Also verifies that the compound pattern from issue #4405
 * (base AND true OR false AND expr) reduces to the base predicate.
 */
class BooleanSimplifierTest {

  private final BooleanSimplifier simplifier = new BooleanSimplifier();

  // --- helpers to build boolean literal coercions ---

  private BooleanExpression trueLiteral() {
    return new BooleanCoercionExpression(new LiteralExpression(Boolean.TRUE, "true"));
  }

  private BooleanExpression falseLiteral() {
    return new BooleanCoercionExpression(new LiteralExpression(Boolean.FALSE, "false"));
  }

  private BooleanExpression someExpr() {
    // Represents a non-constant predicate, e.g. n.k5 = -1862396709
    return new ComparisonExpression(
        new PropertyAccessExpression("n0", "k5"),
        ComparisonExpression.Operator.EQUALS,
        new LiteralExpression(-1862396709, "-1862396709")
    );
  }

  // --- AND identity: x AND true → x ---

  @Test
  void andTrueRightReducesToLeft() {
    final BooleanExpression expr = new LogicalExpression(
        LogicalExpression.Operator.AND, someExpr(), trueLiteral());

    final BooleanExpression result = (BooleanExpression) simplifier.rewrite(expr);

    assertThat(result).isInstanceOf(ComparisonExpression.class);
  }

  @Test
  void andTrueLeftReducesToRight() {
    final BooleanExpression expr = new LogicalExpression(
        LogicalExpression.Operator.AND, trueLiteral(), someExpr());

    final BooleanExpression result = (BooleanExpression) simplifier.rewrite(expr);

    assertThat(result).isInstanceOf(ComparisonExpression.class);
  }

  // --- AND annihilator: x AND false → false ---

  @Test
  void andFalseRightReducesToFalse() {
    final BooleanExpression expr = new LogicalExpression(
        LogicalExpression.Operator.AND, someExpr(), falseLiteral());

    final BooleanExpression result = (BooleanExpression) simplifier.rewrite(expr);

    assertThat(result).isInstanceOf(BooleanCoercionExpression.class);
    final BooleanCoercionExpression coerced = (BooleanCoercionExpression) result;
    assertThat(coerced.getExpression()).isInstanceOf(LiteralExpression.class);
    assertThat(((LiteralExpression) coerced.getExpression()).getValue()).isEqualTo(Boolean.FALSE);
  }

  @Test
  void andFalseLeftReducesToFalse() {
    final BooleanExpression expr = new LogicalExpression(
        LogicalExpression.Operator.AND, falseLiteral(), someExpr());

    final BooleanExpression result = (BooleanExpression) simplifier.rewrite(expr);

    assertThat(result).isInstanceOf(BooleanCoercionExpression.class);
    final BooleanCoercionExpression coerced = (BooleanCoercionExpression) result;
    assertThat(((LiteralExpression) coerced.getExpression()).getValue()).isEqualTo(Boolean.FALSE);
  }

  // --- OR annihilator: x OR true → true ---

  @Test
  void orTrueRightReducesToTrue() {
    final BooleanExpression expr = new LogicalExpression(
        LogicalExpression.Operator.OR, someExpr(), trueLiteral());

    final BooleanExpression result = (BooleanExpression) simplifier.rewrite(expr);

    assertThat(result).isInstanceOf(BooleanCoercionExpression.class);
    assertThat(((LiteralExpression) ((BooleanCoercionExpression) result).getExpression()).getValue()).isEqualTo(Boolean.TRUE);
  }

  @Test
  void orTrueLeftReducesToTrue() {
    final BooleanExpression expr = new LogicalExpression(
        LogicalExpression.Operator.OR, trueLiteral(), someExpr());

    final BooleanExpression result = (BooleanExpression) simplifier.rewrite(expr);

    assertThat(result).isInstanceOf(BooleanCoercionExpression.class);
    assertThat(((LiteralExpression) ((BooleanCoercionExpression) result).getExpression()).getValue()).isEqualTo(Boolean.TRUE);
  }

  // --- OR identity: x OR false → x ---

  @Test
  void orFalseRightReducesToLeft() {
    final BooleanExpression expr = new LogicalExpression(
        LogicalExpression.Operator.OR, someExpr(), falseLiteral());

    final BooleanExpression result = (BooleanExpression) simplifier.rewrite(expr);

    assertThat(result).isInstanceOf(ComparisonExpression.class);
  }

  @Test
  void orFalseLeftReducesToRight() {
    final BooleanExpression expr = new LogicalExpression(
        LogicalExpression.Operator.OR, falseLiteral(), someExpr());

    final BooleanExpression result = (BooleanExpression) simplifier.rewrite(expr);

    assertThat(result).isInstanceOf(ComparisonExpression.class);
  }

  // --- NOT (NOT x) → x ---

  @Test
  void doubleNegationElimination() {
    final BooleanExpression inner = someExpr();
    final BooleanExpression notInner = new LogicalExpression(LogicalExpression.Operator.NOT, inner);
    final BooleanExpression notNotInner = new LogicalExpression(LogicalExpression.Operator.NOT, notInner);

    final BooleanExpression result = (BooleanExpression) simplifier.rewrite(notNotInner);

    assertThat(result).isInstanceOf(ComparisonExpression.class);
  }

  // --- Compound pattern from issue #4405: base AND true OR false AND expr → base ---

  @Test
  void compoundPatternIssue4405() {
    // Construct: (base AND true) OR (false AND irrelevantExpr)
    // Expected: base (a ComparisonExpression)
    final BooleanExpression base = someExpr();

    // irrelevant expression: n0.k5 IS NULL OR n0.k5 IS NOT NULL
    final BooleanExpression irrelevantExpr = new LogicalExpression(
        LogicalExpression.Operator.OR,
        new IsNullExpression(new PropertyAccessExpression("n0", "k5"), false),
        new IsNullExpression(new PropertyAccessExpression("n0", "k5"), true)
    );

    final BooleanExpression andTrue = new LogicalExpression(
        LogicalExpression.Operator.AND, base, trueLiteral());

    final BooleanExpression falseAndIrrelevant = new LogicalExpression(
        LogicalExpression.Operator.AND, falseLiteral(), irrelevantExpr);

    final BooleanExpression compound = new LogicalExpression(
        LogicalExpression.Operator.OR, andTrue, falseAndIrrelevant);

    final BooleanExpression result = (BooleanExpression) simplifier.rewrite(compound);

    // Should simplify: (base AND true) → base, (false AND x) → false, base OR false → base
    assertThat(result).isInstanceOf(ComparisonExpression.class);
  }

  // --- No simplification when no constants ---

  @Test
  void andWithTwoNonConstantsIsUnchanged() {
    final BooleanExpression left = someExpr();
    final BooleanExpression right = new ComparisonExpression(
        new PropertyAccessExpression("n0", "k10"),
        ComparisonExpression.Operator.EQUALS,
        new LiteralExpression(42, "42")
    );
    final BooleanExpression expr = new LogicalExpression(
        LogicalExpression.Operator.AND, left, right);

    final BooleanExpression result = (BooleanExpression) simplifier.rewrite(expr);

    assertThat(result).isInstanceOf(LogicalExpression.class);
    assertThat(((LogicalExpression) result).getOperator()).isEqualTo(LogicalExpression.Operator.AND);
  }
}
