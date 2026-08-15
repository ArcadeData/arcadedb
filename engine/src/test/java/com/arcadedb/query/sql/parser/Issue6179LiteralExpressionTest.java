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
package com.arcadedb.query.sql.parser;

import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6179: {@code isEarlyCalculated()} answers "can this be computed without a record", which is not the same
 * question as "is this a constant". These pin both predicates, and above all the cases where they disagree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6179LiteralExpressionTest extends AbstractParserTest {

  private final CommandContext context = new BasicCommandContext();

  private Expression rightOperandOf(final String query) {
    final SelectStatement statement = (SelectStatement) checkRightSyntax(query);
    final BooleanExpression term = statement.whereClause.flatten().getFirst().subBlocks.getFirst();
    return ((BinaryCondition) term).right;
  }

  @Test
  void aLiteralIsBothEarlyCalculatedAndLiteral() {
    for (final String literal : new String[] { "1", "'x'", "true", "1 + 2 * 3", "(1 + 2)", "['a', 'b']", "[]", "#12:0" }) {
      final Expression expression = rightOperandOf("select from Foo where a = " + literal);
      assertThat(expression.isEarlyCalculated(context)).as(literal).isTrue();
      assertThat(expression.isLiteral()).as(literal).isTrue();
    }
  }

  /**
   * The one shape where the narrower predicate says yes and the wider one says no: {@code null} is a constant, but
   * {@code BaseExpression.isEarlyCalculated} has never admitted it, and it must not start to - an index lookup on
   * a null key is not the same question as the per-record comparison the filter would make.
   */
  @Test
  void theNullLiteralIsALiteralButNotEarlyCalculated() {
    final Expression expression = rightOperandOf("select from Foo where a = null");
    assertThat(expression.isLiteral()).isTrue();
    assertThat(expression.isEarlyCalculated(context)).isFalse();
  }

  @Test
  void aPropertyIsNeither() {
    final Expression expression = rightOperandOf("select from Foo where a = b");
    assertThat(expression.isEarlyCalculated(context)).isFalse();
    assertThat(expression.isLiteral()).isFalse();
  }

  /**
   * A function call is never a literal, whatever its arguments are. That it stays "early calculated" - the point
   * of the issue - is pinned where a database is at hand, in {@code Issue6179EarlyCalculatedModifierTest}.
   */
  @Test
  void aFunctionOverLiteralsIsNotALiteral() {
    for (final String call : new String[] { "uuid()", "sysdate()", "concat('a', 'b')" }) {
      final Expression expression = rightOperandOf("select from Foo where a = " + call);
      assertThat(expression.isLiteral()).as(call).isFalse();
      assertThat(expression.isLiteral(true)).as(call).isFalse();
    }
  }

  @Test
  void anInputParameterIsALiteralOnlyWhenTheCallerSaysSo() {
    final Expression expression = rightOperandOf("select from Foo where a = :p");
    assertThat(expression.isEarlyCalculated(context)).isTrue();
    assertThat(expression.isLiteral()).isFalse();
    assertThat(expression.isLiteral(true)).isTrue();
  }

  @Test
  void aCollectionIsLiteralOnlyWhenEveryItemIs() {
    assertThat(rightOperandOf("select from Foo where a = ['x', 1]").isLiteral()).isTrue();
    assertThat(rightOperandOf("select from Foo where a = ['x', b]").isLiteral()).isFalse();
    assertThat(rightOperandOf("select from Foo where a = ['x', :p]").isLiteral()).isFalse();
    assertThat(rightOperandOf("select from Foo where a = ['x', :p]").isLiteral(true)).isTrue();
  }

  @Test
  void aModifierOverALiteralIsEarlyCalculatedButNotLiteral() {
    for (final String modified : new String[] { "'x'.append('y')", "[1, 2][0]", "'abc'.substring(1)" }) {
      final Expression expression = rightOperandOf("select from Foo where a = " + modified);
      assertThat(expression.isEarlyCalculated(context)).as(modified).isTrue();
      assertThat(expression.isLiteral()).as(modified).isFalse();
    }
  }

  @Test
  void aModifierReachingTheRecordIsNotEarlyCalculated() {
    for (final String modified : new String[] { "'x'.append(b)", "[1, 2][b]", "[1, 2][b - 1]", "'x'.append('y').append(b)",
        "'x'.append(b).append('y')" }) {
      final Expression expression = rightOperandOf("select from Foo where a = " + modified);
      assertThat(expression.isEarlyCalculated(context)).as(modified).isFalse();
      assertThat(expression.isLiteral()).as(modified).isFalse();
    }
  }

  @Test
  void aCaseIsALiteralOnlyWhenEveryBranchIs() {
    assertThat(rightOperandOf("select from Foo where a = CASE 1 WHEN 1 THEN 'x' ELSE 'y' END").isLiteral()).isTrue();
    assertThat(rightOperandOf("select from Foo where a = CASE b WHEN 1 THEN 'x' ELSE 'y' END").isLiteral()).isFalse();
    assertThat(rightOperandOf("select from Foo where a = CASE 1 WHEN 1 THEN b ELSE 'y' END").isLiteral()).isFalse();
    assertThat(rightOperandOf("select from Foo where a = CASE 1 WHEN 1 THEN 'x' ELSE b END").isLiteral()).isFalse();
    // the simple form's WHEN is a boolean expression, which carries no constant-ness of its own
    assertThat(rightOperandOf("select from Foo where a = CASE WHEN 1 = 1 THEN 'x' ELSE 'y' END").isLiteral()).isFalse();
  }

  @Test
  void aCaseIsEarlyCalculatedOnlyWhenNoBranchReadsTheRecord() {
    assertThat(rightOperandOf("select from Foo where a = CASE 1 WHEN 1 THEN 'x' ELSE 'y' END").isEarlyCalculated(
        context)).isTrue();
    assertThat(rightOperandOf("select from Foo where a = CASE b WHEN 1 THEN 'x' ELSE 'y' END").isEarlyCalculated(
        context)).isFalse();
    assertThat(rightOperandOf("select from Foo where a = CASE 1 WHEN 1 THEN b ELSE 'y' END").isEarlyCalculated(
        context)).isFalse();
    assertThat(rightOperandOf("select from Foo where a = CASE WHEN 1 = 1 THEN 'x' ELSE 'y' END").isEarlyCalculated(
        context)).isFalse();
  }

  @Test
  void aTraversalMethodIsNotEarlyCalculated() {
    final Expression expression = rightOperandOf("select from Foo where a = 'x'.out('E')");
    assertThat(expression.isEarlyCalculated(context)).isFalse();
    assertThat(expression.isLiteral()).isFalse();
  }
}
