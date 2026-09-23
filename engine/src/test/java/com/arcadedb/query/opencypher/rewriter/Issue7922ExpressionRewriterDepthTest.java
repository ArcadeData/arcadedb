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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The second of the two readers issue #7922 covers on the Cypher side, tested on its own.
 * <p>
 * {@code ExpressionRewriter} used to read {@code CYPHER_MAX_EXPRESSION_DEPTH} off the enum although the setting is
 * declared {@code SCOPE.DATABASE}. It runs on {@code CypherASTBuilder.AST_REWRITER}, a single instance shared and
 * visited concurrently by every query on the JVM, so it cannot hold any one query's database - the parse binds the
 * limit on the calling thread instead, and this pins that plumbing: the bound value wins, it is restored rather
 * than cleared so a nested parse does not drop the outer one's, and with nothing bound the JVM default stands.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7922ExpressionRewriterDepthTest {

  /** A rewriter with no rules of its own: what is under test is the depth guard in {@code rewrite()}. */
  private final ExpressionRewriter rewriter = new ExpressionRewriter() {
  };

  @AfterEach
  void clearBinding() {
    ExpressionRewriter.restoreMaxExpressionDepth(null);
  }

  @Test
  void theBoundLimitIsWhatTheGuardEnforces() {
    ExpressionRewriter.bindMaxExpressionDepth(3);

    assertThatThrownBy(() -> rewriter.rewrite(chainedOr(10)))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("exceeds 3 levels");
  }

  @Test
  void anExpressionInsideTheBoundLimitIsRewritten() {
    ExpressionRewriter.bindMaxExpressionDepth(200);

    assertThat(rewriter.rewrite(chainedOr(10))).isNotNull();
  }

  @Test
  void withNothingBoundTheJvmDefaultStands() {
    final int jvmDefault = GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH.getValueAsInteger();

    assertThatThrownBy(() -> rewriter.rewrite(chainedOr(jvmDefault + 10)))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("exceeds " + jvmDefault + " levels");
  }

  @Test
  void aNestedBindRestoresTheOuterLimitRatherThanClearingIt() {
    final Integer outerPrevious = ExpressionRewriter.bindMaxExpressionDepth(200);
    assertThat(outerPrevious).isNull();

    final Integer innerPrevious = ExpressionRewriter.bindMaxExpressionDepth(3);
    assertThat(innerPrevious).isEqualTo(200);
    ExpressionRewriter.restoreMaxExpressionDepth(innerPrevious);

    // back to the outer parse's limit, not to the JVM default and not to the inner one
    assertThat(rewriter.rewrite(chainedOr(10))).isNotNull();

    ExpressionRewriter.restoreMaxExpressionDepth(outerPrevious);
  }

  /** A left-deep {@code OR} chain: {@code depth} nested LogicalExpressions, so {@code rewrite()} recurses that far. */
  private static BooleanExpression chainedOr(final int depth) {
    BooleanExpression expression = comparison(0);
    for (int i = 1; i <= depth; i++)
      expression = new LogicalExpression(LogicalExpression.Operator.OR, expression, comparison(i));
    return expression;
  }

  private static ComparisonExpression comparison(final int value) {
    final Expression literal = new LiteralExpression(value, String.valueOf(value));
    return new ComparisonExpression(literal, ComparisonExpression.Operator.EQUALS, literal);
  }
}
