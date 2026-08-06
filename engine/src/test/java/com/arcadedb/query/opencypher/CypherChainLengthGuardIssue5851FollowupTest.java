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
package com.arcadedb.query.opencypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Follow-up to issue #5851 / PR #5859: {@code ExpressionRewriter}'s recursion guard only fires from
 * {@code CypherASTBuilder#visitWhereClause}, so a long OR/AND/NOT/comparison/arithmetic chain written
 * anywhere <em>other</em> than a top-level {@code WHERE} condition - a {@code RETURN} projection, an
 * {@code ORDER BY} item, a function argument, inside a list literal, ... - was never rewritten and so never
 * hit that guard either. Confirmed to still crash after PR #5859: a 30000-term OR chain in a {@code RETURN}
 * projection overflowed {@code CypherSemanticValidator#checkExpressionScope} (a completely different
 * recursive walker from {@code ExpressionRewriter}, invoked during semantic validation on every clause).
 * <p>
 * Rather than chase down and patch every such walker individually - which is exactly the kind of fragile,
 * incomplete-prone fix that let this gap through PR #5859 in the first place - {@code
 * CypherExpressionDepthGuard} (already attached to the parser for nesting depth) now also bounds the term
 * count of every {@code (OP operand)*}-shaped grammar rule directly, using each rule's own generated
 * accessor. This rejects an oversized chain during parsing, before any AST tree is built, regardless of
 * which clause it appears in and regardless of which pass would eventually have walked it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherChainLengthGuardIssue5851FollowupTest extends TestHelper {

  @AfterEach
  void resetConfig() {
    GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH.setValue(GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH.getDefValue());
  }

  // ===================== the confirmed gap: a chain outside WHERE =====================

  @Test
  void longOrChainInAReturnProjectionIsRejectedNotAStackOverflow() {
    final StringBuilder cypher = new StringBuilder("MATCH (n) RETURN n, (a=1");
    for (int i = 0; i < 30_000; i++)
      cypher.append(" OR a=1");
    cypher.append(") AS r");

    assertThatThrownBy(() -> new Cypher25AntlrParser().parse(cypher.toString()))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("chained")
        .hasMessageContaining("arcadedb.cypher.maxExpressionDepth");
  }

  @Test
  void longAndChainInAnOrderByItemIsRejectedNotAStackOverflow() {
    final StringBuilder cypher = new StringBuilder("MATCH (n) RETURN n ORDER BY (n.a=1");
    for (int i = 0; i < 30_000; i++)
      cypher.append(" AND n.a=1");
    cypher.append(")");

    assertThatThrownBy(() -> new Cypher25AntlrParser().parse(cypher.toString()))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("arcadedb.cypher.maxExpressionDepth");
  }

  @Test
  void aFewOrTermsInAReturnProjectionStillParseFine() {
    final String cypher = "MATCH (n) RETURN n, (n.a=1 OR n.a=2 OR n.a=3) AS r";

    assertThatCode(() -> new Cypher25AntlrParser().parse(cypher)).doesNotThrowAnyException();
  }

  // ===================== chained comparisons (a < b < c < ...) and arithmetic chains =====================

  @Test
  void longChainedComparisonIsRejectedNotAStackOverflow() {
    final StringBuilder cypher = new StringBuilder("MATCH (n) WHERE 0");
    for (int i = 0; i < 30_000; i++)
      cypher.append(" < ").append(i + 1);
    cypher.append(" RETURN n");

    assertThatThrownBy(() -> new Cypher25AntlrParser().parse(cypher.toString()))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("arcadedb.cypher.maxExpressionDepth");
  }

  /** A short chained comparison (issue #5284: {@code a < b < c} means {@code a < b AND b < c}) still parses. */
  @Test
  void aShortChainedComparisonStillParsesFine() {
    final String cypher = "MATCH (n) WHERE 1 < 2 < 3 RETURN n";

    assertThatCode(() -> new Cypher25AntlrParser().parse(cypher)).doesNotThrowAnyException();
  }

  @Test
  void longMultiplicationChainIsRejectedNotAStackOverflow() {
    final StringBuilder cypher = new StringBuilder("MATCH (n) WHERE (1");
    for (int i = 0; i < 30_000; i++)
      cypher.append("*2");
    cypher.append(") = 0 RETURN n");

    assertThatThrownBy(() -> new Cypher25AntlrParser().parse(cypher.toString()))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("arcadedb.cypher.maxExpressionDepth");
  }

  @Test
  void longPowerChainIsRejectedNotAStackOverflow() {
    final StringBuilder cypher = new StringBuilder("MATCH (n) WHERE (1");
    for (int i = 0; i < 30_000; i++)
      cypher.append("^2");
    cypher.append(") = 0 RETURN n");

    assertThatThrownBy(() -> new Cypher25AntlrParser().parse(cypher.toString()))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("arcadedb.cypher.maxExpressionDepth");
  }

  @Test
  void aFewArithmeticTermsStillParseFine() {
    final String cypher = "MATCH (n) WHERE (1*2*3*4) = 24 RETURN n";

    assertThatCode(() -> new Cypher25AntlrParser().parse(cypher)).doesNotThrowAnyException();
  }
}
