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
 * Regression tests for issue #5851: a Cypher query with a few thousand nested parentheses, or tens of
 * thousands of chained AND/OR/string-concatenation terms, crashed the parsing thread with a
 * {@link StackOverflowError} instead of failing with a normal parse error. The SQL parser handled the same
 * inputs without trouble because its hand-written recursive-descent implementation costs far fewer Java
 * stack frames per nesting level than the ANTLR4-generated Cypher parser's operator-precedence cascade.
 * <p>
 * Investigation found two independent recursion sites, not one:
 * <ul>
 *   <li>The ANTLR-generated parser itself, which re-enters its {@code expression} rule (and walks the full
 *   ~10-level precedence cascade) once per nesting nesting level for parentheses, list/map literals and
 *   function arguments - the shape the issue reports and reproduces with 1001 nested parentheses.</li>
 *   <li>{@code ExpressionRewriter.rewrite()}, ArcadeDB's own post-parse AST rewrite visitor used to
 *   normalize/fold/simplify every {@code WHERE} condition. The grammar's {@code (OR expression11)*} and
 *   {@code (PLUS expression5)*} loops do <em>not</em> recurse in the parser (they are ANTLR quantifiers,
 *   not self-referencing rules), so a long flat chain of OR/AND/string-concatenation terms parses fine and
 *   only overflows afterwards, while this shared, statically-cached rewriter instance recursively walks the
 *   resulting deep expression tree. This is a distinct bug the issue's own diagnosis did not isolate: its
 *   captured stack trace was for the parentheses case only. Its "NOT NOT NOT ... true" shape is a
 *   Kleene-star loop in the grammar (not recursion), so it never reproduces in the parser at the depth the
 *   issue reports - but a long enough NOT chain still builds a deeply nested AST and is still caught, by
 *   this same rewriter-side guard - see {@link #longNotChainIsRejectedAsAParseErrorNotAStackOverflow()}.
 * </ul>
 * Both sites are bounded by {@link GlobalConfiguration#CYPHER_MAX_EXPRESSION_DEPTH}, converting the crash
 * into a {@link CommandParsingException} (an ordinary client/parse error, HTTP 400) with a message that
 * names the limit and the config key to raise it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherExpressionDepthGuardIssue5851Test extends TestHelper {

  @AfterEach
  void resetConfig() {
    GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH.setValue(GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH.getDefValue());
  }

  // ===================== the issue's own reproducer: nested parentheses =====================

  @Test
  void deeplyNestedParenthesesAreRejectedAsAParseErrorNotAStackOverflow() {
    final String cypher = "MATCH (n) WHERE " + "(".repeat(1001) + "1=1" + ")".repeat(1001) + " RETURN n";

    assertThatThrownBy(() -> new Cypher25AntlrParser().parse(cypher))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("nest")
        .hasMessageContaining("arcadedb.cypher.maxExpressionDepth");
  }

  /**
   * The issue's own reproducer, verbatim: {@code db.getQueryEngine("opencypher").analyze(cypher)} on a
   * ~2KB query with 1001 nested parentheses.
   */
  @Test
  void issueReproducerThroughTheQueryEngineAnalyzeApi() {
    database.getSchema().createVertexType("V");

    final int depth = 1001;
    final String cypher = "MATCH (n) WHERE " + "(".repeat(depth) + "1=1" + ")".repeat(depth);

    assertThatThrownBy(() -> database.getQueryEngine("opencypher").analyze(cypher))
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void moderatelyNestedParenthesesStillParseFine() {
    final String cypher = "MATCH (n) WHERE " + "(".repeat(50) + "1=1" + ")".repeat(50) + " RETURN n";

    assertThatCode(() -> new Cypher25AntlrParser().parse(cypher)).doesNotThrowAnyException();
  }

  // ===================== flat chains: a different bug than the parser recursion =====================

  @Test
  void longOrChainIsRejectedAsAParseErrorNotAStackOverflow() {
    final StringBuilder cypher = new StringBuilder("MATCH (n) WHERE a=1");
    for (int i = 0; i < 30_000; i++)
      cypher.append(" OR a=1");
    cypher.append(" RETURN n");

    assertThatThrownBy(() -> new Cypher25AntlrParser().parse(cypher.toString()))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("deeply nested or chained")
        .hasMessageContaining("arcadedb.cypher.maxExpressionDepth");
  }

  @Test
  void longStringConcatenationChainIsRejectedAsAParseErrorNotAStackOverflow() {
    final StringBuilder cypher = new StringBuilder("MATCH (n) WHERE ('x'");
    for (int i = 0; i < 30_000; i++)
      cypher.append("+'x'");
    cypher.append(") = 'x' RETURN n");

    assertThatThrownBy(() -> new Cypher25AntlrParser().parse(cypher.toString()))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("arcadedb.cypher.maxExpressionDepth");
  }

  @Test
  void aHandfulOfOrTermsStillParsesFine() {
    final String cypher = "MATCH (n) WHERE n.x=1 OR n.x=2 OR n.x=3 RETURN n";

    assertThatCode(() -> new Cypher25AntlrParser().parse(cypher)).doesNotThrowAnyException();
  }

  /**
   * {@code NOT*} is a Kleene-star quantifier in the grammar ({@code expression9: NOT* expression8}), not a
   * self-referencing rule, so a chain of {@code NOT}s does not re-enter {@code expression} and never trips
   * the parser-side guard - confirmed here by an input depth (2000) at which the un-guarded parser is
   * already known not to overflow. It does, however, get built into a chain of nested {@code
   * LogicalExpression(NOT, ...)} AST nodes, which {@code ExpressionRewriter} then walks recursively, so a
   * long enough chain is still caught by the rewriter-side guard - a strictly wider protection than the
   * issue's own diagnosis asked for.
   */
  @Test
  void moderateNotChainStillParsesFine() {
    final String cypher = "MATCH (n) WHERE " + "NOT ".repeat(50) + "true RETURN n";

    assertThatCode(() -> new Cypher25AntlrParser().parse(cypher)).doesNotThrowAnyException();
  }

  @Test
  void longNotChainIsRejectedAsAParseErrorNotAStackOverflow() {
    final String cypher = "MATCH (n) WHERE " + "NOT ".repeat(2000) + "true RETURN n";

    assertThatThrownBy(() -> new Cypher25AntlrParser().parse(cypher))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("arcadedb.cypher.maxExpressionDepth");
  }

  // ===================== the limit is a real, effective knob =====================

  @Test
  void raisingTheConfiguredLimitAllowsADeeperQueryThatWasPreviouslyRejected() {
    final String cypher = "MATCH (n) WHERE " + "(".repeat(300) + "1=1" + ")".repeat(300) + " RETURN n";

    // Rejected at the default (200)
    assertThatThrownBy(() -> new Cypher25AntlrParser().parse(cypher)).isInstanceOf(CommandParsingException.class);

    // Accepted once the operator raises the knob
    GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH.setValue(500);
    assertThatCode(() -> new Cypher25AntlrParser().parse(cypher)).doesNotThrowAnyException();
  }
}
