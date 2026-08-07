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
 * Investigation found two independent problems, not one:
 * <ul>
 *   <li><b>Nesting.</b> The ANTLR-generated parser itself re-enters its {@code expression} rule (and walks
 *   the full ~10-level precedence cascade) once per nesting level for parentheses, list/map literals and
 *   function arguments - the shape the issue reports and reproduces with 1001 nested parentheses.</li>
 *   <li><b>Chain length.</b> The grammar's {@code (OR expression11)*}, {@code (PLUS expression5)*} and
 *   similar loops do <em>not</em> recurse in the parser (they are ANTLR quantifiers, not self-referencing
 *   rules), so a long flat chain of OR/AND/NOT/string-concatenation terms parses fine on its own - the
 *   issue's own diagnosis stopped there and assumed this shape was safe. It is not: every AST builder folds
 *   such a chain into a left-associative binary tree exactly as deep as the term count, and that tree is
 *   then walked recursively by several independent passes - {@code ExpressionRewriter} (WHERE-clause
 *   normalization - fixed first, see below), {@code CypherExpressionWalker} and {@code
 *   CypherSemanticValidator#checkExpressionScope} (both reachable from <em>any</em> clause, not just WHERE)
 *   were all confirmed to overflow independently, each on its own call stack shape.</li>
 * </ul>
 * {@link com.arcadedb.query.opencypher.parser.CypherExpressionDepthGuard}, the same {@code
 * ParseTreeListener} that bounds nesting, also bounds chain length directly on the grammar's own
 * {@code (OP operand)*} term counts (using each rule's generated accessor, exactly as every AST builder
 * would) - this rejects an oversized chain during parsing itself, before any tree is built, which protects
 * every downstream walker at once rather than requiring each one to be found and patched individually.
 * {@code ExpressionRewriter.rewrite()} carries its own, narrower {@code ThreadLocal} depth guard as
 * defense-in-depth for the WHERE-clause tree shape specifically, in case something ever builds one without
 * going through the guarded grammar rules.
 * <p>
 * Both mechanisms are bounded by {@link GlobalConfiguration#CYPHER_MAX_EXPRESSION_DEPTH}, converting the
 * crash into a {@link CommandParsingException} (an ordinary client/parse error, HTTP 400) with a message
 * that names the limit and the config key to raise it.
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
        .hasMessageContaining("chained")
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
   * the parser's nesting guard on its own. It does get built into a chain of nested AST nodes exactly as
   * deep as the NOT count, which the parser's chain-length guard now catches directly (by counting {@code
   * NOT} tokens on the {@code expression9} context) - a strictly wider protection than the issue's own
   * diagnosis asked for.
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
