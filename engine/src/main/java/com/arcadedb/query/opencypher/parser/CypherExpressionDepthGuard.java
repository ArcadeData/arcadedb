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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * Bounds how deeply the {@code expression} grammar rule may re-enter itself while parsing a single
 * Cypher query, so that a pathologically nested or long input (thousands of nested parentheses, list/map
 * literals or function arguments) is rejected as a normal {@link CommandParsingException} instead of
 * crashing the calling thread with a {@link StackOverflowError}.
 * <p>
 * Every nested sub-expression, regardless of the syntax that introduces it, re-enters
 * {@code Cypher25Parser.RULE_expression} exactly once: {@code parenthesizedExpression: LPAREN expression
 * RPAREN}, function call arguments, list/map literal values, {@code CASE} branches and comprehensions all
 * funnel back through the same rule. The generated parser walks its full operator-precedence cascade
 * (roughly ten Java stack frames) for every such re-entry, so counting re-entries is a precise, allocation-free
 * proxy for the additional native stack consumed by one more nesting level - see issue #5851.
 * <p>
 * This is attached to the parser with {@link org.antlr.v4.runtime.Parser#addParseListener}, which ANTLR calls
 * synchronously from {@code enterRule}/{@code exitRule} before descending into any nested rule. Throwing from
 * {@link #enterEveryRule} therefore aborts the recursion before it goes one level deeper, unwinding cleanly
 * through the generated rule methods' {@code finally} blocks; {@link Cypher25AntlrParser#parseQuery} then
 * lets the {@link CommandParsingException} surface as-is.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CypherExpressionDepthGuard implements ParseTreeListener {

  private final int maxDepth;
  private int        depth;

  CypherExpressionDepthGuard(final int maxDepth) {
    this.maxDepth = maxDepth;
  }

  @Override
  public void enterEveryRule(final ParserRuleContext ctx) {
    if (ctx.getRuleIndex() == Cypher25Parser.RULE_expression && ++depth > maxDepth)
      throw new CommandParsingException(
          "Expression nesting exceeds the maximum allowed depth of " + maxDepth
              + " (parentheses, list/map literals and function arguments nested inside one another). "
              + "This protects the server against a stack overflow from pathologically nested queries; "
              + "raise 'arcadedb.cypher.maxExpressionDepth' if this is a legitimate query.");
  }

  @Override
  public void exitEveryRule(final ParserRuleContext ctx) {
    if (ctx.getRuleIndex() == Cypher25Parser.RULE_expression)
      --depth;
  }

  @Override
  public void visitTerminal(final TerminalNode node) {
    // no-op
  }

  @Override
  public void visitErrorNode(final ErrorNode node) {
    // no-op
  }
}
