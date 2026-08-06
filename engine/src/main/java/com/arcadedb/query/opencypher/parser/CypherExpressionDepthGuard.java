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
 * Bounds two independent sources of unbounded depth while parsing a single Cypher query, so that a
 * pathologically nested or long input is rejected as a normal {@link CommandParsingException} instead of
 * crashing the calling thread (or a later pass) with a {@link StackOverflowError}. See issue #5851 and its
 * follow-up.
 * <ol>
 *   <li><b>Nesting.</b> How deeply the {@code expression} grammar rule re-enters itself. Every nested
 *   sub-expression, regardless of the syntax that introduces it, re-enters {@code
 *   Cypher25Parser.RULE_expression} exactly once: {@code parenthesizedExpression: LPAREN expression
 *   RPAREN}, function call arguments, list/map literal values, {@code CASE} branches and comprehensions
 *   all funnel back through the same rule. The generated parser walks its full operator-precedence cascade
 *   (roughly ten Java stack frames) for every such re-entry, so counting re-entries is a precise,
 *   allocation-free proxy for the additional native stack consumed by one more nesting level - this is
 *   what the original issue reports and reproduces with 1001 nested parentheses.</li>
 *   <li><b>Chain length.</b> How many terms a flat {@code (OP operand)*} repetition matches - {@code
 *   expression: expression11 (OR expression11)*} and the seven other precedence levels shaped the same
 *   way (XOR, AND, {@code NOT*}, chained comparisons, {@code +}/{@code -}/{@code ||}, {@code *}/{@code /}/
 *   {@code %}, {@code ^}). ANTLR compiles a quantifier as a loop, not recursion, so the parser itself
 *   handles tens of thousands of chained terms without trouble - the issue's own diagnosis stopped there
 *   and assumed this shape was safe. It is not: every AST builder in this package folds such a list into a
 *   left-associative <em>binary tree</em> exactly as deep as the term count, and that tree is then walked
 *   recursively by several independent, unrelated passes - {@code ExpressionRewriter} (WHERE-clause
 *   normalization), {@code CypherExpressionWalker} (generic visitor, semantic validation and more) and
 *   {@code CypherSemanticValidator#checkExpressionScope} were all confirmed to overflow on a long enough
 *   chain, each on its own call stack shape. Rejecting an oversized chain here, at the one place its length
 *   is known before any tree gets built, protects all of them at once - including any future or
 *   undiscovered walker - without touching a single one of them.
 * </ol>
 * This is attached to the parser with {@link org.antlr.v4.runtime.Parser#addParseListener}, which ANTLR calls
 * synchronously from {@code enterRule}/{@code exitRule} before descending into any nested rule and after a
 * rule's children (including every match of a repeated sub-rule) are attached to its context. Throwing from
 * {@link #enterEveryRule} or {@link #exitEveryRule} therefore aborts the parse before any further work is
 * done, unwinding cleanly through the generated rule methods' {@code finally} blocks; {@link
 * Cypher25AntlrParser#parseQuery} then lets the {@link CommandParsingException} surface as-is.
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
      throw tooDeep("nested (parentheses, list/map literals and function arguments nested inside one another)");
  }

  @Override
  public void exitEveryRule(final ParserRuleContext ctx) {
    if (ctx.getRuleIndex() == Cypher25Parser.RULE_expression)
      --depth;
    checkChainLength(ctx); // no-op for any rule index not covered below
  }

  /**
   * Checks the term count of every {@code (OP operand)*}-shaped rule, using each rule's own generated
   * accessor - the same one every AST builder in this package calls - so the count is exact and never
   * drifts from what would actually be folded into a tree. Dispatches on {@link
   * ParserRuleContext#getRuleIndex()} rather than the context's runtime type: this fires on every rule
   * exit in the entire query, so an int switch (one comparison against a dense set of constants) is cheaper
   * on that hot path than a chain of {@code instanceof} checks that misses for every irrelevant rule.
   */
  private void checkChainLength(final ParserRuleContext ctx) {
    final int termCount = switch (ctx.getRuleIndex()) {
      case Cypher25Parser.RULE_expression -> ((Cypher25Parser.ExpressionContext) ctx).expression11().size();    // OR
      case Cypher25Parser.RULE_expression11 -> ((Cypher25Parser.Expression11Context) ctx).expression10().size(); // XOR
      case Cypher25Parser.RULE_expression10 -> ((Cypher25Parser.Expression10Context) ctx).expression9().size();  // AND
      case Cypher25Parser.RULE_expression9 -> ((Cypher25Parser.Expression9Context) ctx).NOT().size() + 1;         // NOT*
      case Cypher25Parser.RULE_expression8 -> ((Cypher25Parser.Expression8Context) ctx).expression7().size();    // chained comparisons
      case Cypher25Parser.RULE_expression6 -> ((Cypher25Parser.Expression6Context) ctx).expression5().size();    // + - ||
      case Cypher25Parser.RULE_expression5 -> ((Cypher25Parser.Expression5Context) ctx).expression4().size();    // * / %
      case Cypher25Parser.RULE_expression4 -> ((Cypher25Parser.Expression4Context) ctx).expression3().size();    // ^
      default -> 1;
    };
    if (termCount - 1 > maxDepth)
      throw tooDeep("chained (a run of AND/OR/XOR/NOT/comparison/arithmetic terms)");
  }

  private CommandParsingException tooDeep(final String shape) {
    return new CommandParsingException(
        "Expression is too deeply " + shape + " - exceeds the maximum allowed depth of " + maxDepth + ". "
            + "This protects the server against a stack overflow from a pathologically nested or long query; "
            + "raise 'arcadedb.cypher.maxExpressionDepth' if this is a legitimate query.");
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
