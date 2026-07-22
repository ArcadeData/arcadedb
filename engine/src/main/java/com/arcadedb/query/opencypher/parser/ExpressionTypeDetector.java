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

import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.StarExpression;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for detecting and parsing different expression types in Cypher.
 * Organizes the complex expression parsing logic into categorized methods.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ExpressionTypeDetector {

  private final CypherExpressionBuilder builder;

  ExpressionTypeDetector(final CypherExpressionBuilder builder) {
    this.builder = builder;
  }

  /**
   * Try to parse special function expressions (count(*), EXISTS, CASE, shortestPath).
   * Returns null if not a special function.
   */
  Expression tryParseSpecialFunctions(final Cypher25Parser.ExpressionContext ctx) {
    // All recursive searches below use a span guard: only match when the found context spans the
    // ENTIRE expression (same start and stop tokens). Without this, expressions like
    // sum(CASE WHEN ... END) would be mis-parsed as just the inner special expression, losing the
    // outer function wrapper, and COUNT { ... } = 0 would be mis-parsed as just the COUNT block,
    // dropping the trailing comparison (issue #5140). Token-boundary checks are exact and, unlike a
    // text-length tolerance, never swallow a short trailing operator such as "= 0".

    // count(*) - special grammar rule
    final Cypher25Parser.CountStarContext countStarCtx = builder.findCountStarRecursive(ctx);
    if (spansFullExpression(countStarCtx, ctx)) {
      final List<Expression> args = new ArrayList<>();
      args.add(new StarExpression());
      return new FunctionCallExpression("count", args, false);
    }

    // EXISTS expression
    final Cypher25Parser.ExistsExpressionContext existsCtx = builder.findExistsExpressionRecursive(ctx);
    if (spansFullExpression(existsCtx, ctx))
      return builder.parseExistsExpression(existsCtx);

    // COLLECT { ... } subquery expression
    final Cypher25Parser.CollectExpressionContext collectCtx = builder.findCollectExpressionRecursive(ctx);
    if (spansFullExpression(collectCtx, ctx))
      return builder.parseCollectExpression(collectCtx);

    // COUNT { ... } subquery expression
    final Cypher25Parser.CountExpressionContext countCtx = builder.findCountExpressionRecursive(ctx);
    if (spansFullExpression(countCtx, ctx))
      return builder.parseCountExpression(countCtx);

    // CASE expressions (both forms)
    final Cypher25Parser.CaseExpressionContext caseCtx = builder.findCaseExpressionRecursive(ctx);
    if (spansFullExpression(caseCtx, ctx))
      return builder.parseCaseExpression(caseCtx);

    final Cypher25Parser.ExtendedCaseExpressionContext extCaseCtx = builder.findExtendedCaseExpressionRecursive(ctx);
    if (spansFullExpression(extCaseCtx, ctx))
      return builder.parseExtendedCaseExpression(extCaseCtx);

    // shortestPath expressions
    final Cypher25Parser.ShortestPathExpressionContext shortestPathCtx = builder.findShortestPathExpressionRecursive(ctx);
    if (spansFullExpression(shortestPathCtx, ctx))
      return builder.parseShortestPathExpression(shortestPathCtx);

    return null;
  }

  /**
   * Returns true when {@code sub} covers the whole {@code full} expression, i.e. they share the
   * same first and last tokens. Using token boundaries (rather than a text-length tolerance) is
   * exact: a trailing operator such as {@code = 0} shifts {@code full}'s stop token past
   * {@code sub}'s, so the special-function context is correctly recognized as only a sub-part of a
   * larger comparison/arithmetic expression (issue #5140).
   */
  private static boolean spansFullExpression(final ParserRuleContext sub, final ParserRuleContext full) {
    return sub != null && sub.getStart() == full.getStart() && sub.getStop() == full.getStop();
  }

  /**
   * Try to parse comprehension expressions (reduce, pattern comprehensions, list comprehensions).
   * Returns null if not a comprehension.
   */
  Expression tryParseComprehensions(final Cypher25Parser.ExpressionContext ctx) {
    // A comprehension is only the whole expression when its parse-tree span covers the entire
    // expression. The previous length-tolerance heuristic (len >= exprText.length() - 2) wrongly
    // swallowed a trailing 2-char arithmetic operator such as /2, *2 or +1, silently dropping it
    // (issue #5342). Token-span equality is exact and lets those cases fall through to arithmetic.

    // reduce expressions
    final Cypher25Parser.ReduceExpressionContext reduceCtx = builder.findReduceExpressionRecursive(ctx);
    if (reduceCtx != null && spansFullExpression(reduceCtx, ctx))
      return builder.parseReduceExpression(reduceCtx);

    // allReduce expressions
    final Cypher25Parser.AllReduceExpressionContext allReduceCtx = builder.findAllReduceExpressionRecursive(ctx);
    if (allReduceCtx != null && spansFullExpression(allReduceCtx, ctx))
      return builder.parseAllReduceExpression(allReduceCtx);

    // Pattern comprehensions
    final Cypher25Parser.PatternComprehensionContext patternCompCtx = builder.findPatternComprehensionRecursive(ctx);
    if (patternCompCtx != null && spansFullExpression(patternCompCtx, ctx))
      return builder.parsePatternComprehension(patternCompCtx);

    // List comprehensions
    final Cypher25Parser.ListComprehensionContext listCompCtx = builder.findListComprehensionRecursive(ctx);
    if (listCompCtx != null && spansFullExpression(listCompCtx, ctx))
      return builder.parseListComprehension(listCompCtx);

    return null;
  }

  /**
   * Try to parse list predicate expressions (all(), any(), none(), single()).
   * Returns null if not a list predicate.
   */
  Expression tryParseListPredicates(final Cypher25Parser.ExpressionContext ctx) {
    final Cypher25Parser.ListItemsPredicateContext listPredCtx = builder.findListItemsPredicateRecursive(ctx);
    if (listPredCtx != null) {
      // Only return list predicate if it covers the entire expression.
      // Otherwise, it's a sub-expression (e.g., none(...) = false) and should
      // be handled by comparison/arithmetic parsing instead.
      final String predText = listPredCtx.getText();
      final String exprText = ctx.getText();
      if (predText.length() >= exprText.length() - 2) // allow for whitespace
        return builder.parseListItemsPredicate(listPredCtx);
    }

    return null;
  }

  /**
   * Try to parse comparison expressions (=, <>, <, >, <=, >=) at top level.
   * Returns null if not a top-level comparison.
   */
  Expression tryParseComparison(final Cypher25Parser.ExpressionContext ctx) {
    final Cypher25Parser.Expression8Context topExpr8 = builder.findTopLevelExpression8(ctx);
    if (topExpr8 != null)
      return builder.parseComparisonFromExpression8(topExpr8);

    return null;
  }

  /**
   * Try to parse string/list comparison (IN, STARTS WITH, ENDS WITH, CONTAINS, =~, IS NULL) at top level.
   * Returns null if not a top-level string comparison.
   */
  Expression tryParseStringComparison(final Cypher25Parser.ExpressionContext ctx) {
    final Cypher25Parser.Expression7Context topExpr7 = builder.findTopLevelExpression7(ctx);
    if (topExpr7 != null) {
      final Cypher25Parser.ComparisonExpression6Context comp = topExpr7.comparisonExpression6();
      if (comp instanceof Cypher25Parser.StringAndListComparisonContext)
        return builder.parseStringComparisonExpression(topExpr7);
      if (comp instanceof Cypher25Parser.NullComparisonContext)
        return builder.parseIsNullExpression((Cypher25Parser.NullComparisonContext) comp);
      if (comp instanceof Cypher25Parser.LabelComparisonContext)
        return builder.parseLabelComparisonExpression(topExpr7);
      // GQL IS [NOT] TYPED type and :: type (issue #3365 section 3.3)
      if (comp instanceof Cypher25Parser.TypeComparisonContext)
        return builder.parseIsTypedExpression((Cypher25Parser.TypeComparisonContext) comp);
    }
    return null;
  }

  /**
   * Try to parse arithmetic expressions at top level (additive, multiplicative, exponentiation).
   * Returns null if not a top-level arithmetic expression.
   */
  Expression tryParseArithmetic(final Cypher25Parser.ExpressionContext ctx) {
    // Additive (+, -)
    final Cypher25Parser.Expression6Context topArith6 = builder.findTopLevelExpression6(ctx);
    if (topArith6 != null)
      return builder.parseArithmeticExpression6(topArith6);

    // Multiplicative (*, /, %)
    final Cypher25Parser.Expression5Context topArith5 = builder.findTopLevelExpression5(ctx);
    if (topArith5 != null)
      return builder.parseArithmeticExpression5(topArith5);

    // Exponentiation (^)
    final Cypher25Parser.Expression4Context topArith4 = builder.findTopLevelExpression4(ctx);
    if (topArith4 != null)
      return builder.parseArithmeticExpression4(topArith4);

    return null;
  }

  /**
   * Try to parse unary expressions (-, +) at top level.
   * Returns null if not a top-level unary expression.
   */
  Expression tryParseUnary(final Cypher25Parser.ExpressionContext ctx) {
    final Cypher25Parser.Expression3Context topExpr3 = builder.findTopLevelExpression3(ctx);
    if (topExpr3 != null)
      return builder.parseArithmeticExpression3(topExpr3);

    return null;
  }

  /**
   * Try to parse postfix expressions (property access, indexing, slicing) at top level.
   * Returns null if not a top-level postfix expression.
   */
  Expression tryParsePostfix(final Cypher25Parser.ExpressionContext ctx) {
    final Cypher25Parser.Expression2Context topExpr2 = builder.findTopLevelExpression2(ctx);
    if (topExpr2 != null && !topExpr2.postFix().isEmpty())
      return builder.parseExpression2WithPostfix(topExpr2);

    return null;
  }

  /**
   * Try to parse primary expressions (function calls, list/map literals, parenthesized).
   * Returns null if not a primary expression.
   */
  Expression tryParsePrimary(final Cypher25Parser.ExpressionContext ctx) {
    // A parenthesized expression that spans the ENTIRE expression must be unwrapped and re-parsed
    // BEFORE the recursive function-invocation finder below. Otherwise an expression such as
    // (toString(null) IS NULL OR toString(null) = '') has its inner function call grabbed by
    // findFunctionInvocationRecursive, silently dropping the surrounding IS NULL / OR / comparison
    // and collapsing the whole expression to just the function result (issue #5383). The span guard
    // (same start and stop tokens) ensures we only unwrap a full-span wrapper, so cases like
    // (a) + (b) or (a) IS NULL - already handled by the arithmetic/comparison detectors above - are
    // left untouched.
    final Cypher25Parser.ParenthesizedExpressionContext fullParenCtx = builder.findParenthesizedExpressionRecursive(ctx);
    if (fullParenCtx != null && fullParenCtx.getStart() == ctx.getStart() && fullParenCtx.getStop() == ctx.getStop())
      return builder.parseExpression(fullParenCtx.expression());

    // Check for vector functions BEFORE list literals, because vector functions
    // contain list literals as arguments (e.g., vector([1.0, 2.0], 2, FLOAT32))
    // and the recursive list finder would incorrectly match the inner list.
    final Cypher25Parser.Expression1Context vecExpr1 = findExpression1OnSpine(ctx);
    if (vecExpr1 != null) {
      if (vecExpr1.vectorFunction() != null)
        return builder.parseVectorFunction(vecExpr1.vectorFunction());
      if (vecExpr1.vectorNormFunction() != null)
        return builder.parseVectorNormFunction(vecExpr1.vectorNormFunction());
      if (vecExpr1.vectorDistanceFunction() != null)
        return builder.parseVectorDistanceFunction(vecExpr1.vectorDistanceFunction());
      // trimFunction and normalizeFunction are special grammar rules in expression1.
      // Check them BEFORE findFunctionInvocationRecursive, which would otherwise find
      // an inner function (e.g., toLower inside trim(toLower(...))) and return it
      // instead of the outer trim/normalize.
      if (vecExpr1.trimFunction() != null)
        return builder.parseTrimFunction(vecExpr1.trimFunction());
      if (vecExpr1.normalizeFunction() != null)
        return builder.parseNormalizeFunction(vecExpr1.normalizeFunction());
    }

    // Check for top-level list literals BEFORE function invocations.
    // Otherwise, [date({...})] would be parsed as date() because findFunctionInvocationRecursive
    // finds the nested function call inside the list. For cases like tail([1,2,3]),
    // the function is at the top level and will be caught below.
    final Cypher25Parser.ListLiteralContext listCtx = builder.findListLiteralRecursive(ctx);
    if (listCtx != null) {
      final String listText = listCtx.getText();
      final String exprText = ctx.getText();
      if (listText.length() >= exprText.length() - 2) // Top-level list (allow for whitespace)
        return builder.parseListLiteral(listCtx);
    }

    // Function invocations
    final Cypher25Parser.FunctionInvocationContext funcCtx = builder.findFunctionInvocationRecursive(ctx);
    if (funcCtx != null)
      return builder.parseFunctionInvocation(funcCtx);

    // Map projections (before list fallback to avoid inner lists taking priority over maps)
    final Cypher25Parser.MapProjectionContext mapProjCtx = builder.findMapProjectionRecursive(ctx);
    if (mapProjCtx != null)
      return builder.parseMapProjection(mapProjCtx);

    // Map literals (before list fallback)
    final Cypher25Parser.MapContext mapCtx = builder.findMapRecursive(ctx);
    if (mapCtx != null)
      return builder.parseMapLiteralExpression(mapCtx);

    // List literals (non-top-level fallback, after map check)
    if (listCtx != null)
      return builder.parseListLiteral(listCtx);

    // Parenthesized expressions
    final Cypher25Parser.ParenthesizedExpressionContext parenCtx = builder.findParenthesizedExpressionRecursive(ctx);
    if (parenCtx != null)
      return builder.parseExpression(parenCtx.expression());

    return null;
  }

  /**
   * Fallback parsing for arithmetic expressions using recursive search.
   */
  Expression tryParseFallbackArithmetic(final Cypher25Parser.ExpressionContext ctx) {
    final Cypher25Parser.Expression6Context arith6Ctx = builder.findArithmeticExpression6Recursive(ctx);
    if (arith6Ctx != null)
      return builder.parseArithmeticExpression6(arith6Ctx);

    final Cypher25Parser.Expression5Context arith5Ctx = builder.findArithmeticExpression5Recursive(ctx);
    if (arith5Ctx != null)
      return builder.parseArithmeticExpression5(arith5Ctx);

    return null;
  }

  /**
   * Fallback parsing for postfix expressions using recursive search.
   */
  Expression tryParseFallbackPostfix(final Cypher25Parser.ExpressionContext ctx) {
    final Cypher25Parser.Expression2Context expr2Ctx = builder.findExpression2Recursive(ctx);
    if (expr2Ctx != null && !expr2Ctx.postFix().isEmpty())
      return builder.parseExpression2WithPostfix(expr2Ctx);

    return null;
  }

  /**
   * Walk the expression grammar spine (Expression → Expression11 → ... → Expression1) to find
   * the Expression1 context without branching into alternatives. Returns null if the spine
   * has branches (e.g., multiple Expression11 children indicating OR).
   */
  private Cypher25Parser.Expression1Context findExpression1OnSpine(final Cypher25Parser.ExpressionContext ctx) {
    final List<Cypher25Parser.Expression11Context> e11List = ctx.expression11();
    if (e11List.size() != 1)
      return null;
    final Cypher25Parser.Expression11Context e11 = e11List.get(0);
    final List<Cypher25Parser.Expression10Context> e10List = e11.expression10();
    if (e10List.size() != 1)
      return null;
    final Cypher25Parser.Expression10Context e10 = e10List.get(0);
    if (e10.expression9().size() != 1)
      return null;
    final Cypher25Parser.Expression9Context e9 = e10.expression9().get(0);
    final Cypher25Parser.Expression8Context e8 = e9.expression8();
    if (e8 == null || e8.expression7().size() != 1)
      return null;
    final Cypher25Parser.Expression7Context e7 = e8.expression7().get(0);
    final Cypher25Parser.Expression6Context e6 = e7.expression6();
    if (e6 == null || e6.expression5().size() != 1)
      return null;
    final Cypher25Parser.Expression5Context e5 = e6.expression5().get(0);
    if (e5.expression4().size() != 1)
      return null;
    final Cypher25Parser.Expression4Context e4 = e5.expression4().get(0);
    if (e4.expression3().size() != 1)
      return null;
    final Cypher25Parser.Expression3Context e3 = e4.expression3().get(0);
    final Cypher25Parser.Expression2Context e2 = e3.expression2();
    if (e2 == null || !e2.postFix().isEmpty())
      return null;
    return e2.expression1();
  }
}
