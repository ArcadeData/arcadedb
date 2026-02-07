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
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;

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
    // count(*) - special grammar rule
    final Cypher25Parser.CountStarContext countStarCtx = builder.findCountStarRecursive(ctx);
    if (countStarCtx != null) {
      final List<Expression> args = new java.util.ArrayList<>();
      args.add(new com.arcadedb.query.opencypher.ast.StarExpression());
      return new com.arcadedb.query.opencypher.ast.FunctionCallExpression("count", args, false);
    }

    // EXISTS expression
    final Cypher25Parser.ExistsExpressionContext existsCtx = builder.findExistsExpressionRecursive(ctx);
    if (existsCtx != null)
      return builder.parseExistsExpression(existsCtx);

    // CASE expressions (both forms)
    final Cypher25Parser.CaseExpressionContext caseCtx = builder.findCaseExpressionRecursive(ctx);
    if (caseCtx != null)
      return builder.parseCaseExpression(caseCtx);

    final Cypher25Parser.ExtendedCaseExpressionContext extCaseCtx = builder.findExtendedCaseExpressionRecursive(ctx);
    if (extCaseCtx != null)
      return builder.parseExtendedCaseExpression(extCaseCtx);

    // shortestPath expressions
    final Cypher25Parser.ShortestPathExpressionContext shortestPathCtx = builder.findShortestPathExpressionRecursive(ctx);
    if (shortestPathCtx != null)
      return builder.parseShortestPathExpression(shortestPathCtx);

    return null;
  }

  /**
   * Try to parse comprehension expressions (reduce, pattern comprehensions, list comprehensions).
   * Returns null if not a comprehension.
   */
  Expression tryParseComprehensions(final Cypher25Parser.ExpressionContext ctx) {
    // reduce expressions
    final Cypher25Parser.ReduceExpressionContext reduceCtx = builder.findReduceExpressionRecursive(ctx);
    if (reduceCtx != null)
      return builder.parseReduceExpression(reduceCtx);

    // Pattern comprehensions
    final Cypher25Parser.PatternComprehensionContext patternCompCtx = builder.findPatternComprehensionRecursive(ctx);
    if (patternCompCtx != null)
      return builder.parsePatternComprehension(patternCompCtx);

    // List comprehensions
    final Cypher25Parser.ListComprehensionContext listCompCtx = builder.findListComprehensionRecursive(ctx);
    if (listCompCtx != null)
      return builder.parseListComprehension(listCompCtx);

    return null;
  }

  /**
   * Try to parse list predicate expressions (all(), any(), none(), single()).
   * Returns null if not a list predicate.
   */
  Expression tryParseListPredicates(final Cypher25Parser.ExpressionContext ctx) {
    final Cypher25Parser.ListItemsPredicateContext listPredCtx = builder.findListItemsPredicateRecursive(ctx);
    if (listPredCtx != null)
      return builder.parseListItemsPredicate(listPredCtx);

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
    // Function invocations
    final Cypher25Parser.FunctionInvocationContext funcCtx = builder.findFunctionInvocationRecursive(ctx);
    if (funcCtx != null)
      return builder.parseFunctionInvocation(funcCtx);

    // List literals
    final Cypher25Parser.ListLiteralContext listCtx = builder.findListLiteralRecursive(ctx);
    if (listCtx != null)
      return builder.parseListLiteral(listCtx);

    // Map projections
    final Cypher25Parser.MapProjectionContext mapProjCtx = builder.findMapProjectionRecursive(ctx);
    if (mapProjCtx != null)
      return builder.parseMapProjection(mapProjCtx);

    // Map literals
    final Cypher25Parser.MapContext mapCtx = builder.findMapRecursive(ctx);
    if (mapCtx != null)
      return builder.parseMapLiteralExpression(mapCtx);

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
}
