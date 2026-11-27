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
import com.arcadedb.query.opencypher.ast.*;
import com.arcadedb.query.opencypher.grammar.Cypher25Lexer;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Handles expression parsing for the Cypher AST builder.
 * Extracts expression-related visitor methods from CypherASTBuilder for better separation of concerns.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
class CypherExpressionBuilder {

  private final ExpressionTypeDetector detector = new ExpressionTypeDetector(this);
  final ExpressionPatternParser patterns = new ExpressionPatternParser(this);
  // Set only while the comparison-form WHEN predicates of an extended CASE are being parsed
  private CaseOperandExpression activeCaseOperand;

  /**
   * Parse an expression into an Expression AST node.
   * Follows the precedence hierarchy defined in {@link ExpressionPrecedence}.
   *
   * Expression parsing order (see ExpressionPrecedence for details):
   * 1. Logical operators (OR, XOR, AND, NOT) - lowest precedence
   * 2. Special functions (count(*), EXISTS, CASE, shortestPath)
   * 3. Comprehensions (list, pattern, reduce)
   * 4. List predicates (all, any, none, single)
   * 5. Comparison operators (=, <>, <, >, <=, >=)
   * 6. String/List comparisons (STARTS WITH, ENDS WITH, CONTAINS, IN, =~, IS NULL)
   * 7. Arithmetic operators (+, -, *, /, %, ^)
   * 8. Unary operators (-, +)
   * 9. Postfix operators (property access, indexing, slicing)
   * 10. Primary expressions (literals, variables, functions, parenthesized) - highest precedence
   */
  Expression parseExpression(final Cypher25Parser.ExpressionContext ctx) {
    // The expression's own text is materialised ONLY where it is used - the two list-literal coverage guards and
    // the text fallback at the end of this method, all of which sit behind checks that fail for an ordinary
    // expression. It used to be computed here, before a single one of the dozen structural checks below had a
    // chance to return, which made a RuleContext.getText() - a concatenation of every token in the subtree into a
    // fresh String - a fixed cost of parsing any expression at all (issue #8132).
    // === LOGICAL OPERATORS (Lowest precedence) ===
    // Grammar: expression = expression11 (OR expression11)*
    final List<Cypher25Parser.Expression11Context> expr11List = ctx.expression11();
    if (expr11List.size() > 1) {
      // OR expression
      Expression result = parseExpressionFromExpression11(expr11List.get(0));
      for (int i = 1; i < expr11List.size(); i++) {
        final Expression right = parseExpressionFromExpression11(expr11List.get(i));
        result = new TernaryLogicalExpression(TernaryLogicalExpression.Operator.OR, result, right);
      }
      return result;
    }

    // Check for XOR/AND/NOT at lower levels
    if (expr11List.size() == 1) {
      final Expression logicalExpr = tryParseLogicalExpression(expr11List.get(0));
      if (logicalExpr != null)
        return logicalExpr;
    }

    // === SPECIAL FUNCTIONS (count(*), EXISTS, CASE, shortestPath) ===
    Expression expr = detector.tryParseSpecialFunctions(ctx);
    if (expr != null)
      return expr;

    // === COMPREHENSIONS (list, pattern, reduce) ===
    expr = detector.tryParseComprehensions(ctx);
    if (expr != null)
      return expr;

    // === LIST PREDICATES (all, any, none, single) ===
    expr = detector.tryParseListPredicates(ctx);
    if (expr != null)
      return expr;

    // === COMPARISON OPERATORS (=, <>, <, >, <=, >=) ===
    expr = detector.tryParseComparison(ctx);
    if (expr != null)
      return expr;

    // === STRING/LIST COMPARISON (STARTS WITH, ENDS WITH, CONTAINS, IN, =~, IS NULL) ===
    expr = detector.tryParseStringComparison(ctx);
    if (expr != null)
      return expr;

    // === ARITHMETIC OPERATORS (+, -, *, /, %, ^) ===
    expr = detector.tryParseArithmetic(ctx);
    if (expr != null)
      return expr;

    // === UNARY OPERATORS (-, +) ===
    expr = detector.tryParseUnary(ctx);
    if (expr != null)
      return expr;

    // === POSTFIX OPERATORS (property access, indexing, slicing) ===
    expr = detector.tryParsePostfix(ctx);
    if (expr != null)
      return expr;

    // === PRIMARY EXPRESSIONS (literals, variables, functions, parenthesized) ===
    expr = detector.tryParsePrimary(ctx);
    if (expr != null)
      return expr;

    // === FALLBACK: Recursive arithmetic checks ===
    expr = detector.tryParseFallbackArithmetic(ctx);
    if (expr != null)
      return expr;

    // === FALLBACK: Recursive postfix checks ===
    expr = detector.tryParseFallbackPostfix(ctx);
    if (expr != null)
      return expr;

    // Check for TOP-LEVEL arithmetic expressions using spine walks.
    // Must be checked BEFORE list/map literals because [1,10,100]+[4,5] should be
    // arithmetic (list concatenation), not just [1,10,100] (ignoring +[4,5])
    final Cypher25Parser.Expression6Context topArith6 = ParseTreeSearch.findTopLevelExpression6(ctx);
    if (topArith6 != null)
      return parseArithmeticExpression6(topArith6);

    // Check for TOP-LEVEL multiplicative (*, /, %) using spine walk
    final Cypher25Parser.Expression5Context topArith5 = ParseTreeSearch.findTopLevelExpression5(ctx);
    if (topArith5 != null)
      return parseArithmeticExpression5(topArith5);

    // Check for TOP-LEVEL exponentiation (^) using spine walk
    final Cypher25Parser.Expression4Context topArith4 = ParseTreeSearch.findTopLevelExpression4(ctx);
    if (topArith4 != null)
      return parseArithmeticExpression4(topArith4);

    // Check for TOP-LEVEL unary operator (-, +) using spine walk
    final Cypher25Parser.Expression3Context topExpr3 = ParseTreeSearch.findTopLevelExpression3(ctx);
    if (topExpr3 != null)
      return parseArithmeticExpression3(topExpr3);

    // Check for TOP-LEVEL postfix expressions (property access, list indexing, slicing)
    // using spine walk. Must be before list/map literals because [1,2,3][0] should be
    // postfix indexing, not just [1,2,3]
    final Cypher25Parser.Expression2Context topExpr2 = ParseTreeSearch.findTopLevelExpression2(ctx);
    if (topExpr2 != null && !topExpr2.postFix().isEmpty())
      return parseExpression2WithPostfix(topExpr2);

    // Check for list literals BEFORE function invocations when the top-level expression
    // is a list. Otherwise, [date({...}), date({...})] would be parsed as a single date()
    // function call because findFunctionInvocationRecursive finds date() inside the list.
    // For cases like tail([1,2,3]), the function invocation is at the top level and will
    // be caught by the postfix/expression2 checks above.
    final Cypher25Parser.ListLiteralContext listCtx = ParseTreeSearch.findListLiteralRecursive(ctx);
    if (listCtx != null) {
      // Only use list literal if it's at the top level (i.e. the list text covers most of the expression)
      final String listText = listCtx.getText();
      if (listText.length() >= ctx.getText().length() - 2) // Allow for whitespace
        return parseListLiteral(listCtx);
    }

    // Check for trim function with extended syntax (LEADING/TRAILING/BOTH ... FROM ...)
    final Cypher25Parser.TrimFunctionContext trimCtx = ParseTreeSearch.findTrimFunctionRecursive(ctx);
    if (trimCtx != null)
      return parseTrimFunction(trimCtx);

    // Check for function invocations (after top-level list check)
    // (tail([1,2,3]) should be parsed as a function call, not as a list literal)
    final Cypher25Parser.FunctionInvocationContext funcCtx = ParseTreeSearch.findFunctionInvocationRecursive(ctx);
    if (funcCtx != null) {
      return parseFunctionInvocation(funcCtx);
    }

    // Check for map projections (before list fallback to avoid nested list taking priority over maps)
    final Cypher25Parser.MapProjectionContext mapProjCtx = ParseTreeSearch.findMapProjectionRecursive(ctx);
    if (mapProjCtx != null) {
      return parseMapProjection(mapProjCtx);
    }

    // Check for map literals (before list fallback)
    final Cypher25Parser.MapContext mapCtx = ParseTreeSearch.findMapRecursive(ctx);
    if (mapCtx != null) {
      return parseMapLiteralExpression(mapCtx);
    }

    // Check for list literals (fallback for other cases, after map check)
    if (listCtx != null) {
      // Guard: only use if list covers most of the expression (avoids inner list taking over)
      final String listText2 = listCtx.getText();
      if (listText2.length() >= ctx.getText().length() - 2)
        return parseListLiteral(listCtx);
    }

    // Fallback: recursive arithmetic expression checks
    final Cypher25Parser.Expression6Context arith6Ctx = ParseTreeSearch.findArithmeticExpression6Recursive(ctx);
    if (arith6Ctx != null) {
      return parseArithmeticExpression6(arith6Ctx);
    }
    final Cypher25Parser.Expression5Context arith5Ctx = ParseTreeSearch.findArithmeticExpression5Recursive(ctx);
    if (arith5Ctx != null) {
      return parseArithmeticExpression5(arith5Ctx);
    }

    // Fallback: recursive postfix expression check
    final Cypher25Parser.Expression2Context expr2Ctx = ParseTreeSearch.findExpression2Recursive(ctx);
    if (expr2Ctx != null && !expr2Ctx.postFix().isEmpty()) {
      return parseExpression2WithPostfix(expr2Ctx);
    }

    // Check for parenthesized expressions that may contain logical operators
    // e.g., (true AND null), (a OR b), (NOT x)
    final Cypher25Parser.ParenthesizedExpressionContext parenCtx = ParseTreeSearch.findParenthesizedExpressionRecursive(ctx);
    if (parenCtx != null)
      return parseExpression(parenCtx.expression());

    // Use the shared text parsing logic
    return parseExpressionText(ctx.getText());
  }

  /**
   * The FIRST occurrence, in the same depth-first pre-order the individual {@code findXxxRecursive} searches use, of
   * each of the five block constructs {@link #parseExpressionFromText} probes for before it dispatches structurally.
   * <p>
   * One traversal answers all five (issue #8132). Descent does NOT stop at a match, because the searches this
   * replaces were independent of one another: a {@code CASE} found first must not hide an {@code EXISTS} nested
   * inside it, which is exactly what {@code findExistsExpressionRecursive} would still have found on its own pass.
   * Only the first of each kind is kept, which is what each of those searches returned.
   */
  private static final class BlockConstructs {
    private static final BlockConstructs NONE = new BlockConstructs();

    private Cypher25Parser.CaseExpressionContext         caseExpression;
    private Cypher25Parser.ExtendedCaseExpressionContext extendedCaseExpression;
    private Cypher25Parser.ExistsExpressionContext       existsExpression;
    private Cypher25Parser.CollectExpressionContext      collectExpression;
    private Cypher25Parser.CountExpressionContext        countExpression;

    static BlockConstructs of(final ParseTree node) {
      final BlockConstructs found = new BlockConstructs();
      found.collectFrom(node);
      // The common case by a wide margin: hand back the shared empty instance so a per-call allocation only
      // survives when it carries something.
      return found.isEmpty() ? NONE : found;
    }

    private void collectFrom(final ParseTree node) {
      if (node == null)
        return;

      if (node instanceof Cypher25Parser.CaseExpressionContext ctx) {
        if (caseExpression == null)
          caseExpression = ctx;
      } else if (node instanceof Cypher25Parser.ExtendedCaseExpressionContext ctx) {
        if (extendedCaseExpression == null)
          extendedCaseExpression = ctx;
      } else if (node instanceof Cypher25Parser.ExistsExpressionContext ctx) {
        if (existsExpression == null)
          existsExpression = ctx;
      } else if (node instanceof Cypher25Parser.CollectExpressionContext ctx) {
        if (collectExpression == null)
          collectExpression = ctx;
      } else if (node instanceof Cypher25Parser.CountExpressionContext ctx) {
        if (countExpression == null)
          countExpression = ctx;
      }

      for (int i = 0; i < node.getChildCount(); i++)
        collectFrom(node.getChild(i));
    }

    private boolean isEmpty() {
      return caseExpression == null && extendedCaseExpression == null && existsExpression == null
          && collectExpression == null && countExpression == null;
    }
  }

  /**
   * Parse any parse tree node as an expression using its text.
   * This is a helper for parsing lower-level expression contexts.
   */
  Expression parseExpressionFromText(final ParseTree node) {
    return parseExpressionFromText(node, false);
  }

  /**
   * @param noBlockConstructs {@code true} when the caller has ALREADY established that this node's subtree holds
   *                          none of the five block constructs, so the probe for them can be skipped entirely.
   *                          Only ever passed on from a level of the same descent that found none: this method
   *                          walks down the grammar's expression cascade by re-entering itself, so a node it
   *                          descends into is a subtree of one already searched, and a construct absent from the
   *                          parent is absent from the child (issue #8132)
   */
  private Expression parseExpressionFromText(final ParseTree node, final boolean noBlockConstructs) {
    // The five block constructs below use a length guard: only match when the found context IS the whole node,
    // preventing mis-parsing of func(CASE ...) as just the inner CASE. getText() carries no whitespace, so the two
    // texts are equal exactly when nothing else is in the node: the slack of two characters this guard used to allow
    // took `COUNT { ... } > 0` for the bare COUNT and dropped the comparison, which only the truthiness of a
    // non-Boolean under AND/OR hid (issue #11760). Anything else - a construct in parentheses included - is reached by
    // the descent of the grammar below, which handles every one of the five at the atom level.
    //
    // Found in ONE traversal, skipped outright once a level above has ruled them out, and the node's own text
    // materialised only if one of them turned up (issue #8132). This method walks the grammar's expression
    // hierarchy by re-entering itself once per level - expression11 down to expression1 is a dozen levels for a
    // trivial predicate like `p.id = 5` - so anything done per call is done a dozen times over what is very nearly
    // the same subtree. Five separate DFS passes plus a RuleContext.getText() (which concatenates every token under
    // the node into a fresh String) came to ~60 subtree walks and a dozen throwaway strings for an expression that
    // has none of these constructs, which is the overwhelmingly common case and was a third of the whole parse.
    final BlockConstructs blocks = noBlockConstructs ? BlockConstructs.NONE : BlockConstructs.of(node);
    final boolean noBlocksBelow = blocks.isEmpty();
    final int nodeTextLength = noBlocksBelow ? 0 : node.getText().length();

    if (blocks.caseExpression != null && blocks.caseExpression.getText().length() == nodeTextLength)
      return parseCaseExpression(blocks.caseExpression);

    if (blocks.extendedCaseExpression != null && blocks.extendedCaseExpression.getText().length() == nodeTextLength)
      return parseExtendedCaseExpression(blocks.extendedCaseExpression);

    // Check for EXISTS expressions
    if (blocks.existsExpression != null && blocks.existsExpression.getText().length() == nodeTextLength)
      return SubqueryExpressionParser.parseExistsExpression(blocks.existsExpression);

    // Check for COLLECT { ... } subqueries
    if (blocks.collectExpression != null && blocks.collectExpression.getText().length() == nodeTextLength)
      return SubqueryExpressionParser.parseCollectExpression(blocks.collectExpression);

    // Check for COUNT { ... } subqueries
    if (blocks.countExpression != null && blocks.countExpression.getText().length() == nodeTextLength)
      return SubqueryExpressionParser.parseCountExpression(blocks.countExpression);

    // Check for logical expressions (AND, OR, XOR, NOT) in the parse tree
    // This handles cases like (a AND b) appearing as children of comparisons
    if (node instanceof Cypher25Parser.ExpressionContext)
      return parseExpression((Cypher25Parser.ExpressionContext) node);

    if (node instanceof Cypher25Parser.Expression11Context) {
      final Expression logicalExpr = tryParseLogicalExpression((Cypher25Parser.Expression11Context) node);
      if (logicalExpr != null)
        return logicalExpr;
    }

    if (node instanceof Cypher25Parser.Expression10Context) {
      final Cypher25Parser.Expression10Context e10 = (Cypher25Parser.Expression10Context) node;
      if (e10.expression9().size() > 1) {
        Expression result = parseExpressionFromExpression9(e10.expression9().get(0));
        for (int i = 1; i < e10.expression9().size(); i++) {
          final Expression right = parseExpressionFromExpression9(e10.expression9().get(i));
          result = new TernaryLogicalExpression(TernaryLogicalExpression.Operator.AND, result, right);
        }
        return result;
      }
      if (e10.expression9().size() == 1)
        return parseExpressionFromExpression9(e10.expression9().get(0));
    }

    if (node instanceof Cypher25Parser.Expression9Context)
      return parseExpressionFromExpression9((Cypher25Parser.Expression9Context) node);

    // Handle Expression2 with postfix operations (property access, indexing, slicing)
    // Must be checked early to avoid inner list/map literals being matched first
    if (node instanceof Cypher25Parser.Expression2Context) {
      final Cypher25Parser.Expression2Context e2 = (Cypher25Parser.Expression2Context) node;
      if (!e2.postFix().isEmpty())
        return parseExpression2WithPostfix(e2);
      // No postfix — delegate to expression1
      return parseExpressionFromText(e2.expression1(), noBlocksBelow);
    }

    // Handle Expression8 (comparison operators: =, <>, <, >, <=, >=)
    if (node instanceof Cypher25Parser.Expression8Context) {
      final Cypher25Parser.Expression8Context expr8 = (Cypher25Parser.Expression8Context) node;
      if (expr8.expression7().size() > 1)
        return parseComparisonFromExpression8(expr8);
      // Single Expression7 child, delegate to it
      if (!expr8.expression7().isEmpty())
        return parseExpressionFromText(expr8.expression7().get(0), noBlocksBelow);
    }

    // Handle Expression7 (IS NULL, STARTS WITH, ENDS WITH, CONTAINS, IN, label check, IS TYPED)
    if (node instanceof Cypher25Parser.Expression7Context) {
      final Cypher25Parser.Expression7Context expr7 = (Cypher25Parser.Expression7Context) node;
      final Cypher25Parser.ComparisonExpression6Context comp = expr7.comparisonExpression6();
      if (comp instanceof Cypher25Parser.NullComparisonContext)
        return parseIsNullExpression((Cypher25Parser.NullComparisonContext) comp);
      if (comp instanceof Cypher25Parser.StringAndListComparisonContext)
        return parseStringComparisonExpression(expr7);
      if (comp instanceof Cypher25Parser.LabelComparisonContext)
        return parseLabelComparisonExpression(expr7);
      if (comp instanceof Cypher25Parser.TypeComparisonContext)
        return parseIsTypedExpression((Cypher25Parser.TypeComparisonContext) comp);
      if (comp instanceof Cypher25Parser.NormalFormComparisonContext normalForm)
        return parseIsNormalizedExpression(normalForm);
      // No comparison, delegate to expression6
      return parseExpressionFromText(expr7.expression6(), noBlocksBelow);
    }

    // Handle intermediate expression contexts by walking down the grammar hierarchy.
    // This prevents recursive searches from crossing parenthesized boundaries.
    if (node instanceof Cypher25Parser.Expression6Context) {
      final Cypher25Parser.Expression6Context e6 = (Cypher25Parser.Expression6Context) node;
      if (e6.expression5().size() > 1)
        return parseArithmeticExpression6(e6);
      return parseExpressionFromText(e6.expression5().get(0), noBlocksBelow);
    }

    if (node instanceof Cypher25Parser.Expression5Context) {
      final Cypher25Parser.Expression5Context e5 = (Cypher25Parser.Expression5Context) node;
      if (e5.expression4().size() > 1)
        return parseArithmeticExpression5(e5);
      return parseExpressionFromText(e5.expression4().get(0), noBlocksBelow);
    }

    if (node instanceof Cypher25Parser.Expression4Context) {
      final Cypher25Parser.Expression4Context e4 = (Cypher25Parser.Expression4Context) node;
      if (e4.expression3().size() > 1)
        return parseArithmeticExpression4(e4);
      return parseExpressionFromText(e4.expression3().get(0), noBlocksBelow);
    }

    if (node instanceof Cypher25Parser.Expression3Context) {
      return parseArithmeticExpression3((Cypher25Parser.Expression3Context) node);
    }

    // Handle Expression1 (atom level: literals, variables, function calls, list predicates, etc.)
    if (node instanceof Cypher25Parser.Expression1Context) {
      final Cypher25Parser.Expression1Context e1 = (Cypher25Parser.Expression1Context) node;
      if (e1.listItemsPredicate() != null)
        return parseListItemsPredicate(e1.listItemsPredicate());
      if (e1.functionInvocation() != null)
        return parseFunctionInvocation(e1.functionInvocation());
      if (e1.caseExpression() != null)
        return parseCaseExpression(e1.caseExpression());
      if (e1.extendedCaseExpression() != null)
        return parseExtendedCaseExpression(e1.extendedCaseExpression());
      if (e1.listLiteral() != null)
        return parseListLiteral(e1.listLiteral());
      if (e1.mapProjection() != null)
        return parseMapProjection(e1.mapProjection());
      if (e1.listComprehension() != null)
        return parseListComprehension(e1.listComprehension());
      if (e1.patternComprehension() != null)
        return parsePatternComprehension(e1.patternComprehension());
      if (e1.reduceExpression() != null)
        return parseReduceExpression(e1.reduceExpression());
      if (e1.allReduceExpression() != null)
        return parseAllReduceExpression(e1.allReduceExpression());
      if (e1.existsExpression() != null)
        return SubqueryExpressionParser.parseExistsExpression(e1.existsExpression());
      if (e1.collectExpression() != null)
        return SubqueryExpressionParser.parseCollectExpression(e1.collectExpression());
      if (e1.countExpression() != null)
        return SubqueryExpressionParser.parseCountExpression(e1.countExpression());
      if (e1.countStar() != null) {
        final List<Expression> e1Args = new ArrayList<>();
        e1Args.add(new StarExpression());
        return new FunctionCallExpression("count", e1Args, false);
      }
      if (e1.parenthesizedExpression() != null)
        return parseExpression(e1.parenthesizedExpression().expression());
      if (e1.shortestPathExpression() != null)
        return patterns.parseShortestPathExpression(e1.shortestPathExpression());
      if (e1.patternExpression() != null)
        return parsePatternPredicate(e1.patternExpression());
      if (e1.vectorFunction() != null)
        return parseVectorFunction(e1.vectorFunction());
      if (e1.vectorNormFunction() != null)
        return parseVectorNormFunction(e1.vectorNormFunction());
      if (e1.vectorDistanceFunction() != null)
        return parseVectorDistanceFunction(e1.vectorDistanceFunction());
      if (e1.trimFunction() != null)
        return parseTrimFunction(e1.trimFunction());
      if (e1.normalizeFunction() != null)
        return parseNormalizeFunction(e1.normalizeFunction());
      // Check for map literal
      final Cypher25Parser.MapContext e1MapCtx = ParseTreeSearch.findMapRecursive(e1);
      if (e1MapCtx != null)
        return parseMapLiteralExpression(e1MapCtx);
      // Fallback: parse as variable or literal
      // (will be handled by the text-based parsing below)
    }

    // Check for IS NULL / IS NOT NULL expressions BEFORE parenthesized expressions
    // because (a AND b) IS NULL should be parsed as IS_NULL(a AND b), not just (a AND b)
    final Cypher25Parser.NullComparisonContext nullCtx = ParseTreeSearch.findNullComparisonRecursive(node);
    if (nullCtx != null)
      return parseIsNullExpression(nullCtx);

    // Check for IS [NOT] TYPED / :: type expressions before parenthesized expressions for the
    // same reason (issue #3365 section 3.3).
    final Cypher25Parser.TypeComparisonContext typeCtx = ParseTreeSearch.findTypeComparisonRecursive(node);
    if (typeCtx != null)
      return parseIsTypedExpression(typeCtx);

    // Check for parenthesized expressions containing logical operators
    final Cypher25Parser.ParenthesizedExpressionContext parenCtx = ParseTreeSearch.findParenthesizedExpressionRecursive(node);
    if (parenCtx != null)
      return parseExpression(parenCtx.expression());

    // Check for arithmetic expressions (+ - * / % ^)
    final Cypher25Parser.Expression6Context arith6Ctx = ParseTreeSearch.findArithmeticExpression6Recursive(node);
    if (arith6Ctx != null)
      return parseArithmeticExpression6(arith6Ctx);

    final Cypher25Parser.Expression5Context arith5Ctx = ParseTreeSearch.findArithmeticExpression5Recursive(node);
    if (arith5Ctx != null)
      return parseArithmeticExpression5(arith5Ctx);

    // Check for list predicates (all/any/none/single) BEFORE function invocations
    // to avoid misinterpreting them as regular function calls
    final Cypher25Parser.ListItemsPredicateContext listPredCtx = ParseTreeSearch.findListItemsPredicateRecursive(node);
    if (listPredCtx != null)
      return parseListItemsPredicate(listPredCtx);

    // Check for reduce expressions
    final Cypher25Parser.ReduceExpressionContext reduceCtx = ParseTreeSearch.findReduceExpressionRecursive(node);
    if (reduceCtx != null)
      return parseReduceExpression(reduceCtx);

    // Check for allReduce expressions
    final Cypher25Parser.AllReduceExpressionContext allReduceCtx = ParseTreeSearch.findAllReduceExpressionRecursive(node);
    if (allReduceCtx != null)
      return parseAllReduceExpression(allReduceCtx);

    // Check for list comprehensions [x IN list | expr]
    final Cypher25Parser.ListComprehensionContext listCompCtx = ParseTreeSearch.findListComprehensionRecursive(node);
    if (listCompCtx != null)
      return parseListComprehension(listCompCtx);

    // Check for function invocations
    final Cypher25Parser.FunctionInvocationContext funcCtx = ParseTreeSearch.findFunctionInvocationRecursive(node);
    if (funcCtx != null)
      return parseFunctionInvocation(funcCtx);

    // Check for list/map literals
    final Cypher25Parser.ListLiteralContext listLitCtx = ParseTreeSearch.findListLiteralRecursive(node);
    if (listLitCtx != null)
      return parseListLiteral(listLitCtx);

    final Cypher25Parser.MapContext mapCtx = ParseTreeSearch.findMapRecursive(node);
    if (mapCtx != null)
      return parseMapLiteralExpression(mapCtx);

    // Fallback to text parsing
    final String text = node.getText();
    return parseExpressionText(text);
  }

  /**
   * Parse expression text into an Expression AST node.
   * Shared logic for parsing expressions from text.
   */
  Expression parseExpressionText(final String text) {
    // Check for parameter: $paramName or $1. Backticks are stripped so an escaped name resolves against
    // the key the caller bound, e.g. $`my param` -> "my param".
    if (text.startsWith("$")) {
      final String parameterName = CypherASTBuilder.stripBackticks(text.substring(1)); // Remove the $ prefix
      return new ParameterExpression(parameterName, text);
    }

    // Check for null literal explicitly (tryParseLiteral returns Java null for it)
    if ("null".equalsIgnoreCase(text))
      return new LiteralExpression(null, text);

    // Try to parse as literal FIRST (before checking for dots)
    // This prevents string literals like 'A.*' from being parsed as property access
    final Object literalValue = LiteralParser.tryParseLiteral(text);
    if (literalValue != null) {
      return new LiteralExpression(literalValue, text);
    }

    // Check for comparison expressions: expr1 op expr2
    // This handles cases like ID(a) = row.source_id, x > 5, etc.
    // We need to handle this before function calls because comparisons like ID(a)=x
    // contain parentheses but should be parsed as comparisons
    final String[] comparisonOps = {"!=", "<=", ">=", "<>", "=", "<", ">"};
    for (final String op : comparisonOps) {
      // Find the operator, but be careful with operators inside parentheses
      final int opIndex = findOperatorOutsideParentheses(text, op);
      if (opIndex > 0 && opIndex < text.length() - op.length()) {
        final String leftText = text.substring(0, opIndex).trim();
        final String rightText = text.substring(opIndex + op.length()).trim();
        if (!leftText.isEmpty() && !rightText.isEmpty()) {
          final Expression left = parseExpressionText(leftText);
          final Expression right = parseExpressionText(rightText);
          final ComparisonExpression.Operator compOp = "<>".equals(op) ?
              ComparisonExpression.Operator.NOT_EQUALS :
              ComparisonExpression.Operator.fromString(op);
          // Return a BooleanExpression wrapped as Expression
          return new ComparisonExpressionWrapper(left, compOp, right);
        }
      }
    }

    // Check for function call: functionName(args)
    // This handles cases like ID(n), count(n), etc. in WHERE clauses
    if (text.contains("(") && text.endsWith(")")) {
      final int openParen = text.indexOf('(');
      final String functionName = text.substring(0, openParen).trim();

      // Make sure it's a valid function name (letters, numbers, underscores)
      if (functionName.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
        final String argsText = text.substring(openParen + 1, text.length() - 1).trim();
        final List<Expression> args = new ArrayList<>();

        if (!argsText.isEmpty()) {
          // Parse arguments (simple split by comma - doesn't handle nested calls)
          // For nested calls, we'd need the full parse tree
          final String[] argParts = argsText.split(",");
          for (final String argPart : argParts) {
            args.add(parseExpressionText(argPart.trim()));
          }
        }

        return new FunctionCallExpression(functionName, args, false);
      }
    }

    // Check for property access: variable.property
    if (text.contains(".") && !text.contains("(")) {
      final String[] parts = text.split("\\.", 2);
      return new PropertyAccessExpression(CypherASTBuilder.stripBackticks(parts[0]), CypherASTBuilder.stripBackticks(parts[1]));
    }

    // Simple variable (strip backticks so escaped keywords like `match` match the name bound by the pattern)
    if (activeCaseOperand != null && CaseOperandExpression.PLACEHOLDER.equals(text))
      return activeCaseOperand;
    return new VariableExpression(CypherASTBuilder.stripBackticks(text));
  }

  /**
   * Parse a list expression into a list of Expression items.
   * Handles list literals like [1, 2, 3] or ['Alice', 'Bob'].
   */
  List<Expression> parseListExpression(final ParseTree node) {
    final List<Expression> items = new ArrayList<>();

    // Try to find a listLiteral context
    final Cypher25Parser.ListLiteralContext listCtx = ParseTreeSearch.findListLiteral(node);
    if (listCtx != null) {
      // Parse each expression in the list
      for (final Cypher25Parser.ExpressionContext exprCtx : listCtx.expression()) {
        items.add(parseExpression(exprCtx));
      }
      return items;
    }

    // Fallback: parse as text and try to extract items
    // This handles simple cases like [1,2,3] or ['a','b','c']
    final String text = node.getText();
    if (text.startsWith("[") && text.endsWith("]")) {
      final String content = text.substring(1, text.length() - 1);
      if (!content.isEmpty()) {
        final String[] parts = content.split(",");
        for (final String part : parts) {
          items.add(parseExpressionText(part.trim()));
        }
      }
    } else {
      // Not a list literal, treat as single item
      items.add(parseExpressionFromText(node));
    }

    return items;
  }

  // ============================================================================
  // Logical Expression Parsing (three-valued logic for AND/OR/NOT/XOR)
  // ============================================================================

  /**
   * Try to parse a logical expression (XOR, AND, NOT) from an Expression11 context.
   * Returns null if no logical operator is found.
   */
  private Expression tryParseLogicalExpression(final Cypher25Parser.Expression11Context ctx) {
    // expression11 = expression10 (XOR expression10)*
    final List<Cypher25Parser.Expression10Context> expr10List = ctx.expression10();

    if (expr10List.size() > 1) {
      // XOR expression
      Expression result = parseExpressionFromExpression10(expr10List.get(0));
      for (int i = 1; i < expr10List.size(); i++) {
        final Expression right = parseExpressionFromExpression10(expr10List.get(i));
        result = new TernaryLogicalExpression(TernaryLogicalExpression.Operator.XOR, result, right);
      }
      return result;
    }

    if (expr10List.size() == 1) {
      final Cypher25Parser.Expression10Context expr10 = expr10List.get(0);
      // expression10 = expression9 (AND expression9)*
      final List<Cypher25Parser.Expression9Context> expr9List = expr10.expression9();

      if (expr9List.size() > 1) {
        // AND expression
        Expression result = parseExpressionFromExpression9(expr9List.get(0));
        for (int i = 1; i < expr9List.size(); i++) {
          final Expression right = parseExpressionFromExpression9(expr9List.get(i));
          result = new TernaryLogicalExpression(TernaryLogicalExpression.Operator.AND, result, right);
        }
        return result;
      }

      if (expr9List.size() == 1) {
        final Cypher25Parser.Expression9Context expr9 = expr9List.get(0);
        // expression9 = NOT* expression8
        if (expr9.NOT() != null && !expr9.NOT().isEmpty()) {
          // NOT expression
          final Expression inner = parseExpressionFromText(expr9.expression8());
          Expression result = inner;
          // Apply NOT for each NOT token (handles double negation)
          for (int i = 0; i < expr9.NOT().size(); i++)
            result = new TernaryLogicalExpression(TernaryLogicalExpression.Operator.NOT, result);
          return result;
        }
      }
    }

    return null;
  }

  /**
   * Parse an Expression11 context into an Expression.
   */
  private Expression parseExpressionFromExpression11(final Cypher25Parser.Expression11Context ctx) {
    // Check for XOR/AND/NOT at this level
    final Expression logicalExpr = tryParseLogicalExpression(ctx);
    if (logicalExpr != null)
      return logicalExpr;
    // No logical operators — delegate to inner expression10 to walk down the grammar
    // hierarchy properly (avoids recursive search that finds arithmetic before list predicates)
    final List<Cypher25Parser.Expression10Context> expr10List = ctx.expression10();
    if (expr10List.size() == 1)
      return parseExpressionFromExpression10(expr10List.get(0));
    // Fallback: parse as text
    return parseExpressionFromText(ctx);
  }

  /**
   * Parse an Expression10 context into an Expression.
   */
  private Expression parseExpressionFromExpression10(final Cypher25Parser.Expression10Context ctx) {
    // expression10 = expression9 (AND expression9)*
    final List<Cypher25Parser.Expression9Context> expr9List = ctx.expression9();
    if (expr9List.size() > 1) {
      Expression result = parseExpressionFromExpression9(expr9List.get(0));
      for (int i = 1; i < expr9List.size(); i++) {
        final Expression right = parseExpressionFromExpression9(expr9List.get(i));
        result = new TernaryLogicalExpression(TernaryLogicalExpression.Operator.AND, result, right);
      }
      return result;
    }
    if (expr9List.size() == 1)
      return parseExpressionFromExpression9(expr9List.get(0));
    return parseExpressionFromText(ctx);
  }

  /**
   * Parse an Expression9 context into an Expression.
   */
  private Expression parseExpressionFromExpression9(final Cypher25Parser.Expression9Context ctx) {
    // expression9 = NOT* expression8
    final boolean hasNot = ctx.NOT() != null && !ctx.NOT().isEmpty();
    final Expression inner = parseExpressionFromText(ctx.expression8());
    if (hasNot) {
      Expression result = inner;
      for (int i = 0; i < ctx.NOT().size(); i++)
        result = new TernaryLogicalExpression(TernaryLogicalExpression.Operator.NOT, result);
      return result;
    }
    return inner;
  }

  // ============================================================================
  // Recursive Find Methods
  // ============================================================================

  /**
   * Parse a trimFunction context into a FunctionCallExpression.
   * Handles both simple trim(source) and extended trim(LEADING/TRAILING/BOTH trimChar FROM source).
   */
  Expression parseTrimFunction(final Cypher25Parser.TrimFunctionContext ctx) {
    final List<Expression> args = new ArrayList<>();

    if (ctx.FROM() != null) {
      // Extended syntax: trim(MODE trimChar FROM source)
      String mode = "BOTH"; // default
      if (ctx.LEADING() != null)
        mode = "LEADING";
      else if (ctx.TRAILING() != null)
        mode = "TRAILING";

      args.add(new LiteralExpression(mode, mode));

      // trimCharacterString may be absent (e.g., trim(LEADING FROM source) - trim whitespace)
      // vs explicitly null (e.g., trim(null FROM source) - return null per Cypher spec).
      // Use empty string to signal "default whitespace" when no character is provided.
      if (ctx.trimCharacterString != null)
        args.add(parseExpression(ctx.trimCharacterString));
      else
        args.add(new LiteralExpression("", "''"));

      args.add(parseExpression(ctx.trimSource));
    } else {
      // Simple syntax: trim(source)
      args.add(parseExpression(ctx.trimSource));
    }

    return new FunctionCallExpression("trim", args, false);
  }

  /**
   * Parse a normalizeFunction context into a FunctionCallExpression.
   * Handles normalize(string) and normalize(string, normalForm).
   * The normalForm keyword (NFC/NFD/NFKC/NFKD) is passed as a string literal argument.
   */
  Expression parseNormalizeFunction(final Cypher25Parser.NormalizeFunctionContext ctx) {
    final List<Expression> args = new ArrayList<>();
    args.add(parseExpression(ctx.expression()));
    if (ctx.normalForm() != null)
      args.add(new LiteralExpression(ctx.normalForm().getText(), ctx.normalForm().getText()));
    return new FunctionCallExpression("normalize", args, false);
  }

  // ============================================================================
  // Parse Methods
  // ============================================================================

  /**
   * Parse an Expression2 that has postfix operations (property access, list indexing, slicing).
   * Grammar: expression2 : expression1 postFix*
   * PostFix can be:
   * - PropertyPostfix: .property
   * - IndexPostfix: [expression]
   * - RangePostfix: [from..to]
   */
  Expression parseExpression2WithPostfix(final Cypher25Parser.Expression2Context ctx) {
    // Start with the base expression (expression1)
    Expression result = parseExpressionFromText(ctx.expression1());

    // Apply each postfix operation in order
    for (final Cypher25Parser.PostFixContext postfix : ctx.postFix()) {
      if (postfix instanceof Cypher25Parser.PropertyPostfixContext) {
        // Property access: expr.property
        final Cypher25Parser.PropertyPostfixContext propCtx = (Cypher25Parser.PropertyPostfixContext) postfix;
        final String propertyName = CypherASTBuilder.stripBackticks(propCtx.property().propertyKeyName().getText());
        // Create a compound expression: treat result as a variable expression
        result = createPropertyAccessFromExpression(result, propertyName);
      } else if (postfix instanceof Cypher25Parser.IndexPostfixContext) {
        // List indexing: expr[index]
        final Cypher25Parser.IndexPostfixContext indexCtx = (Cypher25Parser.IndexPostfixContext) postfix;
        final Expression indexExpr = parseExpression(indexCtx.expression());
        result = new ListIndexExpression(result, indexExpr);
      } else if (postfix instanceof Cypher25Parser.RangePostfixContext) {
        // Range slicing: expr[from..to]
        final Cypher25Parser.RangePostfixContext rangeCtx = (Cypher25Parser.RangePostfixContext) postfix;
        final Expression fromExpr = rangeCtx.fromExp != null ? parseExpression(rangeCtx.fromExp) : null;
        final Expression toExpr = rangeCtx.toExp != null ? parseExpression(rangeCtx.toExp) : null;
        result = new ListSliceExpression(result, fromExpr, toExpr);
      }
    }

    return result;
  }

  /**
   * Create a property access expression from a base expression and property name.
   * If the base is a VariableExpression, create a PropertyAccessExpression.
   * Otherwise, this is a chained property access (e.g., a.b.c or result[0].property)
   * which needs special handling.
   */
  private Expression createPropertyAccessFromExpression(final Expression baseExpr, final String propertyName) {
    if (baseExpr instanceof VariableExpression) {
      return new PropertyAccessExpression(((VariableExpression) baseExpr).getVariableName(), propertyName);
    }
    // For other base expressions (like list[index]), we need to wrap in a property access
    // that evaluates the base expression first, then accesses the property
    return new ChainedPropertyAccessExpression(baseExpr, propertyName);
  }

  /**
   * Parse a list literal context into a ListExpression.
   * Example: [1, 2, 3] or ['a', 'b', 'c']
   */
  ListExpression parseListLiteral(final Cypher25Parser.ListLiteralContext ctx) {
    final List<Expression> elements = new ArrayList<>();

    // Parse each expression in the list
    if (ctx.expression() != null) {
      for (final Cypher25Parser.ExpressionContext exprCtx : ctx.expression()) {
        elements.add(parseExpression(exprCtx));
      }
    }

    return new ListExpression(elements, ctx.getText());
  }

  /**
   * Parse a function invocation into an Expression.
   * <p>
   * Most invocations produce a {@link FunctionCallExpression}. The one exception is the deprecated
   * function-style pattern predicate {@code exists((a)-[:R]->(b))}: when {@code exists} is applied to a
   * single graph pattern argument it must behave like the {@code EXISTS { ... }} subquery (test whether
   * the pattern matches), not like the property {@code exists()} function (a non-null check). Without this
   * the pattern argument evaluates to a non-null value and {@code exists()} always returns true, which
   * silently breaks conditions such as {@code CASE WHEN exists((u)-[:R]->()) ... } (issue #4993).
   */
  Expression parseFunctionInvocation(final Cypher25Parser.FunctionInvocationContext ctx) {
    final String functionName = ParserUtils.qualifiedName(ctx.functionName().namespace(), ctx.functionName().symbolicNameString());
    final boolean distinct = ctx.DISTINCT() != null;
    final List<Expression> arguments = new ArrayList<>();

    // Function-style pattern predicate: exists((a)-[:R]->(b)) -> pattern existence check.
    if (!distinct && "exists".equalsIgnoreCase(functionName) && ctx.functionArgument().size() == 1) {
      final Cypher25Parser.ExpressionContext argExpr = ctx.functionArgument(0).expression();
      final Cypher25Parser.PatternExpressionContext patternExpr = ParseTreeSearch.findPatternExpressionRecursive(argExpr);
      if (patternExpr != null)
        return parsePatternPredicate(patternExpr);
    }

    // Parse arguments
    for (final Cypher25Parser.FunctionArgumentContext argCtx : ctx.functionArgument()) {
      final String argText = argCtx.expression().getText();

      // Special handling for asterisk (though count(*) is typically handled by CountStarContext)
      if ("*".equals(argText)) {
        arguments.add(new StarExpression());
      } else {
        arguments.add(parseExpression(argCtx.expression()));
      }
    }

    return new FunctionCallExpression(functionName, arguments, distinct);
  }

  /**
   * A pattern expression where a boolean is expected (a CASE WHEN condition, the WHERE of a comprehension or of a list
   * predicate, the argument of {@code exists()}): true when the pattern has a match. The parser refuses a pattern expression
   * of a WHERE in a position that is not a boolean (issue #11888).
   */
  Expression parsePatternPredicate(final Cypher25Parser.PatternExpressionContext ctx) {
    final PathPattern pathPattern = patterns.parsePathPatternNonEmpty(ctx.pathPatternNonEmpty());
    return new BooleanWrapperExpression(new PatternPredicateExpression(pathPattern, false, CypherASTBuilder.getOriginalText(ctx)));
  }

  /**
   * Parse a simple CASE expression (no case value).
   * Example: CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END
   */
  CaseExpression parseCaseExpression(final Cypher25Parser.CaseExpressionContext ctx) {
    final List<CaseAlternative> alternatives = new ArrayList<>();

    // Parse each WHEN...THEN alternative
    for (final Cypher25Parser.CaseAlternativeContext altCtx : ctx.caseAlternative()) {
      final Expression whenExpr = parseExpression(altCtx.expression(0)); // First expression is WHEN
      // A list literal cannot be coerced to a boolean (issue #12975)
      BooleanOperandValidator.refuseListPredicate(whenExpr);
      final Expression thenExpr = parseExpression(altCtx.expression(1)); // Second expression is THEN
      alternatives.add(new CaseAlternative(whenExpr, thenExpr));
    }

    // Parse optional ELSE clause
    Expression elseExpr = null;
    if (ctx.expression() != null) {
      elseExpr = parseExpression(ctx.expression());
    }

    return new CaseExpression(alternatives, elseExpr);
  }

  /**
   * Builds the predicate a comparison-form {@code WHEN} stands for by parsing {@code <operand placeholder> <when text>}
   * with its whitespace intact, so that every comparison the grammar allows behaves exactly as it does written out in
   * full, over an operand that is evaluated once.
   */
  private Expression parseOperandPredicate(final CaseOperandExpression operandHolder,
      final Cypher25Parser.ExtendedWhenContext whenCtx) {
    final String text = CaseOperandExpression.PLACEHOLDER + " " + CypherASTBuilder.getOriginalText(whenCtx);
    final CypherErrorListener errorListener = new CypherErrorListener();
    final Cypher25Lexer lexer = new Cypher25Lexer(CharStreams.fromString(text));
    lexer.removeErrorListeners();
    lexer.addErrorListener(errorListener);
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final Cypher25Parser parser = new Cypher25Parser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(errorListener);
    // The text is the operand placeholder plus a WHEN form that already passed the depth guard in the original query,
    // so this small parse needs none of its own. The placeholder resolves to the shared holder while it is parsed.
    final CaseOperandExpression previous = activeCaseOperand;
    activeCaseOperand = operandHolder;
    try {
      final Cypher25Parser.ExpressionContext predicate = parser.expression();
      if (tokens.LA(1) != Token.EOF)
        throw new CommandParsingException("Unexpected input in CASE WHEN: " + text);
      return parseExpression(predicate);
    } finally {
      activeCaseOperand = previous;
    }
  }

  /**
   * Parse an extended CASE expression (with case value).
   * Example: CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END
   */
  CaseExpression parseExtendedCaseExpression(final Cypher25Parser.ExtendedCaseExpressionContext ctx) {
    // Parse the case expression (the value being tested)
    final Expression caseExpr = parseExpression(ctx.expression(0));
    final CaseOperandExpression operandHolder = new CaseOperandExpression();
    boolean usesOperandHolder = false;

    final List<CaseAlternative> alternatives = new ArrayList<>();

    // Parse each WHEN...THEN alternative
    for (final Cypher25Parser.ExtendedCaseAlternativeContext altCtx : ctx.extendedCaseAlternative()) {
      // In extended form, WHEN contains one or more comma-separated values to match against: each one becomes its own
      // alternative sharing the same THEN expression (evaluated lazily, so only the matching branch runs).
      final Expression thenExpr = parseExpression(altCtx.expression());
      for (final Cypher25Parser.ExtendedWhenContext whenCtx : altCtx.extendedWhen()) {
        // A plain value is parsed from its parse tree: re-parsing its whitespace-less text turned a list or map literal
        // into something that never equals the operand (issue #8996).
        if (whenCtx instanceof Cypher25Parser.WhenEqualsContext equalsCtx)
          alternatives.add(new CaseAlternative(parseExpression(equalsCtx.expression()), thenExpr));
        else {
          // A comparison form (WHEN > 5, WHEN IS NULL, WHEN STARTS WITH 'a') is the predicate "<operand> <form>" over
          // the operand evaluated once
          alternatives.add(new CaseAlternative(parseOperandPredicate(operandHolder, whenCtx), thenExpr,
              CypherASTBuilder.getOriginalText(whenCtx)));
          usesOperandHolder = true;
        }
      }
    }

    // Parse optional ELSE clause
    Expression elseExpr = null;
    if (ctx.elseExp != null) {
      elseExpr = parseExpression(ctx.elseExp);
    }

    return new CaseExpression(caseExpr, alternatives, elseExpr, usesOperandHolder ? operandHolder : null);
  }

  /**
   * Parse a comparison expression from Expression8Context.
   * Handles: <, >, <=, >=, =, !=
   */
  Expression parseComparisonFromExpression8(final Cypher25Parser.Expression8Context ctx) {
    // Expression8 has multiple expression7 children with comparison operators between them
    if (ctx.expression7().size() > 1) {
      // Collect all operators, in textual order, for chained comparisons (issue #5284).
      // Per the OpenCypher spec a chain like "a < b < c" is equivalent to "a < b AND b < c":
      // every adjacent comparison must hold, not only the first one.
      final List<ComparisonExpression.Operator> operators = new ArrayList<>();
      for (int i = 1; i < ctx.getChildCount(); i++) {
        if (ctx.getChild(i) instanceof TerminalNode) {
          final TerminalNode terminal = (TerminalNode) ctx.getChild(i);
          final int type = terminal.getSymbol().getType();

          ComparisonExpression.Operator op = null;
          if (type == Cypher25Parser.EQ) op = ComparisonExpression.Operator.EQUALS;
          else if (type == Cypher25Parser.NEQ || type == Cypher25Parser.INVALID_NEQ)
            op = ComparisonExpression.Operator.NOT_EQUALS;
          else if (type == Cypher25Parser.LT) op = ComparisonExpression.Operator.LESS_THAN;
          else if (type == Cypher25Parser.GT) op = ComparisonExpression.Operator.GREATER_THAN;
          else if (type == Cypher25Parser.LE) op = ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
          else if (type == Cypher25Parser.GE) op = ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;

          if (op != null)
            operators.add(op);
        }
      }

      if (!operators.isEmpty()) {
        BooleanExpression result = null;
        for (int i = 0; i < operators.size(); i++) {
          final Expression left = parseExpressionFromText(ctx.expression7(i));
          final Expression right = parseExpressionFromText(ctx.expression7(i + 1));
          final ComparisonExpression comparison = new ComparisonExpression(left, operators.get(i), right);
          result = result == null ? comparison : new LogicalExpression(LogicalExpression.Operator.AND, result, comparison);
        }
        // Wrap the (possibly conjoined) BooleanExpression in an Expression adapter
        return new BooleanWrapperExpression(result);
      }
    }

    // No comparison found, parse as text
    return parseExpressionText(ctx.getText());
  }

  /**
   * Parse IS NULL / IS NOT NULL expression and wrap it for use in Expression contexts.
   */
  Expression parseIsNullExpression(final Cypher25Parser.NullComparisonContext ctx) {
    // Get the parent expression7 context to find the left side
    ParseTree parent = ctx.getParent();
    if (parent instanceof Cypher25Parser.ComparisonExpression6Context) {
      parent = parent.getParent(); // Get expression7
    }

    if (parent instanceof Cypher25Parser.Expression7Context) {
      final Cypher25Parser.Expression7Context expr7 = (Cypher25Parser.Expression7Context) parent;
      final Expression leftExpr = parseExpressionFromText(expr7.expression6());
      final boolean isNot = ctx.NOT() != null;
      final IsNullExpression isNullExpr = new IsNullExpression(leftExpr, isNot);
      return new BooleanWrapperExpression(isNullExpr);
    }

    // Fallback: parse as text
    return parseExpressionText(ctx.getText());
  }

  /**
   * Parse {@code IS [NOT] TYPED type} or {@code :: type} expression and wrap it for use in
   * Expression contexts (issue #3365 section 3.3). The type-spec extraction is shared with
   * {@link TypePredicateBuilder#buildIsTypedExpression}.
   */
  Expression parseIsTypedExpression(final Cypher25Parser.TypeComparisonContext ctx) {
    ParseTree parent = ctx.getParent();
    if (parent instanceof Cypher25Parser.ComparisonExpression6Context)
      parent = parent.getParent();

    if (parent instanceof Cypher25Parser.Expression7Context) {
      final Cypher25Parser.Expression7Context expr7 = (Cypher25Parser.Expression7Context) parent;
      final Expression leftExpr = parseExpressionFromText(expr7.expression6());
      return new BooleanWrapperExpression(TypePredicateBuilder.buildIsTypedExpression(leftExpr, ctx));
    }
    return parseExpressionText(ctx.getText());
  }

  /**
   * Parse {@code IS [NOT] [form] NORMALIZED} and wrap it for use in Expression contexts (issue #9940). The predicate itself is
   * built by {@link TypePredicateBuilder#buildIsNormalizedExpression}, shared with the boolean path.
   */
  Expression parseIsNormalizedExpression(final Cypher25Parser.NormalFormComparisonContext ctx) {
    final Cypher25Parser.Expression7Context expr7 = (Cypher25Parser.Expression7Context) ctx.getParent();
    return new BooleanWrapperExpression(
        TypePredicateBuilder.buildIsNormalizedExpression(parseExpressionFromText(expr7.expression6()), ctx));
  }

  /**
   * Parse string/list comparison from Expression7Context that has a comparisonExpression6.
   * Handles STARTS WITH, ENDS WITH, CONTAINS, =~ (regex), and IN operators
   * when they appear in expression positions (e.g., RETURN expressions).
   */
  Expression parseStringComparisonExpression(final Cypher25Parser.Expression7Context ctx) {
    final Cypher25Parser.ComparisonExpression6Context compCtx = ctx.comparisonExpression6();
    final Expression leftExpr = parseExpressionFromText(ctx.expression6());

    if (compCtx instanceof Cypher25Parser.StringAndListComparisonContext) {
      final Cypher25Parser.StringAndListComparisonContext strCtx =
          (Cypher25Parser.StringAndListComparisonContext) compCtx;

      if (strCtx.STARTS() != null && strCtx.WITH() != null) {
        final Expression pattern = parseExpressionFromText(strCtx.expression6());
        return new BooleanWrapperExpression(new StringMatchExpression(leftExpr, pattern, StringMatchExpression.MatchType.STARTS_WITH));
      }

      if (strCtx.ENDS() != null && strCtx.WITH() != null) {
        final Expression pattern = parseExpressionFromText(strCtx.expression6());
        return new BooleanWrapperExpression(new StringMatchExpression(leftExpr, pattern, StringMatchExpression.MatchType.ENDS_WITH));
      }

      if (strCtx.CONTAINS() != null) {
        final Expression pattern = parseExpressionFromText(strCtx.expression6());
        return new BooleanWrapperExpression(new StringMatchExpression(leftExpr, pattern, StringMatchExpression.MatchType.CONTAINS));
      }

      if (strCtx.REGEQ() != null) {
        final Expression pattern = parseExpressionFromText(strCtx.expression6());
        return new BooleanWrapperExpression(new RegexExpression(leftExpr, pattern));
      }

      if (strCtx.IN() != null) {
        // Parse the right side as a single expression (not a list literal)
        // so that expressions like [3]+4 are correctly handled as arithmetic
        final Expression rhsExpr = parseExpressionFromText(strCtx.expression6());
        final List<Expression> listItems = new ArrayList<>();
        listItems.add(rhsExpr);
        return new BooleanWrapperExpression(new InExpression(leftExpr, listItems));
      }
    }

    // Fallback: parse as text
    return parseExpressionText(ctx.getText());
  }

  /**
   * Parse a label comparison expression from Expression7Context (e.g., a:B, n:A:B, n:Person|Developer).
   * Returns a BooleanWrapperExpression wrapping a LabelCheckExpression.
   */
  Expression parseLabelComparisonExpression(final Cypher25Parser.Expression7Context ctx) {
    final Expression leftExpr = parseExpressionFromText(ctx.expression6());
    final Cypher25Parser.LabelComparisonContext labelCtx = (Cypher25Parser.LabelComparisonContext) ctx.comparisonExpression6();
    final Cypher25Parser.LabelExpressionContext labelExprCtx = labelCtx.labelExpression();
    TypePredicateBuilder.rejectDirectedPredicate(labelExprCtx);

    // A negation, the wildcard or a mix of '&' and '|' is evaluated as the tree it is (issue #8992).
    final LabelPredicate predicate = ParserUtils.buildLabelPredicate(labelExprCtx, this::parseExpression);
    if (predicate != null)
      return new BooleanWrapperExpression(new LabelCheckExpression(leftExpr, predicate, ctx.getText()));

    // See parseLabelCheckExpression in CypherASTBuilder: extract through the grammar so backticks
    // are stripped and a quoted label is never split on a character of its own name. This also
    // drops the ':'/'IS' prefix at the token level, so both spellings land on the same label.
    final List<String> labels = ParserUtils.extractLabels(labelExprCtx);
    final LabelCheckExpression.LabelOperator operator = ParserUtils.isLabelDisjunction(labelExprCtx) ?
        LabelCheckExpression.LabelOperator.OR :
        LabelCheckExpression.LabelOperator.AND;

    // The dynamic $(expr) labels are evaluated per row with the static ones; dropped, the check was always true (issue #11776)
    return new BooleanWrapperExpression(new LabelCheckExpression(leftExpr, labels,
        ParserUtils.parseDynamicLabels(labelExprCtx, this::parseExpression), operator, ctx.getText()));
  }

  // ============================================================================
  // Arithmetic Expression Parsing
  // ============================================================================

  /**
   * Parse an arithmetic expression from Expression6Context (handles + - ||)
   */
  Expression parseArithmeticExpression6(final Cypher25Parser.Expression6Context ctx) {
    final List<Cypher25Parser.Expression5Context> operands = ctx.expression5();
    if (operands.size() == 1)
      return parseArithmeticExpression5(operands.get(0));

    // Build left-associative expression tree
    Expression result = parseArithmeticExpression5(operands.get(0));

    int operandIndex = 1;
    for (int i = 0; i < ctx.getChildCount() && operandIndex < operands.size(); i++) {
      if (ctx.getChild(i) instanceof TerminalNode) {
        final TerminalNode terminal = (TerminalNode) ctx.getChild(i);
        final int type = terminal.getSymbol().getType();

        ArithmeticExpression.Operator op = null;
        if (type == Cypher25Parser.PLUS)
          op = ArithmeticExpression.Operator.ADD;
        else if (type == Cypher25Parser.MINUS)
          op = ArithmeticExpression.Operator.SUBTRACT;
        else if (type == Cypher25Parser.DOUBLEBAR)
          // Cypher 25 / GQL concatenation operator: STRING||STRING or LIST||LIST only, no implicit
          // coercion of non-STRING operands (issue #5298). Distinct from + which appends/coerces.
          op = ArithmeticExpression.Operator.CONCAT;

        if (op != null) {
          final Expression right = parseArithmeticExpression5(operands.get(operandIndex));
          result = new ArithmeticExpression(result, op, right);
          operandIndex++;
        }
      }
    }

    return result;
  }

  /**
   * Parse an arithmetic expression from Expression5Context (handles * / %)
   */
  Expression parseArithmeticExpression5(final Cypher25Parser.Expression5Context ctx) {
    final List<Cypher25Parser.Expression4Context> operands = ctx.expression4();
    if (operands.size() == 1)
      return parseArithmeticExpression4(operands.get(0));

    // Build left-associative expression tree
    Expression result = parseArithmeticExpression4(operands.get(0));

    int operandIndex = 1;
    for (int i = 0; i < ctx.getChildCount() && operandIndex < operands.size(); i++) {
      if (ctx.getChild(i) instanceof TerminalNode) {
        final TerminalNode terminal = (TerminalNode) ctx.getChild(i);
        final int type = terminal.getSymbol().getType();

        ArithmeticExpression.Operator op = null;
        if (type == Cypher25Parser.TIMES)
          op = ArithmeticExpression.Operator.MULTIPLY;
        else if (type == Cypher25Parser.DIVIDE)
          op = ArithmeticExpression.Operator.DIVIDE;
        else if (type == Cypher25Parser.PERCENT)
          op = ArithmeticExpression.Operator.MODULO;

        if (op != null) {
          final Expression right = parseArithmeticExpression4(operands.get(operandIndex));
          result = new ArithmeticExpression(result, op, right);
          operandIndex++;
        }
      }
    }

    return result;
  }

  /**
   * Parse an arithmetic expression from Expression4Context (handles ^/POW)
   */
  Expression parseArithmeticExpression4(final Cypher25Parser.Expression4Context ctx) {
    final List<Cypher25Parser.Expression3Context> operands = ctx.expression3();
    if (operands.size() == 1)
      return parseArithmeticExpression3(operands.get(0));

    // Build left-associative expression tree for power (a ^ b ^ c = (a ^ b) ^ c)
    Expression result = parseArithmeticExpression3(operands.get(0));
    for (int i = 1; i < operands.size(); i++) {
      final Expression right = parseArithmeticExpression3(operands.get(i));
      result = new ArithmeticExpression(result, ArithmeticExpression.Operator.POWER, right);
    }

    return result;
  }

  /**
   * Parse an arithmetic expression from Expression3Context (handles unary + -)
   */
  Expression parseArithmeticExpression3(final Cypher25Parser.Expression3Context ctx) {
    // Check for unary plus/minus
    if (ctx.getChildCount() > 1) {
      final ParseTree firstChild = ctx.getChild(0);
      if (firstChild instanceof TerminalNode) {
        final TerminalNode terminal = (TerminalNode) firstChild;
        final int type = terminal.getSymbol().getType();
        if (type == Cypher25Parser.MINUS) {
          // Unary minus: -expression
          final Expression inner = parseExpressionFromText(ctx.expression2());
          return new ArithmeticExpression(new LiteralExpression(0L, "0"), ArithmeticExpression.Operator.SUBTRACT, inner,
              ctx.getText());
        }
        // Unary plus is a no-op
        if (type == Cypher25Parser.PLUS)
          return parseExpressionFromText(ctx.expression2());
      }
    }

    return parseExpressionFromText(ctx.expression2());
  }

  // ============================================================================
  // Map Literal Parsing
  // ============================================================================

  /**
   * Parse a map literal into a MapExpression.
   * Example: {name: 'Alice', age: 30}
   */
  MapExpression parseMapLiteralExpression(final Cypher25Parser.MapContext ctx) {
    final Map<String, Expression> entries = new LinkedHashMap<>();

    final List<Cypher25Parser.PropertyKeyNameContext> keys = ctx.propertyKeyName();
    final List<Cypher25Parser.ExpressionContext> values = ctx.expression();

    for (int i = 0; i < keys.size() && i < values.size(); i++) {
      final String key = CypherASTBuilder.stripBackticks(keys.get(i).getText());
      final Expression valueExpr = parseExpression(values.get(i));
      entries.put(key, valueExpr);
    }

    return new MapExpression(entries, ctx.getText());
  }

  // ============================================================================
  // List Comprehension Parsing
  // ============================================================================

  /**
   * Parse a list comprehension into a ListComprehensionExpression.
   * Syntax: [variable IN listExpression WHERE filterExpression | mapExpression]
   * Examples: [x IN [1,2,3] | x * 2], [x IN list WHERE x > 5 | x.name]
   */
  ListComprehensionExpression parseListComprehension(final Cypher25Parser.ListComprehensionContext ctx) {
    final String variable = CypherASTBuilder.stripBackticks(ctx.variable().getText());

    // The main expression after IN - it's the first expression in the list
    final List<Cypher25Parser.ExpressionContext> expressions = ctx.expression();
    final Expression listExpression = parseExpression(expressions.get(0));

    // Optional WHERE clause
    Expression whereExpression = null;
    if (ctx.whereExp != null) {
      whereExpression = parseExpression(ctx.whereExp);
      // A list literal cannot be coerced to a boolean (issue #12975)
      BooleanOperandValidator.refuseListPredicate(whereExpression);
    }

    // Optional mapping expression after |
    Expression mapExpression = null;
    if (ctx.barExp != null)
      mapExpression = parseExpression(ctx.barExp);

    return new ListComprehensionExpression(variable, listExpression, whereExpression, mapExpression, ctx.getText());
  }

  // ============================================================================
  // Pattern Comprehension Parsing
  // ============================================================================

  /**
   * Parse a pattern comprehension into a PatternComprehensionExpression.
   * Syntax: [(variable =)? pathPattern (WHERE filterExpression)? | mapExpression]
   * Examples: [(a)-->(friend) WHERE friend.name <> 'B' | friend.name]
   */
  PatternComprehensionExpression parsePatternComprehension(final Cypher25Parser.PatternComprehensionContext ctx) {
    // Optional path variable
    String pathVariable = null;
    if (ctx.variable() != null)
      pathVariable = CypherASTBuilder.stripBackticks(ctx.variable().getText());

    // Parse the path pattern
    final PathPattern pathPattern = patterns.parsePathPatternNonEmpty(ctx.pathPatternNonEmpty());

    // Optional WHERE clause
    Expression whereExpression = null;
    if (ctx.whereExp != null) {
      whereExpression = parseExpression(ctx.whereExp);
      // A list literal cannot be coerced to a boolean (issue #12975)
      BooleanOperandValidator.refuseListPredicate(whereExpression);
    }

    // Required mapping expression after |
    final Expression mapExpression = parseExpression(ctx.barExp);

    return new PatternComprehensionExpression(pathVariable, pathPattern, whereExpression, mapExpression, ctx.getText());
  }

  // ============================================================================
  // List Predicate Parsing
  // ============================================================================

  /**
   * Parse a list items predicate into a ListPredicateExpression.
   * Syntax: (ALL|ANY|NONE|SINGLE)(variable IN listExpression WHERE filterExpression)
   * Examples: all(x IN [1,2,3] WHERE x > 0), any(x IN list WHERE x = 4)
   */
  ListPredicateExpression parseListItemsPredicate(final Cypher25Parser.ListItemsPredicateContext ctx) {
    final ListPredicateExpression.PredicateType predicateType;
    if (ctx.ALL() != null)
      predicateType = ListPredicateExpression.PredicateType.ALL;
    else if (ctx.ANY() != null)
      predicateType = ListPredicateExpression.PredicateType.ANY;
    else if (ctx.NONE() != null)
      predicateType = ListPredicateExpression.PredicateType.NONE;
    else if (ctx.SINGLE() != null)
      predicateType = ListPredicateExpression.PredicateType.SINGLE;
    else
      throw new IllegalStateException("Unknown list predicate type: " + ctx.getText());

    final String variable = CypherASTBuilder.stripBackticks(ctx.variable().getText());
    final Expression listExpression = parseExpression(ctx.inExp);

    Expression whereExpression = null;
    if (ctx.whereExp != null) {
      whereExpression = parseExpression(ctx.whereExp);
      // A list literal cannot be coerced to a boolean (issue #12975)
      BooleanOperandValidator.refuseListPredicate(whereExpression);
    }

    return new ListPredicateExpression(predicateType, variable, listExpression, whereExpression, ctx.getText());
  }

  // ============================================================================
  // Reduce Expression Parsing
  // ============================================================================

  /**
   * Parse a reduce expression into a ReduceExpression.
   * Syntax: reduce(accumulator = initial, variable IN list | expression)
   * Examples: reduce(total = 0, n IN [1,2,3] | total + n) -> 6
   */
  ReduceExpression parseReduceExpression(final Cypher25Parser.ReduceExpressionContext ctx) {
    // Grammar: REDUCE LPAREN variable EQ expression COMMA variable IN expression BAR expression RPAREN
    // The grammar gives us: variable(0) = expression(0), variable(1) IN expression(1) BAR expression(2)
    final List<Cypher25Parser.VariableContext> variables = ctx.variable();
    final List<Cypher25Parser.ExpressionContext> expressions = ctx.expression();

    final String accumulatorVariable = CypherASTBuilder.stripBackticks(variables.get(0).getText());
    final Expression initialValue = parseExpression(expressions.get(0));

    final String iteratorVariable = CypherASTBuilder.stripBackticks(variables.get(1).getText());
    final Expression listExpression = parseExpression(expressions.get(1));

    final Expression reduceExpression = parseExpression(expressions.get(2));

    return new ReduceExpression(accumulatorVariable, initialValue, iteratorVariable,
        listExpression, reduceExpression, ctx.getText());
  }

  /**
   * Parse an allReduce expression into an AllReduceExpression.
   * Syntax: allReduce(accumulator = initial, variable IN list | expression, predicate)
   */
  AllReduceExpression parseAllReduceExpression(final Cypher25Parser.AllReduceExpressionContext ctx) {
    if (ctx.allReduceExpressionValidArguments() != null) {
      final Cypher25Parser.AllReduceExpressionValidArgumentsContext validCtx = ctx.allReduceExpressionValidArguments();
      final List<Cypher25Parser.VariableContext> variables = validCtx.variable();
      final List<Cypher25Parser.ExpressionContext> expressions = validCtx.expression();

      final String accumulatorVariable = CypherASTBuilder.stripBackticks(variables.get(0).getText());
      final Expression initialValue = parseExpression(expressions.get(0));

      final String iteratorVariable = CypherASTBuilder.stripBackticks(variables.get(1).getText());
      final Expression listExpression = parseExpression(expressions.get(1));

      final Expression reduceExpression = parseExpression(expressions.get(2));
      final Expression predicateExpression = parseExpression(expressions.get(3));
      // A list literal cannot be coerced to a boolean (issue #12975)
      BooleanOperandValidator.refuseListPredicate(predicateExpression);

      return new AllReduceExpression(accumulatorVariable, initialValue, iteratorVariable,
          listExpression, reduceExpression, predicateExpression, ctx.getText());
    }

    throw new IllegalArgumentException("Invalid allReduce() syntax: " + ctx.getText());
  }

  // ============================================================================
  // Shortest Path Expression Parsing
  // ============================================================================

  // ============================================================================
  // Map Projection Parsing
  // ============================================================================

  /**
   * Parse a map projection into a MapProjectionExpression.
   * Syntax: variable{.property1, .property2, key: expression, .*}
   * Examples: n{.name, .age}, n{.*, totalAge: n.age * 2}
   */
  MapProjectionExpression parseMapProjection(final Cypher25Parser.MapProjectionContext ctx) {
    final String variableName = CypherASTBuilder.stripBackticks(ctx.variable().getText());
    final List<MapProjectionExpression.ProjectionElement> elements = new ArrayList<>();

    for (final Cypher25Parser.MapProjectionElementContext elemCtx : ctx.mapProjectionElement()) {
      if (elemCtx.propertyKeyName() != null && elemCtx.expression() != null) {
        // key: expression
        final String key = CypherASTBuilder.stripBackticks(elemCtx.propertyKeyName().getText());
        final Expression expr = parseExpression(elemCtx.expression());
        elements.add(new MapProjectionExpression.ProjectionElement(key, expr));
      } else if (elemCtx.property() != null) {
        // .propertyName
        final String propName = CypherASTBuilder.stripBackticks(elemCtx.property().propertyKeyName().getText());
        elements.add(new MapProjectionExpression.ProjectionElement(propName));
      } else if (elemCtx.variable() != null) {
        // variable (include another variable's value)
        final String varName = CypherASTBuilder.stripBackticks(elemCtx.variable().getText());
        elements.add(new MapProjectionExpression.ProjectionElement(varName, new VariableExpression(varName)));
      } else if (elemCtx.DOT() != null && elemCtx.TIMES() != null) {
        // .* (all properties)
        elements.add(new MapProjectionExpression.ProjectionElement(true));
      }
    }

    return new MapProjectionExpression(variableName, elements, ctx.getText());
  }

  // ============================================================================
  // Utility Methods
  // ============================================================================

  /**
   * Find an operator outside of parentheses in the given text.
   * Delegates to ParserUtils for implementation.
   */
  int findOperatorOutsideParentheses(final String text, final String op) {
    return ParserUtils.findOperatorOutsideParentheses(text, op);
  }

  /**
   * Parse vector(values, dimension, type) into a FunctionCallExpression.
   * Delegates to the "vector.create" Cypher function which converts a list to float[].
   */
  Expression parseVectorFunction(final Cypher25Parser.VectorFunctionContext ctx) {
    final List<Expression> args = new ArrayList<>();
    args.add(parseExpression(ctx.vectorValue));
    args.add(parseExpression(ctx.dimension));
    // the coordinate type, in its canonical spelling, decides the array the vector is built as (#10917)
    final StringBuilder spelling = new StringBuilder();
    for (int i = 0; i < ctx.vectorCoordinateType().getChildCount(); i++) {
      if (ctx.vectorCoordinateType().getChild(i) instanceof Cypher25Parser.TypeNullabilityContext)
        continue;
      if (!spelling.isEmpty())
        spelling.append(' ');
      spelling.append(ctx.vectorCoordinateType().getChild(i).getText().toUpperCase(Locale.ROOT));
    }
    final String coordinateType = CypherTypeName.canonical(spelling.toString());
    args.add(new LiteralExpression(coordinateType, coordinateType));
    return new FunctionCallExpression("vector.create", args, false);
  }

  /**
   * Parse vector_norm(vector, metric) into a FunctionCallExpression.
   * Maps EUCLIDEAN to SQL vector.magnitude, MANHATTAN to SQL vector.l1Norm.
   */
  Expression parseVectorNormFunction(final Cypher25Parser.VectorNormFunctionContext ctx) {
    final String metric = ctx.vectorNormDistanceMetric().getText().toUpperCase(Locale.ROOT);
    final String functionName = switch (metric) {
      case "EUCLIDEAN" -> "vector.magnitude";
      case "MANHATTAN" -> "vector.l1Norm";
      default -> throw new CommandParsingException("Unsupported vector_norm metric: " + metric);
    };

    final List<Expression> args = new ArrayList<>();
    args.add(parseExpression(ctx.vectorValue));
    return new FunctionCallExpression(functionName, args, false);
  }

  /**
   * Parse vector_distance(v1, v2, metric) into a FunctionCallExpression.
   * Reuses SQL vector functions where available, falls back to Cypher-specific for others.
   */
  Expression parseVectorDistanceFunction(final Cypher25Parser.VectorDistanceFunctionContext ctx) {
    final String metric = ctx.vectorDistanceMetric().getText().toUpperCase(Locale.ROOT);

    final List<Expression> args = new ArrayList<>();
    args.add(parseExpression(ctx.vector1));
    args.add(parseExpression(ctx.vector2));

    final String functionName = switch (metric) {
      case "EUCLIDEAN" -> "vector.l2Distance";
      case "MANHATTAN" -> "vector.distance.manhattan";
      case "COSINE" -> "vector.distance.cosine";
      case "DOT" -> "vector.dotProduct";
      case "EUCLIDEAN_SQUARED", "HAMMING" -> {
        args.add(new LiteralExpression(metric, "'" + metric + "'"));
        yield "vector.distance";
      }
      default -> throw new CommandParsingException("Unsupported vector_distance metric: " + metric);
    };

    return new FunctionCallExpression(functionName, args, false);
  }
}
