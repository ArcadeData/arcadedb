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

import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.ast.*;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;
import java.util.Map;

/**
 * Handles expression parsing for the Cypher AST builder.
 * Extracts expression-related visitor methods from CypherASTBuilder for better separation of concerns.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
class CypherExpressionBuilder {

  private final ExpressionTypeDetector detector = new ExpressionTypeDetector(this);

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
    final String text = ctx.getText();

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
    final Cypher25Parser.Expression6Context topArith6 = findTopLevelExpression6(ctx);
    if (topArith6 != null)
      return parseArithmeticExpression6(topArith6);

    // Check for TOP-LEVEL multiplicative (*, /, %) using spine walk
    final Cypher25Parser.Expression5Context topArith5 = findTopLevelExpression5(ctx);
    if (topArith5 != null)
      return parseArithmeticExpression5(topArith5);

    // Check for TOP-LEVEL exponentiation (^) using spine walk
    final Cypher25Parser.Expression4Context topArith4 = findTopLevelExpression4(ctx);
    if (topArith4 != null)
      return parseArithmeticExpression4(topArith4);

    // Check for TOP-LEVEL unary operator (-, +) using spine walk
    final Cypher25Parser.Expression3Context topExpr3 = findTopLevelExpression3(ctx);
    if (topExpr3 != null)
      return parseArithmeticExpression3(topExpr3);

    // Check for TOP-LEVEL postfix expressions (property access, list indexing, slicing)
    // using spine walk. Must be before list/map literals because [1,2,3][0] should be
    // postfix indexing, not just [1,2,3]
    final Cypher25Parser.Expression2Context topExpr2 = findTopLevelExpression2(ctx);
    if (topExpr2 != null && !topExpr2.postFix().isEmpty())
      return parseExpression2WithPostfix(topExpr2);

    // Check for list literals BEFORE function invocations when the top-level expression
    // is a list. Otherwise, [date({...}), date({...})] would be parsed as a single date()
    // function call because findFunctionInvocationRecursive finds date() inside the list.
    // For cases like tail([1,2,3]), the function invocation is at the top level and will
    // be caught by the postfix/expression2 checks above.
    final Cypher25Parser.ListLiteralContext listCtx = findListLiteralRecursive(ctx);
    if (listCtx != null) {
      // Only use list literal if it's at the top level (i.e. the list text covers most of the expression)
      final String listText = listCtx.getText();
      if (listText.length() >= text.length() - 2) // Allow for whitespace
        return parseListLiteral(listCtx);
    }

    // Check for function invocations (after top-level list check)
    // (tail([1,2,3]) should be parsed as a function call, not as a list literal)
    final Cypher25Parser.FunctionInvocationContext funcCtx = findFunctionInvocationRecursive(ctx);
    if (funcCtx != null) {
      return parseFunctionInvocation(funcCtx);
    }

    // Check for list literals (fallback for other cases)
    if (listCtx != null) {
      return parseListLiteral(listCtx);
    }

    // Check for map projections
    final Cypher25Parser.MapProjectionContext mapProjCtx = findMapProjectionRecursive(ctx);
    if (mapProjCtx != null) {
      return parseMapProjection(mapProjCtx);
    }

    // Check for map literals
    final Cypher25Parser.MapContext mapCtx = findMapRecursive(ctx);
    if (mapCtx != null) {
      return parseMapLiteralExpression(mapCtx);
    }

    // Fallback: recursive arithmetic expression checks
    final Cypher25Parser.Expression6Context arith6Ctx = findArithmeticExpression6Recursive(ctx);
    if (arith6Ctx != null) {
      return parseArithmeticExpression6(arith6Ctx);
    }
    final Cypher25Parser.Expression5Context arith5Ctx = findArithmeticExpression5Recursive(ctx);
    if (arith5Ctx != null) {
      return parseArithmeticExpression5(arith5Ctx);
    }

    // Fallback: recursive postfix expression check
    final Cypher25Parser.Expression2Context expr2Ctx = findExpression2Recursive(ctx);
    if (expr2Ctx != null && !expr2Ctx.postFix().isEmpty()) {
      return parseExpression2WithPostfix(expr2Ctx);
    }

    // Check for parenthesized expressions that may contain logical operators
    // e.g., (true AND null), (a OR b), (NOT x)
    final Cypher25Parser.ParenthesizedExpressionContext parenCtx = findParenthesizedExpressionRecursive(ctx);
    if (parenCtx != null)
      return parseExpression(parenCtx.expression());

    // Use the shared text parsing logic
    return parseExpressionText(text);
  }

  /**
   * Parse any parse tree node as an expression using its text.
   * This is a helper for parsing lower-level expression contexts.
   */
  Expression parseExpressionFromText(final ParseTree node) {
    // Check for CASE expressions in the parse tree
    final Cypher25Parser.CaseExpressionContext caseCtx = findCaseExpressionRecursive(node);
    if (caseCtx != null)
      return parseCaseExpression(caseCtx);

    final Cypher25Parser.ExtendedCaseExpressionContext extCaseCtx = findExtendedCaseExpressionRecursive(node);
    if (extCaseCtx != null)
      return parseExtendedCaseExpression(extCaseCtx);

    // Check for EXISTS expressions
    final Cypher25Parser.ExistsExpressionContext existsCtx = findExistsExpressionRecursive(node);
    if (existsCtx != null)
      return parseExistsExpression(existsCtx);

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
      return parseExpressionFromText(e2.expression1());
    }

    // Handle Expression8 (comparison operators: =, <>, <, >, <=, >=)
    if (node instanceof Cypher25Parser.Expression8Context) {
      final Cypher25Parser.Expression8Context expr8 = (Cypher25Parser.Expression8Context) node;
      if (expr8.expression7().size() > 1)
        return parseComparisonFromExpression8(expr8);
      // Single Expression7 child, delegate to it
      if (!expr8.expression7().isEmpty())
        return parseExpressionFromText(expr8.expression7().get(0));
    }

    // Handle Expression7 (IS NULL, STARTS WITH, ENDS WITH, CONTAINS, IN, label check)
    if (node instanceof Cypher25Parser.Expression7Context) {
      final Cypher25Parser.Expression7Context expr7 = (Cypher25Parser.Expression7Context) node;
      final Cypher25Parser.ComparisonExpression6Context comp = expr7.comparisonExpression6();
      if (comp instanceof Cypher25Parser.NullComparisonContext)
        return parseIsNullExpression((Cypher25Parser.NullComparisonContext) comp);
      if (comp instanceof Cypher25Parser.StringAndListComparisonContext)
        return parseStringComparisonExpression(expr7);
      if (comp instanceof Cypher25Parser.LabelComparisonContext)
        return parseLabelComparisonExpression(expr7);
      // No comparison, delegate to expression6
      return parseExpressionFromText(expr7.expression6());
    }

    // Handle intermediate expression contexts by walking down the grammar hierarchy.
    // This prevents recursive searches from crossing parenthesized boundaries.
    if (node instanceof Cypher25Parser.Expression6Context) {
      final Cypher25Parser.Expression6Context e6 = (Cypher25Parser.Expression6Context) node;
      if (e6.expression5().size() > 1)
        return parseArithmeticExpression6(e6);
      return parseExpressionFromText(e6.expression5().get(0));
    }

    if (node instanceof Cypher25Parser.Expression5Context) {
      final Cypher25Parser.Expression5Context e5 = (Cypher25Parser.Expression5Context) node;
      if (e5.expression4().size() > 1)
        return parseArithmeticExpression5(e5);
      return parseExpressionFromText(e5.expression4().get(0));
    }

    if (node instanceof Cypher25Parser.Expression4Context) {
      final Cypher25Parser.Expression4Context e4 = (Cypher25Parser.Expression4Context) node;
      if (e4.expression3().size() > 1)
        return parseArithmeticExpression4(e4);
      return parseExpressionFromText(e4.expression3().get(0));
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
      if (e1.existsExpression() != null)
        return parseExistsExpression(e1.existsExpression());
      if (e1.countStar() != null) {
        final java.util.List<Expression> e1Args = new java.util.ArrayList<>();
        e1Args.add(new StarExpression());
        return new FunctionCallExpression("count", e1Args, false);
      }
      if (e1.parenthesizedExpression() != null)
        return parseExpression(e1.parenthesizedExpression().expression());
      if (e1.shortestPathExpression() != null)
        return parseShortestPathExpression(e1.shortestPathExpression());
      // Check for map literal
      final Cypher25Parser.MapContext e1MapCtx = findMapRecursive(e1);
      if (e1MapCtx != null)
        return parseMapLiteralExpression(e1MapCtx);
      // Fallback: parse as variable or literal
      // (will be handled by the text-based parsing below)
    }

    // Check for IS NULL / IS NOT NULL expressions BEFORE parenthesized expressions
    // because (a AND b) IS NULL should be parsed as IS_NULL(a AND b), not just (a AND b)
    final Cypher25Parser.NullComparisonContext nullCtx = findNullComparisonRecursive(node);
    if (nullCtx != null)
      return parseIsNullExpression(nullCtx);

    // Check for parenthesized expressions containing logical operators
    final Cypher25Parser.ParenthesizedExpressionContext parenCtx = findParenthesizedExpressionRecursive(node);
    if (parenCtx != null)
      return parseExpression(parenCtx.expression());

    // Check for arithmetic expressions (+ - * / % ^)
    final Cypher25Parser.Expression6Context arith6Ctx = findArithmeticExpression6Recursive(node);
    if (arith6Ctx != null)
      return parseArithmeticExpression6(arith6Ctx);

    final Cypher25Parser.Expression5Context arith5Ctx = findArithmeticExpression5Recursive(node);
    if (arith5Ctx != null)
      return parseArithmeticExpression5(arith5Ctx);

    // Check for list predicates (all/any/none/single) BEFORE function invocations
    // to avoid misinterpreting them as regular function calls
    final Cypher25Parser.ListItemsPredicateContext listPredCtx = findListItemsPredicateRecursive(node);
    if (listPredCtx != null)
      return parseListItemsPredicate(listPredCtx);

    // Check for reduce expressions
    final Cypher25Parser.ReduceExpressionContext reduceCtx = findReduceExpressionRecursive(node);
    if (reduceCtx != null)
      return parseReduceExpression(reduceCtx);

    // Check for list comprehensions [x IN list | expr]
    final Cypher25Parser.ListComprehensionContext listCompCtx = findListComprehensionRecursive(node);
    if (listCompCtx != null)
      return parseListComprehension(listCompCtx);

    // Check for function invocations
    final Cypher25Parser.FunctionInvocationContext funcCtx = findFunctionInvocationRecursive(node);
    if (funcCtx != null)
      return parseFunctionInvocation(funcCtx);

    // Check for list/map literals
    final Cypher25Parser.ListLiteralContext listLitCtx = findListLiteralRecursive(node);
    if (listLitCtx != null)
      return parseListLiteral(listLitCtx);

    final Cypher25Parser.MapContext mapCtx = findMapRecursive(node);
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
    // Check for parameter: $paramName or $1
    if (text.startsWith("$")) {
      final String parameterName = text.substring(1); // Remove the $ prefix
      return new ParameterExpression(parameterName, text);
    }

    // Check for null literal explicitly (tryParseLiteral returns Java null for it)
    if ("null".equalsIgnoreCase(text))
      return new LiteralExpression(null, text);

    // Try to parse as literal FIRST (before checking for dots)
    // This prevents string literals like 'A.*' from being parsed as property access
    final Object literalValue = tryParseLiteral(text);
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
          final ComparisonExpression.Operator compOp = op.equals("<>") ?
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
      return new PropertyAccessExpression(parts[0], CypherASTBuilder.stripBackticks(parts[1]));
    }

    // Simple variable
    return new VariableExpression(text);
  }

  /**
   * Parse a list expression into a list of Expression items.
   * Handles list literals like [1, 2, 3] or ['Alice', 'Bob'].
   */
  List<Expression> parseListExpression(final ParseTree node) {
    final List<Expression> items = new ArrayList<>();

    // Try to find a listLiteral context
    final Cypher25Parser.ListLiteralContext listCtx = findListLiteral(node);
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
   * Recursively find ParenthesizedExpressionContext in the parse tree.
   */
  Cypher25Parser.ParenthesizedExpressionContext findParenthesizedExpressionRecursive(final ParseTree node) {
    if (node instanceof Cypher25Parser.ParenthesizedExpressionContext)
      return (Cypher25Parser.ParenthesizedExpressionContext) node;
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ParenthesizedExpressionContext found = findParenthesizedExpressionRecursive(node.getChild(i));
      if (found != null)
        return found;
    }
    return null;
  }

  /**
   * Recursively find countStar context in the parse tree.
   * count(*) has special grammar handling as CountStarContext.
   */
  Cypher25Parser.CountStarContext findCountStarRecursive(final ParseTree node) {
    if (node == null) {
      return null;
    }

    // Check if this node is count(*)
    if (node instanceof Cypher25Parser.CountStarContext) {
      return (Cypher25Parser.CountStarContext) node;
    }

    // Only traverse into single-child wrapper nodes (expression precedence layers)
    // Stop at any node that is NOT a simple wrapper (multiple children means it's a compound expression)
    if (node.getChildCount() == 1) {
      return findCountStarRecursive(node.getChild(0));
    }

    return null;
  }

  /**
   * Recursively find list literal context in the parse tree.
   */
  Cypher25Parser.ListLiteralContext findListLiteralRecursive(final ParseTree node) {
    if (node == null) {
      return null;
    }

    // Check if this node is a list literal
    if (node instanceof Cypher25Parser.ListLiteralContext) {
      return (Cypher25Parser.ListLiteralContext) node;
    }

    // Recursively search all children
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ListLiteralContext found = findListLiteralRecursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Recursively find a ListLiteralContext in the parse tree.
   */
  Cypher25Parser.ListLiteralContext findListLiteral(final ParseTree node) {
    if (node instanceof Cypher25Parser.ListLiteralContext) {
      return (Cypher25Parser.ListLiteralContext) node;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ListLiteralContext found = findListLiteral(node.getChild(i));
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /**
   * Recursively find function invocation in the parse tree using depth-first search.
   * Does NOT recurse into MapContext to avoid matching nested function calls inside map literals.
   * This ensures that {typeR: type(r)} is recognized as a map, not as the type() function call.
   */
  Cypher25Parser.FunctionInvocationContext findFunctionInvocationRecursive(
      final ParseTree node) {
    if (node == null) {
      return null;
    }

    if (node instanceof Cypher25Parser.FunctionInvocationContext) {
      return (Cypher25Parser.FunctionInvocationContext) node;
    }

    // Do NOT recurse into map literals - they should be recognized separately
    // and their internal expressions (including function calls) parsed later
    if (node instanceof Cypher25Parser.MapContext) {
      return null;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.FunctionInvocationContext found = findFunctionInvocationRecursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Recursively find EXISTS expression in the parse tree.
   */
  Cypher25Parser.ExistsExpressionContext findExistsExpressionRecursive(
      final ParseTree node) {
    if (node == null) {
      return null;
    }

    if (node instanceof Cypher25Parser.ExistsExpressionContext) {
      return (Cypher25Parser.ExistsExpressionContext) node;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ExistsExpressionContext found = findExistsExpressionRecursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Recursively find CASE expression in the parse tree.
   */
  Cypher25Parser.CaseExpressionContext findCaseExpressionRecursive(
      final ParseTree node) {
    if (node == null) {
      return null;
    }

    if (node instanceof Cypher25Parser.CaseExpressionContext) {
      return (Cypher25Parser.CaseExpressionContext) node;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.CaseExpressionContext found = findCaseExpressionRecursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Recursively find extended CASE expression in the parse tree.
   */
  Cypher25Parser.ExtendedCaseExpressionContext findExtendedCaseExpressionRecursive(
      final ParseTree node) {
    if (node == null) {
      return null;
    }

    if (node instanceof Cypher25Parser.ExtendedCaseExpressionContext) {
      return (Cypher25Parser.ExtendedCaseExpressionContext) node;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ExtendedCaseExpressionContext found = findExtendedCaseExpressionRecursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Recursively find Expression8 context (handles comparison operators).
   */
  Cypher25Parser.Expression8Context findExpression8Recursive(
      final ParseTree node) {
    if (node == null) {
      return null;
    }

    if (node instanceof Cypher25Parser.Expression8Context) {
      // Only return if it contains a comparison operator
      final Cypher25Parser.Expression8Context expr8 = (Cypher25Parser.Expression8Context) node;
      if (expr8.expression7().size() > 1) {
        return expr8;
      }
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.Expression8Context found = findExpression8Recursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Walk the expression tree spine to find the top-level Expression8 with comparison operators.
   * Returns the Expression8 only when it has multiple expression7 children (i.e., has = < > etc.)
   * and there are no higher-level operators (OR, AND, NOT) above it.
   */
  Cypher25Parser.Expression8Context findTopLevelExpression8(final Cypher25Parser.ExpressionContext ctx) {
    final List<Cypher25Parser.Expression11Context> e11List = ctx.expression11();
    if (e11List == null || e11List.size() != 1)
      return null;

    final List<Cypher25Parser.Expression10Context> e10List = e11List.get(0).expression10();
    if (e10List == null || e10List.size() != 1)
      return null;

    final List<Cypher25Parser.Expression9Context> e9List = e10List.get(0).expression9();
    if (e9List == null || e9List.size() != 1)
      return null;

    final Cypher25Parser.Expression9Context e9 = e9List.get(0);
    if (e9.NOT() != null && !e9.NOT().isEmpty())
      return null;

    final Cypher25Parser.Expression8Context e8 = e9.expression8();
    if (e8 != null && e8.expression7().size() > 1)
      return e8;

    return null;
  }

  /**
   * Walk the expression tree spine (non-recursive) to find the top-level Expression7.
   * Returns the Expression7 only when there's a single child at each level
   * (no OR, AND, NOT, or comparison operators at higher levels).
   * This prevents matching comparisons inside nested expressions like function arguments
   * or quantifier WHERE clauses.
   */
  Cypher25Parser.Expression7Context findTopLevelExpression7(final Cypher25Parser.ExpressionContext ctx) {
    final List<Cypher25Parser.Expression11Context> e11List = ctx.expression11();
    if (e11List == null || e11List.size() != 1)
      return null;

    final List<Cypher25Parser.Expression10Context> e10List = e11List.get(0).expression10();
    if (e10List == null || e10List.size() != 1)
      return null;

    final List<Cypher25Parser.Expression9Context> e9List = e10List.get(0).expression9();
    if (e9List == null || e9List.size() != 1)
      return null;

    final Cypher25Parser.Expression9Context e9 = e9List.get(0);
    if (e9.NOT() != null && !e9.NOT().isEmpty())
      return null;

    final Cypher25Parser.Expression8Context e8 = e9.expression8();
    if (e8 == null || e8.expression7().size() != 1)
      return null;

    final Cypher25Parser.Expression7Context e7 = e8.expression7().get(0);
    if (e7.comparisonExpression6() != null)
      return e7;

    return null;
  }

  /**
   * Walk the expression tree spine (non-recursive) to find the top-level Expression6
   * that has arithmetic operators (+, -, ||).
   * Returns null if there's no top-level arithmetic at the Expression6 level.
   */
  Cypher25Parser.Expression6Context findTopLevelExpression6(final Cypher25Parser.ExpressionContext ctx) {
    final List<Cypher25Parser.Expression11Context> e11List = ctx.expression11();
    if (e11List == null || e11List.size() != 1)
      return null;

    final List<Cypher25Parser.Expression10Context> e10List = e11List.get(0).expression10();
    if (e10List == null || e10List.size() != 1)
      return null;

    final List<Cypher25Parser.Expression9Context> e9List = e10List.get(0).expression9();
    if (e9List == null || e9List.size() != 1)
      return null;

    final Cypher25Parser.Expression9Context e9 = e9List.get(0);
    if (e9.NOT() != null && !e9.NOT().isEmpty())
      return null;

    final Cypher25Parser.Expression8Context e8 = e9.expression8();
    if (e8 == null || e8.expression7().size() != 1)
      return null;

    final Cypher25Parser.Expression7Context e7 = e8.expression7().get(0);
    if (e7.comparisonExpression6() != null)
      return null; // Has IN/STARTS WITH comparison, not arithmetic

    final Cypher25Parser.Expression6Context e6 = e7.expression6();
    if (e6 != null && e6.expression5().size() > 1)
      return e6; // Has arithmetic operators

    return null;
  }

  /**
   * Walk the expression tree spine to find the top-level Expression5 with multiplicative operators (* / %).
   * Continues from where findTopLevelExpression6 leaves off.
   */
  Cypher25Parser.Expression5Context findTopLevelExpression5(final Cypher25Parser.ExpressionContext ctx) {
    final List<Cypher25Parser.Expression11Context> e11List = ctx.expression11();
    if (e11List == null || e11List.size() != 1)
      return null;

    final List<Cypher25Parser.Expression10Context> e10List = e11List.get(0).expression10();
    if (e10List == null || e10List.size() != 1)
      return null;

    final List<Cypher25Parser.Expression9Context> e9List = e10List.get(0).expression9();
    if (e9List == null || e9List.size() != 1)
      return null;

    final Cypher25Parser.Expression9Context e9 = e9List.get(0);
    if (e9.NOT() != null && !e9.NOT().isEmpty())
      return null;

    final Cypher25Parser.Expression8Context e8 = e9.expression8();
    if (e8 == null || e8.expression7().size() != 1)
      return null;

    final Cypher25Parser.Expression7Context e7 = e8.expression7().get(0);
    if (e7.comparisonExpression6() != null)
      return null;

    final Cypher25Parser.Expression6Context e6 = e7.expression6();
    if (e6 == null || e6.expression5().size() != 1)
      return null; // Has +/- operators at expression6 level

    final Cypher25Parser.Expression5Context e5 = e6.expression5().get(0);
    if (e5 != null && e5.expression4().size() > 1)
      return e5; // Has * / % operators

    return null;
  }

  /**
   * Walk the expression tree spine to find the top-level Expression4 with exponentiation (^).
   */
  Cypher25Parser.Expression4Context findTopLevelExpression4(final Cypher25Parser.ExpressionContext ctx) {
    final List<Cypher25Parser.Expression11Context> e11List = ctx.expression11();
    if (e11List == null || e11List.size() != 1)
      return null;

    final List<Cypher25Parser.Expression10Context> e10List = e11List.get(0).expression10();
    if (e10List == null || e10List.size() != 1)
      return null;

    final List<Cypher25Parser.Expression9Context> e9List = e10List.get(0).expression9();
    if (e9List == null || e9List.size() != 1)
      return null;

    final Cypher25Parser.Expression9Context e9 = e9List.get(0);
    if (e9.NOT() != null && !e9.NOT().isEmpty())
      return null;

    final Cypher25Parser.Expression8Context e8 = e9.expression8();
    if (e8 == null || e8.expression7().size() != 1)
      return null;

    final Cypher25Parser.Expression7Context e7 = e8.expression7().get(0);
    if (e7.comparisonExpression6() != null)
      return null;

    final Cypher25Parser.Expression6Context e6 = e7.expression6();
    if (e6 == null || e6.expression5().size() != 1)
      return null;

    final Cypher25Parser.Expression5Context e5 = e6.expression5().get(0);
    if (e5 == null || e5.expression4().size() != 1)
      return null; // Has * / % operators

    final Cypher25Parser.Expression4Context e4 = e5.expression4().get(0);
    if (e4 != null && e4.expression3().size() > 1)
      return e4; // Has ^ operator

    return null;
  }

  /**
   * Walk the expression tree spine to find top-level unary operator (expression3 level).
   * Returns the Expression3Context if it has a unary PLUS or MINUS prefix.
   */
  Cypher25Parser.Expression3Context findTopLevelExpression3(final Cypher25Parser.ExpressionContext ctx) {
    final List<Cypher25Parser.Expression11Context> e11List = ctx.expression11();
    if (e11List == null || e11List.size() != 1)
      return null;

    final List<Cypher25Parser.Expression10Context> e10List = e11List.get(0).expression10();
    if (e10List == null || e10List.size() != 1)
      return null;

    final List<Cypher25Parser.Expression9Context> e9List = e10List.get(0).expression9();
    if (e9List == null || e9List.size() != 1)
      return null;

    final Cypher25Parser.Expression9Context e9 = e9List.get(0);
    if (e9.NOT() != null && !e9.NOT().isEmpty())
      return null;

    final Cypher25Parser.Expression8Context e8 = e9.expression8();
    if (e8 == null || e8.expression7().size() != 1)
      return null;

    final Cypher25Parser.Expression7Context e7 = e8.expression7().get(0);
    if (e7.comparisonExpression6() != null)
      return null;

    final Cypher25Parser.Expression6Context e6 = e7.expression6();
    if (e6 == null || e6.expression5().size() != 1)
      return null;

    final Cypher25Parser.Expression5Context e5 = e6.expression5().get(0);
    if (e5 == null || e5.expression4().size() != 1)
      return null;

    final Cypher25Parser.Expression4Context e4 = e5.expression4().get(0);
    if (e4 == null || e4.expression3().size() != 1)
      return null;

    final Cypher25Parser.Expression3Context e3 = e4.expression3().get(0);
    // Check if this expression3 has a unary operator (MINUS or PLUS prefix)
    if (e3.getChildCount() > 1 && e3.getChild(0) instanceof TerminalNode)
      return e3;

    return null;
  }

  /**
   * Walk the expression tree spine (non-recursive) to find the top-level Expression2
   * that has postfix operations (property access, indexing, slicing).
   */
  Cypher25Parser.Expression2Context findTopLevelExpression2(final Cypher25Parser.ExpressionContext ctx) {
    final List<Cypher25Parser.Expression11Context> e11List = ctx.expression11();
    if (e11List == null || e11List.size() != 1)
      return null;

    final List<Cypher25Parser.Expression10Context> e10List = e11List.get(0).expression10();
    if (e10List == null || e10List.size() != 1)
      return null;

    final List<Cypher25Parser.Expression9Context> e9List = e10List.get(0).expression9();
    if (e9List == null || e9List.size() != 1)
      return null;

    final Cypher25Parser.Expression9Context e9 = e9List.get(0);
    if (e9.NOT() != null && !e9.NOT().isEmpty())
      return null;

    final Cypher25Parser.Expression8Context e8 = e9.expression8();
    if (e8 == null || e8.expression7().size() != 1)
      return null;

    final Cypher25Parser.Expression7Context e7 = e8.expression7().get(0);
    if (e7.comparisonExpression6() != null)
      return null;

    final Cypher25Parser.Expression6Context e6 = e7.expression6();
    if (e6 == null || e6.expression5().size() != 1)
      return null; // Has arithmetic at this level

    final Cypher25Parser.Expression5Context e5 = e6.expression5().get(0);
    if (e5 == null || e5.expression4().size() != 1)
      return null; // Has * / % operators

    final Cypher25Parser.Expression4Context e4 = e5.expression4().get(0);
    if (e4 == null || e4.expression3().size() != 1)
      return null; // Has ^ operator

    // expression3 → (MINUS | PLUS)? expression2
    final Cypher25Parser.Expression3Context e3 = e4.expression3().get(0);
    final Cypher25Parser.Expression2Context e2 = e3.expression2();
    if (e2 != null && !e2.postFix().isEmpty())
      return e2;

    return null;
  }

  /**
   * Recursively find Expression7Context that has a comparisonExpression6 child
   * (STARTS WITH, ENDS WITH, CONTAINS, =~, IN, label check).
   */
  Cypher25Parser.Expression7Context findExpression7WithComparisonRecursive(final ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.Expression7Context) {
      final Cypher25Parser.Expression7Context expr7 = (Cypher25Parser.Expression7Context) node;
      if (expr7.comparisonExpression6() != null)
        return expr7;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.Expression7Context found = findExpression7WithComparisonRecursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Recursively find NullComparison context (IS NULL / IS NOT NULL).
   */
  Cypher25Parser.NullComparisonContext findNullComparisonRecursive(
      final ParseTree node) {
    if (node == null) {
      return null;
    }

    if (node instanceof Cypher25Parser.NullComparisonContext) {
      return (Cypher25Parser.NullComparisonContext) node;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.NullComparisonContext found = findNullComparisonRecursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Recursively find Expression6Context with arithmetic operators (+ - ||)
   */
  Cypher25Parser.Expression6Context findArithmeticExpression6Recursive(
      final ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.Expression6Context) {
      final Cypher25Parser.Expression6Context ctx = (Cypher25Parser.Expression6Context) node;
      // Only return if it has multiple expression5 children (i.e., has operators)
      if (ctx.expression5().size() > 1)
        return ctx;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.Expression6Context found = findArithmeticExpression6Recursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Recursively find Expression5Context with arithmetic operators (* / %)
   */
  Cypher25Parser.Expression5Context findArithmeticExpression5Recursive(
      final ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.Expression5Context) {
      final Cypher25Parser.Expression5Context ctx = (Cypher25Parser.Expression5Context) node;
      // Only return if it has multiple expression4 children (i.e., has operators)
      if (ctx.expression4().size() > 1)
        return ctx;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.Expression5Context found = findArithmeticExpression5Recursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Recursively find MapContext in the parse tree (for map literals like {name: 'Alice'})
   * Does NOT recurse into FunctionInvocationContext to avoid matching map literals inside function arguments.
   * This ensures that func({a: 1}) is recognized as a function call, not as the map literal.
   */
  Cypher25Parser.MapContext findMapRecursive(final ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.MapContext)
      return (Cypher25Parser.MapContext) node;

    // Do NOT recurse into function invocations - they should be recognized separately
    // and their internal arguments (including map literals) parsed later
    if (node instanceof Cypher25Parser.FunctionInvocationContext)
      return null;

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.MapContext found = findMapRecursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Recursively find ListComprehensionContext in the parse tree.
   */
  Cypher25Parser.ListComprehensionContext findListComprehensionRecursive(
      final ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.ListComprehensionContext)
      return (Cypher25Parser.ListComprehensionContext) node;

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ListComprehensionContext found = findListComprehensionRecursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Recursively find PatternComprehensionContext in the parse tree.
   */
  Cypher25Parser.PatternComprehensionContext findPatternComprehensionRecursive(final ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.PatternComprehensionContext)
      return (Cypher25Parser.PatternComprehensionContext) node;

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.PatternComprehensionContext found = findPatternComprehensionRecursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Recursively find ListItemsPredicateContext in the parse tree.
   */
  Cypher25Parser.ListItemsPredicateContext findListItemsPredicateRecursive(final ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.ListItemsPredicateContext)
      return (Cypher25Parser.ListItemsPredicateContext) node;

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ListItemsPredicateContext found = findListItemsPredicateRecursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Recursively find ShortestPathExpressionContext in the parse tree.
   */
  Cypher25Parser.ShortestPathExpressionContext findShortestPathExpressionRecursive(
      final ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.ShortestPathExpressionContext)
      return (Cypher25Parser.ShortestPathExpressionContext) node;

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ShortestPathExpressionContext found = findShortestPathExpressionRecursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Recursively find ReduceExpressionContext in the parse tree.
   */
  Cypher25Parser.ReduceExpressionContext findReduceExpressionRecursive(
      final ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.ReduceExpressionContext)
      return (Cypher25Parser.ReduceExpressionContext) node;

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ReduceExpressionContext found = findReduceExpressionRecursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Recursively find MapProjectionContext in the parse tree.
   */
  Cypher25Parser.MapProjectionContext findMapProjectionRecursive(
      final ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.MapProjectionContext)
      return (Cypher25Parser.MapProjectionContext) node;

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.MapProjectionContext found = findMapProjectionRecursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Recursively find Expression2 context (which may have postfix operations).
   */
  Cypher25Parser.Expression2Context findExpression2Recursive(final ParseTree node) {
    if (node == null) {
      return null;
    }

    if (node instanceof Cypher25Parser.Expression2Context) {
      return (Cypher25Parser.Expression2Context) node;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.Expression2Context found = findExpression2Recursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
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
   * Expression for chained property access where the base is not a simple variable.
   * Example: list[0].property, func().property
   */
  private static class ChainedPropertyAccessExpression implements Expression {
    private final Expression baseExpression;
    private final String propertyName;

    ChainedPropertyAccessExpression(final Expression baseExpression, final String propertyName) {
      this.baseExpression = baseExpression;
      this.propertyName = propertyName;
    }

    @Override
    public Object evaluate(final Result result, final CommandContext context) {
      final Object baseValue = baseExpression.evaluate(result, context);
      if (baseValue == null) {
        return null;
      }

      // Handle Document types
      if (baseValue instanceof Document) {
        return ((Document) baseValue).get(propertyName);
      }

      // Handle Map types
      if (baseValue instanceof Map) {
        return ((Map<?, ?>) baseValue).get(propertyName);
      }

      // Handle Result types
      if (baseValue instanceof Result) {
        return ((Result) baseValue).getProperty(propertyName);
      }

      return null;
    }

    @Override
    public boolean isAggregation() {
      return baseExpression.isAggregation();
    }

    @Override
    public boolean containsAggregation() {
      return baseExpression.containsAggregation();
    }

    @Override
    public String getText() {
      return baseExpression.getText() + "." + propertyName;
    }
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
   * Parse a function invocation into a FunctionCallExpression.
   */
  FunctionCallExpression parseFunctionInvocation(final Cypher25Parser.FunctionInvocationContext ctx) {
    final String functionName = ctx.functionName().getText();
    final boolean distinct = ctx.DISTINCT() != null;
    final List<Expression> arguments = new ArrayList<>();

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
   * Parse an EXISTS expression.
   * Examples: EXISTS { MATCH (n)-[:KNOWS]->(m) }, EXISTS { (n)-[:KNOWS]->() WHERE n.age > 18 }
   */
  ExistsExpression parseExistsExpression(final Cypher25Parser.ExistsExpressionContext ctx) {
    // Extract the subquery text between { and }
    // For now, we'll extract it as text and execute it as a separate query
    final String fullText = ctx.getText();
    final String text = fullText;

    // Remove "EXISTS{" prefix and "}" suffix
    String subquery = fullText.substring(7, fullText.length() - 1); // Remove "EXISTS{" and "}"

    // If it's a pattern without MATCH, add MATCH prefix
    if (!subquery.trim().toUpperCase().startsWith("MATCH")
        && !subquery.trim().toUpperCase().startsWith("WITH")
        && !subquery.trim().toUpperCase().startsWith("RETURN")) {
      subquery = "MATCH " + subquery + " RETURN true";
    }

    return new ExistsExpression(subquery, text);
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
      final Expression thenExpr = parseExpression(altCtx.expression(1)); // Second expression is THEN
      alternatives.add(new CaseAlternative(whenExpr, thenExpr));
    }

    // Parse optional ELSE clause
    Expression elseExpr = null;
    if (ctx.expression() != null) {
      elseExpr = parseExpression(ctx.expression());
    }

    return new CaseExpression(alternatives, elseExpr, ctx.getText());
  }

  /**
   * Parse an extended CASE expression (with case value).
   * Example: CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END
   */
  CaseExpression parseExtendedCaseExpression(final Cypher25Parser.ExtendedCaseExpressionContext ctx) {
    // Parse the case expression (the value being tested)
    final Expression caseExpr = parseExpression(ctx.expression(0));

    final List<CaseAlternative> alternatives = new ArrayList<>();

    // Parse each WHEN...THEN alternative
    for (final Cypher25Parser.ExtendedCaseAlternativeContext altCtx : ctx.extendedCaseAlternative()) {
      // In extended form, WHEN contains value(s) to match against
      // Use the first extendedWhen's text to create an expression
      final String whenText = altCtx.extendedWhen(0).getText();
      final Expression whenExpr = parseExpressionText(whenText);
      final Expression thenExpr = parseExpression(altCtx.expression());
      alternatives.add(new CaseAlternative(whenExpr, thenExpr));
    }

    // Parse optional ELSE clause
    Expression elseExpr = null;
    if (ctx.elseExp != null) {
      elseExpr = parseExpression(ctx.elseExp);
    }

    return new CaseExpression(caseExpr, alternatives, elseExpr, ctx.getText());
  }

  /**
   * Parse a comparison expression from Expression8Context.
   * Handles: <, >, <=, >=, =, !=
   */
  Expression parseComparisonFromExpression8(final Cypher25Parser.Expression8Context ctx) {
    // Expression8 has multiple expression7 children with comparison operators between them
    if (ctx.expression7().size() > 1) {
      // Found a comparison, get the operator
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

          if (op != null) {
            final Expression left = parseExpressionFromText(ctx.expression7(0));
            final Expression right = parseExpressionFromText(ctx.expression7(1));
            final ComparisonExpression comparison = new ComparisonExpression(left, op, right);
            // Wrap BooleanExpression in an Expression adapter
            return new BooleanWrapperExpression(comparison);
          }
        }
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
        return new BooleanWrapperExpression(new InExpression(leftExpr, listItems, false));
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

    final List<String> labels = new ArrayList<>();
    LabelCheckExpression.LabelOperator operator = LabelCheckExpression.LabelOperator.AND;

    String labelText = labelExprCtx.getText();
    if (labelText.startsWith(":"))
      labelText = labelText.substring(1);
    else if (labelText.toUpperCase().startsWith("IS"))
      labelText = labelText.substring(2).trim();

    if (labelText.contains("|")) {
      operator = LabelCheckExpression.LabelOperator.OR;
      final String[] parts = labelText.split("\\|:?");
      for (final String part : parts) {
        final String label = part.trim();
        if (!label.isEmpty())
          labels.add(label);
      }
    } else if (labelText.contains("&") || labelText.contains(":")) {
      final String[] parts = labelText.split("[&:]");
      for (final String part : parts) {
        final String label = part.trim();
        if (!label.isEmpty())
          labels.add(label);
      }
    } else {
      labels.add(labelText.trim());
    }

    return new BooleanWrapperExpression(new LabelCheckExpression(leftExpr, labels, operator, ctx.getText()));
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
    final String variable = ctx.variable().getText();

    // The main expression after IN - it's the first expression in the list
    final List<Cypher25Parser.ExpressionContext> expressions = ctx.expression();
    final Expression listExpression = parseExpression(expressions.get(0));

    // Optional WHERE clause
    Expression whereExpression = null;
    if (ctx.whereExp != null)
      whereExpression = parseExpression(ctx.whereExp);

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
      pathVariable = ctx.variable().getText();

    // Parse the path pattern
    final PathPattern pathPattern = parsePathPatternNonEmpty(ctx.pathPatternNonEmpty());

    // Optional WHERE clause
    Expression whereExpression = null;
    if (ctx.whereExp != null)
      whereExpression = parseExpression(ctx.whereExp);

    // Required mapping expression after |
    final Expression mapExpression = parseExpression(ctx.barExp);

    return new PatternComprehensionExpression(pathVariable, pathPattern, whereExpression, mapExpression, ctx.getText());
  }

  /**
   * Parse a pathPatternNonEmpty into a PathPattern.
   * Grammar: nodePattern (relationshipPattern nodePattern)+
   */
  private PathPattern parsePathPatternNonEmpty(final Cypher25Parser.PathPatternNonEmptyContext ctx) {
    final List<NodePattern> nodes = new ArrayList<>();
    final List<RelationshipPattern> relationships = new ArrayList<>();

    // First node
    nodes.add(parseNodePattern(ctx.nodePattern(0)));

    // Relationships and subsequent nodes
    for (int i = 0; i < ctx.relationshipPattern().size(); i++) {
      relationships.add(parseRelationshipPattern(ctx.relationshipPattern(i)));
      nodes.add(parseNodePattern(ctx.nodePattern(i + 1)));
    }

    return new PathPattern(nodes, relationships);
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

    final String variable = ctx.variable().getText();
    final Expression listExpression = parseExpression(ctx.inExp);

    Expression whereExpression = null;
    if (ctx.whereExp != null)
      whereExpression = parseExpression(ctx.whereExp);

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

    final String accumulatorVariable = variables.get(0).getText();
    final Expression initialValue = parseExpression(expressions.get(0));

    final String iteratorVariable = variables.get(1).getText();
    final Expression listExpression = parseExpression(expressions.get(1));

    final Expression reduceExpression = parseExpression(expressions.get(2));

    return new ReduceExpression(accumulatorVariable, initialValue, iteratorVariable,
        listExpression, reduceExpression, ctx.getText());
  }

  // ============================================================================
  // Shortest Path Expression Parsing
  // ============================================================================

  /**
   * Parse a shortestPath or allShortestPaths expression.
   * Syntax: shortestPath((a)-[:KNOWS*]-(b)) or allShortestPaths((a)-[:KNOWS*]-(b))
   */
  ShortestPathExpression parseShortestPathExpression(final Cypher25Parser.ShortestPathExpressionContext ctx) {
    // Grammar: shortestPathExpression : shortestPathPattern ;
    // shortestPathPattern : (SHORTEST_PATH | ALL_SHORTEST_PATHS) LPAREN patternElement RPAREN ;
    final Cypher25Parser.ShortestPathPatternContext patternCtx = ctx.shortestPathPattern();
    final boolean isAllPaths = patternCtx.ALL_SHORTEST_PATHS() != null;

    // Parse the inner pattern element
    final PathPattern innerPattern = parsePatternElement(patternCtx.patternElement());

    return new ShortestPathExpression(innerPattern, isAllPaths, ctx.getText());
  }

  /**
   * Parse a patternElement into a PathPattern.
   * This is used for shortestPath patterns which contain a patternElement.
   */
  private PathPattern parsePatternElement(final Cypher25Parser.PatternElementContext ctx) {
    final List<NodePattern> nodes = new ArrayList<>();
    final List<RelationshipPattern> relationships = new ArrayList<>();

    // First node
    if (!ctx.nodePattern().isEmpty()) {
      nodes.add(parseNodePattern(ctx.nodePattern(0)));

      // Relationships and subsequent nodes
      for (int i = 0; i < ctx.relationshipPattern().size(); i++) {
        relationships.add(parseRelationshipPattern(ctx.relationshipPattern(i)));
        if (i + 1 < ctx.nodePattern().size()) {
          nodes.add(parseNodePattern(ctx.nodePattern(i + 1)));
        }
      }
    }

    if (relationships.isEmpty()) {
      return new PathPattern(nodes.get(0));
    } else {
      return new PathPattern(nodes, relationships);
    }
  }

  /**
   * Parse a nodePattern into a NodePattern.
   */
  private NodePattern parseNodePattern(final Cypher25Parser.NodePatternContext ctx) {
    String variable = null;
    List<String> labels = null;
    Map<String, Object> properties = null;

    if (ctx.variable() != null) {
      variable = ctx.variable().getText();
    }

    if (ctx.labelExpression() != null) {
      labels = extractLabels(ctx.labelExpression());
    }

    if (ctx.properties() != null && ctx.properties().map() != null) {
      properties = parseMapProperties(ctx.properties().map());
    }

    return new NodePattern(variable, labels, properties);
  }

  /**
   * Parse a relationshipPattern into a RelationshipPattern.
   */
  private RelationshipPattern parseRelationshipPattern(final Cypher25Parser.RelationshipPatternContext ctx) {
    String variable = null;
    List<String> types = null;
    Map<String, Object> properties = null;
    Integer minHops = null;
    Integer maxHops = null;

    if (ctx.variable() != null) {
      variable = ctx.variable().getText();
    }

    if (ctx.labelExpression() != null) {
      types = extractLabels(ctx.labelExpression());
    }

    if (ctx.properties() != null && ctx.properties().map() != null) {
      properties = parseMapProperties(ctx.properties().map());
    }

    // Path length (variable-length relationships)
    if (ctx.pathLength() != null) {
      final Cypher25Parser.PathLengthContext pathLen = ctx.pathLength();
      if (pathLen.from != null) {
        minHops = Integer.parseInt(pathLen.from.getText());
      }
      if (pathLen.to != null) {
        maxHops = Integer.parseInt(pathLen.to.getText());
      }
      if (pathLen.single != null) {
        minHops = maxHops = Integer.parseInt(pathLen.single.getText());
      }
    }

    // Direction
    final Direction direction;
    if (ctx.leftArrow() != null && ctx.rightArrow() != null) {
      direction = Direction.BOTH;
    } else if (ctx.leftArrow() != null) {
      direction = Direction.IN;
    } else if (ctx.rightArrow() != null) {
      direction = Direction.OUT;
    } else {
      direction = Direction.BOTH;
    }

    return new RelationshipPattern(variable, types, direction, properties, minHops, maxHops);
  }

  /**
   * Extract labels from a labelExpression context.
   */
  private List<String> extractLabels(final Cypher25Parser.LabelExpressionContext ctx) {
    final String text = ctx.getText();
    final String cleanText = text.replaceAll("^:+", "");
    final String[] parts = cleanText.split("[:&|]+");
    final List<String> labels = new ArrayList<>();
    for (String part : parts) {
      if (!part.isEmpty()) {
        labels.add(part);
      }
    }
    return labels;
  }

  /**
   * Parse map properties from a MapContext.
   */
  private Map<String, Object> parseMapProperties(final Cypher25Parser.MapContext ctx) {
    final Map<String, Object> map = new LinkedHashMap<>();
    final List<Cypher25Parser.PropertyKeyNameContext> keys = ctx.propertyKeyName();
    final List<Cypher25Parser.ExpressionContext> values = ctx.expression();

    for (int i = 0; i < keys.size() && i < values.size(); i++) {
      final String key = CypherASTBuilder.stripBackticks(keys.get(i).getText());
      final Expression expr = parseExpression(values.get(i));
      if (expr instanceof LiteralExpression) {
        map.put(key, ((LiteralExpression) expr).getValue());
      } else {
        map.put(key, expr);
      }
    }

    return map;
  }

  // ============================================================================
  // Map Projection Parsing
  // ============================================================================

  /**
   * Parse a map projection into a MapProjectionExpression.
   * Syntax: variable{.property1, .property2, key: expression, .*}
   * Examples: n{.name, .age}, n{.*, totalAge: n.age * 2}
   */
  MapProjectionExpression parseMapProjection(final Cypher25Parser.MapProjectionContext ctx) {
    final String variableName = ctx.variable().getText();
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
        final String varName = elemCtx.variable().getText();
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
   * Try to parse text as a literal value (number, string, boolean, null).
   */
  Object tryParseLiteral(final String text) {
    // Null
    if ("null".equalsIgnoreCase(text)) {
      return null; // Return null as a marker that we found a literal
    }

    // Boolean
    if ("true".equalsIgnoreCase(text)) {
      return Boolean.TRUE;
    }
    if ("false".equalsIgnoreCase(text)) {
      return Boolean.FALSE;
    }

    // String (quoted) - strip quotes and decode escape sequences
    if (text.startsWith("'") && text.endsWith("'") && text.length() >= 2) {
      return CypherASTBuilder.decodeStringLiteral(text.substring(1, text.length() - 1));
    }
    if (text.startsWith("\"") && text.endsWith("\"") && text.length() >= 2) {
      return CypherASTBuilder.decodeStringLiteral(text.substring(1, text.length() - 1));
    }

    // Number
    try {
      // Handle floats without integer part (.1, .5e-3, etc.)
      if (text.startsWith(".") && text.length() > 1 && Character.isDigit(text.charAt(1)))
        return parseDoubleChecked(text);
      if (text.startsWith("-.") && text.length() > 2 && Character.isDigit(text.charAt(2)))
        return parseDoubleChecked(text);

      // Handle negative numbers
      if (text.startsWith("-") || text.startsWith("+") || Character.isDigit(text.charAt(0))) {
        // Check hex/octal BEFORE float detection because hex digits can contain 'e'/'E'
        if (text.startsWith("0x") || text.startsWith("0X") ||
            text.startsWith("-0x") || text.startsWith("-0X")) {
          final String hexPart = text.startsWith("-") ? text.substring(3) : text.substring(2);
          return parseSignedHexOrOctal(hexPart, 16, text.startsWith("-"), text);
        } else if (text.startsWith("0o") || text.startsWith("0O") ||
            text.startsWith("-0o") || text.startsWith("-0O")) {
          final String octPart = text.startsWith("-") ? text.substring(3) : text.substring(2);
          return parseSignedHexOrOctal(octPart, 8, text.startsWith("-"), text);
        } else if (text.contains(".") || text.contains("e") || text.contains("E")) {
          return parseDoubleChecked(text);
        } else {
          return Long.parseLong(text);
        }
      }
    } catch (final NumberFormatException e) {
      // Check if it looks like a number but overflowed or is invalid
      final String numText = text.startsWith("-") || text.startsWith("+") ? text.substring(1) : text;
      if (numText.startsWith("0x") || numText.startsWith("0X") ||
          numText.startsWith("0o") || numText.startsWith("0O"))
        throw new CommandParsingException("InvalidNumberLiteral: Invalid number literal: " + text);
      if (!numText.isEmpty() && Character.isDigit(numText.charAt(0))) {
        if (numText.chars().allMatch(c -> Character.isDigit(c)))
          throw new CommandParsingException("IntegerOverflow: Integer literal is too large: " + text);
        throw new CommandParsingException("InvalidNumberLiteral: Invalid number literal: " + text);
      }
    }

    return null; // Not a literal
  }

  private static double parseDoubleChecked(final String text) {
    final double d = Double.parseDouble(text);
    if (Double.isInfinite(d))
      throw new CommandParsingException("FloatingPointOverflow: Floating point number is too large: " + text);
    return d;
  }

  private static long parseSignedHexOrOctal(final String digits, final int radix,
      final boolean negative, final String originalText) {
    final long val;
    try {
      val = Long.parseUnsignedLong(digits, radix);
    } catch (final NumberFormatException e) {
      throw new CommandParsingException("IntegerOverflow: Integer literal is too large: " + originalText);
    }
    if (negative) {
      // -0x8000000000000000 is Long.MIN_VALUE, anything more negative overflows
      if (val == Long.MIN_VALUE)
        return Long.MIN_VALUE; // -(-2^63) would overflow, but -val wraps correctly for MIN_VALUE
      if (val < 0) // unsigned value > Long.MAX_VALUE (bit 63 set), so magnitude > 2^63
        throw new CommandParsingException("IntegerOverflow: Integer literal is too large: " + originalText);
      return -val;
    } else {
      if (val < 0) // unsigned value > Long.MAX_VALUE (bit 63 set)
        throw new CommandParsingException("IntegerOverflow: Integer literal is too large: " + originalText);
      return val;
    }
  }
}
