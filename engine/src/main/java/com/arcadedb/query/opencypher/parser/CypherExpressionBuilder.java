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
 */
class CypherExpressionBuilder {

  /**
   * Parse an expression into an Expression AST node.
   * Handles variables, property access, function calls, list literals, and literals.
   */
  Expression parseExpression(final Cypher25Parser.ExpressionContext ctx) {
    final String text = ctx.getText();

    // Check for count(*) special case - has its own grammar rule
    final Cypher25Parser.CountStarContext countStarCtx = findCountStarRecursive(ctx);
    if (countStarCtx != null) {
      // count(*) is treated as count(asterisk) where asterisk evaluates to a non-null marker
      final List<Expression> args = new ArrayList<>();
      args.add(new StarExpression());
      return new FunctionCallExpression("count", args, false);
    }

    // Check for EXISTS expression
    final Cypher25Parser.ExistsExpressionContext existsCtx = findExistsExpressionRecursive(ctx);
    if (existsCtx != null) {
      return parseExistsExpression(existsCtx);
    }

    // Check for CASE expressions (check both forms)
    final Cypher25Parser.CaseExpressionContext caseCtx = findCaseExpressionRecursive(ctx);
    if (caseCtx != null) {
      return parseCaseExpression(caseCtx);
    }

    final Cypher25Parser.ExtendedCaseExpressionContext extCaseCtx = findExtendedCaseExpressionRecursive(ctx);
    if (extCaseCtx != null) {
      return parseExtendedCaseExpression(extCaseCtx);
    }

    // Check for shortestPath expressions
    // shortestPath((a)-[:KNOWS*]-(b)) and allShortestPaths((a)-[:KNOWS*]-(b))
    final Cypher25Parser.ShortestPathExpressionContext shortestPathCtx = findShortestPathExpressionRecursive(ctx);
    if (shortestPathCtx != null) {
      return parseShortestPathExpression(shortestPathCtx);
    }

    // Check for reduce expressions BEFORE list comprehensions
    // reduce(acc = 0, n IN list | acc + n) is a fold operation over a list
    final Cypher25Parser.ReduceExpressionContext reduceCtx = findReduceExpressionRecursive(ctx);
    if (reduceCtx != null) {
      return parseReduceExpression(reduceCtx);
    }

    // Check for list comprehensions BEFORE comparison expressions
    // List comprehensions can contain comparisons in WHERE clause, e.g., [x IN list WHERE x > 2 | x * 10]
    // If we check comparisons first, the inner comparison gets matched instead
    final Cypher25Parser.ListComprehensionContext listCompCtx = findListComprehensionRecursive(ctx);
    if (listCompCtx != null) {
      return parseListComprehension(listCompCtx);
    }

    // Check for comparison expressions (before function invocations)
    // This is critical for expressions like ID(a) = row.source_id
    // where we need to recognize the comparison operator at the top level
    final Cypher25Parser.Expression8Context expr8Ctx = findExpression8Recursive(ctx);
    if (expr8Ctx != null) {
      // Check if this Expression8 actually contains a comparison (has 2+ expression7 children)
      if (expr8Ctx.expression7().size() > 1) {
        return parseComparisonFromExpression8(expr8Ctx);
      }
    }

    // Check for function invocations BEFORE list literals
    // (tail([1,2,3]) should be parsed as a function call, not as a list literal)
    final Cypher25Parser.FunctionInvocationContext funcCtx = findFunctionInvocationRecursive(ctx);
    if (funcCtx != null) {
      return parseFunctionInvocation(funcCtx);
    }

    // Check for list literals (after list comprehensions to avoid matching inner lists)
    final Cypher25Parser.ListLiteralContext listCtx = findListLiteralRecursive(ctx);
    if (listCtx != null) {
      return parseListLiteral(listCtx);
    }

    // Check for map projections BEFORE arithmetic expressions
    // Map projections have syntax: n{.name, .age} and can contain arithmetic inside
    final Cypher25Parser.MapProjectionContext mapProjCtx = findMapProjectionRecursive(ctx);
    if (mapProjCtx != null) {
      return parseMapProjection(mapProjCtx);
    }

    // Check for map literals BEFORE arithmetic expressions
    // Map literals can contain arithmetic expressions inside, e.g., {doubled: n.age * 2}
    final Cypher25Parser.MapContext mapCtx = findMapRecursive(ctx);
    if (mapCtx != null) {
      return parseMapLiteralExpression(mapCtx);
    }

    // Check for arithmetic expressions (+ - * / % ^)
    // Must be checked BEFORE falling back to text parsing
    final Cypher25Parser.Expression6Context arith6Ctx = findArithmeticExpression6Recursive(ctx);
    if (arith6Ctx != null) {
      return parseArithmeticExpression6(arith6Ctx);
    }
    final Cypher25Parser.Expression5Context arith5Ctx = findArithmeticExpression5Recursive(ctx);
    if (arith5Ctx != null) {
      return parseArithmeticExpression5(arith5Ctx);
    }

    // Check for IS NULL / IS NOT NULL expressions
    final Cypher25Parser.NullComparisonContext nullCtx = findNullComparisonRecursive(ctx);
    if (nullCtx != null) {
      return parseIsNullExpression(nullCtx);
    }

    // Check for postfix expressions (property access, list indexing, slicing)
    // This must be checked BEFORE falling back to text parsing
    final Cypher25Parser.Expression2Context expr2Ctx = findExpression2Recursive(ctx);
    if (expr2Ctx != null && !expr2Ctx.postFix().isEmpty()) {
      return parseExpression2WithPostfix(expr2Ctx);
    }

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
    if (caseCtx != null) {
      return parseCaseExpression(caseCtx);
    }

    final Cypher25Parser.ExtendedCaseExpressionContext extCaseCtx = findExtendedCaseExpressionRecursive(node);
    if (extCaseCtx != null) {
      return parseExtendedCaseExpression(extCaseCtx);
    }

    // Check for EXISTS expressions
    final Cypher25Parser.ExistsExpressionContext existsCtx = findExistsExpressionRecursive(node);
    if (existsCtx != null) {
      return parseExistsExpression(existsCtx);
    }

    // Check for IS NULL / IS NOT NULL expressions
    final Cypher25Parser.NullComparisonContext nullCtx = findNullComparisonRecursive(node);
    if (nullCtx != null) {
      return parseIsNullExpression(nullCtx);
    }

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
      return new PropertyAccessExpression(parts[0], parts[1]);
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
  // Recursive Find Methods
  // ============================================================================

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

    // Recursively search all children
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.CountStarContext found = findCountStarRecursive(node.getChild(i));
      if (found != null) {
        return found;
      }
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
        final String propertyName = propCtx.property().propertyKeyName().getText();
        // Create a compound expression: treat result as a variable expression
        result = createPropertyAccessFromExpression(result, propertyName);
      } else if (postfix instanceof Cypher25Parser.IndexPostfixContext) {
        // List indexing: expr[index]
        final Cypher25Parser.IndexPostfixContext indexCtx = (Cypher25Parser.IndexPostfixContext) postfix;
        final Expression indexExpr = parseExpression(indexCtx.expression());
        result = new ListIndexExpression(result, indexExpr);
      } else if (postfix instanceof Cypher25Parser.RangePostfixContext) {
        // Range slicing: expr[from..to]
        // TODO: Implement range slicing if needed
        throw new UnsupportedOperationException("Range slicing (list[from..to]) is not yet implemented");
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
            final String leftText = ctx.expression7(0).getText();
            final String rightText = ctx.expression7(1).getText();
            final Expression left = parseExpressionText(leftText);
            final Expression right = parseExpressionText(rightText);
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
      final String leftText = expr7.expression6().getText();
      final Expression leftExpr = parseExpressionText(leftText);
      final boolean isNot = ctx.NOT() != null;
      final IsNullExpression isNullExpr = new IsNullExpression(leftExpr, isNot);
      // Wrap BooleanExpression as Expression
      return new BooleanWrapperExpression(isNullExpr);
    }

    // Fallback: parse as text
    return parseExpressionText(ctx.getText());
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
          result = new ArithmeticExpression(result, op, right, ctx.getText());
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
          result = new ArithmeticExpression(result, op, right, ctx.getText());
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

    // Build right-associative expression tree for power (a ^ b ^ c = a ^ (b ^ c))
    Expression result = parseArithmeticExpression3(operands.get(operands.size() - 1));
    for (int i = operands.size() - 2; i >= 0; i--) {
      final Expression left = parseArithmeticExpression3(operands.get(i));
      result = new ArithmeticExpression(left, ArithmeticExpression.Operator.POWER, result, ctx.getText());
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
      final String key = keys.get(i).getText();
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
      final String key = keys.get(i).getText();
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
        final String key = elemCtx.propertyKeyName().getText();
        final Expression expr = parseExpression(elemCtx.expression());
        elements.add(new MapProjectionExpression.ProjectionElement(key, expr));
      } else if (elemCtx.property() != null) {
        // .propertyName
        final String propName = elemCtx.property().propertyKeyName().getText();
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
   * This ensures we don't match operators inside function calls like ID(a).
   */
  int findOperatorOutsideParentheses(final String text, final String op) {
    int parenDepth = 0;
    int bracketDepth = 0;
    boolean inString = false;
    char stringChar = 0;

    for (int i = 0; i <= text.length() - op.length(); i++) {
      final char c = text.charAt(i);

      // Track string literals
      if ((c == '\'' || c == '"') && (i == 0 || text.charAt(i - 1) != '\\')) {
        if (!inString) {
          inString = true;
          stringChar = c;
        } else if (c == stringChar) {
          inString = false;
        }
        continue;
      }

      if (inString) {
        continue;
      }

      // Track parentheses
      if (c == '(') {
        parenDepth++;
        continue;
      }
      if (c == ')') {
        parenDepth--;
        continue;
      }
      // Track brackets
      if (c == '[') {
        bracketDepth++;
        continue;
      }
      if (c == ']') {
        bracketDepth--;
        continue;
      }

      // Only match operator at top level
      if (parenDepth == 0 && bracketDepth == 0) {
        if (text.substring(i).startsWith(op)) {
          return i;
        }
      }
    }
    return -1;
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
      // Handle negative numbers
      if (text.startsWith("-") || text.startsWith("+") || Character.isDigit(text.charAt(0))) {
        if (text.contains(".")) {
          return Double.parseDouble(text);
        } else {
          return Long.parseLong(text);
        }
      }
    } catch (final NumberFormatException e) {
      // Not a number
    }

    return null; // Not a literal
  }
}
