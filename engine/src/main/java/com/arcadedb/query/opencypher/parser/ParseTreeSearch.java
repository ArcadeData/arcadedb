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

import com.arcadedb.query.opencypher.ast.*;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import java.util.List;

/**
 * Finds, in the parse tree of an expression, the node of one grammar rule: the first list literal, function call, map
 * or comprehension below a node, or the operator that sits at the top level of the expression rather than inside a
 * parenthesis or an argument. They read the tree and build nothing.
 * <p>
 * Split out of {@link CypherExpressionBuilder}, which decides what an expression is and builds it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ParseTreeSearch {
  private ParseTreeSearch() {
  }

  /**
   * Recursively find ParenthesizedExpressionContext in the parse tree.
   */
  static Cypher25Parser.ParenthesizedExpressionContext findParenthesizedExpressionRecursive(final ParseTree node) {
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
   * Recursively find list literal context in the parse tree.
   */
  static Cypher25Parser.ListLiteralContext findListLiteralRecursive(final ParseTree node) {
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
  static Cypher25Parser.ListLiteralContext findListLiteral(final ParseTree node) {
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
  static Cypher25Parser.FunctionInvocationContext findFunctionInvocationRecursive(
      final ParseTree node) {
    if (node == null) {
      return null;
    }

    if (node instanceof Cypher25Parser.FunctionInvocationContext) {
      return (Cypher25Parser.FunctionInvocationContext) node;
    }

    // Do NOT recurse into map literals or map projections - they should be recognized
    // separately and their internal expressions (including function calls) parsed later.
    // Otherwise a computed field like {upperName: toUpper(p.name)} would be parsed as
    // a bare function call and the surrounding projection would be lost.
    if (node instanceof Cypher25Parser.MapContext
        || node instanceof Cypher25Parser.MapProjectionContext) {
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
   * Recursively find trimFunction in the parse tree.
   */
  static Cypher25Parser.TrimFunctionContext findTrimFunctionRecursive(final ParseTree node) {
    if (node == null)
      return null;
    if (node instanceof Cypher25Parser.TrimFunctionContext)
      return (Cypher25Parser.TrimFunctionContext) node;
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.TrimFunctionContext found = findTrimFunctionRecursive(node.getChild(i));
      if (found != null)
        return found;
    }
    return null;
  }

  /**
   * Recursively find EXISTS expression in the parse tree.
   */
  static Cypher25Parser.ExistsExpressionContext findExistsExpressionRecursive(
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
   * Recursively find Expression8 context (handles comparison operators).
   */
  static Cypher25Parser.Expression8Context findExpression8Recursive(
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
  static Cypher25Parser.Expression8Context findTopLevelExpression8(final Cypher25Parser.ExpressionContext ctx) {
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
  static Cypher25Parser.Expression7Context findTopLevelExpression7(final Cypher25Parser.ExpressionContext ctx) {
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
  static Cypher25Parser.Expression6Context findTopLevelExpression6(final Cypher25Parser.ExpressionContext ctx) {
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
  static Cypher25Parser.Expression5Context findTopLevelExpression5(final Cypher25Parser.ExpressionContext ctx) {
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
  static Cypher25Parser.Expression4Context findTopLevelExpression4(final Cypher25Parser.ExpressionContext ctx) {
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
  static Cypher25Parser.Expression3Context findTopLevelExpression3(final Cypher25Parser.ExpressionContext ctx) {
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
  static Cypher25Parser.Expression2Context findTopLevelExpression2(final Cypher25Parser.ExpressionContext ctx) {
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
  static Cypher25Parser.Expression7Context findExpression7WithComparisonRecursive(final ParseTree node) {
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
  static Cypher25Parser.NullComparisonContext findNullComparisonRecursive(
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
   * Recursively find TypeComparison context (IS [NOT] TYPED type / :: type).
   */
  static Cypher25Parser.TypeComparisonContext findTypeComparisonRecursive(final ParseTree node) {
    if (node == null)
      return null;
    if (node instanceof Cypher25Parser.TypeComparisonContext)
      return (Cypher25Parser.TypeComparisonContext) node;
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.TypeComparisonContext found = findTypeComparisonRecursive(node.getChild(i));
      if (found != null)
        return found;
    }
    return null;
  }

  /**
   * Recursively find Expression6Context with arithmetic operators (+ - ||)
   */
  static Cypher25Parser.Expression6Context findArithmeticExpression6Recursive(
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
  static Cypher25Parser.Expression5Context findArithmeticExpression5Recursive(
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
   * <p>
   * Does NOT recurse into a map projection either: a map literal inside one ({@code n { c: { e: 1 } }}) is the value of
   * one of its entries, and the projection is what the expression is (issue #11112). {@link #findMapProjectionRecursive}
   * stops at a map literal for the same reason, so between the two the OUTERMOST of them is the one found.
   */
  static Cypher25Parser.MapContext findMapRecursive(final ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.MapContext)
      return (Cypher25Parser.MapContext) node;

    // Do NOT recurse into function invocations - they should be recognized separately
    // and their internal arguments (including map literals) parsed later
    if (node instanceof Cypher25Parser.FunctionInvocationContext || node instanceof Cypher25Parser.MapProjectionContext)
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
  static Cypher25Parser.ListComprehensionContext findListComprehensionRecursive(
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
   * Recursively find ListItemsPredicateContext in the parse tree.
   */
  static Cypher25Parser.ListItemsPredicateContext findListItemsPredicateRecursive(final ParseTree node) {
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
   * Recursively find ReduceExpressionContext in the parse tree.
   */
  static Cypher25Parser.ReduceExpressionContext findReduceExpressionRecursive(
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
   * Recursively find AllReduceExpressionContext in the parse tree.
   */
  static Cypher25Parser.AllReduceExpressionContext findAllReduceExpressionRecursive(
      final ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.AllReduceExpressionContext)
      return (Cypher25Parser.AllReduceExpressionContext) node;

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.AllReduceExpressionContext found = findAllReduceExpressionRecursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Recursively find MapProjectionContext in the parse tree.
   * <p>
   * Does NOT recurse into a map literal: a projection inside one ({@code { a: n { .k }, b: 1 }}) is the value of one of
   * its entries, and answering it for the whole expression dropped the literal's keys and every other entry (issue
   * #11112). {@link #findMapRecursive} stops at a projection for the same reason.
   */
  static Cypher25Parser.MapProjectionContext findMapProjectionRecursive(
      final ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.MapProjectionContext)
      return (Cypher25Parser.MapProjectionContext) node;

    if (node instanceof Cypher25Parser.MapContext)
      return null;

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
  static Cypher25Parser.Expression2Context findExpression2Recursive(final ParseTree node) {
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

  /**
   * Recursively find a {@link Cypher25Parser.PatternExpressionContext} within the given parse tree,
   * used to detect a graph pattern passed to the function-style {@code exists(pattern)} predicate.
   */
  static Cypher25Parser.PatternExpressionContext findPatternExpressionRecursive(final ParseTree node) {
    if (node instanceof Cypher25Parser.PatternExpressionContext)
      return (Cypher25Parser.PatternExpressionContext) node;
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.PatternExpressionContext found = findPatternExpressionRecursive(node.getChild(i));
      if (found != null)
        return found;
    }
    return null;
  }
}
