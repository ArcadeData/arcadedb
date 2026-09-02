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
package com.arcadedb.query.opencypher.rewriter;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.ast.*;

/**
 * Abstract base class for AST expression rewriters.
 * Provides a visitor pattern for traversing and transforming expression trees.
 *
 * AST nodes are immutable, so rewriting creates new instances with transformed children.
 * Subclasses override specific visit methods to implement rewrite rules.
 *
 * Design:
 * - Default implementation recursively rewrites children and creates new nodes
 * - Subclasses override methods to apply specific transformations
 * - Returns original expression if no transformation applied
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class ExpressionRewriter {

  // Bounds the recursion depth of rewrite()/visitXxx() so a pathologically deep or long expression tree
  // (e.g. tens of thousands of chained OR/AND terms, or string-concatenation terms) fails with a normal
  // CommandParsingException instead of a StackOverflowError - see issue #5851. A ThreadLocal is required
  // rather than a plain instance field because CypherASTBuilder.AST_REWRITER is a single ExpressionRewriter
  // instance shared and visited concurrently by every query on the JVM.
  private static final ThreadLocal<int[]> REWRITE_DEPTH = ThreadLocal.withInitial(() -> new int[1]);

  /**
   * Main entry point: rewrite an expression or boolean expression.
   * Dispatches to specific visit methods based on expression type.
   *
   * Returns either Expression or BooleanExpression depending on input type.
   * Note: Expression and BooleanExpression are separate interfaces in Cypher AST.
   */
  public Object rewrite(final Object expression) {
    if (expression == null)
      return null;

    final int[] depth = REWRITE_DEPTH.get();
    if (++depth[0] > GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH.getValueAsInteger()) {
      --depth[0];
      throw new CommandParsingException(
          "Expression is too deeply nested or chained (exceeds " + GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH.getValueAsInteger()
              + " levels). This protects the server against a stack overflow from a pathologically nested or long "
              + "expression, for example thousands of chained AND/OR terms; raise 'arcadedb.cypher.maxExpressionDepth' "
              + "if this is a legitimate query.");
    }
    try {
      return rewriteDispatch(expression);
    } finally {
      --depth[0];
    }
  }

  private Object rewriteDispatch(final Object expression) {
    // Dispatch to specific visit methods based on concrete type
    // BooleanExpression implementers
    if (expression instanceof BooleanCoercionExpression)
      return visitBooleanCoercion((BooleanCoercionExpression) expression);
    if (expression instanceof ComparisonExpression)
      return visitComparison((ComparisonExpression) expression);
    if (expression instanceof LogicalExpression)
      return visitLogical((LogicalExpression) expression);
    if (expression instanceof InExpression)
      return visitIn((InExpression) expression);
    if (expression instanceof IsNullExpression)
      return visitIsNull((IsNullExpression) expression);
    if (expression instanceof StringMatchExpression)
      return visitStringMatch((StringMatchExpression) expression);
    if (expression instanceof RegexExpression)
      return visitRegex((RegexExpression) expression);
    if (expression instanceof LabelCheckExpression)
      return visitLabelCheck((LabelCheckExpression) expression);
    if (expression instanceof PatternPredicateExpression)
      return visitPatternPredicate((PatternPredicateExpression) expression);

    // Expression implementers
    if (expression instanceof ArithmeticExpression)
      return visitArithmetic((ArithmeticExpression) expression);
    if (expression instanceof FunctionCallExpression)
      return visitFunctionCall((FunctionCallExpression) expression);
    if (expression instanceof PropertyAccessExpression)
      return visitPropertyAccess((PropertyAccessExpression) expression);
    if (expression instanceof VariableExpression)
      return visitVariable((VariableExpression) expression);
    if (expression instanceof LiteralExpression)
      return visitLiteral((LiteralExpression) expression);
    if (expression instanceof ListExpression)
      return visitList((ListExpression) expression);
    if (expression instanceof MapExpression)
      return visitMap((MapExpression) expression);
    if (expression instanceof CaseExpression)
      return visitCase((CaseExpression) expression);
    if (expression instanceof ListComprehensionExpression)
      return visitListComprehension((ListComprehensionExpression) expression);
    if (expression instanceof PatternComprehensionExpression)
      return visitPatternComprehension((PatternComprehensionExpression) expression);
    if (expression instanceof ReduceExpression)
      return visitReduce((ReduceExpression) expression);
    if (expression instanceof AllReduceExpression)
      return visitAllReduce((AllReduceExpression) expression);
    if (expression instanceof ListPredicateExpression)
      return visitListPredicate((ListPredicateExpression) expression);
    if (expression instanceof ExistsExpression)
      return visitExists((ExistsExpression) expression);
    if (expression instanceof ListIndexExpression)
      return visitListIndex((ListIndexExpression) expression);
    if (expression instanceof ListSliceExpression)
      return visitListSlice((ListSliceExpression) expression);
    if (expression instanceof BooleanWrapperExpression)
      return visitBooleanWrapper((BooleanWrapperExpression) expression);
    if (expression instanceof MapProjectionExpression)
      return visitMapProjection((MapProjectionExpression) expression);
    if (expression instanceof ParameterExpression)
      return visitParameter((ParameterExpression) expression);
    if (expression instanceof ShortestPathExpression)
      return visitShortestPath((ShortestPathExpression) expression);
    if (expression instanceof StarExpression)
      return visitStar((StarExpression) expression);

    // Unknown expression type: hand it to the fail-open/fail-closed hook below.
    return visitUnknown(expression);
  }

  /**
   * Handles an expression whose concrete type {@code rewriteDispatch} does not know.
   * <p>
   * The default is to fail OPEN - return the node untouched, without descending into whatever it
   * wraps - which is right for a rewriter: a rewrite that does not reach a subtree merely misses an
   * optimization. A subclass whose answer is a SAFETY property rather than an optimization must
   * override this and fail CLOSED, because for it a subtree the walk never reached is a wrong answer
   * and not a missed one. {@code CypherExecutionPlan.GraphReadDetector} is such a subclass.
   * <p>
   * Two live types reach this today: {@code TernaryLogicalExpression}, which the general expression
   * parser builds for a top-level {@code AND}/{@code OR}/{@code XOR}/{@code NOT}, and
   * {@code CypherExpressionBuilder.ChainedPropertyAccessExpression}, which is package-private and so
   * cannot be dispatched on from here at all.
   */
  protected Object visitUnknown(final Object expression) {
    return expression;
  }

  /**
   * Rewrite a boolean coercion expression (wraps an Expression as a BooleanExpression).
   * Default: return as-is (the wrapped expression is a leaf-level value).
   * Note: BooleanCoercionExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitBooleanCoercion(final BooleanCoercionExpression expr) {
    return expr;
  }

  /**
   * Rewrite a comparison expression (=, <>, <, >, <=, >=).
   * Default: recursively rewrite children.
   * Note: ComparisonExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitComparison(final ComparisonExpression expr) {
    final Expression newLeft = (Expression) rewrite(expr.getLeft());
    final Expression newRight = (Expression) rewrite(expr.getRight());

    if (newLeft != expr.getLeft() || newRight != expr.getRight())
      return new ComparisonExpression(newLeft, expr.getOperator(), newRight);

    return expr;
  }

  /**
   * Rewrite a logical expression (AND, OR, NOT, XOR).
   * Default: recursively rewrite children.
   * Note: LogicalExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitLogical(final LogicalExpression expr) {
    final BooleanExpression newLeft = (BooleanExpression) rewrite(expr.getLeft());
    final BooleanExpression newRight = expr.getRight() != null ? (BooleanExpression) rewrite(expr.getRight()) : null;

    if (newLeft != expr.getLeft() || newRight != expr.getRight())
      return new LogicalExpression(expr.getOperator(), newLeft, newRight);

    return expr;
  }

  /**
   * Rewrite an arithmetic expression (+, -, *, /, %, ^).
   * Default: recursively rewrite children.
   */
  protected Expression visitArithmetic(final ArithmeticExpression expr) {
    final Expression newLeft = (Expression) rewrite(expr.getLeft());
    final Expression newRight = (Expression) rewrite(expr.getRight());

    if (newLeft != expr.getLeft() || newRight != expr.getRight())
      return new ArithmeticExpression(newLeft, expr.getOperator(), newRight);

    return expr;
  }

  /**
   * Rewrite a function call expression.
   * Default: recursively rewrite arguments.
   */
  protected Expression visitFunctionCall(final FunctionCallExpression expr) {
    // TODO: Implement when FunctionCallExpression exposes getArguments()
    return expr;
  }

  /**
   * Rewrite a property access expression (e.g., n.name).
   * Default: recursively rewrite base expression.
   */
  protected Expression visitPropertyAccess(final PropertyAccessExpression expr) {
    // TODO: Implement when PropertyAccessExpression exposes getBase()
    return expr;
  }

  /**
   * Rewrite a variable expression (e.g., n, x, count).
   * Default: return as-is (variables are leaves).
   */
  protected Expression visitVariable(final VariableExpression expr) {
    return expr;
  }

  /**
   * Rewrite a literal expression (e.g., 42, 'hello', true).
   * Default: return as-is (literals are leaves).
   */
  protected Expression visitLiteral(final LiteralExpression expr) {
    return expr;
  }

  /**
   * Rewrite a list expression (e.g., [1, 2, 3]).
   * Default: recursively rewrite elements.
   */
  protected Expression visitList(final ListExpression expr) {
    // TODO: Implement when ListExpression exposes getElements()
    return expr;
  }

  /**
   * Rewrite a map expression (e.g., {name: 'Alice', age: 30}).
   * Default: recursively rewrite values.
   */
  protected Expression visitMap(final MapExpression expr) {
    // TODO: Implement when MapExpression exposes getEntries()
    return expr;
  }

  /**
   * Rewrite an IN expression (e.g., x IN [1, 2, 3]).
   * Default: recursively rewrite expression and list.
   * Note: InExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitIn(final InExpression expr) {
    // TODO: Implement when InExpression exposes getExpression() and getList()
    return expr;
  }

  /**
   * Rewrite an IS NULL expression (e.g., x IS NULL, x IS NOT NULL).
   * Default: recursively rewrite inner expression.
   * Note: IsNullExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitIsNull(final IsNullExpression expr) {
    // TODO: Implement when IsNullExpression exposes getExpression()
    return expr;
  }

  /**
   * Rewrite a string match expression (STARTS WITH, ENDS WITH, CONTAINS).
   * Default: recursively rewrite both sides.
   * Note: StringMatchExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitStringMatch(final StringMatchExpression expr) {
    // TODO: Implement when StringMatchExpression exposes getLeft() and getRight()
    return expr;
  }

  /**
   * Rewrite a CASE expression.
   * Default: recursively rewrite all alternatives and else expression.
   */
  protected Expression visitCase(final CaseExpression expr) {
    // TODO: Implement when CaseExpression exposes alternatives
    return expr;
  }

  /**
   * Rewrite a list comprehension expression.
   * Default: recursively rewrite filter and map expressions.
   */
  protected Expression visitListComprehension(final ListComprehensionExpression expr) {
    // TODO: Implement when ListComprehensionExpression exposes expressions
    return expr;
  }

  /**
   * Rewrite a pattern comprehension expression.
   * Default: recursively rewrite projection expression.
   */
  protected Expression visitPatternComprehension(final PatternComprehensionExpression expr) {
    // TODO: Implement when PatternComprehensionExpression exposes projection
    return expr;
  }

  /**
   * Rewrite a reduce expression.
   * Default: recursively rewrite initial value and accumulator expression.
   */
  protected Expression visitReduce(final ReduceExpression expr) {
    // TODO: Implement when ReduceExpression exposes expressions
    return expr;
  }

  /**
   * Rewrite an allReduce expression.
   * Default: return as-is.
   */
  protected Expression visitAllReduce(final AllReduceExpression expr) {
    return expr;
  }

  /**
   * Rewrite a list predicate expression (all, any, none, single).
   * Default: recursively rewrite predicate expression.
   */
  protected Expression visitListPredicate(final ListPredicateExpression expr) {
    // TODO: Implement when ListPredicateExpression exposes predicate
    return expr;
  }

  /**
   * Rewrite an EXISTS expression.
   * Default: return as-is (contains pattern, not expression children).
   */
  protected Expression visitExists(final ExistsExpression expr) {
    return expr;
  }

  /**
   * Rewrite a list index expression (e.g., list[0]).
   * Default: recursively rewrite list and index expressions.
   */
  protected Expression visitListIndex(final ListIndexExpression expr) {
    // TODO: Implement when ListIndexExpression exposes getList() and getIndex()
    return expr;
  }

  /**
   * Rewrite a list slice expression (e.g., list[1..3]).
   * Default: recursively rewrite list and slice bounds.
   */
  protected Expression visitListSlice(final ListSliceExpression expr) {
    // TODO: Implement when ListSliceExpression exposes expressions
    return expr;
  }

  /**
   * Rewrite a regex expression (e.g., x =~ '.*pattern.*').
   * Default: recursively rewrite both sides.
   * Note: RegexExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitRegex(final RegexExpression expr) {
    // TODO: Implement when RegexExpression exposes getExpression() and getPattern()
    return expr;
  }

  /**
   * Rewrite a boolean wrapper expression.
   * Default: recursively rewrite inner expression.
   */
  protected Expression visitBooleanWrapper(final BooleanWrapperExpression expr) {
    // TODO: Implement when BooleanWrapperExpression exposes getInner()
    return expr;
  }

  /**
   * Rewrite a label check expression (e.g., n:Person).
   * Default: recursively rewrite base expression.
   * Note: LabelCheckExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitLabelCheck(final LabelCheckExpression expr) {
    // TODO: Implement when LabelCheckExpression exposes getExpression()
    return expr;
  }

  /**
   * Rewrite a map projection expression (e.g., n{.name, age: n.age * 2}).
   * Default: recursively rewrite projection expressions.
   */
  protected Expression visitMapProjection(final MapProjectionExpression expr) {
    // TODO: Implement when MapProjectionExpression exposes projections
    return expr;
  }

  /**
   * Rewrite a parameter expression (e.g., $param).
   * Default: return as-is (parameters are leaves).
   */
  protected Expression visitParameter(final ParameterExpression expr) {
    return expr;
  }

  /**
   * Rewrite a pattern predicate expression.
   * Default: return as-is (contains pattern, not expression children).
   * Note: PatternPredicateExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitPatternPredicate(final PatternPredicateExpression expr) {
    return expr;
  }

  /**
   * Rewrite a shortest path expression.
   * Default: return as-is (contains pattern, not expression children).
   */
  protected Expression visitShortestPath(final ShortestPathExpression expr) {
    return expr;
  }

  /**
   * Rewrite a star expression (e.g., count(*)).
   * Default: return as-is (star is a leaf).
   */
  protected Expression visitStar(final StarExpression expr) {
    return expr;
  }
}
