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
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.ast.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

  // The limit in force for the parse the calling thread is running, bound by Cypher25AntlrParser around it
  // (issue #7922). CYPHER_MAX_EXPRESSION_DEPTH is declared SCOPE.DATABASE, so `ALTER DATABASE
  // 'arcadedb.cypher.maxExpressionDepth' <n>` is the documented way to raise it and is what the refusal below
  // tells the user to do; reading the enum here consulted the JVM-wide value instead, so the advice could not be
  // followed. Like REWRITE_DEPTH this has to be a ThreadLocal rather than a field: CypherASTBuilder.AST_REWRITER
  // is a single instance shared by every query on the JVM, so it cannot hold any one query's database.
  private static final ThreadLocal<Integer> MAX_DEPTH = new ThreadLocal<>();

  /**
   * The per-database expression-depth limit, or the JVM-wide default when there is no database to consult
   * (a syntax-only parse). See {@link #bindMaxExpressionDepth(int)}.
   */
  public static int maxExpressionDepth(final Database database) {
    return database == null ?
        GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH.getValueAsInteger() :
        database.getConfiguration().getValueAsInteger(GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH);
  }

  /**
   * Binds the limit {@link #rewrite} enforces for the calling thread, for the duration of one parse.
   *
   * @return the previously bound limit, to hand back to {@link #restoreMaxExpressionDepth(Integer)} in a
   * {@code finally}. Restoring rather than removing keeps a nested parse - a rewriter that parses a sub-query, or
   * a parse started from inside another - from dropping the outer parse's limit on the way out.
   */
  public static Integer bindMaxExpressionDepth(final int maxDepth) {
    final Integer previous = MAX_DEPTH.get();
    MAX_DEPTH.set(maxDepth);
    return previous;
  }

  /**
   * Restores what {@link #bindMaxExpressionDepth(int)} returned. A null clears the binding, so a thread pooled
   * between queries carries nothing over into the next one.
   */
  public static void restoreMaxExpressionDepth(final Integer maxDepth) {
    if (maxDepth == null)
      MAX_DEPTH.remove();
    else
      MAX_DEPTH.set(maxDepth);
  }

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

    final Integer bound = MAX_DEPTH.get();
    final int maxDepth = bound != null ? bound : GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH.getValueAsInteger();

    final int[] depth = REWRITE_DEPTH.get();
    if (++depth[0] > maxDepth) {
      --depth[0];
      throw new CommandParsingException(
          "Expression is too deeply nested or chained (exceeds " + maxDepth
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
   * {@code ChainedPropertyAccessExpression}, which this dispatch has no arm for. The default rewrites
   * their operands (issue #12337); they are handled here rather than given an arm of their own so a
   * fail-closed override keeps receiving them.
   */
  protected Object visitUnknown(final Object expression) {
    if (expression instanceof TernaryLogicalExpression ternary) {
      final Expression left = rewriteExpressionChild(ternary.getLeft());
      final Expression right = rewriteExpressionChild(ternary.getRight());
      if (left == ternary.getLeft() && right == ternary.getRight())
        return expression;
      return right == null ? new TernaryLogicalExpression(ternary.getOperator(), left) :
          new TernaryLogicalExpression(ternary.getOperator(), left, right);
    }
    if (expression instanceof ChainedPropertyAccessExpression chained) {
      final Expression base = rewriteExpressionChild(chained.getBaseExpression());
      return base == chained.getBaseExpression() ? expression : new ChainedPropertyAccessExpression(base, chained.getPropertyName());
    }
    return expression;
  }

  /**
   * Rewrite a boolean coercion expression (wraps an Expression as a BooleanExpression).
   * Default: recursively rewrite the wrapped expression.
   * Note: BooleanCoercionExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitBooleanCoercion(final BooleanCoercionExpression expr) {
    final Expression inner = rewriteExpressionChild(expr.getExpression());
    return inner == expr.getExpression() ? expr : new BooleanCoercionExpression(inner);
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
    final List<Expression> elements = rewriteExpressionList(expr.getElements());
    // The source text is kept: it is what names the column, as for every rewritten node
    return elements == expr.getElements() ? expr : new ListExpression(elements, expr.getText());
  }

  /**
   * Rewrite a map expression (e.g., {name: 'Alice', age: 30}).
   * Default: recursively rewrite values.
   */
  protected Expression visitMap(final MapExpression expr) {
    final Map<String, Expression> entries = expr.getEntries();
    Map<String, Expression> rewritten = null;
    for (final Map.Entry<String, Expression> entry : entries.entrySet()) {
      final Expression value = entry.getValue();
      final Expression newValue = rewriteExpressionChild(value);
      if (newValue != value) {
        if (rewritten == null)
          rewritten = new LinkedHashMap<>(entries);
        rewritten.put(entry.getKey(), newValue);
      }
    }
    return rewritten == null ? expr : new MapExpression(rewritten, expr.getText());
  }

  /**
   * Rewrite an IN expression (e.g., x IN [1, 2, 3]).
   * Default: recursively rewrite expression and list.
   * Note: InExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitIn(final InExpression expr) {
    final Expression expression = rewriteExpressionChild(expr.getExpression());
    final List<Expression> list = rewriteExpressionList(expr.getList());
    if (expression == expr.getExpression() && list == expr.getList())
      return expr;
    return new InExpression(expression, list);
  }

  /**
   * Rewrite an IS NULL expression (e.g., x IS NULL, x IS NOT NULL).
   * Default: recursively rewrite inner expression.
   * Note: IsNullExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitIsNull(final IsNullExpression expr) {
    final Expression expression = rewriteExpressionChild(expr.getExpression());
    return expression == expr.getExpression() ? expr : new IsNullExpression(expression, expr.isNot());
  }

  /**
   * Rewrite a string match expression (STARTS WITH, ENDS WITH, CONTAINS).
   * Default: recursively rewrite both sides.
   * Note: StringMatchExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitStringMatch(final StringMatchExpression expr) {
    final Expression expression = rewriteExpressionChild(expr.getExpression());
    final Expression pattern = rewriteExpressionChild(expr.getPattern());
    if (expression == expr.getExpression() && pattern == expr.getPattern())
      return expr;
    return new StringMatchExpression(expression, pattern, expr.getMatchType());
  }

  /**
   * Rewrite a CASE expression.
   * Default: recursively rewrite the operand, every WHEN and THEN, and the ELSE. The operand placeholder a comparison-form
   * WHEN reads ({@code WHEN > 5}) is a leaf, so the rebuilt alternatives keep pointing at the holder handed to the copy.
   */
  protected Expression visitCase(final CaseExpression expr) {
    final Expression operand = rewriteExpressionChild(expr.getCaseExpression());
    final Expression elseExpression = rewriteExpressionChild(expr.getElseExpression());

    final List<CaseAlternative> alternatives = expr.getAlternatives();
    List<CaseAlternative> rewritten = null;
    for (int i = 0; i < alternatives.size(); i++) {
      final CaseAlternative alternative = alternatives.get(i);
      final Expression when = rewriteExpressionChild(alternative.getWhenExpression());
      final Expression then = rewriteExpressionChild(alternative.getThenExpression());
      if (when != alternative.getWhenExpression() || then != alternative.getThenExpression()) {
        if (rewritten == null)
          rewritten = new ArrayList<>(alternatives);
        rewritten.set(i, new CaseAlternative(when, then, alternative.isPredicate() ? alternative.getWhenText() : null));
      }
    }

    if (operand == expr.getCaseExpression() && elseExpression == expr.getElseExpression() && rewritten == null)
      return expr;
    return new CaseExpression(operand, rewritten != null ? rewritten : alternatives, elseExpression, expr.getOperandHolder());
  }

  /**
   * Rewrite a list comprehension expression.
   * Default: recursively rewrite filter and map expressions.
   */
  protected Expression visitListComprehension(final ListComprehensionExpression expr) {
    final Expression list = rewriteExpressionChild(expr.getListExpression());
    final Expression where = rewriteInScope(expr.getWhereExpression(), expr.getVariable(), null);
    final Expression map = rewriteInScope(expr.getMapExpression(), expr.getVariable(), null);
    if (list == expr.getListExpression() && where == expr.getWhereExpression() && map == expr.getMapExpression())
      return expr;
    return new ListComprehensionExpression(expr.getVariable(), list, where, map, expr.getText());
  }

  /**
   * Rewrites a child evaluated in a scope that binds {@code localVariable} (and {@code secondLocalVariable} when not null),
   * such as the WHERE and the projection of {@code [x IN list WHERE ... | ...]}. The default rewrites it like any other
   * child; a rewriter that renames variables overrides it so a local that shadows, or would capture, a name it rewrites is
   * left alone.
   */
  protected Expression rewriteInScope(final Expression child, final String localVariable, final String secondLocalVariable) {
    return rewriteExpressionChild(child);
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
    final Expression initial = rewriteExpressionChild(expr.getInitialValue());
    final Expression list = rewriteExpressionChild(expr.getListExpression());
    final Expression step = rewriteInScope(expr.getReduceExpression(), expr.getAccumulatorVariable(), expr.getIteratorVariable());
    if (initial == expr.getInitialValue() && list == expr.getListExpression() && step == expr.getReduceExpression())
      return expr;
    return new ReduceExpression(expr.getAccumulatorVariable(), initial, expr.getIteratorVariable(), list, step, expr.getText());
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
    final Expression list = rewriteExpressionChild(expr.getListExpression());
    final Expression where = rewriteInScope(expr.getWhereExpression(), expr.getVariable(), null);
    if (list == expr.getListExpression() && where == expr.getWhereExpression())
      return expr;
    return new ListPredicateExpression(expr.getPredicateType(), expr.getVariable(), list, where, expr.getText());
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
    final Expression list = rewriteExpressionChild(expr.getListExpression());
    final Expression index = rewriteExpressionChild(expr.getIndexExpression());
    if (list == expr.getListExpression() && index == expr.getIndexExpression())
      return expr;
    return new ListIndexExpression(list, index);
  }

  /**
   * Rewrite a list slice expression (e.g., list[1..3]).
   * Default: recursively rewrite list and slice bounds.
   */
  protected Expression visitListSlice(final ListSliceExpression expr) {
    final Expression list = rewriteExpressionChild(expr.getListExpression());
    final Expression from = rewriteExpressionChild(expr.getFromExpression());
    final Expression to = rewriteExpressionChild(expr.getToExpression());
    if (list == expr.getListExpression() && from == expr.getFromExpression() && to == expr.getToExpression())
      return expr;
    return new ListSliceExpression(list, from, to);
  }

  /**
   * Rewrite a regex expression (e.g., x =~ '.*pattern.*').
   * Default: recursively rewrite both sides.
   * Note: RegexExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitRegex(final RegexExpression expr) {
    final Expression expression = rewriteExpressionChild(expr.getExpression());
    final Expression pattern = rewriteExpressionChild(expr.getPattern());
    if (expression == expr.getExpression() && pattern == expr.getPattern())
      return expr;
    return new RegexExpression(expression, pattern);
  }

  /**
   * Rewrite a boolean wrapper expression.
   * Default: recursively rewrite inner expression.
   */
  protected Expression visitBooleanWrapper(final BooleanWrapperExpression expr) {
    // A predicate in value position (ORDER BY n:A, RETURN n.x IS NULL) is held by this wrapper, so a rewrite that does not
    // descend here never reaches it (issue #12337)
    final BooleanExpression inner = expr.getBooleanExpression();
    final Object rewritten = rewrite(inner);
    if (rewritten != inner && rewritten instanceof BooleanExpression newInner)
      return new BooleanWrapperExpression(newInner);
    return expr;
  }

  /**
   * Rewrite a label check expression (e.g., n:Person, n:A|$(expr), n:!A).
   * Default: rewrite the subject and every dynamic {@code $(expr)} label; the static labels, the operator, the label
   * predicate and the source text are kept (issue #12337: left untouched, a label check of a variable a collapsing
   * projection renamed was not re-pointed at its output column and failed as an undefined variable).
   * Note: LabelCheckExpression implements BooleanExpression, not Expression.
   */
  protected BooleanExpression visitLabelCheck(final LabelCheckExpression expr) {
    final Expression subject = expr.getVariableExpression();
    final Expression newSubject = rewriteExpressionChild(subject);

    final List<Expression> dynamicLabels = expr.getDynamicLabels();
    final List<Expression> newDynamicLabels = rewriteExpressionList(dynamicLabels);

    if (newSubject == subject && newDynamicLabels == dynamicLabels)
      return expr;

    if (expr.getPredicate() != null)
      return new LabelCheckExpression(newSubject, expr.getPredicate(), expr.getText());
    return new LabelCheckExpression(newSubject, expr.getLabels(), newDynamicLabels, expr.getOperator(), expr.getText());
  }

  /**
   * Rewrites a child held as an {@link Expression}, keeping the original when the rewrite produced anything else (a
   * {@link BooleanExpression} that is not also an {@link Expression} cannot take its place).
   */
  protected Expression rewriteExpressionChild(final Expression child) {
    if (child == null)
      return null;
    final Object rewritten = rewrite(child);
    return rewritten instanceof Expression expression ? expression : child;
  }

  /**
   * Rewrites every element with {@link #rewriteExpressionChild}, returning the very same list when no element changed so the
   * caller can keep its node.
   */
  protected List<Expression> rewriteExpressionList(final List<Expression> elements) {
    if (elements == null)
      return null;
    List<Expression> rewritten = null;
    for (int i = 0; i < elements.size(); i++) {
      final Expression element = elements.get(i);
      final Expression newElement = rewriteExpressionChild(element);
      if (newElement != element) {
        if (rewritten == null)
          rewritten = new ArrayList<>(elements);
        rewritten.set(i, newElement);
      }
    }
    return rewritten == null ? elements : rewritten;
  }

  /**
   * Rewrite a map projection expression (e.g., n{.name, age: n.age * 2}).
   * Default: recursively rewrite projection expressions.
   */
  protected Expression visitMapProjection(final MapProjectionExpression expr) {
    final List<MapProjectionExpression.ProjectionElement> elements = expr.getElements();
    List<MapProjectionExpression.ProjectionElement> rewritten = null;
    for (int i = 0; i < elements.size(); i++) {
      final MapProjectionExpression.ProjectionElement element = elements.get(i);
      if (!element.isKeyExpression())
        continue;
      final Expression value = rewriteExpressionChild(element.getExpression());
      if (value != element.getExpression()) {
        if (rewritten == null)
          rewritten = new ArrayList<>(elements);
        rewritten.set(i, new MapProjectionExpression.ProjectionElement(element.getKey(), value));
      }
    }
    return rewritten == null ? expr : new MapProjectionExpression(expr.getVariableName(), rewritten, expr.getText());
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
