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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.query.opencypher.ast.AllReduceExpression;
import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.BooleanCoercionExpression;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.CaseAlternative;
import com.arcadedb.query.opencypher.ast.CaseExpression;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CollectExpression;
import com.arcadedb.query.opencypher.ast.CountExpression;
import com.arcadedb.query.opencypher.ast.CreateClause;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.DeleteClause;
import com.arcadedb.query.opencypher.ast.ExistsExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.ForeachClause;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.IsNullExpression;
import com.arcadedb.query.opencypher.ast.LabelCheckExpression;
import com.arcadedb.query.opencypher.ast.ListComprehensionExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.ListIndexExpression;
import com.arcadedb.query.opencypher.ast.ListPredicateExpression;
import com.arcadedb.query.opencypher.ast.ListSliceExpression;
import com.arcadedb.query.opencypher.ast.MapExpression;
import com.arcadedb.query.opencypher.ast.MapProjectionExpression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PatternComprehensionExpression;
import com.arcadedb.query.opencypher.ast.PatternPredicateExpression;
import com.arcadedb.query.opencypher.ast.ReduceExpression;
import com.arcadedb.query.opencypher.ast.RegexExpression;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.RemoveClause;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.ast.ShortestPathExpression;
import com.arcadedb.query.opencypher.ast.StringMatchExpression;
import com.arcadedb.query.opencypher.ast.TernaryLogicalExpression;
import com.arcadedb.query.opencypher.ast.UnionStatement;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.executor.steps.CallStep;
import com.arcadedb.query.opencypher.executor.steps.OrderByStep;
import com.arcadedb.query.opencypher.executor.steps.TypeCountStep;
import com.arcadedb.query.opencypher.rewriter.ExpressionRewriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Whether what follows a clause reads the graph: a later MATCH, a pattern predicate or comprehension, a subquery, a
 * function over stored records inside a SET, REMOVE, CREATE, FOREACH or RETURN. A writing clause followed by none of them
 * has nothing downstream that could observe its writes half done.
 * <p>
 * Split out of {@link CypherExecutionPlan}; it reads only the clauses it is handed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class GraphReadAnalysis {
  private GraphReadAnalysis() {
  }

  /**
   * Attempts to optimize a linear chain MATCH with {@code RETURN count(*)} into a
   * CSR count-push-down step that avoids materializing intermediate rows.
   * <p>
   * Detects pattern:
   * <pre>
   *   MATCH (a:A)-[:T1]->(b:B)-[:T2]->(c:C) ... RETURN count(*) AS alias
   * </pre>
   * <p>
   * Requirements:
   * <ul>
   *   <li>Exactly one non-optional MATCH clause with exactly one path pattern</li>
   *   <li>No WHERE clause (neither MATCH-level nor statement-level)</li>
   *   <li>RETURN has exactly one item: count(*)</li>
   *   <li>At least one relationship in the path (otherwise use TypeCountStep)</li>
   *   <li>All relationships are fixed-length and anonymous (no edge variables)</li>
   *   <li>No path variable</li>
   *   <li>No other clauses (WITH, CREATE, etc.)</li>
   * </ul>
   *
   * @return optimized CountChainPathsStep if pattern matches, null otherwise
   */
  /**
   * Checks if the clause at the given index in the clause list is followed by a RETURN clause
   * that contains only count(*) aggregations (no property access needed from procedure results).
   * Used to enable the count-only fast path in CallStep.
   */
  /**
   * Tells whether any clause after {@code clauseIndex} reads the graph, which makes the updating
   * clause at {@code clauseIndex} eager with respect to that read (issue #6922).
   * <p>
   * The pipeline pulls in batches of 100 rows, so a write step that applies one batch of writes per
   * pull lets a following reader observe a snapshot taken at an arbitrary batch boundary rather than
   * the pre- or post-write graph. openCypher resolves the same read/write conflict by making the
   * update eager; this predicate decides when to pay for that.
   * <p>
   * The set is deliberately conservative on CALL: a procedure is opaque here, so any procedure call
   * after the update counts as a read - which is what {@code CALL meta.stats()} in issue #6922 is.
   * SUBQUERY counts because a CALL { ... } block can contain a MATCH.
   * <p>
   * A later FOREACH counts only when its own body reads. MATCH and CALL are not valid inside a FOREACH
   * body at all, so that means a MERGE (at any nesting depth), or an expression that reads: the list
   * the FOREACH iterates, or - because the grammar lets a general expression stand wherever a value
   * does - a CREATE property value, a SET right-hand side, or a DELETE/REMOVE target. Those clauses
   * act on variables the row already carries, but the values they compute need not.
   * <p>
   * A terminal RETURN's ORDER BY is deliberately NOT scanned. It is parsed onto the statement rather
   * than onto the ReturnClause, and unlike the WITH case just below it is unreachable: OrderByStep
   * materializes its whole input before the comparator that evaluates those expressions ever runs, so
   * the FOREACH has already finished by then no matter what this predicate answered.
   * <p>
   * RETURN, WITH and UNWIND are not reads in themselves - they project rows the pipeline already
   * produced - but the expressions they carry can be. {@code EXISTS { }}, {@code COUNT { }} and
   * {@code COLLECT { }} run a real query against the live graph per row, and a pattern predicate,
   * pattern comprehension or shortestPath() traverses it, so those clauses are scanned for such an
   * expression instead of being waved through on their clause type alone.
   *
   * @param clausesInOrder the query's clauses in textual order
   * @param clauseIndex    index of the updating clause, or a negative value when it is not in the list
   */
  static boolean graphReadFollows(final List<ClauseEntry> clausesInOrder, final int clauseIndex) {
    if (clauseIndex < 0)
      return false;
    return clausesReadGraph(clausesInOrder, clauseIndex + 1);
  }

  /**
   * Tells whether the body of a {@code CALL { }} subquery reads the graph, on the terms of {@link #graphReadFollows}: a MATCH,
   * a MERGE, a procedure call, a nested subquery, or an expression that goes back to the graph, in any branch of a UNION. A body
   * that kept no clause order cannot be inspected and counts as a read (issue #13653).
   */
  static boolean subqueryBodyReadsGraph(final CypherStatement body) {
    if (body == null)
      return false;
    if (body instanceof UnionStatement union) {
      for (final CypherStatement branch : union.getQueries())
        if (subqueryBodyReadsGraph(branch))
          return true;
      return false;
    }
    final List<ClauseEntry> clauses = body.getClausesInOrder();
    if (clauses == null || clauses.isEmpty())
      return true;
    return clausesReadGraph(clauses, 0);
  }

  private static boolean clausesReadGraph(final List<ClauseEntry> clausesInOrder, final int fromIndex) {
    for (int i = fromIndex; i < clausesInOrder.size(); i++) {
      final ClauseEntry laterClause = clausesInOrder.get(i);
      switch (laterClause.getType()) {
      case MATCH:
      case MERGE:
      case CALL:
      case SUBQUERY:
        return true;
      case FOREACH:
        if (foreachBodyReadsGraph(laterClause.getTypedClause()))
          return true;
        break;
      case RETURN:
        if (returnItemsReadGraph(((ReturnClause) laterClause.getClause()).getReturnItems()))
          return true;
        break;
      case WITH:
        final WithClause withClause = laterClause.getTypedClause();
        if (returnItemsReadGraph(withClause.getItems()))
          return true;
        if (withClause.getWhereClause() != null
            && expressionReadsGraph(withClause.getWhereClause().getConditionExpression()))
          return true;
        if (withClause.getOrderByClause() != null)
          for (final OrderByClause.OrderByItem orderByItem : withClause.getOrderByClause().getItems())
            if (expressionReadsGraph(orderByItem.getExpressionAST()))
              return true;
        break;
      case UNWIND:
        if (expressionReadsGraph(((UnwindClause) laterClause.getClause()).getListExpression()))
          return true;
        break;
      case SET:
        if (setItemsReadGraph(laterClause.getTypedClause()))
          return true;
        break;
      case REMOVE:
        if (removeItemsReadGraph(laterClause.getTypedClause()))
          return true;
        break;
      default:
        break;
      }
    }
    return false;
  }

  /**
   * Tells whether a SET clause reads the graph. Every right-hand side counts - the assigned value, an expression
   * target, a dynamic property key, and a Cypher 25 dynamic label ({@code SET n:$(expr)}, issue #7059) - because
   * each is evaluated per row against the live graph. Shared by {@link #graphReadFollows(List, int)} and
   * {@link #foreachBodyReadsGraph(ForeachClause)} so a SET is classified the same wherever it sits.
   */
  private static boolean setItemsReadGraph(final SetClause setClause) {
    for (final SetClause.SetItem setItem : setClause.getItems())
      if (expressionReadsGraph(setItem.getValueExpression()) || expressionReadsGraph(setItem.getKeyExpression())
          || expressionReadsGraph(setItem.getTargetExpression())
          || expressionsReadGraph(setItem.getLabelExpressions()))
        return true;
    return false;
  }

  /** {@link #setItemsReadGraph(SetClause)}, for the target expression, dynamic key and dynamic label of a REMOVE clause. */
  private static boolean removeItemsReadGraph(final RemoveClause removeClause) {
    for (final RemoveClause.RemoveItem removeItem : removeClause.getItems())
      if (expressionReadsGraph(removeItem.getKeyExpression())
          || expressionReadsGraph(removeItem.getTargetExpression())
          || expressionsReadGraph(removeItem.getLabelExpressions()))
        return true;
    return false;
  }

  /**
   * Tells whether a FOREACH reads the graph: a MERGE in its body at any nesting depth, or a read in
   * the list expression it iterates over. Used by {@link #graphReadFollows(List, int)} so that a read
   * tucked inside a following FOREACH arms the eager mode just like a bare one would (issue #6922).
   */
  private static boolean foreachBodyReadsGraph(final ForeachClause foreachClause) {
    // The list a FOREACH drives off is an expression like any other: it can be a pattern
    // comprehension or hold a subquery, and it is evaluated per row against the live graph.
    if (expressionReadsGraph(foreachClause.getListExpression()))
      return true;
    for (final ClauseEntry innerClause : foreachClause.getInnerClauses()) {
      switch (innerClause.getType()) {
      case MERGE:
        return true;
      case FOREACH:
        if (foreachBodyReadsGraph(innerClause.getTypedClause()))
          return true;
        break;
      case CREATE:
        if (createValuesReadGraph(innerClause.getTypedClause()))
          return true;
        break;
      case SET:
        if (setItemsReadGraph(innerClause.getTypedClause()))
          return true;
        break;
      case REMOVE:
        if (removeItemsReadGraph(innerClause.getTypedClause()))
          return true;
        break;
      case DELETE:
        for (final Expression deleteExpression : ((DeleteClause) innerClause.getClause()).getExpressions())
          if (expressionReadsGraph(deleteExpression))
            return true;
        break;
      default:
        break;
      }
    }
    return false;
  }

  /**
   * Tells whether a CREATE inside a FOREACH body reads the graph through one of its property values.
   * <p>
   * The pattern itself creates rather than matches, but the grammar lets a general expression stand
   * as a property value, and a general expression can be {@code count { ... }}: in
   * {@code FOREACH (x IN [1] | CREATE (:S {n: count {{ MATCH (m:L1) }}}))} the value is evaluated per
   * row against the live graph, which makes the FOREACH before it a reader after all.
   */
  private static boolean createValuesReadGraph(final CreateClause createClause) {
    for (final PathPattern pathPattern : createClause.getPathPatterns()) {
      for (final NodePattern nodePattern : pathPattern.getNodes())
        if (propertyValuesReadGraph(nodePattern.getProperties()) || expressionsReadGraph(nodePattern.getDynamicLabels()))
          return true;
      for (final RelationshipPattern relationshipPattern : pathPattern.getRelationships())
        if (propertyValuesReadGraph(relationshipPattern.getProperties()))
          return true;
    }
    return false;
  }

  /**
   * Tells whether any value of an inline property map reads the graph. The map holds already-evaluated
   * literals alongside unevaluated {@code Expression} nodes, so every value is offered to the detector
   * and a non-expression simply answers false.
   */
  private static boolean propertyValuesReadGraph(final Map<String, Object> properties) {
    if (properties == null)
      return false;
    for (final Object value : properties.values())
      if (expressionReadsGraph(value))
        return true;
    return false;
  }

  private static boolean expressionsReadGraph(final List<Expression> expressions) {
    if (expressions == null)
      return false;
    for (final Expression expression : expressions)
      if (expressionReadsGraph(expression))
        return true;
    return false;
  }

  private static boolean returnItemsReadGraph(final List<ReturnClause.ReturnItem> returnItems) {
    for (final ReturnClause.ReturnItem returnItem : returnItems)
      if (expressionReadsGraph(returnItem.getExpression()))
        return true;
    return false;
  }

  /**
   * Tells whether an expression tree holds anything that goes back to the graph while the row is
   * being projected: a subquery expression ({@code EXISTS { }}, {@code COUNT { }}, {@code COLLECT { }},
   * which all run a real query per row through {@code CorrelatedSubqueryRunner}) or a pattern that is
   * traversed ({@code (n)-->()} as a predicate, a pattern comprehension, {@code shortestPath()}).
   * Used by {@link #graphReadFollows(List, int)} for issue #6922.
   */
  private static boolean expressionReadsGraph(final Object expression) {
    if (expression == null)
      return false;
    final GraphReadDetector detector = new GraphReadDetector();
    detector.rewrite(expression);
    return detector.found;
  }

  /**
   * Walks an expression tree looking for a node that reads the graph, reusing {@link ExpressionRewriter}'s
   * traversal instead of re-deriving one: the check sits in the {@code rewrite} entry point every node
   * passes through, so it also catches the expression types the rewriter's own dispatch does not know
   * (COUNT and COLLECT subqueries among them). Nothing is rewritten - every visit returns its input.
   * <p>
   * Eighteen of the base class's visits return without recursing into what they wrap, and each one can
   * hide a real case here: {@code size(collect {{ ... }})} behind {@code visitFunctionCall}, and
   * {@code WHERE exists {{ ... }}} behind the boolean coercion/wrapper pair. Every one is overridden
   * below rather than filled in on the shared base class, where every other rewriter would inherit a
   * traversal change none of them asked for. {@code visitUnknown} then closes the remaining hole: a
   * type the base dispatch does not know at all counts as a read rather than as a leaf.
   */
  private static final class GraphReadDetector extends ExpressionRewriter {
    private boolean found = false;

    @Override
    public Object rewrite(final Object expression) {
      if (found || expression == null)
        return expression;
      if (expression instanceof ExistsExpression || expression instanceof CountExpression
          || expression instanceof CollectExpression || expression instanceof PatternPredicateExpression
          || expression instanceof PatternComprehensionExpression || expression instanceof ShortestPathExpression) {
        found = true;
        return expression;
      }
      // The general expression parser builds this for a top-level AND/OR/XOR/NOT, so it is far too
      // common to leave to the conservative visitUnknown default: doing so would arm the eager mode
      // behind every `RETURN a AND b` following a FOREACH, whether or not anything there reads.
      if (expression instanceof TernaryLogicalExpression ternary) {
        rewrite(ternary.getLeft());
        if (!found)
          rewrite(ternary.getRight());
        return expression;
      }
      return super.rewrite(expression);
    }

    /**
     * Fails CLOSED where the base class fails open: an expression type {@link ExpressionRewriter}'s
     * dispatch does not know is treated as a possible read rather than waved through.
     * <p>
     * For the rewriters the base class was written for, an unreached subtree costs an optimization.
     * Here it costs correctness - a missed read leaves the FOREACH streaming and the query answers
     * from a batch-boundary snapshot - so the two defaults have to differ. This also covers
     * {@code ChainedPropertyAccessExpression} (which the base dispatch has no arm for) and any
     * {@code Expression} subtype added later, which would otherwise each need their own fix to this class.
     */
    @Override
    protected Object visitUnknown(final Object expression) {
      found = true;
      return expression;
    }

    @Override
    protected Expression visitFunctionCall(final FunctionCallExpression expr) {
      return descend(expr, expr.getArguments());
    }

    @Override
    protected BooleanExpression visitBooleanCoercion(final BooleanCoercionExpression expr) {
      return descend(expr, expr.getExpression());
    }

    @Override
    protected Expression visitBooleanWrapper(final BooleanWrapperExpression expr) {
      return descend(expr, expr.getBooleanExpression());
    }

    @Override
    protected Expression visitCase(final CaseExpression expr) {
      rewrite(expr.getCaseExpression());
      for (final CaseAlternative alternative : expr.getAlternatives()) {
        rewrite(alternative.getWhenExpression());
        rewrite(alternative.getThenExpression());
      }
      return descend(expr, expr.getElseExpression());
    }

    @Override
    protected Expression visitList(final ListExpression expr) {
      return descend(expr, expr.getElements());
    }

    @Override
    protected Expression visitMap(final MapExpression expr) {
      return descend(expr, expr.getEntries().values());
    }

    @Override
    protected Expression visitMapProjection(final MapProjectionExpression expr) {
      for (final MapProjectionExpression.ProjectionElement element : expr.getElements())
        rewrite(element.getExpression());
      return expr;
    }

    @Override
    protected Expression visitListComprehension(final ListComprehensionExpression expr) {
      rewrite(expr.getListExpression());
      rewrite(expr.getWhereExpression());
      return descend(expr, expr.getMapExpression());
    }

    @Override
    protected Expression visitListPredicate(final ListPredicateExpression expr) {
      rewrite(expr.getListExpression());
      return descend(expr, expr.getWhereExpression());
    }

    @Override
    protected Expression visitReduce(final ReduceExpression expr) {
      rewrite(expr.getInitialValue());
      rewrite(expr.getListExpression());
      return descend(expr, expr.getReduceExpression());
    }

    @Override
    protected Expression visitAllReduce(final AllReduceExpression expr) {
      rewrite(expr.getInitialValue());
      rewrite(expr.getListExpression());
      rewrite(expr.getReduceExpression());
      return descend(expr, expr.getPredicateExpression());
    }

    @Override
    protected Expression visitListIndex(final ListIndexExpression expr) {
      rewrite(expr.getListExpression());
      return descend(expr, expr.getIndexExpression());
    }

    @Override
    protected Expression visitListSlice(final ListSliceExpression expr) {
      rewrite(expr.getListExpression());
      rewrite(expr.getFromExpression());
      return descend(expr, expr.getToExpression());
    }

    @Override
    protected Expression visitArithmetic(final ArithmeticExpression expr) {
      rewrite(expr.getLeft());
      return descend(expr, expr.getRight());
    }

    @Override
    protected BooleanExpression visitIn(final InExpression expr) {
      rewrite(expr.getExpression());
      return descend(expr, expr.getList());
    }

    @Override
    protected BooleanExpression visitIsNull(final IsNullExpression expr) {
      return descend(expr, expr.getExpression());
    }

    @Override
    protected BooleanExpression visitStringMatch(final StringMatchExpression expr) {
      rewrite(expr.getExpression());
      return descend(expr, expr.getPattern());
    }

    @Override
    protected BooleanExpression visitRegex(final RegexExpression expr) {
      rewrite(expr.getExpression());
      return descend(expr, expr.getPattern());
    }

    @Override
    protected BooleanExpression visitLabelCheck(final LabelCheckExpression expr) {
      for (final Expression dynamicLabel : expr.getDynamicLabels())
        rewrite(dynamicLabel);
      return descend(expr, expr.getVariableExpression());
    }

    private <T> T descend(final T expr, final Object child) {
      rewrite(child);
      return expr;
    }

    private <T> T descend(final T expr, final Collection<? extends Object> children) {
      for (final Object child : children)
        rewrite(child);
      return expr;
    }
  }
}
