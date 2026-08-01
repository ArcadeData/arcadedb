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

import com.arcadedb.query.opencypher.ast.AllReduceExpression;
import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.BooleanCoercionExpression;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.CallClause;
import com.arcadedb.query.opencypher.ast.CaseAlternative;
import com.arcadedb.query.opencypher.ast.CaseExpression;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CollectExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpressionWrapper;
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
import com.arcadedb.query.opencypher.ast.IsTypedExpression;
import com.arcadedb.query.opencypher.ast.LabelCheckExpression;
import com.arcadedb.query.opencypher.ast.ListComprehensionExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.ListIndexExpression;
import com.arcadedb.query.opencypher.ast.ListPredicateExpression;
import com.arcadedb.query.opencypher.ast.ListSliceExpression;
import com.arcadedb.query.opencypher.ast.LoadCSVClause;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.MapExpression;
import com.arcadedb.query.opencypher.ast.MapProjectionExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.MergeClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PatternComprehensionExpression;
import com.arcadedb.query.opencypher.ast.PatternPredicateExpression;
import com.arcadedb.query.opencypher.ast.ReduceExpression;
import com.arcadedb.query.opencypher.ast.RegexExpression;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.ast.ShortestPathExpression;
import com.arcadedb.query.opencypher.ast.StringMatchExpression;
import com.arcadedb.query.opencypher.ast.SubqueryClause;
import com.arcadedb.query.opencypher.ast.TernaryLogicalExpression;
import com.arcadedb.query.opencypher.ast.UnionStatement;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.ast.WithClause;

import java.util.List;
import java.util.Map;

/**
 * Visits every expression a statement contains, wherever it appears: inside another expression, inside a predicate,
 * inside a graph pattern, and inside the body of a nested subquery.
 * <p>
 * Written for {@link CypherSemanticValidator}, whose parse-time checks each used to carry their own partial recursion:
 * one that only descended into a function's arguments, another that only knew about four expression shapes. A single
 * traversal means a check written once applies wherever an expression can appear, which is what let the argument
 * validation of #5484 - previously reaching only {@code RETURN} and {@code WITH} items - extend to {@code WHERE},
 * {@code UNWIND}, {@code SET}, {@code CREATE} and {@code MERGE} without each clause growing its own walker
 * (issue #5602).
 * <p>
 * The visitor is called on every expression node including the root, parents before children.
 * <p>
 * <b>Subquery bodies are part of the walk.</b> A {@code CALL { ... }} clause and the three subquery expressions -
 * {@code EXISTS { ... }}, {@code COUNT { ... }}, {@code COLLECT { ... }} - each hold a statement of their own, and the
 * walk descends into it. Until issue #5626 it did not, and this class asserted the opposite: that those bodies were
 * "parsed on their own and validated then". They were not - the three keep their body as text and re-parse it once per
 * outer row, where a failure is absorbed into the expression's neutral value, and a {@code CALL} body was never handed
 * to this phase at all. So {@code WHERE EXISTS { MATCH (m) WHERE abs('x') > 0 RETURN m }} was accepted while the
 * identical call written in the outer {@code WHERE} was rejected before the query ran.
 * <p>
 * Crossing into a body changes the variable scope, so the descent goes through
 * {@link Visitor#forNestedStatement(CypherStatement)}: a check that reads variable kinds re-binds itself to the inner
 * scope there, and one that does not simply keeps the same visitor.
 * <p>
 * <b>Add a case here when introducing an expression type that nests other expressions.</b> The {@code default} arms
 * treat an unrecognised type as a leaf, which is right for a literal or a variable but silently hides whatever a new
 * composite type contains: everything inside it would escape every check that runs through this walker, with nothing
 * failing to say so.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CypherExpressionWalker {

  /**
   * Receives every expression the walk reaches, and decides what happens at the boundary of a nested statement.
   */
  @FunctionalInterface
  public interface Visitor {
    /**
     * Called once per expression node, parents before children. Once means once - a visitor that accumulates, a
     * counter or a collector, is safe - and it is structural rather than guarded: each arm of the walk covers a part
     * of the tree no other arm does. Preserve that when adding an arm; walking a clause from two places is how the
     * property gets lost.
     */
    void visit(Expression expression);

    /**
     * Called once per graph pattern the walk reaches, wherever it was written: a {@code MATCH}, a {@code CREATE} or
     * {@code MERGE}, a pattern predicate in a {@code WHERE}, a pattern comprehension, a {@code shortestPath}. For a
     * check about the shape of a pattern rather than about an expression inside it - two relationships sharing a
     * variable, say - this is the hook, and using it is what keeps the check from applying only to the one clause
     * whose patterns someone remembered to iterate.
     */
    default void visitPattern(final PathPattern pattern) {
      // Most visitors are about expressions.
    }

    /**
     * Called once per predicate node the walk reaches - the condition of a {@code WHERE}, an inline pattern
     * predicate, and every predicate nested inside one. {@link #visit} does not see these: a
     * {@link BooleanExpression} is not an {@link Expression}, and the walk descends through a predicate into the
     * expressions it holds without ever handing the predicate itself to a visitor.
     * <p>
     * A check about expressions does not need this. One that has to account for <b>every</b> node it crosses - a
     * collector that must know when it met a shape it does not model - does, because the {@code default} arm of the
     * predicate walk treats an unrecognised type as a leaf and would otherwise hide it.
     */
    default void visitPredicate(final BooleanExpression predicate) {
      // Most visitors are about expressions.
    }

    /**
     * Called when the walk is about to descend into a statement that binds its own variables - the body of a
     * {@code CALL { ... }} clause or of an {@code EXISTS}/{@code COUNT}/{@code COLLECT} subquery expression, and each
     * branch of a {@code UNION}. Returning {@code this} keeps the same check running inside it; returning a different
     * visitor is how a check whose meaning depends on the variable scope re-binds itself to the inner one; returning
     * {@code null} skips it.
     */
    default Visitor forNestedStatement(final CypherStatement statement) {
      return this;
    }
  }

  private CypherExpressionWalker() {
    // utility class
  }

  /**
   * Visits every expression of {@code statement}: its projections, the expressions of each of its clauses, its
   * {@code ORDER BY}/{@code SKIP}/{@code LIMIT}, and - through {@link Visitor#forNestedStatement} - the bodies of the
   * subqueries it contains.
   */
  public static void walk(final CypherStatement statement, final Visitor visitor) {
    if (statement == null)
      return;

    // Each branch of a UNION binds its own variables, so each is entered as a nested statement in its own right:
    // a check that reads variable kinds gets the kinds of the branch it is looking at, not an intersection over all
    // of them, which would silently skip a variable only one branch declares.
    if (statement instanceof UnionStatement union) {
      for (final CypherStatement query : union.getQueries())
        walkNested(query, visitor);
      return;
    }

    final ReturnClause returnClause = statement.getReturnClause();
    if (returnClause != null)
      for (final ReturnClause.ReturnItem item : returnClause.getReturnItems())
        walk(item.getExpression(), visitor);

    // The top-level ORDER BY / SKIP / LIMIT belong to the statement rather than to any clause entry, and are walked
    // here for the same reason walkWith() walks a WITH's: leaving them out is the clause-dependent asymmetry this
    // traversal exists to remove.
    walkOrderBy(statement.getOrderByClause(), visitor);
    walk(statement.getSkip(), visitor);
    walk(statement.getLimit(), visitor);

    // The ordered clause list is the whole clause tree, WITH included: StatementBuilder.addWith is the one place a
    // WITH is registered and it puts the same object on both getWithClauses() and this list, so walking the second
    // collection as well would walk every WITH twice rather than reach one this misses. That invariant is what makes
    // once-only structural here instead of guarded, and CypherSubqueryParseTimeValidationIssue5626Test pins it -
    // a builder path that ever registers a WITH on the statement alone has to add it here too, or the walk, and every
    // check running through it, will not see it.
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses != null)
      for (int i = 0; i < clauses.size(); i++)
        walkClause(clauses.get(i), visitor);
  }

  /**
   * Visits every expression of one clause.
   */
  private static void walkClause(final ClauseEntry entry, final Visitor visitor) {
    switch (entry.getType()) {
    case MATCH -> {
      final MatchClause matchClause = entry.getTypedClause();
      if (matchClause.hasWhereClause())
        walk(matchClause.getWhereClause().getConditionExpression(), visitor);
      if (matchClause.getPathPatterns() != null)
        for (final PathPattern pattern : matchClause.getPathPatterns())
          walk(pattern, visitor);
    }
    case WITH -> walkWith(entry.getTypedClause(), visitor);
    case UNWIND -> walk(((UnwindClause) entry.getTypedClause()).getListExpression(), visitor);
    case CREATE -> {
      final CreateClause createClause = entry.getTypedClause();
      if (createClause.getPathPatterns() != null)
        for (final PathPattern pattern : createClause.getPathPatterns())
          walk(pattern, visitor);
    }
    case MERGE -> {
      final MergeClause mergeClause = entry.getTypedClause();
      walk(mergeClause.getPathPattern(), visitor);
      walkSet(mergeClause.getOnCreateSet(), visitor);
      walkSet(mergeClause.getOnMatchSet(), visitor);
    }
    case SET -> walkSet(entry.getTypedClause(), visitor);
    case DELETE -> {
      final DeleteClause deleteClause = entry.getTypedClause();
      if (deleteClause.getExpressions() != null)
        for (final Expression expression : deleteClause.getExpressions())
          walk(expression, visitor);
    }
    case FOREACH -> {
      final ForeachClause foreachClause = entry.getTypedClause();
      walk(foreachClause.getListExpression(), visitor);
      if (foreachClause.getInnerClauses() != null)
        for (final ClauseEntry inner : foreachClause.getInnerClauses())
          walkClause(inner, visitor);
    }
    case CALL -> {
      final CallClause callClause = entry.getTypedClause();
      if (callClause.getArguments() != null)
        for (final Expression argument : callClause.getArguments())
          walk(argument, visitor);
      if (callClause.getYieldWhere() != null)
        walk(callClause.getYieldWhere().getConditionExpression(), visitor);
    }
    case SUBQUERY -> {
      final SubqueryClause subqueryClause = entry.getTypedClause();
      walk(subqueryClause.getBatchSize(), visitor);
      walkNested(subqueryClause.getInnerStatement(), visitor);
    }
    case LOAD_CSV -> walk(((LoadCSVClause) entry.getTypedClause()).getUrlExpression(), visitor);
    default -> {
      // RETURN is walked from the statement, and REMOVE / FINISH name variables, properties and labels rather than
      // carrying an expression of their own.
    }
    }
  }

  /**
   * Descends into the body of a nested subquery, handing the visitor the chance to re-bind itself to the inner
   * variable scope first.
   */
  private static void walkNested(final CypherStatement statement, final Visitor visitor) {
    if (statement == null)
      return;

    final Visitor nested = visitor.forNestedStatement(statement);
    if (nested != null)
      walk(statement, nested);
  }

  private static void walkWith(final WithClause withClause, final Visitor visitor) {
    if (withClause == null)
      return;
    for (final ReturnClause.ReturnItem item : withClause.getItems())
      walk(item.getExpression(), visitor);
    if (withClause.getWhereClause() != null)
      walk(withClause.getWhereClause().getConditionExpression(), visitor);
    walkOrderBy(withClause.getOrderByClause(), visitor);
    walk(withClause.getSkip(), visitor);
    walk(withClause.getLimit(), visitor);
  }

  private static void walkSet(final SetClause setClause, final Visitor visitor) {
    if (setClause == null)
      return;
    for (final SetClause.SetItem item : setClause.getItems()) {
      walk(item.getTargetExpression(), visitor);
      walk(item.getKeyExpression(), visitor);
      walk(item.getValueExpression(), visitor);
    }
  }

  private static void walkOrderBy(final OrderByClause orderBy, final Visitor visitor) {
    if (orderBy == null || orderBy.getItems() == null)
      return;
    for (final OrderByClause.OrderByItem item : orderBy.getItems())
      // An ORDER BY item keeps its expression as text and, when the builder could parse one, as an AST too.
      walk(item.getExpressionAST(), visitor);
  }

  /**
   * Visits {@code expr} and everything nested inside it.
   */
  public static void walk(final Expression expr, final Visitor visitor) {
    if (expr == null)
      return;

    visitor.visit(expr);

    switch (expr) {
    case FunctionCallExpression func -> walkAll(func.getArguments(), visitor);
    case ArithmeticExpression arithmetic -> {
      walk(arithmetic.getLeft(), visitor);
      walk(arithmetic.getRight(), visitor);
    }
    case ListExpression list -> walkAll(list.getElements(), visitor);
    case CaseExpression caseExpr -> {
      walk(caseExpr.getCaseExpression(), visitor);
      for (final CaseAlternative alternative : caseExpr.getAlternatives()) {
        walk(alternative.getWhenExpression(), visitor);
        walk(alternative.getThenExpression(), visitor);
      }
      walk(caseExpr.getElseExpression(), visitor);
    }
    case MapExpression map -> {
      for (final Expression value : map.getEntries().values())
        walk(value, visitor);
    }
    case MapProjectionExpression projection -> {
      for (final MapProjectionExpression.ProjectionElement element : projection.getElements())
        walk(element.getExpression(), visitor);
    }
    case ListIndexExpression index -> {
      walk(index.getListExpression(), visitor);
      walk(index.getIndexExpression(), visitor);
    }
    case ListSliceExpression slice -> {
      walk(slice.getListExpression(), visitor);
      walk(slice.getFromExpression(), visitor);
      walk(slice.getToExpression(), visitor);
    }
    case ListComprehensionExpression comprehension -> {
      walk(comprehension.getListExpression(), visitor);
      walk(comprehension.getWhereExpression(), visitor);
      walk(comprehension.getMapExpression(), visitor);
    }
    case ListPredicateExpression predicate -> {
      walk(predicate.getListExpression(), visitor);
      walk(predicate.getWhereExpression(), visitor);
    }
    case ReduceExpression reduce -> {
      walk(reduce.getInitialValue(), visitor);
      walk(reduce.getListExpression(), visitor);
      walk(reduce.getReduceExpression(), visitor);
    }
    case AllReduceExpression reduce -> {
      walk(reduce.getInitialValue(), visitor);
      walk(reduce.getListExpression(), visitor);
      walk(reduce.getReduceExpression(), visitor);
      walk(reduce.getPredicateExpression(), visitor);
    }
    case TernaryLogicalExpression ternary -> {
      walk(ternary.getLeft(), visitor);
      walk(ternary.getRight(), visitor);
    }
    case BooleanWrapperExpression wrapper -> walk(wrapper.getBooleanExpression(), visitor);
    case ComparisonExpressionWrapper wrapper -> walk(wrapper.getComparison(), visitor);
    case PatternComprehensionExpression comprehension -> {
      walk(comprehension.getPathPattern(), visitor);
      walk(comprehension.getWhereExpression(), visitor);
      walk(comprehension.getMapExpression(), visitor);
    }
    case ShortestPathExpression shortestPath -> walk(shortestPath.getPathPattern(), visitor);
    case ExistsExpression exists -> walkNested(exists.getParsedSubquery(), visitor);
    case CountExpression count -> walkNested(count.getParsedSubquery(), visitor);
    case CollectExpression collect -> walkNested(collect.getParsedSubquery(), visitor);
    default -> {
      // A leaf: literal, variable, parameter or property access.
    }
    }
  }

  /**
   * Visits every expression nested inside a predicate.
   */
  public static void walk(final BooleanExpression expr, final Visitor visitor) {
    if (expr == null)
      return;

    visitor.visitPredicate(expr);

    switch (expr) {
    case ComparisonExpression comparison -> {
      walk(comparison.getLeft(), visitor);
      walk(comparison.getRight(), visitor);
    }
    case LogicalExpression logical -> {
      walk(logical.getLeft(), visitor);
      walk(logical.getRight(), visitor);
    }
    case InExpression in -> {
      walk(in.getExpression(), visitor);
      walkAll(in.getList(), visitor);
    }
    case IsNullExpression isNull -> walk(isNull.getExpression(), visitor);
    case RegexExpression regex -> {
      walk(regex.getExpression(), visitor);
      walk(regex.getPattern(), visitor);
    }
    case StringMatchExpression match -> {
      walk(match.getExpression(), visitor);
      walk(match.getPattern(), visitor);
    }
    case LabelCheckExpression labelCheck -> walk(labelCheck.getVariableExpression(), visitor);
    case IsTypedExpression typed -> walk(typed.getValueExpression(), visitor);
    case BooleanCoercionExpression coercion -> walk(coercion.getExpression(), visitor);
    case PatternPredicateExpression pattern -> walk(pattern.getPathPattern(), visitor);
    default -> {
      // A predicate with no nested expression to reach.
    }
    }
  }

  /**
   * Hands the pattern itself to {@link Visitor#visitPattern}, then visits every expression nested inside it: the
   * inline property values of each node and relationship, their dynamic labels, and any inline {@code WHERE}. This is
   * how {@code CREATE (n:P {age: abs('x')})} is reached.
   */
  public static void walk(final PathPattern pattern, final Visitor visitor) {
    if (pattern == null)
      return;

    visitor.visitPattern(pattern);

    if (pattern.getNodes() != null)
      for (final NodePattern node : pattern.getNodes()) {
        if (node == null)
          continue;
        walkPatternProperties(node.getProperties(), visitor);
        walkAll(node.getDynamicLabels(), visitor);
        walk(node.getWhereExpression(), visitor);
      }

    if (pattern.getRelationships() != null)
      for (final RelationshipPattern relationship : pattern.getRelationships()) {
        if (relationship == null)
          continue;
        walkPatternProperties(relationship.getProperties(), visitor);
        walk(relationship.getWhereExpression(), visitor);
      }
  }

  /**
   * Inline pattern properties are held as {@code Map<String, Object>}: a value is an {@link Expression} when the query
   * wrote one and a plain constant when the builder already folded it, so only the former can be walked.
   */
  private static void walkPatternProperties(final Map<String, Object> properties, final Visitor visitor) {
    if (properties == null || properties.isEmpty())
      return;

    for (final Object value : properties.values())
      if (value instanceof Expression expression)
        walk(expression, visitor);
  }

  private static void walkAll(final List<Expression> expressions, final Visitor visitor) {
    if (expressions == null)
      return;

    for (int i = 0; i < expressions.size(); i++)
      walk(expressions.get(i), visitor);
  }
}
