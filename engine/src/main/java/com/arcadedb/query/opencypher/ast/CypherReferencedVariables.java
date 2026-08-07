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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Every variable name a statement could read, and whether that list is known to be the whole of it. Not the
 * statement's free variables - see {@link #getNames()} - but a superset of them, which is what makes it safe here.
 * <p>
 * The question is asked by {@code CypherExecutionPlan} when a statement runs from a <b>seed row</b> - the body of a
 * {@code CALL { }} clause, or of an {@code EXISTS}/{@code COUNT}/{@code COLLECT} expression, handed the outer row as
 * its first input. The two count push-downs there answer from the schema and the CSR arrays and never look at the
 * incoming rows, so a body that reads one of the seeded names must not be answered by them: a body counting
 * {@code MATCH (n)-[:KNOWS]->(m)} with {@code n} already bound to one vertex would be given the count over every
 * {@code n} in the graph. A body that reads none of them keeps the fast path, because ignoring a row it never looks
 * at cannot change its answer (issue #5686).
 * <p>
 * <b>The two ways of being wrong are not equal.</b> Naming one variable too many costs a slower count. Missing one
 * gives a silently wrong count, which is the failure #5674 removed. So every shape this class does not model answers
 * {@link #referencesAny} with {@code true}: {@link #isComplete()} is false and the caller loses the optimization
 * rather than the correctness. That is why the collector is an allow-list - a clause type, an expression type or a
 * predicate type introduced later is unknown until it is added here, and unknown disables the push-down.
 * <p>
 * <b>Completeness is claimed for a shape, not in general.</b> The clause types modelled are {@code MATCH},
 * {@code RETURN}, {@code WITH} and {@code UNWIND}; every other one - anything that writes, a {@code FOREACH}, a
 * {@code CALL { }} clause, a {@code LOAD CSV} - makes the answer incomplete, at any nesting depth. The push-downs
 * the caller gates on require {@code isMatchReturnOnlyStatement()}, a subset of those four, <i>and</i> a
 * {@code RETURN} of exactly one count item, which rules out the one {@code MATCH}/{@code RETURN} shape this class
 * declines - {@code RETURN *}. So a statement they would accept is always one this collector models. Nothing in the
 * code ties the two together, so {@code CypherUncorrelatedSubqueryCountPushDownIssue5686Test} asserts it rather than
 * assuming it.
 * <p>
 * It lives beside the AST it analyses, and beside {@link CypherStatement#getReferencedVariables()}, which returns it
 * and memoizes it - so no statement type has to name something outside this package to state its own contract. The
 * one thing it reaches for is {@link CypherExpressionWalker}, a traversal over these same nodes that happens to be
 * filed under the parser that first needed it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CypherReferencedVariables {
  /**
   * The expression types that carry no variable name of their own: either a leaf holding a literal, a parameter or a
   * star, or a composite whose every child {@link CypherExpressionWalker} visits in its own right. Membership is by
   * exact class, so a subclass of one of these is unknown until it is listed too.
   */
  private static final Set<Class<?>> NAMELESS_EXPRESSIONS = Set.of(
      AllReduceExpression.class, ArithmeticExpression.class, BooleanWrapperExpression.class, CaseExpression.class,
      ComparisonExpressionWrapper.class, FunctionCallExpression.class, ListComprehensionExpression.class,
      ListExpression.class, ListIndexExpression.class, ListPredicateExpression.class, ListSliceExpression.class,
      LiteralExpression.class, MapExpression.class, ParameterExpression.class, PatternComprehensionExpression.class,
      ReduceExpression.class, ShortestPathExpression.class, StarExpression.class, TernaryLogicalExpression.class);

  /**
   * The predicate types that carry no variable name of their own. Each holds its operands as expressions or as a
   * pattern, and the walk reaches both separately.
   */
  private static final Set<Class<?>> NAMELESS_PREDICATES = Set.of(
      BooleanCoercionExpression.class, ComparisonExpression.class, InExpression.class, IsNullExpression.class,
      IsTypedExpression.class, LabelCheckExpression.class, LogicalExpression.class, PatternPredicateExpression.class,
      RegexExpression.class, StringMatchExpression.class);

  /**
   * The types {@code Collector.visit} handles in an arm of its own rather than through the nameless sets. The first
   * three carry a name it reads. The last three carry none: their arm checks that the body has an AST at all, since
   * one that does not runs from its text and can name anything, and the names <i>inside</i> a body that does are
   * collected by the walk descending into it.
   * <p>
   * Listed again here only so {@link #classifies} can answer for them, so a type added to one has to be added to
   * the other.
   */
  private static final Set<Class<?>> SEPARATELY_HANDLED_EXPRESSIONS = Set.of(
      VariableExpression.class, PropertyAccessExpression.class, MapProjectionExpression.class,
      ExistsExpression.class, CountExpression.class, CollectExpression.class);

  /** The answer for a statement whose shape is not modelled: no names, and every question answers "referenced". */
  private static final CypherReferencedVariables UNKNOWN = new CypherReferencedVariables(Set.of(), false);

  private final Set<String> names;
  private final boolean     complete;

  private CypherReferencedVariables(final Set<String> names, final boolean complete) {
    this.names = names;
    this.complete = complete;
  }

  /**
   * Collects the variable names {@code statement} could read.
   *
   * @return the collected names, or an incomplete answer when the statement's shape is one this class does not model
   */
  public static CypherReferencedVariables of(final CypherStatement statement) {
    if (statement == null)
      return UNKNOWN;

    final Collector collector = new Collector();
    collector.checkShape(statement);
    if (!collector.complete)
      return UNKNOWN;

    CypherExpressionWalker.walk(statement, collector);
    if (!collector.complete)
      return UNKNOWN;

    return new CypherReferencedVariables(Collections.unmodifiableSet(collector.names), true);
  }

  /** The answer to use when there is no statement to inspect. */
  public static CypherReferencedVariables unknown() {
    return UNKNOWN;
  }

  /**
   * Whether {@code type} is an expression or predicate type this class has been told about, rather than one it would
   * meet and give up on.
   * <p>
   * Exposed for the guard test that walks the AST package: an expression type added without being classified here
   * fails nothing on its own - it makes every statement containing it incomplete, which costs the push-down silently
   * and everywhere. This is what lets that be an assertion instead of a review habit.
   */
  public static boolean classifies(final Class<?> type) {
    return SEPARATELY_HANDLED_EXPRESSIONS.contains(type) || NAMELESS_EXPRESSIONS.contains(type)
        || NAMELESS_PREDICATES.contains(type);
  }

  /**
   * Whether any of {@code candidates} could be read by the statement. An incomplete answer says yes to all of them:
   * the caller loses its optimization rather than applying it on a guess.
   */
  public boolean referencesAny(final Collection<String> candidates) {
    // Asked about nothing, the answer is no whether or not the collection is complete: there is no name to have read.
    if (candidates == null || candidates.isEmpty())
      return false;
    if (!complete)
      return true;
    if (names.isEmpty())
      return false;

    for (final String candidate : candidates)
      if (names.contains(candidate))
        return true;
    return false;
  }

  /**
   * The names collected. Empty when {@link #isComplete()} is false, where it means nothing.
   * <p>
   * <b>This is not a free-variable set.</b> A name a statement binds for itself is collected as readily as one it
   * takes from outside: the iteration variable of a list comprehension, an {@code UNWIND ... AS x} alias, a
   * {@code WITH ... AS y} alias. That is deliberate for the question this class exists to answer - a body's
   * {@code MATCH (n:Person)} written under an outer {@code n} <i>is</i> the outer {@code n}, and naming one variable
   * too many only ever costs the caller its optimization. A caller wanting the free variables of a statement, where
   * over-collection would be an error rather than a cost, needs a different collector.
   */
  public Set<String> getNames() {
    return names;
  }

  /** Whether {@link #getNames()} is the whole of what the statement could read. */
  public boolean isComplete() {
    return complete;
  }

  @Override
  public String toString() {
    return complete ? names.toString() : "<unknown>";
  }

  /**
   * Accumulates the names and, in the same pass, whether anything crossed was outside what is modelled.
   */
  private static final class Collector implements CypherExpressionWalker.Visitor {
    private final Set<String> names    = new HashSet<>();
    private       boolean     complete = true;

    @Override
    public void visit(final Expression expression) {
      switch (expression) {
      case VariableExpression variable -> add(variable.getVariableName());
      case PropertyAccessExpression property -> add(property.getVariableName());
      case MapProjectionExpression projection -> add(projection.getVariableName());
      // A subquery expression with no parsed body runs from its text through CorrelatedSubqueryRewriter, which the
      // walk cannot enter and which can name anything the outer row carries.
      case ExistsExpression exists -> requireParsedBody(exists.getParsedSubquery());
      case CountExpression count -> requireParsedBody(count.getParsedSubquery());
      case CollectExpression collect -> requireParsedBody(collect.getParsedSubquery());
      default -> {
        if (!NAMELESS_EXPRESSIONS.contains(expression.getClass()))
          complete = false;
      }
      }
    }

    @Override
    public void visitPredicate(final BooleanExpression predicate) {
      if (!NAMELESS_PREDICATES.contains(predicate.getClass()))
        complete = false;
    }

    /**
     * The variables a pattern binds are references too: a body's {@code MATCH (n:Person)} written under an outer
     * {@code n} <i>is</i> the outer {@code n}, which is exactly the correlation the push-downs cannot see.
     */
    @Override
    public void visitPattern(final PathPattern pattern) {
      add(pattern.getPathVariable());

      if (pattern.getNodes() != null)
        for (final NodePattern node : pattern.getNodes()) {
          if (node == null)
            continue;
          add(node.getVariable());
          // Properties written as a bare parameter map keep no expression for the walk to reach. Only a parameter
          // can be written there, never a variable, so this cannot hide one - but staying unsure costs nothing: a
          // pattern carrying properties is one both push-downs reject anyway.
          if (node.getPropertiesParameterName() != null)
            complete = false;
        }

      if (pattern.getRelationships() != null)
        for (final RelationshipPattern relationship : pattern.getRelationships()) {
          if (relationship == null)
            continue;
          add(relationship.getVariable());
          if (relationship.getPropertiesParameterName() != null)
            complete = false;
        }
    }

    /**
     * A nested body is entered with the same collector - a body auto-correlates to the names in scope where it was
     * written, so what it reads is what the enclosing statement reads - and its shape is checked in its own right.
     */
    @Override
    public CypherExpressionWalker.Visitor forNestedStatement(final CypherStatement statement) {
      checkShape(statement);
      return this;
    }

    private void requireParsedBody(final CypherStatement body) {
      if (body == null)
        complete = false;
    }

    private void add(final String name) {
      if (name != null && !name.isEmpty())
        names.add(name);
    }

    /**
     * Rejects a statement made of parts the walk does not reach, or that this class does not model. What the walk
     * does reach is left to the visits above.
     */
    private void checkShape(final CypherStatement statement) {
      if (statement == null) {
        complete = false;
        return;
      }

      if (statement instanceof UnionStatement union) {
        // Each branch is entered by the walk as a nested statement, and checked there. An empty UNION is not.
        //
        // A union's OWN ORDER BY / SKIP / LIMIT are deliberately not modelled: the walk does not reach them either,
        // so a name referenced only there would go uncollected while this still answered complete. That is sound
        // only because a UNION body is rejected by both push-downs, so no such statement reaches the gate. Whoever
        // widens the push-downs to a union body has to teach the collector about those clauses here first - the same
        // rule the statement-level WHERE below is held to.
        final List<CypherStatement> branches = union.getQueries();
        if (branches == null || branches.isEmpty())
          complete = false;
        return;
      }

      // A statement-level WHERE belongs to no clause entry, so the walk never reaches it. Nothing builds one today;
      // if something starts to, this collector has to be taught about it rather than quietly skip it.
      if (statement.getWhereClause() != null) {
        complete = false;
        return;
      }

      checkOrderBy(statement.getOrderByClause());
      checkProjection(statement.getReturnClause());

      final List<ClauseEntry> clauses = statement.getClausesInOrder();
      if (clauses == null || clauses.isEmpty()) {
        // Without the ordered clause list the walk sees only the RETURN, and a legacy statement keeps its MATCH
        // clauses elsewhere.
        complete = false;
        return;
      }

      for (int i = 0; i < clauses.size(); i++) {
        final ClauseEntry entry = clauses.get(i);
        switch (entry.getType()) {
        case MATCH -> checkMatch(entry.getTypedClause());
        case RETURN -> {
          // The walk projects from statement.getReturnClause(), not from this entry, so a RETURN registered only
          // here would never be visited.
          if (entry.getTypedClause() != statement.getReturnClause())
            complete = false;
        }
        case WITH -> {
          final WithClause with = entry.getTypedClause();
          if (with == null)
            complete = false;
          else {
            checkProjection(with.getItems());
            checkOrderBy(with.getOrderByClause());
          }
        }
        case UNWIND -> {
          final UnwindClause unwind = entry.getTypedClause();
          if (unwind == null || unwind.getListExpression() == null)
            complete = false;
        }
        // CREATE, MERGE, SET, REMOVE, DELETE, CALL, FOREACH, SUBQUERY, LOAD_CSV and FINISH all name variables the
        // walk does not hand to a visitor, or hold clauses of their own. None of them can appear in a statement the
        // count push-downs accept.
        default -> complete = false;
        }

        if (!complete)
          return;
      }
    }

    private void checkMatch(final MatchClause match) {
      // A MATCH built from a raw pattern string (the pre-AST constructor) has no parsed pattern for the walk.
      if (match == null || !match.hasPathPatterns())
        complete = false;
    }

    private void checkProjection(final ReturnClause returnClause) {
      if (returnClause != null)
        checkProjection(returnClause.getReturnItems());
    }

    private void checkProjection(final List<ReturnClause.ReturnItem> items) {
      if (items == null) {
        complete = false;
        return;
      }

      for (int i = 0; i < items.size(); i++) {
        final ReturnClause.ReturnItem item = items.get(i);
        if (item == null || item.getExpression() == null) {
          complete = false;
          return;
        }
        // RETURN * / WITH * project every variable in scope by name, the seeded ones included.
        if ("*".equals(item.getOutputName())) {
          complete = false;
          return;
        }
      }
    }

    private void checkOrderBy(final OrderByClause orderBy) {
      if (orderBy == null || orderBy.getItems() == null)
        return;

      for (final OrderByClause.OrderByItem item : orderBy.getItems())
        // An ORDER BY item always keeps its text and only sometimes an AST; the walk reads the AST.
        if (item == null || item.getExpressionAST() == null) {
          complete = false;
          return;
        }
    }
  }
}
