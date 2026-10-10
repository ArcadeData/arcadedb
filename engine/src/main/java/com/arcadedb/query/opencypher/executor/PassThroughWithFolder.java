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

import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SimpleCypherStatement;
import com.arcadedb.query.opencypher.ast.UnionStatement;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WithClause;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Folds away the {@code WITH} clauses of a {@code MATCH ... RETURN} statement that only pass variables on, for the count
 * push-downs (issue #9597).
 * <p>
 * A {@code WITH} made of variables, aliased or not, or of {@code *} - with no aggregation, {@code DISTINCT},
 * {@code WHERE}, {@code ORDER BY}, {@code SKIP} or {@code LIMIT} - emits each incoming row once, so it changes no row
 * count, and the {@code MATCH} clauses on either side of it mean what they would mean as consecutive clauses: separate
 * clauses share no relationship uniqueness either way. What it does change is <b>names</b>, and the fold has to keep
 * them apart:
 * <ul>
 * <li>an alias, {@code WITH m AS x}, makes the {@code x} written after it the {@code m} written before, so the node
 * patterns that bind {@code x} later are rewritten to bind {@code m};</li>
 * <li>a variable the {@code WITH} drops is out of scope after it, so a later clause that binds the same name binds a new
 * variable. Concatenated, the two would be one variable and the count a join; the fold refuses instead.</li>
 * </ul>
 * Only node patterns are renamed. An alias read by an expression - a {@code WHERE}, an inline property value, the
 * {@code RETURN} - or carried by a relationship or a path variable makes the fold refuse rather than rewrite expressions:
 * the count push-downs would not take such a statement anyway, and a refused fold leaves the statement to the
 * ordinary pipeline, exactly as before.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PassThroughWithFolder {
  private PassThroughWithFolder() {
  }

  /**
   * The statement as a count push-down reads it.
   * <p>
   * A function of the statement's text alone: it reads no schema, data or parameter, which is what lets the result be
   * kept on the cached statement for every later execution ({@code SimpleCypherStatement.getCountPushDownForm()}, issue
   * #9652). A fold that came to depend on anything else would have to stop being kept there.
   *
   * @return the statement itself when it holds no {@code WITH}, the folded statement when every {@code WITH} is a
   * pass-through, null when one is not or when a name could not be kept apart
   */
  static CypherStatement fold(final CypherStatement statement) {
    if (!(statement instanceof SimpleCypherStatement simple) || statement instanceof UnionStatement)
      return statement;
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses == null || !hasWith(clauses))
      return statement;

    // visible name -> the variable of the folded statement it stands for
    Map<String, String> scope = new HashMap<>();
    // every variable of the folded statement, in scope or dropped by a WITH
    final Set<String> bound = new HashSet<>();
    final List<MatchClause> matches = new ArrayList<>();
    final List<ClauseEntry> entries = new ArrayList<>();
    ReturnClause returnClause = null;

    for (final ClauseEntry entry : clauses) {
      if (returnClause != null)
        return null;
      switch (entry.getType()) {
      case MATCH -> {
        final MatchClause folded = foldMatch(entry.getTypedClause(), scope, bound);
        if (folded == null)
          return null;
        matches.add(folded);
        entries.add(new ClauseEntry(ClauseEntry.ClauseType.MATCH, folded, entries.size()));
      }
      case WITH -> {
        scope = passThroughScope(entry.getTypedClause(), scope);
        if (scope == null)
          return null;
      }
      case RETURN -> {
        returnClause = entry.getTypedClause();
        if (!readsOnlyUnrenamedNames(returnClause, scope))
          return null;
        entries.add(new ClauseEntry(ClauseEntry.ClauseType.RETURN, returnClause, entries.size()));
      }
      default -> {
        return null;
      }
      }
    }

    return new SimpleCypherStatement(simple.getOriginalQuery(), matches, simple.getWhereClause(), simple.getReturnClause(),
        simple.getOrderByClause(), simple.getSkip(), simple.getLimit(), null, null, null, null, null, null, null, null,
        entries, false, false, false, false);
  }

  private static boolean hasWith(final List<ClauseEntry> clauses) {
    for (final ClauseEntry clause : clauses)
      if (clause.getType() == ClauseEntry.ClauseType.WITH)
        return true;
    return false;
  }

  /**
   * The scope after a pass-through {@code WITH}, or null when it is not one.
   */
  private static Map<String, String> passThroughScope(final WithClause with, final Map<String, String> scope) {
    if (with.isDistinct() || with.getWhereClause() != null || with.getOrderByClause() != null || with.getSkip() != null
        || with.getLimit() != null)
      return null;

    final Map<String, String> next = new HashMap<>();
    for (final ReturnClause.ReturnItem item : with.getItems())
      if (item.isStar())
        next.putAll(scope);
    for (final ReturnClause.ReturnItem item : with.getItems()) {
      if (item.isStar())
        continue;
      if (!(item.getExpression() instanceof VariableExpression variable))
        return null;
      final String target = scope.get(variable.getVariableName());
      if (target == null)
        return null;
      final String name = item.getAlias() != null ? item.getAlias() : variable.getVariableName();
      final String previous = next.put(name, target);
      if (previous != null && !previous.equals(target))
        return null;
    }
    return next;
  }

  /**
   * The MATCH with the names its patterns bind resolved against the scope, or null when it cannot be folded: it binds
   * anew a name a WITH dropped, renames a relationship or a path, or reads an alias in an expression.
   * <p>
   * Adds the names the clause binds to {@code scope} and {@code bound}, which are the fold's own working state:
   * {@link #fold} owns both, and a WITH replaces the scope with a fresh map rather than sharing it.
   */
  private static MatchClause foldMatch(final MatchClause match, final Map<String, String> scope, final Set<String> bound) {
    if (!match.hasPathPatterns())
      return null;

    // the names this clause binds anew, which no expression of it may read under another name
    final Set<String> introduced = new HashSet<>();
    final List<PathPattern> patterns = new ArrayList<>(match.getPathPatterns().size());
    boolean renamed = false;
    for (final PathPattern pattern : match.getPathPatterns()) {
      if (!resolveNonNodeName(pattern.getPathVariable(), scope, bound, introduced))
        return null;
      for (final RelationshipPattern relationship : pattern.getRelationships())
        if (!resolveNonNodeName(relationship.getVariable(), scope, bound, introduced))
          return null;

      List<NodePattern> nodes = null;
      for (int i = 0; i < pattern.getNodeCount(); i++) {
        final NodePattern node = pattern.getNode(i);
        final String name = node.getVariable();
        if (name == null || name.isEmpty())
          continue;
        final String target = scope.get(name);
        if (target == null) {
          if (!introduced.contains(name) && !bound.add(name))
            return null; // a name a WITH dropped, bound again: a new variable the fold would merge with the old one
          introduced.add(name);
        } else if (!target.equals(name)) {
          if (pattern.getClass() != PathPattern.class)
            return null;
          if (nodes == null)
            nodes = new ArrayList<>(pattern.getNodes());
          nodes.set(i, node.withVariable(target));
        }
      }
      if (nodes != null) {
        patterns.add(new PathPattern(nodes, pattern.getRelationships(), pattern.getPathVariable(), pattern.getPathMode()));
        renamed = true;
      } else
        patterns.add(pattern);
    }

    for (final String name : introduced)
      scope.put(name, name);

    // every expression of the clause reads its names as they are, so none of them may be an alias
    if (match.hasWhereClause()) {
      if (match.getWhereClause().getConditionExpression() == null
          || !readsOnlyUnrenamed(CypherReferencedVariables.of(match.getWhereClause().getConditionExpression()), scope))
        return null;
    }
    for (final PathPattern pattern : match.getPathPatterns()) {
      for (final NodePattern node : pattern.getNodes())
        if (!valuesReadOnlyUnrenamed(node.getProperties(), scope)
            || (node.hasDynamicLabels() && !expressionsReadOnlyUnrenamed(node.getDynamicLabels(), scope))
            || (node.hasWhereExpression() && !readsOnlyUnrenamed(CypherReferencedVariables.of(node.getWhereExpression()), scope)))
          return null;
      for (final RelationshipPattern relationship : pattern.getRelationships())
        if (!valuesReadOnlyUnrenamed(relationship.getProperties(), scope) || (relationship.hasWhereExpression()
            && !readsOnlyUnrenamed(CypherReferencedVariables.of(relationship.getWhereExpression()), scope)))
          return null;
    }

    return renamed ? new MatchClause(patterns, match.isOptional(), match.getWhereClause()) : match;
  }

  /** A relationship or path variable: kept as it is, which an alias it would have to take refuses. */
  private static boolean resolveNonNodeName(final String name, final Map<String, String> scope, final Set<String> bound,
      final Set<String> introduced) {
    if (name == null || name.isEmpty())
      return true;
    final String target = scope.get(name);
    if (target != null)
      return target.equals(name);
    if (introduced.contains(name))
      return true;
    if (!bound.add(name))
      return false;
    introduced.add(name);
    return true;
  }

  private static boolean readsOnlyUnrenamedNames(final ReturnClause returnClause, final Map<String, String> scope) {
    if (returnClause == null)
      return true;
    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems())
      if (!item.isStar() && (item.getExpression() == null
          || !readsOnlyUnrenamed(CypherReferencedVariables.of(item.getExpression()), scope)))
        return false;
    return true;
  }

  private static boolean valuesReadOnlyUnrenamed(final Map<String, Object> properties,
      final Map<String, String> scope) {
    if (properties == null)
      return true;
    for (final Object value : properties.values())
      if (value instanceof Expression expression && !readsOnlyUnrenamed(CypherReferencedVariables.of(expression), scope))
        return false;
    return true;
  }

  private static boolean expressionsReadOnlyUnrenamed(final List<Expression> expressions, final Map<String, String> scope) {
    for (final Expression expression : expressions)
      if (!readsOnlyUnrenamed(CypherReferencedVariables.of(expression), scope))
        return false;
    return true;
  }

  /** Whether every name read is in scope under its own name. */
  private static boolean readsOnlyUnrenamed(final CypherReferencedVariables referenced, final Map<String, String> scope) {
    if (!referenced.isComplete())
      return false;
    for (final String name : referenced.getNames())
      if (!name.equals(scope.get(name)))
        return false;
    return true;
  }
}
