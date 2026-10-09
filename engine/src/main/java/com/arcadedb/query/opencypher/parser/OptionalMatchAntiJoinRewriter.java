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

import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.IsNullExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PatternPredicateExpression;
import com.arcadedb.query.opencypher.ast.QuantifiedPathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SimpleCypherStatement;
import com.arcadedb.query.opencypher.ast.UnionStatement;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.ast.WithClause;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Plans the "no such pattern" spelled with an {@code OPTIONAL MATCH} as the anti-join it means (issue #9598):
 * <pre>
 *   MATCH (t1:Tag)&lt;-[:HAS_TAG]-(m:Message)&lt;-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]-&gt;(t2:Tag)
 *   OPTIONAL MATCH (c)-[h:HAS_TAG]-&gt;(t1)
 *   WITH t1, t2, h WHERE t1 &lt;&gt; t2 AND h IS NULL
 *   RETURN count(*)
 * </pre>
 * is {@code MATCH ... WHERE NOT (c)-[:HAS_TAG]->(t1) AND t1 <> t2 RETURN count(*)}, LSQB Q8 as written, which the count
 * push-down answers by a merge of adjacency arrays where the optional match ran row by row.
 * <p>
 * <b>Why it is the same query.</b> For a row with k &gt; 0 matches of the optional pattern, the OPTIONAL MATCH emits k rows
 * whose new variables are all bound, so {@code v IS NULL} on any of them drops all k; for a row with no match it emits the
 * row once with every new variable null, and the test keeps it. That is exactly the row filtered by {@code NOT pattern},
 * whose uniqueness scope is, like the optional match's, the pattern alone. It holds when:
 * <ul>
 *   <li>the variables the pattern shares with the rows are never null there: a null endpoint matches nothing in the
 *   OPTIONAL MATCH, while a pattern predicate would read it as unbound and look for any vertex. MATCH, WITH and UNWIND are
 *   followed to know the scope: every shared name is bound by a non-optional MATCH, or carried on as it is by a WITH, and
 *   the pattern shares at least one;</li>
 *   <li>the variables it introduces are read by nothing but the {@code IS NULL} test and the projection that carries them
 *   to it - removing them from the scope then changes no answer. A name read anywhere later keeps the query as written, and
 *   so does any read the collector cannot prove absent ({@link CypherReferencedVariables} answers "unknown");</li>
 *   <li>the test is a conjunct of the WHERE of the {@code WITH} right after it, and that {@code WITH} neither aggregates nor
 *   orders nor pages, so filtering before its projection is filtering after it.</li>
 * </ul>
 * The negated pattern joins the WHERE of the non-optional MATCH right before, or becomes a {@code WITH * WHERE} filter in
 * the optional match's place after any other clause. The {@code WITH} keeps what it projected minus the removed variables; when that leaves a plain
 * pass-through right before the RETURN, it is folded into the same WHERE too, which is what hands the count push-downs the
 * single MATCH they read. A pass-through {@code WITH} written by the user is left alone: it is how a query fences a MATCH off
 * the push-downs, and the shape tests use it as their row-by-row oracle.
 * <p>
 * Runs once per parse, after semantic validation, so errors still name the query as written; the parsed statement is cached.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class OptionalMatchAntiJoinRewriter {
  private OptionalMatchAntiJoinRewriter() {
  }

  /**
   * @return the statement with every OPTIONAL MATCH that is only tested for absence planned as {@code NOT pattern}, or the
   * same instance when there is none
   */
  public static CypherStatement rewrite(final CypherStatement statement) {
    if (statement instanceof UnionStatement union) {
      final List<CypherStatement> queries = new ArrayList<>(union.getQueries().size());
      boolean changed = false;
      for (final CypherStatement query : union.getQueries()) {
        final CypherStatement rewritten = rewrite(query);
        changed |= rewritten != query;
        queries.add(rewritten);
      }
      return changed ? new UnionStatement(queries, union.getUnionAllFlags()) : statement;
    }

    if (!(statement instanceof SimpleCypherStatement simple) || !simple.isReadOnly() || !hasModelledClausesOnly(simple))
      return statement;

    SimpleCypherStatement current = simple;
    // every rewrite removes one OPTIONAL MATCH, so this ends; each pass rescans the clauses, quadratic in the OPTIONAL MATCH
    // clauses of one query, which is a handful, and paid once per parse
    for (SimpleCypherStatement next = rewriteFirst(current); next != null; next = rewriteFirst(current))
      current = next;
    return current;
  }

  /** MATCH, WITH, UNWIND and RETURN: the clauses whose scope and reads this class models. */
  private static boolean hasModelledClausesOnly(final SimpleCypherStatement statement) {
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses == null || clauses.isEmpty())
      return false;
    for (final ClauseEntry entry : clauses)
      switch (entry.getType()) {
      case MATCH, WITH, UNWIND, RETURN -> {
        // modelled
      }
      default -> {
        return false;
      }
      }
    return true;
  }

  private static SimpleCypherStatement rewriteFirst(final SimpleCypherStatement statement) {
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    // from 1: an OPTIONAL MATCH that opens the query has no rows before it to filter, and is followed by a WITH at least
    for (int i = 1; i + 1 < clauses.size(); i++) {
      final ClauseEntry entry = clauses.get(i);
      if (entry.getType() == ClauseEntry.ClauseType.MATCH && ((MatchClause) entry.getTypedClause()).isOptional()) {
        final SimpleCypherStatement rewritten = rewriteAt(statement, i);
        if (rewritten != null)
          return rewritten;
      }
    }
    return null;
  }

  /** The rewrite of the OPTIONAL MATCH at {@code index}, or null when it is not one only tested for absence. */
  private static SimpleCypherStatement rewriteAt(final SimpleCypherStatement statement, final int index) {
    final List<ClauseEntry> clauses = statement.getClausesInOrder();

    // The scope before it, and the names in it that are never null: a non-optional MATCH binds them, or a WITH carries one
    // on as it is. A name an OPTIONAL MATCH, an UNWIND or a computed projection binds may be null
    Set<String> inScope = new HashSet<>();
    Set<String> neverNull = new HashSet<>();
    for (int i = 0; i < index; i++) {
      final ClauseEntry entry = clauses.get(i);
      switch (entry.getType()) {
      case MATCH -> {
        final MatchClause match = entry.getTypedClause();
        for (final PathPattern path : match.getPathPatterns()) {
          final Set<String> names = patternNames(path);
          inScope.addAll(names);
          if (!match.isOptional())
            neverNull.addAll(names);
        }
      }
      case WITH -> {
        final Set<String> projected = new HashSet<>();
        final Set<String> projectedNeverNull = new HashSet<>();
        for (final ReturnClause.ReturnItem item : ((WithClause) entry.getTypedClause()).getItems()) {
          if (item.isStar()) {
            projected.addAll(inScope);
            projectedNeverNull.addAll(neverNull);
          } else {
            projected.add(item.getOutputName());
            if (item.getExpression() instanceof VariableExpression variable && neverNull.contains(variable.getVariableName())
                && !item.getExpression().containsAggregation())
              projectedNeverNull.add(item.getOutputName());
          }
        }
        inScope = projected;
        neverNull = projectedNeverNull;
      }
      case UNWIND -> {
        final String alias = ((UnwindClause) entry.getTypedClause()).getVariable();
        inScope.add(alias);
        neverNull.remove(alias);
      }
      default -> {
        return null;
      }
      }
    }

    final MatchClause optional = clauses.get(index).getTypedClause();
    final PathPattern pattern = singlePlainPath(optional);
    if (pattern == null)
      return null;

    // The names it introduces, each written once so that it can become anonymous; a shared one must be a never-null node
    final Set<String> introduced = new HashSet<>();
    for (final NodePattern node : pattern.getNodes()) {
      final String name = node.getVariable();
      if (name == null || name.isEmpty())
        continue;
      if (inScope.contains(name)) {
        if (!neverNull.contains(name))
          return null;
      } else if (!introduced.add(name))
        return null;
    }
    for (final RelationshipPattern relationship : pattern.getRelationships()) {
      final String name = relationship.getVariable();
      if (name == null || name.isEmpty())
        continue;
      if (inScope.contains(name) || !introduced.add(name))
        return null;
    }
    if (introduced.isEmpty())
      return null;
    // sharing no node with the rows, the predicate would ask "does such a pattern exist anywhere" again for every row: the
    // same search the OPTIONAL MATCH makes, so there is nothing to gain
    boolean sharesANode = false;
    for (final NodePattern node : pattern.getNodes())
      if (node.getVariable() != null && inScope.contains(node.getVariable()))
        sharesANode = true;
    if (!sharesANode)
      return null;

    // The WITH right after it, whose WHERE tests one of them for null
    if (clauses.get(index + 1).getType() != ClauseEntry.ClauseType.WITH)
      return null;
    final WithClause with = clauses.get(index + 1).getTypedClause();
    if (with.getWhereClause() == null || with.getWhereClause().getConditionExpression() == null || with.hasAggregations()
        || with.getOrderByClause() != null || with.getSkip() != null || with.getLimit() != null)
      return null;

    final List<BooleanExpression> conjuncts = new ArrayList<>();
    flattenAnd(with.getWhereClause().getConditionExpression(), conjuncts);
    final List<BooleanExpression> remaining = new ArrayList<>(conjuncts.size());
    boolean testedForNull = false;
    for (final BooleanExpression conjunct : conjuncts) {
      if (isNullTestOf(conjunct, introduced)) {
        testedForNull = true;
        continue;
      }
      if (CypherReferencedVariables.of(conjunct).referencesAny(introduced))
        return null;
      remaining.add(conjunct);
    }
    if (!testedForNull)
      return null;

    final List<ReturnClause.ReturnItem> items = new ArrayList<>(with.getItems().size());
    boolean projectsStar = false;
    for (final ReturnClause.ReturnItem item : with.getItems()) {
      if (item.isStar()) {
        projectsStar = true;
        items.add(item);
        continue;
      }
      if (item.getExpression() instanceof VariableExpression variable && introduced.contains(variable.getVariableName())) {
        // carried under its own name only: under another one it would be read later as the alias
        if (!variable.getVariableName().equals(item.getOutputName()))
          return null;
        continue;
      }
      if (CypherReferencedVariables.of(item.getExpression()).referencesAny(introduced))
        return null;
      items.add(item);
    }
    // a WITH projecting nothing else cannot be written; widening it to * would bring names back into scope
    if (items.isEmpty())
      return null;

    if (readsAfter(statement, index + 2, introduced, projectsStar))
      return null;

    final PathPattern absent = anonymized(pattern, introduced, inScope);
    final BooleanExpression notPattern = new LogicalExpression(LogicalExpression.Operator.NOT,
        new PatternPredicateExpression(absent, false, render(absent)));
    final BooleanExpression rest = and(remaining);

    // The negated pattern joins the WHERE of the non-optional MATCH before it, or filters in the OPTIONAL MATCH's place
    final List<ClauseEntry> rewritten = new ArrayList<>(clauses.size());
    for (int i = 0; i < index - 1; i++)
      rewritten.add(clauses.get(i));

    final ClauseEntry beforeEntry = clauses.get(index - 1);
    // DISTINCT keeps its meaning without the removed names: every row the test keeps holds null in all of them
    final WithClause carrier = new WithClause(items, with.isDistinct(), rest != null ? new WhereClause(rest) : null, null, null,
        null);
    if (beforeEntry.getType() != ClauseEntry.ClauseType.MATCH || ((MatchClause) beforeEntry.getTypedClause()).isOptional()) {
      // the WHERE of an OPTIONAL MATCH is part of it, and a WITH or an UNWIND has no MATCH to join: a filter of its own
      rewritten.add(beforeEntry);
      rewritten.add(new ClauseEntry(ClauseEntry.ClauseType.WITH, filter(notPattern), 0));
      rewritten.add(new ClauseEntry(ClauseEntry.ClauseType.WITH, carrier, 0));
    } else {
      // a parsed MatchClause is its patterns, its OPTIONAL flag and its WHERE: rebuilding it from them loses nothing
      final MatchClause before = beforeEntry.getTypedClause();
      final BooleanExpression beforeWhere = before.hasWhereClause() ? before.getWhereClause().getConditionExpression() : null;
      final boolean fold = isPassThrough(carrier) && index + 3 == clauses.size()
          && clauses.get(index + 2).getType() == ClauseEntry.ClauseType.RETURN && !returnsStar(statement);
      final BooleanExpression where = fold ? and(beforeWhere, and(notPattern, rest)) : and(beforeWhere, notPattern);
      rewritten.add(new ClauseEntry(ClauseEntry.ClauseType.MATCH,
          CypherASTBuilder.newMatchClause(before.getPathPatterns(), false, new WhereClause(where)), 0));
      if (!fold)
        rewritten.add(new ClauseEntry(ClauseEntry.ClauseType.WITH, carrier, 0));
    }
    for (int i = index + 2; i < clauses.size(); i++)
      rewritten.add(clauses.get(i));

    return rebuild(statement, rewritten);
  }

  /** The one path of an OPTIONAL MATCH a pattern predicate can say as it is, or null. */
  private static PathPattern singlePlainPath(final MatchClause optional) {
    if (optional.hasWhereClause() || optional.getPathPatterns().size() != 1)
      return null;
    final PathPattern path = optional.getPathPatterns().get(0);
    if (path.getRelationshipCount() == 0 || path.hasPathVariable() || path.getPathMode() != null)
      return null;
    for (final NodePattern node : path.getNodes())
      if (node.hasProperties() || node.getPropertiesParameterName() != null || node.hasDynamicLabels()
          || node.hasWhereExpression())
        return null;
    for (final RelationshipPattern relationship : path.getRelationships())
      if (relationship instanceof QuantifiedPathPattern || relationship.hasProperties()
          || relationship.getPropertiesParameterName() != null || relationship.hasWhereExpression())
        return null;
    return path;
  }

  /**
   * Whether a clause from {@code from} on reads one of the removed names, or could: a clause this class does not model is a
   * read, and so is a {@code *} projection when the removed names were carried by one.
   */
  private static boolean readsAfter(final SimpleCypherStatement statement, final int from, final Set<String> removed,
      final boolean carriedByStar) {
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    for (int i = from; i < clauses.size(); i++) {
      final ClauseEntry entry = clauses.get(i);
      switch (entry.getType()) {
      case MATCH -> {
        final MatchClause match = entry.getTypedClause();
        if (match.hasWhereClause() && reads(CypherReferencedVariables.of(match.getWhereClause().getConditionExpression()),
            removed))
          return true;
        for (final PathPattern path : match.getPathPatterns())
          if (reads(CypherReferencedVariables.of(path), removed))
            return true;
      }
      case WITH -> {
        final WithClause with = entry.getTypedClause();
        if (readsItems(with.getItems(), removed, carriedByStar) || readsOrderBy(with.getOrderByClause(), removed)
            || reads(with.getSkip(), removed) || reads(with.getLimit(), removed))
          return true;
        if (with.getWhereClause() != null
            && reads(CypherReferencedVariables.of(with.getWhereClause().getConditionExpression()), removed))
          return true;
      }
      case UNWIND -> {
        final UnwindClause unwind = entry.getTypedClause();
        if (removed.contains(unwind.getVariable()) || reads(unwind.getListExpression(), removed))
          return true;
      }
      case RETURN -> {
        final ReturnClause returnClause = entry.getTypedClause();
        if (readsItems(returnClause.getReturnItems(), removed, carriedByStar))
          return true;
      }
      default -> {
        return true;
      }
      }
    }
    return readsOrderBy(statement.getOrderByClause(), removed) || reads(statement.getSkip(), removed)
        || reads(statement.getLimit(), removed);
  }

  private static boolean readsItems(final List<ReturnClause.ReturnItem> items, final Set<String> removed,
      final boolean carriedByStar) {
    if (items == null)
      return false;
    for (final ReturnClause.ReturnItem item : items) {
      if (item.isStar()) {
        if (carriedByStar)
          return true;
        continue;
      }
      if (reads(item.getExpression(), removed) || removed.contains(item.getOutputName()))
        return true;
    }
    return false;
  }

  private static boolean readsOrderBy(final OrderByClause orderBy, final Set<String> removed) {
    if (orderBy == null)
      return false;
    for (final OrderByClause.OrderByItem item : orderBy.getItems())
      if (item.getExpressionAST() == null || reads(item.getExpressionAST(), removed))
        return true;
    return false;
  }

  private static boolean reads(final Expression expression, final Set<String> removed) {
    return expression != null && reads(CypherReferencedVariables.of(expression), removed);
  }

  private static boolean reads(final CypherReferencedVariables read, final Set<String> removed) {
    return read.referencesAny(removed);
  }

  /** {@code v IS NULL} for one of the introduced variables. */
  private static boolean isNullTestOf(final BooleanExpression conjunct, final Set<String> introduced) {
    return conjunct instanceof IsNullExpression isNull && !isNull.isNot()
        && isNull.getExpression() instanceof VariableExpression variable && introduced.contains(variable.getVariableName());
  }

  private static void flattenAnd(final BooleanExpression expression, final List<BooleanExpression> conjuncts) {
    if (expression instanceof LogicalExpression logical && logical.getOperator() == LogicalExpression.Operator.AND) {
      flattenAnd(logical.getLeft(), conjuncts);
      flattenAnd(logical.getRight(), conjuncts);
    } else
      conjuncts.add(expression);
  }

  private static BooleanExpression and(final List<BooleanExpression> conjuncts) {
    BooleanExpression result = null;
    for (final BooleanExpression conjunct : conjuncts)
      result = and(result, conjunct);
    return result;
  }

  private static BooleanExpression and(final BooleanExpression left, final BooleanExpression right) {
    if (left == null)
      return right;
    if (right == null)
      return left;
    return new LogicalExpression(LogicalExpression.Operator.AND, left, right);
  }

  /** A projection of plain variables under their own names, or {@code *}: it keeps every row and every value. */
  private static boolean isPassThrough(final WithClause with) {
    if (with.isDistinct())
      return false;
    for (final ReturnClause.ReturnItem item : with.getItems())
      if (!item.isStar() && !(item.getExpression() instanceof VariableExpression variable && variable.getVariableName()
          .equals(item.getOutputName())))
        return false;
    return true;
  }

  private static boolean returnsStar(final SimpleCypherStatement statement) {
    final ReturnClause returnClause = statement.getReturnClause();
    return returnClause == null || returnClause.isReturnAll();
  }

  private static WithClause filter(final BooleanExpression condition) {
    final List<ReturnClause.ReturnItem> star = new ArrayList<>(1);
    star.add(ReturnClause.ReturnItem.star());
    return new WithClause(star, false, new WhereClause(condition), null, null, null);
  }

  /**
   * The path with the introduced names dropped, read from a shared end when it has one: the predicate probes a single hop
   * from a bound start node directly and hands any other shape to an EXISTS subquery. The relationships are rebuilt from
   * their types, direction and bounds alone, which is all {@link #singlePlainPath} lets through: relaxing it to admit
   * properties or an inline WHERE has to carry them here, and to {@link #render}, too.
   */
  private static PathPattern anonymized(final PathPattern path, final Set<String> introduced, final Set<String> inScope) {
    final List<NodePattern> nodes = new ArrayList<>(path.getNodes().size());
    for (final NodePattern node : path.getNodes())
      nodes.add(node.getVariable() != null && introduced.contains(node.getVariable()) ?
          new NodePattern(null, node.getLabels(), null, null, null, node.isLabelDisjunction(), null) :
          node);
    final List<RelationshipPattern> relationships = new ArrayList<>(path.getRelationships().size());
    for (final RelationshipPattern relationship : path.getRelationships())
      relationships.add(relationship.getVariable() != null && !relationship.getVariable().isEmpty() ?
          new RelationshipPattern(null, relationship.getTypes(), relationship.getDirection(), null, null,
              relationship.getMinHops(), relationship.getMaxHops(), null) :
          relationship);

    final String first = nodes.getFirst().getVariable();
    final String last = nodes.getLast().getVariable();
    if ((first == null || !inScope.contains(first)) && last != null && inScope.contains(last)) {
      Collections.reverse(nodes);
      Collections.reverse(relationships);
      for (int i = 0; i < relationships.size(); i++) {
        final RelationshipPattern relationship = relationships.get(i);
        relationships.set(i, new RelationshipPattern(relationship.getVariable(), relationship.getTypes(),
            relationship.getDirection().reverse(), null, null, relationship.getMinHops(), relationship.getMaxHops(), null));
      }
    }
    return new PathPattern(nodes, relationships);
  }

  /** The pattern as Cypher text, which the predicate parses again when it runs as an EXISTS subquery. */
  private static String render(final PathPattern path) {
    final StringBuilder text = new StringBuilder();
    for (int i = 0; i < path.getNodes().size(); i++) {
      final NodePattern node = path.getNode(i);
      text.append('(');
      if (node.getVariable() != null)
        text.append(quoted(node.getVariable()));
      for (int l = 0; l < node.getLabels().size(); l++)
        text.append(l == 0 ? ":" : node.isLabelDisjunction() ? "|" : ":").append(quoted(node.getLabels().get(l)));
      text.append(')');

      if (i < path.getRelationshipCount()) {
        final RelationshipPattern relationship = path.getRelationship(i);
        text.append(relationship.getDirection() == Direction.IN ? "<-[" : "-[");
        for (int t = 0; t < relationship.getTypes().size(); t++)
          text.append(t == 0 ? ":" : "|").append(quoted(relationship.getTypes().get(t)));
        if (relationship.isVariableLength()) {
          text.append('*');
          if (relationship.getMinHops() != null)
            text.append(relationship.getMinHops());
          text.append("..");
          if (relationship.getMaxHops() != null)
            text.append(relationship.getMaxHops());
        }
        text.append(relationship.getDirection() == Direction.OUT ? "]->" : "]-");
      }
    }
    return text.toString();
  }

  private static String quoted(final String name) {
    return '`' + name.replace("`", "``") + '`';
  }

  private static Set<String> patternNames(final PathPattern path) {
    final Set<String> names = new HashSet<>();
    if (path.getPathVariable() != null)
      names.add(path.getPathVariable());
    for (final NodePattern node : path.getNodes())
      if (node.getVariable() != null && !node.getVariable().isEmpty())
        names.add(node.getVariable());
    for (final RelationshipPattern relationship : path.getRelationships())
      if (relationship.getVariable() != null && !relationship.getVariable().isEmpty())
        names.add(relationship.getVariable());
    return names;
  }

  /**
   * The statement over the new clause list. Only MATCH, WITH, UNWIND and RETURN reach here ({@link #hasModelledClausesOnly}),
   * so the write clauses and their flags the constructor takes are null and false by construction, not dropped.
   */
  private static SimpleCypherStatement rebuild(final SimpleCypherStatement statement, final List<ClauseEntry> entries) {
    final List<ClauseEntry> clauses = new ArrayList<>(entries.size());
    final List<MatchClause> matches = new ArrayList<>();
    final List<WithClause> withs = new ArrayList<>();
    final List<UnwindClause> unwinds = new ArrayList<>();
    for (final ClauseEntry entry : entries) {
      clauses.add(new ClauseEntry(entry.getType(), entry.getTypedClause(), clauses.size()));
      switch (entry.getType()) {
      case MATCH -> matches.add(entry.getTypedClause());
      case WITH -> withs.add(entry.getTypedClause());
      case UNWIND -> unwinds.add(entry.getTypedClause());
      default -> {
        // RETURN is the statement's own field, and nothing else is rewritten
      }
      }
    }
    return new SimpleCypherStatement(statement.getOriginalQuery(), matches, statement.getWhereClause(),
        statement.getReturnClause(), statement.getOrderByClause(), statement.getSkip(), statement.getLimit(), null, null, null,
        null, unwinds, withs, new ArrayList<>(), new ArrayList<>(), clauses, false, false, false, false);
  }
}
