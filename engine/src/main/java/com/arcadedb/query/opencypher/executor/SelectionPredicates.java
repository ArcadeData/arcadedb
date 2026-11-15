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

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.QuantifiedPathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The predicates that selected the records a row binds, as they stand at one clause of a statement: the inline property maps and
 * the WHERE of the MATCH clauses that bound them, and the WHERE of a later WITH, carried through every WITH that passes the
 * variables they read on unchanged (issue #11818).
 * <p>
 * A stand-alone SET reloads its target to the latest committed version before writing it (issue #5227), so a commit that landed
 * between the MATCH that read the record and the SET is built upon rather than overwritten; so does a REMOVE. The predicates were
 * decided on the record as read, though, and that commit may have changed what they read: both {@code MATCH (r {k: $k}) WHERE
 * r.claimed IS NULL SET r.claimed = $me} of two concurrent transactions claimed the record. {@link #requireStillSelected}
 * evaluates them again on the reloaded record, and refuses the write as a retryable conflict when one no longer holds: the retry
 * runs the statement against the new state.
 * <p>
 * Kept is only what can be evaluated again against the row the write receives: the predicates of an OPTIONAL MATCH select no row,
 * and a predicate that reads a variable a WITH drops, renames or computes cannot be evaluated against that row any more.
 */
public final class SelectionPredicates {
  /** No predicate: the write of a statement whose predicates are not known. */
  public static final SelectionPredicates NONE = new SelectionPredicates(List.of(), null);

  // One entry of an inline property map, kept or dropped on its own: a WITH that drops a variable one value reads leaves the other
  // entries in force. reads: the variables the value reads, which select the record of the variable as much as it does
  private record InlineMap(String variable, Map<String, Object> properties, Set<String> reads) {
  }

  private record Condition(BooleanExpression expression, Set<String> variables) {
  }

  private final List<ClauseEntry> clauses;
  private final Object            clause;
  // Collected on the first requireStillSelected(): only a write whose target was reloaded with changed content, or loaded lazily,
  // needs them, so planning pays nothing for them. Not thread-safe, like the step that holds this: it applies its rows one at a time
  private       List<InlineMap>   inlineMaps;
  private       List<Condition>   conditions;

  private SelectionPredicates(final List<ClauseEntry> clauses, final Object clause) {
    this.clauses = clauses;
    this.clause = clause;
    if (clauses.isEmpty()) {
      // NONE is shared by every step: nothing is ever collected into it
      inlineMaps = List.of();
      conditions = List.of();
    }
  }

  /**
   * The predicates in force at {@code clause}: those of the clauses of {@code clauses} before it.
   *
   * @param clauses the clauses of the statement, in order
   * @param clause  the clause (not its entry) the predicates are wanted at, found by identity
   */
  public static SelectionPredicates before(final List<ClauseEntry> clauses, final Object clause) {
    return new SelectionPredicates(clauses, clause);
  }

  /**
   * Whether there is no predicate at all: a write then has nothing to evaluate again, and a DELETE nothing to reload its targets for.
   */
  public boolean isEmpty() {
    if (inlineMaps == null)
      collect();
    return inlineMaps.isEmpty() && conditions.isEmpty();
  }

  /**
   * Refuses, with a retryable {@link ConcurrentModificationException}, the write of the record {@code rid}, which was reloaded with
   * content a concurrent commit changed since the row read it, when a predicate that reads the record (through any
   * variable of {@code row} bound to it) no longer holds on {@code row}, which binds the reloaded record already. A predicate that
   * also reads other variables reads them as the row holds them. A predicate is evaluated again as it was the first time, so a
   * function with side effects runs once more.
   */
  public void requireStillSelected(final RID rid, final Result row, final CommandContext context) {
    if (!holdFor(rid, row, context))
      throw new ConcurrentModificationException("Record " + rid + " was modified by a concurrent transaction after the statement "
          + "selected it, and no longer satisfies the predicates that selected it. Please retry the operation");
  }

  private boolean holdFor(final RID rid, final Result row, final CommandContext context) {
    if (inlineMaps == null)
      collect();
    for (final InlineMap inlineMap : inlineMaps) {
      // A map of another variable whose values read the record no longer selects that variable's record either
      if (boundTo(rid, row, inlineMap.variable()) == null && !readsRecord(rid, row, inlineMap.reads()))
        continue;
      final Document record = InlineProperties.boundRecord(row.getProperty(inlineMap.variable()));
      if (record != null && !InlineProperties.matches(record, inlineMap.properties(), row, context))
        return false;
    }
    for (final Condition condition : conditions)
      if (readsRecord(rid, row, condition.variables()) && !condition.expression().evaluate(row, context))
        return false;
    return true;
  }

  private void collect() {
    final List<InlineMap> collectedMaps = new ArrayList<>();
    final List<Condition> collectedConditions = new ArrayList<>();
    for (final ClauseEntry entry : clauses) {
      if (entry.getClause() == clause)
        break;
      switch (entry.getType()) {
      case MATCH -> {
        final MatchClause match = entry.getTypedClause();
        if (!match.isOptional())
          addMatch(match, collectedMaps, collectedConditions);
      }
      case WITH -> {
        final WithClause with = entry.getTypedClause();
        keepPassedOn(with, collectedMaps, collectedConditions);
        addConditions(with.getWhereClause(), collectedConditions);
      }
      default -> {
        // no other clause filters rows or rebinds a variable
      }
      }
    }
    inlineMaps = collectedMaps;
    conditions = collectedConditions;
  }

  private static void addMatch(final MatchClause match, final List<InlineMap> inlineMaps, final List<Condition> conditions) {
    for (final PathPattern path : match.getPathPatterns()) {
      for (final NodePattern node : path.getNodes())
        addInlineMap(node.getVariable(), node.getProperties(), inlineMaps);
      for (final RelationshipPattern relationship : path.getRelationships())
        // A quantified group binds lists of records, which no write target is
        if (!(relationship instanceof QuantifiedPathPattern))
          addInlineMap(relationship.getVariable(), relationship.getProperties(), inlineMaps);
    }
    addConditions(match.getWhereClause(), conditions);
  }

  private static void addInlineMap(final String variable, final Map<String, Object> properties, final List<InlineMap> inlineMaps) {
    if (variable == null || properties == null)
      return;
    for (final Map.Entry<String, Object> entry : properties.entrySet())
      inlineMaps.add(new InlineMap(variable, Collections.singletonMap(entry.getKey(), entry.getValue()),
          entry.getValue() instanceof Expression expression ? WhereClause.collectExpressionVariables(expression) : Set.of()));
  }

  private static void addConditions(final WhereClause where, final List<Condition> conditions) {
    if (where == null || where.getConditionExpression() == null)
      return;
    for (final BooleanExpression conjunct : WhereClause.conjuncts(where.getConditionExpression()))
      conditions.add(new Condition(conjunct, WhereClause.collectVariables(conjunct)));
  }

  /**
   * Drops what reads a variable {@code with} does not pass on under its own name. {@code WITH *} (and a {@code LET}, which is
   * {@code WITH *, expr AS x}) passes every variable on.
   */
  private static void keepPassedOn(final WithClause with, final List<InlineMap> inlineMaps, final List<Condition> conditions) {
    final Set<String> passedOn = new HashSet<>();
    for (final ReturnClause.ReturnItem item : with.getItems()) {
      if (item.isStar())
        return;
      if (item.getExpression() instanceof VariableExpression variable && (item.getAlias() == null || item.getAlias()
          .equals(variable.getVariableName())))
        passedOn.add(variable.getVariableName());
    }
    inlineMaps.removeIf(inlineMap -> !passedOn.contains(inlineMap.variable()) || !passedOn.containsAll(inlineMap.reads()));
    conditions.removeIf(condition -> !passedOn.containsAll(condition.variables()));
  }

  private static Document boundTo(final RID rid, final Result row, final String variable) {
    final Document record = InlineProperties.boundRecord(row.getProperty(variable));
    return record != null && rid.equals(record.getIdentity()) ? record : null;
  }

  private static boolean readsRecord(final RID rid, final Result row, final Set<String> variables) {
    for (final String variable : variables)
      if (boundTo(rid, row, variable) != null)
        return true;
    return false;
  }
}
