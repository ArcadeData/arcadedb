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
import com.arcadedb.query.opencypher.ast.MergeClause;
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
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The predicates that selected the records a row binds, as they stand at one clause of a statement: the inline property maps and
 * the WHERE of the MATCH clauses that bound them, the inline property maps of the pattern a MERGE matched, and the WHERE of a later
 * WITH, carried through every WITH that passes the variables they read on, under their own name or another one (issues #11818,
 * #12593).
 * <p>
 * A stand-alone SET reloads its target to the latest committed version before writing it (issue #5227), so a commit that landed
 * between the MATCH that read the record and the SET is built upon rather than overwritten; so do a REMOVE, a MERGE action and a
 * label write. The predicates were decided on the record as read, though, and that commit may have changed what they read: both
 * {@code MATCH (r {k: $k}) WHERE r.claimed IS NULL SET r.claimed = $me} of two concurrent transactions claimed the record.
 * {@link #requireStillSelected} evaluates them again on the reloaded record, and refuses the write as a retryable conflict when one
 * no longer holds: the retry runs the statement against the new state.
 * <p>
 * The write of a body (a {@code FOREACH}, a {@code CALL { }}) is checked against the predicates of the enclosing statement too: a
 * body is planned with the predicates in force where it starts ({@link #inBody}), and its own clauses add theirs
 * ({@link #nestedBefore}).
 * <p>
 * Kept is only what can be evaluated again against the row the write receives: the predicates of an OPTIONAL MATCH select no row,
 * a predicate that reads a variable a WITH drops or computes cannot be evaluated against that row any more, and neither can one a
 * body cannot see (a {@code CALL} body that does not import it, a {@code FOREACH} variable that shadows it). A predicate behind a
 * WITH that renames a variable it reads is evaluated against the row with that variable bound under the name the predicate reads.
 */
public final class SelectionPredicates {
  /** No predicate: the write of a statement whose predicates are not known. */
  public static final SelectionPredicates NONE = new SelectionPredicates(null, null, null, List.of(), null, false);
  // What a rename answers for an entry that reads a variable the WITH does not pass on: compared by identity
  private static final Map<String, String> DROPPED = Map.of("", "");

  // One entry of an inline property map, kept or dropped on its own: a WITH that drops a variable one value reads leaves the other
  // entries in force. reads: the variables the value reads, which select the record of the variable as much as it does. names: the
  // name the row binds now for each name the entry reads that a WITH renamed since (empty while none was)
  private record InlineMap(String variable, Map<String, Object> properties, Set<String> reads, Map<String, String> names) {
  }

  private record Condition(BooleanExpression expression, Set<String> variables, Map<String, String> names) {
  }

  // The predicates in force where the clauses below start, for the clauses of a body; null for the clauses of a statement
  private final SelectionPredicates enclosing;
  // The names of the enclosing predicates a body can see: those a CALL imports (null: all of them)
  private final Set<String>         visible;
  // The name a body binds for itself over an enclosing one: the variable of a FOREACH (null: none)
  private final String              hidden;
  private final List<ClauseEntry>   clauses;
  private final Object              clause;
  // Whether the predicates the clause itself adds are in force: those of the pattern of a MERGE, for its own actions
  private final boolean             inclusive;
  // Collected on the first requireStillSelected(): only a write whose target was reloaded with changed content, or loaded lazily,
  // needs them, so planning pays nothing for them. Not thread-safe, like the step that holds this: it applies its rows one at a time
  private       List<InlineMap>     inlineMaps;
  private       List<Condition>     conditions;

  private SelectionPredicates(final SelectionPredicates enclosing, final Set<String> visible, final String hidden,
      final List<ClauseEntry> clauses, final Object clause, final boolean inclusive) {
    this.enclosing = enclosing;
    this.visible = visible;
    this.hidden = hidden;
    this.clauses = clauses;
    this.clause = clause;
    this.inclusive = inclusive;
    if (enclosing == null && clauses.isEmpty()) {
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
    return clauses == null ? NONE : new SelectionPredicates(null, null, null, clauses, clause, false);
  }

  /**
   * The predicates in force at the actions of {@code merge}: those of the clauses before it and those of its own pattern, which
   * selected the record an {@code ON MATCH SET} writes.
   */
  public static SelectionPredicates through(final List<ClauseEntry> clauses, final MergeClause merge) {
    return clauses == null ? NONE : new SelectionPredicates(null, null, null, clauses, merge, true);
  }

  /**
   * The predicates of this statement in force where a body planned on its own starts: a {@code CALL { }} body that sees only the
   * outer variables of {@code visible}, or a {@code FOREACH} body that binds {@code hidden} over an outer variable of that name.
   *
   * @param visible the outer variables the body sees, null for all of them
   * @param hidden  the variable the body binds for itself, null for none
   */
  public SelectionPredicates inBody(final Set<String> visible, final String hidden) {
    if (this == NONE)
      return NONE;
    return new SelectionPredicates(this, visible, hidden, List.of(), null, false);
  }

  /** {@link #before}, for a clause of a body whose enclosing predicates are these ({@link #inBody}). */
  public SelectionPredicates nestedBefore(final List<ClauseEntry> bodyClauses, final Object bodyClause) {
    return new SelectionPredicates(this, null, null, bodyClauses != null ? bodyClauses : List.of(), bodyClause, false);
  }

  /** {@link #through}, for a MERGE of a body whose enclosing predicates are these ({@link #inBody}). */
  public SelectionPredicates nestedThrough(final List<ClauseEntry> bodyClauses, final MergeClause merge) {
    return new SelectionPredicates(this, null, null, bodyClauses != null ? bodyClauses : List.of(), merge, true);
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
    collectIfNeeded();
    for (final InlineMap inlineMap : inlineMaps) {
      final String variable = current(inlineMap.names(), inlineMap.variable());
      // A map of another variable whose values read the record no longer selects that variable's record either
      if (boundTo(rid, row, variable) == null && !readsRecord(rid, row, inlineMap.names(), inlineMap.reads()))
        continue;
      final Document record = InlineProperties.boundRecord(row.getProperty(variable));
      if (record != null && !InlineProperties.matches(record, inlineMap.properties(), rowFor(inlineMap.names(), row), context))
        return false;
    }
    for (final Condition condition : conditions)
      if (readsRecord(rid, row, condition.names(), condition.variables()) && !condition.expression()
          .evaluate(rowFor(condition.names(), row), context))
        return false;
    return true;
  }

  private void collectIfNeeded() {
    if (inlineMaps != null)
      return;
    final List<InlineMap> collectedMaps = new ArrayList<>();
    final List<Condition> collectedConditions = new ArrayList<>();
    if (enclosing != null) {
      enclosing.collectIfNeeded();
      for (final InlineMap inlineMap : enclosing.inlineMaps)
        if (inScope(inlineMap.names(), inlineMap.variable()) && inScope(inlineMap.names(), inlineMap.reads()))
          collectedMaps.add(inlineMap);
      for (final Condition condition : enclosing.conditions)
        if (inScope(condition.names(), condition.variables()))
          collectedConditions.add(condition);
    }
    for (final ClauseEntry entry : clauses) {
      final boolean last = entry.getClause() == clause;
      if (last && !inclusive)
        break;
      switch (entry.getType()) {
      case MATCH -> {
        final MatchClause match = entry.getTypedClause();
        if (!match.isOptional()) {
          for (final PathPattern path : match.getPathPatterns())
            addPattern(path, collectedMaps);
          addConditions(match.getWhereClause(), collectedConditions);
        }
      }
      case MERGE -> {
        // The pattern a MERGE matched selected the record it binds, as a MATCH does. A record the MERGE created is the
        // transaction's own copy, which no write checks again
        final MergeClause merge = entry.getTypedClause();
        addPattern(merge.getPathPattern(), collectedMaps);
      }
      case WITH -> {
        final WithClause with = entry.getTypedClause();
        passOn(with, collectedMaps, collectedConditions);
        addConditions(with.getWhereClause(), collectedConditions);
      }
      default -> {
        // no other clause filters rows or rebinds a variable
      }
      }
      if (last)
        break;
    }
    inlineMaps = collectedMaps;
    conditions = collectedConditions;
  }

  private boolean inScope(final Map<String, String> names, final String name) {
    final String now = current(names, name);
    return !now.equals(hidden) && (visible == null || visible.contains(now));
  }

  private boolean inScope(final Map<String, String> names, final Set<String> read) {
    for (final String name : read)
      if (!inScope(names, name))
        return false;
    return true;
  }

  private static void addPattern(final PathPattern path, final List<InlineMap> inlineMaps) {
    if (path == null)
      return;
    for (final NodePattern node : path.getNodes())
      addInlineMap(node.getVariable(), node.getProperties(), inlineMaps);
    for (final RelationshipPattern relationship : path.getRelationships())
      // A quantified group binds lists of records, which no write target is
      if (!(relationship instanceof QuantifiedPathPattern))
        addInlineMap(relationship.getVariable(), relationship.getProperties(), inlineMaps);
  }

  private static void addInlineMap(final String variable, final Map<String, Object> properties, final List<InlineMap> inlineMaps) {
    if (variable == null || properties == null)
      return;
    for (final Map.Entry<String, Object> entry : properties.entrySet())
      inlineMaps.add(new InlineMap(variable, Collections.singletonMap(entry.getKey(), entry.getValue()),
          entry.getValue() instanceof Expression expression ? WhereClause.collectExpressionVariables(expression) : Set.of(), Map.of()));
  }

  private static void addConditions(final WhereClause where, final List<Condition> conditions) {
    if (where == null || where.getConditionExpression() == null)
      return;
    for (final BooleanExpression conjunct : WhereClause.conjuncts(where.getConditionExpression()))
      conditions.add(new Condition(conjunct, WhereClause.collectVariables(conjunct), Map.of()));
  }

  /**
   * Keeps what reads only variables {@code with} passes on, under their own name or another one ({@code WITH r AS x}), and drops
   * the rest. {@code WITH *} (and a {@code LET}, which is {@code WITH *, expr AS x}) passes every variable on under its own name.
   */
  private static void passOn(final WithClause with, final List<InlineMap> inlineMaps, final List<Condition> conditions) {
    // The name each variable the WITH passes on is bound to after it: its own when the WITH passes it on under it too
    final Map<String, String> passedOn = new HashMap<>();
    for (final ReturnClause.ReturnItem item : with.getItems()) {
      if (item.isStar())
        return;
      if (item.getExpression() instanceof VariableExpression variable) {
        final String name = variable.getVariableName();
        final String alias = item.getAlias() != null ? item.getAlias() : name;
        if (alias.equals(name))
          passedOn.put(name, name);
        else
          passedOn.putIfAbsent(name, alias);
      }
    }
    for (int i = inlineMaps.size() - 1; i >= 0; i--) {
      final InlineMap inlineMap = inlineMaps.get(i);
      final Map<String, String> names = passOn(inlineMap.names(), inlineMap.variable(), inlineMap.reads(), passedOn);
      if (names == null)
        inlineMaps.remove(i);
      else if (names != inlineMap.names())
        inlineMaps.set(i, new InlineMap(inlineMap.variable(), inlineMap.properties(), inlineMap.reads(), names));
    }
    for (int i = conditions.size() - 1; i >= 0; i--) {
      final Condition condition = conditions.get(i);
      final Map<String, String> names = passOn(condition.names(), null, condition.variables(), passedOn);
      if (names == null)
        conditions.remove(i);
      else if (names != condition.names())
        conditions.set(i, new Condition(condition.expression(), condition.variables(), names));
    }
  }

  /**
   * @return the names the row binds after the WITH for the names an entry reads ({@code variable}, if not null, and {@code read}):
   * {@code names} itself when none changed, null when the WITH does not pass one of them on
   */
  private static Map<String, String> passOn(final Map<String, String> names, final String variable, final Set<String> read,
      final Map<String, String> passedOn) {
    Map<String, String> renamed = null;
    if (variable != null) {
      renamed = rename(names, variable, passedOn, renamed);
      if (renamed == DROPPED)
        return null;
    }
    for (final String name : read) {
      renamed = rename(names, name, passedOn, renamed);
      if (renamed == DROPPED)
        return null;
    }
    return renamed != null ? renamed : names;
  }

  private static Map<String, String> rename(final Map<String, String> names, final String name, final Map<String, String> passedOn,
      Map<String, String> renamed) {
    final String now = current(names, name);
    final String after = passedOn.get(now);
    if (after == null)
      return DROPPED;
    if (after.equals(now))
      return renamed;
    if (renamed == null)
      renamed = new HashMap<>(names);
    if (after.equals(name))
      renamed.remove(name);
    else
      renamed.put(name, after);
    return renamed;
  }

  /** The name the row binds now for {@code name}, as an entry reads it. */
  private static String current(final Map<String, String> names, final String name) {
    if (names.isEmpty())
      return name;
    final String now = names.get(name);
    return now != null ? now : name;
  }

  /**
   * The row an entry is evaluated against: {@code row} itself while no WITH renamed what the entry reads, otherwise a copy of it
   * that binds each renamed variable under the name the entry reads too. Only a write whose target was reloaded with other content
   * evaluates an entry, so the copy costs nothing to the uncontended write.
   */
  private static Result rowFor(final Map<String, String> names, final Result row) {
    if (names.isEmpty())
      return row;
    final ResultInternal view = new ResultInternal();
    for (final String property : row.getPropertyNames())
      view.setProperty(property, row.getProperty(property));
    for (final Map.Entry<String, String> renamed : names.entrySet())
      view.setProperty(renamed.getKey(), row.getProperty(renamed.getValue()));
    return view;
  }

  private static Document boundTo(final RID rid, final Result row, final String variable) {
    final Document record = InlineProperties.boundRecord(row.getProperty(variable));
    return record != null && rid.equals(record.getIdentity()) ? record : null;
  }

  private static boolean readsRecord(final RID rid, final Result row, final Map<String, String> names, final Set<String> variables) {
    for (final String variable : variables)
      if (boundTo(rid, row, current(names, variable)) != null)
        return true;
    return false;
  }
}
