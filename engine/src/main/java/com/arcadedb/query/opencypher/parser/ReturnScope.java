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

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.InternalVariables;
import com.arcadedb.query.opencypher.ast.CallClause;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CreateClause;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.LoadCSVClause;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.MergeClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.QuantifiedPathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SubqueryClause;
import com.arcadedb.query.opencypher.ast.UnionStatement;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The variables in scope at the RETURN of a statement, read from the statement without running it. It answers the columns a
 * zero-row {@code RETURN *} declares (issue #11543), and the variables the star of {@code RETURN *, expr AS alias} stands for
 * (issue #11556).
 */
public final class ReturnScope {

  private ReturnScope() {
  }

  /**
   * Replaces the star of a RETURN that also has explicit items ({@code RETURN *, z.i AS i}) with the variables in scope at the
   * RETURN, in place, so every execution path (projection, DISTINCT, the aggregations and their grouping keys) sees ordinary
   * items. A bare {@code RETURN *} is left alone. Each branch of a UNION is expanded on its own.
   * <p>
   * A variable an explicit item also projects is not repeated: the explicit item answers it. When the scope cannot be read
   * without running (it passes through a procedure the engine does not know, or a subquery whose own {@code RETURN *} scope
   * is not known), the star is kept and the projection resolves it at run time; an aggregation cannot group by a key unknown
   * at planning time, so that case is refused rather than answered without its grouping keys.
   */
  public static void expandStarMixedWithItems(final CypherStatement statement) {
    if (statement instanceof UnionStatement union) {
      for (final CypherStatement branch : union.getQueries())
        expandStarMixedWithItems(branch);
      return;
    }

    final ReturnClause returnClause = statement.getReturnClause();
    if (returnClause == null || returnClause.isReturnAll())
      return;

    final List<ReturnClause.ReturnItem> items = returnClause.getReturnItems();
    int starIndex = -1;
    for (int i = 0; i < items.size(); i++)
      if (items.get(i).isStar()) {
        starIndex = i;
        break;
      }
    if (starIndex < 0)
      return;

    final Set<String> scope = scopeAtReturn(statement);
    if (scope == null) {
      if (returnClause.hasAggregations())
        throw new CommandParsingException(
            "RETURN *, combined with an aggregation is not supported when the variables in scope are only known at run time; "
                + "list the grouping keys explicitly");
      return;
    }

    final Set<String> explicit = new HashSet<>();
    for (final ReturnClause.ReturnItem item : items)
      if (!item.isStar())
        explicit.add(item.getOutputName());

    final List<ReturnClause.ReturnItem> expanded = new ArrayList<>(scope.size());
    for (final String variable : scope)
      if (!explicit.contains(variable)) {
        final ReturnClause.ReturnItem item = new ReturnClause.ReturnItem(new VariableExpression(variable), null);
        item.setExpressionText(variable);
        expanded.add(item);
      }

    items.remove(starIndex);
    items.addAll(starIndex, expanded);
  }

  /** The names a CALL ... YIELD binds, or {@code null} when it has no YIELD or yields {@code *} of a procedure not known. */
  public static List<String> yieldedFields(final CallClause call) {
    if (!call.hasYield())
      return null;
    if (call.isYieldAll())
      return procedureOutputs(call);
    final List<String> names = new ArrayList<>(call.getYieldItems().size());
    for (final CallClause.YieldItem item : call.getYieldItems())
      names.add(item.getOutputName());
    return names;
  }

  /** Every output of the procedure a CALL runs, or {@code null} when the engine does not know the procedure. */
  public static List<String> procedureOutputs(final CallClause call) {
    final CypherProcedure procedure = call.resolveProcedure();
    return procedure != null ? new ArrayList<>(procedure.getYieldFields()) : null;
  }

  /**
   * The columns a RETURN answers, in order: its items, with the variables in scope at it in place of {@code *}. {@code null}
   * when the scope of a {@code *} is not known without running the statement.
   */
  public static List<String> returnedColumns(final CypherStatement statement, final ReturnClause returnClause) {
    final List<ReturnClause.ReturnItem> items = returnClause.getReturnItems();
    final List<String> columns = new ArrayList<>(items.size());
    for (final ReturnClause.ReturnItem item : items) {
      if (item.isStar()) {
        final Set<String> scope = scopeAtReturn(statement);
        if (scope == null)
          return null;
        for (final String variable : scope)
          if (!columns.contains(variable))
            columns.add(variable);
      } else
        columns.add(item.getOutputName());
    }
    return columns;
  }

  /** What the scope walk of {@link #scopeAtReturn} does after a clause. */
  private enum ScopeWalk {
    /** the clause is accounted for: go on with the next one */
    NEXT,
    /** the RETURN is reached: the scope is complete */
    DONE,
    /** the clause binds names the walk cannot know without running: the scope is unknown */
    UNKNOWN
  }

  /**
   * The variables in scope at the RETURN of a statement, in the order its clauses bind them, which is the order a {@code RETURN
   * *} row carries them in (Neo4j sorts them alphabetically instead: #11557 tracks switching the rows and this together).
   * Anonymous pattern elements, and the names the executor generates for them, are not variables. {@code null} when the scope
   * passes through something this walk does not follow.
   */
  public static Set<String> scopeAtReturn(final CypherStatement statement) {
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses == null || clauses.isEmpty())
      return null;
    final Set<String> scope = new LinkedHashSet<>();
    for (final ClauseEntry entry : clauses) {
      switch (bindClause(entry, scope)) {
      case NEXT -> {
      }
      case DONE -> {
        return scope;
      }
      case UNKNOWN -> {
        return null;
      }
      }
    }
    return scope;
  }

  /**
   * Adds the names one clause binds to the scope. A switch expression with no default on purpose: a new
   * {@link ClauseEntry.ClauseType} does not compile until it says what it binds, so {@code RETURN *} cannot silently miss it.
   */
  private static ScopeWalk bindClause(final ClauseEntry entry, final Set<String> scope) {
    return switch (entry.getType()) {
      case MATCH -> {
        final MatchClause match = entry.getTypedClause();
        if (match.hasPathPatterns())
          for (final PathPattern path : match.getPathPatterns())
            bindPattern(path, scope);
        yield ScopeWalk.NEXT;
      }
      case CREATE -> {
        final CreateClause create = entry.getTypedClause();
        if (create != null && !create.isEmpty())
          for (final PathPattern path : create.getPathPatterns())
            bindPattern(path, scope);
        yield ScopeWalk.NEXT;
      }
      case MERGE -> {
        final MergeClause merge = entry.getTypedClause();
        if (merge != null)
          bindPattern(merge.getPathPattern(), scope);
        yield ScopeWalk.NEXT;
      }
      case WITH -> {
        final WithClause with = entry.getTypedClause();
        boolean forwardsAll = false;
        final List<String> projected = new ArrayList<>(with.getItems().size());
        for (final ReturnClause.ReturnItem item : with.getItems())
          if (item.isStar())
            forwardsAll = true;
          else
            projected.add(item.getOutputName());
        if (!forwardsAll)
          scope.clear();
        for (final String name : projected)
          bind(name, scope);
        yield ScopeWalk.NEXT;
      }
      case UNWIND -> {
        bind(((UnwindClause) entry.getTypedClause()).getVariable(), scope);
        yield ScopeWalk.NEXT;
      }
      case LOAD_CSV -> {
        bind(((LoadCSVClause) entry.getTypedClause()).getVariable(), scope);
        yield ScopeWalk.NEXT;
      }
      case CALL -> {
        // An in-query CALL with no YIELD is a side-effect call: it binds nothing
        final CallClause call = entry.getTypedClause();
        if (!call.hasYield())
          yield ScopeWalk.NEXT;
        final List<String> yielded = yieldedFields(call);
        if (yielded == null)
          yield ScopeWalk.UNKNOWN;
        for (final String name : yielded)
          bind(name, scope);
        yield ScopeWalk.NEXT;
      }
      case SUBQUERY -> {
        // What a CALL { } subquery returns joins the enclosing scope, after the names already in it (the order the executor
        // merges the body's row into the outer one); a unit subquery (no RETURN) adds nothing. The branches of a UNION body
        // return the same columns, under the names of the first one. A RETURN * of the body is the scope at that RETURN,
        // walked the same way (issue #11571): the names the body imports are outer names already, so the walk does not need
        // to seed them, and the WITH that imports them clears and rebinds the body's scope as any WITH does
        final CypherStatement body = ((SubqueryClause) entry.getTypedClause()).getInnerStatement();
        final CypherStatement inner = body instanceof UnionStatement union ? union.getQueries().getFirst() : body;
        if (inner == null || inner.getReturnClause() == null)
          yield ScopeWalk.NEXT;
        final List<String> returned = returnedColumns(inner, inner.getReturnClause());
        if (returned == null)
          yield ScopeWalk.UNKNOWN;
        for (final String name : returned)
          bind(name, scope);
        yield ScopeWalk.NEXT;
      }
      // the FOREACH variable, and what its inner clauses bind, live and die inside the loop
      case SET, REMOVE, DELETE, FOREACH -> ScopeWalk.NEXT;
      case RETURN -> ScopeWalk.DONE;
      // FINISH answers no columns (declaredColumns refuses it first)
      case FINISH -> ScopeWalk.UNKNOWN;
    };
  }

  /** Binds the variables of a pattern in the order the match binds them: each element along the path, then the path. */
  private static void bindPattern(final PathPattern path, final Set<String> scope) {
    if (path == null)
      return;
    final List<NodePattern> nodes = path.getNodes();
    final List<RelationshipPattern> relationships = path.getRelationships();
    for (int i = 0; i < nodes.size(); i++) {
      bind(nodes.get(i).getVariable(), scope);
      if (i < relationships.size()) {
        final RelationshipPattern relationship = relationships.get(i);
        bind(relationship.getVariable(), scope);
        if (relationship instanceof QuantifiedPathPattern quantified)
          for (final String groupVariable : quantified.getGroupVariables())
            bind(groupVariable, scope);
      }
    }
    if (path.hasPathVariable())
      bind(path.getPathVariable(), scope);
  }

  private static void bind(final String variable, final Set<String> scope) {
    if (variable != null && !variable.isEmpty() && !InternalVariables.isInternal(variable))
      scope.add(variable);
  }
}
