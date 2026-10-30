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

import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CollectExpression;
import com.arcadedb.query.opencypher.ast.CountExpression;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.ExistsExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PatternComprehensionExpression;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SimpleCypherStatement;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;
import com.arcadedb.query.sql.executor.CorrelatedSubQueryCache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Decides whether the last clauses of a read-only {@code RETURN DISTINCT} can run as a semi-join (issue #11009).
 * <p>
 * From some clause on, nothing binds a name the RETURN items read: every row the remaining clauses produce for one
 * input row carries that input row's values for those names, unchanged, and DISTINCT folds all those rows into one.
 * So only <i>whether</i> the remaining clauses produce a row for an input row can change the result, and they run as
 * the chain of a {@code SemiJoinStep}, which stops at the first row. Three things make that cheaper than the full
 * expansion, and each is decided here:
 * <ul>
 * <li>the chain stops at its first row, where the expansion enumerated every combination;</li>
 * <li>an input row whose returned names hold values that already qualified is not tested at all: DISTINCT would drop
 * whatever it produced ({@link Plan#distinctKey()});</li>
 * <li>when the chain reads none of the returned names, but only passes them on, its answer is a function of the other
 * names it reads, and is remembered by their values ({@link Plan#memoKey()}): in the shape of the report the chain
 * reads the rule and not the host, so it runs once per rule instead of once per (host, rule) pair.</li>
 * </ul>
 * The remaining clauses may be MATCH and OPTIONAL MATCH, UNWIND, and WITHs that forward every returned name as it is
 * and neither aggregate, deduplicate, order, skip nor limit: those look across rows, which a per-row test cannot.
 * <p>
 * <b>The RETURN items must be functions of the names they read.</b> {@code RETURN DISTINCT h.name, rand()} gives one
 * row per combination the expansion produces, each with its own random number, so stopping at the first combination
 * would drop rows. Every function a RETURN item calls must be confirmed deterministic, or nothing is planned. The same
 * holds for the memo, which also refuses a chain holding a pattern inside an expression or a subquery expression: the
 * names those read are not modelled as plain reads here, and a remembered answer is only right when every name it
 * depends on is in its key.
 * <p>
 * Being wrong is not symmetric: a refused shape keeps the full expansion, a wrongly accepted one returns fewer rows.
 * Every shape that is not understood in full is therefore refused.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class DistinctSemiJoin {
  /**
   * The Cypher functions that can answer differently for the same arguments within one statement, or that read the
   * row's hidden context rather than a named variable ({@code file()}, {@code linenumber()}), under the name they are
   * registered by ({@code apoc.} and {@code sql.} are stripped first). A bare name resolving to a built-in SQL function is
   * classified by {@link CorrelatedSubQueryCache#NON_REPEATABLE_FUNCTIONS}. The no-argument temporal constructors are
   * not here: they answer from one clock per statement.
   * <p>
   * {@code Issue11009DistinctSemiJoinTest} lists every other registered Cypher function as reviewed, and fails on a
   * newly registered name until it is classified on one side or the other: a varying function wrongly read as pure
   * would make the semi-join drop rows.
   */
  private static final Set<String> VARYING_FUNCTIONS = Set.of("rand", "randomuuid", "timestamp", "file", "linenumber",
      "text.random", "create.uuid", "create.uuidbase64", "date.currenttimestamp", "create.vnode", "create.vrelationship");

  /**
   * Where the semi-join starts and what it keys on.
   *
   * @param start       the index of the first clause the chain runs
   * @param distinctKey the names the RETURN items and ORDER BY read: two input rows with the same values for them
   *                    project the same row, so once one qualified the other need not be tested
   * @param memoKey     the names of the input row the chain's answer depends on, or null when it is not remembered
   */
  public record Plan(int start, List<String> distinctKey, List<String> memoKey) {
  }

  private DistinctSemiJoin() {
  }

  /**
   * Whether a call of {@code rawName} can answer differently for the same arguments within one statement, or is not
   * known to be a function of its arguments at all (a user function).
   */
  public static boolean isVaryingFunction(final String rawName) {
    String name = rawName.toLowerCase(Locale.ROOT);
    if (name.startsWith("apoc."))
      name = name.substring("apoc.".length());
    else if (name.startsWith("sql."))
      name = name.substring("sql.".length());
    return VARYING_FUNCTIONS.contains(name) || name.endsWith(".realtime")
        || CorrelatedSubQueryCache.NON_REPEATABLE_FUNCTIONS.contains(name)
        || !SimpleCypherStatement.isConfirmedPureFunctionName(rawName);
  }

  /**
   * @param statement      the statement being planned
   * @param clausesInOrder its clauses, the last one being its RETURN
   *
   * @return where the semi-join starts, or null when the statement cannot take one
   */
  public static Plan plan(final CypherStatement statement, final List<ClauseEntry> clausesInOrder) {
    final ReturnClause returnClause = statement.getReturnClause();
    final int returnIndex = clausesInOrder.size() - 1;
    if (returnClause == null || !returnClause.isDistinct() || returnClause.hasAggregations() || returnClause.isReturnAll()
        || statement.getWhereClause() != null || !statement.isReadOnly() || returnIndex < 2
        || clausesInOrder.get(returnIndex).getType() != ClauseEntry.ClauseType.RETURN)
      return null;

    final Set<String> readNames = new HashSet<>();
    final Set<String> returnNames = new HashSet<>();
    final PerRowCheck returnCheck = new PerRowCheck(false);
    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
      if (item.isStar() || !addReadNames(CypherReferencedVariables.of(item.getExpression()), readNames))
        return null;
      CypherExpressionWalker.walk(item.getExpression(), returnCheck);
      returnNames.add(item.getOutputName());
    }
    if (!returnCheck.ok)
      return null;
    // An ORDER BY after a DISTINCT reads the projected columns, and what it reads besides them is part of the key
    if (statement.getOrderByClause() != null)
      for (final OrderByClause.OrderByItem item : statement.getOrderByClause().getItems()) {
        final Set<String> orderNames = new HashSet<>();
        if (!addReadNames(CypherReferencedVariables.of(item.getExpressionAST()), orderNames))
          return null;
        orderNames.removeAll(returnNames);
        readNames.addAll(orderNames);
      }
    if (readNames.isEmpty())
      return null;
    final List<String> distinctKey = sorted(readNames);

    Plan earliest = null;
    final Set<String> scope = new HashSet<>();
    for (int start = 0; start < returnIndex; start++) {
      if (start > 0 && scope.containsAll(readNames) && isChain(clausesInOrder, start, returnIndex, readNames)) {
        final List<String> memoKey = memoKey(clausesInOrder, start, returnIndex, scope, readNames);
        if (memoKey != null)
          return new Plan(start, distinctKey, memoKey);
        if (earliest == null)
          // Without a memo the earliest start answers for the fewest input rows
          earliest = new Plan(start, distinctKey, null);
      }
      if (!applyToScope(clausesInOrder.get(start), scope))
        return earliest;
    }
    return earliest;
  }

  /**
   * The names the chain {@code [from, to)} reads off its input row, when its answer is a function of them alone and
   * none of them is a returned name; null otherwise.
   */
  private static List<String> memoKey(final List<ClauseEntry> clausesInOrder, final int from, final int to,
      final Set<String> scope, final Set<String> readNames) {
    final Set<String> reads = new HashSet<>();
    // What the chain binds for itself, so every name it reads is known to come from the input row or from itself
    final Set<String> bound = new HashSet<>(scope);
    final PerRowCheck check = new PerRowCheck(true);
    for (int i = from; i < to; i++) {
      final ClauseEntry entry = clausesInOrder.get(i);
      switch (entry.getType()) {
      case MATCH -> {
        final MatchClause match = entry.getTypedClause();
        for (final PathPattern pattern : match.getPathPatterns()) {
          if (!addReadNames(CypherReferencedVariables.of(pattern), reads))
            return null;
          check.topLevelPattern = pattern;
          CypherExpressionWalker.walk(pattern, check);
          check.topLevelPattern = null;
          addPatternVariables(pattern, bound);
        }
        if (!readsPredicate(match.hasWhereClause() ? match.getWhereClause().getConditionExpression() : null, reads, check))
          return null;
      }
      case WITH -> {
        final WithClause with = entry.getTypedClause();
        for (final ReturnClause.ReturnItem item : with.getItems()) {
          if (item.isStar() || isPassThrough(item))
            continue;
          if (!addReadNames(CypherReferencedVariables.of(item.getExpression()), reads))
            return null;
          CypherExpressionWalker.walk(item.getExpression(), check);
          bound.add(item.getOutputName());
        }
        if (!readsPredicate(with.getWhereClause() != null ? with.getWhereClause().getConditionExpression() : null, reads,
            check))
          return null;
      }
      case UNWIND -> {
        final UnwindClause unwind = entry.getTypedClause();
        if (!addReadNames(CypherReferencedVariables.of(unwind.getListExpression()), reads))
          return null;
        CypherExpressionWalker.walk(unwind.getListExpression(), check);
        bound.add(unwind.getVariable());
      }
      default -> {
        return null;
      }
      }
    }
    if (!check.ok || !Collections.disjoint(reads, readNames) || !bound.containsAll(reads))
      return null;
    reads.retainAll(scope);
    return sorted(reads);
  }

  private static boolean readsPredicate(final BooleanExpression predicate, final Set<String> reads,
      final PerRowCheck check) {
    if (predicate == null)
      return true;
    if (!addReadNames(CypherReferencedVariables.of(predicate), reads))
      return false;
    CypherExpressionWalker.walk(predicate, check);
    return true;
  }

  /** Whether the clauses {@code [from, to)} can run as a semi-join chain for rows bound to {@code readNames}. */
  private static boolean isChain(final List<ClauseEntry> clausesInOrder, final int from, final int to,
      final Set<String> readNames) {
    boolean matches = false;
    for (int i = from; i < to; i++) {
      final ClauseEntry entry = clausesInOrder.get(i);
      switch (entry.getType()) {
      case MATCH -> matches = true;
      case WITH -> {
        final WithClause with = entry.getTypedClause();
        if (with.isLetBinding() || with.isDistinct() || with.hasAggregations() || with.getOrderByClause() != null
            || with.getSkip() != null || with.getLimit() != null || !forwardsUnchanged(with, readNames))
          return false;
      }
      case UNWIND -> {
        if (readNames.contains(((UnwindClause) entry.getTypedClause()).getVariable()))
          return false;
      }
      default -> {
        return false;
      }
      }
    }
    return matches;
  }

  /** Whether {@code with} passes every one of {@code names} on as it is, and binds none of them to anything else. */
  private static boolean forwardsUnchanged(final WithClause with, final Set<String> names) {
    boolean star = false;
    final Set<String> forwarded = new HashSet<>();
    for (final ReturnClause.ReturnItem item : with.getItems()) {
      if (item.isStar()) {
        star = true;
        continue;
      }
      final String name = item.getOutputName();
      if (!names.contains(name))
        continue;
      if (!isPassThrough(item))
        return false;
      forwarded.add(name);
    }
    return star || forwarded.containsAll(names);
  }

  private static boolean isPassThrough(final ReturnClause.ReturnItem item) {
    return item.getExpression() instanceof VariableExpression variable && variable.getVariableName()
        .equals(item.getOutputName());
  }

  /** Applies what a MATCH, WITH or UNWIND leaves bound to {@code scope}, or answers false for any other clause. */
  private static boolean applyToScope(final ClauseEntry entry, final Set<String> scope) {
    switch (entry.getType()) {
    case MATCH -> {
      for (final PathPattern pattern : ((MatchClause) entry.getTypedClause()).getPathPatterns())
        addPatternVariables(pattern, scope);
      return true;
    }
    case WITH -> {
      final WithClause with = entry.getTypedClause();
      boolean star = false;
      final List<String> projected = new ArrayList<>(with.getItems().size());
      for (final ReturnClause.ReturnItem item : with.getItems())
        if (item.isStar())
          star = true;
        else
          projected.add(item.getOutputName());
      if (!star)
        scope.clear();
      scope.addAll(projected);
      return true;
    }
    case UNWIND -> {
      scope.add(((UnwindClause) entry.getTypedClause()).getVariable());
      return true;
    }
    default -> {
      return false;
    }
    }
  }

  private static void addPatternVariables(final PathPattern pattern, final Set<String> target) {
    for (final NodePattern node : pattern.getNodes())
      if (node.getVariable() != null)
        target.add(node.getVariable());
    for (final RelationshipPattern relationship : pattern.getRelationships())
      if (relationship.getVariable() != null)
        target.add(relationship.getVariable());
    if (pattern.hasPathVariable())
      target.add(pattern.getPathVariable());
  }

  private static boolean addReadNames(final CypherReferencedVariables referenced, final Set<String> target) {
    if (!referenced.isComplete())
      return false;
    target.addAll(referenced.getNames());
    return true;
  }

  private static List<String> sorted(final Set<String> names) {
    final List<String> list = new ArrayList<>(names);
    Collections.sort(list);
    return list;
  }

  /**
   * Clears {@link #ok} on a function that can answer differently for the same arguments, or that is not known to be a
   * function of its arguments at all. For the memo ({@code strict}) also on a pattern inside an expression and on a
   * subquery expression, whose reads are not modelled as plain reads of the row.
   */
  private static final class PerRowCheck implements CypherExpressionWalker.Visitor {
    private final boolean     strict;
    private       PathPattern topLevelPattern;
    private       boolean     ok = true;

    private PerRowCheck(final boolean strict) {
      this.strict = strict;
    }

    @Override
    public void visit(final Expression expression) {
      if (expression instanceof FunctionCallExpression call) {
        if (isVaryingFunction(call.getFunctionName()))
          ok = false;
      } else if (strict && (expression instanceof ExistsExpression || expression instanceof CountExpression
          || expression instanceof CollectExpression || expression instanceof PatternComprehensionExpression))
        ok = false;
    }

    @Override
    public void visitPattern(final PathPattern pattern) {
      if (strict && pattern != topLevelPattern)
        ok = false;
    }
  }
}
