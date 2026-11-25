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
package com.arcadedb.query.opencypher.optimizer;

import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.executor.DistinctSemiJoin;
import com.arcadedb.query.opencypher.executor.operators.ExpandAll;
import com.arcadedb.query.opencypher.executor.operators.ExpandToDistinctEnds;
import com.arcadedb.query.opencypher.executor.operators.FilterOperator;
import com.arcadedb.query.opencypher.executor.operators.PhysicalOperator;
import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Replaces the chain of anonymous hops at the top of a plan with {@link ExpandToDistinctEnds} when the statement reads only
 * the distinct far ends of the chain: {@code MATCH (p)-[:K]-()-[:K]-(x) WHERE ... RETURN count(DISTINCT x)}, the shape of a
 * friends-of-friends count. The operator produces each far end once per input row instead of once per path, which on a
 * well-connected graph is the difference between hundreds of thousands of rows and a few thousand.
 * <p>
 * Being wrong is not symmetric: a refused statement keeps the plain walk, a wrongly accepted one returns different counts.
 * So the statement must provably not tell how many times a row comes:
 * <ul>
 *   <li>read-only, one MATCH that is not OPTIONAL, then the RETURN, with no path variable, no {@code *}, no SKIP or LIMIT;</li>
 *   <li>the RETURN either is {@code RETURN DISTINCT} without aggregations, or aggregates only with {@code count}, {@code sum},
 *   {@code avg}, {@code min} or {@code max} over {@code DISTINCT} arguments ({@code min} and {@code max} also without), every
 *   other item being a grouping key;</li>
 *   <li>every function anywhere in the statement's WHERE, RETURN and ORDER BY is one answering the same for the same
 *   arguments, and none of them holds a pattern or a subquery: those could tell two copies of a row apart.</li>
 * </ul>
 * The plan must be one chain of at least two {@link ExpandAll} hops, under nothing but filters, where every relationship is
 * anonymous, every intermediate node is anonymous, the far end is named, and no filter reads a name the engine gave to an
 * anonymous node. The anonymous names are then read by nobody but the chain itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class DistinctEndsRewrite {
  private static final Set<String> DISTINCT_AGGREGATIONS = Set.of("count", "sum", "avg", "min", "max");
  private static final Set<String> PLAIN_AGGREGATIONS    = Set.of("min", "max");

  private DistinctEndsRewrite() {
  }

  /** The plan with its top chain replaced, or {@code root} as it is when the statement or the plan does not qualify. */
  static PhysicalOperator apply(final CypherStatement statement, final PhysicalOperator root) {
    if (root == null || !readsOnlyDistinctRows(statement))
      return root;

    // Filters at the top, then the chain
    FilterOperator lowestFilter = null;
    PhysicalOperator operator = root;
    while (operator.getClass() == FilterOperator.class) {
      final FilterOperator filter = (FilterOperator) operator;
      if (filter.getPredicate() == null || readsAnonymousName(filter.getPredicate()))
        return root;
      lowestFilter = filter;
      operator = filter.getChild();
    }

    final List<ExpandAll> chain = new ArrayList<>();
    while (operator != null && operator.getClass() == ExpandAll.class) {
      chain.addFirst((ExpandAll) operator);
      operator = operator.getChild();
    }
    // A run of anonymous hops at the top: the lowest hop may start from anything the input row binds
    while (chain.size() > 1 && !qualifies(chain))
      chain.removeFirst();
    if (chain.size() < 2 || !qualifies(chain))
      return root;

    final ExpandToDistinctEnds distinctEnds = new ExpandToDistinctEnds(chain);
    if (lowestFilter == null)
      return distinctEnds;
    lowestFilter.setChild(distinctEnds);
    return root;
  }

  /** Whether the hops, from the one reading the input row up, form one chain of anonymous hops ending on a named node. */
  private static boolean qualifies(final List<ExpandAll> chain) {
    for (int i = 0; i < chain.size(); i++) {
      final ExpandAll hop = chain.get(i);
      if (hop.getEdgeVariable() != null)
        return false;
      final boolean last = i == chain.size() - 1;
      if (last ? isAnonymous(hop.getTargetVariable()) : !isAnonymous(hop.getTargetVariable()))
        return false;
      if (i > 0 && !hop.getSourceVariable().equals(chain.get(i - 1).getTargetVariable()))
        return false;
      if (hop.getEdgeTrackingVar() != null && !isAnonymous(hop.getEdgeTrackingVar()))
        return false;
    }
    return true;
  }

  /**
   * Whether nothing after the MATCH can tell how many times a row comes, and nothing in it can answer differently for two
   * copies of one row.
   */
  static boolean readsOnlyDistinctRows(final CypherStatement statement) {
    if (statement == null || !statement.isReadOnly() || statement.getSkip() != null || statement.getLimit() != null)
      return false;

    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses == null || clauses.size() != 2 || clauses.get(0).getType() != ClauseEntry.ClauseType.MATCH
        || clauses.get(1).getType() != ClauseEntry.ClauseType.RETURN)
      return false;
    final MatchClause match = clauses.get(0).getTypedClause();
    if (match.isOptional())
      return false;
    for (final PathPattern pattern : match.getPathPatterns())
      if (pattern.hasPathVariable())
        return false;

    final ReturnClause returnClause = statement.getReturnClause();
    if (returnClause == null || returnClause.hasStar())
      return false;
    final boolean distinctRows = returnClause.isDistinct();
    if (!distinctRows && !returnClause.hasAggregations())
      return false;

    final Check check = new Check();
    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems())
      CypherExpressionWalker.walk(item.getExpression(), check);
    if (statement.getWhereClause() != null && statement.getWhereClause().getConditionExpression() != null)
      CypherExpressionWalker.walk(statement.getWhereClause().getConditionExpression(), check);
    if (match.getWhereClause() != null && match.getWhereClause().getConditionExpression() != null)
      CypherExpressionWalker.walk(match.getWhereClause().getConditionExpression(), check);
    final OrderByClause orderBy = statement.getOrderByClause();
    if (orderBy != null)
      for (final OrderByClause.OrderByItem item : orderBy.getItems())
        if (item.getExpressionAST() != null)
          CypherExpressionWalker.walk(item.getExpressionAST(), check);
        else
          return false;
    return check.ok && (distinctRows ? !check.aggregates : check.aggregates);
  }

  /** Whether the filter reads a name the engine gave to an anonymous node: the inline property map of one, for example. */
  private static boolean readsAnonymousName(final BooleanExpression predicate) {
    for (final String variable : WhereClause.collectVariables(predicate))
      if (isAnonymous(variable))
        return true;
    // A filter the planner synthesized names its variable in its text only (a label check, for example)
    return predicate.getText() == null || predicate.getText().contains("  ");
  }

  /** Whether {@code name} is one the engine gave: no identifier of a statement starts with two spaces. */
  private static boolean isAnonymous(final String name) {
    return name != null && name.startsWith("  ");
  }

  /** Every function deterministic, every aggregation multiplicity-insensitive, no pattern and no subquery. */
  private static final class Check implements CypherExpressionWalker.Visitor {
    private boolean ok = true;
    private boolean aggregates;

    @Override
    public void visit(final Expression expression) {
      if (!(expression instanceof FunctionCallExpression call))
        return;
      final String name = call.getFunctionName().toLowerCase(Locale.ROOT);
      if (call.isAggregation()) {
        aggregates = true;
        if (!(call.isDistinct() ? DISTINCT_AGGREGATIONS.contains(name) : PLAIN_AGGREGATIONS.contains(name)))
          ok = false;
      } else if (DistinctSemiJoin.isVaryingFunction(name))
        ok = false;
    }

    @Override
    public void visitPattern(final PathPattern pattern) {
      ok = false;
    }

    @Override
    public CypherExpressionWalker.Visitor forNestedStatement(final CypherStatement statement) {
      ok = false;
      return null;
    }
  }
}
