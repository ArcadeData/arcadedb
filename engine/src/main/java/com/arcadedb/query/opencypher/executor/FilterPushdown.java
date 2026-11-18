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

import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.executor.steps.MatchNodeStep;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Splits a WHERE clause into the conjuncts the scan of a node can apply while it reads and the residue a filter step
 * still has to check, leaving out the id equalities a lookup by id has already answered.
 * <p>
 * Split out of {@link CypherExecutionPlan}, whose MATCH step builders hand it each clause's WHERE.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class FilterPushdown {
  private final Map<String, Object> parameters;

  FilterPushdown(final Map<String, Object> parameters) {
    this.parameters = parameters;
  }

  /**
   * Extracts WHERE predicates that can be pushed down into a MatchNodeStep.
   * Only predicates referencing the current variable (and already-bound variables) are eligible.
   * The pushed-down predicates are evaluated inline during scanning, reducing pipeline overhead.
   *
   * @param whereClause    the WHERE clause to analyze
   * @param currentVar     the variable being scanned by the MatchNodeStep
   * @param boundVariables variables bound in previous MATCH clauses
   * @param matchVariables variables bound earlier in the current MATCH clause
   * @param pushedConjuncts conjuncts an earlier node step of the clause decides already: every row this step sees passed them
   * @return the extractable predicate, or null if none qualifies
   */
  BooleanExpression extractPushdownFilter(final WhereClause whereClause, final String currentVar,
      final Set<String> boundVariables, final Set<String> matchVariables, final Set<BooleanExpression> pushedConjuncts) {
    if (whereClause == null || whereClause.getConditionExpression() == null)
      return null;

    // Available variables = already bound + already matched in this clause + the current variable
    final Set<String> available = new HashSet<>();
    available.addAll(boundVariables);
    available.addAll(matchVariables);
    available.add(currentVar);

    final BooleanExpression extracted = WhereClause.extractForVariables(
        WhereClause.withoutConjuncts(whereClause.getConditionExpression(), pushedConjuncts), available);
    // Strip {@code id(var) = X} / {@code elementId(var) = X} predicates whose RHS is statically
    // resolvable (literal or parameter): the static idFilter optimisation already enforces them via
    // direct RID lookup, so re-evaluating them in the row-by-row pushdown filter is wasted work and
    // would also drop the row after id() was made Neo4j-compatible (returns Long instead of an RID
    // string, issue #4183) because the cross-type comparison Long vs. String fails. Predicates with
    // dynamic RHS (e.g. {@code id(a) = b.id}) are left alone because extractIdFilter does not consume
    // them.
    return stripStaticIdEqualities(extracted);
  }

  /**
   * Records the conjuncts the node step decides on every row it passes on, and returns the step. Only what the step actually kept
   * is recorded: an {@code id(var) = X} it enforces through its id lookup instead is left to the clause's filter, as before.
   */
  static MatchNodeStep takePushdown(final MatchNodeStep step, final Set<BooleanExpression> pushedConjuncts) {
    pushedConjuncts.addAll(WhereClause.conjuncts(step.getWhereFilter()));
    return step;
  }

  /**
   * The part of a MATCH clause's own WHERE that no node step of the clause decided, or {@code null} when nothing is left to decide
   * (issue #11102). A node step evaluates its pushed predicate on the very row it passes on, and the steps of a clause form one
   * chain ending in the clause's filter, so every row that reaches the filter passed every pushed conjunct already: evaluating
   * them again cost one more call per surviving row, twice the price of an expensive predicate.
   */
  static WhereClause residualWhere(final MatchClause matchClause, final Set<BooleanExpression> pushedConjuncts) {
    if (!matchClause.hasWhereClause())
      return null;
    final WhereClause where = matchClause.getWhereClause();
    final BooleanExpression condition = where.getConditionExpression();
    if (condition == null || pushedConjuncts.isEmpty())
      return where;
    final BooleanExpression residual = WhereClause.withoutConjuncts(condition, pushedConjuncts);
    if (residual == null)
      return null;
    return residual == condition ? where : new WhereClause(residual);
  }

  /**
   * Removes every top-level {@code id(var) = X} / {@code elementId(var) = X} equality (in either argument order) whose RHS resolves to a constant at plan time
   * (literal or parameter). Walks AND chains; leaves OR / NOT subtrees untouched because the static idFilter optimisation only consumes AND-chained equalities.
   */
  private BooleanExpression stripStaticIdEqualities(final BooleanExpression expr) {
    if (expr == null)
      return null;

    if (expr instanceof ComparisonExpression comp
        && comp.getOperator() == ComparisonExpression.Operator.EQUALS) {
      if (isStaticIdEquality(comp.getLeft(), comp.getRight())
          || isStaticIdEquality(comp.getRight(), comp.getLeft()))
        return null;
    }

    if (expr instanceof LogicalExpression logical && logical.getOperator() == LogicalExpression.Operator.AND) {
      final BooleanExpression left = stripStaticIdEqualities(logical.getLeft());
      final BooleanExpression right = stripStaticIdEqualities(logical.getRight());
      if (left != null && right != null)
        return new LogicalExpression(LogicalExpression.Operator.AND, left, right);
      if (left != null)
        return left;
      return right;
    }

    return expr;
  }

  private boolean isStaticIdEquality(final Expression idSide, final Expression valueSide) {
    if (!(idSide instanceof FunctionCallExpression func))
      return false;
    final String name = func.getFunctionName();
    if (!("id".equalsIgnoreCase(name) || "elementid".equalsIgnoreCase(name)) || func.getArguments().size() != 1)
      return false;
    if (!(func.getArguments().get(0) instanceof VariableExpression))
      return false;
    if (valueSide instanceof LiteralExpression)
      return true;
    if (valueSide instanceof ParameterExpression paramExpr)
      return parameters != null && parameters.containsKey(paramExpr.getParameterName());
    return false;
  }
}
