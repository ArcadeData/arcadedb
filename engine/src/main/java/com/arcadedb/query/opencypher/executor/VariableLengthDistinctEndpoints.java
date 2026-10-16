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
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.QuantifiedPathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SimpleCypherStatement;
import com.arcadedb.query.opencypher.ast.VariableExpression;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Decides whether a variable-length hop has to report each (start, end) vertex pair only once, which lets it walk the graph
 * with a visited set instead of enumerating every path between the two (issue #10062).
 * <p>
 * The answer is yes only for a statement that is nothing but {@code MATCH (a)-[r*0..n]->(b) [WHERE ...] RETURN ...} and whose
 * RETURN cannot tell two paths between the same pair apart: it is {@code DISTINCT} over properties of the two end nodes, or it
 * groups by them and only aggregates them with a function that ignores repeated values ({@code count}, {@code collect},
 * {@code sum} and {@code avg} of {@code DISTINCT} values, {@code min} and {@code max}). The relationship and path variables must
 * be unused, because they are the only things that differ between two paths with the same ends.
 * <p>
 * "Ignores repeated values" is a statement about the SET of values the aggregate sees. For {@code collect} the list also carries an
 * order, the order the walk met the values in: breadth first here, depth first when the paths are enumerated, so the same data can
 * give the same values in another order depending on whether the planner took this shortcut (issue #10152). Cypher promises no
 * order for {@code collect} without an {@code ORDER BY}, as it promises none for the rows of a {@code MATCH}, and the reference
 * documentation says so. A statement that reads the order of the list ({@code head(collect(DISTINCT ...))},
 * {@code collect(DISTINCT ...)[0]}) is not one of the shapes above, so it keeps the order of the enumeration. The differential
 * tests compare the lists of a {@code collect} as sets of values, and everything else row by row.
 * <p>
 * The lower bound must be 0 or 1:
 * the shortest walk to a node is a simple path, so a visited set finds every node some path of at most {@code n} relationships
 * reaches, whatever the path mode (the start node needs care, see {@code BreadthFirstTraverser#traverseDistinctEndpoints}). From
 * a lower bound of 2 on, a node reached sooner than the bound might be reachable at the bound only by a walk that repeats a
 * relationship, which the TRAIL rule forbids, so those patterns keep enumerating paths.
 * <p>
 * Everything the statement does with a row afterwards (the WHERE, the target label and property tests, the projection) reads only
 * the pair, so dropping the repeated rows for a pair cannot change any result - but for the order of a collected list, above.
 */
public final class VariableLengthDistinctEndpoints {
  private VariableLengthDistinctEndpoints() {
  }

  /**
   * @param statement      the whole statement being planned
   * @param pathPattern    the path pattern holding the hop, or null when the caller has only the relationship
   * @param relationship   the variable-length relationship pattern
   * @param sourceVariable the written variable of the hop's first node, null when anonymous
   * @param targetVariable the written variable of the hop's last node, null when anonymous
   * @param pathVariable   the named path the hop belongs to, null or empty when none
   *
   * @return true if the hop may report each (start, end) pair once, whatever its path mode
   */
  public static boolean applies(final CypherStatement statement, final PathPattern pathPattern,
      final RelationshipPattern relationship, final String sourceVariable, final String targetVariable,
      final String pathVariable) {
    if (!(statement instanceof SimpleCypherStatement simple))
      return false;
    if (relationship == null || relationship instanceof QuantifiedPathPattern || !relationship.isVariableLength()
        || relationship.getEffectiveMinHops() > 1)
      return false;
    if (pathVariable != null && !pathVariable.isEmpty())
      return false;
    if (pathPattern != null && (pathPattern.hasPathVariable() || pathPattern.getRelationshipCount() != 1))
      return false;

    final List<ClauseEntry> clauses = simple.getClausesInOrder();
    if (clauses == null || clauses.size() != 2 || clauses.get(0).getType() != ClauseEntry.ClauseType.MATCH
        || clauses.get(1).getType() != ClauseEntry.ClauseType.RETURN)
      return false;
    final MatchClause match = clauses.get(0).getTypedClause();
    if (match == null || match.isOptional() || !match.hasPathPatterns() || match.getPathPatterns().size() != 1
        || match.getPathPatterns().get(0).getRelationshipCount() != 1)
      return false;

    final String relationshipVariable = relationship.getVariable();
    if (relationshipVariable != null && !relationshipVariable.isEmpty()
        && CypherVariableUsage.isEdgeVariableReferenced(statement, relationshipVariable))
      return false;

    final ReturnClause returnClause = simple.getReturnClause();
    if (returnClause == null || returnClause.isReturnAll() || returnClause.getReturnItems().isEmpty())
      return false;

    final Set<String> ends = new HashSet<>(2);
    if (sourceVariable != null && !sourceVariable.isEmpty())
      ends.add(sourceVariable);
    if (targetVariable != null && !targetVariable.isEmpty())
      ends.add(targetVariable);
    if (ends.isEmpty())
      return false;

    final Set<String> outputNames = new HashSet<>();
    boolean aggregates = false;
    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
      if (item.isStar())
        return false;
      final Expression expression = item.getExpression();
      if (isEndValue(expression, ends)) {
        outputNames.add(item.getOutputName());
        continue;
      }
      if (isRepeatInsensitiveAggregate(expression, ends)) {
        aggregates = true;
        outputNames.add(item.getOutputName());
        continue;
      }
      return false;
    }
    // Either DISTINCT over end values only, or end values (the grouping keys) with aggregates that ignore repeated end values
    if (aggregates ? returnClause.isDistinct() : !returnClause.isDistinct())
      return false;

    final OrderByClause orderBy = simple.getOrderByClause();
    if (orderBy != null && !orderBy.isEmpty())
      for (final OrderByClause.OrderByItem item : orderBy.getItems()) {
        final Expression expression = item.getExpressionAST();
        if (expression == null)
          return false;
        if (isEndValue(expression, ends))
          continue;
        if (expression instanceof VariableExpression variable && outputNames.contains(variable.getVariableName()))
          continue;
        return false;
      }
    return true;
  }

  /** A start or end node, or one property of it: a value that two paths with the same ends share. */
  private static boolean isEndValue(final Expression expression, final Set<String> ends) {
    if (expression instanceof VariableExpression variable)
      return ends.contains(variable.getVariableName());
    if (expression instanceof PropertyAccessExpression property)
      return ends.contains(property.getVariableName());
    return false;
  }

  /**
   * An aggregate of one start or end value whose set of values is the same whether it sees a value once or many times: the
   * {@code DISTINCT} forms of count, collect, sum and avg, and min and max, which ignore repeats with or without {@code DISTINCT}.
   * The list of a {@code collect} is the one result that also carries an order, which is the walk's and not a promise (see the
   * class comment).
   */
  private static boolean isRepeatInsensitiveAggregate(final Expression expression, final Set<String> ends) {
    if (!(expression instanceof FunctionCallExpression call) || call.getArguments().size() != 1
        || !isEndValue(call.getArguments().get(0), ends))
      return false;
    return switch (call.getFunctionName().toLowerCase(Locale.ROOT)) {
      case "count", "collect", "sum", "avg" -> call.isDistinct();
      case "min", "max" -> true;
      default -> false;
    };
  }
}
