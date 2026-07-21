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
package com.arcadedb.query.opencypher.rewriter;

import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.LabelCheckExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WhereClause;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Clause-level rewrite that moves a single-label predicate expressed in a MATCH's WHERE clause into
 * the corresponding node pattern, so that {@code MATCH (n) WHERE n:Person} is planned exactly like
 * {@code MATCH (n:Person)}.
 * <p>
 * Without this rewrite the unlabeled node pattern forces the planner onto the traditional execution
 * path, where the node is resolved by chaining an iterator over <i>every</i> vertex type in the
 * database and discarding the non-matching rows one by one: runtime then grows with the size of
 * types that have nothing to do with the query (issue #5363). Neo4j solves the {@code HasLabels}
 * predicate with a label scan in both spellings.
 * <p>
 * The rewrite is only applied when it is provably semantics-preserving:
 * <ul>
 *   <li>the predicate is a top-level AND conjunct (never under OR, NOT or XOR);</li>
 *   <li>it carries a single label with AND semantics (a disjunction {@code n:A|B} is left alone);</li>
 *   <li>its subject is a plain variable bound by a node pattern of this same MATCH clause;</li>
 *   <li>that node pattern carries no label and no dynamic label yet.</li>
 * </ul>
 * A hoisted conjunct is removed from the WHERE clause: matching a label in the pattern and testing
 * it with {@code n:Label} both resolve to {@code type.instanceOf(label)}, so the filter would be
 * redundant.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class LabelPredicateHoister {

  private LabelPredicateHoister() {
  }

  /**
   * Rewrites the given MATCH clause by hoisting eligible label predicates from its WHERE clause into
   * the node patterns.
   *
   * @param match the MATCH clause to rewrite
   *
   * @return the rewritten clause, or the very same instance when nothing could be hoisted
   */
  public static MatchClause hoist(final MatchClause match) {
    if (match == null || !match.hasWhereClause() || !match.hasPathPatterns())
      return match;

    final BooleanExpression condition = match.getWhereClause().getConditionExpression();
    if (condition == null)
      return match;

    // Candidate variables: bound by a node pattern of this MATCH that has no label yet
    final Set<String> candidates = new HashSet<>();
    for (final PathPattern path : match.getPathPatterns())
      for (final NodePattern node : path.getNodes())
        if (node.getVariable() != null && !node.hasLabels() && !node.hasDynamicLabels())
          candidates.add(node.getVariable());

    if (candidates.isEmpty())
      return match;

    final Map<String, String> hoisted = new HashMap<>();
    final BooleanExpression residual = collect(condition, candidates, hoisted);
    if (hoisted.isEmpty())
      return match;

    final List<PathPattern> newPaths = new ArrayList<>(match.getPathPatterns().size());
    for (final PathPattern path : match.getPathPatterns())
      newPaths.add(applyLabels(path, hoisted));

    final WhereClause newWhere = residual != null ? new WhereClause(residual) : null;
    return new MatchClause(newPaths, match.isOptional(), newWhere);
  }

  /**
   * Walks the top-level AND spine, recording the labels that can be hoisted and returning the
   * residual predicate (null when every conjunct was hoisted).
   */
  private static BooleanExpression collect(final BooleanExpression expr, final Set<String> candidates,
      final Map<String, String> hoisted) {
    if (expr instanceof LogicalExpression logical && logical.getOperator() == LogicalExpression.Operator.AND) {
      final BooleanExpression left = collect(logical.getLeft(), candidates, hoisted);
      final BooleanExpression right = collect(logical.getRight(), candidates, hoisted);
      if (left != null && right != null)
        return left == logical.getLeft() && right == logical.getRight() ?
            logical :
            new LogicalExpression(LogicalExpression.Operator.AND, left, right);
      return left != null ? left : right;
    }

    final String variable = hoistableVariable(expr, candidates, hoisted);
    return variable == null ? expr : null;
  }

  /**
   * Returns the variable name when the conjunct is a hoistable single-label check, registering the
   * label in {@code hoisted}. Returns null when the conjunct must stay in the WHERE clause.
   */
  private static String hoistableVariable(final BooleanExpression expr, final Set<String> candidates,
      final Map<String, String> hoisted) {
    BooleanExpression candidate = expr;
    while (candidate instanceof BooleanWrapperExpression wrapper)
      candidate = wrapper.getBooleanExpression();

    if (!(candidate instanceof LabelCheckExpression labelCheck))
      return null;
    if (labelCheck.getOperator() != LabelCheckExpression.LabelOperator.AND || labelCheck.getLabels().size() != 1)
      return null;

    final Expression subject = labelCheck.getVariableExpression();
    if (!(subject instanceof VariableExpression variableExpression))
      return null;

    final String variable = variableExpression.getVariableName();
    if (!candidates.contains(variable) || hoisted.containsKey(variable))
      // Only one label per pattern: a second conjunct on the same variable stays a runtime filter,
      // because a multi-label pattern carries composite-type semantics
      return null;

    hoisted.put(variable, labelCheck.getLabels().get(0));
    return variable;
  }

  private static PathPattern applyLabels(final PathPattern path, final Map<String, String> hoisted) {
    List<NodePattern> newNodes = null;
    final List<NodePattern> nodes = path.getNodes();
    for (int i = 0; i < nodes.size(); i++) {
      final NodePattern node = nodes.get(i);
      final String label = node.getVariable() != null && !node.hasLabels() ? hoisted.get(node.getVariable()) : null;
      if (label == null)
        continue;
      if (newNodes == null)
        newNodes = new ArrayList<>(nodes);
      newNodes.set(i, node.withLabels(List.of(label)));
    }

    if (newNodes == null)
      return path;

    return new PathPattern(newNodes, path.getRelationships(), path.getPathVariable(), path.getPathMode());
  }
}
