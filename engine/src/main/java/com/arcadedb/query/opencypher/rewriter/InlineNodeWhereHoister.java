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
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.WhereClause;

import java.util.ArrayList;
import java.util.List;

/**
 * Clause-level rewrite that moves an inline {@code WHERE} predicate declared inside a node pattern
 * into the enclosing MATCH clause's WHERE clause, so that {@code MATCH (n:Person WHERE n.age > 18)}
 * is planned exactly like {@code MATCH (n:Person) WHERE n.age > 18}. The two spellings are
 * equivalent by the openCypher specification, and this is also how Neo4j evaluates them.
 * <p>
 * Before issue #5464 the inline node predicate was parsed by the grammar but never reached any
 * execution step, so it silently matched everything: {@code MATCH (n:A WHERE n.v = 99)} returned
 * every {@code :A} node. Normalizing it into the clause WHERE fixes the semantics and, as a bonus,
 * lets the optimizer apply the usual filter pushdown and index selection to the predicate.
 * <p>
 * The predicate keeps its original position in the conjunction spine: inline predicates are ANDed in
 * pattern order and the clause-level WHERE, when present, is ANDed last. AND is commutative for
 * Cypher's three-valued logic, so evaluation order does not change the result.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class InlineNodeWhereHoister {

  private InlineNodeWhereHoister() {
  }

  /**
   * Rewrites the given MATCH clause by hoisting every node inline {@code WHERE} predicate into the
   * clause's WHERE clause.
   *
   * @param match the MATCH clause to rewrite
   *
   * @return the rewritten clause, or the very same instance when no inline predicate was found
   */
  public static MatchClause hoist(final MatchClause match) {
    if (match == null || !match.hasPathPatterns())
      return match;

    BooleanExpression hoisted = null;
    List<PathPattern> newPaths = null;

    final List<PathPattern> paths = match.getPathPatterns();
    for (int p = 0; p < paths.size(); p++) {
      final PathPattern path = paths.get(p);
      List<NodePattern> newNodes = null;
      final List<NodePattern> nodes = path.getNodes();
      for (int i = 0; i < nodes.size(); i++) {
        final NodePattern node = nodes.get(i);
        if (!node.hasWhereExpression())
          continue;
        hoisted = and(hoisted, node.getWhereExpression());
        if (newNodes == null)
          newNodes = new ArrayList<>(nodes);
        newNodes.set(i, node.withoutWhereExpression());
      }
      if (newNodes == null)
        continue;
      if (newPaths == null)
        newPaths = new ArrayList<>(paths);
      newPaths.set(p, new PathPattern(newNodes, path.getRelationships(), path.getPathVariable(), path.getPathMode()));
    }

    if (hoisted == null)
      return match;

    final BooleanExpression existing = match.hasWhereClause() ? match.getWhereClause().getConditionExpression() : null;
    final BooleanExpression condition = and(hoisted, existing);
    return new MatchClause(newPaths, match.isOptional(), new WhereClause(condition));
  }

  private static BooleanExpression and(final BooleanExpression left, final BooleanExpression right) {
    if (left == null)
      return right;
    if (right == null)
      return left;
    return new LogicalExpression(LogicalExpression.Operator.AND, left, right);
  }
}
