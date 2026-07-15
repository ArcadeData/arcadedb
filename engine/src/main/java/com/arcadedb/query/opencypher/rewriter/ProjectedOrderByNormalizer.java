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

import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites the ORDER BY items of a DISTINCT projection that repeat one of the projected expressions
 * so they reference that projection's output column instead (issue #5283).
 * <p>
 * A DISTINCT projection collapses its input rows, so the variables that fed it no longer have a
 * single well-defined value per surviving row. ORDER BY is therefore restricted to the projected
 * columns, and {@code RETURN DISTINCT n.active AS active ORDER BY n.name} is correctly an error. But
 * when the ORDER BY expression <em>is</em> one of the projected expressions, it is constant within
 * every dedup group by construction, so it stays well defined:
 * <pre>
 *   MATCH (n:BugA) RETURN DISTINCT n.active AS active ORDER BY n.active
 * </pre>
 * must sort exactly as {@code ORDER BY active} does. Neo4j, Memgraph and FalkorDB all accept this
 * form; Neo4j resolves it the same way, by rewriting such an ORDER BY expression to the projection's
 * alias before the scope check runs.
 * <p>
 * Rewriting (rather than merely widening the scope check to let the original variable through) is
 * what makes the sort well defined rather than accidentally correct: the sort then reads the
 * already-projected column instead of re-evaluating the expression against whichever member of the
 * dedup group happened to survive. It is also cheaper, since it replaces a per-comparison property
 * lookup on the record with a map lookup on the projected row.
 * <p>
 * Identity is decided on the canonical parse-tree text of both sides, so it is insensitive to
 * whitespace ({@code n.age+1} matches a projected {@code n.age + 1}) but never conflates two
 * different expressions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ProjectedOrderByNormalizer {

  private ProjectedOrderByNormalizer() {
  }

  /**
   * Returns an ORDER BY clause whose items that repeat a projected expression have been replaced by
   * references to the corresponding output column.
   *
   * @param orderBy     the ORDER BY clause to normalize, may be null
   * @param items       the projected items of the RETURN/WITH clause
   * @param distinct    whether the projection is DISTINCT; no rewrite is applied otherwise, since a
   *                    non-DISTINCT projection keeps the input scope and needs no normalization
   *
   * @return the original clause when nothing matched, a rewritten copy otherwise
   */
  public static OrderByClause normalize(final OrderByClause orderBy, final List<ReturnClause.ReturnItem> items,
      final boolean distinct) {
    if (!distinct || orderBy == null || orderBy.isEmpty() || items == null || items.isEmpty())
      return orderBy;

    List<OrderByClause.OrderByItem> rewritten = null;

    final List<OrderByClause.OrderByItem> orderByItems = orderBy.getItems();
    for (int i = 0; i < orderByItems.size(); i++) {
      final OrderByClause.OrderByItem orderByItem = orderByItems.get(i);
      final String outputName = findProjectedOutputName(items, orderByItem.getExpression());
      if (outputName == null)
        continue;

      if (rewritten == null)
        rewritten = new ArrayList<>(orderByItems);

      rewritten.set(i, new OrderByClause.OrderByItem(outputName, orderByItem.isAscending(), new VariableExpression(outputName)));
    }

    return rewritten == null ? orderBy : new OrderByClause(rewritten);
  }

  /**
   * Returns the output column name of the projected item whose expression is the same as the given
   * ORDER BY expression text, or null when no item projects it.
   */
  private static String findProjectedOutputName(final List<ReturnClause.ReturnItem> items, final String orderByExpressionText) {
    if (orderByExpressionText == null)
      return null;

    for (final ReturnClause.ReturnItem item : items) {
      // An item already referenced by its output name needs no rewrite: ORDER BY resolves it directly.
      if (orderByExpressionText.equals(item.getOutputName()))
        return null;

      if (orderByExpressionText.equals(item.getExpressionText()))
        return item.getOutputName();
    }
    return null;
  }
}
