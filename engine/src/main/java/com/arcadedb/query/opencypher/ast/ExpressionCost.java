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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;

/**
 * Tells whether evaluating an expression can run a graph traversal of its own - one per outer row - rather than reading
 * values already bound on the row.
 * <p>
 * {@code COUNT { }}, {@code EXISTS { }}, {@code COLLECT { }}, a pattern predicate, a pattern comprehension and
 * {@code shortestPath()} all re-enter the executor for every row, and the work grows with the neighbourhood of that
 * row: one node with 400,000 relationships turns a {@code WHERE} into a 400,000-record scan. A property comparison costs
 * a lookup. The two are not interchangeable operands of {@code AND}/{@code OR}, so the logical operators ask this class
 * which one to evaluate first (issues #9579, #9580). The answer is a coarse two-way split, deliberately: a finer ranking
 * would have to guess at selectivity, and the only claim made is the one that always holds - never run a traversal to
 * find out what an inexpensive operand would already have told you.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ExpressionCost {
  private ExpressionCost() {
  }

  static boolean isCostly(final Expression expression) {
    if (expression == null)
      return false;
    if (expression instanceof TernaryLogicalExpression logical)
      return logical.isCostly();
    final Probe probe = new Probe();
    CypherExpressionWalker.walk(expression, probe);
    return probe.costly;
  }

  static boolean isCostly(final BooleanExpression predicate) {
    if (predicate == null)
      return false;
    // A logical node already knows: its flag was computed from its operands when it was built, and asking again would
    // re-walk the whole chain once per link.
    if (predicate instanceof LogicalExpression logical)
      return logical.isCostly();
    final Probe probe = new Probe();
    CypherExpressionWalker.walk(predicate, probe);
    return probe.costly;
  }

  private static final class Probe implements CypherExpressionWalker.Visitor {
    private boolean costly;

    @Override
    public void visit(final Expression expression) {
      if (expression instanceof CountExpression || expression instanceof ExistsExpression
          || expression instanceof CollectExpression || expression instanceof PatternComprehensionExpression
          || expression instanceof ShortestPathExpression)
        costly = true;
    }

    @Override
    public void visitPredicate(final BooleanExpression predicate) {
      if (predicate instanceof PatternPredicateExpression)
        costly = true;
    }

    @Override
    public CypherExpressionWalker.Visitor forNestedStatement(final CypherStatement statement) {
      // The body is already a subquery, which is what made the expression costly; there is nothing more to learn in it.
      return null;
    }
  }
}
