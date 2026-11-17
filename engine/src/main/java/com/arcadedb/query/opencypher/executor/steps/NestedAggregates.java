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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.query.opencypher.ast.AllReduceExpression;
import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.CaseAlternative;
import com.arcadedb.query.opencypher.ast.CaseExpression;
import com.arcadedb.query.opencypher.ast.ChainedPropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpressionWrapper;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.ListComprehensionExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.ListIndexExpression;
import com.arcadedb.query.opencypher.ast.ListPredicateExpression;
import com.arcadedb.query.opencypher.ast.ListSliceExpression;
import com.arcadedb.query.opencypher.ast.MapExpression;
import com.arcadedb.query.opencypher.ast.MapProjectionExpression;
import com.arcadedb.query.opencypher.ast.PatternComprehensionExpression;
import com.arcadedb.query.opencypher.ast.ReduceExpression;
import com.arcadedb.query.opencypher.ast.TernaryLogicalExpression;
import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;

import java.util.function.Consumer;

/**
 * The one walk both aggregation steps ({@link AggregationStep}, {@link GroupByAggregationStep}) use to find the aggregate
 * calls nested inside a projection item. It must descend through every operand {@link Expression#containsAggregation()}
 * descends through: an item that answers {@code true} there but in which this walk finds no aggregate becomes a grouping
 * key evaluated once per input row, or loses its column (issue #11770). Each step used to carry its own copy of the walk,
 * and both missed the same node kinds.
 * <p>
 * Not {@link CypherExpressionWalker}: that walk cannot stop at an aggregate (whose arguments belong to it, not to the
 * item), and it descends into the bodies of {@code EXISTS}/{@code COUNT}/{@code COLLECT} subqueries, whose aggregates
 * aggregate the subquery's rows, not this step's.
 */
final class NestedAggregates {
  private NestedAggregates() {
  }

  /**
   * Hands {@code action} every aggregate call in {@code expr}, outermost first, without descending into an aggregate's own
   * arguments.
   */
  static void forEach(final Expression expr, final Consumer<FunctionCallExpression> action) {
    switch (expr) {
    case null -> {
    }
    case FunctionCallExpression function -> {
      if (function.isAggregation())
        action.accept(function);
      else
        for (final Expression argument : function.getArguments())
          forEach(argument, action);
    }
    case ArithmeticExpression arithmetic -> {
      forEach(arithmetic.getLeft(), action);
      forEach(arithmetic.getRight(), action);
    }
    case ListComprehensionExpression comprehension -> {
      forEach(comprehension.getListExpression(), action);
      forEach(comprehension.getWhereExpression(), action);
      forEach(comprehension.getMapExpression(), action);
    }
    case ListPredicateExpression predicate -> {
      forEach(predicate.getListExpression(), action);
      forEach(predicate.getWhereExpression(), action);
    }
    case PatternComprehensionExpression pattern -> {
      forEach(pattern.getWhereExpression(), action);
      forEach(pattern.getMapExpression(), action);
    }
    case ListExpression list -> {
      for (final Expression element : list.getElements())
        forEach(element, action);
    }
    case ComparisonExpressionWrapper comparison -> {
      forEach(comparison.getComparison().getLeft(), action);
      forEach(comparison.getComparison().getRight(), action);
    }
    case CaseExpression caseExpression -> {
      forEach(caseExpression.getCaseExpression(), action);
      for (final CaseAlternative alternative : caseExpression.getAlternatives()) {
        forEach(alternative.getWhenExpression(), action);
        forEach(alternative.getThenExpression(), action);
      }
      forEach(caseExpression.getElseExpression(), action);
    }
    case MapExpression map -> {
      for (final Expression value : map.getEntries().values())
        forEach(value, action);
    }
    case MapProjectionExpression projection -> {
      for (final MapProjectionExpression.ProjectionElement element : projection.getElements())
        if (element.isKeyExpression())
          forEach(element.getExpression(), action);
    }
    case ListSliceExpression slice -> {
      forEach(slice.getListExpression(), action);
      forEach(slice.getFromExpression(), action);
      forEach(slice.getToExpression(), action);
    }
    case ListIndexExpression index -> {
      forEach(index.getListExpression(), action);
      forEach(index.getIndexExpression(), action);
    }
    case ChainedPropertyAccessExpression chained -> forEach(chained.getBaseExpression(), action);
    case ReduceExpression reduce -> {
      forEach(reduce.getInitialValue(), action);
      forEach(reduce.getListExpression(), action);
      forEach(reduce.getReduceExpression(), action);
    }
    case AllReduceExpression allReduce -> {
      forEach(allReduce.getInitialValue(), action);
      forEach(allReduce.getListExpression(), action);
      forEach(allReduce.getReduceExpression(), action);
      forEach(allReduce.getPredicateExpression(), action);
    }
    case BooleanWrapperExpression wrapper ->
        BooleanWrapperExpression.forEachOperand(wrapper.getBooleanExpression(), operand -> forEach(operand, action));
    case TernaryLogicalExpression logical -> {
      // AND, OR, XOR and NOT in expression position (issue #11597)
      forEach(logical.getLeft(), action);
      forEach(logical.getRight(), action);
    }
    default -> {
    }
    }
  }
}
