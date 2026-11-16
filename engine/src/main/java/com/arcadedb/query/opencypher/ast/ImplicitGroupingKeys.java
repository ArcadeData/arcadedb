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

import java.util.ArrayList;
import java.util.List;

/**
 * The variables an aggregating projection groups by without projecting them: the variable of every map projection that
 * aggregates, {@code RETURN p{.name, friends: collect(f.name)}}.
 * <p>
 * That is the idiom Neo4j answers with one row per {@code p}: the projection reads {@code p} outside its aggregate, so
 * {@code p} has to be single-valued per group, and the only way it is is to be a grouping key. Without it every input row
 * was folded into a single group and the map read {@code p} from whichever row came first (issue #12316). A map
 * <i>literal</i> or any other expression reading a variable that is not a grouping key stays ambiguous and is refused by
 * the validator, as in Neo4j.
 * <p>
 * Only the map projections reached outside every aggregate call are collected, through the composite shapes that keep
 * their operands evaluated once per group: a map literal entry, a list element, a non-aggregating function argument, an
 * arithmetic operand, a {@code CASE} part. A map projection inside an aggregate's argument is evaluated per input row and
 * groups nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ImplicitGroupingKeys {
  private ImplicitGroupingKeys() {
  }

  /**
   * The implicit grouping variables of {@code items}, in the order met, without repetition. Empty when no item has one,
   * which is the answer for every projection that is not an aggregating map projection.
   */
  public static List<String> of(final List<ReturnClause.ReturnItem> items) {
    List<String> variables = null;
    for (final ReturnClause.ReturnItem item : items) {
      final Expression expression = item.getExpression();
      if (expression != null && expression.containsAggregation())
        variables = collect(expression, variables);
    }
    return variables != null ? variables : List.of();
  }

  private static List<String> collect(final Expression expression, List<String> variables) {
    switch (expression) {
    case MapProjectionExpression projection -> {
      if (projection.containsAggregation()) {
        if (variables == null)
          variables = new ArrayList<>(2);
        if (!variables.contains(projection.getVariableName()))
          variables.add(projection.getVariableName());
      }
      for (final MapProjectionExpression.ProjectionElement element : projection.getElements())
        if (element.isKeyExpression())
          variables = collect(element.getExpression(), variables);
    }
    case FunctionCallExpression function -> {
      if (!function.isAggregation())
        for (final Expression argument : function.getArguments())
          variables = collect(argument, variables);
    }
    case MapExpression map -> {
      for (final Expression entry : map.getEntries().values())
        variables = collect(entry, variables);
    }
    case ListExpression list -> {
      for (final Expression element : list.getElements())
        variables = collect(element, variables);
    }
    case ArithmeticExpression arithmetic -> {
      variables = collect(arithmetic.getLeft(), variables);
      variables = collect(arithmetic.getRight(), variables);
    }
    case CaseExpression caseExpression -> {
      if (caseExpression.getCaseExpression() != null)
        variables = collect(caseExpression.getCaseExpression(), variables);
      for (final CaseAlternative alternative : caseExpression.getAlternatives()) {
        variables = collect(alternative.getWhenExpression(), variables);
        variables = collect(alternative.getThenExpression(), variables);
      }
      if (caseExpression.getElseExpression() != null)
        variables = collect(caseExpression.getElseExpression(), variables);
    }
    case null, default -> {
      // A leaf, or a shape that does not keep its operands once per group
    }
    }
    return variables;
  }
}
