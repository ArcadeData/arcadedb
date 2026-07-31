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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.query.opencypher.ast.AllReduceExpression;
import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.BooleanCoercionExpression;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.CaseAlternative;
import com.arcadedb.query.opencypher.ast.CaseExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpressionWrapper;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.IsNullExpression;
import com.arcadedb.query.opencypher.ast.IsTypedExpression;
import com.arcadedb.query.opencypher.ast.LabelCheckExpression;
import com.arcadedb.query.opencypher.ast.ListComprehensionExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.ListIndexExpression;
import com.arcadedb.query.opencypher.ast.ListPredicateExpression;
import com.arcadedb.query.opencypher.ast.ListSliceExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.MapExpression;
import com.arcadedb.query.opencypher.ast.MapProjectionExpression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PatternComprehensionExpression;
import com.arcadedb.query.opencypher.ast.PatternPredicateExpression;
import com.arcadedb.query.opencypher.ast.ReduceExpression;
import com.arcadedb.query.opencypher.ast.RegexExpression;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ShortestPathExpression;
import com.arcadedb.query.opencypher.ast.StringMatchExpression;
import com.arcadedb.query.opencypher.ast.TernaryLogicalExpression;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Visits every expression nested inside an expression, a predicate or a graph pattern.
 * <p>
 * Written for {@link CypherSemanticValidator}, whose parse-time checks each used to carry their own partial recursion:
 * one that only descended into a function's arguments, another that only knew about four expression shapes. A single
 * traversal means a check written once applies wherever an expression can appear, which is what let the argument
 * validation of #5484 - previously reaching only {@code RETURN} and {@code WITH} items - extend to {@code WHERE},
 * {@code UNWIND}, {@code SET}, {@code CREATE} and {@code MERGE} without each clause growing its own walker
 * (issue #5602).
 * <p>
 * The visitor is called on every expression node including the root, parents before children. Traversal stops at a
 * subquery held as unparsed text ({@code EXISTS { ... }}, {@code COUNT { ... }}, {@code COLLECT { ... }}): those are
 * parsed on their own and validated then, and there is no AST here to walk.
 * <p>
 * <b>Add a case here when introducing an expression type that nests other expressions.</b> The {@code default} arms
 * treat an unrecognised type as a leaf, which is right for a literal or a variable but silently hides whatever a new
 * composite type contains: everything inside it would escape every check that runs through this walker, with nothing
 * failing to say so.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CypherExpressionWalker {

  private CypherExpressionWalker() {
    // utility class
  }

  /**
   * Visits {@code expr} and everything nested inside it.
   */
  public static void walk(final Expression expr, final Consumer<Expression> visitor) {
    if (expr == null)
      return;

    visitor.accept(expr);

    switch (expr) {
    case FunctionCallExpression func -> walkAll(func.getArguments(), visitor);
    case ArithmeticExpression arithmetic -> {
      walk(arithmetic.getLeft(), visitor);
      walk(arithmetic.getRight(), visitor);
    }
    case ListExpression list -> walkAll(list.getElements(), visitor);
    case CaseExpression caseExpr -> {
      walk(caseExpr.getCaseExpression(), visitor);
      for (final CaseAlternative alternative : caseExpr.getAlternatives()) {
        walk(alternative.getWhenExpression(), visitor);
        walk(alternative.getThenExpression(), visitor);
      }
      walk(caseExpr.getElseExpression(), visitor);
    }
    case MapExpression map -> {
      for (final Expression value : map.getEntries().values())
        walk(value, visitor);
    }
    case MapProjectionExpression projection -> {
      for (final MapProjectionExpression.ProjectionElement element : projection.getElements())
        walk(element.getExpression(), visitor);
    }
    case ListIndexExpression index -> {
      walk(index.getListExpression(), visitor);
      walk(index.getIndexExpression(), visitor);
    }
    case ListSliceExpression slice -> {
      walk(slice.getListExpression(), visitor);
      walk(slice.getFromExpression(), visitor);
      walk(slice.getToExpression(), visitor);
    }
    case ListComprehensionExpression comprehension -> {
      walk(comprehension.getListExpression(), visitor);
      walk(comprehension.getWhereExpression(), visitor);
      walk(comprehension.getMapExpression(), visitor);
    }
    case ListPredicateExpression predicate -> {
      walk(predicate.getListExpression(), visitor);
      walk(predicate.getWhereExpression(), visitor);
    }
    case ReduceExpression reduce -> {
      walk(reduce.getInitialValue(), visitor);
      walk(reduce.getListExpression(), visitor);
      walk(reduce.getReduceExpression(), visitor);
    }
    case AllReduceExpression reduce -> {
      walk(reduce.getInitialValue(), visitor);
      walk(reduce.getListExpression(), visitor);
      walk(reduce.getReduceExpression(), visitor);
      walk(reduce.getPredicateExpression(), visitor);
    }
    case TernaryLogicalExpression ternary -> {
      walk(ternary.getLeft(), visitor);
      walk(ternary.getRight(), visitor);
    }
    case BooleanWrapperExpression wrapper -> walk(wrapper.getBooleanExpression(), visitor);
    case ComparisonExpressionWrapper wrapper -> walk(wrapper.getComparison(), visitor);
    case PatternComprehensionExpression comprehension -> {
      walk(comprehension.getPathPattern(), visitor);
      walk(comprehension.getWhereExpression(), visitor);
      walk(comprehension.getMapExpression(), visitor);
    }
    case ShortestPathExpression shortestPath -> walk(shortestPath.getPathPattern(), visitor);
    default -> {
      // A leaf: literal, variable, parameter, property access, or a subquery kept as text.
    }
    }
  }

  /**
   * Visits every expression nested inside a predicate.
   */
  public static void walk(final BooleanExpression expr, final Consumer<Expression> visitor) {
    if (expr == null)
      return;

    switch (expr) {
    case ComparisonExpression comparison -> {
      walk(comparison.getLeft(), visitor);
      walk(comparison.getRight(), visitor);
    }
    case LogicalExpression logical -> {
      walk(logical.getLeft(), visitor);
      walk(logical.getRight(), visitor);
    }
    case InExpression in -> {
      walk(in.getExpression(), visitor);
      walkAll(in.getList(), visitor);
    }
    case IsNullExpression isNull -> walk(isNull.getExpression(), visitor);
    case RegexExpression regex -> {
      walk(regex.getExpression(), visitor);
      walk(regex.getPattern(), visitor);
    }
    case StringMatchExpression match -> {
      walk(match.getExpression(), visitor);
      walk(match.getPattern(), visitor);
    }
    case LabelCheckExpression labelCheck -> walk(labelCheck.getVariableExpression(), visitor);
    case IsTypedExpression typed -> walk(typed.getValueExpression(), visitor);
    case BooleanCoercionExpression coercion -> walk(coercion.getExpression(), visitor);
    case PatternPredicateExpression pattern -> walk(pattern.getPathPattern(), visitor);
    default -> {
      // A predicate with no nested expression to reach, or one whose body is kept as unparsed text.
    }
    }
  }

  /**
   * Visits every expression nested inside a graph pattern: the inline property values of each node and relationship,
   * their dynamic labels, and any inline {@code WHERE}. This is how {@code CREATE (n:P {age: abs('x')})} is reached.
   */
  public static void walk(final PathPattern pattern, final Consumer<Expression> visitor) {
    if (pattern == null)
      return;

    if (pattern.getNodes() != null)
      for (final NodePattern node : pattern.getNodes()) {
        if (node == null)
          continue;
        walkPatternProperties(node.getProperties(), visitor);
        walkAll(node.getDynamicLabels(), visitor);
        walk(node.getWhereExpression(), visitor);
      }

    if (pattern.getRelationships() != null)
      for (final RelationshipPattern relationship : pattern.getRelationships()) {
        if (relationship == null)
          continue;
        walkPatternProperties(relationship.getProperties(), visitor);
        walk(relationship.getWhereExpression(), visitor);
      }
  }

  /**
   * Inline pattern properties are held as {@code Map<String, Object>}: a value is an {@link Expression} when the query
   * wrote one and a plain constant when the builder already folded it, so only the former can be walked.
   */
  private static void walkPatternProperties(final Map<String, Object> properties, final Consumer<Expression> visitor) {
    if (properties == null || properties.isEmpty())
      return;

    for (final Object value : properties.values())
      if (value instanceof Expression expression)
        walk(expression, visitor);
  }

  private static void walkAll(final List<Expression> expressions, final Consumer<Expression> visitor) {
    if (expressions == null)
      return;

    for (int i = 0; i < expressions.size(); i++)
      walk(expressions.get(i), visitor);
  }
}
