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

import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.CollectExpression;
import com.arcadedb.query.opencypher.ast.CountExpression;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.ExistsExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PatternComprehensionExpression;
import com.arcadedb.query.opencypher.ast.PatternPredicateExpression;
import com.arcadedb.query.opencypher.ast.ShortestPathExpression;
import com.arcadedb.query.opencypher.ast.SimpleCypherStatement;
import com.arcadedb.query.opencypher.executor.steps.VertexPredicate;
import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The property predicates of a counted pattern that a count push-down can apply per vertex (issue #9595): the inline
 * property map of a node, when every value is a constant of the execution, and the {@code WHERE} conjuncts that read one
 * node variable and nothing else.
 * <p>
 * Both are evaluated once per distinct vertex instead of once per row, so they have to be functions of the vertex alone.
 * A conjunct is refused when it reads two variables (a join between positions, which no per-vertex filter can express),
 * none (it is not about a node at all), a variable the pattern does not bind at a position, or anything whose answer for
 * one vertex could differ from one row to the next: a non-deterministic function, a subquery or a pattern predicate, whose
 * relationships the row pipeline matches against the rest of the row. The caller declines the push-down for a statement
 * holding a predicate this refuses, so the failure direction is the row pipeline, never a different count.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CountPushDownPredicates {
  private static final Set<String> NON_DETERMINISTIC_FUNCTIONS = Set.of("rand", "randomuuid");

  private final CommandContext                 context;
  private final Map<String, BooleanExpression> whereByVariable = new HashMap<>();

  CountPushDownPredicates(final CommandContext context) {
    this.context = context;
  }

  /** The conjuncts of a predicate: the operands of its top-level ANDs, the predicate itself when it is not one. */
  static List<BooleanExpression> conjuncts(final BooleanExpression predicate) {
    final List<BooleanExpression> result = new ArrayList<>();
    collectConjuncts(predicate, result);
    return result;
  }

  private static void collectConjuncts(final BooleanExpression predicate, final List<BooleanExpression> result) {
    if (predicate instanceof LogicalExpression logical && logical.getOperator() == LogicalExpression.Operator.AND) {
      collectConjuncts(logical.getLeft(), result);
      collectConjuncts(logical.getRight(), result);
    } else if (predicate != null)
      result.add(predicate);
  }

  /**
   * The one variable a conjunct reads, when it can be evaluated per vertex; null when it reads none, several, an
   * unmodelled shape, or anything a per-vertex evaluation would answer differently from the row pipeline.
   */
  static String singleVariableOf(final BooleanExpression conjunct) {
    final CypherReferencedVariables referenced = CypherReferencedVariables.of(conjunct);
    if (!referenced.isComplete() || referenced.getNames().size() != 1)
      return null;
    final PerVertexShape shape = new PerVertexShape();
    CypherExpressionWalker.walk(conjunct, shape);
    return shape.perVertex ? referenced.getNames().iterator().next() : null;
  }

  /**
   * Records a {@code WHERE} conjunct as a predicate on the node variable it reads.
   *
   * @param nodeVariables the node variables the pattern binds at a position the operator filters
   *
   * @return false when the conjunct is not a per-vertex predicate on one of them, which the caller turns into declining
   */
  boolean addWhereConjunct(final BooleanExpression conjunct, final Set<String> nodeVariables) {
    final String variable = singleVariableOf(conjunct);
    if (variable == null || !nodeVariables.contains(variable))
      return false;
    whereByVariable.merge(variable, conjunct, (a, b) -> new LogicalExpression(LogicalExpression.Operator.AND, a, b));
    return true;
  }

  /** Whether the node's inline property map, if any, can be applied per vertex: an explicit map of constants. */
  static boolean inlinePropertiesArePerVertex(final NodePattern node) {
    if (!node.hasProperties())
      return true;
    // a bare parameter map keeps no entries to compare
    if (node.getPropertiesParameterName() != null || node.getProperties() == null)
      return false;
    for (final Object value : node.getProperties().values())
      if (value instanceof Expression expression && !isConstant(expression))
        return false;
    return true;
  }

  /**
   * The predicate of one node: its inline property map, resolved for this execution, and the conjuncts recorded for its
   * variable. Null when the node carries neither.
   */
  VertexPredicate predicateFor(final NodePattern node) {
    final String variable = node.getVariable() == null || node.getVariable().isEmpty() ? null : node.getVariable();
    final BooleanExpression where = variable != null ? whereByVariable.get(variable) : null;
    final Map<String, Object> properties = node.hasProperties() ?
        InlineProperties.resolveAll(node.getProperties(), null, context) : null;
    if (where == null && (properties == null || properties.isEmpty()))
      return null;
    return new VertexPredicate(variable, properties, where, context);
  }

  /** The predicate of a variable written at several positions: the conjuncts recorded for it, null for none. */
  VertexPredicate whereFor(final String variable) {
    final BooleanExpression where = whereByVariable.get(variable);
    return where == null ? null : new VertexPredicate(variable, null, where, context);
  }

  /** Whether any conjunct was recorded for the variable. */
  boolean hasWhereFor(final String variable) {
    return whereByVariable.containsKey(variable);
  }

  /** An expression that reads no variable and gives the same value every time it is evaluated in one execution. */
  private static boolean isConstant(final Expression expression) {
    final CypherReferencedVariables referenced = CypherReferencedVariables.of(expression);
    if (!referenced.isComplete() || !referenced.getNames().isEmpty())
      return false;
    final PerVertexShape shape = new PerVertexShape();
    CypherExpressionWalker.walk(expression, shape);
    return shape.perVertex;
  }

  /** Clears {@link #perVertex} on meeting anything that a per-vertex evaluation could answer differently from a row. */
  private static final class PerVertexShape implements CypherExpressionWalker.Visitor {
    private boolean perVertex = true;

    @Override
    public void visit(final Expression expression) {
      switch (expression) {
      case FunctionCallExpression call -> {
        // A built-in is a function of its arguments, but for the two random ones. A name that may resolve to a
        // DEFINE FUNCTION body or a polyglot function is not known to be: it can read anything and answer
        // differently on every call, so it is left to the row pipeline, which calls it once per row
        if (NON_DETERMINISTIC_FUNCTIONS.contains(call.getFunctionName().toLowerCase(Locale.ROOT))
            || !SimpleCypherStatement.isConfirmedPureFunctionName(call.getFunctionName()))
          perVertex = false;
      }
      case ExistsExpression ignored -> perVertex = false;
      case CountExpression ignored -> perVertex = false;
      case CollectExpression ignored -> perVertex = false;
      case PatternComprehensionExpression ignored -> perVertex = false;
      case ShortestPathExpression ignored -> perVertex = false;
      default -> {
      }
      }
    }

    @Override
    public void visitPredicate(final BooleanExpression predicate) {
      if (predicate instanceof PatternPredicateExpression)
        perVertex = false;
    }
  }
}
