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

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.DocumentType;

import java.util.ArrayList;
import java.util.List;

/**
 * Label check expression for WHERE clauses.
 * Checks if a node has a specific label (or labels).
 * <p>
 * Example: n:Person, n:Person|Developer, n:Person&Employee
 * <p>
 * Supports:
 * - Single label: n:Person (node has label Person)
 * - OR labels: n:Person|Developer (node has Person OR Developer label)
 * - AND labels: n:Person&Employee (node has BOTH Person AND Employee labels)
 * - Cypher 25 dynamic labels: n:$(expr), n:A:$(expr), evaluated per row and joined to the static labels with the same
 *   operator (issue #11776: before, the dynamic part was dropped and the check was always true)
 */
public class LabelCheckExpression implements BooleanExpression {
  private final Expression variableExpression;
  private final List<String> labels;
  private final List<Expression> dynamicLabels;
  private final LabelOperator operator;
  private final LabelPredicate predicate;
  private final String text;

  /**
   * Operator for combining multiple labels.
   */
  public enum LabelOperator {
    /**
     * Single label or multiple labels with implicit AND (like :Person:Developer in pattern).
     */
    AND,
    /**
     * Multiple labels with OR operator (like :Person|Developer).
     */
    OR,
    /**
     * Any other label expression - a negation, the {@code %} wildcard, a mix of {@code &} and {@code |} - evaluated
     * through {@link #getPredicate()} (issue #8992). {@link #getLabels()} is empty for it.
     */
    EXPRESSION
  }

  public LabelCheckExpression(final Expression variableExpression, final List<String> labels,
                              final LabelOperator operator, final String text) {
    this(variableExpression, labels, null, operator, text);
  }

  /**
   * A check against static labels plus Cypher 25 dynamic {@code $(expr)} labels, each evaluated per row and appended to the
   * static ones (a string is one label, a list is that many, {@code null} adds none, as on every other read path).
   */
  public LabelCheckExpression(final Expression variableExpression, final List<String> labels,
                              final List<Expression> dynamicLabels, final LabelOperator operator, final String text) {
    this.variableExpression = variableExpression;
    this.labels = labels;
    this.dynamicLabels = dynamicLabels == null ? List.of() : dynamicLabels;
    this.operator = operator;
    this.predicate = null;
    this.text = text;
  }

  /**
   * A check against a full label expression ({@code n:!A}, {@code n:%}, {@code n:(A|C)&!B}), see
   * {@link LabelPredicate}.
   */
  public LabelCheckExpression(final Expression variableExpression, final LabelPredicate predicate, final String text) {
    this.variableExpression = variableExpression;
    this.labels = List.of();
    this.dynamicLabels = List.of();
    this.operator = LabelOperator.EXPRESSION;
    this.predicate = predicate;
    this.text = text;
  }

  public LabelCheckExpression(final Expression variableExpression, final String label, final String text) {
    this(variableExpression, List.of(label), LabelOperator.AND, text);
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    final Object ternary = evaluateTernary(result, context);
    return Boolean.TRUE.equals(ternary);
  }

  @Override
  public Object evaluateTernary(final Result result, final CommandContext context) {
    final Object value = variableExpression.evaluate(result, context);

    if (value == null)
      return null; // null:Label -> null in Cypher 3VL

    if (predicate != null) {
      if (value instanceof Vertex vertex)
        return predicate.matchesVertexType(vertex.getType());
      if (value instanceof Edge edge)
        return predicate.matchesEdgeType(edge.getType());
      return false;
    }

    if (operator == LabelOperator.OR && !dynamicLabels.isEmpty())
      return evaluateDisjunctionWithDynamicLabels(value, result, context);

    final List<String> labels = resolveLabels(result, context);

    if (value instanceof Vertex vertex)
      // Same meaning a disjunction has when it is written on the pattern instead of in the WHERE (issue #6338).
      return Labels.matches(vertex, labels, operator == LabelOperator.OR);

    if (value instanceof Edge edge) {
      // For relationships, ":Type" is a type-equality predicate. With multiple
      // labels, the OR operator (e.g. r:KNOWS|LIKES) checks if the edge is one
      // of the listed types; a bare conjunction of type names cannot match an
      // edge (which has exactly one type), so it evaluates to false.
      // Polymorphic like the pattern [r:Type]: an edge of a type that extends the label matches (issue #9124).
      if (labels.isEmpty() && !dynamicLabels.isEmpty())
        // only a dynamic label that evaluated to null gets here: no constraint, as for a vertex
        return true;
      if (operator == LabelOperator.OR || labels.size() == 1) {
        final DocumentType edgeType = edge.getType();
        for (final String label : labels) {
          if (edgeType.instanceOf(label))
            return true;
        }
        return false;
      }
      return false;
    }

    return false;
  }

  /**
   * {@code A|$(expr)}: {@code $(expr)} is {@code $all(expr)}, so a list value is ONE operand that requires every label in
   * it, {@code A OR (B AND C)}, and an edge (exactly one type) can only satisfy a one-label operand. A dynamic operand that
   * evaluates to null or to an empty list adds no operand; with no operand left at all there is no constraint, as for a
   * conjunction.
   */
  private Object evaluateDisjunctionWithDynamicLabels(final Object value, final Result result, final CommandContext context) {
    if (!(value instanceof Vertex) && !(value instanceof Edge))
      return false;

    boolean anyOperand = false;
    for (final String label : labels) {
      anyOperand = true;
      if (matchesOperand(value, List.of(label)))
        return true;
    }

    final List<String> operand = new ArrayList<>();
    for (final Expression dynamicLabel : dynamicLabels) {
      operand.clear();
      Labels.appendDynamicMatchLabels(operand, OpenCypherQueryEngine.getExpressionEvaluator().evaluate(dynamicLabel, result, context));
      if (operand.isEmpty())
        continue;
      anyOperand = true;
      if (matchesOperand(value, operand))
        return true;
    }
    return !anyOperand;
  }

  /** Whether a vertex has every label of the operand, or an edge is of its only type (polymorphic, issue #9124). */
  private static boolean matchesOperand(final Object value, final List<String> operand) {
    if (value instanceof Vertex vertex)
      return Labels.matches(vertex, operand, false);
    return operand.size() == 1 && ((Edge) value).getType().instanceOf(operand.getFirst());
  }

  private List<String> resolveLabels(final Result result, final CommandContext context) {
    if (dynamicLabels.isEmpty())
      return labels;
    final List<String> resolved = new ArrayList<>(labels.size() + dynamicLabels.size());
    resolved.addAll(labels);
    for (final Expression dynamicLabel : dynamicLabels)
      Labels.appendDynamicMatchLabels(resolved, OpenCypherQueryEngine.getExpressionEvaluator().evaluate(dynamicLabel, result, context));
    return resolved;
  }

  @Override
  public String getText() {
    return text;
  }

  public Expression getVariableExpression() {
    return variableExpression;
  }

  public List<String> getLabels() {
    return labels;
  }

  /** The Cypher 25 dynamic {@code $(expr)} labels, evaluated per row; empty when there are none. */
  public List<Expression> getDynamicLabels() {
    return dynamicLabels;
  }

  public boolean hasDynamicLabels() {
    return !dynamicLabels.isEmpty();
  }

  public LabelOperator getOperator() {
    return operator;
  }

  /**
   * The label expression this check evaluates when {@link #getOperator()} is {@link LabelOperator#EXPRESSION},
   * {@code null} otherwise.
   */
  public LabelPredicate getPredicate() {
    return predicate;
  }
}
