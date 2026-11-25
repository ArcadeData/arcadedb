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
import com.arcadedb.graph.VirtualEdge;
import com.arcadedb.graph.VirtualVertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

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
   * static ones (a string is one label, a list is that many, anything else a type error, as on every other read path).
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
    // Not evaluated from here - the tree evaluates its own operands - but listed, so every analysis that walks the dynamic
    // labels (scope validation, the expression walker, the variables a filter reads) sees the ones inside the tree too
    this.dynamicLabels = predicate.dynamicExpressions();
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

    // A virtual node or relationship (create.vNode, create.vRelationship, meta.graph) has no schema type: its labels, or its
    // type name, are its own, and reading the null type as "matches nothing" made n:!B true for a node labelled B (issue #12109)
    if (predicate != null) {
      if (value instanceof VirtualVertex virtual)
        return predicate.matchesLabels(virtual.getLabels(), result, context);
      if (value instanceof VirtualEdge virtual)
        return predicate.matchesTypeName(virtual.getTypeName(), result, context);
      if (value instanceof Vertex vertex)
        return predicate.matchesVertexType(vertex.getType(), result, context);
      if (value instanceof Edge edge)
        return predicate.matchesEdgeType(edge.getType(), result, context);
      return false;
    }

    if (operator == LabelOperator.OR && !dynamicLabels.isEmpty())
      return evaluateDisjunctionWithDynamicLabels(value, result, context);

    final List<String> labels = resolveLabels(result, context);

    if (value instanceof VirtualEdge virtual)
      // Same rule as a stored relationship below: a disjunction holds when one label is the type name, a conjunction when every one is
      return matchesTypeName(virtual.getTypeName(), labels, operator == LabelOperator.OR);

    if (value instanceof Vertex vertex)
      // Same meaning a disjunction has when it is written on the pattern instead of in the WHERE (issue #6338).
      return Labels.matches(vertex, labels, operator == LabelOperator.OR);

    if (value instanceof Edge edge) {
      // Polymorphic like the pattern [r:Type]: an edge of a type that extends the label matches (issue #9124). A disjunction
      // (r:KNOWS|LIKES) holds when the edge type is an instance of one of the types, a conjunction (r:R&R, r:A&B) when it is an
      // instance of every one of them, as the pattern [r:A&B] answers: it used to be false without a look at the types, so
      // r:R&R was false for an R relationship the pattern matched (issue #12339). An empty conjunction - only a dynamic
      // $([]) - holds, as it does for a vertex
      return Labels.matches(edge.getType(), labels, operator == LabelOperator.OR);
    }

    return false;
  }

  /**
   * {@code A|$(expr)}: {@code $(expr)} is {@code $all(expr)}, so a list value is ONE operand that requires every label in
   * it, {@code A OR (B AND C)}, and an edge satisfies it when its type is an instance of every label in it. A dynamic
   * operand that evaluates to an empty list is the empty conjunction, which every element satisfies.
   */
  private Object evaluateDisjunctionWithDynamicLabels(final Object value, final Result result, final CommandContext context) {
    if (!(value instanceof Vertex) && !(value instanceof Edge))
      return false;

    boolean matched = false;
    for (final String label : labels)
      if (matchesOperand(value, List.of(label))) {
        matched = true;
        break;
      }

    // every dynamic operand is evaluated, even past one that already matched: a value that is not a label is the same type
    // error whatever the order of the operands (issue #12338)
    final List<String> operand = new ArrayList<>();
    for (final Expression dynamicLabel : dynamicLabels) {
      operand.clear();
      Labels.appendDynamicMatchLabels(operand, OpenCypherQueryEngine.getExpressionEvaluator().evaluate(dynamicLabel, result, context));
      if (!matched && matchesOperand(value, operand))
        matched = true;
    }
    return matched;
  }

  /**
   * Whether a vertex has every label of the operand, or an edge's type is an instance of every one of them (polymorphic,
   * issues #9124 and #12339). The empty operand holds.
   */
  private static boolean matchesOperand(final Object value, final List<String> operand) {
    if (value instanceof Vertex vertex)
      return Labels.matches(vertex, operand, false);
    if (value instanceof VirtualEdge virtual)
      return matchesTypeName(virtual.getTypeName(), operand, false);
    return Labels.matches(((Edge) value).getType(), operand, false);
  }

  private static boolean matchesTypeName(final String typeName, final List<String> labels, final boolean disjunction) {
    if (disjunction)
      return labels.contains(typeName);
    for (final String label : labels)
      if (!label.equals(typeName))
        return false;
    return true;
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

  /**
   * The Cypher 25 dynamic {@code $(expr)} labels, evaluated per row; empty when there are none. For a check through a
   * {@link LabelPredicate} tree these are the expressions of its {@link LabelPredicate.Dynamic} operands, which the tree
   * evaluates itself.
   */
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
