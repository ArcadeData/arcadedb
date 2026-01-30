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

import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

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
 */
public class LabelCheckExpression implements BooleanExpression {
  private final Expression variableExpression;
  private final List<String> labels;
  private final LabelOperator operator;
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
    OR
  }

  public LabelCheckExpression(final Expression variableExpression, final List<String> labels,
                              final LabelOperator operator, final String text) {
    this.variableExpression = variableExpression;
    this.labels = labels;
    this.operator = operator;
    this.text = text;
  }

  public LabelCheckExpression(final Expression variableExpression, final String label, final String text) {
    this(variableExpression, List.of(label), LabelOperator.AND, text);
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    // Evaluate the variable expression to get the vertex
    final Object value = variableExpression.evaluate(result, context);

    if (value == null)
      return false;

    if (!(value instanceof Vertex))
      return false;

    final Vertex vertex = (Vertex) value;

    if (operator == LabelOperator.OR) {
      // OR: vertex must have at least one of the labels
      for (final String label : labels) {
        if (Labels.hasLabel(vertex, label))
          return true;
      }
      return false;
    } else {
      // AND: vertex must have all labels
      for (final String label : labels) {
        if (!Labels.hasLabel(vertex, label))
          return false;
      }
      return true;
    }
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

  public LabelOperator getOperator() {
    return operator;
  }
}
