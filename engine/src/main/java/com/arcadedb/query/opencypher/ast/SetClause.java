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

import com.arcadedb.query.opencypher.Labels;

import java.util.List;

/**
 * Represents a SET clause in a Cypher query.
 * Updates properties on existing vertices and edges.
 * <p>
 * Examples:
 * - SET n.name = 'Alice'
 * - SET n = {name: 'Alice', age: 30}
 * - SET n += {name: 'Alice'}
 * - SET n:Label
 * - SET n:$(expr)   (Cypher 25 dynamic label)
 */
public class SetClause {
  private final List<SetItem> items;

  public SetClause(final List<SetItem> items) {
    this.items = items;
  }

  public List<SetItem> getItems() {
    return items;
  }

  public boolean isEmpty() {
    return items == null || items.isEmpty();
  }

  public enum SetType {
    PROPERTY,       // SET n.prop = value
    REPLACE_MAP,    // SET n = {map}
    MERGE_MAP,      // SET n += {map}
    LABELS          // SET n:Label:Label2
  }

  /**
   * Represents a single item in a SET clause.
   */
  public static class SetItem {
    private final String variable;
    private final String property;
    private final Expression keyExpression;
    private final Expression valueExpression;
    private final Expression targetExpression;
    private final SetType type;
    private final List<String> labels;
    private final List<Expression> labelExpressions;

    /** Property assignment: SET n.prop = value */
    public SetItem(final String variable, final String property, final Expression valueExpression) {
      this.variable = variable;
      this.property = property;
      this.keyExpression = null;
      this.valueExpression = valueExpression;
      this.targetExpression = null;
      this.type = SetType.PROPERTY;
      this.labels = null;
      this.labelExpressions = List.of();
    }

    /** Property assignment with expression target: SET (CASE ... THEN n END).prop = value */
    public SetItem(final Expression targetExpression, final String property, final Expression valueExpression) {
      this.variable = null;
      this.property = property;
      this.keyExpression = null;
      this.targetExpression = targetExpression;
      this.valueExpression = valueExpression;
      this.type = SetType.PROPERTY;
      this.labels = null;
      this.labelExpressions = List.of();
    }

    /** Map replacement (SET n = expr) or map merge (SET n += expr) */
    public SetItem(final String variable, final Expression valueExpression, final SetType type) {
      this.variable = variable;
      this.property = null;
      this.keyExpression = null;
      this.valueExpression = valueExpression;
      this.targetExpression = null;
      this.type = type;
      this.labels = null;
      this.labelExpressions = List.of();
    }

    /** Label assignment: SET n:Label */
    public SetItem(final String variable, final List<String> labels) {
      this(variable, labels, List.of());
    }

    /**
     * Label assignment with Cypher 25 dynamic labels: {@code SET n:Static:$(expr)}. The static labels are known at
     * parse time; each expression in {@code labelExpressions} is evaluated per row and contributes the label (or
     * list of labels) it yields. Labels are a set, so the two lists are merged without preserving the order they
     * were written in - {@link Labels#ensureCompositeType} sorts them anyway.
     */
    public SetItem(final String variable, final List<String> labels, final List<Expression> labelExpressions) {
      this.variable = variable;
      this.property = null;
      this.keyExpression = null;
      this.valueExpression = null;
      this.targetExpression = null;
      this.type = SetType.LABELS;
      this.labels = labels;
      this.labelExpressions = labelExpressions != null ? labelExpressions : List.of();
    }

    /**
     * Dynamic property assignment: SET n[keyExpr] = value. The property name is computed at
     * runtime by evaluating {@code keyExpression}. When {@code targetExpression} is null the
     * base is the plain variable {@code variable}; otherwise the base is the evaluated
     * {@code targetExpression} (e.g. SET (CASE ... END)[k] = value).
     */
    public SetItem(final String variable, final Expression targetExpression, final Expression keyExpression,
        final Expression valueExpression) {
      this.variable = variable;
      this.property = null;
      this.keyExpression = keyExpression;
      this.valueExpression = valueExpression;
      this.targetExpression = targetExpression;
      this.type = SetType.PROPERTY;
      this.labels = null;
      this.labelExpressions = List.of();
    }

    public String getVariable() {
      return variable;
    }

    public String getProperty() {
      return property;
    }

    public Expression getKeyExpression() {
      return keyExpression;
    }

    public Expression getValueExpression() {
      return valueExpression;
    }

    public Expression getTargetExpression() {
      return targetExpression;
    }

    public SetType getType() {
      return type;
    }

    public List<String> getLabels() {
      return labels;
    }

    /** The Cypher 25 {@code $(expr)} labels of this item, evaluated per row. Never null; empty when there are none. */
    public List<Expression> getLabelExpressions() {
      return labelExpressions;
    }

    public boolean hasLabelExpressions() {
      return !labelExpressions.isEmpty();
    }
  }
}
