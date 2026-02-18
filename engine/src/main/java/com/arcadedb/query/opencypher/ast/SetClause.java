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
    private final Expression valueExpression;
    private final Expression targetExpression;
    private final SetType type;
    private final List<String> labels;

    /** Property assignment: SET n.prop = value */
    public SetItem(final String variable, final String property, final Expression valueExpression) {
      this.variable = variable;
      this.property = property;
      this.valueExpression = valueExpression;
      this.targetExpression = null;
      this.type = SetType.PROPERTY;
      this.labels = null;
    }

    /** Property assignment with expression target: SET (CASE ... THEN n END).prop = value */
    public SetItem(final Expression targetExpression, final String property, final Expression valueExpression) {
      this.variable = null;
      this.property = property;
      this.targetExpression = targetExpression;
      this.valueExpression = valueExpression;
      this.type = SetType.PROPERTY;
      this.labels = null;
    }

    /** Map replacement (SET n = expr) or map merge (SET n += expr) */
    public SetItem(final String variable, final Expression valueExpression, final SetType type) {
      this.variable = variable;
      this.property = null;
      this.valueExpression = valueExpression;
      this.targetExpression = null;
      this.type = type;
      this.labels = null;
    }

    /** Label assignment: SET n:Label */
    public SetItem(final String variable, final List<String> labels) {
      this.variable = variable;
      this.property = null;
      this.valueExpression = null;
      this.targetExpression = null;
      this.type = SetType.LABELS;
      this.labels = labels;
    }

    public String getVariable() {
      return variable;
    }

    public String getProperty() {
      return property;
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
  }
}
