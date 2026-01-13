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
 * - SET n.name = 'Alice', n.age = 30
 * - SET r.weight = 1.5
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

  /**
   * Represents a single property assignment in a SET clause.
   * Example: n.name = 'Alice'
   */
  public static class SetItem {
    private final String variable;
    private final String property;
    private final String valueExpression;

    public SetItem(final String variable, final String property, final String valueExpression) {
      this.variable = variable;
      this.property = property;
      this.valueExpression = valueExpression;
    }

    public String getVariable() {
      return variable;
    }

    public String getProperty() {
      return property;
    }

    public String getValueExpression() {
      return valueExpression;
    }
  }
}
