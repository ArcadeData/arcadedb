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
 * AST node for REMOVE clause in Cypher.
 * REMOVE is used to remove properties from nodes/edges or labels from nodes.
 * <p>
 * Examples:
 * - REMOVE n.property - removes a property from a node
 * - REMOVE n:Label - removes a label from a node (not yet supported)
 */
public class RemoveClause {
  private final List<RemoveItem> items;

  public RemoveClause(final List<RemoveItem> items) {
    this.items = items != null ? items : new ArrayList<>();
  }

  public List<RemoveItem> getItems() {
    return items;
  }

  public boolean isEmpty() {
    return items.isEmpty();
  }

  /**
   * Represents a single item to remove.
   */
  public static class RemoveItem {
    /**
     * Type of removal operation.
     */
    public enum RemoveType {
      PROPERTY,   // Remove a property from node/edge
      LABELS      // Remove labels from a node (not yet implemented)
    }

    private final RemoveType type;
    private final String variable;
    private final String property;  // For PROPERTY type
    private final List<String> labels;  // For LABELS type

    /**
     * Constructor for property removal.
     *
     * @param variable the variable name (node or edge)
     * @param property the property name to remove
     */
    public RemoveItem(final String variable, final String property) {
      this.type = RemoveType.PROPERTY;
      this.variable = variable;
      this.property = property;
      this.labels = null;
    }

    /**
     * Constructor for label removal.
     *
     * @param variable the variable name (node)
     * @param labels   the labels to remove
     */
    public RemoveItem(final String variable, final List<String> labels) {
      this.type = RemoveType.LABELS;
      this.variable = variable;
      this.property = null;
      this.labels = labels;
    }

    public RemoveType getType() {
      return type;
    }

    public String getVariable() {
      return variable;
    }

    public String getProperty() {
      return property;
    }

    public List<String> getLabels() {
      return labels;
    }

    @Override
    public String toString() {
      if (type == RemoveType.PROPERTY) {
        return variable + "." + property;
      } else {
        return variable + ":" + String.join(":", labels);
      }
    }
  }

  @Override
  public String toString() {
    if (items.isEmpty()) {
      return "REMOVE";
    }
    final StringBuilder sb = new StringBuilder("REMOVE ");
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(items.get(i));
    }
    return sb.toString();
  }
}
