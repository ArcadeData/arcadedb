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
package com.arcadedb.opencypher.optimizer.plan;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Logical representation of a node pattern in the query.
 * Used for optimization planning.
 */
public class LogicalNode {
  private final String variable;
  private final List<String> labels;
  private final Map<String, Object> properties;

  public LogicalNode(final String variable, final List<String> labels, final Map<String, Object> properties) {
    this.variable = variable;
    this.labels = labels != null ? labels : Collections.emptyList();
    this.properties = properties != null ? properties : Collections.emptyMap();
  }

  public String getVariable() {
    return variable;
  }

  public List<String> getLabels() {
    return labels;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public boolean hasLabels() {
    return !labels.isEmpty();
  }

  public boolean hasProperties() {
    return !properties.isEmpty();
  }

  public String getFirstLabel() {
    return labels.isEmpty() ? null : labels.get(0);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("(");
    if (variable != null) {
      sb.append(variable);
    }
    if (!labels.isEmpty()) {
      sb.append(":").append(String.join(":", labels));
    }
    if (!properties.isEmpty()) {
      sb.append(" ").append(properties);
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof LogicalNode)) return false;
    final LogicalNode that = (LogicalNode) o;
    return variable != null ? variable.equals(that.variable) : that.variable == null;
  }

  @Override
  public int hashCode() {
    return variable != null ? variable.hashCode() : 0;
  }
}
