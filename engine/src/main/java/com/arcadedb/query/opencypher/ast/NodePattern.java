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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Represents a node pattern in a Cypher query.
 * Examples:
 * - (n) - anonymous node
 * - (n:Person) - node with label
 * - (n:Person {name: 'John'}) - node with label and properties
 * - (:Person) - anonymous node with label
 * - (n:$(label)) - node with a dynamic label resolved at runtime (Cypher 25)
 */
public class NodePattern implements PatternElement {
  private final String variable;
  private final List<String> labels;
  private final List<Expression> dynamicLabels;
  private final Map<String, Object> properties;
  private final boolean explicitProperties;
  private final String propertiesParameterName;

  public NodePattern(final String variable, final List<String> labels, final Map<String, Object> properties) {
    this(variable, labels, properties, null);
  }

  public NodePattern(final String variable, final List<String> labels, final Map<String, Object> properties,
      final String propertiesParameterName) {
    this(variable, labels, null, properties, propertiesParameterName);
  }

  public NodePattern(final String variable, final List<String> labels, final List<Expression> dynamicLabels,
      final Map<String, Object> properties, final String propertiesParameterName) {
    this.variable = variable;
    this.labels = labels != null ? labels : Collections.emptyList();
    this.dynamicLabels = dynamicLabels != null ? dynamicLabels : Collections.emptyList();
    this.properties = properties != null ? properties : Collections.emptyMap();
    this.explicitProperties = properties != null || propertiesParameterName != null;
    this.propertiesParameterName = propertiesParameterName;
  }

  @Override
  public String getVariable() {
    return variable;
  }

  /**
   * Returns the list of labels for this node.
   *
   * @return list of labels (may be empty)
   */
  public List<String> getLabels() {
    return labels;
  }

  /**
   * Returns the property constraints for this node.
   *
   * @return map of property name to value (may be empty)
   */
  public Map<String, Object> getProperties() {
    return properties;
  }

  /**
   * Returns true if this node pattern has labels.
   *
   * @return true if has labels
   */
  public boolean hasLabels() {
    return !labels.isEmpty();
  }

  /**
   * Returns dynamic label expressions (Cypher 25 {@code $(expression)} syntax) that must be
   * evaluated at runtime against the current binding/parameters to produce additional labels.
   *
   * @return list of dynamic label expressions (may be empty)
   */
  public List<Expression> getDynamicLabels() {
    return dynamicLabels;
  }

  /**
   * Returns true if this node pattern has dynamic label expressions.
   *
   * @return true if has dynamic labels
   */
  public boolean hasDynamicLabels() {
    return !dynamicLabels.isEmpty();
  }

  /**
   * Returns true if this node pattern has property constraints.
   *
   * @return true if has properties
   */
  public boolean hasProperties() {
    return !properties.isEmpty() || propertiesParameterName != null;
  }

  /**
   * Returns true if properties were explicitly specified (even if empty, e.g., n {}).
   */
  public boolean hasExplicitProperties() {
    return explicitProperties;
  }

  /**
   * Returns the parameter name when properties are provided as a bare parameter (e.g., $props).
   *
   * @return parameter name or null if properties are specified inline
   */
  public String getPropertiesParameterName() {
    return propertiesParameterName;
  }

  /**
   * Returns the first label if present.
   *
   * @return first label or null
   */
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
}
