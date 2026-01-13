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
package com.arcadedb.opencypher.ast;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Represents a relationship pattern in a Cypher query.
 * Examples:
 * - -[r]-> - simple directed relationship
 * - -[r:KNOWS]-> - relationship with type
 * - -[r:KNOWS {since: 2020}]-> - relationship with type and properties
 * - -[r:KNOWS*1..3]-> - variable-length path
 * - -[r]- - undirected relationship
 */
public class RelationshipPattern implements PatternElement {
  private final String variable;
  private final List<String> types;
  private final Direction direction;
  private final Map<String, Object> properties;
  private final Integer minHops;
  private final Integer maxHops;

  public RelationshipPattern(final String variable, final List<String> types, final Direction direction,
      final Map<String, Object> properties, final Integer minHops, final Integer maxHops) {
    this.variable = variable;
    this.types = types != null ? types : Collections.emptyList();
    this.direction = direction != null ? direction : Direction.BOTH;
    this.properties = properties != null ? properties : Collections.emptyMap();
    this.minHops = minHops;
    this.maxHops = maxHops;
  }

  @Override
  public String getVariable() {
    return variable;
  }

  /**
   * Returns the list of relationship types.
   *
   * @return list of types (may be empty)
   */
  public List<String> getTypes() {
    return types;
  }

  /**
   * Returns the direction of this relationship.
   *
   * @return relationship direction
   */
  public Direction getDirection() {
    return direction;
  }

  /**
   * Returns the property constraints for this relationship.
   *
   * @return map of property name to value (may be empty)
   */
  public Map<String, Object> getProperties() {
    return properties;
  }

  /**
   * Returns the minimum hops for variable-length paths.
   *
   * @return minimum hops or null if not variable-length
   */
  public Integer getMinHops() {
    return minHops;
  }

  /**
   * Returns the maximum hops for variable-length paths.
   *
   * @return maximum hops or null if not variable-length
   */
  public Integer getMaxHops() {
    return maxHops;
  }

  /**
   * Returns true if this is a variable-length path pattern.
   *
   * @return true if variable-length
   */
  public boolean isVariableLength() {
    return minHops != null || maxHops != null;
  }

  /**
   * Returns true if this relationship pattern has types.
   *
   * @return true if has types
   */
  public boolean hasTypes() {
    return !types.isEmpty();
  }

  /**
   * Returns true if this relationship pattern has property constraints.
   *
   * @return true if has properties
   */
  public boolean hasProperties() {
    return !properties.isEmpty();
  }

  /**
   * Returns the first type if present.
   *
   * @return first type or null
   */
  public String getFirstType() {
    return types.isEmpty() ? null : types.get(0);
  }

  /**
   * Returns the effective minimum hops (default 1 if not specified).
   *
   * @return minimum hops (at least 1)
   */
  public int getEffectiveMinHops() {
    if (minHops == null)
      return 1;
    return Math.max(1, minHops);
  }

  /**
   * Returns the effective maximum hops (default Integer.MAX_VALUE if not specified).
   *
   * @return maximum hops
   */
  public int getEffectiveMaxHops() {
    if (maxHops == null)
      return isVariableLength() ? Integer.MAX_VALUE : 1;
    return maxHops;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(direction == Direction.IN ? "<" : "");
    sb.append("-[");
    if (variable != null) {
      sb.append(variable);
    }
    if (!types.isEmpty()) {
      sb.append(":").append(String.join("|", types));
    }
    if (isVariableLength()) {
      sb.append("*");
      if (minHops != null)
        sb.append(minHops);
      sb.append("..");
      if (maxHops != null)
        sb.append(maxHops);
    }
    if (!properties.isEmpty()) {
      sb.append(" ").append(properties);
    }
    sb.append("]-");
    sb.append(direction == Direction.OUT ? ">" : "");
    return sb.toString();
  }
}
