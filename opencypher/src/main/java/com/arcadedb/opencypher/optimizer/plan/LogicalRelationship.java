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

import com.arcadedb.opencypher.ast.Direction;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Logical representation of a relationship pattern in the query.
 * Used for optimization planning.
 */
public class LogicalRelationship {
  private final String variable;
  private final String sourceVariable;
  private final String targetVariable;
  private final List<String> types;
  private final Direction direction;
  private final Map<String, Object> properties;
  private final boolean isVariableLength;
  private final Integer minHops;
  private final Integer maxHops;

  public LogicalRelationship(final String variable, final String sourceVariable, final String targetVariable,
                            final List<String> types, final Direction direction, final Map<String, Object> properties,
                            final Integer minHops, final Integer maxHops) {
    this.variable = variable;
    this.sourceVariable = sourceVariable;
    this.targetVariable = targetVariable;
    this.types = types != null ? types : Collections.emptyList();
    this.direction = direction != null ? direction : Direction.BOTH;
    this.properties = properties != null ? properties : Collections.emptyMap();
    this.minHops = minHops;
    this.maxHops = maxHops;
    this.isVariableLength = minHops != null || maxHops != null;
  }

  public String getVariable() {
    return variable;
  }

  public String getSourceVariable() {
    return sourceVariable;
  }

  public String getTargetVariable() {
    return targetVariable;
  }

  public List<String> getTypes() {
    return types;
  }

  public Direction getDirection() {
    return direction;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public boolean isVariableLength() {
    return isVariableLength;
  }

  public Integer getMinHops() {
    return minHops;
  }

  public Integer getMaxHops() {
    return maxHops;
  }

  public boolean hasTypes() {
    return !types.isEmpty();
  }

  public String getFirstType() {
    return types.isEmpty() ? null : types.get(0);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("(").append(sourceVariable).append(")");

    final String leftArrow = direction == Direction.IN ? "<-" : "-";
    final String rightArrow = direction == Direction.OUT ? "->" : "-";

    sb.append(leftArrow).append("[");
    if (variable != null) {
      sb.append(variable);
    }
    if (!types.isEmpty()) {
      sb.append(":").append(String.join("|", types));
    }
    if (isVariableLength) {
      sb.append("*");
      if (minHops != null) {
        sb.append(minHops);
      }
      sb.append("..");
      if (maxHops != null) {
        sb.append(maxHops);
      }
    }
    sb.append("]").append(rightArrow);

    sb.append("(").append(targetVariable).append(")");
    return sb.toString();
  }
}
