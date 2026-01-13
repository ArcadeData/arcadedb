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

import com.arcadedb.opencypher.optimizer.statistics.IndexStatistics;

/**
 * Result of anchor selection algorithm.
 * Identifies the best starting node for query execution.
 */
public class AnchorSelection {
  private final String variable;
  private final LogicalNode node;
  private final boolean useIndex;
  private final IndexStatistics index;
  private final String propertyName;
  private final Object propertyValue;  // NEW: Value for index seek
  private final double estimatedCost;
  private final long estimatedCardinality;

  public AnchorSelection(final String variable, final LogicalNode node, final boolean useIndex,
                        final IndexStatistics index, final String propertyName,
                        final double estimatedCost, final long estimatedCardinality) {
    this(variable, node, useIndex, index, propertyName, null, estimatedCost, estimatedCardinality);
  }

  public AnchorSelection(final String variable, final LogicalNode node, final boolean useIndex,
                        final IndexStatistics index, final String propertyName, final Object propertyValue,
                        final double estimatedCost, final long estimatedCardinality) {
    this.variable = variable;
    this.node = node;
    this.useIndex = useIndex;
    this.index = index;
    this.propertyName = propertyName;
    this.propertyValue = propertyValue;
    this.estimatedCost = estimatedCost;
    this.estimatedCardinality = estimatedCardinality;
  }

  /**
   * Returns the variable name of the anchor node.
   */
  public String getVariable() {
    return variable;
  }

  /**
   * Returns the logical node selected as anchor.
   */
  public LogicalNode getNode() {
    return node;
  }

  /**
   * Returns true if an index should be used for this anchor.
   */
  public boolean useIndex() {
    return useIndex;
  }

  /**
   * Returns the index to use, if useIndex() is true.
   */
  public IndexStatistics getIndex() {
    return index;
  }

  /**
   * Returns the property name used for index seek.
   */
  public String getPropertyName() {
    return propertyName;
  }

  /**
   * Returns the property value for index seek (from WHERE clause or inline properties).
   */
  public Object getPropertyValue() {
    return propertyValue;
  }

  /**
   * Returns the estimated cost of accessing this anchor.
   */
  public double getEstimatedCost() {
    return estimatedCost;
  }

  /**
   * Returns the estimated cardinality (rows) from this anchor.
   */
  public long getEstimatedCardinality() {
    return estimatedCardinality;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("AnchorSelection{");
    sb.append("variable='").append(variable).append('\'');
    sb.append(", useIndex=").append(useIndex);
    if (useIndex) {
      sb.append(", index=").append(index.getIndexName());
      sb.append(", property=").append(propertyName);
    }
    sb.append(", cost=").append(String.format("%.2f", estimatedCost));
    sb.append(", cardinality=").append(estimatedCardinality);
    sb.append('}');
    return sb.toString();
  }
}
