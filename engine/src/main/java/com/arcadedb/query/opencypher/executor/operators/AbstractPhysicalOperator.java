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
package com.arcadedb.query.opencypher.executor.operators;

/**
 * Abstract base class for physical operators.
 * Provides common functionality for cost tracking and tree structure.
 */
public abstract class AbstractPhysicalOperator implements PhysicalOperator {
  protected PhysicalOperator child;
  protected final double estimatedCost;
  protected final long estimatedCardinality;

  protected AbstractPhysicalOperator(final double estimatedCost, final long estimatedCardinality) {
    this.estimatedCost = estimatedCost;
    this.estimatedCardinality = estimatedCardinality;
    this.child = null;
  }

  protected AbstractPhysicalOperator(final PhysicalOperator child, final double estimatedCost,
                                    final long estimatedCardinality) {
    this.child = child;
    this.estimatedCost = estimatedCost;
    this.estimatedCardinality = estimatedCardinality;
  }

  @Override
  public double getEstimatedCost() {
    return estimatedCost;
  }

  @Override
  public long getEstimatedCardinality() {
    return estimatedCardinality;
  }

  @Override
  public PhysicalOperator getChild() {
    return child;
  }

  @Override
  public void setChild(final PhysicalOperator child) {
    this.child = child;
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = "  ".repeat(depth);

    sb.append(indent).append("+ ").append(getOperatorType());
    sb.append(" [cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");

    if (child != null) {
      sb.append(child.explain(depth + 1));
    }

    return sb.toString();
  }

  @Override
  public String toString() {
    return getOperatorType() + "{cost=" + String.format(Locale.US, "%.2f", estimatedCost) +
           ", cardinality=" + estimatedCardinality + "}";
  }

  /**
   * Helper method to format indentation for explain output.
   *
   * @param depth indentation depth
   * @return indentation string
   */
  protected String getIndent(final int depth) {
    return "  ".repeat(depth);
  }
}
