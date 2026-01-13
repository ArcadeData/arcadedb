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

/**
 * Physical execution plan.
 * Represents "how" the query will be executed - the optimized operator tree.
 *
 * This is the output of the optimizer and input to the execution engine.
 * The plan is a tree of physical operators that will be converted to execution steps.
 */
public class PhysicalPlan {
  private final LogicalPlan logicalPlan;
  private final AnchorSelection anchor;
  private final double totalEstimatedCost;
  private final long totalEstimatedCardinality;

  // TODO: Phase 2 will add rootOperator field when operators are implemented
  // private final PhysicalOperator rootOperator;

  public PhysicalPlan(final LogicalPlan logicalPlan, final AnchorSelection anchor,
                     final double totalEstimatedCost, final long totalEstimatedCardinality) {
    this.logicalPlan = logicalPlan;
    this.anchor = anchor;
    this.totalEstimatedCost = totalEstimatedCost;
    this.totalEstimatedCardinality = totalEstimatedCardinality;
  }

  /**
   * Returns the logical plan this physical plan was derived from.
   */
  public LogicalPlan getLogicalPlan() {
    return logicalPlan;
  }

  /**
   * Returns the anchor selection (starting point) for execution.
   */
  public AnchorSelection getAnchor() {
    return anchor;
  }

  /**
   * Returns the total estimated cost of this plan.
   */
  public double getTotalEstimatedCost() {
    return totalEstimatedCost;
  }

  /**
   * Returns the total estimated cardinality (final row count).
   */
  public long getTotalEstimatedCardinality() {
    return totalEstimatedCardinality;
  }

  /**
   * Generates an EXPLAIN output for this physical plan.
   * Shows the operator tree with costs and cardinalities.
   */
  public String explain() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Physical Plan:\n");
    sb.append("-------------\n");
    sb.append("Anchor: ").append(anchor).append("\n");
    sb.append("\nEstimated Cost: ").append(String.format("%.2f", totalEstimatedCost)).append("\n");
    sb.append("Estimated Rows: ").append(totalEstimatedCardinality).append("\n");

    // TODO: Phase 2 will add operator tree visualization
    // sb.append("\nOperator Tree:\n");
    // sb.append(rootOperator.explain(0));

    return sb.toString();
  }

  @Override
  public String toString() {
    return "PhysicalPlan{" +
        "anchor=" + anchor +
        ", cost=" + String.format("%.2f", totalEstimatedCost) +
        ", cardinality=" + totalEstimatedCardinality +
        '}';
  }
}
