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
package com.arcadedb.query.opencypher.optimizer.rules;

import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.executor.operators.NodeByLabelScan;
import com.arcadedb.query.opencypher.executor.operators.NodeIndexRangeScan;
import com.arcadedb.query.opencypher.executor.operators.NodeIndexSeek;
import com.arcadedb.query.opencypher.executor.operators.PhysicalOperator;
import com.arcadedb.query.opencypher.optimizer.RangePredicate;
import com.arcadedb.query.opencypher.optimizer.plan.AnchorSelection;
import com.arcadedb.query.opencypher.optimizer.plan.LogicalPlan;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.query.opencypher.optimizer.statistics.CostModel;
import com.arcadedb.query.opencypher.optimizer.statistics.StatisticsProvider;

import java.util.List;

/**
 * Optimization rule that decides between index seek and full table scan.
 * <p>
 * Decision Formula:
 * Use index if:
 * 1. index_cost < scan_cost
 * 2. AND selectivity < 0.1 (10% threshold)
 * <p>
 * Expected Speedup: 10-100x for selective queries
 * <p>
 * Example:
 * Query: MATCH (n:Person {id: 123}) RETURN n
 * Before: Full scan of 10,000 Person nodes → Cost: 10,000
 * After:  Index seek on Person.id → Cost: 5.1
 * Speedup: 1,960x faster ⚡
 */
public class IndexSelectionRule implements OptimizationRule {
  private final StatisticsProvider statisticsProvider;
  private final CostModel costModel;

  public IndexSelectionRule(final StatisticsProvider statisticsProvider, final CostModel costModel) {
    this.statisticsProvider = statisticsProvider;
    this.costModel = costModel;
  }

  @Override
  public String getRuleName() {
    return "IndexSelection";
  }

  @Override
  public int getPriority() {
    return 10; // Highest priority - run first
  }

  @Override
  public String getDescription() {
    return "Selects between index seek and full table scan based on cost estimation";
  }

  @Override
  public boolean isApplicable(final LogicalPlan logicalPlan) {
    // Always applicable - we always need to choose scan vs seek for anchor
    return !logicalPlan.getNodes().isEmpty();
  }

  @Override
  public PhysicalPlan apply(final LogicalPlan logicalPlan, final PhysicalPlan physicalPlan) {
    // This rule is typically applied during anchor selection
    // For now, it's integrated into the optimizer's main flow
    // Future: Could be used to retrofit plans with better index choices

    // If physical plan already exists, don't modify
    if (physicalPlan != null) {
      return physicalPlan;
    }

    // Create initial physical plan based on anchor selection
    // This is a placeholder - actual implementation happens in CypherOptimizer
    return physicalPlan;
  }

  /**
   * Decides whether to use index seek, range scan, or full scan for a given anchor.
   *
   * @param anchor the selected anchor node
   * @return physical operator (NodeIndexSeek, NodeIndexRangeScan, or NodeByLabelScan)
   */
  public PhysicalOperator createAnchorOperator(final AnchorSelection anchor) {
    if (anchor.useIndex()) {
      if (anchor.isRangeScan()) {
        // RANGE SCAN - pass predicates for runtime parameter resolution
        return new NodeIndexRangeScan(
            anchor.getVariable(),
            anchor.getNode().getFirstLabel(),
            anchor.getPropertyName(),
            anchor.getRangePredicates(),
            anchor.getIndex().getIndexName(),
            anchor.getEstimatedCost(),
            anchor.getEstimatedCardinality()
        );
      } else {
        // INDEX SEEK (equality)
        final Object propertyValue = anchor.getPropertyValue();
        return new NodeIndexSeek(
            anchor.getVariable(),
            anchor.getNode().getFirstLabel(),
            anchor.getPropertyName(),
            propertyValue,
            anchor.getIndex().getIndexName(),
            anchor.getEstimatedCost(),
            anchor.getEstimatedCardinality()
        );
      }
    } else {
      // FULL SCAN
      return new NodeByLabelScan(
          anchor.getVariable(),
          anchor.getNode().getFirstLabel(),
          anchor.getEstimatedCost(),
          anchor.getEstimatedCardinality()
      );
    }
  }

  /**
   * Compares index seek cost vs full scan cost.
   *
   * @param typeName     type name
   * @param typeCount    total records in type
   * @param selectivity  estimated selectivity (0.0-1.0)
   * @return true if index is more cost-effective
   */
  public boolean shouldUseIndex(final String typeName, final long typeCount, final double selectivity) {
    final long estimatedRows = (long) (typeCount * selectivity);

    final double scanCost = costModel.estimateScanCost(typeName);
    // Compute index cost directly using formula
    final double indexCost = costModel.INDEX_SEEK_COST + (estimatedRows * costModel.INDEX_LOOKUP_COST_PER_ROW);

    // Use index if:
    // 1. It's cheaper than scan
    // 2. AND selectivity is good enough (< 10%)
    return indexCost < scanCost && selectivity < 0.1;
  }
}
