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
package com.arcadedb.opencypher.optimizer;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.opencypher.ast.CypherStatement;
import com.arcadedb.opencypher.executor.operators.PhysicalOperator;
import com.arcadedb.opencypher.optimizer.plan.AnchorSelection;
import com.arcadedb.opencypher.optimizer.plan.LogicalNode;
import com.arcadedb.opencypher.optimizer.plan.LogicalPlan;
import com.arcadedb.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.opencypher.optimizer.rules.*;
import com.arcadedb.opencypher.optimizer.statistics.CostModel;
import com.arcadedb.opencypher.optimizer.statistics.StatisticsProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Main Cost-Based Query Optimizer for ArcadeDB Native Cypher.
 * <p>
 * Transforms logical plans (AST) into optimized physical execution plans
 * using runtime statistics and cost estimation.
 * <p>
 * Optimization Flow:
 * <pre>
 * 1. Build LogicalPlan from CypherStatement (AST)
 * 2. Collect runtime statistics (type counts, indexes)
 * 3. Select anchor node (best starting point)
 * 4. Apply optimization rules:
 *    - IndexSelectionRule (seek vs scan)
 *    - FilterPushdownRule (move filters closer to data)
 *    - ExpandIntoRule (semi-join for bounded patterns)
 *    - JoinOrderRule (reorder expansions by cardinality)
 * 5. Build PhysicalPlan (tree of physical operators)
 * </pre>
 * <p>
 * Expected Performance Improvements:
 * - Index selection: 10-100x speedup for selective queries
 * - ExpandInto: 5-10x speedup for bounded patterns
 * - Join ordering: 10-100x speedup for complex patterns
 * - Filter pushdown: 2-10x speedup depending on selectivity
 */
public class CypherOptimizer {
  private final DatabaseInternal database;
  private final CypherStatement statement;
  private final Map<String, Object> parameters;

  private final StatisticsProvider statisticsProvider;
  private final CostModel costModel;
  private final AnchorSelector anchorSelector;

  private final List<OptimizationRule> rules;

  public CypherOptimizer(final DatabaseInternal database,
                        final CypherStatement statement,
                        final Map<String, Object> parameters) {
    this.database = database;
    this.statement = statement;
    this.parameters = parameters;

    // Initialize statistics and cost model
    this.statisticsProvider = new StatisticsProvider(database);
    this.costModel = new CostModel(statisticsProvider);
    this.anchorSelector = new AnchorSelector(statisticsProvider, costModel);

    // Initialize optimization rules (in priority order)
    this.rules = new ArrayList<>();
    this.rules.add(new IndexSelectionRule(statisticsProvider, costModel));
    this.rules.add(new FilterPushdownRule());
    this.rules.add(new ExpandIntoRule());
    this.rules.add(new JoinOrderRule(statisticsProvider, costModel));
  }

  /**
   * Optimizes the Cypher query and produces a physical execution plan.
   *
   * @return optimized physical plan
   */
  public PhysicalPlan optimize() {
    // 1. Build logical plan from AST
    final LogicalPlan logicalPlan = buildLogicalPlan();

    // 2. Collect runtime statistics
    final List<String> typeNames = extractTypeNames(logicalPlan);
    statisticsProvider.collectStatistics(typeNames);

    // 3. Select anchor node (best starting point)
    final AnchorSelection anchor = anchorSelector.selectAnchor(logicalPlan);

    // 4. Create anchor operator (index seek or scan)
    final PhysicalOperator anchorOperator = createAnchorOperator(anchor);

    // 5. Build expansion chain (ordered by cardinality)
    // TODO: Implement in Phase 4 - will add ExpandAll/ExpandInto operators
    //       For now, we just have the anchor operator

    // 6. Apply ExpandInto optimization
    // TODO: Implement in Phase 4 - will detect bounded patterns
    //       and replace ExpandAll with ExpandInto operators

    // 7. Push down filters
    // TODO: Implement in Phase 4 - will wrap operators with FilterOperator
    //       where appropriate

    // 8. Build physical plan with root operator
    final PhysicalPlan physicalPlan = new PhysicalPlan(
        logicalPlan,
        anchor,
        anchorOperator,  // Root operator for now (just the anchor)
        anchor.getEstimatedCost(),
        anchor.getEstimatedCardinality()
    );

    return physicalPlan;
  }

  /**
   * Builds a logical plan from the Cypher AST.
   *
   * @return logical plan
   */
  private LogicalPlan buildLogicalPlan() {
    // Extract nodes, relationships, filters, and returns from AST
    return LogicalPlan.fromAST(statement);
  }

  /**
   * Extracts type names from the logical plan for statistics collection.
   *
   * @param plan the logical plan
   * @return list of type names
   */
  private List<String> extractTypeNames(final LogicalPlan plan) {
    final List<String> typeNames = new ArrayList<>();

    // Collect vertex type names
    for (final LogicalNode node : plan.getNodes().values()) {
      final String label = node.getFirstLabel();
      if (label != null) {
        typeNames.add(label);
      }
    }

    // Collect edge type names
    plan.getRelationships().forEach(rel -> {
      if (rel.getTypes() != null) {
        typeNames.addAll(rel.getTypes());
      }
    });

    return typeNames;
  }

  /**
   * Creates the appropriate physical operator for the anchor node.
   *
   * @param anchor the selected anchor
   * @return physical operator (NodeIndexSeek or NodeByLabelScan)
   */
  private PhysicalOperator createAnchorOperator(final AnchorSelection anchor) {
    final IndexSelectionRule indexRule = (IndexSelectionRule) rules.get(0);
    return indexRule.createAnchorOperator(anchor);
  }

  /**
   * Applies all optimization rules to the plan.
   *
   * @param logicalPlan  the logical plan
   * @param physicalPlan the initial physical plan
   * @return optimized physical plan
   */
  private PhysicalPlan applyOptimizationRules(final LogicalPlan logicalPlan, PhysicalPlan physicalPlan) {
    for (final OptimizationRule rule : rules) {
      if (rule.isApplicable(logicalPlan)) {
        physicalPlan = rule.apply(logicalPlan, physicalPlan);
      }
    }
    return physicalPlan;
  }

  public StatisticsProvider getStatisticsProvider() {
    return statisticsProvider;
  }

  public CostModel getCostModel() {
    return costModel;
  }

  public AnchorSelector getAnchorSelector() {
    return anchorSelector;
  }

  public List<OptimizationRule> getRules() {
    return rules;
  }
}
