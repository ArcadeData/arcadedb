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
    PhysicalOperator rootOperator = anchorOperator;
    if (!logicalPlan.getRelationships().isEmpty()) {
      rootOperator = buildExpansionChain(logicalPlan, anchor, anchorOperator);
    }

    // 6. Apply ExpandInto optimization
    // ExpandInto is detected during expansion chain building (step 5)
    // and applied automatically when both endpoints are bound

    // 7. Push down filters
    if (!logicalPlan.getWhereFilters().isEmpty()) {
      rootOperator = applyFilterPushdown(logicalPlan, rootOperator);
    }

    // 8. Calculate total cost and cardinality
    double totalCost = rootOperator.getEstimatedCost();
    long totalCardinality = rootOperator.getEstimatedCardinality();

    // 9. Build physical plan with complete operator tree
    final PhysicalPlan physicalPlan = new PhysicalPlan(
        logicalPlan,
        anchor,
        rootOperator,
        totalCost,
        totalCardinality
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
   * Builds the expansion chain by ordering relationships and creating expand operators.
   * Uses JoinOrderRule for ordering and ExpandIntoRule for optimization.
   *
   * @param logicalPlan    the logical plan
   * @param anchor         the selected anchor
   * @param anchorOperator the anchor operator to build upon
   * @return root operator of the expansion chain
   */
  private PhysicalOperator buildExpansionChain(final LogicalPlan logicalPlan,
                                                final AnchorSelection anchor,
                                                final PhysicalOperator anchorOperator) {
    // Get optimization rules
    final JoinOrderRule joinOrderRule = (JoinOrderRule) rules.get(3);
    final ExpandIntoRule expandIntoRule = (ExpandIntoRule) rules.get(2);

    // Determine which variables are bound
    final java.util.Set<String> boundVariables = expandIntoRule.determineBoundVariables(
        logicalPlan, anchor.getVariable());

    // Order relationships by estimated cardinality
    final List<com.arcadedb.opencypher.optimizer.plan.LogicalRelationship> orderedRels =
        joinOrderRule.orderRelationships(
            logicalPlan.getRelationships(),
            anchor.getVariable(),
            boundVariables
        );

    // Build operator chain
    PhysicalOperator currentOp = anchorOperator;

    for (final com.arcadedb.opencypher.optimizer.plan.LogicalRelationship rel : orderedRels) {
      // Check if we should use ExpandInto (both endpoints bound)
      if (expandIntoRule.shouldUseExpandInto(rel, boundVariables)) {
        // Use ExpandInto for bounded patterns
        currentOp = createExpandIntoOperator(rel, currentOp, boundVariables);
      } else {
        // Use ExpandAll for unbounded patterns
        currentOp = createExpandAllOperator(rel, currentOp);
      }

      // Update bound variables after expansion
      boundVariables.add(rel.getSourceVariable());
      boundVariables.add(rel.getTargetVariable());
    }

    return currentOp;
  }

  /**
   * Creates an ExpandAll operator for relationship traversal.
   *
   * @param relationship the logical relationship
   * @param input        the input operator
   * @return ExpandAll operator
   */
  private PhysicalOperator createExpandAllOperator(
      final com.arcadedb.opencypher.optimizer.plan.LogicalRelationship relationship,
      final PhysicalOperator input) {
    // Extract parameters from relationship
    final String sourceVariable = relationship.getSourceVariable();
    final String edgeVariable = relationship.getVariable();
    final String targetVariable = relationship.getTargetVariable();
    final com.arcadedb.opencypher.ast.Direction direction = relationship.getDirection();
    final String[] edgeTypes = relationship.getTypes().toArray(new String[0]);

    // Estimate cost and cardinality for this expansion
    final long inputCardinality = input.getEstimatedCardinality();
    final double avgDegree = 10.0; // TODO: Use statistics to estimate average degree
    final long outputCardinality = inputCardinality * (long) avgDegree;
    final double expansionCost = inputCardinality * avgDegree * costModel.EXPAND_COST_PER_ROW;
    final double totalCost = input.getEstimatedCost() + expansionCost;

    return new com.arcadedb.opencypher.executor.operators.ExpandAll(
        input,
        sourceVariable,
        edgeVariable,
        targetVariable,
        direction,
        edgeTypes,
        totalCost,
        outputCardinality
    );
  }

  /**
   * Creates an ExpandInto operator for bounded pattern checking.
   *
   * @param relationship    the logical relationship
   * @param input           the input operator
   * @param boundVariables  the currently bound variables
   * @return ExpandInto operator
   */
  private PhysicalOperator createExpandIntoOperator(
      final com.arcadedb.opencypher.optimizer.plan.LogicalRelationship relationship,
      final PhysicalOperator input,
      final java.util.Set<String> boundVariables) {
    // Extract parameters from relationship
    final String sourceVariable = relationship.getSourceVariable();
    final String targetVariable = relationship.getTargetVariable();
    final String edgeVariable = relationship.getVariable();
    final com.arcadedb.opencypher.ast.Direction direction = relationship.getDirection();
    final String[] edgeTypes = relationship.getTypes().toArray(new String[0]);

    // Estimate cost and cardinality for ExpandInto
    // ExpandInto is much cheaper than ExpandAll because it's just an existence check
    final long inputCardinality = input.getEstimatedCardinality();
    final double selectivity = 0.1; // Estimate 10% of input rows have matching connections
    final long outputCardinality = (long) (inputCardinality * selectivity);
    final double expandIntoCost = inputCardinality * 1.0; // O(1) per input row for existence check
    final double totalCost = input.getEstimatedCost() + expandIntoCost;

    return new com.arcadedb.opencypher.executor.operators.ExpandInto(
        input,
        sourceVariable,
        targetVariable,
        edgeVariable,
        direction,
        edgeTypes,
        totalCost,
        outputCardinality
    );
  }

  /**
   * Applies filter pushdown optimization by wrapping operators with FilterOperator.
   *
   * @param logicalPlan the logical plan
   * @param rootOperator the root operator to wrap
   * @return root operator with filters applied
   */
  private PhysicalOperator applyFilterPushdown(final LogicalPlan logicalPlan,
                                                final PhysicalOperator rootOperator) {
    // Wrap the root operator with a FilterOperator for each WHERE clause
    // In a full implementation, we would analyze which filters can be pushed down
    // to lower operators (closer to data sources) for better performance.
    // For now, we apply all filters at the top level.

    PhysicalOperator currentOp = rootOperator;

    for (final com.arcadedb.opencypher.ast.WhereClause whereClause : logicalPlan.getWhereFilters()) {
      final com.arcadedb.opencypher.ast.BooleanExpression filterExpression = whereClause.getConditionExpression();

      if (filterExpression != null) {
        // Estimate cost and cardinality for this filter
        final long inputCardinality = currentOp.getEstimatedCardinality();
        final double selectivity = 0.5; // Default selectivity estimate (50% pass through)
        final long outputCardinality = (long) (inputCardinality * selectivity);
        final double filterCost = inputCardinality * costModel.FILTER_COST_PER_ROW;
        final double totalCost = currentOp.getEstimatedCost() + filterCost;

        // Wrap with FilterOperator
        currentOp = new com.arcadedb.opencypher.executor.operators.FilterOperator(
            currentOp,
            filterExpression,
            totalCost,
            outputCardinality
        );
      }
    }

    return currentOp;
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
