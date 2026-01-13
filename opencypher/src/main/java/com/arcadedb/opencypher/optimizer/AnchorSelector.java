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

import com.arcadedb.opencypher.optimizer.plan.AnchorSelection;
import com.arcadedb.opencypher.optimizer.plan.LogicalNode;
import com.arcadedb.opencypher.optimizer.plan.LogicalPlan;
import com.arcadedb.opencypher.optimizer.statistics.CostModel;
import com.arcadedb.opencypher.optimizer.statistics.IndexStatistics;
import com.arcadedb.opencypher.optimizer.statistics.StatisticsProvider;
import com.arcadedb.opencypher.optimizer.statistics.TypeStatistics;

import java.util.List;
import java.util.Map;

/**
 * Selects the optimal anchor node (starting point) for query execution.
 * <p>
 * Algorithm:
 * 1. For each node in MATCH pattern:
 *    - Check for equality predicate on indexed property → PREFERRED (lowest cost)
 *    - Check for equality predicate without index → ACCEPTABLE
 *    - No predicates → FALLBACK (highest cost)
 * 2. Return node with LOWEST estimated cost
 * <p>
 * Directionality Rule:
 * - If anchor is source in (a)-[r]->(b): Use OUT direction
 * - If anchor is target in (a)-[r]->(b): Use IN direction (reverse)
 * <p>
 * Key Insight: Start from most selective node, expand outward
 */
public class AnchorSelector {
  private final StatisticsProvider statisticsProvider;
  private final CostModel costModel;

  public AnchorSelector(final StatisticsProvider statisticsProvider, final CostModel costModel) {
    this.statisticsProvider = statisticsProvider;
    this.costModel = costModel;
  }

  /**
   * Selects the optimal anchor node for the given logical plan.
   *
   * @param plan the logical plan to analyze
   * @return anchor selection with the best starting node
   */
  public AnchorSelection selectAnchor(final LogicalPlan plan) {
    if (plan.getNodes().isEmpty()) {
      throw new IllegalArgumentException("Cannot select anchor from empty logical plan");
    }

    AnchorSelection bestAnchor = null;
    double lowestCost = Double.MAX_VALUE;

    // Evaluate each node as a potential anchor
    for (final LogicalNode node : plan.getNodes().values()) {
      final AnchorSelection candidate = evaluateNode(node, plan);

      if (candidate.getEstimatedCost() < lowestCost) {
        lowestCost = candidate.getEstimatedCost();
        bestAnchor = candidate;
      }
    }

    return bestAnchor;
  }

  /**
   * Evaluates a node as a potential anchor and estimates its cost.
   *
   * @param node the node to evaluate
   * @param plan the logical plan (for context)
   * @return anchor selection with cost estimate
   */
  private AnchorSelection evaluateNode(final LogicalNode node, final LogicalPlan plan) {
    final String variable = node.getVariable();
    final String label = node.getFirstLabel();

    // Get type statistics
    final TypeStatistics typeStats = statisticsProvider.getTypeStatistics(label);
    final long typeCount = typeStats != null ? typeStats.getRecordCount() : 1000; // Default estimate

    // Check for equality predicates on this node's properties (inline properties)
    final Map<String, Object> properties = node.getProperties();

    // ALSO check WHERE clause for equality predicates on indexed properties
    final Map<String, Object> wherePredicates = extractEqualityPredicates(variable, plan);

    // Merge inline properties with WHERE clause predicates
    final Map<String, Object> allPredicates = new java.util.HashMap<>();
    if (properties != null) {
      allPredicates.putAll(properties);
    }
    allPredicates.putAll(wherePredicates);

    if (!allPredicates.isEmpty()) {
      // Look for indexed properties with equality predicates
      final List<IndexStatistics> indexes = statisticsProvider.getIndexesForType(label);

      for (final Map.Entry<String, Object> property : allPredicates.entrySet()) {
        final String propertyName = property.getKey();
        final Object propertyValue = property.getValue();

        // Check if there's an index on this property
        final IndexStatistics indexStats = findIndexForProperty(indexes, propertyName);

        if (indexStats != null) {
          // INDEX SEEK - PREFERRED (lowest cost)
          final double selectivity = indexStats.isUnique() ? 0.001 : 0.1; // 0.1% for unique, 10% otherwise
          final long estimatedRows = (long) (typeCount * selectivity);
          final double cost = costModel.estimateIndexSeekCost(label, propertyName, selectivity);

          return new AnchorSelection(
              variable,
              node,
              true, // useIndex
              indexStats,
              propertyName,
              propertyValue, // Pass the value from WHERE clause or inline properties
              cost,
              estimatedRows
          );
        }
      }

      // FILTERED SCAN - ACCEPTABLE (no index available)
      // Use first property for selectivity estimation
      final Map.Entry<String, Object> firstProperty = properties.entrySet().iterator().next();
      final double selectivity = 0.1; // Assume 10% selectivity for equality without index
      final long estimatedRows = (long) (typeCount * selectivity);
      final double cost = costModel.estimateScanCost(label) * selectivity;

      return new AnchorSelection(
          variable,
          node,
          false, // useIndex = false
          null, // no index
          firstProperty.getKey(),
          cost,
          estimatedRows
      );
    }

    // FULL SCAN - FALLBACK (no predicates)
    final double cost = costModel.estimateScanCost(label);

    return new AnchorSelection(
        variable,
        node,
        false, // useIndex = false
        null, // no index
        null, // no property filter
        cost,
        typeCount
    );
  }

  /**
   * Finds an index that covers the given property.
   *
   * @param indexes      list of available indexes
   * @param propertyName property to search for
   * @return index statistics if found, null otherwise
   */
  private IndexStatistics findIndexForProperty(final List<IndexStatistics> indexes, final String propertyName) {
    if (indexes == null) {
      return null;
    }

    for (final IndexStatistics index : indexes) {
      // Check if this is a single-property index on the target property
      if (index.getPropertyNames().size() == 1 && index.getPropertyNames().contains(propertyName)) {
        return index;
      }

      // For composite indexes, check if property is the first column (can use index prefix)
      if (!index.getPropertyNames().isEmpty() && index.getPropertyNames().get(0).equals(propertyName)) {
        return index;
      }
    }

    return null;
  }

  /**
   * Extracts equality predicates from WHERE clauses for a given variable.
   * Example: WHERE p.id = 500 → {"id": 500}
   *
   * @param variable the variable name to search for
   * @param plan     the logical plan containing WHERE clauses
   * @return map of property names to their equality values
   */
  private Map<String, Object> extractEqualityPredicates(final String variable, final LogicalPlan plan) {
    final Map<String, Object> predicates = new java.util.HashMap<>();

    if (plan.getWhereFilters() == null || plan.getWhereFilters().isEmpty()) {
      return predicates;
    }

    for (final com.arcadedb.opencypher.ast.WhereClause whereClause : plan.getWhereFilters()) {
      final com.arcadedb.opencypher.ast.BooleanExpression condition = whereClause.getConditionExpression();
      if (condition == null) {
        continue;
      }

      // Check if it's a comparison expression
      if (condition instanceof com.arcadedb.opencypher.ast.ComparisonExpression) {
        final com.arcadedb.opencypher.ast.ComparisonExpression comparison =
            (com.arcadedb.opencypher.ast.ComparisonExpression) condition;

        // Only handle EQUALS comparisons
        if (comparison.getOperator() != com.arcadedb.opencypher.ast.ComparisonExpression.Operator.EQUALS) {
          continue;
        }

        // Check if left side is a property access on our variable
        final com.arcadedb.opencypher.ast.Expression left = comparison.getLeft();
        final com.arcadedb.opencypher.ast.Expression right = comparison.getRight();

        if (left instanceof com.arcadedb.opencypher.ast.PropertyAccessExpression) {
          final com.arcadedb.opencypher.ast.PropertyAccessExpression propAccess =
              (com.arcadedb.opencypher.ast.PropertyAccessExpression) left;

          if (propAccess.getVariableName().equals(variable)) {
            // Extract the property name and value
            final String propertyName = propAccess.getPropertyName();

            // Try to extract constant value from right side
            if (right instanceof com.arcadedb.opencypher.ast.LiteralExpression) {
              final Object value = ((com.arcadedb.opencypher.ast.LiteralExpression) right).getValue();
              predicates.put(propertyName, value);
            } else if (right instanceof com.arcadedb.opencypher.ast.ParameterExpression) {
              // For parameters, we'll mark it as having a predicate but value unknown
              // The index can still be used at runtime
              predicates.put(propertyName, null);
            }
          }
        }

        // Also check reverse: value = property (e.g., 500 = p.id)
        if (right instanceof com.arcadedb.opencypher.ast.PropertyAccessExpression) {
          final com.arcadedb.opencypher.ast.PropertyAccessExpression propAccess =
              (com.arcadedb.opencypher.ast.PropertyAccessExpression) right;

          if (propAccess.getVariableName().equals(variable)) {
            final String propertyName = propAccess.getPropertyName();

            if (left instanceof com.arcadedb.opencypher.ast.LiteralExpression) {
              final Object value = ((com.arcadedb.opencypher.ast.LiteralExpression) left).getValue();
              predicates.put(propertyName, value);
            } else if (left instanceof com.arcadedb.opencypher.ast.ParameterExpression) {
              predicates.put(propertyName, null);
            }
          }
        }
      }

      // TODO: Handle logical expressions (AND/OR) to extract more predicates
    }

    return predicates;
  }

  /**
   * Determines if a node should use index seek vs full scan.
   * Used for cost comparison.
   *
   * @param node  the node to evaluate
   * @param label the node's label/type
   * @return true if index seek should be used
   */
  public boolean shouldUseIndex(final LogicalNode node, final String label) {
    final Map<String, Object> properties = node.getProperties();

    if (properties == null || properties.isEmpty()) {
      return false; // No predicates, can't use index
    }

    final List<IndexStatistics> indexes = statisticsProvider.getIndexesForType(label);
    if (indexes == null || indexes.isEmpty()) {
      return false; // No indexes available
    }

    // Check if any property has an index
    for (final String propertyName : properties.keySet()) {
      if (findIndexForProperty(indexes, propertyName) != null) {
        return true; // Index available for this property
      }
    }

    return false;
  }
}
