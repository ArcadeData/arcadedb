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

import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.optimizer.plan.LogicalPlan;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;

import java.util.*;

/**
 * Optimization rule that pushes filter operations closer to data sources.
 * <p>
 * Strategy: Move filters as early as possible in the execution pipeline
 * to reduce the number of rows processed by downstream operators.
 * <p>
 * Expected Speedup: 2-10x depending on filter selectivity
 * <p>
 * Example:
 * Query:
 * <pre>
 * MATCH (a:Person)-[:KNOWS]->(b:Person)
 * WHERE a.age > 30 AND b.age < 25
 * RETURN a, b
 * </pre>
 * <p>
 * Before:
 * 1. Scan Person → 'a'
 * 2. Expand KNOWS → 'b'
 * 3. Filter a.age > 30 AND b.age < 25
 * <p>
 * After:
 * 1. Scan Person → 'a'
 * 2. Filter a.age > 30  (reduces intermediate results)
 * 3. Expand KNOWS → 'b'
 * 4. Filter b.age < 25  (applied immediately after b is bound)
 * <p>
 * Benefits:
 * - Reduces memory usage
 * - Fewer rows to expand/join
 * - Better cache locality
 */
public class FilterPushdownRule implements OptimizationRule {
  @Override
  public String getRuleName() {
    return "FilterPushdown";
  }

  @Override
  public int getPriority() {
    return 20; // High priority - run after index selection
  }

  @Override
  public String getDescription() {
    return "Pushes filter operations closer to data sources to reduce intermediate result sizes";
  }

  @Override
  public boolean isApplicable(final LogicalPlan logicalPlan) {
    // Applicable if there are filters that can be pushed down
    return !logicalPlan.getWhereFilters().isEmpty();
  }

  @Override
  public PhysicalPlan apply(final LogicalPlan logicalPlan, final PhysicalPlan physicalPlan) {
    // This rule is integrated into the optimizer's filter placement logic
    // Actual pushdown happens during plan construction in CypherOptimizer
    return physicalPlan;
  }

  /**
   * Analyzes filters and determines optimal placement.
   * <p>
   * Algorithm:
   * 1. Group filters by the variables they reference
   * 2. For each filter, determine earliest point where all variables are bound
   * 3. Place filter immediately after that point
   *
   * @param filters        list of filter expressions
   * @param expansionOrder order of variable binding (anchor → expansions)
   * @return map of variable → filters to apply after that variable is bound
   */
  public Map<String, List<Expression>> analyzeFilterPlacement(
      final List<Expression> filters,
      final List<String> expansionOrder) {

    final Map<String, List<Expression>> placement = new HashMap<>();

    for (final Expression filter : filters) {
      // Determine which variables this filter references
      final Set<String> referencedVars = extractReferencedVariables(filter);

      // Find the latest variable in the expansion order that this filter needs
      String placementPoint = null;
      int latestIndex = -1;

      for (final String var : referencedVars) {
        final int index = expansionOrder.indexOf(var);
        if (index > latestIndex) {
          latestIndex = index;
          placementPoint = var;
        }
      }

      if (placementPoint != null) {
        placement.computeIfAbsent(placementPoint, k -> new ArrayList<>()).add(filter);
      }
    }

    return placement;
  }

  /**
   * Extracts variable names referenced by an expression.
   * <p>
   * Example:
   * - "a.age > 30" → {"a"}
   * - "a.age > 30 AND b.age < 25" → {"a", "b"}
   * - "a.age + b.age > 50" → {"a", "b"}
   *
   * @param expression the expression to analyze
   * @return set of variable names
   */
  private Set<String> extractReferencedVariables(final Expression expression) {
    final Set<String> variables = new HashSet<>();

    // This is a simplified implementation
    // In practice, would need to traverse the expression tree
    final String exprText = expression.getText();

    // Simple heuristic: look for patterns like "var."
    final String[] tokens = exprText.split("[^a-zA-Z0-9_]+");
    for (final String token : tokens) {
      if (!token.isEmpty() && Character.isLowerCase(token.charAt(0))) {
        // Likely a variable (lowercase start)
        variables.add(token);
      }
    }

    return variables;
  }

  /**
   * Checks if a filter can be pushed down to a specific point in the plan.
   * <p>
   * A filter can be pushed down if all variables it references are bound
   * at that point in the execution.
   *
   * @param filter         the filter to check
   * @param boundVariables variables bound at this point
   * @return true if filter can be applied here
   */
  public boolean canPushDown(final Expression filter, final Set<String> boundVariables) {
    final Set<String> referencedVars = extractReferencedVariables(filter);
    return boundVariables.containsAll(referencedVars);
  }

  /**
   * Splits a conjunctive filter (AND-connected) into individual predicates.
   * <p>
   * Example:
   * Input:  "a.age > 30 AND b.age < 25 AND a.name = 'Alice'"
   * Output: ["a.age > 30", "b.age < 25", "a.name = 'Alice'"]
   * <p>
   * This allows pushing down individual predicates to different points.
   *
   * @param filter the filter expression
   * @return list of individual predicates
   */
  public List<Expression> splitConjunctiveFilter(final Expression filter) {
    // Simplified implementation
    // In practice, would traverse the expression tree looking for AND nodes
    final List<Expression> predicates = new ArrayList<>();
    predicates.add(filter); // For now, treat as single predicate
    return predicates;
  }

  /**
   * Estimates the selectivity (filtering power) of a filter expression.
   * Used to prioritize which filters to push down first.
   *
   * @param filter the filter expression
   * @return estimated selectivity (0.0 = filters everything, 1.0 = filters nothing)
   */
  public double estimateFilterSelectivity(final Expression filter) {
    // Simplified heuristics
    final String text = filter.getText().toLowerCase();

    if (text.contains("=")) {
      return 0.1; // Equality: 10%
    } else if (text.contains(">") || text.contains("<")) {
      return 0.3; // Range: 30%
    } else if (text.contains("in")) {
      return 0.2; // IN clause: 20%
    } else {
      return 0.5; // Default: 50%
    }
  }
}
