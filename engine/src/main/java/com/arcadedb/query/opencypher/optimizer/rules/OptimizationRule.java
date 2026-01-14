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

import com.arcadedb.query.opencypher.optimizer.plan.LogicalPlan;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;

/**
 * Base interface for query optimization rules.
 * <p>
 * Optimization rules transform logical plans into physical plans or
 * improve existing physical plans through various transformations:
 * <ul>
 *   <li>Index selection (seek vs scan)</li>
 *   <li>Filter pushdown (move filters closer to data)</li>
 *   <li>Join order optimization (reorder expansions by cardinality)</li>
 *   <li>ExpandInto optimization (semi-join for bounded patterns)</li>
 * </ul>
 * <p>
 * Rules are applied in sequence by the CypherOptimizer.
 * Each rule should be independent and composable.
 */
public interface OptimizationRule {
  /**
   * Gets the name of this optimization rule.
   *
   * @return rule name for logging and debugging
   */
  String getRuleName();

  /**
   * Checks if this rule is applicable to the given logical plan.
   * Rules should return false if they cannot improve the plan.
   *
   * @param logicalPlan the logical plan to check
   * @return true if rule can be applied
   */
  boolean isApplicable(LogicalPlan logicalPlan);

  /**
   * Applies this optimization rule to transform the logical plan.
   * <p>
   * Rules may:
   * - Create a new physical plan from scratch
   * - Modify an existing physical plan
   * - Return the input plan unchanged if no optimization is possible
   *
   * @param logicalPlan  the logical plan to optimize
   * @param physicalPlan the current physical plan (may be null for first rule)
   * @return optimized physical plan
   */
  PhysicalPlan apply(LogicalPlan logicalPlan, PhysicalPlan physicalPlan);

  /**
   * Gets the priority of this rule (lower number = higher priority).
   * Rules are applied in priority order:
   * 1. IndexSelectionRule (10)
   * 2. FilterPushdownRule (20)
   * 3. ExpandIntoRule (30)
   * 4. JoinOrderRule (40)
   *
   * @return priority value
   */
  default int getPriority() {
    return 100; // Default low priority
  }

  /**
   * Gets a description of what this rule does.
   * Used for EXPLAIN output and debugging.
   *
   * @return rule description
   */
  default String getDescription() {
    return "Optimization rule: " + getRuleName();
  }
}
