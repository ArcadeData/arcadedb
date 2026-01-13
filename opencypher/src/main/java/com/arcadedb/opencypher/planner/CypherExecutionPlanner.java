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
package com.arcadedb.opencypher.planner;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.opencypher.ast.CypherStatement;
import com.arcadedb.opencypher.executor.CypherExecutionPlan;
import com.arcadedb.opencypher.optimizer.CypherOptimizer;
import com.arcadedb.opencypher.optimizer.plan.PhysicalPlan;

import java.util.Map;

/**
 * Creates optimized execution plans for Cypher statements.
 * Analyzes the query and determines the best execution strategy.
 *
 * Phase 4: Integrated with Cost-Based Query Optimizer for MATCH queries.
 */
public class CypherExecutionPlanner {
  private final DatabaseInternal database;
  private final CypherStatement statement;
  private final Map<String, Object> parameters;

  public CypherExecutionPlanner(final DatabaseInternal database, final CypherStatement statement,
      final Map<String, Object> parameters) {
    this.database = database;
    this.statement = statement;
    this.parameters = parameters;
  }

  /**
   * Creates an execution plan for the Cypher statement.
   * Phase 4: Uses Cost-Based Query Optimizer for applicable queries.
   *
   * @param configuration context configuration
   * @return execution plan
   */
  public CypherExecutionPlan createExecutionPlan(final ContextConfiguration configuration) {
    PhysicalPlan physicalPlan = null;

    // Use optimizer for applicable queries
    // Currently: Simple MATCH queries with basic patterns
    // Future: Expand to more complex queries
    if (shouldUseOptimizer()) {
      try {
        final CypherOptimizer optimizer = new CypherOptimizer(database, statement, parameters);
        physicalPlan = optimizer.optimize();
      } catch (final Exception e) {
        // If optimization fails, fall back to non-optimized execution
        // This ensures backward compatibility during Phase 4 rollout
        physicalPlan = null;
      }
    }

    return new CypherExecutionPlan(database, statement, parameters, configuration, physicalPlan);
  }

  /**
   * Determines if the optimizer should be used for this query.
   * Currently conservative - only optimizes simple MATCH queries.
   *
   * Criteria:
   * - Must have at least one MATCH clause
   * - Should not have complex features that aren't yet supported
   *
   * @return true if optimizer should be used
   */
  private boolean shouldUseOptimizer() {
    // Must have MATCH clauses
    if (statement.getMatchClauses() == null || statement.getMatchClauses().isEmpty()) {
      return false;
    }

    // For Phase 4: Start with simple queries only
    // Disable optimizer for queries with features not yet fully integrated:
    // - OPTIONAL MATCH (needs special handling)
    // - Complex write operations after MATCH
    // - Variable-length paths (already work but have known issues)

    // Check for OPTIONAL MATCH
    for (final com.arcadedb.opencypher.ast.MatchClause match : statement.getMatchClauses()) {
      if (match.isOptional()) {
        return false; // Not yet supported in optimizer
      }
    }

    // Phase 4: Conservative rollout - only optimize read-only queries
    // Exclude queries with write operations until Phase 5
    if (statement.hasCreate() || statement.hasMerge() || statement.hasDelete()) {
      return false; // Write operations not yet fully integrated with optimizer
    }

    if (statement.getSetClause() != null && !statement.getSetClause().isEmpty()) {
      return false; // SET operations not yet fully integrated
    }

    // Enable optimizer for read-only MATCH queries
    return true;
  }
}
