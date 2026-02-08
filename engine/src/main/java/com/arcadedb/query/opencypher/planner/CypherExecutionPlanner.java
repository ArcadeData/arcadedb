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
package com.arcadedb.query.opencypher.planner;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.opencypher.ast.*;
import com.arcadedb.query.opencypher.executor.CypherExecutionPlan;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.optimizer.CypherOptimizer;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Creates optimized execution plans for Cypher statements.
 * Analyzes the query and determines the best execution strategy.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class CypherExecutionPlanner {
  private final DatabaseInternal    database;
  private final CypherStatement     statement;
  private final Map<String, Object> parameters;
  private final ExpressionEvaluator expressionEvaluator;

  public CypherExecutionPlanner(final DatabaseInternal database, final CypherStatement statement,
                                final Map<String, Object> parameters, final ExpressionEvaluator expressionEvaluator) {
    this.database = database;
    this.statement = statement;
    this.parameters = parameters;
    this.expressionEvaluator = expressionEvaluator;
  }

  /**
   * Creates an execution plan for the Cypher statement.
   * Phase 4: Uses Cost-Based Query Optimizer for applicable queries.
   *
   * @param configuration context configuration
   * @return execution plan
   */
  public CypherExecutionPlan createExecutionPlan(final ContextConfiguration configuration) {
    // Handle UNION statements specially
    if (statement instanceof UnionStatement) {
      return createUnionExecutionPlan((UnionStatement) statement, configuration);
    }

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

    return new CypherExecutionPlan(database, statement, parameters, configuration, physicalPlan, expressionEvaluator);
  }

  /**
   * Creates an execution plan for a UNION statement.
   * Combines results from multiple subqueries.
   *
   * @param unionStatement the UNION statement
   * @param configuration  context configuration
   * @return execution plan that combines subquery results
   */
  private CypherExecutionPlan createUnionExecutionPlan(final UnionStatement unionStatement,
                                                       final ContextConfiguration configuration) {
    // Create execution plans for each subquery
    final List<CypherExecutionPlan> subqueryPlans = new ArrayList<>();
    for (final CypherStatement subquery : unionStatement.getQueries()) {
      final CypherExecutionPlanner subPlanner = new CypherExecutionPlanner(database, subquery, parameters,
          expressionEvaluator);
      subqueryPlans.add(subPlanner.createExecutionPlan(configuration));
    }

    // Determine if all unions are UNION ALL (no deduplication needed)
    final boolean removeDuplicates = !unionStatement.isAllUnionAll();

    // Return a special execution plan for UNION
    return new CypherExecutionPlan(database, unionStatement, parameters, configuration, null, expressionEvaluator,
        subqueryPlans, removeDuplicates);
  }

  /**
   * Determines if the optimizer should be used for this query.
   * Currently conservative - only optimizes simple MATCH queries.
   * <p>
   * Criteria:
   * - Must have exactly one MATCH clause (multiple MATCH not yet supported)
   * - All nodes must have labels (no unlabeled nodes)
   * - Should not have complex features that aren't yet supported
   *
   * @return true if optimizer should be used
   */
  private boolean shouldUseOptimizer() {
    // Must have MATCH clauses
    if (statement.getMatchClauses() == null || statement.getMatchClauses().isEmpty())
      return false;

    // Phase 4: Only support single MATCH clause (multiple MATCH requires Cartesian product handling)
    if (statement.getMatchClauses().size() > 1)
      return false; // Multiple MATCH clauses not yet fully integrated

    // For Phase 4: Start with simple queries only
    // Disable optimizer for queries with features not yet fully integrated:
    // - OPTIONAL MATCH (needs special handling)
    // - Complex write operations after MATCH
    // - Variable-length paths (already work but have known issues)
    // - Unlabeled nodes (optimizer requires labels for physical operators)

    // Check for OPTIONAL MATCH, unlabeled nodes, and disconnected patterns
    for (final MatchClause match : statement.getMatchClauses()) {
      if (match.isOptional())
        return false; // Not yet supported in optimizer

      // Multiple path patterns in a single MATCH (e.g., MATCH (a:T1), (b:T2))
      // require Cartesian product which the optimizer doesn't support yet
      if (match.hasPathPatterns() && match.getPathPatterns().size() > 1)
        return false;

      // Check if all nodes have labels, no named path variables, and no unsupported property constraints
      if (match.hasPathPatterns()) {
        for (final PathPattern path : match.getPathPatterns()) {
          // Named path variables not yet supported (e.g., "p = (a)-[r]->(b)")
          if (path.hasPathVariable())
            return false;

          for (final NodePattern node : path.getNodes()) {
            if (!node.hasLabels())
              return false; // Unlabeled nodes not supported yet

            // Phase 4: Property constraints without indexes not yet supported
            // The optimizer doesn't apply property filters when using NodeByLabelScan
            // This will be fixed in Phase 5
            if (node.hasProperties())
              return false; // Property constraints not yet fully integrated
          }
        }
      }
    }

    // Disable optimizer for FOREACH queries (FOREACH contains write operations)
    if (statement.getClausesInOrder() != null &&
        statement.getClausesInOrder().stream().anyMatch(c -> c.getType() == ClauseEntry.ClauseType.FOREACH))
      return false;

    // Phase 4: Conservative rollout - only optimize read-only queries
    // Exclude queries with write operations until Phase 5
    if (statement.hasCreate() || statement.hasMerge() || statement.hasDelete())
      return false; // Write operations not yet fully integrated with optimizer

    if (statement.getSetClause() != null && !statement.getSetClause().isEmpty())
      return false; // SET operations not yet fully integrated

    // Phase 4: Aggregation functions not yet fully integrated with optimizer
    // The optimizer doesn't handle GROUP BY and aggregation properly yet
    if (statement.getReturnClause() != null && statement.getReturnClause().hasAggregations())
      return false; // Aggregation queries use traditional execution

    // Enable optimizer for simple read-only single MATCH queries with all labeled nodes
    return true;
  }
}
