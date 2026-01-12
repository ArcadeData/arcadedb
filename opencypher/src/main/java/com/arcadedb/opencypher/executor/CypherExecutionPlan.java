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
package com.arcadedb.opencypher.executor;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.opencypher.ast.CypherStatement;
import com.arcadedb.opencypher.ast.MatchClause;
import com.arcadedb.opencypher.ast.NodePattern;
import com.arcadedb.opencypher.ast.PathPattern;
import com.arcadedb.opencypher.ast.RelationshipPattern;
import com.arcadedb.opencypher.executor.steps.CreateStep;
import com.arcadedb.opencypher.executor.steps.ExpandPathStep;
import com.arcadedb.opencypher.executor.steps.FilterPropertiesStep;
import com.arcadedb.opencypher.executor.steps.MatchNodeStep;
import com.arcadedb.opencypher.executor.steps.MatchRelationshipStep;
import com.arcadedb.opencypher.executor.steps.ProjectReturnStep;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Execution plan for a Cypher query.
 * Contains the chain of execution steps and executes them.
 */
public class CypherExecutionPlan {
  private final DatabaseInternal database;
  private final CypherStatement statement;
  private final Map<String, Object> parameters;
  private final ContextConfiguration configuration;

  public CypherExecutionPlan(final DatabaseInternal database, final CypherStatement statement,
      final Map<String, Object> parameters, final ContextConfiguration configuration) {
    this.database = database;
    this.statement = statement;
    this.parameters = parameters;
    this.configuration = configuration;
  }

  /**
   * Executes the query plan and returns results.
   * Phase 3: Builds and executes the step chain.
   *
   * @return result set
   */
  public ResultSet execute() {
    // Build execution step chain
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);

    AbstractExecutionStep rootStep = buildExecutionSteps(context);

    if (rootStep == null) {
      // No steps to execute - return empty result
      return new IteratorResultSet(new ArrayList<ResultInternal>().iterator());
    }

    // Execute the step chain
    // Use default batch size instead of Integer.MAX_VALUE for lazy evaluation
    return rootStep.syncPull(context, 100);
  }

  /**
   * Builds the execution step chain from the parsed statement.
   */
  private AbstractExecutionStep buildExecutionSteps(final CommandContext context) {
    AbstractExecutionStep currentStep = null;

    // Step 1: MATCH clause - fetch nodes
    if (!statement.getMatchClauses().isEmpty()) {
      final MatchClause matchClause = statement.getMatchClauses().get(0);

      if (matchClause.hasPathPatterns()) {
        // Phase 2+: Use parsed path patterns
        final PathPattern pathPattern = matchClause.getPathPatterns().get(0);

        if (pathPattern.isSingleNode()) {
          // Simple node pattern: MATCH (n:Person)
          final NodePattern nodePattern = pathPattern.getFirstNode();
          final String variable = nodePattern.getVariable() != null ? nodePattern.getVariable() : "n";
          currentStep = new MatchNodeStep(variable, nodePattern, context);
        } else {
          // Relationship pattern: MATCH (a)-[r]->(b)
          final NodePattern sourceNode = pathPattern.getFirstNode();
          final String sourceVar = sourceNode.getVariable() != null ? sourceNode.getVariable() : "a";

          // Start with source node
          currentStep = new MatchNodeStep(sourceVar, sourceNode, context);

          // Add relationship traversal for each relationship in the path
          for (int i = 0; i < pathPattern.getRelationshipCount(); i++) {
            final RelationshipPattern relPattern = pathPattern.getRelationship(i);
            final NodePattern targetNode = pathPattern.getNode(i + 1);
            final String relVar = relPattern.getVariable();
            final String targetVar = targetNode.getVariable() != null ? targetNode.getVariable() : ("n" + i);

            AbstractExecutionStep nextStep;
            if (relPattern.isVariableLength()) {
              // Variable-length path
              nextStep = new ExpandPathStep(sourceVar, relVar, targetVar, relPattern, context);
            } else {
              // Fixed-length relationship
              nextStep = new MatchRelationshipStep(sourceVar, relVar, targetVar, relPattern, context);
            }

            nextStep.setPrevious(currentStep);
            currentStep = nextStep;
          }
        }
      } else {
        // Phase 1: Use raw pattern string - create a simple stub
        final ResultInternal stubResult = new ResultInternal();
        stubResult.setProperty("message", "Pattern parsing not available for: " + matchClause.getPattern());
        return null;
      }
    }

    // Step 2: WHERE clause - filter results
    if (statement.getWhereClause() != null && currentStep != null) {
      final FilterPropertiesStep filterStep = new FilterPropertiesStep(statement.getWhereClause(), context);
      filterStep.setPrevious(currentStep);
      currentStep = filterStep;
    }

    // Step 3: CREATE clause - create vertices/edges
    if (statement.getCreateClause() != null && !statement.getCreateClause().isEmpty()) {
      final CreateStep createStep = new CreateStep(statement.getCreateClause(), context);
      if (currentStep != null) {
        // Chained CREATE (after MATCH/WHERE)
        createStep.setPrevious(currentStep);
      }
      // else: Standalone CREATE (no previous step)
      currentStep = createStep;
    }

    // Step 4: RETURN clause - project results
    if (statement.getReturnClause() != null && currentStep != null) {
      final ProjectReturnStep returnStep = new ProjectReturnStep(statement.getReturnClause(), context);
      returnStep.setPrevious(currentStep);
      currentStep = returnStep;
    }

    // Step 5: ORDER BY clause - sort results
    if (statement.getOrderByClause() != null && currentStep != null) {
      final com.arcadedb.opencypher.executor.steps.OrderByStep orderByStep = new com.arcadedb.opencypher.executor.steps.OrderByStep(
          statement.getOrderByClause(), context);
      orderByStep.setPrevious(currentStep);
      currentStep = orderByStep;
    }

    // Step 6: SKIP clause - skip first N results
    if (statement.getSkip() != null && currentStep != null) {
      final com.arcadedb.opencypher.executor.steps.SkipStep skipStep = new com.arcadedb.opencypher.executor.steps.SkipStep(
          statement.getSkip(), context);
      skipStep.setPrevious(currentStep);
      currentStep = skipStep;
    }

    // Step 7: LIMIT clause - limit number of results
    if (statement.getLimit() != null && currentStep != null) {
      final com.arcadedb.opencypher.executor.steps.LimitStep limitStep = new com.arcadedb.opencypher.executor.steps.LimitStep(
          statement.getLimit(), context);
      limitStep.setPrevious(currentStep);
      currentStep = limitStep;
    }

    return currentStep;
  }
}
