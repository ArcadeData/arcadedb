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
import com.arcadedb.opencypher.executor.steps.AggregationStep;
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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.function.DefaultSQLFunctionFactory;

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
    final ResultSet resultSet = rootStep.syncPull(context, 100);

    // IMPORTANT: For write operations without RETURN, we need to consume the ResultSet
    // to force execution (since ResultSet is lazy). Otherwise operations won't execute
    // until the ResultSet is consumed by the caller.
    final boolean hasWriteOps = statement.getCreateClause() != null ||
                                 (statement.getSetClause() != null && !statement.getSetClause().isEmpty()) ||
                                 (statement.getDeleteClause() != null && !statement.getDeleteClause().isEmpty()) ||
                                 statement.getMergeClause() != null;

    if (statement.getReturnClause() == null && hasWriteOps) {
      // Consume the ResultSet to force write operation execution
      final List<ResultInternal> materializedResults = new ArrayList<>();
      while (resultSet.hasNext()) {
        materializedResults.add((ResultInternal) resultSet.next());
      }
      // Return the modified/created elements so they're available in the result
      return new IteratorResultSet(materializedResults.iterator());
    }

    return resultSet;
  }

  /**
   * Builds the execution step chain from the parsed statement.
   */
  private AbstractExecutionStep buildExecutionSteps(final CommandContext context) {
    AbstractExecutionStep currentStep = null;

    // Initialize function factory for expression evaluation
    final DefaultSQLFunctionFactory sqlFunctionFactory = new DefaultSQLFunctionFactory();
    final CypherFunctionFactory functionFactory = new CypherFunctionFactory(sqlFunctionFactory);

    // Special case: RETURN without MATCH (standalone expressions)
    // E.g., RETURN abs(-42), RETURN 1+1
    if (statement.getMatchClauses().isEmpty() && statement.getReturnClause() != null) {
      // Create a dummy row to evaluate expressions against
      final ResultInternal dummyRow = new ResultInternal();
      final List<Result> singleRow = List.of(dummyRow);

      // Return the single row via an initial step
      currentStep = new AbstractExecutionStep(context) {
        private boolean consumed = false;

        @Override
        public ResultSet syncPull(final CommandContext ctx, final int nRecords) {
          if (consumed) {
            return new IteratorResultSet(List.<ResultInternal>of().iterator());
          }
          consumed = true;
          return new IteratorResultSet(singleRow.iterator());
        }

        @Override
        public String prettyPrint(final int depth, final int indent) {
          return "  ".repeat(Math.max(0, depth * indent)) + "+ DUMMY ROW (for standalone expressions)";
        }
      };
    }

    // Step 1: MATCH clauses - fetch nodes
    // Process ALL MATCH clauses (not just the first)
    if (!statement.getMatchClauses().isEmpty()) {
      for (final MatchClause matchClause : statement.getMatchClauses()) {
        if (matchClause.hasPathPatterns()) {
          // Phase 2+: Use parsed path patterns
          final List<PathPattern> pathPatterns = matchClause.getPathPatterns();

          // Track the step before this MATCH clause for OPTIONAL MATCH wrapping
          final AbstractExecutionStep stepBeforeMatch = currentStep;
          final java.util.Set<String> matchVariables = new java.util.HashSet<>();
          final boolean isOptional = matchClause.isOptional();

          // For optional match, we build the match chain separately (not chained to stepBeforeMatch)
          // Then wrap it in OptionalMatchStep which manages the input
          AbstractExecutionStep matchChainStart = null;

          // Process all comma-separated patterns in the MATCH clause
          for (int patternIndex = 0; patternIndex < pathPatterns.size(); patternIndex++) {
            final PathPattern pathPattern = pathPatterns.get(patternIndex);

          if (pathPattern.isSingleNode()) {
            // Simple node pattern: MATCH (n:Person) or MATCH (a), (b)
            final NodePattern nodePattern = pathPattern.getFirstNode();
            final String variable = nodePattern.getVariable() != null ? nodePattern.getVariable() : ("n" + patternIndex);
            matchVariables.add(variable); // Track variable for OPTIONAL MATCH
            final MatchNodeStep matchStep = new MatchNodeStep(variable, nodePattern, context);

            if (isOptional) {
              // For optional match, chain within the match steps only
              if (matchChainStart == null) {
                matchChainStart = matchStep;
                currentStep = matchStep;
              } else {
                matchStep.setPrevious(currentStep);
                currentStep = matchStep;
              }
            } else {
              // For regular match, chain to previous step
              if (currentStep != null) {
                matchStep.setPrevious(currentStep);
              }
              currentStep = matchStep;
            }
          } else {
            // Relationship pattern: MATCH (a)-[r]->(b)
            final NodePattern sourceNode = pathPattern.getFirstNode();
            final String sourceVar = sourceNode.getVariable() != null ? sourceNode.getVariable() : "a";
            matchVariables.add(sourceVar); // Track variable for OPTIONAL MATCH

            // Start with source node (or chain if we have previous patterns)
            final MatchNodeStep sourceStep = new MatchNodeStep(sourceVar, sourceNode, context);

            if (isOptional) {
              // For optional match, chain within the match steps only
              if (matchChainStart == null) {
                matchChainStart = sourceStep;
                currentStep = sourceStep;
              } else {
                sourceStep.setPrevious(currentStep);
                currentStep = sourceStep;
              }
            } else {
              // For regular match, chain to previous step
              if (currentStep != null) {
                sourceStep.setPrevious(currentStep);
              }
              currentStep = sourceStep;
            }

            // Add relationship traversal for each relationship in the path
            // Check if this path has a named variable (e.g., p = (a)-[r]->(b))
            final String pathVariable = pathPattern.hasPathVariable() ? pathPattern.getPathVariable() : null;
            if (pathVariable != null) {
              matchVariables.add(pathVariable); // Track path variable
            }

            for (int i = 0; i < pathPattern.getRelationshipCount(); i++) {
              final RelationshipPattern relPattern = pathPattern.getRelationship(i);
              final NodePattern targetNode = pathPattern.getNode(i + 1);
              final String relVar = relPattern.getVariable();
              final String targetVar = targetNode.getVariable() != null ? targetNode.getVariable() : ("n" + i);

              // Track variables for OPTIONAL MATCH
              if (relVar != null && !relVar.isEmpty()) {
                matchVariables.add(relVar);
              }
              matchVariables.add(targetVar);

              AbstractExecutionStep nextStep;
              if (relPattern.isVariableLength()) {
                // Variable-length path
                nextStep = new ExpandPathStep(sourceVar, relVar, targetVar, relPattern, context);
                // ExpandPathStep already handles path variables internally
              } else {
                // Fixed-length relationship - pass path variable
                nextStep = new MatchRelationshipStep(sourceVar, relVar, targetVar, relPattern, pathVariable, context);
              }

              nextStep.setPrevious(currentStep);
              currentStep = nextStep;
            }
          }
          }

          // Wrap in OptionalMatchStep if this is an OPTIONAL MATCH
          if (isOptional && matchChainStart != null) {
            // We built a separate match chain - wrap it in OptionalMatchStep
            // matchChainStart is the first step, currentStep is the last step
            final com.arcadedb.opencypher.executor.steps.OptionalMatchStep optionalStep =
                new com.arcadedb.opencypher.executor.steps.OptionalMatchStep(currentStep, matchVariables, context);

            // OptionalMatchStep pulls from stepBeforeMatch and feeds to the match chain
            if (stepBeforeMatch != null) {
              optionalStep.setPrevious(stepBeforeMatch);
            }

            currentStep = optionalStep;
          }
        } else {
          // Phase 1: Use raw pattern string - create a simple stub
          final ResultInternal stubResult = new ResultInternal();
          stubResult.setProperty("message", "Pattern parsing not available for: " + matchClause.getPattern());
          return null;
        }
      }
    }

    // Step 2: WHERE clause - filter results
    if (statement.getWhereClause() != null && currentStep != null) {
      final FilterPropertiesStep filterStep = new FilterPropertiesStep(statement.getWhereClause(), context);
      filterStep.setPrevious(currentStep);
      currentStep = filterStep;
    }

    // Step 3: MERGE clause - find or create pattern
    if (statement.getMergeClause() != null) {
      final com.arcadedb.opencypher.executor.steps.MergeStep mergeStep = new com.arcadedb.opencypher.executor.steps.MergeStep(
          statement.getMergeClause(), context);
      // MERGE is typically standalone, but can be chained
      if (currentStep != null) {
        mergeStep.setPrevious(currentStep);
      }
      currentStep = mergeStep;
    }

    // Step 4: CREATE clause - create vertices/edges
    if (statement.getCreateClause() != null && !statement.getCreateClause().isEmpty()) {
      final CreateStep createStep = new CreateStep(statement.getCreateClause(), context);
      if (currentStep != null) {
        // Chained CREATE (after MATCH/WHERE)
        createStep.setPrevious(currentStep);
      }
      // else: Standalone CREATE (no previous step)
      currentStep = createStep;
    }

    // Step 5: SET clause - update properties
    if (statement.getSetClause() != null && !statement.getSetClause().isEmpty() && currentStep != null) {
      final com.arcadedb.opencypher.executor.steps.SetStep setStep = new com.arcadedb.opencypher.executor.steps.SetStep(
          statement.getSetClause(), context);
      setStep.setPrevious(currentStep);
      currentStep = setStep;
    }

    // Step 6: DELETE clause - delete vertices/edges
    if (statement.getDeleteClause() != null && !statement.getDeleteClause().isEmpty() && currentStep != null) {
      final com.arcadedb.opencypher.executor.steps.DeleteStep deleteStep = new com.arcadedb.opencypher.executor.steps.DeleteStep(
          statement.getDeleteClause(), context);
      deleteStep.setPrevious(currentStep);
      currentStep = deleteStep;
    }

    // Step 7: RETURN clause - project results or aggregate
    if (statement.getReturnClause() != null && currentStep != null) {
      // Check if RETURN contains aggregation functions
      if (statement.getReturnClause().hasAggregations()) {
        // Use aggregation step for aggregation functions
        final AggregationStep aggStep = new AggregationStep(statement.getReturnClause(), context, functionFactory);
        aggStep.setPrevious(currentStep);
        currentStep = aggStep;
      } else {
        // Use regular projection for non-aggregation expressions
        final ProjectReturnStep returnStep = new ProjectReturnStep(statement.getReturnClause(), context, functionFactory);
        returnStep.setPrevious(currentStep);
        currentStep = returnStep;
      }
    }

    // Step 8: ORDER BY clause - sort results
    if (statement.getOrderByClause() != null && currentStep != null) {
      final com.arcadedb.opencypher.executor.steps.OrderByStep orderByStep = new com.arcadedb.opencypher.executor.steps.OrderByStep(
          statement.getOrderByClause(), context);
      orderByStep.setPrevious(currentStep);
      currentStep = orderByStep;
    }

    // Step 9: SKIP clause - skip first N results
    if (statement.getSkip() != null && currentStep != null) {
      final com.arcadedb.opencypher.executor.steps.SkipStep skipStep = new com.arcadedb.opencypher.executor.steps.SkipStep(
          statement.getSkip(), context);
      skipStep.setPrevious(currentStep);
      currentStep = skipStep;
    }

    // Step 10: LIMIT clause - limit number of results
    if (statement.getLimit() != null && currentStep != null) {
      final com.arcadedb.opencypher.executor.steps.LimitStep limitStep = new com.arcadedb.opencypher.executor.steps.LimitStep(
          statement.getLimit(), context);
      limitStep.setPrevious(currentStep);
      currentStep = limitStep;
    }

    return currentStep;
  }
}
