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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.executor.steps.AggregationStep;
import com.arcadedb.query.opencypher.executor.steps.CreateStep;
import com.arcadedb.query.opencypher.executor.steps.ExpandPathStep;
import com.arcadedb.query.opencypher.executor.steps.FilterPropertiesStep;
import com.arcadedb.query.opencypher.executor.steps.MatchNodeStep;
import com.arcadedb.query.opencypher.executor.steps.MatchRelationshipStep;
import com.arcadedb.query.opencypher.executor.steps.ProjectReturnStep;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
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
 *
 * Phase 4: Enhanced with Cost-Based Query Optimizer support.
 */
public class CypherExecutionPlan {
  private final DatabaseInternal database;
  private final CypherStatement statement;
  private final Map<String, Object> parameters;
  private final ContextConfiguration configuration;
  private final PhysicalPlan physicalPlan;

  /**
   * Constructor for backward compatibility (without optimizer).
   */
  public CypherExecutionPlan(final DatabaseInternal database, final CypherStatement statement,
      final Map<String, Object> parameters, final ContextConfiguration configuration) {
    this(database, statement, parameters, configuration, null);
  }

  /**
   * Constructor with optional physical plan from optimizer.
   * Phase 4: Supports optimized execution when physicalPlan is provided.
   *
   * @param database      database instance
   * @param statement     parsed Cypher statement
   * @param parameters    query parameters
   * @param configuration context configuration
   * @param physicalPlan  optional optimized physical plan (null for non-optimized)
   */
  public CypherExecutionPlan(final DatabaseInternal database, final CypherStatement statement,
      final Map<String, Object> parameters, final ContextConfiguration configuration,
      final PhysicalPlan physicalPlan) {
    this.database = database;
    this.statement = statement;
    this.parameters = parameters;
    this.configuration = configuration;
    this.physicalPlan = physicalPlan;
  }

  /**
   * Executes the query plan and returns results.
   * Phase 4: Uses optimized physical plan when available, falls back to step chain otherwise.
   *
   * @return result set
   */
  public ResultSet execute() {
    // Build execution context
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);

    AbstractExecutionStep rootStep;

    // Phase 4: Use optimized physical plan if available
    if (physicalPlan != null && physicalPlan.getRootOperator() != null) {
      // Use optimizer - execute physical operators directly
      // Note: For Phase 4, we only optimize MATCH patterns
      // RETURN, ORDER BY, LIMIT are still handled by execution steps
      rootStep = buildExecutionStepsWithOptimizer(context);
    } else {
      // Fall back to non-optimized execution
      rootStep = buildExecutionSteps(context);
    }

    if (rootStep == null) {
      // No steps to execute - return empty result
      return new IteratorResultSet(new ArrayList<ResultInternal>().iterator());
    }

    // Execute the step chain
    final ResultSet resultSet = rootStep.syncPull(context, 100);

    // IMPORTANT: For write operations, we need to materialize the ResultSet immediately
    // to force execution (since ResultSet is lazy). This is crucial for CREATE/SET/DELETE/MERGE
    // operations to actually execute, even when there's a RETURN clause.
    final boolean hasWriteOps = statement.getCreateClause() != null ||
                                 (statement.getSetClause() != null && !statement.getSetClause().isEmpty()) ||
                                 (statement.getDeleteClause() != null && !statement.getDeleteClause().isEmpty()) ||
                                 statement.getMergeClause() != null;

    if (hasWriteOps) {
      // Materialize the ResultSet to force write operation execution
      final List<ResultInternal> materializedResults = new ArrayList<>();
      while (resultSet.hasNext()) {
        materializedResults.add((ResultInternal) resultSet.next());
      }
      // Return the materialized results
      return new IteratorResultSet(materializedResults.iterator());
    }

    return resultSet;
  }

  /**
   * Returns EXPLAIN output showing the query execution plan.
   * Displays physical operators with cost and cardinality estimates.
   *
   * @return result set containing explain output
   */
  public ResultSet explain() {
    final List<ResultInternal> results = new ArrayList<>();

    // Generate explain output
    final StringBuilder explainOutput = new StringBuilder();
    explainOutput.append("OpenCypher Native Execution Plan\n");
    explainOutput.append("=================================\n\n");

    if (physicalPlan != null && physicalPlan.getRootOperator() != null) {
      // Show optimized physical plan
      explainOutput.append("Using Cost-Based Query Optimizer\n\n");
      explainOutput.append("Physical Plan:\n");
      explainOutput.append(physicalPlan.getRootOperator().explain(0));
      explainOutput.append("\n");
      explainOutput.append(String.format("Total Estimated Cost: %.2f\n", physicalPlan.getTotalEstimatedCost()));
      explainOutput.append(String.format("Total Estimated Rows: %d\n", physicalPlan.getTotalEstimatedCardinality()));
    } else {
      // Show traditional execution path
      explainOutput.append("Using Traditional Execution (Non-Optimized)\n\n");
      explainOutput.append("Reason: Query pattern not yet supported by optimizer\n");
      explainOutput.append("Execution will use step-by-step interpretation\n");
    }

    // Create result row
    final ResultInternal result = new ResultInternal();
    result.setProperty("plan", explainOutput.toString());
    results.add(result);

    return new IteratorResultSet(results.iterator());
  }

  /**
   * Builds execution steps using the optimized physical plan.
   * Phase 4: Integrates physical operators with execution steps.
   *
   * Strategy:
   * - Physical operators handle MATCH pattern execution (optimized)
   * - Execution steps handle RETURN, ORDER BY, SKIP, LIMIT (unchanged)
   *
   * @param context command context
   * @return root execution step
   */
  private AbstractExecutionStep buildExecutionStepsWithOptimizer(final CommandContext context) {
    // Initialize function factory for expression evaluation
    final DefaultSQLFunctionFactory sqlFunctionFactory = new DefaultSQLFunctionFactory();
    final CypherFunctionFactory functionFactory = new CypherFunctionFactory(sqlFunctionFactory);
    final ExpressionEvaluator evaluator = new ExpressionEvaluator(functionFactory);

    // Create a wrapper step that executes the physical operators
    AbstractExecutionStep currentStep = new AbstractExecutionStep(context) {
      private ResultSet operatorResults = null;

      @Override
      public ResultSet syncPull(final CommandContext ctx, final int nRecords) {
        if (operatorResults == null) {
          // Execute physical operators on first pull
          operatorResults = physicalPlan.getRootOperator().execute(ctx, nRecords);
        }
        return operatorResults;
      }

      @Override
      public String prettyPrint(final int depth, final int indent) {
        return "  ".repeat(Math.max(0, depth * indent)) + "+ OPTIMIZED MATCH (physical operators)\n" +
               physicalPlan.explain();
      }
    };

    // Apply post-MATCH operations using existing execution steps
    // These are not yet optimized and use the original implementation

    // Step 2: CREATE clause (if any)
    if (statement.getCreateClause() != null && !statement.getCreateClause().isEmpty()) {
      final CreateStep createStep = new CreateStep(statement.getCreateClause(), context);
      createStep.setPrevious(currentStep);
      currentStep = createStep;
    }

    // Step 3: SET clause (if any)
    if (statement.getSetClause() != null && !statement.getSetClause().isEmpty()) {
      final com.arcadedb.query.opencypher.executor.steps.SetStep setStep =
          new com.arcadedb.query.opencypher.executor.steps.SetStep(statement.getSetClause(), context);
      setStep.setPrevious(currentStep);
      currentStep = setStep;
    }

    // Step 4: DELETE clause (if any)
    if (statement.getDeleteClause() != null && !statement.getDeleteClause().isEmpty()) {
      final com.arcadedb.query.opencypher.executor.steps.DeleteStep deleteStep =
          new com.arcadedb.query.opencypher.executor.steps.DeleteStep(statement.getDeleteClause(), context);
      deleteStep.setPrevious(currentStep);
      currentStep = deleteStep;
    }

    // Step 5: MERGE clause (if any)
    if (statement.getMergeClause() != null) {
      final com.arcadedb.query.opencypher.executor.steps.MergeStep mergeStep =
          new com.arcadedb.query.opencypher.executor.steps.MergeStep(statement.getMergeClause(), context);
      mergeStep.setPrevious(currentStep);
      currentStep = mergeStep;
    }

    // Step 6: UNWIND clause (if any)
    if (!statement.getUnwindClauses().isEmpty()) {
      for (final com.arcadedb.query.opencypher.ast.UnwindClause unwind : statement.getUnwindClauses()) {
        final com.arcadedb.query.opencypher.executor.steps.UnwindStep unwindStep =
            new com.arcadedb.query.opencypher.executor.steps.UnwindStep(unwind, context, evaluator);
        unwindStep.setPrevious(currentStep);
        currentStep = unwindStep;
      }
    }

    // Step 7: RETURN clause (if any)
    if (statement.getReturnClause() != null) {
      // Check if RETURN contains aggregation functions
      if (statement.getReturnClause().hasAggregations()) {
        // Check if there are also non-aggregated expressions (implicit GROUP BY)
        if (statement.getReturnClause().hasNonAggregations()) {
          // Use GROUP BY aggregation step (implicit grouping)
          final com.arcadedb.query.opencypher.executor.steps.GroupByAggregationStep groupByAggStep =
              new com.arcadedb.query.opencypher.executor.steps.GroupByAggregationStep(
                  statement.getReturnClause(), context, functionFactory);
          groupByAggStep.setPrevious(currentStep);
          currentStep = groupByAggStep;
        } else {
          // Use aggregation step for pure aggregations (no grouping)
          final AggregationStep aggStep = new AggregationStep(statement.getReturnClause(), context, functionFactory);
          aggStep.setPrevious(currentStep);
          currentStep = aggStep;
        }
      } else {
        // Use regular projection for non-aggregation expressions
        final ProjectReturnStep returnStep = new ProjectReturnStep(statement.getReturnClause(), context, functionFactory);
        returnStep.setPrevious(currentStep);
        currentStep = returnStep;
      }
    }

    // Step 8: ORDER BY (if any)
    if (statement.getOrderByClause() != null) {
      final com.arcadedb.query.opencypher.executor.steps.OrderByStep orderByStep =
          new com.arcadedb.query.opencypher.executor.steps.OrderByStep(statement.getOrderByClause(), context);
      orderByStep.setPrevious(currentStep);
      currentStep = orderByStep;
    }

    // Step 9: SKIP (if any)
    if (statement.getSkip() != null) {
      final com.arcadedb.query.opencypher.executor.steps.SkipStep skipStep =
          new com.arcadedb.query.opencypher.executor.steps.SkipStep(statement.getSkip(), context);
      skipStep.setPrevious(currentStep);
      currentStep = skipStep;
    }

    // Step 10: LIMIT (if any)
    if (statement.getLimit() != null) {
      final com.arcadedb.query.opencypher.executor.steps.LimitStep limitStep =
          new com.arcadedb.query.opencypher.executor.steps.LimitStep(statement.getLimit(), context);
      limitStep.setPrevious(currentStep);
      currentStep = limitStep;
    }

    return currentStep;
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

            // Check if source node is already bound (for multiple MATCH clauses or OPTIONAL MATCH)
            // If the source node has no labels/properties and there's a previous step,
            // it's likely referring to an already-bound variable - skip creating MatchNodeStep
            final boolean sourceAlreadyBound = stepBeforeMatch != null &&
                !sourceNode.hasLabels() && !sourceNode.hasProperties();

            if (!sourceAlreadyBound) {
              // Only track the source variable if we're creating a new binding for it
              matchVariables.add(sourceVar);

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
            } else {
              // Source is already bound - for optional match, start the chain with
              // a dummy step or set currentStep to null to indicate we'll start
              // directly with the relationship step
              // The relationship step will look for sourceVar in the input
              if (isOptional && matchChainStart == null) {
                // We'll start the optional chain with the relationship step
                currentStep = null;
              }
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
                // Variable-length path - pass path variable for named path support
                nextStep = new ExpandPathStep(sourceVar, pathVariable, targetVar, relPattern, context);
              } else {
                // Fixed-length relationship - pass path variable
                nextStep = new MatchRelationshipStep(sourceVar, relVar, targetVar, relPattern, pathVariable, context);
              }

              // Chain the relationship step
              if (isOptional && matchChainStart == null) {
                // This is the first step in the optional match chain
                matchChainStart = nextStep;
                // Don't set previous yet - OptionalMatchStep will manage the input
                currentStep = nextStep;
              } else if (sourceAlreadyBound && currentStep == null) {
                // For non-optional match where source is already bound and we didn't create a MatchNodeStep
                // The relationship step becomes the first step, but it will pull from stepBeforeMatch
                nextStep.setPrevious(stepBeforeMatch);
                currentStep = nextStep;
              } else {
                nextStep.setPrevious(currentStep);
                currentStep = nextStep;
              }
            }
          }
          }

          // Apply WHERE clause scoped to this MATCH (if present)
          // For OPTIONAL MATCH, this filters within the optional match chain
          if (matchClause.hasWhereClause() && currentStep != null) {
            final FilterPropertiesStep filterStep = new FilterPropertiesStep(matchClause.getWhereClause(), context);

            if (isOptional) {
              // For OPTIONAL MATCH: apply WHERE within the match chain (before wrapping)
              filterStep.setPrevious(currentStep);
              currentStep = filterStep;
              // Update matchChainStart if this is the first step
              if (matchChainStart == null) {
                matchChainStart = filterStep;
              }
            } else {
              // For regular MATCH: apply WHERE after the match
              filterStep.setPrevious(currentStep);
              currentStep = filterStep;
            }
          }

          // Wrap in OptionalMatchStep if this is an OPTIONAL MATCH
          if (isOptional && matchChainStart != null) {
            // We built a separate match chain - wrap it in OptionalMatchStep
            // Pass matchChainStart (first step) not currentStep (last step)
            // because OptionalMatchStep needs to feed input into the start of the chain
            final com.arcadedb.query.opencypher.executor.steps.OptionalMatchStep optionalStep =
                new com.arcadedb.query.opencypher.executor.steps.OptionalMatchStep(matchChainStart, matchVariables, context);

            // OptionalMatchStep pulls from stepBeforeMatch
            if (stepBeforeMatch != null) {
              optionalStep.setPrevious(stepBeforeMatch);
            }

            // The output of OptionalMatchStep becomes currentStep
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

    // Step 2: WHERE clause - now scoped to individual MATCH clauses (applied above)
    // Statement-level WHERE is only for non-MATCH contexts (WITH, etc.)
    if (statement.getWhereClause() != null && currentStep != null) {
      final FilterPropertiesStep filterStep = new FilterPropertiesStep(statement.getWhereClause(), context);
      filterStep.setPrevious(currentStep);
      currentStep = filterStep;
    }

    // Step 2.5: UNWIND clauses - expand lists into rows (can be chained)
    for (final com.arcadedb.query.opencypher.ast.UnwindClause unwindClause : statement.getUnwindClauses()) {
      final ExpressionEvaluator evaluator = new ExpressionEvaluator(functionFactory);
      final com.arcadedb.query.opencypher.executor.steps.UnwindStep unwindStep =
          new com.arcadedb.query.opencypher.executor.steps.UnwindStep(unwindClause, context, evaluator);
      if (currentStep != null) {
        unwindStep.setPrevious(currentStep);
      }
      // else: Standalone UNWIND (no previous step)
      currentStep = unwindStep;
    }

    // Step 3: MERGE clause - find or create pattern
    if (statement.getMergeClause() != null) {
      final com.arcadedb.query.opencypher.executor.steps.MergeStep mergeStep = new com.arcadedb.query.opencypher.executor.steps.MergeStep(
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
      final com.arcadedb.query.opencypher.executor.steps.SetStep setStep = new com.arcadedb.query.opencypher.executor.steps.SetStep(
          statement.getSetClause(), context);
      setStep.setPrevious(currentStep);
      currentStep = setStep;
    }

    // Step 6: DELETE clause - delete vertices/edges
    if (statement.getDeleteClause() != null && !statement.getDeleteClause().isEmpty() && currentStep != null) {
      final com.arcadedb.query.opencypher.executor.steps.DeleteStep deleteStep = new com.arcadedb.query.opencypher.executor.steps.DeleteStep(
          statement.getDeleteClause(), context);
      deleteStep.setPrevious(currentStep);
      currentStep = deleteStep;
    }

    // Step 7: RETURN clause - project results or aggregate
    if (statement.getReturnClause() != null && currentStep != null) {
      // Check if RETURN contains aggregation functions
      if (statement.getReturnClause().hasAggregations()) {
        // Check if there are also non-aggregated expressions (implicit GROUP BY)
        if (statement.getReturnClause().hasNonAggregations()) {
          // Use GROUP BY aggregation step (implicit grouping)
          final com.arcadedb.query.opencypher.executor.steps.GroupByAggregationStep groupByAggStep =
              new com.arcadedb.query.opencypher.executor.steps.GroupByAggregationStep(
                  statement.getReturnClause(), context, functionFactory);
          groupByAggStep.setPrevious(currentStep);
          currentStep = groupByAggStep;
        } else {
          // Use aggregation step for pure aggregations (no grouping)
          final AggregationStep aggStep = new AggregationStep(statement.getReturnClause(), context, functionFactory);
          aggStep.setPrevious(currentStep);
          currentStep = aggStep;
        }
      } else {
        // Use regular projection for non-aggregation expressions
        final ProjectReturnStep returnStep = new ProjectReturnStep(statement.getReturnClause(), context, functionFactory);
        returnStep.setPrevious(currentStep);
        currentStep = returnStep;
      }
    }

    // Step 8: ORDER BY clause - sort results
    if (statement.getOrderByClause() != null && currentStep != null) {
      final com.arcadedb.query.opencypher.executor.steps.OrderByStep orderByStep = new com.arcadedb.query.opencypher.executor.steps.OrderByStep(
          statement.getOrderByClause(), context);
      orderByStep.setPrevious(currentStep);
      currentStep = orderByStep;
    }

    // Step 9: SKIP clause - skip first N results
    if (statement.getSkip() != null && currentStep != null) {
      final com.arcadedb.query.opencypher.executor.steps.SkipStep skipStep = new com.arcadedb.query.opencypher.executor.steps.SkipStep(
          statement.getSkip(), context);
      skipStep.setPrevious(currentStep);
      currentStep = skipStep;
    }

    // Step 10: LIMIT clause - limit number of results
    if (statement.getLimit() != null && currentStep != null) {
      final com.arcadedb.query.opencypher.executor.steps.LimitStep limitStep = new com.arcadedb.query.opencypher.executor.steps.LimitStep(
          statement.getLimit(), context);
      limitStep.setPrevious(currentStep);
      currentStep = limitStep;
    }

    return currentStep;
  }
}
