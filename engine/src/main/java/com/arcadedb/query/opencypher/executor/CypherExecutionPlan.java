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
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.CallClause;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.CreateClause;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.DeleteClause;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.ForeachClause;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LoadCSVClause;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.MergeClause;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PatternPredicateExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.RemoveClause;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.ast.ShortestPathPattern;
import com.arcadedb.query.opencypher.ast.StarExpression;
import com.arcadedb.query.opencypher.ast.SubqueryClause;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.executor.operators.GAVFusedChainOperator;
import com.arcadedb.query.opencypher.executor.steps.AggregationStep;
import com.arcadedb.query.opencypher.executor.steps.CallStep;
import com.arcadedb.query.opencypher.executor.steps.AntiJoinChainOp;
import com.arcadedb.query.opencypher.executor.steps.CSRCountStep;
import com.arcadedb.query.opencypher.executor.steps.CountChainedEdgesStep;
import com.arcadedb.query.opencypher.executor.steps.CountOp;
import com.arcadedb.query.opencypher.executor.steps.DegreeProductOp;
import com.arcadedb.query.opencypher.executor.steps.PairHashJoinOp;
import com.arcadedb.query.opencypher.executor.steps.PartitionedTriangleOp;
import com.arcadedb.query.opencypher.executor.steps.PropagateChainOp;
import com.arcadedb.query.opencypher.executor.steps.CountEdgesReturnStep;
import com.arcadedb.query.opencypher.executor.steps.CountEdgesStep;
import com.arcadedb.query.opencypher.executor.steps.CreateStep;
import com.arcadedb.query.opencypher.executor.steps.DeleteStep;
import com.arcadedb.query.opencypher.executor.steps.ExpandPathStep;
import com.arcadedb.query.opencypher.executor.steps.FilterPropertiesStep;
import com.arcadedb.query.opencypher.executor.steps.FinalProjectionStep;
import com.arcadedb.query.opencypher.executor.steps.ForeachStep;
import com.arcadedb.query.opencypher.executor.steps.GroupByAggregationStep;
import com.arcadedb.query.opencypher.executor.steps.LimitStep;
import com.arcadedb.query.opencypher.executor.steps.LoadCSVStep;
import com.arcadedb.query.opencypher.executor.steps.MatchNodeStep;
import com.arcadedb.query.opencypher.executor.steps.MatchRelationshipStep;
import com.arcadedb.query.opencypher.executor.steps.MergeStep;
import com.arcadedb.query.opencypher.executor.steps.OptionalMatchStep;
import com.arcadedb.query.opencypher.executor.steps.OrderByStep;
import com.arcadedb.query.opencypher.executor.steps.ProjectReturnStep;
import com.arcadedb.query.opencypher.executor.steps.RemoveStep;
import com.arcadedb.query.opencypher.executor.steps.SetStep;
import com.arcadedb.query.opencypher.executor.steps.ShortestPathStep;
import com.arcadedb.query.opencypher.executor.steps.SkipStep;
import com.arcadedb.query.opencypher.executor.steps.SubqueryStep;
import com.arcadedb.query.opencypher.executor.steps.TypeCountStep;
import com.arcadedb.query.opencypher.executor.steps.UnionStep;
import com.arcadedb.query.opencypher.executor.steps.UnwindStep;
import com.arcadedb.query.opencypher.executor.steps.VariableProjectionStep;
import com.arcadedb.query.opencypher.executor.steps.WithStep;
import com.arcadedb.query.opencypher.executor.steps.ZeroLengthPathStep;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.ExplainResultSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Execution plan for a Cypher query.
 * Contains the chain of execution steps and executes them.
 * <p>
 * Phase 4: Enhanced with Cost-Based Query Optimizer support.
 */
public class CypherExecutionPlan {
  private final DatabaseInternal     database;
  private final CypherStatement      statement;
  private final Map<String, Object>  parameters;
  private final ContextConfiguration configuration;
  private final PhysicalPlan         physicalPlan;
  private final ExpressionEvaluator  expressionEvaluator;

  // Query-level counter for unique anonymous variable names across MATCH clauses
  private int anonymousVarCounter = 0;

  // UNION support
  private final List<CypherExecutionPlan> unionSubqueryPlans;
  private final boolean                   unionRemoveDuplicates;

  /**
   * Constructor for backward compatibility (without optimizer, without evaluator).
   */
  public CypherExecutionPlan(final DatabaseInternal database, final CypherStatement statement,
      final Map<String, Object> parameters, final ContextConfiguration configuration) {
    this(database, statement, parameters, configuration, null, null);
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
    this(database, statement, parameters, configuration, physicalPlan, null);
  }

  /**
   * Full constructor with physical plan and expression evaluator.
   *
   * @param database            database instance
   * @param statement           parsed Cypher statement
   * @param parameters          query parameters
   * @param configuration       context configuration
   * @param physicalPlan        optional optimized physical plan (null for non-optimized)
   * @param expressionEvaluator shared expression evaluator (stateless and thread-safe)
   */
  public CypherExecutionPlan(final DatabaseInternal database, final CypherStatement statement,
      final Map<String, Object> parameters, final ContextConfiguration configuration,
      final PhysicalPlan physicalPlan, final ExpressionEvaluator expressionEvaluator) {
    this(database, statement, parameters, configuration, physicalPlan, expressionEvaluator, null, false);
  }

  /**
   * Constructor for UNION queries.
   *
   * @param database              database instance
   * @param statement             parsed Cypher statement (UnionStatement)
   * @param parameters            query parameters
   * @param configuration         context configuration
   * @param physicalPlan          optional optimized physical plan (null for UNION)
   * @param expressionEvaluator   shared expression evaluator
   * @param unionSubqueryPlans    execution plans for each subquery in the UNION
   * @param unionRemoveDuplicates true for UNION (dedup), false for UNION ALL
   */
  public CypherExecutionPlan(final DatabaseInternal database, final CypherStatement statement,
      final Map<String, Object> parameters, final ContextConfiguration configuration,
      final PhysicalPlan physicalPlan, final ExpressionEvaluator expressionEvaluator,
      final List<CypherExecutionPlan> unionSubqueryPlans, final boolean unionRemoveDuplicates) {
    this.database = database;
    this.statement = statement;
    this.parameters = parameters;
    this.configuration = configuration;
    this.physicalPlan = physicalPlan;
    this.expressionEvaluator = expressionEvaluator;
    this.unionSubqueryPlans = unionSubqueryPlans;
    this.unionRemoveDuplicates = unionRemoveDuplicates;
  }

  /**
   * Executes the query plan and returns results.
   * Phase 4: Uses optimized physical plan when available, falls back to step chain otherwise.
   *
   * @return result set
   */
  public ResultSet execute() {
    // Handle UNION queries specially
    if (unionSubqueryPlans != null && !unionSubqueryPlans.isEmpty())
      return executeUnion();

    // Build execution context
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    setupFunctionResolver(context);

    AbstractExecutionStep rootStep;

    // FAST PATH: Specialized count-push-down optimizations.
    // Must be checked BEFORE the optimizer dispatch, because the optimizer produces
    // GAVExpandAll operators that still materialize individual rows (O(paths) memory).
    rootStep = tryOptimizeCountStar(context);

    if (rootStep == null) {
      // Phase 4: Use optimized physical plan if available
      // Use pre-computed flags from the cached CypherStatement to avoid scanning clause lists per execution
      if (physicalPlan != null && physicalPlan.getRootOperator() != null
          && !statement.hasUnwindBeforeMatch() && !statement.hasSubquery()
          && !statement.hasWithBeforeMatch() && !statement.hasVariableLengthPath()) {
        // Use optimizer - execute physical operators directly
        // Note: For Phase 4, we only optimize MATCH patterns
        // RETURN, ORDER BY, LIMIT are still handled by execution steps
        rootStep = buildExecutionStepsWithOptimizer(context);
      } else {
        // Fall back to non-optimized execution
        // This path correctly handles clause ordering (UNWIND before MATCH), VLP patterns, etc.
        rootStep = buildExecutionSteps(context);
      }
    }

    if (rootStep == null) {
      // No steps to execute - return empty result
      return new IteratorResultSet(new ArrayList<ResultInternal>().iterator());
    }

    // Execute the step chain
    final ResultSet resultSet = rootStep.syncPull(context, 100);

    // IMPORTANT: For write operations, we need to materialize the ResultSet immediately
    // to force execution (since ResultSet is lazy). This is crucial for CREATE/SET/DELETE/MERGE/REMOVE
    // operations to actually execute, even when there's a RETURN clause.
    // Use pre-computed readOnly flag to avoid re-checking on every execution.
    final boolean hasWriteOps = !statement.isReadOnly();

    if (hasWriteOps) {
      // Materialize the ResultSet to force write operation execution
      final List<ResultInternal> materializedResults = new ArrayList<>();
      while (resultSet.hasNext()) {
        materializedResults.add((ResultInternal) resultSet.next());
      }
      // If no RETURN clause, return empty results (write side effects still happened)
      if (statement.getReturnClause() == null)
        return new IteratorResultSet(Collections.<Result>emptyList().iterator());
      // Return the materialized results
      return new IteratorResultSet(materializedResults.iterator());
    }

    return resultSet;
  }

  /**
   * Executes the query plan seeded with an initial input row.
   * Used by CALL subqueries to inject outer scope variables into the inner query.
   * The seed row provides variables that the inner query's WITH clause can import.
   *
   * @param seedRow the initial row providing outer scope variables
   *
   * @return result set from the inner query execution
   */
  public ResultSet executeWithSeedRow(final Result seedRow) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    setupFunctionResolver(context);

    // Create a seed step that returns the seed row
    final AbstractExecutionStep seedStep = new AbstractExecutionStep(context) {
      private boolean consumed = false;

      @Override
      public ResultSet syncPull(final CommandContext ctx, final int nRecords) {
        if (consumed)
          return new IteratorResultSet(List.<ResultInternal>of().iterator());
        consumed = true;
        // Copy the seed row into a ResultInternal
        final ResultInternal seedResult = new ResultInternal();
        for (final String prop : seedRow.getPropertyNames())
          seedResult.setProperty(prop, seedRow.getProperty(prop));
        return new IteratorResultSet(List.of(seedResult).iterator());
      }

      @Override
      public String prettyPrint(final int depth, final int indent) {
        return "  ".repeat(Math.max(0, depth * indent)) + "+ SUBQUERY SEED ROW";
      }
    };

    // Build execution steps with the seed as the initial step
    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    final AbstractExecutionStep rootStep;
    if (clausesInOrder != null && !clausesInOrder.isEmpty())
      rootStep = buildExecutionStepsWithOrder(context, clausesInOrder, seedStep);
    else
      rootStep = seedStep; // Fallback: just return the seed

    if (rootStep == null)
      return new IteratorResultSet(new ArrayList<ResultInternal>().iterator());

    return rootStep.syncPull(context, 100);
  }

  /**
   * Executes a UNION query by combining results from all subqueries.
   *
   * @return combined result set
   */
  private ResultSet executeUnion() {
    // Use UnionStep to combine results from all subqueries
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    setupFunctionResolver(context);

    final UnionStep unionStep =
        new UnionStep(unionSubqueryPlans, unionRemoveDuplicates, context);

    return unionStep.syncPull(context, 100);
  }

  /**
   * Stores a function resolver in the context so that FunctionCallExpression.evaluate()
   * can look up and execute functions when called from compound expressions (AND, OR,
   * CASE, etc.) that evaluate their children directly.
   */
  private void setupFunctionResolver(final BasicCommandContext context) {
    if (expressionEvaluator != null) {
      final CypherFunctionFactory factory = expressionEvaluator.getFunctionFactory();
      context.setVariable(FunctionCallExpression.FUNCTION_RESOLVER_KEY,
          (Function<String, StatelessFunction>) name -> {
            try {
              return factory.getFunctionExecutor(name);
            } catch (final Exception e) {
              return null;
            }
          });
    }
  }

  /**
   * Returns EXPLAIN output showing the query execution plan.
   * Displays physical operators with cost and cardinality estimates.
   * Returns an {@link ExplainResultSet} so the server handler populates the
   * dedicated {@code explain} field in the JSON response.
   *
   * @return result set containing explain output via {@code getExecutionPlan()}
   */
  public ResultSet explain() {
    final StringBuilder explainOutput = new StringBuilder();
    explainOutput.append("OpenCypher Native Execution Plan\n");
    explainOutput.append("=================================\n\n");

    if (physicalPlan != null && physicalPlan.getRootOperator() != null) {
      explainOutput.append("Using Cost-Based Query Optimizer\n\n");
      explainOutput.append("Physical Plan:\n");
      explainOutput.append(physicalPlan.getRootOperator().explain(0));
      explainOutput.append("\n");
      explainOutput.append(String.format("Total Estimated Cost: %.2f\n", physicalPlan.getTotalEstimatedCost()));
      explainOutput.append(String.format("Total Estimated Rows: %d\n", physicalPlan.getTotalEstimatedCardinality()));
    } else {
      explainOutput.append("Using Traditional Execution (Non-Optimized)\n\n");
      explainOutput.append("Reason: Query pattern not yet supported by optimizer\n");
      explainOutput.append("Execution will use step-by-step interpretation\n");
    }

    return new ExplainResultSet(new OpenCypherExplainExecutionPlan(explainOutput.toString()));
  }

  /**
   * Executes the query with profiling enabled.
   * The query is executed to collect real metrics, but only the profiling
   * information is returned (actual query results are discarded).
   * Returns an {@link ExplainResultSet} so the server handler populates the
   * dedicated {@code explain} field in the JSON response.
   *
   * @return result set containing profiling metrics via {@code getExecutionPlan()}
   */
  public ResultSet profile() {
    final long startTime = System.nanoTime();

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    setupFunctionResolver(context);
    context.setProfiling(true);

    final InternalResultSet results = new InternalResultSet();
    String errorMessage = null;
    AbstractExecutionStep rootStep = null;

    try {
      if (unionSubqueryPlans != null && !unionSubqueryPlans.isEmpty()) {
        final UnionStep unionStep =
            new UnionStep(unionSubqueryPlans, unionRemoveDuplicates, context);
        final ResultSet resultSet = unionStep.syncPull(context, Integer.MAX_VALUE);
        while (resultSet.hasNext())
          results.add(resultSet.next());
      } else {
        // FAST PATH: Count-push-down (same logic as execute())
        rootStep = tryOptimizeCountStar(context);

        if (rootStep == null) {
          final boolean hasUnwindBeforeMatch = hasUnwindPrecedingMatch();
          final boolean hasWithBeforeMatch2 = hasWithPrecedingMatch();

          final boolean hasVLP2 = hasVariableLengthPath();
          if (physicalPlan != null && physicalPlan.getRootOperator() != null && !hasUnwindBeforeMatch && !hasWithBeforeMatch2
              && !hasVLP2)
            rootStep = buildExecutionStepsWithOptimizer(context);
          else
            rootStep = buildExecutionSteps(context);
        }

        if (rootStep != null) {
          final ResultSet resultSet = rootStep.syncPull(context, Integer.MAX_VALUE);
          while (resultSet.hasNext())
            results.add(resultSet.next());
        }
      }
    } catch (final Exception e) {
      errorMessage = e.getMessage();
    }

    final long endTime = System.nanoTime();
    final double executionTimeMs = (endTime - startTime) / 1_000_000.0;
    final long rowCount = results.countEntries();

    final StringBuilder profileOutput = new StringBuilder();
    profileOutput.append("OpenCypher Query Profile\n");
    profileOutput.append("========================\n\n");
    if (Boolean.TRUE.equals(context.getVariable(CommandContext.CSR_ACCELERATED_VAR)))
      profileOutput.append("CSR-accelerated via Graph Analytical View\n");
    profileOutput.append(String.format("Execution Time: %.3f ms\n", executionTimeMs));
    profileOutput.append(String.format("Rows Returned: %d\n", rowCount));

    if (errorMessage != null)
      profileOutput.append(String.format("\nError: %s\n", errorMessage));

    if (physicalPlan != null && physicalPlan.getRootOperator() != null) {
      profileOutput.append("\nExecution Plan (Cost-Based Optimizer):\n");
      profileOutput.append(physicalPlan.getRootOperator().explain(0));
      profileOutput.append(String.format("\nEstimated Cost: %.2f\n", physicalPlan.getTotalEstimatedCost()));
      profileOutput.append(String.format("Estimated Rows: %d\n", physicalPlan.getTotalEstimatedCardinality()));
    } else {
      profileOutput.append("\nExecution Plan (Traditional):\n");
      if (rootStep != null) {
        // Collect all steps in the chain from root to first
        final List<AbstractExecutionStep> stepChain = new ArrayList<>();
        AbstractExecutionStep current = rootStep;
        while (current != null) {
          stepChain.add(current);
          current = (AbstractExecutionStep) current.getPrev();
        }
        // Print steps in reverse order (first step first)
        Collections.reverse(stepChain);
        for (final AbstractExecutionStep step : stepChain) {
          profileOutput.append(step.prettyPrint(0, 2));
          profileOutput.append("\n");
        }
      } else
        profileOutput.append("No execution steps generated\n");
    }

    // Collect execution steps for structured plan data
    final List<ExecutionStep> executionSteps = new ArrayList<>();
    if (rootStep != null) {
      AbstractExecutionStep current = rootStep;
      while (current != null) {
        executionSteps.add(current);
        current = (AbstractExecutionStep) current.getPrev();
      }
      Collections.reverse(executionSteps);
    }

    results.setPlan(new OpenCypherExplainExecutionPlan(profileOutput.toString(), executionSteps, endTime - startTime));
    return results;
  }

  /**
   * Builds execution steps using the optimized physical plan.
   * Phase 4: Integrates physical operators with execution steps.
   * <p>
   * Strategy:
   * - Physical operators handle MATCH pattern execution (optimized)
   * - Execution steps handle RETURN, ORDER BY, SKIP, LIMIT (unchanged)
   *
   * @param context command context
   *
   * @return root execution step
   */
  private AbstractExecutionStep buildExecutionStepsWithOptimizer(final CommandContext context) {
    // Get function factory from evaluator for steps that need it
    final CypherFunctionFactory functionFactory = expressionEvaluator != null ?
        expressionEvaluator.getFunctionFactory() : null;

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

    // Apply post-MATCH operations using clausesInOrder to respect the order they appear
    // in the query (e.g. WITH before UNWIND, not the other way around).
    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    if (clausesInOrder != null) {
      for (final ClauseEntry entry : clausesInOrder) {
        switch (entry.getType()) {
        case MATCH: {
          // MATCH pattern is handled by the optimizer above, but WHERE clauses
          // attached to MATCH clauses still need to be applied as filters.
          final MatchClause matchClause = entry.getTypedClause();
          if (matchClause.hasWhereClause()) {
            final FilterPropertiesStep filterStep =
                new FilterPropertiesStep(matchClause.getWhereClause(), context);
            filterStep.setPrevious(currentStep);
            currentStep = filterStep;
          }
          break;
        }

        case CREATE: {
          final CreateClause createClause = entry.getTypedClause();
          if (!createClause.isEmpty()) {
            final CreateStep createStep = new CreateStep(createClause, context, functionFactory);
            createStep.setPrevious(currentStep);
            currentStep = createStep;
          }
          break;
        }

        case SET: {
          final SetClause setClause = entry.getTypedClause();
          if (!setClause.isEmpty()) {
            final SetStep setStep = new SetStep(setClause, context, functionFactory);
            setStep.setPrevious(currentStep);
            currentStep = setStep;
          }
          break;
        }

        case DELETE: {
          final DeleteClause deleteClause = entry.getTypedClause();
          if (!deleteClause.isEmpty()) {
            final DeleteStep deleteStep = new DeleteStep(deleteClause, context);
            deleteStep.setPrevious(currentStep);
            currentStep = deleteStep;
          }
          break;
        }

        case REMOVE: {
          final RemoveClause removeClause = entry.getTypedClause();
          if (!removeClause.isEmpty()) {
            final RemoveStep removeStep = new RemoveStep(removeClause, context);
            removeStep.setPrevious(currentStep);
            currentStep = removeStep;
          }
          break;
        }

        case MERGE: {
          final MergeClause mergeClause = entry.getTypedClause();
          final MergeStep mergeStep = new MergeStep(mergeClause, context, functionFactory);
          mergeStep.setPrevious(currentStep);
          currentStep = mergeStep;
          break;
        }

        case UNWIND: {
          final UnwindClause unwindClause = entry.getTypedClause();
          final UnwindStep unwindStep = new UnwindStep(unwindClause, context, functionFactory);
          unwindStep.setPrevious(currentStep);
          currentStep = unwindStep;
          break;
        }

        case WITH: {
          final WithClause withClause = entry.getTypedClause();
          currentStep = buildWithStepForOptimizer(withClause, currentStep, context, functionFactory);
          break;
        }

        case LOAD_CSV: {
          final LoadCSVClause loadCSVClause = entry.getTypedClause();
          final LoadCSVStep loadCSVStep = new LoadCSVStep(loadCSVClause, context, functionFactory);
          loadCSVStep.setPrevious(currentStep);
          currentStep = loadCSVStep;
          break;
        }

        case FOREACH:
        case SUBQUERY:
        case CALL:
        case RETURN:
          // Handled elsewhere or not applicable here
          break;
        }
      }
    }

    // Statement-level WHERE clause (not scoped to any MATCH clause)
    if (statement.getWhereClause() != null && currentStep != null) {
      final FilterPropertiesStep filterStep = new FilterPropertiesStep(statement.getWhereClause(), context);
      filterStep.setPrevious(currentStep);
      currentStep = filterStep;
    }

    // Step 7: RETURN clause (if any)
    if (statement.getReturnClause() != null) {
      // Try count-edges optimization: MATCH (p)-[:TYPE]->(x) RETURN expr, count(x) AS cnt
      final AbstractExecutionStep countOpt = tryOptimizeMatchCountReturn(
          statement.getClausesInOrder(), statement.getReturnClause(), currentStep, context);
      if (countOpt != null) {
        currentStep = countOpt;
      } else if (statement.getReturnClause().hasAggregations()) {
        // Check if there are also non-aggregated expressions (implicit GROUP BY)
        if (statement.getReturnClause().hasNonAggregations()) {
          // Use GROUP BY aggregation step (implicit grouping)
          final GroupByAggregationStep groupByAggStep =
              new GroupByAggregationStep(
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
        final ProjectReturnStep returnStep = new ProjectReturnStep(statement.getReturnClause(), context,
            functionFactory);
        returnStep.setPrevious(currentStep);
        currentStep = returnStep;
      }
    }

    // Step 8: ORDER BY (if any)
    if (statement.getOrderByClause() != null) {
      // Evaluate LIMIT before creating OrderByStep for Top-K optimization
      // When SKIP is also present, TopK must keep SKIP + LIMIT results so SKIP can discard from them
      Integer limitVal = statement.getLimit() != null ?
          new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getLimit(),
              new ResultInternal(), context) : null;
      if (limitVal != null && statement.getSkip() != null) {
        final int skipVal = new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getSkip(),
            new ResultInternal(), context);
        limitVal = limitVal + skipVal;
      }

      // Top-K must account for SKIP so enough rows survive after skipping
      final Integer skipVal = statement.getSkip() != null ?
          new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getSkip(),
              new ResultInternal(), context) : null;
      final Integer topKVal = limitVal != null ? limitVal + (skipVal != null ? skipVal : 0) : null;

      final OrderByStep orderByStep =
          new OrderByStep(statement.getOrderByClause(), context, functionFactory, topKVal);
      orderByStep.setPrevious(currentStep);
      currentStep = orderByStep;
    }

    // Step 9: SKIP (if any)
    if (statement.getSkip() != null) {
      final SkipStep skipStep =
          new SkipStep(new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getSkip(),
              new ResultInternal(), context), context);
      skipStep.setPrevious(currentStep);
      currentStep = skipStep;
    }

    // Step 10: LIMIT (if any)
    if (statement.getLimit() != null) {
      final int limitVal = new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getLimit(),
          new ResultInternal(), context);
      final LimitStep limitStep = new LimitStep(limitVal, context);
      limitStep.setPrevious(currentStep);
      currentStep = limitStep;
    }

    // Step 11: Final projection - filter to only requested RETURN properties
    // This removes intermediate variables that were needed for ORDER BY but shouldn't be in the final result
    if (statement.getReturnClause() != null && currentStep != null) {
      final FinalProjectionStep finalProjectionStep = new FinalProjectionStep(statement.getReturnClause(), context);
      finalProjectionStep.setPrevious(currentStep);
      currentStep = finalProjectionStep;
    }

    return currentStep;
  }

  /**
   * Checks if the query has UNWIND before MATCH in clause order.
   * This is used to disable the optimizer for such queries because the optimizer
   * doesn't handle clause ordering correctly.
   */
  private boolean hasUnwindPrecedingMatch() {
    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    if (clausesInOrder == null || clausesInOrder.isEmpty()) {
      // Fall back to checking if both UNWIND and MATCH exist
      return !statement.getUnwindClauses().isEmpty() && !statement.getMatchClauses().isEmpty();
    }

    // Find the first UNWIND and first MATCH in clause order
    int firstUnwindOrder = Integer.MAX_VALUE;
    int firstMatchOrder = Integer.MAX_VALUE;

    for (final ClauseEntry entry : clausesInOrder) {
      if (entry.getType() == ClauseEntry.ClauseType.UNWIND) {
        firstUnwindOrder = Math.min(firstUnwindOrder, entry.getOrder());
      } else if (entry.getType() == ClauseEntry.ClauseType.MATCH) {
        firstMatchOrder = Math.min(firstMatchOrder, entry.getOrder());
      }
    }

    // Return true if UNWIND appears before MATCH
    return firstUnwindOrder < firstMatchOrder;
  }

  /**
   * Checks if the query has WITH before MATCH in clause order.
   * The optimizer path processes all MATCH clauses first via the physical plan,
   * which breaks queries like: WITH date(...) AS x MATCH (d:Duration) RETURN x + d.dur
   * because WITH needs to execute before MATCH to provide variables.
   */
  private boolean hasWithPrecedingMatch() {
    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    if (clausesInOrder == null || clausesInOrder.isEmpty())
      return !statement.getWithClauses().isEmpty() && !statement.getMatchClauses().isEmpty();

    int firstWithOrder = Integer.MAX_VALUE;
    int firstMatchOrder = Integer.MAX_VALUE;

    for (final ClauseEntry entry : clausesInOrder) {
      if (entry.getType() == ClauseEntry.ClauseType.WITH)
        firstWithOrder = Math.min(firstWithOrder, entry.getOrder());
      else if (entry.getType() == ClauseEntry.ClauseType.MATCH)
        firstMatchOrder = Math.min(firstMatchOrder, entry.getOrder());
    }

    return firstWithOrder < firstMatchOrder;
  }

  /**
   * Checks if the query contains a CALL subquery clause.
   * The optimizer path doesn't handle SUBQUERY steps, so we fall back to the
   * non-optimized execution path when subqueries are present.
   */
  private boolean hasSubqueryClause() {
    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    if (clausesInOrder == null || clausesInOrder.isEmpty())
      return false;

    for (final ClauseEntry entry : clausesInOrder) {
      if (entry.getType() == ClauseEntry.ClauseType.SUBQUERY)
        return true;
    }
    return false;
  }

  /**
   * Checks if any MATCH clause contains a variable-length path pattern.
   * The optimizer doesn't support VLP (it only uses ExpandAll for fixed-length),
   * so we fall back to the step-based execution path.
   */
  private boolean hasVariableLengthPath() {
    for (final MatchClause matchClause : statement.getMatchClauses()) {
      for (final PathPattern path : matchClause.getPathPatterns()) {
        for (int i = 0; i < path.getRelationshipCount(); i++) {
          if (path.getRelationship(i).isVariableLength())
            return true;
        }
      }
    }
    return false;
  }

  /**
   * Builds the execution step chain from the parsed statement.
   */
  private AbstractExecutionStep buildExecutionSteps(final CommandContext context) {
    // Check if we have clause order information available
    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    if (clausesInOrder != null && !clausesInOrder.isEmpty()) {
      return buildExecutionStepsWithOrder(context, clausesInOrder);
    }
    // Fall back to legacy processing if no order info
    return buildExecutionStepsLegacy(context);
  }

  /**
   * Builds execution steps respecting the order clauses appear in the query.
   * This is essential for queries like UNWIND...MATCH where UNWIND must run first.
   */
  private AbstractExecutionStep buildExecutionStepsWithOrder(final CommandContext context,
      final List<ClauseEntry> clausesInOrder) {
    return buildExecutionStepsWithOrder(context, clausesInOrder, null);
  }

  /**
   * Builds execution steps respecting clause order, optionally seeded with an initial step.
   * When initialStep is provided (e.g., for CALL subqueries), it serves as the starting point
   * of the step chain, providing input rows to the first clause.
   */
  private AbstractExecutionStep buildExecutionStepsWithOrder(final CommandContext context,
      final List<ClauseEntry> clausesInOrder,
      final AbstractExecutionStep initialStep) {
    AbstractExecutionStep currentStep = initialStep;

    // Get function factory from evaluator for steps that need it
    final CypherFunctionFactory functionFactory = expressionEvaluator != null ?
        expressionEvaluator.getFunctionFactory() : null;

    // Track variables bound across MATCH clauses so subsequent MATCHes
    // can detect already-bound variables and avoid Cartesian products
    final Set<String> boundVariables = new HashSet<>();

    // OPTIMIZATION: Check for simple COUNT(*) pattern that can use Type.count() O(1) operation
    // Pattern: MATCH (a:TypeName) RETURN COUNT(a) as alias
    final AbstractExecutionStep typeCountStep = tryCreateTypeCountOptimization(context);
    if (typeCountStep != null)
      return typeCountStep;

    // OPTIMIZATION: Count-push-down for chain/star/triangle/pair-join patterns with RETURN count(*)
    // Instead of materializing all paths (O(paths) memory), propagates counts through
    // CSR arrays level-by-level (O(nodes) memory). Critical for large-fanout chains.
    final AbstractExecutionStep countStep = tryOptimizeCountStar(context);
    if (countStep != null)
      return countStep;

    // Special case: no MATCH as first clause (standalone expressions, WITH before MATCH, etc.)
    // E.g., RETURN abs(-42), WITH collect([0, 0.0]) AS numbers UNWIND ...
    // Skip this when a seed step is provided (e.g., CALL subquery) since the seed provides input
    final boolean firstClauseIsMatch = !clausesInOrder.isEmpty() &&
        clausesInOrder.get(0).getType() == ClauseEntry.ClauseType.MATCH;
    if (initialStep == null && !firstClauseIsMatch) {
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

    // Process clauses in order (indexed loop to support look-ahead for optimizations)
    for (int entryIndex = 0; entryIndex < clausesInOrder.size(); entryIndex++) {
      final ClauseEntry entry = clausesInOrder.get(entryIndex);
      switch (entry.getType()) {
      case UNWIND:
        final UnwindClause unwindClause = entry.getTypedClause();
        final UnwindStep unwindStep =
            new UnwindStep(unwindClause, context, functionFactory);
        if (currentStep != null) {
          unwindStep.setPrevious(currentStep);
        }
        currentStep = unwindStep;
        // Track the UNWIND variable as bound so subsequent MATCH clauses can
        // push down WHERE predicates referencing it (e.g., WHERE a.uid = e.src)
        boundVariables.add(unwindClause.getVariable());
        break;

      case LOAD_CSV:
        final LoadCSVClause loadCSVClause = entry.getTypedClause();
        final LoadCSVStep loadCSVStep =
            new LoadCSVStep(loadCSVClause, context, functionFactory);
        if (currentStep != null) {
          loadCSVStep.setPrevious(currentStep);
        }
        currentStep = loadCSVStep;
        boundVariables.add(loadCSVClause.getVariable());
        break;

      case MATCH:
        final MatchClause matchClause = entry.getTypedClause();
        if (matchClause.isOptional()) {
          // Try chained count optimization first (handles 2 consecutive OPTIONAL MATCH + count)
          final AbstractExecutionStep chainedOptimized = tryOptimizeChainedOptionalMatchCount(
              matchClause, clausesInOrder, entryIndex, currentStep, context, boundVariables);
          if (chainedOptimized != null) {
            currentStep = chainedOptimized;
            entryIndex += 2; // skip both the next OPTIONAL MATCH and the WITH clause
            // Update boundVariables from the WITH clause
            final WithClause nextWith = ((ClauseEntry) clausesInOrder.get(entryIndex)).getTypedClause();
            boundVariables.clear();
            for (final ReturnClause.ReturnItem item : nextWith.getItems()) {
              final String alias = item.getAlias();
              boundVariables.add(alias != null ? alias : item.getExpression().getText());
            }
            break;
          }

          // Try single OPTIONAL MATCH count optimization
          final AbstractExecutionStep optimized = tryOptimizeOptionalMatchCount(
              matchClause, clausesInOrder, entryIndex, currentStep, context, boundVariables);
          if (optimized != null) {
            currentStep = optimized;
            entryIndex++; // skip the WITH clause (already handled)
            // Update boundVariables from the WITH clause
            final WithClause nextWith = ((ClauseEntry) clausesInOrder.get(entryIndex)).getTypedClause();
            boundVariables.clear();
            for (final ReturnClause.ReturnItem item : nextWith.getItems()) {
              final String alias = item.getAlias();
              boundVariables.add(alias != null ? alias : item.getExpression().getText());
            }
            break;
          }
        }
        currentStep = buildMatchStep(matchClause, currentStep, context, boundVariables);
        break;

      case WITH:
        final WithClause withClause = entry.getTypedClause();
        currentStep = buildWithStep(withClause, currentStep, context, functionFactory);
        // WITH resets the scope: only WITH output variables are in scope afterwards
        boundVariables.clear();
        for (final ReturnClause.ReturnItem item : withClause.getItems()) {
          final String alias = item.getAlias();
          boundVariables.add(alias != null ? alias : item.getExpression().getText());
        }
        break;

      case MERGE:
        final MergeClause mergeClause = entry.getTypedClause();
        final MergeStep mergeStep =
            new MergeStep(mergeClause, context, functionFactory);
        if (currentStep != null) {
          mergeStep.setPrevious(currentStep);
        }
        currentStep = mergeStep;
        break;

      case CREATE:
        final CreateClause createClause = entry.getTypedClause();
        if (!createClause.isEmpty()) {
          final CreateStep createStep = new CreateStep(createClause, context, functionFactory);
          if (currentStep != null) {
            createStep.setPrevious(currentStep);
          }
          currentStep = createStep;
        }
        break;

      case SET:
        final SetClause setClause = entry.getTypedClause();
        if (!setClause.isEmpty() && currentStep != null) {
          final SetStep setStep =
              new SetStep(setClause, context, functionFactory);
          setStep.setPrevious(currentStep);
          currentStep = setStep;
        }
        break;

      case REMOVE:
        final RemoveClause removeClause = entry.getTypedClause();
        if (!removeClause.isEmpty() && currentStep != null) {
          final RemoveStep removeStep =
              new RemoveStep(removeClause, context);
          removeStep.setPrevious(currentStep);
          currentStep = removeStep;
        }
        break;

      case DELETE:
        final DeleteClause deleteClause = entry.getTypedClause();
        if (!deleteClause.isEmpty() && currentStep != null) {
          final DeleteStep deleteStep =
              new DeleteStep(deleteClause, context);
          deleteStep.setPrevious(currentStep);
          currentStep = deleteStep;
        }
        break;

      case RETURN:
        // RETURN is handled at the end
        break;

      case CALL:
        final CallClause callClause = entry.getTypedClause();
        final CallStep callStep =
            new CallStep(callClause, context, functionFactory);
        if (currentStep != null) {
          callStep.setPrevious(currentStep);
        }
        // Detect count-only pattern: CALL ... YIELD ... RETURN count(*)
        // When detected, enable fast-path that skips per-row Result object creation
        if (isFollowedByCountOnlyReturn(clausesInOrder, clausesInOrder.indexOf(entry))) {
          callStep.setCountOnlyOptimization(true);
        }
        currentStep = callStep;
        break;

      case FOREACH:
        final ForeachClause foreachClause = entry.getTypedClause();
        final ForeachStep foreachStep =
            new ForeachStep(foreachClause, context, functionFactory);
        if (currentStep != null) {
          foreachStep.setPrevious(currentStep);
        }
        currentStep = foreachStep;
        break;

      case SUBQUERY:
        final SubqueryClause subqueryClause = entry.getTypedClause();
        final SubqueryStep subqueryStep =
            new SubqueryStep(subqueryClause, context, database, parameters, expressionEvaluator);
        if (currentStep != null) {
          subqueryStep.setPrevious(currentStep);
        }
        currentStep = subqueryStep;
        break;
      }
    }

    // Apply statement-level WHERE clause if present
    if (statement.getWhereClause() != null && currentStep != null) {
      final FilterPropertiesStep filterStep = new FilterPropertiesStep(statement.getWhereClause(), context);
      filterStep.setPrevious(currentStep);
      currentStep = filterStep;
    }

    // Process RETURN clause
    if (statement.getReturnClause() != null && currentStep != null) {
      // OPTIMIZATION: try CountEdgesReturnStep to avoid materializing target vertices
      // Only if no statement-level WHERE (which would require filtering before aggregation)
      final AbstractExecutionStep countOpt = statement.getWhereClause() == null
          ? tryOptimizeMatchCountReturn(clausesInOrder, statement.getReturnClause(), currentStep, context)
          : null;
      if (countOpt != null)
        currentStep = countOpt;
      else if (statement.getReturnClause().hasAggregations()) {
        if (statement.getReturnClause().hasNonAggregations()) {
          final GroupByAggregationStep groupByAggStep =
              new GroupByAggregationStep(
                  statement.getReturnClause(), context, functionFactory);
          groupByAggStep.setPrevious(currentStep);
          currentStep = groupByAggStep;
        } else {
          final AggregationStep aggStep = new AggregationStep(statement.getReturnClause(), context, functionFactory);
          aggStep.setPrevious(currentStep);
          currentStep = aggStep;
        }
      } else {
        final ProjectReturnStep returnStep = new ProjectReturnStep(statement.getReturnClause(), context,
            functionFactory);
        returnStep.setPrevious(currentStep);
        currentStep = returnStep;
      }
    }

    // ORDER BY
    if (statement.getOrderByClause() != null && currentStep != null) {
      // Evaluate LIMIT before creating OrderByStep for Top-K optimization
      // When SKIP is also present, TopK must keep SKIP + LIMIT results
      Integer limitVal = statement.getLimit() != null ?
          new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getLimit(),
              new ResultInternal(), context) : null;
      if (limitVal != null && statement.getSkip() != null) {
        final int skipVal = new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getSkip(),
            new ResultInternal(), context);
        limitVal = limitVal + skipVal;
      }

      // Top-K must account for SKIP so enough rows survive after skipping
      final Integer skipVal = statement.getSkip() != null ?
          new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getSkip(),
              new ResultInternal(), context) : null;
      final Integer topKVal = limitVal != null ? limitVal + (skipVal != null ? skipVal : 0) : null;

      final OrderByStep orderByStep =
          new OrderByStep(statement.getOrderByClause(), context, functionFactory, topKVal);
      orderByStep.setPrevious(currentStep);
      currentStep = orderByStep;
    }

    // SKIP
    if (statement.getSkip() != null && currentStep != null) {
      final SkipStep skipStep =
          new SkipStep(new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getSkip(),
              new ResultInternal(), context), context);
      skipStep.setPrevious(currentStep);
      currentStep = skipStep;
    }

    // LIMIT
    if (statement.getLimit() != null && currentStep != null) {
      final Integer limitVal = new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getLimit(),
          new ResultInternal(), context);
      final LimitStep limitStep = new LimitStep(limitVal, context);
      limitStep.setPrevious(currentStep);
      currentStep = limitStep;
    }

    // Final projection
    if (statement.getReturnClause() != null && currentStep != null) {
      final FinalProjectionStep finalProjectionStep = new FinalProjectionStep(statement.getReturnClause(), context);
      finalProjectionStep.setPrevious(currentStep);
      currentStep = finalProjectionStep;
    }

    return currentStep;
  }

  /**
   * Builds execution step for a WITH clause.
   */
  private AbstractExecutionStep buildWithStep(final WithClause withClause,
      AbstractExecutionStep currentStep, final CommandContext context,
      final CypherFunctionFactory functionFactory) {
    if (withClause.hasAggregations()) {
      if (withClause.hasNonAggregations()) {
        final GroupByAggregationStep groupByStep =
            new GroupByAggregationStep(
                new ReturnClause(withClause.getItems(), false),
                context, functionFactory);
        if (currentStep != null) {
          groupByStep.setPrevious(currentStep);
        }
        currentStep = groupByStep;
      } else {
        final AggregationStep aggStep =
            new AggregationStep(
                new ReturnClause(withClause.getItems(), false),
                context, functionFactory);
        if (currentStep != null) {
          aggStep.setPrevious(currentStep);
        }
        currentStep = aggStep;
      }

      // Apply WHERE clause after aggregation (post-aggregation filtering, like SQL HAVING)
      if (withClause.getWhereClause() != null) {
        final FilterPropertiesStep filterStep =
            new FilterPropertiesStep(withClause.getWhereClause(), context);
        filterStep.setPrevious(currentStep);
        currentStep = filterStep;
      }
    } else {
      final WithStep withStep =
          new WithStep(withClause, context, functionFactory);
      if (currentStep != null) {
        withStep.setPrevious(currentStep);
      }
      currentStep = withStep;
    }

    // Apply ORDER BY if present in WITH
    if (withClause.getOrderByClause() != null) {
      // Evaluate LIMIT before creating OrderByStep for Top-K optimization
      // When SKIP is also present, TopK must keep SKIP + LIMIT results
      Integer limitVal = withClause.getLimit() != null ?
          new ExpressionEvaluator(functionFactory).evaluateSkipLimit(withClause.getLimit(),
              new ResultInternal(), context) : null;
      final Integer originalLimitVal = limitVal;
      if (limitVal != null && withClause.getSkip() != null) {
        final int skipVal = new ExpressionEvaluator(functionFactory).evaluateSkipLimit(withClause.getSkip(),
            new ResultInternal(), context);
        limitVal = limitVal + skipVal;
      }

      // Top-K must account for SKIP so enough rows survive after skipping
      final Integer skipVal = withClause.getSkip() != null ?
          new ExpressionEvaluator(functionFactory).evaluateSkipLimit(withClause.getSkip(),
              new ResultInternal(), context) : null;
      final Integer topKVal = limitVal != null ? limitVal + (skipVal != null ? skipVal : 0) : null;

      final OrderByStep orderByStep =
          new OrderByStep(withClause.getOrderByClause(), context, functionFactory, topKVal);
      if (currentStep != null)
        orderByStep.setPrevious(currentStep);
      currentStep = orderByStep;

      // Chain SKIP/LIMIT after ORDER BY so pagination happens after sorting
      if (skipVal != null) {
        final SkipStep skipStep = new SkipStep(skipVal, context);
        skipStep.setPrevious(currentStep);
        currentStep = skipStep;
      }
      if (withClause.getLimit() != null) {
        final LimitStep limitStep = new LimitStep(originalLimitVal, context);
        limitStep.setPrevious(currentStep);
        currentStep = limitStep;
      }

      // Strip non-projected variables that were kept for ORDER BY evaluation
      currentStep = addWithProjection(withClause, currentStep, context);
    }

    return currentStep;
  }

  /**
   * Builds a WITH step for the optimizer path, including GAV fusion attempt for aggregations.
   */
  private AbstractExecutionStep buildWithStepForOptimizer(final WithClause withClause,
      AbstractExecutionStep currentStep, final CommandContext context,
      final CypherFunctionFactory functionFactory) {
    if (withClause.hasAggregations()) {
      if (withClause.hasNonAggregations()) {
        // Try to fuse aggregation into the GAVFusedChainOperator for parallel count(*)
        if (!tryFuseAggregationIntoChain(withClause, currentStep)) {
          // Fallback: GROUP BY aggregation (implicit grouping)
          final GroupByAggregationStep groupByStep =
              new GroupByAggregationStep(
                  new ReturnClause(withClause.getItems(), false),
                  context, functionFactory);
          groupByStep.setPrevious(currentStep);
          currentStep = groupByStep;
        }
      } else {
        // Pure aggregation (no grouping)
        final AggregationStep aggStep =
            new AggregationStep(
                new ReturnClause(withClause.getItems(), false),
                context, functionFactory);
        aggStep.setPrevious(currentStep);
        currentStep = aggStep;
      }

      // Apply WHERE clause after aggregation (post-aggregation filtering, like SQL HAVING)
      if (withClause.getWhereClause() != null) {
        final FilterPropertiesStep filterStep =
            new FilterPropertiesStep(withClause.getWhereClause(), context);
        filterStep.setPrevious(currentStep);
        currentStep = filterStep;
      }
    } else {
      // Regular WITH step (no aggregation)
      final WithStep withStep =
          new WithStep(withClause, context, functionFactory);
      withStep.setPrevious(currentStep);
      currentStep = withStep;
    }

    // Apply ORDER BY if present in WITH
    if (withClause.getOrderByClause() != null) {
      Integer limitVal = withClause.getLimit() != null ?
          new ExpressionEvaluator(functionFactory).evaluateSkipLimit(withClause.getLimit(),
              new ResultInternal(), context) : null;
      final Integer originalLimitVal = limitVal;
      if (limitVal != null && withClause.getSkip() != null) {
        final int skipVal = new ExpressionEvaluator(functionFactory).evaluateSkipLimit(withClause.getSkip(),
            new ResultInternal(), context);
        limitVal = limitVal + skipVal;
      }

      final Integer skipVal = withClause.getSkip() != null ?
          new ExpressionEvaluator(functionFactory).evaluateSkipLimit(withClause.getSkip(),
              new ResultInternal(), context) : null;
      final Integer topKVal = limitVal != null ? limitVal + (skipVal != null ? skipVal : 0) : null;

      final OrderByStep orderByStep =
          new OrderByStep(withClause.getOrderByClause(), context, functionFactory, topKVal);
      orderByStep.setPrevious(currentStep);
      currentStep = orderByStep;

      if (skipVal != null) {
        final SkipStep skipStep = new SkipStep(skipVal, context);
        skipStep.setPrevious(currentStep);
        currentStep = skipStep;
      }
      if (withClause.getLimit() != null) {
        final LimitStep limitStep = new LimitStep(originalLimitVal, context);
        limitStep.setPrevious(currentStep);
        currentStep = limitStep;
      }

      currentStep = addWithProjection(withClause, currentStep, context);
    }

    return currentStep;
  }

  /**
   * Builds execution step for a MATCH clause.
   * Backward-compatible overload without bound variable tracking.
   */
  private AbstractExecutionStep buildMatchStep(final MatchClause matchClause, AbstractExecutionStep currentStep,
      final CommandContext context) {
    return buildMatchStep(matchClause, currentStep, context, new HashSet<>());
  }

  /**
   * Builds execution step for a MATCH clause with bound variable tracking.
   *
   * @param matchClause    the MATCH clause to build
   * @param currentStep    current step in the execution chain
   * @param context        command context
   * @param boundVariables set of variable names already bound in previous steps (updated in-place)
   */
  private AbstractExecutionStep buildMatchStep(final MatchClause matchClause, AbstractExecutionStep currentStep,
      final CommandContext context, final Set<String> boundVariables) {
    if (!matchClause.hasPathPatterns()) {
      return currentStep;
    }

    final List<PathPattern> pathPatterns = matchClause.getPathPatterns();
    final AbstractExecutionStep stepBeforeMatch = currentStep;
    final Set<String> matchVariables = new HashSet<>();
    final boolean isOptional = matchClause.isOptional();

    // Extract ID filters from WHERE clause (if present) for pushdown optimization
    final WhereClause whereClause = matchClause.hasWhereClause() ? matchClause.getWhereClause() :
        statement.getWhereClause();

    AbstractExecutionStep matchChainStart = null;

    for (int patternIndex = 0; patternIndex < pathPatterns.size(); patternIndex++) {
      final PathPattern pathPattern = pathPatterns.get(patternIndex);

      if (pathPattern.isSingleNode()) {
        final NodePattern nodePattern = pathPattern.getFirstNode();
        final String variable = nodePattern.getVariable() != null ? nodePattern.getVariable() :
            ("  nd" + anonymousVarCounter++);
        matchVariables.add(variable);

        // Check if this variable was already bound in a previous MATCH clause
        if (boundVariables.contains(variable)) {
          // Variable already bound - skip creating a new MatchNodeStep
          // But still handle zero-length named paths
          final String singlePathVar = pathPattern.hasPathVariable() ? pathPattern.getPathVariable() : null;
          if (singlePathVar != null) {
            matchVariables.add(singlePathVar);
            final ZeroLengthPathStep zeroPathStep = new ZeroLengthPathStep(variable, singlePathVar, context);
            zeroPathStep.setPrevious(currentStep);
            currentStep = zeroPathStep;
          }
          continue;
        }

        // OPTIMIZATION: Extract ID filter for this variable to avoid Cartesian product
        final String idFilter = extractIdFilter(whereClause, variable);
        // OPTIMIZATION: Extract WHERE predicates referencing only available variables for pushdown
        final BooleanExpression pushdownFilter = extractPushdownFilter(whereClause, variable,
            boundVariables, matchVariables);
        final MatchNodeStep matchStep = new MatchNodeStep(variable, nodePattern, context, idFilter,
            pushdownFilter);

        if (isOptional) {
          if (matchChainStart == null) {
            matchChainStart = matchStep;
            currentStep = matchStep;
          } else {
            matchStep.setPrevious(currentStep);
            currentStep = matchStep;
          }
        } else {
          if (currentStep != null) {
            matchStep.setPrevious(currentStep);
          }
          currentStep = matchStep;
        }

        // Handle zero-length named paths: p = (n)
        final String singlePathVar = pathPattern.hasPathVariable() ? pathPattern.getPathVariable() : null;
        if (singlePathVar != null) {
          matchVariables.add(singlePathVar);
          final ZeroLengthPathStep zeroPathStep = new ZeroLengthPathStep(variable, singlePathVar, context);
          zeroPathStep.setPrevious(currentStep);
          currentStep = zeroPathStep;
          if (isOptional && matchChainStart == matchStep)
            matchChainStart = zeroPathStep;
        }
      } else if (pathPattern instanceof ShortestPathPattern) {
        // Handle shortestPath or allShortestPaths patterns
        final ShortestPathPattern shortestPathPattern = (ShortestPathPattern) pathPattern;
        final NodePattern sourceNode = pathPattern.getFirstNode();
        final NodePattern targetNode = pathPattern.getLastNode();
        final String sourceVar = sourceNode.getVariable() != null ? sourceNode.getVariable() :
            ("  src" + anonymousVarCounter++);
        final String targetVar = targetNode.getVariable() != null ? targetNode.getVariable() :
            ("  tgt" + anonymousVarCounter++);
        final String pathVariable = pathPattern.hasPathVariable() ? pathPattern.getPathVariable() : null;

        // Track path variable
        if (pathVariable != null) {
          matchVariables.add(pathVariable);
        }

        // For shortestPath, both endpoints must be matched first
        // Check both boundVariables (from previous MATCH clauses) and matchVariables (from earlier
        // patterns in this same MATCH clause) to avoid re-matching already-bound variables

        // Source node matching (if not already bound)
        if (!boundVariables.contains(sourceVar) && !matchVariables.contains(sourceVar)) {
          final String sourceIdFilter = extractIdFilter(whereClause, sourceVar);
          final BooleanExpression sourcePushdown = extractPushdownFilter(whereClause, sourceVar,
              boundVariables, matchVariables);
          final MatchNodeStep sourceStep = new MatchNodeStep(sourceVar, sourceNode, context, sourceIdFilter,
              sourcePushdown);
          if (currentStep != null) {
            sourceStep.setPrevious(currentStep);
          }
          currentStep = sourceStep;
          matchVariables.add(sourceVar); // Track as bound for subsequent patterns
        }

        // Target node matching (if not already bound)
        if (!boundVariables.contains(targetVar) && !matchVariables.contains(targetVar)) {
          final String targetIdFilter = extractIdFilter(whereClause, targetVar);
          final BooleanExpression targetPushdown = extractPushdownFilter(whereClause, targetVar,
              boundVariables, matchVariables);
          final MatchNodeStep targetStep = new MatchNodeStep(targetVar, targetNode, context, targetIdFilter,
              targetPushdown);
          if (currentStep != null) {
            targetStep.setPrevious(currentStep);
          }
          currentStep = targetStep;
          matchVariables.add(targetVar); // Track as bound for subsequent patterns
        }

        // Now add the ShortestPathStep to compute the path
        final ShortestPathStep shortestStep = new ShortestPathStep(sourceVar, targetVar, pathVariable,
            shortestPathPattern, context);
        if (currentStep != null) {
          shortestStep.setPrevious(currentStep);
        }
        currentStep = shortestStep;

        if (isOptional && matchChainStart == null) {
          matchChainStart = shortestStep;
        }
      } else {
        NodePattern sourceNode = pathPattern.getFirstNode();
        String sourceVar = sourceNode.getVariable() != null ? sourceNode.getVariable() :
            ("  src" + anonymousVarCounter++);

        // Check if source node variable is already bound (either from previous MATCH or
        // from earlier patterns in this same MATCH clause)
        boolean sourceAlreadyBound = stepBeforeMatch != null &&
            (boundVariables.contains(sourceVar) || matchVariables.contains(sourceVar));

        // OPTIMIZATION: For single-hop patterns where source is unbound but target IS bound,
        // reverse the traversal direction. Instead of scanning all source vertices and checking
        // if each connects to the bound target (O(N)), start from the bound target and follow
        // edges in the reverse direction (O(degree)).
        // Example: OPTIONAL MATCH (c:Comment)-[:COMMENTED_ON]->(q) where q is bound
        // Without reversal: scan all Comments → check if each connects to q (slow!)
        // With reversal: start from q → follow INCOMING COMMENTED_ON edges (fast!)
        boolean reversed = false;
        if (!sourceAlreadyBound && pathPattern.getRelationshipCount() == 1
            && !pathPattern.getRelationship(0).isVariableLength()) {
          final NodePattern targetNode = pathPattern.getLastNode();
          final String targetVar = targetNode.getVariable();
          if (targetVar != null && stepBeforeMatch != null
              && (boundVariables.contains(targetVar) || matchVariables.contains(targetVar))) {
            // Target IS bound — reverse the traversal
            reversed = true;
            sourceNode = targetNode;
            sourceVar = targetVar;
            sourceAlreadyBound = true;
          }
        }

        if (!sourceAlreadyBound) {
          matchVariables.add(sourceVar);
        }

        // Always create MatchNodeStep even for bound variables - it handles them
        // correctly (uses bound vertex and validates labels/properties)
        final String sourceIdFilter = sourceAlreadyBound ? null : extractIdFilter(whereClause, sourceVar);
        final BooleanExpression sourcePushdown = sourceAlreadyBound ? null :
            extractPushdownFilter(whereClause, sourceVar, boundVariables, matchVariables);
        final MatchNodeStep sourceStep = new MatchNodeStep(sourceVar, sourceNode, context, sourceIdFilter,
            sourcePushdown);

        if (isOptional) {
          if (matchChainStart == null) {
            matchChainStart = sourceStep;
            currentStep = sourceStep;
          } else {
            sourceStep.setPrevious(currentStep);
            currentStep = sourceStep;
          }
        } else {
          if (currentStep != null) {
            sourceStep.setPrevious(currentStep);
          }
          currentStep = sourceStep;
        }

        final String pathVariable = pathPattern.hasPathVariable() ? pathPattern.getPathVariable() : null;
        if (pathVariable != null) {
          matchVariables.add(pathVariable);
        }

        // Handle zero-length named paths: p = (n) with no relationships
        if (pathVariable != null && pathPattern.getRelationshipCount() == 0) {
          final ZeroLengthPathStep zeroPathStep = new ZeroLengthPathStep(sourceVar, pathVariable, context);
          if (isOptional) {
            if (matchChainStart == null) {
              zeroPathStep.setPrevious(currentStep);
              matchChainStart = zeroPathStep;
            } else
              zeroPathStep.setPrevious(currentStep);
          } else
            zeroPathStep.setPrevious(currentStep);
          currentStep = zeroPathStep;
        }

        // Track current source variable through multi-hop patterns
        // For the first hop, use sourceVar; for subsequent hops, use the previous targetVar
        String currentSourceVar = sourceVar;

        // Smart GAV eligibility: for each anonymous hop, check if its edge types overlap
        // with any other hop's types in the same pattern. If disjoint, null relVar enables
        // fast path (GAV/CSR). If overlapping, generate an internal anonymous variable to
        // force edge-loading for cross-hop uniqueness checking.
        final boolean[] hopNeedsEdgeTracking = computeHopEdgeTrackingNeeds(pathPattern);

        for (int i = 0; i < pathPattern.getRelationshipCount(); i++) {
          final RelationshipPattern relPattern = pathPattern.getRelationship(i);
          final NodePattern targetNode = pathPattern.getNode(i + 1);
          // Named edge: keep user variable if actually referenced in the query or if VLP
          // (VLP steps always need the variable for pre-bound path validation).
          // For unreferenced fixed-length edges, treat as anonymous for GAV eligibility.
          // Anonymous edge: null if GAV-eligible, internal var if edge tracking needed.
          final String relVar;
          if (relPattern.getVariable() != null && !relPattern.getVariable().isEmpty()) {
            if (relPattern.isVariableLength() || isEdgeVariableReferenced(relPattern.getVariable()))
              relVar = relPattern.getVariable();
            else
              relVar = hopNeedsEdgeTracking[i] ? ("  rel" + anonymousVarCounter++) : null;
          } else
            relVar = hopNeedsEdgeTracking[i] ? ("  rel" + anonymousVarCounter++) : null;
          String targetVar = targetNode.getVariable() != null ? targetNode.getVariable() :
              ("  tgt" + anonymousVarCounter++);

          // When reversed, swap source/target variables and use the original source as target
          final String effectiveSourceVar;
          final String effectiveTargetVar;
          final NodePattern effectiveTargetNode;
          final Direction directionOverride;
          if (reversed) {
            effectiveSourceVar = currentSourceVar; // already swapped to bound target
            effectiveTargetVar = pathPattern.getFirstNode().getVariable() != null ?
                pathPattern.getFirstNode().getVariable() : targetVar;
            targetVar = effectiveTargetVar;
            effectiveTargetNode = pathPattern.getFirstNode(); // original source becomes target for label filtering
            directionOverride = relPattern.getDirection().reverse();
          } else {
            effectiveSourceVar = currentSourceVar;
            effectiveTargetVar = targetVar;
            effectiveTargetNode = targetNode;
            directionOverride = null;
          }

          // Track relationship and target variables for cross-MATCH uniqueness scoping.
          // Only add variables that are NEW to this MATCH clause — already-bound variables
          // (from previous MATCHes or WITH) should not be treated as new match variables,
          // otherwise OPTIONAL MATCH will incorrectly classify them when setting NULLs.
          if (relVar != null && !boundVariables.contains(relVar))
            matchVariables.add(relVar);
          if (!boundVariables.contains(effectiveTargetVar))
            matchVariables.add(effectiveTargetVar);

          AbstractExecutionStep nextStep;
          if (relPattern.isVariableLength()) {
            nextStep = new ExpandPathStep(effectiveSourceVar, pathVariable, relVar, effectiveTargetVar, relPattern,
                true, effectiveTargetNode, pathPattern.getEffectivePathMode(), context);
          } else {
            // Pass target node pattern for label filtering, bound variables for identity
            // checking, and a snapshot for relationship uniqueness scoping.
            // The snapshot captures only variables from previous steps (via WITH/previous MATCHes).
            // Relationship uniqueness only applies within a single MATCH clause.
            nextStep = new MatchRelationshipStep(effectiveSourceVar, relVar, effectiveTargetVar, relPattern,
                pathVariable, effectiveTargetNode, boundVariables, new HashSet<>(boundVariables),
                directionOverride, context);
          }

          // Update source for next hop in multi-hop patterns
          currentSourceVar = effectiveTargetVar;

          if (isOptional && matchChainStart == null) {
            matchChainStart = nextStep;
            currentStep = nextStep;
          } else {
            nextStep.setPrevious(currentStep);
            currentStep = nextStep;
          }
        }
      }
    }

    // Apply WHERE clause scoped to this MATCH
    if (matchClause.hasWhereClause() && currentStep != null) {
      final FilterPropertiesStep filterStep = new FilterPropertiesStep(matchClause.getWhereClause(), context);

      if (isOptional) {
        filterStep.setPrevious(currentStep);
        currentStep = filterStep;
        if (matchChainStart == null) {
          matchChainStart = filterStep;
        }
      } else {
        filterStep.setPrevious(currentStep);
        currentStep = filterStep;
      }
    }

    // Wrap in OptionalMatchStep if this is an OPTIONAL MATCH
    if (isOptional && matchChainStart != null) {
      final OptionalMatchStep optionalStep =
          new OptionalMatchStep(matchChainStart, currentStep, matchVariables, context);

      if (stepBeforeMatch != null) {
        optionalStep.setPrevious(stepBeforeMatch);
      }
      currentStep = optionalStep;
    }

    // Update bound variables with newly bound variables from this MATCH
    boundVariables.addAll(matchVariables);

    return currentStep;
  }

  /**
   * Legacy method for building execution steps (fixed order).
   * Used when clause order information is not available.
   */
  private AbstractExecutionStep buildExecutionStepsLegacy(final CommandContext context) {
    AbstractExecutionStep currentStep = null;

    // Get function factory from evaluator for steps that need it
    final CypherFunctionFactory functionFactory = expressionEvaluator != null ?
        expressionEvaluator.getFunctionFactory() : null;

    // OPTIMIZATION: Check for simple COUNT(*) pattern that can use Type.count() O(1) operation
    // Pattern: MATCH (a:TypeName) RETURN COUNT(a) as alias
    final AbstractExecutionStep typeCountStep = tryCreateTypeCountOptimization(context);
    if (typeCountStep != null)
      return typeCountStep;

    // OPTIMIZATION: Count-push-down for chain/star/triangle/pair-join patterns with RETURN count(*)
    final AbstractExecutionStep countStep = tryOptimizeCountStar(context);
    if (countStep != null)
      return countStep;

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

    // Track variables bound across MATCH clauses so subsequent MATCHes
    // can detect already-bound variables and avoid Cartesian products
    final Set<String> legacyBoundVariables = new HashSet<>();

    // Step 1: MATCH clauses - fetch nodes
    // Process ALL MATCH clauses (not just the first)
    if (!statement.getMatchClauses().isEmpty()) {
      for (final MatchClause matchClause : statement.getMatchClauses()) {
        if (matchClause.hasPathPatterns()) {
          // Phase 2+: Use parsed path patterns
          final List<PathPattern> pathPatterns = matchClause.getPathPatterns();

          // Track the step before this MATCH clause for OPTIONAL MATCH wrapping
          final AbstractExecutionStep stepBeforeMatch = currentStep;
          final Set<String> matchVariables = new HashSet<>();
          final boolean isOptional = matchClause.isOptional();

          // For optional match, we build the match chain separately (not chained to stepBeforeMatch)
          // Then wrap it in OptionalMatchStep which manages the input
          AbstractExecutionStep matchChainStart = null;

          // Process all comma-separated patterns in the MATCH clause
          for (int patternIndex = 0; patternIndex < pathPatterns.size(); patternIndex++) {
            final PathPattern pathPattern = pathPatterns.get(patternIndex);

            if (pathPattern instanceof ShortestPathPattern) {
              // Handle shortestPath or allShortestPaths patterns in legacy path
              final ShortestPathPattern shortestPathPattern = (ShortestPathPattern) pathPattern;
              final NodePattern sourceNode = pathPattern.getFirstNode();
              final NodePattern targetNode = pathPattern.getLastNode();
              final String sourceVar = sourceNode.getVariable() != null ? sourceNode.getVariable() :
                  ("  src" + anonymousVarCounter++);
              final String targetVar = targetNode.getVariable() != null ? targetNode.getVariable() :
                  ("  tgt" + anonymousVarCounter++);
              final String pathVariable = pathPattern.hasPathVariable() ? pathPattern.getPathVariable() : null;

              // Track path variable
              if (pathVariable != null) {
                matchVariables.add(pathVariable);
              }

              // Check both legacyBoundVariables (from previous MATCH clauses) and matchVariables (from earlier
              // patterns in this same MATCH clause) to avoid re-matching already-bound variables

              // Source node matching (if not already bound)
              if (!legacyBoundVariables.contains(sourceVar) && !matchVariables.contains(sourceVar)) {
                final WhereClause matchWhere = matchClause.hasWhereClause() ? matchClause.getWhereClause() :
                    statement.getWhereClause();
                final String sourceIdFilter = extractIdFilter(matchWhere, sourceVar);
                final BooleanExpression sourcePushdown = extractPushdownFilter(matchWhere, sourceVar,
                    legacyBoundVariables, matchVariables);
                final MatchNodeStep sourceStep = new MatchNodeStep(sourceVar, sourceNode, context, sourceIdFilter,
                    sourcePushdown);
                if (currentStep != null) {
                  sourceStep.setPrevious(currentStep);
                }
                currentStep = sourceStep;
                matchVariables.add(sourceVar);
              }

              // Target node matching (if not already bound)
              if (!legacyBoundVariables.contains(targetVar) && !matchVariables.contains(targetVar)) {
                final WhereClause matchWhere = matchClause.hasWhereClause() ? matchClause.getWhereClause() :
                    statement.getWhereClause();
                final String targetIdFilter = extractIdFilter(matchWhere, targetVar);
                final BooleanExpression targetPushdown = extractPushdownFilter(matchWhere, targetVar,
                    legacyBoundVariables, matchVariables);
                final MatchNodeStep targetStep = new MatchNodeStep(targetVar, targetNode, context, targetIdFilter,
                    targetPushdown);
                if (currentStep != null) {
                  targetStep.setPrevious(currentStep);
                }
                currentStep = targetStep;
                matchVariables.add(targetVar);
              }

              // Now add the ShortestPathStep to compute the path
              final ShortestPathStep shortestStep = new ShortestPathStep(sourceVar, targetVar, pathVariable,
                  shortestPathPattern, context);
              if (currentStep != null) {
                shortestStep.setPrevious(currentStep);
              }
              currentStep = shortestStep;

              if (isOptional && matchChainStart == null) {
                matchChainStart = shortestStep;
              }
            } else if (pathPattern.isSingleNode()) {
              // Simple node pattern: MATCH (n:Person) or MATCH (a), (b)
              final NodePattern nodePattern = pathPattern.getFirstNode();
              final String variable = nodePattern.getVariable() != null ? nodePattern.getVariable() :
                  ("  nd" + anonymousVarCounter++);
              matchVariables.add(variable); // Track variable for OPTIONAL MATCH

              // Check if this variable was already bound in a previous MATCH clause
              if (legacyBoundVariables.contains(variable)) {
                // Variable already bound - skip creating a new MatchNodeStep
                continue;
              }

              // OPTIMIZATION: Extract ID filter from WHERE clause (if present) for pushdown
              final WhereClause matchWhere = matchClause.hasWhereClause() ? matchClause.getWhereClause() :
                  statement.getWhereClause();
              final String idFilter = extractIdFilter(matchWhere, variable);
              // OPTIMIZATION: Extract WHERE predicates for inline pushdown
              final BooleanExpression pushdownFilter = extractPushdownFilter(matchWhere, variable,
                  legacyBoundVariables, matchVariables);
              final MatchNodeStep matchStep = new MatchNodeStep(variable, nodePattern, context, idFilter,
                  pushdownFilter);

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
              final String sourceVar = sourceNode.getVariable() != null ? sourceNode.getVariable() :
                  ("  src" + anonymousVarCounter++);

              // Check if source node is already bound (for multiple MATCH clauses or OPTIONAL MATCH)
              final boolean sourceAlreadyBound = stepBeforeMatch != null &&
                  (legacyBoundVariables.contains(sourceVar) || matchVariables.contains(sourceVar));

              if (!sourceAlreadyBound) {
                // Only track the source variable if we're creating a new binding for it
                matchVariables.add(sourceVar);

                // OPTIMIZATION: Extract ID filter from WHERE clause (if present) for pushdown
                final WhereClause matchWhere = matchClause.hasWhereClause() ? matchClause.getWhereClause() :
                    statement.getWhereClause();
                final String sourceIdFilter = extractIdFilter(matchWhere, sourceVar);
                // OPTIMIZATION: Extract WHERE predicates for inline pushdown
                final BooleanExpression sourcePushdown = extractPushdownFilter(matchWhere, sourceVar,
                    legacyBoundVariables, matchVariables);

                // Start with source node (or chain if we have previous patterns)
                final MatchNodeStep sourceStep = new MatchNodeStep(sourceVar, sourceNode, context, sourceIdFilter,
                    sourcePushdown);

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

              // Handle zero-length named paths: p = (n) with no relationships
              if (pathVariable != null && pathPattern.getRelationshipCount() == 0) {
                final ZeroLengthPathStep zeroPathStep = new ZeroLengthPathStep(sourceVar, pathVariable, context);
                if (isOptional) {
                  if (matchChainStart == null) {
                    zeroPathStep.setPrevious(currentStep);
                    matchChainStart = zeroPathStep;
                  } else
                    zeroPathStep.setPrevious(currentStep);
                } else
                  zeroPathStep.setPrevious(currentStep);
                currentStep = zeroPathStep;
              }

              // Track current source variable through multi-hop patterns
              // For the first hop, use sourceVar; for subsequent hops, use the previous targetVar
              String currentSourceVar = sourceVar;

              // Smart GAV eligibility: same analysis as ordered path
              final boolean[] hopNeedsEdgeTrackingLegacy = computeHopEdgeTrackingNeeds(pathPattern);

              for (int i = 0; i < pathPattern.getRelationshipCount(); i++) {
                final RelationshipPattern relPattern = pathPattern.getRelationship(i);
                final NodePattern targetNode = pathPattern.getNode(i + 1);
                final String relVar;
                if (relPattern.getVariable() != null && !relPattern.getVariable().isEmpty()) {
                  if (relPattern.isVariableLength() || isEdgeVariableReferenced(relPattern.getVariable()))
                    relVar = relPattern.getVariable();
                  else
                    relVar = hopNeedsEdgeTrackingLegacy[i] ? ("  rel" + anonymousVarCounter++) : null;
                } else
                  relVar = hopNeedsEdgeTrackingLegacy[i] ? ("  rel" + anonymousVarCounter++) : null;
                final String targetVar = targetNode.getVariable() != null ? targetNode.getVariable() :
                    ("  tgt" + anonymousVarCounter++);

                // Track relationship and target variables for cross-MATCH uniqueness scoping
                if (relVar != null)
                  matchVariables.add(relVar);
                matchVariables.add(targetVar);

                AbstractExecutionStep nextStep;
                if (relPattern.isVariableLength()) {
                  // Variable-length path - pass path variable, relationship variable, and target node for label
                  // filtering
                  nextStep = new ExpandPathStep(currentSourceVar, pathVariable, relVar, targetVar, relPattern, true,
                      targetNode, pathPattern.getEffectivePathMode(), context);
                } else {
                  // Fixed-length relationship - pass path variable, target node pattern, and bound variables
                  nextStep = new MatchRelationshipStep(currentSourceVar, relVar, targetVar, relPattern, pathVariable,
                      targetNode, legacyBoundVariables, context);
                }

                // Update source for next hop in multi-hop patterns
                currentSourceVar = targetVar;

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
            // Pass matchChainStart (first step) for feeding input and currentStep (last step)
            // for pulling results through the entire chain including any filter steps
            final OptionalMatchStep optionalStep =
                new OptionalMatchStep(matchChainStart, currentStep, matchVariables, context);

            // OptionalMatchStep pulls from stepBeforeMatch
            if (stepBeforeMatch != null) {
              optionalStep.setPrevious(stepBeforeMatch);
            }

            // The output of OptionalMatchStep becomes currentStep
            currentStep = optionalStep;
          }

          // Update bound variables with newly bound variables from this MATCH
          legacyBoundVariables.addAll(matchVariables);
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
    for (final UnwindClause unwindClause : statement.getUnwindClauses()) {
      final UnwindStep unwindStep =
          new UnwindStep(unwindClause, context, functionFactory);
      if (currentStep != null) {
        unwindStep.setPrevious(currentStep);
      }
      // else: Standalone UNWIND (no previous step)
      currentStep = unwindStep;
    }

    // Step 2.6: WITH clauses - project and transform results (can be chained)
    for (final WithClause withClause : statement.getWithClauses()) {
      // Handle aggregations in WITH clause
      if (withClause.hasAggregations()) {
        if (withClause.hasNonAggregations()) {
          // GROUP BY aggregation (implicit grouping)
          final GroupByAggregationStep groupByStep =
              new GroupByAggregationStep(
                  new ReturnClause(withClause.getItems(), false),
                  context, functionFactory);
          if (currentStep != null) {
            groupByStep.setPrevious(currentStep);
          }
          currentStep = groupByStep;
        } else {
          // Pure aggregation (no grouping)
          final AggregationStep aggStep =
              new AggregationStep(
                  new ReturnClause(withClause.getItems(), false),
                  context, functionFactory);
          if (currentStep != null) {
            aggStep.setPrevious(currentStep);
          }
          currentStep = aggStep;
        }

        // Apply WHERE clause after aggregation (post-aggregation filtering, like SQL HAVING)
        if (withClause.getWhereClause() != null) {
          final FilterPropertiesStep filterStep =
              new FilterPropertiesStep(withClause.getWhereClause(), context);
          if (currentStep != null) {
            filterStep.setPrevious(currentStep);
          }
          currentStep = filterStep;
        }
      } else {
        // Regular WITH step (no aggregation)
        final WithStep withStep =
            new WithStep(withClause, context, functionFactory);
        if (currentStep != null) {
          withStep.setPrevious(currentStep);
        }
        currentStep = withStep;
      }

      // Apply ORDER BY if present in WITH
      if (withClause.getOrderByClause() != null) {
        // Evaluate LIMIT before creating OrderByStep for Top-K optimization
        // When SKIP is also present, TopK must keep SKIP + LIMIT results
        Integer limitVal = withClause.getLimit() != null ?
            new ExpressionEvaluator(functionFactory).evaluateSkipLimit(withClause.getLimit(),
                new ResultInternal(), context) : null;
        final Integer originalLimitVal = limitVal;
        if (limitVal != null && withClause.getSkip() != null) {
          final int skipVal = new ExpressionEvaluator(functionFactory).evaluateSkipLimit(withClause.getSkip(),
              new ResultInternal(), context);
          limitVal = limitVal + skipVal;
        }

        // Top-K must account for SKIP so enough rows survive after skipping
        final Integer skipVal = withClause.getSkip() != null ?
            new ExpressionEvaluator(functionFactory).evaluateSkipLimit(withClause.getSkip(),
                new ResultInternal(), context) : null;
        final Integer topKVal = limitVal != null ? limitVal + (skipVal != null ? skipVal : 0) : null;

        final OrderByStep orderByStep =
            new OrderByStep(withClause.getOrderByClause(), context, functionFactory, topKVal);
        if (currentStep != null)
          orderByStep.setPrevious(currentStep);
        currentStep = orderByStep;

        // Chain SKIP/LIMIT after ORDER BY so pagination happens after sorting
        if (skipVal != null) {
          final SkipStep skipStep = new SkipStep(skipVal, context);
          skipStep.setPrevious(currentStep);
          currentStep = skipStep;
        }
        if (withClause.getLimit() != null) {
          final LimitStep limitStep = new LimitStep(originalLimitVal, context);
          limitStep.setPrevious(currentStep);
          currentStep = limitStep;
        }

        // Strip non-projected variables that were kept for ORDER BY evaluation
        currentStep = addWithProjection(withClause, currentStep, context);
      }
    }

    // Step 3: MERGE clause - find or create pattern
    if (statement.getMergeClause() != null) {
      final MergeStep mergeStep = new MergeStep(
          statement.getMergeClause(), context, functionFactory);
      // MERGE is typically standalone, but can be chained
      if (currentStep != null) {
        mergeStep.setPrevious(currentStep);
      }
      currentStep = mergeStep;
    }

    // Step 4: CREATE clause - create vertices/edges
    if (statement.getCreateClause() != null && !statement.getCreateClause().isEmpty()) {
      final CreateStep createStep = new CreateStep(statement.getCreateClause(), context, functionFactory);
      if (currentStep != null) {
        // Chained CREATE (after MATCH/WHERE)
        createStep.setPrevious(currentStep);
      }
      // else: Standalone CREATE (no previous step)
      currentStep = createStep;
    }

    // Step 5: SET clause - update properties
    if (statement.getSetClause() != null && !statement.getSetClause().isEmpty() && currentStep != null) {
      final SetStep setStep = new SetStep(
          statement.getSetClause(), context, functionFactory);
      setStep.setPrevious(currentStep);
      currentStep = setStep;
    }

    // Step 6: DELETE clause - delete vertices/edges
    if (statement.getDeleteClause() != null && !statement.getDeleteClause().isEmpty() && currentStep != null) {
      final DeleteStep deleteStep = new DeleteStep(
          statement.getDeleteClause(), context);
      deleteStep.setPrevious(currentStep);
      currentStep = deleteStep;
    }

    // Step 6a: REMOVE clauses - remove properties
    for (final RemoveClause removeClause : statement.getRemoveClauses()) {
      if (!removeClause.isEmpty() && currentStep != null) {
        final RemoveStep removeStep = new RemoveStep(removeClause, context);
        removeStep.setPrevious(currentStep);
        currentStep = removeStep;
      }
    }

    // Step 7: RETURN clause - project results or aggregate
    if (statement.getReturnClause() != null && currentStep != null) {
      // Try count-edges optimization: MATCH (p)-[:TYPE]->(x) RETURN expr, count(x) AS cnt
      final AbstractExecutionStep countOpt = tryOptimizeMatchCountReturn(
          statement.getClausesInOrder(), statement.getReturnClause(), currentStep, context);
      if (countOpt != null) {
        currentStep = countOpt;
      } else if (statement.getReturnClause().hasAggregations()) {
        // Check if there are also non-aggregated expressions (implicit GROUP BY)
        if (statement.getReturnClause().hasNonAggregations()) {
          // Use GROUP BY aggregation step (implicit grouping)
          final GroupByAggregationStep groupByAggStep =
              new GroupByAggregationStep(
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
        final ProjectReturnStep returnStep = new ProjectReturnStep(statement.getReturnClause(), context,
            functionFactory);
        returnStep.setPrevious(currentStep);
        currentStep = returnStep;
      }
    }

    // Step 8: ORDER BY clause - sort results
    if (statement.getOrderByClause() != null && currentStep != null) {
      // Evaluate LIMIT before creating OrderByStep for Top-K optimization
      // When SKIP is also present, TopK must keep SKIP + LIMIT results
      Integer limitVal = statement.getLimit() != null ?
          new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getLimit(), new ResultInternal(),
              context) : null;
      if (limitVal != null && statement.getSkip() != null) {
        final int skipVal = new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getSkip(),
            new ResultInternal(), context);
        limitVal = limitVal + skipVal;
      }

      // Top-K must account for SKIP so enough rows survive after skipping
      final Integer skipVal = statement.getSkip() != null ?
          new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getSkip(), new ResultInternal(),
              context) : null;
      final Integer topKVal = limitVal != null ? limitVal + (skipVal != null ? skipVal : 0) : null;

      final OrderByStep orderByStep = new OrderByStep(
          statement.getOrderByClause(), context, functionFactory, topKVal);
      orderByStep.setPrevious(currentStep);
      currentStep = orderByStep;
    }

    // Step 9: SKIP clause - skip first N results
    if (statement.getSkip() != null && currentStep != null) {
      final SkipStep skipStep = new SkipStep(
          new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getSkip(), new ResultInternal(),
              context), context);
      skipStep.setPrevious(currentStep);
      currentStep = skipStep;
    }

    // Step 10: LIMIT clause - limit number of results
    if (statement.getLimit() != null && currentStep != null) {
      final Integer limitVal = new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getLimit(),
          new ResultInternal(),
          context);
      final LimitStep limitStep = new LimitStep(limitVal, context);
      limitStep.setPrevious(currentStep);
      currentStep = limitStep;
    }

    // Step 11: Final projection - filter to only requested RETURN properties
    // This removes intermediate variables that were needed for ORDER BY but shouldn't be in the final result
    if (statement.getReturnClause() != null && currentStep != null) {
      final FinalProjectionStep finalProjectionStep = new FinalProjectionStep(statement.getReturnClause(), context);
      finalProjectionStep.setPrevious(currentStep);
      currentStep = finalProjectionStep;
    }

    return currentStep;
  }

  /**
   * Returns the physical plan for this execution plan (if optimizer was used).
   * Used by plan cache to store optimized plans for reuse.
   *
   * @return the physical plan, or null if optimizer was not used
   */
  public PhysicalPlan getPhysicalPlan() {
    return physicalPlan;
  }

  /**
   * Attempts to optimize chained OPTIONAL MATCH + count() pattern.
   * <p>
   * Detects pattern:
   * OPTIONAL MATCH (bound)-[r1:TYPE1]->(intermediate)
   * OPTIONAL MATCH (target)-[r2:TYPE2]->(intermediate)
   * WITH bound, count(target) AS cnt
   * <p>
   * Uses vertex.getVertices() for first hop + vertex.countEdges() for second hop,
   * avoiding materialization of target vertices.
   *
   * @return optimized CountChainedEdgesStep if pattern matches, null otherwise
   */
  private AbstractExecutionStep tryOptimizeChainedOptionalMatchCount(final MatchClause firstMatch,
      final List<ClauseEntry> clausesInOrder,
      final int currentIndex,
      final AbstractExecutionStep currentStep,
      final CommandContext context,
      final Set<String> boundVariables) {
    // 1. First OPTIONAL MATCH must have exactly one path pattern (single hop)
    if (!firstMatch.hasPathPatterns() || firstMatch.getPathPatterns().size() != 1)
      return null;

    final PathPattern firstPattern = firstMatch.getPathPatterns().get(0);
    if (firstPattern.getRelationshipCount() != 1)
      return null;

    final RelationshipPattern firstRel = firstPattern.getRelationship(0);
    if (firstRel.isVariableLength())
      return null;

    // No property constraints
    if (firstRel.getProperties() != null && !firstRel.getProperties().isEmpty())
      return null;

    // No WHERE clause
    if (firstMatch.hasWhereClause())
      return null;

    // 2. Next clause must be another OPTIONAL MATCH
    if (currentIndex + 1 >= clausesInOrder.size())
      return null;

    final ClauseEntry secondEntry = clausesInOrder.get(currentIndex + 1);
    if (secondEntry.getType() != ClauseEntry.ClauseType.MATCH)
      return null;

    final MatchClause secondMatch = secondEntry.getTypedClause();
    if (!secondMatch.isOptional())
      return null;

    // Second OPTIONAL MATCH must also have exactly one path pattern (single hop)
    if (!secondMatch.hasPathPatterns() || secondMatch.getPathPatterns().size() != 1)
      return null;

    final PathPattern secondPattern = secondMatch.getPathPatterns().get(0);
    if (secondPattern.getRelationshipCount() != 1)
      return null;

    final RelationshipPattern secondRel = secondPattern.getRelationship(0);
    if (secondRel.isVariableLength())
      return null;

    // No property constraints
    if (secondRel.getProperties() != null && !secondRel.getProperties().isEmpty())
      return null;

    // No WHERE clause
    if (secondMatch.hasWhereClause())
      return null;

    // 3. After second OPTIONAL MATCH must be a WITH clause
    if (currentIndex + 2 >= clausesInOrder.size())
      return null;

    final ClauseEntry withEntry = clausesInOrder.get(currentIndex + 2);
    if (withEntry.getType() != ClauseEntry.ClauseType.WITH)
      return null;

    final WithClause withClause = withEntry.getTypedClause();

    // WITH must have aggregations + non-aggregations
    if (!withClause.hasAggregations() || !withClause.hasNonAggregations())
      return null;

    // WITH must not have ORDER BY, SKIP, LIMIT, WHERE
    if (withClause.getOrderByClause() != null || withClause.getSkip() != null
        || withClause.getLimit() != null || withClause.getWhereClause() != null)
      return null;

    // 4. Analyze the pattern structure
    // First pattern: (node1)-[r1]->(node2)
    final NodePattern firstNode1 = firstPattern.getFirstNode();
    final NodePattern firstNode2 = firstPattern.getLastNode();
    final String firstVar1 = firstNode1.getVariable();
    final String firstVar2 = firstNode2.getVariable();

    if (firstVar1 == null || firstVar2 == null)
      return null;

    // Check node patterns don't have property constraints
    if (firstNode1.getProperties() != null && !firstNode1.getProperties().isEmpty())
      return null;
    if (firstNode2.getProperties() != null && !firstNode2.getProperties().isEmpty())
      return null;

    // Second pattern: (node3)-[r2]->(node4)
    final NodePattern secondNode1 = secondPattern.getFirstNode();
    final NodePattern secondNode2 = secondPattern.getLastNode();
    final String secondVar1 = secondNode1.getVariable();
    final String secondVar2 = secondNode2.getVariable();

    if (secondVar1 == null || secondVar2 == null)
      return null;

    // Check node patterns don't have property constraints
    if (secondNode1.getProperties() != null && !secondNode1.getProperties().isEmpty())
      return null;
    if (secondNode2.getProperties() != null && !secondNode2.getProperties().isEmpty())
      return null;

    // 5. Determine the pattern structure
    // We need: (bound)-[r1]->(intermediate) and (target)-[r2]->(intermediate)
    // where bound is already in boundVariables and intermediate is the shared node

    // Check which variable is bound
    final String boundVar;
    final String intermediateVar;
    final boolean firstVarIsBound = boundVariables.contains(firstVar1);
    final boolean secondVarIsBound = boundVariables.contains(firstVar2);

    if (firstVarIsBound && !secondVarIsBound) {
      boundVar = firstVar1;
      intermediateVar = firstVar2;
    } else if (secondVarIsBound && !firstVarIsBound) {
      boundVar = firstVar2;
      intermediateVar = firstVar1;
    } else {
      return null; // Both bound or neither bound
    }

    // The intermediate variable must appear in the second pattern
    final String targetVar;
    if (secondVar1.equals(intermediateVar)) {
      targetVar = secondVar2;
    } else if (secondVar2.equals(intermediateVar)) {
      targetVar = secondVar1;
    } else {
      return null; // Patterns don't share a variable
    }

    // 6. Analyze the WITH clause
    final List<ReturnClause.ReturnItem> groupingKeys = new ArrayList<>();
    FunctionCallExpression countExpr = null;
    String countAlias = null;
    int aggregationCount = 0;

    for (final ReturnClause.ReturnItem item : withClause.getItems()) {
      if (item.getExpression().containsAggregation()) {
        aggregationCount++;
        if (!(item.getExpression() instanceof FunctionCallExpression))
          return null;
        final FunctionCallExpression funcExpr = (FunctionCallExpression) item.getExpression();
        if (!"count".equals(funcExpr.getFunctionName()))
          return null;
        if (funcExpr.isDistinct())
          return null;
        if (funcExpr.getArguments().size() != 1 || !(funcExpr.getArguments().get(0) instanceof VariableExpression))
          return null;
        countExpr = funcExpr;
        countAlias = item.getAlias() != null ? item.getAlias() : item.getExpression().getText();
      } else
        groupingKeys.add(item);
    }

    // Must have exactly one count aggregation
    if (aggregationCount != 1 || countExpr == null)
      return null;

    // Count argument must be the target variable
    final String countArgVariable = ((VariableExpression) countExpr.getArguments().get(0)).getVariableName();
    if (!countArgVariable.equals(targetVar))
      return null;

    // All grouping keys must reference already-bound variables
    for (final ReturnClause.ReturnItem key : groupingKeys) {
      final String keyExprText = key.getExpression() instanceof VariableExpression
          ? ((VariableExpression) key.getExpression()).getVariableName()
          : key.getExpression().getText();
      if (!boundVariables.contains(keyExprText))
        return null;
    }

    // The bound vertex must be in the grouping keys
    boolean boundVertexInGroupingKeys = false;
    for (final ReturnClause.ReturnItem key : groupingKeys) {
      final String keyExprText = key.getExpression() instanceof VariableExpression
          ? ((VariableExpression) key.getExpression()).getVariableName()
          : key.getExpression().getText();
      if (keyExprText.equals(boundVar)) {
        boundVertexInGroupingKeys = true;
        break;
      }
    }
    if (!boundVertexInGroupingKeys)
      return null;

    // 7. Compute directions and types
    // First hop: bound -> intermediate
    final Vertex.DIRECTION firstHopDirection;
    final Direction firstRelDirection = firstRel.getDirection();

    if (firstVar1.equals(boundVar)) {
      // bound is firstNode
      if (firstRelDirection == Direction.OUT)
        firstHopDirection = Vertex.DIRECTION.OUT;
      else if (firstRelDirection == Direction.IN)
        firstHopDirection = Vertex.DIRECTION.IN;
      else
        firstHopDirection = Vertex.DIRECTION.BOTH;
    } else {
      // bound is lastNode — reverse the direction
      if (firstRelDirection == Direction.OUT)
        firstHopDirection = Vertex.DIRECTION.IN;
      else if (firstRelDirection == Direction.IN)
        firstHopDirection = Vertex.DIRECTION.OUT;
      else
        firstHopDirection = Vertex.DIRECTION.BOTH;
    }

    // Second hop: direction is FROM intermediate's perspective (used in intermediate.countEdges())
    // The pattern describes direction between target and intermediate, so we must compute
    // the direction as seen by the intermediate vertex.
    final Vertex.DIRECTION secondHopDirection;
    final Direction secondRelDirection = secondRel.getDirection();

    if (secondVar1.equals(targetVar)) {
      // target is firstNode, intermediate is lastNode
      // Pattern: (target)-[OUT]->(intermediate) means edges come IN to intermediate
      if (secondRelDirection == Direction.OUT)
        secondHopDirection = Vertex.DIRECTION.IN;
      else if (secondRelDirection == Direction.IN)
        secondHopDirection = Vertex.DIRECTION.OUT;
      else
        secondHopDirection = Vertex.DIRECTION.BOTH;
    } else {
      // target is lastNode, intermediate is firstNode
      // Pattern: (intermediate)-[OUT]->(target) means edges go OUT from intermediate
      if (secondRelDirection == Direction.OUT)
        secondHopDirection = Vertex.DIRECTION.OUT;
      else if (secondRelDirection == Direction.IN)
        secondHopDirection = Vertex.DIRECTION.IN;
      else
        secondHopDirection = Vertex.DIRECTION.BOTH;
    }

    // Edge types
    final List<String> firstRelTypes = firstRel.getTypes();
    final String[] firstHopTypes = (firstRelTypes != null && !firstRelTypes.isEmpty())
        ? firstRelTypes.toArray(new String[0]) : null;

    final List<String> secondRelTypes = secondRel.getTypes();
    final String[] secondHopTypes = (secondRelTypes != null && !secondRelTypes.isEmpty())
        ? secondRelTypes.toArray(new String[0]) : null;

    // Build pass-through aliases map
    final Map<String, String> passThroughAliases = new LinkedHashMap<>();
    for (final ReturnClause.ReturnItem key : groupingKeys) {
      final String alias = key.getAlias() != null ? key.getAlias() : key.getExpression().getText();
      final String varName = key.getExpression() instanceof VariableExpression
          ? ((VariableExpression) key.getExpression()).getVariableName()
          : key.getExpression().getText();
      passThroughAliases.put(alias, varName);
    }

    // Build the optimized step
    final CountChainedEdgesStep chainedStep = new CountChainedEdgesStep(
        boundVar, firstHopDirection, firstHopTypes, secondHopDirection, secondHopTypes,
        countAlias, passThroughAliases, context);
    if (currentStep != null)
      chainedStep.setPrevious(currentStep);

    return chainedStep;
  }

  /**
   * Attempts to optimize OPTIONAL MATCH + count() pattern into a direct countEdges() call.
   * <p>
   * Detects pattern: OPTIONAL MATCH (x)-[r:TYPE]->(y) ... WITH y, count(x) AS cnt
   * where the OPTIONAL MATCH variables are only used for counting.
   *
   * @return optimized CountEdgesStep if pattern matches, null otherwise
   */
  private AbstractExecutionStep tryOptimizeOptionalMatchCount(final MatchClause matchClause,
      final List<ClauseEntry> clausesInOrder,
      final int currentIndex,
      final AbstractExecutionStep currentStep,
      final CommandContext context,
      final Set<String> boundVariables) {

    // 1. Must be OPTIONAL MATCH with exactly one path pattern
    if (!matchClause.hasPathPatterns() || matchClause.getPathPatterns().size() != 1)
      return null;

    final PathPattern pathPattern = matchClause.getPathPatterns().get(0);

    // 2. Must have exactly one relationship (single hop, not variable-length)
    if (pathPattern.getRelationshipCount() != 1)
      return null;

    final RelationshipPattern relPattern = pathPattern.getRelationship(0);
    if (relPattern.isVariableLength())
      return null;

    // 3. No property constraints on the relationship
    if (relPattern.getProperties() != null && !relPattern.getProperties().isEmpty())
      return null;

    // 4. No WHERE clause on the OPTIONAL MATCH
    if (matchClause.hasWhereClause())
      return null;

    // 5. Next clause must be a WITH
    if (currentIndex + 1 >= clausesInOrder.size())
      return null;

    final ClauseEntry nextEntry = clausesInOrder.get(currentIndex + 1);
    if (nextEntry.getType() != ClauseEntry.ClauseType.WITH)
      return null;

    final WithClause withClause = nextEntry.getTypedClause();

    // WITH must have aggregations + non-aggregations (group by)
    if (!withClause.hasAggregations() || !withClause.hasNonAggregations())
      return null;

    // WITH must not have ORDER BY, SKIP, LIMIT, WHERE (keep optimization simple)
    if (withClause.getOrderByClause() != null || withClause.getSkip() != null
        || withClause.getLimit() != null || withClause.getWhereClause() != null)
      return null;

    // Classify WITH items into grouping keys and aggregations
    final List<ReturnClause.ReturnItem> groupingKeys = new ArrayList<>();
    FunctionCallExpression countExpr = null;
    String countAlias = null;
    int aggregationCount = 0;

    for (final ReturnClause.ReturnItem item : withClause.getItems()) {
      if (item.getExpression().containsAggregation()) {
        aggregationCount++;
        // Must be exactly count(variable) — a direct FunctionCallExpression
        if (!(item.getExpression() instanceof FunctionCallExpression))
          return null;
        final FunctionCallExpression funcExpr = (FunctionCallExpression) item.getExpression();
        if (!"count".equals(funcExpr.getFunctionName()))
          return null;
        // 9. count must not be DISTINCT
        if (funcExpr.isDistinct())
          return null;
        // count argument must be a simple variable
        if (funcExpr.getArguments().size() != 1 || !(funcExpr.getArguments().get(0) instanceof VariableExpression))
          return null;
        countExpr = funcExpr;
        countAlias = item.getAlias() != null ? item.getAlias() : item.getExpression().getText();
      } else
        groupingKeys.add(item);
    }

    // Must have exactly one aggregation
    if (aggregationCount != 1 || countExpr == null)
      return null;

    // Get the count argument variable name
    final String countArgVariable = ((VariableExpression) countExpr.getArguments().get(0)).getVariableName();

    // 6/7. Identify bound and unbound endpoints
    final NodePattern firstNode = pathPattern.getFirstNode();
    final NodePattern lastNode = pathPattern.getLastNode();
    final String firstVar = firstNode.getVariable();
    final String lastVar = lastNode.getVariable();

    if (firstVar == null || lastVar == null)
      return null;

    // Check node patterns don't have property constraints (would need filtering)
    if (firstNode.getProperties() != null && !firstNode.getProperties().isEmpty())
      return null;
    if (lastNode.getProperties() != null && !lastNode.getProperties().isEmpty())
      return null;

    // Determine which endpoint is bound and which is unbound
    final String boundVar;
    final String unboundVar;
    final boolean firstIsBound = boundVariables.contains(firstVar);
    final boolean lastIsBound = boundVariables.contains(lastVar);

    if (firstIsBound && !lastIsBound) {
      boundVar = firstVar;
      unboundVar = lastVar;
    } else if (lastIsBound && !firstIsBound) {
      boundVar = lastVar;
      unboundVar = firstVar;
    } else
      return null; // Both bound or neither bound — can't optimize

    // The count argument must be the unbound variable
    if (!countArgVariable.equals(unboundVar))
      return null;

    // 8. Relationship variable (if named) must NOT be referenced in grouping keys
    final String relVar = relPattern.getVariable();
    if (relVar != null) {
      for (final ReturnClause.ReturnItem key : groupingKeys) {
        if (key.getExpression().getText().contains(relVar))
          return null;
      }
    }

    // All grouping keys must reference only already-bound variables
    for (final ReturnClause.ReturnItem key : groupingKeys) {
      final String keyExprText = key.getExpression() instanceof VariableExpression
          ? ((VariableExpression) key.getExpression()).getVariableName()
          : key.getExpression().getText();
      if (!boundVariables.contains(keyExprText))
        return null;
    }

    // The bound vertex must be in the grouping keys to ensure correct aggregation.
    // Without this, CountEdgesStep would emit one row per input row instead of aggregating.
    // Example: MATCH (q)-[:HAS_ANSWER]->(a) ... WITH q, count(c) AS cnt
    // If 'a' is the bound vertex but only 'q' is in grouping keys, and there are multiple
    // answers per question, CountEdgesStep would produce multiple rows per question.
    boolean boundVertexInGroupingKeys = false;
    for (final ReturnClause.ReturnItem key : groupingKeys) {
      final String keyExprText = key.getExpression() instanceof VariableExpression
          ? ((VariableExpression) key.getExpression()).getVariableName()
          : key.getExpression().getText();
      if (keyExprText.equals(boundVar)) {
        boundVertexInGroupingKeys = true;
        break;
      }
    }
    if (!boundVertexInGroupingKeys)
      return null;

    // Compute direction relative to bound vertex
    // Pattern direction is from firstNode to lastNode
    final Vertex.DIRECTION direction;
    final Direction relDirection = relPattern.getDirection();

    if (firstVar.equals(boundVar)) {
      // bound is firstNode
      if (relDirection == Direction.OUT)
        direction = Vertex.DIRECTION.OUT;
      else if (relDirection == Direction.IN)
        direction = Vertex.DIRECTION.IN;
      else
        direction = Vertex.DIRECTION.BOTH;
    } else {
      // bound is lastNode — reverse the direction
      if (relDirection == Direction.OUT)
        direction = Vertex.DIRECTION.IN;
      else if (relDirection == Direction.IN)
        direction = Vertex.DIRECTION.OUT;
      else
        direction = Vertex.DIRECTION.BOTH;
    }

    // Edge types
    final List<String> relTypes = relPattern.getTypes();
    final String[] edgeTypes = (relTypes != null && !relTypes.isEmpty())
        ? relTypes.toArray(new String[0]) : null;

    // Build pass-through aliases map
    final Map<String, String> passThroughAliases = new LinkedHashMap<>();
    for (final ReturnClause.ReturnItem key : groupingKeys) {
      final String alias = key.getAlias() != null ? key.getAlias() : key.getExpression().getText();
      final String varName = key.getExpression() instanceof VariableExpression
          ? ((VariableExpression) key.getExpression()).getVariableName()
          : key.getExpression().getText();
      passThroughAliases.put(alias, varName);
    }

    // Build the optimized step
    final CountEdgesStep countEdgesStep = new CountEdgesStep(
        boundVar, direction, edgeTypes, countAlias, passThroughAliases, context);
    if (currentStep != null)
      countEdgesStep.setPrevious(currentStep);

    return countEdgesStep;
  }

  /**
   * Attempts to optimize MATCH + RETURN count() into a direct countEdges() call.
   * <p>
   * Detects pattern:
   * MATCH (p:Label)-[:TYPE]->(x) RETURN expr(p) AS alias, count(x) AS cnt
   * <p>
   * Replaces MatchRelationshipStep + GroupByAggregationStep with CountEdgesReturnStep,
   * avoiding materialization of all target vertices.
   * <p>
   * Requirements:
   * - Exactly one MATCH clause (non-optional) with exactly one single-hop relationship
   * - No WHERE clause on the MATCH
   * - RETURN has exactly one count() aggregation on the target variable
   * - count() is not DISTINCT
   * - Target variable is not used in grouping expressions
   * - No relationship property filters
   * - No target node property filters
   *
   * @return optimized CountEdgesReturnStep if pattern matches, null otherwise
   */
  /**
   * Tries to fuse a GROUP BY count(*) aggregation into the GAVFusedChainOperator.
   * When successful, the chain aggregates internally in parallel — bypassing the
   * single-threaded GroupByAggregationStep entirely.
   *
   * @return true if fused successfully, false to fall back to GroupByAggregationStep
   */
  private boolean tryFuseAggregationIntoChain(final WithClause withClause,
      final AbstractExecutionStep currentStep) {
    if (physicalPlan == null || !(physicalPlan.getRootOperator() instanceof GAVFusedChainOperator))
      return false;

    // Check WITH items: need non-aggregated grouping keys + exactly one count(*) or count(var)
    final List<String> groupVarNames = new ArrayList<>();
    final List<String> groupOutputNames = new ArrayList<>();
    String countOutput = null;
    int aggCount = 0;

    for (final var item : withClause.getItems()) {
      final Expression expr = item.getExpression();
      if (expr.isAggregation() && expr instanceof FunctionCallExpression funcExpr) {
        if (!"count".equals(funcExpr.getFunctionName()) || funcExpr.isDistinct())
          return false;
        aggCount++;
        countOutput = item.getOutputName();
      } else if (expr instanceof VariableExpression varExpr) {
        groupVarNames.add(varExpr.getVariableName());
        groupOutputNames.add(item.getOutputName() != null ? item.getOutputName() : varExpr.getVariableName());
      } else
        return false; // complex grouping expression — can't fuse
    }

    if (aggCount != 1 || countOutput == null || groupVarNames.size() > 2)
      return false; // only support 1-2 grouping keys packed into a long

    final GAVFusedChainOperator chain = (GAVFusedChainOperator) physicalPlan.getRootOperator();
    chain.setFusedAggregation(
        groupVarNames.toArray(new String[0]),
        groupOutputNames.toArray(new String[0]),
        countOutput);
    return true;
  }

  private AbstractExecutionStep tryOptimizeMatchCountReturn(
      final List<ClauseEntry> clausesInOrder,
      final ReturnClause returnClause,
      final AbstractExecutionStep currentStep,
      final CommandContext context) {

    if (returnClause == null || !returnClause.hasAggregations() || !returnClause.hasNonAggregations())
      return null;
    if (returnClause.isDistinct())
      return null;

    // Find the single non-optional MATCH clause.
    // Bail out if any OPTIONAL MATCH, WITH, or UNWIND exists — the optimization only
    // works for simple MATCH...RETURN patterns without intermediate transformations.
    MatchClause matchClause = null;
    int matchCount = 0;
    for (final ClauseEntry entry : clausesInOrder) {
      if (entry.getType() == ClauseEntry.ClauseType.MATCH) {
        final MatchClause mc = entry.getTypedClause();
        if (mc.isOptional())
          return null;
        matchClause = mc;
        matchCount++;
      } else if (entry.getType() == ClauseEntry.ClauseType.WITH
          || entry.getType() == ClauseEntry.ClauseType.UNWIND)
        return null; // Intermediate WITH/UNWIND changes the result set shape
    }
    if (matchCount != 1 || matchClause == null)
      return null;

    // Must have exactly one path pattern with one relationship
    if (!matchClause.hasPathPatterns() || matchClause.getPathPatterns().size() != 1)
      return null;

    final PathPattern pathPattern = matchClause.getPathPatterns().get(0);
    if (pathPattern.getRelationshipCount() != 1)
      return null;

    final RelationshipPattern relPattern = pathPattern.getRelationship(0);
    if (relPattern.isVariableLength())
      return null;
    if (relPattern.getProperties() != null && !relPattern.getProperties().isEmpty())
      return null;

    // No WHERE clause
    if (matchClause.hasWhereClause())
      return null;

    // Get source and target nodes
    final NodePattern sourceNode = pathPattern.getFirstNode();
    final NodePattern targetNode = pathPattern.getLastNode();
    final String sourceVar = sourceNode.getVariable();
    final String targetVar = targetNode.getVariable();

    // At least one side must have a variable
    if (sourceVar == null && targetVar == null)
      return null;

    // Target node must not have property filters (would need filtering)
    if (targetNode.getProperties() != null && !targetNode.getProperties().isEmpty())
      return null;
    // Source node must not have property filters
    if (sourceNode.getProperties() != null && !sourceNode.getProperties().isEmpty())
      return null;

    // Classify RETURN items into grouping and aggregation
    final List<ReturnClause.ReturnItem> groupingItems = new ArrayList<>();
    FunctionCallExpression countExpr = null;
    String countAlias = null;
    int aggregationCount = 0;

    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
      if (item.getExpression().containsAggregation()) {
        aggregationCount++;
        if (!(item.getExpression() instanceof FunctionCallExpression))
          return null;
        final FunctionCallExpression funcExpr = (FunctionCallExpression) item.getExpression();
        if (!"count".equals(funcExpr.getFunctionName()))
          return null;
        if (funcExpr.isDistinct())
          return null;

        // Accept count(variable) or count(*) — in a single-hop MATCH, count(*) equals count(targetVar)
        if (funcExpr.getArguments().size() == 1 && funcExpr.getArguments().get(0) instanceof VariableExpression) {
          // count(variable) — variable must be target or source (the expand endpoint)
        } else if (funcExpr.getArguments().size() == 1 && funcExpr.getArguments().get(0) instanceof StarExpression) {
          // count(*) — equivalent to counting edges in single-hop MATCH
        } else
          return null;

        countExpr = funcExpr;
        countAlias = item.getAlias() != null ? item.getAlias() : item.getExpression().getText();
      } else
        groupingItems.add(item);
    }

    if (aggregationCount != 1 || countExpr == null)
      return null;

    // Determine which side is the anchor (the one with grouping keys) and which is the counted side
    // Standard pattern: MATCH (anchor)-[:TYPE]->(counted) RETURN anchor.prop, count(counted)
    // Reverse pattern: MATCH (:Type)-[:TYPE]->(anchor) RETURN anchor.prop, count(*)
    final String countArgVar;
    if (countExpr.getArguments().get(0) instanceof StarExpression)
      countArgVar = null; // count(*) — edges will be counted from anchor side
    else
      countArgVar = ((VariableExpression) countExpr.getArguments().get(0)).getVariableName();

    // Determine the anchor variable: the one used in grouping (non-aggregated RETURN items)
    // For normal pattern: anchor=sourceVar, counted=targetVar
    // For reverse pattern (Q9): anchor=targetVar (b:Badge), source has no variable
    final String anchorVar;
    final NodePattern anchorNode;
    final Vertex.DIRECTION countDirection;

    if (sourceVar != null && (countArgVar == null || countArgVar.equals(targetVar))) {
      // Normal: anchor=source, count target's edges
      anchorVar = sourceVar;
      anchorNode = sourceNode;
      final Direction relDirection = relPattern.getDirection();
      countDirection = relDirection == Direction.OUT ? Vertex.DIRECTION.OUT
          : relDirection == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH;
    } else if (targetVar != null && (countArgVar == null || countArgVar.equals(sourceVar))) {
      // Reverse: anchor=target, count source's edges (reverse direction)
      anchorVar = targetVar;
      anchorNode = targetNode;
      final Direction relDirection = relPattern.getDirection();
      // Reverse direction since we're counting from the other end
      countDirection = relDirection == Direction.OUT ? Vertex.DIRECTION.IN
          : relDirection == Direction.IN ? Vertex.DIRECTION.OUT : Vertex.DIRECTION.BOTH;
    } else
      return null;

    // Grouping expressions must not reference the counted variable
    if (countArgVar != null) {
      for (final ReturnClause.ReturnItem item : groupingItems) {
        final String text = item.getExpression().getText();
        // Check for exact variable reference or property access (e.g., "a" or "a.name")
        if (text.equals(countArgVar) || text.startsWith(countArgVar + "."))
          return null;
      }
    }

    // Edge types
    final List<String> relTypes = relPattern.getTypes();
    final String[] edgeTypes = (relTypes != null && !relTypes.isEmpty())
        ? relTypes.toArray(new String[0]) : null;

    // Build grouping expressions and aliases
    final Expression[] groupingExpressions = new Expression[groupingItems.size()];
    final String[] groupingAliases = new String[groupingItems.size()];
    for (int i = 0; i < groupingItems.size(); i++) {
      final ReturnClause.ReturnItem item = groupingItems.get(i);
      groupingExpressions[i] = item.getExpression();
      groupingAliases[i] = item.getAlias() != null ? item.getAlias() : item.getExpression().getText();
    }

    // The optimized step replaces MatchRelationshipStep + GroupByAggregationStep.
    // Walk back to find the MatchNodeStep — the previous step chain may vary:
    // - Legacy path: MatchNodeStep → MatchRelationshipStep (currentStep)
    // - Optimizer path: physical operator wrapper step (currentStep)
    // In either case, we need the step that provides anchor vertices.
    AbstractExecutionStep nodeStep = currentStep;
    // Try to find MatchNodeStep: walk back through MatchRelationshipStep if present
    if (nodeStep instanceof MatchRelationshipStep) {
      nodeStep = (AbstractExecutionStep) nodeStep.getPrev();
      if (!(nodeStep instanceof MatchNodeStep))
        return null;
    }
    // For optimizer path: the physical operator wrapper already handles the full traversal,
    // so we need to rebuild with just a MatchNodeStep for the anchor variable.
    if (!(nodeStep instanceof MatchNodeStep))
      nodeStep = new MatchNodeStep(anchorVar, anchorNode, context);

    // Determine target label for filtering (the counted node's label, if any)
    final NodePattern countedNode = anchorNode == sourceNode ? targetNode : sourceNode;
    final String targetLabel = countedNode.hasLabels() ? countedNode.getLabels().get(0) : null;

    final CountEdgesReturnStep countStep = new CountEdgesReturnStep(
        anchorVar, countDirection, edgeTypes, countAlias, targetLabel,
        groupingExpressions, groupingAliases, context,
        expressionEvaluator != null ? expressionEvaluator.getFunctionFactory() : null);
    countStep.setPrevious(nodeStep);
    return countStep;
  }

  /**
   * Attempts to create an optimized TYPE COUNT step for simple count queries.
   * <p>
   * Optimizes queries matching this pattern:
   * MATCH (variable:TypeName) RETURN COUNT(variable) as alias
   * <p>
   * Requirements:
   * - Exactly one MATCH clause with one node pattern that has a label
   * - No WHERE clause
   * - RETURN clause with exactly one item: COUNT(variable)
   * - No other clauses (WITH, ORDER BY, SKIP, LIMIT, etc.)
   * <p>
   * Uses O(1) database.countType() instead of O(n) iteration.
   *
   * @param context command context
   *
   * @return optimized TypeCountStep if pattern matches, null otherwise
   */
  private AbstractExecutionStep tryCreateTypeCountOptimization(final CommandContext context) {
    // Must have exactly one MATCH clause
    if (statement.getMatchClauses() == null || statement.getMatchClauses().size() != 1)
      return null;

    final MatchClause matchClause = statement.getMatchClauses().get(0);

    // Must not be OPTIONAL MATCH
    if (matchClause.isOptional())
      return null;

    // Must not have WHERE clause
    if (matchClause.hasWhereClause() || statement.getWhereClause() != null)
      return null;

    // Must have path patterns
    if (!matchClause.hasPathPatterns() || matchClause.getPathPatterns().size() != 1)
      return null;

    final PathPattern pathPattern = matchClause.getPathPatterns().get(0);

    // Must be a single node pattern (not a relationship pattern)
    if (!pathPattern.isSingleNode())
      return null;

    final NodePattern nodePattern = pathPattern.getFirstNode();

    // Node must have at least one label
    if (!nodePattern.hasLabels())
      return null;

    // Node must not have property constraints
    if (nodePattern.hasProperties())
      return null;

    final String variable = nodePattern.getVariable();
    if (variable == null)
      return null;

    // Get the first label (for simplicity, use the first one if multiple labels exist)
    final String typeName = nodePattern.getLabels().get(0);

    // Must have RETURN clause
    if (statement.getReturnClause() == null)
      return null;

    // RETURN must have exactly one item
    if (statement.getReturnClause().getReturnItems().size() != 1)
      return null;

    final ReturnClause.ReturnItem returnItem = statement.getReturnClause().getReturnItems().get(0);
    final Expression returnExpr = returnItem.getExpression();

    // Must be a function call
    if (!(returnExpr instanceof FunctionCallExpression))
      return null;

    final FunctionCallExpression funcExpr =
        (FunctionCallExpression) returnExpr;

    // Function must be COUNT
    if (!"count".equalsIgnoreCase(funcExpr.getFunctionName()))
      return null;

    // COUNT must have exactly one argument
    if (funcExpr.getArguments().size() != 1)
      return null;

    final Expression countArg = funcExpr.getArguments().get(0);

    // Argument must be a variable reference
    if (!(countArg instanceof VariableExpression))
      return null;

    final VariableExpression varExpr =
        (VariableExpression) countArg;

    // Variable in COUNT must match the MATCH variable
    if (!variable.equals(varExpr.getVariableName()))
      return null;

    // Must not have any other clauses that would invalidate the optimization
    if (!statement.getUnwindClauses().isEmpty())
      return null;

    if (!statement.getWithClauses().isEmpty())
      return null;

    if (statement.getOrderByClause() != null)
      return null;

    if (statement.getSkip() != null)
      return null;

    if (statement.getLimit() != null)
      return null;

    if (statement.getCreateClause() != null && !statement.getCreateClause().isEmpty())
      return null;

    if (statement.getSetClause() != null && !statement.getSetClause().isEmpty())
      return null;

    if (statement.getDeleteClause() != null && !statement.getDeleteClause().isEmpty())
      return null;

    if (!statement.getRemoveClauses().isEmpty())
      return null;

    if (statement.getMergeClause() != null)
      return null;

    // All conditions met - create optimized TypeCountStep
    final String outputAlias = returnItem.getOutputName();
    return new TypeCountStep(typeName, outputAlias, context);
  }

  /**
   * Extracts ID filters from a WHERE clause for a specific variable.
   * Looks for predicates like: ID(variable) = "value" or ID(variable) = $param
   * <p>
   * This optimization is critical for performance when matching by ID.
   * Without it, MATCH (a),(b) WHERE ID(a) = x AND ID(b) = y would create
   * a Cartesian product of ALL vertices before filtering (extremely slow).
   * <p>
   * Adds a VariableProjectionStep after ORDER BY + SKIP + LIMIT to strip non-projected
   * variables that were kept in the merged scope for ORDER BY evaluation.
   */
  private AbstractExecutionStep addWithProjection(final WithClause withClause,
      AbstractExecutionStep currentStep, final CommandContext context) {
    // Collect projected variable names from WITH items
    final Set<String> projectedVars = new LinkedHashSet<>();
    for (final ReturnClause.ReturnItem item : withClause.getItems()) {
      if ("*".equals(item.getOutputName()))
        return currentStep; // WITH * keeps everything
      projectedVars.add(item.getOutputName());
    }
    final VariableProjectionStep projStep = new VariableProjectionStep(projectedVars, context);
    projStep.setPrevious(currentStep);
    return projStep;
  }

  /**
   * @param whereClause the WHERE clause to analyze
   * @param variable    the variable to extract ID filter for
   *
   * @return the ID value to filter by, or null if no ID filter found
   */

  /**
   * Extracts WHERE predicates that can be pushed down into a MatchNodeStep.
   * Only predicates referencing the current variable (and already-bound variables) are eligible.
   * The pushed-down predicates are evaluated inline during scanning, reducing pipeline overhead.
   *
   * @param whereClause    the WHERE clause to analyze
   * @param currentVar     the variable being scanned by the MatchNodeStep
   * @param boundVariables variables bound in previous MATCH clauses
   * @param matchVariables variables bound earlier in the current MATCH clause
   * @return the extractable predicate, or null if none qualifies
   */
  private static BooleanExpression extractPushdownFilter(final WhereClause whereClause, final String currentVar,
      final Set<String> boundVariables, final Set<String> matchVariables) {
    if (whereClause == null || whereClause.getConditionExpression() == null)
      return null;

    // Available variables = already bound + already matched in this clause + the current variable
    final Set<String> available = new HashSet<>();
    available.addAll(boundVariables);
    available.addAll(matchVariables);
    available.add(currentVar);

    return WhereClause.extractForVariables(whereClause.getConditionExpression(), available);
  }

  /**
   * Checks whether an edge variable is actually referenced in the query outside its
   * MATCH pattern declaration. If the variable appears only in the pattern (e.g.,
   * {@code [r:KNOWS]}) but is never used elsewhere, it can be treated as anonymous
   * for GAV/fast-path purposes.
   * <p>
   * Rather than enumerating every clause type, this method counts how many times
   * the variable appears across all relationship patterns in all MATCH clauses.
   * If it appears more than once, it's a bound reference in another pattern.
   * Then it checks all expression texts from all other clauses (RETURN, WHERE,
   * ORDER BY, WITH, UNWIND, SET, DELETE, CREATE, MERGE) for the variable name.
   *
   * @param variable the edge variable name to check
   * @return true if the variable is referenced elsewhere in the query
   */
  private boolean isEdgeVariableReferenced(final String variable) {
    // 1. Check if the variable appears as a relationship variable in OTHER MATCH patterns
    //    (e.g., MATCH ()-[r:E]-() MATCH p = ()-[r]-() — r is bound in the second MATCH)
    int relVarCount = 0;
    for (final MatchClause match : statement.getMatchClauses()) {
      if (match.hasPathPatterns()) {
        for (final PathPattern path : match.getPathPatterns()) {
          for (int i = 0; i < path.getRelationshipCount(); i++) {
            final RelationshipPattern rel = path.getRelationship(i);
            if (variable.equals(rel.getVariable()))
              relVarCount++;
          }
        }
      }
    }
    // If the variable appears in more than one relationship pattern, it's referenced elsewhere
    if (relVarCount > 1)
      return true;

    // 2. Check all expression texts from non-MATCH clauses.
    //    Use clausesInOrder to cover everything: RETURN, WHERE, WITH, UNWIND, SET, DELETE, etc.
    if (statement.getClausesInOrder() != null) {
      for (final ClauseEntry entry : statement.getClausesInOrder()) {
        switch (entry.getType()) {
        case RETURN: {
          final ReturnClause rc = entry.getTypedClause();
          for (final ReturnClause.ReturnItem item : rc.getReturnItems())
            if (expressionReferencesVariable(item.getExpression().getText(), variable))
              return true;
          break;
        }
        case WITH: {
          final WithClause wc = entry.getTypedClause();
          for (final ReturnClause.ReturnItem item : wc.getItems())
            if (expressionReferencesVariable(item.getExpression().getText(), variable))
              return true;
          if (wc.getWhereClause() != null && wc.getWhereClause().getConditionExpression() != null)
            if (expressionReferencesVariable(wc.getWhereClause().getConditionExpression().getText(), variable))
              return true;
          break;
        }
        case UNWIND: {
          final UnwindClause uc = entry.getTypedClause();
          if (expressionReferencesVariable(uc.getListExpression().getText(), variable))
            return true;
          break;
        }
        case SET: {
          final SetClause sc = entry.getTypedClause();
          for (final SetClause.SetItem item : sc.getItems())
            if (variable.equals(item.getVariable()))
              return true;
          break;
        }
        case DELETE: {
          final DeleteClause dc = entry.getTypedClause();
          if (dc.getVariables().contains(variable))
            return true;
          break;
        }
        default:
          break;
        }
      }
    }

    // 3. Check statement-level WHERE, ORDER BY (may not be in clausesInOrder)
    if (statement.getWhereClause() != null && statement.getWhereClause().getConditionExpression() != null)
      if (expressionReferencesVariable(statement.getWhereClause().getConditionExpression().getText(), variable))
        return true;

    for (final MatchClause match : statement.getMatchClauses())
      if (match.hasWhereClause() && match.getWhereClause().getConditionExpression() != null)
        if (expressionReferencesVariable(match.getWhereClause().getConditionExpression().getText(), variable))
          return true;

    if (statement.getOrderByClause() != null)
      for (final OrderByClause.OrderByItem item : statement.getOrderByClause().getItems())
        if (expressionReferencesVariable(item.getExpression(), variable))
          return true;

    // 4. Check RETURN clause (may not be in clausesInOrder for simple queries)
    if (statement.getReturnClause() != null)
      for (final ReturnClause.ReturnItem item : statement.getReturnClause().getReturnItems())
        if (expressionReferencesVariable(item.getExpression().getText(), variable))
          return true;

    // 5. Check UNWIND clauses (may not be in clausesInOrder for legacy path)
    for (final UnwindClause uc : statement.getUnwindClauses())
      if (expressionReferencesVariable(uc.getListExpression().getText(), variable))
        return true;

    return false;
  }

  /**
   * Checks if an expression text references a variable as a standalone identifier.
   * Uses word-boundary matching to avoid false positives (e.g., "relation" matching "r").
   */
  private static boolean expressionReferencesVariable(final String expressionText, final String variable) {
    if (expressionText == null || variable == null)
      return false;
    // Find the variable as a standalone identifier (not part of a longer word)
    int idx = 0;
    while ((idx = expressionText.indexOf(variable, idx)) >= 0) {
      final boolean startOk = idx == 0 || !Character.isLetterOrDigit(expressionText.charAt(idx - 1))
          && expressionText.charAt(idx - 1) != '_';
      final int end = idx + variable.length();
      final boolean endOk = end >= expressionText.length() || !Character.isLetterOrDigit(expressionText.charAt(end))
          && expressionText.charAt(end) != '_';
      if (startOk && endOk)
        return true;
      idx++;
    }
    return false;
  }

  /**
   * Determines which hops in a path pattern need edge tracking for Cypher's relationship
   * uniqueness constraint. A hop needs tracking only if its edge types could overlap with
   * another hop's types in the same pattern AND the overlapping hops could match the same
   * physical edge. Two hops with the same edge type but type-disjoint source or target
   * vertex labels cannot match the same edge.
   * <p>
   * Rules:
   * <ul>
   *   <li>Single-hop pattern → no tracking needed (no other hop to conflict with)</li>
   *   <li>Untyped hop (matches all edge types) → always needs tracking</li>
   *   <li>Typed hop whose types are disjoint from all other hops → no tracking</li>
   *   <li>Typed hop with type overlap but disjoint vertex labels on the edge endpoints → no tracking</li>
   *   <li>Typed hop with type overlap and compatible vertex labels → needs tracking</li>
   * </ul>
   *
   * @param pathPattern the path pattern to analyze
   * @return boolean array, true at index i if hop i needs edge tracking
   */
  private boolean[] computeHopEdgeTrackingNeeds(final PathPattern pathPattern) {
    final int hopCount = pathPattern.getRelationshipCount();
    final boolean[] needs = new boolean[hopCount];
    if (hopCount <= 1)
      return needs; // single-hop: all false

    // Collect edge types per hop (null = untyped, matches all)
    final List<String>[] hopTypes = new List[hopCount];
    boolean hasUntyped = false;
    for (int i = 0; i < hopCount; i++) {
      final RelationshipPattern rel = pathPattern.getRelationship(i);
      if (rel.hasTypes())
        hopTypes[i] = rel.getTypes();
      else {
        hopTypes[i] = null;
        hasUntyped = true;
      }
    }

    for (int i = 0; i < hopCount; i++) {
      if (hopTypes[i] == null) {
        // Untyped hop: overlaps with everything
        needs[i] = true;
        continue;
      }
      if (hasUntyped) {
        // Another hop is untyped → overlaps with us
        needs[i] = true;
        continue;
      }
      // Check if our types overlap with any other hop's types
      for (int j = 0; j < hopCount; j++) {
        if (i == j)
          continue;
        // Both typed: check intersection
        for (final String type : hopTypes[i]) {
          if (hopTypes[j].contains(type)) {
            // Same edge type — check if the hops are guaranteed to match different
            // physical edges based on their vertex endpoint labels
            if (areHopsDisjointByEndpointLabels(pathPattern, i, j))
              continue; // Disjoint endpoints → skip this overlap
            needs[i] = true;
            break;
          }
        }
        if (needs[i])
          break;
      }
    }
    return needs;
  }

  /**
   * Checks if two hops with the same edge type are guaranteed to match different physical edges
   * based on the vertex labels at their edge endpoints.
   * <p>
   * An edge has exactly one OUT vertex and one IN vertex. If we can prove that the OUT vertex
   * (or IN vertex) for hop i must be of a different type than for hop j, the edges are distinct.
   * <p>
   * Uses the pattern direction to map pattern nodes to edge OUT/IN endpoints:
   * <ul>
   *   <li>OUT direction: edge OUT = source node (node[i]), edge IN = target node (node[i+1])</li>
   *   <li>IN direction: edge OUT = target node (node[i+1]), edge IN = source node (node[i])</li>
   *   <li>BOTH direction: cannot determine mapping → conservative (not disjoint)</li>
   * </ul>
   */
  private boolean areHopsDisjointByEndpointLabels(final PathPattern pathPattern, final int hopI, final int hopJ) {
    final Direction dirI = pathPattern.getRelationship(hopI).getDirection();
    final Direction dirJ = pathPattern.getRelationship(hopJ).getDirection();

    // BOTH direction: can't determine which node is the edge's OUT/IN vertex
    if (dirI == Direction.BOTH || dirJ == Direction.BOTH)
      return false;

    // Map pattern nodes to edge endpoints based on direction
    final NodePattern edgeOutI = dirI == Direction.OUT ? pathPattern.getNode(hopI) : pathPattern.getNode(hopI + 1);
    final NodePattern edgeOutJ = dirJ == Direction.OUT ? pathPattern.getNode(hopJ) : pathPattern.getNode(hopJ + 1);
    final NodePattern edgeInI = dirI == Direction.OUT ? pathPattern.getNode(hopI + 1) : pathPattern.getNode(hopI);
    final NodePattern edgeInJ = dirJ == Direction.OUT ? pathPattern.getNode(hopJ + 1) : pathPattern.getNode(hopJ);

    // If the OUT vertex labels are type-disjoint, the edges are different
    if (nodeLabelsAreTypeDisjoint(edgeOutI, edgeOutJ))
      return true;

    // If the IN vertex labels are type-disjoint, the edges are different
    return nodeLabelsAreTypeDisjoint(edgeInI, edgeInJ);
  }

  /**
   * Checks whether two node patterns have labels that are type-disjoint in the schema,
   * meaning no vertex can match both labels simultaneously.
   * <p>
   * Two labels are disjoint if neither is a supertype/subtype of the other AND no type
   * in the schema is a subtype of both (which would allow a vertex to match both labels).
   */
  private boolean nodeLabelsAreTypeDisjoint(final NodePattern node1, final NodePattern node2) {
    if (!node1.hasLabels() || !node2.hasLabels())
      return false; // Without labels, can't prove disjointness

    // Use the first label from each node for the check
    final String label1 = node1.getLabels().get(0);
    final String label2 = node2.getLabels().get(0);

    if (label1.equals(label2))
      return false; // Same label → not disjoint

    if (!database.getSchema().existsType(label1) || !database.getSchema().existsType(label2))
      return false; // Unknown type → conservative

    final com.arcadedb.schema.DocumentType type1 = database.getSchema().getType(label1);
    final com.arcadedb.schema.DocumentType type2 = database.getSchema().getType(label2);

    // Direct hierarchy check: if one extends the other, not disjoint
    if (type1.instanceOf(label2) || type2.instanceOf(label1))
      return false;

    // Check for common subtypes: if any type extends both, a vertex could match both labels
    for (final com.arcadedb.schema.DocumentType schemaType : database.getSchema().getTypes())
      if (schemaType.instanceOf(label1) && schemaType.instanceOf(label2))
        return false;

    return true;
  }

  /**
   * Attempts to optimize a linear chain MATCH with {@code RETURN count(*)} into a
   * CSR count-push-down step that avoids materializing intermediate rows.
   * <p>
   * Detects pattern:
   * <pre>
   *   MATCH (a:A)-[:T1]->(b:B)-[:T2]->(c:C) ... RETURN count(*) AS alias
   * </pre>
   * <p>
   * Requirements:
   * <ul>
   *   <li>Exactly one non-optional MATCH clause with exactly one path pattern</li>
   *   <li>No WHERE clause (neither MATCH-level nor statement-level)</li>
   *   <li>RETURN has exactly one item: count(*)</li>
   *   <li>At least one relationship in the path (otherwise use TypeCountStep)</li>
   *   <li>All relationships are fixed-length and anonymous (no edge variables)</li>
   *   <li>No path variable</li>
   *   <li>No other clauses (WITH, CREATE, etc.)</li>
   * </ul>
   *
   * @return optimized CountChainPathsStep if pattern matches, null otherwise
   */
  /**
   * Checks if the clause at the given index in the clause list is followed by a RETURN clause
   * that contains only count(*) aggregations (no property access needed from procedure results).
   * Used to enable the count-only fast path in CallStep.
   */
  private boolean isFollowedByCountOnlyReturn(final List<ClauseEntry> clausesInOrder, final int callIndex) {
    if (callIndex < 0)
      return false;
    final ReturnClause returnClause = statement.getReturnClause();
    if (returnClause == null || !returnClause.hasAggregations())
      return false;
    // Check that RETURN is the only clause after CALL
    for (int i = callIndex + 1; i < clausesInOrder.size(); i++) {
      final ClauseEntry.ClauseType type = clausesInOrder.get(i).getType();
      if (type != ClauseEntry.ClauseType.RETURN)
        return false; // There's a WITH, ORDER BY, etc. between CALL and RETURN
    }
    // Check that ALL return items are aggregation functions (count, sum, avg, etc.)
    // and none access individual row properties
    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
      if (!(item.getExpression() instanceof FunctionCallExpression func))
        return false;
      if (!func.isAggregation())
        return false;
    }
    return true;
  }

  /**
   * Checks if the RETURN clause is exactly {@code RETURN count(*) AS alias}.
   *
   * @return the alias (or "count(*)"), or null if not a count(*) return
   */
  private String isCountStarReturn() {
    final ReturnClause returnClause = statement.getReturnClause();
    if (returnClause == null || returnClause.isDistinct())
      return null;
    final List<ReturnClause.ReturnItem> items = returnClause.getReturnItems();
    if (items.size() != 1)
      return null;
    final ReturnClause.ReturnItem item = items.get(0);
    if (!(item.getExpression() instanceof FunctionCallExpression))
      return null;
    final FunctionCallExpression func = (FunctionCallExpression) item.getExpression();
    if (!"count".equals(func.getFunctionName()))
      return null;
    if (func.getArguments().size() != 1 || !(func.getArguments().get(0) instanceof StarExpression))
      return null;
    return item.getAlias() != null ? item.getAlias() : "count(*)";
  }

  /**
   * Checks that there are no clause types other than MATCH and RETURN.
   */
  private boolean hasOnlyMatchAndReturnClauses() {
    if (statement.getClausesInOrder() == null)
      return true;
    for (final ClauseEntry entry : statement.getClausesInOrder()) {
      final ClauseEntry.ClauseType type = entry.getType();
      if (type != ClauseEntry.ClauseType.MATCH && type != ClauseEntry.ClauseType.RETURN)
        return false;
    }
    return true;
  }

  /**
   * Unified entry point: tries all count-push-down patterns and wraps the result in a CSRCountStep.
   */
  private AbstractExecutionStep tryOptimizeCountStar(final CommandContext context) {
    CountOp op = tryDetectChainCountStar();
    if (op == null)
      op = tryDetectAntiJoinChainCountStar();
    if (op == null)
      op = tryDetectStarCountStar();
    if (op == null)
      op = tryDetectTriangleCountStar();
    if (op == null)
      op = tryDetectPairJoinCountStar();
    if (op == null)
      return null;
    final String alias = isCountStarReturn();
    return new CSRCountStep(op, alias, context);
  }

  private CountOp tryDetectChainCountStar() {
    if (isCountStarReturn() == null || !hasOnlyMatchAndReturnClauses())
      return null;

    // Exactly one MATCH clause
    if (statement.getMatchClauses() == null || statement.getMatchClauses().size() != 1)
      return null;
    final MatchClause matchClause = statement.getMatchClauses().get(0);
    if (matchClause.isOptional())
      return null;

    // WHERE: allow simple inequality (var1 <> var2) or no WHERE
    String inequalityVar1 = null;
    String inequalityVar2 = null;
    final WhereClause whereClause = matchClause.hasWhereClause() ? matchClause.getWhereClause() : statement.getWhereClause();
    if (whereClause != null) {
      final String[] ineqPair = extractSimpleInequality(whereClause);
      if (ineqPair == null)
        return null;
      inequalityVar1 = ineqPair[0];
      inequalityVar2 = ineqPair[1];
    }

    // Exactly one path pattern with at least one relationship
    if (!matchClause.hasPathPatterns() || matchClause.getPathPatterns().size() != 1)
      return null;
    final PathPattern pathPattern = matchClause.getPathPatterns().get(0);
    if (pathPattern.getRelationshipCount() < 1)
      return null;
    if (pathPattern.hasPathVariable())
      return null;

    // All relationships must be fixed-length, anonymous, no properties
    final int hopCount = pathPattern.getRelationshipCount();
    final String[] nodeLabels = new String[hopCount + 1];
    final String[] edgeTypes = new String[hopCount];
    final Vertex.DIRECTION[] directions = new Vertex.DIRECTION[hopCount];

    for (int i = 0; i <= hopCount; i++) {
      final NodePattern node = pathPattern.getNode(i);
      nodeLabels[i] = node.hasLabels() ? node.getLabels().get(0) : null;
    }

    for (int i = 0; i < hopCount; i++) {
      final RelationshipPattern rel = pathPattern.getRelationship(i);
      if (rel.isVariableLength())
        return null;
      if (rel.getVariable() != null && !rel.getVariable().isEmpty())
        return null;
      if (rel.hasProperties())
        return null;
      if (!rel.hasTypes() || rel.getTypes().size() != 1)
        return null;

      edgeTypes[i] = rel.getTypes().get(0);
      final Direction dir = rel.getDirection();
      if (dir == Direction.OUT)
        directions[i] = Vertex.DIRECTION.OUT;
      else if (dir == Direction.IN)
        directions[i] = Vertex.DIRECTION.IN;
      else
        directions[i] = Vertex.DIRECTION.BOTH;
    }

    // Count-push-down does NOT enforce edge uniqueness, so it's only safe when:
    // (a) all edge types are disjoint, OR
    // (b) there's an inequality filter
    final Set<String> seenTypes = new HashSet<>();
    boolean hasDuplicateTypes = false;
    for (final String et : edgeTypes)
      if (!seenTypes.add(et))
        hasDuplicateTypes = true;

    if (hasDuplicateTypes && inequalityVar1 == null)
      return null;

    // Resolve inequality variable positions in the chain
    int inequalityIdxA = -1;
    int inequalityIdxB = -1;
    if (inequalityVar1 != null) {
      for (int i = 0; i <= hopCount; i++) {
        final NodePattern node = pathPattern.getNode(i);
        final String nv = node.getVariable();
        if (nv != null) {
          if (nv.equals(inequalityVar1))
            inequalityIdxA = i;
          else if (nv.equals(inequalityVar2))
            inequalityIdxB = i;
        }
      }
      if (inequalityIdxA < 0 || inequalityIdxB < 0)
        return null;
    }

    return new PropagateChainOp(nodeLabels, edgeTypes, directions, inequalityIdxA, inequalityIdxB);
  }

  /**
   * Attempts to optimize a star-join pattern with {@code RETURN count(*)}.
   * <p>
   * Detects patterns where multiple MATCH/OPTIONAL MATCH path patterns share a single
   * central node variable, and all other nodes are anonymous. For each central node,
   * the count is the product of degrees (or max(1,degree) for optional arms).
   * <p>
   * Covers Q4: {@code MATCH (:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_CREATOR]->(:Person), (m)<-[:LIKES]-(:Person), (m)<-[:REPLY_OF]-(:Comment)}
   * Covers Q7: same mandatory + OPTIONAL MATCH arms
   *
   * @return optimized CountStarJoinStep if pattern matches, null otherwise
   */
  private CountOp tryDetectStarCountStar() {
    if (isCountStarReturn() == null || !hasOnlyMatchAndReturnClauses())
      return null;

    // Must have at least one MATCH clause
    if (statement.getMatchClauses() == null || statement.getMatchClauses().isEmpty())
      return null;

    // No statement-level WHERE
    if (statement.getWhereClause() != null)
      return null;

    // Find the central variable: the one that appears in multiple path patterns.
    // First pass: count occurrences of each variable across all path patterns.
    final java.util.HashMap<String, Integer> varCounts = new java.util.HashMap<>();
    for (final MatchClause mc : statement.getMatchClauses()) {
      if (!mc.hasPathPatterns())
        continue;
      for (final PathPattern pp : mc.getPathPatterns())
        for (int i = 0; i <= pp.getRelationshipCount(); i++) {
          final String nv = pp.getNode(i).getVariable();
          if (nv != null && !nv.isEmpty())
            varCounts.merge(nv, 1, Integer::sum);
        }
    }
    // The central variable must be the ONLY variable appearing in multiple patterns.
    // If two or more variables appear in multiple patterns, it's a pair-join, not a star.
    String centralVar = null;
    String centralLabel = null;
    for (final var entry : varCounts.entrySet())
      if (entry.getValue() > 1) {
        if (centralVar != null)
          return null; // two shared variables → not a star join (likely a pair join)
        centralVar = entry.getKey();
      }
    if (centralVar == null)
      return null; // no variable appears in multiple patterns

    // Find the label for the central variable
    for (final MatchClause mc : statement.getMatchClauses()) {
      if (!mc.hasPathPatterns()) continue;
      for (final PathPattern pp : mc.getPathPatterns())
        for (int i = 0; i <= pp.getRelationshipCount(); i++) {
          final NodePattern node = pp.getNode(i);
          if (centralVar.equals(node.getVariable()) && node.hasLabels()) {
            centralLabel = node.getLabels().get(0);
            break;
          }
        }
      if (centralLabel != null) break;
    }

    final java.util.ArrayList<DegreeProductOp.Arm> armList = new java.util.ArrayList<>();

    for (final MatchClause matchClause : statement.getMatchClauses()) {
      if (matchClause.hasWhereClause())
        return null;
      if (!matchClause.hasPathPatterns())
        return null;
      final boolean isOptional = matchClause.isOptional();

      for (final PathPattern pathPattern : matchClause.getPathPatterns()) {
        if (pathPattern.hasPathVariable())
          return null;

        if (pathPattern.getRelationshipCount() < 1) {
          // Single-node pattern: skip (e.g., anchor node for central variable)
          if (pathPattern.isSingleNode())
            continue;
          return null;
        }

        // Find the central variable's position in this path pattern.
        // Non-central named variables are ignored — they're endpoint bindings
        // that don't affect the degree-product logic for count(*).
        int centralNodeIdx = -1;
        for (int i = 0; i <= pathPattern.getRelationshipCount(); i++) {
          final NodePattern node = pathPattern.getNode(i);
          if (centralVar.equals(node.getVariable())) {
            centralNodeIdx = i;
            break;
          }
        }

        if (centralNodeIdx < 0)
          return null;

        final int totalHops = pathPattern.getRelationshipCount();

        if (centralNodeIdx == 0) {
          final DegreeProductOp.Arm arm = buildArmForward(pathPattern, 0, totalHops, isOptional);
          if (arm == null) return null;
          armList.add(arm);
        } else if (centralNodeIdx == totalHops) {
          final DegreeProductOp.Arm arm = buildArmBackward(pathPattern, totalHops, 0, isOptional);
          if (arm == null) return null;
          armList.add(arm);
        } else {
          final DegreeProductOp.Arm leftArm = buildArmBackward(pathPattern, centralNodeIdx, 0, isOptional);
          if (leftArm == null) return null;
          armList.add(leftArm);
          final DegreeProductOp.Arm rightArm = buildArmForward(pathPattern, centralNodeIdx, totalHops, isOptional);
          if (rightArm == null) return null;
          armList.add(rightArm);
        }
      }
    }

    if (centralVar == null || centralLabel == null || armList.isEmpty())
      return null;

    return new DegreeProductOp(centralLabel, armList.toArray(new DegreeProductOp.Arm[0]));
  }

  /**
   * Detects the Q3 "triangle in country" pattern:
   * <pre>
   *   MATCH (co:Anchor)
   *   MATCH (p1:Node)-[:CHAIN1]->(:Mid)-[:CHAIN2]->(co)
   *   MATCH (p2:Node)-[:CHAIN1]->(:Mid)-[:CHAIN2]->(co)
   *   MATCH (p3:Node)-[:CHAIN1]->(:Mid)-[:CHAIN2]->(co)
   *   MATCH (p1)-[:TRI]-(p2)-[:TRI]-(p3)-[:TRI]-(p1)
   *   RETURN count(*) AS count
   * </pre>
   * Requires: 5+ MATCH clauses, no WHERE, RETURN count(*), one cycle MATCH, three partition MATCHes.
   */
  private CountOp tryDetectTriangleCountStar() {
    if (isCountStarReturn() == null || !hasOnlyMatchAndReturnClauses())
      return null;
    if (statement.getMatchClauses() == null || statement.getMatchClauses().size() < 4)
      return null;
    if (statement.getWhereClause() != null)
      return null;

    // No MATCH clause should have WHERE
    for (final MatchClause mc : statement.getMatchClauses())
      if (mc.hasWhereClause() || mc.isOptional())
        return null;

    // Find the cycle MATCH: a path pattern where first and last node share the same variable
    // e.g., (p1)-[:KNOWS]-(p2)-[:KNOWS]-(p3)-[:KNOWS]-(p1)
    MatchClause cycleMC = null;
    PathPattern cyclePP = null;
    String cycleEdgeType = null;
    final java.util.ArrayList<String> cycleVars = new java.util.ArrayList<>();
    for (final MatchClause mc : statement.getMatchClauses()) {
      if (!mc.hasPathPatterns() || mc.getPathPatterns().size() != 1)
        continue;
      final PathPattern pp = mc.getPathPatterns().get(0);
      if (pp.getRelationshipCount() < 3)
        continue;
      final String firstVar = pp.getFirstNode().getVariable();
      final String lastVar = pp.getLastNode().getVariable();
      if (firstVar != null && firstVar.equals(lastVar) && pp.getRelationshipCount() == 3) {
        // Check all relationships use the same edge type and are anonymous
        boolean valid = true;
        String edgeType = null;
        for (int i = 0; i < 3; i++) {
          final RelationshipPattern rel = pp.getRelationship(i);
          if (rel.isVariableLength() || (rel.getVariable() != null && !rel.getVariable().isEmpty())
              || !rel.hasTypes() || rel.getTypes().size() != 1) {
            valid = false;
            break;
          }
          if (edgeType == null)
            edgeType = rel.getTypes().get(0);
          else if (!edgeType.equals(rel.getTypes().get(0))) {
            valid = false;
            break;
          }
        }
        if (valid) {
          cycleMC = mc;
          cyclePP = pp;
          cycleEdgeType = edgeType;
          // Collect the 3 distinct variables (first=last, so 3 unique vars)
          for (int i = 0; i < 3; i++) {
            final String nv = pp.getNode(i).getVariable();
            if (nv != null && !cycleVars.contains(nv))
              cycleVars.add(nv);
          }
          break;
        }
      }
    }
    if (cycleMC == null || cycleVars.size() != 3)
      return null;

    // Find the anchor MATCH: single node pattern (e.g., (co:Country))
    String anchorVar = null;
    for (final MatchClause mc : statement.getMatchClauses()) {
      if (mc == cycleMC)
        continue;
      if (!mc.hasPathPatterns() || mc.getPathPatterns().size() != 1)
        continue;
      final PathPattern pp = mc.getPathPatterns().get(0);
      if (pp.isSingleNode() && pp.getFirstNode().getVariable() != null) {
        anchorVar = pp.getFirstNode().getVariable();
        break;
      }
    }

    // Find partition chain MATCHes: each cycle var linked to the anchor via a chain
    // e.g., (p1:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(co)
    String[] partitionEdgeTypes = null;
    Vertex.DIRECTION[] partitionDirections = null;
    int chainMatchCount = 0;
    for (final MatchClause mc : statement.getMatchClauses()) {
      if (mc == cycleMC)
        continue;
      if (!mc.hasPathPatterns() || mc.getPathPatterns().size() != 1)
        continue;
      final PathPattern pp = mc.getPathPatterns().get(0);
      if (pp.isSingleNode())
        continue; // anchor match

      // Check: first node is a cycle var, last node is anchor var
      final String firstVar = pp.getFirstNode().getVariable();
      final String lastVar = pp.getLastNode().getVariable();
      if (firstVar == null || lastVar == null)
        continue;
      if (!cycleVars.contains(firstVar) || !lastVar.equals(anchorVar))
        continue;

      // Extract chain edge types and directions
      final int hops = pp.getRelationshipCount();
      final String[] chainET = new String[hops];
      final Vertex.DIRECTION[] chainDir = new Vertex.DIRECTION[hops];
      boolean valid = true;
      for (int i = 0; i < hops; i++) {
        final RelationshipPattern rel = pp.getRelationship(i);
        if (rel.isVariableLength() || !rel.hasTypes() || rel.getTypes().size() != 1) {
          valid = false;
          break;
        }
        chainET[i] = rel.getTypes().get(0);
        final Direction d = rel.getDirection();
        chainDir[i] = d == Direction.OUT ? Vertex.DIRECTION.OUT
            : d == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH;
      }
      if (!valid)
        continue;

      // All chain MATCHes must have the same chain structure
      if (partitionEdgeTypes == null) {
        partitionEdgeTypes = chainET;
        partitionDirections = chainDir;
      } else {
        if (chainET.length != partitionEdgeTypes.length)
          return null;
        for (int i = 0; i < chainET.length; i++)
          if (!chainET[i].equals(partitionEdgeTypes[i]) || chainDir[i] != partitionDirections[i])
            return null;
      }
      chainMatchCount++;
    }

    // Must have exactly 3 chain MATCHes (one per cycle variable)
    if (chainMatchCount != 3 || partitionEdgeTypes == null)
      return null;

    return new PartitionedTriangleOp(partitionEdgeTypes, partitionDirections, cycleEdgeType);
  }

  /**
   * Detects two comma-separated path patterns sharing two endpoint variables + count(*).
   * One pattern is a single-hop "probe" edge, the other is a multi-hop "build" chain.
   * <pre>
   *   MATCH (p1:Person)-[:KNOWS]-(p2:Person),
   *         (p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2)
   *   RETURN count(*) AS count
   * </pre>
   */
  private CountOp tryDetectPairJoinCountStar() {
    if (isCountStarReturn() == null || !hasOnlyMatchAndReturnClauses())
      return null;

    // Exactly one non-optional MATCH with exactly 2 path patterns
    if (statement.getMatchClauses() == null || statement.getMatchClauses().size() != 1)
      return null;
    final MatchClause matchClause = statement.getMatchClauses().get(0);
    if (matchClause.isOptional() || matchClause.hasWhereClause())
      return null;
    if (!matchClause.hasPathPatterns() || matchClause.getPathPatterns().size() != 2)
      return null;
    if (statement.getWhereClause() != null)
      return null;

    // Identify probe (single-hop) and build (multi-hop) patterns
    final PathPattern pp0 = matchClause.getPathPatterns().get(0);
    final PathPattern pp1 = matchClause.getPathPatterns().get(1);

    PathPattern probePattern, buildPattern;
    if (pp0.getRelationshipCount() == 1 && pp1.getRelationshipCount() >= 2) {
      probePattern = pp0;
      buildPattern = pp1;
    } else if (pp1.getRelationshipCount() == 1 && pp0.getRelationshipCount() >= 2) {
      probePattern = pp1;
      buildPattern = pp0;
    } else
      return null; // Neither is single-hop

    // Probe pattern: must be a single anonymous relationship with one edge type
    final RelationshipPattern probeRel = probePattern.getRelationship(0);
    if (probeRel.isVariableLength() || !probeRel.hasTypes() || probeRel.getTypes().size() != 1)
      return null;
    if (probeRel.getVariable() != null && !probeRel.getVariable().isEmpty())
      return null;
    final String probeEdgeType = probeRel.getTypes().get(0);
    final Direction probeDir = probeRel.getDirection();
    final Vertex.DIRECTION probeDirection = probeDir == Direction.OUT ? Vertex.DIRECTION.OUT
        : probeDir == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH;

    // Get the two shared endpoint variables from probe pattern
    final String probeVar1 = probePattern.getFirstNode().getVariable();
    final String probeVar2 = probePattern.getLastNode().getVariable();
    if (probeVar1 == null || probeVar2 == null)
      return null;

    // Build pattern: extract chain and find which hops reach the shared endpoints
    final int buildHops = buildPattern.getRelationshipCount();
    final String[] buildEdgeTypes = new String[buildHops];
    final Vertex.DIRECTION[] buildDirections = new Vertex.DIRECTION[buildHops];

    // Find the build chain's start node (a non-shared variable, e.g., "c" in Q2)
    // The shared variables (probeVar1, probeVar2) should appear as targets of hops
    int startNodeIdx = -1;
    for (int i = 0; i <= buildHops; i++) {
      final String nv = buildPattern.getNode(i).getVariable();
      if (nv != null && !nv.equals(probeVar1) && !nv.equals(probeVar2)) {
        startNodeIdx = i;
        break;
      }
    }
    // If no non-shared named variable found, try anonymous nodes
    if (startNodeIdx < 0) {
      for (int i = 0; i <= buildHops; i++) {
        final String nv = buildPattern.getNode(i).getVariable();
        if (nv == null || nv.isEmpty()) {
          startNodeIdx = i;
          break;
        }
      }
    }
    if (startNodeIdx < 0)
      return null;

    // Determine the build chain start label
    final NodePattern startNode = buildPattern.getNode(startNodeIdx);
    final String buildStartLabel = startNode.hasLabels() ? startNode.getLabels().get(0) : null;
    if (buildStartLabel == null)
      return null;

    // Build two arms from startNodeIdx: backward (toward position 0) and forward (toward end).
    // Each arm reaches one of the shared endpoint variables.

    // Walk backward from startNodeIdx to find endpoint reaching probeVar1 or probeVar2
    final java.util.ArrayList<String> bwdET = new java.util.ArrayList<>();
    final java.util.ArrayList<Vertex.DIRECTION> bwdDir = new java.util.ArrayList<>();
    final java.util.ArrayList<String> bwdLabels = new java.util.ArrayList<>();
    String bwdEndpointVar = null;
    for (int i = startNodeIdx - 1; i >= 0; i--) {
      final RelationshipPattern rel = buildPattern.getRelationship(i);
      if (rel.isVariableLength() || !rel.hasTypes() || rel.getTypes().size() != 1
          || (rel.getVariable() != null && !rel.getVariable().isEmpty()))
        return null;
      bwdET.add(rel.getTypes().get(0));
      final Direction d = rel.getDirection().reverse();
      bwdDir.add(d == Direction.OUT ? Vertex.DIRECTION.OUT
          : d == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH);
      final NodePattern targetNode = buildPattern.getNode(i);
      bwdLabels.add(targetNode.hasLabels() ? targetNode.getLabels().get(0) : null);
      final String nodeVar = targetNode.getVariable();
      if (nodeVar != null && (nodeVar.equals(probeVar1) || nodeVar.equals(probeVar2))) {
        bwdEndpointVar = nodeVar;
        break;
      }
    }

    // Walk forward from startNodeIdx to find the other endpoint
    final java.util.ArrayList<String> fwdET = new java.util.ArrayList<>();
    final java.util.ArrayList<Vertex.DIRECTION> fwdDir = new java.util.ArrayList<>();
    final java.util.ArrayList<String> fwdLabels = new java.util.ArrayList<>();
    String fwdEndpointVar = null;
    for (int i = startNodeIdx; i < buildHops; i++) {
      final RelationshipPattern rel = buildPattern.getRelationship(i);
      if (rel.isVariableLength() || !rel.hasTypes() || rel.getTypes().size() != 1
          || (rel.getVariable() != null && !rel.getVariable().isEmpty()))
        return null;
      fwdET.add(rel.getTypes().get(0));
      final Direction d = rel.getDirection();
      fwdDir.add(d == Direction.OUT ? Vertex.DIRECTION.OUT
          : d == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH);
      final NodePattern targetNode = buildPattern.getNode(i + 1);
      fwdLabels.add(targetNode.hasLabels() ? targetNode.getLabels().get(0) : null);
      final String nodeVar = targetNode.getVariable();
      if (nodeVar != null && (nodeVar.equals(probeVar1) || nodeVar.equals(probeVar2))) {
        fwdEndpointVar = nodeVar;
        break;
      }
    }

    if (bwdEndpointVar == null || fwdEndpointVar == null)
      return null;
    if (bwdEndpointVar.equals(fwdEndpointVar))
      return null; // Both arms reach the same endpoint

    // Arm reaching probeVar1 and arm reaching probeVar2
    final String[] arm1ET, arm2ET, arm1Labels, arm2Labels;
    final Vertex.DIRECTION[] arm1Dir, arm2Dir;
    if (bwdEndpointVar.equals(probeVar1)) {
      arm1ET = bwdET.toArray(new String[0]);
      arm1Dir = bwdDir.toArray(new Vertex.DIRECTION[0]);
      arm1Labels = bwdLabels.toArray(new String[0]);
      arm2ET = fwdET.toArray(new String[0]);
      arm2Dir = fwdDir.toArray(new Vertex.DIRECTION[0]);
      arm2Labels = fwdLabels.toArray(new String[0]);
    } else {
      arm1ET = fwdET.toArray(new String[0]);
      arm1Dir = fwdDir.toArray(new Vertex.DIRECTION[0]);
      arm1Labels = fwdLabels.toArray(new String[0]);
      arm2ET = bwdET.toArray(new String[0]);
      arm2Dir = bwdDir.toArray(new Vertex.DIRECTION[0]);
      arm2Labels = bwdLabels.toArray(new String[0]);
    }

    return new PairHashJoinOp(buildStartLabel, arm1ET, arm1Dir, arm1Labels,
        arm2ET, arm2Dir, arm2Labels, probeEdgeType, probeDirection);
  }

  /**
   * Extracts a simple inequality predicate from a WHERE clause.
   * Returns [var1, var2] if the WHERE is exactly "var1 <> var2", null otherwise.
   */
  private static String[] extractSimpleInequality(final WhereClause whereClause) {
    if (whereClause == null || whereClause.getConditionExpression() == null)
      return null;
    final BooleanExpression condition = whereClause.getConditionExpression();
    if (!(condition instanceof ComparisonExpression))
      return null;
    final ComparisonExpression cmp = (ComparisonExpression) condition;
    if (cmp.getOperator() != ComparisonExpression.Operator.NOT_EQUALS)
      return null;
    final Expression left = cmp.getLeft();
    final Expression right = cmp.getRight();
    if (!(left instanceof VariableExpression) || !(right instanceof VariableExpression))
      return null;
    return new String[]{((VariableExpression) left).getVariableName(),
        ((VariableExpression) right).getVariableName()};
  }

  /**
   * Detects a chain pattern with a negative path predicate (anti-join) in WHERE.
   * <p>
   * Handles patterns like:
   * <pre>
   *   MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag)
   *   WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3
   *   RETURN count(*) AS count
   * </pre>
   * The WHERE clause must contain a negated single-hop pattern predicate between two chain nodes,
   * optionally combined with a simple inequality via AND.
   */
  private CountOp tryDetectAntiJoinChainCountStar() {
    if (isCountStarReturn() == null || !hasOnlyMatchAndReturnClauses())
      return null;

    // Exactly one MATCH clause
    if (statement.getMatchClauses() == null || statement.getMatchClauses().size() != 1)
      return null;
    final MatchClause matchClause = statement.getMatchClauses().get(0);
    if (matchClause.isOptional())
      return null;

    // Must have a WHERE clause with an anti-join pattern
    final WhereClause whereClause = matchClause.hasWhereClause() ? matchClause.getWhereClause() : statement.getWhereClause();
    if (whereClause == null || whereClause.getConditionExpression() == null)
      return null;

    // Parse the WHERE clause: extract anti-join pattern and optional inequality
    final AntiJoinInfo antiJoin = extractAntiJoinInfo(whereClause);
    if (antiJoin == null)
      return null;

    // Exactly one path pattern with at least one relationship
    if (!matchClause.hasPathPatterns() || matchClause.getPathPatterns().size() != 1)
      return null;
    final PathPattern pathPattern = matchClause.getPathPatterns().get(0);
    if (pathPattern.getRelationshipCount() < 2) // need at least 2 hops for anti-join to make sense
      return null;
    if (pathPattern.hasPathVariable())
      return null;

    // Extract chain structure
    final int hopCount = pathPattern.getRelationshipCount();
    final String[] nodeLabels = new String[hopCount + 1];
    final String[] edgeTypes = new String[hopCount];
    final Vertex.DIRECTION[] directions = new Vertex.DIRECTION[hopCount];

    for (int i = 0; i <= hopCount; i++) {
      final NodePattern node = pathPattern.getNode(i);
      nodeLabels[i] = node.hasLabels() ? node.getLabels().get(0) : null;
    }

    for (int i = 0; i < hopCount; i++) {
      final RelationshipPattern rel = pathPattern.getRelationship(i);
      if (rel.isVariableLength())
        return null;
      if (rel.getVariable() != null && !rel.getVariable().isEmpty())
        return null;
      if (rel.hasProperties())
        return null;
      if (!rel.hasTypes() || rel.getTypes().size() != 1)
        return null;

      edgeTypes[i] = rel.getTypes().get(0);
      final Direction dir = rel.getDirection();
      if (dir == Direction.OUT)
        directions[i] = Vertex.DIRECTION.OUT;
      else if (dir == Direction.IN)
        directions[i] = Vertex.DIRECTION.IN;
      else
        directions[i] = Vertex.DIRECTION.BOTH;
    }

    // Resolve anti-join variable positions in the chain
    int antiJoinSourceIdx = -1;
    int antiJoinTargetIdx = -1;
    for (int i = 0; i <= hopCount; i++) {
      final NodePattern node = pathPattern.getNode(i);
      final String nv = node.getVariable();
      if (nv != null) {
        if (nv.equals(antiJoin.sourceVar))
          antiJoinSourceIdx = i;
        if (nv.equals(antiJoin.targetVar))
          antiJoinTargetIdx = i;
      }
    }
    if (antiJoinSourceIdx < 0 || antiJoinTargetIdx < 0)
      return null;

    // Do NOT swap source/target — the direction depends on the original order.
    // AntiJoinChainOp handles both cases:
    //   Case A (Q9): anti-join from anchor(0) to later position → merge-scan
    //   Case B (Q8): anti-join from later position to anchor(0) → per-frontier binary search

    // Resolve inequality positions (if present)
    int inequalityIdxA = -1;
    int inequalityIdxB = -1;
    if (antiJoin.inequalityVar1 != null) {
      for (int i = 0; i <= hopCount; i++) {
        final NodePattern node = pathPattern.getNode(i);
        final String nv = node.getVariable();
        if (nv != null) {
          if (nv.equals(antiJoin.inequalityVar1))
            inequalityIdxA = i;
          else if (nv.equals(antiJoin.inequalityVar2))
            inequalityIdxB = i;
        }
      }
      if (inequalityIdxA < 0 || inequalityIdxB < 0)
        return null;
    }

    return new AntiJoinChainOp(nodeLabels, edgeTypes, directions,
        antiJoinSourceIdx, antiJoinTargetIdx,
        antiJoin.antiJoinEdgeType, antiJoin.antiJoinDirection,
        inequalityIdxA, inequalityIdxB);
  }

  /**
   * Information extracted from a WHERE clause containing an anti-join pattern.
   */
  private static final class AntiJoinInfo {
    final String sourceVar;
    final String targetVar;
    final String antiJoinEdgeType;
    final Vertex.DIRECTION antiJoinDirection;
    final String inequalityVar1; // null if no inequality
    final String inequalityVar2;

    AntiJoinInfo(final String sourceVar, final String targetVar,
        final String antiJoinEdgeType, final Vertex.DIRECTION antiJoinDirection,
        final String inequalityVar1, final String inequalityVar2) {
      this.sourceVar = sourceVar;
      this.targetVar = targetVar;
      this.antiJoinEdgeType = antiJoinEdgeType;
      this.antiJoinDirection = antiJoinDirection;
      this.inequalityVar1 = inequalityVar1;
      this.inequalityVar2 = inequalityVar2;
    }
  }

  /**
   * Extracts anti-join pattern info from a WHERE clause.
   * <p>
   * Supported forms:
   * <ul>
   *   <li>{@code WHERE NOT (a)-[:TYPE]-(b)} — anti-join only</li>
   *   <li>{@code WHERE NOT (a)-[:TYPE]-(b) AND a <> b} — anti-join + inequality (either order)</li>
   * </ul>
   *
   * @return extracted info, or null if the WHERE clause doesn't match
   */
  private static AntiJoinInfo extractAntiJoinInfo(final WhereClause whereClause) {
    if (whereClause == null || whereClause.getConditionExpression() == null)
      return null;

    final BooleanExpression condition = whereClause.getConditionExpression();

    // Case 1: Simple negated pattern predicate — either PatternPredicateExpression(negated=true)
    // or LogicalExpression(NOT, PatternPredicateExpression)
    final PatternPredicateExpression directNeg = extractNegatedPattern(condition);
    if (directNeg != null)
      return extractFromPatternPredicate(directNeg, null, null);

    // Case 2: AND of two conditions (anti-join + inequality, in either order)
    if (condition instanceof LogicalExpression) {
      final LogicalExpression logical = (LogicalExpression) condition;
      if (logical.getOperator() != LogicalExpression.Operator.AND)
        return null;

      final BooleanExpression left = logical.getLeft();
      final BooleanExpression right = logical.getRight();

      // Try: left = anti-join, right = inequality
      final PatternPredicateExpression leftNeg = extractNegatedPattern(left);
      if (leftNeg != null && right instanceof ComparisonExpression) {
        final String[] ineq = extractInequalityFromComparison((ComparisonExpression) right);
        if (ineq != null)
          return extractFromPatternPredicate(leftNeg, ineq[0], ineq[1]);
      }

      // Try: left = inequality, right = anti-join
      final PatternPredicateExpression rightNeg = extractNegatedPattern(right);
      if (rightNeg != null && left instanceof ComparisonExpression) {
        final String[] ineq = extractInequalityFromComparison((ComparisonExpression) left);
        if (ineq != null)
          return extractFromPatternPredicate(rightNeg, ineq[0], ineq[1]);
      }
    }

    return null;
  }

  /**
   * Extracts a negated pattern predicate from a boolean expression.
   * Handles two forms:
   * <ul>
   *   <li>{@code PatternPredicateExpression(isNegated=true)}</li>
   *   <li>{@code LogicalExpression(NOT, PatternPredicateExpression)}</li>
   * </ul>
   *
   * @return the pattern predicate (always with isNegated semantics), or null
   */
  private static PatternPredicateExpression extractNegatedPattern(final BooleanExpression expr) {
    if (expr instanceof PatternPredicateExpression) {
      final PatternPredicateExpression ppe = (PatternPredicateExpression) expr;
      return ppe.isNegated() ? ppe : null;
    }
    if (expr instanceof LogicalExpression) {
      final LogicalExpression logical = (LogicalExpression) expr;
      if (logical.getOperator() == LogicalExpression.Operator.NOT
          && logical.getLeft() instanceof PatternPredicateExpression) {
        // NOT wrapping a non-negated pattern predicate = negated pattern
        return (PatternPredicateExpression) logical.getLeft();
      }
    }
    return null;
  }

  /**
   * Extracts anti-join info from a negated pattern predicate.
   * The pattern must be a single-hop, single-type, anonymous relationship between two variables.
   */
  /**
   * Extracts anti-join info from a pattern predicate expression.
   * The pattern must be a single-hop, single-type, anonymous relationship between two variables.
   * Note: the negation may come from either PatternPredicateExpression.isNegated() or from
   * a wrapping LogicalExpression(NOT, ...) — the caller ensures negation semantics.
   */
  private static AntiJoinInfo extractFromPatternPredicate(final PatternPredicateExpression ppe,
      final String inequalityVar1, final String inequalityVar2) {
    final PathPattern pp = ppe.getPathPattern();
    if (pp == null || pp.getRelationshipCount() != 1)
      return null;

    final RelationshipPattern rel = pp.getRelationship(0);
    if (rel.isVariableLength())
      return null;
    if (!rel.hasTypes() || rel.getTypes().size() != 1)
      return null;

    final String sourceVar = pp.getFirstNode().getVariable();
    final String targetVar = pp.getLastNode().getVariable();
    if (sourceVar == null || targetVar == null)
      return null;

    final String edgeType = rel.getTypes().get(0);
    final Direction dir = rel.getDirection();
    final Vertex.DIRECTION direction = dir == Direction.OUT ? Vertex.DIRECTION.OUT
        : dir == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH;

    return new AntiJoinInfo(sourceVar, targetVar, edgeType, direction,
        inequalityVar1, inequalityVar2);
  }

  /**
   * Extracts inequality info from a comparison expression.
   * Returns [var1, var2] if the expression is "var1 <> var2", null otherwise.
   */
  private static String[] extractInequalityFromComparison(final ComparisonExpression cmp) {
    if (cmp.getOperator() != ComparisonExpression.Operator.NOT_EQUALS)
      return null;
    final Expression left = cmp.getLeft();
    final Expression right = cmp.getRight();
    if (!(left instanceof VariableExpression) || !(right instanceof VariableExpression))
      return null;
    return new String[]{((VariableExpression) left).getVariableName(),
        ((VariableExpression) right).getVariableName()};
  }

  /**
   * Builds a star-join arm going forward from centralIdx toward endIdx in the path pattern.
   * Direction is preserved as-is from the pattern.
   */
  private DegreeProductOp.Arm buildArmForward(final PathPattern pathPattern, final int centralIdx,
      final int endIdx, final boolean optional) {
    final int hops = endIdx - centralIdx;
    final String[] edgeTypes = new String[hops];
    final Vertex.DIRECTION[] directions = new Vertex.DIRECTION[hops];
    for (int i = 0; i < hops; i++) {
      final RelationshipPattern rel = pathPattern.getRelationship(centralIdx + i);
      if (rel.isVariableLength() || (rel.getVariable() != null && !rel.getVariable().isEmpty())
          || rel.hasProperties() || !rel.hasTypes() || rel.getTypes().size() != 1)
        return null;
      edgeTypes[i] = rel.getTypes().get(0);
      final Direction dir = rel.getDirection();
      directions[i] = dir == Direction.OUT ? Vertex.DIRECTION.OUT
          : dir == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH;
    }
    return new DegreeProductOp.Arm(edgeTypes, directions, optional);
  }

  /**
   * Builds a star-join arm going backward from centralIdx toward endIdx in the path pattern.
   * Directions are reversed since we traverse from the central node toward position 0.
   */
  private DegreeProductOp.Arm buildArmBackward(final PathPattern pathPattern, final int centralIdx,
      final int endIdx, final boolean optional) {
    final int hops = centralIdx - endIdx;
    final String[] edgeTypes = new String[hops];
    final Vertex.DIRECTION[] directions = new Vertex.DIRECTION[hops];
    for (int i = 0; i < hops; i++) {
      // Walk backward from centralIdx: rel at (centralIdx-1), (centralIdx-2), ...
      final RelationshipPattern rel = pathPattern.getRelationship(centralIdx - 1 - i);
      if (rel.isVariableLength() || (rel.getVariable() != null && !rel.getVariable().isEmpty())
          || rel.hasProperties() || !rel.hasTypes() || rel.getTypes().size() != 1)
        return null;
      edgeTypes[i] = rel.getTypes().get(0);
      // Reverse direction since we're traversing the arm in the opposite direction
      final Direction dir = rel.getDirection().reverse();
      directions[i] = dir == Direction.OUT ? Vertex.DIRECTION.OUT
          : dir == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH;
    }
    return new DegreeProductOp.Arm(edgeTypes, directions, optional);
  }

  private String extractIdFilter(final WhereClause whereClause, final String variable) {
    if (whereClause == null || whereClause.getConditionExpression() == null)
      return null;

    final BooleanExpression condition = whereClause.getConditionExpression();

    // Try to extract ID filter from the condition expression
    // The condition may be an AND expression containing multiple predicates
    // We need to find the one that matches: ID(variable) = value
    return extractIdFilterFromExpression(condition, variable);
  }

  /**
   * Recursively extracts ID filter from a boolean expression.
   */
  private String extractIdFilterFromExpression(final BooleanExpression expr, final String variable) {
    if (expr == null)
      return null;

    // Check if this is a comparison expression (ID(var) = value)
    if (expr instanceof ComparisonExpression) {
      final ComparisonExpression compExpr = (ComparisonExpression) expr;

      // Check if left side is ID(variable)
      final Expression left = compExpr.getLeft();
      if (left instanceof FunctionCallExpression) {
        final FunctionCallExpression funcExpr = (FunctionCallExpression) left;
        if ("id".equalsIgnoreCase(funcExpr.getFunctionName()) && funcExpr.getArguments().size() == 1) {
          final Expression arg = funcExpr.getArguments().get(0);
          if (arg instanceof VariableExpression) {
            final VariableExpression varExpr = (VariableExpression) arg;
            if (variable.equals(varExpr.getVariableName())) {
              // Found ID(variable) - extract the value from right side
              final Expression right = compExpr.getRight();
              return evaluateIdValue(right);
            }
          }
        }
      }
    }

    // Check if this is a logical AND expression - recursively search both sides
    if (expr instanceof LogicalExpression) {
      final LogicalExpression logExpr = (LogicalExpression) expr;
      if (logExpr.getOperator() == LogicalExpression.Operator.AND) {
        final String leftResult = extractIdFilterFromExpression(logExpr.getLeft(), variable);
        if (leftResult != null)
          return leftResult;
        return extractIdFilterFromExpression(logExpr.getRight(), variable);
      }
    }

    return null;
  }

  /**
   * Evaluates an expression to extract the ID value (literal or parameter).
   */
  private String evaluateIdValue(final Expression expr) {
    if (expr == null)
      return null;

    // Handle literal string values
    if (expr instanceof LiteralExpression) {
      final LiteralExpression litExpr = (LiteralExpression) expr;
      final Object value = litExpr.getValue();
      return value != null ? value.toString() : null;
    }

    // Handle parameter references
    if (expr instanceof ParameterExpression) {
      final ParameterExpression paramExpr = (ParameterExpression) expr;
      final String paramName = paramExpr.getParameterName();
      if (parameters != null && parameters.containsKey(paramName)) {
        final Object value = parameters.get(paramName);
        return value != null ? value.toString() : null;
      }
    }

    // Handle property access for UNWIND scenarios (e.g., row.source_id)
    if (expr instanceof PropertyAccessExpression) {
      // Can't evaluate at plan time - would need runtime context
      // This is handled differently via parameter substitution
      return null;
    }

    return null;
  }
}
