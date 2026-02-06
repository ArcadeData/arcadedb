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
import com.arcadedb.query.opencypher.ast.*;
import com.arcadedb.query.opencypher.executor.steps.AggregationStep;
import com.arcadedb.query.opencypher.executor.steps.CallStep;
import com.arcadedb.query.opencypher.executor.steps.CreateStep;
import com.arcadedb.query.opencypher.executor.steps.DeleteStep;
import com.arcadedb.query.opencypher.executor.steps.ExpandPathStep;
import com.arcadedb.query.opencypher.executor.steps.FilterPropertiesStep;
import com.arcadedb.query.opencypher.executor.steps.ForeachStep;
import com.arcadedb.query.opencypher.executor.steps.FinalProjectionStep;
import com.arcadedb.query.opencypher.executor.steps.GroupByAggregationStep;
import com.arcadedb.query.opencypher.executor.steps.LimitStep;
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
import com.arcadedb.query.opencypher.executor.steps.TypeCountStep;
import com.arcadedb.query.opencypher.executor.steps.UnionStep;
import com.arcadedb.query.opencypher.executor.steps.UnwindStep;
import com.arcadedb.query.opencypher.executor.steps.WithStep;
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

import java.util.*;

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
  private final ExpressionEvaluator expressionEvaluator;

  // UNION support
  private final List<CypherExecutionPlan> unionSubqueryPlans;
  private final boolean unionRemoveDuplicates;

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

    AbstractExecutionStep rootStep;

    // Phase 4: Use optimized physical plan if available
    // BUT: Disable optimizer when UNWIND precedes MATCH in the query
    // The optimizer doesn't handle clause ordering correctly - it puts the WHERE filter
    // before UNWIND, which breaks queries like:
    //   UNWIND $batch as row MATCH (a) WHERE ID(a) = row.source_id
    // In this case, the WHERE filter needs to run AFTER UNWIND to access 'row'.
    final boolean hasUnwindBeforeMatch = hasUnwindPrecedingMatch();

    if (physicalPlan != null && physicalPlan.getRootOperator() != null && !hasUnwindBeforeMatch) {
      // Use optimizer - execute physical operators directly
      // Note: For Phase 4, we only optimize MATCH patterns
      // RETURN, ORDER BY, LIMIT are still handled by execution steps
      rootStep = buildExecutionStepsWithOptimizer(context);
    } else {
      // Fall back to non-optimized execution
      // This path correctly handles clause ordering (UNWIND before MATCH)
      rootStep = buildExecutionSteps(context);
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
    final boolean hasForeach = statement.getClausesInOrder() != null &&
                                statement.getClausesInOrder().stream()
                                    .anyMatch(c -> c.getType() == ClauseEntry.ClauseType.FOREACH);
    final boolean hasWriteOps = statement.getCreateClause() != null ||
                                 (statement.getSetClause() != null && !statement.getSetClause().isEmpty()) ||
                                 (statement.getDeleteClause() != null && !statement.getDeleteClause().isEmpty()) ||
                                 !statement.getRemoveClauses().isEmpty() ||
                                 statement.getMergeClause() != null ||
                                 hasForeach;

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
   * Executes a UNION query by combining results from all subqueries.
   *
   * @return combined result set
   */
  private ResultSet executeUnion() {
    // Use UnionStep to combine results from all subqueries
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);

    final UnionStep unionStep =
        new UnionStep(unionSubqueryPlans, unionRemoveDuplicates, context);

    return unionStep.syncPull(context, 100);
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
   * Executes the query with profiling enabled.
   * Returns the query results along with execution metrics.
   *
   * @return result set containing results and profiling metrics
   */
  public ResultSet profile() {
    // Record start time
    final long startTime = System.nanoTime();

    // Build execution context with profiling enabled
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    context.setProfiling(true); // Enable profiling

    // Execute the query and collect results
    final List<ResultInternal> allResults = new ArrayList<>();
    long rowCount = 0;

    try {
      // Handle UNION queries specially
      if (unionSubqueryPlans != null && !unionSubqueryPlans.isEmpty()) {
        final UnionStep unionStep =
            new UnionStep(unionSubqueryPlans, unionRemoveDuplicates, context);
        final ResultSet resultSet = unionStep.syncPull(context, Integer.MAX_VALUE);
        while (resultSet.hasNext()) {
          allResults.add((ResultInternal) resultSet.next());
          rowCount++;
        }
      } else {
        // Build execution steps
        AbstractExecutionStep rootStep;
        final boolean hasUnwindBeforeMatch = hasUnwindPrecedingMatch();

        if (physicalPlan != null && physicalPlan.getRootOperator() != null && !hasUnwindBeforeMatch) {
          rootStep = buildExecutionStepsWithOptimizer(context);
        } else {
          rootStep = buildExecutionSteps(context);
        }

        if (rootStep != null) {
          final ResultSet resultSet = rootStep.syncPull(context, Integer.MAX_VALUE);
          while (resultSet.hasNext()) {
            allResults.add((ResultInternal) resultSet.next());
            rowCount++;
          }
        }
      }
    } catch (final Exception e) {
      // Include error in profile output
      final ResultInternal errorResult = new ResultInternal();
      errorResult.setProperty("error", e.getMessage());
      allResults.add(errorResult);
    }

    // Calculate execution time
    final long endTime = System.nanoTime();
    final double executionTimeMs = (endTime - startTime) / 1_000_000.0;

    // Generate profile output
    final StringBuilder profileOutput = new StringBuilder();
    profileOutput.append("OpenCypher Query Profile\n");
    profileOutput.append("========================\n\n");
    profileOutput.append(String.format("Execution Time: %.3f ms\n", executionTimeMs));
    profileOutput.append(String.format("Rows Returned: %d\n", rowCount));

    if (physicalPlan != null && physicalPlan.getRootOperator() != null) {
      profileOutput.append("\nExecution Plan (Cost-Based Optimizer):\n");
      profileOutput.append(physicalPlan.getRootOperator().explain(0));
      profileOutput.append(String.format("\nEstimated Cost: %.2f\n", physicalPlan.getTotalEstimatedCost()));
      profileOutput.append(String.format("Estimated Rows: %d\n", physicalPlan.getTotalEstimatedCardinality()));
    } else {
      profileOutput.append("\nExecution Plan (Traditional):\n");
      profileOutput.append("Step-by-step interpretation\n");
    }

    // Add profile as first result
    final ResultInternal profileResult = new ResultInternal();
    profileResult.setProperty("profile", profileOutput.toString());
    allResults.add(0, profileResult);

    return new IteratorResultSet(allResults.iterator());
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
    // Get function factory from evaluator for steps that need it
    final CypherFunctionFactory functionFactory = expressionEvaluator != null ? expressionEvaluator.getFunctionFactory() : null;

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
      final SetStep setStep =
          new SetStep(statement.getSetClause(), context, functionFactory);
      setStep.setPrevious(currentStep);
      currentStep = setStep;
    }

    // Step 4: DELETE clause (if any)
    if (statement.getDeleteClause() != null && !statement.getDeleteClause().isEmpty()) {
      final DeleteStep deleteStep =
          new DeleteStep(statement.getDeleteClause(), context);
      deleteStep.setPrevious(currentStep);
      currentStep = deleteStep;
    }

    // Step 4a: REMOVE clauses (if any)
    for (final RemoveClause removeClause : statement.getRemoveClauses()) {
      if (!removeClause.isEmpty()) {
        final RemoveStep removeStep =
            new RemoveStep(removeClause, context);
        removeStep.setPrevious(currentStep);
        currentStep = removeStep;
      }
    }

    // Step 5: MERGE clause (if any)
    if (statement.getMergeClause() != null) {
      final MergeStep mergeStep =
          new MergeStep(statement.getMergeClause(), context, functionFactory);
      mergeStep.setPrevious(currentStep);
      currentStep = mergeStep;
    }

    // Step 6: UNWIND clause (if any)
    if (!statement.getUnwindClauses().isEmpty()) {
      for (final UnwindClause unwind : statement.getUnwindClauses()) {
        final UnwindStep unwindStep =
            new UnwindStep(unwind, context, functionFactory);
        unwindStep.setPrevious(currentStep);
        currentStep = unwindStep;
      }
    }

    // Step 6.5: WITH clauses (if any)
    if (!statement.getWithClauses().isEmpty()) {
      for (final WithClause withClause : statement.getWithClauses()) {
        // Handle aggregations in WITH clause
        if (withClause.hasAggregations()) {
          if (withClause.hasNonAggregations()) {
            // GROUP BY aggregation (implicit grouping)
            final GroupByAggregationStep groupByStep =
                new GroupByAggregationStep(
                    new ReturnClause(withClause.getItems(), false),
                    context, functionFactory);
            groupByStep.setPrevious(currentStep);
            currentStep = groupByStep;
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
          final OrderByStep orderByStep =
              new OrderByStep(withClause.getOrderByClause(), context, functionFactory);
          orderByStep.setPrevious(currentStep);
          currentStep = orderByStep;

          // Chain SKIP/LIMIT after ORDER BY so pagination happens after sorting
          if (withClause.getSkip() != null) {
            final SkipStep skipStep = new SkipStep(withClause.getSkip(), context);
            skipStep.setPrevious(currentStep);
            currentStep = skipStep;
          }
          if (withClause.getLimit() != null) {
            final LimitStep limitStep = new LimitStep(withClause.getLimit(), context);
            limitStep.setPrevious(currentStep);
            currentStep = limitStep;
          }
        }
      }
    }

    // Step 7: RETURN clause (if any)
    if (statement.getReturnClause() != null) {
      // Check if RETURN contains aggregation functions
      if (statement.getReturnClause().hasAggregations()) {
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
        final ProjectReturnStep returnStep = new ProjectReturnStep(statement.getReturnClause(), context, functionFactory);
        returnStep.setPrevious(currentStep);
        currentStep = returnStep;
      }
    }

    // Step 8: ORDER BY (if any)
    if (statement.getOrderByClause() != null) {
      final OrderByStep orderByStep =
          new OrderByStep(statement.getOrderByClause(), context, functionFactory);
      orderByStep.setPrevious(currentStep);
      currentStep = orderByStep;
    }

    // Step 9: SKIP (if any)
    if (statement.getSkip() != null) {
      final SkipStep skipStep =
          new SkipStep(statement.getSkip(), context);
      skipStep.setPrevious(currentStep);
      currentStep = skipStep;
    }

    // Step 10: LIMIT (if any)
    if (statement.getLimit() != null) {
      final LimitStep limitStep =
          new LimitStep(statement.getLimit(), context);
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
    AbstractExecutionStep currentStep = null;

    // Get function factory from evaluator for steps that need it
    final CypherFunctionFactory functionFactory = expressionEvaluator != null ? expressionEvaluator.getFunctionFactory() : null;

    // Track variables bound across MATCH clauses so subsequent MATCHes
    // can detect already-bound variables and avoid Cartesian products
    final Set<String> boundVariables = new HashSet<>();

    // OPTIMIZATION: Check for simple COUNT(*) pattern that can use Type.count() O(1) operation
    // Pattern: MATCH (a:TypeName) RETURN COUNT(a) as alias
    final AbstractExecutionStep typeCountStep = tryCreateTypeCountOptimization(context);
    if (typeCountStep != null)
      return typeCountStep;

    // Special case: RETURN without MATCH (standalone expressions)
    // E.g., RETURN abs(-42), RETURN 1+1
    if (statement.getMatchClauses().isEmpty() && statement.getReturnClause() != null &&
        clausesInOrder.stream().noneMatch(c -> c.getType() == ClauseEntry.ClauseType.UNWIND)) {
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

    // Process clauses in order
    for (final ClauseEntry entry : clausesInOrder) {
      switch (entry.getType()) {
        case UNWIND:
          final UnwindClause unwindClause = entry.getTypedClause();
          final UnwindStep unwindStep =
              new UnwindStep(unwindClause, context, functionFactory);
          if (currentStep != null) {
            unwindStep.setPrevious(currentStep);
          }
          currentStep = unwindStep;
          break;

        case MATCH:
          final MatchClause matchClause = entry.getTypedClause();
          currentStep = buildMatchStep(matchClause, currentStep, context, boundVariables);
          break;

        case WITH:
          final WithClause withClause = entry.getTypedClause();
          currentStep = buildWithStep(withClause, currentStep, context, functionFactory);
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
            final CreateStep createStep = new CreateStep(createClause, context);
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
      if (statement.getReturnClause().hasAggregations()) {
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
        final ProjectReturnStep returnStep = new ProjectReturnStep(statement.getReturnClause(), context, functionFactory);
        returnStep.setPrevious(currentStep);
        currentStep = returnStep;
      }
    }

    // ORDER BY
    if (statement.getOrderByClause() != null && currentStep != null) {
      final OrderByStep orderByStep =
          new OrderByStep(statement.getOrderByClause(), context, functionFactory);
      orderByStep.setPrevious(currentStep);
      currentStep = orderByStep;
    }

    // SKIP
    if (statement.getSkip() != null && currentStep != null) {
      final SkipStep skipStep =
          new SkipStep(statement.getSkip(), context);
      skipStep.setPrevious(currentStep);
      currentStep = skipStep;
    }

    // LIMIT
    if (statement.getLimit() != null && currentStep != null) {
      final LimitStep limitStep =
          new LimitStep(statement.getLimit(), context);
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
      AbstractExecutionStep currentStep, final CommandContext context, final CypherFunctionFactory functionFactory) {
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
      final OrderByStep orderByStep =
          new OrderByStep(withClause.getOrderByClause(), context, functionFactory);
      if (currentStep != null)
        orderByStep.setPrevious(currentStep);
      currentStep = orderByStep;

      // Chain SKIP/LIMIT after ORDER BY so pagination happens after sorting
      if (withClause.getSkip() != null) {
        final SkipStep skipStep = new SkipStep(withClause.getSkip(), context);
        skipStep.setPrevious(currentStep);
        currentStep = skipStep;
      }
      if (withClause.getLimit() != null) {
        final LimitStep limitStep = new LimitStep(withClause.getLimit(), context);
        limitStep.setPrevious(currentStep);
        currentStep = limitStep;
      }
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
   * @param matchClause     the MATCH clause to build
   * @param currentStep     current step in the execution chain
   * @param context         command context
   * @param boundVariables  set of variable names already bound in previous steps (updated in-place)
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
    final WhereClause whereClause = matchClause.hasWhereClause() ? matchClause.getWhereClause() : statement.getWhereClause();

    AbstractExecutionStep matchChainStart = null;

    for (int patternIndex = 0; patternIndex < pathPatterns.size(); patternIndex++) {
      final PathPattern pathPattern = pathPatterns.get(patternIndex);

      if (pathPattern.isSingleNode()) {
        final NodePattern nodePattern = pathPattern.getFirstNode();
        final String variable = nodePattern.getVariable() != null ? nodePattern.getVariable() : ("n" + patternIndex);
        matchVariables.add(variable);

        // Check if this variable was already bound in a previous MATCH clause
        if (boundVariables.contains(variable)) {
          // Variable already bound - skip creating a new MatchNodeStep
          // The bound value will be used from the input result
          continue;
        }

        // OPTIMIZATION: Extract ID filter for this variable to avoid Cartesian product
        final String idFilter = extractIdFilter(whereClause, variable);
        final MatchNodeStep matchStep = new MatchNodeStep(variable, nodePattern, context, idFilter);

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
      } else if (pathPattern instanceof ShortestPathPattern) {
        // Handle shortestPath or allShortestPaths patterns
        final ShortestPathPattern shortestPathPattern = (ShortestPathPattern) pathPattern;
        final NodePattern sourceNode = pathPattern.getFirstNode();
        final NodePattern targetNode = pathPattern.getLastNode();
        final String sourceVar = sourceNode.getVariable() != null ? sourceNode.getVariable() : "a";
        final String targetVar = targetNode.getVariable() != null ? targetNode.getVariable() : "b";
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
          final MatchNodeStep sourceStep = new MatchNodeStep(sourceVar, sourceNode, context, sourceIdFilter);
          if (currentStep != null) {
            sourceStep.setPrevious(currentStep);
          }
          currentStep = sourceStep;
          matchVariables.add(sourceVar); // Track as bound for subsequent patterns
        }

        // Target node matching (if not already bound)
        if (!boundVariables.contains(targetVar) && !matchVariables.contains(targetVar)) {
          final String targetIdFilter = extractIdFilter(whereClause, targetVar);
          final MatchNodeStep targetStep = new MatchNodeStep(targetVar, targetNode, context, targetIdFilter);
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
        final NodePattern sourceNode = pathPattern.getFirstNode();
        final String sourceVar = sourceNode.getVariable() != null ? sourceNode.getVariable() : "a";

        // Check if source node variable is already bound (either from previous MATCH or
        // from being in the boundVariables set). Previously this only checked for
        // unlabeled/unpropertied nodes, which broke when labels were repeated.
        final boolean sourceAlreadyBound = stepBeforeMatch != null &&
            (boundVariables.contains(sourceVar) || (!sourceNode.hasLabels() && !sourceNode.hasProperties()));

        if (!sourceAlreadyBound) {
          matchVariables.add(sourceVar);

          // OPTIMIZATION: Extract ID filter for source variable to avoid Cartesian product
          final String sourceIdFilter = extractIdFilter(whereClause, sourceVar);
          final MatchNodeStep sourceStep = new MatchNodeStep(sourceVar, sourceNode, context, sourceIdFilter);

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
        } else {
          if (isOptional && matchChainStart == null) {
            currentStep = null;
          }
        }

        final String pathVariable = pathPattern.hasPathVariable() ? pathPattern.getPathVariable() : null;
        if (pathVariable != null) {
          matchVariables.add(pathVariable);
        }

        // Track current source variable through multi-hop patterns
        // For the first hop, use sourceVar; for subsequent hops, use the previous targetVar
        String currentSourceVar = sourceVar;

        for (int i = 0; i < pathPattern.getRelationshipCount(); i++) {
          final RelationshipPattern relPattern = pathPattern.getRelationship(i);
          final NodePattern targetNode = pathPattern.getNode(i + 1);
          final String relVar = relPattern.getVariable();
          final String targetVar = targetNode.getVariable() != null ? targetNode.getVariable() : ("n" + i);

          if (relVar != null && !relVar.isEmpty()) {
            matchVariables.add(relVar);
          }
          matchVariables.add(targetVar);

          AbstractExecutionStep nextStep;
          if (relPattern.isVariableLength()) {
            nextStep = new ExpandPathStep(currentSourceVar, pathVariable, targetVar, relPattern, context);
          } else {
            // Pass target node pattern for label filtering and bound variables
            // for identity checking on already-bound target variables
            nextStep = new MatchRelationshipStep(currentSourceVar, relVar, targetVar, relPattern, pathVariable,
                targetNode, boundVariables, context);
          }

          // Update source for next hop in multi-hop patterns
          currentSourceVar = targetVar;

          if (isOptional && matchChainStart == null) {
            matchChainStart = nextStep;
            currentStep = nextStep;
          } else if (sourceAlreadyBound && currentStep == null) {
            nextStep.setPrevious(stepBeforeMatch);
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
          new OptionalMatchStep(matchChainStart, matchVariables, context);

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
    final CypherFunctionFactory functionFactory = expressionEvaluator != null ? expressionEvaluator.getFunctionFactory() : null;

    // OPTIMIZATION: Check for simple COUNT(*) pattern that can use Type.count() O(1) operation
    // Pattern: MATCH (a:TypeName) RETURN COUNT(a) as alias
    final AbstractExecutionStep typeCountStep = tryCreateTypeCountOptimization(context);
    if (typeCountStep != null)
      return typeCountStep;

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
            final String sourceVar = sourceNode.getVariable() != null ? sourceNode.getVariable() : "a";
            final String targetVar = targetNode.getVariable() != null ? targetNode.getVariable() : "b";
            final String pathVariable = pathPattern.hasPathVariable() ? pathPattern.getPathVariable() : null;

            // Track path variable
            if (pathVariable != null) {
              matchVariables.add(pathVariable);
            }

            // Check both legacyBoundVariables (from previous MATCH clauses) and matchVariables (from earlier
            // patterns in this same MATCH clause) to avoid re-matching already-bound variables

            // Source node matching (if not already bound)
            if (!legacyBoundVariables.contains(sourceVar) && !matchVariables.contains(sourceVar)) {
              final WhereClause matchWhere = matchClause.hasWhereClause() ? matchClause.getWhereClause() : statement.getWhereClause();
              final String sourceIdFilter = extractIdFilter(matchWhere, sourceVar);
              final MatchNodeStep sourceStep = new MatchNodeStep(sourceVar, sourceNode, context, sourceIdFilter);
              if (currentStep != null) {
                sourceStep.setPrevious(currentStep);
              }
              currentStep = sourceStep;
              matchVariables.add(sourceVar);
            }

            // Target node matching (if not already bound)
            if (!legacyBoundVariables.contains(targetVar) && !matchVariables.contains(targetVar)) {
              final WhereClause matchWhere = matchClause.hasWhereClause() ? matchClause.getWhereClause() : statement.getWhereClause();
              final String targetIdFilter = extractIdFilter(matchWhere, targetVar);
              final MatchNodeStep targetStep = new MatchNodeStep(targetVar, targetNode, context, targetIdFilter);
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
            final String variable = nodePattern.getVariable() != null ? nodePattern.getVariable() : ("n" + patternIndex);
            matchVariables.add(variable); // Track variable for OPTIONAL MATCH

            // Check if this variable was already bound in a previous MATCH clause
            if (legacyBoundVariables.contains(variable)) {
              // Variable already bound - skip creating a new MatchNodeStep
              continue;
            }

            // OPTIMIZATION: Extract ID filter from WHERE clause (if present) for pushdown
            final WhereClause matchWhere = matchClause.hasWhereClause() ? matchClause.getWhereClause() : statement.getWhereClause();
            final String idFilter = extractIdFilter(matchWhere, variable);
            final MatchNodeStep matchStep = new MatchNodeStep(variable, nodePattern, context, idFilter);

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
            // Check both legacy bound variables AND the old heuristic (no labels/properties)
            final boolean sourceAlreadyBound = stepBeforeMatch != null &&
                (legacyBoundVariables.contains(sourceVar) || (!sourceNode.hasLabels() && !sourceNode.hasProperties()));

            if (!sourceAlreadyBound) {
              // Only track the source variable if we're creating a new binding for it
              matchVariables.add(sourceVar);

              // OPTIMIZATION: Extract ID filter from WHERE clause (if present) for pushdown
              final WhereClause matchWhere = matchClause.hasWhereClause() ? matchClause.getWhereClause() : statement.getWhereClause();
              final String sourceIdFilter = extractIdFilter(matchWhere, sourceVar);

              // Start with source node (or chain if we have previous patterns)
              final MatchNodeStep sourceStep = new MatchNodeStep(sourceVar, sourceNode, context, sourceIdFilter);

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

            // Track current source variable through multi-hop patterns
            // For the first hop, use sourceVar; for subsequent hops, use the previous targetVar
            String currentSourceVar = sourceVar;

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
                nextStep = new ExpandPathStep(currentSourceVar, pathVariable, targetVar, relPattern, context);
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
            // Pass matchChainStart (first step) not currentStep (last step)
            // because OptionalMatchStep needs to feed input into the start of the chain
            final OptionalMatchStep optionalStep =
                new OptionalMatchStep(matchChainStart, matchVariables, context);

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
        final OrderByStep orderByStep =
            new OrderByStep(withClause.getOrderByClause(), context, functionFactory);
        if (currentStep != null)
          orderByStep.setPrevious(currentStep);
        currentStep = orderByStep;

        // Chain SKIP/LIMIT after ORDER BY so pagination happens after sorting
        if (withClause.getSkip() != null) {
          final SkipStep skipStep = new SkipStep(withClause.getSkip(), context);
          skipStep.setPrevious(currentStep);
          currentStep = skipStep;
        }
        if (withClause.getLimit() != null) {
          final LimitStep limitStep = new LimitStep(withClause.getLimit(), context);
          limitStep.setPrevious(currentStep);
          currentStep = limitStep;
        }
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
      // Check if RETURN contains aggregation functions
      if (statement.getReturnClause().hasAggregations()) {
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
        final ProjectReturnStep returnStep = new ProjectReturnStep(statement.getReturnClause(), context, functionFactory);
        returnStep.setPrevious(currentStep);
        currentStep = returnStep;
      }
    }

    // Step 8: ORDER BY clause - sort results
    if (statement.getOrderByClause() != null && currentStep != null) {
      final OrderByStep orderByStep = new OrderByStep(
          statement.getOrderByClause(), context, functionFactory);
      orderByStep.setPrevious(currentStep);
      currentStep = orderByStep;
    }

    // Step 9: SKIP clause - skip first N results
    if (statement.getSkip() != null && currentStep != null) {
      final SkipStep skipStep = new SkipStep(
          statement.getSkip(), context);
      skipStep.setPrevious(currentStep);
      currentStep = skipStep;
    }

    // Step 10: LIMIT clause - limit number of results
    if (statement.getLimit() != null && currentStep != null) {
      final LimitStep limitStep = new LimitStep(
          statement.getLimit(), context);
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
   *
   * @param whereClause the WHERE clause to analyze
   * @param variable the variable to extract ID filter for
   * @return the ID value to filter by, or null if no ID filter found
   */
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
