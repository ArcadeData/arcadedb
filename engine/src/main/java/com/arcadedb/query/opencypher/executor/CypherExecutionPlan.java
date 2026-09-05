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
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.function.graph.IdFunction;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.opencypher.ast.AllReduceExpression;
import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.BooleanCoercionExpression;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.CallClause;
import com.arcadedb.query.opencypher.ast.CaseAlternative;
import com.arcadedb.query.opencypher.ast.CaseExpression;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CollectExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpressionWrapper;
import com.arcadedb.query.opencypher.ast.CountExpression;
import com.arcadedb.query.opencypher.ast.CreateClause;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.DeleteClause;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.ExistsExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.ForeachClause;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.IsNullExpression;
import com.arcadedb.query.opencypher.ast.LabelCheckExpression;
import com.arcadedb.query.opencypher.ast.ListComprehensionExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.ListIndexExpression;
import com.arcadedb.query.opencypher.ast.ListPredicateExpression;
import com.arcadedb.query.opencypher.ast.ListSliceExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LoadCSVClause;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.MapExpression;
import com.arcadedb.query.opencypher.ast.MapProjectionExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.MergeClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PatternComprehensionExpression;
import com.arcadedb.query.opencypher.ast.PatternPredicateExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.QuantifiedPathPattern;
import com.arcadedb.query.opencypher.ast.ReduceExpression;
import com.arcadedb.query.opencypher.ast.RegexExpression;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.RemoveClause;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.ast.ShortestPathExpression;
import com.arcadedb.query.opencypher.ast.ShortestPathPattern;
import com.arcadedb.query.opencypher.ast.StarExpression;
import com.arcadedb.query.opencypher.ast.StringMatchExpression;
import com.arcadedb.query.opencypher.ast.SubqueryClause;
import com.arcadedb.query.opencypher.ast.TernaryLogicalExpression;
import com.arcadedb.query.opencypher.ast.UnionStatement;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.executor.operators.GAVFusedChainOperator;
import com.arcadedb.query.opencypher.executor.operators.InListValues;
import com.arcadedb.query.opencypher.executor.steps.AggregationStep;
import com.arcadedb.query.opencypher.executor.steps.AntiJoinChainOp;
import com.arcadedb.query.opencypher.executor.steps.CSRCountStep;
import com.arcadedb.query.opencypher.executor.steps.CallStep;
import com.arcadedb.query.opencypher.executor.steps.ConstantCountStep;
import com.arcadedb.query.opencypher.executor.steps.CountChainedEdgesStep;
import com.arcadedb.query.opencypher.executor.steps.CountEdgesReturnStep;
import com.arcadedb.query.opencypher.executor.steps.CountEdgesStep;
import com.arcadedb.query.opencypher.executor.steps.CountOp;
import com.arcadedb.query.opencypher.executor.steps.CreateStep;
import com.arcadedb.query.opencypher.executor.steps.DegreeProductOp;
import com.arcadedb.query.opencypher.executor.steps.DeleteStep;
import com.arcadedb.query.opencypher.executor.steps.ExpandPathStep;
import com.arcadedb.query.opencypher.executor.steps.FilterPropertiesStep;
import com.arcadedb.query.opencypher.executor.steps.FinalProjectionStep;
import com.arcadedb.query.opencypher.executor.steps.ForeachStep;
import com.arcadedb.query.opencypher.executor.steps.GroupByAggregationStep;
import com.arcadedb.query.opencypher.executor.steps.IndexSeekStep;
import com.arcadedb.query.opencypher.executor.steps.LimitStep;
import com.arcadedb.query.opencypher.executor.steps.LoadCSVStep;
import com.arcadedb.query.opencypher.executor.steps.MatchEdgeByIndexStep;
import com.arcadedb.query.opencypher.executor.steps.MatchNodeStep;
import com.arcadedb.query.opencypher.executor.steps.MatchRelationshipStep;
import com.arcadedb.query.opencypher.executor.steps.MergeStep;
import com.arcadedb.query.opencypher.executor.steps.OptionalMatchStep;
import com.arcadedb.query.opencypher.executor.steps.OrderByStep;
import com.arcadedb.query.opencypher.executor.steps.PairHashJoinOp;
import com.arcadedb.query.opencypher.executor.steps.PartitionedTriangleOp;
import com.arcadedb.query.opencypher.executor.steps.ProjectReturnStep;
import com.arcadedb.query.opencypher.executor.steps.PropagateChainOp;
import com.arcadedb.query.opencypher.executor.steps.QuantifiedPathStep;
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
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.opencypher.rewriter.ExpressionRewriter;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.query.sql.parser.ExplainResultSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * Execution plan for a Cypher query.
 * Contains the chain of execution steps and executes them.
 * <p>
 * Phase 4: Enhanced with Cost-Based Query Optimizer support.
 */
public class CypherExecutionPlan {
  /**
   * The column a count push-down publishes under when it is answering "how many rows does this statement produce"
   * rather than a {@code count()} the statement projects. Space-prefixed the way every internal name on a row is,
   * so no Cypher identifier can collide with it.
   */
  private static final String ROW_COUNT_ALIAS = " rowCount";

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
    return execute(null);
  }

  /**
   * Executes the query plan as part of an enclosing statement, whose context supplies what a nested plan must
   * share rather than re-derive - today the statement clock, so that every {@code timestamp()} and temporal
   * constructor across the whole statement answers from one frozen instant (issue #7052).
   *
   * @param outerContext the enclosing statement's context, or {@code null} for a top-level statement
   *
   * @return result set
   */
  public ResultSet execute(final CommandContext outerContext) {
    // Handle UNION queries specially
    if (unionSubqueryPlans != null && !unionSubqueryPlans.isEmpty())
      return executeUnion(outerContext);

    // Build execution context
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    setupFunctionResolver(context);
    CypherFunctionHelper.inheritStatementTime(context, outerContext);

    AbstractExecutionStep rootStep;

    // FAST PATH: Specialized count-push-down optimizations.
    // Must be checked BEFORE the optimizer dispatch, because the optimizer produces
    // GAVExpandAll operators that still materialize individual rows (O(paths) memory).
    rootStep = tryCountPushDown(context, false);

    if (rootStep == null) {
      // Phase 4: Use optimized physical plan if available
      // Use pre-computed flags from the cached CypherStatement to avoid scanning clause lists per execution
      if (canUseOptimizedPhysicalPlan()) {
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
      // Materialize the ResultSet to force write operation execution. The drain is the statement, so the
      // command deadline is tested here too - nothing downstream of it can (issue #6266).
      final WorkGuard guard = WorkGuard.forCommandDeadline(context);
      final List<ResultInternal> materializedResults = new ArrayList<>();
      while (resultSet.hasNext()) {
        guard.check();
        materializedResults.add((ResultInternal) resultSet.next());
      }
      // Surface the CRUD-count accumulator built up by the mutation steps (CreateStep, SetStep,
      // DeleteStep, RemoveStep, MergeStep) on the returned result set. Always present after a
      // write statement, even if it performed no actual mutation (containsUpdates() is false then).
      final QueryStatistics stats = context.getStatistics();

      // A bare CALL (no YIELD/RETURN, e.g. a write-classified custom function or write procedure) is its
      // own projection: CallStep already yields its implicit "all columns" row, so a missing RETURN clause
      // here does not mean "side effects only" the way it does for CREATE/SET/DELETE/MERGE/REMOVE. Issue #6446.
      // Deliberately scoped to a CALL that is the statement's ONLY clause: a CALL chained after other
      // clauses (e.g. WITH ... CALL proc() YIELD x, or multiple chained CALLs), still with no trailing
      // RETURN, is a separate, narrower gap left for #6450 rather than widened here.
      final List<ClauseEntry> clauses = statement.getClausesInOrder();
      final boolean bareCallOwnsProjection = clauses.size() == 1 && clauses.get(0).getType() == ClauseEntry.ClauseType.CALL;

      // If no RETURN clause (or GQL FINISH was used), return empty results
      // (write side effects still happened). Issue #3365 section 1.3.
      if (statement.hasFinishClause() || (statement.getReturnClause() == null && !bareCallOwnsProjection)) {
        final IteratorResultSet empty = new IteratorResultSet(Collections.<Result>emptyList().iterator());
        empty.setStatistics(stats);
        return empty;
      }
      // Return the materialized results
      final IteratorResultSet out = new IteratorResultSet(materializedResults.iterator());
      out.setStatistics(stats);
      return out;
    }

    // Read-only path: GQL FINISH still suppresses any rows the MATCH would have produced.
    if (statement.hasFinishClause()) {
      final WorkGuard guard = WorkGuard.forCommandDeadline(context);
      while (resultSet.hasNext()) {
        guard.check();
        resultSet.next();
      }
      return new IteratorResultSet(Collections.<Result>emptyList().iterator());
    }

    return resultSet;
  }

  /**
   * Makes a nested plan share the enclosing command's deadline instead of starting a fresh budget from now.
   * Without this a statement could buy itself unlimited time by nesting: every {@code CALL { }} body, every
   * correlated {@code COUNT { }} probe and every UNION branch runs on a context of its own (issue #6266).
   * <p>
   * What reaching the deadline means travels with it: an outer {@code TIMEOUT n RETURN} must end the nested
   * plan's rows too, not abort it with the exception the clause promised not to raise (issue #6304).
   */
  private static void inheritCommandDeadline(final BasicCommandContext context, final CommandContext outerContext) {
    if (outerContext != null)
      context.setCommandDeadline(outerContext.getCommandDeadline(), outerContext.getCommandDeadlineDescription(),
          outerContext.isCommandDeadlinePartial());
  }

  private boolean canUseOptimizedPhysicalPlan() {
    return physicalPlan != null && physicalPlan.getRootOperator() != null
        && !statement.hasUnwindBeforeMatch() && !statement.hasSubquery()
        && !statement.hasWithBeforeMatch()
        && !statement.hasWriteBeforeMatch();
  }

  /**
   * Executes the query plan seeded with an initial input row.
   * Used by CALL subqueries to inject outer scope variables into the inner query.
   * The seed row provides variables that the inner query's WITH clause can import.
   *
   * @param seedRow      the initial row providing outer scope variables
   * @param outerContext the outer command context whose QueryStatistics accumulator is shared
   *                     with the inner query's context, so writes performed inside the CALL
   *                     subquery are folded into the outer plan's statistics. May be {@code null}.
   *
   * @return result set from the inner query execution
   */
  public ResultSet executeWithSeedRow(final Result seedRow, final CommandContext outerContext) {
    // Handle UNION inside CALL subqueries: execute each branch with the seed row
    if (statement instanceof UnionStatement unionStmt) {
      final List<CypherExecutionPlan> branchPlans = new ArrayList<>();
      for (final CypherStatement branch : unionStmt.getQueries())
        branchPlans.add(new CypherExecutionPlan(database, branch, parameters, configuration, null, expressionEvaluator));

      final boolean removeDuplicates = !unionStmt.isAllUnionAll();
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(database);
      ctx.setInputParameters(parameters);
      setupFunctionResolver(ctx);
      // The deadline is inherited because the WorkGuard below reads it; nothing else is, because nothing else
      // is read. No expression is ever evaluated against this ctx - each branch re-enters this method with the
      // REAL outerContext - so inheriting the statement clock here would be dead weight.
      inheritCommandDeadline(ctx, outerContext);

      // Execute each branch with the seed row, collect all results
      final WorkGuard unionGuard = WorkGuard.forCommandDeadline(ctx);
      final List<ResultInternal> allResults = new ArrayList<>();
      final Set<String> seen = removeDuplicates ? new HashSet<>() : null;
      for (final CypherExecutionPlan branchPlan : branchPlans) {
        // No inherit() here on purpose: the branch runs through this same method with the REAL outerContext,
        // so it takes the non-union path below and does its own inherit under the same gate. A second copy
        // here would be one more thing to keep in step with that gate (issue #6977).
        final ResultSet rs = branchPlan.executeWithSeedRow(seedRow, outerContext);
        while (rs.hasNext()) {
          unionGuard.check();
          final Result row = rs.next();
          if (removeDuplicates) {
            final String key = buildResultKey(row);
            if (!seen.add(key))
              continue;
          }
          final ResultInternal copy = new ResultInternal();
          for (final String prop : row.getPropertyNames())
            copy.setProperty(prop, row.getProperty(prop));
          allResults.add(copy);
        }
      }
      return new IteratorResultSet(allResults.iterator());
    }

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    setupFunctionResolver(context);
    // Only share the outer statistics accumulator for a write CALL body: getStatistics() lazily
    // allocates, so sharing it unconditionally would allocate a QueryStatistics even for a
    // fully read-only CALL, violating the "read queries allocate nothing" constraint.
    if (outerContext != null && !statement.isReadOnly()) {
      context.setStatistics(outerContext.getStatistics());
      // A label write moves the record, so the map of what it displaced has to outlive this plan: the body of a
      // CALL { } is re-planned for every outer row, and without this the second row met the vertex the first one
      // deleted (issue #6977). Same gate as the statistics above - a read-only body performs no label write and
      // must not make the enclosing statement allocate a map for one.
      LabelReplacements.inherit(context, outerContext);
    }
    inheritCommandDeadline(context, outerContext);
    CypherFunctionHelper.inheritStatementTime(context, outerContext);

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
      rootStep = buildExecutionStepsWithOrder(context, clausesInOrder, seedStep, seedCorrelationOf(seedRow),
          seedRow.getPropertyNames());
    else
      rootStep = seedStep; // Fallback: just return the seed

    if (rootStep == null)
      return new IteratorResultSet(new ArrayList<ResultInternal>().iterator());

    return rootStep.syncPull(context, 100);
  }

  /**
   * How many rows this statement produces when seeded with {@code seedRow} - what {@code COUNT { }} asks for.
   * <p>
   * A {@code COUNT { }} body with no {@code RETURN} of its own is normalised to one row per match, and the
   * expression counted those rows: {@code COUNT { MATCH (m:Big) }} materialised a full scan for a number the type
   * counter already holds, while the same question written as {@code COLLECT { MATCH (m:Big) RETURN count(m) }} was
   * answered in O(1) (issue #5715). When the body's row count <b>is</b> its match count, the same two push-downs
   * answer it; otherwise the rows are produced and counted exactly as before.
   *
   * @param seedRow      the outer row the body is correlated to, empty when there is none
   * @param outerContext the enclosing command context, may be null
   */
  public long countRows(final Result seedRow, final CommandContext outerContext) {
    final Long pushedDown = tryCountRowsPushDown(seedRow, outerContext);
    if (pushedDown != null)
      return pushedDown;

    final WorkGuard guard = WorkGuard.forCommandDeadline(outerContext);
    long count = 0L;
    try (final ResultSet resultSet = executeWithSeedRow(seedRow, outerContext)) {
      while (resultSet.hasNext()) {
        guard.check();
        resultSet.next();
        count++;
      }
    }
    return count;
  }

  /**
   * The row count read straight off a count push-down, or null when no push-down applies and the rows have to be
   * produced.
   * <p>
   * {@code SKIP} and {@code LIMIT} disqualify the body here rather than being applied: they cut the rows the body
   * produces, and a push-down never produces them, so the arithmetic that would relate the two is not the one
   * {@link #applySkipAndLimit} does to the single row a count comes back as.
   */
  private Long tryCountRowsPushDown(final Result seedRow, final CommandContext outerContext) {
    if (statement instanceof UnionStatement)
      return null;

    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    if (clausesInOrder == null || clausesInOrder.isEmpty())
      return null;
    if (statement.getSkip() != null || statement.getLimit() != null)
      return null;

    final SeedCorrelation correlation = seedCorrelationOf(seedRow);
    if (!correlation.isSeedable())
      return null;

    // The outer statistics accumulator is deliberately not shared, for the reason executeWithSeedRow shares it only
    // for a write body: getStatistics() allocates lazily, and a count push-down writes nothing there is to count.
    // A COUNT { } body cannot write at all - the parser rejects an update clause inside one - so there is no
    // mutation this could drop from the enclosing plan's statistics.
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    setupFunctionResolver(context);
    inheritCommandDeadline(context, outerContext);

    final AbstractExecutionStep countStep = tryCountPushDown(context, true, correlation);
    if (countStep == null)
      return null;

    try (final ResultSet resultSet = countStep.syncPull(context, 1)) {
      if (!resultSet.hasNext())
        return null;
      final Object value = resultSet.next().getProperty(ROW_COUNT_ALIAS);
      return value instanceof Number number ? number.longValue() : null;
    }
  }

  /**
   * <b>Which</b> of the names the seed row carries this statement could read, and the row that bound them - what the
   * count push-downs need in order to decide whether the correlation is one they can start from.
   * <p>
   * The question is asked because of the two count push-downs in {@link #buildExecutionStepsWithOrder}: they answer
   * from the schema and the CSR arrays and never look at the incoming rows, so they are only valid for a body that
   * does not read one, <i>or</i> for one whose every read name is a position the operator can seed itself from.
   * Asking whether the seed row <b>carries</b> a variable, rather than whether the body <b>reads</b> one, was correct
   * but coarse - an uncorrelated body written inside a correlated query lost an O(1) count for a name it never
   * mentions, once per outer row (issue #5686). Asking only <i>whether</i> a name is read, rather than <i>which</i>,
   * was the next step of the same coarseness: a genuinely correlated body lost the count too, where a bound anchor
   * makes it an O(degree) read of the adjacency arrays rather than a scan (issue #5758).
   * <p>
   * {@link CypherReferencedVariables} is deliberately unsure by default, and an incomplete answer is reported here as
   * {@link SeedCorrelation#isSeedable()} {@code == false}: no push-down at all, exactly as before. Only a complete
   * answer can name the positions to seed, and naming them wrongly is what would give a wrong count.
   * <p>
   * The {@code statement} read here is <b>this plan's own</b>, and this plan is only ever built around the body -
   * {@link #executeWithSeedRow} is what a {@code CALL { }} clause and the three subquery expressions call, each on a
   * plan constructed over the body they hold. Asking the enclosing query instead would compare the seed against the
   * names of whoever produced it, which every seeded row would match: the question has to be put to the statement
   * that is about to ignore the seed.
   */
  private SeedCorrelation seedCorrelationOf(final Result seedRow) {
    if (seedRow == null)
      return SeedCorrelation.UNCORRELATED;

    final Set<String> seedNames = seedRow.getPropertyNames();
    // Answering this case here is what keeps a body seeded with nothing - the common one, a CALL { } importing
    // nothing - from forcing the collection over the statement at all.
    if (seedNames.isEmpty())
      return SeedCorrelation.UNCORRELATED;

    final CypherReferencedVariables referenced = statement.getReferencedVariables();
    if (!referenced.isComplete())
      return SeedCorrelation.UNKNOWN;

    // Built without a mutable set for the shape that is almost always the whole of it - one seeded name read - since
    // this runs once per outer row, including for the correlations that end up refused.
    Set<String> readNames = null;
    for (final String seedName : seedNames) {
      if (!referenced.getNames().contains(seedName))
        continue;
      if (readNames == null)
        readNames = Set.of(seedName);
      else {
        final Set<String> more = new HashSet<>(readNames);
        more.add(seedName);
        readNames = more;
      }
    }

    return readNames == null ? SeedCorrelation.UNCORRELATED : new SeedCorrelation(seedRow, readNames);
  }

  /**
   * How a seeded body relates to the row it was handed: not correlated at all, correlated through named positions a
   * push-down may be able to seed itself from, or correlated in a way that cannot be described.
   * <p>
   * The three are kept apart rather than folded into a boolean because they lead to three different plans.
   * {@link #UNCORRELATED} keeps every push-down, {@link #UNKNOWN} keeps none, and a named correlation keeps only the
   * ones that can start from a bound anchor.
   */
  private static final class SeedCorrelation {
    /** The body reads none of the seeded names, so the seed cannot change its answer (issue #5686). */
    static final SeedCorrelation UNCORRELATED = new SeedCorrelation(null, Set.of());
    /** The body's shape is one {@link CypherReferencedVariables} does not model, so what it reads is not known. */
    static final SeedCorrelation UNKNOWN      = new SeedCorrelation(null, null);

    private final Result      seedRow;
    private final Set<String> readNames;

    private SeedCorrelation(final Result seedRow, final Set<String> readNames) {
      this.seedRow = seedRow;
      this.readNames = readNames;
    }

    /** Whether a push-down may be built at all: false only for a correlation that could not be described. */
    boolean isSeedable() {
      return readNames != null;
    }

    /** Whether the body reads any seeded name. An unknown correlation answers yes, as the coarse guard did. */
    boolean isCorrelated() {
      return readNames == null || !readNames.isEmpty();
    }

    /** The seeded names the body reads. Empty when uncorrelated; never asked when unknown. */
    Set<String> readNames() {
      return readNames;
    }

    /** The value the outer row bound {@code name} to. */
    Object boundValue(final String name) {
      return seedRow.getProperty(name);
    }
  }

  private static String buildResultKey(final Result result) {
    final StringBuilder sb = new StringBuilder();
    for (final String prop : result.getPropertyNames()) {
      sb.append(prop).append("=");
      final Object value = result.getProperty(prop);
      sb.append(value == null ? "null" : value.toString());
      sb.append("|");
    }
    return sb.toString();
  }

  /**
   * Executes a UNION query by combining results from all subqueries.
   *
   * @param outerContext the enclosing statement's context, or {@code null} for a top-level statement
   *
   * @return combined result set
   */
  private ResultSet executeUnion(final CommandContext outerContext) {
    // Use UnionStep to combine results from all subqueries
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    setupFunctionResolver(context);
    // Every branch runs on a context of its own, so the statement clock has to travel from here into each of
    // them - and into here from an enclosing statement when this UNION is a CALL body (issue #7052).
    CypherFunctionHelper.inheritStatementTime(context, outerContext);

    final UnionStep unionStep =
        new UnionStep(unionSubqueryPlans, unionRemoveDuplicates, context);

    // Read UNION: stay lazy/streaming, no statistics to surface.
    if (statement.isReadOnly())
      return unionStep.syncPull(context, 100);

    // Write UNION: materialize to force each branch's mutation steps to execute (mirroring the
    // non-union write path), then surface the summed per-branch statistics.
    final ResultSet rs = unionStep.syncPull(context, 100);
    final List<Result> rows = new ArrayList<>();
    try {
      while (rs.hasNext())
        rows.add(rs.next());
    } finally {
      rs.close();
    }
    final IteratorResultSet out = new IteratorResultSet(rows.iterator());
    // Always attach the accumulator for a write UNION, even when no branch actually mutated
    // anything (aggregated.containsUpdates() false then): presence signals "this was a write",
    // mirroring the non-union write path above.
    out.setStatistics(unionStep.getAggregatedStatistics());
    return out;
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

    // The chain described below, kept so a client reading the plan as data sees what the text shows.
    List<ExecutionStep> describedSteps = Collections.emptyList();

    // The push-down is asked FIRST because execute() asks it first: it replaces the whole chain, so a query it
    // claims never reaches the optimizer, and describing that query by the physical plan the optimizer built for it
    // names a plan the engine does not run. That gap predates this issue, which widens it by routing the plainest
    // counting queries through the fast path as well (issue #5715).
    final AbstractExecutionStep countPushDown = countPushDownForDescription();
    if (countPushDown != null) {
      explainOutput.append("Using Count Push-Down\n\n");
      explainOutput.append("Execution Plan:\n");
      appendStepChain(explainOutput, countPushDown);
      explainOutput.append("\n");
      describedSteps = stepChainOf(countPushDown);
    }

    if (canUseOptimizedPhysicalPlan()) {
      explainOutput.append(countPushDown != null
          ? "Using Cost-Based Query Optimizer (superseded by the count push-down above)\n\n"
          : "Using Cost-Based Query Optimizer\n\n");
      explainOutput.append("Physical Plan:\n");
      explainOutput.append(physicalPlan.getRootOperator().explain(0));
      explainOutput.append("\n");
      explainOutput.append(String.format("Total Estimated Cost: %.2f\n", physicalPlan.getTotalEstimatedCost()));
      explainOutput.append(String.format("Total Estimated Rows: %d\n", physicalPlan.getTotalEstimatedCardinality()));
      if (countPushDown == null) {
        // Not a second optimization: buildExecutionStepsWithOptimizer wraps THIS plan's physicalPlan - the same
        // instance the text above is printed from - so the structured steps and the text cannot describe two
        // different plans.
        try {
          describedSteps = stepChainOf(stepsForDescription());
          appendStepsAfterTheOptimizedMatch(explainOutput, describedSteps);
        } catch (final Exception e) {
          explainOutput.append("\n");
          appendPlanBuildFailure(explainOutput, e);
        }
      }
    } else if (isUnion()) {
      // A UNION has no plan of its own to be optimized or not: the planner leaves its physicalPlan null and plans
      // each branch on its own, so a branch below can perfectly well be the optimizer's while the union is not.
      // Saying "not yet supported by optimizer" here contradicted the `+ OPTIMIZED MATCH` printed underneath it.
      explainOutput.append("Using Per-Branch Planning (UNION)\n\n");
      explainOutput.append("Reason: each branch of a UNION is planned on its own - see the branches below\n\n");
      describedSteps = appendPlanDescription(explainOutput);
    } else if (countPushDown == null) {
      explainOutput.append("Using Traditional Execution (Non-Optimized)\n\n");
      explainOutput.append("Reason: Query pattern not yet supported by optimizer\n\n");
      describedSteps = appendPlanDescription(explainOutput);
    }

    return new ExplainResultSet(new OpenCypherExplainExecutionPlan(explainOutput.toString(), describedSteps, -1));
  }

  /** Whether this plan is a UNION, which has no plan of its own: it runs the plan of each of its branches. */
  private boolean isUnion() {
    return unionSubqueryPlans != null && !unionSubqueryPlans.isEmpty();
  }

  /**
   * Describes the chain this plan would run, and returns it.
   * <p>
   * EXPLAIN used to stop at the reason above, which left the one command whose entire purpose is inspecting a plan
   * WITHOUT running it strictly less informative than PROFILE - and PROFILE is no workaround for a slow query, nor
   * for a writing one, since it executes (issue #6323). The steps are the ones {@link #execute()} builds, and they
   * are built and never pulled: construction reads the schema, the work is all in {@code syncPull}.
   * <p>
   * A UNION is answered branch by branch, so it is described branch by branch: each sub-plan describes the chain it
   * would run, which for a branch the optimizer claims is that branch's own answer.
   */
  private List<ExecutionStep> appendPlanDescription(final StringBuilder output) {
    if (isUnion()) {
      output.append("Execution Plan:\n");
      output.append("+ UNION").append(unionRemoveDuplicates ? "" : " ALL")
          .append(" (").append(unionSubqueryPlans.size()).append(" queries)\n");

      final List<ExecutionStep> allSteps = new ArrayList<>();
      for (int i = 0; i < unionSubqueryPlans.size(); i++) {
        output.append("  Branch ").append(i + 1).append(":\n");
        final StringBuilder branchOutput = new StringBuilder();
        allSteps.addAll(unionSubqueryPlans.get(i).appendPlanDescription(branchOutput));
        output.append(branchOutput.toString().indent(2));
      }
      return allSteps;
    }

    final AbstractExecutionStep rootStep;
    try {
      rootStep = stepsForDescription();
    } catch (final Exception e) {
      appendPlanBuildFailure(output, e);
      return Collections.emptyList();
    }

    if (rootStep == null) {
      // The statement built no chain at all, which execute() answers with an empty result set. This used to read
      // "Execution will use step-by-step interpretation" - a route-selection message, true when this branch was
      // reached without ever building anything, and misleading now that it means there was nothing to describe.
      output.append("No execution steps: this statement produces no rows to execute\n");
      return Collections.emptyList();
    }

    output.append("Execution Plan:\n");
    appendStepChain(output, rootStep);
    return stepChainOf(rootStep);
  }

  /**
   * Reports a plan that could not be built as part of the description, rather than as a failed EXPLAIN or as
   * nothing at all.
   * <p>
   * EXPLAIN answers a question about a query, so a query that cannot be planned is an answer to it: the statement
   * would fail the same way when run, and naming the failure beats both raising it - which would leave the user
   * with no plan and no reason - and swallowing it into a log line nobody has enabled (issue #6323). The stack
   * trace still goes to the log, since only the message belongs in a plan.
   * <p>
   * The message tells the caller nothing running the same statement would not: {@link #execute()} builds this very
   * chain and lets what it throws reach the client. EXPLAIN needs no privilege beyond running the query it
   * describes, so there is no audience here that could not have obtained the same message by asking directly.
   */
  private void appendPlanBuildFailure(final StringBuilder output, final Exception cause) {
    LogManager.instance().log(this, Level.FINE, "Error on building the execution plan to describe it", cause);
    output.append("Execution plan not available: ")
        .append(cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName())
        .append("\n");
  }

  /**
   * The step chain this query would run, built only to be described, or null when the statement produces none.
   * Building it is the same work {@link #execute()} does before pulling anything, so it fails on the statements
   * that would fail there: the callers report that failure instead of propagating it.
   */
  private AbstractExecutionStep stepsForDescription() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    setupFunctionResolver(context);
    return canUseOptimizedPhysicalPlan() ? buildExecutionStepsWithOptimizer(context) : buildExecutionSteps(context);
  }

  /**
   * Appends the steps that run after the optimized MATCH, which the physical plan does not describe.
   * <p>
   * The optimizer claims the MATCH pattern only: RETURN, ORDER BY, LIMIT and every write clause stay execution
   * steps appended to the operator chain. Printing the physical plan alone therefore answered
   * {@code EXPLAIN MATCH (n) WHERE ... DELETE n} with a scan and no mention of the delete (issue #6323). The first
   * element of the chain is the operator wrapper the physical plan above already describes, so it is skipped.
   */
  private static void appendStepsAfterTheOptimizedMatch(final StringBuilder output, final List<ExecutionStep> chain) {
    if (chain.size() < 2)
      return;

    output.append("\nSteps After the Optimized Match:\n");
    for (final ExecutionStep step : chain.subList(1, chain.size())) {
      output.append(((AbstractExecutionStep) step).prettyPrint(0, 2));
      output.append("\n");
    }
  }

  /** A step chain as a first-step-first list, which is the order it is read in and the reverse of how it is linked. */
  private static List<ExecutionStep> stepChainOf(final AbstractExecutionStep rootStep) {
    final List<ExecutionStep> stepChain = new ArrayList<>();
    for (AbstractExecutionStep current = rootStep; current != null; current = (AbstractExecutionStep) current.getPrev())
      stepChain.add(current);
    Collections.reverse(stepChain);
    return stepChain;
  }

  /**
   * The count push-down this query would run, built only to be described, or null when it would not take one.
   * <p>
   * It builds the same steps {@link #execute()} would and pulls none of them, so no count is computed. What it does
   * read is the schema, through the emptiness check that decides between a real push-down and a constant - and, on a
   * counter that has never been computed, the one bucket scan {@link #typeIsProvablyEmpty} describes.
   */
  private AbstractExecutionStep countPushDownForDescription() {
    if (unionSubqueryPlans != null && !unionSubqueryPlans.isEmpty())
      return null;

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    setupFunctionResolver(context);
    return tryCountPushDown(context, false);
  }

  /** Appends a step chain first-step-first, which is the order it is read in and the reverse of how it is linked. */
  private static void appendStepChain(final StringBuilder output, final AbstractExecutionStep rootStep) {
    for (final ExecutionStep step : stepChainOf(rootStep)) {
      output.append(((AbstractExecutionStep) step).prettyPrint(0, 2));
      output.append("\n");
    }
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
    boolean countPushedDown = false;

    try {
      if (unionSubqueryPlans != null && !unionSubqueryPlans.isEmpty()) {
        final UnionStep unionStep =
            new UnionStep(unionSubqueryPlans, unionRemoveDuplicates, context);
        final ResultSet resultSet = unionStep.syncPull(context, Integer.MAX_VALUE);
        while (resultSet.hasNext())
          results.add(resultSet.next());
      } else {
        // FAST PATH: Count-push-down (same logic as execute())
        rootStep = tryCountPushDown(context, false);
        countPushedDown = rootStep != null;

        if (rootStep == null) {
          if (canUseOptimizedPhysicalPlan()) {
            rootStep = buildExecutionStepsWithOptimizer(context);
          } else
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

    // Collect execution steps for structured plan data
    final List<ExecutionStep> executionSteps = stepChainOf(rootStep);

    // Asked before canUseOptimizedPhysicalPlan() for the reason explain() asks it first: the push-down replaced the
    // chain, so the optimizer's physical plan is not what ran (issue #5715).
    if (countPushedDown) {
      profileOutput.append("\nExecution Plan (Count Push-Down):\n");
      appendStepChain(profileOutput, rootStep);
    }

    if (canUseOptimizedPhysicalPlan()) {
      profileOutput.append(countPushedDown
          ? "\nExecution Plan (Cost-Based Optimizer, superseded by the count push-down above):\n"
          : "\nExecution Plan (Cost-Based Optimizer):\n");
      profileOutput.append(physicalPlan.getRootOperator().explain(0));
      profileOutput.append(String.format("\nEstimated Cost: %.2f\n", physicalPlan.getTotalEstimatedCost()));
      profileOutput.append(String.format("Estimated Rows: %d\n", physicalPlan.getTotalEstimatedCardinality()));
      if (!countPushedDown)
        appendStepsAfterTheOptimizedMatch(profileOutput, executionSteps);
    } else if (!countPushedDown) {
      profileOutput.append("\nExecution Plan (Traditional):\n");
      if (rootStep != null)
        appendStepChain(profileOutput, rootStep);
      else
        profileOutput.append("No execution steps generated\n");
    }

    results.setPlan(new OpenCypherExplainExecutionPlan(profileOutput.toString(), executionSteps, endTime - startTime));
    // Surface the CRUD-count accumulator built up by the mutation steps during the profiled run,
    // mirroring execute()'s write path so a profiled write still reports its counters. Read-only
    // statements never attach a statistics accumulator, matching execute()'s read path.
    if (!statement.isReadOnly())
      results.setStatistics(context.getStatistics());
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
      private boolean closed = false;

      @Override
      public ResultSet syncPull(final CommandContext ctx, final int nRecords) {
        // Once closed this step stays closed: re-executing the operator tree here would open a second
        // set of cursors that the already-spent close() chain would never reach.
        if (operatorResults == null && !closed) {
          // Execute physical operators on first pull
          operatorResults = physicalPlan.getRootOperator().execute(ctx, nRecords);
        }
        return operatorResults != null ? operatorResults : new IteratorResultSet(Collections.<Result>emptyList().iterator());
      }

      /**
       * The physical-operator tree hangs off this step's result set, not off a previous step, so
       * without this override the close() chain stopped one step short of the operators and every
       * cursor they hold stayed open for as long as the plan was retained (issue #7010, and #5635
       * for why an index cursor has to be closed explicitly).
       */
      @Override
      public void close() {
        if (!closed) {
          closed = true;
          if (operatorResults != null)
            operatorResults.close();
        }
        super.close();
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
    // MATCH clauses seen since the last WITH (or since the start), i.e. the ones that actually feed
    // whichever DELETE segment is reached next - see matchClausesNeedEagerDelete().
    final List<MatchClause> currentSegmentMatchClauses = new ArrayList<>();
    if (clausesInOrder != null) {
      for (final ClauseEntry entry : clausesInOrder) {
        switch (entry.getType()) {
        case MATCH: {
          // MATCH pattern is handled by the optimizer above, but WHERE clauses
          // attached to MATCH clauses still need to be applied as filters.
          final MatchClause matchClause = entry.getTypedClause();
          currentSegmentMatchClauses.add(matchClause);
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
            final DeleteStep deleteStep = new DeleteStep(deleteClause, context,
                matchClausesNeedEagerDelete(currentSegmentMatchClauses));
            deleteStep.setPrevious(currentStep);
            currentStep = deleteStep;
          }
          break;
        }

        case REMOVE: {
          final RemoveClause removeClause = entry.getTypedClause();
          if (!removeClause.isEmpty()) {
            final RemoveStep removeStep = new RemoveStep(removeClause, context, functionFactory);
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
          // A WITH boundary starts a new segment: MATCH clauses before it no longer feed a DELETE
          // that comes after it (issue #6631). Unlike buildExecutionStepsWithOrder()'s WITH case, a
          // plain clear() here (no taint tracking for a DELETE that plainly forwards a disconnected
          // variable through this WITH) is safe: CypherExecutionPlanner.shouldUseOptimizer() already
          // refuses to build a physical plan at all when a mutating clause (DELETE included) follows any
          // WITH, so this method never builds a DELETE fed by an already-cleared segment.
          currentSegmentMatchClauses.clear();
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
    return buildExecutionStepsWithOrder(context, clausesInOrder, null, SeedCorrelation.UNCORRELATED, Set.of());
  }

  /**
   * Builds execution steps respecting clause order, optionally seeded with an initial step.
   * When initialStep is provided (e.g., for CALL subqueries), it serves as the starting point
   * of the step chain, providing input rows to the first clause.
   *
   * @param correlation        which of the seeded names this body reads, and the row that bound them. A body that
   *                           reads none of them is answered the same way with the seed as without it; a body that
   *                           reads one at a position an operator can start its walk from is answered from that one
   *                           anchor. Both are what let the count push-downs below still apply.
   * @param seedVariableNames  the seed row's own variable names, empty when there is no seed. Pre-populates
   *                           {@code boundVariables} the way an explicit leading {@code WITH} would: without it, a
   *                           body's first clause has no way to tell that one of its own pattern variables (a
   *                           relationship in particular - see #5696) is not fresh but already carries the outer
   *                           binding, because {@link CypherVariableUsage#isEdgeVariableReferenced} only sees this
   *                           body's own text, never the enclosing query that actually names it again.
   */
  private AbstractExecutionStep buildExecutionStepsWithOrder(final CommandContext context,
      final List<ClauseEntry> clausesInOrder,
      final AbstractExecutionStep initialStep, final SeedCorrelation correlation, final Set<String> seedVariableNames) {
    AbstractExecutionStep currentStep = initialStep;

    // Get function factory from evaluator for steps that need it
    final CypherFunctionFactory functionFactory = expressionEvaluator != null ?
        expressionEvaluator.getFunctionFactory() : null;

    // Track variables bound across MATCH clauses so subsequent MATCHes
    // can detect already-bound variables and avoid Cartesian products
    final Set<String> boundVariables = new HashSet<>(seedVariableNames);

    // MATCH clauses seen since the last WITH (or since the start), i.e. the ones that actually feed
    // whichever DELETE/FOREACH segment is reached next - see matchClausesNeedEagerDelete().
    final List<MatchClause> currentSegmentMatchClauses = new ArrayList<>();
    // Variables bound by a segment that needed the eager guard (disconnected patterns, or a
    // variable-length relationship), kept forever once tainted (even across a WITH that merely forwards
    // them unchanged) - see closeMatchSegment() and deleteMayTargetTaintedVariable().
    final Set<String> disconnectedTaintedVariables = new HashSet<>();

    // Both count push-downs answer from the schema and the CSR arrays alone: they read the statement's patterns and
    // never look at the incoming rows. That makes the enumerating form of them wrong the moment the seed row binds
    // one of those pattern variables - a seeded body counting `MATCH (n)-[:KNOWS]->(m)` with `n` already bound to one
    // vertex would be answered with the count over every `n` in the graph.
    //
    // A body that READS none of the seeded names keeps them as they are: an uncorrelated body - `MATCH (n:P) RETURN
    // COLLECT { MATCH (m:Big) RETURN count(m) }`, or a `CALL { }` that imports nothing - has taken no name from the
    // enclosing query, so ignoring the seed cannot change its answer and a large type keeps its O(1) count (#5686).
    //
    // A body that reads one keeps the chain push-down in its SEEDED form: the walk starts from the RID the outer row
    // bound rather than from a label's bucket set, which is the same question asked of one anchor instead of all of
    // them, and an O(degree) read rather than a scan (#5758). Which names are read is decided by
    // CypherReferencedVariables, which answers "unknown" for every shape it does not model, and an unknown
    // correlation takes no push-down at all - being unsure costs the optimization rather than the correctness.
    if (correlation.isSeedable()) {
      // OPTIMIZATION: the O(1) Type.count() push-down and the CSR one for chain/star/triangle/pair-join patterns.
      // Instead of materializing all paths (O(paths) memory), counts are propagated through the CSR arrays
      // level-by-level (O(nodes) memory). Critical for large-fanout chains.
      final AbstractExecutionStep countStep = tryCountPushDown(context, false, correlation);
      if (countStep != null)
        return countStep;
    }

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
        currentSegmentMatchClauses.add(matchClause);
        if (matchClause.isOptional()) {
          // Try chained count optimization first (handles 2 consecutive OPTIONAL MATCH + count)
          final AbstractExecutionStep chainedOptimized = tryOptimizeChainedOptionalMatchCount(
              matchClause, clausesInOrder, entryIndex, currentStep, context, boundVariables);
          if (chainedOptimized != null) {
            currentStep = chainedOptimized;
            entryIndex += 2; // skip both the next OPTIONAL MATCH and the WITH clause
            // Update boundVariables from the WITH clause
            final WithClause nextWith = ((ClauseEntry) clausesInOrder.get(entryIndex)).getTypedClause();
            applyProjectionToScope(nextWith.getItems(), boundVariables);
            // the skipped WITH starts a new segment (issue #6631)
            closeMatchSegment(currentSegmentMatchClauses, disconnectedTaintedVariables);
            propagateTaintThroughRenames(nextWith, disconnectedTaintedVariables);
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
            applyProjectionToScope(nextWith.getItems(), boundVariables);
            // the skipped WITH starts a new segment (issue #6631)
            closeMatchSegment(currentSegmentMatchClauses, disconnectedTaintedVariables);
            propagateTaintThroughRenames(nextWith, disconnectedTaintedVariables);
            break;
          }
        }
        currentStep = buildMatchStep(matchClause, currentStep, context, boundVariables);
        break;

      case WITH:
        final WithClause withClause = entry.getTypedClause();
        currentStep = buildWithStep(withClause, currentStep, context, functionFactory);
        // An explicit WITH resets the scope to its own output variables; WITH * forwards the incoming one
        applyProjectionToScope(withClause.getItems(), boundVariables);
        // A WITH boundary starts a new segment (issue #6631) - but a WITH that plainly forwards a
        // variable bound by a disconnected-pattern MATCH (e.g. WITH n, o) does not resolve the #6491
        // hazard for that variable, since rows still flow through it one at a time rather than being
        // fully consumed; closeMatchSegment() taints it before the segment is cleared.
        closeMatchSegment(currentSegmentMatchClauses, disconnectedTaintedVariables);
        // A rename (WITH n AS m) doesn't change how rows flow either - propagate the taint onto the
        // new name too, or a later DELETE of m would find nothing tainted under that name.
        propagateTaintThroughRenames(withClause, disconnectedTaintedVariables);
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
              new RemoveStep(removeClause, context, functionFactory);
          removeStep.setPrevious(currentStep);
          currentStep = removeStep;
        }
        break;

      case DELETE:
        final DeleteClause deleteClause = entry.getTypedClause();
        if (!deleteClause.isEmpty() && currentStep != null) {
          final boolean eagerMaterialize = matchClausesNeedEagerDelete(currentSegmentMatchClauses)
              || deleteMayTargetTaintedVariable(deleteClause.getVariables(), disconnectedTaintedVariables);
          final DeleteStep deleteStep = new DeleteStep(deleteClause, context, eagerMaterialize);
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
        if (isFollowedByCountOnlyReturn(clausesInOrder, entryIndex)) {
          callStep.setCountOnlyOptimization(true);
        }
        currentStep = callStep;
        // A CALL binds the names it yields the way UNWIND binds its variable: they belong to the scope a
        // following MATCH starts from, not to that MATCH's own clause. Registering them is what lets the
        // MATCH push a predicate that reads one of them down into its scan, and what keeps a yielded
        // relationship out of the MATCH's clause-scoped uniqueness set (issue #7165).
        collectCallOutputVariables(callClause, boundVariables);
        break;

      case FOREACH:
        final ForeachClause foreachClause = entry.getTypedClause();
        final boolean foreachEagerMaterialize = foreachClause.containsDelete()
            && (matchClausesNeedEagerDelete(currentSegmentMatchClauses)
                || deleteMayTargetTaintedVariable(collectForeachDeleteTargetVariables(foreachClause), disconnectedTaintedVariables));
        final boolean foreachEagerExecution =
            database.getConfiguration().getValueAsBoolean(GlobalConfiguration.OPENCYPHER_FOREACH_EAGER_READ)
                && graphReadFollows(clausesInOrder, entryIndex);
        final ForeachStep foreachStep =
            new ForeachStep(foreachClause, context, functionFactory, foreachEagerMaterialize, foreachEagerExecution);
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
   * Applies a {@code WITH} projection to the set of variables in scope after it.
   * <p>
   * An explicit projection list resets the scope to exactly what it names, which is what makes a variable the
   * following clauses do not import unavailable. {@code WITH *} names nothing: it forwards the incoming scope
   * unchanged, so the names stay bound (#6311). Treating its single {@code "*"} item as if it were a projected
   * variable used to leave the scope holding the literal name {@code *} and nothing else, which is how a
   * variable shared with a following MATCH stopped being recognised as already bound - and a shared variable
   * that is not recognised is a Cartesian product rather than a join. A {@code WITH *, expr AS alias} both
   * forwards and adds.
   * <p>
   * The star is recognised by {@link ReturnClause.ReturnItem#isStar()}, not by the projected name being {@code *}:
   * a user can write a variable called {@code `*`}, and {@code WITH n AS `*`} is an ordinary projection that resets
   * the scope to that one name, not a star that forwards it (issue #6334).
   */
  private static void applyProjectionToScope(final List<ReturnClause.ReturnItem> items, final Set<String> scope) {
    boolean forwardsAll = false;
    final List<String> projected = new ArrayList<>(items.size());
    for (final ReturnClause.ReturnItem item : items) {
      if (item.isStar())
        forwardsAll = true;
      else {
        final String alias = item.getAlias();
        projected.add(alias != null ? alias : item.getExpression().getText());
      }
    }
    if (!forwardsAll)
      scope.clear();
    scope.addAll(projected);
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

    final AbstractExecutionStep stepBeforeMatch = currentStep;
    final Set<String> matchVariables = new HashSet<>();
    // The other half of the relationship-uniqueness scope: see clauseRelationshipVariables().
    final Set<String> clauseRelVariables = clauseRelationshipVariables(matchClause);
    final boolean isOptional = matchClause.isOptional();

    // Reorder independent (disconnected) comma-separated pattern parts so the expensive edge-bearing
    // component drives the Cartesian product as the outer loop regardless of the written order
    // (issue #5117). The legacy path chains parts left-deep and re-executes every later part once per
    // outer row; writing a cheap standalone node first would otherwise re-run an expensive traversal
    // once per node. Scoped to a leading, non-optional MATCH (no prior input row) so it never
    // interacts with input-driven reverse-traversal or OPTIONAL null semantics. Cartesian product is
    // commutative, so the result set is unchanged.
    final List<PathPattern> pathPatterns = (!isOptional && stepBeforeMatch == null
        && matchClause.getPathPatterns().size() > 1)
        ? reorderIndependentComponents(matchClause.getPathPatterns())
        : matchClause.getPathPatterns();

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

        // Optimization: when the variable is already bound from a previous clause AND
        // the new node pattern carries no additional constraints (no labels, no inline
        // properties), skip the redundant MatchNodeStep - the carried binding already
        // satisfies the empty constraint. We can only skip safely if the carried
        // value is guaranteed to be a vertex (i.e. it came from a non-OPTIONAL MATCH).
        // When the pattern adds labels or inline properties, MatchNodeStep must run
        // so the constraints are checked against the bound vertex (and a null binding
        // from an OPTIONAL MATCH correctly filters the row out - issue #4102).
        if (boundVariables.contains(variable)
            && !nodePattern.hasLabels()
            && !nodePattern.hasProperties()) {
          // Variable already bound and no new constraints - skip creating a new MatchNodeStep
          // But still handle zero-length named paths
          final String singlePathVar = pathPattern.hasPathVariable() ? pathPattern.getPathVariable() : null;
          if (singlePathVar != null) {
            matchVariables.add(singlePathVar);
            final ZeroLengthPathStep zeroPathStep = new ZeroLengthPathStep(variable, singlePathVar, context);
            zeroPathStep.setPrevious(currentStep);
            currentStep = zeroPathStep;
            // With no MatchNodeStep created above, this is the first step of the OPTIONAL chain and has to be
            // registered as such: OptionalMatchStep feeds every input row into matchChainStart, and the next
            // pattern part re-seats matchChainStart on its own head, so a chain start left null here would
            // orphan this step and leave the path variable permanently unbound (issue #6544).
            if (isOptional && matchChainStart == null)
              matchChainStart = zeroPathStep;
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
        // Note: matchChainStart must stay pointed at matchStep (the actual chain head that
        // OptionalMatchStep feeds each input row into) - it was already correctly assigned above.
        // Reassigning it to zeroPathStep here would make OptionalMatchStep call
        // zeroPathStep.setPrevious(...) directly, discarding the setPrevious(currentStep) link to
        // matchStep below and leaving zeroPathStep with no previous step (issue #6378).
        final String singlePathVar = pathPattern.hasPathVariable() ? pathPattern.getPathVariable() : null;
        if (singlePathVar != null) {
          matchVariables.add(singlePathVar);
          final ZeroLengthPathStep zeroPathStep = new ZeroLengthPathStep(variable, singlePathVar, context);
          zeroPathStep.setPrevious(currentStep);
          currentStep = zeroPathStep;
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
          if (isOptional && matchChainStart == null)
            matchChainStart = sourceStep;
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
          if (isOptional && matchChainStart == null)
            matchChainStart = targetStep;
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
        // Issue #740: drive a single-hop pattern from the edge type's own index when the endpoints are
        // unselective and the edge carries a full-key equality filter, mirroring the SQL FETCH FROM INDEX
        // path. Only attempted as the leading (seed) step of a non-optional MATCH whose WHERE clause is
        // re-applied above, so the index seek is always a safe prefilter.
        if (!isOptional && currentStep == null) {
          final MatchEdgeByIndexStep edgeIndexSeed = tryBuildEdgeIndexScanSeed(matchClause, pathPattern,
              boundVariables, matchVariables, context);
          if (edgeIndexSeed != null) {
            currentStep = edgeIndexSeed;
            continue;
          }
        }

        NodePattern sourceNode = pathPattern.getFirstNode();
        String sourceVar = sourceNode.getVariable() != null ? sourceNode.getVariable() :
            ("  src" + anonymousVarCounter++);
        final String writtenSourceVar = sourceVar;

        final boolean reversedFromIndexedAnchor =
            shouldReverseVariableLengthPathFromIndexedAnchor(matchClause, pathPattern);
        boolean reversed = reversedFromIndexedAnchor;
        if (reversed) {
          sourceNode = pathPattern.getLastNode();
          sourceVar = sourceNode.getVariable();
        }

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
        if (!reversed && !sourceAlreadyBound && pathPattern.getRelationshipCount() == 1
            && !pathPattern.getRelationship(0).isVariableLength()) {
          final NodePattern targetNode = pathPattern.getLastNode();
          final String targetVar = targetNode.getVariable();
          if (targetVar != null && stepBeforeMatch != null
              && (boundVariables.contains(targetVar) || matchVariables.contains(targetVar))) {
            // Target IS bound - reverse the traversal for bidirectional edges only.
            // Unidirectional edges don't store incoming links on the target vertex,
            // so reverse traversal would return 0 results. In that case, keep the
            // original direction and scan from the unbound source side.
            final RelationshipPattern relCheck = pathPattern.getRelationship(0);
            if (!isAnyEdgeTypeUnidirectional(relCheck.getTypes())) {
              reversed = true;
              sourceNode = targetNode;
              sourceVar = targetVar;
              sourceAlreadyBound = true;
            }
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
        final AbstractExecutionStep sourceStep;
        if (reversedFromIndexedAnchor && physicalPlan.getAnchor().getPropertyValue() instanceof InListValues) {
          final var anchor = physicalPlan.getAnchor();
          sourceStep = new IndexSeekStep(anchor.getVariable(), anchor.getIndex().getTypeName(),
              anchor.getPropertyName(), anchor.getPropertyValue(), anchor.getIndex().getIndexName(),
              anchor.getIndex().getPropertyNames(),
              anchor.getEstimatedCost(), anchor.getEstimatedCardinality(), context);
        } else
          sourceStep = new MatchNodeStep(sourceVar, sourceNode, context, sourceIdFilter, sourcePushdown);

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

        // Smart GAV eligibility: for each anonymous hop, check whether any other hop of the same MATCH
        // clause - this pattern part or another one across a comma - could be the same physical edge. If
        // none can, a null relVar enables the fast path (GAV/CSR). If one can, generate an internal
        // anonymous variable to force edge-loading, so relationship uniqueness has something to compare.
        final boolean[] hopNeedsEdgeTracking = computeHopEdgeTrackingNeeds(pathPatterns, patternIndex);

        for (int i = 0; i < pathPattern.getRelationshipCount(); i++) {
          final RelationshipPattern relPattern = pathPattern.getRelationship(i);
          final NodePattern targetNode = pathPattern.getNode(i + 1);
          // Named edge: keep user variable if actually referenced in the query or if VLP
          // (VLP steps always need the variable for pre-bound path validation).
          // For unreferenced fixed-length edges, treat as anonymous for GAV eligibility.
          // Anonymous edge: null if GAV-eligible, internal var if edge tracking needed.
          final String relVar;
          if (relPattern instanceof QuantifiedPathPattern)
            // A quantified group binds no relationship of its own: its inner relationship variables are
            // group variables bound to lists by QuantifiedPathStep, and it does its own isomorphism
            // bookkeeping. Handing it a synthetic edge-tracking variable would register a name in
            // matchVariables that nothing ever binds.
            relVar = null;
          else if (relPattern.getVariable() != null && !relPattern.getVariable().isEmpty()) {
            if (relPattern.isVariableLength() || CypherVariableUsage.isEdgeVariableReferenced(statement, relPattern.getVariable())
                || boundVariables.contains(relPattern.getVariable()))
              relVar = relPattern.getVariable();
            else
              relVar = hopNeedsEdgeTracking[i] ? ("  rel" + anonymousVarCounter++) : null;
          } else
            relVar = hopNeedsEdgeTracking[i] ? ("  rel" + anonymousVarCounter++) : null;
          String targetVar = targetNode.getVariable() != null ? targetNode.getVariable() :
              ("  tgt" + anonymousVarCounter++);

          // When reversed, swap source/target variables and use the original source as target.
          // This mapping only holds for a single-relationship pattern: the written source is the
          // end of the one reversed hop. Both entry points into `reversed` enforce that (the
          // bound-target reversal below and shouldReverseVariableLengthPathFromIndexedAnchor);
          // reversing a longer pattern requires rebuilding the whole pattern back to front.
          final String effectiveSourceVar;
          final String effectiveTargetVar;
          final NodePattern effectiveTargetNode;
          final Direction directionOverride;
          if (reversed) {
            effectiveSourceVar = currentSourceVar; // already swapped to bound target
            effectiveTargetVar = writtenSourceVar;
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
          if (relPattern instanceof QuantifiedPathPattern quantified) {
            // GQL Quantified Path Pattern, Phase B (issue #4531): a repeated inner sub-pattern that no
            // variable-length hop can express. Its inner variables surface here as group variables.
            for (final String groupVariable : quantified.getGroupVariables())
              if (!boundVariables.contains(groupVariable))
                matchVariables.add(groupVariable);
            nextStep = new QuantifiedPathStep(effectiveSourceVar, effectiveTargetVar, pathVariable,
                bindsGroupPathVariable(pathPattern), quantified, effectiveTargetNode, matchVariables,
                clauseRelVariables, context);
          } else if (relPattern.isVariableLength()) {
            // DFS, not BFS: DFS's active stack is bounded by maxHops regardless of branching
            // factor, while BFS's frontier queue must hold an entire level's children before it
            // can dequeue the first one - level-order expansion via a single FIFO queue offers no
            // way around that. A MATCH's result order is unspecified without ORDER BY, so this is
            // a pure implementation-strategy change, not a semantic one (#6097).
            nextStep = new ExpandPathStep(effectiveSourceVar, pathVariable, relVar, effectiveTargetVar, relPattern,
                false, effectiveTargetNode, pathPattern.getEffectivePathMode(), matchVariables,
                clauseRelVariables, directionOverride, reversed, context);
          } else {
            // Check if this hop requires IN traversal on a unidirectional edge.
            // Unidirectional edges don't store incoming links, so we must restructure:
            // instead of (bound)-[IN]->(target), scan target type and go (target)-[OUT]->(bound).
            // #6311: the names a hop must identity-check its target against are the ones the row already
            // carries when the hop RUNS: everything bound before this MATCH plus everything this MATCH has
            // bound so far (earlier comma-separated patterns, earlier hops). Snapshot them here rather than
            // handing the step the planner's live `boundVariables` set: that set is mutated after the clause
            // is planned - which is what used to supply this clause's own variables - and a following WITH
            // clears it, so the join on a variable shared between two patterns of the SAME MATCH silently
            // degraded into a Cartesian product the moment a `WITH *` followed.
            //
            // The snapshot also carries this hop's own target and relationship names, added just above. That is
            // deliberate and costs nothing: the check reads a name only when the row ALSO already holds a vertex
            // under it, and a name this hop is about to bind for the first time is absent from the row. Excluding
            // them would instead need the set rebuilt per hop in the opposite order, for no gain.
            final Set<String> targetIdentityVars = new HashSet<>(boundVariables);
            targetIdentityVars.addAll(matchVariables);

            final Direction effectiveDir = directionOverride != null ? directionOverride : relPattern.getDirection();
            final boolean needsReverseOnUnidirectional = !reversed
                && effectiveDir == Direction.IN
                && (boundVariables.contains(effectiveSourceVar) || matchVariables.contains(effectiveSourceVar))
                && isAnyEdgeTypeUnidirectional(relPattern.getTypes());

            if (needsReverseOnUnidirectional) {
              // Restructure: scan target type with MatchNodeStep, then traverse OUT to validate
              // against the bound source. The bound source becomes the "target" of the relationship.
              final Set<String> boundWithSource = new HashSet<>(targetIdentityVars);
              boundWithSource.add(effectiveSourceVar);
              final MatchNodeStep scanStep = new MatchNodeStep(effectiveTargetVar, effectiveTargetNode, context);
              if (isOptional && matchChainStart == null) {
                matchChainStart = scanStep;
                currentStep = scanStep;
              } else {
                scanStep.setPrevious(currentStep);
                currentStep = scanStep;
              }
              // Swap source/target and reverse direction: go OUT from scanned target to bound source
              nextStep = new MatchRelationshipStep(effectiveTargetVar, relVar, effectiveSourceVar, relPattern,
                  pathVariable, sourceNode, boundWithSource, matchVariables, clauseRelVariables, Direction.OUT,
                  context);
            } else {
              // Normal case: pass target node pattern for label filtering and bound variables for identity
              // checking. The relationship-uniqueness scope is published once the clause is complete.
              nextStep = new MatchRelationshipStep(effectiveSourceVar, relVar, effectiveTargetVar, relPattern,
                  pathVariable, effectiveTargetNode, targetIdentityVars, matchVariables, clauseRelVariables,
                  directionOverride, context);
            }
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
   * Adds the names a {@code CALL} makes visible to the clauses that follow it: the YIELD aliases when the
   * clause lists them, otherwise the procedure's own declared output fields, which is what {@code YIELD *}
   * and a bare {@code CALL} put into the row. A procedure the registry does not know contributes nothing -
   * the CALL itself fails later with a clearer message than anything this could raise.
   */
  private static void collectCallOutputVariables(final CallClause callClause, final Set<String> boundVariables) {
    if (callClause.hasYield() && !callClause.isYieldAll()) {
      for (final CallClause.YieldItem item : callClause.getYieldItems())
        boundVariables.add(item.getOutputName());
      return;
    }
    final CypherProcedure procedure = CypherProcedureRegistry.get(callClause.getProcedureName());
    if (procedure == null)
      return;
    final List<String> yieldFields = procedure.getYieldFields();
    if (yieldFields != null)
      boundVariables.addAll(yieldFields);
  }

  /**
   * Builds a {@link MatchEdgeByIndexStep} seed for a single-hop pattern whose edge type has an index
   * whose key, or a leading prefix of it, is covered by equality or {@code IN}-list predicates on the
   * edge variable, when neither endpoint is selective (issue #740). Returns {@code null} - deferring to
   * the ordinary vertex-scan + expansion plan - whenever any precondition is not met. On success it also
   * registers the source, target and edge variables in {@code matchVariables}, since the seed step binds
   * all three.
   * <p>
   * Safety: the seek returns every edge whose leading key columns take one of the predicate values - a
   * superset of the rows the MATCH's WHERE clause keeps, whether the key is complete or a prefix and
   * whether a column is pinned to one value or to a list. That WHERE clause is re-applied by the
   * {@link FilterPropertiesStep} added above this step, so the seek can never let through a row the
   * unoptimised plan would have rejected.
   */
  private MatchEdgeByIndexStep tryBuildEdgeIndexScanSeed(final MatchClause matchClause,
      final PathPattern pathPattern, final Set<String> boundVariables, final Set<String> matchVariables,
      final CommandContext context) {
    // The WHERE clause must belong to this MATCH so its FilterPropertiesStep re-validates above the seed.
    if (!matchClause.hasWhereClause() || matchClause.getWhereClause() == null)
      return null;
    if (pathPattern.hasPathVariable() || pathPattern.getRelationshipCount() != 1)
      return null;

    final RelationshipPattern rel = pathPattern.getRelationship(0);
    if (rel.isVariableLength() || rel.hasProperties() || rel.hasWhereExpression()
        || rel.getPropertiesParameterName() != null)
      return null;

    // Only a directed hop maps unambiguously to the edge's out/in endpoints; BOTH is skipped.
    final Direction dir = rel.getDirection();
    if (dir != Direction.OUT && dir != Direction.IN)
      return null;

    // Exactly one edge type, which must be a declared edge type.
    if (!rel.hasTypes() || rel.getTypes().size() != 1)
      return null;
    final String edgeTypeName = rel.getTypes().get(0);
    if (!context.getDatabase().getSchema().existsType(edgeTypeName)
        || !(context.getDatabase().getSchema().getType(edgeTypeName) instanceof EdgeType edgeType))
      return null;

    // The WHERE clause references the edge, so the edge must carry a named variable that is still free.
    final String edgeVar = rel.getVariable();
    if (edgeVar == null || edgeVar.isEmpty()
        || boundVariables.contains(edgeVar) || matchVariables.contains(edgeVar))
      return null;

    // Both endpoints must be unselective (no labels, no inline properties) and not already bound - there
    // is nothing to validate on them beyond what the WHERE filter above already re-checks. Note that a
    // variable repeated at both endpoints is unselective by this measure yet still constrains the hop; see
    // MatchEdgeByIndexStep's class javadoc for why the step, not this test, enforces it (issue #7008).
    final NodePattern srcNode = pathPattern.getFirstNode();
    final NodePattern tgtNode = pathPattern.getLastNode();
    if (isSelectiveEndpoint(srcNode, boundVariables, matchVariables)
        || isSelectiveEndpoint(tgtNode, boundVariables, matchVariables))
      return null;

    // Collect the row-independent equality and IN-list predicates on the edge variable from the WHERE clause.
    final Map<String, Object> predicates = new LinkedHashMap<>();
    extractEdgeKeyPredicates(edgeVar, matchClause.getWhereClause().getConditionExpression(), predicates);
    if (predicates.isEmpty())
      return null;

    // Pick the index whose longest leading key prefix is covered by those predicates: a complete key is an
    // exact lookup, a prefix walks the contiguous range of the ordered index sharing it (the same seek
    // NodeIndexSeek performs for a vertex anchor, and the one SQL performs for `WHERE transactionId IN (...)`
    // on a (transactionId, date) index). On an equal prefix the narrower index wins: nothing extra to walk.
    // Polymorphic, for the same reason node patterns are (issue #7021): an index declared on a parent edge
    // type is inherited by this one, and a relationship pattern already matches every subtype of the type it
    // names. MatchEdgeByIndexStep filters an inherited index's cursor back down to that same rule.
    TypeIndex bestIndex = null;
    int bestPrefix = 0;
    for (final TypeIndex index : edgeType.getAllIndexes(true)) {
      if (index.getType() != Schema.INDEX_TYPE.LSM_TREE)
        continue; // a full-text or vector index does not answer an equality on its key
      final List<String> keyProps = index.getPropertyNames();
      int prefix = 0;
      while (prefix < keyProps.size() && predicates.containsKey(keyProps.get(prefix)))
        prefix++;
      if (prefix == 0)
        continue;
      if (prefix < keyProps.size() && !index.supportsOrderedIterations())
        continue; // a prefix seek needs the range cursor
      if (bestIndex == null || prefix > bestPrefix
          || (prefix == bestPrefix && keyProps.size() < bestIndex.getPropertyNames().size())) {
        bestIndex = index;
        bestPrefix = prefix;
      }
    }
    if (bestIndex == null)
      return null;

    final String[] propertyNames = new String[bestPrefix];
    final Object[] keyValues = new Object[bestPrefix];
    for (int i = 0; i < bestPrefix; i++) {
      propertyNames[i] = bestIndex.getPropertyNames().get(i);
      keyValues[i] = predicates.get(propertyNames[i]);
    }

    final String effectiveSourceVar = srcNode.getVariable() != null ? srcNode.getVariable()
        : ("  src" + anonymousVarCounter++);
    final String effectiveTargetVar = tgtNode.getVariable() != null ? tgtNode.getVariable()
        : ("  tgt" + anonymousVarCounter++);

    matchVariables.add(effectiveSourceVar);
    matchVariables.add(effectiveTargetVar);
    matchVariables.add(edgeVar);

    return new MatchEdgeByIndexStep(edgeTypeName, propertyNames, keyValues, bestIndex.getName(), dir,
        effectiveSourceVar, edgeVar, effectiveTargetVar, context);
  }

  /** An endpoint is selective (and so must anchor the plan itself) if it has labels, inline properties, or is already bound. */
  private boolean isSelectiveEndpoint(final NodePattern node, final Set<String> boundVariables,
      final Set<String> matchVariables) {
    if (node.hasLabels() || node.hasDynamicLabels() || node.hasProperties())
      return true;
    final String var = node.getVariable();
    return var != null && (boundVariables.contains(var) || matchVariables.contains(var));
  }

  /**
   * Collects the predicates on the edge variable that can seed an index seek, keyed by property: the value each
   * property is pinned to, as the {@link Expression} of an {@code edgeVar.property = <value>} equality (or
   * reversed) or as the {@link InListValues} of an {@code edgeVar.property IN [...]}. The two are kept apart even
   * for a one-element list, since that element may be a parameter standing for a whole list ({@code IN $ids}),
   * which the seek expands into one key per element where an equality would seek the list as a single key. Only
   * row-independent values qualify (a literal, a parameter, or a function of those such as {@code date('...')});
   * a value that would need the current row cannot seed a static lookup, so its predicate is left out and the
   * optimization simply does not apply to it.
   * <p>
   * An {@code AND} contributes both sides; when both pin the same property the one with fewer values is kept, as
   * either already bounds the rows. An {@code OR} contributes a property only when <em>both</em> sides pin it, as
   * the union of the two sides' values: a row satisfying the disjunction takes one of its sides' values, so the
   * union is a superset - whereas a property pinned on one side only bounds nothing about rows from the other.
   * {@code (t.k = 'a' OR t.k = 'b')} therefore seeds the same two-key seek as {@code t.k IN ['a', 'b']}.
   */
  private void extractEdgeKeyPredicates(final String edgeVar, final BooleanExpression expression,
      final Map<String, Object> predicates) {
    if (expression == null)
      return;

    if (expression instanceof BooleanWrapperExpression wrapper) {
      extractEdgeKeyPredicates(edgeVar, wrapper.getBooleanExpression(), predicates);
      return;
    }
    if (expression instanceof LogicalExpression logical) {
      if (logical.getOperator() == LogicalExpression.Operator.AND) {
        extractEdgeKeyPredicates(edgeVar, logical.getLeft(), predicates);
        extractEdgeKeyPredicates(edgeVar, logical.getRight(), predicates);
      } else if (logical.getOperator() == LogicalExpression.Operator.OR) {
        final Map<String, Object> left = new LinkedHashMap<>();
        final Map<String, Object> right = new LinkedHashMap<>();
        extractEdgeKeyPredicates(edgeVar, logical.getLeft(), left);
        extractEdgeKeyPredicates(edgeVar, logical.getRight(), right);
        for (final Map.Entry<String, Object> entry : left.entrySet()) {
          final Object otherSide = right.get(entry.getKey());
          if (otherSide == null)
            continue;
          final List<Expression> union = new ArrayList<>(edgeKeyValues(entry.getValue()));
          union.addAll(edgeKeyValues(otherSide));
          addEdgeKeyPredicate(predicates, entry.getKey(), new InListValues(union));
        }
      }
      return;
    }

    if (expression instanceof InExpression inExpr) {
      // No negated form to decline: Cypher has no NOT IN operator. The negation is written NOT x IN list and
      // parses as a TernaryLogicalExpression(NOT) around this node, which this walk has no branch for.
      if (!(inExpr.getExpression() instanceof PropertyAccessExpression prop) || !edgeVar.equals(prop.getVariableName()))
        return;
      List<Expression> list = inExpr.getList();
      // A list literal (x IN [a, b, c]) is parsed as a single ListExpression element: unwrap it to its values.
      if (list != null && list.size() == 1 && list.getFirst() instanceof ListExpression listExpr)
        list = listExpr.getElements();
      if (list == null || list.isEmpty())
        return;
      for (final Expression element : list)
        if (!isRowIndependentExpression(element))
          return;
      addEdgeKeyPredicate(predicates, prop.getPropertyName(), new InListValues(list));
      return;
    }

    if (!(expression instanceof ComparisonExpression comparison)
        || comparison.getOperator() != ComparisonExpression.Operator.EQUALS)
      return;

    final Expression left = comparison.getLeft();
    final Expression right = comparison.getRight();
    if (left instanceof PropertyAccessExpression prop && edgeVar.equals(prop.getVariableName())
        && isRowIndependentExpression(right))
      addEdgeKeyPredicate(predicates, prop.getPropertyName(), right);
    else if (right instanceof PropertyAccessExpression prop && edgeVar.equals(prop.getVariableName())
        && isRowIndependentExpression(left))
      addEdgeKeyPredicate(predicates, prop.getPropertyName(), left);
  }

  /** Keeps the value with fewer candidates for a property pinned twice: either already bounds the seek. */
  private static void addEdgeKeyPredicate(final Map<String, Object> predicates, final String property, final Object value) {
    final Object existing = predicates.get(property);
    if (existing == null || edgeKeyValues(value).size() < edgeKeyValues(existing).size())
      predicates.put(property, value);
  }

  /** The candidate expressions behind a seed value: the one of an equality, the elements of an IN-list. */
  private static List<Expression> edgeKeyValues(final Object value) {
    return value instanceof InListValues inList ? inList.getValues() : List.of((Expression) value);
  }

  /** True when an expression can be evaluated without a row: a literal, a parameter, or a function of such. */
  private boolean isRowIndependentExpression(final Expression expression) {
    if (expression instanceof LiteralExpression || expression instanceof ParameterExpression)
      return true;
    if (expression instanceof FunctionCallExpression func) {
      for (final Expression arg : func.getArguments())
        if (!isRowIndependentExpression(arg))
          return false;
      return true;
    }
    return false;
  }

  /**
   * The physical operators do not yet implement variable-length expansion, but their cost-based
   * anchor selection is still useful to the traditional executor. Limit this bridge to a single
   * relationship whose indexed target can be reached through stored incoming adjacency.
   * <p>
   * Two of the conditions below are load-bearing rather than merely conservative:
   * <ul>
   *   <li>a single relationship, because the reversal in {@code buildMatchStep} maps the one hop
   *   back onto the written source node and cannot express a longer reversed pattern;</li>
   *   <li>a single-property index, because {@code NodeIndexSeek} and {@link IndexSeekStep} seek
   *   with a one-element key, which a composite index rejects.</li>
   * </ul>
   * The remaining conditions bound the shapes this bridge has been proven against; see issue #5358
   * for the tracked relaxations and for the native variable-length expansion operator that makes
   * this bridge unnecessary.
   */
  private boolean shouldReverseVariableLengthPathFromIndexedAnchor(final MatchClause matchClause,
      final PathPattern pathPattern) {
    if (physicalPlan == null || physicalPlan.getAnchor() == null || !physicalPlan.getAnchor().useIndex()
        || physicalPlan.getAnchor().isRangeScan() || physicalPlan.getAnchor().getIndex() == null
        || physicalPlan.getAnchor().getIndex().getPropertyNames() == null
        || physicalPlan.getAnchor().getIndex().getPropertyNames().size() != 1)
      return false;
    if (!statement.isReadOnly() || statement.hasUnwindBeforeMatch() || statement.hasWithBeforeMatch()
        || statement.hasSubquery() || statement.hasWriteBeforeMatch()
        || statement.getMatchClauses().size() != 1 || matchClause.isOptional()
        || matchClause.getPathPatterns().size() != 1 || pathPattern.getRelationshipCount() != 1)
      return false;

    if (physicalPlan.getAnchor().getPropertyValue() instanceof InListValues) {
      final NodePattern indexedTarget = pathPattern.getLastNode();
      final String indexedType = physicalPlan.getAnchor().getIndex().getTypeName();
      if (indexedTarget.hasProperties() || indexedTarget.hasDynamicLabels()
          || indexedTarget.getLabels().size() != 1 || indexedType == null
          || !indexedType.equals(indexedTarget.getLabels().getFirst()))
        return false;
    }

    final RelationshipPattern relationship = pathPattern.getRelationship(0);
    if (!relationship.isVariableLength() || !relationship.hasTypes()
        || isAnyEdgeTypeUnidirectional(relationship.getTypes()))
      return false;

    final String sourceVariable = pathPattern.getFirstNode().getVariable();
    final String targetVariable = pathPattern.getLastNode().getVariable();
    return targetVariable != null && !targetVariable.equals(sourceVariable)
        && targetVariable.equals(physicalPlan.getAnchor().getVariable());
  }

  /**
   * Returns true when a named path over {@code pathPattern} names nothing but a single quantified group,
   * which ISO/IEC 39075 §15.4 (grouped path assignment) binds to a {@code LIST<PATH>} - one path per
   * repetition - rather than to one path.
   * <p>
   * A path that also spans ordinary hops around the group has no such list form and stays a single
   * concatenated path, so the distinction is made here, on the written pattern, and not inside the step.
   */
  private static boolean bindsGroupPathVariable(final PathPattern pathPattern) {
    return pathPattern.getRelationshipCount() == 1
        && pathPattern.getRelationship(0) instanceof QuantifiedPathPattern;
  }

  /**
   * Reorders the comma-separated pattern parts of a MATCH so that independent (disconnected)
   * components are evaluated cheapest-to-recompute last (issue #5117).
   * <p>
   * Parts that share at least one node variable belong to the same connected component and keep
   * their written order (intra-component order drives the reverse-traversal / bound-variable
   * optimizations and must be preserved). Independent components are stable-sorted by descending
   * relationship count: an edge-bearing component is more expensive to re-execute and typically more
   * selective, so it must drive the left-deep nested-loop Cartesian product as the outer loop, while
   * cheap standalone-node components become the inner loop that is re-scanned per outer row.
   * <p>
   * The Cartesian product of disconnected parts is commutative, so the returned order yields the same
   * result set. When there is a single connected component the input list is returned unchanged.
   * <p>
   * Safety gate: reordering is only attempted when every part is <i>structurally pure</i> - it has no
   * inline node/relationship properties, no dynamic labels, and no inline relationship WHERE. Those
   * are the only places one part can reference a variable defined by another part (e.g. the
   * correlated {@code MATCH (person), (p:Person {name: person.name})...} rewrite emitted by
   * {@code COUNT { ... }}). Such a reference is a data dependency that forbids moving the referencing
   * part ahead of its producer; when any part is impure we conservatively keep the written order. The
   * top-level WHERE is intentionally ignored here because it is re-applied by a filter step after the
   * whole match chain, so it never constrains the safe ordering.
   */
  private static List<PathPattern> reorderIndependentComponents(final List<PathPattern> pathPatterns) {
    final int n = pathPatterns.size();

    // Node variables per pattern part (anonymous nodes never bridge two parts). Bail out entirely if
    // any part is impure, since an inline expression could reference another part's variable.
    final List<Set<String>> partVars = new ArrayList<>(n);
    for (final PathPattern part : pathPatterns) {
      if (!isStructurallyPure(part))
        return pathPatterns;
      final Set<String> vars = new HashSet<>();
      for (final NodePattern node : part.getNodes())
        if (node.getVariable() != null)
          vars.add(node.getVariable());
      partVars.add(vars);
    }

    // Union-find: connect parts sharing at least one node variable.
    final int[] parent = new int[n];
    for (int i = 0; i < n; i++)
      parent[i] = i;
    for (int i = 0; i < n; i++)
      for (int j = i + 1; j < n; j++)
        if (!Collections.disjoint(partVars.get(i), partVars.get(j)))
          parent[find(parent, i)] = find(parent, j);

    // Group parts by component root, preserving first-appearance order within each component.
    final LinkedHashMap<Integer, List<Integer>> components = new LinkedHashMap<>();
    for (int i = 0; i < n; i++)
      components.computeIfAbsent(find(parent, i), k -> new ArrayList<>()).add(i);

    // Order the parts inside every component so the most selective occurrence of a shared variable
    // binds first (issue #5116). A node variable may repeat across single-node parts with different
    // label constraints (e.g. (n0), (n0:L1:L5)); chaining the bare occurrence first would bind n0 to
    // every node (full scan) and only afterwards filter by the labels. Stable-sorting the single-node
    // parts by descending label count lifts the labeled occurrence ahead of the bare one, so the
    // variable is bound to the small label class and the bare occurrence is then skipped. Relationship
    // parts (the traversal spine) keep their absolute positions, so this never reorders a traversal.
    for (final List<Integer> component : components.values())
      reorderSingleNodePartsBySelectivity(pathPatterns, component);

    if (components.size() < 2)
      return rebuild(pathPatterns, components.values()); // single component: only within-part order changed

    // Stable sort components by descending relationship count, then by descending label count so the
    // more selective component drives the Cartesian product as the outer loop. The label-count
    // tie-break makes the chosen order independent of the textual order (issue #5116): two disconnected
    // node components (0 relationships each) that differ only in their label constraints would otherwise
    // be left in written order, so swapping them changed which one re-scanned per outer row. Only when
    // both counts tie do we fall back to the earliest original index.
    final List<List<Integer>> ordered = new ArrayList<>(components.values());
    ordered.sort((a, b) -> {
      final int wa = componentRelationshipCount(pathPatterns, a);
      final int wb = componentRelationshipCount(pathPatterns, b);
      if (wa != wb)
        return Integer.compare(wb, wa);
      final int la = componentLabelCount(pathPatterns, a);
      final int lb = componentLabelCount(pathPatterns, b);
      if (la != lb)
        return Integer.compare(lb, la);
      return Integer.compare(a.get(0), b.get(0));
    });

    return rebuild(pathPatterns, ordered);
  }

  private static List<PathPattern> rebuild(final List<PathPattern> pathPatterns,
      final Iterable<List<Integer>> ordered) {
    final List<PathPattern> result = new ArrayList<>(pathPatterns.size());
    for (final List<Integer> component : ordered)
      for (final int idx : component)
        result.add(pathPatterns.get(idx));
    return result;
  }

  /**
   * Reorders the single-node parts of a component in place so the ones carrying more labels come
   * first, while relationship-bearing parts keep their absolute positions. Stable within equal label
   * counts, so the outcome is independent of the textual order of otherwise-equivalent occurrences.
   */
  private static void reorderSingleNodePartsBySelectivity(final List<PathPattern> pathPatterns,
      final List<Integer> component) {
    // Collect the positions occupied by single-node parts and the part indices sitting in them.
    final List<Integer> slots = new ArrayList<>();
    final List<Integer> singleNodeParts = new ArrayList<>();
    for (int pos = 0; pos < component.size(); pos++) {
      final int partIndex = component.get(pos);
      if (pathPatterns.get(partIndex).isSingleNode()) {
        slots.add(pos);
        singleNodeParts.add(partIndex);
      }
    }
    if (singleNodeParts.size() < 2)
      return; // nothing to reorder

    // Stable sort the single-node parts by descending label count (most selective first).
    singleNodeParts.sort((a, b) -> Integer.compare(
        pathPatterns.get(b).getFirstNode().getLabels().size(),
        pathPatterns.get(a).getFirstNode().getLabels().size()));

    // Refill the single-node slots in the new order; relationship parts stay where they were.
    for (int i = 0; i < slots.size(); i++)
      component.set(slots.get(i), singleNodeParts.get(i));
  }

  private static int componentLabelCount(final List<PathPattern> pathPatterns, final List<Integer> component) {
    int count = 0;
    for (final int idx : component)
      for (final NodePattern node : pathPatterns.get(idx).getNodes())
        count += node.getLabels().size();
    return count;
  }

  /**
   * A pattern part is <i>structurally pure</i> when it carries no inline node/relationship properties,
   * no dynamic labels, and no inline relationship WHERE. Only these constructs can embed an expression
   * that references another part's variable, so a pure part can never depend on a sibling part and is
   * always safe to reorder. Static labels, relationship types, direction and variable-length bounds do
   * not reference row variables and are ignored.
   */
  private static boolean isStructurallyPure(final PathPattern part) {
    for (final NodePattern node : part.getNodes()) {
      if (node.hasProperties() || node.getPropertiesParameterName() != null || node.hasDynamicLabels()
          || node.hasWhereExpression())
        return false;
    }
    for (int i = 0; i < part.getRelationshipCount(); i++) {
      final RelationshipPattern rel = part.getRelationship(i);
      if (rel.hasProperties() || rel.getPropertiesParameterName() != null || rel.hasWhereExpression())
        return false;
    }
    return true;
  }

  private static int componentRelationshipCount(final List<PathPattern> pathPatterns, final List<Integer> component) {
    int count = 0;
    for (final int idx : component)
      count += pathPatterns.get(idx).getRelationshipCount();
    return count;
  }

  private static int find(final int[] parent, final int x) {
    int root = x;
    while (parent[root] != root)
      root = parent[root];
    // Path compression.
    int node = x;
    while (parent[node] != root) {
      final int next = parent[node];
      parent[node] = root;
      node = next;
    }
    return root;
  }

  /**
   * True when the given MATCH clauses, combined, have disconnected path patterns (see
   * {@link MatchClause#hasDisconnectedPathPatterns(List)}). A DELETE fed by such a MATCH must fully
   * read the upstream row set before deleting anything, or a later row can dereference a vertex/edge an
   * earlier row already deleted (issue #6491).
   * <p>
   * Checked across every MATCH clause combined, not one clause at a time: a disconnected/cross-join
   * shape can be spelled as comma-separated patterns within one {@code MATCH} or as separate,
   * consecutive {@code MATCH} keywords, and the step-chaining below treats both spellings identically
   * (a fresh {@code MatchNodeStep} chained onto {@code currentStep} either way), so both carry the same
   * hazard.
   * <p>
   * Callers pass only the MATCH clause(s) of the segment feeding the DELETE/FOREACH in question
   * (the MATCH clauses since the last WITH, tracked as {@code currentSegmentMatchClauses} while
   * walking {@code clausesInOrder}), not every MATCH clause in the whole statement: a multi-segment
   * statement with more than one WITH-separated MATCH/DELETE pair must not force eager materialization
   * on a DELETE whose own segment has no disconnected pattern just because an unrelated, earlier
   * segment does (issue #6631).
   */
  private static boolean matchClausesHaveDisconnectedPatterns(final List<MatchClause> matchClauses) {
    return MatchClause.hasDisconnectedPathPatterns(matchClauses);
  }

  /**
   * True when a DELETE (or a FOREACH containing one) fed by the given MATCH clause(s) must read its whole
   * upstream row set before applying the first deletion, because that MATCH can let a later row observe
   * what an earlier row's deletion already removed.
   * <p>
   * Two independent shapes carry that hazard, and either one alone is enough:
   * <ul>
   *   <li>disconnected path patterns, which re-enumerate one component per row of the other and so can
   *       bind the very same vertex/edge from more than one row (issue #6491);</li>
   *   <li>a variable-length or quantified relationship, whose depth-first traverser keeps edge-segment
   *       cursors open across the rows it is still producing, so a {@code DETACH DELETE} of a bound node
   *       unlinks chunks that same cursor is about to follow (issue #7023).</li>
   * </ul>
   *
   * The answer is deliberately segment-wide rather than per-variable: it is not narrowed to whether the
   * DELETE's own target is one of the variables the hazardous pattern binds, so {@code MATCH (a)-[*1..3]->(b),
   * (c:Foo) DETACH DELETE c} materializes eagerly even though {@code c} touches neither the cross join's
   * other component nor the traversed edges. That over-approximation is inherited from the #6491 gate this
   * extends and is kept on purpose - a DELETE reaches the graph through more than its named target
   * (DETACH sweeps incident edges; a path variable expands to entities never named in the DELETE), so
   * proving non-overlap is harder than it looks, and getting it wrong reintroduces a wrong-results bug to
   * save memory on a shape that is rare in practice.
   *
   * @param matchClauses the MATCH clause(s) of the segment feeding the write clause (the ones since the
   *                     last WITH), not every MATCH clause of the statement - see issue #6631
   */
  private static boolean matchClausesNeedEagerDelete(final List<MatchClause> matchClauses) {
    return matchClausesHaveDisconnectedPatterns(matchClauses)
        || MatchClause.hasVariableLengthRelationships(matchClauses);
  }

  /**
   * Closes the MATCH segment tracked in {@code currentSegmentMatchClauses} at a WITH boundary: if the
   * segment being closed needs the eager guard at all ({@link #matchClausesNeedEagerDelete}), every node
   * AND relationship variable it bound is added to {@code disconnectedTaintedVariables} before the
   * segment list is cleared for the next one - a disconnected MATCH can rebind the same underlying edge
   * across rows exactly as it can a vertex, and a variable-length traverser is mid-walk over both (see
   * {@code DeleteStep}'s own {@code eagerMaterialize} field doc), so {@code DELETE r} needs the same
   * guard as {@code DELETE n}.
   * <p>
   * A WITH that plainly forwards such a variable (e.g. {@code WITH n, o}) does not neutralize the
   * issue #6491 hazard for it: rows out of a disconnected-pattern MATCH still flow one at a time through
   * a non-aggregating WITH, so a later DELETE of that same variable can still race a not-yet-produced
   * row exactly as it would with no WITH in between. Once tainted, a variable stays tainted for the rest
   * of the statement - this can only make {@link #deleteMayTargetTaintedVariable} keep guarding a DELETE
   * that no longer strictly needs it (e.g. after several more WITH-forwarding hops), never miss one that
   * does; see the class-level trade-off note on {@link #matchClausesHaveDisconnectedPatterns}.
   */
  private static void closeMatchSegment(final List<MatchClause> currentSegmentMatchClauses,
      final Set<String> disconnectedTaintedVariables) {
    if (matchClausesNeedEagerDelete(currentSegmentMatchClauses))
      for (final MatchClause match : currentSegmentMatchClauses)
        for (final PathPattern path : match.getPathPatterns()) {
          for (final NodePattern node : path.getNodes())
            if (node.getVariable() != null)
              disconnectedTaintedVariables.add(node.getVariable());
          for (final RelationshipPattern relationship : path.getRelationships())
            if (relationship.getVariable() != null)
              disconnectedTaintedVariables.add(relationship.getVariable());
        }
    currentSegmentMatchClauses.clear();
  }

  /**
   * Propagates taint through a {@code WITH ... AS alias} rename: a rename doesn't change how rows flow
   * any more than a same-name passthrough does (see {@link #closeMatchSegment}), so if the item being
   * renamed is a bare reference to an already-tainted variable, its new alias is tainted too - otherwise
   * a later DELETE of the alias would find nothing tainted under that name, even though it is exactly as
   * hazardous as deleting the original variable would have been.
   * <p>
   * Only a bare variable reference is recognised as "the same entity under a new name" ({@code WITH n AS
   * m}); an item computed from a tainted variable by any other expression ({@code WITH n.id AS m}, {@code
   * WITH count(n) AS m}) produces a value that is no longer that entity, so it carries no taint forward.
   */
  private static void propagateTaintThroughRenames(final WithClause withClause,
      final Set<String> disconnectedTaintedVariables) {
    if (disconnectedTaintedVariables.isEmpty())
      return;
    for (final ReturnClause.ReturnItem item : withClause.getItems())
      if (!item.isStar() && item.getAlias() != null
          && item.getExpression() instanceof VariableExpression variableExpression
          && disconnectedTaintedVariables.contains(variableExpression.getVariableName()))
        disconnectedTaintedVariables.add(item.getAlias());
  }

  /**
   * True when at least one of the given DELETE targets might read a variable in {@code
   * disconnectedTaintedVariables} - see {@link #closeMatchSegment}.
   * <p>
   * A target that is a bare variable name is checked directly. A target that is a chained-access or
   * function-call expression (e.g. {@code endNode(r)}, {@code p.node.id}) is not parsed here - it is
   * conservatively treated as a possible reference to any tainted variable, mirroring how {@code
   * DeleteStep} itself only special-cases the plain-variable form and falls back to full expression
   * evaluation otherwise.
   */
  private static boolean deleteMayTargetTaintedVariable(final List<String> deleteTargets,
      final Set<String> disconnectedTaintedVariables) {
    if (disconnectedTaintedVariables.isEmpty() || deleteTargets == null)
      return false;
    for (final String target : deleteTargets) {
      if (target.indexOf('.') < 0 && target.indexOf('[') < 0 && target.indexOf('(') < 0) {
        if (disconnectedTaintedVariables.contains(target))
          return true;
      } else
        return true; // non-trivial expression: cannot rule out a reference to a tainted variable
    }
    return false;
  }

  /**
   * Collects the DELETE target variables of every DELETE clause in a FOREACH body, including nested
   * FOREACH bodies - the same traversal as {@link ForeachClause#containsDelete()}, but gathering the
   * targets instead of just checking for their presence.
   */
  private static List<String> collectForeachDeleteTargetVariables(final ForeachClause foreachClause) {
    final List<String> targets = new ArrayList<>();
    collectForeachDeleteTargetVariables(foreachClause, targets);
    return targets;
  }

  private static void collectForeachDeleteTargetVariables(final ForeachClause foreachClause, final List<String> targets) {
    for (final ClauseEntry entry : foreachClause.getInnerClauses()) {
      if (entry.getType() == ClauseEntry.ClauseType.DELETE) {
        final DeleteClause deleteClause = entry.getTypedClause();
        if (deleteClause.getVariables() != null)
          targets.addAll(deleteClause.getVariables());
      } else if (entry.getType() == ClauseEntry.ClauseType.FOREACH) {
        collectForeachDeleteTargetVariables((ForeachClause) entry.getTypedClause(), targets);
      }
    }
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

    // OPTIMIZATION: the O(1) Type.count() push-down and the CSR one for chain/star/triangle/pair-join patterns.
    final AbstractExecutionStep countStep = tryCountPushDown(context, false);
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
          final Set<String> clauseRelVariables = clauseRelationshipVariables(matchClause);
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
                if (isOptional && matchChainStart == null)
                  matchChainStart = sourceStep;
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
                if (isOptional && matchChainStart == null)
                  matchChainStart = targetStep;
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

              // Smart GAV eligibility: same clause-scoped analysis as the ordered path
              final boolean[] hopNeedsEdgeTrackingLegacy = computeHopEdgeTrackingNeeds(pathPatterns, patternIndex);

              for (int i = 0; i < pathPattern.getRelationshipCount(); i++) {
                final RelationshipPattern relPattern = pathPattern.getRelationship(i);
                final NodePattern targetNode = pathPattern.getNode(i + 1);
                final String relVar;
                if (relPattern instanceof QuantifiedPathPattern)
                  relVar = null; // see the matching note in the ordered builder above
                else if (relPattern.getVariable() != null && !relPattern.getVariable().isEmpty()) {
                  if (relPattern.isVariableLength() || CypherVariableUsage.isEdgeVariableReferenced(statement, relPattern.getVariable()))
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
                if (relPattern instanceof QuantifiedPathPattern quantified) {
                  // GQL Quantified Path Pattern, Phase B (issue #4531) - see the ordered builder above
                  for (final String groupVariable : quantified.getGroupVariables())
                    if (!legacyBoundVariables.contains(groupVariable))
                      matchVariables.add(groupVariable);
                  nextStep = new QuantifiedPathStep(currentSourceVar, targetVar, pathVariable,
                      bindsGroupPathVariable(pathPattern), quantified, targetNode, matchVariables, clauseRelVariables,
                      context);
                } else if (relPattern.isVariableLength()) {
                  // Variable-length path - pass path variable, relationship variable, and target node for label
                  // filtering.
                  // DFS, not BFS: see the matching comment in the optimizer plan builder above (#6097).
                  nextStep = new ExpandPathStep(currentSourceVar, pathVariable, relVar, targetVar, relPattern, false,
                      targetNode, pathPattern.getEffectivePathMode(), matchVariables, clauseRelVariables, context);
                } else {
                  // Fixed-length relationship - pass path variable, target node pattern, and bound variables.
                  // #6311: the same snapshot rule as the ordered builder above - the hop identity-checks its
                  // target against what the row carries when it RUNS, so it gets the names bound before this
                  // MATCH plus the ones this MATCH has bound so far (this hop's own included, harmlessly - see
                  // the note there), never the planner's live set, which a following WITH empties.
                  final Set<String> targetIdentityVars = new HashSet<>(legacyBoundVariables);
                  targetIdentityVars.addAll(matchVariables);
                  nextStep = new MatchRelationshipStep(currentSourceVar, relVar, targetVar, relPattern, pathVariable,
                      targetNode, targetIdentityVars, matchVariables, clauseRelVariables, context);
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
    // Unscoped (statement.getMatchClauses()) is fine here, unlike the other two DeleteStep construction
    // sites (issue #6631): this method only runs when statement.getClausesInOrder() is null/empty, which
    // for a real parsed statement only happens when it has no tracked clauses of any kind - a
    // WITH-containing, multi-segment statement always populates clausesInOrder. This method is also
    // single-segment by construction regardless (it reads statement.getDeleteClause() and
    // statement.getMatchClauses() - one flat list each, not one per WITH-delimited segment), so it never
    // sees the multi-segment shape #6631 is about.
    if (statement.getDeleteClause() != null && !statement.getDeleteClause().isEmpty() && currentStep != null) {
      final DeleteStep deleteStep = new DeleteStep(
          statement.getDeleteClause(), context, matchClausesNeedEagerDelete(statement.getMatchClauses()));
      deleteStep.setPrevious(currentStep);
      currentStep = deleteStep;
    }

    // Step 6a: REMOVE clauses - remove properties
    for (final RemoveClause removeClause : statement.getRemoveClauses()) {
      if (!removeClause.isEmpty() && currentStep != null) {
        final RemoveStep removeStep = new RemoveStep(removeClause, context, functionFactory);
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
   * Checks if any of the given edge type names correspond to a unidirectional edge type.
   * Unidirectional edges only store outgoing links on the source vertex - the target vertex
   * has no incoming edge records. This means reverse traversal (IN direction) returns 0 results.
   */
  private boolean isAnyEdgeTypeUnidirectional(final List<String> edgeTypeNames) {
    if (edgeTypeNames == null || edgeTypeNames.isEmpty())
      return false;
    for (final String typeName : edgeTypeNames)
      if (database.getSchema().existsType(typeName)
          && database.getSchema().getType(typeName) instanceof EdgeType et
          && !et.isBidirectional())
        return true;
    return false;
  }

  /**
   * Variable names referenced by a WITH clause's non-aggregated (grouping-key) items, keyed by
   * plain variable name where the item is a bare variable, or by its full expression text
   * otherwise - matching how {@code boundVariables} containment is checked elsewhere for the
   * same items.
   */
  private static Set<String> groupingKeyVariableNames(final List<ReturnClause.ReturnItem> groupingKeys) {
    final Set<String> names = new HashSet<>();
    for (final ReturnClause.ReturnItem key : groupingKeys)
      names.add(key.getExpression() instanceof VariableExpression
          ? ((VariableExpression) key.getExpression()).getVariableName()
          : key.getExpression().getText());
    return names;
  }

  /**
   * Variables a single clause binds into scope, for the sole purpose of judging whether it can
   * fan a single input row out into several. Returns {@code null} when the clause's effect on
   * row cardinality cannot be proven safe, which the caller treats as "assume it can fan out".
   */
  private static Set<String> variablesIntroducedByClause(final ClauseEntry entry) {
    switch (entry.getType()) {
    case UNWIND:
      final UnwindClause unwind = entry.getTypedClause();
      return Collections.singleton(unwind.getVariable());

    case LOAD_CSV:
      final LoadCSVClause loadCsv = entry.getTypedClause();
      return Collections.singleton(loadCsv.getVariable());

    case MATCH:
      final MatchClause match = entry.getTypedClause();
      final Set<String> vars = new HashSet<>();
      if (match.hasPathPatterns()) {
        for (final PathPattern pattern : match.getPathPatterns()) {
          for (final NodePattern node : pattern.getNodes()) {
            // An anonymous node has no name to check against the grouping keys, so its
            // multiplicity can never be ruled out - bail rather than silently ignore it.
            if (node.getVariable() == null)
              return null;
            vars.add(node.getVariable());
          }
          for (final RelationshipPattern rel : pattern.getRelationships()) {
            // Same reasoning for an anonymous relationship, plus: a variable-length relationship
            // (named or not) can match several distinct paths between the same two endpoints,
            // fanning a row out on its own even when every variable is otherwise accounted for.
            if (rel.getVariable() == null || rel.isVariableLength())
              return null;
            vars.add(rel.getVariable());
          }
          if (pattern.hasPathVariable())
            vars.add(pattern.getPathVariable());
        }
      }
      return vars;

    case WITH:
    case CREATE:
    case SET:
    case REMOVE:
    case DELETE:
    case FOREACH:
      // These clauses project, mutate or (for WITH) possibly aggregate the existing row
      // stream - none of them can turn one input row into several, so they carry no fan-out
      // risk regardless of which variables they touch.
      return Collections.emptySet();

    default:
      // MERGE can return one row PER EXISTING MATCH when its pattern matches more than one
      // element (see MergeStep, "MERGE returns ALL matching elements"), so it is a fan-out
      // source like any MATCH and is deliberately not in the safe list above. CALL / SUBQUERY /
      // FINISH and any future clause type are equally unanalyzed here: assume the worst rather
      // than silently mis-optimize.
      return null;
    }
  }

  /**
   * True when every clause before {@code currentIndex} is guaranteed not to fan a single input
   * row out into several for the same combination of {@code groupingKeyVariables} - the
   * precondition the CountEdgesStep optimization relies on, since it emits one output row PER
   * input row rather than aggregating (issue #6629: an UNWIND before an OPTIONAL MATCH + WITH
   * count() produced one row per UNWIND element instead of one grouped row; the same break
   * happens through a cartesian-product MATCH, a variable-length hop, or any other clause that
   * introduces a variable the WITH does not group by).
   */
  private static boolean isRowPerGroupUpToClause(final List<ClauseEntry> clausesInOrder, final int currentIndex,
      final Set<String> groupingKeyVariables) {
    for (int i = 0; i < currentIndex; i++) {
      final Set<String> introduced = variablesIntroducedByClause(clausesInOrder.get(i));
      if (introduced == null)
        return false;
      for (final String var : introduced)
        if (!groupingKeyVariables.contains(var))
          return false;
    }
    return true;
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

    // A preceding clause that fans a single input row out into several rows for the same
    // grouping-key values invalidates this optimization: CountEdgesStep emits one output row
    // PER input row, so the WITH aggregation would see per-row counts instead of one grouped
    // count (issue #6629). Any variable introduced upstream that is not itself part of the
    // grouping key is a potential fan-out source (UNWIND, LOAD CSV, a cartesian-product MATCH,
    // a variable-length hop, ...) and rules the fast path out.
    if (!isRowPerGroupUpToClause(clausesInOrder, currentIndex, groupingKeyVariableNames(groupingKeys)))
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

    // Note: the bound vertex does not need to be among the grouping keys itself - see the identical
    // reasoning in tryOptimizeOptionalMatchCount (issue #6629): CountChainedEdgesStep groups its
    // input rows by the grouping-key VALUES and sums the per-row count within each group.

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
    final String[] firstHopTypes = firstRelTypes != null && !firstRelTypes.isEmpty()
        ? firstRelTypes.toArray(new String[0]) : null;

    final List<String> secondRelTypes = secondRel.getTypes();
    final String[] secondHopTypes = secondRelTypes != null && !secondRelTypes.isEmpty()
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

    // A preceding clause that fans a single input row out into several rows for the same
    // grouping-key values invalidates this optimization: CountEdgesStep emits one output row
    // PER input row, so the WITH aggregation would see per-row counts instead of one grouped
    // count (issue #6629). Any variable introduced upstream that is not itself part of the
    // grouping key is a potential fan-out source (UNWIND, LOAD CSV, a cartesian-product MATCH,
    // a variable-length hop, ...) and rules the fast path out.
    if (!isRowPerGroupUpToClause(clausesInOrder, currentIndex, groupingKeyVariableNames(groupingKeys)))
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

    // Note: the bound vertex does not need to be among the grouping keys itself - CountEdgesStep
    // groups its input rows by the grouping-key VALUES and sums the per-row edge count within each
    // group (issue #6629), so it is correct even when several input rows (a fan-out MATCH hop, an
    // UNWIND) share the same grouping key but carry different bound-vertex identities, e.g.
    // MATCH (q)-[:HAS_ANSWER]->(a) OPTIONAL MATCH (a)-[:HAS_COMMENT]->(c) WITH q, count(c) AS cnt.

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
    final String[] edgeTypes = relTypes != null && !relTypes.isEmpty()
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
      // This requires reverse traversal (IN direction at the target vertex), which only
      // works for bidirectional edges. Unidirectional edges don't store incoming links.
      if (isAnyEdgeTypeUnidirectional(relPattern.getTypes()))
        return null;
      anchorVar = targetVar;
      anchorNode = targetNode;
      final Direction relDirection = relPattern.getDirection();
      // Reverse direction since we're counting from the other end
      countDirection = relDirection == Direction.OUT ? Vertex.DIRECTION.IN
          : relDirection == Direction.IN ? Vertex.DIRECTION.OUT : Vertex.DIRECTION.BOTH;
    } else
      return null;

    // Grouping expressions are evaluated on the anchor row only, so they must not reference
    // any other variable (the counted node, a named relationship, etc.). Anything else must
    // fall back to the general GroupByAggregationStep that sees the fully expanded rows (#5206).
    for (final ReturnClause.ReturnItem item : groupingItems)
      if (!referencesOnlyVariable(item.getExpression(), anchorVar))
        return null;

    // Edge types
    final List<String> relTypes = relPattern.getTypes();
    final String[] edgeTypes = relTypes != null && !relTypes.isEmpty()
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

    // Determine target label for filtering (the counted node's label, if any). The step keys the
    // filter on one name, which is not what a conjunction or a disjunction asks for, so a label set
    // that name cannot stand for declines the push-down (issue #6322).
    final NodePattern countedNode = anchorNode == sourceNode ? targetNode : sourceNode;
    if (!hasPushDownRepresentableLabel(countedNode))
      return null;
    final String targetLabel = countedNode.hasLabels() ? countedNode.getLabels().get(0) : null;

    final CountEdgesReturnStep countStep = new CountEdgesReturnStep(
        anchorVar, countDirection, edgeTypes, countAlias, targetLabel,
        groupingExpressions, groupingAliases, context,
        expressionEvaluator != null ? expressionEvaluator.getFunctionFactory() : null);
    countStep.setPrevious(nodeStep);
    return countStep;
  }

  /**
   * Returns true only if the expression is guaranteed to reference no variable other than
   * {@code allowedVar}. Used to validate the count-edges fast path (#5206): grouping
   * expressions are evaluated on the anchor row alone, so a reference to any other pattern
   * variable (counted node, named relationship) would silently evaluate to null. Unknown
   * expression types are conservatively treated as referencing other variables.
   */
  private static boolean referencesOnlyVariable(final Expression expr, final String allowedVar) {
    if (expr == null)
      return true;
    if (expr instanceof LiteralExpression || expr instanceof ParameterExpression || expr instanceof StarExpression)
      return true;
    if (expr instanceof VariableExpression varExpr)
      return varExpr.getVariableName().equals(allowedVar);
    if (expr instanceof PropertyAccessExpression propAccess)
      return propAccess.getVariableName().equals(allowedVar);
    if (expr instanceof FunctionCallExpression funcExpr) {
      for (final Expression arg : funcExpr.getArguments())
        if (!referencesOnlyVariable(arg, allowedVar))
          return false;
      return true;
    }
    if (expr instanceof ArithmeticExpression arith)
      return referencesOnlyVariable(arith.getLeft(), allowedVar) && referencesOnlyVariable(arith.getRight(), allowedVar);
    if (expr instanceof CaseExpression caseExpr) {
      if (!referencesOnlyVariable(caseExpr.getCaseExpression(), allowedVar))
        return false;
      for (final CaseAlternative alternative : caseExpr.getAlternatives())
        if (!referencesOnlyVariable(alternative.getWhenExpression(), allowedVar)
            || !referencesOnlyVariable(alternative.getThenExpression(), allowedVar))
          return false;
      return referencesOnlyVariable(caseExpr.getElseExpression(), allowedVar);
    }
    if (expr instanceof ListExpression listExpr) {
      for (final Expression element : listExpr.getElements())
        if (!referencesOnlyVariable(element, allowedVar))
          return false;
      return true;
    }
    if (expr instanceof ListIndexExpression listIndex)
      return referencesOnlyVariable(listIndex.getListExpression(), allowedVar)
          && referencesOnlyVariable(listIndex.getIndexExpression(), allowedVar);
    if (expr instanceof ListSliceExpression listSlice)
      return referencesOnlyVariable(listSlice.getListExpression(), allowedVar)
          && referencesOnlyVariable(listSlice.getFromExpression(), allowedVar)
          && referencesOnlyVariable(listSlice.getToExpression(), allowedVar);
    if (expr instanceof MapExpression mapExpr) {
      for (final Expression value : mapExpr.getEntries().values())
        if (!referencesOnlyVariable(value, allowedVar))
          return false;
      return true;
    }
    if (expr instanceof TernaryLogicalExpression ternary)
      return referencesOnlyVariable(ternary.getLeft(), allowedVar) && referencesOnlyVariable(ternary.getRight(), allowedVar);
    if (expr instanceof BooleanWrapperExpression boolWrapper)
      return booleanReferencesOnlyVariable(boolWrapper.getBooleanExpression(), allowedVar);
    if (expr instanceof ComparisonExpressionWrapper compWrapper)
      return booleanReferencesOnlyVariable(compWrapper.getComparison(), allowedVar);
    // Unknown expression type: assume it may reference other variables
    return false;
  }

  private static boolean booleanReferencesOnlyVariable(final BooleanExpression expr, final String allowedVar) {
    if (expr == null)
      return true;
    if (expr instanceof ComparisonExpression comp)
      return referencesOnlyVariable(comp.getLeft(), allowedVar) && referencesOnlyVariable(comp.getRight(), allowedVar);
    if (expr instanceof LogicalExpression logical)
      return booleanReferencesOnlyVariable(logical.getLeft(), allowedVar)
          && booleanReferencesOnlyVariable(logical.getRight(), allowedVar);
    if (expr instanceof IsNullExpression isNull)
      return referencesOnlyVariable(isNull.getExpression(), allowedVar);
    if (expr instanceof BooleanCoercionExpression coercion)
      return referencesOnlyVariable(coercion.getExpression(), allowedVar);
    if (expr instanceof InExpression inExpr) {
      if (!referencesOnlyVariable(inExpr.getExpression(), allowedVar))
        return false;
      for (final Expression element : inExpr.getList())
        if (!referencesOnlyVariable(element, allowedVar))
          return false;
      return true;
    }
    if (expr instanceof StringMatchExpression strMatch)
      return referencesOnlyVariable(strMatch.getExpression(), allowedVar)
          && referencesOnlyVariable(strMatch.getPattern(), allowedVar);
    if (expr instanceof RegexExpression regex)
      return referencesOnlyVariable(regex.getExpression(), allowedVar)
          && referencesOnlyVariable(regex.getPattern(), allowedVar);
    if (expr instanceof LabelCheckExpression labelCheck)
      return referencesOnlyVariable(labelCheck.getVariableExpression(), allowedVar);
    // Unknown boolean expression type: assume it may reference other variables
    return false;
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
   * - RETURN clause with exactly one item: COUNT(variable) or COUNT(*)
   * - No other clauses (WITH, ORDER BY, etc.); SKIP and LIMIT are applied to the row it produces
   * <p>
   * Uses O(1) database.countType() instead of O(n) iteration.
   *
   * @param context       command context
   * @param countRowsMode see {@link #typeCountOutputAlias}, which is what it selects here
   *
   * @return optimized TypeCountStep if pattern matches, null otherwise
   */
  private AbstractExecutionStep tryCreateTypeCountOptimization(final CommandContext context,
      final boolean countRowsMode) {
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

    // Only a single label can be counted with the O(1) countType() shortcut. A multi-label
    // conjunction pattern (n:A:B) has no single type representing "instanceOf A AND instanceOf B"
    // (each label maps to its own supertype, and the composite type also carries any other
    // labels the node was created with), so counting by the first label alone would over-count
    // (issue #5084). Multi-label (and label-disjunction) patterns fall back to the regular
    // materialization path, which filters on every label correctly.
    if (nodePattern.getLabels().size() != 1 || nodePattern.isLabelDisjunction())
      return null;

    // Node must not have property constraints
    if (nodePattern.hasProperties())
      return null;

    final String variable = nodePattern.getVariable();
    final String typeName = nodePattern.getLabels().get(0);

    // MATCH (n:Label) matches only vertices. If the label collides with an existing edge or
    // document type (labels and relationship types are separate namespaces in Cypher), the O(1)
    // countType() shortcut would wrongly count those edges/documents. Fall back to the regular
    // path (MatchNodeStep), which yields 0 rows for a non-vertex label. A non-existent type is
    // left to TypeCountStep, which already returns 0 for it (issue #5226, consistent with #5194).
    if (context.getDatabase().getSchema().existsType(typeName)
        && !(context.getDatabase().getSchema().getType(typeName) instanceof VertexType))
      return null;

    final String outputAlias = typeCountOutputAlias(countRowsMode, variable);
    if (outputAlias == null)
      return null;

    // Must not have any other clauses that would invalidate the optimization
    if (!statement.getUnwindClauses().isEmpty())
      return null;

    if (!statement.getWithClauses().isEmpty())
      return null;

    if (statement.getOrderByClause() != null)
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

    // Reject any remaining clause type that the typed accessors above do not cover (FOREACH,
    // CALL, SUBQUERY, LOAD_CSV, FINISH, ...). These can carry write side effects (e.g. a
    // FOREACH ... CREATE) or alter the result set, and short-circuiting to a bare TypeCountStep
    // would silently discard them (issue #5166: FOREACH CREATE nodes were never persisted).
    // The optimization is only valid for a query made of exactly the single MATCH and the RETURN.
    if (statement.getClausesInOrder() != null)
      for (final ClauseEntry clause : statement.getClausesInOrder()) {
        final ClauseEntry.ClauseType type = clause.getType();
        if (type != ClauseEntry.ClauseType.MATCH && type != ClauseEntry.ClauseType.RETURN)
          return null;
      }

    // All conditions met - create optimized TypeCountStep
    return new TypeCountStep(typeName, outputAlias, context);
  }

  /**
   * The name a {@link TypeCountStep} over a single-node pattern publishes its count under, or null when the RETURN
   * is not asking for that count.
   * <p>
   * {@code count(*)} is accepted alongside {@code count(<the MATCH variable>)}: a single-node pattern produces
   * exactly one row per node, so the two are the same number. Before issue #5715 this detector required the
   * argument to name the variable while every {@code count(*)} detector required at least one relationship, so
   * {@code MATCH (m:Big) RETURN count(*)} - the spelling Neo4j documents - fell between the two and scanned. With
   * the star accepted the pattern does not even need to bind a variable, so {@code MATCH (:Big) RETURN count(*)} is
   * answered too.
   */
  private String typeCountOutputAlias(final boolean countRowsMode, final String matchVariable) {
    if (countRowsMode)
      return rowCountAlias();

    final ReturnClause returnClause = statement.getReturnClause();
    if (returnClause == null || returnClause.isDistinct() || returnClause.getReturnItems().size() != 1)
      return null;

    final ReturnClause.ReturnItem returnItem = returnClause.getReturnItems().get(0);
    if (!(returnItem.getExpression() instanceof FunctionCallExpression funcExpr))
      return null;
    if (!"count".equalsIgnoreCase(funcExpr.getFunctionName()) || funcExpr.getArguments().size() != 1)
      return null;

    final Expression countArg = funcExpr.getArguments().get(0);
    if (countArg instanceof VariableExpression varExpr) {
      if (matchVariable == null || !matchVariable.equals(varExpr.getVariableName()))
        return null;
    } else if (!(countArg instanceof StarExpression))
      return null;

    return returnItem.getOutputName();
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
      if (item.isStar())
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
  private BooleanExpression extractPushdownFilter(final WhereClause whereClause, final String currentVar,
      final Set<String> boundVariables, final Set<String> matchVariables) {
    if (whereClause == null || whereClause.getConditionExpression() == null)
      return null;

    // Available variables = already bound + already matched in this clause + the current variable
    final Set<String> available = new HashSet<>();
    available.addAll(boundVariables);
    available.addAll(matchVariables);
    available.add(currentVar);

    final BooleanExpression extracted = WhereClause.extractForVariables(whereClause.getConditionExpression(), available);
    // Strip {@code id(var) = X} / {@code elementId(var) = X} predicates whose RHS is statically
    // resolvable (literal or parameter): the static idFilter optimisation already enforces them via
    // direct RID lookup, so re-evaluating them in the row-by-row pushdown filter is wasted work and
    // would also drop the row after id() was made Neo4j-compatible (returns Long instead of an RID
    // string, issue #4183) because the cross-type comparison Long vs. String fails. Predicates with
    // dynamic RHS (e.g. {@code id(a) = b.id}) are left alone because extractIdFilter does not consume
    // them.
    return stripStaticIdEqualities(extracted);
  }

  /**
   * Removes every top-level {@code id(var) = X} / {@code elementId(var) = X} equality (in either argument order) whose RHS resolves to a constant at plan time
   * (literal or parameter). Walks AND chains; leaves OR / NOT subtrees untouched because the static idFilter optimisation only consumes AND-chained equalities.
   */
  private BooleanExpression stripStaticIdEqualities(final BooleanExpression expr) {
    if (expr == null)
      return null;

    if (expr instanceof ComparisonExpression comp
        && comp.getOperator() == ComparisonExpression.Operator.EQUALS) {
      if (isStaticIdEquality(comp.getLeft(), comp.getRight())
          || isStaticIdEquality(comp.getRight(), comp.getLeft()))
        return null;
    }

    if (expr instanceof LogicalExpression logical && logical.getOperator() == LogicalExpression.Operator.AND) {
      final BooleanExpression left = stripStaticIdEqualities(logical.getLeft());
      final BooleanExpression right = stripStaticIdEqualities(logical.getRight());
      if (left != null && right != null)
        return new LogicalExpression(LogicalExpression.Operator.AND, left, right);
      if (left != null)
        return left;
      return right;
    }

    return expr;
  }

  private boolean isStaticIdEquality(final Expression idSide, final Expression valueSide) {
    if (!(idSide instanceof FunctionCallExpression func))
      return false;
    final String name = func.getFunctionName();
    if (!("id".equalsIgnoreCase(name) || "elementid".equalsIgnoreCase(name)) || func.getArguments().size() != 1)
      return false;
    if (!(func.getArguments().get(0) instanceof VariableExpression))
      return false;
    if (valueSide instanceof LiteralExpression)
      return true;
    if (valueSide instanceof ParameterExpression paramExpr)
      return parameters != null && parameters.containsKey(paramExpr.getParameterName());
    return false;
  }

  /**
   * Determines which hops of one comma-separated pattern part have to bind their edge so that Cypher's
   * relationship uniqueness can be enforced.
   * <p>
   * Uniqueness is scoped to the whole MATCH clause, not to one pattern part: two parts separated by a comma
   * are one pattern and may never bind the same relationship (two separate MATCH clauses may - that is the
   * documented difference between the comma and a second MATCH). The check itself is a comparison against the
   * edges the row already carries, so a hop that is never asked to bind its edge is invisible to it. Scoping
   * this analysis to a single part therefore did not merely miss an optimisation, it dropped the constraint:
   * a part that cannot collide with itself - a single hop, typically - bound nothing, and the hop of a later
   * part had nothing to compare against (issue #6310).
   * <p>
   * A hop is left free - which is what keeps it on the GAV/CSR fast path - only when no other hop in the
   * clause can be the same physical edge:
   * <ul>
   *   <li>the only hop in the clause has nothing to collide with;</li>
   *   <li>two hops whose edge types are disjoint in the schema are always different edges. An edge has
   *   exactly one type, so a hop's type list is a disjunction: the pair is disjoint only when every
   *   combination is. Subtyping counts as overlap - {@code [:BASE]} and {@code [:SUB]} both match a
   *   {@code SUB} edge;</li>
   *   <li>two hops whose OUT or IN endpoint patterns are type-disjoint cannot be the same edge either,
   *   whatever their types.</li>
   * </ul>
   *
   * @param clausePatterns the comma-separated pattern parts of the MATCH clause being planned
   * @param patternIndex   index in {@code clausePatterns} of the part being planned
   *
   * @return boolean array, true at index i if hop i of that part must bind its edge
   */
  private boolean[] computeHopEdgeTrackingNeeds(final List<PathPattern> clausePatterns, final int patternIndex) {
    final PathPattern pathPattern = clausePatterns.get(patternIndex);
    final boolean[] needs = new boolean[pathPattern.getRelationshipCount()];

    for (int i = 0; i < needs.length; i++) {
      for (int p = 0; p < clausePatterns.size() && !needs[i]; p++) {
        final PathPattern other = clausePatterns.get(p);
        for (int j = 0; j < other.getRelationshipCount(); j++) {
          if (p == patternIndex && i == j)
            continue;
          if (hopsCanMatchTheSameEdge(pathPattern, i, other, j)) {
            needs[i] = true;
            break;
          }
        }
      }
    }
    return needs;
  }

  /**
   * Whether two hops of the same MATCH clause could bind the same physical edge, and so have to be compared
   * against each other for relationship uniqueness. Conservative: it answers yes unless the schema proves
   * otherwise, because a wrong no silently returns rows Cypher forbids.
   */
  private boolean hopsCanMatchTheSameEdge(final PathPattern patternI, final int hopI, final PathPattern patternJ,
      final int hopJ) {
    final RelationshipPattern relI = patternI.getRelationship(hopI);
    final RelationshipPattern relJ = patternJ.getRelationship(hopJ);

    // A hop capped at zero edges - [*0..0] - binds none, so it has nothing to share with anybody.
    if (bindsNoEdge(relI) || bindsNoEdge(relJ))
      return false;

    // A GQL quantified group (issue #4531) binds whatever its inner pattern's own hops bind, but the
    // synthetic hop standing in for it carries PLACEHOLDER types and direction, not the inner ones -
    // neither test below can say anything true about it. Answer conservatively here rather than let
    // those placeholders decide it indirectly, which is what QuantifiedPathPattern's class javadoc asks
    // of every loop over getRelationships(). This is load-bearing: a "no" would let
    // computeHopEdgeTrackingNeeds drop the sibling hop's relationship variable, and without it on the
    // row QuantifiedPathStep#collectBoundRelationships cannot see that edge and the group would reuse
    // it, breaking relationship isomorphism.
    if (relI instanceof QuantifiedPathPattern || relJ instanceof QuantifiedPathPattern)
      return true;

    if (relI.hasTypes() && relJ.hasTypes() && edgeTypesAreDisjoint(relI.getTypes(), relJ.getTypes()))
      return false;

    return !areHopsDisjointByEndpointLabels(patternI, hopI, patternJ, hopJ);
  }

  /**
   * Whether no edge in the schema can satisfy both type lists. An edge carries exactly one type, so each list
   * is a disjunction and the two are disjoint only when every pair of alternatives is.
   */
  private boolean edgeTypesAreDisjoint(final List<String> typesI, final List<String> typesJ) {
    for (final String typeI : typesI)
      for (final String typeJ : typesJ)
        if (!labelsAreTypeDisjoint(typeI, typeJ))
          return false;
    return true;
  }

  /**
   * Every relationship variable a MATCH clause writes, whether or not this clause is the one that binds it.
   * <p>
   * The other half of the uniqueness scope, alongside the variables the clause binds. A relationship variable
   * an earlier clause already bound - a {@code CALL}'s yield, a {@code WITH} - is still one of this clause's
   * relationship patterns when the clause names it again, so it is a co-participant the clause's other
   * patterns must be distinct from. The freshly-bound set alone cannot say so: it deliberately omits an
   * already-bound name, because {@code OPTIONAL MATCH} nulls what it lists and must not null a carried
   * binding.
   */
  private static Set<String> clauseRelationshipVariables(final MatchClause matchClause) {
    if (!matchClause.hasPathPatterns())
      return Set.of();
    Set<String> variables = null;
    for (final PathPattern pathPattern : matchClause.getPathPatterns())
      for (final RelationshipPattern relationship : pathPattern.getRelationships()) {
        final String variable = relationship.getVariable();
        if (variable != null && !variable.isEmpty()) {
          if (variables == null)
            variables = new HashSet<>();
          variables.add(variable);
        }
        if (relationship instanceof QuantifiedPathPattern quantified) {
          if (variables == null)
            variables = new HashSet<>();
          variables.addAll(quantified.getGroupVariables());
        }
      }
    return variables == null ? Set.of() : variables;
  }

  /**
   * Checks if two hops of the same MATCH clause - each one identified by its pattern part and its index
   * inside it, so the pair may straddle a comma - are guaranteed to match different physical edges based
   * on the vertex labels at their edge endpoints.
   * <p>
   * An edge has exactly one OUT vertex and one IN vertex. If we can prove that the OUT vertex
   * (or IN vertex) for hop i must be of a different type than for hop j, the edges are distinct.
   * <p>
   * Uses the pattern direction to map pattern nodes to edge OUT/IN endpoints:
   * <ul>
   *   <li>OUT direction: edge OUT = source node (node[i]), edge IN = target node (node[i+1])</li>
   *   <li>IN direction: edge OUT = target node (node[i+1]), edge IN = source node (node[i])</li>
   *   <li>BOTH direction: cannot determine mapping → conservative (not disjoint)</li>
   *   <li>a hop that can span more than one edge: the mapping describes the whole path, not one edge
   *   → conservative (not disjoint)</li>
   * </ul>
   */
  private boolean areHopsDisjointByEndpointLabels(final PathPattern patternI, final int hopI,
      final PathPattern patternJ, final int hopJ) {
    final RelationshipPattern relI = patternI.getRelationship(hopI);
    final RelationshipPattern relJ = patternJ.getRelationship(hopJ);

    // A variable-length hop's endpoint patterns constrain its first and last edge only: everything in
    // between starts and ends at a node the pattern says nothing about, so proving the declared endpoints
    // disjoint proves nothing about those edges. Only a hop pinned to a single edge can be argued about
    // this way; the type proof above stays valid, since every edge of the hop must match its type list.
    if (spansMoreThanOneEdge(relI) || spansMoreThanOneEdge(relJ))
      return false;

    final Direction dirI = relI.getDirection();
    final Direction dirJ = relJ.getDirection();

    // BOTH direction: can't determine which node is the edge's OUT/IN vertex
    if (dirI == Direction.BOTH || dirJ == Direction.BOTH)
      return false;

    // Map pattern nodes to edge endpoints based on direction
    final NodePattern edgeOutI = dirI == Direction.OUT ? patternI.getNode(hopI) : patternI.getNode(hopI + 1);
    final NodePattern edgeOutJ = dirJ == Direction.OUT ? patternJ.getNode(hopJ) : patternJ.getNode(hopJ + 1);
    final NodePattern edgeInI = dirI == Direction.OUT ? patternI.getNode(hopI + 1) : patternI.getNode(hopI);
    final NodePattern edgeInJ = dirJ == Direction.OUT ? patternJ.getNode(hopJ + 1) : patternJ.getNode(hopJ);

    // If the OUT vertex labels are type-disjoint, the edges are different
    if (nodeLabelsAreTypeDisjoint(edgeOutI, edgeOutJ))
      return true;

    // If the IN vertex labels are type-disjoint, the edges are different
    return nodeLabelsAreTypeDisjoint(edgeInI, edgeInJ);
  }

  /** Whether the hop is pinned to zero edges, and so binds none at all. */
  private static boolean bindsNoEdge(final RelationshipPattern rel) {
    final Integer maxHops = rel.getMaxHops();
    return rel.isVariableLength() && maxHops != null && maxHops == 0;
  }

  /** Whether the hop can bind more than one edge, so its declared endpoints are not one edge's endpoints. */
  private static boolean spansMoreThanOneEdge(final RelationshipPattern rel) {
    if (!rel.isVariableLength())
      return false;
    final Integer maxHops = rel.getMaxHops();
    return maxHops == null || maxHops > 1;
  }

  /**
   * Checks whether two node patterns have labels that are type-disjoint in the schema,
   * meaning no vertex can match both patterns simultaneously.
   * <p>
   * Two labels are disjoint if neither is a supertype/subtype of the other AND no type
   * in the schema is a subtype of both (which would allow a vertex to match both labels).
   * <p>
   * A node can write more than one label, and this is the one place where reading only the first of
   * them can go wrong in <em>either</em> direction, because the answer is a proof rather than a
   * filter: the caller uses it to conclude that two hops cannot be the same edge, so a wrong "yes"
   * drops matches (issue #6322). The two spellings are therefore taken apart:
   * <ul>
   *   <li>{@code (a:A:B)} requires both, so proving <em>any</em> of its labels disjoint from any of
   *   the other node's requirements proves the nodes disjoint;</li>
   *   <li>{@code (a:A|B)} requires either, so the pattern is only disjoint when <em>every</em>
   *   alternative is.</li>
   * </ul>
   * Reading the first label alone answers the conjunction too narrowly, which merely misses an
   * opportunity, and the disjunction too widely, which is the wrong answer.
   */
  private boolean nodeLabelsAreTypeDisjoint(final NodePattern node1, final NodePattern node2) {
    if (!node1.hasLabels() || !node2.hasLabels())
      return false; // Without labels, can't prove disjointness

    // Every alternative of node1 must be disjoint from every alternative of node2. A conjunction is
    // a single alternative listing all its labels; a disjunction is one alternative per label.
    for (final List<String> alternative1 : labelAlternatives(node1))
      for (final List<String> alternative2 : labelAlternatives(node2))
        if (!alternativesAreTypeDisjoint(alternative1, alternative2))
          return false;

    return true;
  }

  /**
   * The label sets a node pattern accepts: one set holding every label for a conjunction, one
   * single-label set per alternative for a disjunction.
   */
  private static List<List<String>> labelAlternatives(final NodePattern node) {
    if (!node.isLabelDisjunction())
      return List.of(node.getLabels());

    final List<List<String>> alternatives = new ArrayList<>(node.getLabels().size());
    for (final String label : node.getLabels())
      alternatives.add(List.of(label));
    return alternatives;
  }

  /**
   * Whether two conjunctions of labels cannot be satisfied by the same vertex, which holds as soon as
   * one label of the first is type-disjoint from one label of the second.
   */
  private boolean alternativesAreTypeDisjoint(final List<String> labels1, final List<String> labels2) {
    for (final String label1 : labels1)
      for (final String label2 : labels2)
        if (labelsAreTypeDisjoint(label1, label2))
          return true;
    return false;
  }

  /** Whether no vertex in the schema can carry both labels. */
  private boolean labelsAreTypeDisjoint(final String label1, final String label2) {
    if (label1.equals(label2))
      return false; // Same label → not disjoint

    if (!database.getSchema().existsType(label1) || !database.getSchema().existsType(label2))
      return false; // Unknown type → conservative

    final DocumentType type1 = database.getSchema().getType(label1);
    final DocumentType type2 = database.getSchema().getType(label2);

    // Direct hierarchy check: if one extends the other, not disjoint
    if (type1.instanceOf(label2) || type2.instanceOf(label1))
      return false;

    // Check for common subtypes: if any type extends both, a vertex could match both labels
    for (final DocumentType schemaType : database.getSchema().getTypes())
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
  /**
   * Tells whether any clause after {@code clauseIndex} reads the graph, which makes the updating
   * clause at {@code clauseIndex} eager with respect to that read (issue #6922).
   * <p>
   * The pipeline pulls in batches of 100 rows, so a write step that applies one batch of writes per
   * pull lets a following reader observe a snapshot taken at an arbitrary batch boundary rather than
   * the pre- or post-write graph. openCypher resolves the same read/write conflict by making the
   * update eager; this predicate decides when to pay for that.
   * <p>
   * The set is deliberately conservative on CALL: a procedure is opaque here, so any procedure call
   * after the update counts as a read - which is what {@code CALL meta.stats()} in issue #6922 is.
   * SUBQUERY counts because a CALL { ... } block can contain a MATCH.
   * <p>
   * A later FOREACH counts only when its own body reads. MATCH and CALL are not valid inside a FOREACH
   * body at all, so that means a MERGE (at any nesting depth), or an expression that reads: the list
   * the FOREACH iterates, or - because the grammar lets a general expression stand wherever a value
   * does - a CREATE property value, a SET right-hand side, or a DELETE/REMOVE target. Those clauses
   * act on variables the row already carries, but the values they compute need not.
   * <p>
   * A terminal RETURN's ORDER BY is deliberately NOT scanned. It is parsed onto the statement rather
   * than onto the ReturnClause, and unlike the WITH case just below it is unreachable: OrderByStep
   * materializes its whole input before the comparator that evaluates those expressions ever runs, so
   * the FOREACH has already finished by then no matter what this predicate answered.
   * <p>
   * RETURN, WITH and UNWIND are not reads in themselves - they project rows the pipeline already
   * produced - but the expressions they carry can be. {@code EXISTS { }}, {@code COUNT { }} and
   * {@code COLLECT { }} run a real query against the live graph per row, and a pattern predicate,
   * pattern comprehension or shortestPath() traverses it, so those clauses are scanned for such an
   * expression instead of being waved through on their clause type alone.
   *
   * @param clausesInOrder the query's clauses in textual order
   * @param clauseIndex    index of the updating clause, or a negative value when it is not in the list
   */
  private boolean graphReadFollows(final List<ClauseEntry> clausesInOrder, final int clauseIndex) {
    if (clauseIndex < 0)
      return false;
    for (int i = clauseIndex + 1; i < clausesInOrder.size(); i++) {
      final ClauseEntry laterClause = clausesInOrder.get(i);
      switch (laterClause.getType()) {
      case MATCH:
      case MERGE:
      case CALL:
      case SUBQUERY:
        return true;
      case FOREACH:
        if (foreachBodyReadsGraph(laterClause.getTypedClause()))
          return true;
        break;
      case RETURN:
        if (returnItemsReadGraph(((ReturnClause) laterClause.getClause()).getReturnItems()))
          return true;
        break;
      case WITH:
        final WithClause withClause = laterClause.getTypedClause();
        if (returnItemsReadGraph(withClause.getItems()))
          return true;
        if (withClause.getWhereClause() != null
            && expressionReadsGraph(withClause.getWhereClause().getConditionExpression()))
          return true;
        if (withClause.getOrderByClause() != null)
          for (final OrderByClause.OrderByItem orderByItem : withClause.getOrderByClause().getItems())
            if (expressionReadsGraph(orderByItem.getExpressionAST()))
              return true;
        break;
      case UNWIND:
        if (expressionReadsGraph(((UnwindClause) laterClause.getClause()).getListExpression()))
          return true;
        break;
      case SET:
        if (setItemsReadGraph(laterClause.getTypedClause()))
          return true;
        break;
      case REMOVE:
        if (removeItemsReadGraph(laterClause.getTypedClause()))
          return true;
        break;
      default:
        break;
      }
    }
    return false;
  }

  /**
   * Tells whether a SET clause reads the graph. Every right-hand side counts - the assigned value, an expression
   * target, a dynamic property key, and a Cypher 25 dynamic label ({@code SET n:$(expr)}, issue #7059) - because
   * each is evaluated per row against the live graph. Shared by {@link #graphReadFollows(List, int)} and
   * {@link #foreachBodyReadsGraph(ForeachClause)} so a SET is classified the same wherever it sits.
   */
  private boolean setItemsReadGraph(final SetClause setClause) {
    for (final SetClause.SetItem setItem : setClause.getItems())
      if (expressionReadsGraph(setItem.getValueExpression()) || expressionReadsGraph(setItem.getKeyExpression())
          || expressionReadsGraph(setItem.getTargetExpression())
          || expressionsReadGraph(setItem.getLabelExpressions()))
        return true;
    return false;
  }

  /** {@link #setItemsReadGraph(SetClause)}, for the dynamic key and dynamic label of a REMOVE clause. */
  private boolean removeItemsReadGraph(final RemoveClause removeClause) {
    for (final RemoveClause.RemoveItem removeItem : removeClause.getItems())
      if (expressionReadsGraph(removeItem.getKeyExpression())
          || expressionsReadGraph(removeItem.getLabelExpressions()))
        return true;
    return false;
  }

  /**
   * Tells whether a FOREACH reads the graph: a MERGE in its body at any nesting depth, or a read in
   * the list expression it iterates over. Used by {@link #graphReadFollows(List, int)} so that a read
   * tucked inside a following FOREACH arms the eager mode just like a bare one would (issue #6922).
   */
  private boolean foreachBodyReadsGraph(final ForeachClause foreachClause) {
    // The list a FOREACH drives off is an expression like any other: it can be a pattern
    // comprehension or hold a subquery, and it is evaluated per row against the live graph.
    if (expressionReadsGraph(foreachClause.getListExpression()))
      return true;
    for (final ClauseEntry innerClause : foreachClause.getInnerClauses()) {
      switch (innerClause.getType()) {
      case MERGE:
        return true;
      case FOREACH:
        if (foreachBodyReadsGraph(innerClause.getTypedClause()))
          return true;
        break;
      case CREATE:
        if (createValuesReadGraph(innerClause.getTypedClause()))
          return true;
        break;
      case SET:
        if (setItemsReadGraph(innerClause.getTypedClause()))
          return true;
        break;
      case REMOVE:
        if (removeItemsReadGraph(innerClause.getTypedClause()))
          return true;
        break;
      case DELETE:
        for (final Expression deleteExpression : ((DeleteClause) innerClause.getClause()).getExpressions())
          if (expressionReadsGraph(deleteExpression))
            return true;
        break;
      default:
        break;
      }
    }
    return false;
  }

  /**
   * Tells whether a CREATE inside a FOREACH body reads the graph through one of its property values.
   * <p>
   * The pattern itself creates rather than matches, but the grammar lets a general expression stand
   * as a property value, and a general expression can be {@code count { ... }}: in
   * {@code FOREACH (x IN [1] | CREATE (:S {n: count {{ MATCH (m:L1) }}}))} the value is evaluated per
   * row against the live graph, which makes the FOREACH before it a reader after all.
   */
  private boolean createValuesReadGraph(final CreateClause createClause) {
    for (final PathPattern pathPattern : createClause.getPathPatterns()) {
      for (final NodePattern nodePattern : pathPattern.getNodes())
        if (propertyValuesReadGraph(nodePattern.getProperties()) || expressionsReadGraph(nodePattern.getDynamicLabels()))
          return true;
      for (final RelationshipPattern relationshipPattern : pathPattern.getRelationships())
        if (propertyValuesReadGraph(relationshipPattern.getProperties()))
          return true;
    }
    return false;
  }

  /**
   * Tells whether any value of an inline property map reads the graph. The map holds already-evaluated
   * literals alongside unevaluated {@code Expression} nodes, so every value is offered to the detector
   * and a non-expression simply answers false.
   */
  private boolean propertyValuesReadGraph(final Map<String, Object> properties) {
    if (properties == null)
      return false;
    for (final Object value : properties.values())
      if (expressionReadsGraph(value))
        return true;
    return false;
  }

  private boolean expressionsReadGraph(final List<Expression> expressions) {
    if (expressions == null)
      return false;
    for (final Expression expression : expressions)
      if (expressionReadsGraph(expression))
        return true;
    return false;
  }

  private boolean returnItemsReadGraph(final List<ReturnClause.ReturnItem> returnItems) {
    for (final ReturnClause.ReturnItem returnItem : returnItems)
      if (expressionReadsGraph(returnItem.getExpression()))
        return true;
    return false;
  }

  /**
   * Tells whether an expression tree holds anything that goes back to the graph while the row is
   * being projected: a subquery expression ({@code EXISTS { }}, {@code COUNT { }}, {@code COLLECT { }},
   * which all run a real query per row through {@code CorrelatedSubqueryRunner}) or a pattern that is
   * traversed ({@code (n)-->()} as a predicate, a pattern comprehension, {@code shortestPath()}).
   * Used by {@link #graphReadFollows(List, int)} for issue #6922.
   */
  private boolean expressionReadsGraph(final Object expression) {
    if (expression == null)
      return false;
    final GraphReadDetector detector = new GraphReadDetector();
    detector.rewrite(expression);
    return detector.found;
  }

  /**
   * Walks an expression tree looking for a node that reads the graph, reusing {@link ExpressionRewriter}'s
   * traversal instead of re-deriving one: the check sits in the {@code rewrite} entry point every node
   * passes through, so it also catches the expression types the rewriter's own dispatch does not know
   * (COUNT and COLLECT subqueries among them). Nothing is rewritten - every visit returns its input.
   * <p>
   * Eighteen of the base class's visits return without recursing into what they wrap, and each one can
   * hide a real case here: {@code size(collect {{ ... }})} behind {@code visitFunctionCall}, and
   * {@code WHERE exists {{ ... }}} behind the boolean coercion/wrapper pair. Every one is overridden
   * below rather than filled in on the shared base class, where every other rewriter would inherit a
   * traversal change none of them asked for. {@code visitUnknown} then closes the remaining hole: a
   * type the base dispatch does not know at all counts as a read rather than as a leaf.
   */
  private static final class GraphReadDetector extends ExpressionRewriter {
    private boolean found = false;

    @Override
    public Object rewrite(final Object expression) {
      if (found || expression == null)
        return expression;
      if (expression instanceof ExistsExpression || expression instanceof CountExpression
          || expression instanceof CollectExpression || expression instanceof PatternPredicateExpression
          || expression instanceof PatternComprehensionExpression || expression instanceof ShortestPathExpression) {
        found = true;
        return expression;
      }
      // The general expression parser builds this for a top-level AND/OR/XOR/NOT, so it is far too
      // common to leave to the conservative visitUnknown default: doing so would arm the eager mode
      // behind every `RETURN a AND b` following a FOREACH, whether or not anything there reads.
      if (expression instanceof TernaryLogicalExpression ternary) {
        rewrite(ternary.getLeft());
        if (!found)
          rewrite(ternary.getRight());
        return expression;
      }
      return super.rewrite(expression);
    }

    /**
     * Fails CLOSED where the base class fails open: an expression type {@link ExpressionRewriter}'s
     * dispatch does not know is treated as a possible read rather than waved through.
     * <p>
     * For the rewriters the base class was written for, an unreached subtree costs an optimization.
     * Here it costs correctness - a missed read leaves the FOREACH streaming and the query answers
     * from a batch-boundary snapshot - so the two defaults have to differ. This also covers
     * {@code CypherExpressionBuilder.ChainedPropertyAccessExpression} (package-private, so it cannot
     * be dispatched on by type from here) and any {@code Expression} subtype added later, which would
     * otherwise each need their own fix to this class.
     */
    @Override
    protected Object visitUnknown(final Object expression) {
      found = true;
      return expression;
    }

    @Override
    protected Expression visitFunctionCall(final FunctionCallExpression expr) {
      return descend(expr, expr.getArguments());
    }

    @Override
    protected BooleanExpression visitBooleanCoercion(final BooleanCoercionExpression expr) {
      return descend(expr, expr.getExpression());
    }

    @Override
    protected Expression visitBooleanWrapper(final BooleanWrapperExpression expr) {
      return descend(expr, expr.getBooleanExpression());
    }

    @Override
    protected Expression visitCase(final CaseExpression expr) {
      rewrite(expr.getCaseExpression());
      for (final CaseAlternative alternative : expr.getAlternatives()) {
        rewrite(alternative.getWhenExpression());
        rewrite(alternative.getThenExpression());
      }
      return descend(expr, expr.getElseExpression());
    }

    @Override
    protected Expression visitList(final ListExpression expr) {
      return descend(expr, expr.getElements());
    }

    @Override
    protected Expression visitMap(final MapExpression expr) {
      return descend(expr, expr.getEntries().values());
    }

    @Override
    protected Expression visitMapProjection(final MapProjectionExpression expr) {
      for (final MapProjectionExpression.ProjectionElement element : expr.getElements())
        rewrite(element.getExpression());
      return expr;
    }

    @Override
    protected Expression visitListComprehension(final ListComprehensionExpression expr) {
      rewrite(expr.getListExpression());
      rewrite(expr.getWhereExpression());
      return descend(expr, expr.getMapExpression());
    }

    @Override
    protected Expression visitListPredicate(final ListPredicateExpression expr) {
      rewrite(expr.getListExpression());
      return descend(expr, expr.getWhereExpression());
    }

    @Override
    protected Expression visitReduce(final ReduceExpression expr) {
      rewrite(expr.getInitialValue());
      rewrite(expr.getListExpression());
      return descend(expr, expr.getReduceExpression());
    }

    @Override
    protected Expression visitAllReduce(final AllReduceExpression expr) {
      rewrite(expr.getInitialValue());
      rewrite(expr.getListExpression());
      rewrite(expr.getReduceExpression());
      return descend(expr, expr.getPredicateExpression());
    }

    @Override
    protected Expression visitListIndex(final ListIndexExpression expr) {
      rewrite(expr.getListExpression());
      return descend(expr, expr.getIndexExpression());
    }

    @Override
    protected Expression visitListSlice(final ListSliceExpression expr) {
      rewrite(expr.getListExpression());
      rewrite(expr.getFromExpression());
      return descend(expr, expr.getToExpression());
    }

    @Override
    protected Expression visitArithmetic(final ArithmeticExpression expr) {
      rewrite(expr.getLeft());
      return descend(expr, expr.getRight());
    }

    @Override
    protected BooleanExpression visitIn(final InExpression expr) {
      rewrite(expr.getExpression());
      return descend(expr, expr.getList());
    }

    @Override
    protected BooleanExpression visitIsNull(final IsNullExpression expr) {
      return descend(expr, expr.getExpression());
    }

    @Override
    protected BooleanExpression visitStringMatch(final StringMatchExpression expr) {
      rewrite(expr.getExpression());
      return descend(expr, expr.getPattern());
    }

    @Override
    protected BooleanExpression visitRegex(final RegexExpression expr) {
      rewrite(expr.getExpression());
      return descend(expr, expr.getPattern());
    }

    @Override
    protected BooleanExpression visitLabelCheck(final LabelCheckExpression expr) {
      return descend(expr, expr.getVariableExpression());
    }

    private <T> T descend(final T expr, final Object child) {
      rewrite(child);
      return expr;
    }

    private <T> T descend(final T expr, final Collection<? extends Object> children) {
      for (final Object child : children)
        rewrite(child);
      return expr;
    }
  }

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
   * Whether this statement is made of nothing but MATCH and RETURN.
   * <p>
   * {@code ORDER BY} is asked about here even though it is not a {@link ClauseEntry.ClauseType}: it is a field of the
   * statement, so a walk of the clause list alone cannot see it, which is how {@code ORDER BY} - and, until issue
   * #5715, {@code SKIP} and {@code LIMIT} - reached a push-down that replaces the whole step chain and drops
   * whatever it did not build. {@code SKIP} and {@code LIMIT} are now applied to the single row a push-down
   * produces, by {@link #applySkipAndLimit}, so they do not disqualify a statement; {@code ORDER BY} still does,
   * because sorting by an aggregate alias is the ordinary pipeline's business.
   */
  private boolean isMatchReturnOnlyStatement() {
    if (statement.getOrderByClause() != null)
      return false;
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
   * The single entry point of both count push-downs, and the only place either is reached from.
   * <p>
   * They used to be two independent detectors reached from different places: {@code tryOptimizeCountStar} from
   * {@link #execute()}, before the optimizer dispatch, and {@code tryCreateTypeCountOptimization} only from
   * {@link #buildExecutionStepsWithOrder}, which a top-level query reaches only when the optimizer declines it. A
   * single-label {@code MATCH (m:Big) RETURN count(m)} satisfies the optimizer, so the O(1) counter was available to
   * a subquery body and not to the plainest counting query there is (issue #5715).
   * <p>
   * Sharing the entry point is also what keeps their preconditions from drifting apart again: whatever holds for one
   * is asked once, here or in {@link #isMatchReturnOnlyStatement()}.
   *
   * @param countRowsMode when true the caller wants the number of <b>rows</b> the statement produces rather than the
   *                      value of a {@code count()} it projects - what {@code COUNT { }} asks for. See
   *                      {@link #countPushDownAlias}.
   */
  private AbstractExecutionStep tryCountPushDown(final CommandContext context, final boolean countRowsMode) {
    return tryCountPushDown(context, countRowsMode, SeedCorrelation.UNCORRELATED);
  }

  /**
   * @param correlation which of the names the seed row carries this body reads, and the row that bound them. Only the
   *                    chain operator can seed itself from a bound anchor, so a correlated body reaches that one
   *                    alone; an uncorrelated one reaches every detector, as before.
   */
  private AbstractExecutionStep tryCountPushDown(final CommandContext context, final boolean countRowsMode,
      final SeedCorrelation correlation) {
    if (!correlation.isSeedable())
      return null;

    // The O(1) type counter answers "how many vertices carry this label", which is not a question a bound anchor
    // narrows: a seeded MATCH (q:Q) is one vertex tested against a label, not a count over the label.
    AbstractExecutionStep step = correlation.isCorrelated() ? null : tryCreateTypeCountOptimization(context, countRowsMode);
    if (step == null)
      step = tryOptimizeCountStar(context, countRowsMode, correlation);
    if (step == null)
      return null;
    return applySkipAndLimit(step, context);
  }

  /**
   * Applies the statement's SKIP and LIMIT to the single row a count push-down produces.
   * <p>
   * A push-down returns a step that replaces the whole chain, so the {@code SkipStep} and {@code LimitStep} the
   * ordinary pipeline would have built are never reached: {@code RETURN count(*) LIMIT 0} answered with a row, and
   * {@code SKIP 1} with the count instead of nothing (issue #5715). Applying them here rather than refusing the
   * statement that carries them keeps a count written with a harmless {@code LIMIT 1} on the fast path.
   */
  private AbstractExecutionStep applySkipAndLimit(final AbstractExecutionStep countStep, final CommandContext context) {
    if (statement.getSkip() == null && statement.getLimit() == null)
      return countStep;

    // Built the way the ordinary pipeline builds it at every other SKIP/LIMIT site, rather than reusing the field:
    // the field is null for a plan constructed without one, and what the evaluation needs is the function factory it
    // carries, not the instance.
    final CypherFunctionFactory functionFactory = expressionEvaluator != null ?
        expressionEvaluator.getFunctionFactory() : null;
    final ExpressionEvaluator evaluator = new ExpressionEvaluator(functionFactory);

    AbstractExecutionStep currentStep = countStep;
    if (statement.getSkip() != null) {
      final SkipStep skipStep =
          new SkipStep(evaluator.evaluateSkipLimit(statement.getSkip(), new ResultInternal(), context), context);
      skipStep.setPrevious(currentStep);
      currentStep = skipStep;
    }
    if (statement.getLimit() != null) {
      final LimitStep limitStep =
          new LimitStep(evaluator.evaluateSkipLimit(statement.getLimit(), new ResultInternal(), context), context);
      limitStep.setPrevious(currentStep);
      currentStep = limitStep;
    }
    return currentStep;
  }

  /**
   * The name the pushed-down count is published under, or null when this statement is not asking for one.
   * <p>
   * In the ordinary mode that is the alias of the {@code count()} the RETURN projects. In {@code countRowsMode} the
   * caller wants the number of rows and the RETURN only has to <b>preserve</b> that number, which is what
   * {@link #returnPreservesRowCount()} decides: a {@code COUNT { MATCH (m:Big) }} body is normalised to one row per
   * match, so its row count <i>is</i> the match count and the same push-downs answer it (issue #5715).
   */
  private String countPushDownAlias(final boolean countRowsMode) {
    return countRowsMode ? rowCountAlias() : isCountStarReturn();
  }

  /**
   * The alias a row-count push-down publishes under, or null when this statement's RETURN would not leave the row
   * count alone.
   * <p>
   * Both detectors ask it, and it is one method rather than a branch in each of them for the reason this whole issue
   * exists: a precondition written down twice is a precondition that drifts.
   */
  private String rowCountAlias() {
    return returnPreservesRowCount() ? ROW_COUNT_ALIAS : null;
  }

  /**
   * Whether the RETURN clause emits exactly one row per matched row.
   * <p>
   * An absent RETURN does (the row reaches the caller as it is), and so does any projection that neither aggregates -
   * which collapses the rows into one per group - nor is {@code DISTINCT}, which drops duplicates.
   * <p>
   * <b>This is a wider shape than the one the ordinary push-down accepts</b>, which is a {@code RETURN} of exactly one
   * count item. {@code RETURN *} and a projection of several non-aggregating items are both accepted here, and both
   * are outside what {@code CypherUncorrelatedSubqueryCountPushDownIssue5686Test} asserts
   * {@link CypherReferencedVariables} models - that tie is about the other entry point and does not carry over to
   * this one. What makes the widening safe is that no projection can add or drop a row, so the row count is the match
   * count whatever the body names - the seed row included, since a seeded body's projection cannot change how many
   * matches there are either.
   * <p>
   * {@code RETURN *} is the one of these a <b>correlated</b> body never reaches: it makes
   * {@link CypherReferencedVariables} incomplete, which {@link #seedCorrelationOf} reports as an unknown correlation,
   * and an unknown correlation takes no push-down at all.
   * <p>
   * An item whose expression is null is read as "does not preserve", so an unmodelled projection costs the
   * optimization rather than the answer.
   */
  private boolean returnPreservesRowCount() {
    final ReturnClause returnClause = statement.getReturnClause();
    if (returnClause == null)
      return true;
    if (returnClause.isDistinct())
      return false;
    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
      final Expression expression = item.getExpression();
      if (expression == null || expression.containsAggregation())
        return false;
    }
    return true;
  }

  /**
   * Unified entry point: tries all count-push-down patterns and wraps the result in a CSRCountStep.
   */
  private AbstractExecutionStep tryOptimizeCountStar(final CommandContext context, final boolean countRowsMode,
      final SeedCorrelation correlation) {
    final String alias = countPushDownAlias(countRowsMode);
    if (alias == null || !isMatchReturnOnlyStatement())
      return null;

    // Count-push-down operators reason only about node labels and edge types; they cannot honor
    // inline property filters (e.g. (a:Node {id: 1})) or dynamic labels on the pattern's nodes.
    // If any node carries such a filter, skip all push-down detectors so the query falls back to
    // the normal materialization pipeline, which applies the filter. See issue #5071.
    if (hasInlineNodePropertyOrDynamicLabel())
      return null;

    CountOp op = tryDetectChainCountStar(context.getDatabase(), correlation);
    // Only the chain operator can start its walk from an anchor the outer row bound. The star, triangle, pair-join
    // and anti-join ones all derive their anchor set from a label, so a correlated body is left to the ordinary
    // pipeline rather than answered over every vertex carrying that label (issue #5758).
    if (op == null && !correlation.isCorrelated()) {
      op = tryDetectAntiJoinChainCountStar();
      if (op == null)
        op = tryDetectStarCountStar();
      if (op == null)
        op = tryDetectTriangleCountStar();
      if (op == null)
        op = tryDetectPairJoinCountStar();
    }
    if (op == null)
      return null;

    // An operator with no set of anchors to walk from answers 0 for a pattern that does match (issue #5715).
    if (!op.canEnumerateAnchors())
      return null;

    // Only asked once a detector has claimed the statement, so this replaces a push-down that would have run
    // anyway rather than changing the plan of an unrelated query.
    if (mandatoryPatternElementIsEmpty(context.getDatabase()))
      return new ConstantCountStep(0L, alias, context);

    return new CSRCountStep(op, alias, context);
  }

  /**
   * Whether some element the matched pattern <b>requires</b> is empty or undeclared, which makes the count 0 whatever
   * the graph holds.
   * <p>
   * The push-down is applied with no cost check at all: 100 vertices with no edge cost 200 record reads to answer 0,
   * and an edge type absent from the schema cost the same (issue #5715). Every node label and relationship type of a
   * non-optional path pattern has to be matched for any row to exist, so one of them holding nothing settles the
   * count without reading anything.
   * <p>
   * Only <b>non-optional</b> MATCH clauses are read: an OPTIONAL arm that matches nothing still contributes its row.
   * A relationship's types are alternatives, so all of them have to be empty; a node's labels are a conjunction
   * unless the pattern is a disjunction, where again all of them have to be. A variable-length relationship is
   * skipped altogether, since one with a zero-length minimum matches without traversing an edge.
   */
  private boolean mandatoryPatternElementIsEmpty(final Database db) {
    if (statement.getMatchClauses() == null)
      return false;

    for (final MatchClause matchClause : statement.getMatchClauses()) {
      if (matchClause.isOptional() || !matchClause.hasPathPatterns())
        continue;

      for (final PathPattern pathPattern : matchClause.getPathPatterns()) {
        final int hopCount = pathPattern.getRelationshipCount();
        for (int i = 0; i <= hopCount; i++) {
          final NodePattern node = pathPattern.getNode(i);
          if (node.hasLabels() && allTypesAreEmpty(db, node.getLabels(), node.isLabelDisjunction()))
            return true;
        }
        for (int i = 0; i < hopCount; i++) {
          final RelationshipPattern rel = pathPattern.getRelationship(i);
          if (!rel.isVariableLength() && rel.hasTypes() && allTypesAreEmpty(db, rel.getTypes(), true))
            return true;
        }
      }
    }
    return false;
  }

  /**
   * Whether the named types settle the element as unmatchable: all of them when they are alternatives, any one of
   * them when they are a conjunction the matched record has to satisfy at once.
   */
  private static boolean allTypesAreEmpty(final Database db, final List<String> names, final boolean alternatives) {
    for (final String name : names) {
      final boolean empty = typeIsProvablyEmpty(db, name);
      if (alternatives) {
        if (!empty)
          return false;
      } else if (empty)
        return true;
    }
    return alternatives;
  }

  /**
   * Whether a type holds no record at all: either it is not declared, or its counter is 0.
   * <p>
   * {@code countType} sums the buckets' cached counters plus the transaction delta, so in the steady state this reads
   * nothing. A counter reading -1 - a fresh open with no statistics entry, or an unclean shutdown - is recomputed
   * once by scanning the bucket, under its file lock; that cold start is paid by the first query to ask for the
   * count either way, and every one after it is O(1).
   * <p>
   * It is only meaningful for a type whose instances <b>are</b> records: a LIGHTWEIGHT edge type keeps no edge
   * record, so its counter is 0 while its edges exist in the vertices' edge lists, and it answers "not empty" here.
   * <p>
   * It is asked by name, and a name is not a namespace: a node label matches only vertices, but a document or edge
   * type may carry the same name, and a populated one of those answers "not empty" for a label no vertex has. That
   * costs the early-out and nothing else - the push-down then runs and computes the same 0, because it filters on
   * the label's buckets, which hold no vertex either. {@link #tryCreateTypeCountOptimization} has to guard the
   * collision because it answers <i>from</i> the counter; this only decides whether to skip work.
   */
  private static boolean typeIsProvablyEmpty(final Database db, final String name) {
    final Schema schema = db.getSchema();
    if (!schema.existsType(name))
      return true;

    final DocumentType type = schema.getType(name);
    if (type instanceof EdgeType && holdsALightweightEdgeType(type))
      return false;

    return db.countType(name, true) == 0;
  }

  /** Whether the edge type, or any type inheriting from it, stores its edges without a record of their own. */
  private static boolean holdsALightweightEdgeType(final DocumentType type) {
    if (type instanceof EdgeType edgeType && edgeType.isLightweight())
      return true;
    for (final DocumentType subType : type.getSubTypes())
      if (holdsALightweightEdgeType(subType))
        return true;
    return false;
  }

  /**
   * Returns true if any node in any MATCH path pattern carries an inline property filter
   * (e.g. {@code {id: 1}} or {@code $props}) or a dynamic label. Such filters cannot be honored by
   * the count-push-down operators, which key purely off node labels and edge types. See issue #5071.
   */
  private boolean hasInlineNodePropertyOrDynamicLabel() {
    if (statement.getMatchClauses() == null)
      return false;
    for (final MatchClause mc : statement.getMatchClauses()) {
      if (!mc.hasPathPatterns())
        continue;
      for (final PathPattern pp : mc.getPathPatterns())
        for (int i = 0; i <= pp.getRelationshipCount(); i++) {
          final NodePattern node = pp.getNode(i);
          if (node.hasProperties() || node.hasDynamicLabels())
            return true;
        }
    }
    return false;
  }

  private CountOp tryDetectChainCountStar(final Database db, final SeedCorrelation correlation) {
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
      // One name per hop is all the operator carries, and a conjunction or a disjunction is not one
      // name; rather than count a set the pattern did not describe, decline (issue #6322).
      if (!hasPushDownRepresentableLabel(node))
        return null;
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

    if (!correlation.isCorrelated())
      return new PropagateChainOp(nodeLabels, edgeTypes, directions, inequalityIdxA, inequalityIdxB);

    return seededChainOp(db, correlation, pathPattern, nodeLabels, edgeTypes, directions, inequalityVar1 != null);
  }

  /**
   * The same chain walked from the vertex the outer row bound, or null when the correlation is not one anchor at one
   * end of the chain.
   * <p>
   * The enumerating operator seeds its propagation from a label's bucket set, which is why a body reading a seeded
   * name had to lose the push-down: {@code MATCH (q)-[:LINKS]->(x:Q) RETURN count(*)} with {@code q} bound would have
   * been answered with the count over every {@code q} in the graph. A bound anchor does not make that count expensive
   * though, it makes it cheaper - an O(degree) read of the adjacency arrays - so the guard narrows from "the body
   * reads a seeded name" to "the body reads a seeded name at a position this operator cannot seed from" (issue
   * #5758).
   * <p>
   * What it can seed from is <b>one</b> name, bound to <b>one</b> vertex, at <b>one</b> end of the chain:
   * <ul>
   * <li>Two seeded names are two anchor sets, and the propagation carries one.</li>
   * <li>A name written at two positions - {@code (q)-[:LINKS]->(q)} - is an equality between the two ends that the
   * propagation does not enforce; it would count every path, not the returning ones.</li>
   * <li>A name in the middle of the chain would have to be walked outwards in both directions at once.</li>
   * <li>A name read somewhere other than a node position - only in the projection, say - names no position at all,
   * so {@code anchorIdx} stays -1 and the same test refuses it.</li>
   * <li>An inequality is refused outright: its two positions are indexes into the chain, and the reversal below
   * would have to renumber them for a case worth no complication.</li>
   * </ul>
   * The far end is reached by reversing the chain rather than by a second walk: {@code (a)-[:E]->(b)} counted from
   * {@code b} is {@code (b)<-[:E]-(a)}, which is the same arrays read the other way. That is what keeps
   * {@code COUNT { (:Person)-[:KNOWS]->(p) }} - "how many know me" - on the fast path alongside its mirror.
   * <p>
   * <b>The bound value is always the outer one.</b> The whole thing rests on the seeded name still meaning what the
   * outer row bound, and what could break that is a body rebinding the name for itself. It cannot: the only clauses
   * that bind a name to something other than a pattern position are {@code UNWIND} and {@code WITH}, and
   * {@link #isMatchReturnOnlyStatement()} - asked by {@link #tryOptimizeCountStar} before any detector runs - admits
   * nothing but {@code MATCH} and {@code RETURN}. A body's own {@code MATCH (q:Q)} written under an outer {@code q}
   * is not a rebinding but the correlation itself, which is exactly what this seeds from.
   */
  private CountOp seededChainOp(final Database db, final SeedCorrelation correlation, final PathPattern pathPattern,
      final String[] nodeLabels, final String[] edgeTypes, final Vertex.DIRECTION[] directions,
      final boolean hasInequality) {
    if (!correlation.isSeedable() || hasInequality || correlation.readNames().size() != 1)
      return null;

    final String seededName = correlation.readNames().iterator().next();
    final int hopCount = edgeTypes.length;

    int anchorIdx = -1;
    for (int i = 0; i <= hopCount; i++)
      if (seededName.equals(pathPattern.getNode(i).getVariable())) {
        if (anchorIdx >= 0)
          return null;
        anchorIdx = i;
      }

    if (anchorIdx != 0 && anchorIdx != hopCount)
      return null;

    final RID anchorRid = boundVertexRid(db, correlation.boundValue(seededName));
    if (anchorRid == null)
      return null;

    if (anchorIdx == 0)
      return new PropagateChainOp(nodeLabels, edgeTypes, directions, -1, -1, anchorRid);

    return new PropagateChainOp(reversed(nodeLabels), reversed(edgeTypes), reversedDirections(directions), -1, -1,
        anchorRid);
  }

  /**
   * The RID of a bound value that is a vertex, or null for anything else.
   * <p>
   * A node pattern matches only vertices, so a name bound to a scalar, to a document or to an edge matches nothing.
   * Rather than teach the operator to answer 0 for those, they are left to the ordinary pipeline, which is the only
   * thing here that knows what each of them means - and which is what answered them before this existed.
   */
  private static RID boundVertexRid(final Database db, final Object value) {
    Object bound = value;
    if (bound instanceof Result result)
      bound = result.getElement().orElse(null);
    if (!(bound instanceof Identifiable identifiable))
      return null;

    final RID rid = identifiable.getIdentity();
    if (rid == null)
      return null;
    return db.getSchema().getTypeByBucketId(rid.getBucketId()) instanceof VertexType ? rid : null;
  }

  private static String[] reversed(final String[] values) {
    final String[] out = new String[values.length];
    for (int i = 0; i < values.length; i++)
      out[i] = values[values.length - 1 - i];
    return out;
  }

  /** Reverses the hop order and flips each direction, so the chain reads identically from its far end. */
  private static Vertex.DIRECTION[] reversedDirections(final Vertex.DIRECTION[] directions) {
    final Vertex.DIRECTION[] out = new Vertex.DIRECTION[directions.length];
    for (int i = 0; i < directions.length; i++) {
      final Vertex.DIRECTION direction = directions[directions.length - 1 - i];
      if (direction == Vertex.DIRECTION.OUT)
        out[i] = Vertex.DIRECTION.IN;
      else if (direction == Vertex.DIRECTION.IN)
        out[i] = Vertex.DIRECTION.OUT;
      else
        out[i] = Vertex.DIRECTION.BOTH;
    }
    return out;
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
    // Must have at least one MATCH clause
    if (statement.getMatchClauses() == null || statement.getMatchClauses().isEmpty())
      return null;

    // No statement-level WHERE
    if (statement.getWhereClause() != null)
      return null;

    // Find the central variable: the one that appears in multiple path patterns.
    // First pass: count occurrences of each variable across all path patterns.
    final HashMap<String, Integer> varCounts = new HashMap<>();
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

    // Find the label for the central variable. The variable is written once per arm and the
    // operator enumerates one type of central node, so every occurrence has to agree on it: a label
    // set the single name cannot stand for, or two occurrences naming different types, declines the
    // push-down and leaves the query to the ordinary pipeline, which applies each of them (#6322).
    // An inline property filter or dynamic label on any occurrence is just as unenforceable as a
    // rejected label set - the operator has no way to check either - so it declines the push-down
    // too, exactly as the arm-endpoint loop below does (#6431). In practice this is caught earlier
    // by hasInlineNodePropertyOrDynamicLabel() in tryOptimizeCountStar (#5071); this check exists so
    // the detector is correct standing alone, not only in combination with that outer guard.
    for (final MatchClause mc : statement.getMatchClauses()) {
      if (!mc.hasPathPatterns()) continue;
      for (final PathPattern pp : mc.getPathPatterns())
        for (int i = 0; i <= pp.getRelationshipCount(); i++) {
          final NodePattern node = pp.getNode(i);
          if (!centralVar.equals(node.getVariable()))
            continue;
          if (node.hasProperties() || node.hasDynamicLabels())
            return null;
          if (!node.hasLabels())
            continue;
          if (!hasPushDownRepresentableLabel(node))
            return null;
          if (labelsConflict(centralLabel, node.getLabels().get(0)))
            return null;
          centralLabel = node.getLabels().get(0);
        }
    }

    final ArrayList<DegreeProductOp.Arm> armList = new ArrayList<>();

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

        // Every non-central node of the arm - the far endpoint and any interior node of a multi-hop
        // arm alike - is a label, inline property filter, or dynamic label the degree product cannot
        // enforce: it counts degree off the arm's edge types and directions alone, with no field on
        // Arm for a per-hop endpoint type or filter, so (:Author), (:Author {status:'active'}) and ()
        // built the same operator and the same, over-counted, answer. Decline the push-down rather
        // than silently drop the filter, exactly as the central variable's own check already does
        // above (issue #6337 for labels, #6431 for properties/dynamic labels, both siblings of #6322).
        for (int i = 0; i <= totalHops; i++) {
          if (i == centralNodeIdx)
            continue;
          final NodePattern node = pathPattern.getNode(i);
          if (node.hasLabels() || node.hasProperties() || node.hasDynamicLabels())
            return null;
        }

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
    final ArrayList<String> cycleVars = new ArrayList<>();
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
    final NodePattern probeNode1 = probePattern.getFirstNode();
    final NodePattern probeNode2 = probePattern.getLastNode();
    final String probeVar1 = probeNode1.getVariable();
    final String probeVar2 = probeNode2.getVariable();
    if (probeVar1 == null || probeVar2 == null)
      return null;
    if (!hasPushDownRepresentableLabel(probeNode1) || !hasPushDownRepresentableLabel(probeNode2))
      return null;

    // The two patterns name the same two variables, so a label written on the probe side constrains the arm
    // that ends there just as one written on the build side does. Reading only the build side dropped it
    // silently, on the CSR path and the OLTP one alike (issue #6304).
    final String probeLabel1 = probeNode1.hasLabels() ? probeNode1.getLabels().get(0) : null;
    final String probeLabel2 = probeNode2.hasLabels() ? probeNode2.getLabels().get(0) : null;

    // Build pattern: extract chain and find which hops reach the shared endpoints
    final int buildHops = buildPattern.getRelationshipCount();

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
    if (!hasPushDownRepresentableLabel(startNode))
      return null;
    final String buildStartLabel = startNode.hasLabels() ? startNode.getLabels().get(0) : null;
    if (buildStartLabel == null)
      return null;

    // Build two arms from startNodeIdx: backward (toward position 0) and forward (toward end).
    // Each arm reaches one of the shared endpoint variables.

    // Walk backward from startNodeIdx to find endpoint reaching probeVar1 or probeVar2
    final ArrayList<String> bwdET = new ArrayList<>();
    final ArrayList<Vertex.DIRECTION> bwdDir = new ArrayList<>();
    final ArrayList<String> bwdLabels = new ArrayList<>();
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
      if (!hasPushDownRepresentableLabel(targetNode))
        return null;
      bwdLabels.add(targetNode.hasLabels() ? targetNode.getLabels().get(0) : null);
      final String nodeVar = targetNode.getVariable();
      if (nodeVar != null && (nodeVar.equals(probeVar1) || nodeVar.equals(probeVar2))) {
        bwdEndpointVar = nodeVar;
        break;
      }
    }

    // Walk forward from startNodeIdx to find the other endpoint
    final ArrayList<String> fwdET = new ArrayList<>();
    final ArrayList<Vertex.DIRECTION> fwdDir = new ArrayList<>();
    final ArrayList<String> fwdLabels = new ArrayList<>();
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
      if (!hasPushDownRepresentableLabel(targetNode))
        return null;
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

    // Each arm's last hop lands on a shared variable, which is the one the probe pattern may also have
    // labelled. Both labels are in force, and the operator carries one per hop: when the two disagree the
    // filter is their intersection, which one name cannot express, so the push-down declines and the ordinary
    // pipeline - which evaluates both - answers instead.
    if (labelsConflict(arm1Labels[arm1Labels.length - 1], probeLabel1)
        || labelsConflict(arm2Labels[arm2Labels.length - 1], probeLabel2))
      return null;
    if (probeLabel1 != null)
      arm1Labels[arm1Labels.length - 1] = probeLabel1;
    if (probeLabel2 != null)
      arm2Labels[arm2Labels.length - 1] = probeLabel2;

    return new PairHashJoinOp(buildStartLabel, arm1ET, arm1Dir, arm1Labels,
        arm2ET, arm2Dir, arm2Labels, probeEdgeType, probeDirection);
  }

  /**
   * Whether a count push-down can express this node's labels as the single name it keys a bucket set on.
   * <p>
   * {@code (a:A:B)} keeps only what carries both and {@code (a:A|B)} keeps what carries either; taking the first
   * of the list turns each of them into {@code (a:A)}, which is neither. An operator built on that filter counts
   * a set the pattern did not describe, so the detector declines and leaves the query to the ordinary pipeline
   * (issue #6304, extended to the remaining detectors by issue #6322).
   * <p>
   * The one place a multi-label node is <em>not</em> answered by declining is
   * {@link #nodeLabelsAreTypeDisjoint(NodePattern, NodePattern)}, which produces a proof rather than a filter
   * and takes the two spellings apart instead.
   */
  private static boolean hasPushDownRepresentableLabel(final NodePattern node) {
    return !node.hasLabels() || (node.getLabels().size() == 1 && !node.isLabelDisjunction());
  }

  /** Whether two labels written on the same variable name different types, which no single label can stand for. */
  private static boolean labelsConflict(final String a, final String b) {
    return a != null && b != null && !a.equals(b);
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
      // One name per hop is all the operator carries, and a conjunction or a disjunction is not one
      // name; rather than count a set the pattern did not describe, decline (issue #6322).
      if (!hasPushDownRepresentableLabel(node))
        return null;
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

      // Check if left side is ID(variable) or elementId(variable)
      final Expression left = compExpr.getLeft();
      if (left instanceof FunctionCallExpression) {
        final FunctionCallExpression funcExpr = (FunctionCallExpression) left;
        final String funcName = funcExpr.getFunctionName();
        if (("id".equalsIgnoreCase(funcName) || "elementid".equalsIgnoreCase(funcName))
            && funcExpr.getArguments().size() == 1) {
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

    // Handle literal values: strings already have the RID format; numeric literals come from id()
    // (Long-encoded RID, issue #4183) and must be decoded back to a #bucketId:offset string so the
    // downstream MatchNodeStep can resolve the record via newRID.
    if (expr instanceof LiteralExpression) {
      final LiteralExpression litExpr = (LiteralExpression) expr;
      final Object value = litExpr.getValue();
      return toRidString(value);
    }

    // Handle parameter references
    if (expr instanceof ParameterExpression) {
      final ParameterExpression paramExpr = (ParameterExpression) expr;
      final String paramName = paramExpr.getParameterName();
      if (parameters != null && parameters.containsKey(paramName)) {
        final Object value = parameters.get(paramName);
        return toRidString(value);
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

  private static String toRidString(final Object value) {
    if (value == null)
      return null;
    if (value instanceof Number number)
      return IdFunction.decodeLongToRidString(number.longValue());
    return value.toString();
  }
}
