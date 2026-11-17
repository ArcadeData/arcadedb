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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.function.graph.IdFunction;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphViewPins;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LoadCSVClause;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SimpleCypherStatement;
import com.arcadedb.query.opencypher.ast.ShortestPathPattern;
import com.arcadedb.query.opencypher.ast.StarExpression;
import com.arcadedb.query.opencypher.ast.UnionStatement;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.executor.operators.GAVExpandAll;
import com.arcadedb.query.opencypher.executor.operators.NodeByLabelScan;
import com.arcadedb.query.opencypher.executor.steps.CSRCountStep;
import com.arcadedb.query.opencypher.executor.steps.CallStep;
import com.arcadedb.query.opencypher.executor.steps.ConstantCountStep;
import com.arcadedb.query.opencypher.executor.steps.CountEdgesReturnStep;
import com.arcadedb.query.opencypher.executor.steps.CountEdgesStep;
import com.arcadedb.query.opencypher.executor.steps.CountOp;
import com.arcadedb.query.opencypher.executor.steps.CreateStep;
import com.arcadedb.query.opencypher.executor.steps.DeleteStep;
import com.arcadedb.query.opencypher.executor.steps.GAVOneHopScanStep;
import com.arcadedb.query.opencypher.executor.steps.LimitStep;
import com.arcadedb.query.opencypher.executor.steps.MatchNodeStep;
import com.arcadedb.query.opencypher.executor.steps.MergeStep;
import com.arcadedb.query.opencypher.executor.steps.ParallelRowSource;
import com.arcadedb.query.opencypher.executor.steps.ProductCountStep;
import com.arcadedb.query.opencypher.executor.steps.PropagateChainOp;
import com.arcadedb.query.opencypher.executor.steps.RemoveStep;
import com.arcadedb.query.opencypher.executor.steps.SetStep;
import com.arcadedb.query.opencypher.executor.steps.SkipStep;
import com.arcadedb.query.opencypher.executor.steps.UnionStep;
import com.arcadedb.query.opencypher.executor.steps.VariableProjectionStep;
import com.arcadedb.query.opencypher.optimizer.AnchorSelector;
import com.arcadedb.query.opencypher.optimizer.plan.AnchorSelection;
import com.arcadedb.query.opencypher.optimizer.plan.LogicalNode;
import com.arcadedb.query.opencypher.optimizer.plan.LogicalPlan;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.query.opencypher.optimizer.statistics.CostModel;
import com.arcadedb.query.opencypher.optimizer.statistics.IndexStatistics;
import com.arcadedb.query.opencypher.optimizer.statistics.StatisticsProvider;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.ParallelRecordScan;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.query.sql.parser.ExplainResultSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;

/**
 * Execution plan for a Cypher query.
 * Contains the chain of execution steps and executes them.
 * <p>
 * Phase 4: Enhanced with Cost-Based Query Optimizer support.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherExecutionPlan {
  /**
   * The column a count push-down publishes under when it is answering "how many rows does this statement produce"
   * rather than a {@code count()} the statement projects. Space-prefixed the way every internal name on a row is,
   * so no Cypher identifier can collide with it.
   */
  private static final String ROW_COUNT_ALIAS = " rowCount";

  /** The rows a seeded body ({@code CALL { }}, a subquery expression) reads at a time when its caller names no demand. */
  public static final int SEEDED_BATCH_SIZE = 100;

  /**
   * How much smaller than its label an index seek's estimated rows must be for a count to be left to the ordinary plan rather than
   * pushed down (issue #11680). The pipeline pays more per row than a push-down pays per vertex it walks: on the issue's graph an
   * anchored chain walked a Record in about 0.25 us and a Section in about 2 us, while the seek plus expansion produced a row in
   * at most 13 us. A factor of 100 keeps the seek ahead even at that worst ratio, so only a seek that clearly reads a small part of
   * the label declines the push-down. With the optimizer's estimates that is a UNIQUE key, or a key of three columns or more.
   */
  private static final int SELECTIVE_ANCHOR_LABEL_SHARE = 100;

  private final DatabaseInternal     database;
  private final CypherStatement      statement;
  private final Map<String, Object>  parameters;
  private final ContextConfiguration configuration;
  /** Name of the step that runs the optimizer's physical operators, first in the chain it builds. */
  private static final String OPTIMIZED_MATCH_STEP_NAME = "OptimizedMatchStep";
  private final PhysicalPlan         physicalPlan;
  private final ExpressionEvaluator  expressionEvaluator;

  /** See {@link #foldedCountPlan()}: null until computed, empty when there is nothing to fold. */
  private volatile Optional<CypherExecutionPlan> foldedCountPlan;
  /**
   * The plan whose ordinary pipeline runs when the count push-downs decline this statement: this one, or, for the folded plan of
   * {@link #foldedCountPlan()}, the plan it was folded from, which keeps the physical plan the folded one is built without
   * (issue #11680). Set before the folded plan is published.
   */
  private CypherExecutionPlan pipelinePlan = this;

  // UNION support
  private final List<CypherExecutionPlan> unionSubqueryPlans;
  private final boolean                   unionRemoveDuplicates;

  /** Recognises the MATCH shapes a count operator answers without materializing a row per match. */
  private final CountStarShapeDetector countStarShapes;

  /** Chains the execution steps of the statement, clause by clause. */
  final CypherStepBuilder stepBuilder;

  /** Splits a WHERE into what the scan of a node can apply and what is left for a filter step. */
  final FilterPushdown filterPushdown;

  /** Decides which hops of a MATCH clause could match the same relationship. */
  final HopEdgeTrackingAnalyzer hopEdges;

  /** Recognises the aggregating statements a single step answers in place of the MATCH pipeline. */
  private final AggregationShortcutPlanner aggregationShortcuts;

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
    this.hopEdges = new HopEdgeTrackingAnalyzer(database, parameters);
    this.filterPushdown = new FilterPushdown(parameters);
    this.countStarShapes = new CountStarShapeDetector(this, database, statement);
    this.aggregationShortcuts = new AggregationShortcutPlanner(this, database, statement, physicalPlan, expressionEvaluator);
    this.stepBuilder = new CypherStepBuilder(this, database, statement, parameters, physicalPlan, expressionEvaluator,
        hopEdges, filterPushdown, aggregationShortcuts);
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
    return execute(outerContext, false);
  }

  /**
   * Executes the query plan with per-step timing turned on, and <b>without changing what it executes</b>: the same
   * streaming chain {@link #execute(CommandContext)} builds, pulled by the caller at the caller's own pace.
   * <p>
   * This is what the server profiler and Studio's {@code profileExecution: "detailed"} use, and it is the whole
   * point of the overload. They used to be routed onto {@link #profile()}, which drains the plan into heap before
   * returning: the number they then reported was the cost of a materialising execution the statement never performs
   * otherwise, and switching the diagnostic on turned every Cypher read on the server into a full in-heap
   * materialisation for the length of the recording window (issue #7330). The step timers accumulate during
   * {@code syncPull}, so no drain is needed to collect them - {@code profile()}'s drain exists to produce a result
   * <i>shape</i>, not to time anything.
   * <p>
   * The returned set carries an {@link ExecutionPlan} built on demand from the live step chain, so a consumer that
   * asks for it after streaming (which is what {@code ProfilingResultSet} and the HTTP handler both do) sees the
   * costs the run actually accumulated. Asking earlier is legal and reports what has been measured so far.
   *
   * @param outerContext the enclosing statement's context, or {@code null} for a top-level statement
   * @param profiling    whether to time each step and attach the plan; {@code false} is the plain streaming path
   *
   * @return result set
   */
  public ResultSet execute(final CommandContext outerContext, final boolean profiling) {
    if (!profiling) {
      final ResultSet rows = executeStreaming(outerContext, null);
      // Only a top-level statement answers a caller that reads the LIMIT back, and only one that declares a LIMIT pays the
      // wrapper (issue #12154)
      return outerContext == null && declaresStatementLimit() ? new StatementLimitResultSet(rows) : rows;
    }

    final StreamingProfile profile = new StreamingProfile();
    return profile.attach(executeStreaming(outerContext, profile));
  }

  /**
   * Whether the rows this statement answers are bounded by a LIMIT it declares: the LIMIT of its {@code RETURN}, or of every
   * branch of a {@code UNION}. A LIMIT of a {@code WITH} bounds the rows of a later clause, not the answer.
   */
  private boolean declaresStatementLimit() {
    if (unionSubqueryPlans != null && !unionSubqueryPlans.isEmpty()) {
      for (final CypherExecutionPlan branch : unionSubqueryPlans)
        if (!branch.declaresStatementLimit())
          return false;
      return true;
    }
    return statement.getReturnClause() != null && statement.getLimit() != null;
  }

  /**
   * The LIMIT this statement declares on the rows it answers, evaluated against its parameters, or 0 when it declares none
   * or it cannot be evaluated here (the execution reports that error itself). A {@code UNION} answers at most the sum of the
   * LIMITs of its branches, and is unbounded when one branch is.
   * <p>
   * What the HTTP endpoints raise their default row cap to, as they do with the LIMIT of a SQL plan (issue #12154): the
   * value can only raise that cap, so a LIMIT smaller than it changes nothing there.
   */
  int getStatementLimit() {
    return Math.max(0, declaredLimit());
  }

  /** {@link #getStatementLimit()}, telling "no LIMIT" (-1) from a LIMIT 0, which bounds a UNION branch to no rows. */
  private int declaredLimit() {
    if (unionSubqueryPlans != null && !unionSubqueryPlans.isEmpty()) {
      long sum = 0;
      for (final CypherExecutionPlan branch : unionSubqueryPlans) {
        final int branchLimit = branch.declaredLimit();
        if (branchLimit < 0)
          return -1;
        sum += branchLimit;
      }
      return (int) Math.min(sum, Integer.MAX_VALUE);
    }

    if (statement.getReturnClause() == null || statement.getLimit() == null)
      return -1;

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    setupFunctionResolver(context);
    try {
      final CypherFunctionFactory functionFactory = expressionEvaluator != null ? expressionEvaluator.getFunctionFactory() : null;
      return new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getLimit(), new ResultInternal(), context);
    } catch (final RuntimeException e) {
      // The LimitStep evaluates the same expression and fails the statement with the reason: nothing to raise a cap to here
      return -1;
    }
  }

  /**
   * The rows of a top-level statement that declares a LIMIT, answering {@link ResultSet#getStatementLimit()} with it. A
   * Cypher statement builds no {@link ExecutionPlan} for its rows, which is where SQL keeps the value, and a plan made up
   * only to carry it would reach every consumer that reads plans (the server profiler, the HTTP explain) as one.
   * Evaluated on demand: a caller that never asks pays nothing but the delegation.
   */
  private final class StatementLimitResultSet implements ResultSet {
    private final ResultSet delegate;

    private StatementLimitResultSet(final ResultSet delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public Result next() {
      return delegate.next();
    }

    @Override
    public void close() {
      delegate.close();
    }

    @Override
    public void reset() {
      delegate.reset();
    }

    @Override
    public Optional<ExecutionPlan> getExecutionPlan() {
      return delegate.getExecutionPlan();
    }

    @Override
    public Optional<QueryStatistics> getStatistics() {
      return delegate.getStatistics();
    }

    @Override
    public FacetResults getFacets() {
      return delegate.getFacets();
    }

    @Override
    public int getStatementLimit() {
      return CypherExecutionPlan.this.getStatementLimit();
    }
  }

  /**
   * @param profile when not null, per-step timing is on and this collector is handed the root step the run built,
   *                so the plan can be reconstructed from it once the caller has finished streaming
   */
  private ResultSet executeStreaming(final CommandContext outerContext, final StreamingProfile profile) {
    // Handle UNION queries specially
    if (unionSubqueryPlans != null && !unionSubqueryPlans.isEmpty())
      return executeUnion(outerContext, profile);

    // Build execution context
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    context.setProfiling(profile != null);
    setupFunctionResolver(context);
    CypherFunctionHelper.inheritStatementTime(context, outerContext);
    inheritIncomingEdgeLookup(context, outerContext);
    inheritGraphViewPins(context, outerContext);

    AbstractExecutionStep rootStep;

    // FAST PATH: Specialized count-push-down optimizations.
    // Must be checked BEFORE the optimizer dispatch, because the optimizer produces
    // GAVExpandAll operators that still materialize individual rows (O(paths) memory).
    rootStep = tryCountPushDown(context, false);

    if (profile != null)
      profile.countPushedDown = rootStep != null;

    if (rootStep == null) {
      // Phase 4: Use optimized physical plan if available
      // Use pre-computed flags from the cached CypherStatement to avoid scanning clause lists per execution
      if (canUseOptimizedPhysicalPlan()) {
        // Use optimizer - execute physical operators directly
        // Note: For Phase 4, we only optimize MATCH patterns
        // RETURN, ORDER BY, LIMIT are still handled by execution steps
        rootStep = stepBuilder.buildExecutionStepsWithOptimizer(context);
      } else {
        // Fall back to non-optimized execution
        // This path correctly handles clause ordering (UNWIND before MATCH), VLP patterns, etc.
        rootStep = stepBuilder.buildExecutionSteps(context);
      }
    }

    if (profile != null) {
      profile.rootStep = rootStep;
      profile.context = context;
    }

    if (rootStep == null) {
      // No steps to execute - return empty result
      return new IteratorResultSet(new ArrayList<ResultInternal>().iterator());
    }

    // Execute the step chain
    final ResultSet resultSet;
    try {
      resultSet = rootStep.syncPull(context, 100);
    } catch (final RuntimeException | Error e) {
      // A step that writes eagerly can fail here, after a label write already held the slots it freed (issue #12336)
      LabelReplacements.endOfStatement(context);
      throw e;
    }

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
      try {
        while (resultSet.hasNext()) {
          guard.check();
          materializedResults.add((ResultInternal) resultSet.next());
        }
      } finally {
        // Nothing reads the step chain after the drain: its cursors, and the heap its buffers reserved from the budget
        // of all the queries, go back now rather than when the garbage collector finds the chain (issue #8591)
        resultSet.close();
        // ... and no binding of a record a label write replaced is left to resolve: its slot may be reused (issue #12336)
        LabelReplacements.endOfStatement(context);
      }
      // Surface the CRUD-count accumulator built up by the mutation steps (CreateStep, SetStep,
      // DeleteStep, RemoveStep, MergeStep) on the returned result set. Always present after a
      // write statement, even if it performed no actual mutation (containsUpdates() is false then).
      final QueryStatistics stats = context.getStatistics();

      // If no RETURN clause (or GQL FINISH was used), return empty results
      // (write side effects still happened). Issue #3365 section 1.3.
      if (writeAnswersNoRows()) {
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
      try {
        while (resultSet.hasNext()) {
          guard.check();
          resultSet.next();
        }
      } finally {
        resultSet.close();
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
   * plan's rows too, not abort it with the exception the clause promised not to raise (issue #6304). So does the
   * statement's termination switch, for the same reason: a nested plan is part of the statement (issue #9680).
   */
  private static void inheritCommandDeadline(final BasicCommandContext context, final CommandContext outerContext) {
    if (outerContext != null) {
      context.setCommandDeadline(outerContext.getCommandDeadline(), outerContext.getCommandDeadlineDescription(),
          outerContext.isCommandDeadlinePartial());
      final RunningQuery runningQuery = outerContext.getRunningQuery();
      if (runningQuery != null)
        context.setRunningQuery(runningQuery);
    }
  }

  /**
   * Makes a nested plan read the enclosing query's scans of the unidirectional edge types instead of taking its own:
   * a correlated {@code COUNT { }} or {@code CALL { }} body runs once per outer row, each time on a context of its own,
   * and would otherwise scan the type once per row (issue #8625).
   */
  private static void inheritIncomingEdgeLookup(final BasicCommandContext context, final CommandContext outerContext) {
    if (outerContext != null)
      context.setIncomingEdgeLookup(outerContext.getIncomingEdgeLookup());
  }

  /**
   * Makes a nested plan read the graph views through the pins of the statement it is part of, and a top-level plan open that
   * statement's scope (issue #10415): every operator of the statement, the nested plans' included, then reads one snapshot of
   * each view however many times it is rebuilt meanwhile.
   */
  private static void inheritGraphViewPins(final BasicCommandContext context, final CommandContext outerContext) {
    final GraphViewPins pins = outerContext != null ? outerContext.getGraphViewPins() : null;
    context.setGraphViewPins(pins != null ? pins : new GraphViewPins());
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
    return executeWithSeedRow(seedRow, outerContext, SEEDED_BATCH_SIZE);
  }

  /**
   * {@link #executeWithSeedRow(Result, CommandContext)}, reading {@code batchSize} rows at a time: a {@code CALL { }} whose
   * rows stream asks for no more than its own consumer did, so the {@code LIMIT 3} after it reads three rows of the body
   * and not a batch of a hundred (issue #11082).
   */
  public ResultSet executeWithSeedRow(final Result seedRow, final CommandContext outerContext, final int batchSize) {
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
      // The deadline is inherited because the UnionStep's WorkGuard reads it; nothing else is, because nothing else
      // is read. No expression is ever evaluated against this ctx - each branch re-enters this method with the
      // REAL outerContext - so inheriting the statement clock here would be dead weight.
      inheritCommandDeadline(ctx, outerContext);

      // Each branch runs seeded with the row, one after the other and only as far as the consumer pulls: collecting every
      // branch first ran the whole body for a LIMIT the first branch could satisfy (issue #11082). The UNION de-duplicates
      // on the canonical keys of the statement-level UNION, under the same heap limit.
      // No inherit() here on purpose: the branch runs through this same method with the REAL outerContext, so it takes
      // the non-union path below and does its own inherit under the same gate. A second copy here would be one more
      // thing to keep in step with that gate (issue #6977).
      final UnionStep unionStep = new UnionStep(branchPlans, removeDuplicates,
          branchPlan -> branchPlan.executeWithSeedRow(seedRow, outerContext, batchSize), ctx);
      return unionStep.syncPull(ctx, batchSize);
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
    inheritIncomingEdgeLookup(context, outerContext);
    inheritGraphViewPins(context, outerContext);

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
      public String getName() {
        return "SubquerySeedRowStep";
      }

      @Override
      public String getType() {
        return getName();
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
      rootStep = stepBuilder.buildExecutionStepsWithOrder(context, clausesInOrder, seedStep, seedCorrelationOf(seedRow),
          seedRow.getPropertyNames());
    else
      rootStep = seedStep; // Fallback: just return the seed

    if (rootStep == null)
      return new IteratorResultSet(new ArrayList<ResultInternal>().iterator());

    return rootStep.syncPull(context, batchSize);
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
    inheritIncomingEdgeLookup(context, outerContext);
    inheritGraphViewPins(context, outerContext);

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
   * The question is asked because of the two count push-downs in {@link CypherStepBuilder#buildExecutionStepsWithOrder}: they answer
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
  static final class SeedCorrelation {
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

  /**
   * Executes a UNION query by combining results from all subqueries.
   *
   * @param outerContext the enclosing statement's context, or {@code null} for a top-level statement
   * @param profile      when not null, per-step timing is on and this collector is handed the UnionStep as the
   *                     plan's root, so the profiled plan describes the same streaming run (issue #7330)
   *
   * @return combined result set
   */
  private ResultSet executeUnion(final CommandContext outerContext, final StreamingProfile profile) {
    // Use UnionStep to combine results from all subqueries
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    context.setProfiling(profile != null);
    setupFunctionResolver(context);
    // Every branch runs on a context of its own, so the statement clock has to travel from here into each of
    // them - and into here from an enclosing statement when this UNION is a CALL body (issue #7052).
    CypherFunctionHelper.inheritStatementTime(context, outerContext);
    inheritIncomingEdgeLookup(context, outerContext);
    inheritGraphViewPins(context, outerContext);

    final UnionStep unionStep =
        new UnionStep(unionSubqueryPlans, unionRemoveDuplicates, context);

    if (profile != null) {
      profile.rootStep = unionStep;
      profile.context = context;
    }

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
    // A read-only statement cannot change what an unindexed chained MATCH re-reads, so it may answer from a snapshot (issue #8695)
    context.setVariable(MatchNodeStep.READ_ONLY_STATEMENT_KEY, statement.isReadOnly());
    if (expressionEvaluator != null)
      installFunctionResolver(context, expressionEvaluator.getFunctionFactory());
  }

  /**
   * Makes the functions of {@code factory} resolvable by a {@link FunctionCallExpression} evaluated against {@code context}.
   * Every context an expression is evaluated against needs it, not only an execution plan's: a logical expression calls
   * {@link FunctionCallExpression#evaluate} directly (issue #9940, the condition of a conditional query).
   */
  public static void installFunctionResolver(final BasicCommandContext context, final CypherFunctionFactory factory) {
    context.setVariable(FunctionCallExpression.FUNCTION_RESOLVER_KEY,
        (Function<String, StatelessFunction>) name -> {
          try {
            return factory.getFunctionExecutor(name);
          } catch (final Exception e) {
            return null;
          }
        });
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
      // Not a second optimization: buildExecutionStepsWithOptimizer wraps THIS plan's physicalPlan - the same
      // instance the text below is printed from - so the structured steps and the text cannot describe two
      // different plans. Built BEFORE the operators are printed: building the steps can fold an aggregation into
      // the operators (a fused chain counting its paths, issue #9738), which the operators' text must show.
      Exception stepsFailure = null;
      if (countPushDown == null)
        try {
          describedSteps = stepChainOf(stepsForDescription());
        } catch (final Exception e) {
          stepsFailure = e;
        }

      explainOutput.append(countPushDown != null
          ? "Using Cost-Based Query Optimizer (superseded by the count push-down above)\n\n"
          : "Using Cost-Based Query Optimizer\n\n");
      explainOutput.append("Physical Plan:\n");
      explainOutput.append(physicalPlan.getRootOperator().explain(0));
      explainOutput.append("\n");
      explainOutput.append(String.format(Locale.ROOT, "Total Estimated Cost: %.2f\n", physicalPlan.getTotalEstimatedCost()));
      explainOutput.append(String.format(Locale.ROOT, "Total Estimated Rows: %d\n", physicalPlan.getTotalEstimatedCardinality()));
      if (countPushDown == null) {
        if (stepsFailure == null)
          appendStepsAfterTheOptimizedMatch(explainOutput, describedSteps);
        else {
          explainOutput.append("\n");
          appendPlanBuildFailure(explainOutput, stepsFailure);
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
    return canUseOptimizedPhysicalPlan() ? stepBuilder.buildExecutionStepsWithOptimizer(context) : stepBuilder.buildExecutionSteps(context);
  }

  /**
   * Appends the steps that run after the optimized MATCH, which the physical plan does not describe.
   * <p>
   * The optimizer claims the MATCH pattern only: RETURN, ORDER BY, LIMIT and every write clause stay execution
   * steps appended to the operator chain. Printing the physical plan alone therefore answered
   * {@code EXPLAIN MATCH (n) WHERE ... DELETE n} with a scan and no mention of the delete (issue #6323). The first
   * element of the chain is normally the operator wrapper the physical plan above already describes, so it is
   * skipped. A rewrite that reads the MATCH without the operators ({@link CountEdgesReturnStep},
   * {@link GAVOneHopScanStep}) leaves no wrapper in the chain, and then the whole chain is what runs (issue #8335).
   */
  private static void appendStepsAfterTheOptimizedMatch(final StringBuilder output, final List<ExecutionStep> chain) {
    final boolean startsWithOperators = !chain.isEmpty() && OPTIMIZED_MATCH_STEP_NAME.equals(chain.getFirst().getName());
    if (!startsWithOperators) {
      if (chain.isEmpty())
        return;
      output.append("\nSteps Run Instead of the Physical Plan Above:\n");
      for (final ExecutionStep step : chain) {
        output.append(((AbstractExecutionStep) step).prettyPrint(0, 2));
        output.append("\n");
      }
      return;
    }
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
   * Whether a WRITE statement answers no rows: it has no RETURN clause, so it is run for its side effects only, or it ends
   * with GQL FINISH. Shared by the streaming {@link #execute} and the eager {@link #profile()}, so PROFILE of a write answers
   * the rows the write answers - none for a RETURN-less one, where it used to answer whatever the last write clause passed
   * on (issue #11386).
   */
  private boolean writeAnswersNoRows() {
    if (statement.hasFinishClause())
      return true;
    if (statement.getReturnClause() != null)
      return false;
    // A bare CALL (no YIELD/RETURN, e.g. a write-classified custom function or write procedure) is its
    // own projection: CallStep already yields its implicit "all columns" row, so a missing RETURN clause
    // here does not mean "side effects only" the way it does for CREATE/SET/DELETE/MERGE/REMOVE. Issue #6446.
    // Deliberately scoped to a CALL that is the statement's ONLY clause: a CALL chained after other
    // clauses (e.g. WITH ... CALL proc() YIELD x, or multiple chained CALLs), still with no trailing
    // RETURN, is a separate, narrower gap left for #6450 rather than widened here.
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    return clauses.size() != 1 || clauses.get(0).getType() != ClauseEntry.ClauseType.CALL;
  }

  /**
   * Executes the query with profiling enabled and <b>eagerly</b>: every row is pulled into heap before this method
   * returns, and the rows are returned alongside the profile via {@code getExecutionPlan()}. The javadoc here used
   * to claim the results were discarded and that the return was an {@code ExplainResultSet}; neither has been true
   * of this implementation, and the drain is precisely what a caller has to know about (issue #7330).
   * <p>
   * This is the {@code PROFILE <statement>} path - a user asking, in the statement itself, for the whole execution
   * to be run and summarised, which is what Neo4j's PROFILE does too. It is deliberately NOT the path taken by the
   * server profiler or by Studio's {@code profileExecution: "detailed"}: those must not change how the statement
   * executes, and use {@link #execute(CommandContext, boolean)} instead, which times the ordinary streaming run.
   *
   * @return result set carrying the rows and, via {@code getExecutionPlan()}, the profiling metrics
   */
  public ResultSet profile() {
    final long startTime = System.nanoTime();

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    setupFunctionResolver(context);
    context.setProfiling(true);
    context.openGraphViewPins();

    final InternalResultSet results = new InternalResultSet();
    AbstractExecutionStep rootStep = null;
    boolean countPushedDown = false;
    // The rows the statement answers, as execute() answers them: the run is still drained for its side effects, but a
    // RETURN-less write (or a FINISH) keeps none. It kept the rows its last write clause passed on, which a client iterating
    // the profiled write read as phantom records (issue #11386)
    final boolean keepRows = statement.isReadOnly() ? !statement.hasFinishClause() : !writeAnswersNoRows();

    // A failure ends the statement here, as it ends the statement PROFILE wraps: it reaches the caller, so the transaction
    // around the statement rolls back what was written before it. It used to be caught into the profile text, which
    // answered a failed write with no rows and no error and let its writes commit (issue #10252); Neo4j's PROFILE fails too
    if (unionSubqueryPlans != null && !unionSubqueryPlans.isEmpty()) {
      final UnionStep unionStep =
          new UnionStep(unionSubqueryPlans, unionRemoveDuplicates, context);
      try (final ResultSet resultSet = unionStep.syncPull(context, Integer.MAX_VALUE)) {
        while (resultSet.hasNext()) {
          final Result row = resultSet.next();
          if (keepRows)
            results.add(row);
        }
      }
    } else {
      // FAST PATH: Count-push-down (same logic as execute())
      rootStep = tryCountPushDown(context, false);
      countPushedDown = rootStep != null;

      if (rootStep == null) {
        if (canUseOptimizedPhysicalPlan()) {
          rootStep = stepBuilder.buildExecutionStepsWithOptimizer(context);
        } else
          rootStep = stepBuilder.buildExecutionSteps(context);
      }

      if (rootStep != null) {
        try (final ResultSet resultSet = rootStep.syncPull(context, Integer.MAX_VALUE)) {
          while (resultSet.hasNext()) {
            final Result row = resultSet.next();
            if (keepRows)
              results.add(row);
          }
        } finally {
          // The run is drained: no binding of a record a label write replaced is left to resolve (issue #12336)
          LabelReplacements.endOfStatement(context);
        }
      }
    }

    final long endTime = System.nanoTime();
    final long rowCount = results.countEntries();

    results.setPlan(buildProfilePlan(context, rootStep, countPushedDown, endTime - startTime, rowCount));
    // Surface the CRUD-count accumulator built up by the mutation steps during the profiled run,
    // mirroring execute()'s write path so a profiled write still reports its counters. Read-only
    // statements never attach a statistics accumulator, matching execute()'s read path.
    if (!statement.isReadOnly())
      results.setStatistics(context.getStatistics());
    return results;
  }

  /**
   * The profile of a run that has just finished: the human-readable report plus the live step chain that carries
   * the per-step costs. Shared by the eager {@link #profile()} and the streaming
   * {@link #execute(CommandContext, boolean)} so the two describe the same run the same way, and one of them cannot
   * quietly grow a section the other lacks.
   *
   * @param elapsedNanos wall clock of the run being described
   * @param rowCount     rows the run produced - for the streaming path, rows the caller actually pulled
   */
  private OpenCypherExplainExecutionPlan buildProfilePlan(final CommandContext context,
      final AbstractExecutionStep rootStep, final boolean countPushedDown, final long elapsedNanos, final long rowCount) {
    final StringBuilder profileOutput = new StringBuilder();
    profileOutput.append("OpenCypher Query Profile\n");
    profileOutput.append("========================\n\n");
    if (Boolean.TRUE.equals(context.getVariable(CommandContext.CSR_ACCELERATED_VAR)))
      profileOutput.append("CSR-accelerated via Graph Analytical View\n");
    profileOutput.append(String.format(Locale.ROOT, "Execution Time: %.3f ms\n", elapsedNanos / 1_000_000.0));
    profileOutput.append(String.format(Locale.ROOT, "Rows Returned: %d\n", rowCount));

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
      profileOutput.append(String.format(Locale.ROOT, "\nEstimated Cost: %.2f\n", physicalPlan.getTotalEstimatedCost()));
      profileOutput.append(String.format(Locale.ROOT, "Estimated Rows: %d\n", physicalPlan.getTotalEstimatedCardinality()));
      if (!countPushedDown)
        appendStepsAfterTheOptimizedMatch(profileOutput, executionSteps);
    } else if (!countPushedDown) {
      profileOutput.append("\nExecution Plan (Traditional):\n");
      if (rootStep != null)
        appendStepChain(profileOutput, rootStep);
      else
        profileOutput.append("No execution steps generated\n");
    }

    // The plan of the profiled rows carries the statement's LIMIT, where a SQL plan carries it (issue #12154)
    return new OpenCypherExplainExecutionPlan(profileOutput.toString(), executionSteps, elapsedNanos, getStatementLimit());
  }

  /**
   * Times a streaming execution without altering it: the caller pulls the rows it wants, at its own pace, and the
   * plan is reconstructed from the live step chain whenever it is asked for.
   * <p>
   * The alternative - the one this replaces - was to reroute the statement onto {@link #profile()}, whose drain
   * changes the execution mode: what got reported was then the cost of materialising every row, which is not what
   * the statement costs when nobody is watching, and every Cypher read on the server was materialised in heap for
   * as long as the recording window lasted (issue #7330).
   * <p>
   * The clock stops at the first of the delegate reporting exhaustion and {@code close()} - never at the plan
   * being asked for, so inspecting the plan mid-stream does not freeze the number a later ask reports. Consumers
   * ask for it after streaming ({@code ProfilingResultSet} at close, {@code PostCommandHandler} after draining),
   * so the elapsed they see covers the whole run.
   */
  private final class StreamingProfile implements ResultSet {
    private final long                  startNanos = System.nanoTime();
    private       ResultSet             delegate;
    private       CommandContext        context;
    private       AbstractExecutionStep rootStep;
    private       boolean               countPushedDown;
    private       long                  rowCount;
    private       long                  elapsedNanos = -1;

    private ResultSet attach(final ResultSet delegate) {
      this.delegate = delegate;
      return this;
    }

    @Override
    public boolean hasNext() {
      final boolean hasNext = delegate.hasNext();
      if (!hasNext)
        stopClock();
      return hasNext;
    }

    @Override
    public Result next() {
      final Result row = delegate.next();
      ++rowCount;
      return row;
    }

    @Override
    public void close() {
      stopClock();
      delegate.close();
    }

    @Override
    public void reset() {
      delegate.reset();
    }

    @Override
    public Optional<QueryStatistics> getStatistics() {
      return delegate.getStatistics();
    }

    @Override
    public Optional<ExecutionPlan> getExecutionPlan() {
      // Built here rather than at construction: the step timers accumulate while the caller streams, so a plan
      // snapshotted before the first row would report a chain that had not run yet.
      //
      // Reading the clock rather than stopping it: a caller that inspects progress mid-stream and asks again at
      // the end must get the elapsed of the whole run the second time, not the reading frozen by the first ask.
      return Optional.of(buildProfilePlan(context, rootStep, countPushedDown, elapsedSoFar(), rowCount));
    }

    /** Without building the profile the plan would carry it in (issue #12154). */
    @Override
    public int getStatementLimit() {
      return CypherExecutionPlan.this.getStatementLimit();
    }

    /** The run's elapsed: final once the run has ended, and how long it has been going otherwise. */
    private long elapsedSoFar() {
      return elapsedNanos < 0 ? System.nanoTime() - startNanos : elapsedNanos;
    }

    /** Ends the run's clock, at the first of the delegate reporting exhaustion and {@code close()}. */
    private void stopClock() {
      if (elapsedNanos < 0)
        elapsedNanos = System.nanoTime() - startNanos;
    }
  }

  /**
   * Whether the step is the one that runs the optimizer's physical plan, which is what the shortcuts reading that plan's
   * operators require of the step they replace.
   */
  static boolean isOptimizedMatchStep(final AbstractExecutionStep step) {
    return step instanceof OptimizedMatchStep;
  }

  /** The step that runs the optimizer's physical plan, first in the chain {@link CypherStepBuilder} builds on it. */
  AbstractExecutionStep newOptimizedMatchStep(final CommandContext context) {
    return new OptimizedMatchStep(context);
  }

  /**
   * The step that executes the physical operators of the optimized MATCH. A label scan at the root of the operators can
   * also hand its rows to an aggregation that consumes them in the workers of a parallel scan (issue #8797).
   */
  private final class OptimizedMatchStep extends AbstractExecutionStep implements ParallelRowSource {
    OptimizedMatchStep(final CommandContext context) {
      super(context);
    }

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
    public String getName() {
      return OPTIMIZED_MATCH_STEP_NAME;
    }

    @Override
    public String getType() {
      return getName();
    }

    @Override
    public String prettyPrint(final int depth, final int indent) {
      return "  ".repeat(Math.max(0, depth * indent)) + "+ OPTIMIZED MATCH (physical operators)\n" +
          physicalPlan.explain();
    }

    @Override
    public String parallelVariable() {
      return physicalPlan.getRootOperator() instanceof NodeByLabelScan scan ? scan.getVariable() : null;
    }

    @Override
    public ParallelRecordScan planParallelRows(final CommandContext ctx, final List<BooleanExpression> filters) {
      // Only before the operators were pulled: a started scan cannot be taken over
      if (operatorResults != null || closed || !(physicalPlan.getRootOperator() instanceof NodeByLabelScan scan))
        return null;
      return scan.planParallelRows(ctx, filters);
    }
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
  boolean isAnyEdgeTypeUnidirectional(final List<String> edgeTypeNames) {
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
  static Set<String> groupingKeyVariableNames(final List<ReturnClause.ReturnItem> groupingKeys) {
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
  static boolean isRowPerGroupUpToClause(final List<ClauseEntry> clausesInOrder, final int currentIndex,
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
  AbstractExecutionStep addWithProjection(final WithClause withClause,
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

  boolean isFollowedByCountOnlyReturn(final List<ClauseEntry> clausesInOrder, final int callIndex) {
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
    // Check that ALL return items are count(*): the fast path replaces the procedure rows with empty ones, so any
    // aggregate that reads a yielded column (count(DISTINCT x), max(x), sum(x)...) would see nothing (issue #9341)
    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
      if (!(item.getExpression() instanceof FunctionCallExpression func))
        return false;
      if (!"count".equalsIgnoreCase(func.getFunctionName()) || func.isDistinct() || func.getArguments().size() != 1
          || !(func.getArguments().get(0) instanceof StarExpression))
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
    if (func.getArguments().size() != 1)
      return null;
    final Expression argument = func.getArguments().get(0);
    if (argument instanceof StarExpression)
      return item.getAlias() != null ? item.getAlias() : "count(*)";
    // count(e) skips only the rows where e is null, and a variable a non-optional MATCH binds is never null there: it counts
    // the rows count(*) does (issue #9600). DISTINCT is a different question.
    if (argument instanceof VariableExpression variable && !func.isDistinct() && isBoundByEveryRow(variable.getVariableName()))
      return item.getOutputName();
    return null;
  }

  /**
   * Whether a node or relationship position of a non-optional MATCH binds the name, so no matched row carries it as null.
   * Sound only for what {@link #isMatchReturnOnlyStatement()} admits: with no WITH, UNWIND or CALL nothing can bind the name
   * again before the RETURN. A quantified group's own variables are not positions of the outer path and are not found here.
   */
  private boolean isBoundByEveryRow(final String name) {
    if (statement.getMatchClauses() == null)
      return false;
    for (final MatchClause match : statement.getMatchClauses()) {
      if (match.isOptional() || !match.hasPathPatterns())
        continue;
      for (final PathPattern path : match.getPathPatterns()) {
        for (int i = 0; i <= path.getRelationshipCount(); i++)
          if (name.equals(path.getNode(i).getVariable()))
            return true;
        for (int i = 0; i < path.getRelationshipCount(); i++)
          if (name.equals(path.getRelationship(i).getVariable()))
            return true;
      }
    }
    return false;
  }

  /**
   * Whether a relationship's variable is named nowhere else in the MATCH clauses, the case where naming it constrains
   * nothing. A count push-down reads the statement only once {@link #isMatchReturnOnlyStatement()} and a count-only RETURN
   * have settled that nothing else reads it, so a variable written once is as good as anonymous; one written twice is the
   * same relationship at two positions, a join the operators do not express.
   * <p>
   * It reads the statement's MATCH clauses, not the clause a detector was handed: a clause joined from the statement's own
   * parts holds the same relationships under the same names, and a name written in another clause is a join all the same.
   */
  boolean isNamedOnce(final String name) {
    int occurrences = 0;
    for (final MatchClause match : statement.getMatchClauses()) {
      if (!match.hasPathPatterns())
        continue;
      for (final PathPattern path : match.getPathPatterns()) {
        if (name.equals(path.getPathVariable()))
          ++occurrences;
        for (int i = 0; i <= path.getRelationshipCount(); i++)
          if (name.equals(path.getNode(i).getVariable()))
            ++occurrences;
        for (int i = 0; i < path.getRelationshipCount(); i++)
          if (name.equals(path.getRelationship(i).getVariable()))
            ++occurrences;
      }
    }
    return occurrences == 1;
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
  boolean isMatchReturnOnlyStatement() {
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
   * {@link CypherStepBuilder#buildExecutionStepsWithOrder}, which a top-level query reaches only when the optimizer declines it. A
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
  AbstractExecutionStep tryCountPushDown(final CommandContext context, final boolean countRowsMode) {
    return tryCountPushDown(context, countRowsMode, SeedCorrelation.UNCORRELATED);
  }

  /**
   * @param correlation which of the names the seed row carries this body reads, and the row that bound them. Only the
   *                    chain operator can seed itself from a bound anchor, so a correlated body reaches that one
   *                    alone; an uncorrelated one reaches every detector, as before.
   */
  AbstractExecutionStep tryCountPushDown(final CommandContext context, final boolean countRowsMode,
      final SeedCorrelation correlation) {
    if (!correlation.isSeedable())
      return null;

    if (!correlation.isCorrelated()) {
      // A WITH that only passes variables on changes no row count, so the push-downs read the statement with it
      // folded away (issue #9597). A correlated body is left as it is: what the seed row binds is named by the body's
      // own text, and a WITH that drops a seeded name makes the same name a new variable after it.
      final CypherExecutionPlan folded = foldedCountPlan();
      if (folded != null)
        return folded.tryCountPushDown(context, countRowsMode, correlation);
    }

    // The O(1) type counter answers "how many vertices carry this label", which is not a question a bound anchor
    // narrows: a seeded MATCH (q:Q) is one vertex tested against a label, not a count over the label.
    AbstractExecutionStep step = correlation.isCorrelated() ? null : aggregationShortcuts.tryCreateTypeCountOptimization(context, countRowsMode);
    if (step == null && !countRowsMode && !correlation.isCorrelated())
      step = aggregationShortcuts.tryCreateIndexMinMaxOptimization(context);
    // Asked before the pattern detectors, which are written for one connected pattern
    if (step == null && !correlation.isCorrelated())
      step = tryCartesianProductCount(context, countRowsMode);
    if (step == null)
      step = tryOptimizeCountStar(context, countRowsMode, correlation);
    if (step == null)
      return null;
    return applySkipAndLimit(step, context);
  }

  /**
   * The plan of this statement with its pass-through {@code WITH} clauses folded away, which is what the count
   * push-downs read (issue #9597); null when there is no {@code WITH} to fold or one that is not a pass-through. See
   * {@link PassThroughWithFolder}.
   */
  private CypherExecutionPlan foldedCountPlan() {
    // Computed without a lock: two threads racing only fold the same statement twice. The folded plan answers null
    // here itself, because a statement with no WITH left folds to the very same statement, which ends the recursion
    Optional<CypherExecutionPlan> folded = foldedCountPlan;
    if (folded == null) {
      final CypherStatement foldedStatement = countPushDownForm();
      if (foldedStatement == statement)
        folded = Optional.empty();
      else {
        final CypherExecutionPlan foldedPlan = new CypherExecutionPlan(database, foldedStatement, parameters, configuration, null,
            expressionEvaluator);
        foldedPlan.pipelinePlan = this;
        folded = Optional.of(foldedPlan);
      }
      foldedCountPlan = folded;
    }
    return folded.orElse(null);
  }

  /**
   * The statement the count push-downs read: the folded one, or this plan's own when there is nothing to fold or the fold
   * is refused. A plan is built for every execution while the statement is cached, so the answer is kept on the statement
   * (issue #9652): a statement no push-down takes used to fold itself again on every execution only to learn that.
   */
  private CypherStatement countPushDownForm() {
    if (!(statement instanceof SimpleCypherStatement simple))
      return statement;
    CypherStatement form = simple.getCountPushDownForm();
    if (form == null) {
      final CypherStatement folded = PassThroughWithFolder.fold(statement);
      form = folded != null ? folded : statement;
      simple.setCountPushDownForm(form);
    }
    return form;
  }

  /**
   * The count of a MATCH made of parts that share no variable, as the product of the parts' counts (issue #9596), or
   * null when the statement is one part or its parts do not multiply (see {@link DisconnectedCountParts}).
   * <p>
   * Each part is counted by a plan of its own, with whatever that plan takes: a count push-down, or the row pipeline
   * over the part alone. Either way the cost is the sum of the parts' costs instead of their product, which is the
   * difference between three O(1) counts and building 4.9 billion rows.
   */
  private AbstractExecutionStep tryCartesianProductCount(final CommandContext context, final boolean countRowsMode) {
    final String alias = countPushDownAlias(countRowsMode);
    if (alias == null || !isMatchReturnOnlyStatement())
      return null;
    final List<CypherStatement> parts = DisconnectedCountParts.split(statement, hopEdges::hopsCanMatchTheSameEdge);
    if (parts == null)
      return null;

    final List<ProductCountStep.Part> counted = new ArrayList<>(parts.size());
    for (final CypherStatement part : parts) {
      final CypherExecutionPlan partPlan = new CypherExecutionPlan(database, part, parameters, configuration, null,
          expressionEvaluator);
      counted.add(new ProductCountStep.Part() {
        @Override
        public long count(final CommandContext ctx) {
          return partPlan.countRows(new ResultInternal(), ctx);
        }

        @Override
        public String describe(final int depth, final int indent) {
          return partPlan.describeRowCount(depth, indent);
        }
      });
    }
    return new ProductCountStep(counted, alias, context);
  }

  /** How {@link #countRows} would count this statement's rows, for EXPLAIN: the push-down it takes, or the pipeline. */
  private String describeRowCount(final int depth, final int indent) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    setupFunctionResolver(context);
    final AbstractExecutionStep step = tryCountPushDown(context, true);
    if (step != null)
      return step.prettyPrint(depth, indent);
    return "  ".repeat(Math.max(0, depth * indent)) + "+ ROW PIPELINE (the rows of this part are produced and counted)";
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
          stepBuilder.newLimitStep(evaluator.evaluateSkipLimit(statement.getLimit(), new ResultInternal(), context), context);
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
  String rowCountAlias() {
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
   * Whether a relationship of the MATCH clauses may walk an edge type declared unidirectional: one of its types, one of
   * their subtypes, or any type at all when it names none.
   */
  private boolean hasUnidirectionalRelationship(final Database db) {
    for (final MatchClause match : statement.getMatchClauses())
      if (match.hasPathPatterns())
        for (final PathPattern path : match.getPathPatterns())
          for (int i = 0; i < path.getRelationshipCount(); i++) {
            final RelationshipPattern rel = path.getRelationship(i);
            if (IncomingEdgeLookup.isAnyUnidirectional(db.getSchema(),
                rel.hasTypes() ? rel.getTypes().toArray(new String[0]) : null))
              return true;
          }
    return false;
  }

  /** Whether a path pattern of the MATCH clauses holds a relationship. */
  private boolean hasRelationship() {
    for (final MatchClause match : statement.getMatchClauses())
      if (match.hasPathPatterns())
        for (final PathPattern path : match.getPathPatterns())
          if (path.getRelationshipCount() > 0)
            return true;
    return false;
  }

  /** Whether every relationship of the MATCH clauses is written as an outgoing hop. */
  private boolean allRelationshipsOutgoing() {
    for (final MatchClause match : statement.getMatchClauses())
      if (match.hasPathPatterns())
        for (final PathPattern path : match.getPathPatterns())
          for (int i = 0; i < path.getRelationshipCount(); i++)
            if (path.getRelationship(i).getDirection() != Direction.OUT)
              return false;
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

    // Every operator below counts walks over edges: the edge count takes one hop, the chain at least one, the anti-join
    // chain at least two, the star at least one arm, the triangle a three-hop cycle, the pair join a one-hop and a
    // two-hop pattern, the joined chain the chain or anti-join over joined parts. A MATCH with no relationship is left to
    // what was asked before this - the type count, the index min/max, the product of parts - or to the pipeline. A
    // detector added below that can take one has to be asked before this check. Answered without reading the WHERE: a
    // count of single nodes filtered on an indexed property is among the commonest statements there are, no detector
    // below takes it, and each of them used to parse its WHERE on every execution to find that out (issue #9652)
    if (!hasRelationship())
      return null;

    // A dynamic label is not a name any count operator can filter on, so the query falls back to the normal
    // materialization pipeline, which applies it (issue #5071). An inline property filter (e.g. (a:Node {id: 1})) is a
    // per-vertex predicate that the chain and star operators (issue #9595), the anti-join and the pair join (issue #9608)
    // apply on top of the label; the triangle operator reasons only about labels and edge types, so it is not asked
    // about a pattern carrying one.
    if (hasDynamicLabel())
      return null;
    final boolean inlineProperties = hasInlineNodeProperty();

    // The operators read adjacency lists directly, in whichever direction their anchors call for: over an edge type
    // declared unidirectional that can be the incoming side, which no vertex stores. The chain operator walks an
    // uncorrelated chain from its first node in the written directions, so a chain of outgoing hops only reads what is
    // stored and keeps the push-down; anything else is left to the ordinary pipeline, whose expansions answer the
    // incoming side through the query's lookup (issue #8625)
    final boolean unidirectional = hasUnidirectionalRelationship(context.getDatabase());
    if (unidirectional && (correlation.isCorrelated() || !allRelationshipsOutgoing()))
      return null;

    // The edge count reads totals rather than walking from an anchor, so it has nothing to seed from a bound name
    CountOp op = correlation.isCorrelated() ? null : countStarShapes.tryDetectEdgeCountStar(context.getDatabase());
    if (op == null)
      op = countStarShapes.tryDetectChainCountStar(context.getDatabase(), correlation, context);
    // Only the chain operator can start its walk from an anchor the outer row bound. The star, triangle, pair-join
    // and anti-join ones all derive their anchor set from a label, so a correlated body is left to the ordinary
    // pipeline rather than answered over every vertex carrying that label (issue #5758).
    if (op == null && !correlation.isCorrelated() && !unidirectional) {
      op = countStarShapes.tryDetectAntiJoinChainCountStar(context);
      if (op == null)
        op = countStarShapes.tryDetectStarCountStar(context);
      if (op == null && !inlineProperties)
        op = countStarShapes.tryDetectTriangleCountStar();
      if (op == null && !inlineProperties)
        op = countStarShapes.tryDetectSingleTriangleCountStar();
      if (op == null)
        op = countStarShapes.tryDetectPairJoinCountStar(context);
    }
    if (op == null)
      op = countStarShapes.tryDetectJoinedChainCountStar(context, correlation, unidirectional);
    if (op == null)
      return null;

    // An operator with no set of anchors to walk from answers 0 for a pattern that does match (issue #5715).
    if (!op.canEnumerateAnchors())
      return null;

    // Only asked once a detector has claimed the statement, so this replaces a push-down that would have run
    // anyway rather than changing the plan of an unrelated query.
    if (mandatoryPatternElementIsEmpty(context.getDatabase()))
      return new ConstantCountStep(0L, alias, context);

    // Every operator above walks a label: the chain visits every vertex of its first node's label, the star every vertex of its
    // central node's label, and an anchor's inline map or WHERE equality only filters what they visit. A statement whose
    // ordinary plan starts from a selective index seek costs that seek and the expansion from it, the same at any graph size,
    // which is what the same statement with sum(1) gets; pushed down it cost the whole label (issue #11680). A correlated body
    // is seeded from the anchor its outer row bound, and a chain from the vertex a UNIQUE key pins (issue #11594), so neither
    // has a label to walk: that walk costs the seek and the paths of one vertex, without a row per path.
    if (!correlation.isCorrelated() && !(op instanceof PropagateChainOp chain && chain.isSeeded())
        && pipelinePlan.ordinaryPlanSeeksASelectiveAnchor())
      return null;

    return new CSRCountStep(op, alias, context);
  }

  /**
   * Whether the ordinary plan of this statement starts from an index seek, or a lookup by id, estimated to find a few vertices of
   * a label (issue #11680). That plan costs the seek and the expansion from what it finds, the same at any graph size, while a
   * count push-down walks a whole label to reach the same answer.
   * <p>
   * The anchor is the one the ordinary plan actually starts from, so the answer is never "seekable" for a plan that would scan:
   * the optimizer's own anchor when its physical plan runs, which an index-backed node always wins (issue #5306), and otherwise
   * the first node of the first MATCH clause, which is where the pipeline of {@link CypherStepBuilder#buildExecutionSteps} and
   * {@link CypherStepBuilder#buildExecutionStepsWithOrder} starts. Both are judged by {@link AnchorSelector}, the estimate the optimizer plans
   * with, rather than by a second index matcher.
   */
  private boolean ordinaryPlanSeeksASelectiveAnchor() {
    final StatisticsProvider statistics = new StatisticsProvider(database);
    if (canUseOptimizedPhysicalPlan())
      return isSelectiveSeek(physicalPlan.getAnchor(), statistics);

    final List<MatchClause> matchClauses = statement.getMatchClauses();
    if (matchClauses == null || matchClauses.isEmpty())
      return false;
    final MatchClause first = matchClauses.getFirst();
    if (first.isOptional() || !first.hasPathPatterns())
      return false;
    final PathPattern path = first.getPathPatterns().getFirst();
    if (path instanceof ShortestPathPattern)
      return false;
    final NodePattern node = path.getFirstNode();
    // the pipeline reads the clause's own WHERE, or the statement's when the clause has none, exactly as here
    final WhereClause where = first.hasWhereClause() ? first.getWhereClause() : statement.getWhereClause();
    // a lookup by id finds one record at most
    if (node.getVariable() != null && extractIdFilter(where, node.getVariable()) != null)
      return true;
    // only a single static label lets the node step seek an index (MatchNodeStep.computeVertexIterator)
    if (node.hasDynamicLabels() || !node.hasLabels() || node.getLabels().size() != 1)
      return false;

    // The node judged over its own pattern and WHERE alone: an OPTIONAL clause's predicates do not narrow where the pipeline starts
    final LogicalPlan plan = LogicalPlan.fromAST(new SimpleCypherStatement("", List.of(new MatchClause(List.of(path), false, where)),
        null, null, false, false, false));
    statistics.collectStatistics(node.getLabels());
    final LogicalNode firstNode = plan.getPatternNodes().values().iterator().next();
    return isSelectiveSeek(new AnchorSelector(statistics, new CostModel(statistics)).evaluateNodeDirect(firstNode, plan), statistics);
  }

  /**
   * Whether an anchor is an index seek estimated to find few vertices: one row of a UNIQUE index whose whole key is bound, or
   * fewer than one row in {@link #SELECTIVE_ANCHOR_LABEL_SHARE} of the label it seeks. A range scan never is, whatever its
   * estimate.
   */
  private static boolean isSelectiveSeek(final AnchorSelection anchor, final StatisticsProvider statistics) {
    if (anchor == null || !anchor.useIndex() || anchor.isRangeScan() || anchor.getNode() == null)
      return false;
    final long rows = Math.max(1L, anchor.getEstimatedCardinality());
    final IndexStatistics index = anchor.getIndex();
    if (rows == 1L && index != null && index.isUnique() && anchor.getKeyValues().size() == index.getPropertyNames().size())
      return true;
    final LogicalNode node = anchor.getNode();
    return rows * SELECTIVE_ANCHOR_LABEL_SHARE < statistics.getMatchingVertexCardinality(node.getLabels(), node.isLabelDisjunction());
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
   * It is only meaningful for a type whose instances <b>are</b> records: an edge type may keep no edge
   * record (light edges, declared LIGHTWEIGHT or not), so its counter is 0 while its edges exist in the vertices' edge
   * lists, and every edge type answers "not empty" here.
   * <p>
   * It is asked by name, and a name is not a namespace: a node label matches only vertices, but a document or edge
   * type may carry the same name, and a populated one of those answers "not empty" for a label no vertex has. That
   * costs the early-out and nothing else - the push-down then runs and computes the same 0, because it filters on
   * the label's buckets, which hold no vertex either. {@link AggregationShortcutPlanner#tryCreateTypeCountOptimization} has to guard the
   * collision because it answers <i>from</i> the counter; this only decides whether to skip work.
   */
  private static boolean typeIsProvablyEmpty(final Database db, final String name) {
    final Schema schema = db.getSchema();
    if (!schema.existsType(name))
      return true;

    final DocumentType type = schema.getType(name);
    // an edge type with no record is not an empty one: light edges keep no record, and they exist in a type that never
    // declared LIGHTWEIGHT as well (MutableVertex.newLightEdge accepts it, and so did the batch before #9383), so the
    // flag only says where they are expected (#9389)
    if (type.getType() == Edge.RECORD_TYPE)
      return false;

    return db.countType(name, true) == 0;
  }

  /**
   * Returns true if any node in any MATCH path pattern carries a dynamic label, which no count-push-down operator can
   * filter on: they key off node labels and edge types (issue #5071).
   */
  private boolean hasDynamicLabel() {
    return anyMatchNode(NodePattern::hasDynamicLabels);
  }

  /**
   * Returns true if any node in any MATCH path pattern carries an inline property filter (e.g. {@code {id: 1}} or
   * {@code $props}). Every count operator but the triangle one applies it, as a per-vertex predicate (issues #9595, #9608).
   */
  private boolean hasInlineNodeProperty() {
    return anyMatchNode(NodePattern::hasProperties);
  }

  private boolean anyMatchNode(final Predicate<NodePattern> test) {
    if (statement.getMatchClauses() == null)
      return false;
    for (final MatchClause mc : statement.getMatchClauses()) {
      if (!mc.hasPathPatterns())
        continue;
      for (final PathPattern pp : mc.getPathPatterns())
        for (int i = 0; i <= pp.getRelationshipCount(); i++)
          if (test.test(pp.getNode(i)))
            return true;
    }
    return false;
  }

  /** Whether two positions of the path bind the same node variable, i.e. the path closes on a vertex it already visited. */
  static boolean repeatsNodeVariable(final PathPattern pathPattern) {
    final int nodeCount = pathPattern.getRelationshipCount() + 1;
    for (int i = 0; i < nodeCount; i++) {
      final String variable = pathPattern.getNode(i).getVariable();
      if (variable == null || variable.isEmpty())
        continue;
      for (int j = i + 1; j < nodeCount; j++)
        if (variable.equals(pathPattern.getNode(j).getVariable()))
          return true;
    }
    return false;
  }

  String extractIdFilter(final WhereClause whereClause, final String variable) {
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
