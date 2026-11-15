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

import com.arcadedb.query.opencypher.executor.CypherExecutionPlan.SeedCorrelation;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.CallClause;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CreateClause;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.DeleteClause;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.ForeachClause;
import com.arcadedb.query.opencypher.ast.LoadCSVClause;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.MergeClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.QuantifiedPathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.RemoveClause;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.ast.SimpleCypherStatement;
import com.arcadedb.query.opencypher.ast.ShortestPathPattern;
import com.arcadedb.query.opencypher.ast.SubqueryClause;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.executor.operators.GAVFusedChainOperator;
import com.arcadedb.query.opencypher.executor.operators.InListValues;
import com.arcadedb.query.opencypher.executor.steps.AggregationStep;
import com.arcadedb.query.opencypher.executor.steps.CallStep;
import com.arcadedb.query.opencypher.executor.steps.CountEdgesReturnStep;
import com.arcadedb.query.opencypher.executor.steps.CreateStep;
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
import com.arcadedb.query.opencypher.executor.steps.ProjectReturnStep;
import com.arcadedb.query.opencypher.executor.steps.QuantifiedPathStep;
import com.arcadedb.query.opencypher.executor.steps.RemoveStep;
import com.arcadedb.query.opencypher.executor.steps.SemiJoinStep;
import com.arcadedb.query.opencypher.executor.steps.SetStep;
import com.arcadedb.query.opencypher.executor.steps.ShortestPathStep;
import com.arcadedb.query.opencypher.executor.steps.SkipStep;
import com.arcadedb.query.opencypher.executor.steps.SubqueryStep;
import com.arcadedb.query.opencypher.executor.steps.UniqueKeyAnchor;
import com.arcadedb.query.opencypher.executor.steps.UnwindStep;
import com.arcadedb.query.opencypher.executor.steps.WithStep;
import com.arcadedb.query.opencypher.executor.steps.ZeroLengthPathStep;
import com.arcadedb.query.opencypher.planner.CypherEagernessAnalyzer;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.opencypher.optimizer.CypherOptimizer;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.ExecutionStepInternal;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Chains the execution steps of a statement, clause by clause, along one of three paths: on top of the optimizer's
 * physical plan, in the order the clauses were written, or over the flat clause lists of a statement parsed without
 * that order. The three share the builders of a MATCH and of a WITH, the seeding of a pattern from an index and the
 * reordering of its independent parts, so a change to how a clause is handled is made here for all of them.
 * <p>
 * Split out of {@link CypherExecutionPlan}, which decides which path runs and executes what it returns.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CypherStepBuilder {
  private final CypherExecutionPlan        plan;
  private final DatabaseInternal           database;
  private final CypherStatement            statement;
  private final Map<String, Object>        parameters;
  private final PhysicalPlan               physicalPlan;
  private final ExpressionEvaluator        expressionEvaluator;
  private final HopEdgeTrackingAnalyzer    hopEdges;
  private final FilterPushdown             filterPushdown;
  private final AggregationShortcutPlanner aggregationShortcuts;

  // Query-level counter for unique anonymous variable names across MATCH clauses
  private int anonymousVarCounter = 0;
  // Set when a semi-join chain was planned but did not read the row it is handed: the plan is then built without one
  private boolean semiJoinDisabled = false;

  CypherStepBuilder(final CypherExecutionPlan plan, final DatabaseInternal database, final CypherStatement statement,
      final Map<String, Object> parameters, final PhysicalPlan physicalPlan, final ExpressionEvaluator expressionEvaluator,
      final HopEdgeTrackingAnalyzer hopEdges, final FilterPushdown filterPushdown, final AggregationShortcutPlanner aggregationShortcuts) {
    this.plan = plan;
    this.database = database;
    this.statement = statement;
    this.parameters = parameters;
    this.physicalPlan = physicalPlan;
    this.expressionEvaluator = expressionEvaluator;
    this.hopEdges = hopEdges;
    this.filterPushdown = filterPushdown;
    this.aggregationShortcuts = aggregationShortcuts;
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
  AbstractExecutionStep buildExecutionStepsWithOptimizer(final CommandContext context) {
    // Get function factory from evaluator for steps that need it
    final CypherFunctionFactory functionFactory = expressionEvaluator != null ?
        expressionEvaluator.getFunctionFactory() : null;

    // Create a wrapper step that executes the physical operators
    AbstractExecutionStep currentStep = plan.newOptimizedMatchStep(context);

    // Apply post-MATCH operations using clausesInOrder to respect the order they appear
    // in the query (e.g. WITH before UNWIND, not the other way around).
    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    // MATCH clauses seen since the last WITH (or since the start), i.e. the ones that actually feed
    // whichever DELETE segment is reached next - see EagerBarriers.matchClausesNeedEagerDelete().
    final List<MatchClause> currentSegmentMatchClauses = new ArrayList<>();
    // See buildExecutionStepsWithOrder(): the same read/write barrier analysis, over the same clause list
    // (issue #7171). This path never sees a CALL or a FOREACH, so only CREATE and MERGE are weighed here.
    final CypherEagernessAnalyzer eagerness = CypherEagernessAnalyzer.forStatement(statement);
    final Set<String> optimizerBoundVariables = new HashSet<>();
    if (clausesInOrder != null) {
      for (final ClauseEntry entry : clausesInOrder) {
        // The physical plan runs every MATCH ahead of the clauses below: only one that starts the statement binds a row each.
        // An identity check on purpose: the loop walks the statement's own list, not a copy
        final boolean startsStatement = entry == clausesInOrder.getFirst();
        switch (entry.getType()) {
        case MATCH: {
          // MATCH pattern AND its WHERE are handled by the optimizer above: the physical plan applies every MATCH-level
          // WHERE (CypherOptimizer.applyFilterPushdown, or the disconnected-pattern join planner). Applying it again here
          // evaluated each predicate twice per row (issue #9202)
          final MatchClause matchClause = entry.getTypedClause();
          currentSegmentMatchClauses.add(matchClause);
          eagerness.observeRead(matchClause);
          eagerness.observeClause(entry, startsStatement, optimizerBoundVariables);
          EagerBarriers.collectPatternVariables(matchClause, optimizerBoundVariables);
          break;
        }

        case CREATE: {
          final CreateClause createClause = entry.getTypedClause();
          if (!createClause.isEmpty()) {
            if (eagerness.needsBarrier(createClause, optimizerBoundVariables))
              currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
            final CreateStep createStep = new CreateStep(createClause, context, functionFactory);
            createStep.setPrevious(currentStep);
            currentStep = createStep;
            eagerness.observeWriteClause();
            eagerness.observeClause(entry, false, optimizerBoundVariables);
            // What the CREATE binds is bound from here on, as in buildExecutionStepsWithOrder(): a later CREATE that names one
            // of them refers to that entity rather than creating a new one each row
            EagerBarriers.collectPatternVariables(createClause.getPathPatterns(), optimizerBoundVariables);
          }
          break;
        }

        case SET: {
          final SetClause setClause = entry.getTypedClause();
          // A SET of what a MERGE before it matches on runs after the MERGE of every row (issue #10889), and one of a key a
          // clause before it read for another row after that read of every row (issue #11005): it gets the barrier, so the
          // MERGE step must not absorb it
          final boolean barrierFirst = !setClause.isEmpty()
              && (eagerness.needsBarrierAfterMerge(setClause) || eagerness.needsBarrierForPropertyWrites(setClause));
          if (!setClause.isEmpty() && (barrierFirst || !EagerBarriers.absorbsSet(currentStep, setClause))) {
            if (currentStep != null && (barrierFirst || eagerness.needsBarrier(setClause)))
              currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
            final SetStep setStep = new SetStep(setClause, context, functionFactory);
            setStep.setPrevious(currentStep);
            currentStep = setStep;
            eagerness.observeWriteClause();
          }
          eagerness.observeClause(entry, false, optimizerBoundVariables);
          break;
        }

        case DELETE: {
          final DeleteClause deleteClause = entry.getTypedClause();
          if (!deleteClause.isEmpty()) {
            final DeleteStep deleteStep = new DeleteStep(deleteClause, context,
                EagerBarriers.matchClausesNeedEagerDelete(currentSegmentMatchClauses));
            deleteStep.setPrevious(currentStep);
            currentStep = deleteStep;
            eagerness.observeWriteClause();
            eagerness.observeClause(entry, false, optimizerBoundVariables);
          }
          break;
        }

        case REMOVE: {
          final RemoveClause removeClause = entry.getTypedClause();
          if (!removeClause.isEmpty()) {
            if (currentStep != null && (eagerness.needsBarrier(removeClause) || eagerness.needsBarrierAfterMerge(removeClause)
                || eagerness.needsBarrierForPropertyWrites(removeClause)))
              currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
            final RemoveStep removeStep = new RemoveStep(removeClause, context, functionFactory);
            removeStep.setPrevious(currentStep);
            currentStep = removeStep;
            eagerness.observeWriteClause();
          }
          break;
        }

        case MERGE: {
          final MergeClause mergeClause = entry.getTypedClause();
          if (eagerness.needsBarrier(mergeClause, optimizerBoundVariables) || eagerness.needsBarrierAfterMerge(mergeClause)
              || eagerness.needsBarrierForPropertyWrites(mergeClause))
            currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
          final MergeStep mergeStep = new MergeStep(mergeClause, context, functionFactory);
          mergeStep.setPrevious(currentStep);
          currentStep = mergeStep;
          eagerness.observeWriteClause();
          eagerness.observeMergeRead(mergeClause);
          eagerness.observeClause(entry, false, optimizerBoundVariables);
          EagerBarriers.collectPatternVariables(List.of(mergeClause.getPathPattern()), optimizerBoundVariables);
          break;
        }

        case UNWIND: {
          final UnwindClause unwindClause = entry.getTypedClause();
          final UnwindStep unwindStep = new UnwindStep(unwindClause, context, functionFactory);
          unwindStep.setPrevious(currentStep);
          currentStep = unwindStep;
          eagerness.observeClause(entry, false, optimizerBoundVariables);
          optimizerBoundVariables.add(unwindClause.getVariable());
          break;
        }

        case WITH: {
          final WithClause withClause = entry.getTypedClause();
          currentStep = EagerBarriers.withLimitBarrier(currentStep, context, eagerness, functionFactory, withClause);
          currentStep = buildWithStepForOptimizer(withClause, currentStep, context, functionFactory);
          EagerBarriers.markDrainOnZeroLimit(currentStep, withClause, eagerness);
          eagerness.observeClause(entry, false, optimizerBoundVariables);
          if (withClause.hasAggregations())
            eagerness.observeAggregationBoundary();
          applyProjectionToScope(withClause.getItems(), optimizerBoundVariables);
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
          eagerness.observeClause(entry, false, optimizerBoundVariables);
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
      final AbstractExecutionStep countOpt = aggregationShortcuts.tryOptimizeMatchCountReturn(
          statement.getClausesInOrder(), statement.getReturnClause(), currentStep, context);
      if (countOpt != null) {
        currentStep = countOpt;
      } else if (statement.getReturnClause().hasAggregations()) {
        // A reachability search counts its end vertices itself: no row is built per end vertex (issue #10548)
        final AbstractExecutionStep endpointCount = aggregationShortcuts.tryCountDistinctEndpoints(statement.getReturnClause().getReturnItems(),
            currentStep, context);
        if (endpointCount != null)
          currentStep = endpointCount;
        // The fused chain counts its paths itself and answers the aggregated rows: no step aggregates them (issue #9738)
        final boolean fusedIntoChain =
            endpointCount != null || aggregationShortcuts.tryFuseAggregationIntoChain(statement.getReturnClause().getReturnItems(), currentStep);
        if (!fusedIntoChain) {
          // One row per edge of a one-hop pattern, read from a Graph Analytical View instead of the operators (#8335)
          final AbstractExecutionStep gavScan = aggregationShortcuts.tryGAVOneHopScan(context);
          if (gavScan != null)
            currentStep = gavScan;

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
        }
      } else {
        // Use regular projection for non-aggregation expressions
        final ProjectReturnStep returnStep = new ProjectReturnStep(statement.getReturnClause(), context,
            functionFactory);
        returnStep.setPrevious(currentStep);
        currentStep = returnStep;
      }
    }

    // Step 8: ORDER BY (if any), unless the operators already produce the rows in that order from an index (issue #8422)
    if (statement.getOrderByClause() != null && !physicalPlan.isIndexOrdered()) {
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
      currentStep = EagerBarriers.withLimitBarrier(currentStep, context, eagerness, functionFactory, statement);
      final int limitVal = new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getLimit(),
          new ResultInternal(), context);
      final LimitStep limitStep = newLimitStep(limitVal, context);
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
  AbstractExecutionStep buildExecutionSteps(final CommandContext context) {
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
  AbstractExecutionStep buildExecutionStepsWithOrder(final CommandContext context,
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
  AbstractExecutionStep buildExecutionStepsWithOrder(final CommandContext context,
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
    // whichever DELETE/FOREACH segment is reached next - see EagerBarriers.matchClausesNeedEagerDelete().
    final List<MatchClause> currentSegmentMatchClauses = new ArrayList<>();
    // Variables bound by a segment that needed the eager guard (disconnected patterns, or a
    // variable-length relationship), kept forever once tainted (even across a WITH that merely forwards
    // them unchanged) - see EagerBarriers.closeMatchSegment() and EagerBarriers.deleteMayTargetTaintedVariable().
    final Set<String> disconnectedTaintedVariables = new HashSet<>();

    // Shape of everything read so far, weighed against each write clause's own shape to decide where an
    // eager read/write barrier has to go so the query never reads back what it just created (issue #7171).
    final CypherEagernessAnalyzer eagerness = CypherEagernessAnalyzer.forStatement(statement);

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
      final AbstractExecutionStep countStep = plan.tryCountPushDown(context, false, correlation);
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

        // Both sites that build this step produce the same thing - one dummy row for a statement whose
        // expressions stand on their own - so they deliberately share a name and aggregate together in the
        // profiler's per-step statistics.
        @Override
        public String getName() {
          return "DummyRowStep";
        }

        @Override
        public String getType() {
          return getName();
        }

        @Override
        public String prettyPrint(final int depth, final int indent) {
          return "  ".repeat(Math.max(0, depth * indent)) + "+ DUMMY ROW (for standalone expressions)";
        }
      };
    }
    // The one-row source the clauses start from (the dummy row, or the seed row of a CALL subquery body), or null
    final AbstractExecutionStep singleRowSource = currentStep;

    // The clauses a RETURN DISTINCT needs only for whether a row survives run as the chain of a SemiJoinStep (#11009).
    // A look-ahead that consumes the start clause together with the one before it leaves the plan as it was.
    final DistinctSemiJoin.Plan semiJoin =
        initialStep == null && !semiJoinDisabled ? DistinctSemiJoin.plan(statement, clausesInOrder) : null;
    AbstractExecutionStep semiJoinOuter = null;
    SemiJoinStep.ChainInputStep semiJoinInput = null;

    // Process clauses in order (indexed loop to support look-ahead for optimizations)
    for (int entryIndex = 0; entryIndex < clausesInOrder.size(); entryIndex++) {
      if (semiJoin != null && entryIndex == semiJoin.start() && currentStep != null) {
        semiJoinOuter = currentStep;
        semiJoinInput = new SemiJoinStep.ChainInputStep(context);
        currentStep = semiJoinInput;
      }
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
        eagerness.observeClause(entry, false, boundVariables);
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
        eagerness.observeClause(entry, false, boundVariables);
        boundVariables.add(loadCSVClause.getVariable());
        break;

      case MATCH:
        final MatchClause matchClause = entry.getTypedClause();
        currentSegmentMatchClauses.add(matchClause);
        // A property an earlier clause writes must be fully written before this one reads it (issue #8733)
        if (currentStep != null && eagerness.needsBarrier(matchClause))
          currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
        eagerness.observeRead(matchClause);
        eagerness.observeClause(entry, currentStep == singleRowSource, boundVariables);
        if (matchClause.isOptional()) {
          // Try chained count optimization first (handles 2 consecutive OPTIONAL MATCH + count)
          final AbstractExecutionStep chainedOptimized = aggregationShortcuts.tryOptimizeChainedOptionalMatchCount(
              matchClause, clausesInOrder, entryIndex, currentStep, context, boundVariables);
          if (chainedOptimized != null) {
            currentStep = chainedOptimized;
            // The optimization swallows the next OPTIONAL MATCH whole, so its patterns are read without ever
            // reaching this switch: fold them into the read footprint before skipping past them.
            if (clausesInOrder.get(entryIndex + 1).getType() == ClauseEntry.ClauseType.MATCH)
              eagerness.observeRead(clausesInOrder.get(entryIndex + 1).getTypedClause());
            entryIndex += 2; // skip both the next OPTIONAL MATCH and the WITH clause
            eagerness.observeClause(clausesInOrder.get(entryIndex - 1), false, boundVariables);
            eagerness.observeClause(clausesInOrder.get(entryIndex), false, boundVariables);
            // Update boundVariables from the WITH clause
            final WithClause nextWith = ((ClauseEntry) clausesInOrder.get(entryIndex)).getTypedClause();
            applyProjectionToScope(nextWith.getItems(), boundVariables);
            // the skipped WITH starts a new segment (issue #6631)
            EagerBarriers.closeMatchSegment(currentSegmentMatchClauses, disconnectedTaintedVariables);
            EagerBarriers.propagateTaintThroughRenames(nextWith, disconnectedTaintedVariables);
            break;
          }

          // Try single OPTIONAL MATCH count optimization
          final AbstractExecutionStep optimized = aggregationShortcuts.tryOptimizeOptionalMatchCount(
              matchClause, clausesInOrder, entryIndex, currentStep, context, boundVariables);
          if (optimized != null) {
            currentStep = optimized;
            entryIndex++; // skip the WITH clause (already handled)
            eagerness.observeClause(clausesInOrder.get(entryIndex), false, boundVariables);
            // Update boundVariables from the WITH clause
            final WithClause nextWith = ((ClauseEntry) clausesInOrder.get(entryIndex)).getTypedClause();
            applyProjectionToScope(nextWith.getItems(), boundVariables);
            // the skipped WITH starts a new segment (issue #6631)
            EagerBarriers.closeMatchSegment(currentSegmentMatchClauses, disconnectedTaintedVariables);
            EagerBarriers.propagateTaintThroughRenames(nextWith, disconnectedTaintedVariables);
            break;
          }
        }
        currentStep = buildMatchStep(matchClause, currentStep, context, boundVariables);
        break;

      case WITH:
        final WithClause withClause = entry.getTypedClause();
        currentStep = EagerBarriers.withLimitBarrier(currentStep, context, eagerness, functionFactory, withClause);
        currentStep = buildWithStep(withClause, currentStep, context, functionFactory);
        EagerBarriers.markDrainOnZeroLimit(currentStep, withClause, eagerness);
        // What the projection reads per row, before an aggregation drains it (issue #11005)
        eagerness.observeClause(entry, false, boundVariables);
        // An explicit WITH resets the scope to its own output variables; WITH * forwards the incoming one
        applyProjectionToScope(withClause.getItems(), boundVariables);
        // A WITH boundary starts a new segment (issue #6631) - but a WITH that plainly forwards a
        // variable bound by a disconnected-pattern MATCH (e.g. WITH n, o) does not resolve the #6491
        // hazard for that variable, since rows still flow through it one at a time rather than being
        // fully consumed; EagerBarriers.closeMatchSegment() taints it before the segment is cleared.
        EagerBarriers.closeMatchSegment(currentSegmentMatchClauses, disconnectedTaintedVariables);
        // A rename (WITH n AS m) doesn't change how rows flow either - propagate the taint onto the
        // new name too, or a later DELETE of m would find nothing tainted under that name.
        EagerBarriers.propagateTaintThroughRenames(withClause, disconnectedTaintedVariables);
        // Only an aggregating WITH provably drains the enumerations feeding it, closing the #7171 hazard for
        // everything read before it; a plain forwarding WITH leaves them just as open as they were.
        if (withClause.hasAggregations())
          eagerness.observeAggregationBoundary();
        break;

      case MERGE:
        final MergeClause mergeClause = entry.getTypedClause();
        if (currentStep != null && (eagerness.needsBarrier(mergeClause, boundVariables)
            || eagerness.needsBarrierAfterMerge(mergeClause) || eagerness.needsBarrierForPropertyWrites(mergeClause)))
          currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
        // A MERGE that starts the statement runs its lookup once: no later write can change what it finds (issue #10889)
        final boolean mergeRunsPerRow = currentStep != null && currentStep != singleRowSource;
        final MergeStep mergeStep =
            new MergeStep(mergeClause, context, functionFactory);
        if (currentStep != null) {
          mergeStep.setPrevious(currentStep);
        }
        currentStep = mergeStep;
        eagerness.observeWrite(mergeClause);
        if (mergeRunsPerRow)
          eagerness.observeMergeRead(mergeClause);
        eagerness.observeClause(entry, false, boundVariables);
        // What the MERGE binds is as much a bound name as what a MATCH binds: a MATCH after it that ends at one of them
        // starts from that entity instead of scanning the side it leaves open (issue #10515)
        EagerBarriers.collectPatternVariables(List.of(mergeClause.getPathPattern()), boundVariables);
        break;

      case CREATE:
        final CreateClause createClause = entry.getTypedClause();
        if (!createClause.isEmpty()) {
          if (currentStep != null && eagerness.needsBarrier(createClause, boundVariables))
            currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
          final CreateStep createStep = new CreateStep(createClause, context, functionFactory);
          if (currentStep != null) {
            createStep.setPrevious(currentStep);
          }
          currentStep = createStep;
          eagerness.observeWriteClause();
          eagerness.observeClause(entry, false, boundVariables);
          EagerBarriers.collectPatternVariables(createClause.getPathPatterns(), boundVariables);
        }
        break;

      case SET:
        final SetClause setClause = entry.getTypedClause();
        if (!setClause.isEmpty() && currentStep != null) {
          // EagerBarriers.absorbsSet() hands the clause over to the step, so it is asked exactly once: a second call finds the
          // clause already taken, answers false and plans the SET a second time as a step of its own (issue #8809).
          // A SET of what a MERGE before it matches on runs after the MERGE of every row (issue #10889): it gets the
          // barrier, so the MERGE step must not absorb it.
          // A SET of a key a clause before it read for another row runs after that read of every row (issue #11005): the same
          // barrier, so the step before it must not absorb it either.
          final boolean barrierFirst = eagerness.needsBarrierAfterMerge(setClause) || eagerness.needsBarrierForPropertyWrites(setClause);
          if (barrierFirst || !EagerBarriers.absorbsSet(currentStep, setClause)) {
            if (barrierFirst || eagerness.needsBarrier(setClause))
              currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
            final SetStep setStep =
                new SetStep(setClause, context, functionFactory);
            setStep.setPrevious(currentStep);
            currentStep = setStep;
          }
          // A SET a MERGE/CREATE absorbs still writes, so it is observed either way
          eagerness.observeWrite(setClause);
          eagerness.observeClause(entry, false, boundVariables);
        }
        break;

      case REMOVE:
        final RemoveClause removeClause = entry.getTypedClause();
        if (!removeClause.isEmpty() && currentStep != null) {
          if (eagerness.needsBarrier(removeClause) || eagerness.needsBarrierAfterMerge(removeClause)
              || eagerness.needsBarrierForPropertyWrites(removeClause))
            currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
          final RemoveStep removeStep =
              new RemoveStep(removeClause, context, functionFactory);
          removeStep.setPrevious(currentStep);
          currentStep = removeStep;
          eagerness.observeWrite(removeClause);
        }
        break;

      case DELETE:
        final DeleteClause deleteClause = entry.getTypedClause();
        if (!deleteClause.isEmpty() && currentStep != null) {
          final boolean eagerMaterialize = EagerBarriers.matchClausesNeedEagerDelete(currentSegmentMatchClauses)
              || EagerBarriers.deleteMayTargetTaintedVariable(deleteClause.getVariables(), disconnectedTaintedVariables);
          final DeleteStep deleteStep = new DeleteStep(deleteClause, context, eagerMaterialize);
          deleteStep.setPrevious(currentStep);
          currentStep = deleteStep;
          eagerness.observeWriteClause();
          eagerness.observeClause(entry, false, boundVariables);
        }
        break;

      case RETURN:
        // RETURN is handled at the end
        break;

      case CALL:
        final CallClause callClause = entry.getTypedClause();
        // A write procedure is opaque to the planner - merge.relationship takes its type as a runtime
        // argument - so it conflicts with every read still in flight ahead of it (issue #7171).
        if (currentStep != null && eagerness.needsBarrierForWriteProcedure()
            && SimpleCypherStatement.isWriteProcedureCall(callClause))
          currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
        if (SimpleCypherStatement.isWriteProcedureCall(callClause))
          eagerness.observeWriteProcedure();
        final CallStep callStep =
            new CallStep(callClause, context, functionFactory);
        if (currentStep != null) {
          callStep.setPrevious(currentStep);
        }
        // Detect count-only pattern: CALL ... YIELD ... RETURN count(*)
        // When detected, enable fast-path that skips per-row Result object creation
        if (plan.isFollowedByCountOnlyReturn(clausesInOrder, entryIndex)) {
          callStep.setCountOnlyOptimization(true);
        }
        currentStep = callStep;
        eagerness.observeClause(entry, false, boundVariables);
        // A CALL binds the names it yields the way UNWIND binds its variable: they belong to the scope a
        // following MATCH starts from, not to that MATCH's own clause. Registering them is what lets the
        // MATCH push a predicate that reads one of them down into its scan, and what keeps a yielded
        // relationship out of the MATCH's clause-scoped uniqueness set (issue #7165).
        collectCallOutputVariables(callClause, boundVariables);
        break;

      case FOREACH:
        final ForeachClause foreachClause = entry.getTypedClause();
        final boolean foreachEagerMaterialize = foreachClause.containsDelete()
            && (EagerBarriers.matchClausesNeedEagerDelete(currentSegmentMatchClauses)
                || EagerBarriers.deleteMayTargetTaintedVariable(EagerBarriers.collectForeachDeleteTargetVariables(foreachClause),
                    disconnectedTaintedVariables));
        final boolean foreachEagerExecution =
            database.getConfiguration().getValueAsBoolean(GlobalConfiguration.OPENCYPHER_FOREACH_EAGER_READ)
                && GraphReadAnalysis.graphReadFollows(clausesInOrder, entryIndex);
        if (currentStep != null && (eagerness.needsBarrier(foreachClause, boundVariables)
            || eagerness.needsBarrierAfterMerge(foreachClause) || eagerness.needsBarrierForPropertyWrites(foreachClause)))
          currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
        final ForeachStep foreachStep =
            new ForeachStep(foreachClause, context, functionFactory, foreachEagerMaterialize, foreachEagerExecution);
        if (currentStep != null) {
          foreachStep.setPrevious(currentStep);
        }
        currentStep = foreachStep;
        eagerness.observeWriteClause();
        eagerness.observeClause(entry, false, boundVariables);
        break;

      case SUBQUERY:
        final SubqueryClause subqueryClause = entry.getTypedClause();
        final CypherStatement subqueryBody = subqueryClause.getInnerStatement();
        // The body runs once per row the enumeration behind it hands over, so a node it deletes is unlinked while that
        // enumeration is still walking it (issue #10512). The rows are read ahead of the first run on the same terms as for
        // a DELETE placed in the outer query, where DeleteStep.eagerMaterialize is decided
        if (currentStep != null && ((EagerBarriers.subqueryBodyDeletes(subqueryBody)
            && (EagerBarriers.matchClausesNeedEagerDelete(currentSegmentMatchClauses) || !disconnectedTaintedVariables.isEmpty()))
            || eagerness.needsBarrierForPropertyWrites(subqueryBody)))
          currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
        final SubqueryStep subqueryStep =
            new SubqueryStep(subqueryClause, context, database, parameters, expressionEvaluator);
        // Nothing in a statement that cannot write needs the body's reads finished before the next clause: its rows stream,
        // so a LIMIT after the CALL stops the body too (issue #11082)
        subqueryStep.setStreamBody(statement.isReadOnly());
        if (currentStep != null) {
          subqueryStep.setPrevious(currentStep);
        }
        currentStep = subqueryStep;
        if (!subqueryClause.getInnerStatement().isReadOnly())
          eagerness.observeWriteClause();
        eagerness.observeClause(entry, false, boundVariables);
        // What the subquery returns joins the outer scope, exactly as a CALL's YIELD names do, so a following
        // MATCH can push a predicate that reads one of them into its scan instead of filtering behind it.
        collectSubqueryOutputVariables(subqueryClause, boundVariables);
        break;
      }
    }

    if (semiJoinOuter != null) {
      if (!feedsFrom(currentStep, semiJoinInput)) {
        // A step that started a source of its own instead of reading the row it was handed would answer the same for
        // every input row: plan the statement again without the semi-join rather than risk it
        semiJoinDisabled = true;
        return buildExecutionStepsWithOrder(context, clausesInOrder, initialStep, correlation, seedVariableNames);
      }
      final SemiJoinStep semiJoinStep = new SemiJoinStep(semiJoinInput, currentStep, semiJoin.distinctKey(),
          semiJoin.memoKey(), context);
      semiJoinStep.setPrevious(semiJoinOuter);
      currentStep = semiJoinStep;
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
          ? aggregationShortcuts.tryOptimizeMatchCountReturn(clausesInOrder, statement.getReturnClause(), currentStep, context)
          : null;
      if (countOpt != null)
        currentStep = countOpt;
      else if (statement.getReturnClause().hasAggregations()) {
        // A distinct-endpoint walk counts its end vertices itself: no row per end vertex, no filter, no aggregation (issue #10557)
        // Not for a body seeded with rows from outside (a subquery), which may be run once per outer row
        final AbstractExecutionStep endpointCount = statement.getReturnClause().hasNonAggregations() || initialStep != null ? null :
            aggregationShortcuts.tryCountDistinctEndpointsOfSteps(clausesInOrder, statement.getReturnClause().getReturnItems(), currentStep, context);
        if (endpointCount != null)
          currentStep = endpointCount;
        else if (statement.getReturnClause().hasNonAggregations()) {
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
      currentStep = EagerBarriers.withLimitBarrier(currentStep, context, eagerness, functionFactory, statement);
      final Integer limitVal = new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getLimit(),
          new ResultInternal(), context);
      final LimitStep limitStep = newLimitStep(limitVal, context);
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

  /** Whether {@code source} is reached from {@code end} through the steps' previous links. */
  private static boolean feedsFrom(final AbstractExecutionStep end, final AbstractExecutionStep source) {
    for (ExecutionStepInternal step = end; step != null; step = step instanceof AbstractExecutionStep abstractStep ?
        abstractStep.getPrev() : null)
      if (step == source)
        return true;
    return false;
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

    return addWithOrderSkipLimit(withClause, currentStep, context, functionFactory);
  }

  /**
   * Chains the ORDER BY, SKIP and LIMIT of a WITH after the step that projects it. A plain {@link WithStep} pages its own
   * rows when there is no ORDER BY, but an aggregating WITH is projected by an aggregation step that knows nothing of SKIP
   * and LIMIT, so they are chained after it here: without them {@code WITH collect(x) AS q LIMIT 0} still passed its row on,
   * and a CREATE behind it ran (issue #10892).
   */
  private AbstractExecutionStep addWithOrderSkipLimit(final WithClause withClause, AbstractExecutionStep currentStep,
      final CommandContext context, final CypherFunctionFactory functionFactory) {
    final boolean ordered = withClause.getOrderByClause() != null;
    if (!ordered && !withClause.hasAggregations())
      return currentStep;

    final ExpressionEvaluator evaluator = new ExpressionEvaluator(functionFactory);
    final Integer skipVal = withClause.getSkip() != null ?
        evaluator.evaluateSkipLimit(withClause.getSkip(), new ResultInternal(), context) : null;
    final Integer limitVal = withClause.getLimit() != null ?
        evaluator.evaluateSkipLimit(withClause.getLimit(), new ResultInternal(), context) : null;

    if (ordered) {
      // Top-K keeps SKIP + LIMIT rows so enough survive the skip; a sum past int range sorts everything instead
      Integer topKVal = null;
      if (limitVal != null) {
        final long topK = (long) limitVal + (skipVal != null ? skipVal : 0);
        topKVal = topK <= Integer.MAX_VALUE ? (int) topK : null;
      }
      final OrderByStep orderByStep = new OrderByStep(withClause.getOrderByClause(), context, functionFactory, topKVal);
      if (currentStep != null)
        orderByStep.setPrevious(currentStep);
      currentStep = orderByStep;
    }

    // Pagination happens after sorting and after aggregating
    if (skipVal != null) {
      final SkipStep skipStep = new SkipStep(skipVal, context);
      if (currentStep != null)
        skipStep.setPrevious(currentStep);
      currentStep = skipStep;
    }
    if (limitVal != null) {
      final LimitStep limitStep = newLimitStep(limitVal, context);
      if (currentStep != null)
        limitStep.setPrevious(currentStep);
      currentStep = limitStep;
    }

    // Strip non-projected variables that were kept for ORDER BY evaluation
    if (ordered)
      currentStep = plan.addWithProjection(withClause, currentStep, context);
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
        if (!aggregationShortcuts.tryFuseAggregationIntoChain(withClause.getItems(), currentStep)) {
          // Fallback: GROUP BY aggregation (implicit grouping)
          final GroupByAggregationStep groupByStep =
              new GroupByAggregationStep(
                  new ReturnClause(withClause.getItems(), false),
                  context, functionFactory);
          groupByStep.setPrevious(currentStep);
          currentStep = groupByStep;
        }
      } else if (!aggregationShortcuts.tryFuseAggregationIntoChain(withClause.getItems(), currentStep)) {
        // Pure aggregation (no grouping), unless the fused chain counts its paths itself (issue #9738)
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

    return addWithOrderSkipLimit(withClause, currentStep, context, functionFactory);
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
    // Handed to every relationship step of this clause BY REFERENCE, never copied: a step built for an early
    // hop has to observe the names later hops add, and it does because planning finishes long before the first
    // row is pulled. Copying it at a call site "to be safe" would give that step a scope missing the very
    // variables the uniqueness rule has to compare against, which is the bug this whole file's #7165 comments
    // are about, reintroduced one hop at a time.
    final Set<String> matchVariables = new HashSet<>();
    // The other half of the relationship-uniqueness scope: see HopEdgeTrackingAnalyzer.clauseRelationshipVariables().
    final Set<String> clauseRelVariables = HopEdgeTrackingAnalyzer.clauseRelationshipVariables(matchClause);
    final boolean isOptional = matchClause.isOptional();
    // The WHERE conjuncts a node step of this clause already decides on every row it passes on: a later node step does not take
    // them again, and the clause's filter applies only the rest (issue #11102). By identity: the conjuncts are the parsed nodes.
    final Set<BooleanExpression> pushedConjuncts = Collections.newSetFromMap(new IdentityHashMap<>());

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
        final String idFilter = plan.extractIdFilter(whereClause, variable);
        // OPTIMIZATION: Extract WHERE predicates referencing only available variables for pushdown
        final BooleanExpression pushdownFilter = filterPushdown.extractPushdownFilter(whereClause, variable,
            boundVariables, matchVariables, pushedConjuncts);
        final MatchNodeStep matchStep = FilterPushdown.takePushdown(new MatchNodeStep(variable, nodePattern, context, idFilter,
            pushdownFilter), pushedConjuncts);

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

        // The path variable is tracked once the ShortestPathStep that binds it is planned: the endpoints' node steps run before
        // it, so a predicate on the path pushed into one of them read an unbound path and dropped every row (issue #11102)

        // For shortestPath, both endpoints must be matched first
        // Check both boundVariables (from previous MATCH clauses) and matchVariables (from earlier
        // patterns in this same MATCH clause) to avoid re-matching already-bound variables

        // Source node matching (if not already bound)
        if (!boundVariables.contains(sourceVar) && !matchVariables.contains(sourceVar)) {
          final String sourceIdFilter = plan.extractIdFilter(whereClause, sourceVar);
          final BooleanExpression sourcePushdown = filterPushdown.extractPushdownFilter(whereClause, sourceVar,
              boundVariables, matchVariables, pushedConjuncts);
          final MatchNodeStep sourceStep = FilterPushdown.takePushdown(new MatchNodeStep(sourceVar, sourceNode, context, sourceIdFilter,
              sourcePushdown), pushedConjuncts);
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
          final String targetIdFilter = plan.extractIdFilter(whereClause, targetVar);
          final BooleanExpression targetPushdown = filterPushdown.extractPushdownFilter(whereClause, targetVar,
              boundVariables, matchVariables, pushedConjuncts);
          final MatchNodeStep targetStep = FilterPushdown.takePushdown(new MatchNodeStep(targetVar, targetNode, context, targetIdFilter,
              targetPushdown), pushedConjuncts);
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
        if (pathVariable != null)
          matchVariables.add(pathVariable);

        if (isOptional && matchChainStart == null) {
          matchChainStart = shortestStep;
        }
      } else {
        // Issue #740: drive a single-hop pattern from the edge type's own index when the endpoints are
        // unselective and the edge carries a full-key equality filter, mirroring the SQL FETCH FROM INDEX
        // path. Only attempted as the leading (seed) step of a non-optional MATCH whose WHERE clause is
        // re-applied above, so the index seek is always a safe prefilter.
        if (!isOptional && currentStep == null) {
          final MatchEdgeByIndexStep edgeIndexSeed = tryBuildEdgeIndexScanSeed(matchClause, pathPatterns, patternIndex,
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
            // Target IS bound - reverse the traversal. Over an edge type declared unidirectional the reversed hop
            // reads the incoming side no vertex stores, which the step answers through the query's lookup
            // (issue #8625): one scan of the type instead of a scan of the source side for every bound target.
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
        final String sourceIdFilter = sourceAlreadyBound ? null : plan.extractIdFilter(whereClause, sourceVar);
        final BooleanExpression sourcePushdown = sourceAlreadyBound ? null :
            filterPushdown.extractPushdownFilter(whereClause, sourceVar, boundVariables, matchVariables, pushedConjuncts);
        final AbstractExecutionStep sourceStep;
        if (reversedFromIndexedAnchor && physicalPlan.getAnchor().getPropertyValue() instanceof InListValues) {
          // The seek decides none of the pushdown: it stays with the clause's filter
          final var anchor = physicalPlan.getAnchor();
          sourceStep = new IndexSeekStep(anchor.getVariable(), anchor.getIndex().getTypeName(),
              anchor.getPropertyName(), anchor.getPropertyValue(), anchor.getIndex().getIndexName(),
              anchor.getIndex().getPropertyNames(),
              anchor.getEstimatedCost(), anchor.getEstimatedCardinality(), context);
        } else
          sourceStep = FilterPushdown.takePushdown(new MatchNodeStep(sourceVar, sourceNode, context, sourceIdFilter, sourcePushdown),
              pushedConjuncts);

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
        final boolean[] hopNeedsEdgeTracking = hopEdges.computeHopEdgeTrackingNeeds(pathPatterns, patternIndex);

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

          // Issue #11699: a target pinned by an equality on every column of a UNIQUE index is one vertex at most. Seeking
          // it before the hop binds both ends, so the hop reads the edges joining the pair off the shorter edge list
          // instead of every edge of the source and a filter on the target. Only from a source that is itself one vertex
          // per row (bound already, or uniquely keyed too), where the seek costs one index lookup per row at most, and with
          // key values readable before the hop: a literal, a parameter, or a variable an earlier clause bound (an UNWIND or WITH
          // row, which the seek reads off the row it runs on). One naming a variable of this pattern has nothing to read yet
          final boolean seekTarget = !isOptional && !reversed && i == 0 && !relPattern.isVariableLength()
              && !(relPattern instanceof QuantifiedPathPattern) && !boundVariables.contains(effectiveTargetVar)
              && !matchVariables.contains(effectiveTargetVar) && (sourceAlreadyBound || isUniquelyKeyed(sourceNode))
              && isUniquelyKeyed(effectiveTargetNode) && readsOnlyEarlierClauses(effectiveTargetNode.getProperties(), boundVariables);

          // Track relationship and target variables for cross-MATCH uniqueness scoping.
          // Only add variables that are NEW to this MATCH clause — already-bound variables
          // (from previous MATCHes or WITH) should not be treated as new match variables,
          // otherwise OPTIONAL MATCH will incorrectly classify them when setting NULLs.
          if (relVar != null && !boundVariables.contains(relVar))
            matchVariables.add(relVar);
          if (!boundVariables.contains(effectiveTargetVar))
            matchVariables.add(effectiveTargetVar);
          if (seekTarget) {
            final MatchNodeStep targetSeek = new MatchNodeStep(effectiveTargetVar, effectiveTargetNode, context);
            targetSeek.setPrevious(currentStep);
            currentStep = targetSeek;
          }

          AbstractExecutionStep nextStep;
          if (relPattern instanceof QuantifiedPathPattern quantified) {
            // GQL Quantified Path Pattern, Phase B (issue #4531): a repeated inner sub-pattern that no
            // variable-length hop can express. Its inner variables surface here as group variables.
            for (final String groupVariable : quantified.getGroupVariables())
              if (!boundVariables.contains(groupVariable))
                matchVariables.add(groupVariable);
            nextStep = new QuantifiedPathStep(effectiveSourceVar, effectiveTargetVar, pathVariable,
                quantified, effectiveTargetNode, matchVariables,
                clauseRelVariables, context);
          } else if (relPattern.isVariableLength()) {
            // DFS, not BFS: DFS's active stack is bounded by maxHops regardless of branching
            // factor, while BFS's frontier queue must hold an entire level's children before it
            // can dequeue the first one - level-order expansion via a single FIFO queue offers no
            // way around that. A MATCH's result order is unspecified without ORDER BY, so this is
            // a pure implementation-strategy change, not a semantic one (#6097).
            final ExpandPathStep expandPath = new ExpandPathStep(effectiveSourceVar, pathVariable, relVar, effectiveTargetVar,
                relPattern, false, effectiveTargetNode, pathPattern.getEffectivePathMode(), matchVariables,
                clauseRelVariables, directionOverride, reversed, context);
            // Nothing downstream can tell two paths between the same pair apart: report each end vertex once (#10062)
            if (VariableLengthDistinctEndpoints.applies(statement, pathPattern, relPattern,
                pathPattern.getFirstNode().getVariable(), pathPattern.getLastNode().getVariable(), pathVariable))
              expandPath.withDistinctEndpoints();
            nextStep = expandPath;
          } else {
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

            // An IN hop over an edge type declared unidirectional reads the incoming side no vertex stores: the step
            // answers it through the query's lookup (issue #8625), so the hop is walked as written. The
            // relationship-uniqueness scope is published once the clause is complete.
            nextStep = new MatchRelationshipStep(effectiveSourceVar, relVar, effectiveTargetVar, relPattern,
                pathVariable, effectiveTargetNode, targetIdentityVars, matchVariables, clauseRelVariables,
                directionOverride, reversed, context);
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

    // Apply WHERE clause scoped to this MATCH: only what no node step of the clause decided already (issue #11102)
    final WhereClause residualWhere = FilterPushdown.residualWhere(matchClause, pushedConjuncts);
    if (residualWhere != null && currentStep != null) {
      final FilterPropertiesStep filterStep = new FilterPropertiesStep(residualWhere, context);

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
      rememberOptionalMatches(optionalStep, matchClause);

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
   * Adds the names a {@code CALL { }} subquery makes visible to the clauses that follow it: its own
   * {@code RETURN} items, which join the outer scope rather than replacing it. A {@code RETURN *} forwards the
   * subquery body's own variables rather than listing them here, so it contributes nothing. That costs the
   * push-down below and nothing else: the join against a variable the subquery already bound is enforced when
   * the following MATCH runs, against the value the row carries, not from this bookkeeping - a
   * {@code RETURN *} subquery answers exactly as the same query written with an explicit list, or with
   * {@code WITH}, which is what {@link
   * com.arcadedb.query.opencypher.CypherClauseScopedRelationshipUniquenessIssue7165Test} pins.
   */
  private static void collectSubqueryOutputVariables(final SubqueryClause subqueryClause,
      final Set<String> boundVariables) {
    if (subqueryClause.getReportStatusVariable() != null)
      boundVariables.add(subqueryClause.getReportStatusVariable());
    final ReturnClause returnClause = subqueryClause.getInnerStatement() != null ?
        subqueryClause.getInnerStatement().getReturnClause() : null;
    if (returnClause == null)
      return;
    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems())
      if (!item.isStar())
        boundVariables.add(item.getOutputName());
  }

  /**
   * Adds the names a {@code CALL} makes visible to the clauses that follow it: the YIELD aliases when the
   * clause lists them, otherwise the procedure's own declared output fields, which is what {@code YIELD *}
   * and a bare {@code CALL} put into the row. A name the registry does not know contributes nothing: it may
   * still run - an ArcadeDB SQL function reached through {@code CALL} resolves further down in
   * {@link com.arcadedb.query.opencypher.executor.steps.CallStep} - it just declares no output fields to read
   * here. What is missed is the push-down below, never a row: relationship uniqueness is scoped to the MATCH
   * clause's own variables and asks this set nothing.
   */
  private static void collectCallOutputVariables(final CallClause callClause, final Set<String> boundVariables) {
    if (callClause.hasYield() && !callClause.isYieldAll()) {
      for (final CallClause.YieldItem item : callClause.getYieldItems())
        boundVariables.add(item.getOutputName());
      return;
    }
    final CypherProcedure procedure = callClause.resolveProcedure();
    if (procedure == null)
      return;
    final List<String> yieldFields = procedure.getYieldFields();
    if (yieldFields != null)
      boundVariables.addAll(yieldFields);
  }

  /**
   * Builds a {@link MatchEdgeByIndexStep} seed for a single-hop pattern whose relationship type has an index whose key, or a
   * leading prefix of it, is covered by equality or {@code IN}-list predicates on the relationship (issue #740). Since issue
   * #11481 every spelling of the lookup qualifies, not only an anonymous-endpoint, single-type, {@code WHERE} one: the predicates
   * come from the MATCH's {@code WHERE} clause and from the relationship's inline property map, a union of types is one seek per
   * type, and an endpoint may carry labels, which the step checks on the vertices of each edge it finds instead of anchoring the
   * plan on a scan of the label (see {@link EdgeIndexSeedPlanner}). Returns {@code null} - deferring to the ordinary vertex-scan +
   * expansion plan - whenever a precondition is not met. On success it also registers the source, target and edge variables in
   * {@code matchVariables}, since the seed step binds all three.
   * <p>
   * Safety: the seek returns every edge whose leading key columns take one of the predicate values - a superset of the rows the
   * MATCH keeps, whether the key is complete or a prefix and whether a column is pinned to one value or to a list. The WHERE clause
   * is re-applied by the {@link FilterPropertiesStep} added above this step, and the inline maps, which nothing above re-applies, are
   * checked by the step itself, so the seek can never let through a row the unoptimised plan would have rejected.
   */
  private MatchEdgeByIndexStep tryBuildEdgeIndexScanSeed(final MatchClause matchClause,
      final List<PathPattern> pathPatterns, final int patternIndex, final Set<String> boundVariables,
      final Set<String> matchVariables, final CommandContext context) {
    final PathPattern pathPattern = pathPatterns.get(patternIndex);
    if (pathPattern.hasPathVariable() || pathPattern.getRelationshipCount() != 1)
      return null;

    final RelationshipPattern rel = pathPattern.getRelationship(0);
    if (rel instanceof QuantifiedPathPattern || rel.isVariableLength() || rel.hasWhereExpression()
        || rel.getPropertiesParameterName() != null || !rel.hasTypes())
      return null;

    // Only a directed hop maps unambiguously to the edge's out/in endpoints; BOTH is skipped.
    final Direction dir = rel.getDirection();
    if (dir != Direction.OUT && dir != Direction.IN)
      return null;

    // A named relationship must still be free; an anonymous one can be pinned by its inline map alone
    final String edgeVar = rel.getVariable();
    final boolean namedEdge = edgeVar != null && !edgeVar.isEmpty();
    if (namedEdge && (boundVariables.contains(edgeVar) || matchVariables.contains(edgeVar)))
      return null;

    // The seed is the first step of the plan: there is no row to evaluate an inline value that reads a variable against
    final Map<String, Object> relProperties = rel.hasProperties() ? rel.getProperties() : null;
    if (!EdgeIndexSeedPlanner.isRowIndependent(relProperties))
      return null;

    // The row-independent equality and IN-list predicates on the relationship. The WHERE clause belongs to this MATCH, so its
    // FilterPropertiesStep re-validates above the seed
    final BooleanExpression where = matchClause.hasWhereClause() && matchClause.getWhereClause() != null ?
        matchClause.getWhereClause().getConditionExpression() : null;
    final Map<String, Object> predicates = new LinkedHashMap<>();
    if (namedEdge)
      EdgeIndexSeedPlanner.collectWherePredicates(edgeVar, where, predicates);
    EdgeIndexSeedPlanner.collectInlinePredicates(relProperties, predicates);
    if (predicates.isEmpty())
      return null;

    final List<EdgeIndexSeedPlanner.TypeSeek> seeks = EdgeIndexSeedPlanner.planSeeks(context.getDatabase().getSchema(),
        rel.getTypes(), predicates);
    if (seeks == null)
      return null;

    boolean exact = true;
    for (final EdgeIndexSeedPlanner.TypeSeek seek : seeks)
      exact &= seek.exact();

    // Note that a variable repeated at both endpoints still constrains the hop; see MatchEdgeByIndexStep's class javadoc for why
    // the step, not this test, enforces it (issue #7008).
    final NodePattern srcNode = pathPattern.getFirstNode();
    final NodePattern tgtNode = pathPattern.getLastNode();
    if (!isSeedableEndpoint(srcNode, where, exact, boundVariables, matchVariables)
        || !isSeedableEndpoint(tgtNode, where, exact, boundVariables, matchVariables))
      return null;

    final String effectiveSourceVar = srcNode.getVariable() != null ? srcNode.getVariable()
        : ("  src" + anonymousVarCounter++);
    final String effectiveTargetVar = tgtNode.getVariable() != null ? tgtNode.getVariable()
        : ("  tgt" + anonymousVarCounter++);

    // An anonymous relationship gets an internal variable when another hop of this MATCH could bind the same edge, as the
    // ordinary hop builder gives it: the later hops' relationship uniqueness compares against what the clause binds, and a
    // relationship bound under no name was free to be bound again (issue #11585)
    final String seededEdgeVar = namedEdge ? edgeVar
        : hopEdges.computeHopEdgeTrackingNeeds(pathPatterns, patternIndex)[0] ? "  rel" + anonymousVarCounter++ : null;

    matchVariables.add(effectiveSourceVar);
    matchVariables.add(effectiveTargetVar);
    if (seededEdgeVar != null)
      matchVariables.add(seededEdgeVar);

    return new MatchEdgeByIndexStep(seeks, dir, effectiveSourceVar, seededEdgeVar, effectiveTargetVar,
        EdgeIndexSeedPlanner.EndpointFilter.of(srcNode), EdgeIndexSeedPlanner.EndpointFilter.of(tgtNode), relProperties, context);
  }

  /**
   * Whether the seed can bind an endpoint and check what the pattern asks of it by itself (issue #11481). Not when it is bound
   * already: the expansion from a bound vertex is the cheaper plan. Not when it carries a dynamic label, an inline {@code WHERE} or
   * a property map given as a whole parameter, which only a row or a pattern evaluator answers. Its labels are always checked by
   * the seek: anchoring on a scan of the label to then expand every edge is exactly what the index avoids. Its own equalities - an
   * inline map, or one in the {@code WHERE} clause - may be what an index of the label answers better, so they are left to the
   * ordinary plan unless the seek resolves at most one edge per value.
   */
  private boolean isSeedableEndpoint(final NodePattern node, final BooleanExpression where, final boolean exactSeek,
      final Set<String> boundVariables, final Set<String> matchVariables) {
    final String var = node.getVariable();
    if (var != null && (boundVariables.contains(var) || matchVariables.contains(var)))
      return false;
    if (node.hasDynamicLabels() || node.hasWhereExpression() || node.getPropertiesParameterName() != null)
      return false;
    if (node.hasProperties() && (!exactSeek || !EdgeIndexSeedPlanner.isRowIndependent(node.getProperties())))
      return false;
    if (!exactSeek && node.hasLabels() && var != null && where != null) {
      final Map<String, Object> pinned = new LinkedHashMap<>();
      EdgeIndexSeedPlanner.collectWherePredicates(var, where, pinned);
      return pinned.isEmpty();
    }
    return true;
  }

  /**
   * Whether a node pattern pins its node by an equality on every column of a UNIQUE index of its one label, so a seek of it
   * finds one vertex at most (issue #11699).
   */
  boolean isUniquelyKeyed(final NodePattern node) {
    return uniqueKeyIndexOf(node) != null;
  }

  /**
   * A UNIQUE index of the one label of a node pattern whose every column the pattern's property map pins by an equality, or null
   * when there is none (issues #11699, #11594).
   */
  private TypeIndex uniqueKeyIndexOf(final NodePattern node) {
    if (!node.hasProperties() || node.getPropertiesParameterName() != null || node.hasDynamicLabels() || !node.hasLabels()
        || node.getLabels().size() != 1)
      return null;
    final Schema schema = database.getSchema();
    if (!schema.existsType(node.getLabels().getFirst()))
      return null;
    for (final TypeIndex index : schema.getType(node.getLabels().getFirst()).getAllIndexes(true)) {
      final List<String> keys = index.isUnique() && index.isReadyForQueries() ? index.getPropertyNamesIfExactKeyLookup() : null;
      if (keys != null && node.getProperties().keySet().containsAll(keys))
        return index;
    }
    return null;
  }

  /**
   * The vertex a node pattern pins by an equality on every column of a UNIQUE index, as a key to look up when the count runs
   * (issue #11594), or null when there is no such index or a value of the key is not a literal or a parameter given a scalar
   * value. A null key is not a key: the index holds an entry per record whose key is null, while the equality matches none.
   */
  UniqueKeyAnchor uniqueKeyAnchorOf(final NodePattern node, final CommandContext context) {
    final TypeIndex index = uniqueKeyIndexOf(node);
    // a document or an edge type holding the key is no vertex: the label walk matches nothing there, as a node pattern must
    if (index != null && !(database.getSchema().getType(node.getLabels().getFirst()) instanceof VertexType))
      return null;
    final List<String> keys = index != null ? index.getPropertyNamesIfExactKeyLookup() : null;
    if (keys == null)
      return null;
    final Object[] key = new Object[keys.size()];
    for (int i = 0; i < key.length; i++) {
      final Object declared = node.getProperties().get(keys.get(i));
      if (!RowIndependentValues.isSeekable(declared))
        return null;
      final Object value = InlineProperties.resolve(declared, null, context);
      if (value == null || value instanceof Collection || value instanceof Map || value.getClass().isArray())
        return null;
      key[i] = TemporalUtil.toIndexKey(value);
    }
    return new UniqueKeyAnchor(index, key);
  }

  /**
   * Whether every value of a property map can be read before the pattern it belongs to is matched: a value an index seek takes
   * without a row, or one reading only variables {@code boundBefore} holds, which an earlier clause bound for the row the seek
   * runs on (issue #11699). Not a value two evaluations can disagree on, such as one calling {@code rand()}: the seek reads it
   * once and the hop's filter on the vertex found once more.
   */
  private static boolean readsOnlyEarlierClauses(final Map<String, Object> properties, final Set<String> boundBefore) {
    if (properties == null)
      return true;
    for (final Object value : properties.values()) {
      if (RowIndependentValues.isSeekable(value))
        continue;
      if (!(value instanceof Expression expression) || !RowIndependentValues.isDeterministic(expression))
        return false;
      final CypherReferencedVariables referenced = CypherReferencedVariables.of(expression);
      if (!referenced.isComplete() || !boundBefore.containsAll(referenced.getNames()))
        return false;
    }
    return true;
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
        || plan.isAnyEdgeTypeUnidirectional(relationship.getTypes()))
      return false;

    final String sourceVariable = pathPattern.getFirstNode().getVariable();
    final String targetVariable = pathPattern.getLastNode().getVariable();
    return targetVariable != null && !targetVariable.equals(sourceVariable)
        && targetVariable.equals(physicalPlan.getAnchor().getVariable());
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
   * Lets an OPTIONAL MATCH hand its matches to the next input rows that hold the same values for what it reads, instead of
   * running its chain again for each (issue #11083). Only in a statement that cannot write: nothing can then change what
   * the chain reads between two input rows.
   */
  private void rememberOptionalMatches(final OptionalMatchStep optionalStep, final MatchClause matchClause) {
    if (statement.isReadOnly())
      optionalStep.setMemoKey(DistinctSemiJoin.matchMemoKey(matchClause));
  }

  /**
   * A LIMIT of this statement. A {@code LIMIT 0} drains its input so the writes behind it run; a statement that cannot write
   * has none, so its {@code LIMIT 0} answers without running anything behind it (issue #11081).
   */
  LimitStep newLimitStep(final int limit, final CommandContext context) {
    return new LimitStep(limit, context).setDrainOnZeroLimit(!statement.isReadOnly());
  }

  /**
   * Legacy method for building execution steps (fixed order).
   * Used when clause order information is not available.
   * <p>
   * It has no {@code CALL} handling, and needs none: {@link com.arcadedb.query.opencypher.parser.StatementBuilder}
   * is the only thing that puts a {@code CallClause} on a statement and it always appends the matching entry to
   * {@code clausesInOrder}, so a statement carrying a CALL never reaches here - this method runs only when that
   * list is null or empty. Should that ever stop holding, a {@code case CALL:} added here has to register the
   * clause's YIELD names the way the ordered builder does (see {@link #collectCallOutputVariables}), or a
   * predicate reading one of them silently stops being pushed into the following MATCH's scan. Relationship
   * uniqueness is not at stake either way: its scope comes from the MATCH clause's own AST rather than from
   * what earlier clauses registered, which is the whole point of #7165.
   */
  private AbstractExecutionStep buildExecutionStepsLegacy(final CommandContext context) {
    AbstractExecutionStep currentStep = null;

    // Get function factory from evaluator for steps that need it
    final CypherFunctionFactory functionFactory = expressionEvaluator != null ?
        expressionEvaluator.getFunctionFactory() : null;

    // OPTIMIZATION: the O(1) Type.count() push-down and the CSR one for chain/star/triangle/pair-join patterns.
    final AbstractExecutionStep countStep = plan.tryCountPushDown(context, false);
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

        // Both sites that build this step produce the same thing - one dummy row for a statement whose
        // expressions stand on their own - so they deliberately share a name and aggregate together in the
        // profiler's per-step statistics.
        @Override
        public String getName() {
          return "DummyRowStep";
        }

        @Override
        public String getType() {
          return getName();
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
          // By reference, never copied - see the note in buildMatchStep().
          final Set<String> matchVariables = new HashSet<>();
          final Set<String> clauseRelVariables = HopEdgeTrackingAnalyzer.clauseRelationshipVariables(matchClause);
          final boolean isOptional = matchClause.isOptional();
          // The WHERE conjuncts a node step of this clause already decides (issue #11102)
          final Set<BooleanExpression> pushedConjuncts = Collections.newSetFromMap(new IdentityHashMap<>());

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

              // The path variable is tracked once its ShortestPathStep is planned: see the ordered builder above (issue #11102)

              // Check both legacyBoundVariables (from previous MATCH clauses) and matchVariables (from earlier
              // patterns in this same MATCH clause) to avoid re-matching already-bound variables

              // Source node matching (if not already bound)
              if (!legacyBoundVariables.contains(sourceVar) && !matchVariables.contains(sourceVar)) {
                final WhereClause matchWhere = matchClause.hasWhereClause() ? matchClause.getWhereClause() :
                    statement.getWhereClause();
                final String sourceIdFilter = plan.extractIdFilter(matchWhere, sourceVar);
                final BooleanExpression sourcePushdown = filterPushdown.extractPushdownFilter(matchWhere, sourceVar,
                    legacyBoundVariables, matchVariables, pushedConjuncts);
                final MatchNodeStep sourceStep = FilterPushdown.takePushdown(new MatchNodeStep(sourceVar, sourceNode, context, sourceIdFilter,
                    sourcePushdown), pushedConjuncts);
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
                final String targetIdFilter = plan.extractIdFilter(matchWhere, targetVar);
                final BooleanExpression targetPushdown = filterPushdown.extractPushdownFilter(matchWhere, targetVar,
                    legacyBoundVariables, matchVariables, pushedConjuncts);
                final MatchNodeStep targetStep = FilterPushdown.takePushdown(new MatchNodeStep(targetVar, targetNode, context, targetIdFilter,
                    targetPushdown), pushedConjuncts);
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
              if (pathVariable != null)
                matchVariables.add(pathVariable);

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
              final String idFilter = plan.extractIdFilter(matchWhere, variable);
              // OPTIMIZATION: Extract WHERE predicates for inline pushdown
              final BooleanExpression pushdownFilter = filterPushdown.extractPushdownFilter(matchWhere, variable,
                  legacyBoundVariables, matchVariables, pushedConjuncts);
              final MatchNodeStep matchStep = FilterPushdown.takePushdown(new MatchNodeStep(variable, nodePattern, context, idFilter,
                  pushdownFilter), pushedConjuncts);

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
                final String sourceIdFilter = plan.extractIdFilter(matchWhere, sourceVar);
                // OPTIMIZATION: Extract WHERE predicates for inline pushdown
                final BooleanExpression sourcePushdown = filterPushdown.extractPushdownFilter(matchWhere, sourceVar,
                    legacyBoundVariables, matchVariables, pushedConjuncts);

                // Start with source node (or chain if we have previous patterns)
                final MatchNodeStep sourceStep = FilterPushdown.takePushdown(new MatchNodeStep(sourceVar, sourceNode, context, sourceIdFilter,
                    sourcePushdown), pushedConjuncts);

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
              final boolean[] hopNeedsEdgeTrackingLegacy = hopEdges.computeHopEdgeTrackingNeeds(pathPatterns, patternIndex);

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
                      quantified, targetNode, matchVariables, clauseRelVariables,
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
          final WhereClause legacyResidualWhere = FilterPushdown.residualWhere(matchClause, pushedConjuncts);
          if (legacyResidualWhere != null && currentStep != null) {
            final FilterPropertiesStep filterStep = new FilterPropertiesStep(legacyResidualWhere, context);

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
            rememberOptionalMatches(optionalStep, matchClause);

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

      currentStep = addWithOrderSkipLimit(withClause, currentStep, context, functionFactory);
    }

    // The read/write barrier analysis of buildExecutionStepsWithOrder(), over the flat clause lists this
    // method works from (issue #7171). It only runs for a statement with no tracked clause order, which is
    // single-segment by construction, so the whole MATCH list is the read footprint for both writes below.
    // legacyBoundVariables already holds every variable the MATCH clauses above bound, which is what tells a
    // CREATE/MERGE node that names an existing entity from one that creates a new one.
    final CypherEagernessAnalyzer eagerness = CypherEagernessAnalyzer.forStatement(statement);
    for (final MatchClause matchClause : statement.getMatchClauses())
      eagerness.observeRead(matchClause);

    // Step 3: MERGE clause - find or create pattern
    if (statement.getMergeClause() != null) {
      if (currentStep != null && eagerness.needsBarrier(statement.getMergeClause(), legacyBoundVariables))
        currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
      final MergeStep mergeStep = new MergeStep(
          statement.getMergeClause(), context, functionFactory);
      // MERGE is typically standalone, but can be chained
      if (currentStep != null) {
        mergeStep.setPrevious(currentStep);
        // run once per row: a later write of what it matches on needs the barrier (issue #10889)
        eagerness.observeMergeRead(statement.getMergeClause());
      }
      currentStep = mergeStep;
      eagerness.observeWriteClause();
    }

    // Step 4: CREATE clause - create vertices/edges
    if (statement.getCreateClause() != null && !statement.getCreateClause().isEmpty()) {
      if (currentStep != null && eagerness.needsBarrier(statement.getCreateClause(), legacyBoundVariables))
        currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
      final CreateStep createStep = new CreateStep(statement.getCreateClause(), context, functionFactory);
      if (currentStep != null) {
        // Chained CREATE (after MATCH/WHERE)
        createStep.setPrevious(currentStep);
      }
      // else: Standalone CREATE (no previous step)
      currentStep = createStep;
      eagerness.observeWriteClause();
    }

    // Step 5: SET clause - update properties
    final boolean setAfterMerge = eagerness.needsBarrierAfterMerge(statement.getSetClause());
    if (statement.getSetClause() != null && !statement.getSetClause().isEmpty() && currentStep != null
        && (setAfterMerge || !EagerBarriers.absorbsSet(currentStep, statement.getSetClause()))) {
      if (setAfterMerge || eagerness.needsBarrier(statement.getSetClause()))
        currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
      final SetStep setStep = new SetStep(
          statement.getSetClause(), context, functionFactory);
      setStep.setPrevious(currentStep);
      currentStep = setStep;
      eagerness.observeWriteClause();
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
          statement.getDeleteClause(), context, EagerBarriers.matchClausesNeedEagerDelete(statement.getMatchClauses()));
      deleteStep.setPrevious(currentStep);
      currentStep = deleteStep;
      eagerness.observeWriteClause();
    }

    // Step 6a: REMOVE clauses - remove properties
    for (final RemoveClause removeClause : statement.getRemoveClauses()) {
      if (!removeClause.isEmpty() && currentStep != null) {
        if (eagerness.needsBarrier(removeClause) || eagerness.needsBarrierAfterMerge(removeClause))
          currentStep = EagerBarriers.withEagerBarrier(currentStep, context, eagerness);
        final RemoveStep removeStep = new RemoveStep(removeClause, context, functionFactory);
        removeStep.setPrevious(currentStep);
        currentStep = removeStep;
        eagerness.observeWriteClause();
      }
    }

    // Step 7: RETURN clause - project results or aggregate
    if (statement.getReturnClause() != null && currentStep != null) {
      // Try count-edges optimization: MATCH (p)-[:TYPE]->(x) RETURN expr, count(x) AS cnt
      final AbstractExecutionStep countOpt = aggregationShortcuts.tryOptimizeMatchCountReturn(
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
      currentStep = EagerBarriers.withLimitBarrier(currentStep, context, eagerness, functionFactory, statement);
      final Integer limitVal = new ExpressionEvaluator(functionFactory).evaluateSkipLimit(statement.getLimit(),
          new ResultInternal(),
          context);
      final LimitStep limitStep = newLimitStep(limitVal, context);
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
}
