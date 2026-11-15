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

import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.DeleteClause;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.ForeachClause;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.ast.SubqueryClause;
import com.arcadedb.query.opencypher.ast.UnionStatement;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.executor.steps.CreateStep;
import com.arcadedb.query.opencypher.executor.steps.DeleteStep;
import com.arcadedb.query.opencypher.executor.steps.EagerStep;
import com.arcadedb.query.opencypher.executor.steps.LimitStep;
import com.arcadedb.query.opencypher.executor.steps.MatchNodeStep;
import com.arcadedb.query.opencypher.executor.steps.MergeStep;
import com.arcadedb.query.opencypher.executor.steps.WithStep;
import com.arcadedb.query.opencypher.planner.CypherEagernessAnalyzer;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultInternal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Places the barriers that make a statement's reads finish before its writes are seen: the eager step before a DELETE,
 * a SET or a LIMIT that would otherwise observe rows half written, and the checks deciding whether one is needed. It
 * also tracks which variables a DELETE could invalidate for the MATCH clauses read before it: the variables a segment of
 * MATCH clauses binds, how a WITH renames them, and whether the targets of a DELETE or of a FOREACH body reach one.
 * <p>
 * Split out of {@link CypherExecutionPlan}, whose step builders call it while chaining the steps of each clause.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class EagerBarriers {
  private EagerBarriers() {
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
   * True when the body of a {@code CALL { }} subquery deletes anything: a DELETE clause, a FOREACH that holds one, or a nested
   * subquery that does, in any branch of a UNION.
   */
  static boolean subqueryBodyDeletes(final CypherStatement body) {
    if (body == null)
      return false;
    if (body instanceof UnionStatement union) {
      for (final CypherStatement branch : union.getQueries())
        if (subqueryBodyDeletes(branch))
          return true;
      return false;
    }
    final List<ClauseEntry> clauses = body.getClausesInOrder();
    // a body that kept no clause order is classified by the flag the parser set
    if (clauses == null || clauses.isEmpty())
      return body.hasDelete();
    for (final ClauseEntry bodyClause : clauses)
      switch (bodyClause.getType()) {
      case DELETE:
        return true;
      case FOREACH:
        if (((ForeachClause) bodyClause.getClause()).containsDelete())
          return true;
        break;
      case SUBQUERY:
        if (subqueryBodyDeletes(((SubqueryClause) bodyClause.getClause()).getInnerStatement()))
          return true;
        break;
      default:
        break;
      }
    return false;
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
  static boolean matchClausesNeedEagerDelete(final List<MatchClause> matchClauses) {
    return matchClausesHaveDisconnectedPatterns(matchClauses)
        || MatchClause.hasVariableLengthRelationships(matchClauses);
  }

  /**
   * Hands a {@code SET} to the {@code CREATE} or {@code MERGE} step right before it when that step can write it with
   * the node it creates, so a new node is saved once instead of twice (issue #8735).
   *
   * @return true when the step applies the clause and no step is to be added for it
   */
  static boolean absorbsSet(final AbstractExecutionStep previous, final SetClause setClause) {
    if (previous instanceof MergeStep mergeStep)
      return mergeStep.absorbSet(setClause);
    if (previous instanceof CreateStep createStep)
      return createStep.absorbSet(setClause);
    return false;
  }

  /**
   * Inserts the eager read/write barrier of issue #7171 ahead of a write clause: everything the pipeline has
   * read is drained into memory before the first row reaches the write, so no enumeration is still open while
   * the write adds entities it could match. Where the barrier is needed is decided by
   * {@link CypherEagernessAnalyzer}, which keeps it off every shape whose writes cannot feed a read.
   */
  static AbstractExecutionStep withEagerBarrier(final AbstractExecutionStep currentStep,
      final CommandContext context, final CypherEagernessAnalyzer eagerness) {
    return withEagerBarrier(currentStep, context, eagerness, -1);
  }

  static AbstractExecutionStep withEagerBarrier(final AbstractExecutionStep currentStep,
      final CommandContext context, final CypherEagernessAnalyzer eagerness, final long keepFirst) {
    if (currentStep == null)
      return null;
    final EagerStep eagerStep = new EagerStep(context, keepFirst);
    eagerStep.setPrevious(currentStep);
    // The barrier closes every enumeration behind it, so the writes that follow it need no second one.
    eagerness.observeBarrier();
    return eagerStep;
  }

  /**
   * Plants the barrier a {@code LIMIT} needs when a write is pending ahead of it, and does nothing otherwise: a LIMIT stops
   * pulling, so without it the write would run only for the rows the pull-model batches had already carried past it (issues
   * #8826, #8827). When no step behind the barrier can drop a row (no WHERE, no DISTINCT) the barrier keeps only the first
   * {@code skip + limit} rows and discards the rest after the writes ran for them, so the memory stays O(limit). A WITH that
   * filters (WHERE) or de-duplicates (DISTINCT) after the barrier, or a SKIP that already ran before it, keeps more rows, up to
   * every row, bounded by the operation heap limit.
   */
  static AbstractExecutionStep withLimitBarrier(final AbstractExecutionStep currentStep, final CommandContext context,
      final CypherEagernessAnalyzer eagerness, final CypherFunctionFactory functionFactory, final WithClause withClause) {
    if (currentStep == null || withClause.getLimit() == null
        || !eagerness.needsBarrierBeforeLimit(withClause.getOrderByClause() != null, withClause.hasAggregations()))
      return currentStep;
    final boolean dropsRows = withClause.getWhereClause() != null || withClause.isDistinct();
    return withEagerBarrier(currentStep, context, eagerness,
        keepFirst(functionFactory, context, dropsRows, withClause.getSkip(), withClause.getLimit()));
  }

  static AbstractExecutionStep withLimitBarrier(final AbstractExecutionStep currentStep, final CommandContext context,
      final CypherEagernessAnalyzer eagerness, final CypherFunctionFactory functionFactory, final CypherStatement statement) {
    final ReturnClause returnClause = statement.getReturnClause();
    if (currentStep == null || statement.getLimit() == null || !eagerness.needsBarrierBeforeLimit(
        statement.getOrderByClause() != null, returnClause != null && returnClause.hasAggregations()))
      return currentStep;
    final boolean dropsRows = returnClause != null && returnClause.isDistinct();
    return withEagerBarrier(currentStep, context, eagerness,
        keepFirst(functionFactory, context, dropsRows, statement.getSkip(), statement.getLimit()));
  }

  /**
   * A {@code WITH ... LIMIT 0} reads nothing, so it never pulls the barrier planted behind an earlier LIMIT; when a write
   * precedes it, it drains its input instead so those writes still run. A read-only query keeps the free LIMIT 0.
   */
  static void markDrainOnZeroLimit(final AbstractExecutionStep step, final WithClause withClause,
      final CypherEagernessAnalyzer eagerness) {
    if (step instanceof WithStep withStep && withClause.getLimit() != null && eagerness.hasObservedWrite())
      withStep.setDrainOnZeroLimit(true);
  }

  private static long keepFirst(final CypherFunctionFactory functionFactory, final CommandContext context, final boolean dropsRows,
      final Expression skip, final Expression limit) {
    if (dropsRows)
      return -1;
    // evaluated again by the LimitStep / WithStep: both evaluations must agree, which holds for the literals and parameters allowed
    final ExpressionEvaluator evaluator = new ExpressionEvaluator(functionFactory);
    final long limitVal = evaluator.evaluateSkipLimit(limit, new ResultInternal(), context);
    final long skipVal = skip != null ? evaluator.evaluateSkipLimit(skip, new ResultInternal(), context) : 0L;
    return limitVal + skipVal;
  }

  /**
   * Adds every node and relationship variable a MATCH binds to {@code target}. Used to tell a CREATE/MERGE node
   * that names an already-matched entity (a reference) from one that creates a new entity, which is what stops
   * {@code MATCH (a:A), (b:B) MERGE (a)-[:R]->(b)} from being read as creating unlabelled vertices.
   */
  static void collectPatternVariables(final MatchClause matchClause, final Set<String> target) {
    collectPatternVariables(matchClause.getPathPatterns(), target);
  }

  /**
   * Adds every node, relationship and path variable of the given patterns to {@code target}: the names a MATCH, a
   * CREATE or a MERGE leaves bound in the row.
   */
  static void collectPatternVariables(final List<PathPattern> pathPatterns, final Set<String> target) {
    for (final PathPattern pathPattern : pathPatterns) {
      for (final NodePattern node : pathPattern.getNodes())
        if (node.getVariable() != null)
          target.add(node.getVariable());
      for (final RelationshipPattern relationship : pathPattern.getRelationships())
        if (relationship.getVariable() != null)
          target.add(relationship.getVariable());
      if (pathPattern.hasPathVariable())
        target.add(pathPattern.getPathVariable());
    }
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
  static void closeMatchSegment(final List<MatchClause> currentSegmentMatchClauses,
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
  static void propagateTaintThroughRenames(final WithClause withClause,
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
  static boolean deleteMayTargetTaintedVariable(final List<String> deleteTargets,
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
  static List<String> collectForeachDeleteTargetVariables(final ForeachClause foreachClause) {
    final List<String> targets = new ArrayList<>();
    collectForeachDeleteTargetVariables(foreachClause, targets);
    return targets;
  }

  static void collectForeachDeleteTargetVariables(final ForeachClause foreachClause, final List<String> targets) {
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
}
