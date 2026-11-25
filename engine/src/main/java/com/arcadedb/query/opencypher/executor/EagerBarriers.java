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
import com.arcadedb.query.opencypher.ast.UnwindClause;
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
import java.util.HashSet;
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
    return subqueryBodyDeletes(body, false);
  }

  /**
   * True when the body of a {@code CALL { }} subquery holds a {@code DETACH DELETE}, in the places {@link #subqueryBodyDeletes}
   * looks: the one delete that removes relationships besides the entity it names (issue #12254, see
   * {@link #detachDeleteNeedsEagerRead}).
   */
  static boolean subqueryBodyDetachDeletes(final CypherStatement body) {
    return subqueryBodyDeletes(body, true);
  }

  private static boolean subqueryBodyDeletes(final CypherStatement body, final boolean detachOnly) {
    if (body == null)
      return false;
    if (body instanceof UnionStatement union) {
      for (final CypherStatement branch : union.getQueries())
        if (subqueryBodyDeletes(branch, detachOnly))
          return true;
      return false;
    }
    final List<ClauseEntry> clauses = body.getClausesInOrder();
    // a body that kept no clause order is classified by the flag the parser set
    if (clauses == null || clauses.isEmpty()) {
      if (!detachOnly)
        return body.hasDelete();
      final DeleteClause delete = body.getDeleteClause();
      return body.hasDelete() && (delete == null || delete.isDetach());
    }
    for (final ClauseEntry bodyClause : clauses)
      switch (bodyClause.getType()) {
      case DELETE:
        if (!detachOnly || ((DeleteClause) bodyClause.getClause()).isDetach())
          return true;
        break;
      case FOREACH:
        final ForeachClause foreach = (ForeachClause) bodyClause.getClause();
        if (detachOnly ? foreach.containsDetachDelete() : foreach.containsDelete())
          return true;
        break;
      case SUBQUERY:
        if (subqueryBodyDeletes(((SubqueryClause) bodyClause.getClause()).getInnerStatement(), detachOnly))
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
   * True when a {@code DETACH DELETE} fed by the given MATCH clause(s), or by the rows of an earlier segment whose walk is
   * still open, must read its whole upstream row set before applying the first deletion (issue #12254).
   * <p>
   * A relationship of a fixed-length pattern is expanded by walking the edge list of the node it starts from while the rows
   * of that node are still being produced. A DETACH DELETE removes every relationship of the node it deletes, so the first
   * row's delete cuts the list the walk is still reading, the walk ends early and the rows it would have produced are
   * silently lost: {@code MATCH (a)-[r]->(b) DETACH DELETE a, b} over 500 relationships deleted 166 nodes and answered 200,
   * and every other write clause of the statement ran for those rows only. Neo4j reads the MATCH to completion first (its
   * Eager operator). The disconnected and variable-length shapes of {@link #matchClausesNeedEagerDelete} are the same hazard
   * reached by a cross join and by a traverser's open cursors.
   * <p>
   * Segment-wide on purpose, like {@link #matchClausesNeedEagerDelete}, and not narrowed to the variables the pattern binds:
   * the node a DETACH removes reaches the walk through its relationships, not through the name the DELETE spells, and the
   * entity arrives under any name a WITH, an UNWIND, a FOREACH or a CALL subquery gives it, or under none ({@code WITH
   * endNode(r) AS b}, {@code nodes(p)}). One carried flag ({@code walkedBefore}) covers all of them where a set of names
   * would need a propagation rule per clause. What it costs is the rows held in memory for a DETACH DELETE of a node that
   * names none of the pattern's variables, which a relationship pattern before it makes rare.
   * <p>
   * What stays streaming: a DELETE without DETACH (it refuses a node that still has a relationship, until the last row), and
   * a DETACH DELETE whose every target is a relationship variable of the segment's own patterns, which removes only the
   * relationship the walk is standing on, the same thing a plain {@code DELETE r} does. It holds only while no earlier walk is
   * open: a relationship the segment deletes can be an entry of an edge list an earlier segment is still reading ({@code
   * MATCH (a)-[]->() WITH a MATCH (a)-[s]->() DETACH DELETE s}), so it is read first then, and so is one carried across a WITH,
   * which is no longer known as a relationship variable. What a streaming delete of a relationship does not cover, and a plain
   * {@code DELETE r} does not either, is a later expansion of the same edge list that starts after the delete: that is
   * {@link #deleteNeedsEagerReRead} (issue #12521).
   *
   * @param segmentMatchClauses the MATCH clause(s) of the segment feeding the DELETE (the ones since the last WITH)
   * @param walkedBefore        true when an earlier segment matched a relationship pattern and no aggregating WITH drained it
   *                            since: see {@link #relationshipsWalkedAfter}
   */
  static boolean detachDeleteNeedsEagerRead(final DeleteClause deleteClause, final List<MatchClause> segmentMatchClauses,
      final boolean walkedBefore) {
    if (!deleteClause.isDetach() || deleteClause.isEmpty())
      return false;
    if (walkedBefore)
      return true;
    return MatchClause.hasRelationshipPatterns(segmentMatchClauses)
        && !deletesOnlyMatchedRelationships(deleteClause, segmentMatchClauses);
  }

  /**
   * True when a {@code DELETE} or {@code DETACH DELETE} fed by the given MATCH clause(s) must read its whole upstream row set
   * before applying the first deletion, because a relationship it deletes can be read again by an expansion that starts after
   * the delete (issue #12521): see {@link #relationshipsMayBeReadAgain}.
   * <p>
   * The delete is judged by what it may remove, not by the names it spells: a DELETE whose every target is a node variable of
   * the segment's own patterns removes no relationship (a node that still has one is refused, until the last row), so it
   * streams as it did. Anything else may remove one, a relationship variable, an expression, a variable an earlier WITH renamed,
   * a path. A {@code DETACH DELETE} of a node removes the relationships of the node, and is read first by
   * {@link #detachDeleteNeedsEagerRead} whenever the segment walks a relationship.
   *
   * @param segmentMatchClauses the MATCH clause(s) of the segment feeding the DELETE (the ones since the last WITH)
   * @param segmentRowSources   the UNWIND, LOAD CSV, CALL and CALL subquery clauses of that segment
   * @param rowsMayRepeatBefore see {@link #rowsMayRepeatAfter}
   */
  static boolean deleteNeedsEagerReRead(final DeleteClause deleteClause, final List<MatchClause> segmentMatchClauses,
      final List<ClauseEntry> segmentRowSources, final boolean rowsMayRepeatBefore) {
    if (deleteClause.isEmpty())
      return false;
    if (!deleteClause.isDetach() && deletesOnlyMatchedNodes(deleteClause, segmentMatchClauses))
      return false;
    return relationshipsMayBeReadAgain(segmentMatchClauses, segmentRowSources, rowsMayRepeatBefore);
  }

  /**
   * True when the relationships the given MATCH clause(s) bind can be reached by more than one row, so that deleting one for the
   * first row it reaches takes it away from the expansion that reaches it again, which then answers fewer rows than the MATCH
   * has (issue #12521, the relationship form of #12254): {@code MATCH (a {k:0}) UNWIND range(1, 500) AS i MATCH (a)-[r]->(b)
   * DELETE r RETURN i} answered 100 rows, one pull batch, and every write clause after the DELETE ran for those only. Neo4j
   * reads the rows of the MATCH to completion first (its Eager operator).
   * <p>
   * What deleting the relationship the walk stands on does not disturb is that same walk (#8997): the expansion of one edge list
   * goes on past the entries behind it. What it disturbs is an expansion that STARTS after the delete and reads an edge list
   * the delete took the relationship out of. One of these holds when it does:
   * <ul>
   *   <li>more than one relationship pattern in the segment: the second expands for every row of the first, and a relationship
   *       of the first is an entry of the lists the second walks (or the same one reached through another node);</li>
   *   <li>a relationship without a direction: it is an entry of the edge list of each of its two ends, and the row of the
   *       second end is produced after the first one deleted it;</li>
   *   <li>an UNWIND, LOAD CSV, CALL or CALL subquery in the segment, or rows of an earlier segment that may repeat
   *       ({@link #rowsMayRepeatAfter}): the expansion starts again for each of their rows.</li>
   * </ul>
   * What stays streaming is the segment that is one relationship pattern of one direction, fed by the scan the MATCH starts from
   * and by nothing else: it reaches each relationship once, so it holds no row in memory, which is what a bulk
   * {@code MATCH ()-[r:T]->() DELETE r} is. The rows an eager read holds are bounded by the operation heap limit, as for the
   * other eager shapes.
   * <p>
   * Segment-wide on purpose, like {@link #matchClausesNeedEagerDelete}: not narrowed to the clause order inside the segment (an
   * UNWIND after the pattern repeats rows that already hold the relationship and starts no expansion, but costs only the memory
   * of the rows to be read first), nor to the variables the DELETE spells, which reach the graph under any name a WITH, an UNWIND
   * or a FOREACH gives them.
   *
   * @param segmentMatchClauses the MATCH clause(s) of the segment feeding the write clause (the ones since the last WITH)
   * @param segmentRowSources   the UNWIND, LOAD CSV, CALL and CALL subquery clauses of that segment
   * @param rowsMayRepeatBefore see {@link #rowsMayRepeatAfter}
   */
  static boolean relationshipsMayBeReadAgain(final List<MatchClause> segmentMatchClauses, final List<ClauseEntry> segmentRowSources,
      final boolean rowsMayRepeatBefore) {
    if (!MatchClause.hasRelationshipPatterns(segmentMatchClauses))
      return false;
    return rowsMayRepeatBefore || !segmentRowSources.isEmpty() || MatchClause.countRelationshipPatterns(segmentMatchClauses) > 1
        || MatchClause.hasUndirectedRelationships(segmentMatchClauses);
  }

  /**
   * Carries across a WITH that closes the segment {@code closingSegment} whether the rows it hands the next segment may repeat:
   * a segment that produced rows (a MATCH, an UNWIND, a LOAD CSV, a CALL, a CALL subquery) hands them one at a time, so a
   * relationship pattern behind the WITH expands for each of them, and the same node can be in more than one of them
   * ({@code MATCH (n) WITH n.group AS g MATCH (m {group: g})-[r]->()}), which reads the same edge list again (issue #12521). Only
   * the rows the statement starts from do not repeat: a WITH over no clause that produced rows (one that starts the statement),
   * or one that aggregates without a grouping key and answers one row whatever it read. Call it before
   * {@link #closeMatchSegment} clears the segment.
   */
  static boolean rowsMayRepeatAfter(final WithClause withClause, final List<MatchClause> closingSegment,
      final List<ClauseEntry> closingRowSources, final boolean repeatBefore) {
    if (withClause.hasAggregations() && !withClause.hasNonAggregations())
      return false;
    return repeatBefore || !closingSegment.isEmpty() || !closingRowSources.isEmpty();
  }

  /**
   * True when a relationship pattern of the open segment, or of an earlier one whose walk is still open, feeds the rows a write
   * clause is about to see: the precondition of {@link #detachDeleteNeedsEagerRead}, asked on its own by a FOREACH body and a
   * CALL subquery body, which delete per row behind the same walk.
   */
  static boolean relationshipsWalked(final List<MatchClause> segmentMatchClauses, final boolean walkedBefore) {
    return walkedBefore || MatchClause.hasRelationshipPatterns(segmentMatchClauses);
  }

  /**
   * Carries the walk of the relationship patterns across a WITH that closes the segment {@code closingSegment}: a WITH that
   * forwards rows one at a time leaves the walk behind it open, and a WITH that aggregates or sorts reads it to the end before
   * it emits its first row, which closes it for everything read before it (the same two rules as the #7171 barrier). Call it
   * before {@link #closeMatchSegment} clears the segment.
   */
  static boolean relationshipsWalkedAfter(final WithClause withClause, final List<MatchClause> closingSegment,
      final boolean walkedBefore) {
    return !withClause.hasAggregations() && withClause.getOrderByClause() == null
        && relationshipsWalked(closingSegment, walkedBefore);
  }

  /** Whether every target of the DELETE is, by its plain name, a relationship variable bound by the given MATCH clause(s). */
  private static boolean deletesOnlyMatchedRelationships(final DeleteClause deleteClause, final List<MatchClause> matchClauses) {
    return deletesOnlyMatched(deleteClause, matchClauses, true);
  }

  /** Whether every target of the DELETE is, by its plain name, a node variable bound by the given MATCH clause(s). */
  private static boolean deletesOnlyMatchedNodes(final DeleteClause deleteClause, final List<MatchClause> matchClauses) {
    return deletesOnlyMatched(deleteClause, matchClauses, false);
  }

  private static boolean deletesOnlyMatched(final DeleteClause deleteClause, final List<MatchClause> matchClauses,
      final boolean relationships) {
    if (matchClauses == null)
      return false;
    final List<String> targets = deleteClause.getVariables();
    final List<Expression> expressions = deleteClause.getExpressions();
    for (int i = 0; i < targets.size(); i++) {
      final Expression expression = expressions != null && i < expressions.size() ? expressions.get(i) : null;
      final String name;
      if (expression instanceof VariableExpression variable)
        name = variable.getVariableName();
      else if (expression == null && isPlainIdentifier(targets.get(i)))
        name = targets.get(i);
      else
        return false;
      if (!(relationships ? bindsRelationship(matchClauses, name) : bindsNode(matchClauses, name)))
        return false;
    }
    return true;
  }

  private static boolean bindsNode(final List<MatchClause> matchClauses, final String variable) {
    for (final MatchClause match : matchClauses)
      for (final PathPattern path : match.getPathPatterns())
        for (final NodePattern node : path.getNodes())
          if (variable.equals(node.getVariable()))
            return true;
    return false;
  }

  private static boolean bindsRelationship(final List<MatchClause> matchClauses, final String variable) {
    for (final MatchClause match : matchClauses)
      for (final PathPattern path : match.getPathPatterns())
        for (final RelationshipPattern relationship : path.getRelationships())
          if (variable.equals(relationship.getVariable()))
            return true;
    return false;
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
  static void closeMatchSegment(final List<MatchClause> currentSegmentMatchClauses, final List<ClauseEntry> currentSegmentBindings,
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
    // An UNWIND or a CALL subquery of the segment bound its names before the segment's MATCH variables were tainted, so
    // they are carried over now, in the order the clauses ran (issue #12323): MATCH (n), (o) UNWIND [o] AS m WITH n, m
    // DETACH DELETE m is as hazardous as deleting o
    for (final ClauseEntry binding : currentSegmentBindings)
      if (binding.getType() == ClauseEntry.ClauseType.UNWIND)
        propagateTaintThroughUnwind(binding.getTypedClause(), disconnectedTaintedVariables);
      else if (binding.getType() == ClauseEntry.ClauseType.SUBQUERY)
        propagateTaintThroughSubquery(binding.getTypedClause(), disconnectedTaintedVariables);
    currentSegmentBindings.clear();
  }

  /**
   * Propagates taint through a {@code WITH ... AS alias}: a rename doesn't change how rows flow any more than a same-name
   * passthrough does (see {@link #closeMatchSegment}), so a later DELETE of the alias is exactly as hazardous as deleting the
   * original variable would have been.
   * <p>
   * The entity can also reach the alias inside a value built from it ({@code WITH [n] AS l}, {@code WITH {v: n} AS m},
   * {@code WITH l[0] AS m}, issue #12323), so every alias of an expression that reads a tainted variable is tainted, not only
   * a bare reference. An alias that is no longer an entity ({@code WITH n.id AS m}) is tainted as well; that costs nothing,
   * since a DELETE of it finds no node or relationship to stream behind the MATCH.
   */
  static void propagateTaintThroughRenames(final WithClause withClause,
      final Set<String> disconnectedTaintedVariables) {
    if (disconnectedTaintedVariables.isEmpty())
      return;
    final List<String> tainted = new ArrayList<>();
    for (final ReturnClause.ReturnItem item : withClause.getItems())
      if (!item.isStar() && item.getAlias() != null
          && CypherVariableUsage.expressionReferencesAnyVariable(item.getExpression(), disconnectedTaintedVariables))
        tainted.add(item.getAlias());
    // added after the scan: the items of one WITH all read the names in scope before it
    disconnectedTaintedVariables.addAll(tainted);
  }

  /**
   * Carries the taint onto the variable an {@code UNWIND} binds from a list that reads a tainted variable ({@code UNWIND [n] AS
   * m}, {@code UNWIND l AS m} for a tainted {@code l}, issue #12323): its elements can be the entity itself.
   */
  static void propagateTaintThroughUnwind(final UnwindClause unwindClause,
      final Set<String> disconnectedTaintedVariables) {
    if (!disconnectedTaintedVariables.isEmpty()
        && CypherVariableUsage.expressionReferencesAnyVariable(unwindClause.getListExpression(), disconnectedTaintedVariables))
      disconnectedTaintedVariables.add(unwindClause.getVariable());
  }

  /**
   * Carries the taint across a {@code CALL} subquery the way {@link #propagateTaintThroughRenames} carries it across a
   * {@code WITH ... AS} (issue #11534): {@code CALL (n0) { RETURN n0 AS n21 }} hands the outer query the same entity under a new
   * name, so a later {@code DELETE n21} races the not-yet-produced rows of the disconnected MATCH exactly as {@code DELETE n0}
   * would. What the body does with an imported variable is not followed, so every name it returns is tainted as soon as it can
   * see a tainted one: through an explicit scope that names it or {@code *}, or through the importing {@code WITH} of a body with
   * no scope clause. A body that imports nothing tainted ({@code CALL () { ... }}) returns nothing tainted.
   */
  static void propagateTaintThroughSubquery(final SubqueryClause subqueryClause,
      final Set<String> disconnectedTaintedVariables) {
    if (disconnectedTaintedVariables.isEmpty() || subqueryClause.getInnerStatement() == null)
      return;
    final ReturnClause returnClause = subqueryClause.getInnerStatement().getReturnClause();
    if (returnClause == null || !importsTaintedVariable(subqueryClause, disconnectedTaintedVariables))
      return;
    for (final ReturnClause.ReturnItem item : returnClause.getReturnItems())
      if (!item.isStar())
        disconnectedTaintedVariables.add(item.getOutputName());
  }

  private static boolean importsTaintedVariable(final SubqueryClause subqueryClause,
      final Set<String> disconnectedTaintedVariables) {
    final List<String> scope = subqueryClause.getScopeVariables();
    if (scope == null)
      return true; // CALL { WITH n ... }: the importing WITH is not parsed here, so assume it imports one
    for (final String variable : scope)
      if ("*".equals(variable) || disconnectedTaintedVariables.contains(variable))
        return true;
    return false;
  }

  /**
   * True when at least one target of the DELETE might read a variable in {@code disconnectedTaintedVariables} - see
   * {@link #closeMatchSegment}.
   * <p>
   * Decided on the parsed target, never on its text, which the parser hands over with the whitespace stripped
   * ({@code CASE WHEN true THEN x END} reads as one identifier): a plain variable is looked up, any other expression is walked
   * for a read of a tainted variable ({@code endNode(r)}, {@code [n][0]}, {@code CASE ... END}). A target with no parsed
   * expression is looked up when it is one identifier, and taken as a possible read of any tainted variable otherwise.
   */
  static boolean deleteMayTargetTaintedVariable(final DeleteClause deleteClause, final Set<String> disconnectedTaintedVariables) {
    if (disconnectedTaintedVariables.isEmpty() || deleteClause == null || deleteClause.getVariables() == null)
      return false;
    final List<String> targets = deleteClause.getVariables();
    final List<Expression> expressions = deleteClause.getExpressions();
    for (int i = 0; i < targets.size(); i++) {
      final Expression expression = expressions != null && i < expressions.size() ? expressions.get(i) : null;
      if (expression instanceof VariableExpression variable) {
        if (disconnectedTaintedVariables.contains(variable.getVariableName()))
          return true;
      } else if (expression != null) {
        if (CypherVariableUsage.expressionReferencesAnyVariable(expression, disconnectedTaintedVariables))
          return true;
      } else if (!isPlainIdentifier(targets.get(i)) || disconnectedTaintedVariables.contains(targets.get(i)))
        return true;
    }
    return false;
  }

  /** Whether the whole target is one identifier: letters, digits and underscores, not starting with a digit. */
  private static boolean isPlainIdentifier(final String target) {
    if (target.isEmpty() || !(Character.isLetter(target.charAt(0)) || target.charAt(0) == '_'))
      return false;
    for (int i = 1; i < target.length(); i++) {
      final char c = target.charAt(i);
      if (!Character.isLetterOrDigit(c) && c != '_')
        return false;
    }
    return true;
  }

  /**
   * True when a DELETE of the FOREACH body, or of a FOREACH nested in it, may target a tainted entity. The FOREACH variable
   * of a list that reads a tainted variable ({@code FOREACH (x IN [n] | DETACH DELETE x)}) is that entity under a new name, so
   * it is tainted for the body it scopes, exactly as an {@code UNWIND} alias is (issue #12323).
   */
  static boolean foreachDeleteMayTargetTaintedVariable(final ForeachClause foreachClause,
      final Set<String> disconnectedTaintedVariables) {
    if (disconnectedTaintedVariables.isEmpty())
      return false;
    Set<String> scope = disconnectedTaintedVariables;
    if (foreachClause.getVariable() != null
        && CypherVariableUsage.expressionReferencesAnyVariable(foreachClause.getListExpression(), disconnectedTaintedVariables)) {
      scope = new HashSet<>(disconnectedTaintedVariables);
      scope.add(foreachClause.getVariable());
    }
    for (final ClauseEntry entry : foreachClause.getInnerClauses()) {
      if (entry.getType() == ClauseEntry.ClauseType.DELETE) {
        if (deleteMayTargetTaintedVariable((DeleteClause) entry.getTypedClause(), scope))
          return true;
      } else if (entry.getType() == ClauseEntry.ClauseType.FOREACH) {
        if (foreachDeleteMayTargetTaintedVariable(entry.getTypedClause(), scope))
          return true;
      }
    }
    return false;
  }
}
