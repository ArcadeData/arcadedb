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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.QuantifiedPathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ShortestPathPattern;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.executor.operators.InListValues;
import com.arcadedb.query.opencypher.executor.steps.BoundNodeCheckStep;
import com.arcadedb.query.opencypher.executor.steps.ExpandPathStep;
import com.arcadedb.query.opencypher.executor.steps.FilterPropertiesStep;
import com.arcadedb.query.opencypher.executor.steps.IndexSeekStep;
import com.arcadedb.query.opencypher.executor.steps.MatchEdgeByIndexStep;
import com.arcadedb.query.opencypher.executor.steps.MatchEdgeByTypeScanStep;
import com.arcadedb.query.opencypher.executor.steps.MatchNodeStep;
import com.arcadedb.query.opencypher.executor.steps.MatchRelationshipStep;
import com.arcadedb.query.opencypher.executor.steps.OptionalMatchStep;
import com.arcadedb.query.opencypher.executor.steps.QuantifiedPathStep;
import com.arcadedb.query.opencypher.executor.steps.ShortestPathStep;
import com.arcadedb.query.opencypher.executor.steps.UniqueKeyAnchor;
import com.arcadedb.query.opencypher.executor.steps.ZeroLengthPathStep;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Builds the steps of one MATCH clause: the scan or seek of each node, the expansion of each hop, the WHERE applied where
 * the earliest step can decide it, and the OPTIONAL wrapping. It picks where a pattern starts - an edge index, a
 * vertex pinned by a UNIQUE key, the indexed end of a variable-length path - and the order of the pattern's independent
 * parts, and hands out the internal names of the anonymous nodes and relationships of the statement.
 * <p>
 * Split out of {@link CypherStepBuilder}, whose three paths each call it for the MATCH clauses they meet.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MatchStepBuilder {
  private final CypherExecutionPlan     plan;
  private final DatabaseInternal        database;
  private final CypherStatement         statement;
  private final PhysicalPlan            physicalPlan;
  private final HopEdgeTrackingAnalyzer hopEdges;
  private final FilterPushdown          filterPushdown;

  // Query-level counter for unique anonymous variable names across MATCH clauses
  private int anonymousVarCounter = 0;

  MatchStepBuilder(final CypherExecutionPlan plan, final DatabaseInternal database, final CypherStatement statement,
      final PhysicalPlan physicalPlan, final HopEdgeTrackingAnalyzer hopEdges, final FilterPushdown filterPushdown) {
    this.plan = plan;
    this.database = database;
    this.statement = statement;
    this.physicalPlan = physicalPlan;
    this.hopEdges = hopEdges;
    this.filterPushdown = filterPushdown;
  }

  /** The next number of the statement's internal names, which no two anonymous nodes or relationships may share. */
  int nextAnonymousId() {
    return anonymousVarCounter++;
  }

  /**
   * Builds execution step for a MATCH clause.
   * Backward-compatible overload without bound variable tracking.
   */
  AbstractExecutionStep buildMatchStep(final MatchClause matchClause, AbstractExecutionStep currentStep,
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
  AbstractExecutionStep buildMatchStep(final MatchClause matchClause, AbstractExecutionStep currentStep,
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
            ("  nd" + nextAnonymousId());
        matchVariables.add(variable);

        // Optimization: when the variable is already bound from a previous clause AND
        // the new node pattern carries no additional constraints (no labels, no inline
        // properties), no MatchNodeStep is needed - the carried binding already satisfies
        // the empty constraint, as long as it is a node. A binding an earlier OPTIONAL MATCH
        // left null cannot match, so a BoundNodeCheckStep drops such a row instead of letting
        // it through as if the pattern had matched (issue #11761). When the pattern adds
        // labels or inline properties, MatchNodeStep runs so the constraints are checked
        // against the bound vertex (and a null binding is filtered out the same way - issue #4102).
        if (boundVariables.contains(variable)
            && !nodePattern.hasLabels()
            && !nodePattern.hasProperties()) {
          final BoundNodeCheckStep checkStep = new BoundNodeCheckStep(variable, context);
          if (currentStep != null && !(isOptional && matchChainStart == null))
            checkStep.setPrevious(currentStep);
          currentStep = checkStep;
          // The first step of an OPTIONAL chain is registered as its start: OptionalMatchStep feeds every input row
          // into matchChainStart, and the next pattern part re-seats matchChainStart on its own head, so a chain start
          // left null here would orphan this step (issue #6544).
          if (isOptional && matchChainStart == null)
            matchChainStart = checkStep;
          // Handle zero-length named paths: p = (n)
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
            ("  src" + nextAnonymousId());
        final String targetVar = targetNode.getVariable() != null ? targetNode.getVariable() :
            ("  tgt" + nextAnonymousId());
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
          // Issue #11874: no index answers the relationship's predicate, which a scan of its types' buckets answers when it
          // requires a property of the relationship, instead of a scan of every vertex and a walk of every edge list
          final MatchEdgeByTypeScanStep edgeTypeScanSeed = tryBuildEdgeTypeScanSeed(matchClause, pathPatterns, patternIndex,
              boundVariables, matchVariables, pushedConjuncts, context);
          if (edgeTypeScanSeed != null) {
            currentStep = edgeTypeScanSeed;
            continue;
          }
        }

        NodePattern sourceNode = pathPattern.getFirstNode();
        String sourceVar = sourceNode.getVariable() != null ? sourceNode.getVariable() :
            ("  src" + nextAnonymousId());
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

        // Issue #12149: a single hop from an unlabelled, unconstrained source to a target a UNIQUE index pins starts from the
        // target: an index lookup and a walk of its relationships backwards, instead of a scan of every vertex of every type
        // (repeated for every row an UNWIND or a WITH hands over) and an expansion of all of them
        if (!reversed && !sourceAlreadyBound
            && startsFromUniquelyKeyedTarget(pathPattern, sourceNode, sourceVar, whereClause, isOptional, boundVariables,
            matchVariables)) {
          final NodePattern targetNode = pathPattern.getLastNode();
          reversed = true;
          sourceNode = targetNode;
          sourceVar = targetNode.getVariable() != null ? targetNode.getVariable() : ("  tgt" + nextAnonymousId());
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
              relVar = hopNeedsEdgeTracking[i] ? ("  rel" + nextAnonymousId()) : null;
          } else
            relVar = hopNeedsEdgeTracking[i] ? ("  rel" + nextAnonymousId()) : null;
          String targetVar = targetNode.getVariable() != null ? targetNode.getVariable() :
              ("  tgt" + nextAnonymousId());

          // When reversed, swap source/target variables and use the original source as target.
          // This mapping only holds for a single-relationship pattern: the written source is the
          // end of the one reversed hop. Every entry point into `reversed` enforces that (the
          // bound-target reversal above, the uniquely keyed target one of issue #12149 and
          // shouldReverseVariableLengthPathFromIndexedAnchor);
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
        : ("  src" + nextAnonymousId());
    final String effectiveTargetVar = tgtNode.getVariable() != null ? tgtNode.getVariable()
        : ("  tgt" + nextAnonymousId());

    // An anonymous relationship gets an internal variable when another hop of this MATCH could bind the same edge, as the
    // ordinary hop builder gives it: the later hops' relationship uniqueness compares against what the clause binds, and a
    // relationship bound under no name was free to be bound again (issue #11585)
    final String seededEdgeVar = namedEdge ? edgeVar
        : hopEdges.computeHopEdgeTrackingNeeds(pathPatterns, patternIndex)[0] ? "  rel" + nextAnonymousId() : null;

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
   * Builds a {@link MatchEdgeByTypeScanStep} seed for a single-hop pattern no index of its relationship types answers, when the
   * MATCH's {@code WHERE} clause or the relationship's inline property map requires a property of the relationship (issue
   * #11874): such a predicate keeps out every light edge, the one kind of edge the walk finds and a scan of the types' buckets
   * does not (see {@link EdgeTypeScanPlanner}). So does a statement that only aggregates properties of the relationship with
   * aggregates that skip a null, {@code RETURN count(e.w)}: a light edge adds nothing to them. The scan reads the edges of the
   * types instead of every vertex and every edge list, and checks the endpoints' labels and inline properties on the two
   * vertices of each edge it keeps.
   * <p>
   * Returns {@code null} - deferring to the ordinary vertex-scan + expansion plan - whenever a precondition is not met, when the
   * statement writes, or when the source endpoint carries labels whose vertices and their share of the edges cost the walk less
   * than the scan of every edge of the types. On success it registers the variables the step binds in {@code matchVariables}, and
   * the conjuncts of the {@code WHERE} clause that read only the relationship in {@code pushedConjuncts}: the step decides them on
   * every row it passes on, before it loads the endpoints, and the clause's filter leaves them out.
   */
  private MatchEdgeByTypeScanStep tryBuildEdgeTypeScanSeed(final MatchClause matchClause, final List<PathPattern> pathPatterns,
      final int patternIndex, final Set<String> boundVariables, final Set<String> matchVariables,
      final Set<BooleanExpression> pushedConjuncts, final CommandContext context) {
    final PathPattern pathPattern = pathPatterns.get(patternIndex);
    // A statement that writes could meet in the buckets the edges its own writes add behind the scan
    if (!statement.isReadOnly() || pathPattern.hasPathVariable() || pathPattern.getRelationshipCount() != 1)
      return null;

    final RelationshipPattern rel = pathPattern.getRelationship(0);
    if (rel instanceof QuantifiedPathPattern || rel.isVariableLength() || rel.hasWhereExpression()
        || rel.getPropertiesParameterName() != null || !rel.hasTypes())
      return null;

    final String edgeVar = rel.getVariable();
    final boolean namedEdge = edgeVar != null && !edgeVar.isEmpty();
    if (namedEdge && (boundVariables.contains(edgeVar) || matchVariables.contains(edgeVar)))
      return null;

    // The seed is the first step of the plan: there is no row to evaluate an inline value that reads a variable against
    final Map<String, Object> relProperties = rel.hasProperties() ? rel.getProperties() : null;
    if (!EdgeIndexSeedPlanner.isRowIndependent(relProperties))
      return null;

    final WhereClause whereClause = matchClause.hasWhereClause() ? matchClause.getWhereClause() : null;
    final BooleanExpression where = whereClause != null ? whereClause.getConditionExpression() : null;
    if ((relProperties == null || relProperties.isEmpty()) && !(namedEdge
        && (EdgeTypeScanPlanner.requiresAPropertyOf(edgeVar, where) || EdgeTypeScanPlanner.onlyAggregatesAPropertyOf(edgeVar, statement))))
      return null;

    final List<String> edgeTypes = EdgeTypeScanPlanner.scanTypes(database.getSchema(), rel.getTypes());
    if (edgeTypes == null || edgeTypes.isEmpty())
      return null;

    final NodePattern srcNode = pathPattern.getFirstNode();
    final NodePattern tgtNode = pathPattern.getLastNode();
    if (!isTypeScannableEndpoint(srcNode, true, whereClause, boundVariables, matchVariables)
        || !isTypeScannableEndpoint(tgtNode, false, whereClause, boundVariables, matchVariables)
        || !isTypeScanCheaperThanWalk(srcNode, edgeTypes))
      return null;
    // A target a UNIQUE index pins is the cheaper start: one lookup and the relationships of one vertex (issue #12149)
    if (startsFromUniquelyKeyedTarget(pathPattern, srcNode, srcNode.getVariable() != null ? srcNode.getVariable() : "  src",
        whereClause, false, boundVariables, matchVariables))
      return null;

    final String effectiveSourceVar = srcNode.getVariable() != null ? srcNode.getVariable()
        : ("  src" + nextAnonymousId());
    final String effectiveTargetVar = tgtNode.getVariable() != null ? tgtNode.getVariable()
        : ("  tgt" + nextAnonymousId());
    // An anonymous relationship another hop of this MATCH could bind again is tracked as the index seed tracks it (issue #11585)
    final String seededEdgeVar = namedEdge ? edgeVar
        : hopEdges.computeHopEdgeTrackingNeeds(pathPatterns, patternIndex)[0] ? "  rel" + nextAnonymousId() : null;

    matchVariables.add(effectiveSourceVar);
    matchVariables.add(effectiveTargetVar);
    if (seededEdgeVar != null)
      matchVariables.add(seededEdgeVar);

    final List<BooleanExpression> relationshipConjuncts = namedEdge ?
        EdgeTypeScanPlanner.relationshipOnlyConjuncts(edgeVar, where) : List.of();
    pushedConjuncts.addAll(relationshipConjuncts);

    final MatchEdgeByTypeScanStep.Pattern pattern = new MatchEdgeByTypeScanStep.Pattern(edgeTypes, rel.getDirection(),
        effectiveSourceVar, seededEdgeVar, effectiveTargetVar, EdgeIndexSeedPlanner.EndpointFilter.of(srcNode),
        EdgeIndexSeedPlanner.EndpointFilter.of(tgtNode));
    return new MatchEdgeByTypeScanStep(pattern, relProperties, EdgeTypeScanPlanner.and(relationshipConjuncts), context);
  }

  /**
   * Whether the type scan can bind an endpoint and check what the pattern asks of it on the vertex of each edge (issue #11874).
   * Not when it is bound already, or carries a dynamic label, an inline {@code WHERE} or a property map given as a parameter, or
   * an inline value that reads a variable. Nor when it is the source and the pattern narrows it: an inline property map or a
   * {@code WHERE} conjunct of its own (an {@code id()} included) is decided on each vertex of the scan the walk starts from, and
   * the walk then reads only the edges of the vertices that pass, which costs less than reading every edge of the types. The
   * target narrows nothing the walk reads: it is checked on each edge's vertex either way.
   */
  private boolean isTypeScannableEndpoint(final NodePattern node, final boolean source, final WhereClause whereClause,
      final Set<String> boundVariables, final Set<String> matchVariables) {
    final String var = node.getVariable();
    if (var != null && (boundVariables.contains(var) || matchVariables.contains(var)))
      return false;
    if (node.hasDynamicLabels() || node.hasWhereExpression() || node.getPropertiesParameterName() != null)
      return false;
    if (node.hasProperties() && (source || !EdgeIndexSeedPlanner.isRowIndependent(node.getProperties())))
      return false;
    return !source || var == null || whereClause == null || whereClause.getConditionExpression() == null
        || WhereClause.extractForVariables(whereClause.getConditionExpression(), Set.of(var)) == null;
  }

  /**
   * Whether scanning every edge of the types costs no more than the walk the plan would otherwise start from the source endpoint
   * (issue #11874). An unlabelled source is a scan of every vertex, whose edge lists hold every edge of the types: the scan of the
   * types reads less. A labelled one reads the vertices of its labels and their edges of the types, estimated at the labels'
   * share of all vertices.
   * <p>
   * A count is refused to a user who may not read the type. The total over every vertex type is only the denominator of an
   * estimate and reads types the statement never names, so a type the user may not read is left out of it rather than refusing a
   * statement that does not touch it. A type the statement does name (a label of the source, a relationship type) keeps the walk,
   * which refuses the user when it opens the buckets.
   */
  private boolean isTypeScanCheaperThanWalk(final NodePattern source, final List<String> edgeTypes) {
    if (!source.hasLabels())
      return true;
    final Schema schema = database.getSchema();
    long anchorVertices = 0;
    long edges = 0;
    try {
      for (final DocumentType type : Labels.matchingVertexTypes(schema, source.getLabels(), source.isLabelDisjunction()))
        anchorVertices += database.countType(type.getName(), false);
      for (final String edgeType : edgeTypes)
        edges += database.countType(edgeType, true);
    } catch (final SecurityException e) {
      return false;
    }
    long allVertices = 0;
    for (final DocumentType type : schema.getTypes())
      if (type instanceof VertexType && SecurityHelper.isReadable(database, type))
        try {
          allVertices += database.countType(type.getName(), false);
        } catch (final SecurityException e) {
          // readable in part only: one of its buckets refuses the count
        }
    return edges <= anchorVertices + (double) edges * anchorVertices / Math.max(1L, allVertices);
  }

  /**
   * Whether a node pattern pins its node by an equality on every column of a UNIQUE index of its one label, so a seek of it
   * finds one vertex at most (issue #11699).
   */
  boolean isUniquelyKeyed(final NodePattern node) {
    return uniqueKeyIndexOf(node) != null;
  }

  /**
   * {@link #isUniquelyKeyed(NodePattern)}, with one column of the index allowed to be keyed by an IN-list of the
   * {@code WHERE} instead ({@link HopEdgeTrackingAnalyzer#whereInListKeys}): a seek of the node then finds one vertex per key
   * (issue #11715).
   */
  boolean isUniquelyKeyed(final NodePattern node, final Map<String, Set<String>> inListKeys) {
    final Set<String> listed = node.getVariable() != null ? inListKeys.getOrDefault(node.getVariable(), Set.of()) : Set.of();
    if (listed.isEmpty())
      return isUniquelyKeyed(node);
    if (node.getPropertiesParameterName() != null || node.hasDynamicLabels() || !node.hasLabels() || node.getLabels().size() != 1)
      return false;
    final Schema schema = database.getSchema();
    if (!schema.existsType(node.getLabels().getFirst()))
      return false;
    final Set<String> inline = node.hasProperties() ? node.getProperties().keySet() : Set.of();
    for (final TypeIndex index : schema.getType(node.getLabels().getFirst()).getAllIndexes(true)) {
      final List<String> keys = index.isUnique() && index.isReadyForQueries() ? index.getPropertyNamesIfExactKeyLookup() : null;
      if (keys == null)
        continue;
      int listedOnly = 0;
      boolean covered = true;
      for (final String key : keys) {
        if (inline.contains(key))
          continue;
        if (listed.contains(key))
          ++listedOnly;
        else
          covered = false;
      }
      if (covered && listedOnly <= 1)
        return true;
    }
    return false;
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
   *
   * @param node    the pattern node, with the equalities of the {@code WHERE} clause added to its property map
   * @param written the node as the pattern writes it, whose own property map is compared through {@link InlineProperties}
   */
  UniqueKeyAnchor uniqueKeyAnchorOf(final NodePattern node, final NodePattern written, final CommandContext context) {
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
      // The = of a WHERE equality reads a RID spelled as text as the id it encodes in a numeric column (issue #11933). The pattern's
      // own map compares through InlineProperties, which does not, so its value is sought as written: the id would find a vertex
      // the map rejects, and a text no id encodes would refuse the count where the walk answers no row
      final Object indexKey = TemporalUtil.toIndexKey(value);
      final boolean fromWrittenMap = written.hasProperties() && written.getProperties().get(keys.get(i)) == declared;
      key[i] = fromWrittenMap ? indexKey : RidTextKeys.keyFor(indexKey, index.getKeyTypes(), i);
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
    for (final Object value : properties.values())
      if (!valueReadsOnlyEarlierClauses(value, boundBefore))
        return false;
    return true;
  }

  /** {@link #readsOnlyEarlierClauses} for one value of a property map, or one side of a {@code WHERE} equality. */
  private static boolean valueReadsOnlyEarlierClauses(final Object value, final Set<String> boundBefore) {
    if (RowIndependentValues.isSeekable(value))
      return true;
    if (!(value instanceof Expression expression) || !RowIndependentValues.isDeterministic(expression))
      return false;
    final CypherReferencedVariables referenced = CypherReferencedVariables.of(expression);
    return referenced.isComplete() && boundBefore.containsAll(referenced.getNames());
  }

  /**
   * Whether a pattern of one fixed-length hop is better started from its target (issue #12149): the source is unlabelled and
   * unconstrained, so starting from it scans every vertex of every type, while the target is pinned by an equality on every
   * column of a UNIQUE index of its label, through its inline map or through the {@code WHERE} clause (equalities or
   * {@code IN}-lists), whose values are literals, parameters or variables an earlier clause bound (an UNWIND or WITH row). The
   * step that starts the pattern then finds the target through that index, once per row, and the hop walks its relationships
   * backwards.
   * <p>
   * Not over a relationship of no type or of a unidirectional type, whose incoming side no vertex stores, and not for an OPTIONAL
   * MATCH or a path variable, which bind the pattern in the order written. Not when the {@code WHERE} clause pins the source
   * either: the step that starts from it may seek an index of some type, where the walk back from a target with many
   * relationships would read them all.
   */
  private boolean startsFromUniquelyKeyedTarget(final PathPattern pathPattern, final NodePattern sourceNode, final String sourceVar,
      final WhereClause whereClause, final boolean isOptional, final Set<String> boundVariables, final Set<String> matchVariables) {
    if (isOptional || pathPattern.hasPathVariable() || pathPattern.getRelationshipCount() != 1)
      return false;
    final RelationshipPattern rel = pathPattern.getRelationship(0);
    if (rel instanceof QuantifiedPathPattern || rel.isVariableLength() || !rel.hasTypes())
      return false;
    for (final String type : rel.getTypes())
      if (!(database.getSchema().getTypeOrNull(type) instanceof EdgeType edgeType) || !edgeType.isBidirectional())
        return false;

    // the source is a scan of every vertex: no label, no property, no inline predicate, no id, no equality or IN-list of the WHERE
    final BooleanExpression where = whereClause != null ? whereClause.getConditionExpression() : null;
    if (sourceNode.hasLabels() || sourceNode.hasDynamicLabels() || sourceNode.hasProperties() || sourceNode.hasWhereExpression()
        || boundVariables.contains(sourceVar) || matchVariables.contains(sourceVar) || plan.extractIdFilter(whereClause, sourceVar) != null
        || !wherePinnedProperties(sourceVar, where, value -> true).isEmpty())
      return false;

    final NodePattern target = pathPattern.getLastNode();
    final String targetVar = target.getVariable();
    if (target.hasDynamicLabels() || target.hasWhereExpression() || target.getPropertiesParameterName() != null)
      return false;
    if (targetVar != null && (targetVar.equals(sourceVar) || boundVariables.contains(targetVar) || matchVariables.contains(targetVar)))
      return false;
    if (isUniquelyKeyed(target) && readsOnlyEarlierClauses(target.getProperties(), boundVariables))
      return true;
    return targetVar != null && isUniquelyKeyedByWhere(target, targetVar, where, boundVariables);
  }

  /**
   * Whether the {@code WHERE} clause pins every column of a UNIQUE index of the node's one label (issue #12149), with the
   * predicates the node step seeks an index with: equalities and {@code IN}-lists whose values are literals, parameters or
   * variables an earlier clause bound.
   */
  private boolean isUniquelyKeyedByWhere(final NodePattern node, final String variable, final BooleanExpression where,
      final Set<String> boundVariables) {
    if (where == null || !node.hasLabels() || node.getLabels().size() != 1
        || !database.getSchema().existsType(node.getLabels().getFirst()))
      return false;
    final Set<String> pinned = wherePinnedProperties(variable, where, value -> valueReadsOnlyEarlierClauses(value, boundVariables));
    if (pinned.isEmpty())
      return false;
    for (final TypeIndex index : database.getSchema().getType(node.getLabels().getFirst()).getAllIndexes(true)) {
      final List<String> keys = index.isUnique() && index.isReadyForQueries() ? index.getPropertyNamesIfExactKeyLookup() : null;
      if (keys != null && pinned.containsAll(keys))
        return true;
    }
    return false;
  }

  /**
   * The properties of {@code variable} the {@code WHERE} clause pins to a value {@code readable} accepts: the row-independent
   * equalities and {@code IN}-lists, an OR of them included ({@link EdgeIndexSeedPlanner#collectWherePredicates}), and the
   * equalities and {@code IN}-lists among the top-level conjuncts whose values an earlier clause decides.
   */
  private static Set<String> wherePinnedProperties(final String variable, final BooleanExpression where,
      final Predicate<Expression> readable) {
    final Set<String> pinned = new HashSet<>();
    if (where == null)
      return pinned;
    final Map<String, Object> rowIndependent = new LinkedHashMap<>();
    EdgeIndexSeedPlanner.collectWherePredicates(variable, where, rowIndependent);
    pinned.addAll(rowIndependent.keySet());
    for (final BooleanExpression conjunct : CountPushDownPredicates.conjuncts(where)) {
      if (conjunct instanceof ComparisonExpression comparison && comparison.getOperator() == ComparisonExpression.Operator.EQUALS) {
        if (comparison.getLeft() instanceof PropertyAccessExpression left && variable.equals(left.getVariableName())
            && readable.test(comparison.getRight()))
          pinned.add(left.getPropertyName());
        else if (comparison.getRight() instanceof PropertyAccessExpression right && variable.equals(right.getVariableName())
            && readable.test(comparison.getLeft()))
          pinned.add(right.getPropertyName());
      } else if (conjunct instanceof InExpression in && in.getExpression() instanceof PropertyAccessExpression listed
          && variable.equals(listed.getVariableName()) && in.getList() != null && !in.getList().isEmpty()
          && in.getList().stream().allMatch(readable))
        pinned.add(listed.getPropertyName());
    }
    return pinned;
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
  void rememberOptionalMatches(final OptionalMatchStep optionalStep, final MatchClause matchClause) {
    if (statement.isReadOnly())
      optionalStep.setMemoKey(DistinctSemiJoin.matchMemoKey(matchClause));
  }
}
