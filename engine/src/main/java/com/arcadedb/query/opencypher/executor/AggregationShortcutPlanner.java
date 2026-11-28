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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.BooleanCoercionExpression;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.CaseAlternative;
import com.arcadedb.query.opencypher.ast.CaseExpression;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpressionWrapper;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.IsNullExpression;
import com.arcadedb.query.opencypher.ast.LabelCheckExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.ListIndexExpression;
import com.arcadedb.query.opencypher.ast.ListSliceExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.MapExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.QuantifiedPathPattern;
import com.arcadedb.query.opencypher.ast.RegexExpression;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.ShortestPathPattern;
import com.arcadedb.query.opencypher.ast.StarExpression;
import com.arcadedb.query.opencypher.ast.StringMatchExpression;
import com.arcadedb.query.opencypher.ast.TernaryLogicalExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.executor.operators.ExpandAll;
import com.arcadedb.query.opencypher.executor.operators.FilterOperator;
import com.arcadedb.query.opencypher.executor.operators.GAVExpandAll;
import com.arcadedb.query.opencypher.executor.operators.GAVFusedChainOperator;
import com.arcadedb.query.opencypher.executor.operators.GAVVarLengthExpand;
import com.arcadedb.query.opencypher.executor.operators.NodeByLabelScan;
import com.arcadedb.query.opencypher.executor.operators.PhysicalOperator;
import com.arcadedb.query.opencypher.executor.steps.AggregationStep;
import com.arcadedb.query.opencypher.executor.steps.DistinctEndpointCountStep;
import com.arcadedb.query.opencypher.executor.steps.CountChainedEdgesStep;
import com.arcadedb.query.opencypher.executor.steps.CountEdgesReturnStep;
import com.arcadedb.query.opencypher.executor.steps.CountEdgesStep;
import com.arcadedb.query.opencypher.executor.steps.ExpandPathStep;
import com.arcadedb.query.opencypher.executor.steps.FilterPropertiesStep;
import com.arcadedb.query.opencypher.executor.steps.GAVOneHopScanStep;
import com.arcadedb.query.opencypher.executor.steps.GroupByAggregationStep;
import com.arcadedb.query.opencypher.executor.steps.IndexMinMaxStep;
import com.arcadedb.query.opencypher.executor.steps.MatchNodeStep;
import com.arcadedb.query.opencypher.executor.steps.MatchRelationshipStep;
import com.arcadedb.query.opencypher.executor.steps.TypeCountStep;
import com.arcadedb.query.opencypher.optimizer.CypherOptimizer;
import com.arcadedb.query.opencypher.optimizer.RangePredicate;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Recognises the aggregating statements one step answers in place of the MATCH pipeline: a count over an OPTIONAL MATCH
 * or a chain of them, a count of distinct endpoints, a one-hop scan over a graph analytical view, a MATCH whose RETURN
 * only counts, a count of a type read off its bucket totals, a min or max read off an index, and an aggregation fused
 * into the expansion chain that feeds it. Each {@code try...} method answers the step, or null (false for the fusion)
 * when the statement is not that shape.
 * <p>
 * Split out of {@link CypherExecutionPlan}, which decides where in the plan each of them is asked.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class AggregationShortcutPlanner {
  private final CypherExecutionPlan plan;
  private final DatabaseInternal    database;
  private final CypherStatement     statement;
  private final PhysicalPlan        physicalPlan;
  private final ExpressionEvaluator expressionEvaluator;

  AggregationShortcutPlanner(final CypherExecutionPlan plan, final DatabaseInternal database, final CypherStatement statement,
      final PhysicalPlan physicalPlan, final ExpressionEvaluator expressionEvaluator) {
    this.plan = plan;
    this.database = database;
    this.statement = statement;
    this.physicalPlan = physicalPlan;
    this.expressionEvaluator = expressionEvaluator;
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
  AbstractExecutionStep tryOptimizeChainedOptionalMatchCount(final MatchClause firstMatch,
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

    // No property constraints, and no inline predicate the edge count cannot apply (issue #11583)
    if (firstRel.getProperties() != null && !firstRel.getProperties().isEmpty())
      return null;
    if (firstRel.hasWhereExpression())
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

    // No property constraints, and no inline predicate the edge count cannot apply (issue #11583)
    if (secondRel.getProperties() != null && !secondRel.getProperties().isEmpty())
      return null;
    if (secondRel.hasWhereExpression())
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

    // The step applies one label on the intermediate where the first hop reaches it and one on the counted target. A label
    // written on the bound node or again on the intermediate, a label set no single name stands for, a dynamic label or an
    // inline WHERE on a node filters what the step cannot see (issue #11583)
    final NodePattern boundNode = firstVar1.equals(boundVar) ? firstNode1 : firstNode2;
    final NodePattern intermediateNode = boundNode == firstNode1 ? firstNode2 : firstNode1;
    final NodePattern secondIntermediateNode = secondVar1.equals(intermediateVar) ? secondNode1 : secondNode2;
    final NodePattern targetNode = secondIntermediateNode == secondNode1 ? secondNode2 : secondNode1;
    if (boundNode.hasLabels() || secondIntermediateNode.hasLabels() || !CountStarShapeDetector.isCountableEndpoint(boundNode)
        || !CountStarShapeDetector.isCountableEndpoint(intermediateNode) || !CountStarShapeDetector.isCountableEndpoint(secondIntermediateNode)
        || !CountStarShapeDetector.isCountableEndpoint(targetNode))
      return null;
    final String intermediateLabel = intermediateNode.hasLabels() ? intermediateNode.getLabels().get(0) : null;
    final String targetLabel = targetNode.hasLabels() ? targetNode.getLabels().get(0) : null;

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
    if (!CypherExecutionPlan.isRowPerGroupUpToClause(clausesInOrder, currentIndex, CypherExecutionPlan.groupingKeyVariableNames(groupingKeys)))
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
        intermediateLabel, targetLabel, countAlias, passThroughAliases, context);
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
  AbstractExecutionStep tryOptimizeOptionalMatchCount(final MatchClause matchClause,
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

    // 3. No property constraints on the relationship, and no inline predicate the edge count cannot apply (issue #11583)
    if (relPattern.getProperties() != null && !relPattern.getProperties().isEmpty())
      return null;
    if (relPattern.hasWhereExpression())
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
    if (!CypherExecutionPlan.isRowPerGroupUpToClause(clausesInOrder, currentIndex, CypherExecutionPlan.groupingKeyVariableNames(groupingKeys)))
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

    // The step applies one label on the counted node. A label written on the bound node, a label set no single name stands
    // for, a dynamic label or an inline WHERE on a node filters what the step cannot see (issue #11583)
    final NodePattern boundNode = firstVar.equals(boundVar) ? firstNode : lastNode;
    final NodePattern countedNode = boundNode == firstNode ? lastNode : firstNode;
    if (boundNode.hasLabels() || !CountStarShapeDetector.isCountableEndpoint(boundNode) || !CountStarShapeDetector.isCountableEndpoint(countedNode))
      return null;
    final String targetLabel = countedNode.hasLabels() ? countedNode.getLabels().get(0) : null;

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
        boundVar, direction, edgeTypes, targetLabel, countAlias, passThroughAliases, context);
    if (currentStep != null)
      countEdgesStep.setPrevious(currentStep);

    return countEdgesStep;
  }

  /**
   * Tries to fuse a count(*) aggregation, grouped or not, into the GAVFusedChainOperator.
   * When successful, the chain aggregates internally in parallel — bypassing the
   * single-threaded GroupByAggregationStep / AggregationStep entirely, and building no row per path (issue #9738).
   * <p>
   * The chain must feed the aggregation directly: a step between them (a WITH projection, an UNWIND, a filter) would
   * read the chain's rows, which a fused chain no longer produces. The items must be exactly one non-DISTINCT count of
   * {@code *} or of a variable the chain binds (never null on a path, so it counts every path), and at most two grouping
   * keys, each a variable the chain binds.
   *
   * @param items the items of the WITH or RETURN clause aggregating the chain's rows
   *
   * @return true if fused successfully, false to fall back to GroupByAggregationStep / AggregationStep
   */
  boolean tryFuseAggregationIntoChain(final List<ReturnClause.ReturnItem> items,
      final AbstractExecutionStep currentStep) {
    if (physicalPlan == null || !(physicalPlan.getRootOperator() instanceof GAVFusedChainOperator chain)
        || !CypherExecutionPlan.isOptimizedMatchStep(currentStep))
      return false;

    // Check items: grouping keys bound by the chain + exactly one count(*) or count(var)
    final List<String> groupVarNames = new ArrayList<>();
    final List<String> groupOutputNames = new ArrayList<>();
    String countOutput = null;
    int aggCount = 0;

    for (final ReturnClause.ReturnItem item : items) {
      final Expression expr = item.getExpression();
      if (expr.isAggregation() && expr instanceof FunctionCallExpression funcExpr) {
        if (!"count".equals(funcExpr.getFunctionName()) || funcExpr.isDistinct() || funcExpr.getArguments().size() != 1)
          return false;
        // count(expr) counts the paths where expr is not null: only * and the chain's own variables are never null
        final Expression argument = funcExpr.getArguments().get(0);
        if (!(argument instanceof StarExpression)
            && !(argument instanceof VariableExpression varArg && chain.bindsVariable(varArg.getVariableName())))
          return false;
        aggCount++;
        countOutput = item.getOutputName();
      } else if (expr instanceof VariableExpression varExpr && chain.bindsVariable(varExpr.getVariableName())) {
        groupVarNames.add(varExpr.getVariableName());
        groupOutputNames.add(item.getOutputName() != null ? item.getOutputName() : varExpr.getVariableName());
      } else
        return false; // complex grouping expression — can't fuse
    }

    if (aggCount != 1 || countOutput == null || groupVarNames.size() > 2)
      return false; // only support 1-2 grouping keys packed into a long

    chain.setFusedAggregation(
        groupVarNames.toArray(new String[0]),
        groupOutputNames.toArray(new String[0]),
        countOutput);
    return true;
  }

  /**
   * Replaces the rows of {@code MATCH (a)-[:T*0|1..k]->(b) [WHERE b <> a] RETURN count(DISTINCT b)}, walked through a Graph
   * Analytical View, with a {@link DistinctEndpointCountStep} that counts the end vertices while the walks run (issue #10548):
   * the expansion would otherwise build one row per end vertex, the filter test each, and the aggregation hash them.
   * <p>
   * Only where nothing but that count reads the rows: the statement is the MATCH and the RETURN, the RETURN is one
   * {@code count(DISTINCT)} of one end of the expansion, and the physical plan is the {@link GAVVarLengthExpand} itself or a
   * filter over it testing nothing but {@code b <> a} between its two ends. The expansion is then in distinct-endpoint mode,
   * the only one a {@link GAVVarLengthExpand} has, so each row it would produce is a distinct (start, end) pair.
   *
   * @return the counting step, or null to keep the rows and aggregate them
   */
  AbstractExecutionStep tryCountDistinctEndpoints(final List<ReturnClause.ReturnItem> items,
      final AbstractExecutionStep currentStep, final CommandContext context) {
    if (physicalPlan == null || !CypherExecutionPlan.isOptimizedMatchStep(currentStep) || items.size() != 1
        || statement.getWhereClause() != null || statement.getClausesInOrder() == null
        || statement.getClausesInOrder().size() != 2)
      return null;
    final String counted = countedDistinctVariable(items.getFirst());
    if (counted == null)
      return null;

    PhysicalOperator root = physicalPlan.getRootOperator();
    boolean excludeSource = false;
    if (root instanceof FilterOperator filter && filter.getChild() instanceof GAVVarLengthExpand expand) {
      if (!(filter.getPredicate() instanceof ComparisonExpression comparison)
          || comparison.getOperator() != ComparisonExpression.Operator.NOT_EQUALS
          || !(comparison.getLeft() instanceof VariableExpression left) || !(comparison.getRight() instanceof VariableExpression right)
          || !isPairOfEnds(expand, left.getVariableName(), right.getVariableName()))
        return null;
      excludeSource = true;
      root = expand;
    }
    if (!(root instanceof GAVVarLengthExpand expand) || expand.getSourceVariable().equals(expand.getTargetVariable()))
      return null;
    // The counted end is the one the walk reaches, or the one the plan starts it from when it anchored on the other end
    final boolean countSources = expand.getSourceVariable().equals(counted);
    if (!countSources && !expand.getTargetVariable().equals(counted))
      return null;
    return new DistinctEndpointCountStep(expand, excludeSource, countSources, items.getFirst().getOutputName(), context);
  }

  private static boolean isPairOfEnds(final DistinctEndpointCountStep.Walk expand, final String left, final String right) {
    final String source = expand.getSourceVariable();
    final String target = expand.getTargetVariable();
    return (left.equals(source) && right.equals(target)) || (left.equals(target) && right.equals(source));
  }

  /**
   * The step-based twin of {@link #tryCountDistinctEndpoints} (issue #10557): replaces the rows of
   * {@code MATCH (a)-[:T*0|1..k]->(b) [WHERE b <> a] RETURN count(DISTINCT b)} that an {@link ExpandPathStep} in distinct-endpoint
   * mode produces, the filter of the {@code WHERE} and the aggregation, with a {@link DistinctEndpointCountStep} counting while the
   * walks run, through a view or the records. The step-based executor plans such a statement when, for instance, the end node has no
   * label.
   * <p>
   * Only where nothing but that count reads the rows: the statement is one non-optional MATCH and the RETURN, the RETURN is one
   * {@code count(DISTINCT)} of one end of the expansion, and the step chain ends with the expansion, under nothing but filters each
   * testing {@code b <> a} between its two ends. What else decides whether a row is produced - the target node pattern, a bound
   * target, a bound relationship list - is tested by {@link ExpandPathStep#countDistinct} exactly as the rows test it.
   *
   * @return the counting step, or null to keep the rows and aggregate them
   */
  AbstractExecutionStep tryCountDistinctEndpointsOfSteps(final List<ClauseEntry> clausesInOrder,
      final List<ReturnClause.ReturnItem> items, final AbstractExecutionStep currentStep, final CommandContext context) {
    if (items.size() != 1 || clausesInOrder == null || clausesInOrder.size() != 2
        || clausesInOrder.getFirst().getType() != ClauseEntry.ClauseType.MATCH
        || clausesInOrder.get(1).getType() != ClauseEntry.ClauseType.RETURN
        || ((MatchClause) clausesInOrder.getFirst().getTypedClause()).isOptional())
      return null;
    final String counted = countedDistinctVariable(items.getFirst());
    if (counted == null)
      return null;

    AbstractExecutionStep root = currentStep;
    final List<FilterPropertiesStep> filters = new ArrayList<>(2);
    while (root instanceof FilterPropertiesStep filter) {
      filters.add(filter);
      root = (AbstractExecutionStep) filter.getPrev();
    }
    if (!(root instanceof ExpandPathStep expand) || !expand.isDistinctEndpoints() || expand.getPrev() == null
        || expand.getSourceVariable().equals(expand.getTargetVariable()))
      return null;
    for (final FilterPropertiesStep filter : filters)
      if (filter.getWhereClause() == null
          || !(filter.getWhereClause().getConditionExpression() instanceof ComparisonExpression comparison)
          || comparison.getOperator() != ComparisonExpression.Operator.NOT_EQUALS
          || !(comparison.getLeft() instanceof VariableExpression left) || !(comparison.getRight() instanceof VariableExpression right)
          || !isPairOfEnds(expand, left.getVariableName(), right.getVariableName()))
        return null;

    // The counted end is the one the walk reaches, or the one it starts from when the planner anchored it on the other end
    final boolean countSources = expand.getSourceVariable().equals(counted);
    if (!countSources && !expand.getTargetVariable().equals(counted))
      return null;
    final DistinctEndpointCountStep step = new DistinctEndpointCountStep(expand, !filters.isEmpty(), countSources,
        items.getFirst().getOutputName(), context);
    // For the plan and the close only: the rows are pulled by the expansion, from that same step
    step.setPrevious(expand.getPrev());
    return step;
  }

  /** The variable a RETURN item {@code count(DISTINCT v)} counts, or null for any other item. */
  private static String countedDistinctVariable(final ReturnClause.ReturnItem item) {
    if (!(item.getExpression() instanceof FunctionCallExpression count) || !"count".equalsIgnoreCase(count.getFunctionName())
        || !count.isDistinct() || count.getArguments().size() != 1
        || !(count.getArguments().getFirst() instanceof VariableExpression counted))
      return null;
    return counted.getVariableName();
  }

  /**
   * Replaces the rows of {@code MATCH (a:A)-[:T]->(b:B) [WHERE pred(a, b)]}, feeding an aggregating RETURN, with a
   * {@link GAVOneHopScanStep} that reads both endpoints from a Graph Analytical View (issue #8335). The optimizer's plan
   * for such a query scans {@code A} through its records and expands every source through the view; the view then
   * serves the traversal but not the rows around it, which cost several times the traversal itself.
   * <p>
   * The replacement answers the same rows only under conditions checked here, and declines otherwise:
   * <ul>
   *   <li>the statement is one non-optional MATCH of a single fixed-length relationship feeding the RETURN, with no
   *   relationship variable, no path variable and no inline property map or WHERE on any element of the pattern: the
   *   pattern's WHERE clause is the only filter, and the step evaluates it whole;</li>
   *   <li>the optimizer planned it as a full label scan expanded through the view, possibly filtered - never an index
   *   seek, which a selective predicate on the source is better served by;</li>
   *   <li>the view is ready, not stale, and covers every vertex type the two labels can match, so enumerating its nodes
   *   finds every source the label scan would; with no label on the far end, it covers every vertex type (a
   *   transaction holding changes the view cannot see gets no view at all from the registry).</li>
   * </ul>
   * The walk always starts from the outgoing end of the relationship, where every edge type keeps its adjacency.
   *
   * @return the step producing the pattern's rows, or null to keep the optimizer's plan
   */
  AbstractExecutionStep tryGAVOneHopScan(final CommandContext context) {
    if (physicalPlan == null || statement.getWhereClause() != null || statement.getClausesInOrder() == null)
      return null;

    MatchClause matchClause = null;
    for (final ClauseEntry entry : statement.getClausesInOrder()) {
      if (entry.getType() == ClauseEntry.ClauseType.MATCH && matchClause == null)
        matchClause = entry.getTypedClause();
      else if (entry.getType() != ClauseEntry.ClauseType.RETURN)
        return null;
    }
    if (matchClause == null || matchClause.isOptional() || !matchClause.hasPathPatterns()
        || matchClause.getPathPatterns().size() != 1)
      return null;

    final PathPattern path = matchClause.getPathPatterns().getFirst();
    if (path instanceof ShortestPathPattern || path.hasPathVariable() || path.getRelationshipCount() != 1)
      return null;

    final RelationshipPattern relationship = path.getRelationship(0);
    if (relationship instanceof QuantifiedPathPattern || relationship.isVariableLength()
        || relationship.getVariable() != null || relationship.hasProperties()
        || relationship.getPropertiesParameterName() != null || relationship.hasWhereExpression())
      return null;

    final NodePattern first = path.getFirstNode();
    final NodePattern last = path.getLastNode();
    if (!isPlainNodePattern(first) || !isPlainNodePattern(last))
      return null;
    if (first.getVariable() != null && first.getVariable().equals(last.getVariable()))
      return null;

    // Walk from the outgoing end
    final boolean fromFirst = relationship.getDirection() != Direction.IN;
    final NodePattern source = fromFirst ? first : last;
    final NodePattern target = fromFirst ? last : first;
    final Vertex.DIRECTION direction =
        relationship.getDirection() == Direction.BOTH ? Vertex.DIRECTION.BOTH : Vertex.DIRECTION.OUT;

    if (!source.hasLabels())
      return null;

    // The sources come in the view's order rather than the label scan's: only where that order cannot show. Asked once
    // the pattern is known to be the one hop this step serves, since it reads the RETURN's text with a regular expression
    // and every statement the optimizer plans reaches this method on every execution (issue #9652)
    if (!CypherOptimizer.rowOrderIsInvisible(statement))
      return null;

    // The label the optimizer scans must be one of the pattern's two: the step enumerates the view by the pattern's
    // labels, so a plan scanning anything else (a rule widening or rewriting the anchor label) is not the plan it
    // replaces, and is kept
    final String scannedLabel = scannedLabelExpandedThroughView(physicalPlan.getRootOperator());
    if (scannedLabel == null || !(scannedLabel.equals(source.getFirstLabel())
        || (target.hasLabels() && scannedLabel.equals(target.getFirstLabel()))))
      return null;

    final List<String> types = relationship.getTypes();
    final String[] edgeTypes = types != null && !types.isEmpty() ? types.toArray(new String[0]) : null;
    // Several views can hold the edge types: take the first one that holds both endpoints, not the first one. Pinned for the
    // statement: the step maps, walks and maps back in one snapshot (issue #10415)
    final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProviderAllowingPartialVertexCoverage(context,
        candidate -> coveredVertexBuckets(candidate, source.getFirstLabel()) != null && (target.hasLabels()
            ? coveredVertexBuckets(candidate, target.getFirstLabel()) != null : candidate.coversVertexType(null)),
        edgeTypes);
    if (provider == null || provider.isStale())
      return null;

    final int[] sourceBuckets = coveredVertexBuckets(provider, source.getFirstLabel());
    if (sourceBuckets == null)
      return null;
    final int[] targetBuckets;
    if (target.hasLabels()) {
      targetBuckets = coveredVertexBuckets(provider, target.getFirstLabel());
      if (targetBuckets == null)
        return null;
    } else {
      if (!provider.coversVertexType(null))
        return null;
      targetBuckets = null;
    }

    final BooleanExpression where = matchClause.hasWhereClause() ? matchClause.getWhereClause().getConditionExpression() : null;
    BooleanExpression sourceFilter = null;
    BooleanExpression edgeFilter = where;
    if (where != null && source.getVariable() != null) {
      sourceFilter = WhereClause.extractForVariables(where, Set.of(source.getVariable()));
      edgeFilter = WhereClause.residualForVariables(where, Set.of(source.getVariable()));
    }

    return new GAVOneHopScanStep(provider, source.getVariable(), source.getFirstLabel(), target.getVariable(),
        target.hasLabels() ? target.getFirstLabel() : null, direction, edgeTypes, sourceFilter, edgeFilter, sourceBuckets,
        targetBuckets, context);
  }

  /** A node with at most one static label and nothing else to match: no property map, no inline WHERE. */
  private static boolean isPlainNodePattern(final NodePattern node) {
    return !node.hasProperties() && node.getPropertiesParameterName() == null && !node.hasWhereExpression()
        && !node.hasDynamicLabels() && !node.isLabelDisjunction() && (node.getLabels() == null || node.getLabels().size() <= 1);
  }

  /**
   * The label scanned when the operator tree is a full label scan expanded once through a view, with filters anywhere
   * in between - the plan {@link GAVOneHopScanStep} replaces - or null for anything else: an index seek, a second
   * expansion, a join, a plan the optimizer preferred for a reason the step cannot reproduce.
   */
  private static String scannedLabelExpandedThroughView(final PhysicalOperator root) {
    int expansions = 0;
    for (PhysicalOperator current = root; current != null; current = current.getChild()) {
      if (current instanceof GAVExpandAll || current instanceof ExpandAll)
        ++expansions;
      else if (current instanceof NodeByLabelScan scan)
        return expansions == 1 && current.getChild() == null ? scan.getLabel() : null;
      else if (!(current instanceof FilterOperator))
        return null;
    }
    return null;
  }

  /**
   * The buckets of the vertices carrying {@code label}, sub-types included, or null when the label is not a vertex
   * type or the view leaves out some of those types: enumerating the view would then miss vertices the label matches.
   */
  private int[] coveredVertexBuckets(final GraphTraversalProvider provider, final String label) {
    if (label == null || !database.getSchema().existsType(label))
      return null;
    final DocumentType type = database.getSchema().getType(label);
    if (!(type instanceof VertexType))
      return null;

    final List<DocumentType> pending = new ArrayList<>();
    pending.add(type);
    final Set<String> visited = new HashSet<>();
    while (!pending.isEmpty()) {
      final DocumentType current = pending.removeLast();
      if (!visited.add(current.getName()))
        continue;
      if (!provider.coversVertexType(current.getName()))
        return null;
      pending.addAll(current.getSubTypes());
    }

    final var buckets = type.getBuckets(true);
    final int[] ids = new int[buckets.size()];
    for (int i = 0; i < ids.length; i++)
      ids[i] = buckets.get(i).getFileId();
    return ids;
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
  AbstractExecutionStep tryOptimizeMatchCountReturn(
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
    if (relPattern.hasProperties())
      return null;
    // An inline predicate on the relationship, -[r:R WHERE r.w = 1]->, filters the edges the step would count, and the step
    // counts every edge of the anchor (issue #11584)
    if (relPattern.hasWhereExpression())
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
    // The same node at both ends, (a)-[:R]->(a), asks for the edges that close on a, its self-loops; the step counts every
    // edge of the anchor (issue #10182, the grouped sibling of #8941)
    if (sourceVar != null && sourceVar.equals(targetVar))
      return null;

    // Target node must not have property filters (would need filtering)
    if (targetNode.hasProperties())
      return null;
    // Source node must not have property filters
    if (sourceNode.hasProperties())
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
      // Reverse: anchor=target, count source's edges (reverse direction). Over an edge type declared unidirectional
      // that is the incoming side no vertex stores, which the step counts through the query's lookup (issue #8625)
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
      // The step the counts replace the expansion on must be the one that binds the anchor, and nothing but it: a
      // chain that also scans the counted side would count the anchor's edges once per row of that scan
      if (!(nodeStep instanceof MatchNodeStep matchNode) || !anchorVar.equals(matchNode.getVariable())
          || nodeStep.getPrev() instanceof MatchNodeStep)
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
    if (!CountStarShapeDetector.hasPushDownRepresentableLabel(countedNode))
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
    if (expr instanceof LabelCheckExpression labelCheck) {
      for (final Expression dynamicLabel : labelCheck.getDynamicLabels())
        if (!referencesOnlyVariable(dynamicLabel, allowedVar))
          return false;
      return referencesOnlyVariable(labelCheck.getVariableExpression(), allowedVar);
    }
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
  AbstractExecutionStep tryCreateTypeCountOptimization(final CommandContext context,
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

    // A node with no label at all is every vertex, the sum of the vertex types' counters (issue #9600). A dynamic label is
    // not "no label": it names a type only at run time.
    if (nodePattern.hasDynamicLabels())
      return null;

    // Only a single label can be counted with the O(1) countType() shortcut. A multi-label
    // conjunction pattern (n:A:B) has no single type representing "instanceOf A AND instanceOf B"
    // (each label maps to its own supertype, and the composite type also carries any other
    // labels the node was created with), so counting by the first label alone would over-count
    // (issue #5084). Multi-label (and label-disjunction) patterns fall back to the regular
    // materialization path, which filters on every label correctly.
    if (nodePattern.hasLabels() && (nodePattern.getLabels().size() != 1 || nodePattern.isLabelDisjunction()))
      return null;

    // Node must not have property constraints
    if (nodePattern.hasProperties())
      return null;

    final String variable = nodePattern.getVariable();
    final String typeName = nodePattern.hasLabels() ? nodePattern.getLabels().get(0) : null;

    // MATCH (n:Label) matches only vertices. If the label collides with an existing edge or
    // document type (labels and relationship types are separate namespaces in Cypher), the O(1)
    // countType() shortcut would wrongly count those edges/documents. Fall back to the regular
    // path (MatchNodeStep), which yields 0 rows for a non-vertex label. A non-existent type is
    // left to TypeCountStep, which already returns 0 for it (issue #5226, consistent with #5194).
    if (typeName != null && context.getDatabase().getSchema().existsType(typeName)
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
   * Optimizes {@code MATCH (n:Label) RETURN min(n.prop)} (and {@code max}) into a read of one end of the index on the
   * property (issue #8666), as SQL's {@code MIN FROM INDEX} does.
   * <p>
   * The shape is the type count's: one non-optional MATCH of one labelled node, no WHERE or property map, nothing but
   * the RETURN, whose only item is the aggregate. On top of that the index has to hold exactly the values the aggregate
   * looks at, in the order Cypher gives them:
   * <ul>
   *   <li>defined on the label itself, not inherited: a parent's index also holds the siblings' vertices;</li>
   *   <li>on that property alone, ordered, and skipping the vertices with no value (with {@code NULL_STRATEGY INDEX} its
   *   first entry can be a null, which {@code min} and {@code max} ignore);</li>
   *   <li>case sensitive, on a key type whose index order is Cypher's ({@link CypherOptimizer#INDEX_ORDERED_KEY_TYPES},
   *   the set the ORDER BY from an index already relies on).</li>
   * </ul>
   *
   * @return the step, or null when the statement or the schema is not that shape, which leaves the aggregate to scan
   */
  AbstractExecutionStep tryCreateIndexMinMaxOptimization(final CommandContext context) {
    if (statement.getMatchClauses() == null || statement.getMatchClauses().size() != 1)
      return null;
    final MatchClause matchClause = statement.getMatchClauses().getFirst();
    // A WHERE is taken only as a range of the aggregated property (issue #8812), checked below once the property is known
    if (matchClause.isOptional() || (matchClause.hasWhereClause() && statement.getWhereClause() != null)
        || !matchClause.hasPathPatterns() || matchClause.getPathPatterns().size() != 1)
      return null;
    final WhereClause whereClause = matchClause.hasWhereClause() ? matchClause.getWhereClause() : statement.getWhereClause();
    final PathPattern pathPattern = matchClause.getPathPatterns().getFirst();
    if (!pathPattern.isSingleNode())
      return null;
    final NodePattern nodePattern = pathPattern.getFirstNode();
    if (nodePattern.getVariable() == null || !nodePattern.hasLabels() || nodePattern.getLabels().size() != 1
        || nodePattern.isLabelDisjunction() || nodePattern.hasProperties())
      return null;

    // The statement is the MATCH and the RETURN, nothing else (a write, a WITH or an UNWIND would be dropped)
    if (!plan.isMatchReturnOnlyStatement() || !statement.getWithClauses().isEmpty() || !statement.getUnwindClauses().isEmpty())
      return null;

    final ReturnClause returnClause = statement.getReturnClause();
    if (returnClause == null || returnClause.isDistinct() || returnClause.getReturnItems().isEmpty())
      return null;

    final String typeName = nodePattern.getLabels().getFirst();
    final Schema schema = context.getDatabase().getSchema();
    if (!schema.existsType(typeName) || !(schema.getType(typeName) instanceof VertexType type))
      return null;

    // Several aggregates of the whole label, RETURN min(n.k), max(n.k): each one is read from its own index (issue #12284)
    if (returnClause.getReturnItems().size() > 1) {
      if (whereClause != null)
        return null;
      final List<String> propertyNames = new ArrayList<>();
      final List<Boolean> maxes = new ArrayList<>();
      final List<String> aliases = new ArrayList<>();
      for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
        final PropertyAccessExpression access = indexedMinMaxArgument(item, nodePattern, type);
        if (access == null)
          return null;
        propertyNames.add(access.getPropertyName());
        maxes.add("max".equalsIgnoreCase(((FunctionCallExpression) item.getExpression()).getFunctionName()));
        aliases.add(item.getOutputName());
      }
      return new IndexMinMaxStep(typeName, propertyNames, maxes, aliases, context);
    }

    final ReturnClause.ReturnItem returnItem = returnClause.getReturnItems().getFirst();
    final PropertyAccessExpression access = indexedMinMaxArgument(returnItem, nodePattern, type);
    if (access == null)
      return null;
    final boolean max = "max".equalsIgnoreCase(((FunctionCallExpression) returnItem.getExpression()).getFunctionName());
    final String propertyName = access.getPropertyName();

    if (whereClause == null)
      return new IndexMinMaxStep(typeName, propertyName, max, returnItem.getOutputName(), context);

    final BooleanExpression condition = whereClause.getConditionExpression();
    final List<RangePredicate> range = new ArrayList<>(2);
    if (condition == null || !collectRangeOn(condition, nodePattern.getVariable(), propertyName, range) || range.isEmpty())
      return null;
    return new IndexMinMaxStep(typeName, propertyName, max, returnItem.getOutputName(), nodePattern.getVariable(), range,
        condition, context);
  }

  /**
   * The property of {@code item} when it is a {@code min(n.p)} or {@code max(n.p)} of the MATCH variable whose answer is one end of
   * the index on p, else null: an ordered LSM index on p alone, defined on the label, that skips the vertices with no value and holds
   * its keys case sensitive, on a key type whose index order is Cypher's.
   */
  private static PropertyAccessExpression indexedMinMaxArgument(final ReturnClause.ReturnItem item, final NodePattern nodePattern,
      final VertexType type) {
    if (!(item.getExpression() instanceof FunctionCallExpression function) || function.getArguments().size() != 1
        || !(function.getArguments().getFirst() instanceof PropertyAccessExpression access)
        || !nodePattern.getVariable().equals(access.getVariableName()))
      return null;
    if (!"max".equalsIgnoreCase(function.getFunctionName()) && !"min".equalsIgnoreCase(function.getFunctionName()))
      return null;

    final String propertyName = access.getPropertyName();
    final Property property = type.getPolymorphicPropertyIfExists(propertyName);
    if (property == null || !CypherOptimizer.INDEX_ORDERED_KEY_TYPES.contains(property.getType()))
      return null;

    final TypeIndex index = type.getIndexByProperties(propertyName);
    if (!(index instanceof RangeIndex) || !index.isReadyForQueries())
      return null;
    // The metadata is read once (issue #9332) and the whole check is guarded: a concurrent DROP INDEX can take the index
    // away between any two of these reads, and the planner then answers as it does for an index that is not there
    try {
      if (!index.supportsOrderedIterations() || index.getType() != Schema.INDEX_TYPE.LSM_TREE
          || index.getNullStrategy() == LSMTreeIndexAbstract.NULL_STRATEGY.INDEX)
        return null;
      final IndexMetadata metadata = index.getMetadata();
      if (metadata == null || metadata.isCaseInsensitive(0))
        return null;
    } catch (final IndexException e) {
      return null;
    }
    return access;
  }

  /**
   * Collects the bounds of a WHERE that is nothing but {@code variable.property <, <=, >, >= literal-or-parameter}, joined
   * by AND (the comparison may be written the other way round, {@code 5 < v.a}).
   *
   * @return false when any part of the condition is something else
   */
  private static boolean collectRangeOn(final BooleanExpression condition, final String variable, final String property,
      final List<RangePredicate> range) {
    if (condition instanceof LogicalExpression logical)
      return logical.getOperator() == LogicalExpression.Operator.AND && logical.getRight() != null
          && collectRangeOn(logical.getLeft(), variable, property, range)
          && collectRangeOn(logical.getRight(), variable, property, range);
    if (!(condition instanceof ComparisonExpression comparison))
      return false;

    ComparisonExpression.Operator operator = comparison.getOperator();
    final Expression valueSide;
    if (isPropertyOf(comparison.getLeft(), variable, property))
      valueSide = comparison.getRight();
    else if (isPropertyOf(comparison.getRight(), variable, property)) {
      valueSide = comparison.getLeft();
      operator = switch (operator) {
        case LESS_THAN -> ComparisonExpression.Operator.GREATER_THAN;
        case GREATER_THAN -> ComparisonExpression.Operator.LESS_THAN;
        case LESS_THAN_OR_EQUAL -> ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
        case GREATER_THAN_OR_EQUAL -> ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
        default -> operator;
      };
    } else
      return false;

    if (operator != ComparisonExpression.Operator.LESS_THAN && operator != ComparisonExpression.Operator.GREATER_THAN
        && operator != ComparisonExpression.Operator.LESS_THAN_OR_EQUAL
        && operator != ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL)
      return false;

    if (valueSide instanceof LiteralExpression literal && literal.getValue() != null)
      range.add(new RangePredicate(property, operator, literal.getValue(), false));
    else if (valueSide instanceof ParameterExpression parameter)
      range.add(new RangePredicate(property, operator, parameter.getParameterName(), true));
    else
      return false;
    return true;
  }

  private static boolean isPropertyOf(final Expression expression, final String variable, final String property) {
    return expression instanceof PropertyAccessExpression access && variable.equals(access.getVariableName())
        && property.equals(access.getPropertyName());
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
      return plan.rowCountAlias();

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
}
