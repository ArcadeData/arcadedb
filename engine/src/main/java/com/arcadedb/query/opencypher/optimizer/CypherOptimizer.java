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
package com.arcadedb.query.opencypher.optimizer;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.executor.operators.CartesianProduct;
import com.arcadedb.query.opencypher.executor.operators.ExpandAll;
import com.arcadedb.query.opencypher.executor.operators.ExpandInto;
import com.arcadedb.query.opencypher.executor.operators.FilterOperator;
import com.arcadedb.query.opencypher.executor.operators.GAVExpandAll;
import com.arcadedb.query.opencypher.executor.operators.GAVExpandInto;
import com.arcadedb.query.opencypher.executor.operators.GAVFusedChainOperator;
import com.arcadedb.query.opencypher.executor.operators.PhysicalOperator;
import com.arcadedb.query.opencypher.optimizer.plan.*;
import com.arcadedb.query.opencypher.optimizer.rules.*;
import com.arcadedb.query.opencypher.optimizer.statistics.CostModel;
import com.arcadedb.query.opencypher.optimizer.statistics.StatisticsProvider;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Main Cost-Based Query Optimizer for ArcadeDB Native Cypher.
 * <p>
 * Transforms logical plans (AST) into optimized physical execution plans
 * using runtime statistics and cost estimation.
 * <p>
 * Optimization Flow:
 * <pre>
 * 1. Build LogicalPlan from CypherStatement (AST)
 * 2. Collect runtime statistics (type counts, indexes)
 * 3. Select anchor node (best starting point)
 * 4. Apply optimization rules:
 *    - IndexSelectionRule (seek vs scan)
 *    - FilterPushdownRule (move filters closer to data)
 *    - ExpandIntoRule (semi-join for bounded patterns)
 *    - JoinOrderRule (reorder expansions by cardinality)
 * 5. Build PhysicalPlan (tree of physical operators)
 * </pre>
 * <p>
 * Expected Performance Improvements:
 * - Index selection: 10-100x speedup for selective queries
 * - ExpandInto: 5-10x speedup for bounded patterns
 * - Join ordering: 10-100x speedup for complex patterns
 * - Filter pushdown: 2-10x speedup depending on selectivity
 */
public class CypherOptimizer {
  // TODO: replace with runtime statistics once the statistics provider tracks per-type average degree
  private static final double DEFAULT_AVG_DEGREE = 10.0;
  // TODO: replace with runtime statistics once histogram-based selectivity estimation is implemented
  private static final double DEFAULT_EXPAND_INTO_SELECTIVITY = 0.1;
  private static final double DEFAULT_FILTER_SELECTIVITY = 0.5;

  private final DatabaseInternal database;
  private final CypherStatement statement;
  private final Map<String, Object> parameters;

  private final StatisticsProvider statisticsProvider;
  private final CostModel costModel;
  private final AnchorSelector anchorSelector;

  private final List<OptimizationRule> rules;

  public CypherOptimizer(final DatabaseInternal database,
                        final CypherStatement statement,
                        final Map<String, Object> parameters) {
    this.database = database;
    this.statement = statement;
    this.parameters = parameters;

    // Initialize statistics and cost model
    this.statisticsProvider = new StatisticsProvider(database);
    this.costModel = new CostModel(statisticsProvider);
    this.anchorSelector = new AnchorSelector(statisticsProvider, costModel);

    // Initialize optimization rules (in priority order)
    this.rules = new ArrayList<>();
    this.rules.add(new IndexSelectionRule(statisticsProvider, costModel));
    this.rules.add(new FilterPushdownRule());
    this.rules.add(new ExpandIntoRule());
    this.rules.add(new JoinOrderRule(statisticsProvider, costModel));
  }

  /**
   * Optimizes the Cypher query and produces a physical execution plan.
   *
   * @return optimized physical plan
   */
  public PhysicalPlan optimize() {
    // 1. Build logical plan from AST
    final LogicalPlan logicalPlan = buildLogicalPlan();

    // 2. Collect runtime statistics
    final List<String> typeNames = extractTypeNames(logicalPlan);
    statisticsProvider.collectStatistics(typeNames);

    // Handle multiple independent MATCH clauses (e.g., MATCH (a:T) MATCH (b:T) CREATE ...)
    // Each MATCH has a single node pattern — create operators for each and chain with CartesianProduct
    if (statement.getMatchClauses().size() > 1 && logicalPlan.getRelationships().isEmpty()) {
      return optimizeMultiMatchIndependent(logicalPlan);
    }

    // 3. Select anchor node (best starting point)
    final AnchorSelection anchor = anchorSelector.selectAnchor(logicalPlan);

    // 4. Create anchor operator (index seek or scan)
    final PhysicalOperator anchorOperator = createAnchorOperator(anchor);

    // 5. Build expansion chain (ordered by cardinality)
    PhysicalOperator rootOperator = anchorOperator;
    if (!logicalPlan.getRelationships().isEmpty()) {
      rootOperator = buildExpansionChain(logicalPlan, anchor, anchorOperator);
    }

    // 6. Apply ExpandInto optimization
    // ExpandInto is detected during expansion chain building (step 5)
    // and applied automatically when both endpoints are bound

    // 7. Push down filters
    if (!logicalPlan.getWhereFilters().isEmpty()) {
      rootOperator = applyFilterPushdown(logicalPlan, rootOperator);
    }

    // 8. Fuse consecutive GAVExpandAll operators into a single GAVFusedChainOperator
    // This eliminates ALL intermediate ResultInternal/HashMap/Vertex allocations
    rootOperator = fuseGAVExpandChain(rootOperator);

    // 9. Defer vertex loading for remaining (non-fused) intermediate GAVExpandAll hops
    applyDeferredVertexLoading(rootOperator);

    // 10. Calculate total cost and cardinality
    final double totalCost = rootOperator.getEstimatedCost();
    final long totalCardinality = rootOperator.getEstimatedCardinality();

    // 11. Build physical plan with complete operator tree
    return new PhysicalPlan(logicalPlan, anchor, rootOperator, totalCost, totalCardinality);
  }

  /**
   * Optimizes multiple independent MATCH clauses by creating an operator per node
   * and chaining them with CartesianProduct.
   * This is optimal for the common edge creation pattern:
   * MATCH (a:T) WHERE a.id=$x MATCH (b:T) WHERE b.id=$y CREATE (a)-[:E]->(b)
   */
  private PhysicalPlan optimizeMultiMatchIndependent(final LogicalPlan logicalPlan) {
    PhysicalOperator rootOperator = null;
    AnchorSelection firstAnchor = null;
    double totalCost = 0;
    long totalCardinality = 1;

    for (final LogicalNode node : logicalPlan.getNodes().values()) {
      // Create a temporary single-node plan to use the anchor selector
      final AnchorSelection anchor = anchorSelector.evaluateNodeDirect(node, logicalPlan);
      final PhysicalOperator nodeOperator = createAnchorOperator(anchor);

      if (firstAnchor == null)
        firstAnchor = anchor;

      if (rootOperator == null) {
        rootOperator = nodeOperator;
      } else {
        // Chain with CartesianProduct
        totalCardinality *= anchor.getEstimatedCardinality();
        totalCost += anchor.getEstimatedCost();
        rootOperator = new CartesianProduct(rootOperator, nodeOperator, totalCost, totalCardinality);
      }

      totalCost += anchor.getEstimatedCost();
      totalCardinality = Math.max(1, anchor.getEstimatedCardinality());
    }

    // Apply filters
    if (!logicalPlan.getWhereFilters().isEmpty())
      rootOperator = applyFilterPushdown(logicalPlan, rootOperator);

    return new PhysicalPlan(logicalPlan, firstAnchor, rootOperator,
        rootOperator.getEstimatedCost(), rootOperator.getEstimatedCardinality());
  }

  /**
   * Builds a logical plan from the Cypher AST.
   *
   * @return logical plan
   */
  private LogicalPlan buildLogicalPlan() {
    // Extract nodes, relationships, filters, and returns from AST
    return LogicalPlan.fromAST(statement);
  }

  /**
   * Extracts type names from the logical plan for statistics collection.
   *
   * @param plan the logical plan
   * @return list of type names
   */
  private List<String> extractTypeNames(final LogicalPlan plan) {
    final List<String> typeNames = new ArrayList<>();

    // Collect vertex type names
    for (final LogicalNode node : plan.getNodes().values()) {
      final String label = node.getFirstLabel();
      if (label != null) {
        typeNames.add(label);
      }
    }

    // Collect edge type names
    plan.getRelationships().forEach(rel -> {
      if (rel.getTypes() != null) {
        typeNames.addAll(rel.getTypes());
      }
    });

    return typeNames;
  }

  /**
   * Creates the appropriate physical operator for the anchor node.
   *
   * @param anchor the selected anchor
   * @return physical operator (NodeIndexSeek or NodeByLabelScan)
   */
  private PhysicalOperator createAnchorOperator(final AnchorSelection anchor) {
    final IndexSelectionRule indexRule = (IndexSelectionRule) rules.get(0);
    return indexRule.createAnchorOperator(anchor);
  }

  /**
   * Builds the expansion chain by ordering relationships and creating expand operators.
   * Uses JoinOrderRule for ordering and ExpandIntoRule for optimization.
   *
   * @param logicalPlan    the logical plan
   * @param anchor         the selected anchor
   * @param anchorOperator the anchor operator to build upon
   * @return root operator of the expansion chain
   */
  private PhysicalOperator buildExpansionChain(final LogicalPlan logicalPlan,
                                                final AnchorSelection anchor,
                                                final PhysicalOperator anchorOperator) {
    // Get optimization rules
    final JoinOrderRule joinOrderRule = (JoinOrderRule) rules.get(3);
    final ExpandIntoRule expandIntoRule = (ExpandIntoRule) rules.get(2);

    // Determine which variables are bound
    final Set<String> boundVariables = expandIntoRule.determineBoundVariables(
        logicalPlan, anchor.getVariable());

    // Order relationships by estimated cardinality
    final List<LogicalRelationship> orderedRels =
        joinOrderRule.orderRelationships(
            logicalPlan.getRelationships(),
            anchor.getVariable(),
            boundVariables,
            logicalPlan
        );

    // Build operator chain
    PhysicalOperator currentOp = anchorOperator;

    for (final LogicalRelationship rel : orderedRels) {
      // Check if we should use ExpandInto (both endpoints bound)
      if (expandIntoRule.shouldUseExpandInto(rel, boundVariables)) {
        // Use ExpandInto for bounded patterns
        currentOp = createExpandIntoOperator(rel, currentOp, boundVariables);
      } else {
        // Use ExpandAll for unbounded patterns
        currentOp = createExpandAllOperator(rel, currentOp, boundVariables);

        // Add label filter for target vertex if required
        currentOp = addTargetLabelFilter(logicalPlan, rel, currentOp);
      }

      // Update bound variables after expansion
      boundVariables.add(rel.getSourceVariable());
      boundVariables.add(rel.getTargetVariable());
    }

    return currentOp;
  }

  /**
   * Creates an ExpandAll operator for relationship traversal.
   * Determines which variable is already bound and adjusts direction accordingly.
   *
   * @param relationship   the logical relationship
   * @param input          the input operator
   * @param boundVariables the currently bound variables
   * @return ExpandAll operator
   */
  private PhysicalOperator createExpandAllOperator(
      final LogicalRelationship relationship,
      final PhysicalOperator input,
      final Set<String> boundVariables) {
    // Extract parameters from relationship
    String sourceVariable = relationship.getSourceVariable();
    final String edgeVariable = relationship.getVariable();
    String targetVariable = relationship.getTargetVariable();
    Direction direction = relationship.getDirection();
    final String[] edgeTypes = relationship.getTypes().toArray(new String[0]);

    // Determine which variable is bound and adjust direction if needed
    final boolean sourceIsBound = boundVariables.contains(sourceVariable);
    final boolean targetIsBound = boundVariables.contains(targetVariable);

    if (targetIsBound && !sourceIsBound) {
      // Anchor is the target, need to reverse direction
      // Swap source and target
      final String temp = sourceVariable;
      sourceVariable = targetVariable;
      targetVariable = temp;

      // Reverse direction
      direction = reverseDirection(direction);
    }

    // Estimate cost and cardinality for this expansion
    final long inputCardinality = input.getEstimatedCardinality();
    final long outputCardinality = inputCardinality * (long) DEFAULT_AVG_DEGREE;
    final double expansionCost = inputCardinality * DEFAULT_AVG_DEGREE * costModel.EXPAND_COST_PER_ROW;
    final double totalCost = input.getEstimatedCost() + expansionCost;

    // Check for GAV provider: use CSR-backed expand when edge variable is not captured
    if (edgeVariable == null) {
      final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(database, edgeTypes);
      if (provider != null) {
        // GAV expand: ~10x cheaper (array access vs linked list traversal)
        final double gavCost = input.getEstimatedCost() + inputCardinality * DEFAULT_AVG_DEGREE * costModel.EXPAND_COST_PER_ROW * 0.1;
        return new GAVExpandAll(input, provider, sourceVariable, targetVariable, direction, edgeTypes,
            gavCost, outputCardinality);
      }
    }

    return new ExpandAll(
        input,
        sourceVariable,
        edgeVariable,
        targetVariable,
        direction,
        edgeTypes,
        totalCost,
        outputCardinality
    );
  }

  /**
   * Reverses a relationship direction.
   * OUT → IN, IN → OUT, BOTH → BOTH
   */
  private Direction reverseDirection(
      final Direction direction) {
    return switch (direction) {
      case OUT -> Direction.IN;
      case IN -> Direction.OUT;
      case BOTH -> Direction.BOTH;
    };
  }

  /**
   * Creates an ExpandInto operator for bounded pattern checking.
   *
   * @param relationship    the logical relationship
   * @param input           the input operator
   * @param boundVariables  the currently bound variables
   * @return ExpandInto operator
   */
  private PhysicalOperator createExpandIntoOperator(
      final LogicalRelationship relationship,
      final PhysicalOperator input,
      final Set<String> boundVariables) {
    // Extract parameters from relationship
    final String sourceVariable = relationship.getSourceVariable();
    final String targetVariable = relationship.getTargetVariable();
    final String edgeVariable = relationship.getVariable();
    final Direction direction = relationship.getDirection();
    final String[] edgeTypes = relationship.getTypes().toArray(new String[0]);

    // Estimate cost and cardinality for ExpandInto
    // ExpandInto is much cheaper than ExpandAll because it's just an existence check
    final long inputCardinality = input.getEstimatedCardinality();
    final double selectivity = DEFAULT_EXPAND_INTO_SELECTIVITY;
    final long outputCardinality = (long) (inputCardinality * selectivity);
    final double expandIntoCost = inputCardinality * 1.0; // O(1) per input row for existence check
    final double totalCost = input.getEstimatedCost() + expandIntoCost;

    // Check for GAV provider: use CSR-backed expand-into when edge variable is not captured
    if (edgeVariable == null) {
      final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(database, edgeTypes);
      if (provider != null) {
        // GAV expand-into: binary search on CSR arrays, no edge loading
        final double gavCost = input.getEstimatedCost() + inputCardinality * 0.1;
        return new GAVExpandInto(input, provider, sourceVariable, targetVariable, direction, edgeTypes,
            gavCost, outputCardinality);
      }
    }

    return new ExpandInto(
        input,
        sourceVariable,
        targetVariable,
        edgeVariable,
        direction,
        edgeTypes,
        totalCost,
        outputCardinality
    );
  }

  /**
   * Applies filter pushdown optimization by wrapping operators with FilterOperator.
   *
   * @param logicalPlan the logical plan
   * @param rootOperator the root operator to wrap
   * @return root operator with filters applied
   */
  private PhysicalOperator applyFilterPushdown(final LogicalPlan logicalPlan,
                                                final PhysicalOperator rootOperator) {
    // Wrap the root operator with a FilterOperator for each WHERE clause
    // In a full implementation, we would analyze which filters can be pushed down
    // to lower operators (closer to data sources) for better performance.
    // For now, we apply all filters at the top level.

    PhysicalOperator currentOp = rootOperator;

    for (final WhereClause whereClause : logicalPlan.getWhereFilters()) {
      final BooleanExpression filterExpression = whereClause.getConditionExpression();

      if (filterExpression != null) {
        // Estimate cost and cardinality for this filter
        final long inputCardinality = currentOp.getEstimatedCardinality();
        final double selectivity = DEFAULT_FILTER_SELECTIVITY;
        final long outputCardinality = (long) (inputCardinality * selectivity);
        final double filterCost = inputCardinality * costModel.FILTER_COST_PER_ROW;
        final double totalCost = currentOp.getEstimatedCost() + filterCost;

        // Wrap with FilterOperator
        currentOp = new FilterOperator(
            currentOp,
            filterExpression,
            totalCost,
            outputCardinality
        );
      }
    }

    return currentOp;
  }

  /**
   * Detects chains of consecutive GAVExpandAll operators and fuses them into a single
   * GAVFusedChainOperator. This eliminates ALL intermediate ResultInternal allocations
   * (HashMap, Vertex objects) during multi-hop traversal — the chain runs entirely
   * with int nodeIds from CSR arrays.
   * <p>
   * Requires at least 2 consecutive GAVExpandAll operators sharing the same provider.
   */
  private PhysicalOperator fuseGAVExpandChain(PhysicalOperator rootOperator) {
    // Walk from root down to collect the GAVExpandAll chain
    final List<GAVExpandAll> chain = new ArrayList<>();
    PhysicalOperator current = rootOperator;

    // Skip filters at the top (they sit above the expand chain)
    PhysicalOperator topFilter = null;
    if (current instanceof FilterOperator) {
      topFilter = current;
      current = ((FilterOperator) current).getChild();
    }

    while (current instanceof GAVExpandAll) {
      chain.add((GAVExpandAll) current);
      current = current.getChild();
    }

    // Need at least 2 GAVExpandAll for fusion to be worthwhile
    if (chain.size() < 2)
      return rootOperator;

    // Verify all use the same provider
    final GraphTraversalProvider provider = chain.get(0).getProvider();
    for (int i = 1; i < chain.size(); i++)
      if (chain.get(i).getProvider() != provider)
        return rootOperator;

    // chain[0] is the outermost (root), chain[last] is the innermost (closest to scan)
    // Reverse to get traversal order: innermost first
    final int chainLen = chain.size();

    // Build hop arrays (in traversal order: innermost → outermost)
    final Vertex.DIRECTION[] hopDirs = new Vertex.DIRECTION[chainLen];
    final String[][] hopEdgeTypes = new String[chainLen][];
    final String[] hopTargetVars = new String[chainLen];
    final int[][] hopTargetBuckets = new int[chainLen][];

    for (int i = 0; i < chainLen; i++) {
      final GAVExpandAll gav = chain.get(chainLen - 1 - i); // reverse order
      hopDirs[i] = gav.getDirection().toArcadeDirection();
      hopEdgeTypes[i] = gav.getEdgeTypes();
      hopTargetVars[i] = gav.getTargetVariable();

      // Resolve target label to bucket IDs for fast filtering
      final String targetLabel = gav.getTargetLabel();
      if (targetLabel != null && database.getSchema().existsType(targetLabel)) {
        final var buckets = database.getSchema().getType(targetLabel).getBuckets(false);
        final int[] ids = new int[buckets.size()];
        for (int b = 0; b < buckets.size(); b++)
          ids[b] = buckets.get(b).getFileId();
        hopTargetBuckets[i] = ids;
      }
    }

    // Source variable = innermost GAVExpandAll's source variable
    final String sourceVar = chain.get(chainLen - 1).getSourceVariable();

    // Determine which variables need materialization (referenced in WHERE/WITH/RETURN)
    final Set<String> usedVariables = new HashSet<>();
    if (statement.getWhereClause() != null && statement.getWhereClause().getConditionExpression() != null)
      usedVariables.addAll(WhereClause.collectVariables(statement.getWhereClause().getConditionExpression()));
    for (final MatchClause mc : statement.getMatchClauses())
      if (mc.hasWhereClause() && mc.getWhereClause().getConditionExpression() != null)
        usedVariables.addAll(WhereClause.collectVariables(mc.getWhereClause().getConditionExpression()));
    if (statement.getReturnClause() != null)
      for (final var item : statement.getReturnClause().getReturnItems())
        collectExpressionVariables(item.getExpression(), usedVariables);
    for (final var wc : statement.getWithClauses())
      for (final var item : wc.getItems())
        collectExpressionVariables(item.getExpression(), usedVariables);

    final boolean[] materialize = new boolean[chainLen + 1];
    materialize[0] = usedVariables.contains(sourceVar); // source
    for (int i = 0; i < chainLen; i++)
      materialize[i + 1] = hopTargetVars[i] != null && usedVariables.contains(hopTargetVars[i]);

    // The child of the innermost GAVExpandAll is the data source (e.g., NodeByLabelScan)
    final PhysicalOperator dataSource = current;

    final GAVFusedChainOperator fused = new GAVFusedChainOperator(
        dataSource, provider, sourceVar,
        hopDirs, hopEdgeTypes, hopTargetVars, hopTargetBuckets, materialize,
        rootOperator.getEstimatedCost() * 0.5, // fusion is cheaper
        rootOperator.getEstimatedCardinality());

    // Push filter INTO the fused chain (evaluated via column store, before creating output objects)
    if (topFilter instanceof FilterOperator) {
      fused.setPushedFilter(((FilterOperator) topFilter).getPredicate());
      // No need for a separate FilterOperator on top — the fused chain handles it
    }
    return fused;
  }

  /**
   * Marks intermediate GAVExpandAll operators for deferred vertex loading.
   * A variable is "intermediate" if it is not referenced in any WHERE, WITH, or RETURN expression —
   * it only serves as a stepping stone for further traversal.
   * Skipping the OLTP vertex load for these variables can save ~2μs per row.
   */
  private void applyDeferredVertexLoading(final PhysicalOperator rootOperator) {
    // Collect variables referenced in WHERE/WITH/RETURN expressions
    final Set<String> usedVariables = new HashSet<>();
    // From WHERE
    if (statement.getWhereClause() != null && statement.getWhereClause().getConditionExpression() != null)
      usedVariables.addAll(WhereClause.collectVariables(statement.getWhereClause().getConditionExpression()));
    for (final MatchClause mc : statement.getMatchClauses())
      if (mc.hasWhereClause() && mc.getWhereClause().getConditionExpression() != null)
        usedVariables.addAll(WhereClause.collectVariables(mc.getWhereClause().getConditionExpression()));
    // From RETURN
    if (statement.getReturnClause() != null)
      for (final var item : statement.getReturnClause().getReturnItems())
        collectExpressionVariables(item.getExpression(), usedVariables);
    // From WITH
    for (final var wc : statement.getWithClauses())
      for (final var item : wc.getItems())
        collectExpressionVariables(item.getExpression(), usedVariables);

    // Walk the operator tree and mark GAVExpandAll operators for deferred loading
    // The root operator's target should NOT be deferred (it's the final output)
    markDeferredRecursive(rootOperator, usedVariables, true);
  }

  private void markDeferredRecursive(final PhysicalOperator op, final Set<String> usedVariables, final boolean isRoot) {
    if (op instanceof GAVExpandAll) {
      final GAVExpandAll gav = (GAVExpandAll) op;
      if (!isRoot) {
        final String targetVar = gav.getTargetVariable();
        if (targetVar != null && !usedVariables.contains(targetVar))
          gav.setDeferTargetLoad(true);
      }
      if (gav.getChild() != null)
        markDeferredRecursive(gav.getChild(), usedVariables, false);
    } else if (op instanceof FilterOperator) {
      if (((FilterOperator) op).getChild() != null)
        markDeferredRecursive(((FilterOperator) op).getChild(), usedVariables, false);
    }
  }

  private static void collectExpressionVariables(final Expression expr, final Set<String> vars) {
    final String text = expr.getText();
    // Extract variable references: "var.prop" → "var", bare "var" → "var"
    for (final String token : text.split("[^a-zA-Z0-9_.]")) {
      if (!token.isEmpty()) {
        final int dot = token.indexOf('.');
        vars.add(dot >= 0 ? token.substring(0, dot) : token);
      }
    }
  }

  /**
   * Adds a label filter for the target vertex if the logical plan specifies a label.
   * This ensures that after relationship expansion, we only keep vertices of the correct type.
   *
   * @param logicalPlan the logical plan
   * @param rel         the logical relationship
   * @param input       the input operator
   * @return operator with label filter applied (or original if no filter needed)
   */
  private PhysicalOperator addTargetLabelFilter(final LogicalPlan logicalPlan,
                                                 final LogicalRelationship rel,
                                                 final PhysicalOperator input) {
    // Determine the actual expand target variable (may differ from rel.getTargetVariable()
    // when the optimizer reverses traversal direction)
    final String expandTargetVariable;
    if (input instanceof GAVExpandAll)
      expandTargetVariable = ((GAVExpandAll) input).getTargetVariable();
    else if (input instanceof ExpandAll)
      expandTargetVariable = ((ExpandAll) input).getTargetVariable();
    else
      expandTargetVariable = rel.getTargetVariable();

    // Look up the LogicalNode for the expand target variable
    final LogicalNode targetNode =
        logicalPlan.getNodes().get(expandTargetVariable);

    if (targetNode == null || !targetNode.hasLabels())
      return input;

    // Get the first label (for now we only support single labels)
    final String targetLabel = targetNode.getFirstLabel();

    if (targetLabel == null)
      return input;

    // Push label filter directly into the expand operator for early filtering
    // instead of wrapping with a separate FilterOperator (avoids materializing
    // rows that will be immediately discarded)
    if (input instanceof GAVExpandAll) {
      ((GAVExpandAll) input).setTargetLabel(targetLabel);
      return input;
    }
    if (input instanceof ExpandAll) {
      ((ExpandAll) input).setTargetLabel(targetLabel);
      return input;
    }

    // Fallback: wrap with FilterOperator for other operator types
    final BooleanExpression labelFilter = new BooleanExpression() {
      @Override
      public boolean evaluate(final Result result,
                              final CommandContext context) {
        final Object vertexObj = result.getProperty(expandTargetVariable);
        if (vertexObj instanceof Vertex) {
          final Vertex vertex = (Vertex) vertexObj;
          return vertex.getType().instanceOf(targetLabel);
        }
        return false;
      }

      @Override
      public String getText() {
        return "labels(" + expandTargetVariable + ") = ['" + targetLabel + "']";
      }
    };

    final long inputCardinality = input.getEstimatedCardinality();
    final double filterCost = inputCardinality * costModel.FILTER_COST_PER_ROW;
    final double totalCost = input.getEstimatedCost() + filterCost;

    return new FilterOperator(
        input,
        labelFilter,
        totalCost,
        inputCardinality
    );
  }

  /**
   * Applies all optimization rules to the plan.
   *
   * @param logicalPlan  the logical plan
   * @param physicalPlan the initial physical plan
   * @return optimized physical plan
   */
  private PhysicalPlan applyOptimizationRules(final LogicalPlan logicalPlan, PhysicalPlan physicalPlan) {
    for (final OptimizationRule rule : rules) {
      if (rule.isApplicable(logicalPlan)) {
        physicalPlan = rule.apply(logicalPlan, physicalPlan);
      }
    }
    return physicalPlan;
  }

  public StatisticsProvider getStatisticsProvider() {
    return statisticsProvider;
  }

  public CostModel getCostModel() {
    return costModel;
  }

  public AnchorSelector getAnchorSelector() {
    return anchorSelector;
  }

  public List<OptimizationRule> getRules() {
    return rules;
  }
}
