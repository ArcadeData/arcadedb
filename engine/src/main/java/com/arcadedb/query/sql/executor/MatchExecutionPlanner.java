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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.Bucket;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.GroupBy;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.Limit;
import com.arcadedb.query.sql.parser.MatchExpression;
import com.arcadedb.query.sql.parser.MatchFilter;
import com.arcadedb.query.sql.parser.MatchPathItem;
import com.arcadedb.query.sql.parser.MatchStatement;
import com.arcadedb.query.sql.parser.MultiMatchPathItem;
import com.arcadedb.query.sql.parser.NestedProjection;
import com.arcadedb.query.sql.parser.OrderBy;
import com.arcadedb.query.sql.parser.Pattern;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;
import com.arcadedb.query.sql.parser.Rid;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Skip;
import com.arcadedb.query.sql.parser.Unwind;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.Pair;

import java.util.*;
import java.util.stream.*;

/**
 * Created by luigidellaquila on 20/09/16.
 */
public class MatchExecutionPlanner {

  static final String DEFAULT_ALIAS_PREFIX = "$ARCADEDB_DEFAULT_ALIAS_";

  protected final List<MatchExpression>  matchExpressions;
  protected final List<MatchExpression>  notMatchExpressions;
  protected final List<Expression>       returnItems;
  protected final List<Identifier>       returnAliases;
  protected final List<NestedProjection> returnNestedProjections;
  final           boolean                returnElements;
  final           boolean                returnPaths;
  final           boolean                returnPatterns;
  final           boolean                returnPathElements;
  final           boolean                returnDistinct;
  protected final Skip                   skip;
  private final   GroupBy                groupBy;
  private final   OrderBy                orderBy;
  private final   Unwind                 unwind;
  protected final Limit                  limit;

  //post-parsing
  private Pattern                  pattern;
  private List<Pattern>            subPatterns;
  private Map<String, WhereClause> aliasFilters;
  private Map<String, String>      aliasTypes;
  private Map<String, String>      aliasBuckets;
  private Map<String, Rid>         aliasRids;
  boolean foundOptional = false;
  private static final long threshold = 100;

  public MatchExecutionPlanner(final MatchStatement stm) {
    this.matchExpressions = stm.getMatchExpressions().stream().map(x -> x.copy()).collect(Collectors.toList());
    this.notMatchExpressions = stm.getNotMatchExpressions().stream().map(x -> x.copy()).collect(Collectors.toList());
    this.returnItems = stm.getReturnItems().stream().map(x -> x.copy()).collect(Collectors.toList());
    this.returnAliases = stm.getReturnAliases().stream().map(x -> x == null ? null : x.copy()).collect(Collectors.toList());
    this.returnNestedProjections = stm.getReturnNestedProjections().stream().map(x -> x == null ? null : x.copy())
        .collect(Collectors.toList());
    this.limit = stm.getLimit() == null ? null : stm.getLimit().copy();
    this.skip = stm.getSkip() == null ? null : stm.getSkip().copy();

    this.returnElements = stm.returnsElements();
    this.returnPaths = stm.returnsPaths();
    this.returnPatterns = stm.returnsPatterns();
    this.returnPathElements = stm.returnsPathElements();
    this.returnDistinct = stm.isReturnDistinct();
    this.groupBy = stm.getGroupBy() == null ? null : stm.getGroupBy().copy();
    this.orderBy = stm.getOrderBy() == null ? null : stm.getOrderBy().copy();
    this.unwind = stm.getUnwind() == null ? null : stm.getUnwind().copy();
  }

  public InternalExecutionPlan createExecutionPlan(final CommandContext context) {

    buildPatterns(context);
    splitDisjointPatterns(context);

    final SelectExecutionPlan result = new SelectExecutionPlan(context);
    final Map<String, Long> estimatedRootEntries = estimateRootEntries(aliasTypes, aliasBuckets, aliasRids, aliasFilters, context);
    final Set<String> aliasesToPrefetch = estimatedRootEntries.entrySet().stream().filter(x -> x.getValue() < threshold)
        .map(x -> x.getKey()).collect(Collectors.toSet());
    if (estimatedRootEntries.containsValue(0L)) {
      result.chain(new EmptyStep(context));
      return result;
    }

    addPrefetchSteps(result, aliasesToPrefetch, context);

    if (subPatterns.size() > 1) {
      final CartesianProductStep step = new CartesianProductStep(context);
      for (final Pattern subPattern : subPatterns) {
        step.addSubPlan(createPlanForPattern(subPattern, context, estimatedRootEntries, aliasesToPrefetch));
      }
      result.chain(step);
    } else {
      final InternalExecutionPlan plan = createPlanForPattern(pattern, context, estimatedRootEntries, aliasesToPrefetch);
      for (final ExecutionStep step : plan.getSteps()) {
        result.chain((ExecutionStepInternal) step);
      }
    }

    manageNotPatterns(result, pattern, notMatchExpressions, context);

    if (foundOptional) {
      result.chain(new RemoveEmptyOptionalsStep(context));
    }

    if (returnElements || returnPaths || returnPatterns || returnPathElements) {
      addReturnStep(result, context);

      if (this.returnDistinct) {
        result.chain(new DistinctExecutionStep(context));
      }
      if (groupBy != null) {
        throw new CommandExecutionException(
            "Cannot execute GROUP BY in MATCH query with RETURN $elements, $pathElements, $patterns or $paths");
      }

      if (this.orderBy != null) {
        result.chain(new OrderByStep(orderBy, context, -1));
      }

      if (this.unwind != null) {
        result.chain(new UnwindStep(unwind, context));
      }

      if (this.skip != null && skip.getValue(context) >= 0) {
        result.chain(new SkipExecutionStep(skip, context));
      }
      if (this.limit != null && limit.getValue(context) >= 0) {
        result.chain(new LimitExecutionStep(limit, context));
      }
    } else {
      final QueryPlanningInfo info = new QueryPlanningInfo();
      final List<ProjectionItem> items = new ArrayList<>();
      for (int i = 0; i < this.returnItems.size(); i++) {
        final ProjectionItem item = new ProjectionItem(returnItems.get(i), this.returnAliases.get(i),
            returnNestedProjections.get(i));
        items.add(item);
      }
      info.projection = new Projection(items, returnDistinct);

      info.projection = SelectExecutionPlanner.translateDistinct(info.projection);
      info.distinct = info.projection != null && info.projection.isDistinct();
      if (info.projection != null) {
        info.projection.setDistinct(false);
      }

      info.groupBy = this.groupBy;
      info.orderBy = this.orderBy;
      info.unwind = this.unwind;
      info.skip = this.skip;
      info.limit = this.limit;

      SelectExecutionPlanner.optimizeQuery(info, context);
      SelectExecutionPlanner.handleProjectionsBlock(result, info, context);
    }

    return result;

  }

  private void manageNotPatterns(final SelectExecutionPlan result, final Pattern pattern,
      final List<MatchExpression> notMatchExpressions, final CommandContext context) {
    for (final MatchExpression exp : notMatchExpressions) {
      if (pattern.aliasToNode.get(exp.getOrigin().getAlias()) == null) {
        throw new CommandExecutionException("This kind of NOT expression is not supported (yet). "
            + "The first alias in a NOT expression has to be present in the positive pattern");
      }

      if (exp.getOrigin().getFilter() != null) {
        throw new CommandExecutionException(
            "This kind of NOT expression is not supported (yet): " + "WHERE condition on the initial alias");
        // TODO implement his
      }

      MatchFilter lastFilter = exp.getOrigin();
      final List<AbstractExecutionStep> steps = new ArrayList<>();
      for (final MatchPathItem item : exp.getItems()) {
        if (item instanceof MultiMatchPathItem) {
          throw new CommandExecutionException("This kind of NOT expression is not supported (yet): " + item);
        }
        final PatternEdge edge = new PatternEdge();
        edge.item = item;
        edge.out = new PatternNode();
        edge.out.alias = lastFilter.getAlias();
        edge.in = new PatternNode();
        edge.in.alias = item.getFilter().getAlias();
        final EdgeTraversal traversal = new EdgeTraversal(edge, true);
        final MatchStep step = new MatchStep(context, traversal);
        steps.add(step);
        lastFilter = item.getFilter();
      }
      result.chain(new FilterNotMatchPatternStep(steps, context));
    }
  }

  private void addReturnStep(final SelectExecutionPlan result, final CommandContext context) {
    if (returnElements) {
      result.chain(new ReturnMatchElementsStep(context));
    } else if (returnPaths) {
      result.chain(new ReturnMatchPathsStep(context));
    } else if (returnPatterns) {
      result.chain(new ReturnMatchPatternsStep(context));
    } else if (returnPathElements) {
      result.chain(new ReturnMatchPathElementsStep(context));
    } else {
      final Projection projection = new Projection(-1);
      projection.setItems(new ArrayList<>());
      for (int i = 0; i < returnAliases.size(); i++) {
        final ProjectionItem item = new ProjectionItem(-1);
        item.setExpression(returnItems.get(i));
        item.setAlias(returnAliases.get(i));
        item.setNestedProjection(returnNestedProjections.get(i));
        projection.getItems().add(item);
      }
      result.chain(new ProjectionCalculationStep(projection, context));
    }
  }

  private InternalExecutionPlan createPlanForPattern(final Pattern pattern, final CommandContext context,
      final Map<String, Long> estimatedRootEntries, final Set<String> prefetchedAliases) {
    final SelectExecutionPlan plan = new SelectExecutionPlan(context);
    final List<EdgeTraversal> sortedEdges = getTopologicalSortedSchedule(estimatedRootEntries, pattern);

    boolean first = true;
    if (sortedEdges.size() > 0) {
      for (final EdgeTraversal edge : sortedEdges) {
        if (edge.edge.out.alias != null) {
          edge.setLeftClass(aliasTypes.get(edge.edge.out.alias));
          edge.setLeftCluster(aliasBuckets.get(edge.edge.out.alias));
          edge.setLeftRid(aliasRids.get(edge.edge.out.alias));
          edge.setLeftClass(aliasTypes.get(edge.edge.out.alias));
          edge.setLeftFilter(aliasFilters.get(edge.edge.out.alias));
        }
        addStepsFor(plan, edge, context, first);
        first = false;
      }
    } else {
      final PatternNode node = pattern.getAliasToNode().values().iterator().next();
      if (prefetchedAliases.contains(node.alias)) {
        //from prefetch
        plan.chain(new MatchFirstStep(context, node));
      } else {
        //from actual execution plan
        final String typez = aliasTypes.get(node.alias);
        final String bucket = aliasBuckets.get(node.alias);
        final Rid rid = aliasRids.get(node.alias);
        final WhereClause filter = aliasFilters.get(node.alias);
        final SelectStatement select = createSelectStatement(typez, bucket, rid, filter);
        plan.chain(new MatchFirstStep(context, node, select.createExecutionPlan(context)));
      }
    }
    return plan;
  }

  /**
   * sort edges in the order they will be matched
   */
  private List<EdgeTraversal> getTopologicalSortedSchedule(final Map<String, Long> estimatedRootEntries, final Pattern pattern) {
    final List<EdgeTraversal> resultingSchedule = new ArrayList<>();
    final Map<String, Set<String>> remainingDependencies = getDependencies(pattern);
    final Set<PatternNode> visitedNodes = new HashSet<>();
    final Set<PatternEdge> visitedEdges = new HashSet<>();

    // Sort the possible root vertices in order of estimated size, since we want to start with a small vertex set.
    final List<Pair<Long, String>> rootWeights = new ArrayList<>();
    for (final Map.Entry<String, Long> root : estimatedRootEntries.entrySet()) {
      rootWeights.add(new Pair<>(root.getValue(), root.getKey()));
    }
    rootWeights.sort(Comparator.comparing(Pair::getFirst));

    // Add the starting vertices, in the correct order, to an ordered set.
    final Set<String> remainingStarts = new LinkedHashSet<>();
    for (final Pair<Long, String> item : rootWeights) {
      remainingStarts.add(item.getSecond());
    }
    // Add all the remaining aliases after all the suggested start points.
    remainingStarts.addAll(pattern.aliasToNode.keySet());

    while (resultingSchedule.size() < pattern.numOfEdges) {
      // Start a new depth-first pass, adding all nodes with satisfied dependencies.
      // 1. Find a starting vertex for the depth-first pass.
      PatternNode startingNode = null;
      final List<String> startsToRemove = new ArrayList<>();
      for (final String currentAlias : remainingStarts) {
        final PatternNode currentNode = pattern.aliasToNode.get(currentAlias);

        if (visitedNodes.contains(currentNode)) {
          // If a previous traversal already visited this alias, remove it from further consideration.
          startsToRemove.add(currentAlias);
        } else if (remainingDependencies.get(currentAlias) == null || remainingDependencies.get(currentAlias).isEmpty()) {
          // If it hasn't been visited, and has all dependencies satisfied, visit it.
          startsToRemove.add(currentAlias);
          startingNode = currentNode;
          break;
        }
      }
      startsToRemove.forEach(remainingStarts::remove);

      if (startingNode == null) {
        // We didn't manage to find a valid root, and yet we haven't constructed a complete schedule.
        // This means there must be a cycle in our dependency graph, or all dependency-free nodes are optional.
        // Therefore, the query is invalid.
        throw new CommandExecutionException("This query contains MATCH conditions that cannot be evaluated, "
            + "like an undefined alias or a circular dependency on a $matched condition.");
      }

      // 2. Having found a starting vertex, traverse its neighbors depth-first,
      //    adding any non-visited ones with satisfied dependencies to our schedule.
      updateScheduleStartingAt(startingNode, visitedNodes, visitedEdges, remainingDependencies, resultingSchedule);
    }

    if (resultingSchedule.size() != pattern.numOfEdges) {
      throw new AssertionError("Incorrect number of edges: " + resultingSchedule.size() + " vs " + pattern.numOfEdges);
    }

    return resultingSchedule;
  }

  /**
   * Start a depth-first traversal from the starting node, adding all viable unscheduled edges and vertices.
   *
   * @param startNode             the node from which to start the depth-first traversal
   * @param visitedNodes          set of nodes that are already visited (mutated in this function)
   * @param visitedEdges          set of edges that are already visited and therefore don't need to be scheduled (mutated in this
   *                              function)
   * @param remainingDependencies dependency map including only the dependencies that haven't yet been satisfied (mutated in this
   *                              function)
   * @param resultingSchedule     the schedule being computed i.e. appended to (mutated in this function)
   */
  private void updateScheduleStartingAt(final PatternNode startNode, final Set<PatternNode> visitedNodes,
      final Set<PatternEdge> visitedEdges, final Map<String, Set<String>> remainingDependencies,
      final List<EdgeTraversal> resultingSchedule) {
    // Arcadedb requires the schedule to contain all edges present in the query, which is a stronger condition
    // than simply visiting all nodes in the query. Consider the following example query:
    //     MATCH {
    //         class: A,
    //         as: foo
    //     }.in() {
    //         as: bar
    //     }, {
    //         class: B,
    //         as: bar
    //     }.out() {
    //         as: foo
    //     } RETURN $matches
    // The schedule for the above query must have two edges, even though there are only two nodes and they can both
    // be visited with the traversal of a single edge.
    //
    // To satisfy it, we obey the following for each non-optional node:
    // - ignore edges to neighboring nodes which have unsatisfied dependencies;
    // - for visited neighboring nodes, add their edge if it wasn't already present in the schedule, but do not
    //   recurse into the neighboring node;
    // - for unvisited neighboring nodes with satisfied dependencies, add their edge and recurse into them.
    visitedNodes.add(startNode);
    for (final Set<String> dependencies : remainingDependencies.values()) {
      dependencies.remove(startNode.alias);
    }

    final Map<PatternEdge, Boolean> edges = new LinkedHashMap<>();
    for (final PatternEdge outEdge : startNode.out) {
      edges.put(outEdge, true);
    }
    for (final PatternEdge inEdge : startNode.in) {
      edges.put(inEdge, false);
    }

    for (final Map.Entry<PatternEdge, Boolean> edgeData : edges.entrySet()) {
      final PatternEdge edge = edgeData.getKey();
      final boolean isOutbound = edgeData.getValue();
      final PatternNode neighboringNode = isOutbound ? edge.in : edge.out;

      if (!remainingDependencies.get(neighboringNode.alias).isEmpty()) {
        // Unsatisfied dependencies, ignore this neighboring node.
        continue;
      }

      if (visitedNodes.contains(neighboringNode)) {
        if (!visitedEdges.contains(edge)) {
          // If we are executing in this block, we are in the following situation:
          // - the startNode has not been visited yet;
          // - it has a neighboringNode that has already been visited;
          // - the edge between the startNode and the neighboringNode has not been scheduled yet.
          //
          // The isOutbound value shows us whether the edge is outbound from the point of view of the startNode.
          // However, if there are edges to the startNode, we must visit the startNode from an already-visited
          // neighbor, to preserve the validity of the traversal. Therefore, we negate the value of isOutbound
          // to ensure that the edge is always scheduled in the direction from the already-visited neighbor
          // toward the startNode. Notably, this is also the case when evaluating "optional" nodes -- we always
          // visit the optional node from its non-optional and already-visited neighbor.
          //
          // The only exception to the above is when we have edges with "while" conditions. We are not allowed
          // to flip their directionality, so we leave them as-is.
          final boolean traversalDirection;
          if (startNode.optional || edge.item.isBidirectional()) {
            traversalDirection = !isOutbound;
          } else {
            traversalDirection = isOutbound;
          }

          visitedEdges.add(edge);
          resultingSchedule.add(new EdgeTraversal(edge, traversalDirection));
        }
      } else if (!startNode.optional) {
        // If the neighboring node wasn't visited, we don't expand the optional node into it, hence the above check.
        // Instead, we'll allow the neighboring node to add the edge we failed to visit, via the above block.
        if (visitedEdges.contains(edge)) {
          // Should never happen.
          throw new AssertionError("The edge was visited, but the neighboring vertex was not: " + edge + " " + neighboringNode);
        }

        visitedEdges.add(edge);
        resultingSchedule.add(new EdgeTraversal(edge, isOutbound));
        updateScheduleStartingAt(neighboringNode, visitedNodes, visitedEdges, remainingDependencies, resultingSchedule);
      }
    }
  }

  /**
   * Calculate the set of dependency aliases for each alias in the pattern.
   *
   * @param pattern
   *
   * @return map of alias to the set of aliases it depends on
   */
  private Map<String, Set<String>> getDependencies(final Pattern pattern) {
    final Map<String, Set<String>> result = new HashMap<>();

    for (final PatternNode node : pattern.aliasToNode.values()) {
      final Set<String> currentDependencies = new HashSet<>();

      final WhereClause filter = aliasFilters.get(node.alias);
      if (filter != null && filter.getBaseExpression() != null) {
        final List<String> involvedAliases = filter.getBaseExpression().getMatchPatternInvolvedAliases();
        if (involvedAliases != null) {
          currentDependencies.addAll(involvedAliases);
        }
      }

      result.put(node.alias, currentDependencies);
    }

    return result;
  }

  private void splitDisjointPatterns(final CommandContext context) {
    if (this.subPatterns != null) {
      return;
    }

    this.subPatterns = pattern.getDisjointPatterns();
  }

  private void addStepsFor(final SelectExecutionPlan plan, final EdgeTraversal edge, final CommandContext context,
      final boolean first) {
    if (first) {
      final PatternNode patternNode = edge.out ? edge.edge.out : edge.edge.in;
      final String typez = this.aliasTypes.get(patternNode.alias);
      final String bucket = this.aliasBuckets.get(patternNode.alias);
      final Rid rid = this.aliasRids.get(patternNode.alias);
      final WhereClause where = aliasFilters.get(patternNode.alias);
      final SelectStatement select = new SelectStatement(-1);
      select.setTarget(new FromClause(-1));
      select.getTarget().setItem(new FromItem(-1));
      if (typez != null) {
        select.getTarget().getItem().setIdentifier(new Identifier(typez));
      } else if (bucket != null) {
        select.getTarget().getItem().setBucket(new Bucket(bucket));
      } else if (rid != null) {
        select.getTarget().getItem().setRids(Collections.singletonList(rid));
      }
      select.setWhereClause(where == null ? null : where.copy());
      final BasicCommandContext subContxt = new BasicCommandContext();
      subContxt.setParentWithoutOverridingChild(context);
      plan.chain(
          new MatchFirstStep(context, patternNode, select.createExecutionPlan(subContxt)));
    }
    if (edge.edge.in.isOptionalNode()) {
      foundOptional = true;
      plan.chain(new OptionalMatchStep(context, edge));
    } else {
      plan.chain(new MatchStep(context, edge));
    }
  }

  private void addPrefetchSteps(final SelectExecutionPlan result, final Set<String> aliasesToPrefetch,
      final CommandContext context) {
    for (final String alias : aliasesToPrefetch) {
      final String targetClass = aliasTypes.get(alias);
      final String targetCluster = aliasBuckets.get(alias);
      final Rid targetRid = aliasRids.get(alias);
      final WhereClause filter = aliasFilters.get(alias);
      final SelectStatement prefetchStm = createSelectStatement(targetClass, targetCluster, targetRid, filter);

      final MatchPrefetchStep step = new MatchPrefetchStep(context, prefetchStm.createExecutionPlan(context),
          alias);
      result.chain(step);
    }
  }

  private SelectStatement createSelectStatement(final String targetClass, final String targetCluster, final Rid targetRid,
      final WhereClause filter) {
    final SelectStatement prefetchStm = new SelectStatement(-1);
    prefetchStm.setWhereClause(filter);
    final FromClause from = new FromClause(-1);
    final FromItem fromItem = new FromItem(-1);
    if (targetRid != null) {
      fromItem.setRids(Collections.singletonList(targetRid));
    } else if (targetClass != null) {
      fromItem.setIdentifier(new Identifier(targetClass));
    } else if (targetCluster != null) {
      fromItem.setBucket(new Bucket(targetCluster));
    }
    from.setItem(fromItem);
    prefetchStm.setTarget(from);
    return prefetchStm;
  }

  private void buildPatterns(final CommandContext context) {
    if (this.pattern != null) {
      return;
    }
    assignDefaultAliases(this.matchExpressions);
    pattern = new Pattern();
    for (final MatchExpression expr : this.matchExpressions) {
      pattern.addExpression(expr.copy());
    }

    final Map<String, WhereClause> aliasFilters = new LinkedHashMap<>();
    final Map<String, String> aliasUserTypes = new LinkedHashMap<>();
    final Map<String, String> aliasClusters = new LinkedHashMap<>();
    final Map<String, Rid> aliasRids = new LinkedHashMap<>();
    for (final MatchExpression expr : this.matchExpressions) {
      addAliases(expr, aliasFilters, aliasUserTypes, aliasClusters, aliasRids, context);
    }

    this.aliasFilters = aliasFilters;
    this.aliasTypes = aliasUserTypes;
    this.aliasBuckets = aliasClusters;
    this.aliasRids = aliasRids;

    rebindFilters(aliasFilters);
  }

  private void rebindFilters(final Map<String, WhereClause> aliasFilters) {
    for (final MatchExpression expression : matchExpressions) {
      WhereClause newFilter = aliasFilters.get(expression.getOrigin().getAlias());
      expression.getOrigin().setFilter(newFilter);

      for (final MatchPathItem item : expression.getItems()) {
        newFilter = aliasFilters.get(item.getFilter().getAlias());
        item.getFilter().setFilter(newFilter);
      }
    }
  }

  private void addAliases(final MatchExpression expr, final Map<String, WhereClause> aliasFilters,
      final Map<String, String> aliasUserTypes, final Map<String, String> aliasClusters, final Map<String, Rid> aliasRids,
      final CommandContext context) {
    addAliases(expr.getOrigin(), aliasFilters, aliasUserTypes, aliasClusters, aliasRids, context);
    for (final MatchPathItem item : expr.getItems()) {
      if (item.getFilter() != null) {
        addAliases(item.getFilter(), aliasFilters, aliasUserTypes, aliasClusters, aliasRids, context);
      }
    }
  }

  private void addAliases(final MatchFilter matchFilter, final Map<String, WhereClause> aliasFilters,
      final Map<String, String> aliasUserTypes, final Map<String, String> aliasClusters, final Map<String, Rid> aliasRids,
      final CommandContext context) {
    final String alias = matchFilter.getAlias();
    final WhereClause filter = matchFilter.getFilter();
    if (alias != null) {
      if (filter != null && filter.getBaseExpression() != null) {
        WhereClause previousFilter = aliasFilters.get(alias);
        if (previousFilter == null) {
          previousFilter = new WhereClause(-1);
          previousFilter.setBaseExpression(new AndBlock(-1));
          aliasFilters.put(alias, previousFilter);
        }
        final AndBlock filterBlock = (AndBlock) previousFilter.getBaseExpression();
        if (filter.getBaseExpression() != null) {
          filterBlock.getSubBlocks().add(filter.getBaseExpression());
        }
      }

      final String typez = matchFilter.getTypeName(context);
      if (typez != null) {
        final String previousClass = aliasUserTypes.get(alias);
        if (previousClass == null) {
          aliasUserTypes.put(alias, typez);
        } else {
          final String lower = getLowerSubclass(context.getDatabase(), typez, previousClass);
          if (lower == null) {
            throw new CommandExecutionException(
                "classes defined for alias " + alias + " (" + typez + ", " + previousClass + ") are not in the same hierarchy");
          }
          aliasUserTypes.put(alias, lower);
        }
      }

      final String bucketName = matchFilter.getBucketName(context);
      if (bucketName != null) {
        final String previousCluster = aliasClusters.get(alias);
        if (previousCluster == null) {
          aliasClusters.put(alias, bucketName);
        } else if (!previousCluster.equalsIgnoreCase(bucketName)) {
          throw new CommandExecutionException(
              "Invalid expression for alias " + alias + " cannot be of both buckets " + previousCluster + " and " + bucketName);
        }
      }

      final Rid rid = matchFilter.getRid(context);
      if (rid != null) {
        final Rid previousRid = aliasRids.get(alias);
        if (previousRid == null) {
          aliasRids.put(alias, rid);
        } else if (!previousRid.equals(rid)) {
          throw new CommandExecutionException(
              "Invalid expression for alias " + alias + " cannot be of both RIDs " + previousRid + " and " + rid);
        }
      }
    }
  }

  private String getLowerSubclass(final Database db, final String className1, final String className2) {
    final Schema schema = db.getSchema();
    final DocumentType class1 = schema.getType(className1);
    final DocumentType class2 = schema.getType(className2);
    if (class1.equals(class2)) {
      return class1.getName();
    }
    return null;
  }

  /**
   * assigns default aliases to pattern nodes that do not have an explicit alias
   *
   * @param matchExpressions
   */
  private void assignDefaultAliases(final List<MatchExpression> matchExpressions) {
    int counter = 0;
    for (final MatchExpression expression : matchExpressions) {
      if (expression.getOrigin().getAlias() == null) {
        expression.getOrigin().setAlias(DEFAULT_ALIAS_PREFIX + (counter++));
      }

      for (final MatchPathItem item : expression.getItems()) {
        if (item.getFilter() == null) {
          item.setFilter(new MatchFilter(-1));
        }
        if (item.getFilter().getAlias() == null) {
          item.getFilter().setAlias(DEFAULT_ALIAS_PREFIX + (counter++));
        }
      }
    }
  }

  private Map<String, Long> estimateRootEntries(final Map<String, String> aliasUserTypes, final Map<String, String> aliasClusters,
      final Map<String, Rid> aliasRids, final Map<String, WhereClause> aliasFilters, final CommandContext context) {
    final Set<String> allAliases = new LinkedHashSet<>();
    allAliases.addAll(aliasUserTypes.keySet());
    allAliases.addAll(aliasFilters.keySet());
    allAliases.addAll(aliasClusters.keySet());
    allAliases.addAll(aliasRids.keySet());

    final Schema schema = context.getDatabase().getSchema();

    final Map<String, Long> result = new LinkedHashMap<>();
    for (final String alias : allAliases) {
      final String typeName = aliasUserTypes.get(alias);
      final String bucketName = aliasClusters.get(alias);
      final Rid rid = aliasRids.get(alias);
      if (typeName == null && bucketName == null) {
        continue;
      }

      if (typeName != null) {
        if (schema.getType(typeName) == null) {
          throw new CommandExecutionException("Type '" + typeName + "' not defined");
        }
        final DocumentType oClass = schema.getType(typeName);
        final long upperBound;
        final WhereClause filter = aliasFilters.get(alias);
        if (filter != null) {
          upperBound = filter.estimate(oClass, threshold, context);
        } else {
          upperBound = context.getDatabase().countType(oClass.getName(), true);
        }
        result.put(alias, upperBound);
      } else if (bucketName != null) {
        final Database db = context.getDatabase();
        if (db.getSchema().getBucketByName(bucketName) == null) {
          throw new CommandExecutionException("Bucket '" + bucketName + "' not defined");
        }
        final int bucketId = db.getSchema().getBucketByName(bucketName).getFileId();
        final DocumentType oClass = db.getSchema().getTypeByBucketId(bucketId);
        if (oClass != null) {
          final long upperBound;
          final WhereClause filter = aliasFilters.get(alias);
          if (filter != null) {
            upperBound = Math.min(db.countBucket(bucketName), filter.estimate(oClass, threshold, context));
          } else {
            upperBound = db.countBucket(bucketName);
          }
          result.put(alias, upperBound);
        } else {
          result.put(alias, db.countBucket(bucketName));
        }
      } else if (rid != null) {
        result.put(alias, 1L);
      }
    }
    return result;
  }

}
