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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.executor.CypherVariableUsage;
import com.arcadedb.query.opencypher.executor.operators.CartesianProduct;
import com.arcadedb.query.opencypher.executor.operators.EquiJoinKey;
import com.arcadedb.query.opencypher.executor.operators.FilterOperator;
import com.arcadedb.query.opencypher.executor.operators.IndexNestedLoopJoin;
import com.arcadedb.query.opencypher.executor.operators.NodeByLabelScan;
import com.arcadedb.query.opencypher.executor.operators.PhysicalOperator;
import com.arcadedb.query.opencypher.executor.operators.RelationshipUniquenessFilter;
import com.arcadedb.query.opencypher.executor.operators.RowBuffer;
import com.arcadedb.query.opencypher.executor.operators.ValueHashJoin;
import com.arcadedb.query.opencypher.optimizer.plan.AnchorSelection;
import com.arcadedb.query.opencypher.optimizer.plan.LogicalNode;
import com.arcadedb.query.opencypher.optimizer.statistics.CostModel;
import com.arcadedb.query.opencypher.optimizer.statistics.IndexStatistics;
import com.arcadedb.query.opencypher.optimizer.statistics.StatisticsProvider;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.UnaryOperator;

/**
 * Plans how the independently matched parts of a pattern are put together: the nodes of {@code MATCH (a:T), (b:T)},
 * the relationship components of {@code MATCH (a)-->(b), (c)-->(d)}, a lone node after a connected pattern.
 * <p>
 * Every such part used to be crossed with a {@link CartesianProduct} and the whole WHERE evaluated above it, so a
 * predicate on one part did not narrow it and a predicate relating two parts ran on every combination, while the
 * product held the whole of its right input in heap (issue #8584). Here:
 * <ul>
 *   <li>each conjunct of the WHERE that reads one part alone is applied to that part, below any join: in its scan
 *   when the part is a lone node, above its expansion otherwise;</li>
 *   <li>the parts are then joined, cheapest first, each one onto what is already joined: by an
 *   {@link IndexNestedLoopJoin} when a lone node has an index on the property a {@code node.property = expression}
 *   conjunct compares, which seeks it per row and buffers nothing; by a {@link ValueHashJoin} on the
 *   {@code left = right} conjuncts relating the two sides; by a {@link CartesianProduct} when nothing relates them;</li>
 *   <li>the conjuncts relating several parts stay in a filter above the joins, which decides every pair the joins let
 *   through.</li>
 * </ul>
 * The join order is greedy and left-deep: the cheapest pair first, then the cheapest part to join onto it. With three or
 * more parts a plan joining two later parts with each other before the first is not considered.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class DisconnectedPatternJoinPlanner {
  /**
   * The cost of holding one row in a join buffer for the rest of the query: the heap it takes from every other query
   * running at the same time, and the reload of its records once the buffer is compact. It is what makes a join that
   * seeks an index per row win over a hash join of the same CPU, which holds the whole of one side.
   */
  static final double JOIN_BUFFER_COST_PER_ROW = 3.0;

  private final DatabaseInternal        database;
  private final StatisticsProvider      statisticsProvider;
  /** Rows a join buffer holds in full before it turns compact; 0 when a write could change a record (see RowBuffer). */
  private final int                     compactAfterRows;
  private final List<BooleanExpression> crossConjuncts    = new ArrayList<>();
  private final List<BooleanExpression> residualConjuncts = new ArrayList<>();
  private       Set<String>             allVariables;
  private       Unit                    driver;

  /** One independently matched part of the pattern. */
  static final class Unit {
    PhysicalOperator operator;
    final Set<String>          variables;
    /** The node, when the part is a lone node: only such a part can be joined by seeking an index. */
    final LogicalNode          node;
    final AnchorSelection      anchor;
    /** Pushes the conjuncts a component's anchor scan can decide into it, answering what is left; null for a node. */
    final UnaryOperator<BooleanExpression> anchorPushdown;
    final List<BooleanExpression>          localConjuncts = new ArrayList<>();
    BooleanExpression localFilter;

    Unit(final PhysicalOperator operator, final Set<String> variables, final LogicalNode node, final AnchorSelection anchor,
        final UnaryOperator<BooleanExpression> anchorPushdown) {
      this.operator = operator;
      this.variables = variables;
      this.node = node;
      this.anchor = anchor;
      this.anchorPushdown = anchorPushdown;
    }
  }

  /** What joining a part onto the parts already joined would cost, and how. */
  private record JoinChoice(Unit left, Unit right, Strategy strategy, double cost, long cardinality, EquiJoinKey[] keys,
                            IndexStatistics index) {
  }

  private enum Strategy {CARTESIAN_PRODUCT, HASH_JOIN, INDEX_NESTED_LOOP}

  DisconnectedPatternJoinPlanner(final DatabaseInternal database, final StatisticsProvider statisticsProvider,
      final boolean recordsReloadable) {
    this.database = database;
    this.statisticsProvider = statisticsProvider;
    this.compactAfterRows = recordsReloadable ? RowBuffer.DEFAULT_COMPACT_AFTER_ROWS : 0;
  }

  /**
   * Joins the parts, each narrowed by the conjuncts that read it alone, and filters the result by the rest of the WHERE.
   *
   * @param units                         the parts, in the order the pattern wrote them
   * @param whereFilters                  the WHERE predicates of the pattern, inline property maps included
   * @param relationshipVariablesByClause the relationship variables of every MATCH clause, for the uniqueness check
   *                                      two components of one clause owe each other
   */
  PhysicalOperator plan(final List<Unit> units, final List<BooleanExpression> whereFilters,
      final Map<Integer, Set<String>> relationshipVariablesByClause) {
    allVariables = new LinkedHashSet<>();
    for (final Unit unit : units)
      allVariables.addAll(unit.variables);

    final List<BooleanExpression> conjuncts = new ArrayList<>();
    for (final BooleanExpression filter : whereFilters)
      collectConjuncts(filter, conjuncts);
    for (final BooleanExpression conjunct : conjuncts)
      assign(conjunct, units);

    for (final Unit unit : units)
      applyLocalConjuncts(unit);

    final BiPredicate<Result, Result> pairFilter = needsUniquenessCheck(relationshipVariablesByClause) ?
        RelationshipUniquenessFilter.pushdownPredicate(relationshipVariablesByClause) : null;

    final List<Unit> remaining = new ArrayList<>(units);
    Unit joined = null;
    if (remaining.size() == 1) {
      joined = remaining.removeFirst();
      driver = joined;
    }
    while (!remaining.isEmpty()) {
      JoinChoice best = null;
      if (joined == null) {
        for (final Unit left : remaining)
          for (final Unit right : remaining)
            if (left != right)
              best = cheaper(best, choose(left, right));
      } else
        for (final Unit right : remaining)
          best = cheaper(best, choose(joined, right));

      if (joined == null) {
        driver = best.left();
        remaining.remove(best.left());
      }
      remaining.remove(best.right());
      joined = join(best, pairFilter);
    }

    final List<BooleanExpression> topConjuncts = new ArrayList<>(crossConjuncts);
    topConjuncts.addAll(residualConjuncts);
    PhysicalOperator root = joined.operator;
    if (!topConjuncts.isEmpty()) {
      final long input = root.getEstimatedCardinality();
      root = new FilterOperator(root, and(topConjuncts),
          CostModel.saturatingAdd(root.getEstimatedCost(), CostModel.saturatingMultiply(input, CostModel.FILTER_COST_PER_ROW)),
          Math.max(1, input / 2));
    }
    return root;
  }

  /** The part the plan starts from: its anchor is the plan's anchor. */
  Unit getDriver() {
    return driver;
  }

  private void assign(final BooleanExpression conjunct, final List<Unit> units) {
    final Set<String> read = variablesRead(conjunct);
    if (read.isEmpty()) {
      residualConjuncts.add(conjunct);
      return;
    }
    for (final Unit unit : units)
      if (unit.variables.containsAll(read)) {
        unit.localConjuncts.add(conjunct);
        return;
      }
    crossConjuncts.add(conjunct);
  }

  private void applyLocalConjuncts(final Unit unit) {
    if (unit.localConjuncts.isEmpty())
      return;
    unit.localFilter = and(unit.localConjuncts);

    BooleanExpression rest = unit.localFilter;
    if (unit.node != null && unit.operator instanceof NodeByLabelScan scan) {
      scan.pushDownFilter(rest);
      rest = null;
    } else if (unit.anchorPushdown != null)
      rest = unit.anchorPushdown.apply(rest);

    if (rest != null) {
      final PhysicalOperator input = unit.operator;
      unit.operator = new FilterOperator(input, rest,
          CostModel.saturatingAdd(input.getEstimatedCost(),
              CostModel.saturatingMultiply(input.getEstimatedCardinality(), CostModel.FILTER_COST_PER_ROW)),
          Math.max(1, input.getEstimatedCardinality() / 2));
    }
  }

  private JoinChoice choose(final Unit left, final Unit right) {
    final double inputCost = CostModel.saturatingAdd(left.operator.getEstimatedCost(), right.operator.getEstimatedCost());
    final long leftRows = Math.max(1, left.operator.getEstimatedCardinality());
    final long rightRows = Math.max(1, right.operator.getEstimatedCardinality());

    // Nothing relates the two sides: every left row meets every right row, which the product holds in its buffer
    final long productRows = CostModel.saturatingCardinalityProduct(leftRows, rightRows);
    JoinChoice best = new JoinChoice(left, right, Strategy.CARTESIAN_PRODUCT,
        CostModel.saturatingAdd(inputCost, CostModel.saturatingAdd(
            CostModel.saturatingMultiply(productRows, CostModel.FILTER_COST_PER_ROW),
            CostModel.saturatingMultiply(rightRows, JOIN_BUFFER_COST_PER_ROW))), productRows, null, null);

    final EquiJoinKey[] keys = equiJoinKeys(left, right);
    if (keys.length > 0) {
      final double build = CostModel.saturatingMultiply(rightRows, CostModel.HASH_BUILD_COST_PER_ROW + JOIN_BUFFER_COST_PER_ROW);
      final double probe = CostModel.saturatingMultiply(leftRows, CostModel.HASH_PROBE_COST_PER_ROW);
      best = cheaper(best, new JoinChoice(left, right, Strategy.HASH_JOIN,
          CostModel.saturatingAdd(inputCost, CostModel.saturatingAdd(build, probe)), Math.max(leftRows, rightRows), keys, null));
    }

    return cheaper(best, indexNestedLoop(left, right, keys));
  }

  /**
   * Joining a lone node by seeking, for every left row, an index whose leading properties the conjuncts pin: each to an
   * expression of the left row or to a constant, at least one to the left row. Null when no index qualifies.
   */
  private JoinChoice indexNestedLoop(final Unit left, final Unit right, final EquiJoinKey[] crossKeys) {
    final LogicalNode node = right.node;
    if (node == null || node.getLabels().size() != 1 || node.isLabelDisjunction())
      return null;
    final String variable = node.getVariable();
    final String label = node.getFirstLabel();

    // property -> the key it is sought by: from the left row first, a constant otherwise
    final Map<String, EquiJoinKey> fromLeft = new HashMap<>();
    for (final EquiJoinKey key : crossKeys)
      if (key.right() instanceof PropertyAccessExpression access && variable.equals(access.getVariableName()))
        fromLeft.putIfAbsent(access.getPropertyName(), key);
    if (fromLeft.isEmpty())
      return null;
    final Map<String, EquiJoinKey> constants = new HashMap<>();
    for (final BooleanExpression conjunct : right.localConjuncts)
      collectConstantEqualities(conjunct, variable, constants);

    IndexStatistics bestIndex = null;
    EquiJoinKey[] bestKeys = null;
    boolean bestUnique = false;
    for (final IndexStatistics candidate : statisticsProvider.getIndexesForType(label)) {
      final TypeIndex index = typeIndex(candidate.getIndexName());
      if (index == null)
        continue;
      final Type[] keyTypes = index.getKeyTypes();
      final List<EquiJoinKey> keys = new ArrayList<>();
      boolean readsLeft = false;
      for (int i = 0; i < candidate.getPropertyNames().size() && i < keyTypes.length; i++) {
        final String property = candidate.getPropertyNames().get(i);
        EquiJoinKey key = fromLeft.get(property);
        if (key != null)
          readsLeft = true;
        else
          key = constants.get(property);
        if (key == null || !IndexNestedLoopJoin.isSeekableKeyType(keyTypes[i]))
          break;
        keys.add(key);
      }
      if (!readsLeft || keys.isEmpty())
        continue;
      final boolean wholeKey = keys.size() == candidate.getPropertyNames().size();
      // A key prefix is a range of the ordered index, which only an LSM tree walks
      if (!wholeKey && index.getType() != Schema.INDEX_TYPE.LSM_TREE)
        continue;
      final boolean unique = wholeKey && candidate.isUnique();
      if (bestKeys == null || keys.size() > bestKeys.length || (keys.size() == bestKeys.length && unique && !bestUnique)) {
        bestIndex = candidate;
        bestKeys = keys.toArray(new EquiJoinKey[0]);
        bestUnique = unique;
      }
    }
    if (bestIndex == null)
      return null;

    final long leftRows = Math.max(1, left.operator.getEstimatedCardinality());
    final long typeRows = Math.max(1, statisticsProvider.getCardinality(label));
    final long rowsPerSeek = bestUnique ? 1 : Math.max(1, (long) (typeRows * Math.pow(CostModel.SELECTIVITY_EQUALITY, bestKeys.length)));
    final double seeks = CostModel.saturatingMultiply(leftRows,
        CostModel.INDEX_SEEK_COST + rowsPerSeek * CostModel.INDEX_LOOKUP_COST_PER_ROW);
    return new JoinChoice(left, right, Strategy.INDEX_NESTED_LOOP, CostModel.saturatingAdd(left.operator.getEstimatedCost(), seeks),
        CostModel.saturatingCardinalityProduct(leftRows, rowsPerSeek), bestKeys, bestIndex);
  }

  private Unit join(final JoinChoice choice, final BiPredicate<Result, Result> pairFilter) {
    final Unit left = choice.left();
    final Unit right = choice.right();
    final PhysicalOperator operator = switch (choice.strategy()) {
      case INDEX_NESTED_LOOP -> new IndexNestedLoopJoin(left.operator, right.node.getVariable(), right.node.getFirstLabel(),
          choice.index().getIndexName(), choice.index().getPropertyNames(), choice.keys(), right.localFilter, choice.cost(),
          choice.cardinality());
      case HASH_JOIN -> {
        final ValueHashJoin hashJoin = new ValueHashJoin(left.operator, right.operator, choice.keys(), choice.cost(),
            choice.cardinality(), pairFilter);
        hashJoin.setCompactAfterRows(compactAfterRows);
        yield hashJoin;
      }
      case CARTESIAN_PRODUCT -> {
        final CartesianProduct product = new CartesianProduct(left.operator, right.operator, choice.cost(), choice.cardinality(),
            pairFilter);
        product.setCompactAfterRows(compactAfterRows);
        yield product;
      }
    };

    final Set<String> variables = new LinkedHashSet<>(left.variables);
    variables.addAll(right.variables);
    return new Unit(operator, variables, null, left.anchor, null);
  }

  /** The conjuncts {@code left = right} of the WHERE whose sides read one of the two parts each. */
  private EquiJoinKey[] equiJoinKeys(final Unit left, final Unit right) {
    final List<EquiJoinKey> keys = new ArrayList<>();
    for (final BooleanExpression conjunct : crossConjuncts) {
      if (!(unwrap(conjunct) instanceof ComparisonExpression comparison)
          || comparison.getOperator() != ComparisonExpression.Operator.EQUALS)
        continue;
      final Set<String> first = variablesRead(comparison.getLeft());
      final Set<String> second = variablesRead(comparison.getRight());
      if (first.isEmpty() || second.isEmpty())
        continue;
      if (left.variables.containsAll(first) && right.variables.containsAll(second))
        keys.add(EquiJoinKey.of(comparison.getLeft(), comparison.getRight()));
      else if (left.variables.containsAll(second) && right.variables.containsAll(first))
        keys.add(EquiJoinKey.of(comparison.getRight(), comparison.getLeft()));
    }
    return keys.toArray(new EquiJoinKey[0]);
  }

  /** The {@code variable.property = constant} conjuncts, keyed by property: the constant side is the key. */
  private static void collectConstantEqualities(final BooleanExpression conjunct, final String variable,
      final Map<String, EquiJoinKey> constants) {
    if (!(unwrap(conjunct) instanceof ComparisonExpression comparison)
        || comparison.getOperator() != ComparisonExpression.Operator.EQUALS)
      return;
    if (comparison.getLeft() instanceof PropertyAccessExpression access && variable.equals(access.getVariableName())
        && isConstant(comparison.getRight()))
      constants.putIfAbsent(access.getPropertyName(), EquiJoinKey.of(comparison.getRight(), comparison.getLeft()));
    else if (comparison.getRight() instanceof PropertyAccessExpression access && variable.equals(access.getVariableName())
        && isConstant(comparison.getLeft()))
      constants.putIfAbsent(access.getPropertyName(), EquiJoinKey.of(comparison.getLeft(), comparison.getRight()));
  }

  private static boolean isConstant(final Expression expression) {
    return expression instanceof LiteralExpression || expression instanceof ParameterExpression;
  }

  private TypeIndex typeIndex(final String indexName) {
    try {
      final Index index = database.getSchema().getIndexByName(indexName);
      return index instanceof TypeIndex typeIndex ? typeIndex : null;
    } catch (final RuntimeException e) {
      // Dropped since the statistics were collected: no seek to plan on it
      return null;
    }
  }

  private Set<String> variablesRead(final BooleanExpression expression) {
    final Set<String> read = new LinkedHashSet<>();
    for (final String variable : allVariables)
      if (CypherVariableUsage.expressionReferencesVariable(expression, variable))
        read.add(variable);
    return read;
  }

  private Set<String> variablesRead(final Expression expression) {
    final Set<String> read = new LinkedHashSet<>();
    for (final String variable : allVariables)
      if (CypherVariableUsage.expressionReferencesVariable(expression, variable))
        read.add(variable);
    return read;
  }

  private static JoinChoice cheaper(final JoinChoice current, final JoinChoice candidate) {
    if (candidate == null)
      return current;
    if (current == null || candidate.cost() < current.cost())
      return candidate;
    return current;
  }

  private static boolean needsUniquenessCheck(final Map<Integer, Set<String>> relationshipVariablesByClause) {
    if (relationshipVariablesByClause == null)
      return false;
    for (final Set<String> variables : relationshipVariablesByClause.values())
      if (variables.size() > 1)
        return true;
    return false;
  }

  private static void collectConjuncts(final BooleanExpression expression, final List<BooleanExpression> conjuncts) {
    if (expression == null)
      return;
    final BooleanExpression unwrapped = unwrap(expression);
    if (unwrapped instanceof LogicalExpression logical && logical.getOperator() == LogicalExpression.Operator.AND) {
      collectConjuncts(logical.getLeft(), conjuncts);
      collectConjuncts(logical.getRight(), conjuncts);
    } else
      conjuncts.add(expression);
  }

  private static BooleanExpression unwrap(BooleanExpression expression) {
    while (expression instanceof BooleanWrapperExpression wrapper)
      expression = wrapper.getBooleanExpression();
    return expression;
  }

  private static BooleanExpression and(final List<BooleanExpression> conjuncts) {
    BooleanExpression result = conjuncts.getFirst();
    for (int i = 1; i < conjuncts.size(); i++)
      result = new LogicalExpression(LogicalExpression.Operator.AND, result, conjuncts.get(i));
    return result;
  }
}
