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

import com.arcadedb.database.Document;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.executor.operators.InListValues;
import com.arcadedb.query.opencypher.parser.CypherASTBuilder;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Decides whether a single-hop relationship pattern can be driven from the index of its relationship type, and with which seek,
 * for both planners: the step-by-step one ({@code CypherExecutionPlan}, {@code MatchEdgeByIndexStep}) and the cost-based one
 * ({@code CypherOptimizer}, {@code EdgeIndexSeek}). One definition, so every spelling of the same lookup reaches the same seek
 * whichever planner takes the statement (issue #11481):
 * <ul>
 * <li>the key predicates come from the {@code WHERE} clause AND from the relationship's inline property map, {@code [e:T {uuid: $u}]}
 * being the equality it stands for;</li>
 * <li>a union of types, {@code [e:A|B]}, is one seek per type, each with its own index, and is refused when one of the types has none
 * (that type would have to be scanned);</li>
 * <li>an endpoint may carry labels and inline properties, which the seek checks on the vertices of each edge it finds instead of
 * anchoring the plan on a scan of the label.</li>
 * </ul>
 * The predicates a seek is built from are a superset filter: the {@code WHERE} clause is still evaluated above it, and the inline
 * maps are checked by the seek on each edge and endpoint, so a seek never lets through a row the scan would have rejected.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class EdgeIndexSeedPlanner {
  /**
   * One seek over the index of one relationship type.
   *
   * @param propertyNames the leading index columns the predicates cover, in key order
   * @param keyValues     one per column: an {@link Expression} for an equality, an {@link InListValues} for a list
   * @param exact         the index is unique and every column of its key is pinned to values: at most one edge per value
   */
  public record TypeSeek(String edgeType, String indexName, String[] propertyNames, Object[] keyValues, boolean exact) {
    /** How many seek keys the values make at most: the product of the per-column value counts. */
    public long keyCount() {
      long count = 1;
      for (final Object value : keyValues)
        count *= keyValuesOf(value).size();
      return count;
    }
  }

  /** What an endpoint of the pattern asks of the vertex the seek binds it to: its labels and its inline properties. */
  public record EndpointFilter(List<String> labels, boolean disjunction, Map<String, Object> properties) {
    public static final EndpointFilter NONE = new EndpointFilter(List.of(), false, Map.of());

    public static EndpointFilter of(final NodePattern node) {
      final List<String> labels = node.hasLabels() ? node.getLabels() : List.of();
      final Map<String, Object> properties = node.hasProperties() ? node.getProperties() : Map.of();
      return labels.isEmpty() && properties.isEmpty() ? NONE : new EndpointFilter(labels, node.isLabelDisjunction(), properties);
    }

    public static EndpointFilter ofLabels(final List<String> labels, final boolean disjunction) {
      return labels == null || labels.isEmpty() ? NONE : new EndpointFilter(labels, disjunction, Map.of());
    }

    public boolean isEmpty() {
      return labels.isEmpty() && properties.isEmpty();
    }

    public boolean matches(final Vertex vertex, final Result row, final CommandContext context) {
      if (!labels.isEmpty() && !Labels.matches(vertex, labels, disjunction))
        return false;
      return properties.isEmpty() || InlineProperties.matches(vertex, properties, row, context);
    }
  }

  private EdgeIndexSeedPlanner() {
  }

  /**
   * Whether an inline property map can be checked by a seek that has no row: every value a literal, a parameter or another
   * row-independent value. A value that reads a variable belongs to a plan that binds it first.
   */
  public static boolean isRowIndependent(final Map<String, Object> properties) {
    if (properties != null)
      for (final Object value : properties.values())
        if (!RowIndependentValues.isSeekable(value))
          return false;
    return true;
  }

  /**
   * Adds the entries of a relationship's inline property map to the key predicates, each as the equality it stands for. An entry
   * already pinned by the {@code WHERE} clause keeps the value with fewer candidates.
   */
  public static void collectInlinePredicates(final Map<String, Object> properties, final Map<String, Object> predicates) {
    if (properties == null)
      return;
    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final Expression value = asExpression(entry.getValue());
      if (value != null && RowIndependentValues.isRowIndependent(value))
        addPredicate(predicates, entry.getKey(), value);
    }
  }

  /**
   * Collects the predicates on the edge variable that can seed an index seek, keyed by property: the value each property is pinned
   * to, as the {@link Expression} of an {@code edgeVar.property = <value>} equality (or reversed) or as the {@link InListValues} of
   * an {@code edgeVar.property IN [...]}. The two are kept apart even for a one-element list, since that element may be a parameter
   * standing for a whole list ({@code IN $ids}), which the seek expands into one key per element where an equality would seek the
   * list as a single key. Only row-independent values qualify, see {@link RowIndependentValues}; a value that would need the current
   * row cannot seed a static lookup, so its predicate is left out and the optimization simply does not apply to it.
   * <p>
   * An {@code AND} contributes both sides; when both pin the same property the one with fewer values is kept, as either already
   * bounds the rows. An {@code OR} contributes a property only when <em>both</em> sides pin it, as the union of the two sides'
   * values: a row satisfying the disjunction takes one of its sides' values, so the union is a superset - whereas a property pinned
   * on one side only bounds nothing about rows from the other. {@code (t.k = 'a' OR t.k = 'b')} therefore seeds the same two-key
   * seek as {@code t.k IN ['a', 'b']}.
   */
  public static void collectWherePredicates(final String edgeVar, final BooleanExpression expression,
      final Map<String, Object> predicates) {
    if (expression == null)
      return;

    if (expression instanceof BooleanWrapperExpression wrapper) {
      collectWherePredicates(edgeVar, wrapper.getBooleanExpression(), predicates);
      return;
    }
    if (expression instanceof LogicalExpression logical) {
      if (logical.getOperator() == LogicalExpression.Operator.AND) {
        collectWherePredicates(edgeVar, logical.getLeft(), predicates);
        collectWherePredicates(edgeVar, logical.getRight(), predicates);
      } else if (logical.getOperator() == LogicalExpression.Operator.OR) {
        final Map<String, Object> left = new LinkedHashMap<>();
        final Map<String, Object> right = new LinkedHashMap<>();
        collectWherePredicates(edgeVar, logical.getLeft(), left);
        collectWherePredicates(edgeVar, logical.getRight(), right);
        for (final Map.Entry<String, Object> entry : left.entrySet()) {
          final Object otherSide = right.get(entry.getKey());
          if (otherSide == null)
            continue;
          final List<Expression> union = new ArrayList<>(keyValuesOf(entry.getValue()));
          union.addAll(keyValuesOf(otherSide));
          addPredicate(predicates, entry.getKey(), new InListValues(union));
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
        if (!RowIndependentValues.isRowIndependent(element))
          return;
      addPredicate(predicates, prop.getPropertyName(), new InListValues(list));
      return;
    }

    if (!(expression instanceof ComparisonExpression comparison)
        || comparison.getOperator() != ComparisonExpression.Operator.EQUALS)
      return;

    final Expression left = comparison.getLeft();
    final Expression right = comparison.getRight();
    if (left instanceof PropertyAccessExpression prop && edgeVar.equals(prop.getVariableName())
        && RowIndependentValues.isRowIndependent(right))
      addPredicate(predicates, prop.getPropertyName(), right);
    else if (right instanceof PropertyAccessExpression prop && edgeVar.equals(prop.getVariableName())
        && RowIndependentValues.isRowIndependent(left))
      addPredicate(predicates, prop.getPropertyName(), left);
  }

  /**
   * The seeks that answer a relationship of {@code typeNames} pinned by {@code predicates}: one per type, over the index whose
   * longest leading key prefix the predicates cover. A complete key is an exact lookup, a prefix walks the contiguous range of the
   * ordered index sharing it; on an equal prefix the narrower index wins, there is nothing extra to walk. Polymorphic, as node
   * patterns are (issue #7021): an index declared on a parent type is inherited by this one, and {@code MatchEdgeByIndexStep}
   * filters an inherited index's cursor back down to the type.
   * <p>
   * A type that does not exist matches nothing and needs no seek; a type that is a subtype of another type of the union is already
   * answered by that one's seek, which returns it, and is left out so its edges are not returned twice.
   *
   * @return the seeks, or null when no type exists or one of them has no index the predicates can seek
   */
  public static List<TypeSeek> planSeeks(final Schema schema, final List<String> typeNames, final Map<String, Object> predicates) {
    if (typeNames == null || typeNames.isEmpty() || predicates.isEmpty())
      return null;

    final List<EdgeType> types = new ArrayList<>(typeNames.size());
    for (final String typeName : typeNames) {
      final DocumentType type = schema.getTypeOrNull(typeName);
      if (type == null)
        continue;
      if (!(type instanceof EdgeType edgeType))
        // A vertex or document type named in a relationship pattern: not a shape this seek answers
        return null;
      if (!types.contains(edgeType))
        types.add(edgeType);
    }

    final List<TypeSeek> seeks = new ArrayList<>(types.size());
    for (final EdgeType type : types) {
      if (coveredByAnother(type, types))
        continue;
      final TypeSeek seek = planSeek(type, predicates);
      if (seek == null)
        return null;
      seeks.add(seek);
    }
    return seeks.isEmpty() ? null : seeks;
  }

  /** The rows a set of seeks is estimated to return out of {@code edgeCount} edges of their types. */
  public static long estimateRows(final List<TypeSeek> seeks, final long edgeCount) {
    long rows = 0;
    for (final TypeSeek seek : seeks) {
      final long keys = seek.keyCount();
      // A unique key resolves one edge per value; otherwise the selectivity a node index seek is costed with, per covered column
      rows += seek.exact() ? keys :
          Math.max(keys, (long) (edgeCount * Math.max(0.001, Math.pow(0.1, seek.propertyNames().length)) * keys));
    }
    return Math.max(1, rows);
  }

  private static boolean coveredByAnother(final EdgeType type, final List<EdgeType> types) {
    for (final EdgeType other : types)
      if (other != type && type.instanceOf(other.getName()))
        return true;
    return false;
  }

  private static TypeSeek planSeek(final EdgeType type, final Map<String, Object> predicates) {
    TypeIndex bestIndex = null;
    List<String> bestKeyProps = null;
    int bestPrefix = 0;
    for (final TypeIndex index : TypeIndex.filterReadyForQueries(type.getAllIndexes(true))) {
      try {
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
        if (bestIndex == null || prefix > bestPrefix || (prefix == bestPrefix && keyProps.size() < bestKeyProps.size())) {
          bestIndex = index;
          bestKeyProps = keyProps;
          bestPrefix = prefix;
        }
      } catch (final IndexException e) {
        // dropped or rebuilt by a concurrent DDL while being read: not a candidate (issue #8918)
      }
    }
    if (bestIndex == null)
      return null;

    final String[] propertyNames = new String[bestPrefix];
    final Object[] keyValues = new Object[bestPrefix];
    boolean equalities = true;
    for (int i = 0; i < bestPrefix; i++) {
      propertyNames[i] = bestKeyProps.get(i);
      keyValues[i] = predicates.get(propertyNames[i]);
      equalities &= !(keyValues[i] instanceof InListValues);
    }
    final boolean exact = bestIndex.isUnique() && bestPrefix == bestKeyProps.size() && equalities;
    return new TypeSeek(type.getName(), bestIndex.getName(), propertyNames, keyValues, exact);
  }

  /** Keeps the value with fewer candidates for a property pinned twice: either already bounds the seek. */
  private static void addPredicate(final Map<String, Object> predicates, final String property, final Object value) {
    final Object existing = predicates.get(property);
    if (existing == null || keyValuesOf(value).size() < keyValuesOf(existing).size())
      predicates.put(property, value);
  }

  /** The candidate expressions behind a seed value: the one of an equality, the elements of an IN-list. */
  private static List<Expression> keyValuesOf(final Object value) {
    return value instanceof InListValues inList ? inList.getValues() : List.of((Expression) value);
  }

  /** An inline property value as the expression the seek evaluates: a literal, a parameter reference, or the expression itself. */
  private static Expression asExpression(final Object value) {
    if (value instanceof Expression expression)
      return expression;
    if (value instanceof CypherASTBuilder.ParameterReference parameter)
      return new ParameterExpression(parameter.getName(), "$" + parameter.getName());
    if (value == null || value instanceof Document)
      return null;
    return new LiteralExpression(value, value instanceof String text ? "'" + text + "'" : String.valueOf(value));
  }
}
