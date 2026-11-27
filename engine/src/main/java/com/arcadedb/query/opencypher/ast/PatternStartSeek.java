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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.database.Identifiable;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.executor.DistinctKeySeek;
import com.arcadedb.query.opencypher.executor.RowIndependentValues;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The candidates of the start node of a pattern whose start no outer variable binds - the uncorrelated start of a pattern
 * comprehension - read from an index when the pattern pins a property of that node to values the outer row decides, instead of
 * from a scan of every vertex of its label (issue #13622).
 * <p>
 * The values come from the conjunction of the start node's inline property map, its inline {@code WHERE} and the
 * comprehension's {@code WHERE}: an equality {@code a.p = expr} or a membership {@code a.p IN list} pins {@code p}, an
 * {@code AND} pins what either side pins, and an {@code OR} pins a property only when both sides pin it, to the union of
 * their values. So the IC14 shape {@code (a.id = startNode(r).id AND ...) OR (a.id = endNode(r).id AND ...)} seeks
 * {@code a} by the two ids. A value may read the outer row but no variable the pattern binds, since it is evaluated once,
 * before the pattern is walked, and must be deterministic, since the predicates evaluate it again for every candidate.
 * <p>
 * The seek only narrows the candidates: every predicate is still evaluated on every candidate it yields, so it answers the
 * same matches as the scan. Anything it cannot answer exactly - no single label, no single-property key index on a pinned
 * property, a value that is not a plain scalar, a concurrent index drop - answers {@code null}, and the scan stays in charge.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
final class PatternStartSeek {
  /**
   * One value source of a pinned property: an inline property value ({@link InlineProperties#resolve}), or an expression of
   * a predicate, whose value is one key or, for the right side of an {@code IN}, a list of keys.
   */
  private record Source(Object declared, boolean inline, boolean list) {
  }

  private final String                    label;
  /** Each pinned property, with the union of the values it is pinned to. In the order the pattern names them. */
  private final Map<String, List<Source>> pinned;

  private PatternStartSeek(final String label, final Map<String, List<Source>> pinned) {
    this.label = label;
    this.pinned = pinned;
  }

  /**
   * The seek of the start node of {@code pattern}, or null when nothing pins a property of it.
   *
   * @param pathVariable the path variable of the comprehension, a name the pattern binds too
   * @param where        the comprehension's {@code WHERE}, or null
   */
  static PatternStartSeek of(final PathPattern pattern, final String pathVariable, final Expression where) {
    if (pattern.getNodeCount() == 0)
      return null;
    final NodePattern start = pattern.getNode(0);
    final List<String> labels = start.getLabels();
    if (labels == null || labels.size() != 1 || start.isLabelDisjunction() || start.hasDynamicLabels())
      return null;

    // Every name the pattern binds: a value reading one is not known before the pattern is walked
    final Set<String> bound = new HashSet<>();
    for (final NodePattern node : pattern.getNodes())
      addName(bound, node.getVariable());
    for (final RelationshipPattern relationship : pattern.getRelationships())
      addName(bound, relationship.getVariable());
    addName(bound, pathVariable);
    addName(bound, pattern.getPathVariable());

    final String variable = start.getVariable() != null && !start.getVariable().isEmpty() ? start.getVariable() : null;

    Map<String, List<Source>> pinned = inlinePins(start.getProperties());
    if (variable != null) {
      if (start.hasWhereExpression())
        pinned = and(pinned, pins(start.getWhereExpression(), variable, bound));
      if (where != null)
        pinned = and(pinned, pins(where, variable, bound));
    }
    return pinned.isEmpty() ? null : new PatternStartSeek(labels.getFirst(), pinned);
  }

  /**
   * The vertices that can satisfy the pins for this row, each once, or null when no index answers them and the label has to
   * be scanned. The caller closes the returned seek.
   */
  Seek candidates(final Result row, final CommandContext context) {
    final DocumentType type = context.getDatabase().getSchema().getTypeOrNull(label);
    if (!(type instanceof VertexType))
      return null;

    final List<TypeIndex> indexes = TypeIndex.filterReadyForQueries(type.getAllIndexes(true));
    TypeIndex bestIndex = null;
    List<Object[]> bestKeys = null;
    for (final Map.Entry<String, List<Source>> entry : pinned.entrySet()) {
      final TypeIndex index = singlePropertyIndex(indexes, entry.getKey());
      if (index == null)
        continue;
      final List<Object> values = evaluate(entry.getValue(), row, context);
      if (values == null)
        continue;
      final List<Object[]> keys = DistinctKeySeek.keysOf(values, index.getKeyTypes());
      if (keys != null && (bestKeys == null || keys.size() < bestKeys.size())) {
        bestIndex = index;
        bestKeys = keys;
      }
    }
    if (bestKeys == null)
      return null;

    // The scan of the label asks the read permission of the type and of its subtypes, whose vertices the index holds too
    SecurityHelper.checkReadOnTypeAndSubTypes(context.getDatabase(), type);

    // No value can equal anything (only nulls): no vertex satisfies the pins
    if (bestKeys.isEmpty())
      return new Seek(Collections.emptyIterator(), null);

    final DistinctKeySeek seek = DistinctKeySeek.open(context, "pattern comprehension", bestIndex, bestKeys);
    if (seek == null)
      return null;
    final Iterator<Identifiable> records = Labels.isInheritedIndex(bestIndex, label) ?
        Labels.filterByLabel(seek, context.getDatabase(), label) :
        seek;
    return new Seek(records, seek);
  }

  /** The candidates of one evaluation, and the index seek to close once they are walked (null when none was opened). */
  record Seek(Iterator<Identifiable> records, DistinctKeySeek seek) implements AutoCloseable {
    @Override
    public void close() {
      if (seek != null)
        seek.close();
    }
  }

  /**
   * The values the sources pin the property to for this row, null elements included (they equal nothing and are dropped
   * later), or null when one of them cannot be evaluated here: the scan then evaluates the predicate itself, error included.
   */
  private static List<Object> evaluate(final List<Source> sources, final Result row, final CommandContext context) {
    final List<Object> values = new ArrayList<>(sources.size());
    try {
      for (final Source source : sources) {
        final Object value = source.inline() ?
            InlineProperties.resolve(source.declared(), row, context) :
            OpenCypherQueryEngine.getExpressionEvaluator().evaluate((Expression) source.declared(), row, context);
        if (!source.list())
          values.add(value);
        else if (value != null)
          // x IN null is null: no element. A value that is no list is refused by the predicate, so the scan answers it
          for (final Object element : InExpression.listOperand(value))
            values.add(element);
      }
    } catch (final Exception e) {
      return null;
    }
    return values;
  }

  private static TypeIndex singlePropertyIndex(final List<TypeIndex> indexes, final String propertyName) {
    for (final TypeIndex index : indexes) {
      final List<String> indexProperties = index.getPropertyNamesIfExactKeyLookup();
      if (indexProperties != null && indexProperties.size() == 1 && indexProperties.getFirst().equals(propertyName))
        return index;
    }
    return null;
  }

  private static Map<String, List<Source>> inlinePins(final Map<String, Object> properties) {
    if (properties == null || properties.isEmpty())
      return Map.of();
    final Map<String, List<Source>> pinned = new LinkedHashMap<>();
    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final Object declared = entry.getValue();
      // The map is resolved again for every candidate: a value that can change between two evaluations is not sought
      if (declared instanceof Expression expression && !RowIndependentValues.isDeterministic(expression))
        continue;
      pinned.put(entry.getKey(), List.of(new Source(declared, true, false)));
    }
    return pinned;
  }

  /** What {@code predicate} pins the property of {@code variable} to; empty when it pins nothing. */
  private static Map<String, List<Source>> pins(final Object predicate, final String variable, final Set<String> bound) {
    if (predicate instanceof BooleanWrapperExpression wrapper)
      return pins(wrapper.getBooleanExpression(), variable, bound);
    if (predicate instanceof ComparisonExpressionWrapper wrapper)
      return pins(wrapper.getComparison(), variable, bound);
    if (predicate instanceof TernaryLogicalExpression logical)
      return switch (logical.getOperator()) {
        case AND -> and(pins(logical.getLeft(), variable, bound), pins(logical.getRight(), variable, bound));
        case OR -> or(pins(logical.getLeft(), variable, bound), pins(logical.getRight(), variable, bound));
        default -> Map.of();
      };
    if (predicate instanceof LogicalExpression logical)
      return switch (logical.getOperator()) {
        case AND -> and(pins(logical.getLeft(), variable, bound), pins(logical.getRight(), variable, bound));
        case OR -> or(pins(logical.getLeft(), variable, bound), pins(logical.getRight(), variable, bound));
        // NOT (a.p <> v) holds exactly where a.p = v does
        case NOT -> logical.getNegatedInequality() != null ? pins(equality(logical.getNegatedInequality()), variable, bound) : Map.of();
        default -> Map.of();
      };
    if (predicate instanceof ComparisonExpression comparison && comparison.getOperator() == ComparisonExpression.Operator.EQUALS) {
      if (isPropertyOf(comparison.getLeft(), variable) && isSeekValue(comparison.getRight(), bound))
        return pin(((PropertyAccessExpression) comparison.getLeft()).getPropertyName(), comparison.getRight(), false);
      if (isPropertyOf(comparison.getRight(), variable) && isSeekValue(comparison.getLeft(), bound))
        return pin(((PropertyAccessExpression) comparison.getRight()).getPropertyName(), comparison.getLeft(), false);
      return Map.of();
    }
    if (predicate instanceof InExpression in && isPropertyOf(in.getExpression(), variable)) {
      final List<Expression> list = in.getList();
      final List<Source> sources = new ArrayList<>(list.size());
      for (final Expression element : list) {
        if (!isSeekValue(element, bound))
          return Map.of();
        // A single right side is the list itself (x IN $ids, x IN [1, 2]); several are the items of a list literal
        sources.add(new Source(element, false, list.size() == 1));
      }
      return Map.of(((PropertyAccessExpression) in.getExpression()).getPropertyName(), sources);
    }
    return Map.of();
  }

  private static ComparisonExpression equality(final ComparisonExpression inequality) {
    return new ComparisonExpression(inequality.getLeft(), ComparisonExpression.Operator.EQUALS, inequality.getRight());
  }

  private static Map<String, List<Source>> pin(final String property, final Expression value, final boolean list) {
    return Map.of(property, List.of(new Source(value, false, list)));
  }

  /** Both hold: a property either side pins is pinned, to the fewer values when both pin it. */
  private static Map<String, List<Source>> and(final Map<String, List<Source>> left, final Map<String, List<Source>> right) {
    if (right.isEmpty())
      return left;
    if (left.isEmpty())
      return right;
    final Map<String, List<Source>> merged = new LinkedHashMap<>(left);
    for (final Map.Entry<String, List<Source>> entry : right.entrySet())
      merged.merge(entry.getKey(), entry.getValue(), (a, b) -> b.size() < a.size() ? b : a);
    return merged;
  }

  /** Either holds: only a property both sides pin is pinned, to the values of both. */
  private static Map<String, List<Source>> or(final Map<String, List<Source>> left, final Map<String, List<Source>> right) {
    if (left.isEmpty() || right.isEmpty())
      return Map.of();
    final Map<String, List<Source>> merged = new LinkedHashMap<>();
    for (final Map.Entry<String, List<Source>> entry : left.entrySet()) {
      final List<Source> other = right.get(entry.getKey());
      if (other != null) {
        final List<Source> union = new ArrayList<>(entry.getValue().size() + other.size());
        union.addAll(entry.getValue());
        union.addAll(other);
        merged.put(entry.getKey(), union);
      }
    }
    return merged;
  }

  private static boolean isPropertyOf(final Expression expression, final String variable) {
    return expression instanceof PropertyAccessExpression property && variable.equals(property.getVariableName());
  }

  /** A value known before the pattern is walked, and the same each time it is evaluated against the row. */
  private static boolean isSeekValue(final Expression expression, final Set<String> bound) {
    return expression != null && !CypherReferencedVariables.of(expression).referencesAny(bound)
        && RowIndependentValues.isDeterministic(expression);
  }

  private static void addName(final Set<String> names, final String name) {
    if (name != null && !name.isEmpty())
      names.add(name);
  }
}
