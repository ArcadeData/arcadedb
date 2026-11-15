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

import com.arcadedb.query.opencypher.ast.BooleanCoercionExpression;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpressionWrapper;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.IsNullExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.RegexExpression;
import com.arcadedb.query.opencypher.ast.StringMatchExpression;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.executor.operators.ParallelSafeExpressions;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Decides whether a single-hop relationship pattern can be driven from a scan of the buckets of its relationship types instead of
 * a scan of every vertex and a walk of its edge lists (issue #11874).
 * <p>
 * The walk finds the light edges of a type too, and a light edge has no record in the buckets of its type: it lives only in the
 * edge lists of its two vertices. A scan of the buckets therefore answers the pattern exactly only when no light edge can be an
 * answer, which holds when the pattern requires a property of the relationship to have a value. A light edge has no property, so
 * such a predicate is false or null for every one of them, and the regular edges of the types (their sub-types included, which the
 * pattern matches and the polymorphic scan reads) are exactly the edges the walk keeps.
 * <p>
 * The predicates that require a property: a comparison, an {@code IN}, a {@code STARTS WITH}/{@code ENDS WITH}/{@code CONTAINS},
 * a regular expression match or a boolean test with {@code r.k} as an operand, {@code r.k IS NOT NULL}, an {@code OR} whose two
 * sides each require one, and any entry of the relationship's inline property map, which never matches a missing property.
 */
public final class EdgeTypeScanPlanner {
  private EdgeTypeScanPlanner() {
  }

  /**
   * The edge types a scan of the relationship types reads, each once: a type that is a sub-type of another of the list is read by
   * that one's polymorphic scan and is left out, and a type that does not exist matches nothing.
   *
   * @return the types, or null when one of the names is a vertex or document type
   */
  public static List<String> scanTypes(final Schema schema, final List<String> typeNames) {
    final List<EdgeType> types = new ArrayList<>(typeNames.size());
    for (final String typeName : typeNames) {
      final DocumentType type = schema.getTypeOrNull(typeName);
      if (type == null)
        continue;
      if (!(type instanceof EdgeType edgeType))
        return null;
      if (!types.contains(edgeType))
        types.add(edgeType);
    }

    final List<String> names = new ArrayList<>(types.size());
    for (final EdgeType type : types)
      if (!isSubTypeOfAnother(type, types))
        names.add(type.getName());
    return names;
  }

  /** Whether a top-level conjunct of {@code where} is false or null for every relationship that lacks a property. */
  public static boolean requiresAPropertyOf(final String relationshipVariable, final BooleanExpression where) {
    for (final BooleanExpression conjunct : WhereClause.conjuncts(where))
      if (requiresAProperty(conjunct, relationshipVariable))
        return true;
    return false;
  }

  /**
   * The top-level conjuncts of {@code where} that read only the relationship (or nothing), in written order, which a scan can
   * evaluate on each edge before it loads the endpoints. Only the expressions {@link ParallelSafeExpressions} accepts qualify:
   * their variables are known exactly, and the workers of a parallel scan can evaluate them.
   */
  public static List<BooleanExpression> relationshipOnlyConjuncts(final String relationshipVariable,
      final BooleanExpression where) {
    final List<BooleanExpression> taken = new ArrayList<>();
    for (final BooleanExpression conjunct : WhereClause.conjuncts(where))
      if (ParallelSafeExpressions.isParallelSafe(conjunct, relationshipVariable))
        taken.add(conjunct);
    return taken;
  }

  /** The conjunction of {@code conjuncts} in their order, or null for none. */
  public static BooleanExpression and(final List<BooleanExpression> conjuncts) {
    BooleanExpression result = null;
    for (final BooleanExpression conjunct : conjuncts)
      result = result == null ? conjunct : new LogicalExpression(LogicalExpression.Operator.AND, result, conjunct);
    return result;
  }

  private static boolean requiresAProperty(final Object expression, final String variable) {
    return switch (expression) {
      case ComparisonExpression c -> readsAProperty(c.getLeft(), variable) || readsAProperty(c.getRight(), variable);
      case ComparisonExpressionWrapper w -> requiresAProperty(w.getComparison(), variable);
      case BooleanWrapperExpression w -> requiresAProperty(w.getBooleanExpression(), variable);
      case BooleanCoercionExpression c -> readsAProperty(c.getExpression(), variable) || requiresAProperty(c.getExpression(), variable);
      case InExpression in -> readsAProperty(in.getExpression(), variable)
          || in.getList() != null && in.getList().size() == 1 && readsAProperty(in.getList().getFirst(), variable);
      case IsNullExpression n -> n.isNot() && readsAProperty(n.getExpression(), variable);
      case StringMatchExpression s -> readsAProperty(s.getExpression(), variable) || readsAProperty(s.getPattern(), variable);
      case RegexExpression r -> readsAProperty(r.getExpression(), variable) || readsAProperty(r.getPattern(), variable);
      case LogicalExpression l when l.getOperator() == LogicalExpression.Operator.AND ->
          requiresAProperty(l.getLeft(), variable) || requiresAProperty(l.getRight(), variable);
      case LogicalExpression l when l.getOperator() == LogicalExpression.Operator.OR ->
          requiresAProperty(l.getLeft(), variable) && requiresAProperty(l.getRight(), variable);
      case null, default -> false;
    };
  }

  /**
   * Whether {@code expression} is {@code variable.k}, null on a light edge. A name starting with {@code @} is left out: a record
   * may answer such a name with its metadata rather than with a property.
   */
  private static boolean readsAProperty(final Object expression, final String variable) {
    return expression instanceof PropertyAccessExpression property && variable.equals(property.getVariableName())
        && !property.getPropertyName().startsWith("@");
  }

  private static boolean isSubTypeOfAnother(final EdgeType type, final List<EdgeType> types) {
    for (final EdgeType other : types)
      if (other != type && type.instanceOf(other.getName()))
        return true;
    return false;
  }
}
