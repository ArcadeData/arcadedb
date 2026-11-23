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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.executor.CypherValues;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Writes a {@code SET} onto a vertex that a {@code CREATE} or {@code MERGE} is about to save for the first time, so a
 * new node is written once (issue #8735). A {@code SET} that follows a save is a second write of the same record,
 * which grows it inside its page: every record after it moves, and the page's slot table is walked to fix the
 * offsets. That costs as much as the create itself for the small vertices a loader writes.
 * <p>
 * Only the shape that is the same wherever it runs is folded: {@code SET n.property = value}, {@code SET n = map} and
 * {@code SET n += map} items on a node the clause creates, whose value reads none of the variables the clause creates.
 * The SET is a simultaneous assignment, so every value reads the state before the clause, and a value that does not
 * read the new node is the same before its first save as after it. A label, a dynamic key or an expression target
 * moves or rewrites the record and keeps the {@link SetClauseApplier} path.
 * <p>
 * The map forms matter most (issue #11925): {@code CREATE (n:N) SET n = row} saved every node empty and gave it its
 * properties at commit, when the page already held hundreds of empty records. Almost no growth fit there, so nearly
 * every record was spilled into a chunk chain, each spill after walking the page's whole slot table.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class NewNodeSetFolding {
  private NewNodeSetFolding() {
  }

  /**
   * @param clause     the SET clause to fold, may be null
   * @param isTarget   whether a variable names a node the creating clause creates
   * @param unreadable the variables the creating clause creates, none of which a value may read
   */
  static boolean isFoldable(final SetClause clause, final Predicate<String> isTarget, final Collection<String> unreadable) {
    if (clause == null || clause.isEmpty())
      return false;
    for (final SetClause.SetItem item : clause.getItems()) {
      if (item.getVariable() == null || !isTarget.test(item.getVariable()) || item.getKeyExpression() != null
          || item.getTargetExpression() != null || item.getValueExpression() == null)
        return false;
      switch (item.getType()) {
      case PROPERTY -> {
        if (item.getProperty() == null)
          return false;
      }
      case REPLACE_MAP, MERGE_MAP -> {
      }
      default -> {
        return false;
      }
      }
      if (CypherReferencedVariables.of(item.getValueExpression()).referencesAny(unreadable))
        return false;
    }
    return true;
  }

  /**
   * Applies {@code items} to a vertex that has not been saved yet, as {@link SetClauseApplier} would apply them to the
   * saved one: every value is evaluated and checked before any is assigned, and the statistic counts what the applier
   * counts: an assignment, the removal of a property the node held, and for {@code SET n = map} every property of the
   * node the map does not set again.
   *
   * @return the number of properties the items count as set
   */
  static int apply(final List<SetClause.SetItem> items, final MutableVertex vertex, final Result result,
      final ExpressionEvaluator evaluator, final CommandContext context) {
    final Object[] values = new Object[items.size()];
    for (int i = 0; i < values.length; i++) {
      final SetClause.SetItem item = items.get(i);
      final Object value = evaluator.evaluate(item.getValueExpression(), result, context);
      values[i] = switch (item.getType()) {
        case PROPERTY -> CypherValues.coerceAndValidatePropertyValue(value, item.getProperty(), item.getValueExpression());
        case REPLACE_MAP -> SetClauseApplier.toPropertyMap(value, "=", item.getValueExpression());
        default -> SetClauseApplier.toPropertyMap(value, "+=", item.getValueExpression());
      };
    }

    int counted = 0;
    for (int i = 0; i < values.length; i++) {
      final SetClause.SetItem item = items.get(i);
      switch (item.getType()) {
      case PROPERTY -> counted += setOrRemove(vertex, item.getProperty(), values[i]);
      case REPLACE_MAP -> counted += replaceMap(vertex, asPropertyMap(values[i]));
      default -> counted += mergeMap(vertex, asPropertyMap(values[i]));
      }
    }
    return counted;
  }

  private static int setOrRemove(final MutableVertex vertex, final String property, final Object value) {
    if (value != null) {
      vertex.set(property, value);
      return 1;
    }
    if (!vertex.has(property))
      return 0;
    vertex.remove(property);
    return 1;
  }

  /** {@code SET n = map}, as {@code SetClauseApplier.applyReplaceMap} writes it. A null map is a no-op. */
  private static int replaceMap(final MutableVertex vertex, final Map<String, Object> map) {
    if (map == null)
      return 0;
    final Set<String> existing = new HashSet<>(vertex.getPropertyNames());
    for (final String property : existing)
      if (!property.startsWith("@"))
        vertex.remove(property);

    int counted = 0;
    for (final Map.Entry<String, Object> entry : map.entrySet())
      if (entry.getValue() != null) {
        vertex.set(entry.getKey(), entry.getValue());
        ++counted;
      }
    return counted + CypherStatisticsHelper.countRemovedProperties(existing, map);
  }

  /** {@code SET n += map}, as {@code SetClauseApplier.applyMergeMap} writes it. A null map is a no-op. */
  private static int mergeMap(final MutableVertex vertex, final Map<String, Object> map) {
    if (map == null)
      return 0;
    int counted = 0;
    for (final Map.Entry<String, Object> entry : map.entrySet())
      counted += setOrRemove(vertex, entry.getKey(), entry.getValue());
    return counted;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> asPropertyMap(final Object value) {
    return (Map<String, Object>) value;
  }
}
