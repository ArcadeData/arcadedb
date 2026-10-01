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
import java.util.List;
import java.util.function.Predicate;

/**
 * Writes a {@code SET} onto a vertex that a {@code CREATE} or {@code MERGE} is about to save for the first time, so a
 * new node is written once (issue #8735). A {@code SET} that follows a save is a second write of the same record,
 * which grows it inside its page: every record after it moves, and the page's slot table is walked to fix the
 * offsets. That costs as much as the create itself for the small vertices a loader writes.
 * <p>
 * Only the shape that is the same wherever it runs is folded: {@code SET n.property = value} items on a node the
 * clause creates, whose value reads none of the variables the clause creates. The SET is a simultaneous assignment,
 * so every value reads the state before the clause, and a value that does not read the new node is the same before
 * its first save as after it. A label, a map, a dynamic key or an expression target moves or rewrites the record and
 * keeps the {@link SetClauseApplier} path.
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
      if (item.getType() != SetClause.SetType.PROPERTY || item.getVariable() == null || !isTarget.test(item.getVariable())
          || item.getProperty() == null || item.getKeyExpression() != null || item.getTargetExpression() != null
          || item.getValueExpression() == null)
        return false;
      if (CypherReferencedVariables.of(item.getValueExpression()).referencesAny(unreadable))
        return false;
    }
    return true;
  }

  /**
   * Applies {@code items} to a vertex that has not been saved yet, as {@link SetClauseApplier} would apply them to the
   * saved one: every value is evaluated and checked before any is assigned, and the statistic counts an assignment,
   * or the removal of a property the node held.
   *
   * @return the number of properties the items count as set
   */
  static int apply(final List<SetClause.SetItem> items, final MutableVertex vertex, final Result result,
      final ExpressionEvaluator evaluator, final CommandContext context) {
    final Object[] values = new Object[items.size()];
    for (int i = 0; i < values.length; i++) {
      final SetClause.SetItem item = items.get(i);
      values[i] = CypherValues.coerceAndValidatePropertyValue(evaluator.evaluate(item.getValueExpression(), result, context),
          item.getProperty(), item.getValueExpression());
    }

    int counted = 0;
    for (int i = 0; i < values.length; i++) {
      final String property = items.get(i).getProperty();
      if (values[i] == null) {
        if (vertex.has(property)) {
          vertex.remove(property);
          ++counted;
        }
      } else {
        vertex.set(property, values[i]);
        ++counted;
      }
    }
    return counted;
  }
}
