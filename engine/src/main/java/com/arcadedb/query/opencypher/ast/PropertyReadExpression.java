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

import com.arcadedb.database.Document;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.Locale;

/**
 * An expression that reads one stored property of a record: {@code n.v}, {@code n['v']}, {@code properties(n).v},
 * {@code properties(n)['v']}, {@code a.link.v}. Every spelling of the same read tells which record and which property it
 * reads, so what depends on the property's declaration - the declared-STRING coercion of {@code =}, {@code <>}, {@code IN}
 * and the simple {@code CASE} (#9695, #9701) - answers the same whichever spelling a query uses.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface PropertyReadExpression extends Expression {

  /** The record and the property an expression reads. */
  record PropertyRead(Document record, String key) {
  }

  /**
   * The record and property this expression reads in this row, or null when it reads no stored property (a map, a
   * temporal component, a value that is not a record). Called only once a comparison has already failed under strict
   * typing, so the sub-expressions it evaluates again cost nothing on the common path.
   *
   * @param context the context to evaluate sub-expressions with; null when the caller has none, in which case only a read
   *                that needs no evaluation (a property of a bound variable) is resolved
   */
  PropertyRead propertyRead(Result row, CommandContext context);

  /** The property read of {@code expression} in this row, or null when it is not a read of a stored property. */
  static PropertyRead of(final Expression expression, final Result row, final CommandContext context) {
    return expression instanceof PropertyReadExpression read && row != null ? read.propertyRead(row, context) : null;
  }

  /**
   * The record {@code base} evaluates to, for a read whose base is itself an expression: the record, or the one a RID
   * points to; for {@code properties(x)}, the record {@code x} evaluates to, since that map holds the record's own
   * properties. Null for any other value, and when there is no context to evaluate with.
   */
  static Document recordOf(final Expression base, final Result row, final CommandContext context) {
    if (row == null)
      return null;
    if (base instanceof VariableExpression variable)
      return InlineProperties.boundRecord(row.getProperty(variable.getVariableName()));
    if (context == null)
      return null;
    if (base instanceof FunctionCallExpression call && call.getArguments().size() == 1 && "properties".equals(
        call.getFunctionName().toLowerCase(Locale.ROOT)))
      return recordOf(call.getArguments().getFirst(), row, context);
    return InlineProperties.boundRecord(OpenCypherQueryEngine.getExpressionEvaluator().evaluate(base, row, context));
  }
}
