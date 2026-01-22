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
package com.arcadedb.query.sql.antlr;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.BaseExpression;
import com.arcadedb.query.sql.parser.SelectStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Expression wrapper that executes a SELECT subquery when evaluated.
 * Used for left-side subqueries in IN conditions: (SELECT ...) IN collection
 */
public class SubqueryExpression extends BaseExpression {
  private final SelectStatement statement;

  public SubqueryExpression(final SelectStatement statement) {
    super(-1);
    this.statement = statement;
  }

  public SelectStatement getStatement() {
    return statement;
  }

  @Override
  public Object execute(final Identifiable currentRecord, final CommandContext context) {
    return executeSubquery(context);
  }

  @Override
  public Object execute(final Result currentRecord, final CommandContext context) {
    return executeSubquery(context);
  }

  private Object executeSubquery(final CommandContext context) {
    final ResultSet rs = statement.execute(context.getDatabase(), context.getInputParameters(), context);
    final List<Object> values = new ArrayList<>();

    while (rs.hasNext()) {
      final Result result = rs.next();
      // If result is a document (not a projection), add the full result
      if (result.isElement()) {
        values.add(result);
      } else if (result.getPropertyNames().size() == 1) {
        // For projections with a single value, extract that value
        values.add(result.getProperty(result.getPropertyNames().iterator().next()));
      } else {
        values.add(result);
      }
    }

    // Always return the list to match JavaCC behavior
    // Note: This is used in INSERT SET field = (SELECT ...) contexts where
    // the list is expected even for single results
    return values;
  }

  @Override
  public String toString() {
    return "(" + statement.toString() + ")";
  }

  @Override
  public SubqueryExpression copy() {
    return new SubqueryExpression(statement);
  }
}
