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
import com.arcadedb.query.sql.parser.Statement;

import java.util.ArrayList;
import java.util.List;

/**
 * Expression wrapper that executes a DML statement (INSERT, UPDATE, DELETE, etc.) when evaluated.
 * Used for nested statements in expressions: INSERT INTO foo SET x = (INSERT INTO bar SET y = 1)
 */
public class StatementExpression extends BaseExpression {
  private final Statement statement;

  public StatementExpression(final Statement statement) {
    super(-1);
    this.statement = statement;
  }

  @Override
  public Object execute(final Identifiable currentRecord, final CommandContext context) {
    Object result = executeStatement(context);

    // Apply modifier if present (e.g., (INSERT ...).name or (INSERT ...)[0])
    if (modifier != null)
      result = modifier.execute(currentRecord, result, context);

    return result;
  }

  @Override
  public Object execute(final Result currentRecord, final CommandContext context) {
    Object result = executeStatement(context);

    // Apply modifier if present (e.g., (INSERT ...).name or (INSERT ...)[0])
    if (modifier != null)
      result = modifier.execute(currentRecord, result, context);

    return result;
  }

  private Object executeStatement(final CommandContext context) {
    final ResultSet rs = statement.execute(context.getDatabase(), context.getInputParameters());
    final List<Object> values = new ArrayList<>();

    while (rs.hasNext()) {
      final Result result = rs.next();
      values.add(result);
    }

    rs.close();

    // Always return the list to match JavaCC behavior
    return values;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("(").append(statement.toString()).append(")");
    if (modifier != null)
      sb.append(modifier.toString());
    return sb.toString();
  }

  @Override
  public StatementExpression copy() {
    final StatementExpression copy = new StatementExpression(statement);
    if (modifier != null)
      copy.setModifier(modifier.copy());
    return copy;
  }
}
