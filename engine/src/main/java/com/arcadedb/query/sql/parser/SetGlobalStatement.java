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
package com.arcadedb.query.sql.parser;

import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Map;
import java.util.Objects;

/**
 * SET GLOBAL statement - sets a database-scoped transient variable.
 * Variables set with this statement are stored in the database and accessible
 * via $varname in SQL and Cypher queries. They persist until the database is
 * closed or the variable is set to NULL.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SetGlobalStatement extends SimpleExecStatement {
  public Identifier variableName;
  public Expression expression;

  public SetGlobalStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeSimple(final CommandContext context) {
    final String varName = SQLQueryEngine.validateVariableName(variableName.getStringValue());
    final Object value = expression != null ? expression.execute((Result) null, context) : null;
    context.getDatabase().setGlobalVariable(varName, value);
    return new InternalResultSet();
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("SET GLOBAL ");
    variableName.toString(params, builder);
    builder.append(" = ");
    if (expression != null)
      expression.toString(params, builder);
    else
      builder.append("NULL");
  }

  @Override
  public SetGlobalStatement copy() {
    final SetGlobalStatement result = new SetGlobalStatement(-1);
    result.variableName = variableName == null ? null : variableName.copy();
    result.expression = expression == null ? null : expression.copy();
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final SetGlobalStatement that = (SetGlobalStatement) o;

    if (!Objects.equals(variableName, that.variableName))
      return false;
    return Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode() {
    int result = variableName != null ? variableName.hashCode() : 0;
    result = 31 * result + (expression != null ? expression.hashCode() : 0);
    return result;
  }
}
