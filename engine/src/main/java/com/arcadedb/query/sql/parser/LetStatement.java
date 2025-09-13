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
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.parser;

import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;

public class LetStatement extends SimpleExecStatement {
  protected Identifier variableName;
  protected Statement  statement;
  protected Expression expression;

  public LetStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeSimple(final CommandContext context) {
    final String varName = SQLQueryEngine.validateVariableName(variableName.getStringValue());

    Object result;
    if (expression != null) {
      result = expression.execute((Result) null, context);
    } else {
      final Map<String, Object> params = context.getInputParameters();
      result = statement.execute(context.getDatabase(), params, context);
    }
    if (result instanceof ResultSet set) {
      final InternalResultSet rs = new InternalResultSet();
      set.stream().forEach(x -> rs.add(x));
      rs.setPlan(set.getExecutionPlan().orElse(null));
      set.close();
      result = rs;
    }

    if (context != null) {
      final CommandContext varContext = context.getContextDeclaredVariable(varName);
      if (varContext != null)
        varContext.setVariable(varName, result);
      else
        // SET IN THE CURRENT CONTEXT
        context.setVariable(varName, result);
    }
    return new InternalResultSet();
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("LET ");
    variableName.toString(params, builder);
    builder.append(" = ");
    if (statement != null) {
      statement.toString(params, builder);
    } else {
      expression.toString(params, builder);
    }
  }

  @Override
  public LetStatement copy() {
    final LetStatement result = new LetStatement(-1);
    result.variableName = variableName == null ? null : variableName.copy();
    result.statement = statement == null ? null : statement.copy();
    result.expression = expression == null ? null : expression.copy();
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final LetStatement that = (LetStatement) o;

    if (!Objects.equals(variableName, that.variableName))
      return false;
    if (!Objects.equals(statement, that.statement))
      return false;
    return Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode() {
    int result = variableName != null ? variableName.hashCode() : 0;
    result = 31 * result + (statement != null ? statement.hashCode() : 0);
    result = 31 * result + (expression != null ? expression.hashCode() : 0);
    return result;
  }

  public Identifier getVariableName() {
    return variableName;
  }
}
/* JavaCC - OriginalChecksum=cc646e5449351ad9ced844f61b687928 (do not edit this line) */
