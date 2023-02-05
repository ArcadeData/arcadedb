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
package com.arcadedb.query.sql;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalExecutionPlan;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.ScriptExecutionPlan;
import com.arcadedb.query.sql.parser.BeginStatement;
import com.arcadedb.query.sql.parser.CommitStatement;
import com.arcadedb.query.sql.parser.LetStatement;
import com.arcadedb.query.sql.parser.Limit;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.ParseException;
import com.arcadedb.query.sql.parser.SqlParser;
import com.arcadedb.query.sql.parser.Statement;

import java.io.*;
import java.util.*;

import static com.arcadedb.query.sql.parser.SqlParserTreeConstants.JJTLIMIT;

public class SQLScriptQueryEngine extends SQLQueryEngine {
  public static final String ENGINE_NAME = "sqlscript";

  public static class SQLScriptQueryEngineFactory implements QueryEngineFactory {
    @Override
    public String getLanguage() {
      return ENGINE_NAME;
    }

    @Override
    public QueryEngine getInstance(final DatabaseInternal database) {
      return new SQLScriptQueryEngine(database);
    }
  }

  protected SQLScriptQueryEngine(final DatabaseInternal database) {
    super(database);
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
  }

  @Override
  public ResultSet query(final String query, final Map<String, Object> parameters) {
    final List<Statement> statements = parseScript(query, database);
    statements.stream().map((statement) -> {
      if (statement.isIdempotent())
        throw new IllegalArgumentException("Query '" + query + "' is not idempotent");
      return null;
    });

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database.getWrappedDatabaseInstance());
    context.setInputParameters(parameters);
    return executeInternal(statements, new BasicCommandContext());
  }

  @Override
  public ResultSet query(final String query, final Object... parameters) {
    final List<Statement> statements = parseScript(query, database);
    statements.stream().map((statement) -> {
      if (statement.isIdempotent())
        throw new IllegalArgumentException("Query '" + query + "' is not idempotent");
      return null;
    });

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database.getWrappedDatabaseInstance());
    context.setInputParameters(parameters);
    return executeInternal(statements, new BasicCommandContext());
  }

  @Override
  public ResultSet command(final String query, final Map<String, Object> parameters) {
    final List<Statement> statements = parseScript(query, database.getWrappedDatabaseInstance());

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database.getWrappedDatabaseInstance());
    context.setInputParameters(parameters);

    return executeInternal(statements, context);
  }

  @Override
  public ResultSet command(final String query, final Object... parameters) {
    final List<Statement> statements = parseScript(query, database.getWrappedDatabaseInstance());

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database.getWrappedDatabaseInstance());
    context.setInputParameters(parameters);
    return executeInternal(statements, context);
  }

  public static List<Statement> parseScript(final String script, final DatabaseInternal database) {
    final InputStream is = new ByteArrayInputStream(addSemicolons(script).getBytes(DatabaseFactory.getDefaultCharset()));
    return parseScript(is, database);
  }

  public static List<Statement> parseScript(final InputStream script, final DatabaseInternal database) {
    try {
      final SqlParser parser = new SqlParser(database, script);
      return parser.ParseScript();
    } catch (final ParseException e) {
      throw new CommandSQLParsingException(e);
    }
  }

  private static String addSemicolons(final String parserText) {
    String[] rows = parserText.split("\n");
    StringBuilder builder = new StringBuilder();
    for (String row : rows) {
      row = row.trim();
      builder.append(row);
      if (!(row.endsWith(";") || row.endsWith("{"))) {
        builder.append(";");
      }
      builder.append("\n");
    }
    return builder.toString();
  }

  private ResultSet executeInternal(final List<Statement> statements, final CommandContext scriptContext) {
    final ScriptExecutionPlan plan = new ScriptExecutionPlan(scriptContext);

    plan.setStatements(statements);

    List<Statement> lastRetryBlock = new ArrayList<>();
    int nestedTxLevel = 0;

    for (final Statement stm : statements) {
      stm.setOriginalStatement(stm);
      stm.setLimit(new Limit(JJTLIMIT).setValue((int) database.getResultSetLimit()));

      if (stm instanceof BeginStatement)
        nestedTxLevel++;

      if (nestedTxLevel <= 0) {
        final InternalExecutionPlan sub = stm.createExecutionPlan(scriptContext);
        plan.chain(sub, false);
      } else
        lastRetryBlock.add(stm);

      if (stm instanceof CommitStatement && nestedTxLevel > 0) {
        nestedTxLevel--;
        if (nestedTxLevel == 0) {

          for (final Statement statement : lastRetryBlock) {
            final InternalExecutionPlan sub = statement.createExecutionPlan(scriptContext);
            plan.chain(sub, false);
          }
          lastRetryBlock = new ArrayList<>();
        }
      }

      if (stm instanceof LetStatement)
        scriptContext.declareScriptVariable(((LetStatement) stm).getName().getStringValue());
    }

    return new LocalResultSet(plan);
  }
}
