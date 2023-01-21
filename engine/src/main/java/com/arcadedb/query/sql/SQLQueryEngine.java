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
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLFunction;
import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.query.sql.function.DefaultSQLFunctionFactory;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.query.sql.method.DefaultSQLMethodFactory;
import com.arcadedb.query.sql.parser.Limit;
import com.arcadedb.query.sql.parser.ParseException;
import com.arcadedb.query.sql.parser.SqlParser;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.MultiIterator;

import java.io.*;
import java.util.*;

import static com.arcadedb.query.sql.parser.SqlParserTreeConstants.JJTLIMIT;

public class SQLQueryEngine implements QueryEngine {
  public static final String                    ENGINE_NAME = "sql";
  private final       DatabaseInternal          database;
  private final       DefaultSQLFunctionFactory functions;
  private final       DefaultSQLMethodFactory   methods;

  public static class SQLQueryEngineFactory implements QueryEngineFactory {
    @Override
    public String getLanguage() {
      return ENGINE_NAME;
    }

    @Override
    public QueryEngine getInstance(final DatabaseInternal database) {
      return new SQLQueryEngine(database);
    }
  }

  protected SQLQueryEngine(final DatabaseInternal database) {
    this.database = database;
    this.functions = new DefaultSQLFunctionFactory();
    this.methods = new DefaultSQLMethodFactory();
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
  }

  @Override
  public ResultSet query(final String query, final Map<String, Object> parameters) {
    final Statement statement = parse(query, database);
    if (!statement.isIdempotent())
      throw new IllegalArgumentException("Query '" + query + "' is not idempotent");

    statement.setLimit(new Limit(JJTLIMIT).setValue((int) database.getResultSetLimit()));

    return statement.execute(database, parameters);
  }

  @Override
  public ResultSet query(final String query, final Object... parameters) {
    final Statement statement = parse(query, database);
    if (!statement.isIdempotent())
      throw new IllegalArgumentException("Query '" + query + "' is not idempotent");

    statement.setLimit(new Limit(JJTLIMIT).setValue((int) database.getResultSetLimit()));

    return statement.execute(database, parameters);
  }

  @Override
  public ResultSet command(final String query, final Map<String, Object> parameters) {
    final Statement statement = parse(query, database);

    statement.setLimit(new Limit(JJTLIMIT).setValue((int) database.getResultSetLimit()));

    return statement.execute(database, parameters);
  }

  @Override
  public ResultSet command(final String query, final Object... parameters) {
    final Statement statement = parse(query, database);

    statement.setLimit(new Limit(JJTLIMIT).setValue((int) database.getResultSetLimit()));

    return statement.execute(database, parameters);
  }

  @Override
  public AnalyzedQuery analyze(final String query) {
    final Statement statement = parse(query, database);
    return new AnalyzedQuery() {
      @Override
      public boolean isIdempotent() {
        return statement.isIdempotent();
      }

      @Override
      public boolean isDDL() {
        return statement.isDDL();
      }
    };
  }

  public static Object foreachRecord(final Callable<Object, Identifiable> iCallable, Object iCurrent, final CommandContext iContext) {
    if (iCurrent == null)
      return null;

    if (iCurrent instanceof Iterable) {
      iCurrent = ((Iterable) iCurrent).iterator();
    }
    if (MultiValue.isMultiValue(iCurrent) || iCurrent instanceof Iterator) {
      final MultiIterator<Object> result = new MultiIterator<>();
      for (final Object o : MultiValue.getMultiValueIterable(iCurrent, false)) {
        if (MultiValue.isMultiValue(o) || o instanceof Iterator) {
          for (final Object inner : MultiValue.getMultiValueIterable(o, false)) {
            result.addIterator(iCallable.call((Identifiable) inner));
          }
        } else
          result.addIterator(iCallable.call((Identifiable) o));
      }
      return result;
    } else if (iCurrent instanceof Identifiable) {
      return iCallable.call((Identifiable) iCurrent);
    } else if (iCurrent instanceof Result) {
      return iCallable.call(((Result) iCurrent).toElement());
    }

    return null;
  }

  public DefaultSQLFunctionFactory getFunctionFactory() {
    return functions;
  }

  public DefaultSQLMethodFactory getMethodFactory() {
    return methods;
  }

  public SQLFunction getFunction(final String name) {
    SQLFunction sqlFunction = functions.getFunctionInstance(name);
    if (sqlFunction == null) {
      final int pos = name.indexOf(".");
      if (pos > -1) {
        // LOOK INTO FUNCTION LIBRARY
        final String libraryName = name.substring(0, pos);
        final String fnName = name.substring(pos + 1);
        final FunctionDefinition function = database.getSchema().getFunction(libraryName, fnName);
        if (function != null) {
          // WRAP LIBRARY FUNCTION TO SQL FUNCTION TO BE EXECUTED BY SQL ENGINE
          sqlFunction = new SQLFunctionAbstract(name) {
            @Override
            public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
                final CommandContext iContext) {
              return function.execute(iParams);
            }

            @Override
            public String getSyntax() {
              return null;
            }
          };
        }
      }
    }

    if (sqlFunction == null)
      throw new CommandExecutionException("Unknown function name '" + name + "'");

    return sqlFunction;
  }

  public SQLMethod getMethod(final String name) {
    return methods.createMethod(name);
  }

  public static Statement parse(final String query, final DatabaseInternal database) {
    return database.getStatementCache().get(query);
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
}
