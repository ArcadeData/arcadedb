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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLFunction;
import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.method.DefaultSQLMethodFactory;
import com.arcadedb.query.sql.parser.Limit;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.MultiIterator;

import com.arcadedb.query.OperationType;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.arcadedb.query.sql.parser.SqlParserTreeConstants.JJTLIMIT;

public class SQLQueryEngine implements QueryEngine {
  public static final String                    ENGINE_NAME             = "sql";
  public static final Set<String>               RESERVED_VARIABLE_NAMES = Set.of(
      "parent", "current", "depth",
      "path", "stack", "history");
  protected final     DatabaseInternal          database;
  protected final     DefaultSQLFunctionFactory functions;
  protected final     DefaultSQLMethodFactory   methods;

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
    this.functions = DefaultSQLFunctionFactory.getInstance();
    this.methods = DefaultSQLMethodFactory.getInstance();
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Map<String, Object> parameters) {
    final Statement statement = parse(query, database);
    if (!statement.isIdempotent())
      throw new QueryNotIdempotentException("Query '" + query + "' is not idempotent");

    statement.setLimit(new Limit(JJTLIMIT).setValue((int) database.getResultSetLimit()));
    return statement.execute(database, parameters);
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Object... parameters) {
    final Statement statement = parse(query, database);
    if (!statement.isIdempotent())
      throw new QueryNotIdempotentException("Query '" + query + "' is not idempotent");

    statement.setLimit(new Limit(JJTLIMIT).setValue((int) database.getResultSetLimit()));
    return statement.execute(database, parameters);
  }

  @Override
  public ResultSet command(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
    final Statement statement = parse(query, database);
    statement.setLimit(new Limit(JJTLIMIT).setValue((int) database.getResultSetLimit()));

    final CommandContext context = new BasicCommandContext();
    context.setInputParameters(parameters);
    context.setConfiguration(configuration);

    return statement.execute(executionDatabase(), parameters, context);
  }

  @Override
  public ResultSet command(final String query, ContextConfiguration configuration, final Object... parameters) {
    final Statement statement = parse(query, database);
    statement.setLimit(new Limit(JJTLIMIT).setValue((int) database.getResultSetLimit()));
    final CommandContext context = new BasicCommandContext();
    context.setConfiguration(configuration);
    return statement.execute(executionDatabase(), parameters, context);
  }

  /**
   * The database a statement executes against, and therefore the one {@code CommandContext.getDatabase()} returns.
   * <p>
   * Statements that commit mid-execution - {@code TRUNCATE TYPE}/{@code BUCKET} batching every
   * {@link com.arcadedb.GlobalConfiguration#TRUNCATE_BATCH_SIZE} records, {@code REBUILD INDEX}, {@code BatchStep} -
   * call {@code commit()} on whatever this returns. Handing them the raw instance means those commits go straight to
   * {@code LocalDatabase.commit()}, which on an HA leader applies the pages locally and never proposes them to Raft:
   * followers then trail by exactly those page versions and the next replicated entry touching one of them fails the
   * version check (#5492).
   * <p>
   * {@code SQLScriptQueryEngine} has always resolved the wrapper for the same reason, which is why the identical
   * statements replicate correctly under {@code sqlscript} and not under {@code sql}. Off HA the wrapper is the
   * instance itself, so this is a no-op there.
   * <p>
   * <b>Which paths must use this:</b> every one that can carry a statement that commits - both {@code command()}
   * overloads and {@code analyze()}'s {@code AnalyzedQuery.execute()}. {@code analyze()} is included because it is
   * not gated on idempotency: the MCP command tool executes writes through it, so it can carry a {@code TRUNCATE}.
   * <p>
   * The two {@code query()} overloads deliberately keep the field. They throw
   * {@link com.arcadedb.exception.QueryNotIdempotentException} before execution for anything non-idempotent, so no
   * statement reaching them can commit, and leaving them alone keeps read traffic - including follower-local reads -
   * resolving exactly as it did before. Add a new entry point that can execute a write, and it belongs on this
   * method; the engine itself is bound to the inner instance
   * ({@code RaftReplicatedDatabase.getQueryEngine} delegates to {@code proxied}), so the field is never the right
   * answer for anything that might commit.
   */
  private DatabaseInternal executionDatabase() {
    return database.getWrappedDatabaseInstance();
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

      @Override
      public Set<OperationType> getOperationTypes() {
        return statement.getOperationTypes();
      }

      @Override
      public ResultSet execute(final Map<String, Object> parameters) {
        final long resultSetLimit = database.getResultSetLimit();
        if (resultSetLimit > 0)
          statement.setLimit(new Limit(JJTLIMIT).setValue((int) resultSetLimit));
        // Same resolution as command(): this is a third execution entry point, not an analysis-only one.
        // The MCP command tool runs SQL exclusively through here (analyze() once, then execute(); the
        // database.command() fallback is only reached by engines whose execute() returns null), so leaving
        // the raw instance here would keep #5492 open for that caller alone.
        return statement.execute(executionDatabase(), parameters);
      }
    };
  }

  public static Object foreachRecord(final Callable<Object, Identifiable> iCallable, Object iCurrent,
      final CommandContext context) {
    if (iCurrent == null)
      return null;

    if (iCurrent instanceof Iterable iterable) {
      iCurrent = iterable.iterator();
    }
    if (MultiValue.isMultiValue(iCurrent) || iCurrent instanceof Iterator) {
      final MultiIterator<Object> result = new MultiIterator<>();
      for (final Object o : MultiValue.getMultiValueIterable(iCurrent, false)) {
        if (MultiValue.isMultiValue(o) || o instanceof Iterator) {
          for (final Object inner : MultiValue.getMultiValueIterable(o, false)) {
            result.addIterator(iCallable.call((Identifiable) inner));
          }
        } else {
          if (o instanceof Identifiable identifiable)
            result.addIterator(iCallable.call(identifiable));
          else if (o instanceof Result result1) {
            if (result1.getIdentity().isPresent())
              result.addIterator(iCallable.call(result1.getIdentity().get()));
          }
        }
      }
      return result;
    } else if (iCurrent instanceof Identifiable identifiable) {
      return iCallable.call(identifiable);
    } else if (iCurrent instanceof Result result) {
      return iCallable.call(result.toElement());
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
        final String libraryName = name.substring(0, pos);
        final String fnName = name.substring(pos + 1);

        // LOOK INTO USER-DEFINED FUNCTION LIBRARY
        if (database.getSchema().hasFunctionLibrary(libraryName)) {
          final FunctionDefinition function = database.getSchema().getFunction(libraryName, fnName);
          if (function != null) {
            // WRAP LIBRARY FUNCTION TO SQL FUNCTION TO BE EXECUTED BY SQL ENGINE
            sqlFunction = new SQLFunctionAbstract(name) {
              @Override
              public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
                  final Object[] params,
                  final CommandContext context) {
                return function.execute(params);
              }

              @Override
              public String getSyntax() {
                return null;
              }
            };
          }
        }

        // LOOK INTO UNIFIED FUNCTION REGISTRY (stateless functions: text, math, convert, date, util, etc.)
        if (sqlFunction == null) {
          final StatelessFunction statelessFn = FunctionRegistry.getStateless(name);
          if (statelessFn != null) {
            sqlFunction = new SQLFunctionAbstract(name) {
              @Override
              public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
                  final Object[] params, final CommandContext context) {
                return statelessFn.execute(params, context);
              }

              @Override
              public String getSyntax() {
                return statelessFn.getName() + "(...)";
              }
            };
          }
        }
      }
    }

    if (sqlFunction == null) {
      if ("distinct".equalsIgnoreCase(name))
        throw new CommandExecutionException(
            "'distinct' is supported only as the whole SELECT projection (e.g. `SELECT distinct(field)` or `SELECT DISTINCT field`), "
                + "not nested inside another function or as the base of a method");
      throw new CommandExecutionException("Unknown function name '" + name + "'");
    }

    return sqlFunction;
  }

  public SQLMethod getMethod(final String name) {
    return methods.createMethod(name);
  }

  public Statement parse(final String query, final DatabaseInternal database) {
    return database.getStatementCache().get(query);
  }

  public static String validateVariableName(String varName) {
    if (varName.startsWith("$"))
      varName = varName.substring(1);

    if (SQLQueryEngine.RESERVED_VARIABLE_NAMES.contains(varName))
      throw new CommandSQLParsingException(varName + " is a reserved variable");

    return varName;
  }
}
