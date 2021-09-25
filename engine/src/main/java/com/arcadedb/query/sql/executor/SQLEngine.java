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
 */
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.function.DefaultSQLFunctionFactory;
import com.arcadedb.query.sql.method.DefaultSQLMethodFactory;
import com.arcadedb.query.sql.parser.ParseException;
import com.arcadedb.query.sql.parser.SqlParser;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.MultiIterator;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

public class SQLEngine {
  private static final SQLEngine                 INSTANCE = new SQLEngine();
  private final        DefaultSQLFunctionFactory functions;
  private final        DefaultSQLMethodFactory   methods;

  protected SQLEngine() {
    functions = new DefaultSQLFunctionFactory();
    methods = new DefaultSQLMethodFactory();
  }

  public static Object foreachRecord(final Callable<Object, Identifiable> iCallable, Object iCurrent, final CommandContext iContext) {
    if (iCurrent == null)
      return null;

    if (iCurrent instanceof Iterable) {
      iCurrent = ((Iterable) iCurrent).iterator();
    }
    if (MultiValue.isMultiValue(iCurrent) || iCurrent instanceof Iterator) {
      final MultiIterator<Object> result = new MultiIterator<>();
      for (Object o : MultiValue.getMultiValueIterable(iCurrent, false)) {
        if (iContext != null && !iContext.checkTimeout())
          return null;

        if (MultiValue.isMultiValue(o) || o instanceof Iterator) {
          for (Object inner : MultiValue.getMultiValueIterable(o, false)) {
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

  public static SQLEngine getInstance() {
    return INSTANCE;
  }

  public DefaultSQLFunctionFactory getFunctionFactory() {
    return functions;
  }

  public DefaultSQLMethodFactory getMethodFactory() {
    return methods;
  }

  public SQLFunction getFunction(final String name) {
    return functions.createFunction(name);
  }

  public SQLMethod getMethod(final String name) {
    return methods.createMethod(name);
  }

  public static Statement parse(final String query, final DatabaseInternal database) {
    return database.getStatementCache().get(query);
  }

  public static List<Statement> parseScript(final String script, final DatabaseInternal database) {
    final InputStream is = new ByteArrayInputStream(script.getBytes(DatabaseFactory.getDefaultCharset()));
    return parseScript(is, database);
  }

  public static List<Statement> parseScript(InputStream script, final DatabaseInternal database) {
    try {
      final SqlParser parser = new SqlParser(script);
      return parser.parseScript();
    } catch (ParseException e) {
      throw new CommandSQLParsingException(e);
    }
  }

}
