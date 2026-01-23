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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is an LRU cache for already parsed SQL statement executors. It stores itself in the storage as a resource. It also
 * acts an an entry point for the SQL parser.
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class StatementCache {
  private final Database               db;
  private final Map<String, Statement> cache;
  private final int                    maxCacheSize;
  private final SQLAntlrParser         antlrParser;

  /**
   * @param size the size of the cache
   */
  public StatementCache(final Database db, final int size) {
    this.db = db;
    this.maxCacheSize = size;
    this.antlrParser = new SQLAntlrParser(db);  // Create ANTLR parser once, reuse for all parses
    this.cache = new LinkedHashMap<>(size) {
      @Override
      protected boolean removeEldestEntry(final Map.Entry<String, Statement> eldest) {
        return super.size() > maxCacheSize;
      }
    };
  }

  /**
   * @param statement an SQL statement
   * @return the corresponding executor, taking it from the internal cache, if it exists
   */
  public Statement get(final String statement) {
    Statement parsedStatement;
    synchronized (cache) {
      //LRU
      parsedStatement = cache.remove(statement);
      if (parsedStatement != null) {
        cache.put(statement, parsedStatement);
      }
    }
    if (parsedStatement == null) {
      parsedStatement = parse(statement);
      synchronized (cache) {
        cache.put(statement, parsedStatement);
      }
    }
    return parsedStatement;
  }

  /**
   * Returns true if the ANTLR parser should be used, false for JavaCC parser.
   */
  private boolean useAntlrParser() {
    String parserType = GlobalConfiguration.SQL_PARSER_IMPLEMENTATION.getValueAsString();
    if (db != null)
      parserType = db.getConfiguration().getValueAsString(GlobalConfiguration.SQL_PARSER_IMPLEMENTATION);

    return !"javacc".equalsIgnoreCase(parserType);
  }

  /**
   * parses an SQL statement and returns the corresponding executor
   *
   * @param statement the SQL statement
   * @return the corresponding executor
   * @throws CommandSQLParsingException if the input parameter is not a valid SQL statement
   */
  protected Statement parse(final String statement) throws CommandSQLParsingException {
    try {
      final Statement result;
      if (useAntlrParser()) {
        // Use ANTLR4-based SQL parser (reuse parser instance for efficiency)
        result = antlrParser.parse(statement);
      } else {
        // Use legacy JavaCC-based SQL parser
        result = parseWithJavaCC(statement);
      }

      result.originalStatementAsString = statement;
      return result;

    } catch (final CommandSQLParsingException e) {
      // Re-throw parsing exceptions as-is
      throw e;
    } catch (final Throwable e) {
      throwParsingException(e, statement);
    }
    return null;
  }

  /**
   * Parse using the legacy JavaCC parser.
   */
  private Statement parseWithJavaCC(final String statement) throws ParseException {
    final InputStream is = new ByteArrayInputStream(statement.getBytes(StandardCharsets.UTF_8));
    final SqlParser parser = new SqlParser(db, is);
    return parser.Parse();
  }

  protected static void throwParsingException(final Throwable e, final String statement) {
    throw new CommandSQLParsingException(e.getMessage(), e, statement);
  }

  protected static void throwParsingException(final TokenMgrError e, final String statement) {
    throw new CommandSQLParsingException(e.getMessage(), e, statement);
  }

  public boolean contains(final String statement) {
    synchronized (cache) {
      return cache.containsKey(statement);
    }
  }

  public void clear() {
    synchronized (cache) {
      cache.clear();
    }
  }
}
