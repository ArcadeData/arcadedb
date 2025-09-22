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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.log.LogManager;

import java.io.*;
import java.nio.charset.*;
import java.util.*;
import java.util.logging.*;

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

  /**
   * @param size the size of the cache
   */
  public StatementCache(final Database db, final int size) {
    this.db = db;
    this.maxCacheSize = size;
    this.cache = new LinkedHashMap<>(size) {
      @Override
      protected boolean removeEldestEntry(final Map.Entry<String, Statement> eldest) {
        return super.size() > maxCacheSize;
      }
    };
  }

  /**
   * @param statement an SQL statement
   *
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
   * parses an SQL statement and returns the corresponding executor
   *
   * @param statement the SQL statement
   *
   * @return the corresponding executor
   *
   * @throws CommandSQLParsingException if the input parameter is not a valid SQL statement
   */
  protected Statement parse(final String statement) throws CommandSQLParsingException {
    try {

      final InputStream is;

      if (db == null) {
        is = new ByteArrayInputStream(statement.getBytes(DatabaseFactory.getDefaultCharset()));
      } else {
        is = new ByteArrayInputStream(statement.getBytes(StandardCharsets.UTF_8));
      }

      SqlParser parser;
      if (db == null) {
        parser = new SqlParser(db, is);
      } else {
        try {
          parser = new SqlParser(db, is, "UTF-8");
        } catch (final UnsupportedEncodingException e2) {
          LogManager.instance().log(this, Level.WARNING, "Unsupported charset for database " + db);
          parser = new SqlParser(db, is);
        }
      }

      final Statement result = parser.Parse();

      result.originalStatementAsString = statement;
      return result;

    } catch (final ParseException e) {
      throwParsingException(e, statement);
    } catch (final TokenMgrError e2) {
      throwParsingException(e2, statement);
    } catch (final Throwable e3) {
      throwParsingException(e3, statement);
    }
    return null;
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
