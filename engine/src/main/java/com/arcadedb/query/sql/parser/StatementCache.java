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
import com.arcadedb.query.literal.LiteralParameterizer.Lookup;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.antlr.SQLLiteralParameterizer;

/**
 * Scan-resistant LRU cache for already parsed SQL statement executors (issue #8286: a burst of one-off statement texts, such
 * as queries that embed their values, must not evict the statements the application keeps re-running). It also acts as an
 * entry point for the SQL parser.
 * <p>
 * {@link #getParameterized(String)} also lets texts that differ only in their literal values share one entry, by extracting
 * those literals into generated parameters (issue #8307, see {@link SQLLiteralParameterizer}).
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class StatementCache {
  private final Database                db;
  private final SQLAntlrParser          antlrParser;
  private final SQLLiteralParameterizer cache;

  /**
   * @param size the size of the cache
   */
  public StatementCache(final Database db, final int size) {
    this.db = db;
    // Create ANTLR parser once, reuse for all parses
    this.antlrParser = new SQLAntlrParser(db);
    this.cache = new SQLLiteralParameterizer(antlrParser, size);
  }

  /**
   * @param statement an SQL statement
   * @return the corresponding executor, taking it from the internal cache, if it exists. The literals stay in the statement
   */
  public Statement get(final String statement) {
    return cache.lookupAsWritten(statement).statement();
  }

  /**
   * Returns the executor of a statement whose literals may have been extracted into generated parameters, so every text that
   * differs only in its literal values shares one cached statement and one plan (issue #8307). The caller binds
   * {@link Lookup#parameters()} with its own ({@link Lookup#mergeParameters(java.util.Map)}). Extraction is skipped when
   * {@link GlobalConfiguration#QUERY_LITERAL_PARAMETERIZATION} is off for the database.
   *
   * @param statement an SQL statement
   * @return the executor, its cache key and the extracted values
   * @throws CommandSQLParsingException if the input parameter is not a valid SQL statement
   */
  public Lookup<Statement> getParameterized(final String statement) {
    // a cache with no database (a syntax-only use) follows the JVM-wide default, like the Cypher one
    return cache.lookup(statement, db == null ?
        GlobalConfiguration.QUERY_LITERAL_PARAMETERIZATION.getValueAsBoolean() :
        db.getConfiguration().getValueAsBoolean(GlobalConfiguration.QUERY_LITERAL_PARAMETERIZATION));
  }

  /**
   * parses an SQL statement and returns the corresponding executor
   *
   * @param statement the SQL statement
   * @return the corresponding executor
   * @throws CommandSQLParsingException if the input parameter is not a valid SQL statement
   */
  protected Statement parse(final String statement) throws CommandSQLParsingException {
    return cache.parse(statement);
  }

  /**
   * Returns whether a statement is idempotent (read-only), using the cached parsed statement.
   * This avoids creating an AnalyzedQuery wrapper object on each call. Takes the same parameterized lookup as the execution
   * that typically follows, so both share one cache entry.
   *
   * @param statement the SQL statement string
   * @return true if the statement is read-only
   * @throws CommandSQLParsingException if the statement is invalid
   */
  public boolean isIdempotent(final String statement) {
    return getParameterized(statement).statement().isIdempotent();
  }

  /**
   * @param statement a statement text, or the cache key of a parameterized lookup
   */
  public boolean contains(final String statement) {
    return cache.contains(statement);
  }

  public void clear() {
    cache.clear();
  }
}
