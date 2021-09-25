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
package com.arcadedb.query.sql;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLEngine;
import com.arcadedb.query.sql.parser.Statement;

import java.util.Map;

public class SQLQueryEngine implements QueryEngine {
  private final DatabaseInternal database;

  public static class SQLQueryEngineFactory implements QueryEngineFactory {
    @Override
    public boolean isAvailable() {
      return true;
    }

    @Override
    public String getLanguage() {
      return "sql";
    }

    @Override
    public QueryEngine create(final DatabaseInternal database) {
      return new SQLQueryEngine(database);
    }
  }

  protected SQLQueryEngine(final DatabaseInternal database) {
    this.database = database;
  }

  @Override
  public ResultSet query(String query, Map<String, Object> parameters) {
    final Statement statement = SQLEngine.parse(query, database);
    if (!statement.isIdempotent())
      throw new IllegalArgumentException("Query '" + query + "' is not idempotent");

    return statement.execute(database, parameters);
  }

  @Override
  public ResultSet query(String query, Object... parameters) {
    final Statement statement = SQLEngine.parse(query, database);
    if (!statement.isIdempotent())
      throw new IllegalArgumentException("Query '" + query + "' is not idempotent");

    return statement.execute(database, parameters);
  }

  @Override
  public ResultSet command(String query, Map<String, Object> parameters) {
    final Statement statement = SQLEngine.parse(query, database);
    return statement.execute(database, parameters);
  }

  @Override
  public ResultSet command(String query, Object... parameters) {
    final Statement statement = SQLEngine.parse(query, database);
    return statement.execute(database, parameters);
  }
}
