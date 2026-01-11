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
package com.arcadedb.opencypher.query;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.opencypher.parser.AntlrCypherParser;
import com.arcadedb.opencypher.ast.CypherStatement;
import com.arcadedb.opencypher.planner.CypherExecutionPlanner;
import com.arcadedb.opencypher.executor.CypherExecutionPlan;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.HashMap;
import java.util.Map;

/**
 * Native OpenCypher query engine for ArcadeDB.
 * Implements direct Cypher query execution without Gremlin translation layer.
 */
public class OpenCypherQueryEngine implements QueryEngine {
  public static final String ENGINE_NAME = "opencypher";

  private final DatabaseInternal database;
  private final AntlrCypherParser parser;

  protected OpenCypherQueryEngine(final DatabaseInternal database) {
    this.database = database;
    this.parser = new AntlrCypherParser(database);
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
  }

  @Override
  public AnalyzedQuery analyze(final String query) {
    try {
      final CypherStatement statement = parser.parse(query);

      return new AnalyzedQuery() {
        @Override
        public boolean isIdempotent() {
          return statement.isReadOnly();
        }

        @Override
        public boolean isDDL() {
          // Cypher doesn't have DDL statements
          return false;
        }
      };
    } catch (final Exception e) {
      throw new CommandParsingException("Error analyzing Cypher query: " + query, e);
    }
  }

  @Override
  public ResultSet query(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
    try {
      final CypherStatement statement = parser.parse(query);

      if (!statement.isReadOnly())
        throw new CommandExecutionException("Query contains write operations. Use command() instead of query()");

      return execute(statement, configuration, parameters);
    } catch (final CommandExecutionException | CommandParsingException e) {
      throw e;
    } catch (final Exception e) {
      throw new CommandExecutionException("Error executing Cypher query: " + query, e);
    }
  }

  @Override
  public ResultSet query(final String query, final ContextConfiguration configuration, final Object... parameters) {
    return query(query, configuration, convertPositionalParameters(parameters));
  }

  @Override
  public ResultSet command(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
    try {
      final CypherStatement statement = parser.parse(query);
      return execute(statement, configuration, parameters);
    } catch (final CommandExecutionException | CommandParsingException e) {
      throw e;
    } catch (final Exception e) {
      throw new CommandExecutionException("Error executing Cypher command: " + query, e);
    }
  }

  @Override
  public ResultSet command(final String query, final ContextConfiguration configuration, final Object... parameters) {
    return command(query, configuration, convertPositionalParameters(parameters));
  }

  /**
   * Executes a parsed Cypher statement.
   *
   * @param statement     the parsed Cypher statement
   * @param configuration context configuration
   * @param parameters    query parameters
   * @return result set
   */
  private ResultSet execute(final CypherStatement statement, final ContextConfiguration configuration,
      final Map<String, Object> parameters) {
    final CypherExecutionPlanner planner = new CypherExecutionPlanner(database, statement, parameters);
    final CypherExecutionPlan plan = planner.createExecutionPlan(configuration);
    return plan.execute();
  }

  /**
   * Converts positional parameters (Object...) to named parameters map.
   * Uses parameter names like $0, $1, $2, etc.
   *
   * @param parameters positional parameters
   * @return named parameters map
   */
  private Map<String, Object> convertPositionalParameters(final Object... parameters) {
    final Map<String, Object> namedParams = new HashMap<>();
    if (parameters != null) {
      for (int i = 0; i < parameters.length; i++)
        namedParams.put(String.valueOf(i), parameters[i]);
    }
    return namedParams;
  }
}
