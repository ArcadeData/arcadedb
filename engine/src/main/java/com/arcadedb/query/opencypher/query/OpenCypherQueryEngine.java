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
package com.arcadedb.query.opencypher.query;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.planner.CypherExecutionPlanner;
import com.arcadedb.query.opencypher.executor.CypherExecutionPlan;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Native OpenCypher query engine for ArcadeDB.
 * Implements direct Cypher query execution using ANTLR4 grammar-based parser.
 */
public class OpenCypherQueryEngine implements QueryEngine {
  public static final String ENGINE_NAME = "opencypher";

  // Shared stateless components - thread-safe and reusable
  private static final DefaultSQLFunctionFactory SQL_FUNCTION_FACTORY = DefaultSQLFunctionFactory.getInstance();
  private static final CypherFunctionFactory CYPHER_FUNCTION_FACTORY = new CypherFunctionFactory(SQL_FUNCTION_FACTORY);
  private static final ExpressionEvaluator EXPRESSION_EVALUATOR = new ExpressionEvaluator(CYPHER_FUNCTION_FACTORY);

  private final DatabaseInternal database;

  protected OpenCypherQueryEngine(final DatabaseInternal database) {
    this.database = database;
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
  }

  @Override
  public AnalyzedQuery analyze(final String query) {
    try {
      // Use statement cache to avoid re-parsing
      final CypherStatement statement = database.getCypherStatementCache().get(query);

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
      // Check for EXPLAIN or PROFILE prefix
      String actualQuery = query.trim();
      final String upperQuery = actualQuery.toUpperCase();
      boolean explain = false;
      boolean profile = false;

      if (upperQuery.startsWith("EXPLAIN ")) {
        explain = true;
        actualQuery = actualQuery.substring(8).trim();
      } else if (upperQuery.startsWith("PROFILE ")) {
        profile = true;
        actualQuery = actualQuery.substring(8).trim();
      }

      // Use statement cache to avoid re-parsing
      final CypherStatement statement = database.getCypherStatementCache().get(actualQuery);

      if (!statement.isReadOnly())
        throw new CommandExecutionException("Query contains write operations. Use command() instead of query()");

      return execute(actualQuery, statement, configuration, parameters, explain, profile);
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
      // Check for EXPLAIN or PROFILE prefix
      String actualQuery = query.trim();
      final String upperQuery = actualQuery.toUpperCase();
      boolean explain = false;
      boolean profile = false;

      if (upperQuery.startsWith("EXPLAIN ")) {
        explain = true;
        actualQuery = actualQuery.substring(8).trim();
      } else if (upperQuery.startsWith("PROFILE ")) {
        profile = true;
        actualQuery = actualQuery.substring(8).trim();
      }

      // Use statement cache to avoid re-parsing
      final CypherStatement statement = database.getCypherStatementCache().get(actualQuery);
      return execute(actualQuery, statement, configuration, parameters, explain, profile);
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
   * @param queryString   the original query string (for plan cache key)
   * @param statement     the parsed Cypher statement
   * @param configuration context configuration
   * @param parameters    query parameters
   * @param explain       if true, return EXPLAIN output instead of executing
   * @param profile       if true, execute with profiling and return metrics
   * @return result set
   */
  private ResultSet execute(final String queryString, final CypherStatement statement, final ContextConfiguration configuration,
      final Map<String, Object> parameters, final boolean explain, final boolean profile) {
    // Try to get cached physical plan first (saves optimization time: 200-500ms)
    PhysicalPlan physicalPlan = null;

    if (!explain && !profile) {
      // Only use plan cache for normal execution (not explain/profile)
      physicalPlan = database.getCypherPlanCache().get(queryString);
    }

    final CypherExecutionPlan plan;
    if (physicalPlan != null) {
      // Reuse cached physical plan (avoids expensive statistics collection and optimization)
      plan = new CypherExecutionPlan(
          database, statement, parameters, configuration, physicalPlan, EXPRESSION_EVALUATOR);
    } else {
      // Create new plan from scratch and cache it
      final CypherExecutionPlanner planner = new CypherExecutionPlanner(database, statement, parameters, EXPRESSION_EVALUATOR);
      plan = planner.createExecutionPlan(configuration);

      // Cache the physical plan for future use (if not explain/profile)
      if (!explain && !profile && plan.getPhysicalPlan() != null)
        database.getCypherPlanCache().put(queryString, plan.getPhysicalPlan());
    }

    if (explain)
      return plan.explain();
    if (profile)
      return plan.profile();
    return plan.execute();
  }

  /**
   * Get the shared ExpressionEvaluator instance.
   * This is used by other components that need access to the evaluator.
   *
   * @return the shared expression evaluator
   */
  public static ExpressionEvaluator getExpressionEvaluator() {
    return EXPRESSION_EVALUATOR;
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
    if (parameters != null && parameters.length > 0) {
      // Cypher uses named parameters, so treat varargs as alternating key-value pairs
      // e.g., database.query("cypher", "RETURN $x", "x", 10) → {"x": 10}
      if (parameters.length % 2 != 0) {
        throw new IllegalArgumentException(
            "Parameters must be provided as key-value pairs (e.g., \"paramName\", paramValue). Found " +
                parameters.length + " arguments.");
      }
      for (int i = 0; i < parameters.length; i += 2) {
        final Object key = parameters[i];
        if (!(key instanceof String)) {
          throw new IllegalArgumentException(
              "Parameter name at index " + i + " must be a String, but got: " +
                  (key != null ? key.getClass().getName() : "null"));
        }
        namedParams.put((String) key, parameters[i + 1]);
      }
    }
    return namedParams;
  }
}
