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
package com.arcadedb.gremlin;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;

import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

/**
 * Gremlin Expression builder.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

public class ArcadeGremlin extends ArcadeQuery {
  private static Long timeout;

  protected ArcadeGremlin(final ArcadeGraph graph, final String query) {
    super(graph, query);
  }

  @Override
  public ResultSet execute() {
    if (graph.database instanceof RemoteDatabase)
      return graph.database.command("gremlin", query);

    try {
      final boolean profileExecution = parameters != null && parameters.containsKey("$profileExecution") ?
          (Boolean) parameters.remove("$profileExecution") :
          false;

      final Iterator resultSet = executeStatement();

      ExecutionPlan executionPlan = null;
      if (profileExecution) {
        final String originalQuery = query;
        query += ".profile()";
        try {
          final Iterator profilerResultSet = executeStatement();
          if (profilerResultSet.hasNext()) {
            final Object result = profilerResultSet.next();
            executionPlan = new ExecutionPlan() {
              @Override
              public List<ExecutionStep> getSteps() {
                return null;
              }

              @Override
              public String prettyPrint(int depth, int indent) {
                return result.toString();
              }

              @Override
              public Result toResult() {
                return null;
              }
            };
          }
        } catch (Exception e) {
          // NO EXECUTION PLAN
        } finally {
          query = originalQuery;
        }
      }

      final ExecutionPlan activeExecutionPlan = executionPlan;

      final IteratorResultSet result = new IteratorResultSet(new Iterator() {
        @Override
        public boolean hasNext() {
          return resultSet.hasNext();
        }

        @Override
        public Object next() {
          final Object next = resultSet.next();
          if (next instanceof Document document)
            return new ResultInternal(document);
          else if (next instanceof ArcadeElement element)
            return new ResultInternal(element.getBaseElement());
          else if (next instanceof Map) {
            final Map<String, Object> stringMap = getStringObjectMap((Map<Object, Object>) next);

            return new ResultInternal(stringMap);
          }
          return new ResultInternal(Map.of("result", next));
        }
      }) {
        @Override
        public Optional<ExecutionPlan> getExecutionPlan() {
          return activeExecutionPlan != null ? Optional.of(activeExecutionPlan) : Optional.empty();
        }
      };

      return result;

    } catch (Exception e) {
      throw new CommandExecutionException("Error on executing command", e);
    }
  }

  public static Map<String, Object> getStringObjectMap(final Map<Object, Object> originalMap) {
    // TRANSFORM TO A MAP WITH STRINGS AS KEYS
    final Map<Object, Object> map = originalMap;
    final Map<String, Object> stringMap = new LinkedHashMap<>(originalMap.size());

    for (Map.Entry<Object, Object> entry : map.entrySet())
      stringMap.put(entry.getKey().toString(), entry.getValue());
    return stringMap;
  }

  public QueryEngine.AnalyzedQuery parse() {
    try {
      final DefaultGraphTraversal resultSet = (DefaultGraphTraversal) executeStatement();

      boolean idempotent = true;
      for (final Object step : resultSet.getSteps()) {
        if (step instanceof Mutating) {
          idempotent = false;
          break;
        }
      }

      final boolean isIdempotent = idempotent;

      return new QueryEngine.AnalyzedQuery() {
        @Override
        public boolean isIdempotent() {
          return isIdempotent;
        }

        @Override
        public boolean isDDL() {
          return false;
        }
      };
    } catch (Exception e) {
      throw new CommandParsingException("Error on parsing command", e);
    }
  }

  public Long getTimeout() {
    return timeout;
  }

  public ArcadeQuery setTimeout(final long timeout, final TimeUnit unit) {
    ArcadeGremlin.timeout = unit.toMillis(timeout);
    return this;
  }

  private Iterator executeStatement() throws ScriptException {
    final BasicDatabase database = graph.getDatabase();
    String gremlinEngine = database instanceof Database d ?
        d.getConfiguration().getValueAsString(GlobalConfiguration.GREMLIN_ENGINE) :
        "auto";

    if ("auto".equals(gremlinEngine)) {
      if (parameters == null || parameters.isEmpty()) {
        // NO PARAMETERS, USES THE NEW ENGINE
        try {
          return executeStatement("java");
        } catch (ScriptException e) {
          // EXCEPTION PARSING, TRYING WITH OLD GROOVY PARSER
          LogManager.instance()
              .log(this, Level.FINE, "The gremlin query '%s' could not be parsed, using the legacy `groovy` parser", e, query);
        }
      }
      gremlinEngine = "groovy";
    }

    return executeStatement(gremlinEngine);
  }

  private Iterator executeStatement(final String gremlinEngine) throws ScriptException {
    final Object result;
    if ("java".equals(gremlinEngine)) {
      // USE THE NATIVE GREMLIN PARSER
      final GremlinLangScriptEngine gremlinEngineImpl = graph.getGremlinJavaEngine();

      final SimpleBindings bindings = new SimpleBindings();
      bindings.put("g", graph.traversal());
      if (parameters != null)
        bindings.putAll(parameters);

      result = gremlinEngineImpl.eval(query, bindings);

    } else if ("groovy".equals(gremlinEngine)) {
      // GROOVY ENGINE - INSECURE, LOG WARNING
      LogManager.instance().log(this, Level.WARNING,
          "SECURITY WARNING: Using insecure Groovy Gremlin engine. This engine is vulnerable to Remote Code Execution (RCE) attacks. " +
          "Authenticated users can execute arbitrary operating system commands. DO NOT USE IN PRODUCTION. " +
          "Switch to the secure Java engine (arcadedb.gremlin.engine=java) immediately.");

      final GremlinGroovyScriptEngine gremlinEngineImpl = graph.getGremlinGroovyEngine();

      final SimpleBindings bindings = new SimpleBindings();
      bindings.put("g", graph.traversal());
      if (parameters != null)
        bindings.putAll(parameters);
      result = gremlinEngineImpl.eval(query, bindings);

    } else
      throw new IllegalArgumentException("Gremlin engine '" + gremlinEngine + "' not supported");

    if (result instanceof GraphTraversal traversal)
      return traversal;

    return Set.of(result).iterator();
  }
}
