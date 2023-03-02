/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.gremlin;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
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
    try {
      final Iterator resultSet = executeStatement();

      return new IteratorResultSet(new Iterator() {
        @Override
        public boolean hasNext() {
          return resultSet.hasNext();
        }

        @Override
        public Object next() {
          final Object next = resultSet.next();
          if (next instanceof Document)
            return new ResultInternal((Document) next);
          else if (next instanceof ArcadeElement)
            return new ResultInternal(((ArcadeElement) next).getBaseElement());
          else if (next instanceof Map)
            return new ResultInternal((Map<String, Object>) next);
          return new ResultInternal(Map.of("result", next));
        }
      });
    } catch (Exception e) {
      throw new CommandExecutionException("Error on executing command", e);
    }
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
    String gremlinEngine = graph.getDatabase().getConfiguration().getValueAsString(GlobalConfiguration.GREMLIN_ENGINE);
    if ("auto".equals(gremlinEngine)) {
      if (parameters == null || parameters.isEmpty()) {
        // NO PARAMETERS, USES THE NEW ENGINE
        try {
          return executeStatement("java");
        } catch (ScriptException e) {
          // EXCEPTION PARSING, TRYING WITH OLD GROOVY PARSER
          LogManager.instance().log(this, Level.FINE, "The gremlin query '%s' could not be parsed, using the legacy `groovy` parser", e, query);
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
      // GROOVY ENGINE
      final GremlinGroovyScriptEngine gremlinEngineImpl = graph.getGremlinGroovyEngine();

      final SimpleBindings bindings = new SimpleBindings();
      bindings.put("g", graph.traversal());
      if (parameters != null)
        bindings.putAll(parameters);
      result = gremlinEngineImpl.eval(query, bindings);

    } else
      throw new IllegalArgumentException("Gremlin engine '" + gremlinEngine + "' not supported");

    if (result instanceof GraphTraversal)
      return (GraphTraversal) result;

    return Collections.singleton(result).iterator();
  }
}
