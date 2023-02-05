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

import com.arcadedb.database.Document;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.DefaultGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;

import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.*;
import java.util.concurrent.*;

/**
 * Gremlin Expression builder.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

public class ArcadeGremlin extends ArcadeQuery {
  private static Long                timeout;
  private final  GremlinScriptEngine engine;

  protected ArcadeGremlin(final ArcadeGraph graph, final String query) {
    super(graph, query);

    String nativeGremlin = System.getProperty("arcadedb.native-gremlin");
    if ("true".equals(nativeGremlin)) {
      // USE THE NATIVE GREMLIN PARSER
      final GremlinLangScriptEngine gremlinLangScriptEngine = new GremlinLangScriptEngine();
      final GremlinScriptEngineFactory factory = gremlinLangScriptEngine.getFactory();
      factory.setCustomizerManager(new DefaultGremlinScriptEngineManager());
      engine = factory.getScriptEngine();
    } else
      engine = null;
  }

  @Override
  public ResultSet execute() throws ExecutionException, InterruptedException {

    try {
      final GraphTraversal resultSet;
      if (engine == null) {
        final GremlinExecutor ge = graph.getGremlinExecutor();
        final CompletableFuture<Object> evalResult = parameters != null ? ge.eval(query, parameters) : ge.eval(query);
        resultSet = (GraphTraversal) evalResult.get();
      } else {
        final SimpleBindings bindings = new SimpleBindings();
        bindings.put("g", graph.traversal());
        if (parameters != null)
          bindings.putAll(parameters);

        resultSet = (GraphTraversal) engine.eval(query, bindings);
      }

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
    } catch (ScriptException e) {
      throw new RuntimeException(e);
    }
  }

  public QueryEngine.AnalyzedQuery parse() throws ExecutionException, InterruptedException {
    final GremlinExecutor ge = graph.getGremlinExecutor();

    final CompletableFuture<Object> evalResult = parameters != null ? ge.eval(query, parameters) : ge.eval(query);

    final DefaultGraphTraversal resultSet = (DefaultGraphTraversal) evalResult.get();

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
  }

  public Long getTimeout() {
    return timeout;
  }

  public ArcadeQuery setTimeout(final long timeout, final TimeUnit unit) {
    ArcadeGremlin.timeout = unit.toMillis(timeout);
    return this;
  }

}
