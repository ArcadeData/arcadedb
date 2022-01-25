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
package org.apache.tinkerpop.gremlin.arcadedb.structure;

import com.arcadedb.database.Document;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
  public ResultSet execute() throws ExecutionException, InterruptedException {
    final GremlinExecutor ge = graph.getGremlinExecutor();

    final CompletableFuture<Object> evalResult = parameters != null ? ge.eval(query, parameters) : ge.eval(query);

    final GraphTraversal resultSet = (GraphTraversal) evalResult.get();

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

        throw new IllegalArgumentException("Result of type '" + next.getClass() + "' is not supported");
      }
    });
  }

  public QueryEngine.AnalyzedQuery parse() throws ExecutionException, InterruptedException {
    final GremlinExecutor ge = graph.getGremlinExecutor();

    final CompletableFuture<Object> evalResult = parameters != null ? ge.eval(query, parameters) : ge.eval(query);

    final DefaultGraphTraversal resultSet = (DefaultGraphTraversal) evalResult.get();

    boolean idempotent = true;
    for (Object step : resultSet.getSteps()) {
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
