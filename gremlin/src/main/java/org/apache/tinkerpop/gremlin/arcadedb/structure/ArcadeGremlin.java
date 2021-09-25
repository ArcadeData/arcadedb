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
package org.apache.tinkerpop.gremlin.arcadedb.structure;

import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.ConcurrentBindings;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

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
  protected Long timeout;

  protected ArcadeGremlin(final ArcadeGraph graph, final String query) {
    super(graph, query);
  }

  @Override
  public ResultSet execute() throws ExecutionException, InterruptedException {
    final GraphTraversalSource g = AnonymousTraversalSource.traversal().withEmbedded(graph);

    final ConcurrentBindings b = new ConcurrentBindings();
    b.putIfAbsent("g", g);

    if (parameters != null)
      // BIND THE PARAMETERS
      for (Map.Entry<String, Object> entry : parameters.entrySet())
        b.put(entry.getKey(), entry.getValue());

    final GremlinExecutor.Builder builder = GremlinExecutor.build();
    builder.globalBindings(b);
    if (timeout != null)
      builder.evaluationTimeout(timeout);
    final GremlinExecutor ge = builder.create();

    final CompletableFuture<Object> evalResult = ge.eval(query);

    final GraphTraversal resultSet = (GraphTraversal) evalResult.get();

    return new IteratorResultSet(new Iterator() {
      @Override
      public boolean hasNext() {
        return resultSet.hasNext();
      }

      @Override
      public Object next() {
        return new ResultInternal((Map<String, Object>) resultSet.next());
      }
    });
  }

  public Long getTimeout() {
    return timeout;
  }

  public ArcadeQuery setTimeout(final long timeout, final TimeUnit unit) {
    this.timeout = unit.toMillis(timeout);
    return this;
  }

}
