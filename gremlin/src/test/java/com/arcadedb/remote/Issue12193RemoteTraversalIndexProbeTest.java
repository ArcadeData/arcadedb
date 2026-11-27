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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12193: the index lookups of a {@link RemoteDocumentType} answer from the server, one round trip each. The Gremlin
 * strategy asks a type for the index of every {@code has()} key of every traversal it compiles, and over a {@link RemoteDatabase}
 * (the embedded fallback of {@code ArcadeGraph.traversal()}) the answer is never usable - a remote index is never ready for
 * queries - so the strategy must not pay the round trips. Lives in {@code com.arcadedb.remote} for the package-private
 * {@code requestClusterConfiguration()}, as {@link ArcadeGraphRemoteTraversalTest} does.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12193RemoteTraversalIndexProbeTest {

  /** A database whose server answers the schema listing with one vertex type and counts the listings of its indexes. */
  private static final class CountingRemoteDatabase extends RemoteDatabase {
    final AtomicInteger indexListings = new AtomicInteger();

    CountingRemoteDatabase() {
      super("localhost", 2480, "issue12193", "root", "unused", new ContextConfiguration());
    }

    @Override
    void requestClusterConfiguration() {
      // NO SERVER: NO LEADER AND NO REPLICA, SO THE TRAVERSAL IS THE EMBEDDED FALLBACK
    }

    @Override
    protected ResultSet commandUncounted(final String language, final String command, final Map<String, Object> params) {
      final InternalResultSet answer = new InternalResultSet();
      if (command.contains("schema:types")) {
        final Map<String, Object> content = new LinkedHashMap<>();
        content.put("name", "Person");
        content.put("type", "vertex");
        content.put("records", 0);
        content.put("buckets", List.of("Person_0"));
        content.put("bucketSelectionStrategy", "round-robin");
        content.put("parentTypes", List.of());
        content.put("properties", List.of());
        answer.add(new ResultInternal(content));
      } else if (command.contains("schema:indexes"))
        indexListings.incrementAndGet();
      return answer;
    }
  }

  @Test
  void compilingAHasTraversalDoesNotListTheIndexesOfTheServer() {
    final CountingRemoteDatabase database = new CountingRemoteDatabase();

    try (final ArcadeGraph graph = ArcadeGraph.open(database)) {
      // the counter counts: asking the type for an index is a listing of the server
      database.getSchema().getType("Person").getPolymorphicIndexByProperties("name");
      assertThat(database.indexListings.get()).isEqualTo(1);
      database.indexListings.set(0);

      final GraphTraversalSource source = graph.traversal();
      source.V().hasLabel("Person").has("name", "Luca").has("age", 40).asAdmin().applyStrategies();

      assertThat(database.indexListings.get()).as("listings of schema:indexes made by the compilation of the traversal").isZero();
    }
  }
}
