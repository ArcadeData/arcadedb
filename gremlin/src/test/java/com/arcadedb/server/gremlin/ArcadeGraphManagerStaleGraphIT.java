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
package com.arcadedb.server.gremlin;

import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerDatabase;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5307: {@link ArcadeGraphManager} caches a per-database
 * {@link ArcadeGraph}/{@link TraversalSource} that wraps a {@link ServerDatabase} whose underlying
 * {@code LocalDatabase} is closed and reopened by {@code ArcadeDBServer.getDatabase()}. After the
 * server swaps in a new {@code ServerDatabase}, the manager keeps returning the stale wrapper and
 * every subsequent traversal fails forever with {@code DatabaseIsClosedException: <dbname>}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ArcadeGraphManagerStaleGraphIT extends BaseGraphServerTest {

  @Test
  void managerRebuildsGraphAfterDatabaseCloseAndReopen() {
    final String dbName = getDatabaseName();

    ArcadeGraphManager.setServer(getServer(0));
    final ArcadeGraphManager manager = new ArcadeGraphManager(new Settings());

    // First access: dynamically register the database as a Gremlin graph.
    final Graph graph1 = manager.getGraph(dbName);
    assertThat(graph1).isInstanceOf(ArcadeGraph.class);
    assertThat(((ArcadeGraph) graph1).getDatabase().isOpen()).isTrue();

    final TraversalSource ts1 = manager.getTraversalSource(dbName);
    assertThat(ts1).isNotNull();
    final long countBefore = ((GraphTraversalSource) ts1).V().count().next();

    // Simulate the real-world close/reopen: something (e.g. a batch of CREATE INDEX IF NOT EXISTS)
    // closes the underlying LocalDatabase; the next getDatabase() reopens it and swaps in a brand
    // new ServerDatabase instance in the server's databases map.
    final ServerDatabase before = getServer(0).getDatabase(dbName);
    before.getEmbedded().close();

    final ServerDatabase after = getServer(0).getDatabase(dbName);
    assertThat(after.isOpen()).as("server must have reopened the database").isTrue();
    assertThat(after).as("server must have swapped in a new ServerDatabase").isNotSameAs(before);

    // Sanity: the graph cached on the first access now wraps the closed instance.
    assertThat(((ArcadeGraph) graph1).getDatabase().isOpen())
        .as("cached graph wraps the now-closed database instance").isFalse();

    // Second access: BEFORE the fix the manager returned the stale, closed graph. AFTER the fix it
    // must detect the stale wrapper, evict it, and rebuild against the reopened database.
    final Graph graph2 = manager.getGraph(dbName);
    assertThat(graph2).isInstanceOf(ArcadeGraph.class);
    assertThat(((ArcadeGraph) graph2).getDatabase().isOpen())
        .as("manager must rebuild the graph against the reopened database").isTrue();

    // The traversal source must likewise be rebuilt and must be usable: before the fix this threw
    // DatabaseIsClosedException; now it must run cleanly against the reopened database and return the
    // same vertex count the server reports.
    final TraversalSource ts2 = manager.getTraversalSource(dbName);
    assertThat(ts2).isNotNull();
    final GraphTraversalSource g = (GraphTraversalSource) ts2;
    assertThat(g.V().count().next()).isEqualTo(countBefore);
  }
}
