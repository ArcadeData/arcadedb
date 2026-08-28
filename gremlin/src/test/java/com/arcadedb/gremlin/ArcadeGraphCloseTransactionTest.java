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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Closing an {@link ArcadeGraph} must end an in-flight unit of work the way TinkerPop says it does, i.e. through the
 * transaction's configured {@code CLOSE_BEHAVIOR}, whose default is rollback. Issue #6820: it used to hard-code a
 * commit, so a unit of work that aborted before {@code commit()} became durable anyway.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ArcadeGraphCloseTransactionTest {

  private static final String DB_PATH = "./target/databases/test-graph-close-transaction";

  @BeforeEach
  void setUp() {
    try (final DatabaseFactory databaseFactory = new DatabaseFactory(DB_PATH)) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();
      final Database database = databaseFactory.create();
      database.getSchema().createVertexType("Person");
      database.close();
    }
  }

  @AfterEach
  void tearDown() {
    try (final DatabaseFactory databaseFactory = new DatabaseFactory(DB_PATH)) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();
    }
  }

  @Test
  void closingTheGraphRollsBackAnUncommittedTransaction() {
    try (final ArcadeGraph graph = ArcadeGraph.open(DB_PATH)) {
      graph.tx().begin();
      graph.addVertex(T.label, "Person", "name", "uncommitted");
    }

    assertThat(countPersons())
        .as("a unit of work never committed must not survive ArcadeGraph.close()")
        .isEqualTo(0L);
  }

  @Test
  void anAbortedUnitOfWorkLeavesNothingDurable() {
    assertThatThrownBy(() -> {
      try (final ArcadeGraph graph = ArcadeGraph.open(DB_PATH)) {
        graph.tx().begin();
        graph.addVertex(T.label, "Person", "name", "first-half");
        throw new IllegalStateException("boom");
      }
    }).isInstanceOf(IllegalStateException.class).hasMessage("boom");

    assertThat(countPersons())
        .as("try-with-resources closing the graph after a failure must not make the half-applied work durable")
        .isEqualTo(0L);
  }

  @Test
  void closingTheGraphHonoursAConfiguredCommitCloseBehaviour() {
    try (final ArcadeGraph graph = ArcadeGraph.open(DB_PATH)) {
      graph.tx().onClose(Transaction.CLOSE_BEHAVIOR.COMMIT);
      graph.tx().begin();
      graph.addVertex(T.label, "Person", "name", "explicitly-kept");
    }

    assertThat(countPersons())
        .as("an application that asks for commit-on-close must still get it")
        .isEqualTo(1L);
  }

  @Test
  void anExplicitCommitIsStillDurable() {
    try (final ArcadeGraph graph = ArcadeGraph.open(DB_PATH)) {
      graph.tx().begin();
      graph.addVertex(T.label, "Person", "name", "committed");
      graph.tx().commit();
    }

    assertThat(countPersons()).isEqualTo(1L);
  }

  @Test
  void aFailingCloseBehaviourStillReleasesTheDatabase() {
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    graph.tx().onClose(tx -> {
      throw new IllegalStateException("boom");
    });
    graph.tx().begin();
    graph.addVertex(T.label, "Person", "name", "half-written");

    assertThatThrownBy(graph::close).isInstanceOf(IllegalStateException.class).hasMessage("boom");

    assertThat(graph.getDatabase().isOpen())
        .as("a close behaviour that blows up must not cost the caller the database handle")
        .isFalse();
  }

  @Test
  void theTraversalIsRefusedOnceTheGraphIsClosed() {
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    graph.traversal().V().hasLabel("Person").count().next();
    graph.close();

    assertThatThrownBy(graph::traversal)
        .as("building a traversal after close() would resurrect the very resources close() released")
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Graph is closed");
  }

  @Test
  void concurrentCallersAllGetTheOneTraversalSource() throws Exception {
    final int threads = 16;
    final Set<GraphTraversalSource> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
    final CountDownLatch startLine = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    final ExecutorService executor = Executors.newFixedThreadPool(threads);

    try (final ArcadeGraph graph = ArcadeGraph.open(DB_PATH)) {
      for (int i = 0; i < threads; i++)
        executor.submit(() -> {
          try {
            startLine.await();
            final GraphTraversalSource mine = graph.traversal();
            synchronized (distinct) {
              distinct.add(mine);
            }
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        });

      startLine.countDown();
      assertThat(done.await(30, TimeUnit.SECONDS)).as("every caller must have finished").isTrue();
    } finally {
      executor.shutdownNow();
    }

    assertThat(distinct)
        .as("callers racing on traversal() must share one source: a loser's would be orphaned, and for a remote "
            + "graph that means a driver cluster nothing is left holding to close")
        .hasSize(1);
  }

  @Test
  void aSharedGraphNeitherDropsNorClosesTheDatabaseItDoesNotOwn() {
    // openShared() IS THE PATH ArcadeGraphManager USES FOR SERVER-MANAGED DATABASES, AND ITS CONTRACT IS THAT THIS
    // GRAPH DOES NOT OWN THE LIFECYCLE. PooledArcadeGraph OVERRIDES drop() OUTRIGHT, SO ONLY THIS EXERCISES THE
    // sharedDatabase GUARD IN ArcadeGraph ITSELF.
    final Database database = new DatabaseFactory(DB_PATH).open();
    try {
      final ArcadeGraph shared = ArcadeGraph.openShared(database);

      assertThatThrownBy(shared::drop)
          .as("a graph that does not own the database must not delete it")
          .isInstanceOf(UnsupportedOperationException.class);
      assertThat(database.isOpen()).as("and must not have got as far as touching it").isTrue();

      shared.close();
      assertThat(database.isOpen()).as("close() must leave an externally managed database open too").isTrue();
    } finally {
      if (database.isOpen())
        database.close();
    }
  }

  private long countPersons() {
    try (final ArcadeGraph graph = ArcadeGraph.open(DB_PATH)) {
      return graph.traversal().V().hasLabel("Person").count().next();
    }
  }
}
