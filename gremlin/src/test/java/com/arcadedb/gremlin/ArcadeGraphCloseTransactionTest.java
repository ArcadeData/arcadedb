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
import org.junit.jupiter.api.Test;

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

  private long countPersons() {
    try (final ArcadeGraph graph = ArcadeGraph.open(DB_PATH)) {
      return graph.traversal().V().hasLabel("Person").count().next();
    }
  }
}
