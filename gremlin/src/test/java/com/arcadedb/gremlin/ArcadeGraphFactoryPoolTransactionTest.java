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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A borrowed pool instance must be handed back clean. Issue #6821: {@code PooledArcadeGraph.close()} put the instance
 * back on the queue with its transaction still open, so the next borrower inherited another caller's writes and
 * committed them along with its own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ArcadeGraphFactoryPoolTransactionTest {

  private static final String DB_PATH = "./target/databases/test-graphfactory-pool-transaction";

  private ArcadeGraphFactory factory;

  @BeforeEach
  void setUp() {
    try (final DatabaseFactory databaseFactory = new DatabaseFactory(DB_PATH)) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();
      final Database database = databaseFactory.create();
      database.getSchema().createVertexType("Person");
      database.close();
    }
    factory = ArcadeGraphFactory.withLocal(DB_PATH);
  }

  @AfterEach
  void tearDown() {
    if (factory != null)
      factory.close();
    try (final DatabaseFactory databaseFactory = new DatabaseFactory(DB_PATH)) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();
    }
  }

  @Test
  void aReleasedInstanceGoesBackToThePoolWithNoOpenTransaction() {
    final ArcadeGraph first = factory.get();
    first.addVertex(T.label, "Person", "name", "uncommitted");
    assertThat(first.tx().isOpen()).as("the implicit begin() must have opened a transaction").isTrue();
    first.close();

    final ArcadeGraph second = factory.get();
    assertThat(second).as("the pool must hand back the very same instance").isSameAs(first);
    assertThat(second.tx().isOpen()).as("a pooled instance must never be borrowed with a transaction already open").isFalse();
    second.close();
  }

  @Test
  void theNextBorrowerDoesNotCommitTheAbandonedWritesOfThePreviousOne() {
    final ArcadeGraph first = factory.get();
    first.addVertex(T.label, "Person", "name", "abandoned");
    first.close();

    final ArcadeGraph second = factory.get();
    second.addVertex(T.label, "Person", "name", "mine");
    second.tx().commit();
    second.close();

    final List<String> names = new ArrayList<>();
    final ArcadeGraph reader = factory.get();
    reader.traversal().V().hasLabel("Person").values("name").forEachRemaining(n -> names.add((String) n));
    reader.close();

    assertThat(names).as("the second borrower must persist only what it wrote itself").containsExactly("mine");
  }

  @Test
  void theFactoryDoesNotMakeAbandonedWritesDurableOnClose() {
    final ArcadeGraph graph = factory.get();
    graph.addVertex(T.label, "Person", "name", "abandoned");
    graph.close();

    factory.close();
    factory = null;

    try (final DatabaseFactory databaseFactory = new DatabaseFactory(DB_PATH)) {
      final Database database = databaseFactory.open();
      try {
        assertThat(database.countType("Person", false))
            .as("disposing the pool must not commit writes nobody ever committed")
            .isEqualTo(0L);
      } finally {
        database.close();
      }
    }
  }
}
