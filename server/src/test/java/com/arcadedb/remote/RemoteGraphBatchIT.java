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

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link RemoteGraphBatch}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RemoteGraphBatchIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-batch";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }

  @Test
  void batchVerticesAndEdges() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE KNOWS");

    try (final RemoteGraphBatch batch = database.batch().build()) {
      final String alice = batch.createVertex("Person", "name", "Alice", "age", 30);
      final String bob = batch.createVertex("Person", "name", "Bob", "age", 25);
      batch.createEdge("KNOWS", alice, bob, "since", 2020);
    }

    // Verify vertices
    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM Person")) {
      final Result r = rs.nextIfAvailable();
      assertThat(((Number) r.getProperty("cnt")).longValue()).isEqualTo(2);
    }

    // Verify edges
    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM KNOWS")) {
      final Result r = rs.nextIfAvailable();
      assertThat(((Number) r.getProperty("cnt")).longValue()).isEqualTo(1);
    }

    // Verify edge properties
    try (final ResultSet rs = database.query("sql", "SELECT since FROM KNOWS")) {
      final Result r = rs.nextIfAvailable();
      assertThat(((Number) r.getProperty("since")).intValue()).isEqualTo(2020);
    }

    database.close();
  }

  @Test
  void batchResult() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE City IF NOT EXISTS");
    database.command("sql", "CREATE EDGE TYPE ROAD IF NOT EXISTS");

    final RemoteGraphBatch batch = database.batch()
        .withLightEdges(true)
        .build();

    final String rome = batch.createVertex("City", "name", "Rome");
    final String milan = batch.createVertex("City", "name", "Milan");
    final String naples = batch.createVertex("City", "name", "Naples");
    batch.createEdge("ROAD", rome, milan);
    batch.createEdge("ROAD", rome, naples);
    batch.close();

    final RemoteBatchResult result = batch.getResult();
    assertThat(result.getVerticesCreated()).isEqualTo(3L);
    assertThat(result.getEdgesCreated()).isEqualTo(2L);
    assertThat(result.getElapsedMs()).isGreaterThanOrEqualTo(0L);

    database.close();
  }

  @Test
  void batchVerticesOnly() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE Product IF NOT EXISTS");

    try (final RemoteGraphBatch batch = database.batch().build()) {
      for (int i = 0; i < 100; i++)
        batch.createVertex("Product", "name", "Product-" + i, "price", i * 10.5);
    }

    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM Product")) {
      final Result r = rs.nextIfAvailable();
      assertThat(((Number) r.getProperty("cnt")).longValue()).isEqualTo(100);
    }

    database.close();
  }

  @Test
  void batchWithBuilderOptions() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE Node IF NOT EXISTS");
    database.command("sql", "CREATE EDGE TYPE Link IF NOT EXISTS");

    try (final RemoteGraphBatch batch = database.batch()
        .withBatchSize(500_000)
        .withCommitEvery(10_000)
        .withLightEdges(false)
        .withBidirectional(true)
        .build()) {

      final String n1 = batch.createVertex("Node", "label", "A");
      final String n2 = batch.createVertex("Node", "label", "B");
      batch.createEdge("Link", n1, n2, "weight", 1.5);
    }

    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM Node")) {
      assertThat(((Number) rs.nextIfAvailable().getProperty("cnt")).longValue()).isEqualTo(2);
    }

    database.close();
  }

  @Test
  void emptyBatch() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    final RemoteGraphBatch batch = database.batch().build();
    batch.close();

    final RemoteBatchResult result = batch.getResult();
    assertThat(result.getVerticesCreated()).isEqualTo(0L);
    assertThat(result.getEdgesCreated()).isEqualTo(0L);

    database.close();
  }

  @Test
  void batchWithSpecialCharacters() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE Article IF NOT EXISTS");

    try (final RemoteGraphBatch batch = database.batch().build()) {
      batch.createVertex("Article", "title", "He said \"hello\"", "body", "line1\nline2\ttab");
    }

    try (final ResultSet rs = database.query("sql", "SELECT title, body FROM Article")) {
      final Result r = rs.nextIfAvailable();
      assertThat((String) r.getProperty("title")).isEqualTo("He said \"hello\"");
      assertThat((String) r.getProperty("body")).isEqualTo("line1\nline2\ttab");
    }

    database.close();
  }

  @Test
  void autoFlushVertices() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE Item IF NOT EXISTS");

    final int total = 250;
    final int flushEvery = 100;

    // Creates 250 vertices with flushEvery=100, so 3 HTTP requests (100 + 100 + 50)
    try (final RemoteGraphBatch batch = database.batch().withFlushEvery(flushEvery).build()) {
      for (int i = 0; i < total; i++)
        batch.createVertex("Item", "idx", i);
    }

    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM Item")) {
      assertThat(((Number) rs.nextIfAvailable().getProperty("cnt")).longValue()).isEqualTo(total);
    }

    database.close();
  }

  @Test
  void autoFlushEdgesWithCrossFlushReferences() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE Person IF NOT EXISTS");
    database.command("sql", "CREATE EDGE TYPE KNOWS IF NOT EXISTS");

    final int vertexCount = 200;
    final int flushEvery = 50;

    // Vertices flush at 50-item boundaries, edges reference across flush boundaries
    final RemoteGraphBatch batch = database.batch().withFlushEvery(flushEvery).build();

    final String[] ids = new String[vertexCount];
    for (int i = 0; i < vertexCount; i++)
      ids[i] = batch.createVertex("Person", "idx", i);

    // Create edges that span flush boundaries:
    // vertex 0 (flushed in batch 1) → vertex 199 (flushed in batch 4)
    // vertex 49 (flushed in batch 1) → vertex 150 (flushed in batch 4)
    for (int i = 0; i < vertexCount - 1; i++)
      batch.createEdge("KNOWS", ids[i], ids[i + 1]);

    batch.close();

    final RemoteBatchResult result = batch.getResult();
    assertThat(result.getVerticesCreated()).isEqualTo(vertexCount);
    assertThat(result.getEdgesCreated()).isEqualTo(vertexCount - 1);

    // Verify a cross-flush edge: vertex 0 → vertex 1
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(out('KNOWS')) FROM Person WHERE idx = 0")) {
      final Result r = rs.nextIfAvailable();
      assertThat(r).isNotNull();
      assertThat(((Number) r.getProperty("idx")).intValue()).isEqualTo(1);
    }

    // Verify an edge deep in the chain: vertex 150 → vertex 151
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(out('KNOWS')) FROM Person WHERE idx = 150")) {
      final Result r = rs.nextIfAvailable();
      assertThat(r).isNotNull();
      assertThat(((Number) r.getProperty("idx")).intValue()).isEqualTo(151);
    }

    database.close();
  }

  @Test
  void explicitFlush() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE Node IF NOT EXISTS");
    database.command("sql", "CREATE EDGE TYPE Link IF NOT EXISTS");

    // Disable auto-flush, control flushing explicitly
    final RemoteGraphBatch batch = database.batch().withFlushEvery(0).build();

    final String a = batch.createVertex("Node", "name", "A");
    final String b = batch.createVertex("Node", "name", "B");
    batch.flush(); // send vertices, resolve temp IDs

    // Edges reference already-flushed vertices (resolved client-side to real RIDs)
    batch.createEdge("Link", a, b);
    batch.close();

    final RemoteBatchResult result = batch.getResult();
    assertThat(result.getVerticesCreated()).isEqualTo(2L);
    assertThat(result.getEdgesCreated()).isEqualTo(1L);

    database.close();
  }

  @Test
  void autoFlushAggregatesResult() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE V IF NOT EXISTS");
    database.command("sql", "CREATE EDGE TYPE E IF NOT EXISTS");

    final int vertexCount = 120;
    final int flushEvery = 50;

    final RemoteGraphBatch batch = database.batch().withFlushEvery(flushEvery).build();

    final String[] ids = new String[vertexCount];
    for (int i = 0; i < vertexCount; i++)
      ids[i] = batch.createVertex("V", "i", i);

    for (int i = 0; i < vertexCount - 1; i++)
      batch.createEdge("E", ids[i], ids[i + 1]);

    batch.close();

    // Result should aggregate across all flushes
    final RemoteBatchResult result = batch.getResult();
    assertThat(result.getVerticesCreated()).isEqualTo(vertexCount);
    assertThat(result.getEdgesCreated()).isEqualTo(vertexCount - 1);

    // Verify actual DB state matches
    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM V")) {
      assertThat(((Number) rs.nextIfAvailable().getProperty("cnt")).longValue()).isEqualTo(vertexCount);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM E")) {
      assertThat(((Number) rs.nextIfAvailable().getProperty("cnt")).longValue()).isEqualTo(vertexCount - 1);
    }

    database.close();
  }
}
