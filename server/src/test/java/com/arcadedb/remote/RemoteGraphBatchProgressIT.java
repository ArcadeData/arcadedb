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

import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7311: the Java driver can be acknowledged per chunk instead of only at the end of a flush. Without a
 * listener the flush sends the request it always sent, so the server-side encoding is opt-in from here too.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class RemoteGraphBatchProgressIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-batch-progress";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = remoteServer();
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = remoteServer();
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }

  @Test
  @Timeout(120)
  void aProgressListenerIsCalledOncePerServerSideChunk() {
    final RemoteDatabase database = remoteDatabase();
    database.command("sql", "CREATE VERTEX TYPE Person");

    final List<JSONObject> progress = new ArrayList<>();
    try (final RemoteGraphBatch batch = database.batch()
        .withVertexBatchSize(2)
        .withProgressListener(progress::add)
        .build()) {
      for (int i = 0; i < 8; i++)
        batch.createVertex("Person", "name", "p" + i);
    }

    assertThat(progress)
        .as("a flush of eight vertices committed two at a time must report more than once")
        .hasSizeGreaterThan(1);
    assertThat(progress.getFirst().getString("phase")).isEqualTo("vertices");
    assertThat(progress.getFirst().getLong("verticesCreated")).isEqualTo(2);
    assertThat(progress.getLast().getLong("verticesCreated"))
        .as("the acknowledgements are cumulative, so the last one is not smaller than the first")
        .isGreaterThanOrEqualTo(progress.getFirst().getLong("verticesCreated"));

    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM Person")) {
      final Result r = rs.nextIfAvailable();
      assertThat(((Number) r.getProperty("cnt")).longValue()).isEqualTo(8);
    }
  }

  /**
   * The load still has to work the same way, and a flush still has to hand back the summary the caller reads its
   * totals from - the listener is an addition to the answer, not a replacement for it.
   */
  @Test
  @Timeout(120)
  void theTotalsAreTheSameWithAndWithoutAListener() {
    final RemoteDatabase database = remoteDatabase();
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE KNOWS");

    final RemoteGraphBatch batch = database.batch().withVertexBatchSize(2)
        .withProgressListener(event -> {
        }).build();
    final String a = batch.createVertex("Person", "name", "Alice");
    final String b = batch.createVertex("Person", "name", "Bob");
    batch.createEdge("KNOWS", a, b, "since", 2020);
    batch.close();

    assertThat(batch.getResult().getVerticesCreated()).isEqualTo(2);
    assertThat(batch.getResult().getEdgesCreated()).isEqualTo(1);
  }

  /**
   * A failed load still fails, and the message still carries how much was attempted: on this encoding the
   * failure is a line under a 200, so a driver that only looked at the status code would report success.
   */
  @Test
  @Timeout(120)
  void aFailedLoadStillFailsWhenItArrivesAsALineRatherThanAStatus() {
    final RemoteDatabase database = remoteDatabase();
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE KNOWS");

    final RemoteGraphBatch batch = database.batch().withVertexBatchSize(1)
        .withProgressListener(event -> {
        }).build();
    batch.createVertex("Person", "name", "Alice");
    // An endpoint no vertex of this payload declares: the server rejects the edge mid-load, with a 200 already
    // on the wire.
    batch.createEdge("KNOWS", "v0", "v999999");

    assertThatThrownBy(batch::flush)
        .isInstanceOf(DatabaseOperationException.class)
        .hasMessageContaining("status 400")
        .hasMessageContaining("attempted before it failed");
  }

  /**
   * A listener must not change what the flush does with the server's answer. The temporary-id mapping a batch
   * needs to resolve an edge against a vertex sent in an EARLIER request travels in the terminal line on this
   * encoding, and a driver that read the progress lines and stopped there would drop every cross-flush edge -
   * silently, since nothing else in the load would fail.
   */
  @Test
  @Timeout(120)
  void anEdgeAcrossTwoFlushesStillResolvesWithAListener() {
    final RemoteDatabase database = remoteDatabase();
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE KNOWS");

    final List<JSONObject> progress = new ArrayList<>();
    final List<String> ids = new ArrayList<>();
    try (final RemoteGraphBatch batch = database.batch()
        .withFlushEvery(4)
        .withVertexBatchSize(2)
        .withProgressListener(progress::add)
        .build()) {
      for (int i = 0; i < 10; i++)
        ids.add(batch.createVertex("Person", "name", "p" + i));
      // The first of these was created in a request that has already been answered, so it can only be resolved
      // through the mapping that request returned.
      batch.createEdge("KNOWS", ids.getFirst(), ids.getLast());
    }

    assertThat(progress).isNotEmpty();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM KNOWS")) {
      assertThat(((Number) rs.nextIfAvailable().getProperty("cnt")).longValue())
          .as("the edge spanning two flushes must exist")
          .isEqualTo(1);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM Person")) {
      assertThat(((Number) rs.nextIfAvailable().getProperty("cnt")).longValue()).isEqualTo(10);
    }
  }

  /**
   * The progress listener is an overload, and an overload is a second path: a flush that asks for no progress
   * has to keep going through {@code sendBatch(content, queryParams)}, which is the method a subclass replaces
   * to intercept every request this client makes. Routing it through the new three-argument sibling instead
   * walks past those overrides in silence - the compiler is perfectly happy to call the method nobody
   * overrode - and the only symptom is a stub that never fires, which reads as a test that stopped reproducing
   * its own bug rather than as a broken client.
   */
  @Test
  @Timeout(120)
  void aFlushWithNoListenerStillGoesThroughTheOverridableSend() {
    final List<String> intercepted = new ArrayList<>();
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", httpPort(), DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS) {
      @Override
      JSONObject sendBatch(final String content, final Map<String, String> queryParams) {
        intercepted.add(content);
        return super.sendBatch(content, queryParams);
      }
    }) {
      database.command("sql", "CREATE VERTEX TYPE Person");

      try (final RemoteGraphBatch batch = database.batch().build()) {
        batch.createVertex("Person", "name", "Alice");
      }

      assertThat(intercepted)
          .as("a flush without a progress listener must still reach the method subclasses override")
          .hasSize(1);
      assertThat(intercepted.getFirst()).contains("Alice");
    }
  }

  private RemoteServer remoteServer() {
    return new RemoteServer("127.0.0.1", httpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  private RemoteDatabase remoteDatabase() {
    return new RemoteDatabase("127.0.0.1", httpPort(), DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  private int httpPort() {
    return getServer(0).getHttpServer().getPort();
  }
}
