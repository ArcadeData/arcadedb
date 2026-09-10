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
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7311: the Java driver can be acknowledged per chunk instead of only at the end of a flush.
 * <p>
 * Issue #7353 made the streaming encoding unconditional here: the temporary-id mapping this client needs to
 * resolve cross-flush edges also arrives on those lines, one committed chunk at a time, so a flush negotiates
 * the stream whether or not the caller asked to see the progress. The listener is now only about who gets told.
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
   * needs to resolve an edge against a vertex sent in an EARLIER request arrives on the progress lines since
   * issue #7353, and a driver that handed those lines to the caller without folding the mapping into its own
   * resolver would drop every cross-flush edge - silently, since nothing else in the load would fail.
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
   * Issue #7353: the mapping this client resolves its cross-flush edges against arrives on the progress lines,
   * not in the terminal object. That is the whole point - with the buffered encoding the server has to build
   * the temporary-id map of an entire flush as one JSON object before it can answer, and this client has to
   * read that object back in one piece: 50,000 entries per flush by default, and every vertex of the load when
   * flushEvery is 0.
   * <p>
   * A load of more than one flush, with an edge crossing them, is what proves the mapping was not merely
   * REPORTED on those lines but actually applied: the edge cannot resolve from anywhere else.
   */
  @Test
  @Timeout(120)
  void theCrossFlushMappingArrivesOnTheProgressLines() {
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
      batch.createEdge("KNOWS", ids.getFirst(), ids.getLast());
    }

    assertThat(progress.stream().filter(p -> p.has("idMapping")).count())
        .as("the mapping must reach the client on the acknowledgements, in more than one piece - one piece "
            + "would be the buffered object with a newline after it")
        .isGreaterThan(1);
    assertThat(progress.stream().filter(p -> p.has("idMapping"))
        .mapToInt(p -> p.getJSONObject("idMapping").length()).max().orElse(0))
        .as("no acknowledgement may carry more than the vertex flush that produced it")
        .isLessThanOrEqualTo(2);

    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM KNOWS")) {
      assertThat(((Number) rs.nextIfAvailable().getProperty("cnt")).longValue())
          .as("the edge spanning two flushes can only resolve through a mapping that was actually applied")
          .isEqualTo(1);
    }
  }

  /**
   * The same, with no listener at all. The encoding is no longer the caller's choice (issue #7353): the mapping
   * has to stream whether or not anybody asked to watch it, or the default configuration of this client would
   * keep the buffering the streaming encoding exists to remove.
   */
  @Test
  @Timeout(120)
  void aBatchWithNoListenerStreamsTheMappingAndStillResolvesCrossFlushEdges() {
    final RemoteDatabase database = remoteDatabase();
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE KNOWS");

    final List<String> ids = new ArrayList<>();
    try (final RemoteGraphBatch batch = database.batch()
        .withFlushEvery(4)
        .withVertexBatchSize(2)
        .build()) {
      for (int i = 0; i < 10; i++)
        ids.add(batch.createVertex("Person", "name", "p" + i));
      batch.createEdge("KNOWS", ids.getFirst(), ids.getLast());
    }

    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM KNOWS")) {
      assertThat(((Number) rs.nextIfAvailable().getProperty("cnt")).longValue()).isEqualTo(1);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM Person")) {
      assertThat(((Number) rs.nextIfAvailable().getProperty("cnt")).longValue()).isEqualTo(10);
    }
  }

  /**
   * There must be exactly ONE method on {@code RemoteDatabase} that puts a batch on the wire, because a subclass
   * replaces it to intercept every request this client makes. It used to be two overloads, and an overload is a
   * second path: whichever one a stub did not override walked past it in silence - the compiler is perfectly
   * happy to call the method nobody overrode - and the only symptom was a stub that never fired, which reads as
   * a test that stopped reproducing its own bug rather than as a broken client. Since {@code RemoteGraphBatch}
   * always negotiates the streaming encoding (issue #7353), the split would have hidden every flush it makes,
   * so the two were folded into {@code sendBatch(content, queryParams, onProgress)}.
   * <p>
   * Asserted with NO listener on purpose: that is the case the old split routed down the other method.
   */
  @Test
  @Timeout(120)
  void aFlushWithNoListenerStillGoesThroughTheOverridableSend() {
    final List<String> intercepted = new ArrayList<>();
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", httpPort(), DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS) {
      @Override
      JSONObject sendBatch(final String content, final Map<String, String> queryParams,
          final Consumer<JSONObject> onProgress) {
        intercepted.add(content);
        return super.sendBatch(content, queryParams, onProgress);
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
