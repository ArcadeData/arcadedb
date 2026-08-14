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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.remote.RemoteGraphBatch;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6070: {@code batch()} on a gRPC connection sent the load as JSONL over plain HTTP
 * to {@code POST /api/v1/batch} instead of over the {@code GraphBatchLoad} streaming RPC that already existed
 * and was implemented server-side. A caller who chose a gRPC connection got the HTTP transport silently, and
 * nothing on the API surface said so.
 *
 * <p>The transport is asserted through the type {@code batch()} returns, which is the only thing that decides
 * it: {@link RemoteGrpcGraphBatch} speaks the streaming RPC and nothing else, the inherited
 * {@link RemoteGraphBatch} speaks HTTP and nothing else. The rest of the tests pin the behaviour the new
 * transport has to keep identical to the HTTP one, and the one thing it does differently: temporary ids that
 * survive a chunk boundary are resolved by the server, which holds them for the whole stream, rather than by
 * the client round-tripping an id mapping per flush.
 */
public class Issue6070GrpcGraphBatchIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT = 50051;
  private static final int    HTTP_PORT = 2480;
  private static final String PERSON    = "Issue6070Person";
  private static final String KNOWS     = "Issue6070Knows";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void openAndPrepare() {
    grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, HTTP_PORT, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);

    grpc.command("sql", "CREATE VERTEX TYPE `" + PERSON + "` IF NOT EXISTS");
    grpc.command("sql", "CREATE EDGE TYPE `" + KNOWS + "` IF NOT EXISTS");
    grpc.command("sql", "DELETE FROM `" + KNOWS + "`");
    grpc.command("sql", "DELETE FROM `" + PERSON + "`");
  }

  @AfterEach
  void closeClient() {
    if (grpc != null) {
      try {
        grpc.command("sql", "DROP TYPE `" + KNOWS + "` IF EXISTS UNSAFE");
        grpc.command("sql", "DROP TYPE `" + PERSON + "` IF EXISTS UNSAFE");
      } catch (final Throwable ignore) {
        // the server may already be going down; the base class drops the database anyway
      }
      grpc.close();
    }
    if (grpcServer != null)
      grpcServer.close();
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void batchOnAGrpcConnectionUsesTheStreamingRpcNotHttp() {
    // The bug: this returned the inherited RemoteGraphBatch.Builder, whose loader posts JSONL over HTTP.
    final RemoteGraphBatch.Builder builder = grpc.batch();
    assertThat(builder).as("batch() on a gRPC connection must build the gRPC loader")
        .isInstanceOf(RemoteGrpcGraphBatch.Builder.class);

    try (final RemoteGraphBatch batch = builder.build()) {
      assertThat(batch).as("the loader built on a gRPC connection must speak the GraphBatchLoad RPC")
          .isInstanceOf(RemoteGrpcGraphBatch.class);
    }
  }

  @Test
  void loadsVerticesAndEdgesOverTheStreamingRpc() {
    try (final RemoteGraphBatch batch = grpc.batch().build()) {
      final String alice = batch.createVertex(PERSON, "name", "Alice", "age", 30);
      final String bob = batch.createVertex(PERSON, "name", "Bob", "age", 25);
      batch.createEdge(KNOWS, alice, bob, "since", 2020);

      assertThatThrownBy(batch::getResult).as("counters are only known once the stream has ended")
          .isInstanceOf(IllegalStateException.class);
    }

    assertThat(countOf(PERSON)).isEqualTo(2);
    assertThat(countOf(KNOWS)).isEqualTo(1);

    assertThat(grpc.query("sql", "SELECT name, age FROM `" + PERSON + "` ORDER BY name").stream()
        .map(r -> r.getProperty("name") + ":" + r.<Object>getProperty("age"))
        .toList()).containsExactly("Alice:30", "Bob:25");

    // The edge connects the two vertices the temporary ids named...
    assertThat(grpc.query("sql", "SELECT name, out(\"" + KNOWS + "\").name AS knows FROM `" + PERSON + "` ORDER BY name")
        .stream().map(r -> r.getProperty("name") + "->" + r.<List<Object>>getProperty("knows")).toList())
        .containsExactly("Alice->[Bob]", "Bob->[]");

    // ...and carries its own properties, which land on the edge rather than on either endpoint.
    assertThat(grpc.query("sql", "SELECT since FROM `" + KNOWS + "`").next().<Object>getProperty("since"))
        .isEqualTo(2020);
  }

  @Test
  void reportsTheServersTotalsAfterClose() {
    final RemoteGraphBatch batch = grpc.batch().build();
    final String a = batch.createVertex(PERSON, "name", "A");
    final String b = batch.createVertex(PERSON, "name", "B");
    final String c = batch.createVertex(PERSON, "name", "C");
    batch.createEdge(KNOWS, a, b);
    batch.createEdge(KNOWS, b, c);
    batch.close();

    assertThat(batch.getResult().getVerticesCreated()).isEqualTo(3);
    assertThat(batch.getResult().getEdgesCreated()).isEqualTo(2);
    assertThat(batch.getResult().getElapsedMs()).isGreaterThanOrEqualTo(0);
  }

  /**
   * The one behaviour the streaming transport implements differently. Over HTTP each flush is an independent
   * request, so an edge sent after the vertex it points at has already been flushed can only be connected
   * because the client asked for the id mapping back and rewrote the reference itself. Here the whole load is
   * one call and the server keeps the mapping for its lifetime, so the temporary id crosses the chunk boundary
   * untouched. A chunk size of 2 puts every vertex in a chunk of its own, well before the edges arrive.
   */
  @Test
  void resolvesTemporaryIdsAcrossChunkBoundaries() {
    try (final RemoteGraphBatch batch = grpc.batch().withFlushEvery(2).build()) {
      final String a = batch.createVertex(PERSON, "name", "A");
      final String b = batch.createVertex(PERSON, "name", "B");
      final String c = batch.createVertex(PERSON, "name", "C");
      final String d = batch.createVertex(PERSON, "name", "D");

      // Every one of these references a vertex sent in an earlier chunk.
      batch.createEdge(KNOWS, a, b);
      batch.createEdge(KNOWS, b, c);
      batch.createEdge(KNOWS, c, d);
      batch.createEdge(KNOWS, d, a);
    }

    assertThat(countOf(PERSON)).isEqualTo(4);
    assertThat(countOf(KNOWS)).as("every cross-chunk reference must have been connected").isEqualTo(4);

    // A dropped reference would leave a vertex without the edge rather than fail, so the shape is what proves it.
    assertThat(grpc.query("sql", "SELECT name, out(\"" + KNOWS + "\").name AS knows FROM `" + PERSON + "` ORDER BY name")
        .stream().map(r -> r.getProperty("name") + "->" + r.<List<?>>getProperty("knows")).toList())
        .containsExactly("A->[B]", "B->[C]", "C->[D]", "D->[A]");
  }

  @Test
  void connectsAnEdgeToAVertexAlreadyInTheDatabaseByRid() {
    grpc.command("sql", "INSERT INTO `" + PERSON + "` SET name = 'Existing'");
    final String existingRid = grpc.query("sql", "SELECT @rid AS rid FROM `" + PERSON + "` WHERE name = 'Existing'")
        .next().getProperty("rid").toString();

    try (final RemoteGraphBatch batch = grpc.batch().build()) {
      final String fresh = batch.createVertex(PERSON, "name", "Fresh");
      batch.createEdge(KNOWS, fresh, existingRid);
    }

    assertThat(grpc.query("sql", "SELECT out(\"" + KNOWS + "\").name AS knows FROM `" + PERSON + "` WHERE name = 'Fresh'")
        .next().<List<Object>>getProperty("knows")).containsExactly("Existing");
  }

  @Test
  void carriesTheBuilderOptionsToTheServer() {
    // vertexBatchSize had no equivalent on the RPC before this fix, so a caller lowering it (which a replicated
    // database needs, one batch being one Raft entry) was silently ignored. Set below the record count so the
    // server has to honour it to load everything.
    try (final RemoteGraphBatch batch = grpc.batch().withVertexBatchSize(2).withFlushEvery(3).withWAL(true).build()) {
      for (int i = 0; i < 7; i++)
        batch.createVertex(PERSON, "name", "P" + i);
    }

    assertThat(countOf(PERSON)).isEqualTo(7);
  }

  /**
   * The gRPC-only options have to be reachable after the inherited ones. An inherited setter that handed back
   * the base builder would compile only while {@code withTimeout} came first, which is not an ordering anyone
   * would expect a builder to impose.
   */
  @Test
  void buildsWithTheGrpcOptionsInAnyPositionOfTheChain() {
    try (final RemoteGraphBatch batch = grpc.batch()
        .withFlushEvery(10)
        .withBatchSize(1000)
        .withTimeout(60_000)
        .withVertexBatchSize(5)
        .build()) {
      batch.createVertex(PERSON, "name", "Chained");
    }

    assertThat(countOf(PERSON)).isEqualTo(1);
  }

  /**
   * The commit-retry knobs reach the server. Zero is the interesting value: it means "do not retry, fail on the
   * first error", which is a setting and not an absence, so the two fields are {@code optional} on the wire -
   * a plain proto3 int could not tell a caller who asked for no retries from one who asked for nothing.
   */
  @Test
  void carriesTheCommitRetryOptionsIncludingZero() {
    try (final RemoteGraphBatch batch = grpc.batch().withCommitRetries(0).withCommitRetryDelay(0).build()) {
      batch.createVertex(PERSON, "name", "NoRetries");
    }

    assertThat(countOf(PERSON)).isEqualTo(1);

    assertThatThrownBy(() -> grpc.batch().withCommitRetries(-1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> grpc.batch().withCommitRetryDelay(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsAVertexAddedAfterAnEdge() {
    try (final RemoteGraphBatch batch = grpc.batch().build()) {
      final String a = batch.createVertex(PERSON, "name", "A");
      final String b = batch.createVertex(PERSON, "name", "B");
      batch.createEdge(KNOWS, a, b);

      assertThatThrownBy(() -> batch.createVertex(PERSON, "name", "TooLate"))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Cannot add vertices after edges");
    }
  }

  @Test
  void rejectsUseAfterClose() {
    final RemoteGraphBatch batch = grpc.batch().build();
    batch.createVertex(PERSON, "name", "A");
    batch.close();

    assertThatThrownBy(() -> batch.createVertex(PERSON, "name", "B")).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> batch.createEdge(KNOWS, "v0", "v0")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void closingAnEmptyBatchNeitherOpensAStreamNorFails() {
    final RemoteGraphBatch batch = grpc.batch().build();
    batch.close();
    batch.close(); // idempotent

    assertThat(batch.getResult().getVerticesCreated()).isZero();
    assertThat(batch.getResult().getEdgesCreated()).isZero();
    assertThat(countOf(PERSON)).isZero();
  }

  /**
   * The batch commits incrementally, so a load that fails is not a load that rolled back. What the server had
   * already committed rides the trailers of the failed call and has to end up somewhere the caller can read it:
   * re-sending the whole load blindly would double everything that did get through.
   */
  @Test
  void aFailedLoadReportsWhatItAlreadyCommitted() {
    grpc.command("sql", "CREATE PROPERTY `" + PERSON + "`.tag IF NOT EXISTS STRING (MANDATORY TRUE)");

    final RemoteGraphBatch batch = grpc.batch().withVertexBatchSize(2).build();
    for (int i = 0; i < 8; i++)
      batch.createVertex(PERSON, "tag", "t" + i);
    // The ninth has no 'tag', so the buffer holding it cannot be created and the load fails there.
    batch.createVertex(PERSON);

    assertThatThrownBy(batch::close).as("a load violating the schema must fail, not pass silently")
        .isInstanceOf(RuntimeException.class);

    assertThat(batch.getResult().getVerticesCreated())
        .as("the counters must say how much of the load is durable, not zero")
        .isEqualTo(8);
    assertThat(countOf(PERSON)).as("and what they claim is durable really is").isEqualTo(8);
  }

  /**
   * The failure above arrives on the single flush that {@code close()} performs. This one arrives on an
   * interior auto-flush, which is the shape a real bulk load has: the exception comes out of
   * {@code createVertex()} in the middle of the body, and the {@code close()} that a try-with-resources runs
   * afterwards must neither re-send the chunk that just failed nor bury the real failure under gRPC's own "call
   * already closed".
   * <p>
   * The loader is written not to depend on which of those happens - it hands the buffer over before the send,
   * and does not half-close a call the server has already failed - but this test does not distinguish the two
   * implementations on its own: current grpc-java reports {@code isReady() == false} on a terminated call, so
   * the readiness wait raises the real failure before a re-send could reach the wire either way. What it pins
   * is the contract a caller sees, which must hold however grpc-java resolves that internally: the committed
   * buffer survives exactly once, and {@code close()} adds no second, misleading failure.
   * <p>
   * <b>WHICH of the two raises it is a race, so this test does not bet on one</b> (issue #6168, item 3). Whether
   * the server's error reaches the client while the body is still sending or only once {@code close()} half-closes
   * depends on scheduling, and asserting the body specifically made this fail 4 of 6 CI runs on a PR whose diff
   * touched no gRPC code at all. What it asserts is what actually holds: the load raises EXACTLY ONE failure,
   * wherever it surfaces, and the committed buffer is durable exactly once.
   */
  @Test
  void aFailureOnAnInteriorFlushIsNotResentByClose() {
    grpc.command("sql", "CREATE PROPERTY `" + PERSON + "`.tag IF NOT EXISTS STRING (MANDATORY TRUE)");

    final AtomicReference<Throwable> fromBody = new AtomicReference<>();
    final AtomicReference<Throwable> fromClose = new AtomicReference<>();

    // Chunk and server-side buffer both 2, so the first two vertices are committed as a whole buffer before the
    // buffer holding the bad one fails. The 400 that follow make sure the failure is observed by a later send
    // in the body rather than only at close(), which is what puts an already-sent chunk at risk of a re-send.
    try (final RemoteGraphBatch batch = grpc.batch().withFlushEvery(2).withVertexBatchSize(2).build()) {
      try {
        batch.createVertex(PERSON, "tag", "ok0");
        batch.createVertex(PERSON, "tag", "ok1");
        batch.createVertex(PERSON, "tag", "ok2");
        batch.createVertex(PERSON); // no 'tag': the server fails the load on this buffer
        for (int i = 0; i < 400; i++)
          batch.createVertex(PERSON, "tag", "after" + i);
      } catch (final RuntimeException e) {
        fromBody.set(e);
      }
    } catch (final RuntimeException raisedByClose) {
      fromClose.set(raisedByClose);
      // close() may raise the failure the body never saw, or re-raise the same one; what it must not do is raise a
      // different, misleading one on top of it, which is what half-closing or re-sending on a call the server
      // already terminated produces.
      assertThat(raisedByClose).as("close() must not invent a failure of its own on top of the real one")
          .hasMessageNotContaining("call already closed")
          .hasMessageNotContaining("already half-closed")
          .hasMessageNotContaining("Stream is already completed");
    }

    assertThat(fromBody.get() != null ? fromBody.get() : fromClose.get())
        .as("the load violates the schema, so it must fail - in the body if the server's error got there in time, "
            + "otherwise at close()").isNotNull();

    // The buffer that completed before the failure is durable, and exactly once: a chunk handed to a terminated
    // stream and then sent again by close() would show up here as a duplicate.
    assertThat(countOf(PERSON)).as("the committed buffer survives, and is not loaded twice").isEqualTo(2);
  }

  private long countOf(final String typeName) {
    return grpc.query("sql", "SELECT count(*) AS c FROM `" + typeName + "`").next().<Number>getProperty("c").longValue();
  }
}
