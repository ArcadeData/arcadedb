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
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityDatabaseUser;
import com.arcadedb.utility.FileUtils;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The client-streaming loads ({@code GraphBatchLoad}, {@code TimeSeriesWriteStream}) authenticate the caller on the first chunk,
 * but gRPC hands every later chunk, the completion and the error callback of one call to whichever thread of its executor is
 * free. Each of those callbacks must therefore apply the per-type grants of the user the call authenticated with, whatever
 * the thread it lands on was doing before: a fresh thread, or one that still holds the user of an unrelated request.
 * <p>
 * The service is driven in process and every callback runs on a thread of its own, so the thread switch that the shared
 * executor makes by chance happens on every call.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GrpcStreamingLoadTypeGrantsTest extends BaseGraphServerTest {
  private static final String NAME        = "GrpcStreamingLoadGrants";
  private static final String GROUP       = "streamingLoadGrants";
  private static final String LOAD_USER   = "streamingloaduser";
  private static final String LOAD_PWD    = "streamingloaduser-pwd";
  private static final String ROOT        = "root";

  private ArcadeDbGrpcService service;

  @BeforeEach
  void createDatabase() {
    service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));

    final ServerDatabase database = getServer(0).createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
    database.getSchema().createVertexType("Open");
    database.getSchema().createVertexType("Closed");
    database.getSchema().createEdgeType("OpenLink");
    database.getSchema().createEdgeType("ClosedLink");
    database.command("sql", "CREATE TIMESERIES TYPE OpenMetric TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.command("sql", "CREATE TIMESERIES TYPE ClosedMetric TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");

    final ServerSecurity security = getServer(0).getSecurity();
    security.saveGroup(NAME, GROUP, new JSONObject().put("access", new JSONArray()).put("types", new JSONObject()
        .put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))
        .put("Open", new JSONObject().put("access", new JSONArray().put("createRecord").put("readRecord").put("updateRecord")))
        .put("OpenLink", new JSONObject().put("access", new JSONArray().put("createRecord").put("readRecord").put("updateRecord")))
        .put("OpenMetric", new JSONObject().put("access", new JSONArray().put("createRecord").put("readRecord")))));
    createLoadUser();
  }

  @AfterEach
  void dropDatabase() {
    if (service != null)
      service.close();
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(LOAD_USER))
      security.dropUser(LOAD_USER);
    security.deleteGroup(NAME, GROUP);
    if (getServer(0).existsDatabase(NAME))
      getServer(0).dropLocalDatabase(getServer(0).getDatabaseForOperator(NAME).getEmbedded(), NAME);
    FileUtils.deleteRecursively(
        new File(getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
  }

  @Test
  void graphLoadChunkOnAnotherThreadKeepsTheGrantsOfTheStreamUser() throws Exception {
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final StreamObserver<GraphBatchChunk> stream = service.graphBatchLoad(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk(1).addRecords(vertex("Open", "a")).build()));
    assertThat(outcome.error).isNull();
    // flushed by the chunk itself: the vertex buffer holds one vertex
    runOnNewThread(() -> stream.onNext(chunk().addRecords(vertex("Closed", "b")).build()));

    assertRefused(outcome);
    assertThat(countOf("Closed")).isZero();
  }

  @Test
  void graphLoadChunkOnAThreadHoldingAnotherUserKeepsTheGrantsOfTheStreamUser() throws Exception {
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final StreamObserver<GraphBatchChunk> stream = service.graphBatchLoad(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk(1).addRecords(vertex("Open", "a")).build()));
    assertThat(outcome.error).isNull();
    runOnThreadHoldingRoot(() -> stream.onNext(chunk().addRecords(vertex("Closed", "b")).build()));

    assertRefused(outcome);
    assertThat(countOf("Closed")).isZero();
  }

  @Test
  void graphLoadCompletionOnAnotherThreadKeepsTheGrantsOfTheStreamUser() throws Exception {
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final StreamObserver<GraphBatchChunk> stream = service.graphBatchLoad(outcome);

    // the vertices wait in the buffer of the stream until the completion flushes them
    runOnNewThread(() -> stream.onNext(firstChunk(0).addRecords(vertex("Open", "a")).addRecords(vertex("Closed", "b")).build()));
    assertThat(outcome.error).isNull();
    runOnNewThread(stream::onCompleted);

    assertRefused(outcome);
    assertThat(countOf("Closed")).isZero();
  }

  @Test
  void graphLoadCompletionOnAThreadHoldingAnotherUserKeepsTheGrantsOfTheStreamUser() throws Exception {
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final StreamObserver<GraphBatchChunk> stream = service.graphBatchLoad(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk(0).addRecords(vertex("Closed", "b")).build()));
    assertThat(outcome.error).isNull();
    runOnThreadHoldingRoot(stream::onCompleted);

    assertRefused(outcome);
    assertThat(countOf("Closed")).isZero();
  }

  @Test
  void graphLoadEdgesOfLaterChunksKeepTheGrantsOfTheStreamUser() throws Exception {
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final StreamObserver<GraphBatchChunk> stream = service.graphBatchLoad(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk(0).addRecords(vertex("Open", "a")).addRecords(vertex("Open", "b")).build()));
    assertThat(outcome.error).isNull();
    runOnNewThread(() -> stream.onNext(chunk().addRecords(edge("ClosedLink", "a", "b")).build()));
    if (outcome.error == null)
      runOnNewThread(stream::onCompleted);

    assertRefused(outcome);
    assertThat(countOf("ClosedLink")).isZero();
  }

  @Test
  void graphLoadOfWhatTheStreamUserMayWriteCompletesOnAnyThread() throws Exception {
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final StreamObserver<GraphBatchChunk> stream = service.graphBatchLoad(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk(1).addRecords(vertex("Open", "a")).build()));
    runOnNewThread(() -> stream.onNext(chunk().addRecords(vertex("Open", "b")).build()));
    runOnThreadHoldingRoot(() -> stream.onNext(chunk().addRecords(edge("OpenLink", "a", "b")).build()));
    runOnNewThread(stream::onCompleted);

    assertThat(outcome.error).isNull();
    assertThat(outcome.completed).isTrue();
    assertThat(outcome.last.getVerticesCreated()).isEqualTo(2);
    assertThat(outcome.last.getEdgesCreated()).isEqualTo(1);
    assertThat(countOf("Open")).isEqualTo(2);
    assertThat(countOf("OpenLink")).isEqualTo(1);
  }

  @Test
  void graphLoadEndsWhenTheStreamUserNoLongerExists() throws Exception {
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final StreamObserver<GraphBatchChunk> stream = service.graphBatchLoad(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk(1).addRecords(vertex("Open", "a")).build()));
    assertThat(outcome.error).isNull();
    getServer(0).getSecurity().dropUser(LOAD_USER);
    runOnNewThread(() -> stream.onNext(chunk().addRecords(vertex("Open", "b")).build()));

    assertThat(outcome.error).isNotNull();
    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(countOf("Open")).isEqualTo(1);

    // the slot of the failed load was given back: the database takes the next load at once
    loadOpenVertexAsRoot("c");
    assertThat(countOf("Open")).isEqualTo(2);
  }

  @Test
  void graphLoadEndedByTheClientWritesItsBufferedEdgesAsTheStreamUser() throws Exception {
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final StreamObserver<GraphBatchChunk> stream = service.graphBatchLoad(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk(0).addRecords(vertex("Open", "a")).addRecords(vertex("Open", "b")).build()));
    runOnNewThread(() -> stream.onNext(chunk().addRecords(edge("OpenLink", "a", "b")).build()));
    assertThat(outcome.error).isNull();
    // the end of the call lands on a thread that holds the operator: what the batch still buffers is written as the stream user
    runOnThreadHoldingRoot(() -> stream.onError(Status.CANCELLED.asRuntimeException()));

    assertThat(countOf("Open")).isEqualTo(2);
    assertThat(countOf("OpenLink")).isEqualTo(1);
    loadOpenVertexAsRoot("c");
  }

  @Test
  void graphLoadEndedByTheClientDoesNotWriteWhatTheStreamUserMayNoLongerWrite() throws Exception {
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final StreamObserver<GraphBatchChunk> stream = service.graphBatchLoad(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk(0).addRecords(vertex("Open", "a")).addRecords(vertex("Open", "b")).build()));
    runOnNewThread(() -> stream.onNext(chunk().addRecords(edge("OpenLink", "a", "b")).build()));
    assertThat(outcome.error).as("the edge waits in the buffer of the batch until the stream ends").isNull();
    // the group of the stream user loses the grant to create the edges it buffered
    getServer(0).getSecurity().saveGroup(NAME, GROUP, new JSONObject().put("access", new JSONArray()).put("types", new JSONObject()
        .put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))
        .put("Open", new JSONObject().put("access", new JSONArray().put("createRecord").put("readRecord").put("updateRecord")))));
    getServer(0).getSecurity().updateSchema(getServer(0).getDatabaseForOperator(NAME).getEmbedded());
    // the operator that holds the thread could write the edge: the stream user cannot, and the end of the call is not told
    runOnThreadHoldingRoot(() -> stream.onError(Status.CANCELLED.asRuntimeException()));

    assertThat(countOf("OpenLink")).isZero();
    // the batch gave its slot back although it could not write what it buffered
    loadOpenVertexAsRoot("c");
    assertThat(countOf("Open")).isEqualTo(3);
  }

  @Test
  void graphLoadEndedByTheClientAfterTheStreamUserWasDroppedWritesNothing() throws Exception {
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final StreamObserver<GraphBatchChunk> stream = service.graphBatchLoad(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk(0).addRecords(vertex("Open", "a")).addRecords(vertex("Open", "b")).build()));
    runOnNewThread(() -> stream.onNext(chunk().addRecords(edge("OpenLink", "a", "b")).build()));
    assertThat(outcome.error).isNull();
    getServer(0).getSecurity().dropUser(LOAD_USER);
    runOnThreadHoldingRoot(() -> stream.onError(Status.CANCELLED.asRuntimeException()));

    // the buffered edge was dropped with the batch: nobody is left to write it as
    assertThat(countOf("OpenLink")).isZero();
    loadOpenVertexAsRoot("c");
    assertThat(countOf("Open")).isEqualTo(3);
  }

  @Test
  void graphLoadEndedByTheClientBeforeItsFirstChunkLeavesTheDatabaseFree() throws Exception {
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final StreamObserver<GraphBatchChunk> stream = service.graphBatchLoad(outcome);

    runOnNewThread(() -> stream.onError(Status.CANCELLED.asRuntimeException()));

    assertThat(outcome.error).isNull();
    loadOpenVertexAsRoot("c");
    assertThat(countOf("Open")).isEqualTo(1);
  }

  @Test
  void timeSeriesChunkOnAnotherThreadKeepsTheGrantsOfTheStreamUser() throws Exception {
    final Outcome<TimeSeriesWriteSummary> outcome = new Outcome<>();
    final StreamObserver<TimeSeriesWriteChunk> stream = service.timeSeriesWriteStream(outcome);

    runOnNewThread(() -> stream.onNext(firstSeriesChunk("OpenMetric")));
    assertThat(outcome.error).isNull();
    runOnNewThread(() -> stream.onNext(seriesChunk("ClosedMetric")));

    assertRefused(outcome);
    assertThat(countOf("ClosedMetric")).isZero();
  }

  @Test
  void timeSeriesChunkOnAThreadHoldingAnotherUserKeepsTheGrantsOfTheStreamUser() throws Exception {
    final Outcome<TimeSeriesWriteSummary> outcome = new Outcome<>();
    final StreamObserver<TimeSeriesWriteChunk> stream = service.timeSeriesWriteStream(outcome);

    runOnNewThread(() -> stream.onNext(firstSeriesChunk("OpenMetric")));
    assertThat(outcome.error).isNull();
    runOnThreadHoldingRoot(() -> stream.onNext(seriesChunk("ClosedMetric")));

    assertRefused(outcome);
    assertThat(countOf("ClosedMetric")).isZero();
  }

  @Test
  void timeSeriesStreamOfWhatTheStreamUserMayWriteCompletesOnAnyThread() throws Exception {
    final Outcome<TimeSeriesWriteSummary> outcome = new Outcome<>();
    final StreamObserver<TimeSeriesWriteChunk> stream = service.timeSeriesWriteStream(outcome);

    runOnNewThread(() -> stream.onNext(firstSeriesChunk("OpenMetric")));
    runOnThreadHoldingRoot(() -> stream.onNext(seriesChunk("OpenMetric")));
    runOnNewThread(stream::onCompleted);

    assertThat(outcome.error).isNull();
    assertThat(outcome.completed).isTrue();
    assertThat(outcome.last.getWritten()).isEqualTo(2);
    assertThat(countOf("OpenMetric")).isEqualTo(2);
  }

  @Test
  void timeSeriesStreamEndsWhenTheStreamUserNoLongerExists() throws Exception {
    final Outcome<TimeSeriesWriteSummary> outcome = new Outcome<>();
    final StreamObserver<TimeSeriesWriteChunk> stream = service.timeSeriesWriteStream(outcome);

    runOnNewThread(() -> stream.onNext(firstSeriesChunk("OpenMetric")));
    assertThat(outcome.error).isNull();
    getServer(0).getSecurity().dropUser(LOAD_USER);
    runOnNewThread(() -> stream.onNext(seriesChunk("OpenMetric")));

    assertThat(outcome.error).isNotNull();
    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(countOf("OpenMetric")).isEqualTo(1);
  }

  // ---------------------------------------------------------------------------------------------------------------

  private void createLoadUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(LOAD_USER))
      security.dropUser(LOAD_USER);
    security.createUser(new JSONObject().put("name", LOAD_USER).put("password", security.encodePassword(LOAD_PWD))
        .put("databases", new JSONObject().put(NAME, new JSONArray().put(GROUP))));
  }

  /** A whole load of one vertex by the operator: it only starts when the database has no other batch open. */
  private void loadOpenVertexAsRoot(final String tempId) throws InterruptedException {
    final Outcome<GraphBatchResult> next = new Outcome<>();
    final StreamObserver<GraphBatchChunk> again = service.graphBatchLoad(next);
    runOnNewThread(() -> again.onNext(
        GraphBatchChunk.newBuilder().setDatabase(NAME).setCredentials(credentials(ROOT, DEFAULT_PASSWORD_FOR_TESTS))
            .addRecords(vertex("Open", tempId)).build()));
    runOnNewThread(again::onCompleted);
    assertThat(next.error).isNull();
    assertThat(next.completed).isTrue();
  }

  private static void assertRefused(final Outcome<?> outcome) {
    assertThat(outcome.error).as("the stream must end with a refusal").isNotNull();
    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  private long countOf(final String type) {
    return getServer(0).getDatabaseForOperator(NAME).query("sql", "SELECT count(*) AS c FROM " + type).next().<Long>getProperty("c");
  }

  private static DatabaseCredentials credentials(final String user, final String password) {
    return DatabaseCredentials.newBuilder().setUsername(user).setPassword(password).build();
  }

  /** The first chunk of a graph load, sent by the user the test grants; a buffer of {@code bufferSize} vertices when above zero. */
  private static GraphBatchChunk.Builder firstChunk(final int bufferSize) {
    final GraphBatchChunk.Builder chunk = GraphBatchChunk.newBuilder().setDatabase(NAME).setCredentials(credentials(LOAD_USER, LOAD_PWD));
    if (bufferSize > 0)
      chunk.setOptions(GraphBatchOptions.newBuilder().setVertexBatchSize(bufferSize));
    return chunk;
  }

  /** A later chunk: no database, no credentials. */
  private static GraphBatchChunk.Builder chunk() {
    return GraphBatchChunk.newBuilder();
  }

  private static GraphBatchRecord vertex(final String type, final String tempId) {
    return GraphBatchRecord.newBuilder().setKind(GraphBatchRecord.Kind.VERTEX).setTypeName(type).setTempId(tempId)
        .putProperties("name", GrpcValue.newBuilder().setStringValue(tempId).build()).build();
  }

  private static GraphBatchRecord edge(final String type, final String from, final String to) {
    return GraphBatchRecord.newBuilder().setKind(GraphBatchRecord.Kind.EDGE).setTypeName(type).setFromRef(from).setToRef(to).build();
  }

  private static TimeSeriesWriteChunk firstSeriesChunk(final String type) {
    return seriesChunk(type).toBuilder().setDatabase(NAME).setCredentials(credentials(LOAD_USER, LOAD_PWD)).build();
  }

  private static TimeSeriesWriteChunk seriesChunk(final String type) {
    return TimeSeriesWriteChunk.newBuilder().setType(type).addPoints(TimeSeriesPoint.newBuilder()
        .setTimestamp(System.nanoTime())
        .putTags("host", GrpcValue.newBuilder().setStringValue("h1").build())
        .putFields("v", GrpcValue.newBuilder().setDoubleValue(1.5).build())).build();
  }

  /** Runs the callback on a thread that never served a request: the case of a call whose next message lands on a new thread. */
  private static void runOnNewThread(final Runnable callback) throws InterruptedException {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try {
        callback.run();
      } catch (final Throwable e) {
        failure.set(e);
      }
    }, "streaming-load-callback");
    thread.start();
    thread.join(60_000);
    assertThat(thread.isAlive()).as("the callback must return").isFalse();
    assertThat(failure.get()).as("a callback reports its failures through the observer").isNull();
  }

  /** Runs the callback on a thread left holding the operator by an unrelated request on the same database. */
  private void runOnThreadHoldingRoot(final Runnable callback) throws InterruptedException {
    runOnNewThread(() -> {
      final DatabaseInternal database = (DatabaseInternal) getServer(0).getDatabaseForClient(NAME);
      final ServerSecurityDatabaseUser root = getServer(0).getSecurity().getUser(ROOT).getDatabaseUser(database);
      DatabaseContext.INSTANCE.init(database).setCurrentUser(root);
      callback.run();
      // the callback leaves the thread as it found it: nothing of the stream user stays behind for the next request
      assertThat(DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath()).getCurrentUser()).isSameAs(root);
    });
  }

  /** What the client of a stream sees: the last message, the end of the call and its failure. */
  private static final class Outcome<T> extends ServerCallStreamObserver<T> {
    volatile T         last;
    volatile Throwable error;
    volatile boolean   completed;

    @Override
    public void onNext(final T value) {
      last = value;
    }

    @Override
    public void onError(final Throwable t) {
      error = t;
    }

    @Override
    public void onCompleted() {
      completed = true;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public void setOnCancelHandler(final Runnable onCancelHandler) {
    }

    @Override
    public void setCompression(final String compression) {
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setOnReadyHandler(final Runnable onReadyHandler) {
    }

    @Override
    public void request(final int count) {
    }

    @Override
    public void setMessageCompression(final boolean enable) {
    }

    @Override
    public void disableAutoInboundFlowControl() {
    }
  }
}
