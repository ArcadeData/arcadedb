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
import com.arcadedb.database.Database;
import com.arcadedb.server.BaseGraphServerTest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7407: the upsert and ignore lookups of {@code insertStream} and {@code insertBidirectional} are real
 * SQL, run from observer callbacks on a thread that is not the one the RPC arrived on. They were metered and
 * traced as {@code protocol="internal"}; they belong to {@code grpc}, as {@code bulkInsert}'s already were.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7407InsertStreamUpsertMetricsIT extends BaseGraphServerTest {
  private static final int ROWS = 20;

  private String              typeName;
  private ArcadeDbGrpcService service;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void createKeyedType() {
    typeName = "Issue7407Keyed_" + System.currentTimeMillis();
    final Database database = getServer(0).getDatabase(getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
    database.command("sql", "CREATE PROPERTY " + typeName + ".name STRING");
    database.command("sql", "CREATE INDEX ON " + typeName + " (name) UNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument(typeName).set("name", "k" + i).set("v", 0).save();
    });
    service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
  }

  @AfterEach
  void dropKeyedType() {
    service.close();
    getServer(0).getDatabase(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
  }

  @Test
  void insertStreamUpsertLookupsAreMeteredAsGrpc() throws Exception {
    final long grpcBefore = timerCount("grpc");
    final long internalBefore = timerCount("internal");

    final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(1);
    final StreamObserver<InsertChunk> req = service.insertStream(new RecordingSummaryObserver(summaryRef, done));

    final InsertChunk.Builder chunk = InsertChunk.newBuilder().setSessionId("issue-7407-stream").setChunkSeq(0)
        .setLast(true).setOptions(upsertOptions());
    for (int i = 0; i < ROWS; i++)
      chunk.addRows(row(i));
    req.onNext(chunk.build());
    req.onCompleted();

    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(summaryRef.get().getUpdated()).isEqualTo(ROWS);
    assertThat(summaryRef.get().getFailed()).isZero();

    assertThat(timerCount("grpc") - grpcBefore).as("one metered lookup per upserted row").isGreaterThanOrEqualTo(ROWS);
    assertThat(timerCount("internal") - internalBefore).as("none of them attributed to internal").isZero();
  }

  @Test
  void insertBidirectionalUpsertLookupsAreMeteredAsGrpc() throws Exception {
    final long grpcBefore = timerCount("grpc");
    final long internalBefore = timerCount("internal");

    final RecordingInsertResponseObserver resp = new RecordingInsertResponseObserver();
    final StreamObserver<InsertRequest> req = service.insertBidirectional(resp);

    req.onNext(InsertRequest.newBuilder().setStart(Start.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setOptions(upsertOptions()).build()).build());
    final Started started = resp.await(resp.started);

    final InsertChunk.Builder chunk = InsertChunk.newBuilder().setSessionId(started.getSessionId()).setChunkSeq(1);
    for (int i = 0; i < ROWS; i++)
      chunk.addRows(row(i));
    req.onNext(InsertRequest.newBuilder().setChunk(chunk.build()).build());
    final BatchAck ack = resp.await(resp.batchAck);
    assertThat(ack.getUpdated()).isEqualTo(ROWS);
    assertThat(ack.getFailed()).isZero();

    req.onNext(InsertRequest.newBuilder().setCommit(Commit.newBuilder().setSessionId(started.getSessionId())
        .setCommit(true).build()).build());
    resp.await(resp.committed);
    req.onCompleted();

    assertThat(timerCount("grpc") - grpcBefore).as("one metered lookup per upserted row").isGreaterThanOrEqualTo(ROWS);
    assertThat(timerCount("internal") - internalBefore).as("none of them attributed to internal").isZero();
  }

  private InsertOptions upsertOptions() {
    return InsertOptions.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setTargetClass(typeName).setConflictMode(InsertOptions.ConflictMode.CONFLICT_UPDATE).addKeyColumns("name")
        .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM).build();
  }

  private GrpcRecord row(final int i) {
    return GrpcRecord.newBuilder().setType(typeName)
        .putProperties("name", GrpcValue.newBuilder().setStringValue("k" + i).build())
        .putProperties("v", GrpcValue.newBuilder().setInt32Value(1).build()).build();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  /** Total count of the {@code arcadedb.query.duration} timers carrying this {@code protocol} tag. */
  private static long timerCount(final String protocol) {
    return Metrics.globalRegistry.find("arcadedb.query.duration").tag("protocol", protocol).timers().stream()
        .mapToLong(Timer::count).sum();
  }

  private static final class RecordingSummaryObserver extends ServerCallStreamObserver<InsertSummary> {
    private final AtomicReference<InsertSummary> summaryRef;
    private final CountDownLatch                 done;

    private RecordingSummaryObserver(final AtomicReference<InsertSummary> summaryRef, final CountDownLatch done) {
      this.summaryRef = summaryRef;
      this.done = done;
    }

    @Override public void onNext(final InsertSummary value) { summaryRef.set(value); }
    @Override public void onError(final Throwable t) { done.countDown(); }
    @Override public void onCompleted() { done.countDown(); }
    @Override public boolean isCancelled() { return false; }
    @Override public void setOnCancelHandler(final Runnable onCancelHandler) { }
    @Override public void setCompression(final String compression) { }
    @Override public boolean isReady() { return true; }
    @Override public void setOnReadyHandler(final Runnable onReadyHandler) { }
    @Override public void request(final int count) { }
    @Override public void setMessageCompression(final boolean enable) { }
    @Override public void disableAutoInboundFlowControl() { }
  }

  private static final class RecordingInsertResponseObserver extends ServerCallStreamObserver<InsertResponse> {
    private final Slot<Started>   started   = new Slot<>();
    private final Slot<BatchAck>  batchAck  = new Slot<>();
    private final Slot<Committed> committed = new Slot<>();
    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private static final class Slot<T> {
      private final AtomicReference<T> value = new AtomicReference<>();
      private final CountDownLatch     latch = new CountDownLatch(1);
    }

    private <T> T await(final Slot<T> slot) throws InterruptedException {
      assertThat(slot.latch.await(30, TimeUnit.SECONDS)).as("timed out waiting for the frame").isTrue();
      if (error.get() != null)
        throw new AssertionError("insertBidirectional errored", error.get());
      return slot.value.get();
    }

    @Override
    public void onNext(final InsertResponse value) {
      switch (value.getMsgCase()) {
      case STARTED -> { started.value.set(value.getStarted()); started.latch.countDown(); }
      case BATCH_ACK -> { batchAck.value.set(value.getBatchAck()); batchAck.latch.countDown(); }
      case COMMITTED -> { committed.value.set(value.getCommitted()); committed.latch.countDown(); }
      default -> { }
      }
    }

    @Override
    public void onError(final Throwable t) {
      error.set(t);
      started.latch.countDown();
      batchAck.latch.countDown();
      committed.latch.countDown();
    }

    @Override public void onCompleted() { }
    @Override public boolean isCancelled() { return false; }
    @Override public void setOnCancelHandler(final Runnable onCancelHandler) { }
    @Override public void setCompression(final String compression) { }
    @Override public boolean isReady() { return true; }
    @Override public void setOnReadyHandler(final Runnable onReadyHandler) { }
    @Override public void request(final int count) { }
    @Override public void setMessageCompression(final boolean enable) { }
    @Override public void disableAutoInboundFlowControl() { }
  }
}
