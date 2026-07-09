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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.server.BaseGraphServerTest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5041: the gRPC insert-stream machinery ({@code InsertContext} in
 * {@code ArcadeDbGrpcService}) mismanages ArcadeDB's thread-bound transactions across gRPC callbacks.
 *
 * <ul>
 *   <li><b>TX-3</b>: {@code InsertContext.close()} was an empty no-op, so {@code closeQuietly()} on an
 *       error/cancel path never rolled back the transaction begun in the ctor. The orphaned, still
 *       active transaction stayed bound to the callback thread and corrupted the next request that
 *       reused it.</li>
 *   <li><b>TX-4</b>: {@code insertStream} left the live transaction parked in the thread-local
 *       {@code DatabaseContext} between callbacks. An unrelated request reusing that pool thread calls
 *       {@code DatabaseContext.init()}, which rolls back the parked transaction, silently discarding
 *       the stream's rows.</li>
 *   <li><b>TX-6</b>: {@code insertBidirectional} half-close without an explicit COMMIT called
 *       {@code flushCommit(false)}, which actually COMMITS the buffered rows for PER_BATCH/PER_ROW
 *       instead of rolling them back.</li>
 * </ul>
 */
public class Issue5041InsertStreamTxLifecycleIT extends BaseGraphServerTest {

  // The insert-stream handlers are driven in-process by constructing ArcadeDbGrpcService directly, so
  // no gRPC port needs to be bound: the GrpcServerPlugin is intentionally NOT enabled here (binding a
  // fixed port across several server-restarting test methods races with itself).

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private GrpcValue stringValue(final String s) {
    return GrpcValue.newBuilder().setStringValue(s).build();
  }

  private InsertChunk perStreamChunk(final String typeName, final int rows) {
    final InsertChunk.Builder chunk = InsertChunk.newBuilder()
        .setSessionId("issue-5041")
        .setChunkSeq(0)
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass(typeName)
            .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
            .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
            .build());
    for (int i = 0; i < rows; i++)
      chunk.addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("v" + i)).build());
    return chunk.build();
  }

  private long countRows(final String typeName) {
    return getServer(0).getDatabase(getDatabaseName())
        .query("sql", "SELECT count(*) AS total FROM " + typeName).next().<Long>getProperty("total");
  }

  /**
   * TX-3: a client error mid-stream must roll back the just-begun transaction so it does not stay
   * active and bound to the callback thread. Before the fix, {@code close()} did nothing and the
   * transaction stayed active on the thread; after the fix, {@code close()} rolls it back.
   */
  @Test
  void streamErrorRollsBackAndLeavesNoActiveTransaction() throws Exception {
    final String typeName = "Issue5041Tx3_" + System.currentTimeMillis();
    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    final ExecutorService pool = Executors.newSingleThreadExecutor(r -> new Thread(r, "issue5041-tx3-pool"));
    try {
      final Database db = getServer(0).getDatabase(getDatabaseName());
      final RecordingResponseObserver resp = new RecordingResponseObserver();
      final StreamObserver<InsertChunk> req = service.insertStream(resp);

      // All callbacks run on ONE pool thread, modelling a reused gRPC pool thread.
      pool.submit(() -> req.onNext(perStreamChunk(typeName, 20))).get(30, TimeUnit.SECONDS);

      // Client aborts mid-stream: onError must clean up the transaction.
      pool.submit(() -> req.onError(new RuntimeException("client aborted"))).get(30, TimeUnit.SECONDS);

      // On the SAME pool thread the orphaned transaction must be gone: no active transaction leaked.
      final boolean stillActive = pool.submit(db::isTransactionActive).get(30, TimeUnit.SECONDS);
      assertThat(stillActive).as("no active transaction may be left bound to the pool thread after onError").isFalse();

      // And the aborted stream's rows must not have been committed.
      assertThat(countRows(typeName)).isZero();
    } finally {
      pool.shutdownNow();
      service.close();
      getServer(0).getDatabase(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  /**
   * TX-4: an unrelated request reusing the same pool thread calls {@code DatabaseContext.init()},
   * which rolls back whatever transaction is parked in the thread-local. Before the fix the insert
   * stream left its transaction parked there, so the unrelated init() silently discarded its rows
   * (deferred commit found a rolled-back transaction). After the fix the stream unbinds its
   * transaction from the thread between callbacks, so the unrelated init() cannot touch it.
   */
  @Test
  void unrelatedInitOnSharedThreadDoesNotDiscardStreamRows() throws Exception {
    final String typeName = "Issue5041Tx4_" + System.currentTimeMillis();
    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    final ExecutorService pool = Executors.newSingleThreadExecutor(r -> new Thread(r, "issue5041-tx4-pool"));
    try {
      final Database db = getServer(0).getDatabase(getDatabaseName());
      final RecordingResponseObserver resp = new RecordingResponseObserver();
      final StreamObserver<InsertChunk> req = service.insertStream(resp);

      // Chunk 1 begins the PER_STREAM transaction and inserts the rows (commit deferred to onCompleted).
      pool.submit(() -> req.onNext(perStreamChunk(typeName, 30))).get(30, TimeUnit.SECONDS);

      // An unrelated request reuses this pool thread: getDatabase() -> DatabaseContext.init() rolls
      // back any transaction parked in the thread-local for this database.
      pool.submit(() -> DatabaseContext.INSTANCE.init((DatabaseInternal) db)).get(30, TimeUnit.SECONDS);

      // Half-close: deferred PER_STREAM commit.
      pool.submit(req::onCompleted).get(30, TimeUnit.SECONDS);

      final InsertSummary summary = resp.summaryRef.get();
      assertThat(summary).isNotNull();
      assertThat(summary.getInserted()).as("all streamed rows must survive the unrelated init() and commit").isEqualTo(30);
      assertThat(summary.getFailed()).isZero();
      assertThat(countRows(typeName)).isEqualTo(30L);
    } finally {
      pool.shutdownNow();
      service.close();
      getServer(0).getDatabase(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  /**
   * TX-6: a bidirectional half-close WITHOUT an explicit COMMIT must roll back the buffered rows.
   * Before the fix the handler called {@code flushCommit(false)}, which COMMITS for PER_BATCH, so the
   * rows were silently persisted even though the client never sent a Commit.
   */
  @Test
  void bidirectionalHalfCloseWithoutCommitCommitsNothing() throws Exception {
    final String typeName = "Issue5041Tx6_" + System.currentTimeMillis();
    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final BidiResponseObserver resp = new BidiResponseObserver();
      final StreamObserver<InsertRequest> req = service.insertBidirectional(resp);

      // START: PER_BATCH with the default server batch size (1000), so a small chunk is buffered but
      // not committed by insertRows; only an explicit COMMIT (or the buggy flushCommit(false)) commits.
      req.onNext(InsertRequest.newBuilder().setStart(Start.newBuilder()
          .setDatabase(getDatabaseName())
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
              .build())
          .build()).build());
      assertThat(resp.startedLatch.await(30, TimeUnit.SECONDS)).isTrue();

      // Capture the dedicated single-thread worker so we can wait for its terminal cleanup to run.
      final Thread worker = awaitBidiWorker();
      assertThat(worker).as("bidirectional stream worker thread").isNotNull();

      final InsertChunk.Builder chunk = InsertChunk.newBuilder().setSessionId(resp.sessionId.get()).setChunkSeq(1);
      for (int i = 0; i < 10; i++)
        chunk.addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("v" + i)).build());
      req.onNext(InsertRequest.newBuilder().setChunk(chunk.build()).build());
      assertThat(resp.batchAckLatch.await(30, TimeUnit.SECONDS)).isTrue();

      // Half-close WITHOUT sending a Commit message.
      req.onCompleted();

      // The worker thread terminates only after the terminal cleanup task has fully run (shutdown()
      // drains the queue first), giving a deterministic happens-before for the assertions below.
      worker.join(30_000);
      assertThat(worker.isAlive()).isFalse();

      assertThat(resp.committed.get()).as("no Committed response on a half-close without COMMIT").isFalse();
      assertThat(countRows(typeName)).as("half-close without COMMIT must commit nothing").isZero();
    } finally {
      service.close();
      getServer(0).getDatabase(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  private static Thread awaitBidiWorker() throws InterruptedException {
    for (int i = 0; i < 300; i++) {
      for (final Thread t : Thread.getAllStackTraces().keySet())
        if (t.isAlive() && t.getName().startsWith("grpc-bidi-stream-"))
          return t;
      Thread.sleep(10);
    }
    return null;
  }

  /**
   * Minimal {@link ServerCallStreamObserver} double for the client-streaming {@code insertStream}
   * handler that captures the single {@link InsertSummary} it emits.
   */
  private static final class RecordingResponseObserver extends ServerCallStreamObserver<InsertSummary> {
    final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();

    @Override public void onNext(final InsertSummary value) { summaryRef.set(value); }
    @Override public void onError(final Throwable t) { }
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

  /**
   * Minimal {@link ServerCallStreamObserver} double for the bidirectional {@code insertBidirectional}
   * handler that latches on the Started/BatchAck responses and records whether a Committed arrived.
   */
  private static final class BidiResponseObserver extends ServerCallStreamObserver<InsertResponse> {
    final CountDownLatch                startedLatch  = new CountDownLatch(1);
    final CountDownLatch                batchAckLatch = new CountDownLatch(1);
    final AtomicReference<String>       sessionId     = new AtomicReference<>("");
    final java.util.concurrent.atomic.AtomicBoolean committed = new java.util.concurrent.atomic.AtomicBoolean(false);

    @Override public void onNext(final InsertResponse value) {
      switch (value.getMsgCase()) {
        case STARTED -> { sessionId.set(value.getStarted().getSessionId()); startedLatch.countDown(); }
        case BATCH_ACK -> batchAckLatch.countDown();
        case COMMITTED -> committed.set(true);
        default -> { }
      }
    }
    @Override public void onError(final Throwable t) { }
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
