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
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Transactions around the insert streams: the transaction {@code insertStream} and {@code insertBidirectional} begin
 * themselves across gRPC callback threads, and the caller-created (external) transaction they, and {@code bulkInsert},
 * join when the request names one.
 */
class GrpcInsertStreamExternalTransactionIT extends BaseGraphServerTest {

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
    return getServer(0).getDatabaseForOperator(getDatabaseName())
        .query("sql", "SELECT count(*) AS total FROM " + typeName).next().<Long>getProperty("total");
  }

  // Regression: #5041
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
   * <p>
   * TX-3: a client error mid-stream must roll back the just-begun transaction so it does not stay
   * active and bound to the callback thread. Before the fix, {@code close()} did nothing and the
   * transaction stayed active on the thread; after the fix, {@code close()} rolls it back.
   */
  @Test
  void streamErrorRollsBackAndLeavesNoActiveTransaction() throws Exception {
    final String typeName = "Issue5041Tx3_" + System.currentTimeMillis();
    getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    final ExecutorService pool = Executors.newSingleThreadExecutor(r -> new Thread(r, "issue5041-tx3-pool"));
    try {
      final Database db = getServer(0).getDatabaseForOperator(getDatabaseName());
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
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  // Regression: #5041
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
    getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    final ExecutorService pool = Executors.newSingleThreadExecutor(r -> new Thread(r, "issue5041-tx4-pool"));
    try {
      final Database db = getServer(0).getDatabaseForOperator(getDatabaseName());
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
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  // Regression: #5041
  /**
   * TX-6: a bidirectional half-close WITHOUT an explicit COMMIT must roll back the buffered rows.
   * Before the fix the handler called {@code flushCommit(false)}, which COMMITS for PER_BATCH, so the
   * rows were silently persisted even though the client never sent a Commit.
   */
  @Test
  void bidirectionalHalfCloseWithoutCommitCommitsNothing() throws Exception {
    final String typeName = "Issue5041Tx6_" + System.currentTimeMillis();
    getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

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
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  // Regression: #5041
  /**
   * CON-1: a bidirectional insert authenticated ONLY through the gRPC context ({@code USER_CONTEXT_KEY},
   * as set by the auth interceptor for header/Bearer clients) - with NO credentials in the request
   * payload - must work. The stream executor runs on a different thread than the inbound callback, so
   * the captured {@code io.grpc.Context} has to be propagated for {@code resolvedUsername()} to see the
   * user. Self-validating: the first stream (no context, no credentials) must be rejected, proving auth
   * is actually enforced; the second (context user, still no credentials) must commit.
   */
  @Test
  void bidirectionalHeaderOnlyAuthIsPropagatedToStreamExecutor() throws Exception {
    final String typeName = "Issue5041Con1_" + System.currentTimeMillis();
    getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final InsertRequest start = InsertRequest.newBuilder().setStart(Start.newBuilder()
          .setDatabase(getDatabaseName())
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              // deliberately NO credentials: the only identity source is the gRPC context
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
              .build())
          .build()).build();

      // (a) No context and no credentials: START must be rejected (auth is enforced).
      final BidiResponseObserver respA = new BidiResponseObserver();
      final StreamObserver<InsertRequest> reqA = service.insertBidirectional(respA);
      reqA.onNext(start);
      assertThat(respA.terminalLatch.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(respA.error.get()).as("unauthenticated START must be rejected").isNotNull();
      assertThat(respA.sessionId.get()).isEmpty();

      // (b) Same payload (no credentials) but the handler is invoked within a gRPC context carrying
      // the authenticated user, exactly as the auth interceptor would set up for a header/Bearer client.
      final BidiResponseObserver respB = new BidiResponseObserver();
      final Context authContext = Context.current().withValue(GrpcAuthInterceptor.USER_CONTEXT_KEY, "root");
      final AtomicReference<StreamObserver<InsertRequest>> reqBHolder = new AtomicReference<>();
      authContext.run(() -> reqBHolder.set(service.insertBidirectional(respB)));
      final StreamObserver<InsertRequest> reqB = reqBHolder.get();

      reqB.onNext(start);
      assertThat(respB.startedLatch.await(30, TimeUnit.SECONDS)).as("context-authenticated START must succeed").isTrue();

      final InsertChunk.Builder chunk = InsertChunk.newBuilder().setSessionId(respB.sessionId.get()).setChunkSeq(1);
      for (int i = 0; i < 5; i++)
        chunk.addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("v" + i)).build());
      reqB.onNext(InsertRequest.newBuilder().setChunk(chunk.build()).build());
      assertThat(respB.batchAckLatch.await(30, TimeUnit.SECONDS)).isTrue();

      reqB.onNext(InsertRequest.newBuilder().setCommit(Commit.newBuilder().setSessionId(respB.sessionId.get()).setCommit(true).build()).build());
      assertThat(respB.terminalLatch.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(respB.committed.get()).as("context-authenticated stream must commit").isTrue();
      assertThat(countRows(typeName)).isEqualTo(5L);
    } finally {
      service.close();
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
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
    final CountDownLatch          startedLatch  = new CountDownLatch(1);
    final CountDownLatch          batchAckLatch = new CountDownLatch(1);
    final CountDownLatch          terminalLatch = new CountDownLatch(1);
    final AtomicReference<String> sessionId     = new AtomicReference<>("");
    final AtomicBoolean           committed     = new AtomicBoolean(false);
    final AtomicReference<Throwable> error      = new AtomicReference<>();

    @Override public void onNext(final InsertResponse value) {
      switch (value.getMsgCase()) {
        case STARTED -> { sessionId.set(value.getStarted().getSessionId()); startedLatch.countDown(); }
        case BATCH_ACK -> batchAckLatch.countDown();
        case COMMITTED -> committed.set(true);
        default -> { }
      }
    }
    @Override public void onError(final Throwable t) { error.set(t); terminalLatch.countDown(); }
    @Override public void onCompleted() { terminalLatch.countDown(); }

    @Override public boolean isCancelled() { return false; }
    @Override public void setOnCancelHandler(final Runnable onCancelHandler) { }
    @Override public void setCompression(final String compression) { }
    @Override public boolean isReady() { return true; }
    @Override public void setOnReadyHandler(final Runnable onReadyHandler) { }
    @Override public void request(final int count) { }
    @Override public void setMessageCompression(final boolean enable) { }
    @Override public void disableAutoInboundFlowControl() { }
  }

  // Regression: #6607
  /**
   * Regression test for issue #6607: {@code insertStream} resolves an external transaction context on
   * the first chunk but never dispatches the actual row insertion onto that transaction's dedicated
   * executor thread the way every other
   * transaction-scoped RPC ({@code bulkInsert}, {@code createRecord}, {@code executeCommand}, ...) does.
   * Because ArcadeDB transactions are bound to the calling thread via the {@code DatabaseContext}
   * {@code ThreadLocal}, inserting on the wrong thread means the external transaction is not actually
   * active there, so the rows are written outside it (auto-committing independently) and survive a
   * rollback of the transaction the client believes wraps them.
   * <p>
   * Rows streamed under an external transaction must not survive a rollback of that transaction:
   * the whole point of passing a {@code transaction_id} is that the caller controls commit/rollback.
   */
  @Test
  void rowsStreamedUnderExternalTransactionDoNotSurviveRollback() throws Exception {
    final String typeName = "Issue6607ExtTxRollback_" + System.currentTimeMillis();
    getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final BeginTransactionResponse begin = beginTransaction(service);
      final String txId = begin.getTransactionId();
      assertThat(txId).isNotBlank();

      final RecordingResponseObserver resp = new RecordingResponseObserver();
      final StreamObserver<InsertChunk> req = service.insertStream(resp);

      final InsertChunk.Builder chunk = InsertChunk.newBuilder()
          .setSessionId("issue-6607-rollback")
          .setChunkSeq(0)
          .setLast(true)
          .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
          // Top-level chunk credentials, distinct from InsertOptions.credentials: resolveAuthorizedTransaction()
          // authorizes the external-transaction path off THIS field (see InsertChunk.credentials in the proto).
          .setCredentials(credentials())
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .build());
      for (int i = 0; i < 25; i++)
        chunk.addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("v" + i)).build());

      req.onNext(chunk.build());
      req.onCompleted();

      final InsertSummary summary = resp.summaryRef.get();
      assertThat(summary).isNotNull();
      assertThat(summary.getInserted()).isEqualTo(25);
      assertThat(summary.getFailed()).isZero();

      // The rows are visible inside the still-open external transaction before it is rolled back...
      final RollbackTransactionResponse rollback = rollbackTransaction(service, txId);
      assertThat(rollback.getSuccess()).isTrue();
      assertThat(rollback.getRolledBack()).isTrue();

      // ...but once rolled back, none of them may have survived outside it.
      assertThat(countRows(typeName))
          .as("rows streamed under an external transaction must not survive its rollback")
          .isZero();
    } finally {
      service.close();
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  // Regression: #6607
  /**
   * Sanity companion to the rollback test above: rows streamed under an external transaction must still
   * commit normally when the client commits it, so the fix does not just make everything vanish.
   */
  @Test
  void rowsStreamedUnderExternalTransactionPersistOnCommit() throws Exception {
    final String typeName = "Issue6607ExtTxCommit_" + System.currentTimeMillis();
    getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final BeginTransactionResponse begin = beginTransaction(service);
      final String txId = begin.getTransactionId();

      final RecordingResponseObserver resp = new RecordingResponseObserver();
      final StreamObserver<InsertChunk> req = service.insertStream(resp);

      final InsertChunk.Builder chunk = InsertChunk.newBuilder()
          .setSessionId("issue-6607-commit")
          .setChunkSeq(0)
          .setLast(true)
          .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
          // Top-level chunk credentials, distinct from InsertOptions.credentials: resolveAuthorizedTransaction()
          // authorizes the external-transaction path off THIS field (see InsertChunk.credentials in the proto).
          .setCredentials(credentials())
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .build());
      for (int i = 0; i < 12; i++)
        chunk.addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("v" + i)).build());

      req.onNext(chunk.build());
      req.onCompleted();

      final InsertSummary summary = resp.summaryRef.get();
      assertThat(summary).isNotNull();
      assertThat(summary.getInserted()).isEqualTo(12);

      final AtomicReference<CommitTransactionResponse> commitRef = new AtomicReference<>();
      service.commitTransaction(
          CommitTransactionRequest.newBuilder()
              .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
              .setCredentials(credentials())
              .build(),
          capturing(commitRef));
      assertThat(commitRef.get().getSuccess()).isTrue();

      assertThat(countRows(typeName)).isEqualTo(12L);
    } finally {
      service.close();
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  // Regression: #6607
  /**
   * Companion regression test for a status-code-fidelity defect found while reviewing #6607:
   * {@code bulkInsert}'s call to {@code resolveAuthorizedTransaction()} used to sit inside the method's
   * outer {@code catch (Exception e)}, which unconditionally rewraps everything as {@code Status.INTERNAL}
   * - so a real {@code PERMISSION_DENIED}/{@code UNAUTHENTICATED} rejection from authorizing the external
   * transaction was silently downgraded, hiding the real cause from the client.
   */
  @Test
  void bulkInsertWithExternalTransactionBadCredentialsPreservesRealStatusCode() throws Exception {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final BeginTransactionResponse begin = beginTransaction(service);
      final String txId = begin.getTransactionId();
      try {
        final BulkInsertRequest request = BulkInsertRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword("definitely-wrong-password").build())
            .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
            .setOptions(InsertOptions.newBuilder().setDatabase(getDatabaseName()).setTargetClass("Person").build())
            .build();

        final CapturingInsertSummaryObserver resp = new CapturingInsertSummaryObserver();
        service.bulkInsert(request, resp);

        assertThat(resp.errorRef.get())
            .as("a bad-credentials rejection must reach the client unmodified, not be downgraded to Status.INTERNAL")
            .isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) resp.errorRef.get()).getStatus().getCode())
            .isIn(Status.Code.PERMISSION_DENIED, Status.Code.UNAUTHENTICATED);
      } finally {
        rollbackTransaction(service, txId);
      }
    } finally {
      service.close();
    }
  }

  // Regression: #6607
  /**
   * {@code bulkInsert}'s external-transaction path only had authorization-status coverage before this
   * PR; it never had a rollback/commit lifecycle test the way {@code insertStream} now does above.
   * Rows inserted via {@code bulkInsert} under an external transaction must not survive its rollback.
   */
  @Test
  void bulkInsertWithExternalTransactionRowsDoNotSurviveRollback() throws Exception {
    final String typeName = "Issue6607BulkExtTxRollback_" + System.currentTimeMillis();
    getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final BeginTransactionResponse begin = beginTransaction(service);
      final String txId = begin.getTransactionId();

      final List<GrpcRecord> records = new ArrayList<>();
      for (int i = 0; i < 10; i++)
        records.add(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("v" + i)).build());

      final BulkInsertRequest request = BulkInsertRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .build())
          .addAllRows(records)
          .build();

      final CapturingInsertSummaryObserver resp = new CapturingInsertSummaryObserver();
      service.bulkInsert(request, resp);

      assertThat(resp.errorRef.get()).isNull();
      assertThat(resp.summaryRef.get()).isNotNull();
      assertThat(resp.summaryRef.get().getInserted()).isEqualTo(10);

      final RollbackTransactionResponse rollback = rollbackTransaction(service, txId);
      assertThat(rollback.getRolledBack()).isTrue();

      assertThat(countRows(typeName))
          .as("rows bulk-inserted under an external transaction must not survive its rollback")
          .isZero();
    } finally {
      service.close();
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  // Regression: #6607
  /**
   * Sanity companion: {@code bulkInsert} rows under an external transaction must still commit normally.
   */
  @Test
  void bulkInsertWithExternalTransactionRowsPersistOnCommit() throws Exception {
    final String typeName = "Issue6607BulkExtTxCommit_" + System.currentTimeMillis();
    getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final BeginTransactionResponse begin = beginTransaction(service);
      final String txId = begin.getTransactionId();

      final List<GrpcRecord> records = new ArrayList<>();
      for (int i = 0; i < 7; i++)
        records.add(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("v" + i)).build());

      final BulkInsertRequest request = BulkInsertRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .build())
          .addAllRows(records)
          .build();

      final CapturingInsertSummaryObserver resp = new CapturingInsertSummaryObserver();
      service.bulkInsert(request, resp);

      assertThat(resp.errorRef.get()).isNull();
      assertThat(resp.summaryRef.get().getInserted()).isEqualTo(7);

      final AtomicReference<CommitTransactionResponse> commitRef = new AtomicReference<>();
      service.commitTransaction(
          CommitTransactionRequest.newBuilder()
              .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
              .setCredentials(credentials())
              .build(),
          capturing(commitRef));
      assertThat(commitRef.get().getSuccess()).isTrue();

      assertThat(countRows(typeName)).isEqualTo(7L);
    } finally {
      service.close();
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  private BeginTransactionResponse beginTransaction(final ArcadeDbGrpcService service) {
    final AtomicReference<BeginTransactionResponse> ref = new AtomicReference<>();
    service.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build(),
        capturing(ref));
    return ref.get();
  }

  private RollbackTransactionResponse rollbackTransaction(final ArcadeDbGrpcService service, final String txId) {
    final AtomicReference<RollbackTransactionResponse> ref = new AtomicReference<>();
    service.rollbackTransaction(
        RollbackTransactionRequest.newBuilder()
            .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
            .setCredentials(credentials())
            .build(),
        capturing(ref));
    return ref.get();
  }

  private <T> StreamObserver<T> capturing(final AtomicReference<T> ref) {
    return new StreamObserver<>() {
      @Override public void onNext(final T value) { ref.set(value); }
      @Override public void onError(final Throwable t) { throw new RuntimeException(t); }
      @Override public void onCompleted() { }
    };
  }

  /**
   * Minimal {@link StreamObserver} double for the unary {@code bulkInsert} handler that records
   * either the {@link InsertSummary} it emits or the {@link Throwable} passed to {@code onError},
   * without throwing - so a test can assert on the captured status after the call returns instead of
   * having to unwind a thrown exception.
   */
  private static final class CapturingInsertSummaryObserver implements StreamObserver<InsertSummary> {
    final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
    final AtomicReference<Throwable>     errorRef   = new AtomicReference<>();

    @Override public void onNext(final InsertSummary value) { summaryRef.set(value); }
    @Override public void onError(final Throwable t) { errorRef.set(t); }
    @Override public void onCompleted() { }
  }

  // Regression: #6795
  /**
   * Regression test for issue #6795 (follow-up on #6607, reported as a comment on the closed issue):
   * {@code insertBidirectional}'s {@code START} handler never read {@code Start.transaction} - it always built
   * its {@code InsertContext} with {@code new InsertContext(opts)}, which begins and owns its OWN transaction,
   * exactly the bug {@code insertStream}/{@code bulkInsert} were fixed for in #6607. Rows streamed under a
   * {@code transaction_id} therefore landed in a fresh internal transaction the stream itself committed on
   * {@code COMMIT}/half-close, never joining the caller's transaction: a client rollback of that transaction
   * could not undo them.
   * <p>
   * Same shape as {@link #rowsStreamedUnderExternalTransactionDoNotSurviveRollback() the #6607 tests}, driving the START/CHUNK/COMMIT
   * request/response protocol {@code insertBidirectional} uses instead of {@code insertStream}'s flatter one.
   * <p>
   * Rows sent via {@code insertBidirectional} under an external transaction must not survive a rollback of
   * that transaction: the whole point of passing {@code Start.transaction} is that the caller controls
   * commit/rollback, not the stream's own {@code COMMIT} message.
   */
  @Test
  void rowsSentViaInsertBidirectionalUnderExternalTransactionDoNotSurviveRollback() throws Exception {
    final String typeName = "Issue6795BidiExtTxRollback_" + System.currentTimeMillis();
    getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final BeginTransactionResponse begin = beginTransaction(service);
      final String txId = begin.getTransactionId();
      assertThat(txId).isNotBlank();

      final RecordingInsertResponseObserver resp = new RecordingInsertResponseObserver();
      final StreamObserver<InsertRequest> req = service.insertBidirectional(resp);

      req.onNext(InsertRequest.newBuilder().setStart(Start.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .build())
          .build()).build());

      final Started started = resp.awaitStarted();
      assertThat(started.getSessionId()).isNotBlank();

      final InsertChunk.Builder chunk = InsertChunk.newBuilder()
          .setSessionId(started.getSessionId())
          .setChunkSeq(1);
      for (int i = 0; i < 15; i++)
        chunk.addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("v" + i)).build());
      req.onNext(InsertRequest.newBuilder().setChunk(chunk.build()).build());

      final BatchAck ack = resp.awaitBatchAck();
      assertThat(ack.getInserted()).isEqualTo(15);
      assertThat(ack.getFailed()).isZero();

      req.onNext(InsertRequest.newBuilder()
          .setCommit(Commit.newBuilder().setSessionId(started.getSessionId()).setCommit(true).build()).build());
      final Committed committed = resp.awaitCommitted();
      assertThat(committed.getSummary().getInserted()).isEqualTo(15);

      // The stream's own COMMIT must be a no-op for an external transaction: the rows are visible inside the
      // still-open external transaction before it is rolled back...
      final RollbackTransactionResponse rollback = rollbackTransaction(service, txId);
      assertThat(rollback.getSuccess()).isTrue();
      assertThat(rollback.getRolledBack()).isTrue();

      // ...but once rolled back, none of them may have survived outside it.
      assertThat(countRows(typeName))
          .as("rows sent via insertBidirectional under an external transaction must not survive its rollback")
          .isZero();
    } finally {
      service.close();
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  // Regression: #6795
  /**
   * Sanity companion: rows sent via {@code insertBidirectional} under an external transaction must still
   * commit normally when the client commits it, so the fix does not just make everything vanish.
   */
  @Test
  void rowsSentViaInsertBidirectionalUnderExternalTransactionPersistOnCommit() throws Exception {
    final String typeName = "Issue6795BidiExtTxCommit_" + System.currentTimeMillis();
    getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final BeginTransactionResponse begin = beginTransaction(service);
      final String txId = begin.getTransactionId();

      final RecordingInsertResponseObserver resp = new RecordingInsertResponseObserver();
      final StreamObserver<InsertRequest> req = service.insertBidirectional(resp);

      req.onNext(InsertRequest.newBuilder().setStart(Start.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .build())
          .build()).build());

      final Started started = resp.awaitStarted();

      final InsertChunk.Builder chunk = InsertChunk.newBuilder()
          .setSessionId(started.getSessionId())
          .setChunkSeq(1);
      for (int i = 0; i < 9; i++)
        chunk.addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("v" + i)).build());
      req.onNext(InsertRequest.newBuilder().setChunk(chunk.build()).build());
      resp.awaitBatchAck();

      req.onNext(InsertRequest.newBuilder()
          .setCommit(Commit.newBuilder().setSessionId(started.getSessionId()).setCommit(true).build()).build());
      resp.awaitCommitted();

      final AtomicReference<CommitTransactionResponse> commitRef = new AtomicReference<>();
      service.commitTransaction(
          CommitTransactionRequest.newBuilder()
              .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
              .setCredentials(credentials())
              .build(),
          capturing(commitRef));
      assertThat(commitRef.get().getSuccess()).isTrue();

      assertThat(countRows(typeName)).isEqualTo(9L);
    } finally {
      service.close();
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  /**
   * Minimal {@link ServerCallStreamObserver} double for the bidi-streaming {@code insertBidirectional}
   * handler. {@code onNext} runs on the service's own per-stream executor thread (a different thread than
   * the test's), so each expected response is handed to the test thread through its own latch instead of a
   * plain field read.
   */
  private static final class RecordingInsertResponseObserver extends ServerCallStreamObserver<InsertResponse> {
    private final AtomicReference<Started>   startedRef   = new AtomicReference<>();
    private final AtomicReference<BatchAck>  batchAckRef  = new AtomicReference<>();
    private final AtomicReference<Committed> committedRef = new AtomicReference<>();
    private final AtomicReference<Throwable> errorRef     = new AtomicReference<>();

    private volatile CountDownLatch startedLatch   = new CountDownLatch(1);
    private volatile CountDownLatch batchAckLatch   = new CountDownLatch(1);
    private volatile CountDownLatch committedLatch  = new CountDownLatch(1);

    @Override
    public void onNext(final InsertResponse value) {
      switch (value.getMsgCase()) {
        case STARTED -> {
          startedRef.set(value.getStarted());
          startedLatch.countDown();
        }
        case BATCH_ACK -> {
          batchAckRef.set(value.getBatchAck());
          batchAckLatch.countDown();
        }
        case COMMITTED -> {
          committedRef.set(value.getCommitted());
          committedLatch.countDown();
        }
        default -> {
        }
      }
    }

    @Override
    public void onError(final Throwable t) {
      errorRef.set(t);
      startedLatch.countDown();
      batchAckLatch.countDown();
      committedLatch.countDown();
    }

    @Override
    public void onCompleted() {
    }

    Started awaitStarted() throws InterruptedException {
      assertThat(startedLatch.await(10, TimeUnit.SECONDS)).as("timed out waiting for Started").isTrue();
      if (errorRef.get() != null)
        throw new AssertionError("insertBidirectional errored before Started", errorRef.get());
      return startedRef.get();
    }

    BatchAck awaitBatchAck() throws InterruptedException {
      assertThat(batchAckLatch.await(10, TimeUnit.SECONDS)).as("timed out waiting for BatchAck").isTrue();
      if (errorRef.get() != null)
        throw new AssertionError("insertBidirectional errored before BatchAck", errorRef.get());
      return batchAckRef.get();
    }

    Committed awaitCommitted() throws InterruptedException {
      assertThat(committedLatch.await(10, TimeUnit.SECONDS)).as("timed out waiting for Committed").isTrue();
      if (errorRef.get() != null)
        throw new AssertionError("insertBidirectional errored before Committed", errorRef.get());
      return committedRef.get();
    }

    @Override public boolean isCancelled() { return false; }
    @Override public void setOnCancelHandler(final Runnable onCancelHandler) { }
    @Override public void setCompression(final String compression) { }
    @Override public boolean isReady() { return true; }
    @Override public void setOnReadyHandler(final Runnable onReadyHandler) { }
    @Override public void request(final int count) { }
    @Override public void setMessageCompression(final boolean enable) { }
    @Override public void disableAutoInboundFlowControl() { }
  }

  // Regression: #6795
  /**
   * Regression test for issue #6795 (follow-up on #6607, reported as a comment on the closed issue):
   * {@code insertStream}'s first-chunk call to {@code resolveAuthorizedTransaction(...)} sat inside the chunk
   * handler's folding {@code try/catch}, so a {@code StatusRuntimeException} it throws (e.g.
   * {@code PERMISSION_DENIED} from {@code authorizeTransactionAccess} when the caller is not the transaction's
   * owner) was caught by the generic per-chunk handler and downgraded into a structured error entry inside an
   * otherwise "successful" {@link InsertSummary} delivered via {@code onCompleted} - instead of reaching the
   * client as the real gRPC status via {@code onError}, the way {@code bulkInsert} already did for the same
   * failure (see {@link #bulkInsertWithExternalTransactionBadCredentialsPreservesRealStatusCode()}).
   */
  @Test
  void insertStreamPreservesRealStatusCodeOnTransactionAuthorizationFailure() throws Exception {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final BeginTransactionResponse begin = beginTransaction(service);
      final String txId = begin.getTransactionId();
      try {
        final CapturingInsertStreamObserver resp = new CapturingInsertStreamObserver();
        final StreamObserver<InsertChunk> req = service.insertStream(resp);

        final InsertChunk chunk = InsertChunk.newBuilder()
            .setSessionId("issue-6795-auth")
            .setChunkSeq(0)
            .setLast(true)
            .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
            // Top-level chunk credentials, distinct from InsertOptions.credentials: resolveAuthorizedTransaction()
            // authorizes the external-transaction path off THIS field (see InsertChunk.credentials in the proto)
            // - deliberately wrong here, exactly the shape a caller guessing another user's transaction id, or
            // supplying a stale password, would hit.
            .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword("definitely-wrong-password").build())
            .setOptions(InsertOptions.newBuilder()
                .setDatabase(getDatabaseName())
                .setCredentials(credentials())
                .setTargetClass("Person")
                .build())
            .build();

        req.onNext(chunk);
        req.onCompleted();

        assertThat(resp.awaitTerminal()).as("timed out waiting for a terminal call").isTrue();
        assertThat(resp.errorRef.get())
            .as("a bad-credentials rejection must reach the client unmodified via onError, not be folded into a "
                + "\"successful\" InsertSummary")
            .isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) resp.errorRef.get()).getStatus().getCode())
            .isIn(Status.Code.PERMISSION_DENIED, Status.Code.UNAUTHENTICATED);
        assertThat(resp.summaryRef.get())
            .as("no InsertSummary should be delivered when the transaction-authorization check itself failed")
            .isNull();
      } finally {
        rollbackTransaction(service, txId);
      }
    } finally {
      service.close();
    }
  }

  /**
   * Minimal {@link ServerCallStreamObserver} double for {@code insertStream}'s response observer, recording
   * either the {@link InsertSummary} delivered via {@code onNext}/{@code onCompleted} or the {@link Throwable}
   * passed to {@code onError}, without throwing.
   */
  private static final class CapturingInsertStreamObserver extends ServerCallStreamObserver<InsertSummary> {
    final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
    final AtomicReference<Throwable>     errorRef   = new AtomicReference<>();
    private final CountDownLatch          terminalLatch = new CountDownLatch(1);

    @Override
    public void onNext(final InsertSummary value) {
      summaryRef.set(value);
    }

    @Override
    public void onError(final Throwable t) {
      errorRef.set(t);
      terminalLatch.countDown();
    }

    @Override
    public void onCompleted() {
      terminalLatch.countDown();
    }

    boolean awaitTerminal() throws InterruptedException {
      return terminalLatch.await(10, TimeUnit.SECONDS);
    }

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
