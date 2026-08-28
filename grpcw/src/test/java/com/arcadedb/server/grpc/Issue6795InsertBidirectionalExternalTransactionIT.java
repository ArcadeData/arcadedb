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

import com.arcadedb.server.BaseGraphServerTest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6795 (follow-up on #6607, reported as a comment on the closed issue):
 * {@code insertBidirectional}'s {@code START} handler never read {@code Start.transaction} - it always built
 * its {@code InsertContext} with {@code new InsertContext(opts)}, which begins and owns its OWN transaction,
 * exactly the bug {@code insertStream}/{@code bulkInsert} were fixed for in #6607. Rows streamed under a
 * {@code transaction_id} therefore landed in a fresh internal transaction the stream itself committed on
 * {@code COMMIT}/half-close, never joining the caller's transaction: a client rollback of that transaction
 * could not undo them.
 * <p>
 * Same shape as {@link Issue6607InsertStreamExternalTransactionIT}, driving the START/CHUNK/COMMIT
 * request/response protocol {@code insertBidirectional} uses instead of {@code insertStream}'s flatter one.
 */
class Issue6795InsertBidirectionalExternalTransactionIT extends BaseGraphServerTest {

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private GrpcValue stringValue(final String s) {
    return GrpcValue.newBuilder().setStringValue(s).build();
  }

  private long countRows(final String typeName) {
    return getServer(0).getDatabase(getDatabaseName())
        .query("sql", "SELECT count(*) AS total FROM " + typeName).next().<Long>getProperty("total");
  }

  /**
   * Rows sent via {@code insertBidirectional} under an external transaction must not survive a rollback of
   * that transaction: the whole point of passing {@code Start.transaction} is that the caller controls
   * commit/rollback, not the stream's own {@code COMMIT} message.
   */
  @Test
  void rowsSentViaInsertBidirectionalUnderExternalTransactionDoNotSurviveRollback() throws Exception {
    final String typeName = "Issue6795BidiExtTxRollback_" + System.currentTimeMillis();
    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

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
      getServer(0).getDatabase(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  /**
   * Sanity companion: rows sent via {@code insertBidirectional} under an external transaction must still
   * commit normally when the client commits it, so the fix does not just make everything vanish.
   */
  @Test
  void rowsSentViaInsertBidirectionalUnderExternalTransactionPersistOnCommit() throws Exception {
    final String typeName = "Issue6795BidiExtTxCommit_" + System.currentTimeMillis();
    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

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
      getServer(0).getDatabase(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
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
}
