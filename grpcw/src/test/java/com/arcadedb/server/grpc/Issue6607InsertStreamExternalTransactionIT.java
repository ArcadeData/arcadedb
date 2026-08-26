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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6607: {@code insertStream} resolves an external transaction context on
 * the first chunk but never dispatches the actual row insertion onto that transaction's dedicated
 * executor thread the way every other
 * transaction-scoped RPC ({@code bulkInsert}, {@code createRecord}, {@code executeCommand}, ...) does.
 * Because ArcadeDB transactions are bound to the calling thread via the {@code DatabaseContext}
 * {@code ThreadLocal}, inserting on the wrong thread means the external transaction is not actually
 * active there, so the rows are written outside it (auto-committing independently) and survive a
 * rollback of the transaction the client believes wraps them.
 */
class Issue6607InsertStreamExternalTransactionIT extends BaseGraphServerTest {

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
   * Rows streamed under an external transaction must not survive a rollback of that transaction:
   * the whole point of passing a {@code transaction_id} is that the caller controls commit/rollback.
   */
  @Test
  void rowsStreamedUnderExternalTransactionDoNotSurviveRollback() throws Exception {
    final String typeName = "Issue6607ExtTxRollback_" + System.currentTimeMillis();
    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

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
      getServer(0).getDatabase(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  /**
   * Sanity companion to the rollback test above: rows streamed under an external transaction must still
   * commit normally when the client commits it, so the fix does not just make everything vanish.
   */
  @Test
  void rowsStreamedUnderExternalTransactionPersistOnCommit() throws Exception {
    final String typeName = "Issue6607ExtTxCommit_" + System.currentTimeMillis();
    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

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
      getServer(0).getDatabase(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

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

  /**
   * {@code bulkInsert}'s external-transaction path only had authorization-status coverage before this
   * PR; it never had a rollback/commit lifecycle test the way {@code insertStream} now does above.
   * Rows inserted via {@code bulkInsert} under an external transaction must not survive its rollback.
   */
  @Test
  void bulkInsertWithExternalTransactionRowsDoNotSurviveRollback() throws Exception {
    final String typeName = "Issue6607BulkExtTxRollback_" + System.currentTimeMillis();
    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

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
      getServer(0).getDatabase(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  /**
   * Sanity companion: {@code bulkInsert} rows under an external transaction must still commit normally.
   */
  @Test
  void bulkInsertWithExternalTransactionRowsPersistOnCommit() throws Exception {
    final String typeName = "Issue6607BulkExtTxCommit_" + System.currentTimeMillis();
    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

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
}
