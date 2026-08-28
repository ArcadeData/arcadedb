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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6795 (follow-up on #6607, reported as a comment on the closed issue):
 * {@code insertStream}'s first-chunk call to {@code resolveAuthorizedTransaction(...)} sat inside the chunk
 * handler's folding {@code try/catch}, so a {@code StatusRuntimeException} it throws (e.g.
 * {@code PERMISSION_DENIED} from {@code authorizeTransactionAccess} when the caller is not the transaction's
 * owner) was caught by the generic per-chunk handler and downgraded into a structured error entry inside an
 * otherwise "successful" {@link InsertSummary} delivered via {@code onCompleted} - instead of reaching the
 * client as the real gRPC status via {@code onError}, the way {@code bulkInsert} already did for the same
 * failure (see {@code Issue6607InsertStreamExternalTransactionIT.bulkInsertWithExternalTransactionBadCredentialsPreservesRealStatusCode}).
 */
class Issue6795InsertStreamAuthFailureNotDowngradedIT extends BaseGraphServerTest {

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  @Test
  void insertStreamPreservesRealStatusCodeOnTransactionAuthorizationFailure() throws Exception {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final BeginTransactionResponse begin = beginTransaction(service);
      final String txId = begin.getTransactionId();
      try {
        final CapturingInsertSummaryObserver resp = new CapturingInsertSummaryObserver();
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

  private BeginTransactionResponse beginTransaction(final ArcadeDbGrpcService service) {
    final AtomicReference<BeginTransactionResponse> ref = new AtomicReference<>();
    service.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build(),
        capturing(ref));
    return ref.get();
  }

  private void rollbackTransaction(final ArcadeDbGrpcService service, final String txId) {
    final AtomicReference<RollbackTransactionResponse> ref = new AtomicReference<>();
    service.rollbackTransaction(
        RollbackTransactionRequest.newBuilder()
            .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
            .setCredentials(credentials())
            .build(),
        capturing(ref));
  }

  private <T> StreamObserver<T> capturing(final AtomicReference<T> ref) {
    return new StreamObserver<>() {
      @Override public void onNext(final T value) { ref.set(value); }
      @Override public void onError(final Throwable t) { throw new RuntimeException(t); }
      @Override public void onCompleted() { }
    };
  }

  /**
   * Minimal {@link ServerCallStreamObserver} double for {@code insertStream}'s response observer, recording
   * either the {@link InsertSummary} delivered via {@code onNext}/{@code onCompleted} or the {@link Throwable}
   * passed to {@code onError}, without throwing.
   */
  private static final class CapturingInsertSummaryObserver extends ServerCallStreamObserver<InsertSummary> {
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
