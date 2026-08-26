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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.RejectedExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ArcadeDbGrpcService#requireTransactionStillActive} and
 * {@link ArcadeDbGrpcService#submitToActiveTransaction}, the guards added for issue #6709: every
 * transaction-scoped RPC resolves a {@code TransactionContext} and then dispatches its real work onto that
 * transaction's dedicated executor, but a concurrent commitTransaction/rollbackTransaction/idle-reap can
 * finalize the same transaction (removing it from the active-transaction map and shutting down its executor)
 * in the window between the resolve and the dispatched task actually running - or even before the task is
 * submitted at all, in which case {@code submit()} itself throws {@link RejectedExecutionException}.
 * These tests simulate both windows deterministically via
 * {@link ArcadeDbGrpcService#registerTransactionForTesting} / {@link ArcadeDbGrpcService#finalizeTransactionForTesting}
 * instead of racing real threads, per the issue's own suggested approach. No server or database is needed:
 * both guards only touch the in-memory active-transaction map and the executor's own shutdown state.
 */
class Issue6709TransactionFinalizeRaceTest {

  @Test
  @DisplayName("a transaction still registered passes revalidation")
  void stillActiveTransactionPassesRevalidation() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 0L, 0L, 0L);
    try {
      final var txCtx = service.registerTransactionForTesting("tx-still-active");

      assertThatCode(() -> service.requireTransactionStillActive(txCtx)).doesNotThrowAnyException();
    } finally {
      service.finalizeTransactionForTesting("tx-still-active");
      service.close();
    }
  }

  @Test
  @DisplayName("a transaction finalized by a concurrent commit/rollback/reap fails revalidation with FAILED_PRECONDITION")
  void finalizedTransactionFailsRevalidation() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 0L, 0L, 0L);
    try {
      final var txCtx = service.registerTransactionForTesting("tx-raced");

      // Simulates a commitTransaction/rollbackTransaction/idle-reap call winning the race and finalizing the
      // transaction between the RPC's resolve and its dispatched task actually running.
      service.finalizeTransactionForTesting("tx-raced");

      assertThatThrownBy(() -> service.requireTransactionStillActive(txCtx))
          .isInstanceOf(StatusRuntimeException.class)
          .extracting(e -> ((StatusRuntimeException) e).getStatus().getCode())
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
    } finally {
      service.close();
    }
  }

  @Test
  @DisplayName("a brand new transaction re-registered under the same id does not satisfy the stale context's revalidation")
  void reusedTransactionIdDoesNotMatchStaleContext() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 0L, 0L, 0L);
    try {
      final var staleTxCtx = service.registerTransactionForTesting("tx-reused");
      service.finalizeTransactionForTesting("tx-reused");

      // A new transaction happens to be registered under the same id afterwards. The identity check (not a
      // mere containsKey) must still reject the caller holding the old TransactionContext reference.
      final var freshTxCtx = service.registerTransactionForTesting("tx-reused");

      assertThatThrownBy(() -> service.requireTransactionStillActive(staleTxCtx))
          .isInstanceOf(StatusRuntimeException.class);
      assertThatCode(() -> service.requireTransactionStillActive(freshTxCtx)).doesNotThrowAnyException();
      assertThat(freshTxCtx).isNotSameAs(staleTxCtx);
    } finally {
      service.finalizeTransactionForTesting("tx-reused");
      service.close();
    }
  }

  @Test
  @DisplayName("submitToActiveTransaction converts a RejectedExecutionException from an already-shut-down "
      + "executor into FAILED_PRECONDITION")
  void submitToActiveTransactionConvertsRejectedExecutionAfterFinalize() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 0L, 0L, 0L);
    try {
      final var txCtx = service.registerTransactionForTesting("tx-shutdown-before-submit");

      // Finalizing also shuts down the executor, so a submit() attempted afterwards - the case where a
      // concurrent commit/rollback/idle-reap fully completes before this RPC even calls submit(), not just
      // before its task runs - throws RejectedExecutionException synchronously, before the task (and
      // requireTransactionStillActive) ever runs.
      service.finalizeTransactionForTesting("tx-shutdown-before-submit");

      assertThatThrownBy(() -> service.submitToActiveTransaction(txCtx, () -> "should never run"))
          .isInstanceOf(StatusRuntimeException.class)
          .extracting(e -> ((StatusRuntimeException) e).getStatus().getCode())
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
    } finally {
      service.close();
    }
  }

  @Test
  @DisplayName("submitToActiveTransaction runs the task and revalidates when the transaction is still active")
  void submitToActiveTransactionRunsTaskWhenStillActive() throws Exception {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 0L, 0L, 0L);
    try {
      final var txCtx = service.registerTransactionForTesting("tx-submit-ok");

      final String result = service.submitToActiveTransaction(txCtx, () -> "ran").get();

      assertThat(result).isEqualTo("ran");
    } finally {
      service.finalizeTransactionForTesting("tx-submit-ok");
      service.close();
    }
  }
}
