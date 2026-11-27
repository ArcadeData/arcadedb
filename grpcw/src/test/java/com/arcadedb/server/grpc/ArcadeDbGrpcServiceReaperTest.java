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

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for the idle-transaction reaper configuration wiring of {@link ArcadeDbGrpcService} (issue #4802).
 * No server is required: the constructor only decides whether to start the reaper thread based on the thresholds.
 */
class ArcadeDbGrpcServiceReaperTest {

  @Test
  void reaperDisabledWhenBothBoundsNonPositive() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 0L, 0L, 30_000L);
    try {
      assertThat(service.isIdleReaperActive()).isFalse();
      assertThat(service.getActiveTransactionCount()).isZero();
    } finally {
      service.close();
    }
  }

  @Test
  void reaperDisabledWhenPeriodNonPositive() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 1_000L, 1_000L, 0L);
    try {
      assertThat(service.isIdleReaperActive()).isFalse();
    } finally {
      service.close();
    }
  }

  @Test
  void reaperEnabledWithIdleBoundOnly() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 1_000L, 0L, 500L);
    try {
      assertThat(service.isIdleReaperActive()).isTrue();
    } finally {
      service.close();
    }
  }

  @Test
  void reaperEnabledWithAgeBoundOnly() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 0L, 1_000L, 500L);
    try {
      assertThat(service.isIdleReaperActive()).isTrue();
    } finally {
      service.close();
    }
  }

  @Test
  void defaultConstructorEnablesReaper() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null);
    try {
      assertThat(service.isIdleReaperActive()).isTrue();
    } finally {
      service.close();
    }
  }

  // Regression: #13253
  @Test
  void transactionWithARunningStatementIsNotIdleHoweverOldItsLastAccess() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 0L, 0L, 0L);
    try {
      final var txCtx = service.registerTransactionForTesting("tx-running-statement");
      final long now = System.currentTimeMillis();
      txCtx.lastAccessMs = now - 10_000L;
      assertThat(txCtx.isIdleFor(1_000L, now)).isTrue();

      txCtx.runningStatements.incrementAndGet();
      assertThat(txCtx.isIdleFor(1_000L, now)).as("a statement is running in it").isFalse();

      txCtx.runningStatements.decrementAndGet();
      assertThat(txCtx.isIdleFor(1_000L, now)).isTrue();
      assertThat(txCtx.isIdleFor(20_000L, now)).as("the last use is more recent than the idle window").isFalse();
    } finally {
      service.finalizeTransactionForTesting("tx-running-statement");
      service.close();
    }
  }

  // Regression: #13253. No sleep-vs-idle-window race: the last access is pinned in the past while the statement runs, so
  // only the running statement keeps the transaction from the reaper, whatever the machine's load.
  @Test
  void reaperLeavesTheTransactionAloneWhileAStatementRunsAndReclaimsItOnceIdleAgain() throws Exception {
    final long maxIdleMs = 50L;
    final long reaperPeriodMs = 10L;
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, maxIdleMs, 0L, reaperPeriodMs);
    final CountDownLatch statementStarted = new CountDownLatch(1);
    final CountDownLatch statementMayEnd = new CountDownLatch(1);
    try {
      final var txCtx = service.registerTransactionForTesting("tx-long-statement");
      // not idle until the statement is known to be running
      txCtx.lastAccessMs = System.currentTimeMillis() + 3_600_000L;

      final Future<String> statement = service.submitToActiveTransaction(txCtx, () -> {
        statementStarted.countDown();
        statementMayEnd.await();
        return "done";
      });
      assertThat(statementStarted.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(txCtx.runningStatements.get()).isEqualTo(1);

      // as old as it gets: on its last access alone the very next sweep reclaims the transaction
      txCtx.lastAccessMs = 0L;
      // many sweeps go by
      Thread.sleep(20 * reaperPeriodMs);
      assertThat(service.getActiveTransactionForTesting("tx-long-statement")).as("reaped under its running statement")
          .isSameAs(txCtx);

      statementMayEnd.countDown();
      assertThat(statement.get(30, TimeUnit.SECONDS)).isEqualTo("done");
      assertThat(txCtx.runningStatements.get()).isZero();
      assertThat(txCtx.lastAccessMs).as("the idle window counts from the end of the statement").isGreaterThan(0L);

      // nobody uses it any more: the reaper takes it once the idle window has gone by
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (service.getActiveTransactionCount() > 0 && System.nanoTime() < deadline)
        Thread.sleep(reaperPeriodMs);
      assertThat(service.getActiveTransactionCount()).as("an idle transaction is still reclaimed").isZero();
    } finally {
      statementMayEnd.countDown();
      service.finalizeTransactionForTesting("tx-long-statement");
      service.close();
    }
  }
}
