/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.server.ha.raft.SnapshotHttpHandler.ProgressTrackingOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Regression tests for issue #4729: the leader's snapshot write watchdog must distinguish a *stalled*
 * transfer (no progress within HA_SNAPSHOT_WRITE_TIMEOUT) from a slow-but-healthy one. The historical
 * implementation force-closed the connection on an absolute deadline, killing large snapshot transfers
 * that legitimately took longer than the timeout to stream and producing a "Premature EOF" resync loop
 * on the follower.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotWriteStallTest {

  private ScheduledExecutorService executor;

  @BeforeEach
  void setUp() {
    executor = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "test-snapshot-watchdog");
      t.setDaemon(true);
      return t;
    });
  }

  @AfterEach
  void tearDown() {
    executor.shutdownNow();
  }

  @Test
  void stallDecisionUsesIdleTimeNotAbsoluteDuration() {
    final long now = 1_000_000L;
    final long timeoutMs = 300_000L;

    // Progress made well within the window: not stalled, even if the transfer started long ago.
    assertThat(SnapshotHttpHandler.isSnapshotWriteStalled(now - 1_000L, now, timeoutMs)).isFalse();
    // Last byte written exactly one timeout ago: stalled.
    assertThat(SnapshotHttpHandler.isSnapshotWriteStalled(now - timeoutMs, now, timeoutMs)).isTrue();
    // Idle longer than the timeout: stalled.
    assertThat(SnapshotHttpHandler.isSnapshotWriteStalled(now - (timeoutMs + 1L), now, timeoutMs)).isTrue();
  }

  @Test
  void progressTrackingStreamPassesBytesThroughAndRecordsProgress() throws Exception {
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    final AtomicLong lastProgressMs = new AtomicLong(0L);

    try (final OutputStream out = new ProgressTrackingOutputStream(sink, lastProgressMs)) {
      out.write('A');
      assertThat(lastProgressMs.get()).isGreaterThan(0L);
      final long afterFirst = lastProgressMs.get();

      out.write(new byte[] { 'B', 'C', 'D' }, 0, 3);
      assertThat(lastProgressMs.get()).isGreaterThanOrEqualTo(afterFirst);
    }

    assertThat(sink.toByteArray()).containsExactly('A', 'B', 'C', 'D');
  }

  /**
   * Core regression: a transfer that takes far longer than the write timeout in TOTAL but keeps writing
   * chunks must never be declared stalled. With the old absolute-deadline watchdog this connection would
   * have been force-closed at the timeout; the stall watchdog leaves it alone.
   */
  @Test
  void slowButProgressingTransferIsNotClosed() {
    final long timeoutMs = 200L;
    final AtomicBoolean completed = new AtomicBoolean(false);
    final AtomicLong lastProgressMs = new AtomicLong(System.currentTimeMillis());
    final AtomicInteger closeCalls = new AtomicInteger(0);

    final ScheduledFuture<?> watchdog = SnapshotHttpHandler.scheduleStallWatchdog(executor, completed,
        lastProgressMs, timeoutMs, 20L, "slow-db", closeCalls::incrementAndGet);
    try {
      // Simulate ~1s of transfer (5x the timeout) writing a chunk every 50ms: progress keeps advancing.
      for (int i = 0; i < 20; i++) {
        lastProgressMs.set(System.currentTimeMillis());
        sleep(50L);
        assertThat(closeCalls.get()).as("must not force-close a progressing transfer (iteration %d)", i).isZero();
      }
    } finally {
      completed.set(true);
      watchdog.cancel(false);
    }
    assertThat(closeCalls.get()).isZero();
  }

  /**
   * Complementary case: once progress genuinely halts for longer than the timeout, the watchdog DOES
   * force-close the connection so the leader's concurrency semaphore slot is released.
   */
  @Test
  void stalledTransferIsClosed() {
    final long timeoutMs = 200L;
    final AtomicBoolean completed = new AtomicBoolean(false);
    final AtomicLong lastProgressMs = new AtomicLong(System.currentTimeMillis());
    final AtomicInteger closeCalls = new AtomicInteger(0);

    final ScheduledFuture<?> watchdog = SnapshotHttpHandler.scheduleStallWatchdog(executor, completed,
        lastProgressMs, timeoutMs, 20L, "stalled-db", closeCalls::incrementAndGet);
    try {
      // No further progress updates: the watchdog should fire once the idle window elapses.
      await().atMost(2, TimeUnit.SECONDS).until(() -> closeCalls.get() > 0);
    } finally {
      completed.set(true);
      watchdog.cancel(false);
    }
    assertThat(closeCalls.get()).isGreaterThan(0);
  }

  private static void sleep(final long ms) {
    try {
      Thread.sleep(ms);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
