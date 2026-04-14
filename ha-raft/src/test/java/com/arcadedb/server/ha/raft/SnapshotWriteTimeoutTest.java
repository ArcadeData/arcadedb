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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the server-side snapshot write timeout mechanism.
 * Verifies that when a snapshot transfer stalls, the watchdog fires and the semaphore
 * slot is released so future snapshot requests are not permanently blocked.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotWriteTimeoutTest {

  /**
   * Simulates the watchdog pattern used in SnapshotHttpHandler:
   * a scheduled task fires after the timeout and signals the blocked thread,
   * which then releases the semaphore in its finally block.
   */
  @Test
  void watchdogReleasesSemaphoreWhenTransferStalls() throws Exception {
    final int maxConcurrent = 2;
    final Semaphore semaphore = new Semaphore(maxConcurrent);
    final int writeTimeoutMs = 200; // Short timeout for test
    final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "test-watchdog");
      t.setDaemon(true);
      return t;
    });

    final AtomicBoolean watchdogFired = new AtomicBoolean(false);
    final CountDownLatch transferDone = new CountDownLatch(1);

    // Simulate a stalling snapshot transfer in a separate thread
    final Thread transferThread = new Thread(() -> {
      assertThat(semaphore.tryAcquire()).isTrue();
      ScheduledFuture<?> timer = null;
      final Thread current = Thread.currentThread();
      try {
        timer = watchdog.schedule(() -> {
          watchdogFired.set(true);
          // In real code this closes the connection; here we interrupt the blocked thread
          current.interrupt();
        }, writeTimeoutMs, TimeUnit.MILLISECONDS);

        // Simulate a stalling write (blocks until interrupted)
        Thread.sleep(60_000);

      } catch (final InterruptedException ignored) {
        // Expected: watchdog interrupted us
      } finally {
        if (timer != null)
          timer.cancel(false);
        semaphore.release();
        transferDone.countDown();
      }
    });
    transferThread.start();

    // Wait for the watchdog to fire and the transfer to clean up
    assertThat(transferDone.await(5, TimeUnit.SECONDS))
        .as("Transfer thread should have been interrupted by watchdog").isTrue();

    assertThat(watchdogFired.get()).as("Watchdog should have fired").isTrue();
    assertThat(semaphore.availablePermits())
        .as("Semaphore should be fully released after watchdog fires").isEqualTo(maxConcurrent);

    watchdog.shutdownNow();
  }

  /**
   * Verifies that on a successful (fast) transfer the watchdog is cancelled
   * and the semaphore is released normally.
   */
  @Test
  void successfulTransferCancelsWatchdogAndReleasesSemaphore() throws Exception {
    final int maxConcurrent = 2;
    final Semaphore semaphore = new Semaphore(maxConcurrent);
    final int writeTimeoutMs = 5_000; // Long timeout - should not fire
    final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "test-watchdog");
      t.setDaemon(true);
      return t;
    });

    final AtomicBoolean watchdogFired = new AtomicBoolean(false);

    assertThat(semaphore.tryAcquire()).isTrue();
    ScheduledFuture<?> timer = null;
    try {
      timer = watchdog.schedule(() -> watchdogFired.set(true), writeTimeoutMs, TimeUnit.MILLISECONDS);

      // Simulate a fast transfer (no blocking)
      Thread.sleep(50);
    } finally {
      if (timer != null)
        timer.cancel(false);
      semaphore.release();
    }

    assertThat(watchdogFired.get()).as("Watchdog should NOT have fired for a fast transfer").isFalse();
    assertThat(semaphore.availablePermits()).isEqualTo(maxConcurrent);

    watchdog.shutdownNow();
  }

  @Test
  void writeTimeoutConfigHasCorrectDefault() {
    assertThat(GlobalConfiguration.HA_SNAPSHOT_WRITE_TIMEOUT.getValueAsInteger()).isEqualTo(300_000);
  }
}
