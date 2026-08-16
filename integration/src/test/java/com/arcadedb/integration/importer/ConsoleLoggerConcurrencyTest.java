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
package com.arcadedb.integration.importer;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code LogListener} is a one-method interface, written once and called from wherever the logger happens to be
 * used - and since the parallel restore of issue #6086 that is several threads at a time. The listeners in this tree
 * write to streams that are not safe for concurrent use (the server's SSE progress channel writes straight to the
 * exchange's output stream), so two overlapping callbacks would produce a corrupt event rather than merely
 * out-of-order ones. The guarantee has to live in the logger, which is what this pins.
 */
class ConsoleLoggerConcurrencyTest {
  @Test
  void listenerCallbacksNeverOverlap() throws Exception {
    final AtomicInteger inside = new AtomicInteger();
    final AtomicInteger overlaps = new AtomicInteger();
    final AtomicInteger delivered = new AtomicInteger();

    final ConsoleLogger logger = new ConsoleLogger(0, message -> {
      if (inside.incrementAndGet() > 1)
        overlaps.incrementAndGet();
      // WIDE ENOUGH THAT AN UNSERIALIZED SECOND CALLER WOULD BE SEEN, NOT MERELY POSSIBLE IN THEORY
      try {
        Thread.sleep(1);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      delivered.incrementAndGet();
      inside.decrementAndGet();
    });

    final int threads = 8;
    final int linesPerThread = 20;
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);

    for (int t = 0; t < threads; t++) {
      final int id = t;
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < linesPerThread; i++)
            logger.logLine(0, "thread %d line %d", id, i);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      }, "ConsoleLoggerConcurrencyTest-" + t);
      thread.setDaemon(true);
      thread.start();
    }

    start.countDown();
    assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();

    assertThat(delivered.get()).isEqualTo(threads * linesPerThread);
    assertThat(overlaps.get()).as("two threads were inside the listener at once").isZero();
  }
}
