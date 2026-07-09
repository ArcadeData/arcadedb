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
package com.arcadedb.server.event;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5033: the event-log timestamp formatter must be thread-safe.
 * The previous implementation shared a single {@link java.text.SimpleDateFormat} instance across all
 * {@code reportEvent()} callers (HTTP handler threads, the server monitor, backup tasks, HA code, ...).
 * {@code SimpleDateFormat} is not thread-safe, so concurrent formatting corrupted its internal
 * {@code Calendar} - producing garbage timestamps or throwing {@code ArrayIndexOutOfBoundsException}
 * out of {@code reportEvent}.
 */
class FileServerEventLogConcurrencyTest {

  @Test
  void concurrentTimestampFormattingIsThreadSafe() throws InterruptedException {
    // No server is needed: formatEventTime() only touches the (previously shared) formatter.
    final FileServerEventLog eventLog = new FileServerEventLog(null);

    final int threads = 32;
    final int iterationsPerThread = 20_000;

    // Each thread formats its OWN distinct instant. With a shared, non-thread-safe formatter the
    // internal Calendar set by one thread bleeds into another thread's field reads, so the output no
    // longer matches the instant that thread asked for. Each thread's expected value is computed
    // up-front in a single-threaded context (so it is correct) and compared against the concurrent one.
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final ConcurrentLinkedQueue<String> mismatches = new ConcurrentLinkedQueue<>();

    for (int t = 0; t < threads; t++) {
      // Distinct instants that differ in every field (year, month, day, time, millis).
      final long epochMillis = 1_000_000_000_000L + t * 86_401_137L;
      final String expected = eventLog.formatEventTime(epochMillis);
      assertThat(expected).matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}");

      final Thread worker = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < iterationsPerThread; i++) {
            final String formatted = eventLog.formatEventTime(epochMillis);
            if (!expected.equals(formatted))
              mismatches.add(expected + " != " + formatted);
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        } finally {
          done.countDown();
        }
      }, "event-log-worker-" + t);
      worker.start();
    }

    start.countDown();
    assertThat(done.await(60, TimeUnit.SECONDS)).as("all workers finished").isTrue();

    assertThat(failure.get()).as("no exception thrown while formatting").isNull();
    assertThat(mismatches).as("every formatted timestamp matches the instant it was asked for").isEmpty();
  }
}
