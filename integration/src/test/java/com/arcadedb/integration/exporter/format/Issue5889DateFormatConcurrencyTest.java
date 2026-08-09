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
package com.arcadedb.integration.exporter.format;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5889: {@code AbstractExporterFormat} (and its {@code AbstractBackupFormat} /
 * {@code AbstractRestoreFormat} siblings) held a <b>static</b> {@code SimpleDateFormat}, which is not
 * thread-safe, used unsynchronised by {@code JsonlExporterFormat} to timestamp the "db" line written by
 * {@code EXPORT DATABASE}. Two concurrent exports (or two threads issuing {@code EXPORT DATABASE} on the
 * server) could corrupt each other's timestamp or throw.
 * <p>
 * {@code dateFormat} is now a {@code DateTimeFormatter}, which is immutable and thread-safe. This test
 * drives many threads through the shared static field concurrently, each formatting its own distinct
 * instant, and checks that every formatted value matches an independently computed reference (no
 * cross-thread corruption) and that no exception is thrown.
 */
class Issue5889DateFormatConcurrencyTest {

  private static final int THREADS    = 16;
  private static final int ITERATIONS = 5_000;

  @Test
  void concurrentFormatDoesNotCorruptOrThrow() throws Exception {
    final DateTimeFormatter reference = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    final CountDownLatch                  start      = new CountDownLatch(1);
    final CountDownLatch                  done       = new CountDownLatch(THREADS);
    final CopyOnWriteArrayList<String>    failures   = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<Throwable> exceptions = new CopyOnWriteArrayList<>();

    for (int t = 0; t < THREADS; ++t) {
      final long baseMillis = System.currentTimeMillis() + (t * 86_400_000L);

      final Thread thread = new Thread(() -> {
        try {
          start.await();

          for (int i = 0; i < ITERATIONS; ++i) {
            final Instant instant  = Instant.ofEpochMilli(baseMillis + i);
            final String  expected = reference.format(instant);
            final String  actual   = AbstractExporterFormat.dateFormat.format(instant);

            if (!expected.equals(actual))
              failures.add("expected '" + expected + "' but got '" + actual + "'");
          }
        } catch (final Throwable e) {
          exceptions.add(e);
        } finally {
          done.countDown();
        }
      });
      thread.start();
    }

    start.countDown();
    assertThat(done.await(60, TimeUnit.SECONDS)).as("all threads completed in time").isTrue();

    assertThat(exceptions).as("no exception thrown while formatting concurrently").isEmpty();
    assertThat(failures).as("no cross-thread corruption while formatting concurrently").isEmpty();
  }
}
