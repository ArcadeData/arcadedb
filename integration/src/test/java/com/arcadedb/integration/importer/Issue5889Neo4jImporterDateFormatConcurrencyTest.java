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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5889: {@code Neo4jImporter} held a static {@code SimpleDateFormat}
 * (not thread-safe) used both to probe whether a Neo4j property string is a date
 * ({@code inferPropertyType}) and to parse it into an epoch millisecond value
 * ({@code setProperties}). The issue flagged this as latent (no concurrent caller today) but one
 * {@code parallel()} call away from the same corruption as the reachable exporter bug.
 * <p>
 * {@code dateTimeISO8601Format} is now a {@code DateTimeFormatter}, which is immutable and
 * thread-safe. This test drives many threads through the shared static field concurrently, each
 * parsing its own distinct ISO-8601-ish string, and checks that every parsed value matches an
 * independently computed reference (no cross-thread corruption) and that no exception is thrown.
 */
class Issue5889Neo4jImporterDateFormatConcurrencyTest {

  private static final int THREADS    = 16;
  private static final int ITERATIONS = 5_000;

  @Test
  void concurrentParseDoesNotCorruptOrThrow() throws Exception {
    final DateTimeFormatter reference = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    final CountDownLatch                  start      = new CountDownLatch(1);
    final CountDownLatch                  done       = new CountDownLatch(THREADS);
    final CopyOnWriteArrayList<String>    failures   = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<Throwable> exceptions = new CopyOnWriteArrayList<>();

    for (int t = 0; t < THREADS; ++t) {
      final int threadIndex = t;

      final Thread thread = new Thread(() -> {
        try {
          start.await();

          for (int i = 0; i < ITERATIONS; ++i) {
            final LocalDateTime base = LocalDateTime.of(2020, 1, 1, 0, 0, 0)
                .plusDays(threadIndex).plusSeconds(i);
            final String text = reference.format(base);

            final LocalDateTime expected = LocalDateTime.parse(text, reference);
            final LocalDateTime actual   = LocalDateTime.parse(text, Neo4jImporter.dateTimeISO8601Format);

            if (!expected.equals(actual))
              failures.add("expected '" + expected + "' but got '" + actual + "' for input '" + text + "'");
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

    assertThat(exceptions).as("no exception thrown while parsing concurrently").isEmpty();
    assertThat(failures).as("no cross-thread corruption while parsing concurrently").isEmpty();
  }
}
