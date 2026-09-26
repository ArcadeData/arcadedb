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
package com.arcadedb.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every decorator this class builds is released by whatever displaces it, however the builds and the
 * installs interleave.
 *
 * <p>Ownership used to be inferred from a single slot holding the decorator built most recently, which
 * is only right if every build is followed straight away by its install. Nothing enforces that:
 * {@link LogManager#createLogger(String)} is public, so a caller can build ahead, and two threads
 * changing the configuration build concurrently. A decorator that loses that slot stops being
 * recognised as ours and is never closed, leaving its batch thread and its connection up for the life
 * of the JVM.
 *
 * <p>In package {@code com.arcadedb.log} for the protected constructor, which keeps this off the
 * process-wide singleton, and in this module because it needs a real exporter on the classpath - the
 * engine ships none, and {@code LogExportSelectionTest} asserts that absence.
 */
class LogExporterOwnershipTest {

  /** A port nothing answers on: this is about lifecycle, not delivery. */
  private static final String ENDPOINT = "http://localhost:1";

  /** Every decorator built here, so nothing is left with a live connection when a test fails. */
  private final List<Logger> built = Collections.synchronizedList(new ArrayList<>());

  @AfterEach
  void releaseWhatIsLeftAndRestoreDefaults() {
    built.forEach(LogExporterOwnershipTest::closeQuietly);
    built.clear();
    System.clearProperty(LogManager.LOG_EXPORT_PROPERTY);
    System.clearProperty(LogManager.LOG_EXPORT_ENDPOINT_PROPERTY);
  }

  /**
   * Builds a decorator through the ordinary path and remembers it.
   *
   * @return the decorator
   */
  private Logger build() {
    final Logger decorator = LogManager.createLogger("default");
    built.add(decorator);
    return decorator;
  }

  @Test
  void aDecoratorInstalledAfterAnotherWasBuiltIsStillReleased() {
    // No threads needed: building mutates shared state, so one caller building ahead reproduces what
    // two overlapping configuration changes produce.
    exportEnabled();
    final LogManager manager = new LogManager();

    final Logger first = build();
    manager.setLogger(first);

    final Logger second = build();
    final Logger third = build();

    manager.setLogger(second);
    assertThat(closed(first)).as("the install that displaces one releases it").isTrue();

    manager.setLogger(third);
    assertThat(closed(second))
        .as("a decorator installed after a later one was built is still ours, and still released")
        .isTrue();
    assertThat(closed(third)).as("the installed one keeps working").isFalse();
  }

  @Test
  void concurrentRebuildsReleaseEveryDecoratorTheyDisplace() throws Exception {
    // A stress check, not the guard. Measured: reinstating the non-atomic read-modify-write in the
    // install does NOT make this fail, at these parameters or at 16 threads x 25 rounds, three runs
    // each. The window is one field read and one field write, and nothing here can widen it without a
    // seam in production code. What this does pin is that a decorator displaced under concurrent
    // rebuilds is released - the part that reproduces, and that
    // aDecoratorInstalledAfterAnotherWasBuiltIsStillReleased covers deterministically.
    exportEnabled();
    final LogManager manager = new LogManager();
    final int threads = 4;
    final int rounds = 15;
    final CyclicBarrier start = new CyclicBarrier(threads);
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      final List<Future<?>> running = new ArrayList<>();
      for (int t = 0; t < threads; t++)
        running.add(pool.submit(() -> {
          start.await();
          for (int i = 0; i < rounds; i++)
            manager.setLogger(build());
          return null;
        }));
      for (final Future<?> task : running)
        task.get(60, TimeUnit.SECONDS);

      // Not a vacuous assertion: every build has to have been decorated, or the rest proves nothing.
      assertThat(built).hasSize(threads * rounds)
          .allSatisfy(logger -> assertThat(logger).isInstanceOf(AutoCloseable.class));

      final Logger installed = manager.getLogger();
      manager.setLogger(new DefaultLogger());

      assertThat(built.stream().filter(logger -> !closed(logger)).toList())
          .as("every decorator built here was released by whatever displaced it")
          .isEmpty();
      assertThat(closed(installed)).as("including the last one, once an ordinary logger takes over").isTrue();
    } finally {
      pool.shutdownNow();
    }
  }

  /** Turns export on for the duration of one test. */
  private static void exportEnabled() {
    System.setProperty(LogManager.LOG_EXPORT_PROPERTY, "true");
    System.setProperty(LogManager.LOG_EXPORT_ENDPOINT_PROPERTY, ENDPOINT);
  }

  /**
   * Whether a decorator has been closed.
   *
   * <p>Read reflectively: {@code OtlpLogger.isClosed()} is package-private API of another package, and
   * this test only needs to know whether the exporter was released.
   *
   * @param logger the decorator to inspect
   *
   * @return whether it has been closed
   */
  static boolean closed(final Logger logger) {
    try {
      final var method = logger.getClass().getDeclaredMethod("isClosed");
      method.setAccessible(true);
      return (boolean) method.invoke(logger);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException("cannot read the exporter's closed state", e);
    }
  }

  /** Closes a decorator without letting teardown fail on it. */
  private static void closeQuietly(final Logger logger) {
    if (logger instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (final Exception ignored) {
        // teardown
      }
    }
  }
}
