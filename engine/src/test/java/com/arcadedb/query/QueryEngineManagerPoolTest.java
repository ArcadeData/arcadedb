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
package com.arcadedb.query;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke tests for the {@link QueryEngineManager} pool's bounded queue + caller-runs fallback.
 * The pool's behaviour matters operationally (it backs every parallel query), so these tests
 * pin the contract independently of the rest of the engine. The pool itself is a JVM-wide
 * singleton, so the tests target observable behaviour rather than reconfiguring the singleton.
 */
class QueryEngineManagerPoolTest {

  /** {@link QueryEngineManager.PoolStats} fields are non-negative and self-consistent at rest. */
  @Test
  void poolStatsExposeSensibleValues() {
    final QueryEngineManager.PoolStats stats = QueryEngineManager.getInstance().getExecutorStats();
    assertThat(stats.poolSize()).as("poolSize").isGreaterThanOrEqualTo(0);
    assertThat(stats.activeThreads()).as("activeThreads").isGreaterThanOrEqualTo(0);
    assertThat(stats.queueDepth()).as("queueDepth").isGreaterThanOrEqualTo(0);
    assertThat(stats.queueCapacityRemaining()).as("queueCapacityRemaining").isGreaterThanOrEqualTo(0);
    assertThat(stats.completedTasks()).as("completedTasks").isGreaterThanOrEqualTo(0L);
    assertThat(stats.callerRunFallbacks()).as("callerRunFallbacks").isGreaterThanOrEqualTo(0L);
  }

  /**
   * Submitted tasks run on pool threads, not the caller. Verifies the basic pool wiring is
   * intact after the bounded-queue refactor.
   */
  @Test
  void submittedTasksRunOnPoolThread() throws Exception {
    final ExecutorService executor = QueryEngineManager.getInstance().getExecutorService();
    final long callerThreadId = Thread.currentThread().threadId();
    final Future<Long> f = executor.submit(() -> Thread.currentThread().threadId());
    final long workerThreadId = f.get(5, TimeUnit.SECONDS);
    assertThat(workerThreadId).as("a normal submit should not run on the caller's thread")
        .isNotEqualTo(callerThreadId);
  }

  /**
   * When the queue saturates, the caller-runs rejection policy executes the new task on the
   * submitter's thread instead of throwing. Bumps {@link QueryEngineManager.PoolStats#callerRunFallbacks}
   * for every fallback so dashboards can detect saturation.
   * <p>
   * Construction: hold every worker thread blocked on a latch, fill the queue with placeholder
   * tasks, then submit one extra. The extra task must run on the test thread (caller-runs) and
   * the {@code callerRunFallbacks} counter must tick by exactly one.
   */
  @Test
  void callerRunsFallbackTicksWhenQueueSaturates() throws Exception {
    final ExecutorService executor = QueryEngineManager.getInstance().getExecutorService();
    final QueryEngineManager.PoolStats before = QueryEngineManager.getInstance().getExecutorStats();
    final int poolSize = Math.max(2, Runtime.getRuntime().availableProcessors());
    final int queueCapacity = before.queueCapacityRemaining() + before.queueDepth();
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch allWorkersBusy = new CountDownLatch(poolSize);

    // Pin every worker thread on a latch so the queue actually fills.
    for (int i = 0; i < poolSize; i++) {
      executor.submit(() -> {
        try {
          allWorkersBusy.countDown();
          release.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    }
    assertThat(allWorkersBusy.await(5, TimeUnit.SECONDS))
        .as("all workers should pick up the blocking task within 5s").isTrue();

    // Fill the queue. Each task is a no-op runnable; they remain queued until release fires.
    for (int i = 0; i < queueCapacity; i++)
      executor.submit(() -> { /* no-op */ });

    try {
      // The next submit must trigger caller-runs. Use a sentinel that records its execution
      // thread so we can assert it ran on this thread.
      final long callerThreadId = Thread.currentThread().threadId();
      final long[] runOn = new long[] { -1L };
      executor.execute(() -> runOn[0] = Thread.currentThread().threadId());
      assertThat(runOn[0]).as("caller-runs fallback must execute the task on the submitter's thread")
          .isEqualTo(callerThreadId);

      final QueryEngineManager.PoolStats after = QueryEngineManager.getInstance().getExecutorStats();
      assertThat(after.callerRunFallbacks() - before.callerRunFallbacks())
          .as("callerRunFallbacks counter must tick by exactly one")
          .isEqualTo(1L);
    } finally {
      release.countDown();
    }
  }

  /**
   * Saturation is operator-visible. The pool emits a throttled WARNING log line on the first
   * caller-runs fallback in each interval (default 1 minute), so the operator sees a clear
   * signal in the server console without spam if the queue saturates briefly. The metric
   * counter still tallies every fallback for dashboards. This test:
   * <ol>
   *   <li>Resets the throttle's last-warn timestamp via reflection so a previous test in the
   *       same JVM cannot suppress the WARNING we want to observe.</li>
   *   <li>Swaps in a capture logger via {@link com.arcadedb.log.LogManager#setLogger} so we
   *       see the WARNING regardless of how the production logger is configured.</li>
   *   <li>Drives a saturation event and asserts the WARNING text is present.</li>
   *   <li>Asserts the throttle prevents a second WARNING for additional fallbacks in the same
   *       interval, while the metric counter keeps ticking.</li>
   * </ol>
   */
  @Test
  void saturationLogsThrottledWarning() throws Exception {
    final ExecutorService executor = QueryEngineManager.getInstance().getExecutorService();

    // Reset the throttle so this test does not depend on test ordering.
    final java.lang.reflect.Field throttleField =
        QueryEngineManager.class.getDeclaredField("lastSaturationWarnMs");
    throttleField.setAccessible(true);
    final java.util.concurrent.atomic.AtomicLong throttle =
        (java.util.concurrent.atomic.AtomicLong) throttleField.get(QueryEngineManager.getInstance());
    throttle.set(0L);

    // Capture WARNING logs via the LogManager logger swap. The production logger writes to the
    // server console (and routes through the slf4j chain in production); the test substitutes a
    // simple list-collector for the duration of the test, then restores.
    final java.util.List<String> warnings = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
    final com.arcadedb.log.Logger originalLogger = readField(com.arcadedb.log.LogManager.instance(), "logger");
    com.arcadedb.log.LogManager.instance().setLogger(new com.arcadedb.log.Logger() {
      @Override public void log(final Object req, final java.util.logging.Level level, final String msg,
          final Throwable th, final String ctx, final Object a1, final Object a2, final Object a3, final Object a4,
          final Object a5, final Object a6, final Object a7, final Object a8, final Object a9, final Object a10,
          final Object a11, final Object a12, final Object a13, final Object a14, final Object a15, final Object a16,
          final Object a17) {
        if (level.intValue() >= java.util.logging.Level.WARNING.intValue())
          warnings.add(msg == null ? "" : msg);
      }
      @Override public void log(final Object req, final java.util.logging.Level level, final String msg,
          final Throwable th, final String ctx, final Object... args) {
        if (level.intValue() >= java.util.logging.Level.WARNING.intValue())
          warnings.add(msg == null ? "" : msg);
      }
      @Override public void flush() {}
    });

    final int poolSize = Math.max(2, Runtime.getRuntime().availableProcessors());
    final QueryEngineManager.PoolStats before = QueryEngineManager.getInstance().getExecutorStats();
    final int queueCapacity = before.queueCapacityRemaining() + before.queueDepth();
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch allWorkersBusy = new CountDownLatch(poolSize);
    try {
      for (int i = 0; i < poolSize; i++) {
        executor.submit(() -> {
          try {
            allWorkersBusy.countDown();
            release.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
      }
      allWorkersBusy.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < queueCapacity; i++)
        executor.submit(() -> { /* no-op */ });

      // First saturation -> WARNING fires.
      executor.execute(() -> { /* no-op */ });
      // Several more in the same interval -> counter ticks but throttle suppresses additional logs.
      for (int i = 0; i < 4; i++)
        executor.execute(() -> { /* no-op */ });

      final long warningsAboutThisPool = warnings.stream()
          .filter(m -> m.contains("Query parallelism pool saturated"))
          .count();
      assertThat(warningsAboutThisPool)
          .as("exactly one WARNING in the throttle interval, even after multiple saturation events")
          .isEqualTo(1L);

      final QueryEngineManager.PoolStats after = QueryEngineManager.getInstance().getExecutorStats();
      assertThat(after.callerRunFallbacks() - before.callerRunFallbacks())
          .as("counter ticks for every fallback, not just the one that emitted the log")
          .isEqualTo(5L);
    } finally {
      release.countDown();
      com.arcadedb.log.LogManager.instance().setLogger(originalLogger);
    }
  }

  private static <T> T readField(final Object target, final String name) throws Exception {
    final java.lang.reflect.Field f = target.getClass().getDeclaredField(name);
    f.setAccessible(true);
    @SuppressWarnings("unchecked")
    final T v = (T) f.get(target);
    return v;
  }
}
