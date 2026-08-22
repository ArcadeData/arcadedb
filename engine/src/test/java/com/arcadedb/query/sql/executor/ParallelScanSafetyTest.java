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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.ParallelScanProducerPool;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.utility.DedicatedThreadPool.PoolStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the parallel bucket scan safety fixes:
 * <ul>
 * <li>#4948: the blocking scan producers ran on the shared {@link QueryEngineManager} pool, whose caller-runs
 * rejection executed a whole bucket scan synchronously on the CONSUMER thread under saturation. For any bucket
 * larger than the bounded result queue (4096), {@code put()} then blocked forever: the only thread that could
 * drain the queue was the thread doing the put. The producers now run on the dedicated
 * {@link ParallelScanProducerPool}, which never runs tasks on the caller.</li>
 * <li>#4950: the same blocking producers pinned the shared pool's workers, starving every non-blocking compute
 * user (graph algorithms, Cypher operators) in the JVM.</li>
 * <li>#4949: every scan worker shared the caller's {@code BasicCommandContext} and raced on its non-thread-safe
 * variables HashMap (each worker's {@code rs.next()} writes {@code $current}). Workers now get their own context
 * copy and {@code $current} on the shared context is written only by the consumer.</li>
 * </ul>
 */
class ParallelScanSafetyTest extends TestHelper {

  private static final String TYPE_NAME = "Big";
  // Two buckets (the default QUERY_PARALLEL_SCAN_MIN_BUCKETS) with > 4096 records EACH, so a caller-run
  // bucket scan would fill the bounded result queue and wedge (#4948).
  private static final int RECORDS = 12_000;
  // Runaway backstop for the saturation loop only: it stops on the pool's own "queue is full" signal, and this
  // bound exists so a pool that never reports full fails the assertion below instead of submitting forever.
  private static final int MAX_SATURATION_SUBMISSIONS = 100_000;

  private void createAndPopulate() {
    database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(2).create();
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE_NAME).set("id", i).save();
    });
  }

  @Test
  // SEPARATE_THREAD is required, not cosmetic: everything this test guards is a wedge, and the default
  // same-thread @Timeout can only report a breach AFTER the method returns - which a wedged thread never
  // does, so the whole build would hang instead of failing. Sized as a hang detector, not as a latency
  // bound: the body's own deadline is 20s.
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void parallelScanCompletesWhileQueryPoolIsSaturated() throws Exception {
    createAndPopulate();

    // ISOLATION: this saturates the JVM-WIDE QueryEngineManager pool for the duration of the test (released
    // in finally). Safe because Surefire runs test classes sequentially in this project (forkCount=1, no
    // parallel mode configured); revisit if JUnit/Surefire parallel execution is ever enabled.
    // Saturate the shared QueryEngineManager pool: occupy every worker thread AND fill the whole task queue
    // with latch-gated tasks. Any further submission to that pool would trigger its caller-runs rejection -
    // which is exactly the condition that used to self-deadlock the parallel scan (#4948).
    final ExecutorService queryPool = QueryEngineManager.getInstance().getExecutorService();

    // The pool is a JVM-wide singleton shared with every other test class, so it does not necessarily start
    // idle. Give whatever ran before us a moment to drain: a single foreign task left in the queue shifts
    // every count below by one, and an over-submission here is not an exception - it is this pool's
    // caller-runs policy handing the task straight back to THIS thread (DedicatedThreadPool.runOnCaller).
    awaitIdleQueryPool();

    assertThat(QueryEngineManager.getInstance().getExecutorStats().queueCapacityRemaining())
        .as("saturating an UNBOUNDED queue is not possible, and this scenario is about the bounded one").isNotNegative();

    final Thread submitter = Thread.currentThread();
    final CountDownLatch releaseSaturation = new CountDownLatch(1);
    try {
      // Ask the POOL when it is full, rather than computing "threads + queue" from QUERY_PARALLELISM_*: the
      // pool was sized when the singleton was first built, and those settings can have been changed and reset
      // many times since by other test classes (GlobalConfiguration.PROFILE rewrites both). Whenever today's
      // settings are wider than the pool actually is, the computed loop over-submits, and the extra tasks come
      // back to run on this thread.
      int submissions = 0;
      while (QueryEngineManager.getInstance().getExecutorStats().queueCapacityRemaining() > 0
          && submissions < MAX_SATURATION_SUBMISSIONS) {
        queryPool.submit(() -> {
          // NEVER PARK THE SUBMITTING THREAD. A task the queue refuses runs on the caller, and the caller is
          // the only thread that reaches the countDown() in the finally below - parking it here would wait for
          // a latch none but itself can release, wedging the whole suite with no test ever failing.
          if (Thread.currentThread() == submitter)
            return null;
          releaseSaturation.await();
          return null;
        });
        submissions++;
      }
      assertThat(QueryEngineManager.getInstance().getExecutorStats().queueCapacityRemaining())
          .as("query pool must be saturated for the scenario to be meaningful (submitted %d tasks)", submissions).isZero();

      // Run the parallel-scan query on its own thread so a regression (wedged consumer) fails the test
      // instead of hanging it.
      final AtomicLong rowsRead = new AtomicLong();
      final CountDownLatch queryDone = new CountDownLatch(1);
      final Thread queryThread = new Thread(() -> {
        try {
          final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME);
          while (rs.hasNext()) {
            rs.next();
            rowsRead.incrementAndGet();
          }
        } finally {
          queryDone.countDown();
        }
      }, "ParallelScanSafetyTest-query");
      queryThread.setDaemon(true);
      queryThread.start();

      final boolean completed = queryDone.await(20, TimeUnit.SECONDS);
      if (!completed)
        // Regression: unwedge the thread (put() is interruptible) so the database can be torn down.
        for (int i = 0; i < 20 && queryThread.isAlive(); i++) {
          queryThread.interrupt();
          queryThread.join(200);
        }

      assertThat(completed)
          .as("parallel scan must complete even with the shared query pool fully saturated (#4948)").isTrue();
      assertThat(rowsRead.get()).as("all rows must be returned").isEqualTo(RECORDS);
    } finally {
      releaseSaturation.countDown();
    }
  }

  /**
   * Waits, briefly and without asserting, for the shared query pool to have no task running and none queued.
   * The saturation loop is written against a pool that starts empty; a foreign leftover only costs the test its
   * meaningfulness (the queue-full assertion catches that), never its liveness (the caller-runs guard does).
   */
  private static void awaitIdleQueryPool() throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline) {
      final PoolStats stats = QueryEngineManager.getInstance().getExecutorStats();
      if (stats.activeThreads() == 0 && stats.queueDepth() == 0)
        return;
      Thread.sleep(10);
    }
  }

  @Test
  void parallelScanRunsOnTheDedicatedProducerPool() {
    createAndPopulate();

    final long before = ParallelScanProducerPool.getInstance().getPoolStats().completedTasks();

    final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME);
    long count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(RECORDS);

    // One producer task per bucket must have run on the dedicated pool (#4950): blocking producers must
    // never colonize the shared QueryEngineManager compute pool. Poll briefly: the executor bumps its
    // completed-task count AFTER the FutureTask outcome is published, so the consumer can finish draining a
    // moment before the bookkeeping lands.
    final long deadline = System.currentTimeMillis() + 5_000;
    while (ParallelScanProducerPool.getInstance().getPoolStats().completedTasks() < before + 2
        && System.currentTimeMillis() < deadline)
      Thread.onSpinWait();
    assertThat(ParallelScanProducerPool.getInstance().getPoolStats().completedTasks())
        .as("scan producer tasks must run on the dedicated pool").isGreaterThanOrEqualTo(before + 2);
  }

  @Test
  void currentVariableIsWrittenOnlyByTheConsumer() {
    createAndPopulate();

    // Drive the step directly with a known context: with the per-worker context copies (#4949), $current on
    // the CALLER's context is written only by the consumer, so at every step it must be the row just
    // returned. With the old shared context, the scan workers concurrently stomped it with rows from other
    // buckets (racing the non-thread-safe HashMap at the same time).
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final FetchFromTypeExecutionStep step = new FetchFromTypeExecutionStep(TYPE_NAME, null, context, null);
    try {
      final ResultSet rs = step.syncPull(context, RECORDS + 1);
      long count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        count++;
        assertThat(context.getVariable("current"))
            .as("$current on the caller's context must be the row just returned to the consumer")
            .isSameAs(result);
      }
      assertThat(count).isEqualTo(RECORDS);
    } finally {
      step.close();
    }
  }

  @Test
  void failingProducerFailsTheQueryInsteadOfTruncatingIt() {
    createAndPopulate();

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final FetchFromTypeExecutionStep step = new FetchFromTypeExecutionStep(TYPE_NAME, null, context, null);
    try {
      // Inject a poisoned sub-step: its producer dies mid-scan. The two healthy bucket producers keep
      // feeding rows, but the consumer must FAIL, never report the (possibly truncated) scan as success.
      // NOTE: an Error thrown here reaches the producer's recovery WRAPPED in DatabaseOperationException
      // (executeInReadLock converts every non-RuntimeException Throwable), so this test locks in the general
      // failure-surfacing guarantee; the producer's catch is still Throwable because an Error raised OUTSIDE
      // the read-lock section (emit loop, context init) would otherwise slip past it and report the
      // truncation as success.
      step.getSubSteps().add(new AbstractExecutionStep(context) {
        @Override
        public ResultSet syncPull(final CommandContext ctx, final int nRecords) {
          throw new AssertionError("injected producer failure");
        }
      });

      final ResultSet rs = step.syncPull(context, RECORDS + 1);
      assertThatThrownBy(() -> {
        while (rs.hasNext())
          rs.next();
      }).as("a failed producer must fail the query, not shrink its result set")
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("Parallel scan failed");
    } finally {
      step.close();
    }
  }

  @Test
  void abandonedResultSetReleasesProducersAndSurfacesFailure() throws Exception {
    createAndPopulate();

    // Shrink the abandonment timeout for the test on the DATABASE's own configuration: the step reads it
    // through db.getConfiguration() at scan start (setting only the global would be too late, the database
    // snapshotted its context configuration at creation - same lesson as the LSM compaction harness).
    database.getConfiguration().setValue(GlobalConfiguration.PARALLEL_SCAN_ABANDONED_TIMEOUT, 200L);
    try {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final FetchFromTypeExecutionStep step = new FetchFromTypeExecutionStep(TYPE_NAME, null, context, null);
      try {
        // Open the parallel ResultSet and then NEVER drain nor close it: the producers fill the bounded
        // result queue, then must give up after the abandonment timeout instead of parking on the full
        // queue forever (leaking their pool threads).
        final ResultSet rs = step.syncPull(context, RECORDS + 1);

        final Field failureField = FetchFromTypeExecutionStep.class.getDeclaredField("parallelScanFailure");
        failureField.setAccessible(true);
        final long deadline = System.currentTimeMillis() + 20_000;
        while (failureField.get(step) == null && System.currentTimeMillis() < deadline)
          Thread.sleep(20);

        assertThat(failureField.get(step))
            .as("producers must abandon a never-consumed, never-closed ResultSet instead of parking forever")
            .isNotNull();

        // If the consumer ever comes back, it must FAIL loudly, not silently receive a truncated result.
        assertThatThrownBy(rs::hasNext)
            .isInstanceOf(CommandExecutionException.class)
            .hasMessageContaining("Parallel scan");
      } finally {
        step.close();
      }
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.PARALLEL_SCAN_ABANDONED_TIMEOUT, 600_000L);
    }
  }
}
