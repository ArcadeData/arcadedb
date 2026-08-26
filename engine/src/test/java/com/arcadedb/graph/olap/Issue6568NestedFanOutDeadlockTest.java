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
package com.arcadedb.graph.olap;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.utility.DedicatedThreadPool;
import com.arcadedb.utility.StallAwareStopwatch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #6568: a thread that waits for a task it submitted to a dedicated pool must RECLAIM that task
 * when it is still queued, never park on it.
 * <p>
 * The issue arrived as a wedged {@code ParallelScanSafetyTest} - a caller-runs execution parked on a latch only a
 * pool task could count down - and asked whether the same shape is reachable in production. It is, and it does not
 * even need the caller-runs path: every blocking fan-out on a bounded pool has it by construction. The dangerous
 * belief is that the caller-runs policy is the safety net. It is not: it fires only when the queue is FULL, and a
 * queue with a thousand free slots accepts the task and parks the submitter, which is the deadlock.
 * <p>
 * Both tests here hang for ever on the pre-#6568 code and complete in milliseconds with the reclaiming wait, so the
 * {@code @Timeout} is a hang detector rather than a latency bound - and it is
 * {@link Timeout.ThreadMode#SEPARATE_THREAD}, because the default same-thread timeout can only report a breach after
 * the method returns, which a wedged thread never does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6568NestedFanOutDeadlockTest {

  /**
   * Both tests are all-or-nothing: they finish at once or not at all. Plain wall clock, because {@code @Timeout}
   * cannot be stall-discounted - so it is sized as a hang detector and never as a bound on anything.
   */
  private static final int  HANG_DETECTOR_SECONDS = 300;
  /**
   * The tripwire between a fan-out that completes and one that is wedged for ever, spent in STALL-DISCOUNTED time
   * (#6260): late in a full-suite run a stop-the-world pause of tens of seconds is routine, and charging one to
   * this budget would report a healthy fan-out as a deadlock. Generous is free here - the wedge it separates from
   * is unbounded, so widening it can only ever cost patience.
   */
  private static final long WEDGE_VERDICT_MS      = 20_000L;
  /** Stall-discounted budget for a task to reach a worker thread on a loaded CI runner. */
  private static final long WORKER_PIN_MS         = 30_000L;
  /** How long each individual poll waits before the budget above is re-checked against the stall counter. */
  private static final long POLL_MS               = 250L;

  /**
   * A private pool, not the JVM-wide query one: this test deliberately occupies every worker with a thread that is
   * blocked waiting, which on a shared pool would poison every later test in the fork if anything went wrong.
   */
  private static final class FanOutPool extends DedicatedThreadPool {
    private FanOutPool(final int threads) {
      // A generously sized queue is the POINT, not an oversight: it guarantees the inner tasks are ACCEPTED rather
      // than rejected, so the caller-runs policy never fires and the wait is left facing the real deadlock.
      super("ArcadeDB-Issue6568-", threads, 256, SaturationPolicy.CALLER_RUNS, DedicatedThreadPool::plainWorker,
          "Issue 6568 test pool", null, GlobalConfiguration.QUERY_PARALLELISM_POOL_THREADS);
    }
  }

  /**
   * The nested fan-out: every worker of the pool runs an outer task that submits an inner task and waits for it.
   * Without reclaiming, the inner tasks sit in a queue no thread will ever reach - each worker is parked on one of
   * them - and the pool is dead with no exception, no failing assertion and nothing in the log to point at.
   */
  @Test
  @Timeout(value = HANG_DETECTOR_SECONDS, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aNestedFanOutCompletesInsteadOfStarvingItsOwnQueue() throws Exception {
    final int threads = 4;
    final FanOutPool pool = new FanOutPool(threads);
    try {
      final ExecutorService executor = pool.getExecutorService();
      // A latch used as a barrier rather than a CyclicBarrier: every outer task waits here until all of them are
      // running, so the inner tasks are submitted only once there is provably no free worker left to run them -
      // and unlike CyclicBarrier.await, a latch wait can be RETRIED, which is what lets the budget below be
      // stall-discounted instead of one stop-the-world pause breaking the barrier for every party.
      final CountDownLatch allOutersRunning = new CountDownLatch(threads);
      final AtomicInteger innerTasksRun = new AtomicInteger();
      final AtomicReference<Throwable> outerFailure = new AtomicReference<>();

      final Future<?>[] outer = new Future<?>[threads];
      for (int i = 0; i < threads; i++)
        outer[i] = executor.submit(() -> {
          try {
            allOutersRunning.countDown();
            if (!awaited(allOutersRunning, WORKER_PIN_MS))
              throw new IllegalStateException("the outer chunks never all reached a worker");
            final Future<?>[] inner = { executor.submit(innerTasksRun::incrementAndGet) };
            GraphAlgorithms.awaitFutures(inner, 1, pool);
          } catch (final Exception e) {
            outerFailure.compareAndSet(null, e);
            throw e;
          }
          return null;
        });

      for (int i = 0; i < threads; i++)
        assertThat(awaited(outer[i], WEDGE_VERDICT_MS))
            .as("outer chunk %d must not park on an inner task no worker is left to run (#6568)", i).isTrue();

      assertThat(outerFailure.get()).isNull();
      assertThat(innerTasksRun.get()).as("every inner task must have run, on the thread that was waiting for it")
          .isEqualTo(threads);
      // Not asserted as exactly `threads`: the first outer to reclaim finishes and frees its worker, which is then
      // free to take a later outer's inner task off the queue legitimately. What has to be true is that a reclaim
      // happened at all, because at the moment the inner tasks were submitted no worker could have run any of them.
      assertThat(pool.getPoolStats().reclaimedTasks())
          .as("the inner tasks were queued behind fully occupied workers, so the wait had to take them back out")
          .isPositive();
      assertThat(pool.getPoolStats().callerRunFallbacks())
          .as("the queue had free slots throughout: the caller-runs policy is NOT what saved this").isZero();
    } finally {
      pool.close();
    }
  }

  /**
   * The same guarantee wired to the pool the production fan-outs actually use, through the {@code awaitFutures}
   * overload they actually call. Every query-pool worker is pinned, the fan-out's chunk is therefore queued behind
   * them, and the caller must run it itself instead of waiting for a worker that is never coming.
   * <p>
   * Pinning discipline, as {@code QueryEngineManagerPoolTest} documents it: the pool is JVM-wide, the release latch
   * is counted down in a {@code finally} with no assertion before the {@code try}, and the test waits for the
   * workers to drain before returning so it cannot hand the next test class a busy pool.
   */
  @Test
  @Timeout(value = HANG_DETECTOR_SECONDS, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void theQueryPoolFanOutRunsItsOwnQueuedChunkRatherThanWaiting() throws Exception {
    final QueryEngineManager queryPool = QueryEngineManager.getInstance();
    final ExecutorService executor = queryPool.getExecutorService();
    final int workers = ((ThreadPoolExecutor) executor).getMaximumPoolSize();

    final CountDownLatch allWorkersPinned = new CountDownLatch(workers);
    final CountDownLatch releasePins = new CountDownLatch(1);
    try {
      for (int i = 0; i < workers; i++)
        executor.submit(() -> {
          allWorkersPinned.countDown();
          releasePins.await();
          return null;
        });

      assertThat(awaited(allWorkersPinned, WORKER_PIN_MS))
          .as("every query-pool worker must be pinned for the scenario to be the one #6568 is about").isTrue();

      final long reclaimedBefore = queryPool.getExecutorStats().reclaimedTasks();
      final AtomicReference<Thread> chunkRanOn = new AtomicReference<>();
      final Future<?>[] chunk = { executor.submit(() -> chunkRanOn.set(Thread.currentThread())) };

      assertThat(queryPool.getExecutorStats().queueCapacityRemaining())
          .as("the queue must still have room, or this would be testing the caller-runs policy instead").isPositive();

      GraphAlgorithms.awaitFutures(chunk, 1);

      assertThat(chunkRanOn.get()).as("a chunk queued behind pinned workers must run on the thread waiting for it")
          .isSameAs(Thread.currentThread());
      assertThat(queryPool.getExecutorStats().reclaimedTasks()).isGreaterThanOrEqualTo(reclaimedBefore + 1);
    } finally {
      releasePins.countDown();
    }

    // Do not hand the next test class a pool whose workers are still unwinding: the pins are released above, but
    // the workers have not necessarily noticed yet, and a shared pool left busy is exactly what makes a later
    // untimed wait look like an unrelated infrastructure timeout.
    final Future<?> canary = executor.submit(() -> null);
    assertThat(awaited(canary, WORKER_PIN_MS)).as("the query pool must be usable again before the next test class")
        .isTrue();
  }

  /**
   * True when the future completed within a STALL-DISCOUNTED budget, false when it is wedged - in which case it is
   * cancelled so nothing outlives the test.
   * <p>
   * Polled rather than waited once, because that is what lets the budget be stall-discounted: each poll is short,
   * and only the time the JVM was actually running counts against it (#6260). A single {@code get(20s)} would be
   * raw wall clock, and one stop-the-world pause would report a healthy fan-out as a deadlock.
   */
  private static boolean awaited(final Future<?> future, final long budgetMs)
      throws InterruptedException, ExecutionException {
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    do {
      try {
        future.get(POLL_MS, TimeUnit.MILLISECONDS);
        return true;
      } catch (final TimeoutException stillRunning) {
        // Not a verdict: only the loop condition, which discounts JVM-wide stalls, decides that.
      }
    } while (watch.effectiveMs() < budgetMs);

    future.cancel(true);
    return false;
  }

  /** {@link #awaited(Future, long)} for a latch: same stall-discounted budget, same reason. */
  private static boolean awaited(final CountDownLatch latch, final long budgetMs) throws InterruptedException {
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    do {
      if (latch.await(POLL_MS, TimeUnit.MILLISECONDS))
        return true;
    } while (watch.effectiveMs() < budgetMs);
    return false;
  }
}
