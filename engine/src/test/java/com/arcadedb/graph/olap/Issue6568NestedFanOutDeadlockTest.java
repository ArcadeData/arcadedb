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
import com.arcadedb.exception.CommandExecutionException;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
   * <p>
   * <b>What that discipline rests on</b>, said out loud because it is a fact about the BUILD and not about this
   * file: Surefire runs this project's test classes sequentially ({@code forkCount} 1, no {@code parallel} mode,
   * and no {@code junit.jupiter.execution.parallel.enabled} in {@code junit-platform.properties}), so nothing else
   * is using the shared pool while every one of its workers is pinned here. Enabling parallel test execution would
   * make this class - and every other one that pins this singleton - able to wedge an unrelated test, so that
   * change needs a home for these tests first. Same caveat {@code ParallelScanSafetyTest} carries.
   */
  @Test
  @Timeout(value = HANG_DETECTOR_SECONDS, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void theQueryPoolFanOutRunsItsOwnQueuedChunkRatherThanWaiting() throws Exception {
    final QueryEngineManager queryPool = QueryEngineManager.getInstance();
    final ExecutorService executor = queryPool.getExecutorService();
    final int workers = ((ThreadPoolExecutor) executor).getMaximumPoolSize();

    final CountDownLatch allWorkersPinned = new CountDownLatch(workers);
    final CountDownLatch releasePins = new CountDownLatch(1);
    // Kept rather than fire-and-forget: they are what the cleanup below waits on. A canary alone proves only that
    // ONE worker came back, so a pin still unwinding could be handed to the next test class in this shared JVM.
    final Future<?>[] pins = new Future<?>[workers];
    try {
      for (int i = 0; i < workers; i++)
        pins[i] = executor.submit(() -> {
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
      // Do not hand the next test class a pool whose workers are still unwinding: releasing the latch is not the
      // same as the workers having noticed it, and a shared pool left busy is exactly what makes a later untimed
      // wait look like an unrelated infrastructure timeout. Every pin is awaited, not just probed with a canary,
      // and inside the finally so it happens on the failing path too.
      releasePins.countDown();
      for (final Future<?> pin : pins)
        if (pin != null && !awaited(pin, WORKER_PIN_MS))
          throw new AssertionError("a pinned query-pool worker did not come back after the latch was released, so "
              + "this test would poison the shared pool for the rest of the fork");
    }

    final Future<?> canary = executor.submit(() -> null);
    assertThat(awaited(canary, WORKER_PIN_MS)).as("the query pool must be running work on its own threads again")
        .isTrue();
  }

  /**
   * A pool shut down with a fan-out still in flight - server shutdown while a query is running - must END the wait
   * and take the outstanding chunks down with it, never let a CancellationException unwind the caller while the
   * rest of the batch keeps writing into arrays it has abandoned.
   * <p>
   * The reclaim is what makes this reachable rather than theoretical: on a shut-down CALLER_RUNS pool
   * {@code runRejectedTask} cancels the task instead of running it (#4961, so that nobody waits for a task no
   * worker will take), and {@code Future.get()} then throws CancellationException - a RuntimeException that used
   * to escape {@code awaitFutures} through neither of its catch clauses. Same leak #4951 closed for the interrupt.
   */
  @Test
  @Timeout(value = HANG_DETECTOR_SECONDS, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aFanOutOnAShutDownPoolFailsAndCancelsTheChunksItLeavesBehind() throws Exception {
    final FanOutPool pool = new FanOutPool(1);
    final CountDownLatch pinWorker = new CountDownLatch(1);
    final CountDownLatch workerPinned = new CountDownLatch(1);
    try {
      final ExecutorService executor = pool.getExecutorService();
      executor.submit(() -> {
        workerPinned.countDown();
        pinWorker.await();
        return null;
      });
      assertThat(awaited(workerPinned, WORKER_PIN_MS)).isTrue();

      final AtomicInteger ran = new AtomicInteger();
      final Future<?>[] chunks = { executor.submit(ran::incrementAndGet), executor.submit(ran::incrementAndGet) };

      // shutdown() and not close(): shutdownNow() DRAINS the queue, which would leave nothing to reclaim and so
      // test a different thing entirely. A graceful shutdown keeps the queued chunks exactly where they are.
      executor.shutdown();

      assertThatThrownBy(() -> GraphAlgorithms.awaitFutures(chunks, 2, pool))
          .as("a shut-down pool must fail the fan-out, not leak a CancellationException past it")
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("cancelled").hasMessageContaining("partial results discarded");

      assertThat(chunks[1].isCancelled())
          .as("the chunk the wait never reached must be cancelled, not left to run behind the caller's back")
          .isTrue();
      assertThat(ran.get()).as("a shut-down pool must not run a reclaimed chunk on the caller either").isZero();
    } finally {
      pinWorker.countDown();
      pool.close();
    }
  }

  /**
   * An already-interrupted waiter must ABORT, not quietly do the pool's work for it.
   * <p>
   * This is the reclaim's own way of undoing #4951. A reclaimed chunk runs inline and {@code FutureTask.run()}
   * never looks at the interrupt flag, so without the check an interrupted caller would run the queued chunk to
   * completion, find {@code get()} returning normally because the future IS now done, and walk the rest of the
   * batch the same way - returning normally, flag still set, from a wait whose whole job was to abort and discard.
   * A query killed by a timeout or a cancel would have merged partial results and reported them as complete.
   */
  @Test
  @Timeout(value = HANG_DETECTOR_SECONDS, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void anInterruptedWaiterAbortsInsteadOfRunningTheQueuedChunkItself() throws Exception {
    final FanOutPool pool = new FanOutPool(1);
    final CountDownLatch pinWorker = new CountDownLatch(1);
    final CountDownLatch workerPinned = new CountDownLatch(1);
    try {
      final ExecutorService executor = pool.getExecutorService();
      executor.submit(() -> {
        workerPinned.countDown();
        pinWorker.await();
        return null;
      });
      assertThat(awaited(workerPinned, WORKER_PIN_MS)).isTrue();

      final AtomicInteger ran = new AtomicInteger();
      final Future<?>[] chunks = { executor.submit(ran::incrementAndGet), executor.submit(ran::incrementAndGet) };

      Thread.currentThread().interrupt();
      assertThatThrownBy(() -> GraphAlgorithms.awaitFutures(chunks, 2, pool))
          .as("an interrupted wait must abort, whether or not its chunks are reclaimable")
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("interrupted").hasMessageContaining("partial results discarded");

      assertThat(ran.get()).as("the interrupted thread must not have run a reclaimed chunk").isZero();
      assertThat(chunks[0].isCancelled()).as("every outstanding chunk must be cancelled").isTrue();
      assertThat(chunks[1].isCancelled()).isTrue();
      assertThat(Thread.currentThread().isInterrupted())
          .as("the interrupt must survive the abort, so the caller above can act on it too").isTrue();
    } finally {
      // Clear the flag before it leaks into the next test in this JVM, and only then release the pin: an
      // interrupted thread cannot wait on anything.
      Thread.interrupted();
      pinWorker.countDown();
      pool.close();
    }
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
