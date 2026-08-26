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
package com.arcadedb.utility;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the shared base of the engine's dedicated executors (issue #6324, item 4).
 * <p>
 * What is worth pinning here is not the {@link java.util.concurrent.ThreadPoolExecutor} - the JDK tests that - but the
 * things the four copies of this code were getting subtly different, and the two combinations the base refuses so that
 * a new pool cannot be built without answering the {@code engine-concurrency} checklist.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DedicatedThreadPoolTest {

  /**
   * Plain wall clock, because {@code @Timeout} cannot be stall-discounted - so it is sized as a hang detector and
   * never as a bound on anything. The tests that wait for something get their actual verdict from
   * {@link #awaited(CountDownLatch)}, whose budget IS stall-discounted (#6260); this only stops a genuine wedge
   * from hanging the build. Same sizing as {@code Issue6568NestedFanOutDeadlockTest}, for the same reason.
   */
  private static final int HANG_DETECTOR_SECONDS = 300;

  private static final class TestPool extends DedicatedThreadPool {
    private TestPool(final int queueCapacity, final SaturationPolicy policy) {
      super("ArcadeDB-DedicatedThreadPoolTest-", 1, queueCapacity, policy, DedicatedThreadPool::plainWorker,
          "Test pool", "nothing at all", GlobalConfiguration.QUERY_PARALLELISM_POOL_THREADS);
    }
  }

  /** An unbounded queue never rejects, so a rejection policy on it is a statement that cannot come true. */
  @Test
  void anUnboundedQueueCannotHaveARejectionPolicy() {
    assertThatThrownBy(() -> new TestPool(DedicatedThreadPool.UNBOUNDED_QUEUE, DedicatedThreadPool.SaturationPolicy.CALLER_RUNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("never rejects");
  }

  /** And a bounded one has to say what happens to the task it refuses - that is the checklist, enforced. */
  @Test
  void aBoundedQueueMustDeclareASaturationPolicy() {
    assertThatThrownBy(() -> new TestPool(4, DedicatedThreadPool.SaturationPolicy.NONE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("no policy");
  }

  /**
   * {@code -1}, not a near-2^31 constant that would read oddly next to the bounded pools on the same dashboard row.
   * {@code ParallelScanProducerPool} used to hard-code this in its own {@code getPoolStats}.
   */
  @Test
  void anUnboundedPoolReportsItsQueueCapacityAsNotApplicable() {
    final TestPool pool = new TestPool(DedicatedThreadPool.UNBOUNDED_QUEUE, DedicatedThreadPool.SaturationPolicy.NONE);
    try {
      assertThat(pool.getPoolStats().queueCapacityRemaining()).isEqualTo(-1);
      assertThat(pool.getPoolStats().callerRunFallbacks()).as("a pool that cannot reject never falls back").isZero();
    } finally {
      pool.close();
    }
  }

  /** Caller-runs means exactly that, and the fallback is counted so an operator sees saturation rather than latency. */
  @Test
  @Timeout(60)
  void aRejectedTaskRunsOnTheSubmitterAndIsCounted() {
    final TestPool pool = new TestPool(4, DedicatedThreadPool.SaturationPolicy.CALLER_RUNS);
    try {
      final AtomicReference<Thread> ranOn = new AtomicReference<>();
      pool.runOnCaller(() -> ranOn.set(Thread.currentThread()), 4, 1);

      assertThat(ranOn.get()).isSameAs(Thread.currentThread());
      assertThat(pool.getPoolStats().callerRunFallbacks()).isEqualTo(1);
    } finally {
      pool.close();
    }
  }

  /**
   * A task rejected by a SHUT-DOWN pool would otherwise be neither run nor completed, leaving a caller blocked in an
   * untimed {@code Future.get()} for ever (#4961). Cancelling its future ends the wait instead.
   */
  @Test
  @Timeout(60)
  void aTaskRejectedByAShutDownPoolHasItsFutureCancelled() {
    final TestPool pool = new TestPool(4, DedicatedThreadPool.SaturationPolicy.CALLER_RUNS);
    pool.close();

    final AtomicReference<Boolean> ran = new AtomicReference<>(false);
    final FutureTask<Void> task = new FutureTask<>(() -> {
      ran.set(true);
      return null;
    });
    pool.runOnCaller(task, 4, 1);

    assertThat(ran.get()).as("a shut-down pool must not run the task").isFalse();
    assertThat(task.isCancelled()).as("...and must not leave its caller waiting for a result that never comes").isTrue();
  }

  /** The same task on a pool that runs it regardless, which is what a pool with no future to cancel needs. */
  @Test
  @Timeout(60)
  void theEvenWhenShutDownPolicyRunsItAnyway() {
    final TestPool pool = new TestPool(4, DedicatedThreadPool.SaturationPolicy.CALLER_RUNS_EVEN_WHEN_SHUT_DOWN);
    pool.close();

    final AtomicReference<Boolean> ran = new AtomicReference<>(false);
    pool.runOnCaller(() -> ran.set(true), 4, 1);

    assertThat(ran.get()).as("the submitter has already counted this as in flight: dropping it strands that count")
        .isTrue();
  }


  /**
   * The #6568 primitive: a task the queue has accepted but no worker has started is handed back to the thread that
   * is about to wait for it, and runs there. What makes it safe is that {@code remove} succeeding is proof no
   * worker took it, so it runs exactly once.
   */
  @Test
  @Timeout(HANG_DETECTOR_SECONDS)
  void aQueuedTaskIsReclaimedAndRunByTheThreadWaitingForIt() throws Exception {
    final TestPool pool = new TestPool(16, DedicatedThreadPool.SaturationPolicy.CALLER_RUNS);
    final CountDownLatch pinWorker = new CountDownLatch(1);
    final CountDownLatch workerPinned = new CountDownLatch(1);
    try {
      // The pool has exactly one worker: pin it, and everything else submitted is queued but unreachable.
      pool.getExecutorService().submit(() -> {
        workerPinned.countDown();
        pinWorker.await();
        return null;
      });
      assertThat(awaited(workerPinned)).as("the pool's single worker must be pinned before anything can be queued")
          .isTrue();

      final AtomicReference<Thread> ranOn = new AtomicReference<>();
      final Future<?> queued = pool.getExecutorService().submit(() -> ranOn.set(Thread.currentThread()));

      assertThat(pool.runQueuedTaskOnCaller((Runnable) queued)).isTrue();
      assertThat(ranOn.get()).as("the reclaimed task runs on the reclaiming thread, not on a worker")
          .isSameAs(Thread.currentThread());
      assertThat(queued.isDone()).as("...and its future is resolved, so the wait that follows returns at once").isTrue();
      assertThat(pool.getPoolStats().reclaimedTasks()).isEqualTo(1);
      assertThat(pool.getPoolStats().callerRunFallbacks())
          .as("a reclaim is the pool being busy, never the pool being full: it must not read as a sizing problem")
          .isZero();
    } finally {
      pinWorker.countDown();
      pool.close();
    }
  }

  /**
   * And it refuses everything it must refuse: a task already taken by a worker (reclaiming it would run it twice),
   * one that has already finished, and one that was never submitted to this pool at all.
   */
  @Test
  @Timeout(HANG_DETECTOR_SECONDS)
  void aTaskAWorkerAlreadyHasIsNeverReclaimed() throws Exception {
    final TestPool pool = new TestPool(16, DedicatedThreadPool.SaturationPolicy.CALLER_RUNS);
    final CountDownLatch releaseRunning = new CountDownLatch(1);
    final CountDownLatch running = new CountDownLatch(1);
    try {
      final Future<?> started = pool.getExecutorService().submit(() -> {
        running.countDown();
        releaseRunning.await();
        return null;
      });
      assertThat(awaited(running)).as("the task must be running on the worker, not still queued").isTrue();

      assertThat(pool.runQueuedTaskOnCaller((Runnable) started))
          .as("a task a worker is already running must never be run a second time on the caller").isFalse();

      final FutureTask<Void> foreign = new FutureTask<>(() -> null);
      assertThat(pool.runQueuedTaskOnCaller(foreign)).as("a task this pool never queued is not this pool's to run")
          .isFalse();
      assertThat(foreign.isDone()).isFalse();

      assertThat(pool.runQueuedTaskOnCaller(null)).as("nothing to reclaim, and no NPE either").isFalse();
      assertThat(pool.getPoolStats().reclaimedTasks()).isZero();
    } finally {
      releaseRunning.countDown();
      pool.close();
    }
  }

  /**
   * A reclaimed task that THROWS must be indistinguishable from a worker-run one that throws: the exception lands in
   * the task's own future, so the caller's {@code get()} surfaces it as an {@link java.util.concurrent.ExecutionException}
   * rather than blowing up in the middle of the reclaim. The whole fix rests on "a reclaim is the same execution a
   * worker would have given it", and this is the half of that claim liveness alone does not pin.
   */
  @Test
  @Timeout(HANG_DETECTOR_SECONDS)
  void aReclaimedTaskThatThrowsFailsItsFutureInsteadOfTheReclaimingThread() throws Exception {
    final TestPool pool = new TestPool(16, DedicatedThreadPool.SaturationPolicy.CALLER_RUNS);
    final CountDownLatch pinWorker = new CountDownLatch(1);
    final CountDownLatch workerPinned = new CountDownLatch(1);
    try {
      pool.getExecutorService().submit(() -> {
        workerPinned.countDown();
        pinWorker.await();
        return null;
      });
      assertThat(awaited(workerPinned)).isTrue();

      final Future<?> queued = pool.getExecutorService().submit(() -> {
        throw new IllegalStateException("boom");
      });

      assertThat(pool.runQueuedTaskOnCaller((Runnable) queued))
          .as("the reclaim itself must succeed - a failing task is still a task this thread ran").isTrue();
      assertThatThrownBy(queued::get).isInstanceOf(ExecutionException.class)
          .cause().isInstanceOf(IllegalStateException.class).hasMessage("boom");
    } finally {
      pinWorker.countDown();
      pool.close();
    }
  }

  /**
   * A reclaim borrows the caller's thread exactly as a caller-runs rejection does, so it must go through the same
   * {@link DedicatedThreadPool#runRejectedTask(Runnable)} hook. {@code AsyncCommandPool} overrides that hook to mark
   * the borrowed thread as one of its own, and the async barrier reads that mark to avoid waiting for the very
   * command it is running - a reclaim that bypassed the hook would silently reintroduce that self-deadlock.
   */
  @Test
  @Timeout(HANG_DETECTOR_SECONDS)
  void aReclaimRunsThroughTheSameHookAsACallerRunsFallback() throws Exception {
    final AtomicReference<Thread> markedOn = new AtomicReference<>();
    final CountDownLatch pinWorker = new CountDownLatch(1);
    final CountDownLatch workerPinned = new CountDownLatch(1);
    final DedicatedThreadPool pool = new DedicatedThreadPool("ArcadeDB-DedicatedThreadPoolTest-hook-", 1, 16,
        DedicatedThreadPool.SaturationPolicy.CALLER_RUNS, DedicatedThreadPool::plainWorker, "Test pool", null,
        GlobalConfiguration.QUERY_PARALLELISM_POOL_THREADS) {
      @Override
      protected void runRejectedTask(final Runnable task) {
        markedOn.set(Thread.currentThread());
        super.runRejectedTask(task);
      }
    };
    try {
      pool.getExecutorService().submit(() -> {
        workerPinned.countDown();
        pinWorker.await();
        return null;
      });
      assertThat(awaited(workerPinned)).isTrue();

      final Future<?> queued = pool.getExecutorService().submit(() -> {
      });
      assertThat(pool.runQueuedTaskOnCaller((Runnable) queued)).isTrue();

      assertThat(markedOn.get())
          .as("the subclass hook must see the reclaim, on the thread that is borrowing itself out to the pool")
          .isSameAs(Thread.currentThread());
    } finally {
      pinWorker.countDown();
      pool.close();
    }
  }

  /**
   * Waits for a readiness latch against a STALL-DISCOUNTED budget (#6260): a stop-the-world pause late in a
   * full-suite run must not be charged to a wait that is only there to order two threads, or the test goes red
   * for the JVM's mood rather than for the code. Polled, because that is what makes the discount possible.
   */
  private static boolean awaited(final CountDownLatch latch) throws InterruptedException {
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    do {
      if (latch.await(250, TimeUnit.MILLISECONDS))
        return true;
    } while (watch.effectiveMs() < 30_000L);
    return false;
  }

  /** The shared sizing: an explicit positive value wins, anything else is cores with a floor of two. */
  @Test
  void sizingTakesTheExplicitValueOrFallsBackToCores() {
    assertThat(DedicatedThreadPool.autoSizeThreads(7)).isEqualTo(7);
    assertThat(DedicatedThreadPool.autoSizeThreads(0))
        .isEqualTo(Math.max(DedicatedThreadPool.DEFAULT_THREADS_FLOOR, Runtime.getRuntime().availableProcessors()));
    assertThat(DedicatedThreadPool.autoSizeThreads(-3)).as("a negative value is coerced, not honoured")
        .isEqualTo(Math.max(DedicatedThreadPool.DEFAULT_THREADS_FLOOR, Runtime.getRuntime().availableProcessors()));

    assertThat(DedicatedThreadPool.queueSizeOrDefault(64)).isEqualTo(64);
    assertThat(DedicatedThreadPool.queueSizeOrDefault(0)).isEqualTo(DedicatedThreadPool.DEFAULT_QUEUE_SIZE);
    assertThat(DedicatedThreadPool.queueSizeOrDefault(-1)).isEqualTo(DedicatedThreadPool.DEFAULT_QUEUE_SIZE);
  }
}
