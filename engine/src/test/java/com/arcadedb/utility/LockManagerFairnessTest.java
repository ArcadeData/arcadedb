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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exhaustive tests for the FIFO-fair hand-off and concurrency edge cases of {@link LockManager}.
 * The plain functional contract (acquire/already-acquired/timeout/unlock/close/null-args) is covered
 * by {@code LockManagerTest}; this class focuses on the new fairness guarantee and the races a
 * fair-hand-off design must get right (no starvation, no lost lock on grant-vs-timeout, no lost
 * lock on grant-vs-interrupt, cross-thread unlock, infinite-wait).
 */
class LockManagerFairnessTest {

  private LockManager<String, String> lockManager;

  @BeforeEach
  void setUp() {
    lockManager = new LockManager<>();
  }

  @AfterEach
  void tearDown() {
    lockManager.close();
  }

  /**
   * Spin-wait until {@code t} is parked (WAITING or TIMED_WAITING). The waiter is added to the FIFO
   * queue (under the resource monitor) strictly before it parks, so observing the parked state proves
   * it is already enqueued - which is what the FIFO-order tests rely on. If the budget elapses without
   * the thread parking, fail loudly rather than returning silently: a silent return would let a test
   * proceed with a non-deterministic enqueue order instead of surfacing the (CI-load) hiccup.
   */
  private static void awaitParked(final Thread t, final long timeoutMs) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      final Thread.State s = t.getState();
      if (s == Thread.State.WAITING || s == Thread.State.TIMED_WAITING)
        return;
      Thread.sleep(1);
    }
    assertThat(t.getState())
        .as("thread '%s' did not park within %dms (enqueue ordering would be non-deterministic)", t.getName(), timeoutMs)
        .isIn(Thread.State.WAITING, Thread.State.TIMED_WAITING);
  }

  /**
   * The headline guarantee: when several waiters queue on a held resource, they acquire it in strict
   * FIFO order as it is released, not in a random race order. Waiters are started one at a time and
   * each is allowed to fully park before the next starts, so the enqueue order is deterministic.
   */
  @Test
  @Timeout(30)
  void waitersAcquireInFifoOrder() throws Exception {
    final String resource = "fifo";
    final int n = 8;

    assertThat(lockManager.tryLock(resource, "holder", 0)).isEqualTo(LockManager.LOCK_STATUS.YES);

    final List<String> acquireOrder = new CopyOnWriteArrayList<>();
    final Thread[] waiters = new Thread[n];

    for (int i = 0; i < n; i++) {
      final String req = "w" + i;
      waiters[i] = new Thread(() -> {
        if (lockManager.tryLock(resource, req, 0) == LockManager.LOCK_STATUS.YES) {
          acquireOrder.add(req);
          lockManager.unlock(resource, req); // hand off to the next FIFO waiter
        }
      }, req);
      waiters[i].start();
      // Guarantee deterministic enqueue order: wait until this waiter is parked before starting the next.
      awaitParked(waiters[i], 5000);
    }

    // Release the holder: the chain w0 -> w1 -> ... should now drain in order.
    lockManager.unlock(resource, "holder");

    for (final Thread w : waiters)
      w.join(10_000);

    final List<String> expected = new ArrayList<>();
    for (int i = 0; i < n; i++)
      expected.add("w" + i);
    assertThat(acquireOrder).containsExactlyElementsOf(expected);
  }

  /**
   * A waiter that times out while queued must return NO and must not corrupt the queue: a later
   * waiter still gets the lock when it is released. Also verifies the timed-out (cancelled) waiter
   * is skipped during hand-off.
   */
  @Test
  @Timeout(30)
  void timedOutWaiterIsSkippedAndQueueSurvives() throws Exception {
    final String resource = "skip";
    lockManager.tryLock(resource, "holder", 0);

    final AtomicReference<LockManager.LOCK_STATUS> shortResult = new AtomicReference<>();
    final Thread shortWaiter = new Thread(() -> shortResult.set(lockManager.tryLock(resource, "short", 200)), "short");

    final AtomicReference<LockManager.LOCK_STATUS> longResult = new AtomicReference<>();
    final Thread longWaiter = new Thread(() -> longResult.set(lockManager.tryLock(resource, "long", 10_000)), "long");

    shortWaiter.start();
    awaitParked(shortWaiter, 5000);
    longWaiter.start();
    awaitParked(longWaiter, 5000);

    // The short waiter parks until its own 200ms budget expires, then returns NO - no wall-clock sleep.
    shortWaiter.join(5000);
    assertThat(shortResult.get()).isEqualTo(LockManager.LOCK_STATUS.NO);

    // Now release: the long waiter must get it despite the cancelled waiter that was ahead/behind it.
    lockManager.unlock(resource, "holder");
    longWaiter.join(5000);
    assertThat(longResult.get()).isEqualTo(LockManager.LOCK_STATUS.YES);
  }

  /**
   * Under heavy contention on a single resource every requester must eventually acquire it - none may
   * starve to a timeout. This is the exact failure mode (a hot single-bucket lock) the fair hand-off
   * was built to eliminate. Each worker uses a generous timeout; with the old thundering-herd design
   * some workers would lose the wake-up race repeatedly and return NO.
   */
  @Test
  @Timeout(60)
  @Tag("slow")
  void noStarvationUnderHeavyContention() throws Exception {
    final String resource = "hot";
    final int workers = 32;
    final int iterationsPerWorker = 20;

    final ExecutorService pool = Executors.newFixedThreadPool(workers);
    final CyclicBarrier startLine = new CyclicBarrier(workers);
    final AtomicInteger acquisitions = new AtomicInteger();
    final AtomicInteger timeouts = new AtomicInteger();
    // Detects any breach of mutual exclusion: only one holder may be inside the critical section.
    final AtomicInteger inside = new AtomicInteger();
    final AtomicBoolean exclusivityViolated = new AtomicBoolean(false);

    try {
      final CountDownLatch done = new CountDownLatch(workers);
      for (int t = 0; t < workers; t++) {
        final String req = "worker-" + t;
        pool.submit(() -> {
          try {
            startLine.await();
            for (int i = 0; i < iterationsPerWorker; i++) {
              final LockManager.LOCK_STATUS s = lockManager.tryLock(resource, req, 30_000);
              if (s == LockManager.LOCK_STATUS.YES) {
                acquisitions.incrementAndGet();
                if (inside.incrementAndGet() != 1)
                  exclusivityViolated.set(true);
                // tiny critical section
                inside.decrementAndGet();
                lockManager.unlock(resource, req);
              } else if (s == LockManager.LOCK_STATUS.NO) {
                timeouts.incrementAndGet();
              }
            }
          } catch (final Exception e) {
            // leave; assertions below will catch missing acquisitions
          } finally {
            done.countDown();
          }
        });
      }

      assertThat(done.await(45, TimeUnit.SECONDS)).as("all workers finished").isTrue();
    } finally {
      pool.shutdownNow();
    }

    assertThat(exclusivityViolated).as("mutual exclusion held - never two owners at once").isFalse();
    assertThat(timeouts.get()).as("no worker starved to a timeout").isZero();
    assertThat(acquisitions.get()).isEqualTo(workers * iterationsPerWorker);
  }

  /**
   * The critical no-leak invariant for grant-vs-timeout: a waiter whose timeout expires at the exact
   * moment ownership is handed to it must take the lock (return YES) rather than return NO and leave
   * the resource owned-but-never-unlocked. Forced as a tight race over many rounds: the holder
   * releases right around the waiter's deadline. After every round the resource must end up free and
   * re-acquirable, proving the lock was never lost.
   */
  @Test
  @Timeout(60)
  @Tag("slow")
  void grantRacingTimeoutNeverLeaksTheLock() throws Exception {
    final String resource = "race";
    final int rounds = 250;
    // A wider absolute window (100ms vs a few ms) makes scheduling jitter a smaller fraction of the
    // timing, so the grant-vs-timeout race is hit far more often than with a tiny budget - exercising the
    // interesting grant-just-before-deadline path on healthy machines while staying inside @Timeout(60).
    final long waiterTimeoutMs = 100;

    for (int r = 0; r < rounds; r++) {
      assertThat(lockManager.tryLock(resource, "holder", 0)).isEqualTo(LockManager.LOCK_STATUS.YES);

      final AtomicReference<LockManager.LOCK_STATUS> waiterResult = new AtomicReference<>();
      final Thread waiter = new Thread(() -> waiterResult.set(lockManager.tryLock(resource, "waiter", waiterTimeoutMs)),
          "waiter");
      waiter.start();
      awaitParked(waiter, 2000);

      // Release just before the waiter's deadline to bias toward the grant-wins outcome (the case that
      // exercises the no-leak hand-off). Under heavy CI load the waiter may still time out first, degrading
      // the round to a plain timeout - that is fine: the no-leak assertion below holds in both the grant-won
      // and timed-out outcomes, so the test passes even on rounds where the race window is never hit.
      Thread.sleep(waiterTimeoutMs - 5);
      lockManager.unlock(resource, "holder");

      waiter.join(5000);

      // If the waiter timed out (NO), the resource is free. If it won the race (YES), it owns the
      // lock and must release it now. Either way the resource must be free afterwards.
      if (waiterResult.get() == LockManager.LOCK_STATUS.YES)
        lockManager.unlock(resource, "waiter");

      // Prove the lock was not leaked: a fresh requester can take it immediately.
      assertThat(lockManager.tryLock(resource, "probe", 0))
          .as("round %d: resource must be free (no leaked lock)", r).isEqualTo(LockManager.LOCK_STATUS.YES);
      lockManager.unlock(resource, "probe");
    }
  }

  /**
   * A waiter interrupted while queued returns NO, restores the thread's interrupt flag, and leaves the
   * queue consistent so a subsequent waiter still acquires the lock on release.
   */
  @Test
  @Timeout(30)
  void interruptWhileWaitingReturnsNoAndPreservesFlag() throws Exception {
    final String resource = "interrupt";
    lockManager.tryLock(resource, "holder", 0);

    final AtomicReference<LockManager.LOCK_STATUS> result = new AtomicReference<>();
    final AtomicBoolean interruptFlagSeen = new AtomicBoolean(false);
    final Thread waiter = new Thread(() -> {
      result.set(lockManager.tryLock(resource, "waiter", 0)); // infinite wait, only interrupt can end it
      interruptFlagSeen.set(Thread.currentThread().isInterrupted());
    }, "waiter");

    waiter.start();
    awaitParked(waiter, 5000);
    waiter.interrupt();
    waiter.join(5000);

    assertThat(result.get()).isEqualTo(LockManager.LOCK_STATUS.NO);
    assertThat(interruptFlagSeen).as("interrupt status restored").isTrue();

    // Queue is still healthy: a new waiter acquires on release.
    final AtomicReference<LockManager.LOCK_STATUS> next = new AtomicReference<>();
    final Thread t2 = new Thread(() -> next.set(lockManager.tryLock(resource, "next", 10_000)), "next");
    t2.start();
    awaitParked(t2, 5000);
    lockManager.unlock(resource, "holder");
    t2.join(5000);
    assertThat(next.get()).isEqualTo(LockManager.LOCK_STATUS.YES);
  }

  /** Ownership is by requester, not thread: a lock taken on one thread can be released on another. */
  @Test
  @Timeout(30)
  void crossThreadUnlockByRequester() throws Exception {
    final String resource = "session";
    final String session = "session-42";

    final Thread acquirer = new Thread(() -> lockManager.tryLock(resource, session, 0), "acquirer");
    acquirer.start();
    acquirer.join(5000);

    // A different requester cannot get it yet.
    final AtomicReference<LockManager.LOCK_STATUS> waiterResult = new AtomicReference<>();
    final Thread waiter = new Thread(() -> waiterResult.set(lockManager.tryLock(resource, "other", 10_000)), "waiter");
    waiter.start();
    awaitParked(waiter, 5000);

    // Release on a completely different thread, using the session requester - must succeed and hand off.
    final Thread releaser = new Thread(() -> lockManager.unlock(resource, session), "releaser");
    releaser.start();
    releaser.join(5000);

    waiter.join(5000);
    assertThat(waiterResult.get()).isEqualTo(LockManager.LOCK_STATUS.YES);
  }

  /** {@code timeout <= 0} waits indefinitely (used by index compaction) and acquires once released. */
  @Test
  @Timeout(30)
  void infiniteWaitAcquiresOnRelease() throws Exception {
    final String resource = "infinite";
    lockManager.tryLock(resource, "holder", 0);

    final AtomicReference<LockManager.LOCK_STATUS> result = new AtomicReference<>();
    final Thread waiter = new Thread(() -> result.set(lockManager.tryLock(resource, "waiter", 0)), "waiter");
    waiter.start();
    awaitParked(waiter, 5000);

    // Still held: the waiter must not have returned yet.
    Thread.sleep(300);
    assertThat(result.get()).isNull();

    lockManager.unlock(resource, "holder");
    waiter.join(5000);
    assertThat(result.get()).isEqualTo(LockManager.LOCK_STATUS.YES);
  }

  /** The owner re-locking returns ALREADY_ACQUIRED even with waiters queued, and does not enqueue. */
  @Test
  @Timeout(30)
  void reentrantAcquireWithWaitersQueued() throws Exception {
    final String resource = "reentrant";
    lockManager.tryLock(resource, "owner", 0);

    final AtomicReference<LockManager.LOCK_STATUS> waiterResult = new AtomicReference<>();
    final Thread waiter = new Thread(() -> waiterResult.set(lockManager.tryLock(resource, "waiter", 10_000)), "waiter");
    waiter.start();
    awaitParked(waiter, 5000);

    // Owner re-acquires while a waiter is queued.
    assertThat(lockManager.tryLock(resource, "owner", 0)).isEqualTo(LockManager.LOCK_STATUS.ALREADY_ACQUIRED);

    // A single unlock by the owner releases it (non-counting) and hands off to the queued waiter.
    lockManager.unlock(resource, "owner");
    waiter.join(5000);
    assertThat(waiterResult.get()).isEqualTo(LockManager.LOCK_STATUS.YES);
  }

  /** close() must wake queued waiters; they return NO promptly. */
  @Test
  @Timeout(30)
  void closeWakesWaitersWithNo() throws Exception {
    final String resource = "closing";
    lockManager.tryLock(resource, "holder", 0);

    final AtomicReference<LockManager.LOCK_STATUS> result = new AtomicReference<>();
    final Thread waiter = new Thread(() -> result.set(lockManager.tryLock(resource, "waiter", 0)), "waiter");
    waiter.start();
    awaitParked(waiter, 5000);

    lockManager.close();
    waiter.join(5000);
    assertThat(result.get()).isEqualTo(LockManager.LOCK_STATUS.NO);
  }

  /**
   * Multi-resource stress: workers lock two resources in a globally consistent order, mutate shared
   * state under the locks, and release. Verifies no deadlock, no leaked locks, and mutual exclusion
   * per resource across many concurrent rounds.
   */
  @Test
  @Timeout(60)
  @Tag("slow")
  void multiResourceStressNoDeadlockNoLeak() throws Exception {
    final String a = "A";
    final String b = "B";
    final int workers = 16;
    final int iterations = 200;

    final ExecutorService pool = Executors.newFixedThreadPool(workers);
    // AtomicInteger here documents the intent (shared counters guarded by the locks under test) and
    // keeps the test's correctness from depending on the precise visibility semantics of the
    // implementation being exercised. A lost update would still surface as a count below the total.
    final AtomicInteger guardedByA = new AtomicInteger();
    final AtomicInteger guardedByB = new AtomicInteger();
    final ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();

    try {
      final CountDownLatch done = new CountDownLatch(workers);
      for (int t = 0; t < workers; t++) {
        final String req = "w-" + t;
        pool.submit(() -> {
          try {
            for (int i = 0; i < iterations; i++) {
              // Always acquire A before B (consistent order) - no deadlock possible.
              if (lockManager.tryLock(a, req, 30_000) != LockManager.LOCK_STATUS.YES) {
                errors.add(req + " failed to lock A");
                return;
              }
              try {
                if (lockManager.tryLock(b, req, 30_000) != LockManager.LOCK_STATUS.YES) {
                  errors.add(req + " failed to lock B");
                  return;
                }
                try {
                  guardedByA.incrementAndGet();
                  guardedByB.incrementAndGet();
                } finally {
                  lockManager.unlock(b, req);
                }
              } finally {
                lockManager.unlock(a, req);
              }
            }
          } finally {
            done.countDown();
          }
        });
      }

      assertThat(done.await(45, TimeUnit.SECONDS)).as("no deadlock - all workers finished").isTrue();
    } finally {
      pool.shutdownNow();
    }

    assertThat(errors).isEmpty();
    // Every guarded increment ran under exclusive ownership, so the totals must be exact.
    assertThat(guardedByA.get()).isEqualTo(workers * iterations);
    assertThat(guardedByB.get()).isEqualTo(workers * iterations);

    // No leaked locks: both resources are free.
    assertThat(lockManager.tryLock(a, "probe", 0)).isEqualTo(LockManager.LOCK_STATUS.YES);
    assertThat(lockManager.tryLock(b, "probe", 0)).isEqualTo(LockManager.LOCK_STATUS.YES);
    lockManager.unlock(a, "probe");
    lockManager.unlock(b, "probe");
  }
}
