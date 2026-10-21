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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Spin-then-park hand-off of {@link LockManager} (issue #10501): the head waiter spins instead of parking, only the head
 * spins, a release wakes the next head early, the spin turns itself off for resources held longer than the spin budget,
 * and close / timeout / interrupt still end a wait that is spinning. The FIFO and exclusion guarantees with the spin on
 * are covered by {@code LockManagerSpinFairnessTest}.
 * <p>
 * The tests that need a thread to stay spinning use a spin budget of minutes, so "spinning" is a state that lasts, not a
 * race: a parked thread is the proof that it did not spin. Nothing here measures time. The spin is forced off on a single
 * core, where those tests are skipped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LockManagerSpinHandOffTest {
  private static final long LONG_SPIN = TimeUnit.MINUTES.toNanos(5);

  private LockManager<String, String> lockManager;

  @AfterEach
  void tearDown() {
    if (lockManager != null)
      lockManager.close();
  }

  @Test
  void negativeSpinIsRejected() {
    assertThatThrownBy(() -> new LockManager<String, String>(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @Timeout(60)
  void zeroSpinParksAtOnce() throws Exception {
    lockManager = new LockManager<>(0);
    assertThat(lockManager.getSpinNanos()).isZero();
    lockManager.tryLock("r", "holder", 0);

    final AtomicReference<LockManager.LOCK_STATUS> result = new AtomicReference<>();
    final Thread waiter = new Thread(() -> result.set(lockManager.tryLock("r", "w", 0)), "w");
    waiter.start();
    awaitParked(waiter);

    lockManager.unlock("r", "holder");
    waiter.join(10_000);

    assertThat(result.get()).isEqualTo(LockManager.LOCK_STATUS.YES);
    assertThat(lockManager.getParkHandOffs()).isEqualTo(1);
    assertThat(lockManager.getSpinHandOffs()).isZero();
    assertThat(lockManager.getSpinsExpired()).isZero();
  }

  /**
   * The head waiter never parks while its spin lasts, and takes the hand-off while running.
   */
  @Test
  @Timeout(60)
  void headWaiterSpinsInsteadOfParking() throws Exception {
    lockManager = spinning(LONG_SPIN);
    lockManager.tryLock("r", "holder", 0);

    final AtomicReference<LockManager.LOCK_STATUS> result = new AtomicReference<>();
    final Thread waiter = new Thread(() -> result.set(lockManager.tryLock("r", "w", 0)), "w");
    waiter.start();
    awaitQueued("r", 1);
    assertStaysRunning(waiter);

    lockManager.unlock("r", "holder");
    waiter.join(10_000);

    assertThat(result.get()).isEqualTo(LockManager.LOCK_STATUS.YES);
    assertThat(lockManager.getSpinHandOffs()).isEqualTo(1);
    assertThat(lockManager.getParkHandOffs()).isZero();
    lockManager.unlock("r", "w");
  }

  /**
   * Only the head spins: the second waiter parks even with a spin budget of minutes. When the head is handed the lock,
   * the release also wakes the second waiter, now the head, which spins while the new owner works and takes the next
   * hand-off running. FIFO order holds throughout.
   */
  @Test
  @Timeout(60)
  void onlyTheHeadSpinsAndAReleaseWakesTheNextHeadEarly() throws Exception {
    lockManager = spinning(LONG_SPIN);
    lockManager.tryLock("r", "holder", 0);

    final List<String> order = new CopyOnWriteArrayList<>();
    final CountDownLatch firstMayRelease = new CountDownLatch(1);
    final Thread first = new Thread(() -> {
      if (lockManager.tryLock("r", "w1", 0) == LockManager.LOCK_STATUS.YES) {
        order.add("w1");
        try {
          firstMayRelease.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        lockManager.unlock("r", "w1");
      }
    }, "w1");
    first.start();
    awaitQueued("r", 1);

    final Thread second = new Thread(() -> {
      if (lockManager.tryLock("r", "w2", 0) == LockManager.LOCK_STATUS.YES) {
        order.add("w2");
        lockManager.unlock("r", "w2");
      }
    }, "w2");
    second.start();
    // Not the head: parks although its spin would last minutes
    awaitParked(second);

    lockManager.unlock("r", "holder");
    // w1 now owns the lock and waits for the latch; w2 was woken by that release and spins as the new head
    awaitOwner("r", "w1");
    awaitRunning(second);
    assertStaysRunning(second);

    firstMayRelease.countDown();
    first.join(10_000);
    second.join(10_000);

    assertThat(order).containsExactly("w1", "w2");
    assertThat(lockManager.getSpinHandOffs()).isEqualTo(2);
    assertThat(lockManager.getParkHandOffs()).isZero();
  }

  /**
   * A resource held, on average, for longer than the spin budget makes its head waiters park at once: the hold time of
   * the first owner (8 times the budget, so the moving average exceeds it) turns the spin off for the waiter that queues
   * after it. The first waiter, which queued before any history, spins and runs out, once.
   */
  @Test
  @Timeout(60)
  void longHoldsTurnTheSpinOff() throws Exception {
    final long spin = TimeUnit.MILLISECONDS.toNanos(20);
    lockManager = spinning(spin);
    lockManager.tryLock("r", "holder", 0);

    final CountDownLatch bMayRelease = new CountDownLatch(1);
    final Thread b = new Thread(() -> {
      if (lockManager.tryLock("r", "b", 0) == LockManager.LOCK_STATUS.YES) {
        try {
          bMayRelease.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        lockManager.unlock("r", "b");
      }
    }, "b");
    b.start();
    awaitQueued("r", 1);
    awaitParked(b);
    // Hold for well over 8 x the budget: one sample moves the average by 1/8 of it
    Thread.sleep(TimeUnit.NANOSECONDS.toMillis(spin) * 8 + 200);
    lockManager.unlock("r", "holder");
    awaitOwner("r", "b");

    final long expiredBefore = lockManager.getSpinsExpired();
    assertThat(expiredBefore).isGreaterThanOrEqualTo(1);

    final AtomicReference<LockManager.LOCK_STATUS> result = new AtomicReference<>();
    final Thread c = new Thread(() -> result.set(lockManager.tryLock("r", "c", 0)), "c");
    c.start();
    awaitQueued("r", 1);
    awaitParked(c);

    bMayRelease.countDown();
    b.join(10_000);
    c.join(10_000);

    assertThat(result.get()).isEqualTo(LockManager.LOCK_STATUS.YES);
    // c never spun: it parked at once, so no spin of its own ran out, and it took the hand-off parked
    assertThat(lockManager.getSpinsExpired()).isEqualTo(expiredBefore);
    assertThat(lockManager.getSpinHandOffs()).isZero();
    assertThat(lockManager.getParkHandOffs()).isEqualTo(2);
    lockManager.unlock("r", "c");
  }

  @Test
  @Timeout(60)
  void closeEndsASpinningWaitWithNo() throws Exception {
    lockManager = spinning(LONG_SPIN);
    lockManager.tryLock("r", "holder", 0);

    final AtomicReference<LockManager.LOCK_STATUS> result = new AtomicReference<>();
    final Thread waiter = new Thread(() -> result.set(lockManager.tryLock("r", "w", 0)), "w");
    waiter.start();
    awaitQueued("r", 1);
    awaitRunning(waiter);

    lockManager.close();
    waiter.join(10_000);

    assertThat(waiter.isAlive()).isFalse();
    assertThat(result.get()).isEqualTo(LockManager.LOCK_STATUS.NO);
  }

  /**
   * The caller's timeout bounds the spin: a waiter whose spin would last minutes still gives up at its deadline and leaves
   * the queue.
   */
  @Test
  @Timeout(60)
  void timeoutEndsASpinningWait() throws Exception {
    lockManager = spinning(LONG_SPIN);
    lockManager.tryLock("r", "holder", 0);

    final AtomicReference<LockManager.LOCK_STATUS> result = new AtomicReference<>();
    final Thread waiter = new Thread(() -> result.set(lockManager.tryLock("r", "w", 300)), "w");
    waiter.start();
    waiter.join(30_000);

    assertThat(waiter.isAlive()).isFalse();
    assertThat(result.get()).isEqualTo(LockManager.LOCK_STATUS.NO);
    assertThat(lockManager.statsSnapshot()).singleElement().satisfies(s -> assertThat(s.waiters()).isZero());
  }

  @Test
  @Timeout(60)
  void interruptEndsASpinningWaitAndKeepsTheFlag() throws Exception {
    lockManager = spinning(LONG_SPIN);
    lockManager.tryLock("r", "holder", 0);

    final AtomicReference<LockManager.LOCK_STATUS> result = new AtomicReference<>();
    final AtomicReference<Boolean> flag = new AtomicReference<>();
    final Thread waiter = new Thread(() -> {
      result.set(lockManager.tryLock("r", "w", 0));
      flag.set(Thread.currentThread().isInterrupted());
    }, "w");
    waiter.start();
    awaitQueued("r", 1);
    awaitRunning(waiter);

    waiter.interrupt();
    waiter.join(10_000);

    assertThat(result.get()).isEqualTo(LockManager.LOCK_STATUS.NO);
    assertThat(flag.get()).isTrue();
    assertThat(lockManager.statsSnapshot()).singleElement().satisfies(s -> assertThat(s.waiters()).isZero());
  }

  private static LockManager<String, String> spinning(final long spinNanos) {
    final LockManager<String, String> manager = new LockManager<>(spinNanos);
    assumeTrue(manager.getSpinNanos() > 0, "the spin is off on a single core");
    return manager;
  }

  private void awaitQueued(final String resource, final int waiters) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline) {
      for (final LockManager.LockStats s : lockManager.statsSnapshot())
        if (s.resource().equals(resource) && s.waiters() == waiters)
          return;
      Thread.sleep(1);
    }
    assertThat(lockManager.statsSnapshot()).as("%d waiters queued on '%s'", waiters, resource)
        .anySatisfy(s -> assertThat(s.waiters()).isEqualTo(waiters));
  }

  private void awaitOwner(final String resource, final String owner) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline) {
      for (final LockManager.LockStats s : lockManager.statsSnapshot())
        if (s.resource().equals(resource) && s.owner().equals(owner))
          return;
      Thread.sleep(1);
    }
    assertThat(lockManager.statsSnapshot()).as("'%s' owned by '%s'", resource, owner)
        .anySatisfy(s -> assertThat(s.owner()).isEqualTo(owner));
  }

  private static void awaitParked(final Thread t) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline && !isParked(t))
      Thread.sleep(1);
    assertThat(t.getState()).as("thread '%s' parked", t.getName()).isIn(Thread.State.WAITING, Thread.State.TIMED_WAITING);
  }

  private static void awaitRunning(final Thread t) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline && t.getState() != Thread.State.RUNNABLE)
      Thread.sleep(1);
    assertThat(t.getState()).as("thread '%s' running", t.getName()).isEqualTo(Thread.State.RUNNABLE);
  }

  /**
   * Samples the thread for a while: a waiter that parked even once in that window did not spin. Proves a state, measures
   * nothing: the spin budget of these tests outlasts the window by minutes.
   */
  private static void assertStaysRunning(final Thread t) throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      assertThat(isParked(t)).as("thread '%s' parked while it should spin", t.getName()).isFalse();
      Thread.sleep(1);
    }
  }

  private static boolean isParked(final Thread t) {
    final Thread.State s = t.getState();
    return s == Thread.State.WAITING || s == Thread.State.TIMED_WAITING;
  }
}
