/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5418: the JVM shutdown hook could make the process unkillable.
 * <p>
 * {@link ArcadeDBServer#start()} holds the lifecycle mutex for the whole startup. If Apache Ratis cannot
 * bind its gRPC port it reports the failure by calling {@code System.exit()} from that same thread;
 * {@code System.exit()} runs the shutdown hooks and waits for them, so the starting thread parks inside
 * {@code Shutdown.exit()} still holding the mutex while the hook waits for a mutex that will never be
 * released. Neither side can proceed and only SIGKILL ends the process - a plain port conflict at startup
 * was enough to trigger it, and in a test run one wedged JVM took every later test class with it.
 * <p>
 * The fix makes the hook's acquisition bounded. These tests pin that: an unavailable lock must never
 * translate into an unbounded wait.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5418ShutdownHookDeadlockTest {

  @Test
  void hookGivesUpWhenTheLifecycleLockIsHeldByAnotherThread() throws Exception {
    final ReentrantLock lock = new ReentrantLock();
    final CountDownLatch held = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);

    // Stands in for the thread parked inside start() -> System.exit(): it holds the lock and never
    // releases it while the hook runs.
    final Thread holder = new Thread(() -> {
      lock.lock();
      try {
        held.countDown();
        release.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        lock.unlock();
      }
    }, "issue5418-lock-holder");
    holder.setDaemon(true);
    holder.start();

    assertThat(held.await(10, TimeUnit.SECONDS)).as("the holder must acquire the lock first").isTrue();

    try {
      final long startedAt = System.nanoTime();
      final boolean acquired = ArcadeDBServer.awaitLifecycleLock(lock, 300);
      final long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000;

      assertThat(acquired).as("the lock is held elsewhere, so the hook must NOT acquire it").isFalse();
      // The point of the fix: it returns. Before it, this call was an unbounded park and the JVM hung.
      assertThat(elapsedMs).as("the hook must give up near its deadline, not wait forever")
          .isLessThan(10_000L);
    } finally {
      release.countDown();
      holder.join(10_000);
    }
  }

  @Test
  void aThrowingStopMustNotEscapeTheHook() {
    // Second deadlock on this path: Apache Ratis installs a global UncaughtExceptionHandler that calls
    // System.exit(). If it fires on the shutdown-hook thread, that System.exit() blocks forever on the
    // java.lang.Shutdown class monitor already held by the thread running the hooks - the JVM hangs just
    // as surely as an unbounded lock wait. So nothing may escape the hook body.
    final AtomicBoolean escaped = new AtomicBoolean(false);
    final Thread hook = new Thread(() -> {
      try {
        throw new IllegalStateException("stop() blew up during shutdown");
      } catch (final Throwable t) {
        // mirrors the production hook: swallow, log, let the exit proceed
      }
    }, "issue5418-hook-body");
    hook.setUncaughtExceptionHandler((t, e) -> escaped.set(true));
    hook.start();
    try {
      hook.join(10_000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertThat(hook.isAlive()).isFalse();
    assertThat(escaped.get())
        .as("an exception escaping a shutdown hook reaches Ratis's exit-calling handler and hangs the JVM")
        .isFalse();
  }

  @Test
  void hookAcquiresTheLockWhenItIsFree() {
    // The normal path - a shutdown signal on a healthy server - must be unaffected: the hook takes the
    // lock immediately and performs a full graceful stop.
    final ReentrantLock lock = new ReentrantLock();
    assertThat(ArcadeDBServer.awaitLifecycleLock(lock, 60_000)).isTrue();
    assertThat(lock.isHeldByCurrentThread()).as("the caller owns the lock and must unlock it").isTrue();
    lock.unlock();
  }

  @Test
  void reentrantAcquisitionSucceedsSoStartCanCallStop() {
    // start() calls stop() on its own failure path, which re-enters the lock. A non-reentrant mutex
    // would deadlock a failing startup against itself.
    final ReentrantLock lock = new ReentrantLock();
    lock.lock();
    try {
      assertThat(ArcadeDBServer.awaitLifecycleLock(lock, 0)).as("the same thread must re-enter").isTrue();
      assertThat(lock.getHoldCount()).isEqualTo(2);
      lock.unlock();
    } finally {
      lock.unlock();
    }
  }

  @Test
  void interruptedAcquisitionGivesUpAndPreservesTheFlag() throws Exception {
    // A hook thread interrupted while waiting must not swallow the interrupt, and must still return so
    // the JVM can finish exiting.
    final ReentrantLock lock = new ReentrantLock();
    final CountDownLatch held = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean acquired = new AtomicBoolean(true);
    final AtomicBoolean interruptFlagSurvived = new AtomicBoolean(false);

    final Thread holder = new Thread(() -> {
      lock.lock();
      try {
        held.countDown();
        release.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        lock.unlock();
      }
    }, "issue5418-lock-holder-2");
    holder.setDaemon(true);
    holder.start();
    assertThat(held.await(10, TimeUnit.SECONDS)).isTrue();

    final Thread waiter = new Thread(() -> {
      acquired.set(ArcadeDBServer.awaitLifecycleLock(lock, 60_000));
      interruptFlagSurvived.set(Thread.currentThread().isInterrupted());
    }, "issue5418-waiter");
    waiter.start();

    // Give the waiter time to enter tryLock, then interrupt it.
    Thread.sleep(200);
    waiter.interrupt();
    waiter.join(10_000);

    assertThat(waiter.isAlive()).as("an interrupted hook must return, not hang").isFalse();
    assertThat(acquired.get()).isFalse();
    assertThat(interruptFlagSurvived.get()).as("the interrupt flag must be restored for the caller").isTrue();

    release.countDown();
    holder.join(10_000);
  }
}
