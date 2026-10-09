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
package com.arcadedb.database;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9548: the JVM runs every shutdown hook concurrently, and the engine's hook closed every open database while the
 * server's own hook was still stopping the Raft HA service that writes into them. A leader committing across the
 * shutdown then failed to publish an entry the cluster had committed, and quarantined its own database. The engine's
 * hook now waits for the hooks registered as owning their databases before it closes anything.
 */
class Issue9548OwningShutdownHookTest {

  /** The core of the fix: a registered hook that is still running holds the engine's hook back until it is done. */
  @Test
  @Timeout(30)
  void theEngineHookWaitsForARunningOwningHook() throws Exception {
    final CountDownLatch ownerRunning = new CountDownLatch(1);
    final CountDownLatch ownerMayFinish = new CountDownLatch(1);
    final Thread owner = new Thread(() -> {
      ownerRunning.countDown();
      try {
        ownerMayFinish.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, "issue9548-owning-hook");
    DatabaseFactory.registerOwningShutdownHook(owner);
    try {
      owner.start();
      assertThat(ownerRunning.await(10, TimeUnit.SECONDS)).isTrue();

      final AtomicBoolean ownerDoneWhenReleased = new AtomicBoolean();
      final Thread engineHook = new Thread(() -> {
        DatabaseFactory.awaitOwningShutdownHooks(0);
        ownerDoneWhenReleased.set(owner.getState() == Thread.State.TERMINATED);
      }, "issue9548-engine-hook");
      engineHook.start();

      // A short wait expected to time out: a stall only makes it more true.
      engineHook.join(300);
      assertThat(engineHook.isAlive()).as("the engine hook must not proceed while the owning hook still runs").isTrue();

      ownerMayFinish.countDown();
      engineHook.join(10_000);
      assertThat(engineHook.isAlive()).isFalse();
      assertThat(ownerDoneWhenReleased.get()).as("released only once the owning hook had finished").isTrue();
    } finally {
      ownerMayFinish.countDown();
      DatabaseFactory.unregisterOwningShutdownHook(owner);
    }
  }

  /**
   * The JVM starts its hooks together and in no order, so the engine's hook can run before the owning one has started:
   * an unstarted hook is waited for too, but only for the grace, so a hook removed from the runtime cannot hold the exit.
   */
  @Test
  @Timeout(30)
  void anUnstartedOwningHookIsWaitedForOnlyForTheGrace() {
    final Thread neverStarted = new Thread(() -> { }, "issue9548-never-started");
    DatabaseFactory.registerOwningShutdownHook(neverStarted);
    try {
      DatabaseFactory.awaitOwningShutdownHooks(50);
      assertThat(neverStarted.getState()).isEqualTo(Thread.State.NEW);
    } finally {
      DatabaseFactory.unregisterOwningShutdownHook(neverStarted);
    }
  }

  /** An owning hook that starts within the grace is then waited for to the end. */
  @Test
  @Timeout(30)
  void anOwningHookStartedWithinTheGraceIsWaitedForToTheEnd() throws Exception {
    final CountDownLatch ownerMayFinish = new CountDownLatch(1);
    final Thread owner = new Thread(() -> {
      try {
        ownerMayFinish.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, "issue9548-late-owning-hook");
    DatabaseFactory.registerOwningShutdownHook(owner);
    try {
      final AtomicBoolean ownerDoneWhenReleased = new AtomicBoolean();
      final Thread engineHook = new Thread(() -> {
        DatabaseFactory.awaitOwningShutdownHooks(5_000);
        ownerDoneWhenReleased.set(owner.getState() == Thread.State.TERMINATED);
      }, "issue9548-engine-hook-late");
      engineHook.start();
      engineHook.join(100);
      owner.start();
      engineHook.join(300);
      assertThat(engineHook.isAlive()).isTrue();

      ownerMayFinish.countDown();
      engineHook.join(10_000);
      assertThat(engineHook.isAlive()).isFalse();
      assertThat(ownerDoneWhenReleased.get()).isTrue();
    } finally {
      ownerMayFinish.countDown();
      DatabaseFactory.unregisterOwningShutdownHook(owner);
    }
  }

  /**
   * A hook that already finished, and the calling thread itself, are never waited for: joining itself, the caller would
   * wait forever, which the timeout turns into a failure.
   */
  @Test
  @Timeout(30)
  void aFinishedHookAndTheCallerItselfAreNotWaitedFor() throws Exception {
    final Thread finished = new Thread(() -> { }, "issue9548-finished");
    finished.start();
    finished.join();
    DatabaseFactory.registerOwningShutdownHook(finished);
    DatabaseFactory.registerOwningShutdownHook(Thread.currentThread());
    try {
      DatabaseFactory.awaitOwningShutdownHooks(0);
      assertThat(DatabaseFactory.isOwningShutdownHook(finished)).isTrue();
    } finally {
      DatabaseFactory.unregisterOwningShutdownHook(finished);
      DatabaseFactory.unregisterOwningShutdownHook(Thread.currentThread());
    }
    assertThat(DatabaseFactory.isOwningShutdownHook(finished)).isFalse();
  }
}
