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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The server's JVM shutdown hook: it cannot make the process unkillable, it points the operator at the right setting, and
 * it owns the closing of its databases.
 */
class ServerShutdownHookTest {
  /*
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
  // Regression: #5418 the JVM shutdown hook could make the process unkillable
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
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      final boolean acquired = ArcadeDBServer.awaitLifecycleLock(lock, 300);

      assertThat(acquired).as("the lock is held elsewhere, so the hook must NOT acquire it").isFalse();
      // The point of the fix: it returns. Before it, this call was an unbounded park and the JVM hung.
      stopwatch.assertGaveUpWithin(10_000L, "a 300ms bounded acquisition from the unbounded park that hung the JVM");
    } finally {
      release.countDown();
      holder.join(10_000);
    }
  }

  // Regression: #5418 the JVM shutdown hook could make the process unkillable
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

  // Regression: #5418 the JVM shutdown hook could make the process unkillable
  @Test
  void hookAcquiresTheLockWhenItIsFree() {
    // The normal path - a shutdown signal on a healthy server - must be unaffected: the hook takes the
    // lock immediately and performs a full graceful stop.
    final ReentrantLock lock = new ReentrantLock();
    assertThat(ArcadeDBServer.awaitLifecycleLock(lock, 60_000)).isTrue();
    assertThat(lock.isHeldByCurrentThread()).as("the caller owns the lock and must unlock it").isTrue();
    lock.unlock();
  }

  // Regression: #5418 the JVM shutdown hook could make the process unkillable
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

  // Regression: #5418 the JVM shutdown hook could make the process unkillable
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

  /*
   * Regression test for issue #6981: the shutdown-hook WARNING (and the setting description) pointed the
   * operator at {@code arcadedb.server.shutdownTimeout} on the one path that ignores it.
   * <p>
   * While the server is {@code STARTING} the hook waits a fixed 2000ms, not the configured timeout; an
   * operator who followed the "raise arcadedb.server.shutdownTimeout" advice on that branch changed a value
   * that provably had no effect. The hint must name the setting only on the branch it governs.
   */
  // Regression: #6981 the shutdown-hook warning pointed at shutdownTimeout on the path that ignores it
  @Test
  void startingWarningMustNotAdviseRaisingTheSettingItIgnores() {
    final String message = ArcadeDBServer.shutdownHookLockTimeoutWarning(2_000, ArcadeDBServer.STATUS.STARTING,
        ArcadeDBServer.ShutdownHookBound.STARTING_NO_DATABASES);

    assertThat(message).contains("2000ms").contains("STARTING");
    assertThat(message)
        .as("on the STARTING branch the configured timeout is not in effect, so 'raise it' advice is misleading")
        .doesNotContain("Raise arcadedb.server.shutdownTimeout");
    assertThat(message)
        .as("the operator should learn the wait is a fixed bound the setting does not govern")
        .contains("not governed by arcadedb.server.shutdownTimeout");
  }

  // Regression: #6981 the shutdown-hook warning pointed at shutdownTimeout on the path that ignores it
  @Test
  void warningOutsideStartingKeepsTheRaiseHint() {
    for (final ArcadeDBServer.STATUS status : new ArcadeDBServer.STATUS[] { ArcadeDBServer.STATUS.OFFLINE,
        ArcadeDBServer.STATUS.ONLINE, ArcadeDBServer.STATUS.SHUTTING_DOWN }) {
      final String message = ArcadeDBServer.shutdownHookLockTimeoutWarning(60_000, status,
          ArcadeDBServer.ShutdownHookBound.CONFIGURED);

      assertThat(message).contains("60000ms").contains(status.toString());
      assertThat(message)
          .as("on every non-STARTING branch the setting IS the bound, so the advice stays")
          .contains("Raise arcadedb.server.shutdownTimeout if a legitimate shutdown needs longer.");
    }
  }

  // Regression: #6981 the shutdown-hook warning pointed at shutdownTimeout on the path that ignores it
  @Test
  void settingDescriptionMustNotClaimToCoverStart() {
    final String description = GlobalConfiguration.SERVER_SHUTDOWN_TIMEOUT.getDescription();

    assertThat(description)
        .as("the setting does not bound the shutdown-hook wait during start(), so its description must not say so")
        .doesNotContain("start()/stop()");
    assertThat(description).contains("this setting does not govern");
    assertThat(description)
        .as("the configured bound DOES apply once the status left STARTING, even before start() releases the lock")
        .contains("the tail of start() after the status has already turned ONLINE");
  }

  // Regression: #6981 the shutdown-hook warning pointed at shutdownTimeout on the path that ignores it
  @Test
  void settingDescriptionMustQuoteTheSameStartingBoundTheHookApplies() {
    // The description spells the STARTING bound out as a literal, because GlobalConfiguration lives in the
    // engine module and cannot see the server's SHUTDOWN_HOOK_STARTING_TIMEOUT_MS. Nothing but this test
    // keeps the two in step, and a description quoting a bound the hook no longer applies is exactly the
    // defect #6981 reports - one module edit away from coming back.
    final Matcher bound = Pattern.compile("fixed (\\d+)ms bound")
        .matcher(ArcadeDBServer.shutdownHookLockTimeoutWarning(2_000, ArcadeDBServer.STATUS.STARTING,
        ArcadeDBServer.ShutdownHookBound.STARTING_NO_DATABASES));

    assertThat(bound.find()).as("the STARTING warning must state the fixed bound it applied").isTrue();

    assertThat(GlobalConfiguration.SERVER_SHUTDOWN_TIMEOUT.getDescription())
        .as("the setting description quotes the STARTING bound as a literal: it must match the constant the hook uses")
        .contains("fixed " + bound.group(1) + "ms bound");
  }

  /*
   * Issue #9548: the server's JVM shutdown hook stops the Raft HA service before it closes the databases, and the engine's
   * own hook, which the JVM runs at the same time, closed them in between - a leader committing across a rolling restart
   * then quarantined its own database. The server registers its hook as the owner of its databases, so the engine's hook
   * waits for it ({@code DatabaseOpenCloseLockTest.OwningShutdownHook} pins the wait itself).
   */
  // Regression: #9548 the engine's own hook must not close the databases before the server's hook stops HA
  @Test
  void theServerRegistersItsShutdownHookAsTheOwnerOfItsDatabases() {
    final ArcadeDBServer server = new ArcadeDBServer(configuration());
    final Thread hook = server.getShutdownHook();
    try {
      assertThat(hook).isNotNull();
      assertThat(hook.getName()).isEqualTo("arcadedb-shutdown-hook");
      assertThat(hook.getState()).as("the hook only runs at the JVM shutdown").isEqualTo(Thread.State.NEW);
      assertThat(DatabaseFactory.isOwningShutdownHook(hook)).isTrue();
    } finally {
      server.stop();
    }
  }

  // Regression: #9548 the engine's own hook must not close the databases before the server's hook stops HA
  /**
   * A stopped server has closed its databases, so its hook has nothing left to order. Left in place,
   * the runtime and the engine's registry would keep one hook, and the whole server it captures, per instance created in
   * the JVM. A start() of the same instance puts it back.
   */
  @Test
  void aStopTakesTheHookAwayAndAStartPutsItBack() {
    final ArcadeDBServer server = new ArcadeDBServer(configuration());
    final Thread hook = server.getShutdownHook();
    assertThat(DatabaseFactory.isOwningShutdownHook(hook)).isTrue();

    server.stop();
    assertThat(DatabaseFactory.isOwningShutdownHook(hook)).isFalse();
    assertThat(Runtime.getRuntime().removeShutdownHook(hook)).as("no longer added to the runtime").isFalse();

    // What start() does first, before bringing anything up
    server.installShutdownHook();
    try {
      assertThat(DatabaseFactory.isOwningShutdownHook(hook)).isTrue();
    } finally {
      server.stop();
    }
    assertThat(DatabaseFactory.isOwningShutdownHook(hook)).isFalse();
  }

  private static ContextConfiguration configuration() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "issue9548");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases");
    return config;
  }
}
