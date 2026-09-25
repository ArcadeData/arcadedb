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
package com.arcadedb.server.ha.raft;

import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8364, the follow-up of #8182.
 * <p>
 * #8182 made {@link ArcadeStateMachine#close()} a bounded termination barrier for the two executors it owns directly.
 * The three helpers it closes afterwards each own a single-worker executor too, and their {@code close()} was
 * {@code shutdownNow()} only: it interrupts the running task and returns without waiting for it, so a task past its
 * last interruption point outlived the state machine's close - the deleter still removing entries under the server's
 * database directory, the seeder still submitting security documents cluster-wide, the catch-up still applying them.
 * <p>
 * Each test holds a helper's task past its interruption point while the close runs, and asserts the task has returned
 * by the time the close does: once through the helper's own close-and-wait, once through the state machine's close.
 */
class Issue8364StateMachineCloseAwaitsHelperExecutorsTest {

  /** How long a held task stays inside its work, ignoring interrupts. Well inside CLOSE_AWAIT_MS. */
  private static final long HOLD_MS = 500L;

  private ArcadeStateMachine sm;

  @AfterEach
  void closeStateMachine() throws IOException {
    Thread.interrupted();
    if (sm != null)
      sm.close();
  }

  private static long closeDeadline() {
    return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ArcadeStateMachine.CLOSE_AWAIT_MS);
  }

  /** Sleeps through interrupts: a task past its last interruption point is exactly what shutdownNow() cannot stop. */
  private static void holdIgnoringInterrupts(final long ms) {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ms);
    boolean interrupted = false;
    long left;
    while ((left = deadline - System.nanoTime()) > 0)
      try {
        TimeUnit.NANOSECONDS.sleep(left);
      } catch (final InterruptedException e) {
        interrupted = true;
      }
    if (interrupted)
      Thread.currentThread().interrupt();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // DeferredDatabaseDeleter
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * A deleter whose executor is running a task that holds past its interruption point. The executor is handed to the
   * deleter, which owns it from then on - the same as the production one, whose recursive delete does not look at the
   * interrupt flag at all.
   */
  private static DeferredDatabaseDeleter deleterRunning(final CountDownLatch entered, final AtomicBoolean finished) {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final DeferredDatabaseDeleter deleter = new DeferredDatabaseDeleter(executor);
    executor.execute(() -> {
      entered.countDown();
      holdIgnoringInterrupts(HOLD_MS);
      finished.set(true);
    });
    return deleter;
  }

  @Test
  void deleterCloseWaitsForTheRunningDeletion() throws Exception {
    final CountDownLatch entered = new CountDownLatch(1);
    final AtomicBoolean finished = new AtomicBoolean();
    final DeferredDatabaseDeleter deleter = deleterRunning(entered, finished);
    assertThat(entered.await(30, TimeUnit.SECONDS)).as("the deletion must start").isTrue();

    deleter.close();
    assertThat(deleter.awaitTermination(closeDeadline())).as("the wait was not interrupted").isTrue();

    assertThat(finished.get()).as("the running deletion must have returned when the wait does").isTrue();
  }

  @Test
  void stateMachineCloseWaitsForTheDeletersRunningDeletion() throws Exception {
    final CountDownLatch entered = new CountDownLatch(1);
    final AtomicBoolean finished = new AtomicBoolean();
    sm = new ArcadeStateMachine();
    sm.setDeferredDatabaseDeleter(deleterRunning(entered, finished));
    assertThat(entered.await(30, TimeUnit.SECONDS)).as("the deletion must start").isTrue();

    sm.close();

    assertThat(finished.get())
        .as("the deleter must not still be removing entries under the database directory after close() returned")
        .isTrue();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // MembershipSecuritySeeder
  // ---------------------------------------------------------------------------------------------------------------

  /** A production-shaped seeder - it builds and owns its worker - whose seed holds past its interruption point. */
  private static MembershipSecuritySeeder seederWhoseSeedHolds(final CountDownLatch entered,
      final AtomicBoolean finished) {
    return seederWhoseSeedHolds(entered, finished, HOLD_MS);
  }

  private static MembershipSecuritySeeder seederWhoseSeedHolds(final CountDownLatch entered,
      final AtomicBoolean finished, final long holdMs) {
    return new MembershipSecuritySeeder(() -> true, () -> 1_000L, retryBudgetMs -> {
      entered.countDown();
      holdIgnoringInterrupts(holdMs);
      finished.set(true);
      return List.of();
    });
  }

  @Test
  void seederCloseWaitsForTheRunningSeed() throws Exception {
    final CountDownLatch entered = new CountDownLatch(1);
    final AtomicBoolean finished = new AtomicBoolean();
    final MembershipSecuritySeeder seeder = seederWhoseSeedHolds(entered, finished);
    seeder.scheduleForTest("a test seed");
    assertThat(entered.await(30, TimeUnit.SECONDS)).as("the seed must start").isTrue();

    seeder.close();
    assertThat(seeder.awaitTermination(closeDeadline())).as("the wait was not interrupted").isTrue();

    assertThat(finished.get()).as("the running seed must have returned when the wait does").isTrue();
  }

  @Test
  void stateMachineCloseWaitsForTheSeedersRunningSeed() throws Exception {
    final CountDownLatch entered = new CountDownLatch(1);
    final AtomicBoolean finished = new AtomicBoolean();
    final MembershipSecuritySeeder seeder = seederWhoseSeedHolds(entered, finished);
    sm = new ArcadeStateMachine();
    sm.setMembershipSecuritySeederForTesting(seeder);
    seeder.scheduleForTest("a test seed");
    assertThat(entered.await(30, TimeUnit.SECONDS)).as("the seed must start").isTrue();

    sm.close();

    assertThat(finished.get())
        .as("the seeder must not still be submitting security documents cluster-wide after close() returned")
        .isTrue();
  }

  /** A seeder on an executor it does not own never shut that executor down, so it has nothing to wait for. */
  @Test
  void seederOnASuppliedExecutorDoesNotWaitForIt() {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final CountDownLatch release = new CountDownLatch(1);
      executor.execute(() -> {
        try {
          release.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      final MembershipSecuritySeeder seeder = new MembershipSecuritySeeder(() -> true, () -> 1_000L,
          retryBudgetMs -> List.of(), executor);
      seeder.close();

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThat(seeder.awaitTermination(closeDeadline())).isTrue();
      stopwatch.assertGaveUpWithin(ArcadeStateMachine.CLOSE_AWAIT_MS / 2,
          "a seeder that returns at once for an executor it does not own from one that waits out the whole bound");
      assertThat(executor.isShutdown()).as("the supplied executor is not the seeder's to stop").isFalse();
      release.countDown();
    } finally {
      executor.shutdownNow();
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // SecurityCatchUp
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * A server whose Raft plugin lookup - the first thing a catch-up attempt does after a snapshot install - runs
   * {@code onCatchUpThread} and then reports no plugin, so the attempt ends without dialling anyone and releases the
   * once-per-start request on its way out: the last thing the task does is observable.
   */
  private static ArcadeDBServer serverWhoseCatchUpRuns(final Runnable onCatchUpThread) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getHA()).thenAnswer(invocation -> {
      if (Thread.currentThread().getName().equals(SecurityCatchUp.THREAD_NAME))
        onCatchUpThread.run();
      return null;
    });
    return server;
  }

  private static void startCatchUpThatHolds(final SecurityCatchUp catchUp, final CountDownLatch entered)
      throws InterruptedException {
    catchUp.afterSnapshotInstall(serverWhoseCatchUpRuns(() -> {
      entered.countDown();
      holdIgnoringInterrupts(HOLD_MS);
    }), mock(RaftHAServer.class));
    assertThat(entered.await(30, TimeUnit.SECONDS)).as("the catch-up must start").isTrue();
    assertThat(catchUp.hasRequestedSinceStart()).as("the catch-up is in flight, holding the request").isTrue();
  }

  @Test
  void catchUpCloseWaitsForTheRunningAttempt() throws Exception {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      startCatchUpThatHolds(catchUp, new CountDownLatch(1));

      catchUp.close();
      assertThat(catchUp.awaitTermination(closeDeadline())).as("the wait was not interrupted").isTrue();

      assertThat(catchUp.hasRequestedSinceStart())
          .as("the running attempt must have run to its end - releasing the request it asked nobody - when the wait "
              + "returns")
          .isFalse();
    }
  }

  @Test
  void stateMachineCloseWaitsForTheRunningCatchUp() throws Exception {
    sm = new ArcadeStateMachine();
    final SecurityCatchUp catchUp = sm.getSecurityCatchUp();
    startCatchUpThatHolds(catchUp, new CountDownLatch(1));

    sm.close();

    assertThat(catchUp.hasRequestedSinceStart())
        .as("the catch-up must not still be working on this node's security documents after close() returned")
        .isFalse();
  }

  /**
   * The self-wait guard on a helper's worker: a catch-up task that ends up closing its own state machine must not wait
   * out the whole bound for the one thread that cannot terminate while it waits - itself.
   */
  @Test
  void closeCalledFromTheCatchUpThreadDoesNotWaitForItself() throws Exception {
    sm = new ArcadeStateMachine();
    final AtomicReference<Throwable> outcome = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(1);
    sm.getSecurityCatchUp().afterSnapshotInstall(serverWhoseCatchUpRuns(() -> {
      try {
        final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
        sm.close();
        stopwatch.assertGaveUpWithin(ArcadeStateMachine.CLOSE_AWAIT_MS / 2,
            "a close that skips waiting for its own thread from one that waits out the whole close bound");
      } catch (final Throwable t) {
        outcome.set(t);
      } finally {
        done.countDown();
      }
    }), mock(RaftHAServer.class));

    assertThat(done.await(30, TimeUnit.SECONDS)).as("the catch-up must reach its plugin lookup and close").isTrue();
    if (outcome.get() instanceof AssertionError e)
      throw e;
    assertThat(outcome.get()).as("close() from the catch-up thread must not throw").isNull();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // The interrupt flag
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * The note left on this issue by the review of PR #8366: {@code close()} restored the caller's interrupt flag after
   * its own two executor waits and BEFORE the helpers' close, so an interrupted caller - a lifecycle task closing its
   * own state machine is interrupted by close()'s own shutdownNow() - would make every helper wait return at once.
   * The flag must be taken off for all of the waits and put back only after the last one.
   * <p>
   * The seed holds three times longer than the other two tasks on purpose. A flag restored early short-circuits only
   * the FIRST helper wait it meets - the {@code InterruptedException} that wait throws clears it again - so with
   * equal holds the later waits would cover for it and the test would pass against the defect.
   */
  @Test
  void anInterruptedCallerStillWaitsForTheHelpersAndGetsItsInterruptBack() throws Exception {
    final CountDownLatch deletionEntered = new CountDownLatch(1);
    final AtomicBoolean deletionFinished = new AtomicBoolean();
    final CountDownLatch seedEntered = new CountDownLatch(1);
    final AtomicBoolean seedFinished = new AtomicBoolean();
    final CountDownLatch catchUpEntered = new CountDownLatch(1);

    sm = new ArcadeStateMachine();
    sm.setDeferredDatabaseDeleter(deleterRunning(deletionEntered, deletionFinished));
    final MembershipSecuritySeeder seeder = seederWhoseSeedHolds(seedEntered, seedFinished, HOLD_MS * 3);
    sm.setMembershipSecuritySeederForTesting(seeder);
    seeder.scheduleForTest("a test seed");
    final SecurityCatchUp catchUp = sm.getSecurityCatchUp();
    startCatchUpThatHolds(catchUp, catchUpEntered);
    assertThat(deletionEntered.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(seedEntered.await(30, TimeUnit.SECONDS)).isTrue();

    Thread.currentThread().interrupt();
    try {
      sm.close();
    } finally {
      assertThat(Thread.interrupted()).as("the caller's interrupt is handed back once close() is done").isTrue();
    }

    assertThat(seedFinished.get()).as("an interrupted caller must still wait for the seeder").isTrue();
    assertThat(catchUp.hasRequestedSinceStart()).as("an interrupted caller must still wait for the catch-up").isFalse();
    assertThat(deletionFinished.get()).as("an interrupted caller must still wait for the deleter").isTrue();
  }
}
