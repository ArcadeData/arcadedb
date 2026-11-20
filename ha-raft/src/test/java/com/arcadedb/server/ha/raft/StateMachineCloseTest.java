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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.utility.StallAwareStopwatch;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Closing and applying on a state machine that is shutting down: the close waits for lifecycle tasks and helper executors, a dead
 * updater is noticed, and an apply on a database closed under it during shutdown leaves the node consistent.
 */
class StateMachineCloseTest {

  /**
   * Regression test for issue #8182 (reported as #8143 and #7969).
   * <p>
   * {@code ArcadeStateMachineMissingDatabaseBootstrapTest} failed in JUnit's {@code @TempDir} teardown, never in an
   * assertion: its {@code @AfterEach} closed the state machine, but {@link ArcadeStateMachine#close()} only called
   * {@code shutdownNow()} on its executors. That interrupts a running task and does not wait for it, so the bootstrap
   * reinstall retry already running on the {@code lifecycleExecutor} kept creating and deleting
   * {@code <db>/.snapshot-new} and the {@code .snapshot-pending} marker while JUnit was walking the directory to delete
   * it - a {@code DirectoryNotEmptyException} on a class whose every assertion had passed.
   * <p>
   * The fix makes {@code close()} a termination barrier, bounded: when it returns, no task it owned is still writing
   * under the database directory. These tests drive the real retry path and hold the retry inside the install, past
   * any interruption point, while {@code close()} runs.
   */
  @Nested
  class StateMachineCloseAwaitsLifecycleTasks {

    private static final RaftPeerId LEADER = RaftPeerId.valueOf("leader-peer");
    private static final RaftPeerId LOCAL  = RaftPeerId.valueOf("local-peer");

    private static final String DB_NAME     = "wiped-and-resynced";
    private static final long   ENTRY_INDEX = 50L;
    /** How long the stubbed retry stays inside the install, ignoring interrupts. */
    private static final long   HOLD_MS     = 500L;

    @TempDir
    private Path serverDir;

    private ArcadeStateMachine sm;

    @AfterEach
    void closeStateMachine() throws IOException {
      if (sm != null)
        sm.close();
    }

    /**
     * A server on which the database was applied in a previous session and is gone now, so the bootstrap entry takes
     * the reinstall arm. The first install runs on the caller and fails (no leader); the retry it schedules runs on the
     * lifecycle thread, where {@code onLifecycleThread} runs as the first thing the install does. It holds no databases
     * and auto-acquire is off, so a leader-initiated install reconciles nothing; with the leader's snapshot-marker read
     * answered locally ({@link NoNetworkMarkerReconciler}, issue #8374) it completes without dialling anyone.
     */
    private ArcadeDBServer serverWhoseRetryRuns(final Runnable onLifecycleThread) {
      return serverWhoseRetryRuns(serverDir, onLifecycleThread);
    }

    private static FakeArcadeDBServer serverWhoseRetryRuns(final Path databaseDirectory, final Runnable onLifecycleThread) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databaseDirectory.toString());
      config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
      config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
      config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
      config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.returns("getConfiguration", config);

      // SnapshotInstaller.install asks for the backup coordinator before it touches the filesystem, so this is the
      // first thing the retry does. A null coordinator is tolerated there (no maintenance slot to take).
      server.on("getBackupCoordinator", args -> {
        if (Thread.currentThread().getName().equals(ArcadeStateMachine.LIFECYCLE_THREAD_NAME))
          onLifecycleThread.run();
        return null;
      });
      return server;
    }

    /**
     * A follower's view of a leader whose address resolution - the first thing a leader-initiated install does on the
     * snapshot-install thread - runs {@code onInstallThread}.
     */
    private static FakeRaftHAServer raftHAWhoseInstallRuns(final Runnable onInstallThread) {
      final FakeRaftHAServer raftHA = FakeRaftHAServer.detached();
      raftHA.leader(false);
      raftHA.localPeerId(LOCAL);
      raftHA.leaderId(LEADER);
      raftHA.clusterToken("cluster-token");
      raftHA.on("getUnambiguousPeerHttpAddress", args -> {
        if (!LEADER.equals(args[0]))
          return null;
        // Only there: the bootstrap install resolves the leader through the same accessor, on other threads.
        if (Thread.currentThread().getName().equals(ArcadeStateMachine.SNAPSHOT_INSTALL_THREAD_NAME))
          onInstallThread.run();
        return "leader-host:2480";
      });
      raftHA.localHttpAddress("local-host:2480");
      raftHA.peerHttpsAddress(LEADER, null);
      return raftHA;
    }

    /** A state machine initialized against Ratis storage, which a leader-initiated install needs. */
    private RaftStorage initializeStateMachine(final ArcadeDBServer server, final RaftHAServer raftHA,
        final Path raftDirectory) throws IOException {
      final RaftStorage storage = RaftStorage.newBuilder()
          .setDirectory(raftDirectory.toFile())
          .setOption(RaftStorage.StartupOption.FORMAT)
          .build();
      sm = new ArcadeStateMachine();
      sm.setServer(server);
      sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);
      try {
        NoNetworkMarkerReconciler.installInto(sm, server);
      } catch (final Exception e) {
        throw new IOException(e);
      }
      sm.setRaftHAServer(raftHA);
      return storage;
    }

    private void applyEntryThatSchedulesTheRetry() throws Exception {
      applyEntryThatSchedulesTheRetry(sm);
    }

    private static void applyEntryThatSchedulesTheRetry(final ArcadeStateMachine target) throws Exception {
      target.writePersistedAppliedIndex(ENTRY_INDEX, DB_NAME);
      final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_NAME, "0".repeat(64), 7L);
      target.applyBootstrapFingerprintEntry(RaftLogEntryCodec.decode(encoded), ENTRY_INDEX);
    }

    private void applyEntryThatSchedulesTheRetry(final ArcadeDBServer server) throws Exception {
      sm = new ArcadeStateMachine();
      sm.setServer(server);
      applyEntryThatSchedulesTheRetry();
    }

    private CompletableFuture<TermIndex> startLeaderInstall() {
      return sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(3L, 10L));
    }

    /**
     * Runs {@code sm.close()} on the current (executor) thread, asserting there that it did not wait out the close bound
     * for itself, and hands any failure back to the test thread through {@code outcome}.
     */
    private void closeFromWithin(final AtomicReference<Throwable> outcome, final CountDownLatch done,
        final Runnable afterClose) {
      try {
        final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
        sm.close();
        stopwatch.assertGaveUpWithin(ArcadeStateMachine.CLOSE_AWAIT_MS / 2,
            "a close that skips waiting for its own thread from one that waits out the whole close bound");
        afterClose.run();
      } catch (final Throwable t) {
        outcome.set(t);
      } finally {
        done.countDown();
      }
    }

    private static void rethrow(final AtomicReference<Throwable> outcome) {
      if (outcome.get() instanceof AssertionError e)
        throw e;
      assertThat(outcome.get()).as("close() from an executor thread must not throw").isNull();
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

    /**
     * The defect. Before the fix {@code close()} returned while the retry was still inside the install, so the install
     * went on to create {@code .snapshot-new} and the pending marker after the caller - JUnit's teardown in the flaky
     * test - believed the state machine was quiet. With the barrier, the whole retry, the durable mark it records on
     * failure included, has finished by the time {@code close()} returns.
     */
    // Regression for #8182: StateMachineCloseAwaitsLifecycleTasks
    @Test
    void closeReturnsOnlyAfterTheRunningBootstrapRetryHasFinished() throws Exception {
      final CountDownLatch retryEntered = new CountDownLatch(1);
      applyEntryThatSchedulesTheRetry(serverWhoseRetryRuns(() -> {
        retryEntered.countDown();
        holdIgnoringInterrupts(HOLD_MS);
      }));

      assertThat(retryEntered.await(30, TimeUnit.SECONDS)).as("the retry must reach the install").isTrue();

      sm.close();

      assertThat(sm.getBootstrapUnreconciledDatabases())
          .as("the retry must have run to its end - the durable mark it records when the install fails - before close() "
              + "returned, not be left writing under the database directory after it")
          .contains(DB_NAME);
      // The last thing the retry does, in its finally: release the readiness holder the apply thread handed it.
      // Asserted rather than the files it staged, because an install interrupted by shutdownNow() can legitimately
      // leave its staging behind for startup recovery - what it must not do is still be touching them.
      assertThat(sm.getBootstrapInstallsInFlight())
          .as("the retry must have released its holder, i.e. returned, before close() returned")
          .isEmpty();
    }

    /**
     * The barrier must not turn into a self-wait. A task on the lifecycle thread that ends up closing its own state
     * machine (a stop or a Ratis restart driven from a lifecycle task) must not wait out the whole close bound for the
     * one thread that can never terminate while it is waiting: itself.
     */
    // Regression for #8182: StateMachineCloseAwaitsLifecycleTasks
    @Test
    void closeCalledFromTheLifecycleThreadDoesNotWaitForItself() throws Exception {
      final AtomicReference<Throwable> outcome = new AtomicReference<>();
      final AtomicBoolean closedFromWithin = new AtomicBoolean();
      final CountDownLatch done = new CountDownLatch(1);

      applyEntryThatSchedulesTheRetry(serverWhoseRetryRuns(() -> closeFromWithin(outcome, done,
          () -> closedFromWithin.set(true))));

      assertThat(done.await(30, TimeUnit.SECONDS)).as("the retry must reach the install and close").isTrue();
      rethrow(outcome);
      assertThat(closedFromWithin.get()).isTrue();
    }

    /**
     * The same barrier on the other executor {@code close()} owns: a leader-initiated snapshot install runs on
     * {@code snapshotInstallExecutor}, writes into the same database directories, and was left running by
     * {@code shutdownNow()} in exactly the same way. The install is held inside its source resolution, past any
     * interruption point, while {@code close()} runs; the future Ratis waits on completes only when the task returns.
     */
    // Regression for #8182: StateMachineCloseAwaitsLifecycleTasks
    @Test
    void closeReturnsOnlyAfterTheRunningLeaderSnapshotInstallHasFinished(@TempDir final Path raftDirectory)
        throws Exception {
      final CountDownLatch installEntered = new CountDownLatch(1);
      final RaftStorage storage = initializeStateMachine(serverWhoseRetryRuns(() -> {
      }), raftHAWhoseInstallRuns(() -> {
        installEntered.countDown();
        holdIgnoringInterrupts(HOLD_MS);
      }), raftDirectory);
      try {
        final CompletableFuture<TermIndex> install = startLeaderInstall();
        assertThat(installEntered.await(30, TimeUnit.SECONDS)).as("the install must reach its source resolution").isTrue();

        sm.close();

        assertThat(install.isDone())
            .as("the leader-initiated install must have returned, one way or the other, before close() returned")
            .isTrue();
      } finally {
        storage.close();
      }
    }

    /**
     * Skipping the wait for its OWN executor must not also skip the wait for the other one (review of PR #8366).
     * {@code shutdownNow()} interrupts a caller that is one of the lifecycle workers; the first revision let that
     * interrupt short-circuit its own wait, restored it, and so short-circuited the snapshot-install wait right after
     * it - returning, silently, while a leader-initiated install on another thread was still writing under the
     * database directories.
     */
    // Regression for #8182: StateMachineCloseAwaitsLifecycleTasks
    @Test
    void closeFromALifecycleTaskStillWaitsForARunningLeaderInstall(@TempDir final Path raftDirectory) throws Exception {
      final CountDownLatch installEntered = new CountDownLatch(1);
      final AtomicReference<Throwable> outcome = new AtomicReference<>();
      final AtomicReference<CompletableFuture<TermIndex>> install = new AtomicReference<>();
      final AtomicBoolean installDoneWhenCloseReturned = new AtomicBoolean();
      final CountDownLatch done = new CountDownLatch(1);

      final RaftStorage storage = initializeStateMachine(serverWhoseRetryRuns(() -> closeFromWithin(outcome, done,
          () -> installDoneWhenCloseReturned.set(install.get().isDone()))), raftHAWhoseInstallRuns(() -> {
        installEntered.countDown();
        // Long enough that close() is certainly called while it is held; well inside CLOSE_AWAIT_MS.
        holdIgnoringInterrupts(HOLD_MS * 2);
      }), raftDirectory);
      try {
        install.set(startLeaderInstall());
        assertThat(installEntered.await(30, TimeUnit.SECONDS)).as("the install must reach its source resolution").isTrue();

        applyEntryThatSchedulesTheRetry();

        assertThat(done.await(30, TimeUnit.SECONDS)).as("the retry must reach the install and close").isTrue();
        rethrow(outcome);
        assertThat(installDoneWhenCloseReturned.get())
            .as("close() called from a lifecycle task must still wait for the install running on the OTHER executor")
            .isTrue();
      } finally {
        storage.close();
      }
    }

    /** The self-wait guard on the other executor: a leader-initiated install that closes its own state machine. */
    // Regression for #8182: StateMachineCloseAwaitsLifecycleTasks
    @Test
    void closeCalledFromTheSnapshotInstallThreadDoesNotWaitForItself(@TempDir final Path raftDirectory)
        throws Exception {
      final AtomicReference<Throwable> outcome = new AtomicReference<>();
      final AtomicBoolean closedFromWithin = new AtomicBoolean();
      final CountDownLatch done = new CountDownLatch(1);

      final RaftStorage storage = initializeStateMachine(serverWhoseRetryRuns(() -> {
      }), raftHAWhoseInstallRuns(() -> closeFromWithin(outcome, done, () -> closedFromWithin.set(true))),
          raftDirectory);
      try {
        startLeaderInstall();

        assertThat(done.await(30, TimeUnit.SECONDS)).as("the install must reach its source resolution and close").isTrue();
        rethrow(outcome);
        assertThat(closedFromWithin.get()).isTrue();
      } finally {
        storage.close();
      }
    }

    /**
     * An interrupt that lands on the closing thread while it waits for the first executor must not skip the wait for
     * the second (review of PR #8366): the first revision of the review fix chained the two waits with {@code ||}, so an
     * interrupt during the lifecycle wait returned without ever looking at a leader install still writing on the other
     * executor. The lifecycle task here interrupts the closer shortly after {@code shutdownNow()} reaches it, i.e. while
     * {@code close()} is waiting for it.
     */
    // Regression for #8182: StateMachineCloseAwaitsLifecycleTasks
    @Test
    void anInterruptDuringTheFirstWaitStillWaitsForTheSecondExecutor(@TempDir final Path raftDirectory)
        throws Exception {
      final CountDownLatch installEntered = new CountDownLatch(1);
      final CountDownLatch retryEntered = new CountDownLatch(1);
      final AtomicReference<Thread> closer = new AtomicReference<>();

      final RaftStorage storage = initializeStateMachine(serverWhoseRetryRuns(() -> {
        retryEntered.countDown();
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(HOLD_MS * 2);
        boolean closerInterrupted = false;
        long left;
        while ((left = deadline - System.nanoTime()) > 0)
          try {
            TimeUnit.NANOSECONDS.sleep(left);
          } catch (final InterruptedException e) {
            // shutdownNow() got here: close() is about to wait for this task. Interrupt it once it is waiting.
            if (!closerInterrupted) {
              holdIgnoringInterrupts(HOLD_MS / 5);
              closer.get().interrupt();
              closerInterrupted = true;
            }
          }
      }), raftHAWhoseInstallRuns(() -> {
        installEntered.countDown();
        holdIgnoringInterrupts(HOLD_MS * 3);
      }), raftDirectory);
      try {
        final CompletableFuture<TermIndex> install = startLeaderInstall();
        assertThat(installEntered.await(30, TimeUnit.SECONDS)).as("the install must reach its source resolution").isTrue();
        applyEntryThatSchedulesTheRetry();
        assertThat(retryEntered.await(30, TimeUnit.SECONDS)).as("the retry must reach the install").isTrue();

        closer.set(Thread.currentThread());
        sm.close();

        final boolean interruptRestored = Thread.interrupted();
        assertThat(interruptRestored).as("the interrupt that arrived during close() is handed back to the caller").isTrue();
        assertThat(install.isDone())
            .as("an interrupt during the lifecycle wait must not skip the wait for the leader install")
            .isTrue();
      } finally {
        Thread.interrupted();
        storage.close();
      }
    }

    /**
     * The self-wait skip is decided by thread IDENTITY, not by the thread name every state machine's lifecycle worker
     * shares (review of PR #8366). Here a lifecycle task of state machine A closes state machine B, whose own lifecycle
     * task is still running: a same-named thread is not B's worker, so B must still be waited for.
     */
    // Regression for #8182: StateMachineCloseAwaitsLifecycleTasks
    @Test
    void anotherStateMachinesLifecycleThreadStillWaitsForThisOnesTask(@TempDir final Path otherDatabaseDirectory)
        throws Exception {
      final CountDownLatch retryEntered = new CountDownLatch(1);
      applyEntryThatSchedulesTheRetry(serverWhoseRetryRuns(() -> {
        retryEntered.countDown();
        holdIgnoringInterrupts(HOLD_MS * 2);
      }));
      assertThat(retryEntered.await(30, TimeUnit.SECONDS)).as("B's retry must reach the install").isTrue();

      final AtomicReference<Throwable> outcome = new AtomicReference<>();
      final AtomicBoolean bRetryFinishedWhenCloseReturned = new AtomicBoolean();
      final CountDownLatch done = new CountDownLatch(1);
      final ArcadeStateMachine other = new ArcadeStateMachine();
      try {
        other.setServer(serverWhoseRetryRuns(otherDatabaseDirectory, () -> {
          try {
            sm.close();
            bRetryFinishedWhenCloseReturned.set(sm.getBootstrapInstallsInFlight().isEmpty());
          } catch (final Throwable t) {
            outcome.set(t);
          } finally {
            done.countDown();
          }
        }));
        applyEntryThatSchedulesTheRetry(other);

        assertThat(done.await(30, TimeUnit.SECONDS)).as("A's retry must reach the install and close B").isTrue();
        rethrow(outcome);
        assertThat(bRetryFinishedWhenCloseReturned.get())
            .as("closing B from A's lifecycle thread must still wait for B's own lifecycle task")
            .isTrue();
      } finally {
        other.close();
      }
    }

    /** The Ratis callback payload the install reads the leader id out of. */
    private static RaftProtos.RoleInfoProto leaderRoleInfo() {
      return RaftProtos.RoleInfoProto.newBuilder()
          .setFollowerInfo(RaftProtos.FollowerInfoProto.newBuilder()
              .setLeaderInfo(RaftProtos.ServerRpcProto.newBuilder()
                  .setId(RaftProtos.RaftPeerProto.newBuilder()
                      .setId(ByteString.copyFromUtf8(LEADER.toString()))
                      .build())
                  .build())
              .build())
          .build();
    }

    /** Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null {@code getId()}. */
    private static RaftServer stubRaftServer() {
      return (RaftServer) Proxy.newProxyInstance(StateMachineCloseTest.class.getClassLoader(),
          new Class<?>[] { RaftServer.class },
          (proxy, method, args) -> {
            if ("getId".equals(method.getName()))
              return RaftPeerId.valueOf("test-peer");
            if ("close".equals(method.getName()) || "start".equals(method.getName()))
              return null;
            throw new UnsupportedOperationException("Stub: " + method.getName());
          });
    }
  }

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
  @Nested
  class StateMachineCloseAwaitsHelperExecutors {

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

    // Regression for #8364: StateMachineCloseAwaitsHelperExecutors
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

    // Regression for #8364: StateMachineCloseAwaitsHelperExecutors
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

    // Regression for #8364: StateMachineCloseAwaitsHelperExecutors
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

    // Regression for #8364: StateMachineCloseAwaitsHelperExecutors
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
    // Regression for #8364: StateMachineCloseAwaitsHelperExecutors
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
    private static FakeArcadeDBServer serverWhoseCatchUpRuns(final Runnable onCatchUpThread) {
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.on("getHA", args -> {
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
      }), FakeRaftHAServer.detached());
      assertThat(entered.await(30, TimeUnit.SECONDS)).as("the catch-up must start").isTrue();
      assertThat(catchUp.hasRequestedSinceStart()).as("the catch-up is in flight, holding the request").isTrue();
    }

    // Regression for #8364: StateMachineCloseAwaitsHelperExecutors
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

    // Regression for #8364: StateMachineCloseAwaitsHelperExecutors
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
    // Regression for #8364: StateMachineCloseAwaitsHelperExecutors
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
      }), FakeRaftHAServer.detached());

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
    // Regression for #8364: StateMachineCloseAwaitsHelperExecutors
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

  /**
   * Regression test for issue #8652: a {@code StateMachineUpdater} that died from any throwable was only noticed through
   * the lifecycle state it left behind, which a zombie corrupts (the closing division stayed {@code CLOSING}, or the node
   * reported {@code RUNNING} again after rejoining while it rejected every append). The state machine now says directly
   * that the thread which applies entries has terminated, and the health monitor restarts the node in place when it sees
   * that on consecutive ticks.

   */
  @Nested
  class DeadStateMachineUpdater {

    // -- the state machine names a dead updater ---------------------------------------------------------------------

    // Regression for #8652: DeadStateMachineUpdater
    @Test
    void aStateMachineThatHasSeenNoUpdaterReportsNoneDead() {
      assertThat(new ArcadeStateMachine().describeDeadApplyThread()).isNull();
    }

    // Regression for #8652: DeadStateMachineUpdater
    @Test
    void aLiveUpdaterIsNotReportedDead() throws Exception {
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      final Object release = new Object();
      final Thread updater = new Thread(() -> {
        sm.recordApplyThreadForTesting();
        synchronized (release) {
          try {
            release.wait();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }, "test-updater-alive");
      updater.start();
      try {
        while (sm.getApplyThreadForTesting() == null)
          Thread.sleep(5);
        assertThat(sm.describeDeadApplyThread()).isNull();
      } finally {
        updater.interrupt();
        updater.join();
      }
    }

    // Regression for #8652: DeadStateMachineUpdater
    @Test
    void anUpdaterThatDiedIsReportedByName() throws Exception {
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      final Thread updater = new Thread(sm::recordApplyThreadForTesting, "test-updater-dead");
      updater.start();
      updater.join();

      assertThat(sm.describeDeadApplyThread()).contains("test-updater-dead").contains("terminated");
    }

    // Regression for #8652: DeadStateMachineUpdater
    @Test
    void aShortLivedCallerOfTheTermIndexCallbackIsNeverReportedAsADeadUpdater() throws Exception {
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      final Thread caller = new Thread(() -> sm.notifyTermIndexUpdated(1L, 1L), "test-other-caller");
      caller.start();
      caller.join();

      assertThat(sm.describeDeadApplyThread()).isNull();
    }

    // Regression for #8652: DeadStateMachineUpdater
    @Test
    void aClosedRaftLogSeenOnConsecutiveTicksRestartsRatis() {
      final int[] closed = { 1 };
      final HealthMonitorTest.FakeHealthTarget target = new HealthMonitorTest.FakeHealthTarget() {
        @Override
        public boolean isRaftLogClosed() {
          return closed[0] == 1;
        }
      };
      final HealthMonitor monitor = new HealthMonitor(target, 0);

      monitor.tick();
      assertThat(target.recoveryCalls.get()).as("one sighting is a restart reopening the log").isZero();
      closed[0] = 0;
      monitor.tick();
      closed[0] = 1;
      monitor.tick();
      assertThat(target.recoveryCalls.get()).as("the streak restarted after a tick with the log open").isZero();
      monitor.tick();
      assertThat(target.recoveryCalls.get()).isEqualTo(1);
    }

    // -- the health monitor recovers it ----------------------------------------------------------------------------

    // Regression for #8652: DeadStateMachineUpdater
    @Test
    void aDeadUpdaterSeenOnConsecutiveTicksRestartsRatis() {
      final HealthMonitorTest.FakeHealthTarget target = new HealthMonitorTest.FakeHealthTarget();
      target.deadUpdater = "the Ratis StateMachineUpdater thread 'x' has terminated";
      final HealthMonitor monitor = new HealthMonitor(target, 0);

      monitor.tick();
      assertThat(target.recoveryCalls.get()).as("one sighting is a shutdown racing the tick").isZero();

      monitor.tick();
      assertThat(target.recoveryCalls.get()).isEqualTo(1);
    }

    // Regression for #8652: DeadStateMachineUpdater
    @Test
    void aDeadUpdaterSeenOnceAndThenGoneIsForgotten() {
      final HealthMonitorTest.FakeHealthTarget target = new HealthMonitorTest.FakeHealthTarget();
      final HealthMonitor monitor = new HealthMonitor(target, 0);

      target.deadUpdater = "gone";
      monitor.tick();
      target.deadUpdater = null;
      monitor.tick();
      target.deadUpdater = "gone";
      monitor.tick();

      assertThat(target.recoveryCalls.get()).as("the streak restarts after a healthy tick").isZero();
    }

    // Regression for #8652: DeadStateMachineUpdater
    @Test
    void aDeadUpdaterOnAHealthyLookingNodeSuppressesTheOtherChecksThatTick() {
      final HealthMonitorTest.FakeHealthTarget target = new HealthMonitorTest.FakeHealthTarget();
      target.deadUpdater = "gone";
      target.lagging = true;
      final HealthMonitor monitor = new HealthMonitor(target, 0);

      monitor.tick();
      monitor.tick();

      assertThat(target.persistentLagRecover.get()).as("a resync cannot help a node that applies nothing").isZero();
      assertThat(target.recoveryCalls.get()).isEqualTo(1);
    }

    // Regression for #8652: DeadStateMachineUpdater
    @Test
    void aDeadUpdaterThatKeepsComingBackCountsTowardTheCrashLoopEscalation() {
      final HealthMonitorTest.FakeHealthTarget target = new HealthMonitorTest.FakeHealthTarget();
      target.deadUpdater = "gone";
      final HealthMonitor monitor = new HealthMonitor(target, 0, 1_000L, 60_000L, false, 0, 2, 20_000L);

      for (int i = 0; i < 40; i++)
        monitor.tick();

      assertThat(target.recoveryCalls.get()).as("the restarts stop once the crash-loop budget is spent").isBetween(1, 3);
    }

    // Regression for #8652: DeadStateMachineUpdater
    @Test
    void aRestartThatLooksHealthyForATickDoesNotResetTheCrashLoopStreak() {
      final HealthMonitorTest.FakeHealthTarget target = new HealthMonitorTest.FakeHealthTarget();
      final HealthMonitor monitor = new HealthMonitor(target, 0, 1_000L, 60_000L, false, 0, 2, 20_000L);

      // A fresh state machine has seen no thread after each restart, so one tick in three reads healthy
      for (int round = 0; round < 40; round++) {
        target.deadUpdater = "gone";
        monitor.tick();
        monitor.tick();
        target.deadUpdater = null;
        monitor.tick();
      }

      assertThat(target.recoveryCalls.get()).as("restarts stop once the crash-loop budget is spent").isBetween(1, 3);
    }
  }

  /**
   * Issue #9550: an apply that fails because its database was closed under it while the node is shutting down leaves the
   * entry for the replay on restart instead of quarantining the database.
   * <p>
   * #9548 made the engine's JVM shutdown hook wait for the server's, so a GRACEFUL stop stops Raft before the databases
   * close. Everything else that closes a database under a live apply thread - the server hook giving up on the lifecycle
   * lock, an embedder calling {@code DatabaseFactory.closeActiveDatabaseInstances()} on its way out - still reached the
   * per-database quarantine: the leader's double-failed publication ({@code publishLocalCommit}) and the follower's
   * {@code applyReplicatedTransaction} alike, and the reconcile in between tried to reopen the database through
   * {@code server.getDatabase}. A quarantine there is the wrong answer: nothing diverged, the node is going away, and the
   * entry is still in the Raft log.
   * <p>
   * The tests drive a real (unstarted) {@link ArcadeDBServer} with a real {@link LocalDatabase} registered in it, so the
   * real {@code databaseFor} lookup runs. Only whether the node is shutting down is set by the test, through the
   * {@code isNodeShuttingDown()} seam: the three real signals (Raft shutdown requested, server status, JVM shutdown) cannot
   * be raised in a unit test without stopping the JVM.
   */
  @Nested
  class ApplyOnDatabaseClosedDuringShutdown {
    private static final String DB_NAME     = "db9550";
    private static final long   ENTRY_INDEX = 7L;

    @TempDir
    Path tempDir;

    private ArcadeDBServer     server;
    private LocalDatabase      db;
    private Path               databaseDirectory;
    private ArcadeStateMachine sm;
    private RaftStorage        storage;
    private volatile boolean   shuttingDown;
    /** Set by the phase-2 fault, so only the reconcile that FOLLOWS a failed publication can be broken. */
    private volatile boolean   publishFailed;
    private volatile boolean   breakTheReconcile;

    @BeforeEach
    void setUp() throws IOException {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
      server = new ArcadeDBServer(config);

      databaseDirectory = tempDir.resolve("databases").resolve(DB_NAME);
      db = (LocalDatabase) new DatabaseFactory(databaseDirectory.toString()).create();
      db.getSchema().createDocumentType("Doc", 1);
      server.registerDatabase(DB_NAME, db);

      storage = RaftStorage.newBuilder().setDirectory(tempDir.resolve("raft").toFile())
          .setOption(RaftStorage.StartupOption.FORMAT).build();
      sm = new ArcadeStateMachine() {
        @Override
        boolean isNodeShuttingDown() {
          return shuttingDown;
        }

        @Override
        DatabaseInternal databaseFor(final String databaseName) {
          if (breakTheReconcile && publishFailed)
            throw new IllegalStateException("the database cannot be reached to reconcile from the replicated payload");
          return super.databaseFor(databaseName);
        }
      };
      sm.setServer(server);
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);
    }

    @AfterEach
    void tearDown() throws IOException {
      RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = null;
      if (sm != null)
        sm.close();
      if (storage != null)
        storage.close();
      if (db != null && db.isOpen()) {
        if (db.isTransactionActive())
          db.rollback();
        db.drop();
      }
      server.stop();
    }

    /**
     * The leader: its own entry comes back to the apply thread with the database closed under it. The publication fails,
     * the reconcile must not reopen the database, and the double failure must leave the entry for replay - not quarantine
     * the database. A later entry of another database still advances the applied index, so the checkpoint is what has to
     * stop short of the entry.
     */
    // Regression for #9550: ApplyOnDatabaseClosedDuringShutdown
    @Test
    void aLocallyOriginatedEntryOnADatabaseClosedDuringShutdownIsLeftForReplay() throws Exception {
      final Prepared prepared = prepare();
      final LocalCommit local = new LocalCommit(db.getName(), CommittedEntryDecoding.peekWalTransactionId(prepared.walData),
          prepared.tx, prepared.phase1, prepared.walData);
      assertThat(sm.registerLocalCommit(local)).isTrue();

      closeUnderTheApplyThread();

      assertThatThrownBy(() -> sm.applyTransaction(ratisContext(txEntry(prepared, ENTRY_INDEX))).get())
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(EntryLeftForReplayException.class);

      assertThat(local.awaitOutcome(1_000)).as("the committing thread is still woken").isEqualTo(LocalCommit.Outcome.FAILED);
      assertThat(local.reconciled()).as("the pages are not on this node").isFalse();
      assertLeftForReplayNotQuarantined();
    }

    /** The follower: the same closed database under {@code applyReplicatedTransaction}. */
    // Regression for #9550: ApplyOnDatabaseClosedDuringShutdown
    @Test
    void aReplicatedEntryOnADatabaseClosedDuringShutdownIsLeftForReplay() throws Exception {
      final Prepared prepared = prepare();

      closeUnderTheApplyThread();

      assertThatThrownBy(() -> sm.applyTransaction(ratisContext(txEntry(prepared, ENTRY_INDEX))).get())
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(EntryLeftForReplayException.class);

      assertLeftForReplayNotQuarantined();
    }

    /**
     * The classification is deliberately broad: once the database is closed under a shutting-down node, a failure that is
     * not a {@code DatabaseIsClosedException} is still taken as caused by the close and deferred to the replay, where a
     * genuine fault meets the quarantine again.
     */
    // Regression for #9550: ApplyOnDatabaseClosedDuringShutdown
    @Test
    void anyFailureOnADatabaseClosedDuringShutdownIsLeftForReplay() throws Exception {
      final Prepared prepared = prepare();
      final LocalCommit local = new LocalCommit(db.getName(), CommittedEntryDecoding.peekWalTransactionId(prepared.walData),
          prepared.tx, prepared.phase1, prepared.walData);
      assertThat(sm.registerLocalCommit(local)).isTrue();

      closeUnderTheApplyThread();
      breakTheReconcile = true;
      RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = name -> {
        publishFailed = true;
        throw new IllegalStateException("not a closed-database failure");
      };

      assertThatThrownBy(() -> sm.applyTransaction(ratisContext(txEntry(prepared, ENTRY_INDEX))).get())
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(EntryLeftForReplayException.class);
      assertLeftForReplayNotQuarantined();
    }

    /** A schema entry resolves its database through the same lookup, and is left for replay the same way. */
    // Regression for #9550: ApplyOnDatabaseClosedDuringShutdown
    @Test
    void aSchemaEntryOnADatabaseClosedDuringShutdownIsLeftForReplay() throws Exception {
      closeUnderTheApplyThread();

      final ByteString schemaEntry = RaftLogEntryCodec.encodeSchemaEntry(DB_NAME, "", Collections.emptyMap(),
          Collections.emptyMap(), Collections.emptyList(), Collections.emptyList());
      assertThatThrownBy(() -> sm.applyTransaction(ratisContext(logEntry(schemaEntry, ENTRY_INDEX))).get())
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(EntryLeftForReplayException.class);

      assertLeftForReplayNotQuarantined();
    }

    /**
     * A drop of a registered database resolved it through {@code server.getDatabase}, which reopened a closed one only to
     * close it again for the drop. While shutting down it is not reopened, and the drop is left for replay: its directory
     * is still there.
     */
    // Regression for #9550: ApplyOnDatabaseClosedDuringShutdown
    @Test
    void aDropOfADatabaseClosedDuringShutdownIsLeftForReplay() throws Exception {
      closeUnderTheApplyThread();

      assertThatThrownBy(() -> sm.applyTransaction(ratisContext(dropEntry(DB_NAME, ENTRY_INDEX))).get())
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(EntryLeftForReplayException.class);

      assertThat(databaseDirectory).as("the drop did not run").isDirectory();
      assertThat(server.existsDatabase(DB_NAME)).isTrue();
      assertLeftForReplayNotQuarantined();
    }

    /**
     * The lookup every apply resolves its database through refuses to reopen a closed database while the node is shutting
     * down: that reopen is the "Found active instance ... already in use" of the #9548 log, and reopening anything on a
     * node that is going away is never wanted.
     */
    // Regression for #9550: ApplyOnDatabaseClosedDuringShutdown
    @Test
    void theDatabaseLookupDoesNotReopenAClosedDatabaseWhileShuttingDown() {
      closeUnderTheApplyThread();

      assertThatThrownBy(() -> sm.databaseFor(DB_NAME)).isInstanceOf(DatabaseIsClosedException.class)
          .hasMessageContaining(DB_NAME);
      assertThat(DatabaseFactory.getActiveDatabaseInstance(databaseDirectory.toString()))
          .as("nothing was opened").isNull();
    }

    /** An open database is served as before while shutting down: only the REOPEN is refused. */
    // Regression for #9550: ApplyOnDatabaseClosedDuringShutdown
    @Test
    void theDatabaseLookupStillServesAnOpenDatabaseWhileShuttingDown() {
      shuttingDown = true;
      assertThat(sm.databaseFor(DB_NAME).isOpen()).isTrue();
    }

    /**
     * Scoped to the closed database: a genuine double failure on a database that is still OPEN is a divergence whether
     * the node is shutting down or not, and is quarantined exactly as before (issue #7602).
     */
    // Regression for #9550: ApplyOnDatabaseClosedDuringShutdown
    @Test
    void aGenuineFailureOnAnOpenDatabaseIsStillQuarantinedWhileShuttingDown() throws Exception {
      final Prepared prepared = prepare();
      final LocalCommit local = new LocalCommit(db.getName(), CommittedEntryDecoding.peekWalTransactionId(prepared.walData),
          prepared.tx, prepared.phase1, prepared.walData);
      assertThat(sm.registerLocalCommit(local)).isTrue();

      shuttingDown = true;
      breakTheReconcile = true;
      RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = name -> {
        publishFailed = true;
        throw new IllegalStateException("the data volume is read-only");
      };

      assertThatThrownBy(() -> sm.applyTransaction(ratisContext(txEntry(prepared, ENTRY_INDEX))).get())
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(ReplicationException.class)
          .satisfies(e -> assertThat(e.getCause()).isNotInstanceOf(EntryLeftForReplayException.class));

      assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();
    }

    /**
     * Scoped to the shutdown: a database closed on a node that is NOT shutting down - an embedder calling
     * {@code DatabaseFactory.closeActiveDatabaseInstances()} while the server keeps running - is not left for replay,
     * since nothing would replay it on a node that keeps running. The lookup reopens it, as it always did, and the entry
     * is applied.
     */
    // Regression for #9550: ApplyOnDatabaseClosedDuringShutdown
    @Test
    void aClosedDatabaseOnANodeThatIsNotShuttingDownIsReopenedAndApplied() throws Exception {
      final Prepared prepared = prepare();
      closeUnderTheApplyThread();
      shuttingDown = false;

      sm.applyTransaction(ratisContext(txEntry(prepared, ENTRY_INDEX))).get();

      assertThat(indexOf(sm.getLastAppliedTermIndex())).isEqualTo(ENTRY_INDEX);
      assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
      final Database reopened = DatabaseFactory.getActiveDatabaseInstance(databaseDirectory.toString());
      assertThat(reopened).as("the lookup reopened the database").isNotNull();
      assertThat(reopened.query("sql", "select from Doc").stream().count()).as("and the entry's record is in it").isEqualTo(1L);
      reopened.drop();
    }

    // Regression for #9550: ApplyOnDatabaseClosedDuringShutdown
    @Test
    void aRunningJvmIsNotShuttingDown() {
      assertThat(ArcadeStateMachine.isJvmShuttingDown()).isFalse();
    }

    /** The production wiring of one of the three signals: the Raft HA service having been asked to stop. */
    // Regression for #9550: ApplyOnDatabaseClosedDuringShutdown
    @Test
    void aRequestedRaftStopIsAShutdown() {
      final ArcadeStateMachine plain = new ArcadeStateMachine();
      plain.setServer(server);
      final FakeRaftHAServer raft = FakeRaftHAServer.detached(server);
      plain.setRaftHAServer(raft);

      raft.shutdownRequested(false);
      assertThat(plain.isNodeShuttingDown()).as("an unstarted server with Raft running").isFalse();

      raft.shutdownRequested(true);
      assertThat(plain.isNodeShuttingDown()).isTrue();
    }

    /** A second entry left for replay keeps the floor at the lowest one: the checkpoint still stops before the first. */
    // Regression for #9550: ApplyOnDatabaseClosedDuringShutdown
    @Test
    void theFloorIsTheLowestEntryLeftForReplay() throws Exception {
      closeUnderTheApplyThread();

      final ByteString schemaEntry = RaftLogEntryCodec.encodeSchemaEntry(DB_NAME, "", Collections.emptyMap(),
          Collections.emptyMap(), Collections.emptyList(), Collections.emptyList());
      assertThatThrownBy(() -> sm.applyTransaction(ratisContext(logEntry(schemaEntry, ENTRY_INDEX))).get())
          .hasCauseInstanceOf(EntryLeftForReplayException.class);
      assertThatThrownBy(() -> sm.applyTransaction(ratisContext(logEntry(schemaEntry, ENTRY_INDEX + 2))).get())
          .hasCauseInstanceOf(EntryLeftForReplayException.class);

      sm.applyTransaction(ratisContext(dropEntry("ghost", ENTRY_INDEX + 3))).get();
      assertThat(sm.takeSnapshot()).isEqualTo(ENTRY_INDEX - 1);
    }

    /** An entry left for replay at the very first index leaves nothing to checkpoint. */
    // Regression for #9550: ApplyOnDatabaseClosedDuringShutdown
    @Test
    void aFloorAtTheFirstIndexAuthorisesNoCheckpoint() throws Exception {
      closeUnderTheApplyThread();

      final ByteString schemaEntry = RaftLogEntryCodec.encodeSchemaEntry(DB_NAME, "", Collections.emptyMap(),
          Collections.emptyMap(), Collections.emptyList(), Collections.emptyList());
      assertThatThrownBy(() -> sm.applyTransaction(ratisContext(logEntry(schemaEntry, 0L))).get())
          .hasCauseInstanceOf(EntryLeftForReplayException.class);
      sm.applyTransaction(ratisContext(dropEntry("ghost", 1L))).get();

      assertThat(sm.takeSnapshot()).isEqualTo(RaftLog.INVALID_LOG_INDEX);
    }

    private void assertLeftForReplayNotQuarantined() throws Exception {
      assertThat(sm.isDatabaseDiverged(DB_NAME)).as("nothing diverged: the node is going away").isFalse();
      assertThat(indexOf(sm.getLastAppliedTermIndex())).as("the entry did not move the applied position")
          .isLessThan(ENTRY_INDEX);
      assertThat(DatabaseFactory.getActiveDatabaseInstance(databaseDirectory.toString()))
          .as("the database was not reopened").isNull();

      // A later entry of another database (a drop of one that does not exist here, which applies as a no-op) still
      // advances the applied index past the entry.
      sm.applyTransaction(ratisContext(dropEntry("ghost", ENTRY_INDEX + 1))).get();
      assertThat(indexOf(sm.getLastAppliedTermIndex())).isEqualTo(ENTRY_INDEX + 1);

      assertThat(sm.takeSnapshot()).as("the checkpoint stops right before the entry left for replay")
          .isEqualTo(ENTRY_INDEX - 1);
    }

    private void closeUnderTheApplyThread() {
      shuttingDown = true;
      db.close();
      assertThat(db.isOpen()).isFalse();
    }

    private record Prepared(TransactionContext tx, TransactionContext.TransactionPhase1 phase1, byte[] walData) {
    }

    /** A phase-1 transaction on the database, as the committing thread leaves it before dispatching the entry. */
    private Prepared prepare() {
      db.begin();
      db.newDocument("Doc").set("v", 1).save();
      final TransactionContext tx = db.getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      return new Prepared(tx, phase1, phase1.result.toByteArray());
    }

    private LogEntryProto txEntry(final Prepared prepared, final long index) {
      final ByteString payload = RaftLogEntryCodec.encodeTxEntry(DB_NAME, prepared.walData,
          prepared.tx.getBucketRecordDelta());
      return logEntry(payload, index);
    }

    private static LogEntryProto dropEntry(final String databaseName, final long index) {
      return logEntry(RaftLogEntryCodec.encodeDropDatabaseEntry(databaseName), index);
    }

    private static LogEntryProto logEntry(final ByteString payload, final long index) {
      return LogEntryProto.newBuilder().setTerm(1L).setIndex(index)
          .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build()).build();
    }

    private org.apache.ratis.statemachine.TransactionContext ratisContext(final LogEntryProto logEntry) {
      return org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
    }

    private static long indexOf(final TermIndex termIndex) {
      return termIndex != null ? termIndex.getIndex() : -1L;
    }

    private RaftServer stubServer() {
      return (RaftServer) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { RaftServer.class },
          (proxy, method, args) -> {
            if ("getId".equals(method.getName()))
              return RaftPeerId.valueOf("test-peer");
            if ("close".equals(method.getName()) || "start".equals(method.getName()))
              return null;
            throw new UnsupportedOperationException("Stub: " + method.getName());
          });
    }
  }
}
