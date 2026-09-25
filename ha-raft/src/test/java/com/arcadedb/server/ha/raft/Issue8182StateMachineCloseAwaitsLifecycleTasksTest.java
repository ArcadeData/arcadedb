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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.StallAwareStopwatch;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8182 (reported as #8143 and #7969).
 * <p>
 * {@code Issue7298BootstrapReplaySkipMissingDatabaseTest} failed in JUnit's {@code @TempDir} teardown, never in an
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
class Issue8182StateMachineCloseAwaitsLifecycleTasksTest {

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
   * and auto-acquire is off, so a leader-initiated install reconciles nothing and completes without dialling anyone.
   */
  private ArcadeDBServer serverWhoseRetryRuns(final Runnable onLifecycleThread) {
    return serverWhoseRetryRuns(serverDir, onLifecycleThread);
  }

  private static ArcadeDBServer serverWhoseRetryRuns(final Path databaseDirectory, final Runnable onLifecycleThread) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databaseDirectory.toString());
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
    config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.existsDatabase(DB_NAME)).thenReturn(false);
    when(server.getDatabaseNames()).thenReturn(Set.of());
    // SnapshotInstaller.install asks for the backup coordinator before it touches the filesystem, so this is the
    // first thing the retry does. A null coordinator is tolerated there (no maintenance slot to take).
    when(server.getBackupCoordinator()).thenAnswer(invocation -> {
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
  private static RaftHAServer raftHAWhoseInstallRuns(final Runnable onInstallThread) {
    final RaftHAServer raftHA = mock(RaftHAServer.class);
    when(raftHA.isLeader()).thenReturn(false);
    when(raftHA.getLocalPeerId()).thenReturn(LOCAL);
    when(raftHA.getLeaderId()).thenReturn(LEADER);
    when(raftHA.getClusterToken()).thenReturn("cluster-token");
    when(raftHA.getUnambiguousPeerHttpAddress(LEADER)).thenAnswer(invocation -> {
      // Only there: the bootstrap install resolves the leader through the same accessor, on other threads.
      if (Thread.currentThread().getName().equals(ArcadeStateMachine.SNAPSHOT_INSTALL_THREAD_NAME))
        onInstallThread.run();
      return "leader-host:2480";
    });
    when(raftHA.getLocalHttpAddress()).thenReturn("local-host:2480");
    when(raftHA.getUnambiguousPeerHttpsAddress(LEADER)).thenReturn(null);
    when(raftHA.getLocalHttpsAddress()).thenReturn(null);
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
    return (RaftServer) Proxy.newProxyInstance(Issue8182StateMachineCloseAwaitsLifecycleTasksTest.class.getClassLoader(),
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
