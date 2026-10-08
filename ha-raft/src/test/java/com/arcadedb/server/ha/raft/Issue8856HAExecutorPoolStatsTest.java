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
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.DedicatedThreadPool.PoolStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8856: what each of the HA layer's per-instance executor rows reads, driven through the pools themselves.
 * Each pool reports its saturations the way its rejection policy handles them - an abort pool as
 * {@code tasks.rejected}, a caller-runs pool as {@code tasks.caller_run_fallbacks} - and only while the pool is
 * running, so an owner stopping does not read as an undersized pool.
 */
class Issue8856HAExecutorPoolStatsTest {

  private static final long AWAIT_SECONDS = 10;

  private final CountDownLatch release = new CountDownLatch(1);

  @AfterEach
  void releaseWorkers() {
    release.countDown();
  }

  /** A refused channel-recovery task is counted as rejected, while the pool runs, and the queue it filled is visible. */
  @Test
  void channelRecoveryCountsRejectionsWhileRunningOnly() throws Exception {
    final RaftHAServer server = new RaftHAServer(detachedServer(), threeNodeConfig());
    final ThreadPoolExecutor executor = executor(server, RaftHAServer.class, "channelRecoveryExecutor");

    final int queueCapacity = blockAndFill(executor);
    assertThat(server.getChannelRecoveryRejections()).isZero();
    final PoolStats full = server.getChannelRecoveryPoolStats();
    assertThat(full.activeThreads()).isEqualTo(1);
    assertThat(full.queueDepth()).isEqualTo(queueCapacity);
    assertThat(full.queueCapacityRemaining()).isZero();

    assertThatThrownBy(() -> executor.execute(() -> {
    })).isInstanceOf(RejectedExecutionException.class);
    assertThat(server.getChannelRecoveryRejections()).isEqualTo(1);
    assertThat(server.getChannelRecoveryPoolStats().callerRunFallbacks()).as("an abort pool never runs on the caller")
        .isZero();

    executor.shutdownNow();
    assertThatThrownBy(() -> executor.execute(() -> {
    })).isInstanceOf(RejectedExecutionException.class);
    assertThat(server.getChannelRecoveryRejections()).as("a refusal by a stopped pool is not a saturation").isEqualTo(1);
  }

  /**
   * A health tick that finds a #8491 hand-off already queued or running does not queue another: that skip is the
   * {@code channel_recovery} row's {@code tasks.coalesced}.
   */
  @Test
  void aReplacingHandOffSkippedBehindAQueuedOneIsCoalesced() throws Exception {
    final RaftHAServer server = new RaftHAServer(detachedServer(), threeNodeConfig()) {
      @Override
      public boolean isLeader() {
        return true;
      }
    };
    final ArcadeStateMachine sm = mock(ArcadeStateMachine.class);
    when(sm.hasLeaderServiceGap()).thenReturn(true);
    final CountDownLatch started = new CountDownLatch(1);
    doAnswer(invocation -> {
      started.countDown();
      release.await(AWAIT_SECONDS, TimeUnit.SECONDS);
      return false;
    }).when(sm).handOffLeadershipWhileReplacingDatabase();
    final Field field = RaftHAServer.class.getDeclaredField("stateMachine");
    field.setAccessible(true);
    field.set(server, sm);

    server.queueReplacingDatabaseHandOff(sm);
    assertThat(started.await(AWAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
    assertThat(server.getReplacingHandOffsCoalesced()).isZero();

    server.queueReplacingDatabaseHandOff(sm);
    server.queueReplacingDatabaseHandOff(sm);
    assertThat(server.getReplacingHandOffsCoalesced()).isEqualTo(2);
  }

  /** The stalled-resync pool runs a task it cannot queue on the submitter, and the row counts that as a fallback. */
  @Test
  void stalledResyncCountsCallerRunsFallbacks() throws Exception {
    final RaftHAServer server = new RaftHAServer(detachedServer(), threeNodeConfig());
    final ThreadPoolExecutor executor = executor(server, RaftHAServer.class, "stalledResyncExecutor");

    blockAndFill(executor);
    final AtomicReference<Thread> ranOn = new AtomicReference<>();
    executor.execute(() -> ranOn.set(Thread.currentThread()));

    assertThat(ranOn.get()).as("caller-runs").isSameAs(Thread.currentThread());
    assertThat(server.getStalledResyncPoolStats().callerRunFallbacks()).isEqualTo(1);

    executor.shutdownNow();
    executor.execute(() -> ranOn.set(null));
    assertThat(ranOn.get()).as("a stopped caller-runs pool discards").isSameAs(Thread.currentThread());
    assertThat(server.getStalledResyncPoolStats().callerRunFallbacks()).isEqualTo(1);
  }

  /**
   * A snapshot install the install executor refuses becomes a failed future for Ratis to retry, and is counted on the
   * {@code snapshot_install} row as rejected - driven through the Ratis callback that submits it.
   */
  @Test
  void aRefusedSnapshotInstallIsCountedAsRejected() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      final ThreadPoolExecutor executor = executor(sm, ArcadeStateMachine.class, "snapshotInstallExecutor");
      final int queueCapacity = blockAndFill(executor);
      assertThat(sm.getSnapshotInstallPoolStats().queueDepth()).isEqualTo(queueCapacity);
      assertThat(sm.getSnapshotInstallRejections()).isZero();

      final CompletableFuture<?> install = sm.notifyInstallSnapshotFromLeader(null, null);

      assertThat(install).isCompletedExceptionally();
      assertThatThrownBy(install::get).isInstanceOf(ExecutionException.class);
      assertThat(sm.getSnapshotInstallRejections()).isEqualTo(1);
    } finally {
      release.countDown();
      sm.close();
    }
  }

  /** The lifecycle worker is unbounded, so its row reports no capacity limit and only queue depth grows. */
  @Test
  void theLifecycleRowReportsAnUnboundedQueue() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      final ThreadPoolExecutor executor = executor(sm, ArcadeStateMachine.class, "lifecycleExecutor");
      final CountDownLatch started = new CountDownLatch(1);
      executor.execute(() -> {
        started.countDown();
        awaitRelease();
      });
      assertThat(started.await(AWAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
      executor.execute(() -> {
      });

      final PoolStats stats = sm.getLifecyclePoolStats();
      assertThat(stats.poolSize()).isEqualTo(1);
      assertThat(stats.activeThreads()).isEqualTo(1);
      assertThat(stats.queueDepth()).isEqualTo(1);
      assertThat(stats.queueCapacityRemaining()).as("unbounded").isEqualTo(-1);
    } finally {
      release.countDown();
      sm.close();
    }
  }

  /** The {@code database_deleter} row is read through whichever deleter the state machine has installed. */
  @Test
  void theDeleterRowFollowsTheInstalledDeleter() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      final PoolStats stats = sm.getDatabaseDeleterPoolStats();
      assertThat(stats.poolSize()).as("the deleter's single core worker is started lazily").isLessThanOrEqualTo(1);
      assertThat(stats.queueCapacityRemaining()).isPositive();
      assertThat(stats.callerRunFallbacks()).isZero();
    } finally {
      sm.close();
    }
  }

  // ---- helpers --------------------------------------------------------------------------------------------------

  /** Occupies the pool's single worker until the test ends and fills its queue; returns how many tasks it queued. */
  private int blockAndFill(final ThreadPoolExecutor executor) throws InterruptedException {
    final CountDownLatch started = new CountDownLatch(1);
    executor.execute(() -> {
      started.countDown();
      awaitRelease();
    });
    assertThat(started.await(AWAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
    final int capacity = executor.getQueue().remainingCapacity();
    for (int i = 0; i < capacity; i++)
      executor.execute(() -> {
      });
    return capacity;
  }

  private void awaitRelease() {
    try {
      release.await(AWAIT_SECONDS, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static ThreadPoolExecutor executor(final Object owner, final Class<?> type, final String fieldName)
      throws Exception {
    final Field field = type.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (ThreadPoolExecutor) field.get(owner);
  }

  private static ArcadeDBServer detachedServer() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer("ArcadeDB_0");
    return server;
  }

  private static ContextConfiguration threeNodeConfig() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");
    return config;
  }
}
