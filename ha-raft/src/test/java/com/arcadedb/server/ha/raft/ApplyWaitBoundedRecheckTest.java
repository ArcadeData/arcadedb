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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #5846: a code path that advances the Raft applied index without also
 * calling {@link RaftHAServer#notifyApplied()} leaves any thread blocked in
 * {@link RaftHAServer#waitForAppliedIndex(long, boolean)} or {@link RaftHAServer#waitForLocalApply()}
 * asleep until the full {@code quorumTimeout} elapses, even though the condition it is waiting on was
 * already satisfied. {@code ArcadeStateMachine.reinitialize()} (a follower reloading after installing a
 * snapshot) and {@code notifyInstallSnapshotFromLeader()} (a follower receiving one directly from the
 * leader) were the two known silent paths; both now call {@code notifyApplied()} explicitly (see
 * {@link ArcadeStateMachineReinitializeNotifiesTest}).
 * <p>
 * This class covers the second, independent half of the fix: the waiters themselves now re-check their
 * condition on a bounded interval instead of only on an explicit notification, so ANY missed
 * notification - including one from a path not yet written - costs at most that interval rather than
 * the full quorum timeout.
 */
class ApplyWaitBoundedRecheckTest {

  /**
   * Builds a {@link RaftHAServer} without starting Ratis (no gRPC, no election), then swaps in a
   * stubbed {@code raftServer} whose reported applied/commit index is fully test-controlled -
   * simulating Ratis's own internal state (the one {@link RaftHAServer#getLastAppliedIndex()} and
   * {@link RaftHAServer#getCommitIndex()} read) advancing without any ArcadeDB code ever calling
   * {@link RaftHAServer#notifyApplied()}.
   */
  private static RaftHAServer newDetachedRaftHAServer(final AtomicLong appliedIndex, final long commitIndex) throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 10_000L);

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("localhost");

    final RaftHAServer raft = new RaftHAServer(mockServer, config);

    final DivisionInfo mockInfo = mock(DivisionInfo.class);
    when(mockInfo.getLastAppliedIndex()).thenAnswer(inv -> appliedIndex.get());

    final RaftLog mockRaftLog = mock(RaftLog.class);
    when(mockRaftLog.getLastCommittedIndex()).thenReturn(commitIndex);

    final RaftServer.Division mockDivision = mock(RaftServer.Division.class);
    when(mockDivision.getInfo()).thenReturn(mockInfo);
    when(mockDivision.getRaftLog()).thenReturn(mockRaftLog);

    final RaftServer mockRaftServer = mock(RaftServer.class);
    when(mockRaftServer.getDivision(any())).thenReturn(mockDivision);

    setPrivateField(raft, "raftServer", mockRaftServer);
    return raft;
  }

  private static void setPrivateField(final Object target, final String name, final Object value) throws Exception {
    final Field f = RaftHAServer.class.getDeclaredField(name);
    f.setAccessible(true);
    f.set(target, value);
  }

  /**
   * Before the fix, {@code applyNotifier.wait(remaining)} blocked for the full remaining quorum
   * timeout because nothing ever called {@code notifyAll()} on a missed notification. The bounded
   * re-check must catch the already-satisfied condition well within one interval.
   */
  @Test
  void waitForAppliedIndexReturnsPromptlyOnMissedNotify() throws Exception {
    final AtomicLong appliedIndex = new AtomicLong(0);
    final RaftHAServer raft = newDetachedRaftHAServer(appliedIndex, 0);

    final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    try {
      // Advance the index directly - the way a silently-broken advance path would - WITHOUT ever
      // calling raft.notifyApplied(). Scheduled well inside the bounded re-check interval so the
      // wait loop, not the 10s quorum timeout, is what has to catch it.
      scheduler.schedule(() -> appliedIndex.set(42), 300, TimeUnit.MILLISECONDS);

      final long start = System.currentTimeMillis();
      raft.waitForAppliedIndex(42, true); // must not throw ReplicationException
      final long elapsed = System.currentTimeMillis() - start;

      assertThat(elapsed)
          .as("a missed notifyApplied() must cost at most one bounded re-check interval, not the full quorum timeout")
          .isLessThan(2_000);
    } finally {
      scheduler.shutdownNow();
    }
  }

  /**
   * Same scenario for the {@code READ_YOUR_WRITES} / bookmark waiter.
   */
  @Test
  void waitForLocalApplyReturnsPromptlyOnMissedNotify() throws Exception {
    final AtomicLong appliedIndex = new AtomicLong(0);
    final RaftHAServer raft = newDetachedRaftHAServer(appliedIndex, 42);

    final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    try {
      scheduler.schedule(() -> appliedIndex.set(42), 300, TimeUnit.MILLISECONDS);

      final long start = System.currentTimeMillis();
      raft.waitForLocalApply();
      final long elapsed = System.currentTimeMillis() - start;

      assertThat(elapsed)
          .as("a missed notifyApplied() must cost at most one bounded re-check interval, not the full quorum timeout")
          .isLessThan(2_000);
    } finally {
      scheduler.shutdownNow();
    }
  }
}
