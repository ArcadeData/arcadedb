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

import com.arcadedb.exception.ConfigurationException;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8592: {@link RaftClusterManager#leaveCluster(boolean)} on a leader handed leadership to
 * the first non-self peer in configuration order, with no lag, priority or reachability screen, and gave that one
 * targeted transfer the whole 10 s budget. A targeted Ratis transfer to a peer that cannot win stays pending for its
 * whole timeout, and while it is pending the leader refuses every write (#8556).
 * <p>
 * The leave now goes through the shared hand-off: the peers {@link RaftHAServer#selectStepDownTargets} ranks, tried
 * in order within one budget, and no bare step-down when none takes over - the removal demotes the leader itself.
 * <p>
 * The candidates are also screened for reachability ({@link RaftHAServer#handoffReachablePeers()}, issue #8556), so
 * the fixture makes both peers reachable unless a test says otherwise (issue #8632).
 */
class Issue8592LeaveClusterHandOffTargetTest {

  private static final RaftPeerId SELF = RaftPeerId.valueOf("peer-a_2434");
  private static final RaftPeerId B    = RaftPeerId.valueOf("peer-b_2435");
  private static final RaftPeerId C    = RaftPeerId.valueOf("peer-c_2436");

  private RaftHAServer   raft;
  private ClusterMonitor monitor;
  private AtomicBoolean  leader;

  private final List<String>  transferTargets = new CopyOnWriteArrayList<>();
  private final List<String>  removed         = new CopyOnWriteArrayList<>();
  private final AtomicInteger bareStepDowns   = new AtomicInteger();

  @BeforeEach
  void setUp() {
    raft = mock(RaftHAServer.class);
    monitor = mock(ClusterMonitor.class);
    leader = new AtomicBoolean(true);
    when(raft.getClient()).thenReturn(mock(RaftClient.class));
    when(raft.getLocalPeerId()).thenReturn(SELF);
    when(raft.isLeader()).thenAnswer(invocation -> leader.get());
    when(raft.getClusterMonitor()).thenReturn(monitor);
    when(raft.getLivePeers()).thenReturn(List.of(peer(SELF, 0), peer(B, 0), peer(C, 0)));
    // Both peers answered this leader recently: without it the #8556 screen drops every candidate (issue #8632)
    when(raft.handoffReachablePeers()).thenReturn(Set.of(B.toString(), C.toString()));
  }

  /** The first configured peer is lagging (the shape a peer that went down takes): the leave hands off to the next. */
  @Test
  void aLaggingFirstPeerIsSkipped() {
    when(monitor.isReplicaLagging(B.toString())).thenReturn(true);

    manager(C).leaveCluster(false);

    assertThat(transferTargets).containsExactly(C.toString());
    assertThat(bareStepDowns.get()).isZero();
    assertThat(removed).containsExactly(SELF.toString());
  }

  /**
   * A peer not proven reachable is never tried (issue #8556), even when it ranks first: a targeted transfer to it would
   * hold every write on this leader refused for its slice of the budget.
   */
  @Test
  void anUnreachableFirstPeerIsNeverTried() {
    when(raft.handoffReachablePeers()).thenReturn(Set.of(C.toString()));

    manager(null).leaveCluster(false);

    assertThat(transferTargets).containsExactly(C.toString());
    assertThat(bareStepDowns.get()).isZero();
    assertThat(removed).containsExactly(SELF.toString());
  }

  /** A priority-0 first peer, while a higher-priority voter exists, is never the target: Ratis would not keep it leader. */
  @Test
  void theHighestPriorityPeerIsTriedFirst() {
    when(raft.getLivePeers()).thenReturn(List.of(peer(SELF, 1), peer(B, 0), peer(C, 1)));

    manager(C).leaveCluster(false);

    assertThat(transferTargets).containsExactly(C.toString());
    assertThat(removed).containsExactly(SELF.toString());
  }

  /** A targeted transfer that fails moves on to the next candidate instead of giving up on the hand-off. */
  @Test
  void aFailedCandidateFallsThroughToTheNext() {
    manager(C).leaveCluster(false);

    assertThat(transferTargets).containsExactly(B.toString(), C.toString());
    assertThat(bareStepDowns.get()).isZero();
    assertThat(removed).containsExactly(SELF.toString());
  }

  /**
   * No peer is eligible: nothing is sent at all - no targeted transfer to hold writes up, and no bare step-down to leave
   * the cluster leaderless for an election timeout. The removal proceeds at once and demotes this leader itself.
   */
  @Test
  void noEligiblePeerSkipsTheHandOffAndStillRemoves() {
    when(monitor.isReplicaLagging(B.toString())).thenReturn(true);
    when(monitor.isReplicaLagging(C.toString())).thenReturn(true);

    manager(null).leaveCluster(false);

    assertThat(transferTargets).isEmpty();
    assertThat(bareStepDowns.get()).isZero();
    assertThat(removed).containsExactly(SELF.toString());
  }

  /** Every candidate fails: still no bare step-down, and the removal still runs. */
  @Test
  void everyCandidateFailingStillRemovesWithoutABareStepDown() {
    manager(null).leaveCluster(false);

    assertThat(transferTargets).containsExactly(B.toString(), C.toString());
    assertThat(bareStepDowns.get()).isZero();
    assertThat(removed).containsExactly(SELF.toString());
  }

  /** A follower leaving hands nothing off. */
  @Test
  void aFollowerLeavesWithoutAnyHandOff() {
    leader.set(false);

    manager(C).leaveCluster(false);

    assertThat(transferTargets).isEmpty();
    assertThat(bareStepDowns.get()).isZero();
    assertThat(removed).containsExactly(SELF.toString());
  }

  /** The other callers of the no-target overload keep their last resort. */
  @Test
  void theNoTargetTransferKeepsItsBareStepDownFallback() {
    when(monitor.isReplicaLagging(B.toString())).thenReturn(true);
    when(monitor.isReplicaLagging(C.toString())).thenReturn(true);

    manager(null).transferLeadership(1_000);

    assertThat(bareStepDowns.get()).isEqualTo(1);
  }

  /**
   * A manager whose targeted transfer succeeds only toward {@code winner} (every other target fails the way an
   * unreachable peer does), and whose removal and bare step-down are recorded instead of sent.
   */
  private RaftClusterManager manager(final RaftPeerId winner) {
    return new RaftClusterManager(raft) {
      @Override
      void transferLeadership(final String targetPeerId, final long timeoutMs) {
        transferTargets.add(targetPeerId);
        if (winner == null || !winner.toString().equals(targetPeerId))
          throw new ConfigurationException("Failed to transfer leadership to " + targetPeerId + ": timed out");
        leader.set(false);
      }

      @Override
      boolean stepDownWithoutTarget(final long timeoutMs) {
        bareStepDowns.incrementAndGet();
        return false;
      }

      @Override
      void removePeer(final String peerId, final boolean force) {
        removed.add(peerId);
      }
    };
  }

  private static RaftPeer peer(final RaftPeerId id, final int priority) {
    return RaftPeer.newBuilder().setId(id).setAddress("localhost:" + id.toString().substring(id.toString().indexOf('_') + 1))
        .setPriority(priority).build();
  }
}
