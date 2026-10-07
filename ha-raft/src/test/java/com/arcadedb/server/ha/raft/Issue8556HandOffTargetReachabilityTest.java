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
import com.arcadedb.utility.SubclassMocks;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8556: the automatic leadership hand-offs chose their target with
 * {@link RaftHAServer#selectStepDownTargets}, which screened a peer for lag only. A peer that is down passes that
 * screen - on an idle cluster its lag does not grow, and right after an election the {@link ClusterMonitor} has no
 * record of any peer, which it reads as "not lagging". A targeted Ratis transfer to it stays pending for the whole
 * budget, and while it is pending the leader refuses every write on every database. The #8491 hand-off then repeated
 * it as soon as the previous attempt returned.
 * <p>
 * Pinned here: a peer must be proven reachable - it answered this leader, recently - to be a target (on every entry
 * point: the no-target transfer, the manual step-down, the quarantine hand-off's eligibility check, the channel
 * escalation); a STALLED peer is not a target; one candidate gets a slice of the budget, not all of it; and the #8491
 * throttle is measured from the end of an attempt and backs off while attempts keep failing.
 */
class Issue8556HandOffTargetReachabilityTest {

  private static final RaftPeerId SELF = RaftPeerId.valueOf("peer-a_2434");
  private static final RaftPeerId B    = RaftPeerId.valueOf("peer-b_2435");
  private static final RaftPeerId C    = RaftPeerId.valueOf("peer-c_2436");

  // -- The reachability rule --

  /**
   * The issue's own scenario: right after this node is elected, Ratis starts every follower record at match index -1,
   * and only an acknowledgement moves it. The peer that is down never acknowledges; the live one did at once.
   */
  @Test
  void aPeerThatHasNotAnsweredThisLeaderIsNotReachable() {
    final Set<String> reachable = RaftHAServer.handoffReachablePeerIds(
        List.of(follower(B, 120L, 40L), follower(C, -1L, 900L)), 5_000L);

    assertThat(reachable).containsExactly(B.toString());
  }

  /** A peer that died mid-term keeps a high match index, but its last answer ages past the window. */
  @Test
  void aPeerSilentForTheWholeWindowIsNotReachable() {
    final Set<String> reachable = RaftHAServer.handoffReachablePeerIds(
        List.of(follower(B, 120L, 4_999L), follower(C, 120L, 5_000L)), 5_000L);

    assertThat(reachable).containsExactly(B.toString());
  }

  /** A degraded entry (membership changing under the read, #4842) carries no match index and proves nothing. */
  @Test
  void aDegradedEntryIsNotReachable() {
    final Map<String, Object> degraded = new LinkedHashMap<>();
    degraded.put("peerId", C.toString());
    degraded.put("lastRpcElapsedMs", 10L);

    assertThat(RaftHAServer.handoffReachablePeerIds(List.of(follower(B, 7L, 10L), degraded), 5_000L))
        .containsExactly(B.toString());
  }

  @Test
  void theContactWindowIsTheElectionTimeoutCappedByTheUnreachableThreshold() {
    assertThat(RaftHAServer.handoffContactWindowMs(5_000L, 10_000L)).isEqualTo(5_000L);
    assertThat(RaftHAServer.handoffContactWindowMs(5_000L, 2_000L)).isEqualTo(2_000L);
    assertThat(RaftHAServer.handoffContactWindowMs(5_000L, 0L)).as("threshold disabled").isEqualTo(5_000L);
  }

  // -- The target selection, on each of its consumers --

  @Test
  void anUnreachablePeerIsNeverAStepDownTarget() {
    final List<RaftPeer> targets = RaftHAServer.selectStepDownTargets(peers(), SELF, new ClusterMonitor(1_000L),
        Set.of(B.toString()));

    assertThat(targets).extracting(p -> p.getId().toString()).containsExactly(B.toString());
  }

  /**
   * Right after {@link ClusterMonitor#reset()} the monitor has no record of any peer, and the lag screen alone let
   * every one through. The reachability screen is what stops the dead one.
   */
  @Test
  void anEmptyMonitorAfterAnElectionDoesNotLetADeadPeerThrough() {
    final ClusterMonitor monitor = new ClusterMonitor(1_000L);
    monitor.updateLeaderCommitIndex(50L);
    monitor.updateReplicaMatchIndex(C.toString(), 50L, 10L);
    monitor.reset();
    assertThat(monitor.isReplicaLagging(C.toString())).as("the lag screen alone passes it").isFalse();

    assertThat(RaftHAServer.selectStepDownTargets(peers(), SELF, monitor, Set.of())).isEmpty();
  }

  /** Caught up but unreachable is STALLED (#5291) with a lag under the threshold: the lag test passes it. */
  @Test
  void aStalledPeerWithASmallLagIsNotAStepDownTarget() {
    final ClusterMonitor monitor = new ClusterMonitor(1_000L, 0L, null, false, 10_000L);
    monitor.updateLeaderCommitIndex(500L);
    monitor.updateReplicaMatchIndex(B.toString(), 500L, 10L);
    monitor.updateReplicaMatchIndex(C.toString(), 500L, 30_000L);
    assertThat(monitor.isReplicaLagging(C.toString())).isFalse();
    assertThat(monitor.getReplicaStatus(C.toString())).isEqualTo(ClusterMonitor.ReplicaStatus.STALLED);

    assertThat(RaftHAServer.selectStepDownTargets(peers(), SELF, monitor))
        .extracting(p -> p.getId().toString()).containsExactly(B.toString());
  }

  /**
   * The quarantine hand-off (#8483) asks whether any peer is eligible before it tries, and reports to the operator when
   * none is. A dead peer counted as eligible and silenced that report.
   */
  @Test
  void aDeadPeerDoesNotCountAsAHandOffTarget() {
    final List<RaftPeer> selfAndDeadPeer = List.of(peer(SELF), peer(C));

    assertThat(RaftHAServer.hasHandoffTarget(selfAndDeadPeer, SELF, null, Set.of())).isFalse();
    assertThat(RaftHAServer.hasHandoffTarget(selfAndDeadPeer, SELF, null, Set.of(C.toString()))).isTrue();
  }

  @Test
  void theChannelEscalationSkipsAnUnreachablePeer() {
    // B is the wedged follower, C is down: nowhere safe to go.
    assertThat(RaftHAServer.selectChannelEscalationTarget(peers(), SELF, B.toString(), null, Set.of())).isNull();
    assertThat(RaftHAServer.selectChannelEscalationTarget(peers(), SELF, B.toString(), null, Set.of(C.toString())))
        .extracting(p -> p.getId().toString()).isEqualTo(C.toString());
  }

  /**
   * The #8491 and #8483 hand-offs and the operator's no-target {@code POST /api/v1/cluster/leader} all run
   * {@link RaftClusterManager#transferLeadership(long)}. The peer that is down is never sent a targeted transfer.
   */
  @Test
  void theNoTargetTransferNeverTargetsAnUnreachablePeer() throws Exception {
    final AdminApi admin = mock(AdminApi.class);
    final RaftHAServer raft = leaderWithPeers(admin);
    when(raft.handoffReachablePeers()).thenReturn(Set.of(B.toString()));
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(eq(B), anyLong())).thenReturn(ok);

    // C first in the configuration order, which is the order equal priorities keep
    when(raft.getLivePeers()).thenReturn(List.of(peer(SELF), peer(C), peer(B)));

    assertThat(manager(raft).transferLeadership(10_000)).isTrue();

    verify(admin, never()).transferLeadership(eq(C), anyLong());
    verify(admin).transferLeadership(eq(B), anyLong());
  }

  /** A candidate that cannot win holds only a slice of the budget, so the next one is tried and gets its own slice. */
  @Test
  void aCandidateThatCannotWinHoldsOnlyASliceOfTheBudget() throws Exception {
    final AdminApi admin = mock(AdminApi.class);
    final RaftHAServer raft = leaderWithPeers(admin);
    when(raft.handoffReachablePeers()).thenReturn(Set.of(B.toString(), C.toString()));
    when(raft.getLeaderId()).thenReturn(SELF);
    final RaftClientReply failed = reply(false);
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(eq(B), anyLong())).thenReturn(failed);
    when(admin.transferLeadership(eq(C), anyLong())).thenReturn(ok);

    assertThat(manager(raft).transferLeadership(10_000)).isTrue();

    final ArgumentCaptor<Long> budgetOfB = ArgumentCaptor.forClass(Long.class);
    verify(admin).transferLeadership(eq(B), budgetOfB.capture());
    assertThat(budgetOfB.getValue()).as("the first candidate's transfer timeout").isLessThanOrEqualTo(2_500L);
    verify(admin).transferLeadership(eq(C), anyLong());
    verify(admin, never()).transferLeadership(isNull(), anyLong());
  }

  @Test
  void theCandidateSliceIsAQuarterFlooredAndCappedByWhatIsLeft() {
    assertThat(RaftClusterManager.candidateTransferBudgetMs(10_000L, 10_000L)).isEqualTo(2_500L);
    assertThat(RaftClusterManager.candidateTransferBudgetMs(10_000L, 1_200L)).isEqualTo(1_200L);
    assertThat(RaftClusterManager.candidateTransferBudgetMs(2_000L, 2_000L)).isEqualTo(1_000L);
    assertThat(RaftClusterManager.candidateTransferBudgetMs(200L, 200L)).isEqualTo(200L);
  }

  /** The manual step-down ({@code POST /api/v1/cluster/stepdown}) shares the selection and the screen. */
  @Test
  void theStepDownNeverTargetsAnUnreachablePeer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");
    final ArcadeDBServer server = SubclassMocks.mock(ArcadeDBServer.class);
    when(server.getServerName()).thenReturn("ArcadeDB_0");
    when(server.getConfiguration()).thenReturn(config);
    final RecordingStepDown raft = new RecordingStepDown(server, config);
    try {
      final List<String> peerIds = new ArrayList<>();
      for (final RaftPeer peer : raft.getLivePeers())
        if (!peer.getId().equals(raft.getLocalPeerId()))
          peerIds.add(peer.getId().toString());
      assertThat(peerIds).hasSize(2);
      raft.reachable = Set.of(peerIds.get(1));

      assertThatThrownBy(raft::stepDown).isInstanceOf(ReplicationException.class);

      assertThat(raft.targets).containsExactly(peerIds.get(1));
    } finally {
      raft.stop();
    }
  }

  // -- The #8491 throttle --

  /**
   * The throttle was stamped when an attempt STARTED, and an attempt that aims a transfer at a peer that cannot win
   * outlasts the interval, so the next health tick started another as soon as it returned: a hand-off running back to
   * back, refusing writes all along. Now the interval runs from the END, and widens while attempts keep failing.
   */
  @Test
  void theReplacingLeaderHandOffPausesAfterAnAttemptEndsAndBacksOff() throws Exception {
    final AtomicLong clock = new AtomicLong(1_000_000L);
    final AtomicInteger attempts = new AtomicInteger();
    final RaftHAServer raft = SubclassMocks.mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(true);
    when(raft.transferLeadership(anyLong(), eq(false))).thenAnswer(invocation -> {
      attempts.incrementAndGet();
      clock.addAndGet(13_000L); // an attempt on a dead peer: the transfer budget plus the confirmation grace
      return false;
    });
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setRaftHAServer(raft);
    sm.replacingLeaderHandOffClock = clock::get;

    sm.runUnderInstallGate("db", () -> {
      assertThat(sm.handOffLeadershipWhileReplacingDatabase()).isFalse();
      assertThat(attempts.get()).isEqualTo(1);

      // 18 s after the first attempt STARTED, 5 s after it ended: the old throttle let the second one run here
      clock.addAndGet(5_000L);
      sm.handOffLeadershipWhileReplacingDatabase();
      assertThat(attempts.get()).as("5 s after the previous attempt ended").isEqualTo(1);

      clock.addAndGet(5_000L);
      sm.handOffLeadershipWhileReplacingDatabase();
      assertThat(attempts.get()).as("a full interval after the first failure ended").isEqualTo(2);

      // Second failure in a row: the pause doubles to 20 s
      clock.addAndGet(10_000L);
      sm.handOffLeadershipWhileReplacingDatabase();
      assertThat(attempts.get()).as("10 s after the second failure ended").isEqualTo(2);
      clock.addAndGet(10_000L);
      sm.handOffLeadershipWhileReplacingDatabase();
      assertThat(attempts.get()).as("20 s after the second failure ended").isEqualTo(3);
    });
  }

  /** A hand-off that moved leadership resets the back-off: the next one waits the base interval only. */
  @Test
  void aSuccessfulHandOffResetsTheBackOff() throws Exception {
    final AtomicLong clock = new AtomicLong(1_000_000L);
    final AtomicInteger attempts = new AtomicInteger();
    final RaftHAServer raft = SubclassMocks.mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(true);
    when(raft.transferLeadership(anyLong(), eq(false))).thenAnswer(invocation -> attempts.incrementAndGet() == 3);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setRaftHAServer(raft);
    sm.replacingLeaderHandOffClock = clock::get;

    sm.runUnderInstallGate("db", () -> {
      sm.handOffLeadershipWhileReplacingDatabase();                 // failure 1
      clock.addAndGet(ArcadeStateMachine.replacingLeaderHandOffIntervalMs(1));
      sm.handOffLeadershipWhileReplacingDatabase();                 // failure 2
      clock.addAndGet(ArcadeStateMachine.replacingLeaderHandOffIntervalMs(2));
      assertThat(sm.handOffLeadershipWhileReplacingDatabase()).isTrue(); // moved
      clock.addAndGet(ArcadeStateMachine.REPLACING_LEADER_HAND_OFF_INTERVAL_MS);
      sm.handOffLeadershipWhileReplacingDatabase();
      assertThat(attempts.get()).isEqualTo(4);
    });
  }

  /** The back-off belongs to one episode: once nothing is being replaced, the next install starts from the base. */
  @Test
  void theBackOffDoesNotOutliveTheReplacement() throws Exception {
    final AtomicLong clock = new AtomicLong(1_000_000L);
    final AtomicInteger attempts = new AtomicInteger();
    final RaftHAServer raft = SubclassMocks.mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(true);
    when(raft.transferLeadership(anyLong(), eq(false))).thenAnswer(invocation -> {
      attempts.incrementAndGet();
      return false;
    });
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setRaftHAServer(raft);
    sm.replacingLeaderHandOffClock = clock::get;

    sm.runUnderInstallGate("db", () -> {
      sm.handOffLeadershipWhileReplacingDatabase();
      clock.addAndGet(ArcadeStateMachine.replacingLeaderHandOffIntervalMs(1));
      sm.handOffLeadershipWhileReplacingDatabase();
      clock.addAndGet(ArcadeStateMachine.replacingLeaderHandOffIntervalMs(2));
      sm.handOffLeadershipWhileReplacingDatabase();
    });
    assertThat(attempts.get()).isEqualTo(3);

    sm.handOffLeadershipWhileReplacingDatabase(); // a health tick with nothing being replaced
    clock.addAndGet(ArcadeStateMachine.REPLACING_LEADER_HAND_OFF_INTERVAL_MS);
    sm.runUnderInstallGate("db", () -> sm.handOffLeadershipWhileReplacingDatabase());
    assertThat(attempts.get()).as("a new replacement waits the base interval, not the widened one").isEqualTo(4);
  }

  /**
   * The same reset through the path a health tick really takes: since #8557 the tick queues the hand-off from
   * RaftHAServer.queueReplacingDatabaseHandOff, which returns before reaching the state machine when nothing is being
   * replaced. That early return must still end the episode, or the next replacement waits out the widened interval.
   */
  @Test
  void theHealthTickEndsTheBackOffWhenNothingIsBeingReplaced() throws Exception {
    final AtomicLong clock = new AtomicLong(1_000_000L);
    final AtomicInteger attempts = new AtomicInteger();
    final RaftHAServer raft = SubclassMocks.mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(true);
    when(raft.transferLeadership(anyLong(), eq(false))).thenAnswer(invocation -> {
      attempts.incrementAndGet();
      return false;
    });
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setRaftHAServer(raft);
    sm.replacingLeaderHandOffClock = clock::get;

    sm.runUnderInstallGate("db", () -> {
      sm.handOffLeadershipWhileReplacingDatabase();
      clock.addAndGet(ArcadeStateMachine.replacingLeaderHandOffIntervalMs(1));
      sm.handOffLeadershipWhileReplacingDatabase();
      clock.addAndGet(ArcadeStateMachine.replacingLeaderHandOffIntervalMs(2));
      sm.handOffLeadershipWhileReplacingDatabase();
    });
    assertThat(attempts.get()).isEqualTo(3);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");
    final ArcadeDBServer server = SubclassMocks.mock(ArcadeDBServer.class);
    when(server.getServerName()).thenReturn("ArcadeDB_0");
    when(server.getConfiguration()).thenReturn(config);
    final RecordingStepDown tick = new RecordingStepDown(server, config);
    try {
      tick.queueReplacingDatabaseHandOff(sm); // a health tick with nothing being replaced
    } finally {
      tick.stop();
    }

    clock.addAndGet(ArcadeStateMachine.REPLACING_LEADER_HAND_OFF_INTERVAL_MS);
    sm.runUnderInstallGate("db", () -> sm.handOffLeadershipWhileReplacingDatabase());
    assertThat(attempts.get()).as("a new replacement waits the base interval, not the widened one").isEqualTo(4);
  }

  @Test
  void theBackOffDoublesUpToItsCeiling() {
    assertThat(ArcadeStateMachine.replacingLeaderHandOffIntervalMs(0)).isEqualTo(10_000L);
    assertThat(ArcadeStateMachine.replacingLeaderHandOffIntervalMs(1)).isEqualTo(10_000L);
    assertThat(ArcadeStateMachine.replacingLeaderHandOffIntervalMs(2)).isEqualTo(20_000L);
    assertThat(ArcadeStateMachine.replacingLeaderHandOffIntervalMs(3)).isEqualTo(40_000L);
    assertThat(ArcadeStateMachine.replacingLeaderHandOffIntervalMs(5)).isEqualTo(
        ArcadeStateMachine.REPLACING_LEADER_HAND_OFF_MAX_INTERVAL_MS);
    assertThat(ArcadeStateMachine.replacingLeaderHandOffIntervalMs(Integer.MAX_VALUE)).isEqualTo(
        ArcadeStateMachine.REPLACING_LEADER_HAND_OFF_MAX_INTERVAL_MS);
  }

  // -- helpers --

  private static RaftHAServer leaderWithPeers(final AdminApi admin) {
    final RaftHAServer raft = SubclassMocks.mock(RaftHAServer.class);
    final RaftClient client = mock(RaftClient.class);
    when(client.admin()).thenReturn(admin);
    when(raft.getClient()).thenReturn(client);
    when(raft.getLocalPeerId()).thenReturn(SELF);
    when(raft.isLeader()).thenReturn(true);
    when(raft.getLivePeers()).thenReturn(peers());
    return raft;
  }

  private static RaftClusterManager manager(final RaftHAServer raft) {
    final RaftClusterManager manager = new RaftClusterManager(raft);
    manager.leaderConfirmGraceMs = 50;
    return manager;
  }

  private static List<RaftPeer> peers() {
    return List.of(peer(SELF), peer(B), peer(C));
  }

  private static RaftPeer peer(final RaftPeerId id) {
    return RaftPeer.newBuilder().setId(id).setAddress("localhost:0").build();
  }

  private static Map<String, Object> follower(final RaftPeerId id, final long matchIndex, final long lastRpcElapsedMs) {
    final Map<String, Object> state = new LinkedHashMap<>();
    state.put("peerId", id.toString());
    state.put("matchIndex", matchIndex);
    state.put("nextIndex", matchIndex + 1);
    state.put("lastRpcElapsedMs", lastRpcElapsedMs);
    return state;
  }

  private static RaftClientReply reply(final boolean success) {
    final RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(success);
    return reply;
  }

  /** A real, never-started server whose leadership transfers are recorded and whose reachability is set by the test. */
  private static final class RecordingStepDown extends RaftHAServer {
    private final List<String> targets   = new ArrayList<>();
    private       Set<String>  reachable = Set.of();

    private RecordingStepDown(final ArcadeDBServer server, final ContextConfiguration config) {
      super(server, config);
    }

    @Override
    public boolean isLeader() {
      return true;
    }

    @Override
    Set<String> handoffReachablePeers() {
      return reachable;
    }

    @Override
    public void transferLeadership(final String targetPeerId, final long timeoutMs) {
      targets.add(targetPeerId);
      throw new ReplicationException("Transfer timed out: " + targetPeerId);
    }

    @Override
    boolean stepDownWithoutTarget(final long timeoutMs) {
      return false;
    }
  }
}
