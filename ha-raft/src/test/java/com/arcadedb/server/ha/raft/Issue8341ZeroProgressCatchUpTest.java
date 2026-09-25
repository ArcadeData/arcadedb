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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.FollowerSample;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8341: a follower that is catching up with ZERO progress was classified as healthy catch-up on both
 * sides of the cluster, so nothing reported it while it silently cost the cluster its fault tolerance.
 * <ul>
 *   <li>Leader side ({@link ClusterMonitor}): a replica over the lag threshold whose {@code matchIndex} does not
 *   move was {@code STALLED} only on a tick where the leader's commit index advanced. When the stuck replica is
 *   the vote the leader is missing, the leader cannot commit at all, so the replica read {@code CATCHING_UP}
 *   ("advancing at 0 entries/tick") forever and the {@code lagging-followers} alert never fired.</li>
 *   <li>Follower side ({@link RaftHAServer}): the {@code catchingUp} flag of the state machine is sticky - it is
 *   cleared only by an apply that reaches the commit index - so a catch-up that stops applying kept exempting
 *   the node from both the stale-term and the persistent-lag checks.</li>
 * </ul>
 */
class Issue8341ZeroProgressCatchUpTest {

  private static final String REPLICA = "proxy_8661";

  // -- Leader side: ClusterMonitor classification --

  /**
   * The step-16 shape from the issue: a new leader (fresh baseline after {@link ClusterMonitor#reset()}) sees a
   * replica 26792 entries behind that does not move, and cannot advance its own commit index because that replica
   * is the missing quorum vote. It must be reported STALLED once the zero-progress spell outlasts the grace.
   */
  @Test
  void zeroProgressReplicaIsStalledEvenWhenTheLeaderCannotCommit() {
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.setClock(now::get);
    monitor.reset();

    monitor.updateLeaderCommitIndex(362_000);
    monitor.updateReplicaMatchIndex(REPLICA, 335_208, 0L);
    assertThat(monitor.getReplicaStatus(REPLICA)).as("first tick of a fresh baseline cannot judge progress")
        .isNotEqualTo(ClusterMonitor.ReplicaStatus.STALLED);

    now.set(5_000);
    monitor.updateLeaderCommitIndex(362_000); // no quorum: the leader is stuck too
    monitor.updateReplicaMatchIndex(REPLICA, 335_208, 0L);
    assertThat(monitor.getReplicaStatus(REPLICA)).as("still inside the zero-progress grace")
        .isNotEqualTo(ClusterMonitor.ReplicaStatus.STALLED);

    now.set(ClusterMonitor.ZERO_PROGRESS_STALL_GRACE_MS);
    monitor.updateLeaderCommitIndex(362_000);
    monitor.updateReplicaMatchIndex(REPLICA, 335_208, 0L);
    assertThat(monitor.getReplicaStatus(REPLICA)).isEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
  }

  /** A replica that is behind but advances every tick is catching up, however quiet the leader is. */
  @Test
  void progressingReplicaWithAQuietLeaderIsCatchingUp() {
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.setClock(now::get);

    long match = 100_000;
    monitor.updateLeaderCommitIndex(200_000);
    monitor.updateReplicaMatchIndex(REPLICA, match, 0L);
    for (int tick = 1; tick <= 10; tick++) {
      now.set(tick * 5_000L);
      match += 500;
      monitor.updateLeaderCommitIndex(200_000);
      monitor.updateReplicaMatchIndex(REPLICA, match, 0L);
      assertThat(monitor.getReplicaStatus(REPLICA)).as("tick %d", tick).isEqualTo(ClusterMonitor.ReplicaStatus.CATCHING_UP);
    }
  }

  /** Any movement of matchIndex ends the zero-progress spell, so two short pauses never add up to a stall. */
  @Test
  void progressRestartsTheZeroProgressSpell() {
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.setClock(now::get);

    monitor.updateLeaderCommitIndex(50_000);
    monitor.updateReplicaMatchIndex(REPLICA, 10_000, 0L); // t=0, spell starts
    now.set(ClusterMonitor.ZERO_PROGRESS_STALL_GRACE_MS - 1);
    monitor.updateReplicaMatchIndex(REPLICA, 10_000, 0L);
    now.set(ClusterMonitor.ZERO_PROGRESS_STALL_GRACE_MS);
    monitor.updateReplicaMatchIndex(REPLICA, 10_001, 0L); // moved: spell over
    assertThat(monitor.getReplicaStatus(REPLICA)).isEqualTo(ClusterMonitor.ReplicaStatus.CATCHING_UP);

    now.set(2 * ClusterMonitor.ZERO_PROGRESS_STALL_GRACE_MS - 1);
    monitor.updateReplicaMatchIndex(REPLICA, 10_001, 0L);
    assertThat(monitor.getReplicaStatus(REPLICA)).as("a new spell has not reached the grace yet")
        .isNotEqualTo(ClusterMonitor.ReplicaStatus.STALLED);

    now.set(3 * ClusterMonitor.ZERO_PROGRESS_STALL_GRACE_MS);
    monitor.updateReplicaMatchIndex(REPLICA, 10_001, 0L);
    assertThat(monitor.getReplicaStatus(REPLICA)).isEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
  }

  /** An idle cluster where the replica is within the lag threshold stays HEALTHY: no progress is needed there. */
  @Test
  void idleCaughtUpReplicaStaysHealthy() {
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.setClock(now::get);
    for (int tick = 0; tick <= 10; tick++) {
      now.set(tick * 5_000L);
      monitor.updateLeaderCommitIndex(5_000);
      monitor.updateReplicaMatchIndex(REPLICA, 4_990, 0L);
      assertThat(monitor.getReplicaStatus(REPLICA)).isEqualTo(ClusterMonitor.ReplicaStatus.HEALTHY);
    }
  }

  /**
   * What an operator reads: the status the monitor reports feeds the {@code lagging-followers} alert of
   * {@code GET /api/v1/cluster}. Before the fix the stuck replica read CATCHING_UP, which that alert ignores.
   */
  @Test
  void zeroProgressReplicaRaisesTheCriticalLaggingFollowersAlert() {
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.setClock(now::get);
    for (long t = 0; t <= ClusterMonitor.ZERO_PROGRESS_STALL_GRACE_MS; t += 5_000) {
      now.set(t);
      monitor.updateLeaderCommitIndex(362_000);
      monitor.updateReplicaMatchIndex(REPLICA, 335_208, 0L);
    }

    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLaggingFollowerAlert(List.of(new FollowerSample(REPLICA, 335_208, 335_209,
        monitor.getReplicaLags().get(REPLICA), 50, monitor.getReplicaStatus(REPLICA).name(),
        monitor.getReplicaLaggingForMs(REPLICA))), alerts);

    assertThat(alerts.length()).isEqualTo(1);
    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("id")).isEqualTo("lagging-followers");
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
    assertThat(alert.getJSONObject("details").getJSONArray("nodes").getJSONObject(0).getString("peerId")).isEqualTo(REPLICA);
  }

  /**
   * The recovery the issue asks for already exists leader-side and must keep engaging in this shape: the
   * leader-driven resync (#4728) fires for a zero-progress replica on a leader that cannot commit.
   */
  @Test
  void zeroProgressReplicaTriggersTheLeaderDrivenResync() {
    final List<String> resynced = new ArrayList<>();
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(1000L, 30_000L, resynced::add);
    monitor.setClock(now::get);
    for (long t = 0; t <= 30_000; t += 5_000) {
      now.set(t);
      monitor.updateLeaderCommitIndex(362_000);
      monitor.updateReplicaMatchIndex(REPLICA, 335_208, 0L);
    }
    assertThat(resynced).containsExactly(REPLICA);
  }

  // -- Follower side: the sticky catchingUp flag --

  @Test
  void catchUpFlagDoesNotExemptAFollowerThatHasAppliedEverythingItCommitted() {
    assertThat(RaftHAServer.isActivelyCatchingUp(true, 10, 10)).isFalse();
    assertThat(RaftHAServer.isActivelyCatchingUp(true, 8, 10)).isTrue();
    assertThat(RaftHAServer.isActivelyCatchingUp(false, 8, 10)).isFalse();
    // Unreadable state: keep the flag's answer rather than guess.
    assertThat(RaftHAServer.isActivelyCatchingUp(true, -1, 10)).isTrue();
    assertThat(RaftHAServer.isActivelyCatchingUp(true, 10, -1)).isTrue();
  }

  @Test
  void catchUpFlagExemptsTheLagCheckOnlyWhileTheAppliedIndexMoves() {
    assertThat(RaftHAServer.catchUpExemptsLagCheck(true, 100, -1)).as("no prior sample").isTrue();
    assertThat(RaftHAServer.catchUpExemptsLagCheck(true, 150, 100)).as("advancing").isTrue();
    assertThat(RaftHAServer.catchUpExemptsLagCheck(true, 100, 100)).as("zero progress").isFalse();
    assertThat(RaftHAServer.catchUpExemptsLagCheck(false, 150, 100)).as("not catching up").isFalse();
  }

  /**
   * The caller, not only the helper: a follower whose state machine still carries a stale {@code catchingUp}
   * flag, with {@code commitIndex == appliedIndex} at a stale term, is reported stuck.
   */
  @Test
  void staleCatchUpFlagNoLongerHidesTheStuckAtStaleTermSignature() throws Exception {
    final ArcadeStateMachine sm = mock(ArcadeStateMachine.class);
    when(sm.isCatchingUp()).thenReturn(true); // set by an earlier gap, never cleared
    when(sm.isSnapshotDownloadPending()).thenReturn(false);
    when(sm.getLastAppliedTermIndex()).thenReturn(TermIndex.valueOf(8, 228_631));

    final RaftHAServer server = followerWith(sm, 9, 228_631, 228_631);
    when(server.isFollowerStuckDiverged()).thenCallRealMethod();

    assertThat(server.isFollowerStuckDiverged()).isTrue();
  }

  /**
   * The caller of the lag check: a follower whose catch-up stopped applying (commit far ahead, applied frozen) is
   * reported lagging on the second tick instead of being exempted forever by the sticky flag.
   */
  @Test
  void stalledCatchUpIsReportedLaggingOnTheSecondTick() throws Exception {
    final ArcadeStateMachine sm = mock(ArcadeStateMachine.class);
    when(sm.isCatchingUp()).thenReturn(true);
    when(sm.isSnapshotDownloadPending()).thenReturn(false);

    final RaftHAServer server = followerWith(sm, 9, 250_000, 228_631);
    when(server.isFollowerLaggingBeyond(1000L)).thenCallRealMethod();
    setField(server, "lastLagCheckAppliedIndex", -1L);

    assertThat(server.isFollowerLaggingBeyond(1000L)).as("first tick only records the baseline").isFalse();
    assertThat(server.isFollowerLaggingBeyond(1000L)).as("zero progress while catching up").isTrue();
  }

  @Test
  void progressingCatchUpIsStillExemptFromTheLagCheck() throws Exception {
    final ArcadeStateMachine sm = mock(ArcadeStateMachine.class);
    when(sm.isCatchingUp()).thenReturn(true);
    when(sm.isSnapshotDownloadPending()).thenReturn(false);

    final RaftHAServer server = followerWith(sm, 9, 250_000, 228_631);
    when(server.isFollowerLaggingBeyond(1000L)).thenCallRealMethod();
    setField(server, "lastLagCheckAppliedIndex", -1L);

    assertThat(server.isFollowerLaggingBeyond(1000L)).isFalse();
    when(server.getLastAppliedIndex()).thenReturn(230_000L);
    assertThat(server.isFollowerLaggingBeyond(1000L)).isFalse();
    when(server.getLastAppliedIndex()).thenReturn(231_000L);
    assertThat(server.isFollowerLaggingBeyond(1000L)).isFalse();
  }

  private static RaftHAServer followerWith(final ArcadeStateMachine sm, final long currentTerm, final long commitIndex,
      final long appliedIndex) throws Exception {
    final RaftHAServer server = mock(RaftHAServer.class);
    setField(server, "raftServer", mock(RaftServer.class));
    setField(server, "stateMachine", sm);
    setField(server, "shutdownRequested", false);
    when(server.isLeader()).thenReturn(false);
    when(server.getLeaderId()).thenReturn(RaftPeerId.valueOf("leader"));
    when(server.getCurrentTerm()).thenReturn(currentTerm);
    when(server.getCommitIndex()).thenReturn(commitIndex);
    when(server.getLastAppliedIndex()).thenReturn(appliedIndex);
    return server;
  }

  private static void setField(final Object target, final String name, final Object value) throws Exception {
    final Field f = RaftHAServer.class.getDeclaredField(name);
    f.setAccessible(true);
    f.set(target, value);
  }
}
