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

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8457: the lag tick must actually forward the follower's {@code nextIndex} and the leader's own log start
 * from {@link RaftHAServer} to {@link ClusterMonitor}. {@link Issue8457InstallSnapshotNotifyLoopTest} drives the
 * monitor directly; this drives {@link RaftClusterStatusExporter#checkReplicaLag()} against a mocked server.
 */
class Issue8457NotifyLoopExporterWiringTest {

  private static final RaftPeerId LEADER = RaftPeerId.valueOf("leader");
  private static final RaftPeerId STUCK  = RaftPeerId.valueOf("stuck");

  private static final class QuietExporter extends RaftClusterStatusExporter {
    QuietExporter(final RaftHAServer haServer, final ClusterMonitor clusterMonitor) {
      super(haServer, clusterMonitor);
    }

    @Override
    void emit(final String output) {
    }
  }

  private final AtomicLong now = new AtomicLong(0);
  private RaftHAServer     haServer;
  private ClusterMonitor   monitor;

  @BeforeEach
  void setUp() {
    haServer = mock(RaftHAServer.class);
    monitor = new ClusterMonitor(1000L, 60_000L, id -> {
    });
    monitor.setClock(now::get);

    when(haServer.isLeader()).thenReturn(true);
    when(haServer.getLeaderId()).thenReturn(LEADER);
    when(haServer.getCurrentTerm()).thenReturn(3L);
    when(haServer.getCommitIndex()).thenReturn(105L);
    when(haServer.getConfiguredServers()).thenReturn(2);
    when(haServer.getReplicationLatencies()).thenReturn(Map.of());
    when(haServer.getStateMachine()).thenReturn(null);
    when(haServer.getLivePeers()).thenReturn(List.of(peer(LEADER), peer(STUCK)));
  }

  @Test
  void lagTickForwardsNextIndexAndLogStartSoTheLoopIsDetected() {
    when(haServer.getRaftLogStartIndex()).thenReturn(100L);
    when(haServer.getFollowerStates()).thenReturn(List.of(state(true)));

    tickAt(0);
    tickAt(60_000);

    assertThat(monitor.getReplicaStatus(STUCK.toString())).isEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
  }

  @Test
  void unknownLeaderLogStartLeavesTheReplicaHealthy() {
    when(haServer.getRaftLogStartIndex()).thenReturn(-1L);
    when(haServer.getFollowerStates()).thenReturn(List.of(state(true)));

    tickAt(0);
    tickAt(60_000);

    assertThat(monitor.getReplicaStatus(STUCK.toString())).isEqualTo(ClusterMonitor.ReplicaStatus.HEALTHY);
  }

  @Test
  void entryWithoutNextIndexLeavesTheReplicaHealthy() {
    when(haServer.getRaftLogStartIndex()).thenReturn(100L);
    when(haServer.getFollowerStates()).thenReturn(List.of(state(false)));

    tickAt(0);
    tickAt(60_000);

    assertThat(monitor.getReplicaStatus(STUCK.toString())).isEqualTo(ClusterMonitor.ReplicaStatus.HEALTHY);
  }

  private void tickAt(final long timeMs) {
    now.set(timeMs);
    new QuietExporter(haServer, monitor).checkReplicaLag();
  }

  /** matchIndex 99, lag 6 (under the 1000 threshold), nextIndex 100 at the leader's log start. */
  private static Map<String, Object> state(final boolean withNextIndex) {
    final Map<String, Object> s = new LinkedHashMap<>();
    s.put("peerId", STUCK.toString());
    s.put("matchIndex", 99L);
    if (withNextIndex)
      s.put("nextIndex", 100L);
    s.put("lastRpcElapsedMs", 1L);
    return s;
  }

  private static RaftPeer peer(final RaftPeerId id) {
    return RaftPeer.newBuilder().setId(id).setAddress(id + ":2434").build();
  }
}
