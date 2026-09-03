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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7041 (follow-up to #4842).
 * <p>
 * When membership changes while the leader reads its follower indices, {@link RaftHAServer#getFollowerStates()}
 * degrades to entries WITHOUT a {@code matchIndex} rather than attributing an index to the wrong peer. The
 * exporter unboxed that key with a raw {@code (Long)} cast at two sites, both inside catch-alls, so the
 * {@code NullPointerException} never surfaced: the CLUSTER CONFIGURATION table was silently suppressed and the
 * lag-monitor tick aborted before the remaining followers were classified - exactly while membership churned.
 * <p>
 * Drives the exporter against a mocked {@link RaftHAServer} that answers with one degraded and one complete
 * follower entry, the shape {@link RaftHAServer#degradedFollowerStates} produces.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7041DegradedFollowerStateTest {

  private static final RaftPeerId LEADER   = RaftPeerId.valueOf("leader");
  private static final RaftPeerId DEGRADED = RaftPeerId.valueOf("degraded");
  private static final RaftPeerId HEALTHY  = RaftPeerId.valueOf("healthy");

  /** Exporter with the log-emission seam captured; data collection runs exactly as in production. */
  private static final class CapturingExporter extends RaftClusterStatusExporter {
    private final List<String> emitted = new ArrayList<>();

    CapturingExporter(final RaftHAServer haServer, final ClusterMonitor clusterMonitor) {
      super(haServer, clusterMonitor);
    }

    @Override
    void emit(final String output) {
      emitted.add(output);
    }
  }

  private RaftHAServer   haServer;
  private ClusterMonitor clusterMonitor;

  @BeforeEach
  void setUp() {
    haServer = mock(RaftHAServer.class);
    clusterMonitor = new ClusterMonitor(10L);

    when(haServer.isLeader()).thenReturn(true);
    when(haServer.getLeaderId()).thenReturn(LEADER);
    when(haServer.getCurrentTerm()).thenReturn(7L);
    when(haServer.getCommitIndex()).thenReturn(100L);
    when(haServer.getConfiguredServers()).thenReturn(3);
    when(haServer.getReplicationLatencies()).thenReturn(Map.of());
    when(haServer.getStateMachine()).thenReturn(null);
    when(haServer.getLivePeers()).thenReturn(List.of(peer(LEADER), peer(DEGRADED), peer(HEALTHY)));

    // The degraded entry comes first so the old raw cast aborted the loop BEFORE the healthy peer was read.
    when(haServer.getFollowerStates()).thenReturn(List.of(degradedState(DEGRADED, 12L), completeState(HEALTHY, 100L, 3L)));
  }

  @Test
  void degradedFollowerKeepsItsRowWithUnknownLagInsteadOfSuppressingTheTable() {
    final CapturingExporter exporter = new CapturingExporter(haServer, clusterMonitor);

    exporter.printClusterConfiguration();

    assertThat(exporter.emitted).as("the table must be emitted even though one entry has no match index").hasSize(1);
    final String table = exporter.emitted.get(0);
    assertThat(table).contains("CLUSTER CONFIGURATION (term=7, commitIndex=100)");
    assertThat(table).contains("degraded");
    assertThat(table).contains("healthy");

    final String degradedRow = rowOf(table, "degraded");
    assertThat(degradedRow).as("lag is reported as unknown, not as a number Ratis never gave").contains("| " + RaftClusterStatusExporter.LAG_UNKNOWN + " ");
    assertThat(degradedRow).as("last contact is still known for a degraded entry").contains("12 ms");

    final String healthyRow = rowOf(table, "healthy");
    assertThat(healthyRow).contains("| 0 ").contains("3 ms");
  }

  @Test
  void lagTickSkipsTheDegradedFollowerAndStillClassifiesTheOthers() {
    final CapturingExporter exporter = new CapturingExporter(haServer, clusterMonitor);

    exporter.checkReplicaLag();

    assertThat(clusterMonitor.getReplicaStatus(HEALTHY.toString()))
        .as("the tick must reach the followers listed after the degraded one")
        .isEqualTo(ClusterMonitor.ReplicaStatus.HEALTHY);
    assertThat(clusterMonitor.getReplicaStatus(DEGRADED.toString()))
        .as("a peer whose index is unknown keeps its previous classification rather than one derived from a placeholder")
        .isEqualTo(ClusterMonitor.ReplicaStatus.UNKNOWN);
    assertThat(exporter.emitted).as("the tick re-emits the table it used to lose").hasSize(1);
  }

  @Test
  void followerStateIndexReadsAbsentKeysAsUnknown() {
    final Map<String, Object> degraded = degradedState(DEGRADED, 5L);
    assertThat(RaftHAServer.hasFollowerStateIndex(degraded, "matchIndex")).isFalse();
    assertThat(RaftHAServer.followerStateIndex(degraded, "matchIndex")).isEqualTo(-1L);
    assertThat(RaftHAServer.followerStateIndex(degraded, "lastRpcElapsedMs")).isEqualTo(5L);

    final Map<String, Object> complete = completeState(HEALTHY, 42L, 1L);
    assertThat(RaftHAServer.hasFollowerStateIndex(complete, "matchIndex")).isTrue();
    assertThat(RaftHAServer.followerStateIndex(complete, "matchIndex")).isEqualTo(42L);
  }

  private static RaftPeer peer(final RaftPeerId id) {
    return RaftPeer.newBuilder().setId(id).setAddress(id + ":2434").build();
  }

  /** Same shape as {@link RaftHAServer#degradedFollowerStates}: peer id and last-RPC elapsed only. */
  private static Map<String, Object> degradedState(final RaftPeerId id, final long lastRpcElapsedMs) {
    final Map<String, Object> state = new LinkedHashMap<>();
    state.put("peerId", id.toString());
    state.put("lastRpcElapsedMs", lastRpcElapsedMs);
    return state;
  }

  private static Map<String, Object> completeState(final RaftPeerId id, final long matchIndex, final long lastRpcElapsedMs) {
    final Map<String, Object> state = degradedState(id, lastRpcElapsedMs);
    state.put("matchIndex", matchIndex);
    state.put("nextIndex", matchIndex + 1);
    return state;
  }

  private static String rowOf(final String table, final String peerId) {
    for (final String line : table.split("\n"))
      if (line.startsWith("| " + peerId + " "))
        return line;
    throw new AssertionError("No row for '" + peerId + "' in:\n" + table);
  }
}
