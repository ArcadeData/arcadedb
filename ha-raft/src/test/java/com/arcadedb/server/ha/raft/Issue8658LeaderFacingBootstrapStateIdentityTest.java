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
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8658: the leader-facing consumers of {@code POST /api/v1/cluster/bootstrap-state} accepted
 * an answer from any node, so an address that resolved to a node other than the leader had that node's databases,
 * snapshot marker or fingerprints acted on as the leader's. Each consumer now checks the {@code peerId} of the answer
 * against the peer it meant to reach: the install fails (so Ratis retries) and the divergence probe answers nothing (so
 * the mark stays reported).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8658LeaderFacingBootstrapStateIdentityTest {

  private static final String ANSWER_OF_A_STRANGER = """
      {"peerId":"stranger","databases":[{"name":"db","fingerprint":"f","lastTxId":5}],"snapshotTerm":3,"snapshotIndex":30}""";
  private static final String ANSWER_OF_THE_LEADER = """
      {"peerId":"leader","databases":[{"name":"db","fingerprint":"f","lastTxId":5}],"snapshotTerm":3,"snapshotIndex":30}""";

  // ---- DatabaseReconciler: the install ----

  @Test
  void anInstallFailsWhenTheListingIsAnsweredByAnotherNodeAndDoesNotDegradeToTheMarker() {
    final StubReconciler reconciler = new StubReconciler();
    configure(reconciler, true);

    assertThatThrownBy(() -> reconciler.reconcileDatabasesFromLeader("leader", "leader:2480", null, null, -1L))
        .isInstanceOf(LeaderDatabaseQuery.WrongPeerAnsweredException.class);
    assertThat(reconciler.fullCalls).isEqualTo(1);
    assertThat(reconciler.markerCalls).as("the marker would be answered by the same stranger").isZero();
  }

  @Test
  void anInstallFailsWhenTheMarkerIsAnsweredByAnotherNode() {
    final StubReconciler reconciler = new StubReconciler();
    configure(reconciler, false);

    assertThatThrownBy(() -> reconciler.reconcileDatabasesFromLeader("leader", "leader:2480", null, null, -1L))
        .isInstanceOf(IOException.class)
        .hasRootCauseInstanceOf(LeaderDatabaseQuery.WrongPeerAnsweredException.class)
        .hasMessageContaining("stranger");
    assertThat(reconciler.markerCalls).isEqualTo(1);
  }

  // ---- BootstrapElection.fetchBootstrapState: the divergence check ----

  @Test
  void aDivergenceProbeAnsweredByAnotherNodeReturnsNoStates() throws Exception {
    try (final Peer peer = new Peer(ANSWER_OF_A_STRANGER)) {
      assertThat(BootstrapElection.fetchBootstrapState(null, "leader", peer.address(), null, "token", Set.of("db"), 5_000L))
          .as("another node's fingerprints must not clear or raise the leader's divergence")
          .isNull();
    }
  }

  @Test
  void aDivergenceProbeAnsweredByTheLeaderReturnsItsStates() throws Exception {
    try (final Peer peer = new Peer(ANSWER_OF_THE_LEADER)) {
      final Map<String, ArcadeStateMachine.BootstrapBaseline> states = BootstrapElection.fetchBootstrapState(null, "leader",
          peer.address(), null, "token", Set.of("db"), 5_000L);
      assertThat(states).containsOnlyKeys("db");
      assertThat(states.get("db").lastTxId()).isEqualTo(5L);
    }
  }

  // ---- LeaderDatabaseQuery over the wire ----

  @Test
  void theQueryRefusesAStrangerOverTheWire() throws Exception {
    try (final Peer peer = new Peer(ANSWER_OF_A_STRANGER)) {
      assertThatThrownBy(() -> LeaderDatabaseQuery.fetch("leader", peer.address(), null, "token", 5_000L, null))
          .isInstanceOf(LeaderDatabaseQuery.WrongPeerAnsweredException.class);
      assertThatThrownBy(() -> LeaderDatabaseQuery.fetchSnapshotMarker("leader", peer.address(), null, "token", 5_000L, null))
          .isInstanceOf(LeaderDatabaseQuery.WrongPeerAnsweredException.class);
    }
  }

  @Test
  void theQueryAcceptsTheLeaderOverTheWire() throws Exception {
    try (final Peer peer = new Peer(ANSWER_OF_THE_LEADER)) {
      assertThat(LeaderDatabaseQuery.fetch("leader", peer.address(), null, "token", 5_000L, null).databases()).hasSize(1);
    }
  }

  // ---- helpers ----

  private static void configure(final DatabaseReconciler reconciler, final boolean autoAcquire) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, autoAcquire);
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.getDatabaseNames()).thenReturn(Set.of("db"));
    reconciler.setServer(server);
  }

  private static final class StubReconciler extends DatabaseReconciler {
    private int fullCalls;
    private int markerCalls;

    @Override
    LeaderDatabaseQuery.BootstrapState fetchBootstrapState(final String leaderPeerId, final String leaderHttpAddr,
        final String leaderHttpsAddr, final String clusterToken) throws IOException {
      fullCalls++;
      throw new LeaderDatabaseQuery.WrongPeerAnsweredException("answered by peer 'stranger'");
    }

    @Override
    LeaderDatabaseQuery.BootstrapState fetchSnapshotMarker(final String leaderPeerId, final String leaderHttpAddr,
        final String leaderHttpsAddr, final String clusterToken) throws IOException {
      markerCalls++;
      throw new LeaderDatabaseQuery.WrongPeerAnsweredException("answered by peer 'stranger'");
    }
  }

  /** A peer answering every request with one fixed document. */
  private static final class Peer implements AutoCloseable {
    private final HttpServer server;

    Peer(final String body) throws IOException {
      server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
      server.createContext("/", exchange -> {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
      });
      server.start();
    }

    String address() {
      return "127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }
}
