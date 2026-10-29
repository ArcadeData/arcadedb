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
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10861: a peer that joined the Raft configuration at runtime (the Kubernetes auto-join, {@code addPeer}) was
 * replicated to and never routed to. Membership came from the live configuration (#7040) and the peer's HTTP address was
 * learnt (#9255), but every path that turns a peer into an ADDRESS still read the group built once from
 * {@code arcadedb.ha.serverList}: the Bolt/gRPC {@code ROUTE} table dropped the peer without a word, and
 * {@code /api/v1/cluster} reported it without an {@code httpAddress}. A joined peer that won an election left the cluster
 * with no writer in the routing table at all.
 * <p>
 * The server is a real, detached {@link RaftHAServer} (Ratis never started) whose live configuration is the one this test
 * says it is: the three pods of the server list plus the one that joined.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10861JoinedPeerAddressTest {
  private static final String SERVER_LIST = "arcadedb-0.svc:2434:2480,arcadedb-1.svc:2434:2480,arcadedb-2.svc:2434:2480";
  private static final int    BOLT_PORT   = 7687;

  private static final RaftPeerId POD_0  = RaftPeerId.valueOf("arcadedb-0.svc_2434");
  private static final RaftPeerId POD_1  = RaftPeerId.valueOf("arcadedb-1.svc_2434");
  private static final RaftPeerId POD_2  = RaftPeerId.valueOf("arcadedb-2.svc_2434");
  private static final RaftPeer   JOINED = RaftPeer.newBuilder().setId("arcadedb-3.svc_2434").setAddress("arcadedb-3.svc:2434")
      .build();

  /** The joined pod is a reader of the Bolt routing table of the leader it joined (it was missing: 3 of 4 pods). */
  @Test
  void aJoinedPeerIsAReaderOfTheRoutingTable() {
    final RaftHAServer raft = serverWithLivePeers(JOINED);

    final HAServerPlugin.RoutingTable table = raft.routingTableFor(HAServerPlugin.ROUTING_PROTOCOL.BOLT, POD_2);

    assertThat(table).isNotNull();
    assertThat(table.writer()).isEqualTo("arcadedb-2.svc:" + BOLT_PORT);
    assertThat(table.readers()).containsExactlyInAnyOrder("arcadedb-0.svc:" + BOLT_PORT, "arcadedb-1.svc:" + BOLT_PORT,
        "arcadedb-3.svc:" + BOLT_PORT);
  }

  /**
   * A joined pod that won an election is the writer. The writer is resolved by id, and resolving the joined pod against the
   * server list answered nothing, so the table was {@code null}: Bolt then advertises the node it was asked as READ/ROUTE
   * only, and a client sees a cluster with no writer.
   */
  @Test
  void aJoinedPeerThatLeadsIsTheWriter() {
    final RaftHAServer raft = serverWithLivePeers(JOINED);

    final HAServerPlugin.RoutingTable table = raft.routingTableFor(HAServerPlugin.ROUTING_PROTOCOL.BOLT, JOINED.getId());

    assertThat(table).isNotNull();
    assertThat(table.writer()).isEqualTo("arcadedb-3.svc:" + BOLT_PORT);
    assertThat(table.readers()).hasSize(3).doesNotContain("arcadedb-3.svc:" + BOLT_PORT);
  }

  /**
   * The address the leader learnt for the joined pod (#9255) is reported by the cluster-status view, and handed out by the
   * guarded accessor every peer-to-peer dial goes through. Before, the learnt address sat in the map and was never read.
   */
  @Test
  void theLearntHttpAddressOfAJoinedPeerIsReported() {
    final RaftHAServer raft = serverWithLivePeers(JOINED);
    raft.getHttpAddresses().put(JOINED.getId(), "arcadedb-3.svc:2480");

    final Map<RaftPeerId, RaftHAServer.PeerHttpEndpoint> endpoints = raft.getPeerHttpEndpoints();

    assertThat(endpoints).containsKeys(POD_0, POD_1, POD_2, JOINED.getId());
    assertThat(endpoints.get(JOINED.getId()).address()).isEqualTo("arcadedb-3.svc:2480");
    assertThat(endpoints.get(JOINED.getId()).ambiguous()).isFalse();
    assertThat(raft.getUnambiguousPeerHttpAddress(JOINED.getId())).isEqualTo("arcadedb-3.svc:2480");
    assertThat(PeerDialAddress.resolve(raft, JOINED.getId(), "peer").httpAddress()).isEqualTo("arcadedb-3.svc:2480");
  }

  /**
   * The joined pod is also a claimant in the ambiguity check: a declared pod whose address collides with the one the
   * joined pod answers on is no longer handed out as if it were its own alone.
   */
  @Test
  void aJoinedPeerCountsAsAClaimantOfItsAddress() {
    final RaftHAServer raft = serverWithLivePeers(JOINED);
    // A misconfiguration the server list cannot show: the joined pod answers on the address pod 1 declared
    raft.getHttpAddresses().put(JOINED.getId(), "arcadedb-1.svc:2480");

    final Map<RaftPeerId, RaftHAServer.PeerHttpEndpoint> endpoints = raft.getPeerHttpEndpoints();
    assertThat(endpoints.get(JOINED.getId()).ambiguous()).isTrue();
    assertThat(endpoints.get(POD_1).ambiguous()).isTrue();
    assertThat(raft.getUnambiguousPeerHttpAddress(POD_1))
        .as("both pods claim it and both stated it: neither may be dialled on it")
        .isNull();
    assertThat(raft.getUnambiguousPeerHttpAddress(JOINED.getId())).isNull();
  }

  /**
   * A pod that rejoined under its old id at a new address is dialled where the cluster committed it, not where the server
   * list declared it: the live configuration's entry wins, as it does in the membership the status endpoint reports.
   */
  @Test
  void aRejoinedPeerIsResolvedAtItsCommittedAddress() {
    final RaftPeer moved = RaftPeer.newBuilder().setId(POD_1).setAddress("arcadedb-1-new.svc:2434").build();
    final RaftHAServer raft = serverWithLiveConfiguration(List.of(declared(POD_0), moved, declared(POD_2)));

    final HAServerPlugin.RoutingTable asLeader = raft.routingTableFor(HAServerPlugin.ROUTING_PROTOCOL.BOLT, POD_1);
    assertThat(asLeader.writer()).isEqualTo("arcadedb-1-new.svc:" + BOLT_PORT);

    final HAServerPlugin.RoutingTable asReader = raft.routingTableFor(HAServerPlugin.ROUTING_PROTOCOL.BOLT, POD_0);
    assertThat(asReader.readers()).contains("arcadedb-1-new.svc:" + BOLT_PORT).doesNotContain("arcadedb-1.svc:" + BOLT_PORT);
  }

  /**
   * The other direction must not regress (#7040): a declared pod the configuration dropped still resolves an address - the
   * step-down and handoff paths rely on it - and is still reported by the status view, but it is never a reader.
   */
  @Test
  void aDeclaredPeerTheConfigurationDroppedStillResolvesButIsNotAReader() {
    final RaftHAServer raft = serverWithLiveConfiguration(List.of(declared(POD_0), declared(POD_1), JOINED));

    final HAServerPlugin.RoutingTable table = raft.routingTableFor(HAServerPlugin.ROUTING_PROTOCOL.BOLT, POD_0);
    assertThat(table.readers()).containsExactlyInAnyOrder("arcadedb-1.svc:" + BOLT_PORT, "arcadedb-3.svc:" + BOLT_PORT);

    assertThat(raft.routingTableFor(HAServerPlugin.ROUTING_PROTOCOL.BOLT, POD_2).writer())
        .as("resolved from the server list, which still declares it")
        .isEqualTo("arcadedb-2.svc:" + BOLT_PORT);
    assertThat(raft.getPeerHttpEndpoints()).containsKey(POD_2);
    assertThat(raft.getUnambiguousPeerHttpAddress(POD_2)).isEqualTo("arcadedb-2.svc:2480");
  }

  /**
   * A peer no address resolves for is refused with a reason that fits it. The old message told the operator to declare the
   * peer's 'http' port in the server list, which a pod that joined at runtime has no entry in.
   */
  @Test
  void anUnresolvablePeerIsNotToldToDeclareAPortInTheServerList() {
    final RaftHAServer raft = serverWithLivePeers();

    final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf("arcadedb-9.svc_2434"), "peer");

    assertThat(dial.refused()).isTrue();
    assertThat(dial.refusal()).contains("none resolves for it").doesNotContain(GlobalConfiguration.HA_SERVER_LIST.getKey());
  }

  /** The server list's three pods, plus {@code joined}, as the live configuration. */
  private static RaftHAServer serverWithLivePeers(final RaftPeer... joined) {
    final List<RaftPeer> live = new ArrayList<>(List.of(declared(POD_0), declared(POD_1), declared(POD_2)));
    live.addAll(List.of(joined));
    return serverWithLiveConfiguration(live);
  }

  /**
   * A detached server built from {@link #SERVER_LIST}, naming itself pod 0, whose live Raft configuration is {@code live}.
   * Bolt has no declared port, so its endpoints derive from each peer's Raft host plus the Bolt port, as on a StatefulSet.
   */
  private static RaftHAServer serverWithLiveConfiguration(final Collection<RaftPeer> live) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
    config.setValue(GlobalConfiguration.BOLT_PORT, BOLT_PORT);
    return new RaftHAServer(TestServerHelper.unstartedServer("ArcadeDB_0"), config) {
      @Override
      public Collection<RaftPeer> getLivePeers() {
        return live;
      }

      @Override
      Collection<RaftPeer> getCommittedPeersOrNull() {
        return live;
      }
    };
  }

  private static RaftPeer declared(final RaftPeerId id) {
    return RaftPeer.newBuilder().setId(id).setAddress(id.toString().replace('_', ':')).build();
  }
}
