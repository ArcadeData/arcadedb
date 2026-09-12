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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7514, the two halves of the refusal that do not need a live cluster to pin.
 * <ul>
 *   <li>{@link PeerReachability} - what the pre-flight probe answers, and the cases in which it
 *       deliberately answers nothing at all;</li>
 *   <li>the message a membership change that ran out of budget is reported with, which is what the caller
 *       sees when the probe passed and the change still did not commit.</li>
 * </ul>
 * The entry points themselves are driven end to end by {@code Issue7514UnreachablePeerFailsFastIT}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7514UnreachablePeerRefusalTest {

  private static final long PROBE_TIMEOUT_MS = 2_000;

  /**
   * The probe is one-sided: something listening means it reports nothing, and the request proceeds exactly
   * as it did before the fix. Proving that is what keeps the probe from being able to turn a working add
   * into a refusal.
   */
  @Test
  void aListeningPortIsNotReportedUnreachable() throws Exception {
    try (final ServerSocket listening = new ServerSocket(0)) {
      assertThat(PeerReachability.unreachableReason("localhost:" + listening.getLocalPort(), PROBE_TIMEOUT_MS))
          .isNull();
    }
  }

  /** The reported case: the address parses, the host is up, and nothing is bound to the port. */
  @Test
  void aPortNothingIsBoundToIsReportedWithAReason() throws Exception {
    final String reason = PeerReachability.unreachableReason(addressNothingListensOn(), PROBE_TIMEOUT_MS);

    assertThat(reason).isNotNull().isNotBlank();
  }

  /**
   * "Not probed" must not read as "unreachable". {@code RaftPeerAddressResolver} always produces
   * {@code host:port}, so these are the defensive half of the contract - and getting them wrong would refuse
   * an add for an address the probe simply could not dial.
   */
  @Test
  void anAddressWithNothingToDialIsNotReportedUnreachable() {
    assertThat(PeerReachability.unreachableReason(null, PROBE_TIMEOUT_MS)).isNull();
    assertThat(PeerReachability.unreachableReason("localhost", PROBE_TIMEOUT_MS)).isNull();
    assertThat(PeerReachability.unreachableReason("localhost:", PROBE_TIMEOUT_MS)).isNull();
    assertThat(PeerReachability.unreachableReason("localhost:raft", PROBE_TIMEOUT_MS)).isNull();
    assertThat(PeerReachability.unreachableReason("localhost:0", PROBE_TIMEOUT_MS)).isNull();
    assertThat(PeerReachability.unreachableReason("localhost:70000", PROBE_TIMEOUT_MS)).isNull();
  }

  /**
   * The refusal has to be an {@link IllegalArgumentException}: that is the single type
   * {@code AbstractServerHttpHandler} maps to HTTP 400 and {@code ArcadeDbGrpcAdminService} maps to gRPC
   * {@code INVALID_ARGUMENT}, so it is what makes the gRPC {@code ConnectCluster} RPC answer the same way
   * the HTTP verb does without a second mapping arm. It also has to name the address and the escape hatch,
   * because a probe budget that is too short for the network is the one way this refusal can be wrong.
   */
  @Test
  void theRefusalMapsToInvalidArgumentAndHttp400() {
    final UnreachablePeerException refusal = new UnreachablePeerException("localhost_2436", "localhost:2436",
        "Connection refused", "localhost_2435", PROBE_TIMEOUT_MS);

    assertThat(refusal).isInstanceOf(IllegalArgumentException.class);
    assertThat(refusal.getMessage())
        .contains("localhost_2436")
        .contains("localhost:2436")
        .contains("Connection refused")
        .contains(GlobalConfiguration.HA_ADD_PEER_PROBE_TIMEOUT.getKey());
  }

  /**
   * Neither add-peer route is leader-routed, so the probe runs on whichever node the request landed on and
   * answers from THAT node's view of the network. Naming it is what separates "the peer is down" from "this
   * follower cannot see a peer the leader can", which are otherwise the same refusal.
   */
  @Test
  void theRefusalNamesTheNodeThatProbed() {
    assertThat(new UnreachablePeerException("localhost_2436", "localhost:2436", "Connection refused",
        "localhost_2435", PROBE_TIMEOUT_MS).getMessage()).contains("probed from node 'localhost_2435'");

    assertThat(new UnreachablePeerException("localhost_2436", "localhost:2436", "Connection refused", null,
        PROBE_TIMEOUT_MS).getMessage()).doesNotContain("probed from node");
  }

  /**
   * The two branches on which the probe must not refuse, both of which would otherwise be regressions:
   * re-adding a peer that is already a member has to stay the idempotent no-op {@code connect cluster}
   * documents (even for a member that is down), and the operator's escape hatch has to actually restore the
   * pre-probe behaviour rather than merely shortening the wait.
   */
  @Test
  void theProbeIsSkippedForAMemberAndWhenTheOperatorTurnedItOff() throws Exception {
    final String unreachable = addressNothingListensOn();

    assertThat(PeerReachability.addRefusalReason(true, PROBE_TIMEOUT_MS, unreachable))
        .as("already a member").isNull();
    assertThat(PeerReachability.addRefusalReason(false, 0, unreachable))
        .as("probe disabled with 0").isNull();
    assertThat(PeerReachability.addRefusalReason(false, -1, unreachable))
        .as("probe disabled with a negative budget").isNull();
    assertThat(PeerReachability.addRefusalReason(false, PROBE_TIMEOUT_MS, unreachable))
        .as("neither skip applies").isNotNull();
  }

  /**
   * When the probe passes and the configuration change still does not commit, the report leads with a
   * sentence and keeps the Ratis text as a labelled tail - instead of being nothing but the serialized
   * {@code SetConfigurationRequest} the issue quotes.
   * <p>
   * Driven with a zero budget so the give-up branch is reached at once rather than after 90 s.
   */
  @Test
  void aMembershipChangeThatRanOutOfBudgetIsReportedInASentence() throws Exception {
    final String ratisText = "Failed SetConfigurationRequest:client-48FB->localhost_2435@group-E6A6, cid=11, "
        + "seq=null, RW, null, ADD, servers:[localhost_2436|localhost:2436], listeners:[] for 60 attempts "
        + "with RetryLimited(maxAttempts=60, sleepTime=1s)";

    final RaftHAServer server = mock(RaftHAServer.class);
    final RaftClient client = mock(RaftClient.class);
    final AdminApi admin = mock(AdminApi.class);
    final RaftClientReply reply = mock(RaftClientReply.class);

    when(server.getClient()).thenReturn(client);
    when(client.admin()).thenReturn(admin);
    when(reply.isSuccess()).thenReturn(false);
    when(reply.getException()).thenReturn(new RaftException(ratisText));
    when(admin.setConfiguration(org.mockito.ArgumentMatchers.<SetConfigurationRequest.Arguments>any()))
        .thenReturn(reply);
    when(server.getLivePeers()).thenReturn(List.of(peer("A"), peer("B")));
    when(server.getHttpAddresses()).thenReturn(new HashMap<>());
    when(server.getRaftGroup()).thenReturn(RaftGroup.valueOf(RaftGroupId.randomId()));

    assertThatThrownBy(() -> new RaftClusterManager(server, 0).addPeer("D", "localhost:2447"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Failed to add peer D at localhost:2447")
        .hasMessageContaining("did not commit within 0 ms")
        .hasMessageContaining("same cluster name and cluster token")
        .hasMessageContaining("Raft reported: " + ratisText);
  }

  /** The removal path shares the helper and gets its own sentence, not the add's. */
  @Test
  void aRemovalThatRanOutOfBudgetIsReportedInItsOwnSentence() throws Exception {
    final RaftHAServer server = mock(RaftHAServer.class);
    final RaftClient client = mock(RaftClient.class);
    final AdminApi admin = mock(AdminApi.class);
    final RaftClientReply reply = mock(RaftClientReply.class);

    when(server.getClient()).thenReturn(client);
    when(client.admin()).thenReturn(admin);
    when(reply.isSuccess()).thenReturn(false);
    when(reply.getException()).thenReturn(new RaftException("no leader"));
    when(admin.setConfiguration(org.mockito.ArgumentMatchers.<SetConfigurationRequest.Arguments>any()))
        .thenReturn(reply);
    when(server.getLivePeers()).thenReturn(List.of(peer("A"), peer("B"), peer("C")));
    when(server.getHttpAddresses()).thenReturn(new HashMap<>());

    assertThatThrownBy(() -> new RaftClusterManager(server, 0).removePeer("C"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Failed to remove peer C")
        .hasMessageContaining("voting majority")
        .hasMessageContaining("Raft reported: no leader");
  }

  // -----------------------------------------------------------------------------------------------

  private static RaftPeer peer(final String id) {
    return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress("localhost:2444").build();
  }

  private static String addressNothingListensOn() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0)) {
      return "localhost:" + socket.getLocalPort();
    }
  }
}
