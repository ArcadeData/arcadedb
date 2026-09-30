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
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.ReconfigurationInProgressException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7802: two concurrent add-peer requests naming one address under two ids used to both commit, because the
 * uniqueness check read the configuration before a {@code Mode.ADD} that carried no predicate. The add is now a
 * compare-and-set built from the configuration the check read, so the loser is rebuilt against the winner and refused.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7802ConcurrentAddSameAddressTest {

  private static RaftPeer peer(final String id, final String address) {
    return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress(address).build();
  }

  /**
   * The race: this request read configuration {A,B,C}, and before its change reached the leader another request
   * committed {A,B,C,X} on the same address. The leader refuses the compare-and-set, the retry re-reads, and the
   * address check now refuses the request instead of committing a second entry for one process.
   */
  @Test
  void theLoserOfTheRaceIsRefusedAgainstTheConfigurationHoldingTheWinner() throws Exception {
    final List<RaftPeer> live = new ArrayList<>(List.of(peer("A", "h1:2434"), peer("B", "h2:2434"), peer("C", "h3:2434")));
    final RaftHAServer server = mock(RaftHAServer.class);
    final RaftClient client = mock(RaftClient.class);
    final AdminApi admin = mock(AdminApi.class);
    final AtomicInteger attempts = new AtomicInteger();

    when(server.getClient()).thenReturn(client);
    when(client.admin()).thenReturn(admin);
    when(server.getHttpAddresses()).thenReturn(new HashMap<>());
    when(server.getRaftGroup()).thenReturn(RaftGroup.valueOf(RaftGroupId.randomId()));
    when(server.getLivePeers()).thenAnswer(invocation -> List.copyOf(live));
    when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(invocation -> {
      attempts.incrementAndGet();
      // The other admin request commits first: the leader's configuration moves under this request's precondition.
      live.add(peer("X", "h4:2434"));
      final RaftClientReply reply = mock(RaftClientReply.class);
      when(reply.isSuccess()).thenReturn(false);
      when(reply.getException()).thenReturn(new ReconfigurationInProgressException("configuration changed"));
      return reply;
    });

    assertThatThrownBy(() -> new RaftClusterManager(server).addPeer("Y", "h4:2434"))
        .isInstanceOf(DuplicatePeerAddressException.class)
        .hasMessageContaining("'X'");
    assertThat(attempts.get()).as("the refused request must not be sent a second time").isEqualTo(1);
    assertThat(live).extracting(p -> p.getId().toString()).containsExactly("A", "B", "C", "X");
  }

  /** Concurrent adds of DIFFERENT peers must still both land: the loser is rebuilt on top of the winner. */
  @Test
  void aRaceBetweenTwoDifferentPeersStillCommitsBoth() throws Exception {
    final List<RaftPeer> live = new ArrayList<>(List.of(peer("A", "h1:2434"), peer("B", "h2:2434"), peer("C", "h3:2434")));
    final RaftHAServer server = mock(RaftHAServer.class);
    final RaftClient client = mock(RaftClient.class);
    final AdminApi admin = mock(AdminApi.class);
    final List<SetConfigurationRequest.Arguments> sent = new ArrayList<>();

    when(server.getClient()).thenReturn(client);
    when(client.admin()).thenReturn(admin);
    when(server.getHttpAddresses()).thenReturn(new HashMap<>());
    when(server.getRaftGroup()).thenReturn(RaftGroup.valueOf(RaftGroupId.randomId()));
    when(server.getLivePeers()).thenAnswer(invocation -> List.copyOf(live));
    when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(invocation -> {
      final SetConfigurationRequest.Arguments args = invocation.getArgument(0);
      sent.add(args);
      final RaftClientReply reply = mock(RaftClientReply.class);
      if (sent.size() == 1) {
        live.add(peer("X", "h4:2434")); // the other request wins the race
        when(reply.isSuccess()).thenReturn(false);
        when(reply.getException()).thenReturn(new ReconfigurationInProgressException("configuration changed"));
      } else {
        live.clear();
        live.addAll(args.getServersInNewConf());
        when(reply.isSuccess()).thenReturn(true);
      }
      return reply;
    });

    new RaftClusterManager(server).addPeer("Y", "h5:2434");

    assertThat(sent).hasSize(2);
    assertThat(sent.get(1).getServersInCurrentConf()).extracting(p -> p.getId().toString())
        .as("the retry is rebuilt on top of the winner").containsExactly("A", "B", "C", "X");
    assertThat(live).extracting(p -> p.getId().toString()).containsExactly("A", "B", "C", "X", "Y");
  }

  @Test
  void aConfigurationHoldingASharedAddressIsReported() {
    final List<List<String>> shared = ClusterMembership.findSharedAddresses(
        List.of(peer("A", "127.0.0.1:2434"), peer("B", "h2:2434"), peer("A2", "localhost:2434"), peer("A3", "127.0.0.1:2434"),
            peer("B2", "H2:2434"), peer("C", "h3:2434")));

    assertThat(shared).containsExactly(List.of("A", "A2", "A3"), List.of("B", "B2"));
  }

  @Test
  void aHealthyConfigurationRaisesNoAlert() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addSharedPeerAddressAlert(ClusterMembership.findSharedAddresses(
        List.of(peer("A", "h1:2434"), peer("B", "h2:2434"))), alerts);
    assertThat(alerts.length()).isZero();
  }

  @Test
  void theAlertNamesEveryIdOfTheSharedAddressAndTheRemedy() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addSharedPeerAddressAlert(ClusterMembership.findSharedAddresses(
        List.of(peer("A", "h1:2434"), peer("B", "h1:2434"))), alerts);

    assertThat(alerts.length()).isEqualTo(1);
    assertThat(alerts.getJSONObject(0).getString("id")).isEqualTo("peers-share-address");
    assertThat(alerts.getJSONObject(0).getString("message")).contains("[[A, B]]");
    assertThat(alerts.getJSONObject(0).getString("recommendation")).contains("DELETE /api/v1/cluster/peer/{id}");
  }
}
