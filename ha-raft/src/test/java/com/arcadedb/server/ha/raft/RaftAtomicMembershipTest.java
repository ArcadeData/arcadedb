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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that the operator-facing {@code addPeer}/{@code removePeer} issue atomic Raft membership
 * changes instead of a read-modify-write full {@code setConfiguration} (last-write-wins), which would
 * silently drop one of two concurrent changes (issue #4795):
 * <ul>
 *   <li>add  -&gt; {@link SetConfigurationRequest.Mode#ADD} (single-peer delta)</li>
 *   <li>remove -&gt; {@link SetConfigurationRequest.Mode#COMPARE_AND_SET} (Ratis 3.2.2 has no REMOVE
 *       delta; CAS commits only if the leader's current config still matches the snapshot)</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftAtomicMembershipTest {

  private static RaftPeer peer(final String id) {
    return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress("localhost:2444").build();
  }

  @Test
  void addPeerUsesAtomicAddMode() throws Exception {
    final RaftHAServer server = mock(RaftHAServer.class);
    final RaftClient client = mock(RaftClient.class);
    final AdminApi admin = mock(AdminApi.class);
    final RaftClientReply reply = mock(RaftClientReply.class);

    final ArgumentCaptor<SetConfigurationRequest.Arguments> captor =
        ArgumentCaptor.forClass(SetConfigurationRequest.Arguments.class);

    when(server.getClient()).thenReturn(client);
    when(client.admin()).thenReturn(admin);
    when(reply.isSuccess()).thenReturn(true);
    when(admin.setConfiguration(captor.capture())).thenReturn(reply);
    when(server.getLivePeers()).thenReturn(List.of(peer("A"), peer("B"), peer("C")));
    when(server.getHttpAddresses()).thenReturn(new HashMap<>());
    when(server.getRaftGroup()).thenReturn(RaftGroup.valueOf(RaftGroupId.randomId()));

    new RaftClusterManager(server).addPeer("D", "localhost:2447");

    final SetConfigurationRequest.Arguments args = captor.getValue();
    assertThat(args.getMode()).isEqualTo(SetConfigurationRequest.Mode.ADD);
    // A delta: only the new peer is in the request, NOT the full rebuilt list.
    assertThat(args.getServersInNewConf()).hasSize(1);
    assertThat(args.getServersInNewConf().getFirst().getId().toString()).isEqualTo("D");
  }

  @Test
  void removePeerUsesCompareAndSetMode() throws Exception {
    final RaftHAServer server = mock(RaftHAServer.class);
    final RaftClient client = mock(RaftClient.class);
    final AdminApi admin = mock(AdminApi.class);
    final RaftClientReply reply = mock(RaftClientReply.class);

    final ArgumentCaptor<SetConfigurationRequest.Arguments> captor =
        ArgumentCaptor.forClass(SetConfigurationRequest.Arguments.class);

    when(server.getClient()).thenReturn(client);
    when(client.admin()).thenReturn(admin);
    when(reply.isSuccess()).thenReturn(true);
    when(admin.setConfiguration(captor.capture())).thenReturn(reply);
    when(server.getLivePeers()).thenReturn(List.of(peer("A"), peer("B"), peer("C")));
    when(server.getHttpAddresses()).thenReturn(new HashMap<>());

    new RaftClusterManager(server).removePeer("C");

    final SetConfigurationRequest.Arguments args = captor.getValue();
    assertThat(args.getMode()).isEqualTo(SetConfigurationRequest.Mode.COMPARE_AND_SET);
    // The CAS precondition is the full current config; the new config is current minus the removed peer.
    assertThat(args.getServersInCurrentConf()).hasSize(3);
    assertThat(args.getServersInNewConf()).hasSize(2);
    assertThat(args.getServersInNewConf().stream().map(p -> p.getId().toString()).toList())
        .containsExactlyInAnyOrder("A", "B");
  }
}
