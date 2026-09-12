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

import com.arcadedb.server.ha.raft.RaftPeerAddressResolver.JoinTarget;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7401: the leader-election priority an address declares has to reach the peer that is actually
 * added, not just the parse.
 * <p>
 * This is the gap a review of the first implementation found, and it is worth its own class because of
 * how invisible it was. {@code Issue7401JoinTargetTest.theObjectFormIsAcceptedToo} asserts
 * {@code target.peer().getPriority()} and passed the whole time; the value was then dropped one layer
 * down, where {@code addPeer} rebuilt a fresh {@link RaftPeer} from an id and an address and left
 * everything else at {@link RaftPeer.Builder}'s defaults. Every other assertion about the join - the
 * id, the address, the {@code Mode.ADD}, the idempotence - held perfectly while the cluster got a
 * configuration the operator had not asked for.
 * <p>
 * The consequence is not cosmetic. {@code RaftHAServer.selectStepDownTargets} reads the live
 * {@code getPriority()} of each peer and, once any peer has a positive priority, skips the priority-0
 * ones as non-electable witnesses - so a priority quietly reset to 0 changes which nodes can take
 * leadership.
 * <p>
 * The assertion therefore reads the {@link SetConfigurationRequest.Arguments} that Ratis is asked to
 * commit, which is the last point the value can still be lost, and it starts from
 * {@code parseJoinTarget} rather than from a hand-built peer so the parse and the membership change
 * are pinned as one path.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7401JoinPriorityTest {

  private static final int DEFAULT_RAFT_PORT = 2434;

  private static RaftPeer peer(final String id) {
    return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress("localhost:2444").build();
  }

  /**
   * Captures the configuration change {@code addPeer} asks Ratis to commit for the peer parsed out of
   * {@code entry}.
   */
  private static RaftPeer addedPeerFor(final String entry) throws Exception {
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

    final JoinTarget target = RaftPeerAddressResolver.parseJoinTarget(entry, DEFAULT_RAFT_PORT, "");
    new RaftClusterManager(server).addPeer(target.peer(), target.name());

    assertThat(captor.getValue().getServersInNewConf()).hasSize(1);
    return captor.getValue().getServersInNewConf().getFirst();
  }

  @Test
  void theObjectFormPriorityReachesTheConfigurationChange() throws Exception {
    final RaftPeer added = addedPeerFor("db2:{raft:2435,http:2481,priority:7}");

    assertThat(added.getPriority()).isEqualTo(7);
    assertThat(added.getId().toString()).isEqualTo("db2_2435");
    assertThat(added.getAddress()).isEqualTo("db2:2435");
  }

  /** The four-field positional form declares a priority too, and must not be the path that loses it. */
  @Test
  void thePositionalFormPriorityReachesTheConfigurationChange() throws Exception {
    assertThat(addedPeerFor("db2:2435:2481:5").getPriority()).isEqualTo(5);
  }

  /**
   * A named peer keeps both halves: the {@code name@} prefix is stripped before the address is parsed,
   * and that must not cost the priority that follows it.
   */
  @Test
  void aNamedEntryKeepsItsPriority() throws Exception {
    assertThat(addedPeerFor("frankfurt@db2:{raft:2435,priority:9}").getPriority()).isEqualTo(9);
  }

  /**
   * An entry that declares no priority gets 0, which is what {@code RaftPeer.Builder} defaults to and
   * what every caller of the three-argument {@code addPeer} - {@code POST /api/v1/cluster/peer}, whose
   * payload has no priority field - has always produced. Without this the test above would also pass
   * against an implementation that invented a priority of its own.
   */
  @Test
  void anEntryWithoutAPriorityIsAddedWithTheDefault() throws Exception {
    assertThat(addedPeerFor("db2:2435").getPriority()).isZero();
  }
}
