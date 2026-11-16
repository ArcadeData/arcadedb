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

import com.arcadedb.exception.ConfigurationException;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #12127: a targeted leadership transfer returned about 3 s after leadership had moved. The
 * RPC went through the shared client, and the old leader's {@code notifyLeaderChanged -> refreshRaftClient()} closes
 * that client while the RPC is still in flight; Ratis's graceful channel shutdown waits up to 3 s for it, and the
 * transfer's reply is only produced after {@code notifyLeaderChanged} returns. The transfer now goes through a client
 * of its own, which no leader change closes, and is closed once the transfer is over.
 * <p>
 * The re-election case of issue #8487 used to reach this code as a closed client. Through its own client it reaches it
 * as Ratis's verdict instead - {@code DIFFERENT_LEADER}, this node being the leader again - and is retried the same way;
 * a refusal Ratis gave while this node stayed the leader is still reported at once.
 */
class Issue12127TargetedTransferOwnClientTest {

  private static final RaftPeerId SELF = RaftPeerId.valueOf("peer-a_2434");
  private static final RaftPeerId B    = RaftPeerId.valueOf("peer-b_2435");

  private FakeRaftHAServer raft;
  private AdminApi         sharedAdmin;
  private RaftClient       ownClient;
  private AdminApi         ownAdmin;

  @BeforeEach
  void setUp() {
    raft = FakeRaftHAServer.detached();
    sharedAdmin = mock(AdminApi.class);
    final RaftClient shared = mock(RaftClient.class);
    when(shared.admin()).thenReturn(sharedAdmin);
    raft.returns("getClient", shared);

    ownAdmin = mock(AdminApi.class);
    ownClient = mock(RaftClient.class);
    when(ownClient.admin()).thenReturn(ownAdmin);
    raft.returns("newMembershipClient", ownClient);

    raft.localPeerId(SELF);
    raft.leader(true);
  }

  /** The crux: the RPC never goes through the client a leader change closes, and the transfer's own is closed after. */
  @Test
  void theTransferGoesThroughAClientOfItsOwn() throws Exception {
    final RaftClientReply ok = reply(true);
    when(ownAdmin.transferLeadership(eq(B), anyLong())).thenReturn(ok);

    assertThatCode(() -> manager().transferLeadership(B.toString(), 10_000)).doesNotThrowAnyException();
    verify(ownAdmin, times(1)).transferLeadership(eq(B), anyLong());
    verify(sharedAdmin, never()).transferLeadership(any(), anyLong());
    verify(ownClient, times(1)).close();
  }

  /** The target lost its election and this node was re-elected: Ratis says DIFFERENT_LEADER, and the transfer is re-sent. */
  @Test
  void aTransferLostToThisNodesReElectionIsRetried() throws Exception {
    final RaftClientReply lost = reply(false);
    when(lost.getException()).thenReturn(new TransferLeadershipException(
        SELF + "@group-1: Failed to transfer leadership to " + B + " (the current leader is " + SELF + "): DIFFERENT_LEADER"));
    final RaftClientReply ok = reply(true);
    when(ownAdmin.transferLeadership(eq(B), anyLong())).thenReturn(lost).thenReturn(ok);
    // this node is the leader again when the failure is settled
    raft.leaderId(SELF);

    assertThatCode(() -> manager().transferLeadership(B.toString(), 10_000)).doesNotThrowAnyException();
    verify(ownAdmin, times(2)).transferLeadership(eq(B), anyLong());
    verify(ownClient, times(1)).close();
  }

  /** A refusal Ratis gave while this node stayed the leader is not a lost election: reported at once, not re-sent. */
  @Test
  void aRefusalNamingThisNodeAsLeaderFailsAtOnce() throws Exception {
    final RaftClientReply refused = reply(false);
    when(refused.getException()).thenReturn(new TransferLeadershipException(
        SELF + "@group-1: Failed to transfer leadership to " + B + " (the current leader is " + SELF + "): NULL_FOLLOWER"));
    when(ownAdmin.transferLeadership(eq(B), anyLong())).thenReturn(refused);
    raft.leaderId(SELF);

    assertThatThrownBy(() -> manager().transferLeadership(B.toString(), 10_000))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("NULL_FOLLOWER");
    verify(ownAdmin, times(1)).transferLeadership(eq(B), anyLong());
    verify(ownClient, times(1)).close();
  }

  /** Re-elections that keep beating the target are not retried forever. */
  @Test
  void lostElectionRetriesAreBounded() throws Exception {
    final RaftClientReply lost = reply(false);
    when(lost.getException()).thenReturn(new TransferLeadershipException(
        SELF + "@group-1: Failed to transfer leadership to " + B + " (the current leader is " + SELF + "): DIFFERENT_LEADER"));
    when(ownAdmin.transferLeadership(eq(B), anyLong())).thenReturn(lost);
    raft.leaderId(SELF);

    assertThatThrownBy(() -> manager().transferLeadership(B.toString(), 10_000))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("is still the leader");
    verify(ownAdmin, times(RaftClusterManager.MAX_TRANSFER_ATTEMPTS)).transferLeadership(eq(B), anyLong());
  }

  /** A client that cannot be built is reported as the ConfigurationException every other failure of a transfer is. */
  @Test
  void aClientThatCannotBeBuiltIsAConfigurationFailure() {
    raft.fails("newMembershipClient", new IllegalStateException("no transport parameters"));

    assertThatThrownBy(() -> manager().transferLeadership(B.toString(), 10_000))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("no transport parameters");
  }

  private RaftClusterManager manager() {
    final RaftClusterManager manager = new RaftClusterManager(raft);
    manager.leaderConfirmGraceMs = 100;
    return manager;
  }

  private static RaftClientReply reply(final boolean success) {
    final RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(success);
    return reply;
  }
}
