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
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8487: the TARGETED {@code transferLeadership(peerId, timeoutMs)} judged a failed call by
 * sampling the leader view once, the instant the failure was caught. Under concurrent writes the call fails with
 * "client-... is already CLOSED" - every leader change makes {@code refreshRaftClient()} close the client the RPC was
 * sent through - so what the method reported depended on which node the view happened to name at that instant.
 * <p>
 * A failed call is now settled rather than sampled: the method waits, within the caller's budget, for a concrete
 * leader, and reports the handoff when that leader is the target. When the leader is this node again and the failure
 * was only the closed client, the transfer is re-sent through the fresh client (Ratis joins a pending transfer to the
 * same target instead of starting a second one). Every other outcome fails and names what actually happened.
 */
class Issue8487TargetedTransferSettlesTest {

  private static final RaftPeerId SELF = RaftPeerId.valueOf("peer-a_2434");
  private static final RaftPeerId B    = RaftPeerId.valueOf("peer-b_2435");
  private static final RaftPeerId C    = RaftPeerId.valueOf("peer-c_2436");

  private FakeRaftHAServer raft;
  private AdminApi     admin;

  @BeforeEach
  void setUp() {
    raft = FakeRaftHAServer.detached();
    admin = mock(AdminApi.class);
    final RaftClient client = mock(RaftClient.class);
    when(client.admin()).thenReturn(admin);
    raft.returns("getClient", client);
    raft.localPeerId(SELF);
    raft.leader(true);
  }

  /** The crux: the target is seen as leader only some polls after the failure, and that is still the handoff. */
  @Test
  void aClosedClientFailureIsSettledByWaitingForTheTarget() throws Exception {
    when(admin.transferLeadership(eq(B), anyLong())).thenThrow(closed());
    raft.leader(true, false);
    // A leaderless window first - the moment the old code sampled - then the target.
    raft.on("getLeaderId", CallLog.inOrder(null, null, null, B));

    assertThatCode(() -> manager().transferLeadership(B.toString(), 10_000)).doesNotThrowAnyException();
    verify(admin, times(1)).transferLeadership(eq(B), anyLong());
  }

  /** Leadership settled on a peer other than the target: a failure, naming who holds leadership, not the closed client. */
  @Test
  void aTransferWhereAnotherPeerWonNamesTheActualLeader() throws Exception {
    when(admin.transferLeadership(eq(B), anyLong())).thenThrow(closed());
    raft.leader(true, false);
    raft.on("getLeaderId", CallLog.inOrder(null, C));

    assertThatThrownBy(() -> manager().transferLeadership(B.toString(), 10_000))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(C.toString())
        .hasMessageContaining("instead of " + B);
    verify(admin, times(1)).transferLeadership(eq(B), anyLong());
  }

  /** No leader settles within the budget: a failure that says so. */
  @Test
  void noLeaderSettlingWithinTheBudgetFails() throws Exception {
    when(admin.transferLeadership(eq(B), anyLong())).thenThrow(closed());
    raft.leader(true, false);
    raft.leaderId(null);

    assertThatThrownBy(() -> manager().transferLeadership(B.toString(), 200))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("no leader");
  }

  /**
   * The client was closed while this node is still the leader - a refresh that raced the call, before or while the
   * transfer ran. The transfer is re-sent through the FRESH client, not reported as failed.
   */
  @Test
  void aClosedClientWhileStillLeaderIsRetriedThroughTheFreshClient() throws Exception {
    final AdminApi freshAdmin = mock(AdminApi.class);
    final RaftClient freshClient = mock(RaftClient.class);
    when(freshClient.admin()).thenReturn(freshAdmin);
    final RaftClient staleClient = raft.getClient();
    raft.on("getClient", CallLog.inOrder(staleClient, freshClient));
    when(admin.transferLeadership(eq(B), anyLong())).thenThrow(closed());
    final RaftClientReply ok = reply(true);
    when(freshAdmin.transferLeadership(eq(B), anyLong())).thenReturn(ok);
    raft.leaderId(SELF);

    assertThatCode(() -> manager().transferLeadership(B.toString(), 10_000)).doesNotThrowAnyException();
    verify(admin, times(1)).transferLeadership(eq(B), anyLong());
    verify(freshAdmin, times(1)).transferLeadership(eq(B), anyLong());
  }

  /**
   * The reproduced shape: the target lost its election, the cluster sat leaderless, and THIS node was re-elected while
   * the budget still had time left. Its client was closed by that re-election; the transfer is tried again.
   */
  @Test
  void aNodeReElectedWithinTheBudgetTriesTheTransferAgain() throws Exception {
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(eq(B), anyLong())).thenThrow(closed()).thenReturn(ok);
    // entry guard, then leaderless while the target's election fails, then this node again
    raft.leader(true, false, false, true);
    raft.on("getLeaderId", CallLog.inOrder(null, null, SELF));

    assertThatCode(() -> manager().transferLeadership(B.toString(), 10_000)).doesNotThrowAnyException();
    verify(admin, times(2)).transferLeadership(eq(B), anyLong());
  }

  /**
   * A refusal Ratis gave while this node is still the leader is a genuine failure: reported at once, with Ratis'
   * reason, and never retried or waited out.
   */
  @Test
  void aRefusalWhileStillLeaderFailsAtOnce() throws Exception {
    final RaftClientReply refused = reply(false);
    when(refused.getException()).thenReturn(new TransferLeadershipException("peer-b_2435 is not in the configuration"));
    when(admin.transferLeadership(eq(B), anyLong())).thenReturn(refused);
    final AtomicInteger polls = new AtomicInteger();
    raft.on("getLeaderId", args -> {
      polls.incrementAndGet();
      return SELF;
    });

    assertThatThrownBy(() -> manager().transferLeadership(B.toString(), 10_000))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("not in the configuration");
    verify(admin, times(1)).transferLeadership(eq(B), anyLong());
    assertThat(polls.get()).as("a refusal while still leader is not waited out").isEqualTo(1);
  }

  /** A client that keeps being closed under the call is not retried forever. */
  @Test
  void closedClientRetriesAreBounded() throws Exception {
    when(admin.transferLeadership(eq(B), anyLong())).thenThrow(closed());
    raft.leaderId(SELF);

    assertThatThrownBy(() -> manager().transferLeadership(B.toString(), 10_000))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("is still the leader");
    verify(admin, times(RaftClusterManager.MAX_TRANSFER_ATTEMPTS)).transferLeadership(eq(B), anyLong());
  }

  /** Control: the plain success path still issues exactly one call and does not poll the leader view. */
  @Test
  void aSuccessfulReplyReturnsAtOnce() throws Exception {
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(eq(B), anyLong())).thenReturn(ok);

    assertThatCode(() -> manager().transferLeadership(B.toString(), 10_000)).doesNotThrowAnyException();
    verify(admin, times(1)).transferLeadership(eq(B), anyLong());
    assertThat(raft.calls("getLeaderId")).hasSize(0);
  }

  private RaftClusterManager manager() {
    final RaftClusterManager manager = new RaftClusterManager(raft);
    manager.leaderConfirmGraceMs = 100;
    return manager;
  }

  private static AlreadyClosedException closed() {
    return new AlreadyClosedException("client-4C16FE41B3C2 is already CLOSED");
  }

  private static RaftClientReply reply(final boolean success) {
    final RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(success);
    return reply;
  }
}
