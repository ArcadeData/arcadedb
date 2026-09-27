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

import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8480: {@code transferLeadership(timeoutMs)}, the form with no target, only asked Ratis
 * to step the leader down. Ratis does that at the same term without telling anyone and replies success at once, so
 * the method returned {@code true} within milliseconds while the cluster had no leader, the followers kept
 * forwarding writes to the ex-leader for a whole election timeout, and the election that finally ran usually
 * re-elected the same node.
 * <p>
 * The no-target form now hands leadership to the best eligible peer with the targeted transfer, and keeps the bare
 * step-down only as a fallback whose success is confirmed by seeing a different leader.
 */
class Issue8480NoTargetTransferIsARealHandoffTest {

  private static final RaftPeerId SELF = RaftPeerId.valueOf("peer-a_2434");
  private static final RaftPeerId B    = RaftPeerId.valueOf("peer-b_2435");
  private static final RaftPeerId C    = RaftPeerId.valueOf("peer-c_2436");

  private RaftHAServer raft;
  private AdminApi     admin;

  @BeforeEach
  void setUp() {
    raft = mock(RaftHAServer.class);
    admin = mock(AdminApi.class);
    final RaftClient client = mock(RaftClient.class);
    when(client.admin()).thenReturn(admin);
    when(raft.getClient()).thenReturn(client);
    when(raft.getLocalPeerId()).thenReturn(SELF);
    when(raft.isLeader()).thenReturn(true);
    when(raft.getLivePeers()).thenReturn(List.of(peer(SELF), peer(B), peer(C)));
  }

  /** The crux: the no-target form picks a peer and makes a targeted transfer, and never issues the bare step-down. */
  @Test
  void theNoTargetTransferHandsLeadershipToAnEligiblePeer() throws Exception {
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(eq(B), anyLong())).thenReturn(ok);

    assertThat(new RaftClusterManager(raft).transferLeadership(10_000)).isTrue();

    verify(admin).transferLeadership(eq(B), anyLong());
    verify(admin, never()).transferLeadership(isNull(), anyLong());
  }

  /** A candidate that cannot take leadership is not the end of the transfer: the next one in the ranking is tried. */
  @Test
  void aFailedTargetedTransferFallsThroughToTheNextCandidate() throws Exception {
    final RaftClientReply failed = reply(false);
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(eq(B), anyLong())).thenReturn(failed);
    when(admin.transferLeadership(eq(C), anyLong())).thenReturn(ok);
    when(raft.getLeaderId()).thenReturn(SELF);

    assertThat(new RaftClusterManager(raft).transferLeadership(10_000)).isTrue();

    verify(admin).transferLeadership(eq(B), anyLong());
    verify(admin).transferLeadership(eq(C), anyLong());
    verify(admin, never()).transferLeadership(isNull(), anyLong());
  }

  /**
   * Leadership moving away while the candidates are tried, with no other leader settling: nothing was handed off,
   * and every remaining candidate would refuse identically. Report false without trying them or stepping down.
   */
  @Test
  void leadershipLostMidTransferWithNoNewLeaderReportsFalse() throws Exception {
    // true for the entry guard, false from the first targeted transfer on
    when(raft.isLeader()).thenReturn(true, false);
    when(raft.getLeaderId()).thenReturn(null);

    assertThat(new RaftClusterManager(raft).transferLeadership(10_000)).isFalse();

    verify(admin, never()).transferLeadership(eq(B), anyLong());
    verify(admin, never()).transferLeadership(eq(C), anyLong());
    verify(admin, never()).transferLeadership(isNull(), anyLong());
  }

  /**
   * A targeted transfer that WON although its call failed - the client closed under the RPC when the leader changed
   * (#8487), before this node saw the target as leader. It must be reported as the handoff it was, not followed by a
   * transfer to the next candidate, which would only be refused.
   */
  @Test
  void aTransferThatWonDespiteAFailedCallIsReportedAsAHandoff() throws Exception {
    when(admin.transferLeadership(eq(B), anyLong())).thenThrow(new IOException("client-1 is already CLOSED"));
    // entry guard, the targeted overload's own guard, then the check after the failure
    when(raft.isLeader()).thenReturn(true, true, false);
    // isLeaderNow(B) right after the failure has not seen B yet; the settle check then does
    when(raft.getLeaderId()).thenReturn(null, B);

    assertThat(new RaftClusterManager(raft).transferLeadership(10_000)).isTrue();

    verify(admin, never()).transferLeadership(eq(C), anyLong());
    verify(admin, never()).transferLeadership(isNull(), anyLong());
  }

  /**
   * No eligible peer: the bare step-down is the only move left. Its success reply is what the old code returned
   * {@code true} on, while the cluster sat leaderless. It is not a handoff until a different leader is seen.
   */
  @Test
  void aBareStepDownIsNotASuccessWhileNoOtherLeaderIsSeen() throws Exception {
    everyPeerLags();
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(isNull(), anyLong())).thenReturn(ok);
    when(raft.getLeaderId()).thenReturn(null);

    assertThat(new RaftClusterManager(raft).transferLeadership(100)).isFalse();

    verify(admin).transferLeadership(isNull(), anyLong());
    verify(admin, never()).transferLeadership(eq(B), anyLong());
  }

  /** The election after a bare step-down re-elected this very node: nothing was handed off. */
  @Test
  void aBareStepDownThatReElectsTheSameNodeIsNotASuccess() throws Exception {
    everyPeerLags();
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(isNull(), anyLong())).thenReturn(ok);
    when(raft.getLeaderId()).thenReturn(null, null, SELF);

    assertThat(new RaftClusterManager(raft).transferLeadership(100)).isFalse();
  }

  /** Control: the fallback still reports a real handoff once a different leader is in place. */
  @Test
  void aBareStepDownReportsSuccessOnceADifferentLeaderIsSeen() throws Exception {
    everyPeerLags();
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(isNull(), anyLong())).thenReturn(ok);
    when(raft.getLeaderId()).thenReturn(null, null, C);

    assertThat(new RaftClusterManager(raft).transferLeadership(10_000)).isTrue();
  }

  /**
   * The forwarding half: an ex-leader that refuses a forwarded write without naming a leader is the refusal the
   * follower must hold until its own view moves. One that names a leader, or any other failure, is not.
   */
  @Test
  void onlyAnUnnamedRefusalFromTheIntendedLeaderIsHeldBack() {
    assertThat(RaftReplicatedDatabase.refusedByTheLeaderItNamedNoOther(
        new ServerIsNotTheLeaderException("leadership moved", null), SELF.toString())).isTrue();
    assertThat(RaftReplicatedDatabase.refusedByTheLeaderItNamedNoOther(
        new ServerIsNotTheLeaderException("leadership moved", ""), SELF.toString())).isTrue();
    assertThat(RaftReplicatedDatabase.refusedByTheLeaderItNamedNoOther(
        new ServerIsNotTheLeaderException("not the leader", "host:2481"), SELF.toString())).isFalse();
    assertThat(RaftReplicatedDatabase.refusedByTheLeaderItNamedNoOther(
        new ServerIsNotTheLeaderException("leadership moved", null), null)).isFalse();
    assertThat(RaftReplicatedDatabase.refusedByTheLeaderItNamedNoOther(
        new TransactionException("boom"), SELF.toString())).isFalse();
  }

  /** The wait ends as soon as the view stops naming the refusing node: on an election in progress (null) too. */
  @Test
  void theForwardWaitEndsWhenTheViewMovesAwayFromTheRefusingNode() {
    final AtomicInteger probes = new AtomicInteger();
    final boolean moved = RaftReplicatedDatabase.awaitLeaderViewMovedFrom(
        () -> probes.incrementAndGet() < 3 ? SELF : null, SELF.toString(), 10_000, 1);

    assertThat(moved).isTrue();
    assertThat(probes.get()).isEqualTo(3);
  }

  /** And it is bounded: a view that never moves gives up and lets the refusal through. */
  @Test
  void theForwardWaitIsBounded() {
    assertThat(RaftReplicatedDatabase.awaitLeaderViewMovedFrom(() -> SELF, SELF.toString(), 50, 5)).isFalse();
  }

  /** A view that already names another node returns at once. */
  @Test
  void theForwardWaitReturnsAtOnceWhenTheViewAlreadyMoved() {
    final AtomicInteger probes = new AtomicInteger();
    assertThat(RaftReplicatedDatabase.awaitLeaderViewMovedFrom(() -> {
      probes.incrementAndGet();
      return B;
    }, SELF.toString(), 10_000, 1)).isTrue();
    assertThat(probes.get()).isEqualTo(1);
  }

  private void everyPeerLags() {
    final ClusterMonitor monitor = mock(ClusterMonitor.class);
    when(monitor.isReplicaLagging(anyString())).thenReturn(true);
    when(raft.getClusterMonitor()).thenReturn(monitor);
  }

  private static RaftClientReply reply(final boolean success) {
    final RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(success);
    return reply;
  }

  private static RaftPeer peer(final RaftPeerId id) {
    return RaftPeer.newBuilder().setId(id).setAddress("localhost:" + id.toString().substring(id.toString().indexOf('_') + 1))
        .build();
  }
}
