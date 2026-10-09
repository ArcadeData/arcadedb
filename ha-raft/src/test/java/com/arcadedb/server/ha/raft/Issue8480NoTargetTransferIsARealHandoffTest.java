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
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.CallLog;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
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
    raft.returns("getLivePeers", List.of(peer(SELF), peer(B), peer(C)));
    // Both peers answer (issue #8556): these tests are about the transfer loop, not reachability.
    raft.returns("handoffReachablePeers", Set.of(B.toString(), C.toString()));
  }

  /** The crux: the no-target form picks a peer and makes a targeted transfer, and never issues the bare step-down. */
  @Test
  void theNoTargetTransferHandsLeadershipToAnEligiblePeer() throws Exception {
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(eq(B), anyLong())).thenReturn(ok);

    assertThat(manager().transferLeadership(10_000)).isTrue();

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
    raft.leaderId(SELF);

    assertThat(manager().transferLeadership(10_000)).isTrue();

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
    raft.leader(true, false);
    raft.leaderId(null);

    // A short budget: the "did another leader settle?" check waits out the caller's remaining budget.
    assertThat(manager().transferLeadership(200)).isFalse();

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
    raft.leader(true, true, false);
    // isLeaderNow(B) right after the failure has not seen B yet; the settle check then does
    raft.on("getLeaderId", CallLog.inOrder(null, B));

    assertThat(manager().transferLeadership(10_000)).isTrue();

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
    raft.leaderId(null);

    assertThat(manager().transferLeadership(100)).isFalse();

    verify(admin).transferLeadership(isNull(), anyLong());
    verify(admin, never()).transferLeadership(eq(B), anyLong());
  }

  /** The election after a bare step-down re-elected this very node: nothing was handed off. */
  @Test
  void aBareStepDownThatReElectsTheSameNodeIsNotASuccess() throws Exception {
    everyPeerLags();
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(isNull(), anyLong())).thenReturn(ok);
    raft.on("getLeaderId", CallLog.inOrder(null, null, SELF));

    assertThat(manager().transferLeadership(100)).isFalse();
  }

  /** Control: the fallback still reports a real handoff once a different leader is in place. */
  @Test
  void aBareStepDownReportsSuccessOnceADifferentLeaderIsSeen() throws Exception {
    everyPeerLags();
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(isNull(), anyLong())).thenReturn(ok);
    raft.on("getLeaderId", CallLog.inOrder(null, null, C));

    assertThat(manager().transferLeadership(10_000)).isTrue();
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

  /**
   * stepDown() has its own candidate loop over the same targeted transfer, and meets the same race: a transfer that
   * won although its call failed must end the step-down, not move on to start a second election.
   */
  @Test
  void stepDownEndsOnATransferThatWonDespiteAFailedCall() {
    final int[] attempts = new int[1];
    final boolean[] leader = { true };
    final RaftHAServer server = new RaftHAServer(detachedServer(), threeNodeConfig()) {
      // Every configured peer answers (issue #8556): these tests are about the step-down loop, not reachability.
      @Override
      Set<String> handoffReachablePeers() {
        return getLivePeers().stream().map(peer -> peer.getId().toString()).collect(Collectors.toSet());
      }

      @Override
      public boolean isLeader() {
        return leader[0];
      }

      @Override
      public RaftPeerId getLeaderId() {
        return leader[0] ? getLocalPeerId() : B;
      }

      @Override
      public void transferLeadership(final String targetPeerId, final long timeoutMs) {
        attempts[0]++;
        leader[0] = false; // the target won...
        throw new ConfigurationException("Failed to transfer leadership to " + targetPeerId + ": client-1 is already CLOSED");
      }
    };

    assertThatCode(server::stepDown).as("the step-down happened: it must not be reported as a failure").doesNotThrowAnyException();
    assertThat(attempts[0]).as("no second transfer against the leader just elected").isEqualTo(1);
  }

  /**
   * The same race seen one candidate later: the first transfer's call failed while this node still read as leader,
   * and the win only shows as the second candidate's not-the-leader refusal. That is the step-down succeeding.
   */
  @Test
  void stepDownEndsWhenTheWinOnlyShowsAtTheNextCandidate() {
    final int[] attempts = new int[1];
    final boolean[] leader = { true };
    final RaftHAServer server = new RaftHAServer(detachedServer(), threeNodeConfig()) {
      // Every configured peer answers (issue #8556): these tests are about the step-down loop, not reachability.
      @Override
      Set<String> handoffReachablePeers() {
        return getLivePeers().stream().map(peer -> peer.getId().toString()).collect(Collectors.toSet());
      }

      @Override
      public boolean isLeader() {
        return leader[0];
      }

      @Override
      public RaftPeerId getLeaderId() {
        return leader[0] ? getLocalPeerId() : B;
      }

      @Override
      public void transferLeadership(final String targetPeerId, final long timeoutMs) {
        if (++attempts[0] == 1)
          throw new ConfigurationException("Failed to transfer leadership to " + targetPeerId + ": client-1 is already CLOSED");
        // ...by now the first target has won
        leader[0] = false;
        throw new NotTheLeaderRefusalException("Refusing to transfer leadership to " + targetPeerId, B);
      }
    };

    assertThatCode(server::stepDown).doesNotThrowAnyException();
    assertThat(attempts[0]).isEqualTo(2);
  }

  /**
   * The bare step-down's RPC and its confirmation share ONE budget: the confirmation gets what the RPC left of it,
   * not a fresh {@code timeoutMs} on top. Counted in leader-view polls rather than elapsed time: with the RPC
   * taking the whole budget, only the grace is left (a few polls at 50 ms), where a fresh budget would poll ~8
   * times. A stall can only reduce the count, so it cannot turn this red.
   */
  @Test
  void theBareStepDownConfirmationGetsOnlyWhatTheRpcLeftOfTheBudget() throws Exception {
    everyPeerLags();
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(isNull(), anyLong())).thenAnswer(invocation -> {
      Thread.sleep(400);
      return ok;
    });
    final AtomicInteger polls = new AtomicInteger();
    raft.on("getLeaderId", args -> {
      polls.incrementAndGet();
      return null;
    });

    assertThat(manager().transferLeadership(400)).isFalse();
    assertThat(polls.get()).as("leader-view polls after the RPC used the whole budget").isLessThanOrEqualTo(4);
  }

  /**
   * The bare step-down re-checks leadership itself: stepDown() reaches it after its candidates failed, and a no-target
   * request sent through a follower's client would be routed to the real leader and step IT down.
   */
  @Test
  void theBareStepDownIsNeverSentFromANodeThatIsNoLongerTheLeader() throws Exception {
    raft.leader(false);

    assertThat(manager().stepDownWithoutTarget(10_000)).isFalse();

    verify(admin, never()).transferLeadership(isNull(), anyLong());
  }

  /** And when leadership was lost with no other leader settling, stepDown() refuses instead of trying more peers. */
  @Test
  void stepDownRefusesWhenLeadershipWasLostWithoutAHandoff() {
    final int[] attempts = new int[1];
    final boolean[] leader = { true };
    final RaftHAServer server = new RaftHAServer(detachedServer(), threeNodeConfig()) {
      // Every configured peer answers (issue #8556): these tests are about the step-down loop, not reachability.
      @Override
      Set<String> handoffReachablePeers() {
        return getLivePeers().stream().map(peer -> peer.getId().toString()).collect(Collectors.toSet());
      }

      @Override
      public boolean isLeader() {
        return leader[0];
      }

      @Override
      boolean leadershipMovedAway() {
        return false;
      }

      @Override
      public void transferLeadership(final String targetPeerId, final long timeoutMs) {
        attempts[0]++;
        leader[0] = false;
        throw new ConfigurationException("Failed to transfer leadership to " + targetPeerId + ": timeout");
      }
    };

    assertThatThrownBy(server::stepDown).isInstanceOf(NotTheLeaderRefusalException.class);
    assertThat(attempts[0]).isEqualTo(1);
  }

  private RaftClusterManager manager() {
    final RaftClusterManager manager = new RaftClusterManager(raft);
    // The "no other leader appeared" outcomes wait out this grace; the real 3 s would only slow the class down.
    manager.leaderConfirmGraceMs = 100;
    return manager;
  }

  private static ArcadeDBServer detachedServer() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer("ArcadeDB_0");
    return server;
  }

  private static ContextConfiguration threeNodeConfig() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");
    return config;
  }

  private void everyPeerLags() {
    final ClusterMonitor monitor = new ClusterMonitor(100);
    for (final RaftPeerId peer : List.of(SELF, B, C))
      lagging(monitor, peer);
    raft.returns("getClusterMonitor", monitor);
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

  /**
   * Has the real {@code monitor} see {@code replica} acknowledge none of the leader's 1000 committed entries, which is
   * past the 100-entry warning threshold it was built with: {@link ClusterMonitor#isReplicaLagging} answers true for it.
   */
  private static void lagging(final ClusterMonitor monitor, final RaftPeerId replica) {
    monitor.updateLeaderCommitIndex(1_000L);
    monitor.updateReplicaMatchIndex(replica.toString(), 0L, 0L);
  }
}
