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
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The leadership transfer / step-down loop of {@link RaftClusterManager}: no-target hand-off, targeted transfers that settle, and concurrent hand-offs.
 */
class RaftClusterManagerLeadershipTransferTest {
  // ---- #8480 ----
  // Regression test for issue #8480: {@code transferLeadership(timeoutMs)}, the form with no target, only asked Ratis
  // to step the leader down. Ratis does that at the same term without telling anyone and replies success at once, so
  // the method returned {@code true} within milliseconds while the cluster had no leader, the followers kept
  // forwarding writes to the ex-leader for a whole election timeout, and the election that finally ran usually
  // re-elected the same node.
  //
  // The no-target form now hands leadership to the best eligible peer with the targeted transfer, and keeps the bare
  // step-down only as a fallback whose success is confirmed by seeing a different leader.

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

  // Regression for #8480: the no target transfer hands leadership to an eligible peer
  /** The crux: the no-target form picks a peer and makes a targeted transfer, and never issues the bare step-down. */
  @Test
  void theNoTargetTransferHandsLeadershipToAnEligiblePeer() throws Exception {
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(eq(B), anyLong())).thenReturn(ok);

    assertThat(manager().transferLeadership(10_000)).isTrue();

    verify(admin).transferLeadership(eq(B), anyLong());
    verify(admin, never()).transferLeadership(isNull(), anyLong());
  }

  // Regression for #8480: a failed targeted transfer falls through to the next candidate
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

  // Regression for #8480: leadership lost mid transfer with no new leader reports false
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

  // Regression for #8480: a transfer that won despite A failed call is reported as A handoff
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

  // Regression for #8480: a bare step down is not A success while no other leader is seen
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

  // Regression for #8480: a bare step down that re elects the same node is not A success
  /** The election after a bare step-down re-elected this very node: nothing was handed off. */
  @Test
  void aBareStepDownThatReElectsTheSameNodeIsNotASuccess() throws Exception {
    everyPeerLags();
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(isNull(), anyLong())).thenReturn(ok);
    raft.on("getLeaderId", CallLog.inOrder(null, null, SELF));

    assertThat(manager().transferLeadership(100)).isFalse();
  }

  // Regression for #8480: a bare step down reports success once A different leader is seen
  /** Control: the fallback still reports a real handoff once a different leader is in place. */
  @Test
  void aBareStepDownReportsSuccessOnceADifferentLeaderIsSeen() throws Exception {
    everyPeerLags();
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(isNull(), anyLong())).thenReturn(ok);
    raft.on("getLeaderId", CallLog.inOrder(null, null, C));

    assertThat(manager().transferLeadership(10_000)).isTrue();
  }

  // Regression for #8480: only an unnamed refusal from the intended leader is held back
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

  // Regression for #8480: the forward wait ends when the view moves away from the refusing node
  /** The wait ends as soon as the view stops naming the refusing node: on an election in progress (null) too. */
  @Test
  void theForwardWaitEndsWhenTheViewMovesAwayFromTheRefusingNode() {
    final AtomicInteger probes = new AtomicInteger();
    final boolean moved = RaftReplicatedDatabase.awaitLeaderViewMovedFrom(
        () -> probes.incrementAndGet() < 3 ? SELF : null, SELF.toString(), 10_000, 1);

    assertThat(moved).isTrue();
    assertThat(probes.get()).isEqualTo(3);
  }

  // Regression for #8480: the forward wait is bounded
  /** And it is bounded: a view that never moves gives up and lets the refusal through. */
  @Test
  void theForwardWaitIsBounded() {
    assertThat(RaftReplicatedDatabase.awaitLeaderViewMovedFrom(() -> SELF, SELF.toString(), 50, 5)).isFalse();
  }

  // Regression for #8480: the forward wait returns at once when the view already moved
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

  // Regression for #8480: step down ends on A transfer that won despite A failed call
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

  // Regression for #8480: step down ends when the win only shows at the next candidate
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

  // Regression for #8480: the bare step down confirmation gets only what the rpc left of the budget
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

  // Regression for #8480: the bare step down is never sent from A node that is no longer the leader
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

  // Regression for #8480: step down refuses when leadership was lost without A handoff
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
  // ---- #8557 ----
  // Regression test for issue #8557: two leadership hand-offs racing on one leader. Ratis keeps ONE pending transfer
  // per leader and refuses, at once, a second targeted transfer naming a different peer ("a previous PendingRequest
  // ... exists"). The losing caller treated that refusal as a failure of the candidate, walked the rest of the list in
  // microseconds and, with its whole budget unspent, fell through to the bare no-target step-down - which Ratis does
  // NOT hold back for the pending transfer, so the leader stepped down at the same term, the pending transfer failed
  // with it, and the cluster sat leaderless for a full election timeout (the #8480 outcome, reached through #8480's own
  // fallback).
  //
  // Three layers, one test group each: the refusal is recognised and ends the hand-off instead of walking the list;
  // the bare step-down is never sent while this node has a targeted transfer in flight; and the #8491 hand-off no longer
  // runs inline on the health thread but on the same single-worker executor the #8483 and #5346 hand-offs use.

  // Regression for #8557: ratis refusal of A second transfer is recognised
  // ---- the refusal is classified -------------------------------------------------------------------------------
  @Test
  void ratisRefusalOfASecondTransferIsRecognised() {
    assertThat(RaftClusterManager.isTransferAlreadyPending(pendingRefusal(B))).isTrue();
    assertThat(RaftClusterManager.isTransferAlreadyPending(new IOException("wrapped", pendingRefusal(B)))).isTrue();
    // other refusals of a transfer are not "someone else is already handing this leadership over"
    assertThat(RaftClusterManager.isTransferAlreadyPending(
        new TransferLeadershipException("peer-a: Failed to transfer leadership to peer-b: not up-to-date"))).isFalse();
    assertThat(RaftClusterManager.isTransferAlreadyPending(new IOException("client-1 is already CLOSED"))).isFalse();
    assertThat(RaftClusterManager.isTransferAlreadyPending(null)).isFalse();
  }

  // Regression for #8557: the targeted transfer reports A pending refusal without retrying
  /** The targeted overload reports the refusal as its own type, and does not retry it the way it retries a closed client. */
  @Test
  void theTargetedTransferReportsAPendingRefusalWithoutRetrying() throws Exception {
    final RaftClientReply refused = refusedReply(C);
    when(admin.transferLeadership(eq(C), anyLong())).thenReturn(refused);
    raft.leaderId(SELF);

    assertThatThrownBy(() -> manager().transferLeadership(C.toString(), 10_000))
        .isInstanceOf(LeadershipTransferInProgressException.class)
        .isInstanceOf(ConfigurationException.class);

    verify(admin, times(1)).transferLeadership(eq(C), anyLong());
  }

  // Regression for #8557: a pending refusal ends the no target transfer without the bare step down
  // ---- the no-target transfer stops at the refusal ---------------------------------------------------------------

  /**
   * The race of the issue: the first candidate is refused because another caller's transfer is pending. The loop must
   * not try the next candidate (refused identically) nor fall back to the bare step-down; it waits for the other
   * transfer and reports whether leadership moved.
   */
  @Test
  void aPendingRefusalEndsTheNoTargetTransferWithoutTheBareStepDown() throws Exception {
    final RaftClientReply refused = refusedReply(B);
    when(admin.transferLeadership(eq(B), anyLong())).thenReturn(refused);
    // this node is still leader when the refusal comes back; the other caller's transfer lands shortly after
    raft.on("getLeaderId", CallLog.inOrder(SELF, SELF, SELF, C));

    assertThat(manager().transferLeadership(10_000)).as("the other caller's hand-off landed").isTrue();

    verify(admin, times(1)).transferLeadership(eq(B), anyLong());
    verify(admin, never()).transferLeadership(eq(C), anyLong());
    verify(admin, never()).transferLeadership(isNull(), anyLong());
  }

  // Regression for #8557: a pending refusal that never lands reports false without the bare step down
  /** The other transfer never lands within the budget: false, and still no bare step-down. */
  @Test
  void aPendingRefusalThatNeverLandsReportsFalseWithoutTheBareStepDown() throws Exception {
    final RaftClientReply refused = refusedReply(B);
    when(admin.transferLeadership(eq(B), anyLong())).thenReturn(refused);
    raft.leaderId(SELF);

    assertThat(manager().transferLeadership(200)).isFalse();

    verify(admin, never()).transferLeadership(eq(C), anyLong());
    verify(admin, never()).transferLeadership(isNull(), anyLong());
  }

  // Regression for #8557: a pending refusal after A client closed retry does not walk the list
  /**
   * The single-driver variant of the issue: #8487's client-closed retry resends to the same candidate, and the resend
   * is refused because Ratis still holds a pending request. That too ends the hand-off rather than walking the list.
   */
  @Test
  void aPendingRefusalAfterAClientClosedRetryDoesNotWalkTheList() throws Exception {
    final RaftClientReply refused = refusedReply(B);
    when(admin.transferLeadership(eq(B), anyLong())).thenThrow(new IOException("client-1 is already CLOSED"))
        .thenReturn(refused);
    raft.leaderId(SELF);

    assertThat(manager().transferLeadership(300)).isFalse();

    verify(admin, times(2)).transferLeadership(eq(B), anyLong());
    verify(admin, never()).transferLeadership(eq(C), anyLong());
    verify(admin, never()).transferLeadership(isNull(), anyLong());
  }

  // Regression for #8557: the bare step down is not sent while A targeted transfer is in flight
  // ---- the bare step-down waits out a targeted transfer in flight -----------------------------------------------

  /**
   * Ratis routes a no-target request to stepDownLeaderAsync, which a pending transfer does not block: sent while this
   * node's own targeted transfer is in flight it would step the leader down under it. It must not be sent.
   */
  @Test
  void theBareStepDownIsNotSentWhileATargetedTransferIsInFlight() throws Exception {
    final CountDownLatch inFlight = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(eq(B), anyLong())).thenAnswer(invocation -> {
      inFlight.countDown();
      release.await(10, TimeUnit.SECONDS);
      return ok;
    });
    raft.leaderId(SELF);

    final RaftClusterManager manager = manager();
    final CompletableFuture<Void> targeted = CompletableFuture.runAsync(() -> manager.transferLeadership(B.toString(), 10_000));
    try {
      assertThat(inFlight.await(10, TimeUnit.SECONDS)).isTrue();

      assertThat(manager.stepDownWithoutTarget(200)).isFalse();

      verify(admin, never()).transferLeadership(isNull(), anyLong());
    } finally {
      release.countDown();
    }
    targeted.get(10, TimeUnit.SECONDS);

    // Control: with nothing in flight any more, the bare step-down is sent as before.
    when(admin.transferLeadership(isNull(), anyLong())).thenReturn(ok);
    manager.stepDownWithoutTarget(100);
    verify(admin).transferLeadership(isNull(), anyLong());
  }

  // Regression for #8557: a targeted transfer is not sent while A bare step down is in flight
  /** The mirror: a targeted transfer is not sent while this node's bare step-down is in flight; it is refused as in progress. */
  @Test
  void aTargetedTransferIsNotSentWhileABareStepDownIsInFlight() throws Exception {
    final CountDownLatch inFlight = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(isNull(), anyLong())).thenAnswer(invocation -> {
      inFlight.countDown();
      release.await(10, TimeUnit.SECONDS);
      return ok;
    });
    raft.leaderId(SELF);

    final RaftClusterManager manager = manager();
    final CompletableFuture<Boolean> bare = CompletableFuture.supplyAsync(() -> manager.stepDownWithoutTarget(300));
    try {
      assertThat(inFlight.await(10, TimeUnit.SECONDS)).isTrue();

      assertThatThrownBy(() -> manager.transferLeadership(B.toString(), 10_000))
          .isInstanceOf(LeadershipTransferInProgressException.class);

      verify(admin, never()).transferLeadership(eq(B), anyLong());
    } finally {
      release.countDown();
    }
    bare.get(10, TimeUnit.SECONDS);
  }

  // Regression for #8557: a backed off bare step down does not block other targeted transfers while it waits
  /**
   * A bare step-down that backed off holds nothing while it waits for the transfer in flight: a third caller's targeted
   * transfer in that window is sent, not refused as "a step-down without a target is in progress" (review of PR #8596).
   */
  @Test
  void aBackedOffBareStepDownDoesNotBlockOtherTargetedTransfersWhileItWaits() throws Exception {
    final CountDownLatch inFlight = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch waiting = new CountDownLatch(1);
    final RaftClientReply ok = reply(true);
    when(admin.transferLeadership(eq(B), anyLong())).thenAnswer(invocation -> {
      inFlight.countDown();
      release.await(10, TimeUnit.SECONDS);
      return ok;
    });
    when(admin.transferLeadership(eq(C), anyLong())).thenReturn(ok);
    // the first leader-view read comes from the backed-off step-down's wait: the B transfer is blocked in its RPC
    raft.on("getLeaderId", args -> {
      waiting.countDown();
      return SELF;
    });

    final RaftClusterManager manager = manager();
    final CompletableFuture<Void> targeted = CompletableFuture.runAsync(() -> manager.transferLeadership(B.toString(), 10_000));
    CompletableFuture<Boolean> bare = null;
    try {
      assertThat(inFlight.await(10, TimeUnit.SECONDS)).isTrue();
      bare = CompletableFuture.supplyAsync(() -> manager.stepDownWithoutTarget(2_000));
      assertThat(waiting.await(10, TimeUnit.SECONDS)).isTrue();

      assertThatCode(() -> manager.transferLeadership(C.toString(), 10_000)).doesNotThrowAnyException();

      verify(admin).transferLeadership(eq(C), anyLong());
      verify(admin, never()).transferLeadership(isNull(), anyLong());
    } finally {
      release.countDown();
    }
    targeted.get(10, TimeUnit.SECONDS);
    bare.get(10, TimeUnit.SECONDS);
  }

  // Regression for #8557: step down stops at A pending refusal and reports the other hand off
  // ---- stepDown() has its own candidate loop --------------------------------------------------------------------
  @Test
  void stepDownStopsAtAPendingRefusalAndReportsTheOtherHandOff() {
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicInteger bareStepDowns = new AtomicInteger();
    final RaftHAServer server = stepDownServer(attempts, bareStepDowns, true);

    assertThatCode(server::stepDown).doesNotThrowAnyException();
    assertThat(attempts.get()).as("no second candidate").isEqualTo(1);
    assertThat(bareStepDowns.get()).as("no bare step-down").isZero();
  }

  // Regression for #8557: step down stops at A pending refusal and fails when the other hand off does not land
  @Test
  void stepDownStopsAtAPendingRefusalAndFailsWhenTheOtherHandOffDoesNotLand() {
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicInteger bareStepDowns = new AtomicInteger();
    final RaftHAServer server = stepDownServer(attempts, bareStepDowns, false);

    assertThatThrownBy(server::stepDown).isInstanceOf(ReplicationException.class)
        .hasMessageContaining("another leadership transfer");
    assertThat(attempts.get()).isEqualTo(1);
    assertThat(bareStepDowns.get()).isZero();
  }

  // Regression for #8557: the replacing database hand off runs on the channel recovery executor
  // ---- the #8491 hand-off is serialised with #8483 and #5346 ------------------------------------------------------

  /**
   * The replacing-database hand-off runs on the channel-recovery executor, the single worker the #8483 and #5346
   * hand-offs already share, and not on the calling (health-monitor) thread; a tick while one is queued queues nothing.
   */
  @Test
  void theReplacingDatabaseHandOffRunsOnTheChannelRecoveryExecutor() throws Exception {
    final RaftHAServer server = new RaftHAServer(detachedServer(), threeNodeConfig()) {
      @Override
      public boolean isLeader() {
        return true;
      }
    };
    final FakeArcadeStateMachine sm = new FakeArcadeStateMachine();
    sm.returns("hasLeaderServiceGap", true);
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicReference<String> ranOn = new AtomicReference<>();
    final AtomicInteger runs = new AtomicInteger();
    final CountDownLatch duplicateRun = new CountDownLatch(1);
    sm.on("handOffLeadershipWhileReplacingDatabase", args -> {
      if (runs.incrementAndGet() > 1) {
        duplicateRun.countDown();
        return false;
      }
      ranOn.set(Thread.currentThread().getName());
      started.countDown();
      try {
        release.await(10, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        // An answer cannot throw a checked exception: keep the interrupt for the caller to see
        Thread.currentThread().interrupt();
      }
      return false;
    });
    // queueReplacingDatabaseHandOff runs a queued hand-off only for the CURRENT state machine
    setStateMachine(server, sm);

    try {
      server.queueReplacingDatabaseHandOff(sm);
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(ranOn.get()).isEqualTo("arcadedb-raft-channel-recovery");

      // a second health tick while the first hand-off is still running queues nothing
      server.queueReplacingDatabaseHandOff(sm);
    } finally {
      release.countDown();
    }
    // A duplicate queued behind the first would run as soon as the single worker is free: wait for it, bounded. A
    // wait expected to time out needs no stall discount.
    assertThat(duplicateRun.await(500, TimeUnit.MILLISECONDS)).as("no second hand-off was queued").isFalse();
    assertThat(runs.get()).isEqualTo(1);
  }

  // Regression for #8557: a hand off queued for A replaced state machine does not run
  /** A hand-off queued for a state machine that restartRatis() has since replaced does not run (review of PR #8596). */
  @Test
  void aHandOffQueuedForAReplacedStateMachineDoesNotRun() throws Exception {
    final RaftHAServer server = new RaftHAServer(detachedServer(), threeNodeConfig()) {
      @Override
      public boolean isLeader() {
        return true;
      }
    };
    final FakeArcadeStateMachine stale = new FakeArcadeStateMachine();
    stale.returns("hasLeaderServiceGap", true);
    final FakeArcadeStateMachine current = new FakeArcadeStateMachine();
    setStateMachine(server, current);
    final CountDownLatch ran = new CountDownLatch(1);
    stale.on("handOffLeadershipWhileReplacingDatabase", args -> {
      ran.countDown();
      return false;
    });

    server.queueReplacingDatabaseHandOff(stale);

    assertThat(ran.await(500, TimeUnit.MILLISECONDS)).isFalse();
  }

  // Regression for #8557: the refusal is recognised after the ratis wire round trip
  /**
   * The refusal survives the client boundary: Ratis serialises the reply to protobuf on the server and the client
   * rebuilds it, so the classification must hold for the rebuilt exception, not only for the one the server created.
   */
  @Test
  void theRefusalIsRecognisedAfterTheRatisWireRoundTrip() {
    final RaftClientReply serverSide = RaftClientReply.newBuilder()
        .setClientId(ClientId.randomId())
        .setServerId(RaftGroupMemberId.valueOf(SELF, RaftGroupId.randomId()))
        .setCallId(7)
        .setException(pendingRefusal(B))
        .build();

    final RaftClientReply clientSide = ClientProtoUtils.toRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(serverSide));

    assertThat(clientSide.isSuccess()).isFalse();
    assertThat(RaftClusterManager.isTransferAlreadyPending(clientSide.getException())).isTrue();
  }

  private static void setStateMachine(final RaftHAServer server, final ArcadeStateMachine sm) throws Exception {
    final Field field = RaftHAServer.class.getDeclaredField("stateMachine");
    field.setAccessible(true);
    field.set(server, sm);
  }

  // Regression for #8557: no replacement queues no hand off
  /** Nothing to hand off: no task is queued at all (the tick stays one map read on a healthy leader). */
  @Test
  void noReplacementQueuesNoHandOff() throws Exception {
    final RaftHAServer server = new RaftHAServer(detachedServer(), threeNodeConfig()) {
      @Override
      public boolean isLeader() {
        return true;
      }
    };
    final FakeArcadeStateMachine sm = new FakeArcadeStateMachine();
    sm.returns("hasLeaderServiceGap", false);

    server.queueReplacingDatabaseHandOff(sm);
    Thread.sleep(100);

    assertThat(sm.calls("handOffLeadershipWhileReplacingDatabase")).isEmpty();
  }

  // ---- helpers --------------------------------------------------------------------------------------------------

  private RaftHAServer stepDownServer(final AtomicInteger attempts, final AtomicInteger bareStepDowns,
      final boolean otherHandOffLands) {
    return new RaftHAServer(detachedServer(), threeNodeConfig()) {
      // Every configured peer answers (issue #8556): these tests are about concurrent hand-offs, not reachability.
      @Override
      Set<String> handoffReachablePeers() {
        return getLivePeers().stream().map(peer -> peer.getId().toString()).collect(Collectors.toSet());
      }

      @Override
      public boolean isLeader() {
        return true;
      }

      @Override
      public RaftPeerId getLeaderId() {
        return getLocalPeerId();
      }

      @Override
      public void transferLeadership(final String targetPeerId, final long timeoutMs) {
        attempts.incrementAndGet();
        throw new LeadershipTransferInProgressException(targetPeerId, pendingRefusal(RaftPeerId.valueOf(targetPeerId)));
      }

      @Override
      boolean concurrentHandOffLanded() {
        return otherHandOffLands;
      }

      @Override
      boolean stepDownWithoutTarget(final long timeoutMs) {
        bareStepDowns.incrementAndGet();
        return false;
      }
    };
  }

  /** The refusal exactly as Ratis 3.3 builds it in TransferLeadership.createReplyFutureFromPreviousRequest. */
  private static TransferLeadershipException pendingRefusal(final RaftPeerId newLeader) {
    return new TransferLeadershipException(SELF + "@group-ABC: Failed to transfer leadership to " + newLeader
        + ": a previous PendingRequest:TransferLeadershipRequest:client-1->" + SELF + "@group-ABC, cid=7, seq=0, RW, "
        + "newLeader: " + C + " exists");
  }

  private static RaftClientReply refusedReply(final RaftPeerId newLeader) {
    final RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(false);
    final TransferLeadershipException refusal = pendingRefusal(newLeader);
    when(reply.getException()).thenReturn(refusal);
    return reply;
  }
  /**
   * Regression test for issue #8487: the TARGETED {@code transferLeadership(peerId, timeoutMs)} judged a failed call by
   * sampling the leader view once, the instant the failure was caught. Under concurrent writes the call fails with
   * "client-... is already CLOSED" - every leader change makes {@code refreshRaftClient()} close the client the RPC was
   * sent through - so what the method reported depended on which node the view happened to name at that instant.
   *
   * A failed call is now settled rather than sampled: the method waits, within the caller's budget, for a concrete
   * leader, and reports the handoff when that leader is the target. When the leader is this node again and the failure
   * was only the closed client, the transfer is re-sent through the fresh client (Ratis joins a pending transfer to the
   * same target instead of starting a second one). Every other outcome fails and names what actually happened.
   */
  @Nested
  class TargetedTransferSettles {

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

    // Regression for #8487: a closed client failure is settled by waiting for the target
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

    // Regression for #8487: a transfer where another peer won names the actual leader
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

    // Regression for #8487: no leader settling within the budget fails
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

    // Regression for #8487: a closed client while still leader is retried through the fresh client
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

    // Regression for #8487: a node re elected within the budget tries the transfer again
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

    // Regression for #8487: a refusal while still leader fails at once
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

    // Regression for #8487: closed client retries are bounded
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

    // Regression for #8487: a successful reply returns at once
    /** Control: the plain success path still issues exactly one call and does not poll the leader view. */
    @Test
    void aSuccessfulReplyReturnsAtOnce() throws Exception {
      final RaftClientReply ok = reply(true);
      when(admin.transferLeadership(eq(B), anyLong())).thenReturn(ok);

      assertThatCode(() -> manager().transferLeadership(B.toString(), 10_000)).doesNotThrowAnyException();
      verify(admin, times(1)).transferLeadership(eq(B), anyLong());
      assertThat(raft.calls("getLeaderId")).isEmpty();
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
}
