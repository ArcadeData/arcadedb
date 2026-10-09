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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.CallLog;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
 * Regression test for issue #8557: two leadership hand-offs racing on one leader. Ratis keeps ONE pending transfer
 * per leader and refuses, at once, a second targeted transfer naming a different peer ("a previous PendingRequest
 * ... exists"). The losing caller treated that refusal as a failure of the candidate, walked the rest of the list in
 * microseconds and, with its whole budget unspent, fell through to the bare no-target step-down - which Ratis does
 * NOT hold back for the pending transfer, so the leader stepped down at the same term, the pending transfer failed
 * with it, and the cluster sat leaderless for a full election timeout (the #8480 outcome, reached through #8480's own
 * fallback).
 * <p>
 * Three layers, one test group each: the refusal is recognised and ends the hand-off instead of walking the list;
 * the bare step-down is never sent while this node has a targeted transfer in flight; and the #8491 hand-off no longer
 * runs inline on the health thread but on the same single-worker executor the #8483 and #5346 hand-offs use.
 */
class Issue8557ConcurrentLeadershipHandOffsTest {

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
    // Every peer answers (issue #8556): these tests are about concurrent hand-offs, not reachability.
    raft.returns("handoffReachablePeers", Set.of(B.toString(), C.toString()));
  }

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

  private RaftClusterManager manager() {
    final RaftClusterManager manager = new RaftClusterManager(raft);
    manager.leaderConfirmGraceMs = 100;
    return manager;
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

  private static RaftClientReply reply(final boolean success) {
    final RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(success);
    return reply;
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

  private static RaftPeer peer(final RaftPeerId id) {
    return RaftPeer.newBuilder().setId(id).setAddress("localhost:" + id.toString().substring(id.toString().indexOf('_') + 1))
        .build();
  }
}
