/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10144: the misdirected-request listener picked the throttling slot of an id outside the cluster by the requestor
 * id, an unauthenticated header field, so a sender that wrote this node's own id there held the slot the repair of this
 * node's own channel to a peer joined after start needed, and that repair was dropped for the whole interval. The slot is
 * now chosen by the addressed id alone, and such a peer is known to the listener from the configuration that added it.
 * <p>
 * The Ratis division is parked until the test lets it go wherever the order in which slots are claimed matters, so both
 * refusals are taken before any recovery task can read the live configuration.
 */
class Issue10144MisdirectedSlotIgnoresRequestorTest {

  // None of these refusals carries a self-dial probe's nonce (issue #10299)
  private static final long NO_PROBE = RaftHAServer.NO_PROBE_CALL_ID;
  private static final String   SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
  private static final RaftPeer JOINED      = RaftPeer.newBuilder().setId("joined_peer").setAddress("localhost:2439").build();
  private static final RaftPeer LEAVING     = RaftPeer.newBuilder().setId("leaving_peer").setAddress("localhost:2440").build();
  // Where a request comes from no longer decides whether it is this node's own: a probe does (issue #10299)
  private static final InetSocketAddress FROM_THIS_HOST = new InetSocketAddress(InetAddress.getLoopbackAddress(), 40_000);
  // A stranger is somewhere else (TEST-NET-1, RFC 5737): a request claiming this node's id from there is no self-dial
  private static final InetSocketAddress FROM_ELSEWHERE = new InetSocketAddress("192.0.2.10", 40_000);
  private static final String   SELF_DIAL   = "This node sent a Raft appendEntries meant for peer 'joined_peer'";
  private static final String   NOT_A_PEER  = "is not a peer of this cluster";

  private CapturingTestLogger log;
  private CountDownLatch      divisionGate;

  @BeforeEach
  void installLogger() {
    log = CapturingTestLogger.install();
    divisionGate = new CountDownLatch(1);
  }

  @AfterEach
  void restore() {
    // Frees any worker still parked in the proxy, so the daemon thread does not outlive the test
    divisionGate.countDown();
    log.uninstall();
  }

  /**
   * The property, not one instance of it: whatever a stranger writes in the requestor field - nothing, a foreign id, or this
   * node's own id - its refusal does not keep the self-dial repair to a peer joined after start from being queued.
   */
  @ParameterizedTest(name = "stranger requestor = {0}")
  @ValueSource(strings = { "<none>", "foreign_peer", "<this node>" })
  void theSelfDialRepairIsQueuedWhateverRequestorAStrangerWrites(final String strangerRequestor) throws Exception {
    final RaftHAServer raft = detachedServer(parkedDivision(List.of(JOINED)));
    raft.onRaftConfigurationApplied(List.of(raft.getLocalPeerId(), JOINED.getId()), List.of());

    final RaftPeerId requestor = switch (strangerRequestor) {
      case "<none>" -> null;
      case "<this node>" -> raft.getLocalPeerId();
      default -> RaftPeerId.valueOf(strangerRequestor);
    };

    returnsWithoutWaiting(() -> raft.onMisdirectedRaftRequest("requestVote", RaftPeerId.valueOf("stranger"), requestor, FROM_ELSEWHERE, NO_PROBE));
    returnsWithoutWaiting(() -> raft.onMisdirectedRaftRequest("appendEntries", JOINED.getId(), raft.getLocalPeerId(), FROM_THIS_HOST, NO_PROBE));
    divisionGate.countDown();

    Awaitility.await("the self-dialled channel is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(SELF_DIAL) == 1);
  }

  /**
   * The requestor does not choose the slot of an id outside the cluster: every such id shares one, whatever the header says,
   * so three strangers inside one interval queue one recovery task and are reported once. Before the fix the one that wrote
   * this node's own id took a second slot of its own.
   */
  @Test
  void aStrangerCannotChooseItsSlotByTheRequestorItWrites() throws Exception {
    final RaftHAServer raft = detachedServer(parkedDivision(List.of()));

    returnsWithoutWaiting(() -> raft.onMisdirectedRaftRequest("requestVote", RaftPeerId.valueOf("stranger_a"), raft.getLocalPeerId(), FROM_ELSEWHERE, NO_PROBE));
    returnsWithoutWaiting(
        () -> raft.onMisdirectedRaftRequest("requestVote", RaftPeerId.valueOf("stranger_b"), RaftPeerId.valueOf("localhost_2435"), FROM_ELSEWHERE, NO_PROBE));
    returnsWithoutWaiting(() -> raft.onMisdirectedRaftRequest("requestVote", RaftPeerId.valueOf("stranger_c"), null, FROM_ELSEWHERE, NO_PROBE));
    // A declared peer has a slot of its own; its task runs after every task queued before it on the single recovery worker,
    // so its report says the strangers' tasks have all run
    returnsWithoutWaiting(() -> raft.onMisdirectedRaftRequest("appendEntries", RaftPeerId.valueOf("localhost_2436"),
        RaftPeerId.valueOf("localhost_2435"), FROM_THIS_HOST, NO_PROBE));
    divisionGate.countDown();

    // A claim of this node's id runs on the probe worker (issue #10299), so the strangers' report is awaited on its own
    Awaitility.await("the recovery worker drained").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining("addressed to peer 'localhost_2436'") == 1
            && log.countFormattedContaining(NOT_A_PEER) >= 1);
    assertThat(log.countFormattedContaining(NOT_A_PEER)).as("three strangers share one slot whatever requestor they wrote")
        .isEqualTo(1);
    assertThat(raft.getMisdirectedRaftRequestsRefused()).isEqualTo(4L);
  }

  /**
   * A live configuration read by anyone - the health-monitor tick, the cluster API - teaches the listener its members too,
   * which covers a peer that came in with a snapshot rather than through a configuration entry this node applied.
   */
  @Test
  void aPeerLearntFromAnyReadOfTheLiveConfigurationGetsASlotOfItsOwn() throws Exception {
    final AtomicBoolean park = new AtomicBoolean(false);
    final RaftHAServer raft = detachedServer(ratis(() -> {
      if (park.get())
        divisionGate.await();
      return liveDivision(List.of(JOINED), List.of());
    }));

    assertThat(raft.getCommittedPeersOrNull()).extracting(RaftPeer::getId).contains(JOINED.getId());
    park.set(true);

    returnsWithoutWaiting(() -> raft.onMisdirectedRaftRequest("requestVote", RaftPeerId.valueOf("stranger"), raft.getLocalPeerId(), FROM_ELSEWHERE, NO_PROBE));
    returnsWithoutWaiting(() -> raft.onMisdirectedRaftRequest("appendEntries", JOINED.getId(), raft.getLocalPeerId(), FROM_THIS_HOST, NO_PROBE));
    divisionGate.countDown();

    Awaitility.await("the self-dialled channel is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(SELF_DIAL) == 1);
  }

  /**
   * A member removed since leaves the copy at the next live read, so it no longer has a slot of its own and shares the
   * strangers' one: the copy stays bounded by the current membership, not by every peer ever seen.
   */
  @Test
  void aRemovedMemberLeavesTheCopyAtTheNextLiveRead() throws Exception {
    final RaftHAServer raft = detachedServer(parkedDivision(List.of()));
    raft.onRaftConfigurationApplied(List.of(raft.getLocalPeerId(), JOINED.getId()), List.of());
    divisionGate.countDown();

    // The live configuration no longer lists the joined peer
    assertThat(raft.getCommittedPeersOrNull()).extracting(RaftPeer::getId).doesNotContain(JOINED.getId());

    final CountDownLatch secondGate = new CountDownLatch(1);
    final Field field = RaftHAServer.class.getDeclaredField("raftServer");
    field.setAccessible(true);
    field.set(raft, ratis(() -> {
      secondGate.await();
      return liveDivision(List.of(), List.of());
    }));
    try {
      returnsWithoutWaiting(() -> raft.onMisdirectedRaftRequest("requestVote", RaftPeerId.valueOf("stranger"), null, FROM_ELSEWHERE, NO_PROBE));
      returnsWithoutWaiting(() -> raft.onMisdirectedRaftRequest("appendEntries", JOINED.getId(), raft.getLocalPeerId(), FROM_ELSEWHERE, NO_PROBE));
      // A declared peer's report says the worker has run everything queued before it
      returnsWithoutWaiting(() -> raft.onMisdirectedRaftRequest("appendEntries", RaftPeerId.valueOf("localhost_2436"),
          RaftPeerId.valueOf("localhost_2435"), FROM_THIS_HOST, NO_PROBE));
    } finally {
      secondGate.countDown();
    }

    Awaitility.await("the recovery worker drained").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining("addressed to peer 'localhost_2436'") == 1);
    assertThat(log.countFormattedContaining("joined_peer")).as("the removed peer shared the strangers' slot and was dropped")
        .isZero();
  }

  /**
   * The caller, not only the capability: the state machine's configuration callback is what hands an applied configuration to
   * the listener, the new peers and, during a joint-consensus change, the old ones too, since this node still dials them.
   */
  @Test
  void theStateMachineHandsEveryAppliedConfigurationToTheListener() throws Exception {
    // Mid joint-consensus change: the live configuration's current peers have the joined one, its previous peers the leaving one
    final RaftHAServer raft = detachedServer(ratis(() -> {
      divisionGate.await();
      return liveDivision(List.of(JOINED), List.of(LEAVING));
    }));
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.setRaftHAServer(raft);
      sm.notifyConfigurationChanged(1, 10,
          configuration(List.of(raft.getLocalPeerId().toString(), "joined_peer"), List.of(raft.getLocalPeerId().toString(), "leaving_peer")));

      returnsWithoutWaiting(() -> raft.onMisdirectedRaftRequest("requestVote", RaftPeerId.valueOf("stranger"), raft.getLocalPeerId(), FROM_ELSEWHERE, NO_PROBE));
      returnsWithoutWaiting(() -> raft.onMisdirectedRaftRequest("appendEntries", JOINED.getId(), raft.getLocalPeerId(), FROM_THIS_HOST, NO_PROBE));
      returnsWithoutWaiting(
          () -> raft.onMisdirectedRaftRequest("appendEntries", RaftPeerId.valueOf("leaving_peer"), RaftPeerId.valueOf("localhost_2435"), FROM_THIS_HOST, NO_PROBE));
      divisionGate.countDown();

      Awaitility.await("both members' refusals are looked at").atMost(10, TimeUnit.SECONDS)
          .until(() -> log.countFormattedContaining(SELF_DIAL) == 1
              && log.countFormattedContaining("addressed to peer 'leaving_peer'") == 1);
    } finally {
      sm.close();
    }
  }

  /**
   * Runs {@code call} on a thread of its own and fails when it has not returned after a generous bound: the division is
   * parked, so a call that waits for it never returns at all.
   */
  private static void returnsWithoutWaiting(final Runnable call) throws Exception {
    final CompletableFuture<Void> handler = CompletableFuture.runAsync(call);
    try {
      handler.get(30, TimeUnit.SECONDS);
    } catch (final TimeoutException e) {
      throw new AssertionError("The listener waited for the Raft division: it runs on the gRPC handler thread and must not "
          + "block (issue #10088)", e);
    }
  }

  private RaftServer parkedDivision(final List<RaftPeer> extraPeers) {
    return ratis(() -> {
      divisionGate.await();
      return liveDivision(extraPeers, List.of());
    });
  }

  /** A division whose live configuration lists {@code peers}, and {@code previousPeers} as the old side of a joint change. */
  private Object liveDivision(final List<RaftPeer> peers, final List<RaftPeer> previousPeers) {
    final RaftConfiguration conf = (RaftConfiguration) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] { RaftConfiguration.class }, (p, m, a) -> switch (m.getName()) {
          case "getCurrentPeers" -> peers;
          case "getPreviousPeers" -> previousPeers;
          default -> null;
        });
    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { RaftServer.Division.class },
        (p, m, a) -> "getRaftConf".equals(m.getName()) ? conf : null);
  }

  @FunctionalInterface
  private interface DivisionAnswer {
    Object answer() throws Exception;
  }

  /** A {@link RaftServer} that answers {@code getDivision} as told and nothing else. */
  private RaftServer ratis(final DivisionAnswer onGetDivision) {
    return (RaftServer) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> "getDivision".equals(method.getName()) ? onGetDivision.answer() : null);
  }

  private static RaftProtos.RaftConfigurationProto configuration(final List<String> peerIds, final List<String> oldPeerIds) {
    final RaftProtos.RaftConfigurationProto.Builder builder = RaftProtos.RaftConfigurationProto.newBuilder();
    for (final String peerId : peerIds)
      builder.addPeers(peer(peerId));
    for (final String peerId : oldPeerIds)
      builder.addOldPeers(peer(peerId));
    return builder.build();
  }

  private static RaftProtos.RaftPeerProto peer(final String id) {
    return RaftProtos.RaftPeerProto.newBuilder().setId(ByteString.copyFromUtf8(id)).setAddress("localhost:2434").build();
  }

  private static RaftHAServer detachedServer(final RaftServer ratis) throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
    final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
    final RaftHAServer raft = new RaftHAServer(arcadeServer, config);

    final Field field = RaftHAServer.class.getDeclaredField("raftServer");
    field.setAccessible(true);
    field.set(raft, ratis);
    // The stand-in Raft server has no channels: this node's own requests come back to it (issue #10299)
    SelfDialChannels.leadBackToItself(raft);
    return raft;
  }
}
