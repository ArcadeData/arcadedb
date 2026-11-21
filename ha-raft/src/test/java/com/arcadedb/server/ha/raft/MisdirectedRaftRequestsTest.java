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
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Handling of Raft requests that reach the wrong node ({@code MisdirectedRaftRequests}): throttled repair slots and a listener that never blocks the handler thread.
 */
class MisdirectedRaftRequestsTest {
  // ---- #10016 ----
  // The per-peer throttle behind {@link RaftHAServer#onMisdirectedRaftRequest} (issue #10016): a peer whose name keeps
  // resolving to the wrong node has every heartbeat refused, and that must queue one channel recovery and log one
  // WARNING per interval, not one per refused RPC.

  private static final RaftPeerId PEER  = RaftPeerId.valueOf("arcade2_2434");
  private static final RaftPeerId OTHER = RaftPeerId.valueOf("arcade3_2434");

  // Regression for #10016: a slot is claimed once per interval
  @Test
  void aSlotIsClaimedOncePerInterval() {
    final Map<RaftPeerId, Long> slots = new ConcurrentHashMap<>();

    assertThat(RaftHAServer.claimThrottleSlot(slots, PEER, 1_000, 5_000)).as("a free slot").isTrue();
    assertThat(RaftHAServer.claimThrottleSlot(slots, PEER, 1_000, 5_000)).as("same instant").isFalse();
    assertThat(RaftHAServer.claimThrottleSlot(slots, PEER, 5_999, 5_000)).as("inside the interval").isFalse();
    assertThat(RaftHAServer.claimThrottleSlot(slots, PEER, 6_000, 5_000)).as("the interval has elapsed").isTrue();
    assertThat(slots.get(PEER)).isEqualTo(6_000L);

    assertThat(RaftHAServer.claimThrottleSlot(slots, OTHER, 6_001, 5_000)).as("slots are per peer").isTrue();
  }

  // Regression for #10016: a released slot is claimable again
  /** A released slot - a recovery the executor rejected - is claimable again at once. */
  @Test
  void aReleasedSlotIsClaimableAgain() {
    final Map<RaftPeerId, Long> slots = new ConcurrentHashMap<>();
    assertThat(RaftHAServer.claimThrottleSlot(slots, PEER, 1_000, 5_000)).isTrue();
    slots.remove(PEER);
    assertThat(RaftHAServer.claimThrottleSlot(slots, PEER, 1_001, 5_000)).isTrue();
  }

  // Regression for #10016: exactly one of many concurrent callers wins
  /** Every gRPC handler thread refusing a heartbeat at the same moment: exactly one wins the slot. */
  @Test
  void exactlyOneOfManyConcurrentCallersWins() throws Exception {
    final int threads = 16;
    for (int round = 0; round < 50; round++) {
      final Map<RaftPeerId, Long> slots = new ConcurrentHashMap<>();
      if (round % 2 == 1)
        slots.put(PEER, 0L); // an expired claim: the replace arm, not the putIfAbsent one
      final AtomicInteger winners = new AtomicInteger();
      final CountDownLatch start = new CountDownLatch(1);
      final ExecutorService pool = Executors.newFixedThreadPool(threads);
      try {
        for (int i = 0; i < threads; i++)
          pool.execute(() -> {
            try {
              start.await();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
            if (RaftHAServer.claimThrottleSlot(slots, PEER, 10_000, 5_000))
              winners.incrementAndGet();
          });
        start.countDown();
      } finally {
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      }
      assertThat(winners.get()).as("round %d", round).isEqualTo(1);
    }
  }
  // ---- #10088 ----
  // Issue #10088: {@code MisdirectedRequestListener} runs on the gRPC handler thread of the refused request and must not
  // block, but {@link RaftHAServer#onMisdirectedRaftRequest} asked {@code isKnownPeer} before it claimed its throttle slot,
  // and that read the live Raft configuration through {@code RaftServer.getDivision}, which Ratis answers with a bare
  // {@code CompletableFuture.get()} while a division is being initialized. The same read, failing during that window,
  // logged a WARNING with a stack trace per refused heartbeat on a path designed to log once per peer per minute.
  //
  // The Ratis server is a {@link Proxy} whose {@code getDivision} parks until the test lets it go, so a handler that reads it
  // never returns and the test sees that, instead of inferring it from a bytecode listing.

  // None of these refusals carries a self-dial probe's nonce (issue #10299)
  private static final long NO_PROBE = MisdirectedRaftRequests.NO_PROBE_CALL_ID;
  private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
  // Every call below arrives from this host; whether one is this node's own is settled by a probe (issue #10299)
  private static final InetSocketAddress FROM_THIS_HOST = new InetSocketAddress(InetAddress.getLoopbackAddress(), 40_000);
  private static final String UNREADABLE  = "Cannot read the live Raft configuration";

  private CapturingTestLogger log;
  private CountDownLatch      divisionGate;
  private AtomicInteger       divisionReads;

  @BeforeEach
  void installLogger() {
    log = CapturingTestLogger.install();
    divisionGate = new CountDownLatch(1);
    divisionReads = new AtomicInteger();
  }

  @AfterEach
  void restore() {
    // Frees any worker still parked in the proxy, so the daemon thread does not outlive the test
    divisionGate.countDown();
    log.uninstall();
  }

  // Regression for #10088: a declared peer is classified without reading the live configuration
  /** A declared peer is known without reading anything: the handler thread must not touch the division at all. */
  @Test
  void aDeclaredPeerIsClassifiedWithoutReadingTheLiveConfiguration() throws Exception {
    final AtomicInteger probes = new AtomicInteger();
    final RaftHAServer raft = detachedServer(blockingRatis(), probes);

    returnsWhileTheDivisionIsParked(() -> raft.onMisdirectedRaftRequest("appendEntries", RaftPeerId.valueOf("localhost_2435"),
        raft.getLocalPeerId(), FROM_THIS_HOST, NO_PROBE));

    // Neither the handler thread nor the recovery task has a reason to ask: the declared list answers. The task is waited
    // for, or the count would be read before it had a chance to ask
    Awaitility.await("the recovery task completed").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining("This node sent a Raft appendEntries meant for peer 'localhost_2435'") == 1);
    assertThat(divisionReads.get()).isZero();
    // The refusal under test, plus the self-dial probe that call queued: the node claimed itself as the requestor, so the
    // recovery task sent a probe whose loop-back arrives at this same listener (issue #10299) and is counted too. The probe
    // is sent before the task logs the line awaited above, so by now the count is settled; read any earlier, it is 1 or more
    // depending on which thread got there first (issue #10706)
    assertThat(raft.getMisdirectedRaftRequestsRefused()).isEqualTo(1L + probes.get());
  }

  // Regression for #10088: an unknown peer does not park the handler thread while the division is initializing
  /** An id nobody declared is the case the live read existed for: it must not park the handler thread either. */
  @Test
  void anUnknownPeerDoesNotParkTheHandlerThreadWhileTheDivisionIsInitializing() throws Exception {
    final RaftHAServer raft = detachedServer(blockingRatis());

    for (int i = 0; i < 5; i++) {
      final RaftPeerId stranger = RaftPeerId.valueOf("stranger_" + i);
      returnsWhileTheDivisionIsParked(() -> raft.onMisdirectedRaftRequest("requestVote", stranger, RaftPeerId.valueOf("localhost_2435"), FROM_THIS_HOST, NO_PROBE));
    }

    assertThat(raft.getMisdirectedRaftRequestsRefused()).isEqualTo(5L);
  }

  // Regression for #10088: an unreadable configuration is not reported with A stack trace per refusal
  /**
   * The read moved to the recovery task is a diagnosis, not a fault: Ratis raises {@code IllegalStateException} for the
   * whole restart window by design, and one stack trace per refused heartbeat is the flood the listener's throttle exists
   * to prevent. The refusal itself is still reported, once.
   */
  @Test
  void anUnreadableConfigurationIsNotReportedWithAStackTracePerRefusal() throws Exception {
    final RaftHAServer raft = detachedServer(ratisThatCannotResolveTheDivision());

    for (int i = 0; i < 10; i++)
      raft.onMisdirectedRaftRequest("appendEntries", RaftPeerId.valueOf("stranger"), RaftPeerId.valueOf("localhost_2435"), FROM_THIS_HOST, NO_PROBE);

    Awaitility.await("the refusal is reported").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining("is not a peer of this cluster", "stranger") == 1);

    // Ten refusals share one throttled recovery task, so the read is attempted once, and it is not a WARNING
    assertThat(log.countContaining(UNREADABLE)).as("one throttled read for ten refusals, not one per refusal").isLessThanOrEqualTo(1);
    assertThat(log.formattedAt(Level.WARNING)).noneMatch(message -> message.contains(UNREADABLE));
  }

  // Regression for #10088: a peer only the live configuration knows is still treated as A member
  /**
   * A peer that joined after this node started is a member of the live configuration and not of the declared list. It is
   * still told apart from a stranger, now by the recovery task: a refusal naming it says its address leads here, not that
   * it is no peer of the cluster.
   */
  @Test
  void aPeerOnlyTheLiveConfigurationKnowsIsStillTreatedAsAMember() throws Exception {
    final RaftPeer joined = RaftPeer.newBuilder().setId("joined_peer").setAddress("localhost:2439").build();
    final RaftHAServer raft = detachedServer(ratisWithLivePeers(List.of(joined)));

    raft.onMisdirectedRaftRequest("appendEntries", joined.getId(), RaftPeerId.valueOf("localhost_2435"), FROM_THIS_HOST, NO_PROBE);

    Awaitility.await("the refusal is reported").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining("Refused a Raft appendEntries", "addressed to peer 'joined_peer'") == 1);
    assertThat(log.countFormattedContaining("is not a peer of this cluster", "joined_peer")).isZero();
  }

  // Regression for #10088: a strangers refusals do not delay the repair of A peer only the live configuration knows
  /**
   * The repair of this node's own channel to a peer only the live configuration knows must not wait behind a stranger. This
   * node dials such a peer only after applying the configuration that added it, which is where the listener learns it, so
   * it has a slot of its own (issue #10144: not one picked by the requestor id, which any sender writes). The division is
   * parked until both have been refused, which fixes the order a shared slot would have been claimed in.
   */
  @Test
  void aStrangersRefusalsDoNotDelayTheRepairOfAPeerOnlyTheLiveConfigurationKnows() throws Exception {
    final RaftPeer joined = RaftPeer.newBuilder().setId("joined_peer").setAddress("localhost:2439").build();
    final RaftHAServer raft = detachedServer(ratis((method, args) -> {
      divisionReads.incrementAndGet();
      divisionGate.await();
      return liveDivision(List.of(joined));
    }));
    raft.onRaftConfigurationApplied(List.of(raft.getLocalPeerId(), joined.getId()), List.of());

    returnsWhileTheDivisionIsParked(
        () -> raft.onMisdirectedRaftRequest("requestVote", RaftPeerId.valueOf("stranger"), RaftPeerId.valueOf("localhost_2435"), FROM_THIS_HOST, NO_PROBE));
    returnsWhileTheDivisionIsParked(() -> raft.onMisdirectedRaftRequest("appendEntries", joined.getId(), raft.getLocalPeerId(), FROM_THIS_HOST, NO_PROBE));
    divisionGate.countDown();

    Awaitility.await("the self-dialled channel is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining("This node sent a Raft appendEntries meant for peer 'joined_peer'") == 1);
  }

  /**
   * Runs {@code call} on a thread of its own and fails when it has not returned after a generous bound. The division is
   * parked for the whole test, so a call that waits for it never returns at all: the bound is a tripwire between a handler
   * that does not wait and one that waits forever.
   */
  private static void returnsWhileTheDivisionIsParked(final Runnable call) throws Exception {
    final CompletableFuture<Void> handler = CompletableFuture.runAsync(call);
    try {
      handler.get(30, TimeUnit.SECONDS);
    } catch (final TimeoutException e) {
      throw new AssertionError("The listener did not return while the Raft division could not be read: it runs on the gRPC "
          + "handler thread and must not block (issue #10088)", e);
    }
  }

  private RaftServer blockingRatis() {
    return ratis((method, args) -> {
      divisionReads.incrementAndGet();
      divisionGate.await();
      throw new IllegalStateException("stateMachineUpdater is uninitialized");
    });
  }

  private RaftServer ratisThatCannotResolveTheDivision() {
    return ratis((method, args) -> {
      divisionReads.incrementAndGet();
      throw new IllegalStateException("stateMachineUpdater is uninitialized");
    });
  }

  private RaftServer ratisWithLivePeers(final List<RaftPeer> extraPeers) {
    return ratis((method, args) -> {
      divisionReads.incrementAndGet();
      return liveDivision(extraPeers);
    });
  }

  /** A division whose live configuration lists {@code peers}. */
  private Object liveDivision(final List<RaftPeer> peers) {
    final RaftConfiguration conf = (RaftConfiguration) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] { RaftConfiguration.class }, (p, m, a) -> "getCurrentPeers".equals(m.getName()) ? peers : null);
    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { RaftServer.Division.class },
        (p, m, a) -> "getRaftConf".equals(m.getName()) ? conf : null);
  }

  @FunctionalInterface
  private interface DivisionAnswer {
    Object answer(String method, Object[] args) throws Exception;
  }

  /** A {@link RaftServer} that answers {@code getDivision} as told and nothing else. */
  private RaftServer ratis(final DivisionAnswer onGetDivision) {
    return (RaftServer) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { RaftServer.class }, (proxy, method, args) -> {
      if ("getDivision".equals(method.getName()))
        return onGetDivision.answer(method.getName(), args);
      return null;
    });
  }

  private static RaftHAServer detachedServer(final RaftServer ratis) throws Exception {
    return detachedServer(ratis, new AtomicInteger());
  }

  /** As {@link #detachedServer(RaftServer)}, counting into {@code probes} the self-dial probes this node sends to itself. */
  private static RaftHAServer detachedServer(final RaftServer ratis, final AtomicInteger probes) throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
    final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
    final RaftHAServer raft = new RaftHAServer(arcadeServer, config);

    final Field field = RaftHAServer.class.getDeclaredField("raftServer");
    field.setAccessible(true);
    field.set(raft, ratis);
    // The stand-in Raft server has no channels: this node's own requests come back to it (issue #10299)
    raft.setSelfDialProbeTransportForTesting(SelfDialChannels.leadingBackTo(raft, probes));
    return raft;
  }
  /**
   * Issue #10144: the misdirected-request listener picked the throttling slot of an id outside the cluster by the requestor
   * id, an unauthenticated header field, so a sender that wrote this node's own id there held the slot the repair of this
   * node's own channel to a peer joined after start needed, and that repair was dropped for the whole interval. The slot is
   * now chosen by the addressed id alone, and such a peer is known to the listener from the configuration that added it.
   *
   * The Ratis division is parked until the test lets it go wherever the order in which slots are claimed matters, so both
   * refusals are taken before any recovery task can read the live configuration.
   */
  @Nested
  class SlotIgnoresRequestor {

    // None of these refusals carries a self-dial probe's nonce (issue #10299)
    private static final long NO_PROBE = MisdirectedRaftRequests.NO_PROBE_CALL_ID;
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

    // Regression for #10144: strings
    /**
     * The property, not one instance of it: whatever a stranger writes in the requestor field - nothing, a foreign id, or this
     * node's own id - its refusal does not keep the self-dial repair to a peer joined after start from being queued.
     */
    @ParameterizedTest(name = "stranger requestor = {0}")
    @ValueSource(strings = { "<none>", "foreign_peer", "<this node>" }
  )
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

    // Regression for #10144: a stranger cannot choose its slot by the requestor it writes
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

    // Regression for #10144: a peer learnt from any read of the live configuration gets A slot of its own
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

    // Regression for #10144: a removed member leaves the copy at the next live read
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

    // Regression for #10144: the state machine hands every applied configuration to the listener
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
}
