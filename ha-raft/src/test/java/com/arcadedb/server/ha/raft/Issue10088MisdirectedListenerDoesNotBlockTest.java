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
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10088: {@code MisdirectedRequestListener} runs on the gRPC handler thread of the refused request and must not
 * block, but {@link RaftHAServer#onMisdirectedRaftRequest} asked {@code isKnownPeer} before it claimed its throttle slot,
 * and that read the live Raft configuration through {@code RaftServer.getDivision}, which Ratis answers with a bare
 * {@code CompletableFuture.get()} while a division is being initialized. The same read, failing during that window,
 * logged a WARNING with a stack trace per refused heartbeat on a path designed to log once per peer per minute.
 * <p>
 * The Ratis server is a {@link Proxy} whose {@code getDivision} parks until the test lets it go, so a handler that reads it
 * never returns and the test sees that, instead of inferring it from a bytecode listing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10088MisdirectedListenerDoesNotBlockTest {

  private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
  // Every call below arrives from this host, as a request this node sent to itself does (issue #10225)
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

  /** A declared peer is known without reading anything: the handler thread must not touch the division at all. */
  @Test
  void aDeclaredPeerIsClassifiedWithoutReadingTheLiveConfiguration() throws Exception {
    final RaftHAServer raft = detachedServer(blockingRatis());

    returnsWhileTheDivisionIsParked(() -> raft.onMisdirectedRaftRequest("appendEntries", RaftPeerId.valueOf("localhost_2435"),
        raft.getLocalPeerId(), FROM_THIS_HOST));

    assertThat(raft.getMisdirectedRaftRequestsRefused()).isEqualTo(1L);
    // Neither the handler thread nor the recovery task has a reason to ask: the declared list answers. The task is waited
    // for, or the count would be read before it had a chance to ask
    Awaitility.await("the recovery task completed").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining("This node sent a Raft appendEntries meant for peer 'localhost_2435'") == 1);
    assertThat(divisionReads.get()).isZero();
  }

  /** An id nobody declared is the case the live read existed for: it must not park the handler thread either. */
  @Test
  void anUnknownPeerDoesNotParkTheHandlerThreadWhileTheDivisionIsInitializing() throws Exception {
    final RaftHAServer raft = detachedServer(blockingRatis());

    for (int i = 0; i < 5; i++) {
      final RaftPeerId stranger = RaftPeerId.valueOf("stranger_" + i);
      returnsWhileTheDivisionIsParked(() -> raft.onMisdirectedRaftRequest("requestVote", stranger, RaftPeerId.valueOf("localhost_2435"), FROM_THIS_HOST));
    }

    assertThat(raft.getMisdirectedRaftRequestsRefused()).isEqualTo(5L);
  }

  /**
   * The read moved to the recovery task is a diagnosis, not a fault: Ratis raises {@code IllegalStateException} for the
   * whole restart window by design, and one stack trace per refused heartbeat is the flood the listener's throttle exists
   * to prevent. The refusal itself is still reported, once.
   */
  @Test
  void anUnreadableConfigurationIsNotReportedWithAStackTracePerRefusal() throws Exception {
    final RaftHAServer raft = detachedServer(ratisThatCannotResolveTheDivision());

    for (int i = 0; i < 10; i++)
      raft.onMisdirectedRaftRequest("appendEntries", RaftPeerId.valueOf("stranger"), RaftPeerId.valueOf("localhost_2435"), FROM_THIS_HOST);

    Awaitility.await("the refusal is reported").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining("is not a peer of this cluster", "stranger") == 1);

    // Ten refusals share one throttled recovery task, so the read is attempted once, and it is not a WARNING
    assertThat(log.countContaining(UNREADABLE)).as("one throttled read for ten refusals, not one per refusal").isLessThanOrEqualTo(1);
    assertThat(log.formattedAt(Level.WARNING)).noneMatch(message -> message.contains(UNREADABLE));
  }

  /**
   * A peer that joined after this node started is a member of the live configuration and not of the declared list. It is
   * still told apart from a stranger, now by the recovery task: a refusal naming it says its address leads here, not that
   * it is no peer of the cluster.
   */
  @Test
  void aPeerOnlyTheLiveConfigurationKnowsIsStillTreatedAsAMember() throws Exception {
    final RaftPeer joined = RaftPeer.newBuilder().setId("joined_peer").setAddress("localhost:2439").build();
    final RaftHAServer raft = detachedServer(ratisWithLivePeers(List.of(joined)));

    raft.onMisdirectedRaftRequest("appendEntries", joined.getId(), RaftPeerId.valueOf("localhost_2435"), FROM_THIS_HOST);

    Awaitility.await("the refusal is reported").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining("Refused a Raft appendEntries", "addressed to peer 'joined_peer'") == 1);
    assertThat(log.countFormattedContaining("is not a peer of this cluster", "joined_peer")).isZero();
  }

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
        () -> raft.onMisdirectedRaftRequest("requestVote", RaftPeerId.valueOf("stranger"), RaftPeerId.valueOf("localhost_2435"), FROM_THIS_HOST));
    returnsWhileTheDivisionIsParked(() -> raft.onMisdirectedRaftRequest("appendEntries", joined.getId(), raft.getLocalPeerId(), FROM_THIS_HOST));
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
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
    final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
    final RaftHAServer raft = new RaftHAServer(arcadeServer, config);

    final Field field = RaftHAServer.class.getDeclaredField("raftServer");
    field.setAccessible(true);
    field.set(raft, ratis);
    return raft;
  }
}
