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
import com.arcadedb.server.ha.raft.RaftHAServer.SelfDialProbeOutcome;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10299: since #10225 a refused Raft request that named this node as its sender reset this node's channel to the
 * addressed member when it came from an address of this host. A source address names a host, not a node, so the check
 * failed both ways: a process on this host, or anything a local proxy or service-mesh sidecar relays, still forced the
 * reset, while this node's own request that crossed a hop rewriting its source (hairpin NAT, a published Docker port, a
 * TCP proxy) arrived from a foreign address and its self-dial was no longer repaired. A claim of this node's id now only
 * queues a probe over this node's own channel to that member, and the channel is reset only when the probe comes back.
 */
class Issue10299SelfDialIsConfirmedByAProbeTest {

  private static final String     SERVER_LIST  = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
  private static final RaftPeerId MEMBER       = RaftPeerId.valueOf("localhost_2435");
  private static final RaftPeerId OTHER_MEMBER = RaftPeerId.valueOf("localhost_2436");
  private static final String     SELF_DIAL    = "This node sent a Raft appendEntries meant for peer 'localhost_2435'";
  private static final String     CLAIM        = "names this node as its sender";

  private CapturingTestLogger log;

  @BeforeEach
  void installLogger() {
    log = CapturingTestLogger.install();
  }

  @AfterEach
  void restore() {
    log.uninstall();
  }

  /**
   * Shape 1 of the issue: a claim of this node's id resets nothing when this node's channel reaches the member, wherever
   * the claim came from - this host's loopback (another local process, a sidecar, a Docker userland proxy), one of its
   * interfaces, another host, or nowhere gRPC could say.
   */
  @ParameterizedTest(name = "source = {0}")
  @ValueSource(strings = { "loopback", "interface", "192.0.2.10", "<none>", "<unresolved>" })
  void aClaimOfThisNodesIdIsNoSelfDialWhenTheChannelReachesTheMember(final String source) throws Exception {
    final SocketAddress remote = source(source);
    final RaftHAServer raft = detachedServer();
    final AtomicInteger probes = SelfDialChannels.reachTheirMembers(raft);

    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), remote, RaftHAServer.NO_PROBE_CALL_ID);

    Awaitility.await("the claim is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(CLAIM, "was answered by that peer") == 1);
    assertThat(log.countFormattedContaining(SELF_DIAL)).as("the member answered, so the request was not this node's").isZero();
    assertThat(probes.get()).isEqualTo(1);
    assertThat(raft.getMisdirectedSelfClaims()).isEqualTo(1L);
    assertThat(raft.getSelfDialProbesUnconfirmed()).isEqualTo(1L);
    assertThat(raft.getSelfDialProbesAnsweredByMember()).as("counted, so an operator can spot it without the log").isEqualTo(1L);
    assertThat(raft.getMisdirectedChannelResets()).isZero();
  }

  /**
   * Shape 2 of the issue: this node's own request is repaired whatever address it arrives from - the loopback and this
   * node's own interface as before #10225, and now a foreign address too, which is what a source-rewriting hop shows.
   */
  @ParameterizedTest(name = "source = {0}")
  @ValueSource(strings = { "loopback", "interface", "192.0.2.10", "<none>", "<unresolved>" })
  void aSelfDialIsRepairedWhateverAddressItArrivesFrom(final String source) throws Exception {
    final SocketAddress remote = source(source);
    final RaftHAServer raft = detachedServer();
    SelfDialChannels.leadBackToItself(raft);

    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), remote, RaftHAServer.NO_PROBE_CALL_ID);

    Awaitility.await("the self-dialled channel is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(SELF_DIAL) == 1);
    assertThat(log.formattedAt(Level.WARNING)).as("the repair is the line the operator needs").anyMatch(m -> m.contains(SELF_DIAL));
    assertThat(log.countFormattedContaining(CLAIM)).isZero();
    assertThat(raft.getSelfDialProbesUnconfirmed()).isZero();
    assertThat(raft.getMisdirectedSelfClaims()).as("the probe that came back is not a claim of its own").isEqualTo(1L);
  }

  /** A probe that fails proves nothing about where the channel leads, so nothing is reset. */
  @Test
  void aProbeThatFailsConfirmsNothing() {
    final RaftHAServer raft = detachedServer();
    raft.setSelfDialProbeTransportForTesting(probe -> {
      throw new IOException("UNAVAILABLE: io exception");
    });

    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), null, RaftHAServer.NO_PROBE_CALL_ID);

    Awaitility.await("the claim is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(CLAIM, "did not come back to this node") == 1);
    assertThat(log.countFormattedContaining(SELF_DIAL)).isZero();
    assertThat(log.countFormattedContaining("cannot be confirmed as this node's own request")).isEqualTo(1);
    assertThat(raft.getSelfDialProbesUnconfirmed()).isEqualTo(1L);
    assertThat(raft.getSelfDialProbesAnsweredByMember()).as("a failure is no proof of an impostor").isZero();
  }

  /** Without a Raft server there is no channel to probe, and the claim is reported as unconfirmed. */
  @Test
  void aClaimThatCannotBeProbedRepairsNothing() {
    final RaftHAServer raft = detachedServer();

    assertThat(raft.probeSelfDial(MEMBER)).isEqualTo(SelfDialProbeOutcome.NOT_SENT);
    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(),
        new InetSocketAddress(InetAddress.getLoopbackAddress(), 51_000), RaftHAServer.NO_PROBE_CALL_ID);

    Awaitility.await("the claim is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(CLAIM, "could not be probed") == 1);
    assertThat(log.countFormattedContaining(SELF_DIAL)).isZero();
  }

  /**
   * The probe is a request no peer acts on - a pre-vote for term 0, whose reply is dropped - addressed exactly as the
   * request it checks, from this node, in this node's group, with an unpredictable call id that differs every time.
   */
  @Test
  void theProbeIsAPreVoteForTermZeroCarryingAFreshNonce() {
    final RaftHAServer raft = detachedServer();
    final List<RequestVoteRequestProto> sent = new ArrayList<>();
    raft.setSelfDialProbeTransportForTesting(probe -> {
      sent.add(probe);
      return null;
    });

    assertThat(raft.probeSelfDial(MEMBER)).as("no answer at all is no proof either way").isEqualTo(SelfDialProbeOutcome.ELSEWHERE);
    raft.probeSelfDial(MEMBER);

    assertThat(sent).hasSize(2);
    final RequestVoteRequestProto probe = sent.getFirst();
    assertThat(probe.getPreVote()).isTrue();
    assertThat(probe.getCandidateTerm()).isZero();
    assertThat(RaftPeerId.valueOf(probe.getServerRequest().getRequestorId())).isEqualTo(raft.getLocalPeerId());
    assertThat(RaftPeerId.valueOf(probe.getServerRequest().getReplyId())).isEqualTo(MEMBER);
    assertThat(probe.getServerRequest().getRaftGroupId().getId()).isEqualTo(raft.getRaftGroup().getGroupId().toByteString());
    assertThat(probe.getServerRequest().getCallId()).isNotEqualTo(RaftHAServer.NO_PROBE_CALL_ID);
    assertThat(sent.get(1).getServerRequest().getCallId()).isNotEqualTo(probe.getServerRequest().getCallId());
  }

  /**
   * A nonce confirms only the probe it belongs to, while it is in flight: the same nonce addressed to another member, or
   * replayed once its probe has returned, is a claim like any other and confirms nothing.
   */
  @Test
  void aNonceConfirmsOnlyItsOwnProbeWhileItIsInFlight() throws Exception {
    final RaftHAServer raft = detachedServer();
    final List<Long> nonces = new ArrayList<>();
    final RaftHAServer.SelfDialProbeTransport member = probe -> {
      final long nonce = probe.getServerRequest().getCallId();
      nonces.add(nonce);
      // While the probe is in flight, a request for another member carrying its nonce
      raft.onMisdirectedRaftRequest("requestVote", OTHER_MEMBER, raft.getLocalPeerId(), null, nonce);
      return SelfDialChannels.answeredByTheMember(probe);
    };
    raft.setSelfDialProbeTransportForTesting(member);

    assertThat(raft.probeSelfDial(MEMBER)).isEqualTo(SelfDialProbeOutcome.MEMBER);
    assertThat(raft.getMisdirectedSelfClaims()).as("the nonce addressed to another member was taken for a claim").isEqualTo(1L);

    // Replayed after its probe returned
    raft.onMisdirectedRaftRequest("requestVote", MEMBER, raft.getLocalPeerId(), null, nonces.getFirst());
    assertThat(raft.getMisdirectedSelfClaims()).as("a spent nonce is a claim like any other").isEqualTo(2L);
  }

  /** Each outcome of the probe itself, through the transport seam. */
  @Test
  void eachProbeOutcomeIsToldApart() {
    final RaftHAServer raft = detachedServer();
    assertThat(raft.probeSelfDial(MEMBER)).as("no Raft server RPC").isEqualTo(SelfDialProbeOutcome.NOT_SENT);

    SelfDialChannels.leadBackToItself(raft);
    assertThat(raft.probeSelfDial(MEMBER)).isEqualTo(SelfDialProbeOutcome.SELF);

    SelfDialChannels.reachTheirMembers(raft);
    assertThat(raft.probeSelfDial(MEMBER)).isEqualTo(SelfDialProbeOutcome.MEMBER);

    // Answered under another id: the channel leads to somebody else, which proves no self-dial
    raft.setSelfDialProbeTransportForTesting(probe -> SelfDialChannels.answeredByTheMember(probe.toBuilder()
        .setServerRequest(probe.getServerRequest().toBuilder().setReplyId(OTHER_MEMBER.toByteString())).build()));
    assertThat(raft.probeSelfDial(MEMBER)).isEqualTo(SelfDialProbeOutcome.ELSEWHERE);

    // A probe that times out, as one to an unresponsive member does after the Raft request timeout
    raft.setSelfDialProbeTransportForTesting(probe -> {
      throw new StatusRuntimeException(Status.DEADLINE_EXCEEDED);
    });
    assertThat(raft.probeSelfDial(MEMBER)).isEqualTo(SelfDialProbeOutcome.ELSEWHERE);
    assertThat(raft.getMisdirectedChannelResets()).isZero();
  }

  /** A claim with no call id is never taken for a probe, even while one is in flight for the same member. */
  @Test
  void aClaimWithoutACallIdIsNeverAProbe() {
    final RaftHAServer raft = detachedServer();
    raft.setSelfDialProbeTransportForTesting(probe -> {
      raft.onMisdirectedRaftRequest("requestVote", MEMBER, raft.getLocalPeerId(), null, RaftHAServer.NO_PROBE_CALL_ID);
      return SelfDialChannels.answeredByTheMember(probe);
    });

    assertThat(raft.probeSelfDial(MEMBER)).isEqualTo(SelfDialProbeOutcome.MEMBER);
    assertThat(raft.getMisdirectedSelfClaims()).isEqualTo(1L);
  }

  /**
   * The review's main point: a probe waits for up to the Raft request timeout when the member does not answer, and any
   * sender can cause one. It runs on a worker of its own, so a peer's report - and with it every channel reset and
   * leadership hand-off the channel-recovery worker runs - goes on while a probe is stuck.
   */
  @Test
  void aStuckProbeDoesNotHoldBackTheChannelRecoveryWorker() throws Exception {
    final RaftHAServer raft = detachedServer();
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch probing = new CountDownLatch(1);
    raft.setSelfDialProbeTransportForTesting(probe -> {
      probing.countDown();
      release.await();
      return SelfDialChannels.answeredByTheMember(probe);
    });
    try {
      raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), null, RaftHAServer.NO_PROBE_CALL_ID);
      assertThat(probing.await(10, TimeUnit.SECONDS)).isTrue();

      raft.onMisdirectedRaftRequest("appendEntries", OTHER_MEMBER, MEMBER, null, RaftHAServer.NO_PROBE_CALL_ID);
      Awaitility.await("the peer's report is not queued behind the probe").atMost(10, TimeUnit.SECONDS)
          .until(() -> log.countFormattedContaining("from peer 'localhost_2435' addressed to peer 'localhost_2436'") == 1);
      assertThat(raft.getSelfDialProbePoolStats().activeThreads()).as("the probe is still waiting").isEqualTo(1);
    } finally {
      release.countDown();
    }
    Awaitility.await("the released probe is reported").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(CLAIM, "was answered by that peer") == 1);
  }

  /**
   * A flood of claims of this node's id cannot hold the slot, nor the WARNING, of a real report about the member they
   * address: a peer whose address for that member leads here is still reported at WARNING.
   */
  @Test
  void claimsOfThisNodesIdDoNotSilenceARealReportAboutTheSameMember() throws Exception {
    final RaftHAServer raft = detachedServer();
    SelfDialChannels.reachTheirMembers(raft);
    final InetSocketAddress foreign = new InetSocketAddress(InetAddress.getByName("192.0.2.10"), 51_000);

    for (int i = 0; i < 5; i++)
      raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), foreign, RaftHAServer.NO_PROBE_CALL_ID);
    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, OTHER_MEMBER,
        new InetSocketAddress(InetAddress.getByName("192.0.2.11"), 51_000), RaftHAServer.NO_PROBE_CALL_ID);

    final String report = "from peer 'localhost_2436' addressed to peer 'localhost_2435'";
    Awaitility.await("the peer's report is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(report) == 1);
    assertThat(log.formattedAt(Level.WARNING)).anyMatch(m -> m.contains(report));
    assertThat(raft.getMisdirectedSelfClaims()).isEqualTo(5L);
  }

  /**
   * Whoever takes the slot, the probe tests the real channel: a stranger's claim of this node's id that comes first cannot
   * keep this node's own looping channel from being repaired - and reported at WARNING. Before the fix a claim from this
   * host took the slot the repair needed with no probe at all.
   */
  @Test
  void aClaimThatTakesTheSlotFirstCannotHoldBackTheRepair() throws Exception {
    final RaftHAServer raft = detachedServer();
    final AtomicInteger probes = SelfDialChannels.leadBackToItself(raft);

    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(),
        new InetSocketAddress(InetAddress.getByName("192.0.2.10"), 51_000), RaftHAServer.NO_PROBE_CALL_ID);
    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(),
        new InetSocketAddress(InetAddress.getLoopbackAddress(), 51_000), RaftHAServer.NO_PROBE_CALL_ID);

    Awaitility.await("the self-dialled channel is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(SELF_DIAL) == 1);
    assertThat(log.formattedAt(Level.WARNING)).anyMatch(m -> m.contains(SELF_DIAL));
    assertThat(probes.get()).as("one probe per member and interval").isEqualTo(1);
  }

  /** A request that names another peer as its sender is reported as before, and probes nothing: it is not this node's. */
  @Test
  void aRequestFromAnotherPeerProbesNothing() {
    final RaftHAServer raft = detachedServer();
    final AtomicInteger probes = SelfDialChannels.leadBackToItself(raft);

    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, OTHER_MEMBER, null, RaftHAServer.NO_PROBE_CALL_ID);

    Awaitility.await("the peer's report is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining("from peer 'localhost_2436' addressed to peer 'localhost_2435'") == 1);
    assertThat(probes.get()).isZero();
    assertThat(raft.getMisdirectedSelfClaims()).isZero();
  }

  private static SocketAddress source(final String source) throws Exception {
    return switch (source) {
      case "<none>" -> null;
      case "<unresolved>" -> InetSocketAddress.createUnresolved("arcadedb-0", 2434);
      case "loopback" -> new InetSocketAddress(InetAddress.getLoopbackAddress(), 51_000);
      case "interface" -> {
        final InetAddress own = firstNonLoopbackInterfaceAddress();
        yield new InetSocketAddress(own != null ? own : InetAddress.getLoopbackAddress(), 51_000);
      }
      default -> new InetSocketAddress(InetAddress.getByName(source), 51_000);
    };
  }

  private static InetAddress firstNonLoopbackInterfaceAddress() throws Exception {
    for (final NetworkInterface nic : Collections.list(NetworkInterface.getNetworkInterfaces())) {
      if (!nic.isUp() || nic.isLoopback())
        continue;
      for (final InetAddress address : Collections.list(nic.getInetAddresses()))
        if (address instanceof Inet4Address)
          return address;
    }
    return null;
  }

  private static RaftHAServer detachedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
    final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
    return new RaftHAServer(arcadeServer, config);
  }
}
