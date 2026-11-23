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
import com.arcadedb.server.ha.raft.RaftHAServer.SelfDialProbeChannels;
import com.arcadedb.server.ha.raft.RaftHAServer.SelfDialProbeOutcome;
import com.arcadedb.server.ha.raft.ratis.RaftPeerChannels;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.AbstractStub;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.ratis.util.TimeDuration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Detecting that a Raft channel dials this very node: the probe that confirms the self-dial and the channels
 * it covers.
 */
class PeerSelfDialProbeTest {

  /**
   * Issue #10299: since #10225 a refused Raft request that named this node as its sender reset this node's channel to the
   * addressed member when it came from an address of this host. A source address names a host, not a node, so the check
   * failed both ways: a process on this host, or anything a local proxy or service-mesh sidecar relays, still forced the
   * reset, while this node's own request that crossed a hop rewriting its source (hairpin NAT, a published Docker port, a
   * TCP proxy) arrived from a foreign address and its self-dial was no longer repaired. A claim of this node's id now only
   * queues a probe over this node's own channel to that member, and the channel is reset only when the probe comes back.
   */
  @Nested
  class SelfDialConfirmedByProbe {

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
    // Regression for #10299: a claim of this nodes id is no self dial when the channel reaches the member
    @ParameterizedTest(name = "source = {0}")
    @ValueSource(strings = { "loopback", "interface", "192.0.2.10", "<none>", "<unresolved>" })
    void aClaimOfThisNodesIdIsNoSelfDialWhenTheChannelReachesTheMember(final String source) throws Exception {
      final SocketAddress remote = source(source);
      final RaftHAServer raft = detachedServer();
      final AtomicInteger probes = SelfDialChannels.reachTheirMembers(raft);

      raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), remote, MisdirectedRaftRequests.NO_PROBE_CALL_ID);

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
    // Regression for #10299: a self dial is repaired whatever address it arrives from
    @ParameterizedTest(name = "source = {0}")
    @ValueSource(strings = { "loopback", "interface", "192.0.2.10", "<none>", "<unresolved>" })
    void aSelfDialIsRepairedWhateverAddressItArrivesFrom(final String source) throws Exception {
      final SocketAddress remote = source(source);
      final RaftHAServer raft = detachedServer();
      SelfDialChannels.leadBackToItself(raft);

      raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), remote, MisdirectedRaftRequests.NO_PROBE_CALL_ID);

      Awaitility.await("the self-dialled channel is looked at").atMost(10, TimeUnit.SECONDS)
          .until(() -> log.countFormattedContaining(SELF_DIAL) == 1);
      assertThat(log.formattedAt(Level.WARNING)).as("the repair is the line the operator needs").anyMatch(m -> m.contains(SELF_DIAL));
      assertThat(log.countFormattedContaining(CLAIM)).isZero();
      assertThat(raft.getSelfDialProbesUnconfirmed()).isZero();
      assertThat(raft.getMisdirectedSelfClaims()).as("the probe that came back is not a claim of its own").isEqualTo(1L);
    }

    /** A probe that fails proves nothing about where the channel leads, so nothing is reset. */
    // Regression for #10299: a probe that fails confirms nothing
    @Test
    void aProbeThatFailsConfirmsNothing() {
      final RaftHAServer raft = detachedServer();
      raft.setSelfDialProbeTransportForTesting(probe -> {
        throw new IOException("UNAVAILABLE: io exception");
      });

      raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), null, MisdirectedRaftRequests.NO_PROBE_CALL_ID);

      Awaitility.await("the claim is looked at").atMost(10, TimeUnit.SECONDS)
          .until(() -> log.countFormattedContaining(CLAIM, "did not come back to this node") == 1);
      assertThat(log.countFormattedContaining(SELF_DIAL)).isZero();
      assertThat(log.countFormattedContaining("cannot be confirmed as this node's own request")).isEqualTo(1);
      assertThat(raft.getSelfDialProbesUnconfirmed()).isEqualTo(1L);
      assertThat(raft.getSelfDialProbesAnsweredByMember()).as("a failure is no proof of an impostor").isZero();
    }

    /** Without a Raft server there is no channel to probe, and the claim is reported as unconfirmed. */
    // Regression for #10299: a claim that cannot be probed repairs nothing
    @Test
    void aClaimThatCannotBeProbedRepairsNothing() {
      final RaftHAServer raft = detachedServer();

      assertThat(raft.probeSelfDial(MEMBER)).isEqualTo(SelfDialProbeOutcome.NOT_SENT);
      raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(),
          new InetSocketAddress(InetAddress.getLoopbackAddress(), 51_000), MisdirectedRaftRequests.NO_PROBE_CALL_ID);

      Awaitility.await("the claim is looked at").atMost(10, TimeUnit.SECONDS)
          .until(() -> log.countFormattedContaining(CLAIM, "could not be probed") == 1);
      assertThat(log.countFormattedContaining(SELF_DIAL)).isZero();
    }

    /**
     * The probe is a request no peer acts on - a pre-vote for term 0, whose reply is dropped - addressed exactly as the
     * request it checks, from this node, in this node's group, with an unpredictable call id that differs every time.
     */
    // Regression for #10299: the probe is a pre vote for term zero carrying a fresh nonce
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
      assertThat(probe.getServerRequest().getCallId()).isNotEqualTo(MisdirectedRaftRequests.NO_PROBE_CALL_ID);
      assertThat(sent.get(1).getServerRequest().getCallId()).isNotEqualTo(probe.getServerRequest().getCallId());
    }

    /**
     * A nonce confirms only the probe it belongs to, while it is in flight: the same nonce addressed to another member, or
     * replayed once its probe has returned, is a claim like any other and confirms nothing.
     */
    // Regression for #10299: a nonce confirms only its own probe while it is in flight
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
    // Regression for #10299: each probe outcome is told apart
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
    // Regression for #10299: a claim without a call id is never a probe
    @Test
    void aClaimWithoutACallIdIsNeverAProbe() {
      final RaftHAServer raft = detachedServer();
      raft.setSelfDialProbeTransportForTesting(probe -> {
        raft.onMisdirectedRaftRequest("requestVote", MEMBER, raft.getLocalPeerId(), null, MisdirectedRaftRequests.NO_PROBE_CALL_ID);
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
    // Regression for #10299: a stuck probe does not hold back the channel recovery worker
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
        raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), null, MisdirectedRaftRequests.NO_PROBE_CALL_ID);
        assertThat(probing.await(10, TimeUnit.SECONDS)).isTrue();

        raft.onMisdirectedRaftRequest("appendEntries", OTHER_MEMBER, MEMBER, null, MisdirectedRaftRequests.NO_PROBE_CALL_ID);
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
    // Regression for #10299: claims of this nodes id do not silence a real report about the same member
    @Test
    void claimsOfThisNodesIdDoNotSilenceARealReportAboutTheSameMember() throws Exception {
      final RaftHAServer raft = detachedServer();
      SelfDialChannels.reachTheirMembers(raft);
      final InetSocketAddress foreign = new InetSocketAddress(InetAddress.getByName("192.0.2.10"), 51_000);

      for (int i = 0; i < 5; i++)
        raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), foreign, MisdirectedRaftRequests.NO_PROBE_CALL_ID);
      raft.onMisdirectedRaftRequest("appendEntries", MEMBER, OTHER_MEMBER,
          new InetSocketAddress(InetAddress.getByName("192.0.2.11"), 51_000), MisdirectedRaftRequests.NO_PROBE_CALL_ID);

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
    // Regression for #10299: a claim that takes the slot first cannot hold back the repair
    @Test
    void aClaimThatTakesTheSlotFirstCannotHoldBackTheRepair() throws Exception {
      final RaftHAServer raft = detachedServer();
      final AtomicInteger probes = SelfDialChannels.leadBackToItself(raft);

      raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(),
          new InetSocketAddress(InetAddress.getByName("192.0.2.10"), 51_000), MisdirectedRaftRequests.NO_PROBE_CALL_ID);
      raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(),
          new InetSocketAddress(InetAddress.getLoopbackAddress(), 51_000), MisdirectedRaftRequests.NO_PROBE_CALL_ID);

      Awaitility.await("the self-dialled channel is looked at").atMost(10, TimeUnit.SECONDS)
          .until(() -> log.countFormattedContaining(SELF_DIAL) == 1);
      assertThat(log.formattedAt(Level.WARNING)).anyMatch(m -> m.contains(SELF_DIAL));
      assertThat(probes.get()).as("one probe per member and interval").isEqualTo(1);
    }

    /** A request that names another peer as its sender is reported as before, and probes nothing: it is not this node's. */
    // Regression for #10299: a request from another peer probes nothing
    @Test
    void aRequestFromAnotherPeerProbesNothing() {
      final RaftHAServer raft = detachedServer();
      final AtomicInteger probes = SelfDialChannels.leadBackToItself(raft);

      raft.onMisdirectedRaftRequest("appendEntries", MEMBER, OTHER_MEMBER, null, MisdirectedRaftRequests.NO_PROBE_CALL_ID);

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

  /**
   * Issue #10374: the self-dial probe went over the primary Raft gRPC channel only, while Ratis holds a second, independent
   * channel per peer for the heartbeat {@code appendEntries} stream (on by default, never turned off here). The misdirected
   * requests are heartbeats, a busy heartbeat channel never resolves the peer's name again, and the idle primary one does:
   * the probe reached the member, was reported as somebody else's claim, and the self-dialled heartbeat channel was never
   * repaired. A probe now goes over every connection Ratis holds for the member, and the member is credited only when it
   * answered over all of them.
   * <p>
   * The tests that build a {@code GrpcServerProtocolClient} read its private fields ({@code channel}, {@code hbChannel},
   * {@code pool}) and call {@code GrpcStubPool.acquire()} by name, as written against <b>Ratis 3.3.1</b>. A
   * {@code NoSuchFieldException} or {@code NoSuchMethodException} here after a Ratis upgrade means those internals moved:
   * re-read {@code GrpcServerProtocolClient} and check that {@link RaftPeerChannels} still finds every channel.
   */
  @Nested
  class ProbeCoversEveryChannel {

    private static final String     SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
    private static final RaftPeerId MEMBER      = RaftPeerId.valueOf("localhost_2435");
    private static final String     SELF_DIAL   = "This node sent a Raft appendEntries meant for peer 'localhost_2435'";
    private static final String     CLAIM       = "names this node as its sender";

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
     * The premise, readable without a cluster: the properties this server gives Ratis keep the separate heartbeat channel on
     * and a stub pool of one, so a peer's proxy holds two connections - which the census finds, and finds complete.
     */
    // Regression for #10374: the properties this server builds give each peer two channels and the census finds both
    @Test
    void thePropertiesThisServerBuildsGiveEachPeerTwoChannelsAndTheCensusFindsBoth() throws Exception {
      final RaftProperties properties = RaftPropertiesBuilder.build(new ContextConfiguration());
      assertThat(GrpcConfigKeys.Server.heartbeatChannel(properties)).as("Ratis's separate heartbeat channel").isTrue();
      assertThat(GrpcConfigKeys.Server.stubPoolSize(properties)).isEqualTo(1);

      final Object client = protocolClient(GrpcConfigKeys.Server.heartbeatChannel(properties),
          GrpcConfigKeys.Server.stubPoolSize(properties));
      try {
        final RaftPeerChannels.Census census = RaftPeerChannels.of(client);
        assertThat(census.channels()).as("the primary channel and the heartbeat channel").hasSize(2);
        assertThat(census.channels().get(0)).isNotSameAs(census.channels().get(1));
        assertThat(census.channels()).contains((ManagedChannel) field(client, "channel"), (ManagedChannel) field(client, "hbChannel"));
        assertThat(census.complete()).isTrue();
        assertThat(census.unreadable()).as("reflection read the proxy").isNull();
      } finally {
        ((Closeable) client).close();
      }
    }

    /** Without the separate heartbeat channel a peer has one connection, and the census says so. */
    // Regression for #10374: without the heartbeat channel there is one connection
    @Test
    void withoutTheHeartbeatChannelThereIsOneConnection() throws Exception {
      final Object client = protocolClient(false, 1);
      try {
        final RaftPeerChannels.Census census = RaftPeerChannels.of(client);
        assertThat(census.channels()).containsExactly((ManagedChannel) field(client, "channel"));
        assertThat(census.complete()).isTrue();
      } finally {
        ((Closeable) client).close();
      }
    }

    /**
     * The {@code readIndex} pool, when configured: a pooled channel Ratis has built is listed, one it has not built is no
     * connection and is not built by the census either.
     */
    // Regression for #10374: a pooled channel is listed once ratis has built it
    @Test
    void aPooledChannelIsListedOnceRatisHasBuiltIt() throws Exception {
      final Object client = protocolClient(true, 3);
      try {
        assertThat(RaftPeerChannels.of(client).channels()).as("no pooled channel built yet").hasSize(2);
        assertThat(RaftPeerChannels.of(client).complete()).isTrue();

        final Object pool = field(client, "pool");
        final Method acquire = pool.getClass().getDeclaredMethod("acquire");
        acquire.setAccessible(true);
        acquire.invoke(pool);

        final RaftPeerChannels.Census census = RaftPeerChannels.of(client);
        assertThat(census.channels()).as("primary, heartbeat and the pooled channel just built").hasSize(3);
        assertThat(census.complete()).isTrue();
      } finally {
        ((Closeable) client).close();
      }
    }

    /** What the census cannot account for makes it incomplete, so the probe never treats it as covering everything. */
    // Regression for #10374: what the census cannot account for makes it incomplete
    @Test
    void whatTheCensusCannotAccountForMakesItIncomplete() {
      final ManagedChannel listed = NettyChannelBuilder.forTarget("127.0.0.1:1").usePlaintext().build();
      final ManagedChannel unlisted = NettyChannelBuilder.forTarget("127.0.0.1:1").usePlaintext().build();
      try {
        assertThat(RaftPeerChannels.of(new WithAStubOverAnUnlistedChannel(listed, unlisted)).complete())
            .as("a stub over a channel that was not found").isFalse();
        assertThat(RaftPeerChannels.of(new WithAMap(listed)).complete()).as("a container the walk does not look into").isFalse();
        assertThat(RaftPeerChannels.of(new Object()).complete()).as("no channel at all").isFalse();
        assertThat(RaftPeerChannels.of(null).complete()).isFalse();
      } finally {
        listed.shutdownNow();
        unlisted.shutdownNow();
      }
    }

    /** A collection that holds itself ends the walk instead of overflowing the stack. */
    // Regression for #10374: a cycle through a collection ends the walk
    @Test
    void aCycleThroughACollectionEndsTheWalk() {
      final ManagedChannel channel = NettyChannelBuilder.forTarget("127.0.0.1:1").usePlaintext().build();
      try {
        final RaftPeerChannels.Census census = RaftPeerChannels.of(new WithACycle(channel));
        assertThat(census.channels()).containsExactly(channel);
        assertThat(census.complete()).isTrue();
      } finally {
        channel.shutdownNow();
      }
    }

    /**
     * The issue's repro, at the probe: the primary channel reaches the member and the heartbeat channel leads back to this
     * node. Before the fix the probe tested the first only and reported somebody else's claim; now the self-dial is
     * confirmed and repaired.
     */
    // Regression for #10374: a heartbeat channel that leads back is found even when the primary one reaches the member
    @Test
    void aHeartbeatChannelThatLeadsBackIsFoundEvenWhenThePrimaryOneReachesTheMember() {
      final RaftHAServer raft = detachedServer();
      final AtomicInteger primary = new AtomicInteger();
      final AtomicInteger heartbeat = new AtomicInteger();
      raft.setSelfDialProbeChannelsForTesting(new SelfDialProbeChannels(
          List.of(SelfDialChannels.reachingTheMember(primary), SelfDialChannels.leadingBackTo(raft, heartbeat)), true));

      assertThat(raft.probeSelfDial(MEMBER)).isEqualTo(SelfDialProbeOutcome.SELF);
      assertThat(primary.get()).isEqualTo(1);
      assertThat(heartbeat.get()).isEqualTo(1);

      raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), null, MisdirectedRaftRequests.NO_PROBE_CALL_ID);

      Awaitility.await("the self-dialled channel is looked at").atMost(10, TimeUnit.SECONDS)
          .until(() -> log.countFormattedContaining(SELF_DIAL) == 1);
      assertThat(log.formattedAt(Level.WARNING)).anyMatch(m -> m.contains(SELF_DIAL));
      assertThat(log.countFormattedContaining(CLAIM)).as("not reported as somebody else's claim").isZero();
      assertThat(raft.getSelfDialProbesAnsweredByMember()).isZero();
      assertThat(raft.getSelfDialProbesUnconfirmed()).isZero();
    }

    /** A connection that fails first does not hide one that comes back after it: the self-dial still wins. */
    // Regression for #10374: a failed connection before one that comes back still confirms the self dial
    @Test
    void aFailedConnectionBeforeOneThatComesBackStillConfirmsTheSelfDial() {
      final RaftHAServer raft = detachedServer();
      final AtomicInteger loop = new AtomicInteger();
      raft.setSelfDialProbeChannelsForTesting(new SelfDialProbeChannels(List.of(probe -> {
        throw new IOException("DEADLINE_EXCEEDED");
      }, SelfDialChannels.leadingBackTo(raft, loop)), true));

      assertThat(raft.probeSelfDial(MEMBER)).isEqualTo(SelfDialProbeOutcome.SELF);
      assertThat(loop.get()).isEqualTo(1);
    }

    /** The member is credited only when it answered over every connection, and then the claim is somebody else's. */
    // Regression for #10374: the member is credited only when it answered over every connection
    @Test
    void theMemberIsCreditedOnlyWhenItAnsweredOverEveryConnection() {
      final RaftHAServer raft = detachedServer();
      final AtomicInteger probes = new AtomicInteger();
      raft.setSelfDialProbeChannelsForTesting(new SelfDialProbeChannels(
          List.of(SelfDialChannels.reachingTheMember(probes), SelfDialChannels.reachingTheMember(probes)), true));
      assertThat(raft.probeSelfDial(MEMBER)).isEqualTo(SelfDialProbeOutcome.MEMBER);
      assertThat(probes.get()).as("one probe per connection").isEqualTo(2);

      // One connection fails: no proof either way
      raft.setSelfDialProbeChannelsForTesting(new SelfDialProbeChannels(List.of(SelfDialChannels.reachingTheMember(probes),
          probe -> {
            throw new IOException("UNAVAILABLE: io exception");
          }), true));
      assertThat(raft.probeSelfDial(MEMBER)).isEqualTo(SelfDialProbeOutcome.ELSEWHERE);
    }

    /**
     * When the connections could not all be listed, the member's answer over the ones that were is inconclusive: neither
     * credited as proof of an impostor nor taken for a self-dial, and the operator is not sent after a foreign process.
     */
    // Regression for #10374: an incomplete census answered by the member is inconclusive
    @Test
    void anIncompleteCensusAnsweredByTheMemberIsInconclusive() {
      final RaftHAServer raft = detachedServer();
      raft.setSelfDialProbeChannelsForTesting(
          new SelfDialProbeChannels(List.of(SelfDialChannels.reachingTheMember(new AtomicInteger())), false));

      assertThat(raft.probeSelfDial(MEMBER)).isEqualTo(SelfDialProbeOutcome.MEMBER_ON_SOME_CHANNELS);

      raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), null, MisdirectedRaftRequests.NO_PROBE_CALL_ID);
      Awaitility.await("the claim is looked at").atMost(10, TimeUnit.SECONDS)
          .until(() -> log.countFormattedContaining(CLAIM, "issue #10374") == 1);
      assertThat(log.countFormattedContaining(CLAIM, "neither confirmed nor ruled out")).isEqualTo(1);
      assertThat(log.countFormattedContaining("Check which process or cluster connects")).isZero();
      assertThat(log.countFormattedContaining(SELF_DIAL)).isZero();
      assertThat(raft.getSelfDialProbesUnconfirmed()).isEqualTo(1L);
      assertThat(raft.getSelfDialProbesAnsweredByMember()).isZero();
      assertThat(raft.getMisdirectedChannelResets()).isZero();
    }

    /** A Ratis {@code GrpcServerProtocolClient}, as {@code GrpcServicesImpl} builds one for a peer. */
    private static Object protocolClient(final boolean separateHeartbeatChannel, final int connections) throws Exception {
      final Class<?> type = Class.forName("org.apache.ratis.grpc.server.GrpcServerProtocolClient");
      final Constructor<?> constructor = type.getDeclaredConstructor(RaftPeer.class, int.class, int.class, TimeDuration.class,
          SslContext.class, boolean.class, EventLoopGroup.class);
      constructor.setAccessible(true);
      // Port 1 on the loopback: nothing connects there, and nothing here needs to
      final RaftPeer peer = RaftPeer.newBuilder().setId(MEMBER).setAddress("127.0.0.1:1").build();
      return constructor.newInstance(peer, connections, 1 << 20, TimeDuration.valueOf(10, TimeUnit.SECONDS), null,
          separateHeartbeatChannel, null);
    }

    private static Object field(final Object owner, final String name) throws Exception {
      final Field field = owner.getClass().getDeclaredField(name);
      field.setAccessible(true);
      return field.get(owner);
    }

    private static RaftHAServer detachedServer() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
      return new RaftHAServer(arcadeServer, config);
    }

    private static final class WithAStubOverAnUnlistedChannel {
      private final ManagedChannel  channel;
      private final AbstractStub<?> stub;

      private WithAStubOverAnUnlistedChannel(final ManagedChannel channel, final ManagedChannel unlisted) {
        this.channel = channel;
        this.stub = RaftServerProtocolServiceGrpc.newStub(unlisted);
      }
    }

    private static final class WithACycle {
      private final ManagedChannel channel;
      private final List<Object>   cycle = new ArrayList<>();

      private WithACycle(final ManagedChannel channel) {
        this.channel = channel;
        cycle.add(cycle);
        cycle.add(new Object[] { cycle, channel });
      }
    }

    private static final class WithAMap {
      private final ManagedChannel              channel;
      private final Map<String, ManagedChannel> more;

      private WithAMap(final ManagedChannel channel) {
        this.channel = channel;
        this.more = Map.of("more", channel);
      }
    }
  }
}
