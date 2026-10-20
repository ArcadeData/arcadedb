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
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.AbstractStub;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.ratis.util.TimeDuration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue10374SelfDialProbeCoversEveryChannelTest {

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

    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), null, RaftHAServer.NO_PROBE_CALL_ID);

    Awaitility.await("the self-dialled channel is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(SELF_DIAL) == 1);
    assertThat(log.formattedAt(Level.WARNING)).anyMatch(m -> m.contains(SELF_DIAL));
    assertThat(log.countFormattedContaining(CLAIM)).as("not reported as somebody else's claim").isZero();
    assertThat(raft.getSelfDialProbesAnsweredByMember()).isZero();
    assertThat(raft.getSelfDialProbesUnconfirmed()).isZero();
  }

  /** A connection that fails first does not hide one that comes back after it: the self-dial still wins. */
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
  @Test
  void anIncompleteCensusAnsweredByTheMemberIsInconclusive() {
    final RaftHAServer raft = detachedServer();
    raft.setSelfDialProbeChannelsForTesting(
        new SelfDialProbeChannels(List.of(SelfDialChannels.reachingTheMember(new AtomicInteger())), false));

    assertThat(raft.probeSelfDial(MEMBER)).isEqualTo(SelfDialProbeOutcome.MEMBER_ON_SOME_CHANNELS);

    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), null, RaftHAServer.NO_PROBE_CALL_ID);
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
