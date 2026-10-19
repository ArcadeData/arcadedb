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

import com.arcadedb.server.ha.raft.ratis.RaftPeerChannels;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerRpcWithProxy;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10374, over the real Raft gRPC transport: the leader's heartbeat channel to a follower leads back to the leader
 * while its primary channel to that follower still reaches it - the state a name that resolved to the wrong host and was
 * then fixed leaves behind, since a busy heartbeat stream never resolves the name again. The leader refuses its own
 * heartbeats; before the fix its probe went over the primary channel only, was answered by the follower, and the
 * heartbeat channel was never repaired. Now the probe goes over every channel and the self-dial is repaired.
 * <p>
 * The swap sets the private {@code hbChannel} and {@code hbAsyncStub} fields of Ratis 3.3.1's
 * {@code GrpcServerProtocolClient} by name: a {@code NoSuchFieldException} after a Ratis upgrade means they moved.
 */
class Issue10374HeartbeatChannelSelfDialIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  /** The real proxy of a peer holds the primary and the heartbeat channel, and the probe covers both. */
  @Test
  void aRealPeerProxyHoldsTwoChannelsAndBothAreProbed() throws Exception {
    final int leader = requireLeaderIndex();
    final RaftHAServer leaderRaft = getRaftPlugin(leader).getRaftHAServer();
    final RaftPeerId followerId = getRaftPlugin(firstFollowerOf(leader)).getRaftHAServer().getLocalPeerId();

    final RaftPeerChannels.Census census = RaftPeerChannels.of(proxyOf(leaderRaft, followerId));
    assertThat(census.channels()).as("the primary channel and the heartbeat channel").hasSize(2);
    assertThat(census.complete()).isTrue();
    assertThat(leaderRaft.probeSelfDial(followerId)).isEqualTo(RaftHAServer.SelfDialProbeOutcome.MEMBER);
  }

  @Test
  void aSelfDialledHeartbeatChannelIsRepairedThoughThePrimaryChannelReachesThePeer() throws Exception {
    final int leader = requireLeaderIndex();
    final RaftHAServer leaderRaft = getRaftPlugin(leader).getRaftHAServer();
    final RaftPeerId followerId = getRaftPlugin(firstFollowerOf(leader)).getRaftHAServer().getLocalPeerId();
    final long resetsBefore = leaderRaft.getMisdirectedChannelResets();
    final long answeredByMemberBefore = leaderRaft.getSelfDialProbesAnsweredByMember();

    // Point the leader's heartbeat channel to the follower at the leader's own Raft port, and break the heartbeat stream
    // that runs on the old one so that the appender reopens it on the loop
    final Object proxy = proxyOf(leaderRaft, followerId);
    final ManagedChannel loop = NettyChannelBuilder.forTarget("127.0.0.1:" + raftPort(leader)).usePlaintext().build();
    try {
      final ManagedChannel previous = (ManagedChannel) field(proxy, "hbChannel").get(proxy);
      field(proxy, "hbAsyncStub").set(proxy, RaftServerProtocolServiceGrpc.newStub(loop));
      field(proxy, "hbChannel").set(proxy, loop);
      previous.shutdownNow();

      Awaitility.await("the self-dialled heartbeat channel is reset").atMost(60, TimeUnit.SECONDS)
          .pollInterval(100, TimeUnit.MILLISECONDS).until(() -> leaderRaft.getMisdirectedChannelResets() > resetsBefore);
      assertThat(leaderRaft.getSelfDialProbesAnsweredByMember())
          .as("the leader's own heartbeats are not taken for somebody else's claim").isEqualTo(answeredByMemberBefore);
    } finally {
      loop.shutdownNow();
    }

    // The reset dropped the self-dialled proxy: the one the leader holds now has two channels of its own, neither the loop
    final RaftPeerChannels.Census rebuilt = RaftPeerChannels.of(proxyOf(leaderRaft, followerId));
    assertThat(rebuilt.channels()).hasSize(2).doesNotContain(loop);
    assertThat(rebuilt.complete()).isTrue();

    Awaitility.await("the cluster settles").atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> findLeaderIndex() >= 0);
  }

  private static Object proxyOf(final RaftHAServer raft, final RaftPeerId peer) throws Exception {
    final RaftServerRpcWithProxy<?, ?> rpc = (RaftServerRpcWithProxy<?, ?>) raft.getRaftServerForTesting().getServerRpc();
    return rpc.getProxies().getProxy(peer);
  }

  private static Field field(final Object owner, final String name) throws Exception {
    final Field field = owner.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }
}
