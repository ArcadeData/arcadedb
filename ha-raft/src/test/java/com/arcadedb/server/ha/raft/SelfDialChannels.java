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

import com.arcadedb.server.ha.raft.ratis.GuardedRaftServer;
import org.apache.ratis.proto.RaftProtos.RaftRpcReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.server.RaftServer;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stands in for the channels of a {@link RaftHAServer} that has no live Raft server, so a test can say where a self-dial
 * probe ends up (issue #10299).
 */
final class SelfDialChannels {

  private SelfDialChannels() {
  }

  /**
   * Every channel of {@code raft} leads back to {@code raft}: a probe is delivered to its own Raft gRPC guard, which refuses
   * it as addressed to another peer and hands it to the listener, as the real listener does with a request that came back.
   *
   * @return how many probes were sent
   */
  static AtomicInteger leadBackToItself(final RaftHAServer raft) {
    final AtomicInteger probes = new AtomicInteger();
    raft.setSelfDialProbeTransportForTesting(leadingBackTo(raft, probes));
    return probes;
  }

  /**
   * One channel of {@code raft} that leads back to {@code raft}, counting the probes sent over it into {@code probes}
   * (issue #10374).
   */
  static RaftHAServer.SelfDialProbeTransport leadingBackTo(final RaftHAServer raft, final AtomicInteger probes) {
    final RaftServer itself = (RaftServer) Proxy.newProxyInstance(RaftServer.class.getClassLoader(),
        new Class<?>[] { RaftServer.class }, (proxy, method, args) -> "getId".equals(method.getName()) ? raft.getLocalPeerId() : null);
    final RaftServer guarded = GuardedRaftServer.wrap(itself, raft::onMisdirectedRaftRequest);
    return probe -> {
      probes.incrementAndGet();
      return guarded.requestVote(probe);
    };
  }

  /** One channel that reaches the member it is meant for, counting the probes sent over it into {@code probes}. */
  static RaftHAServer.SelfDialProbeTransport reachingTheMember(final AtomicInteger probes) {
    return probe -> {
      probes.incrementAndGet();
      return answeredByTheMember(probe);
    };
  }

  /**
   * Every channel of {@code raft} reaches the member it is meant for, which answers the probe under its own id.
   *
   * @return how many probes were sent
   */
  static AtomicInteger reachTheirMembers(final RaftHAServer raft) {
    final AtomicInteger probes = new AtomicInteger();
    raft.setSelfDialProbeTransportForTesting(probe -> {
      probes.incrementAndGet();
      return answeredByTheMember(probe);
    });
    return probes;
  }

  /** The reply the member {@code probe} is addressed to sends back: a rejected pre-vote, under its own id. */
  static RequestVoteReplyProto answeredByTheMember(final RequestVoteRequestProto probe) {
    return RequestVoteReplyProto.newBuilder().setServerReply(RaftRpcReplyProto.newBuilder()
        .setRequestorId(probe.getServerRequest().getRequestorId())
        .setReplyId(probe.getServerRequest().getReplyId())).build();
  }
}
