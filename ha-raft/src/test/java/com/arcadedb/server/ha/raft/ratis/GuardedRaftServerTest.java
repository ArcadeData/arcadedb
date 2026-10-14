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
package com.arcadedb.server.ha.raft.ratis;

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftGroupIdProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.ReadIndexRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionReplyProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.proto.RaftProtos.TermIndexProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link GuardedRaftServer}: the Raft server the gRPC services see delegates everything to the real
 * one, refuses a server RPC addressed to another peer (issue #10016), and still runs the #8533 catch-up hook.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GuardedRaftServerTest {

  private static final RaftPeerId  SELF  = RaftPeerId.valueOf("n1");
  private static final RaftPeerId  OTHER = RaftPeerId.valueOf("n2");
  private static final RaftPeerId  THIRD = RaftPeerId.valueOf("n3");
  private static final RaftGroupId GROUP = RaftGroupId.randomId();

  private final List<String> calls = new ArrayList<>();
  private final List<String> refused = new ArrayList<>();

  /** A server named {@link #SELF} that records every call it receives and answers each with a default reply. */
  private RaftServer realServer() {
    return (RaftServer) Proxy.newProxyInstance(RaftServer.class.getClassLoader(), new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          calls.add(method.getName());
          return switch (method.getName()) {
            case "getId" -> SELF;
            case "requestVote" -> RequestVoteReplyProto.getDefaultInstance();
            case "startLeaderElection" -> StartLeaderElectionReplyProto.getDefaultInstance();
            case "appendEntriesAsync" -> CompletableFuture.completedFuture(AppendEntriesReplyProto.getDefaultInstance());
            // getDivision fails: the #8533 gate must let the request through as asked rather than fail the election.
            case "getDivision" -> throw new IOException("no such group");
            case "close" -> throw new IOException("close failed");
            default -> null;
          };
        });
  }

  private RaftServer guarded() {
    return GuardedRaftServer.wrap(realServer(),
        (rpc, addressedTo, requestor) -> refused.add(rpc + ":" + addressedTo + "<-" + requestor));
  }

  private static RaftRpcRequestProto header(final RaftPeerId requestor, final RaftPeerId addressedTo) {
    final RaftRpcRequestProto.Builder builder = RaftRpcRequestProto.newBuilder()
        .setRaftGroupId(RaftGroupIdProto.newBuilder().setId(GROUP.toByteString()))
        .setRequestorId(requestor.toByteString());
    if (addressedTo != null)
      builder.setReplyId(addressedTo.toByteString());
    return builder.build();
  }

  private static RequestVoteRequestProto vote(final RaftPeerId requestor, final RaftPeerId addressedTo) {
    return RequestVoteRequestProto.newBuilder().setServerRequest(header(requestor, addressedTo)).setCandidateTerm(3)
        .build();
  }

  /**
   * The reported shape: a candidate's request for another peer's vote reached the candidate itself. Answering it
   * handed the candidate a vote in its own name, which Ratis counted among the other peers' votes and failed on.
   */
  @Test
  void aVoteRequestAddressedToAnotherPeerIsRefusedWithoutReachingRatis() {
    final RaftServer server = guarded();

    assertThatThrownBy(() -> server.requestVote(vote(SELF, OTHER)))
        .isInstanceOf(MisdirectedRaftRequestException.class)
        .hasMessageContaining("requestVote addressed to " + OTHER)
        .hasMessageContaining("reached " + SELF);

    assertThat(calls).doesNotContain("requestVote");
    assertThat(refused).containsExactly("requestVote:" + OTHER + "<-" + SELF);
  }

  /**
   * The other half of the report: a leader that received its own heartbeat stepped down. The refusal must come out of
   * the call as an {@link IOException} - not wrapped, not as a failed future - so the gRPC stream sends it back for a
   * heartbeat as well, which Ratis answers out of order and would otherwise leave without any reply.
   */
  @Test
  void anAppendEntriesAddressedToAnotherPeerIsRefusedAsAnIOException() {
    final RaftServer server = guarded();
    final AppendEntriesRequestProto heartbeat = AppendEntriesRequestProto.newBuilder()
        .setServerRequest(header(SELF, OTHER)).setLeaderTerm(3).build();

    assertThatThrownBy(() -> server.appendEntriesAsync(heartbeat)).isInstanceOf(MisdirectedRaftRequestException.class);
    assertThat(calls).doesNotContain("appendEntriesAsync");
  }

  /** A peer whose address for another leads here is refused as well; the listener names who sent it. */
  @Test
  void everyServerRequestTypeAddressedToAnotherPeerIsRefused() {
    final RaftServer server = guarded();

    assertThatThrownBy(() -> server.installSnapshot(
        InstallSnapshotRequestProto.newBuilder().setServerRequest(header(THIRD, OTHER)).build()))
        .isInstanceOf(MisdirectedRaftRequestException.class);
    assertThatThrownBy(() -> server.startLeaderElection(
        StartLeaderElectionRequestProto.newBuilder().setServerRequest(header(THIRD, OTHER)).build()))
        .isInstanceOf(MisdirectedRaftRequestException.class);
    assertThatThrownBy(() -> server.readIndexAsync(
        ReadIndexRequestProto.newBuilder().setServerRequest(header(THIRD, OTHER)).build()))
        .isInstanceOf(MisdirectedRaftRequestException.class);
    assertThatThrownBy(() -> server.appendEntries(
        AppendEntriesRequestProto.newBuilder().setServerRequest(header(THIRD, OTHER)).build()))
        .isInstanceOf(MisdirectedRaftRequestException.class);

    assertThat(calls).doesNotContain("installSnapshot", "startLeaderElection", "readIndexAsync", "appendEntries");
    assertThat(refused).containsExactly(
        "installSnapshot:" + OTHER + "<-" + THIRD,
        "startLeaderElection:" + OTHER + "<-" + THIRD,
        "readIndexAsync:" + OTHER + "<-" + THIRD,
        "appendEntries:" + OTHER + "<-" + THIRD);
  }

  @Test
  void aRequestAddressedToThisNodeReachesRatis() throws Exception {
    final RaftServer server = guarded();

    assertThat(server.requestVote(vote(OTHER, SELF))).isSameAs(RequestVoteReplyProto.getDefaultInstance());
    assertThat(server.appendEntriesAsync(AppendEntriesRequestProto.newBuilder().setServerRequest(header(OTHER, SELF))
        .build()).get()).isSameAs(AppendEntriesReplyProto.getDefaultInstance());

    assertThat(calls).contains("requestVote", "appendEntriesAsync");
    assertThat(refused).isEmpty();
  }

  /** Nothing to compare a request with no target id against, so it is not this guard's to refuse. */
  @Test
  void aRequestWithNoTargetIdReachesRatis() throws Exception {
    assertThat(guarded().requestVote(vote(OTHER, null))).isSameAs(RequestVoteReplyProto.getDefaultInstance());
    assertThat(refused).isEmpty();
  }

  /** A listener that fails must not turn the refusal into anything else, nor let the request through. */
  @Test
  void aFailingListenerDoesNotChangeTheRefusal() {
    final RaftServer server = GuardedRaftServer.wrap(realServer(), (rpc, addressedTo, requestor) -> {
      throw new IllegalStateException("listener failure");
    });

    assertThatThrownBy(() -> server.requestVote(vote(SELF, OTHER))).isInstanceOf(MisdirectedRaftRequestException.class);
    assertThat(calls).doesNotContain("requestVote");
  }

  @Test
  void theWrappedServerHandsEveryCallToTheRealOneAndItsFailuresBack() throws Exception {
    final RaftServer wrapped = GuardedRaftServer.wrap(realServer(), null);

    assertThat(wrapped.getId()).isEqualTo(SELF);

    // A StartLeaderElection addressed to this node still runs the #8533 catch-up hook before reaching Ratis.
    final StartLeaderElectionRequestProto request = StartLeaderElectionRequestProto.newBuilder()
        .setServerRequest(header(OTHER, SELF))
        .setLeaderLastEntry(TermIndexProto.newBuilder().setTerm(15).setIndex(724)).build();
    assertThat(wrapped.startLeaderElection(request)).isSameAs(StartLeaderElectionReplyProto.getDefaultInstance());
    assertThat(calls).containsSubsequence("getDivision", "startLeaderElection");

    // A checked exception of the real server comes out as itself, not as an UndeclaredThrowableException.
    assertThatThrownBy(wrapped::close).isInstanceOf(IOException.class).hasMessage("close failed");
  }
}
