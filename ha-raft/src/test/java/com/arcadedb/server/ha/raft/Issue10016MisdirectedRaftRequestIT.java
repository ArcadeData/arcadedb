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

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftGroupIdProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.TermIndexProto;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Regression test for issue #10016: in a 3-node cluster that lost its leader, the survivors elected a new leader that
 * stepped down about 30 seconds later, and from then on every election failed with a bare
 * {@code IllegalStateException} from {@code RaftConfigurationImpl.hasMajority} and the cluster never had a leader again.
 * <p>
 * The trigger was the stopped node's host name resolving to the node doing the asking once the JVM's DNS cache expired
 * (the reporter ran Docker Desktop on Windows, and the same failure reproduces in containers by pointing the stopped
 * node's name at the loopback address). Every Raft RPC meant for the dead peer then reached the sender itself, and Apache
 * Ratis answers a server RPC without checking that it is the peer the request was addressed to:
 * <ul>
 * <li>the new leader received its own {@code AppendEntries}, recognised itself as the leader of the current term,
 * and stepped down to follower ({@code RaftServerImpl.appendEntriesAsync} calls {@code changeToFollowerAndPersistMetadata});</li>
 * <li>a candidate received its own {@code RequestVote}, granted it - it had voted for itself in that term - and
 * counted its own id among the votes of the OTHER peers, which trips the {@code !others.contains(selfId)}
 * precondition and kills the election thread.</li>
 * </ul>
 * Both are reproduced here without DNS, by sending over the real Raft gRPC listener what the misresolved channel
 * carried: a request addressed to one peer, delivered to another.
 */
class Issue10016MisdirectedRaftRequestIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * A candidate whose request for another peer's vote comes back to itself must not be handed a vote in that peer's
   * name. Before the fix the reply carried the candidate's own id, which is what made {@code hasMajority} throw.
   */
  @Test
  void aVoteRequestThatReachesTheCandidateItselfIsRefused() throws Exception {
    final int leader = requireLeaderIndex();
    final int candidate = firstFollowerOf(leader);
    final int addressedTo = otherFollower(leader, candidate);
    final RaftHAServer candidateRaft = getRaftPlugin(candidate).getRaftHAServer();
    final RaftPeerId candidateId = candidateRaft.getLocalPeerId();
    final RaftPeerId addressedToId = getRaftPlugin(addressedTo).getRaftHAServer().getLocalPeerId();
    final long refusedBefore = candidateRaft.getMisdirectedRaftRequestsRefused();
    final long resetsBefore = candidateRaft.getMisdirectedChannelResets();
    final long unconfirmedBefore = candidateRaft.getSelfDialProbesUnconfirmed();

    // A pre-vote, so a request that does get through cannot move any term and disturb the cluster.
    final RequestVoteRequestProto request = RequestVoteRequestProto.newBuilder()
        .setServerRequest(header(candidateRaft, candidateId, addressedToId))
        .setCandidateTerm(candidateRaft.getCurrentTerm() + 1)
        .setCandidateLastEntry(TermIndexProto.getDefaultInstance())
        .setPreVote(true)
        .build();

    final ManagedChannel channel = channelTo(candidate);
    try {
      final RequestVoteReplyProto reply = RaftServerProtocolServiceGrpc.newBlockingStub(channel)
          .withDeadlineAfter(10, TimeUnit.SECONDS).requestVote(request);
      fail("A vote request addressed to " + addressedToId + " was answered by " + RaftPeerId.valueOf(
          reply.getServerReply().getReplyId()) + " instead of being refused");
    } catch (final StatusRuntimeException e) {
      assertThat(e.getStatus().getDescription()).contains("addressed to " + addressedToId);
    } finally {
      channel.shutdownNow();
    }

    assertThat(candidateRaft.getMisdirectedRaftRequestsRefused()).isGreaterThan(refusedBefore);

    // This test, not the candidate, sent it - from 127.0.0.1, an address of the candidate's host, as any local process or
    // sidecar would (issue #10299). The candidate probes its own channel to that peer, which reaches the peer, so the claim
    // is found not to be its own and the working channel is left alone.
    Awaitility.await("the claim is probed").atMost(10, TimeUnit.SECONDS)
        .pollInterval(50, TimeUnit.MILLISECONDS).until(() -> candidateRaft.getSelfDialProbesUnconfirmed() > unconfirmedBefore);
    assertThat(candidateRaft.getMisdirectedChannelResets()).isEqualTo(resetsBefore);
  }

  /**
   * Issue #10299: the probe goes over this node's real Raft channel to the peer, whose real Raft server answers it - and
   * does nothing else: a pre-vote for term 0 moves no term and elects nobody.
   */
  @Test
  void aSelfDialProbeOverAWorkingChannelIsAnsweredByThePeerAndDisturbsNothing() throws Exception {
    final int leader = requireLeaderIndex();
    final int prober = firstFollowerOf(leader);
    final RaftHAServer proberRaft = getRaftPlugin(prober).getRaftHAServer();
    final RaftHAServer leaderRaft = getRaftPlugin(leader).getRaftHAServer();
    final long term = leaderRaft.getCurrentTerm();

    assertThat(proberRaft.probeSelfDial(leaderRaft.getLocalPeerId())).isEqualTo(RaftHAServer.SelfDialProbeOutcome.MEMBER);
    assertThat(proberRaft.probeSelfDial(getRaftPlugin(otherFollower(leader, prober)).getRaftHAServer().getLocalPeerId()))
        .isEqualTo(RaftHAServer.SelfDialProbeOutcome.MEMBER);

    Awaitility.await("the probes disturb nobody").during(1, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS)
        .pollInterval(50, TimeUnit.MILLISECONDS).until(() -> leaderRaft.isLeader() && leaderRaft.getCurrentTerm() == term);
  }

  /**
   * Issue #10299: a channel that does lead back to this node is confirmed by the probe coming back through the real Raft
   * gRPC listener and its guard, and is reset - whatever address the connection shows, which is no longer consulted.
   */
  @Test
  void aChannelThatLeadsBackToThisNodeIsConfirmedByTheProbeAndReset() throws Exception {
    final int leader = requireLeaderIndex();
    final int node = firstFollowerOf(leader);
    final RaftHAServer raft = getRaftPlugin(node).getRaftHAServer();
    final RaftPeerId addressedToId = getRaftPlugin(otherFollower(leader, node)).getRaftHAServer().getLocalPeerId();
    final long resetsBefore = raft.getMisdirectedChannelResets();

    // This node's channel to that peer leads to this node's own Raft port, as a misresolved name or a hairpin NAT makes it
    final ManagedChannel loop = channelTo(node);
    raft.setSelfDialProbeTransportForTesting(
        probe -> RaftServerProtocolServiceGrpc.newBlockingStub(loop).withDeadlineAfter(10, TimeUnit.SECONDS).requestVote(probe));
    try {
      assertThat(raft.probeSelfDial(addressedToId)).isEqualTo(RaftHAServer.SelfDialProbeOutcome.SELF);

      // ...and a request claiming this node's id, from wherever, now leads to the repair
      raft.onMisdirectedRaftRequest("appendEntries", addressedToId, raft.getLocalPeerId(), null, RaftHAServer.NO_PROBE_CALL_ID);
      Awaitility.await("the self-dialled channel is reset").atMost(10, TimeUnit.SECONDS)
          .pollInterval(50, TimeUnit.MILLISECONDS).until(() -> raft.getMisdirectedChannelResets() > resetsBefore);
    } finally {
      raft.setSelfDialProbeTransportForTesting(null);
      loop.shutdownNow();
    }
  }

  /**
   * An id that is not a peer of this cluster is refused all the same, and repairs nothing: there is no channel to it.
   * Ids from the wire share one throttling slot, so no sender can grow the node's bookkeeping.
   */
  @Test
  void aRequestAddressedToAnUnknownPeerIsRefused() throws Exception {
    final int leader = requireLeaderIndex();
    final int receiver = firstFollowerOf(leader);
    final RaftHAServer receiverRaft = getRaftPlugin(receiver).getRaftHAServer();
    final RaftPeerId receiverId = receiverRaft.getLocalPeerId();
    final long resetsBefore = receiverRaft.getMisdirectedChannelResets();

    final ManagedChannel channel = channelTo(receiver);
    try {
      for (int i = 0; i < 20; i++) {
        final RaftPeerId stranger = RaftPeerId.valueOf("stranger_" + i);
        final RequestVoteRequestProto request = RequestVoteRequestProto.newBuilder()
            .setServerRequest(header(receiverRaft, receiverId, stranger))
            .setCandidateTerm(receiverRaft.getCurrentTerm() + 1)
            .setCandidateLastEntry(TermIndexProto.getDefaultInstance())
            .setPreVote(true)
            .build();
        try {
          RaftServerProtocolServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS)
              .requestVote(request);
          fail("A vote request addressed to " + stranger + " was answered instead of being refused");
        } catch (final StatusRuntimeException e) {
          assertThat(e.getStatus().getDescription()).contains("addressed to " + stranger);
        }
      }
    } finally {
      channel.shutdownNow();
    }

    // Held for a moment: a reset would be queued on another thread.
    Awaitility.await().during(1, TimeUnit.SECONDS).atMost(3, TimeUnit.SECONDS)
        .until(() -> receiverRaft.getMisdirectedChannelResets() == resetsBefore);
  }

  /**
   * A leader whose heartbeat for a follower comes back to itself must stay the leader. Before the fix it recognised
   * the request's leader id - its own - as the leader of the current term and stepped down.
   */
  @Test
  void anAppendEntriesThatReachesTheLeaderItselfDoesNotDemoteIt() throws Exception {
    final int leader = requireLeaderIndex();
    final int addressedTo = firstFollowerOf(leader);
    final RaftHAServer leaderRaft = getRaftPlugin(leader).getRaftHAServer();
    final RaftPeerId leaderId = leaderRaft.getLocalPeerId();
    final RaftPeerId addressedToId = getRaftPlugin(addressedTo).getRaftHAServer().getLocalPeerId();
    final long term = leaderRaft.getCurrentTerm();
    final long resetsBefore = leaderRaft.getMisdirectedChannelResets();
    final long unconfirmedBefore = leaderRaft.getSelfDialProbesUnconfirmed();

    final TermIndex last = leaderRaft.getRaftServerForTesting().getDivision(leaderRaft.getRaftGroup().getGroupId())
        .getRaftLog().getLastEntryTermIndex();
    final AppendEntriesRequestProto heartbeat = AppendEntriesRequestProto.newBuilder()
        .setServerRequest(header(leaderRaft, leaderId, addressedToId))
        .setLeaderTerm(term)
        .setPreviousLog(TermIndexProto.newBuilder().setTerm(last.getTerm()).setIndex(last.getIndex()))
        .setLeaderCommit(leaderRaft.getCommitIndex())
        .build();

    final CompletableFuture<AppendEntriesReplyProto> outcome = new CompletableFuture<>();
    final ManagedChannel channel = channelTo(leader);
    try {
      final StreamObserver<AppendEntriesRequestProto> stream = RaftServerProtocolServiceGrpc.newStub(channel)
          .appendEntries(new StreamObserver<>() {
            @Override
            public void onNext(final AppendEntriesReplyProto reply) {
              outcome.complete(reply);
            }

            @Override
            public void onError(final Throwable t) {
              outcome.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
              outcome.completeExceptionally(new IllegalStateException("stream completed with no reply"));
            }
          });
      stream.onNext(heartbeat);

      try {
        final AppendEntriesReplyProto reply = outcome.get(10, TimeUnit.SECONDS);
        fail("An AppendEntries addressed to " + addressedToId + " was answered by " + RaftPeerId.valueOf(
            reply.getServerReply().getReplyId()) + " instead of being refused");
      } catch (final ExecutionException e) {
        assertThat(e.getCause()).isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) e.getCause()).getStatus().getDescription())
            .contains("addressed to " + addressedToId);
      }
    } finally {
      channel.shutdownNow();
    }

    // The step-down, when it happens, is synchronous with the reply. Hold the check over a couple of heartbeat rounds
    // anyway, and require the leadership to be untouched at the very same term throughout.
    Awaitility.await("the leader must not step down on its own misdirected heartbeat")
        .during(2, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
        .until(() -> leaderRaft.isLeader() && leaderRaft.getCurrentTerm() == term);

    // The heartbeat came over the appendEntries stream from 127.0.0.1 naming the leader as sender, but this test sent it:
    // the leader's probe of its channel to that follower is answered by the follower, so nothing is reset (issue #10299).
    Awaitility.await("the claim is probed").atMost(10, TimeUnit.SECONDS)
        .pollInterval(50, TimeUnit.MILLISECONDS).until(() -> leaderRaft.getSelfDialProbesUnconfirmed() > unconfirmedBefore);
    assertThat(leaderRaft.getMisdirectedChannelResets()).isEqualTo(resetsBefore);
  }

  private int otherFollower(final int leader, final int follower) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader && i != follower)
        return i;
    throw new IllegalStateException("no third node");
  }

  private static RaftRpcRequestProto header(final RaftHAServer raft, final RaftPeerId requestor,
      final RaftPeerId addressedTo) {
    return RaftRpcRequestProto.newBuilder()
        .setRequestorId(requestor.toByteString())
        .setReplyId(addressedTo.toByteString())
        .setRaftGroupId(RaftGroupIdProto.newBuilder().setId(raft.getRaftGroup().getGroupId().toByteString()))
        .setCallId(1)
        .build();
  }

  private ManagedChannel channelTo(final int serverIndex) {
    return NettyChannelBuilder.forTarget("127.0.0.1:" + raftPort(serverIndex)).usePlaintext().build();
  }
}
