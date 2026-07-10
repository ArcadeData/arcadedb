/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression coverage for issue #4842: {@link RaftHAServer#getFollowerStates()} must not attribute a
 * follower's match/next index to the wrong peer id when the leader's membership snapshot shifts between
 * the three independent Ratis reads (follower peer-id list, match-index array, next-index array).
 * <p>
 * The correlation logic is exercised directly via the pure {@link RaftHAServer#correlateFollowerStates}
 * and {@link RaftHAServer#degradedFollowerStates} helpers so the race is reproduced deterministically,
 * without standing up a live cluster.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FollowerStatesCorrelationTest {

  private static RaftProtos.ServerRpcProto follower(final String peerId, final long lastRpcElapsedMs) {
    return RaftProtos.ServerRpcProto.newBuilder()
        .setId(RaftProtos.RaftPeerProto.newBuilder().setId(ByteString.copyFromUtf8(peerId)))
        .setLastRpcElapsedTimeMs(lastRpcElapsedMs)
        .build();
  }

  @Test
  void stableSnapshotCorrelatesByPosition() {
    final List<RaftProtos.ServerRpcProto> peers = List.of(follower("a", 10), follower("b", 20), follower("c", 30));
    final long[] match = { 100, 200, 300 };
    final long[] next = { 101, 201, 301 };

    final List<Map<String, Object>> states = RaftHAServer.correlateFollowerStates(peers, match, next, peers);

    assertThat(states).isNotNull().hasSize(3);
    assertThat(states.getFirst()).containsEntry("peerId", "a").containsEntry("matchIndex", 100L)
        .containsEntry("nextIndex", 101L).containsEntry("lastRpcElapsedMs", 10L);
    assertThat(states.get(1)).containsEntry("peerId", "b").containsEntry("matchIndex", 200L)
        .containsEntry("nextIndex", 201L).containsEntry("lastRpcElapsedMs", 20L);
    assertThat(states.get(2)).containsEntry("peerId", "c").containsEntry("matchIndex", 300L)
        .containsEntry("nextIndex", 301L).containsEntry("lastRpcElapsedMs", 30L);
  }

  @Test
  void shorterIndexArraysRejectCorrelation() {
    // Peer 'b' was removed between reading the peer list and reading the index arrays, so the arrays are
    // shorter. The old min(...) guard would have zipped 'b' onto c's index (300/301). We must refuse.
    final List<RaftProtos.ServerRpcProto> before = List.of(follower("a", 10), follower("b", 20), follower("c", 30));
    final List<RaftProtos.ServerRpcProto> after = List.of(follower("a", 10), follower("c", 30));
    final long[] match = { 100, 300 };
    final long[] next = { 101, 301 };

    assertThat(RaftHAServer.correlateFollowerStates(before, match, next, after)).isNull();
  }

  @Test
  void equalSizeReorderRejectsCorrelation() {
    // A node replacement (b -> d) keeps the cluster size identical, so a length check alone passes, but
    // the index arrays now belong to a different peer ordering. Correlation must be rejected.
    final List<RaftProtos.ServerRpcProto> before = List.of(follower("a", 10), follower("b", 20), follower("c", 30));
    final List<RaftProtos.ServerRpcProto> after = List.of(follower("a", 10), follower("d", 25), follower("c", 30));
    final long[] match = { 100, 200, 300 };
    final long[] next = { 101, 201, 301 };

    assertThat(RaftHAServer.correlateFollowerStates(before, match, next, after)).isNull();
  }

  @Test
  void mismatchedArrayLengthsRejectCorrelation() {
    final List<RaftProtos.ServerRpcProto> peers = List.of(follower("a", 10), follower("b", 20));
    final long[] match = { 100, 200 };
    final long[] next = { 101 }; // next array grew/shrank independently

    assertThat(RaftHAServer.correlateFollowerStates(peers, match, next, peers)).isNull();
  }

  @Test
  void degradedStatesOmitIndicesButKeepPeerAndContact() {
    // When membership keeps churning, we report peers with their last-contact time but no indices, so
    // downstream lag math treats the index as unknown instead of reading a misattributed value.
    final List<RaftProtos.ServerRpcProto> peers = List.of(follower("a", 10), follower("b", 20));

    final List<Map<String, Object>> states = RaftHAServer.degradedFollowerStates(peers);

    assertThat(states).hasSize(2);
    assertThat(states.getFirst()).containsEntry("peerId", "a").containsEntry("lastRpcElapsedMs", 10L)
        .doesNotContainKey("matchIndex").doesNotContainKey("nextIndex");
    assertThat(states.get(1)).containsEntry("peerId", "b").containsEntry("lastRpcElapsedMs", 20L)
        .doesNotContainKey("matchIndex").doesNotContainKey("nextIndex");
  }
}
