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
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Issue #7819: which configuration sequences arm the security-convergence readiness gate, and - the half the issue
 * warned about - which must not. Each sequence is the one a real node applies, in the order Ratis delivers it.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class RuntimeJoinDetectorTest {

  private static final RaftPeerId SELF = RaftPeerId.valueOf("arcadedb-3");

  /**
   * A peer added by {@code addPeer}, {@code connect cluster} or a {@code KubernetesAutoJoin} self-join: it applies
   * the cluster's existing configuration first, then the joint-consensus entry that adds it, then the final one.
   */
  @Test
  void aPeerAddedByAConfigurationChangeIsArmed() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), List.of());
    assertThat(detector.hasJoinedAtRuntime()).isFalse();

    assertThat(detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2"))).as("the joint entry that adds this node arms it").isTrue();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of());
    assertThat(detector.hasJoinedAtRuntime()).isTrue();
  }

  /**
   * The baseline trap the issue named. When the joint entry is the FIRST configuration the joiner observes - its
   * earlier entries were compacted into the snapshot it installed, and the snapshot's configuration equalled the
   * starting group so Ratis never notified it - there is no earlier observation to diff against. The joint entry
   * describes its own change, so it still arms.
   */
  @Test
  void theJointEntryArmsWithoutAnyBaseline() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"));

    assertThat(detector.hasJoinedAtRuntime()).isTrue();
  }

  /**
   * The fallback: a joiner that fell behind and caught up by a snapshot install carrying the FINAL configuration
   * never applies the joint entry. It did observe a configuration without itself earlier in this process.
   */
  @Test
  void aFinalConfigurationFollowingOneWithoutThisNodeArms() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1"), List.of());
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-3"), List.of());

    assertThat(detector.hasJoinedAtRuntime()).isTrue();
  }

  /**
   * A member of a freshly formed cluster: its first observed configuration is the leader's startup entry, which
   * names it. Getting the baseline backwards - treating "not observed yet" as "not a member" - would arm it here,
   * on every node of every cluster, which is the always-fire failure.
   */
  @Test
  void aMemberOfAFreshlyFormedClusterIsNotArmed() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of());
    // A later leader's startup entry repeats the same membership.
    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of());

    assertThat(detector.hasJoinedAtRuntime()).isFalse();
  }

  /**
   * A static member watching SOMEONE ELSE join: the joint entry lists this node on both sides, which is a change
   * to the cluster, not to this node's membership.
   */
  @Test
  void aMemberWatchingAnotherPeerJoinIsNotArmed() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-3"), List.of());
    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-3", "arcadedb-4"), peers("arcadedb-1", "arcadedb-3"));
    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-3", "arcadedb-4"), List.of());

    assertThat(detector.hasJoinedAtRuntime()).isFalse();
  }

  /**
   * A statically configured node that restarts replays configuration entries, and every one of them names it -
   * including the joint entries of peers added since, which name it on the old side too. Replay must not arm it.
   */
  @Test
  void aStaticMemberReplayingItsLogOnRestartIsNotArmed() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of());
    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-2", "arcadedb-3", "arcadedb-4"),
        peers("arcadedb-1", "arcadedb-2", "arcadedb-3"));
    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-2", "arcadedb-3", "arcadedb-4"), List.of());

    assertThat(detector.hasJoinedAtRuntime()).isFalse();
  }

  /**
   * Removal does not arm, and removal-then-re-add does: a node taken out while it was down and re-added by
   * {@code KubernetesAutoJoin}'s startup self-check (issue #5275) is exactly as stale as a new peer.
   */
  @Test
  void aRemovalDoesNotArmButARemovalThenReAddDoes() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-3"), List.of());
    detector.onConfiguration(SELF, peers("arcadedb-1"), peers("arcadedb-1", "arcadedb-3"));
    detector.onConfiguration(SELF, peers("arcadedb-1"), List.of());
    assertThat(detector.hasJoinedAtRuntime()).as("removed, not joined").isFalse();

    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-3"), peers("arcadedb-1"));
    assertThat(detector.hasJoinedAtRuntime()).as("re-added").isTrue();
  }

  /** Once armed, a later configuration does not disarm it: convergence releases the gate, membership does not. */
  @Test
  void armingIsNeverUndoneByALaterConfiguration() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"));
    assertThat(detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), List.of()))
        .as("a later configuration does not re-report the arming").isFalse();
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3", "arcadedb-5"), peers("arcadedb-0", "arcadedb-3"));

    assertThat(detector.hasJoinedAtRuntime()).isTrue();
  }

  /** A state machine Ratis has not initialized cannot say which peer is itself, and records nothing. */
  @Test
  void anUnknownSelfRecordsNothing() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    assertThat(detector.onConfiguration(null, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"))).isFalse();
    // And it did not consume a baseline either: the first real observation still behaves as a first one.
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), List.of());

    assertThat(detector.hasJoinedAtRuntime()).isFalse();
  }

  /**
   * The wiring. The state machine decodes {@code oldPeers} from the configuration proto and hands both lists to
   * the detector - on an unwired state machine, whose {@code getId()} is {@code null}, without throwing and without
   * arming, since it cannot know which peer it is.
   */
  @Test
  void theStateMachineCallbackIsSafeAndInertBeforeRatisInitializesIt() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      assertThatNoException().isThrownBy(() -> sm.notifyConfigurationChanged(1, 11,
          configuration(List.of("arcadedb-0", "arcadedb-3"), List.of("arcadedb-0"))));
      assertThat(sm.getRuntimeJoinDetector().hasJoinedAtRuntime()).isFalse();
    } finally {
      sm.close();
    }
  }

  /** The detector the state machine records into is the one handed to it, so its owner reads what it records. */
  @Test
  void theStateMachineRecordsIntoTheDetectorItIsGiven() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      final RuntimeJoinDetector owned = new RuntimeJoinDetector();
      sm.setRuntimeJoinDetector(owned);
      assertThat(sm.getRuntimeJoinDetector()).isSameAs(owned);
    } finally {
      sm.close();
    }
  }

  // -----------------------------------------------------------------------------------------------------------

  private static List<RaftPeerId> peers(final String... ids) {
    final List<RaftPeerId> peers = new ArrayList<>(ids.length);
    for (final String id : ids)
      peers.add(RaftPeerId.valueOf(id));
    return peers;
  }

  private static RaftProtos.RaftConfigurationProto configuration(final List<String> peerIds,
      final List<String> oldPeerIds) {
    final RaftProtos.RaftConfigurationProto.Builder builder = RaftProtos.RaftConfigurationProto.newBuilder();
    for (final String peerId : peerIds)
      builder.addPeers(peer(peerId));
    for (final String peerId : oldPeerIds)
      builder.addOldPeers(peer(peerId));
    return builder.build();
  }

  private static RaftProtos.RaftPeerProto peer(final String id) {
    return RaftProtos.RaftPeerProto.newBuilder().setId(ByteString.copyFromUtf8(id)).setAddress("localhost:2434")
        .build();
  }
}
