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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests of the runtime-join detector over a real Ratis cluster: the configuration entries Ratis actually
 * delivers must arm the detector on the peer that was added and on nobody else, and a remove followed by a re-add moves
 * the join index forward on that peer. Complements the hand-built sequences of {@link RuntimeJoinDetectorTest}.
 */
class RuntimeJoinDetectorRaftIT extends BaseMiniRaftTest {

  private static final long   CONVERGENCE_TIMEOUT_MS = 30_000L;
  private static final String DB_NAME                = "mini-raft-test";

  @Override
  protected int getPeerCount() {
    return 3;
  }

  // Regression for #7819: the configuration entries Ratis delivers arm only the peer that was added
  /**
   * Integration test for issue #7819 over a real Ratis cluster: the configuration entries Ratis actually delivers
   * must arm the runtime-join detector on the peer that was added, and on nobody else.
   * <p>
   * The unit test pins the decision on hand-built sequences; this one pins that those sequences are the ones Ratis
   * produces. In particular it proves the premise the detector rests on - that the change adding a peer reaches
   * that peer as a joint-consensus entry carrying the old membership - rather than assuming it from reading Ratis.
   * <p>
   * The members of a freshly formed cluster were never added to anything: none is armed.
   */
  @Test
  void theMembersOfAFreshlyFormedClusterAreNotArmed() throws Exception {
    // Wait until every peer has applied the leader's startup configuration entry, so "not armed" is an answer
    // rather than "has not observed anything yet".
    submitSchemaEntry(DB_NAME, null);
    for (final RaftPeer peer : getPeers())
      awaitApplied(peer.getId());
    for (final RaftPeer peer : getPeers())
      assertThat(detectorOf(peer.getId()).hasJoinedAtRuntime())
          .as("static member %s", peer.getId()).isFalse();
  }

  // Regression for #7819: the configuration entries Ratis delivers arm only the peer that was added
  /**
   * A peer admitted by a {@code Mode.ADD} - the call {@code KubernetesAutoJoin} makes, and the one
   * {@code addPeer} and {@code connect cluster} reduce to - is armed; the members that watched it join are not.
   */
  @Test
  void aPeerAddedAtRuntimeIsArmedAndTheExistingMembersAreNot() throws Exception {
    final RaftPeerId leaderId = getCluster().getLeader().getId();
    final RaftPeer joining = addPeerAtRuntime(leaderId);

    awaitArmed(joining.getId());

    for (final RaftPeer peer : getPeers())
      assertThat(detectorOf(peer.getId()).hasJoinedAtRuntime())
          .as("member %s watched another peer join; it did not join itself", peer.getId()).isFalse();
  }

  // Regression for #7819: the configuration entries Ratis delivers arm only the peer that was added
  /**
   * Ratis replays configuration entries on restart. A statically configured member that restarts replays the
   * startup entries AND the joint entry that added somebody else, and must come back unarmed.
   */
  @Test
  void aStaticMemberThatRestartsAfterAnotherPeerJoinedIsNotArmed() throws Exception {
    final RaftPeerId leaderId = getCluster().getLeader().getId();
    final RaftPeer joining = addPeerAtRuntime(leaderId);
    awaitArmed(joining.getId());

    int followerIndex = -1;
    for (int i = 0; i < getPeers().size(); i++)
      if (!getPeers().get(i).getId().equals(leaderId)) {
        followerIndex = i;
        break;
      }
    assertThat(followerIndex).isNotNegative();
    final RaftPeerId followerId = getPeers().get(followerIndex).getId();

    restartPeer(followerIndex);
    // Something has to commit after the restart for the replay to have run by the time it is read.
    submitSchemaEntry(DB_NAME, null);
    awaitApplied(followerId);

    assertThat(detectorOf(followerId).hasJoinedAtRuntime())
        .as("the restarted static member replayed a joint entry that added somebody else").isFalse();
  }

  // -----------------------------------------------------------------------------------------------------------

  private RuntimeJoinDetector detectorOf(final RaftPeerId peerId) {
    final RaftServer.Division division = getCluster().getDivision(peerId);
    return ((ArcadeStateMachine) division.getStateMachine()).getRuntimeJoinDetector();
  }

  private void awaitArmed(final RaftPeerId peerId) throws Exception {
    final long deadline = System.currentTimeMillis() + CONVERGENCE_TIMEOUT_MS;
    while (!detectorOf(peerId).hasJoinedAtRuntime() && System.currentTimeMillis() < deadline)
      Thread.sleep(100);
    assertThat(detectorOf(peerId).hasJoinedAtRuntime()).as("the peer added at runtime, %s", peerId).isTrue();
  }

  /** Waits until {@code peerId} has applied everything the leader has committed. */
  private void awaitApplied(final RaftPeerId peerId) throws Exception {
    final long target = getCluster().getLeader().getInfo().getLastAppliedIndex();
    final long deadline = System.currentTimeMillis() + CONVERGENCE_TIMEOUT_MS;
    while (getCluster().getDivision(peerId).getInfo().getLastAppliedIndex() < target
        && System.currentTimeMillis() < deadline)
      Thread.sleep(100);
    assertThat(getCluster().getDivision(peerId).getInfo().getLastAppliedIndex()).isGreaterThanOrEqualTo(target);
  }

  private RaftPeer addPeerAtRuntime(final RaftPeerId leaderId) throws Exception {
    final List<RaftPeer> added = new ArrayList<>(getCluster().addNewPeers(1, true).getAddedPeers());
    assertThat(added).hasSize(1);

    try (final RaftClient client = getCluster().createClient(leaderId)) {
      final SetConfigurationRequest.Arguments args = SetConfigurationRequest.Arguments.newBuilder()
          .setServersInNewConf(added)
          .setMode(SetConfigurationRequest.Mode.ADD)
          .build();
      final RaftClientReply reply = client.admin().setConfiguration(args);
      assertThat(reply.isSuccess()).as("the Mode.ADD must commit: %s", reply.getException()).isTrue();
    }
    return added.getFirst();
  }

  // Regression for #8317: a remove and re-add moves the join index forward on the peer
  /**
   * Integration test for issue #8317 over a real Ratis cluster: the configuration index Ratis delivers with the entry
   * that adds a peer is what the join-relative security-convergence signal measures from, and a remove followed by a
   * re-add moves it forward on the peer - so the installs of its previous membership stop counting.
   */
  @Test
  void aRemovedAndReAddedPeerMeasuresConvergenceFromTheReAdd() throws Exception {
    submitSchemaEntry(DB_NAME, null);
    final RaftPeerId leaderId = getCluster().getLeader().getId();

    final List<RaftPeer> added = new ArrayList<>(getCluster().addNewPeers(1, true).getAddedPeers());
    assertThat(added).hasSize(1);
    final RaftPeer joining = added.getFirst();
    setConfiguration(leaderId, List.of(joining), SetConfigurationRequest.Mode.ADD);

    final long firstJoin = awaitJoinIndexAbove(joining.getId(), 0L);
    assertThat(detectorOf(joining.getId()).securityDocumentsNotInstalledSinceJoin())
        .as("nothing installed on the joiner since it was added").containsExactly("users", "groups", "API tokens");

    // Stand-in for an install from the first membership: once past the first join it converges the document.
    detectorOf(joining.getId()).onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, firstJoin + 1);
    assertThat(detectorOf(joining.getId()).securityDocumentsNotInstalledSinceJoin()).doesNotContain("users");

    // Removed, then re-added: the joint entry of the re-add has old peers without it.
    setConfiguration(leaderId, getPeers(), SetConfigurationRequest.Mode.SET_UNCONDITIONALLY);
    setConfiguration(leaderId, List.of(joining), SetConfigurationRequest.Mode.ADD);

    final long reAdd = awaitJoinIndexAbove(joining.getId(), firstJoin + 1);
    assertThat(reAdd).isGreaterThan(firstJoin);
    assertThat(detectorOf(joining.getId()).securityDocumentsNotInstalledSinceJoin())
        .as("the users install from before the re-add no longer counts").contains("users");
  }

  private long awaitJoinIndexAbove(final RaftPeerId peerId, final long floor) throws Exception {
    final long deadline = System.currentTimeMillis() + CONVERGENCE_TIMEOUT_MS;
    while (detectorOf(peerId).joinIndex() <= floor && System.currentTimeMillis() < deadline)
      Thread.sleep(100);
    final long joinIndex = detectorOf(peerId).joinIndex();
    assertThat(joinIndex).as("join index of %s", peerId).isGreaterThan(floor);
    return joinIndex;
  }

  private void setConfiguration(final RaftPeerId leaderId, final List<RaftPeer> servers,
      final SetConfigurationRequest.Mode mode) throws Exception {
    try (final RaftClient client = getCluster().createClient(leaderId)) {
      final SetConfigurationRequest.Arguments args = SetConfigurationRequest.Arguments.newBuilder()
          .setServersInNewConf(servers)
          .setMode(mode)
          .build();
      final RaftClientReply reply = client.admin().setConfiguration(args);
      assertThat(reply.isSuccess()).as("setConfiguration %s must commit: %s", mode, reply.getException()).isTrue();
    }
  }
}
