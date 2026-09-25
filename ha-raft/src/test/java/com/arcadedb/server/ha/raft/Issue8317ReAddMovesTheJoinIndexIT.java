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
 * Integration test for issue #8317 over a real Ratis cluster: the configuration index Ratis delivers with the entry
 * that adds a peer is what the join-relative security-convergence signal measures from, and a remove followed by a
 * re-add moves it forward on the peer - so the installs of its previous membership stop counting.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8317ReAddMovesTheJoinIndexIT extends BaseMiniRaftTest {

  private static final long   CONVERGENCE_TIMEOUT_MS = 30_000L;
  private static final String DB_NAME                = "mini-raft-test";

  @Override
  protected int getPeerCount() {
    return 3;
  }

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

  // -----------------------------------------------------------------------------------------------------------

  private RuntimeJoinDetector detectorOf(final RaftPeerId peerId) {
    final RaftServer.Division division = getCluster().getDivision(peerId);
    return ((ArcadeStateMachine) division.getStateMachine()).getRuntimeJoinDetector();
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
