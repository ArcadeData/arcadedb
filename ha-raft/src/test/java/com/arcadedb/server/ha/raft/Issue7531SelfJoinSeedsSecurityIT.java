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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for issue #7531 over a real Ratis cluster: the {@code Mode.ADD} a self-joining pod issues
 * for itself must make the <b>leader</b> seed the cluster security documents.
 * <p>
 * The membership change below is the call {@code KubernetesAutoJoin.tryAutoJoin} makes, issued the same way -
 * by a client, for a peer that is not yet a member, with nobody running the admitting-side seed that
 * {@code POST /api/v1/cluster/peer} and {@code connect cluster} run (issue #7521). Before the fix nothing in
 * ArcadeDB reacted to that at all, and the new pod served requests against its own
 * {@code server-users.jsonl}, {@code server-groups.json} and {@code server-api-tokens.json} - none of which a
 * Raft snapshot install carries.
 * <p>
 * What is real here: the Ratis cluster, the configuration change, the {@code ArcadeStateMachine} on every
 * peer, its {@code notifyConfigurationChanged} callback and its Raft-division role read. What is substituted
 * is the last step, the submit of the three documents, which {@code Issue7521SecuritySeedRetryTest} and
 * {@code RaftUserSeedOnPeerAdd3NodesIT} cover against real {@code ServerSecurity} and real replication.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7531SelfJoinSeedsSecurityIT extends BaseMiniRaftTest {

  /** How long the whole cluster gets to apply a configuration entry before an assertion gives up. */
  private static final long CONVERGENCE_TIMEOUT_MS = 30_000L;

  private final Map<RaftPeerId, AtomicInteger> seedsByPeer = new HashMap<>();

  @Override
  protected int getPeerCount() {
    return 3;
  }

  /**
   * Substitutes the seeder on every peer's state machine with one that records instead of submitting, and
   * keeps the production leader read and retry budget so those stay under test.
   */
  private void installRecorderOn(final RaftPeerId peerId) {
    final RaftServer.Division division = getCluster().getDivision(peerId);
    if (division == null)
      return;
    final ArcadeStateMachine sm = (ArcadeStateMachine) division.getStateMachine();
    final AtomicInteger seeds = seedsByPeer.computeIfAbsent(peerId, id -> new AtomicInteger());
    sm.setMembershipSecuritySeederForTesting(new MembershipSecuritySeeder(
        sm::isLocalNodeRaftLeader, sm::securitySeedRetryBudgetMs,
        retryBudgetMs -> {
          seeds.incrementAndGet();
          return List.of();
        },
        Runnable::run));
  }

  private int seedsOn(final RaftPeerId peerId) {
    final AtomicInteger seeds = seedsByPeer.get(peerId);
    return seeds == null ? 0 : seeds.get();
  }

  private void awaitSeedsOn(final RaftPeerId peerId, final int expected) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + CONVERGENCE_TIMEOUT_MS;
    while (seedsOn(peerId) < expected && System.currentTimeMillis() < deadline)
      Thread.sleep(100);
    assertThat(seedsOn(peerId)).as("seeds issued by the leader %s", peerId).isEqualTo(expected);
  }

  /**
   * A self-issued {@code Mode.ADD} is seeded by the leader, and by the leader only.
   * <p>
   * Two membership changes, because the first configuration a state machine observes is its baseline and
   * seeds nothing by design - on a live leader that first observation is Ratis's own startup configuration
   * entry, which the recorders installed mid-test have missed. The first add supplies the baseline; the
   * second is the assertion.
   */
  @Test
  void aSelfIssuedModeAddMakesTheLeaderSeedTheSecurityDocuments() throws Exception {
    final RaftPeerId leaderId = getCluster().getLeader().getId();

    for (final RaftPeer peer : getPeers())
      installRecorderOn(peer.getId());

    // First change: establishes the baseline every recorder needs.
    final RaftPeer first = addSelfJoiningPeer(leaderId);
    installRecorderOn(first.getId());
    final int seedsAfterBaseline = seedsOn(leaderId);

    // Second change: this is the one under test. A leadership change in between would move the seed to a
    // node whose recorder has a different baseline, so the bookkeeping above only holds while it does not.
    assertThat(getCluster().getLeader().getId())
        .as("leadership must not move mid-test for the seed count to mean anything").isEqualTo(leaderId);

    final RaftPeer joining = addSelfJoiningPeer(leaderId);
    installRecorderOn(joining.getId());

    awaitSeedsOn(leaderId, seedsAfterBaseline + 1);

    // And nobody else seeds. The joining peer above all: its own documents are the stale ones.
    for (final Map.Entry<RaftPeerId, AtomicInteger> entry : seedsByPeer.entrySet())
      if (!entry.getKey().equals(leaderId))
        assertThat(entry.getValue().get())
            .as("peer %s is not the leader and must not seed its own security documents", entry.getKey())
            .isZero();
  }

  /**
   * Issues the membership change the way {@code KubernetesAutoJoin} does: a client-side
   * {@code setConfiguration} carrying the single new peer with {@link SetConfigurationRequest.Mode#ADD}.
   *
   * @return the peer that was added
   */
  private RaftPeer addSelfJoiningPeer(final RaftPeerId leaderId) throws Exception {
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
}
