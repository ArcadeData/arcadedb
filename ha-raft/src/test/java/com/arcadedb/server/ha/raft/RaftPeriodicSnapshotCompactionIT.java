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

import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5345: on a low-write cluster the count-based auto-snapshot threshold is
 * never reached, so the snapshot index never advances, no Raft log segment is ever purged, and the log
 * grows until the volume is full.
 * <p>
 * Exercises the exact mechanism {@link RaftLogCompactionScheduler} relies on against real Ratis peers:
 * a node-local {@link SnapshotManagementRequest} creates a snapshot and so advances the index Ratis
 * purges log segments against. Crucially it asserts this on <b>every</b> peer, leader and follower
 * alike - the node that wedged in the report was a follower, and each Ratis server purges its own log
 * against its own snapshot index.
 */
class RaftPeriodicSnapshotCompactionIT extends BaseMiniRaftTest {

  private static final String DB_NAME     = "mini-raft-test";
  private static final int    ENTRY_COUNT = 20;

  private final ClientId  clientId = ClientId.randomId();
  private final AtomicLong callId  = new AtomicLong();

  @Override
  protected int getPeerCount() {
    return 3;
  }

  @Test
  void nodeLocalSnapshotRequestAdvancesSnapshotIndexOnEveryPeer() throws Exception {
    for (int i = 0; i < ENTRY_COUNT; i++)
      submitSchemaEntry(DB_NAME, null);

    assertAllPeersConverged(ENTRY_COUNT);

    // Precondition: the count-based trigger has NOT fired - this is the low-write cluster of the issue.
    // Without the periodic trigger the snapshot index would stay here forever and nothing would purge.
    for (final RaftPeerId peerId : peerIds()) {
      final RaftServer.Division division = getCluster().getDivision(peerId);
      assertThat(division.getStateMachine().getLatestSnapshot())
          .as("peer %s must have no snapshot before the periodic trigger fires", peerId)
          .isNull();
    }

    // A periodic tick: ask every node to snapshot locally with a creation gap of 1.
    for (final RaftPeerId peerId : peerIds()) {
      final RaftServer.Division division = getCluster().getDivision(peerId);
      final TermIndex applied = division.getStateMachine().getLastAppliedTermIndex();
      assertThat(applied).as("peer %s must have applied entries", peerId).isNotNull();

      final RaftClientReply reply = division.getRaftServer().snapshotManagement(
          SnapshotManagementRequest.newCreate(clientId, peerId, division.getMemberId().getGroupId(),
              callId.incrementAndGet(), 30_000L, 1L));

      assertThat(reply.isSuccess())
          .as("node-local snapshot request must succeed on peer %s (leader and followers alike)", peerId)
          .isTrue();

      assertThat(division.getStateMachine().getLatestSnapshot())
          .as("peer %s must have a snapshot after the periodic trigger", peerId)
          .isNotNull();
      // The snapshot index is the value Ratis purges log segments against when
      // arcadedb.ha.logPurgeUptoSnapshot is enabled, so it must be a real, positive index.
      assertThat(division.getStateMachine().getLatestSnapshot().getIndex())
          .as("peer %s snapshot index must be a real log index so segments below it become purgeable", peerId)
          .isPositive();
    }
  }

  @Test
  void snapshotRequestIsANoOpWhenFewerEntriesThanTheCreationGapWereApplied() throws Exception {
    submitSchemaEntry(DB_NAME, null);
    assertAllPeersConverged(1);

    // An idle cluster must not pay for a tick: a gap far above the applied count short-circuits in Ratis,
    // which is what makes arcadedb.ha.snapshotMinEntries a cheap floor rather than busywork.
    for (final RaftPeerId peerId : peerIds()) {
      final RaftServer.Division division = getCluster().getDivision(peerId);
      final RaftClientReply reply = division.getRaftServer().snapshotManagement(
          SnapshotManagementRequest.newCreate(clientId, peerId, division.getMemberId().getGroupId(),
              callId.incrementAndGet(), 30_000L, 1_000_000L));

      assertThat(reply.isSuccess()).as("an under-gap snapshot request must still report success").isTrue();
      assertThat(division.getStateMachine().getLatestSnapshot())
          .as("peer %s must not have taken a snapshot below the creation gap", peerId)
          .isNull();
    }
  }

  private java.util.List<RaftPeerId> peerIds() {
    return getPeers().stream().map(p -> p.getId()).toList();
  }
}
