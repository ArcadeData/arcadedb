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

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.arcadedb.database.Database;
import com.arcadedb.exception.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cluster membership changes: reading the group, removing a peer, the quorum guard that refuses a removal, and
 * the peer display-name registry.
 * <p>
 * Every method here runs against its own 3-node cluster - {@code beginTest}/{@code endTest} are
 * {@code @BeforeEach}/{@code @AfterEach} and the Raft storage lives under the database directory the setup
 * deletes - so a removal never reaches the next method. What it does reach is <em>this</em> method's teardown:
 * the base class waits for every configured server to catch up to the leader and then compares their databases,
 * and a peer this test evicted does neither. It never applies another entry, so the wait can only burn its full
 * budget and log a timeout, and the comparison can only report the divergence the eviction asked for - both
 * charged to {@code endTest}, which is the wrong place to read about a peer some earlier line removed on
 * purpose. {@link #getServerToCheck()} below takes the evicted servers out of both (issue #6267).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DynamicMembershipTest extends BaseRaftHATest {

  /** Servers this test took out of the Raft group; see {@link #getServerToCheck()}. */
  private final Set<Integer> evictedServers = new HashSet<>();

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * The cluster as this test left it: the servers still in the Raft group. An evicted peer keeps running and
   * keeps its database open - it is simply no longer a replica, and holding it to a replica's contract at
   * teardown reports a failure nobody can act on.
   */
  @Override
  protected int[] getServerToCheck() {
    return serversMatching(i -> !evictedServers.contains(i));
  }

  @Test
  void getLivePeersReturnsAllConfiguredPeers() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raftServer = getRaftPlugin(leaderIndex).getRaftHAServer();
    final Collection<RaftPeer> livePeers = raftServer.getLivePeers();
    assertThat(livePeers).hasSize(3);
  }

  @Test
  void removePeerDecreasesClusterSize() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    // Pick a non-leader peer to remove, since Ratis requires the leader to process the change
    final int targetIndex = leaderIndex == 0 ? 2 : 0;

    final RaftHAServer raftServer = getRaftPlugin(leaderIndex).getRaftHAServer();
    assertThat(raftServer.getLivePeers()).hasSize(3);

    evict(raftServer, targetIndex, false);
    assertThat(raftServer.getLivePeers()).hasSize(2);
  }

  /**
   * A peer removed from the group stops replicating, and the cluster's remaining members must not be held to
   * matching it. Reproduces the {@code endTest > checkDatabasesAreIdentical} failure this class used to leave
   * behind: without {@link #getServerToCheck()} the write below reaches the two members and not the evicted
   * peer, and the teardown comparison fails in a method that did nothing wrong.
   */
  @Test
  void removedPeerIsNotHeldToTheClusterConsistencyCheck() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final int targetIndex = leaderIndex == 0 ? 2 : 0;
    final RaftHAServer raftServer = getRaftPlugin(leaderIndex).getRaftHAServer();
    evict(raftServer, targetIndex, false);

    // A write the remaining members commit and the evicted peer, by definition, never sees.
    final Database leaderDB = getServer(leaderIndex).getDatabase(getDatabaseName());
    leaderDB.transaction(() -> {
      leaderDB.command("sql", "CREATE DOCUMENT TYPE AfterEviction");
      leaderDB.command("sql", "INSERT INTO AfterEviction SET id = 1");
    });

    assertThat(leaderDB.getSchema().existsType("AfterEviction")).isTrue();
    assertThat(getServer(targetIndex).getDatabase(getDatabaseName()).getSchema().existsType("AfterEviction"))
        .as("an evicted peer must not receive entries committed after its removal")
        .isFalse();
  }

  @Test
  void removePeerRefusedWhenItWouldBreakQuorum() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raftServer = getRaftPlugin(leaderIndex).getRaftHAServer();
    assertThat(raftServer.getLivePeers()).hasSize(3);

    // 3 -> 2 is allowed (quorum of 3 is 2). Remove a non-leader so the leader can commit the change.
    final int firstTarget = leaderIndex == 0 ? 2 : 0;
    evict(raftServer, firstTarget, false);
    assertThat(raftServer.getLivePeers()).hasSize(2);

    // 2 -> 1 would drop below quorum: must be refused without force.
    final int secondTarget = pickRemainingNonLeader(raftServer, leaderIndex, firstTarget);
    Assertions.assertThatThrownBy(() -> raftServer.removePeer(peerIdForIndex(secondTarget)))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("quorum");

    // The cluster configuration is untouched by the refused removal, so the peer is still a member.
    assertThat(raftServer.getLivePeers()).hasSize(2);

    // With force=true the same removal proceeds.
    evict(raftServer, secondTarget, true);
    assertThat(raftServer.getLivePeers()).hasSize(1);
  }

  /**
   * Removes a peer from the group and records that it is no longer a replica. Recorded only once the removal
   * has returned: a refused removal leaves the peer a member, and excluding it from the teardown check would
   * hide exactly the divergence the refusal exists to prevent.
   * <p>
   * Returning is also enough to make what follows deterministic rather than a race: {@code removePeer} drives
   * Ratis's blocking {@code admin().setConfiguration(...)} and returns only on a successful reply, so the new
   * configuration is committed by then. A write issued after this call cannot still reach the evicted peer,
   * and the {@code getLivePeers()} assertions the methods above make immediately afterwards read committed
   * membership rather than a request in flight.
   */
  private void evict(final RaftHAServer raftServer, final int serverIndex, final boolean force) {
    if (force)
      raftServer.removePeer(peerIdForIndex(serverIndex), true);
    else
      raftServer.removePeer(peerIdForIndex(serverIndex));
    evictedServers.add(serverIndex);
  }

  private int pickRemainingNonLeader(final RaftHAServer raftServer, final int leaderIndex, final int alreadyRemoved) {
    final String localPeer = raftServer.getLocalPeerId().toString();
    for (int i = 0; i < getServerCount(); i++) {
      if (i == leaderIndex || i == alreadyRemoved)
        continue;
      if (!peerIdForIndex(i).equals(localPeer))
        return i;
    }
    throw new IllegalStateException("No remaining non-leader peer to remove");
  }

  @Test
  void removePeerThrowsForUnknownPeer() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raftServer = getRaftPlugin(leaderIndex).getRaftHAServer();
    Assertions.assertThatThrownBy(() -> raftServer.removePeer("nonexistent"))
        .isInstanceOf(ConfigurationException.class);
  }

  @Test
  void registerPeerDisplayNameUpdatesExistingPeer() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raftServer = getRaftPlugin(leaderIndex).getRaftHAServer();
    final RaftPeerId localId = raftServer.getLocalPeerId();

    raftServer.registerPeerDisplayName(localId, "frankfurt");
    assertThat(raftServer.getPeerDisplayName(localId)).startsWith("frankfurt");
  }

  @Test
  void registerPeerDisplayNameIgnoresBlankName() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raftServer = getRaftPlugin(leaderIndex).getRaftHAServer();
    final RaftPeerId localId = raftServer.getLocalPeerId();
    final String before = raftServer.getPeerDisplayName(localId);

    raftServer.registerPeerDisplayName(localId, null);
    raftServer.registerPeerDisplayName(localId, "");
    assertThat(raftServer.getPeerDisplayName(localId)).isEqualTo(before);
  }
}
