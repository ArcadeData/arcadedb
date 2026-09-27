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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.server.ServerDatabase;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8490, over the real wire: a leader-driven resync order that reaches a follower which has caught up since
 * the order was decided must be refused with the database left in place, while an order that still describes the
 * follower's state is carried out. {@code Issue8490StaleForcedResyncTest} covers the decision and each check in
 * isolation; this drives {@link RaftHAServer#requestRemoteResync} against a live follower's
 * {@code POST /api/v1/cluster/resync/{database}}, so the order's JSON, the handler's reading of it and the leader's
 * reading of the 409 are all exercised together.
 */
@Tag("slow")
class Issue8490StaleResyncOrderIT extends BaseRaftHATest {

  private static final String TYPE  = "Issue8490";
  private static final int    COUNT = 50;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void aCaughtUpFollowerRefusesAStaleOrderAndKeepsItsDatabase() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = (leaderIndex + 1) % getServerCount();

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(TYPE))
        leaderDb.getSchema().createVertexType(TYPE);
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < COUNT; i++)
        leaderDb.newVertex(TYPE).set("idx", i).save();
    });
    assertClusterConsistency();

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final String followerHttpAddr = leaderRaft.getPeerHttpAddress(RaftPeerId.valueOf(peerIdForIndex(replicaIndex)));
    assertThat(followerHttpAddr).as("leader must resolve the follower's HTTP address").isNotNull();

    final Database before = embeddedOf(replicaIndex);

    // The reported shape: decided while the follower looked never-appended (-1), against a commit index the follower
    // has since reached. The follower is fully caught up (assertClusterConsistency above), so it is not behind.
    final StalledResyncOrder stale = new StalledResyncOrder(leaderRaft.getCurrentTerm(), -1, leaderRaft.getCommitIndex());
    assertThatThrownBy(() -> leaderRaft.requestRemoteResync(followerHttpAddr, getDatabaseName(),
        leaderRaft.getClusterToken(), false, stale))
        .as("a caught-up follower must decline the order, not carry it out")
        .isInstanceOf(StalledResyncDeclinedException.class)
        .hasMessageContaining("not behind");

    final Database after = embeddedOf(replicaIndex);
    assertThat(after).as("a declined order must not replace the follower's database").isSameAs(before);
    assertThat(after.isOpen()).isTrue();
    assertThat(after.countType(TYPE, true)).isEqualTo((long) COUNT);

    // Control: an order that still describes the follower - the leader's commit index well past what it has applied
    // - is carried out, so the refusal above is the stale check and not a broken wire format.
    final StalledResyncOrder current = new StalledResyncOrder(leaderRaft.getCurrentTerm(), -1,
        leaderRaft.getCommitIndex() + 1_000_000);
    leaderRaft.requestRemoteResync(followerHttpAddr, getDatabaseName(), leaderRaft.getClusterToken(), false, current);

    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType(TYPE, true)).isEqualTo((long) COUNT);
    assertClusterConsistency();
  }

  /** The database instance a resync replaces: the embedded one under the server and Raft wrappers. */
  private Database embeddedOf(final int serverIndex) {
    return ((ServerDatabase) getServerDatabase(serverIndex, getDatabaseName())).getWrappedDatabaseInstance().getEmbedded();
  }
}
