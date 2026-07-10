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

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a READ-ONLY statement routed through {@code Database.command()} (i.e. the
 * {@code /api/v1/command} endpoint) honors the read-consistency header on a follower, exactly like
 * {@code Database.query()} does. Before the fix, command() never invoked the read barrier, so a
 * SELECT sent to /api/v1/command with LINEARIZABLE/READ_YOUR_WRITES silently degraded to EVENTUAL -
 * this is precisely how the original Jepsen stale read slipped through.
 */
class RaftCommandReadConsistencyIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void readOnlyCommandHonorsReadConsistencyOnFollower() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("CmdCtx"))
        leaderDb.getSchema().createVertexType("CmdCtx");
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < 25; i++) {
        final MutableVertex v = leaderDb.newVertex("CmdCtx");
        v.set("index", i);
        v.save();
      }
    });

    final long bookmark = leaderRaft.getLastAppliedIndex();

    final Database followerDb = getServerDatabase(followerIndex, getDatabaseName());

    // READ_YOUR_WRITES via command(): the barrier must wait until the follower applied the bookmark.
    try {
      RaftReplicatedDatabase.applyReadConsistencyContext(
          Database.READ_CONSISTENCY.READ_YOUR_WRITES, bookmark);
      final var rs = followerDb.command("sql", "SELECT count(*) as cnt FROM CmdCtx");
      assertThat(rs.hasNext()).isTrue();
      final long count = rs.next().getProperty("cnt");
      assertThat(count).as("READ_YOUR_WRITES command() read must see all 25 committed rows").isEqualTo(25);
    } finally {
      RaftReplicatedDatabase.removeReadConsistencyContext();
    }

    // LINEARIZABLE via command(): the follower must contact the leader (ReadIndex) and catch up.
    try {
      RaftReplicatedDatabase.applyReadConsistencyContext(
          Database.READ_CONSISTENCY.LINEARIZABLE, -1);
      final var rs = followerDb.command("sql", "SELECT count(*) as cnt FROM CmdCtx");
      assertThat(rs.hasNext()).isTrue();
      final long count = rs.next().getProperty("cnt");
      assertThat(count).as("LINEARIZABLE command() read must see all 25 committed rows").isEqualTo(25);
    } finally {
      RaftReplicatedDatabase.removeReadConsistencyContext();
    }
  }
}
