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
 * Verifies that reads on follower nodes return data consistent with
 * the leader's committed state (linearizable reads via leader lease).
 */
class RaftReadConsistencyIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void followerReadsAreConsistentWithLeaderWrites() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    // Find a follower
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Write on the leader
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("ReadConsistency"))
        leaderDb.getSchema().createVertexType("ReadConsistency");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableVertex v = leaderDb.newVertex("ReadConsistency");
        v.set("index", i);
        v.save();
      }
    });

    // Wait for replication
    assertClusterConsistency();

    // Read from follower
    final Database followerDb = getServerDatabase(followerIndex, getDatabaseName());
    final long count = followerDb.countType("ReadConsistency", true);
    assertThat(count).as("Follower should see all 100 records written on leader").isEqualTo(100);

    // Write more on leader, verify follower catches up
    leaderDb.transaction(() -> {
      for (int i = 100; i < 200; i++) {
        final MutableVertex v = leaderDb.newVertex("ReadConsistency");
        v.set("index", i);
        v.save();
      }
    });

    assertClusterConsistency();

    final long countAfter = followerDb.countType("ReadConsistency", true);
    assertThat(countAfter).as("Follower should see all 200 records after second batch").isEqualTo(200);
  }

  /**
   * Regression guard for the stale LINEARIZABLE follower-read bug: {@code fetchReadIndex()} must
   * return the leader's quorum-confirmed commit index, NOT {@link org.apache.ratis.protocol.RaftClientReply#getLogIndex()}
   * which is always {@code 0} on a read-only reply in Apache Ratis 3.2.x. A read index of 0 made the
   * subsequent apply-wait a no-op and let followers serve stale state.
   */
  @Test
  void fetchReadIndexReturnsLeaderCommitIndexNotZero() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Produce a healthy non-zero commit index with some committed writes.
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("ReadIndexProbe"))
        leaderDb.getSchema().createVertexType("ReadIndexProbe");
      for (int i = 0; i < 10; i++)
        leaderDb.newVertex("ReadIndexProbe").set("i", i).save();
    });
    assertClusterConsistency();

    final long commitIndex = leaderRaft.getCommitIndex();
    assertThat(commitIndex).as("commit index after writes must be positive").isPositive();

    // The whole point of the fix: the read index tracks the leader commit index, never 0.
    final long readIndex = leaderRaft.fetchReadIndex(true);
    assertThat(readIndex)
        .as("fetchReadIndex() must return the leader commit index, not getLogIndex()==0")
        .isGreaterThanOrEqualTo(commitIndex);

    // And the follower must also obtain the same (non-zero) safe read index from the leader.
    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final RaftHAServer followerRaft = getRaftPlugin(followerIndex).getRaftHAServer();
    final long followerReadIndex = followerRaft.fetchReadIndex(false);
    assertThat(followerReadIndex)
        .as("follower fetchReadIndex() must return the leader's non-zero commit index")
        .isGreaterThanOrEqualTo(commitIndex);
  }
}
