/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static com.arcadedb.server.ha.raft.Issue10993SizedOnFirstVectorHATest.DIMENSIONS;
import static com.arcadedb.server.ha.raft.Issue10993SizedOnFirstVectorHATest.TYPE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10993: an index that takes its dimensions from its first vector, across a failover. The node that becomes leader
 * decides, or refuses, the size of the vectors written from then on: it must hold the size the old leader decided, also when
 * it never searched the index, and an index nobody sized yet must be sized by the first vector the new leader receives.
 * <p>
 * Keeps a fresh cluster per test ({@link BaseRaftHATest}); each test stops the leader for good.
 */
@Tag("slow")
class Issue10993SizedOnFirstVectorFailoverIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  @Test
  void theNewLeaderHoldsTheSizeTheOldOneDecided() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    Issue10993SizedOnFirstVectorHATest.createUnsizedIndex(leader);
    Issue10993SizedOnFirstVectorHATest.insert(leader, 0, 10);
    waitForAllServers();

    getServer(leaderIndex).stop();
    final int newLeaderIndex = waitForNewLeader(leaderIndex);
    assertThat(newLeaderIndex).as("a new leader must be elected").isGreaterThanOrEqualTo(0).isNotEqualTo(leaderIndex);
    final Database newLeader = getServerDatabase(newLeaderIndex, getDatabaseName());

    // the new leader never searched the index: a vector of another length is refused as by any sized index
    assertThatThrownBy(() -> newLeader.transaction(() -> newLeader.newVertex(TYPE_NAME).set("id", 100, "embedding", new float[DIMENSIONS / 2]).save()))
        .hasStackTraceContaining("Vector dimension does not match index dimension " + DIMENSIONS);

    Issue10993SizedOnFirstVectorHATest.insert(newLeader, 10, 5);
    final RID expected = recordOf(newLeader, 12);
    final RID found = Issue10993SizedOnFirstVectorHATest.nearest(newLeader, Issue10993SizedOnFirstVectorHATest.embedding(12));
    assertThat(found).isEqualTo(expected);
  }

  @Test
  void anIndexNobodySizedIsSizedByTheFirstVectorOfTheNewLeader() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    Issue10993SizedOnFirstVectorHATest.createUnsizedIndex(getServerDatabase(leaderIndex, getDatabaseName()));
    waitForAllServers();

    getServer(leaderIndex).stop();
    final int newLeaderIndex = waitForNewLeader(leaderIndex);
    assertThat(newLeaderIndex).as("a new leader must be elected").isGreaterThanOrEqualTo(0).isNotEqualTo(leaderIndex);
    final Database newLeader = getServerDatabase(newLeaderIndex, getDatabaseName());

    Issue10993SizedOnFirstVectorHATest.insert(newLeader, 0, 10);
    waitForReplicationIsCompleted(newLeaderIndex);

    for (int server = 0; server < getServerCount(); server++) {
      if (server == leaderIndex)
        continue;
      final int serverIndex = server;
      final RID expected = withResyncRetry(serverIndex, db -> recordOf(db, 3));
      final RID found = withResyncRetry(serverIndex,
          db -> Issue10993SizedOnFirstVectorHATest.nearest(db, Issue10993SizedOnFirstVectorHATest.embedding(3)));
      assertThat(found).as("server %d must find record 3", serverIndex).isEqualTo(expected);
    }
  }

  private static RID recordOf(final Database db, final int id) {
    try (final ResultSet rs = db.query("sql", "SELECT @rid AS rid FROM " + TYPE_NAME + " WHERE id = ?", id)) {
      return rs.next().getProperty("rid");
    }
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // One server is stopped on purpose; the assertions above are about the survivors
  }

  @Override
  protected int[] getServerToCheck() {
    return startedServers();
  }

  private int waitForNewLeader(final int excludeIndex) {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      for (int i = 0; i < getServerCount(); i++) {
        if (i == excludeIndex)
          continue;
        final RaftHAPlugin plugin = getRaftPlugin(i);
        if (plugin != null && plugin.isLeader())
          return i;
      }
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return -1;
      }
    }
    return -1;
  }
}
