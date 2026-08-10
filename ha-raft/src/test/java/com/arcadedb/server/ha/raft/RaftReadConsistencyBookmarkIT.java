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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RaftReadConsistencyBookmarkIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void followerWaitsForAppliedIndexBeforeRead() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final RaftHAServer followerRaft = getRaftPlugin(followerIndex).getRaftHAServer();

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("BookmarkTest"))
        leaderDb.getSchema().createVertexType("BookmarkTest");
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++)
        leaderDb.newVertex("BookmarkTest").set("index", i).save();
    });

    final long bookmark = leaderRaft.getLastAppliedIndex();
    assertThat(bookmark).isGreaterThan(0);

    followerRaft.waitForAppliedIndex(bookmark);

    final Database followerDb = getServerDatabase(followerIndex, getDatabaseName());
    final long count = followerDb.countType("BookmarkTest", true);
    assertThat(count).isEqualTo(50);
  }

  @Test
  void readConsistencyContextAppliesOnFollowerQueries() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("ContextTest"))
        leaderDb.getSchema().createVertexType("ContextTest");
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < 20; i++)
        leaderDb.newVertex("ContextTest").set("index", i).save();
    });

    final long bookmark = leaderRaft.getLastAppliedIndex();

    // Resolved and retried through withResyncRetry(), not a handle cached before this point: a snapshot-
    // reinstall resync can close and reinstall the follower's database at any time, and a stale handle then
    // throws DatabaseIsClosedException, which reads like infrastructure noise rather than the resync it is
    // (issue #5977).
    final long count = withResyncRetry(followerIndex, followerDb -> {
      try {
        RaftReplicatedDatabase.applyReadConsistencyContext(
            Database.READ_CONSISTENCY.READ_YOUR_WRITES, bookmark);
        final var rs = followerDb.query("sql", "SELECT count(*) as cnt FROM ContextTest");
        assertThat(rs.hasNext()).isTrue();
        final long cnt = rs.next().getProperty("cnt");
        return cnt;
      } finally {
        RaftReplicatedDatabase.removeReadConsistencyContext();
      }
    });
    assertThat(count).isEqualTo(20);
  }
}
