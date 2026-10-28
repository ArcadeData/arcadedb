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
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A follower's projection is fed by the Raft apply ({@code TransactionManager.applyChanges} marks the pages it writes), never by a
 * replicated projection file, and converges to what GROUP BY answers on that follower (plan 3, Task 23).
 */
class ColumnarFollowerConvergenceIT extends ColumnarHATestSupport {

  @Test
  @Timeout(180)
  void followersConvergeThroughRaftApply() throws Exception {
    final int leader = requireLeaderIndex();
    final Database leaderDb = database(leader);
    final ColumnarTestData data = new ColumnarTestData(2301L);
    data.createSchema(leaderDb);
    assertClusterConsistency();

    data.insertAssets(leaderDb, 5_000);
    // updates (and the fixture's share of inserts), then deletes only
    data.mutate(leaderDb, 1_000);
    data.deleteRatio = 1.0;
    data.mutate(leaderDb, 500);
    assertClusterConsistency();

    assertEquivalentEverywhere();
    for (int i = 0; i < getServerCount(); i++) {
      if (i == leader)
        continue;
      assertThat(manager(i).getStats().applyPagesCount(ColumnarTestData.TYPE))
          .as("follower %d projected the pages its Raft apply wrote", i).isPositive();
      assertThat(database(i).countType(ColumnarTestData.TYPE, true)).isEqualTo(leaderDb.countType(ColumnarTestData.TYPE, true));
    }
  }

  @Test
  @Timeout(180)
  void createIndexOnFollowerSchemaEntryBuilds() throws Exception {
    final int leader = requireLeaderIndex();
    final Database leaderDb = database(leader);
    final ColumnarTestData data = new ColumnarTestData(2302L);
    data.columnarIndexes = false;
    data.createSchema(leaderDb);
    data.insertAssets(leaderDb, 3_000);
    data.mutate(leaderDb, 500);
    assertClusterConsistency();

    leaderDb.command("sql", "CREATE INDEX ON " + ColumnarTestData.TYPE + " (brand) COLUMNAR");
    assertClusterConsistency();

    for (int i = 0; i < getServerCount(); i++) {
      awaitCaughtUpOn(i);
      FacetOracle.assertEquivalent(database(i), ColumnarTestData.TYPE, "brand", null);
      assertThat(manager(i).getStats().rebuilds(ColumnarTestData.TYPE, "create"))
          .as("server %d builds the new column with one create rebuild (%s)", i, i == leader ? "ColumnarIndex.build" : "ADD_COLUMN")
          .isEqualTo(1);
      assertThat(manager(i).getStats().rebuildsTotal()).as("server %d rebuilds for nothing else (%s)", i, rebuildsByReason(i))
          .isEqualTo(1);
    }
  }

  @Test
  @Timeout(180)
  void followerRestartRecoversWithoutRebuild() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database leaderDb = database(leader);
    final ColumnarTestData data = new ColumnarTestData(2303L);
    data.deleteRatio = 0.1;
    data.createSchema(leaderDb);
    // every column published before the first record: building them is the empty publish, not a rebuild
    assertClusterConsistency();
    awaitCaughtUpEverywhere();
    data.insertAssets(leaderDb, 2_000);
    data.mutate(leaderDb, 500);
    assertClusterConsistency();
    assertEquivalentOn(follower);
    assertThat(manager(follower).getStats().rebuildsTotal())
        .as("indexes created on an empty type rebuild nothing (%s)", rebuildsByReason(follower)).isZero();

    restartServer(follower);
    // writes while the restarted follower catches up through the Raft log
    data.insertAssets(database(leader), 500);
    data.mutate(database(leader), 300);
    assertClusterConsistency();

    assertEquivalentOn(follower);
    assertThat(manager(follower).getStats().rebuildsTotal())
        .as("a clean restart recovers the projection without a rebuild (%s)", rebuildsByReason(follower)).isZero();
    assertEquivalentOn(leader);
  }
}
