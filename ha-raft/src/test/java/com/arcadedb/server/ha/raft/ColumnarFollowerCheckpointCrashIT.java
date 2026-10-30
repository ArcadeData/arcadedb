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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.log.LogManager;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A follower crash right after a Raft checkpoint, before any projection flush (plan 3, Task 22). A follower writes no
 * local WAL, so the pages its replicated applies marked are known only to the columnar trackers, and the restart replays
 * only the entries that follow the checkpoint. The checkpoint therefore persists the recovery set before it registers
 * its marker: the restarted follower takes those pages back, re-applies them, and its projection answers what GROUP BY
 * answers, with no rebuild.
 */
class ColumnarFollowerCheckpointCrashIT extends BaseRaftHATest {
  /** No time-driven flush while the test runs: the overlay is what the crash loses. */
  private static final long NO_FLUSH_MS = 3_600_000L;

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL.setValue(NO_FLUSH_MS);
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    config.setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, NO_FLUSH_MS);
  }

  @Test
  void aFollowerKilledAfterACheckpointRecoversItsProjectionWithoutARebuild() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());

    final ColumnarTestData data = new ColumnarTestData(22L);
    data.createSchema(leaderDb);
    waitForReplicationIsCompleted(follower);
    FacetOracle.awaitCaughtUp(getServerDatabase(follower, getDatabaseName()));
    final long flushesBefore = manager(follower).getStats().flushCount(ColumnarTestData.TYPE);

    data.insertAssets(leaderDb, 1_500);
    data.mutate(leaderDb, 400);
    waitForReplicationIsCompleted(follower);
    final Database followerDb = getServerDatabase(follower, getDatabaseName());
    FacetOracle.awaitCaughtUp(followerDb);
    assertThat(manager(follower).getStats().flushCount(ColumnarTestData.TYPE))
        .as("the fixture: the replicated rows are only in the follower's overlay").isEqualTo(flushesBefore);
    assertThat(followerDb.countType(ColumnarTestData.TYPE, true)).isEqualTo(leaderDb.countType(ColumnarTestData.TYPE, true));

    final long checkpoint = getRaftPlugin(follower).getRaftHAServer().getStateMachine().takeSnapshot();
    assertThat(checkpoint).as("the follower's checkpoint completes").isNotEqualTo(RaftLog.INVALID_LOG_INDEX);

    // kill -9: no close, no flush, no recovery set written by a close; the Raft log after the checkpoint is all a replay gets
    LogManager.instance().log(this, Level.INFO, "TEST: killing follower %d after the checkpoint at %d", follower, checkpoint);
    ((DatabaseInternal) followerDb).getEmbedded().kill();
    getServer(follower).stop();
    restartServer(follower);

    final Database restarted = getServerDatabase(follower, getDatabaseName());
    for (final String property : new String[] { "brand", "tags", "fileSize", "publishDate", "author" })
      FacetOracle.assertEquivalent(restarted, ColumnarTestData.TYPE, property, null);
    assertThat(manager(follower).getStats().rebuildsTotal()).as("crash recovery never rebuilds").isZero();
  }

  private ColumnarIndexManager manager(final int serverIndex) {
    return ((LocalDatabase) ((DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName())).getEmbedded())
        .getColumnarIndexManager();
  }
}
