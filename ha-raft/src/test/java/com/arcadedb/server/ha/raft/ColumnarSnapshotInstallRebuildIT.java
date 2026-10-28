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
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestAccess;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A snapshot never carries the projection: installing one on a follower drops the follower's projection and rebuilds it with reason
 * {@code snapshot_install} over the installed files, then the Raft apply keeps it current (plan 3, Task 23). The install goes through
 * the real {@link SnapshotInstaller} path against the open, registered database, as {@code Issue4749SnapshotInstallClosesRegisteredDatabaseIT}.
 */
class ColumnarSnapshotInstallRebuildIT extends ColumnarHATestSupport {
  /** No time-driven flush: the generations the test compares move only by the flushes it asks for, and by the rebuild. */
  private static final long NO_FLUSH_MS = 3_600_000L;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL.setValue(NO_FLUSH_MS);
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, NO_FLUSH_MS);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 1);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 100L);
  }

  @Test
  @Timeout(180)
  void snapshotInstallTriggersRebuildWithReason() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final ColumnarTestData data = new ColumnarTestData(2305L);
    data.deleteRatio = 0.1;
    data.createSchema(database(leader));
    // every column published before the first record: building them is the empty publish, not a rebuild
    assertClusterConsistency();
    awaitCaughtUpEverywhere();
    data.insertAssets(database(leader), 2_000);
    data.mutate(database(leader), 500);
    assertClusterConsistency();
    assertEquivalentOn(follower);
    assertThat(manager(follower).getStats().rebuildsTotal())
        .as("indexes created on an empty type rebuild nothing (%s)", rebuildsByReason(follower)).isZero();

    installSnapshot(leader, follower);

    assertEquivalentOn(follower);
    assertThat(manager(follower).getStats().rebuilds(ColumnarTestData.TYPE, "snapshot_install"))
        .as("the install rebuilds the follower's projection once, with its reason").isEqualTo(1);
    assertThat(manager(follower).getStats().rebuildsTotal()).as("the reopened database rebuilt for nothing else (%s)",
        rebuildsByReason(follower)).isEqualTo(1);

    data.insertAssets(database(leader), 500);
    data.mutate(database(leader), 300);
    assertClusterConsistency();
    assertEquivalentEverywhere();
    assertThat(manager(follower).getStats().rebuildsTotal()).as("writes after the install are applied, not rebuilt").isEqualTo(1);
  }

  @Test
  @Timeout(180)
  void snapshotInstallMidApply() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final ColumnarTestData data = new ColumnarTestData(2306L);
    data.deleteRatio = 0.1;
    data.createSchema(database(leader));
    data.insertAssets(database(leader), 1_500);
    awaitCaughtUpOn(follower);
    manager(follower).flushNow();
    data.mutate(database(leader), 300);
    awaitCaughtUpOn(follower);
    manager(follower).flushNow();
    final int bucketId = assetBucketId(follower);
    final long generationBefore = ColumnarTestAccess.publishedGeneration(manager(follower), bucketId);
    assertThat(generationBefore).as("the fixture: two flushes moved the follower's projection past its first generations")
        .isGreaterThan(2);

    final ColumnarIndexManager heldManager = manager(follower);
    final AutoCloseable hold = heldManager.suspendApply();
    try {
      data.insertAssets(database(leader), 800);
      data.mutate(database(leader), 300);
      assertClusterConsistency();
      assertThat(heldManager.awaitCaughtUp(0)).as("the fixture: the follower holds replicated pages it did not apply").isFalse();

      installSnapshot(leader, follower);
    } finally {
      hold.close();
    }

    assertThat(manager(follower)).as("the install reopened the database: the held manager and its overlay are gone")
        .isNotSameAs(heldManager);
    assertEquivalentOn(follower);
    assertThat(manager(follower).getStats().rebuilds(ColumnarTestData.TYPE, "snapshot_install")).isEqualTo(1);
    assertThat(ColumnarTestAccess.publishedGeneration(manager(follower), bucketId))
        .as("the projection restarted from the rebuild, no state of the replaced files survives").isLessThan(generationBefore);

    data.mutate(database(leader), 300);
    assertClusterConsistency();
    assertEquivalentEverywhere();
  }

  private void installSnapshot(final int leader, final int follower) throws Exception {
    final String dbName = getDatabaseName();
    final ArcadeDBServer followerServer = getServer(follower);
    final String dbPath = ((DatabaseInternal) followerServer.getDatabase(dbName)).getDatabasePath();
    final String leaderHttpAddress = "localhost:" + getServer(leader).getHttpServer().getPort();
    final String token = getRaftPlugin(follower).getRaftHAServer().getClusterToken();
    SnapshotInstaller.install(dbName, dbPath, leaderHttpAddress, null, token, followerServer);
    assertThat(followerServer.existsDatabase(dbName)).as("the follower's database stays registered after the install").isTrue();
    final Database installed = database(follower);
    assertThat(installed.countType(ColumnarTestData.TYPE, true)).isEqualTo(database(leader).countType(ColumnarTestData.TYPE, true));
  }
}
