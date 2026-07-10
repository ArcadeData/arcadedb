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
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for the never-seen database acquisition added in issue #4727. Exercises
 * {@link SnapshotInstaller#acquireNewDatabase} directly: a follower that does NOT have a database pulls it from
 * the leader over the real snapshot HTTP endpoint, stages it under the reserved {@code .acquire-<name>} directory,
 * publishes it with an atomic rename, and registers it - with no leftover staging/marker files.
 * <p>
 * This drives the new code deterministically (a direct synchronous call) rather than depending on Ratis to
 * trigger an InstallSnapshot, which is timing-dependent in-process.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotAcquireNewDatabaseIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // Skip the cross-node page comparator: this test deletes and re-acquires a database on one node, so a
    // page-level identity check at teardown is neither meaningful nor needed (logical equality is asserted).
  }

  @Test
  void followerAcquiresNeverSeenDatabaseFromLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    // Populate the database on the leader and let it replicate.
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("Item"))
        leaderDb.getSchema().createVertexType("Item");
      for (int i = 0; i < 30; i++)
        leaderDb.newVertex("Item").set("name", "v-" + i).save();
    });
    // Ensure the follower has fully replicated the database before we delete and re-acquire it.
    waitForReplicationIsCompleted(followerIndex);

    final long expected = leaderDb.countType("Item", true);
    assertThat(expected).isEqualTo(30);

    // Make the database never-seen on the follower: close + deregister + delete its on-disk copy.
    final DatabaseInternal followerDb = (DatabaseInternal) getServerDatabase(followerIndex, getDatabaseName());
    final String dbPath = followerDb.getDatabasePath();
    followerDb.getEmbedded().close();
    getServer(followerIndex).removeDatabase(getDatabaseName());
    FileUtils.deleteRecursively(new File(dbPath));
    assertThat(getServer(followerIndex).existsDatabase(getDatabaseName()))
        .as("database must be absent on the follower before acquisition").isFalse();

    // Acquire it from the leader via the new crash-safe path.
    final String leaderHttp = "localhost:" + getServer(leaderIndex).getHttpServer().getPort();
    final String token = getRaftPlugin(followerIndex).getRaftHAServer().getClusterToken();
    LogManager.instance().log(this, Level.INFO, "TEST: Acquiring never-seen database '%s' from leader %s",
        getDatabaseName(), leaderHttp);
    SnapshotInstaller.acquireNewDatabase(getDatabaseName(), () -> leaderHttp, () -> null, token,
        getServer(followerIndex));

    // The follower now has the database with all records, and no staging/marker files are left behind.
    assertThat(getServer(followerIndex).existsDatabase(getDatabaseName()))
        .as("follower should have acquired the never-seen database").isTrue();
    assertThat(getServerDatabase(followerIndex, getDatabaseName()).countType("Item", true))
        .as("acquired database should contain all records").isEqualTo(expected);

    final Path acquiredPath = Path.of(getServerDatabase(followerIndex, getDatabaseName()).getDatabasePath());
    final Path databasesDir = acquiredPath.getParent();
    assertThat(databasesDir.resolve(SnapshotInstaller.ACQUIRE_STAGING_PREFIX + getDatabaseName()))
        .as("reserved acquisition staging dir must be gone").doesNotExist();
    assertThat(acquiredPath.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
    assertThat(acquiredPath.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR)).doesNotExist();

    // The acquired copy must keep receiving replication: a subsequent write on the leader must reach the
    // newly-registered follower copy (the part that matters operationally, beyond the one-time pull).
    leaderDb.transaction(() -> leaderDb.newVertex("Item").set("name", "after-acquire").save());
    final long afterWrite = leaderDb.countType("Item", true);
    waitForReplicationIsCompleted(followerIndex);
    assertThat(getServerDatabase(followerIndex, getDatabaseName()).countType("Item", true))
        .as("a write after acquisition must replicate to the acquired follower copy").isEqualTo(afterWrite);
  }

  /**
   * Covers the concurrent-create fallback branch: when the database already exists locally (e.g. it materialised
   * via an INSTALL_DATABASE_ENTRY replay while a reconcile was deciding to acquire it), acquireNewDatabase must
   * fall back to the in-place {@code install} refresh rather than acquiring, and the database must remain present
   * with its data.
   */
  @Test
  void acquireOnAnExistingDatabaseRefreshesInsteadOfAcquiring() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("Item"))
        leaderDb.getSchema().createVertexType("Item");
      for (int i = 0; i < 15; i++)
        leaderDb.newVertex("Item").set("name", "v-" + i).save();
    });
    waitForReplicationIsCompleted(followerIndex);

    // The follower already has the database; acquireNewDatabase must detect this and refresh in place.
    assertThat(getServer(followerIndex).existsDatabase(getDatabaseName())).isTrue();

    final String leaderHttp = "localhost:" + getServer(leaderIndex).getHttpServer().getPort();
    final String token = getRaftPlugin(followerIndex).getRaftHAServer().getClusterToken();
    SnapshotInstaller.acquireNewDatabase(getDatabaseName(), () -> leaderHttp, () -> null, token,
        getServer(followerIndex));

    assertThat(getServer(followerIndex).existsDatabase(getDatabaseName()))
        .as("database must still be present after the refresh fallback").isTrue();
    assertThat(getServerDatabase(followerIndex, getDatabaseName()).countType("Item", true))
        .as("refreshed database should still contain all records").isEqualTo(leaderDb.countType("Item", true));

    final Path dbDir = Path.of(getServerDatabase(followerIndex, getDatabaseName()).getDatabasePath());
    assertThat(dbDir.getParent().resolve(SnapshotInstaller.ACQUIRE_STAGING_PREFIX + getDatabaseName()))
        .as("no acquisition staging dir should be created on the refresh path").doesNotExist();
  }
}
