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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.backup.BackupCoordinator;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8035 end to end: a {@code drop database} issued on the leader must wait, on a follower, for a backup that
 * follower is running, instead of closing and deleting the database under it - while the leader's own drop, whose
 * request thread holds the DROP slot for the whole of its local apply, must not wait on itself.
 */
class Issue8035ReplicatedDropWaitsForPeerBackupIT extends BaseRaftHATest {

  private static final String DB_NAME = "Issue8035Drop";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    // Far longer than the leader's own wait for its local apply: if that apply waited on the verb's slot, the drop
    // would fail on the leader instead of completing.
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS, TimeUnit.MINUTES.toMillis(10));
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // The only database this test creates is dropped by it.
  }

  @Test
  void aDropIssuedOnTheLeaderWaitsForAFollowersBackup() throws Exception {
    final int leaderIndex = findLeaderIndex();
    final ArcadeDBServer leader = getServer(leaderIndex);
    new ServerControlPlane(leader).createDatabase(DB_NAME);
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            if (!getServer(i).existsDatabase(DB_NAME))
              return false;
          return true;
        });

    final int backingUpIndex = (leaderIndex + 1) % getServerCount();
    final int otherFollowerIndex = (leaderIndex + 2) % getServerCount();
    final ArcadeDBServer backingUp = getServer(backingUpIndex);
    final BackupCoordinator backupSlot = backingUp.getBackupCoordinator();
    assertThat(backupSlot.begin(DB_NAME)).as("the follower's backup takes its slot").isTrue();

    boolean backupRunning = true;
    try {
      // The verb returns once the LEADER's copy is gone; run as the HTTP handler would, and do not block on it
      // before checking the follower - the verb never waits for followers.
      final CompletableFuture<Void> drop = CompletableFuture.runAsync(() -> new ServerControlPlane(leader).dropDatabase(DB_NAME));
      drop.get(2, TimeUnit.MINUTES);
      assertThat(leader.existsDatabase(DB_NAME)).isFalse();

      // The follower with nothing running drops it; the one running a backup keeps it until the backup ends.
      Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
          .until(() -> !getServer(otherFollowerIndex).existsDatabase(DB_NAME));
      assertThat(backingUp.existsDatabase(DB_NAME)).as("the drop must not tear down the follower's backup").isTrue();
      assertThat(backingUp.getDatabase(DB_NAME).isOpen()).isTrue();

      backupSlot.end(DB_NAME);
      backupRunning = false;

      Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
          .until(() -> !backingUp.existsDatabase(DB_NAME));
      assertThat(backupSlot.isInProgress(DB_NAME)).as("the follower's apply released the slot it took").isFalse();
      assertThat(leader.getBackupCoordinator().isInProgress(DB_NAME)).isFalse();
    } finally {
      if (backupRunning)
        backupSlot.end(DB_NAME);
    }
  }
}
