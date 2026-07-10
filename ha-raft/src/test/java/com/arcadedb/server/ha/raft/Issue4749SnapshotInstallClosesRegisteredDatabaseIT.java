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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.*;

/**
 * Regression test for issue #4749.
 * <p>
 * In 26.6.1 the snapshot-install path ({@code ArcadeStateMachine.notifyInstallSnapshotFromLeader})
 * closed the follower's live database by calling {@code db.close()} on the
 * {@link ServerDatabase} wrapper. That wrapper rejects {@code close()} with
 * {@link UnsupportedOperationException} ("Embedded database taken from the server are shared and
 * therefore cannot be closed"), so a follower asked to do a full snapshot resync crashed on every
 * attempt, never rejoined the quorum, and the cluster lost write availability.
 * <p>
 * The fix routes the install through {@link SnapshotInstaller}, which closes the underlying embedded
 * instance ({@code getEmbedded().close()}) instead of the shared wrapper. This test pins both halves
 * of the contract: the wrapper still refuses {@code close()}, and installing a snapshot over an
 * <b>open, server-registered</b> database completes without that exception and leaves the database
 * registered, holding its data, and still replicating.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4749SnapshotInstallClosesRegisteredDatabaseIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    // 3 nodes so a majority (2) keeps the cluster writable while one follower is resynced.
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    // Keep the install fast: a single retry with a short backoff.
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 1);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 100L);
  }

  @Test
  @Timeout(120)
  void snapshotInstallOverOpenRegisteredDatabaseDoesNotThrow() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final String dbName = getDatabaseName();

    final Database leaderDb = getServerDatabase(leaderIndex, dbName);
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("Issue4749"))
        leaderDb.getSchema().createVertexType("Issue4749");
      for (int i = 0; i < 25; i++)
        leaderDb.newVertex("Issue4749").set("index", i).save();
    });
    assertClusterConsistency();

    final ArcadeDBServer followerServer = getServer(followerIndex);

    // Precondition that defined the bug: the follower's database is OPEN and registered, and the
    // ServerDatabase wrapper returned by the server forbids close(). The buggy install called this
    // exact method and crashed with UnsupportedOperationException.
    final ServerDatabase wrapper = followerServer.getDatabase(dbName);
    assertThat(followerServer.existsDatabase(dbName)).as("Follower DB is open and registered").isTrue();
    assertThatThrownBy(wrapper::close)
        .as("Closing the shared ServerDatabase wrapper is rejected (the bug's trigger)")
        .isInstanceOf(UnsupportedOperationException.class);

    final String dbPath = ((DatabaseInternal) wrapper).getDatabasePath();
    final String leaderHttpAddr = "localhost:" + getServer(leaderIndex).getHttpServer().getPort();
    final String token = getRaftPlugin(followerIndex).getRaftHAServer().getClusterToken();

    // The real install path against a live, registered database. On 26.6.1 this aborted with
    // UnsupportedOperationException; with the SnapshotInstaller rework it must complete cleanly.
    assertThatCode(() ->
        SnapshotInstaller.install(dbName, dbPath, leaderHttpAddr, null, token, followerServer))
        .as("Installing a snapshot over an open, registered database must not throw")
        .doesNotThrowAnyException();

    // The follower is left registered and serving with the leader's data, not closed/deregistered.
    assertThat(followerServer.existsDatabase(dbName)).as("Follower DB stays registered after install").isTrue();
    assertThat(getServerDatabase(followerIndex, dbName).countType("Issue4749", true))
        .as("Follower holds the leader's data after the snapshot install").isEqualTo(25);

    // Replication resumes after the install: writes committed on the leader reach the follower.
    leaderDb.transaction(() -> {
      for (int i = 25; i < 40; i++)
        leaderDb.newVertex("Issue4749").set("index", i).save();
    });
    assertClusterConsistency();
    assertThat(getServerDatabase(followerIndex, dbName).countType("Issue4749", true))
        .as("Follower receives writes committed after the snapshot install").isEqualTo(40);
  }
}
