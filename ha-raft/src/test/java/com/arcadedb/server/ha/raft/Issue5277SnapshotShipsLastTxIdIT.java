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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.server.ServerDatabase;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5277: the leader-shipped snapshot did not include the
 * {@code last-tx-id.bin} recency marker (it ships config + schema + component files only), and the
 * marker is otherwise written on a clean close - so a follower that received its database via
 * snapshot install and was then force-killed reported {@code lastTxId=-1} for fully intact data and
 * was needlessly re-installed from a full snapshot at the next cold bootstrap.
 * <p>
 * This test forces a genuine snapshot install on a lagging replica (log compaction while it is
 * down) and asserts the installed database directory contains the marker with the leader's
 * transaction recency, so the recency signal survives even an immediate force-kill.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue5277SnapshotShipsLastTxIdIT extends BaseRaftHATest {

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    // Snapshot every 10 log entries so the leader compacts past the stopped replica's log position,
    // forcing a full snapshot install (not log replay) on its restart.
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_THRESHOLD, 10L);
  }

  @Test
  void installedSnapshotCarriesLastTxIdMarker() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int replicaIndex = (leaderIndex + 1) % getServerCount();

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("LastTxIdShip"))
        leaderDb.getSchema().createVertexType("LastTxIdShip");
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < 20; i++)
        leaderDb.newVertex("LastTxIdShip").set("phase", 1).set("i", i).save();
    });
    assertClusterConsistency();

    // Stop the replica and write past the snapshot threshold so its log position is compacted away.
    getServer(replicaIndex).stop();
    leaderDb.transaction(() -> {
      for (int i = 0; i < 100; i++)
        leaderDb.newVertex("LastTxIdShip").set("phase", 2).set("i", i).save();
    });

    // Restart: the replica must receive a full snapshot install.
    restartServer(replicaIndex);
    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("LastTxIdShip", true))
        .as("replica must have been reinstalled with all records").isEqualTo(120);

    // The installed database directory must contain the shipped recency marker: without it, a
    // force-kill right now (no clean close, no WAL) would make this intact database report
    // lastTxId=-1 at the next bootstrap and be re-installed from scratch (issue #5277).
    final ServerDatabase replicaDb = (ServerDatabase) getServerDatabase(replicaIndex, getDatabaseName());
    final LocalDatabase replicaLocal = (LocalDatabase) replicaDb.getEmbedded();
    final File marker = new File(replicaLocal.getDatabasePath(), TransactionManager.LAST_TX_ID_FILE_NAME);
    assertThat(marker).as("snapshot install must ship the last-tx-id.bin recency marker").exists();

    final long shipped;
    try (final DataInputStream in = new DataInputStream(new FileInputStream(marker))) {
      shipped = in.readLong();
    }
    assertThat(shipped).as("shipped recency must reflect the leader's committed transactions").isGreaterThan(0);

    // And the live counter agrees the database has committed history.
    assertThat(replicaLocal.getLastTransactionId()).isGreaterThan(0);
  }
}
