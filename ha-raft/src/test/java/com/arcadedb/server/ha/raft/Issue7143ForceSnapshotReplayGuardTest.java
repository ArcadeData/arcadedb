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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7143 (first item): a {@code forceSnapshot} {@code INSTALL_DATABASE_ENTRY} had no
 * replay guard. The normal-create branch skips by existence, but existence is exactly what the force branch
 * ignores, so every Ratis replay of that entry - which happens on any restart, and on each in-place
 * {@code restartRatisIfNeeded} the health monitor performs - re-downloaded and re-installed the whole
 * database from the leader. The outcome was correct, the cost was not: a node restarting repeatedly re-pulls
 * a multi-GB database on each attempt and competes for the bandwidth the cluster needs to recover.
 * <p>
 * The guard uses the same per-database applied index {@code applyBootstrapFingerprintEntry} consults.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7143ForceSnapshotReplayGuardTest {

  private static final String DB   = "db-force";
  private static final long   ENTRY_INDEX = 42L;

  @TempDir
  private Path          serverDir;
  private ArcadeDBServer server;
  private LocalDatabase  localDb;
  private String         dbPath;

  @BeforeEach
  void setUp() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    server = new ArcadeDBServer(config);

    dbPath = serverDir.resolve(DB).toString();
    localDb = (LocalDatabase) new DatabaseFactory(dbPath).create();
    server.registerDatabase(DB, localDb);
  }

  @AfterEach
  void tearDown() {
    if (localDb != null && localDb.isOpen())
      localDb.close();
    if (dbPath != null)
      FileUtils.deleteRecursively(new File(dbPath));
  }

  @Test
  void aReplayedForceSnapshotEntryDoesNotPullTheDatabaseAgain() {
    final ArcadeStateMachine sm = newStateMachine();
    // A previous session applied this entry and Raft has replicated this database forward since.
    sm.writePersistedAppliedIndex(ENTRY_INDEX + 8, DB);

    assertThatCode(() -> sm.applyInstallDatabaseEntry(forceSnapshotEntry(), ENTRY_INDEX))
        .as("a replayed forceSnapshot entry must return without touching the snapshot download path")
        .doesNotThrowAnyException();
  }

  @Test
  void aForceSnapshotEntryThisNodeHasNotAppliedStillPullsTheDatabase() {
    final ArcadeStateMachine sm = newStateMachine();
    // Applied index BELOW the entry: this node has not run this install, so it must not be skipped.
    sm.writePersistedAppliedIndex(ENTRY_INDEX - 1, DB);

    // Reaching the download path is what the test asserts; it cannot complete here because this unit
    // test has no Raft server to resolve the leader from, and that failure is the proof it was reached.
    assertThatThrownBy(() -> sm.applyInstallDatabaseEntry(forceSnapshotEntry(), ENTRY_INDEX));
  }

  @Test
  void theGuardIsPerDatabaseSoAnotherDatabasesProgressDoesNotSuppressTheReinstall() {
    final ArcadeStateMachine sm = newStateMachine();
    // A co-located database advanced the GLOBAL index well past this entry. That must not suppress the
    // reinstall of a database with no per-database evidence of its own (issue #4824).
    sm.writePersistedAppliedIndex(ENTRY_INDEX + 100, "some-other-db");

    // Same reasoning as above: reaching the download path is what is asserted.
    assertThatThrownBy(() -> sm.applyInstallDatabaseEntry(forceSnapshotEntry(), ENTRY_INDEX));
  }

  private ArcadeStateMachine newStateMachine() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    return sm;
  }

  private static RaftLogEntryCodec.DecodedEntry forceSnapshotEntry() {
    return RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeInstallDatabaseEntry(DB, true));
  }
}
