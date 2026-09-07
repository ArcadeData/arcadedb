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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7221: the {@code forceSnapshot} replay guard added for #7143 returned early on
 * the persisted applied index alone. That index lives in {@code <databaseDirectory>/.raft/applied-index}, a
 * sibling of the per-database directories rather than a file inside them, so deleting a database's directory
 * leaves its entry in the map untouched and the guard skips the reinstall of a database that is no longer
 * there. The wipe-and-resync recovery an operator reaches for - stop the node, delete the bad copy, start it
 * again - therefore left the database gone for good.
 * <p>
 * The guard now has to prove the premise it states, the same way the normal-create arm of the very same
 * method does: skip only when the database is actually present on this node.
 */
class Issue7221ForceSnapshotGuardMissingDatabaseTest {

  private static final String DB          = "db-wiped";
  private static final long   ENTRY_INDEX = 42L;

  @TempDir
  private Path           serverDir;
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
  void aReplayedForceSnapshotEntryStillReinstallsWhenTheDatabaseIsGone() {
    final ArcadeStateMachine before = newStateMachine();
    // A previous session ran this install and Raft replicated the database forward from there.
    before.writePersistedAppliedIndex(ENTRY_INDEX + 8, DB);

    wipeTheLocalCopy();

    // The restart: a fresh state machine reads the applied index back from disk.
    final ArcadeStateMachine after = newStateMachine();
    assertThat(after.readPersistedAppliedIndex(DB))
        .as(".raft/applied-index is a sibling of the database directories, so wiping one leaves this behind")
        .isEqualTo(ENTRY_INDEX + 8);

    // Reaching the download path is what this asserts; it cannot complete in a unit test with no Raft server
    // to resolve a leader from, and that failure is the proof the guard did not swallow the entry. The guard
    // firing would instead return normally, which is exactly what this test used to observe.
    assertThatThrownBy(() -> after.applyInstallDatabaseEntry(forceSnapshotEntry(), ENTRY_INDEX),
        "an applied index at or beyond the entry must not skip the reinstall of an absent database");
  }

  @Test
  void theGuardStillSkipsTheReDownloadWhenTheDatabaseIsStillThere() {
    final ArcadeStateMachine sm = newStateMachine();
    sm.writePersistedAppliedIndex(ENTRY_INDEX + 8, DB);

    // Nothing was wiped here, so #7143's guard must still suppress the multi-GB re-download on replay.
    assertThatCode(() -> sm.applyInstallDatabaseEntry(forceSnapshotEntry(), ENTRY_INDEX))
        .as("a replayed entry for a database that IS present must still return without re-downloading it")
        .doesNotThrowAnyException();
  }

  /**
   * The operator's wipe-and-resync recovery: stop the node, remove the bad copy, start it again. Closing and
   * deregistering is what a restart leaves behind - {@code loadDatabases()} finds no directory to register.
   */
  private void wipeTheLocalCopy() {
    localDb.close();
    server.removeDatabase(DB);
    FileUtils.deleteRecursively(new File(dbPath));
    assertThat(server.existsDatabase(DB)).isFalse();
    assertThat(new File(dbPath)).doesNotExist();
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
