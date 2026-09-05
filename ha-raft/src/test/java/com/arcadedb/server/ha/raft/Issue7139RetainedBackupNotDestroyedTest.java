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
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7139: every {@code install()} began by deleting {@code .snapshot-backup}, which is
 * the documented reconciliation source for a previous install whose ROLLBACK failed and which deliberately left
 * its pending marker in place. A second install attempt therefore destroyed the only local copy of the database
 * before its own download had produced anything.
 * <p>
 * The sequence needs two consecutive install failures with the first one's rollback also failing, which is
 * narrow - but the condition that causes the first (a volume that has just filled, the #7037 scenario) is
 * exactly the condition that causes the second, which is what makes it plausible rather than hypothetical.
 * <p>
 * Startup recovery then read the resulting state - no completion marker, no backup - as "the download was
 * interrupted before the backup was created", cleaned up quietly and accepted a torn {@code dbPath} as a healthy
 * database, with nothing in the log saying so. That premise is only one of the two ways to reach that state, and
 * this covers both halves: the state is prevented, and it is reported if it arises another way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7139RetainedBackupNotDestroyedTest {

  private static final String DB = "mydb";

  // -------------------------------------------------------------------------------------------------
  // Part 1: a new install must not destroy a backup a failed rollback left behind
  // -------------------------------------------------------------------------------------------------

  /**
   * The #7139 sequence: attempt 1 tore {@code dbPath} and its rollback failed, leaving the marker and the backup.
   * Attempt 2 starts and its own download fails. The node must end up with the previous database restored, not
   * with a torn directory and no copy of anything.
   */
  @Test
  void aNewInstallReconcilesTheRetainedBackupInsteadOfDeletingIt(@TempDir final Path databasesDir) throws Exception {
    final Path dbPath = databasesDir.resolve(DB);
    final Path backup = dbPath.resolve(".snapshot-backup");
    Files.createDirectories(backup);
    // dbPath is torn: clearLiveDatabaseFiles ran, restoreBackup did not. Everything is in the backup.
    Files.writeString(backup.resolve("schema.json"), "{\"original\":true}");
    Files.writeString(backup.resolve("data.dat"), "original-data");
    Files.writeString(dbPath.resolve(".snapshot-pending"), "");

    // The second attempt cannot download either (no leader address resolves), exactly like the first.
    assertThatThrownBy(() -> SnapshotInstaller.install(DB, dbPath.toString(), () -> null, () -> null, null,
        serverThatCannotDownload(databasesDir)))
        .isInstanceOf(IOException.class);

    assertThat(dbPath.resolve("schema.json"))
        .as("the retained backup must be reconciled into the live directory, not deleted")
        .exists();
    assertThat(Files.readString(dbPath.resolve("data.dat"))).isEqualTo("original-data");
    assertThat(backup).doesNotExist();
    assertThat(dbPath.resolve(".snapshot-pending")).doesNotExist();
  }

  /**
   * When the reconciliation itself cannot complete, the install is refused and the backup stays: proceeding into
   * a download that may fail for the same reason would leave nothing behind at all.
   */
  @Test
  void anInstallIsRefusedWhenTheRetainedBackupCannotBeReconciled(@TempDir final Path databasesDir) throws Exception {
    final Path dbPath = databasesDir.resolve(DB);
    final Path backup = dbPath.resolve(".snapshot-backup");
    Files.createDirectories(backup.resolve("bucket"));
    Files.writeString(backup.resolve("bucket").resolve("0.bucket"), "backed-up");
    Files.writeString(backup.resolve("schema.json"), "{\"original\":true}");
    // A non-empty directory of the same name still in dbPath makes the restoring move fail, the way a
    // partially-cleared directory does when the volume that caused the first failure is still full.
    Files.createDirectories(dbPath.resolve("bucket"));
    Files.writeString(dbPath.resolve("bucket").resolve("leftover.bucket"), "torn");
    Files.writeString(dbPath.resolve(".snapshot-pending"), "");

    assertThatThrownBy(() -> SnapshotInstaller.install(DB, dbPath.toString(), () -> null, () -> null, null,
        serverThatCannotDownload(databasesDir)))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("only intact copy");

    assertThat(backup).as("the last copy of the database must survive a refused install").exists();
    assertThat(backup.resolve("schema.json")).exists();
    assertThat(dbPath.resolve(".snapshot-pending")).exists();
  }

  // -------------------------------------------------------------------------------------------------
  // Part 2: the "orphaned" recovery branch must prove its premise
  // -------------------------------------------------------------------------------------------------

  /**
   * No completion marker and no backup is reachable two ways, and only one of them leaves {@code dbDir} intact.
   * A torn directory must not be blessed as healthy and must keep its marker for an operator.
   */
  @Test
  void recoveryRefusesToBlessATornDirectoryAsOrphaned(@TempDir final Path databasesDir) throws Exception {
    final Path dbDir = databasesDir.resolve(DB);
    Files.createDirectories(dbDir);
    // What a failed rollback leaves: some files, but not a database - clearLiveDatabaseFiles took schema.json.
    Files.writeString(dbDir.resolve("leftover.bucket"), "half-cleared");
    Files.writeString(dbDir.resolve(".snapshot-pending"), "");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(dbDir.resolve(".snapshot-pending"))
        .as("a torn directory keeps its marker so the state is preserved rather than silently accepted")
        .exists();
    assertThat(dbDir.resolve("leftover.bucket")).exists();
  }

  /** Control: a genuinely intact database whose download was interrupted before any backup is still cleaned up. */
  @Test
  void recoveryStillCleansUpAGenuinelyOrphanedDownload(@TempDir final Path databasesDir) throws Exception {
    final Path dbDir = databasesDir.resolve(DB);
    final Path snapshotNew = dbDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotNew);
    Files.writeString(dbDir.resolve("schema.json"), "{\"original\":true}");
    Files.writeString(dbDir.resolve("data.dat"), "existing-data");
    Files.writeString(dbDir.resolve(".snapshot-pending"), "");
    Files.writeString(snapshotNew.resolve("partial.dat"), "incomplete");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(Files.readString(dbDir.resolve("data.dat"))).isEqualTo("existing-data");
    assertThat(snapshotNew).doesNotExist();
    assertThat(dbDir.resolve(".snapshot-pending")).doesNotExist();
  }

  /**
   * A server whose snapshot download can never succeed (no leader address resolves), with retries turned off so
   * the test does not pay the backoff.
   */
  private static ArcadeDBServer serverThatCannotDownload(final Path databasesDir) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 1);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.getDatabasesLock()).thenReturn(new Object());
    when(server.existsDatabase(DB)).thenReturn(false);
    return server;
  }
}
