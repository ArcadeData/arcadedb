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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link SnapshotInstaller#recoverPendingSnapshotSwaps(Path)} using synthetic
 * filesystem state. No HTTP downloads or Raft clusters are involved.
 */
class SnapshotSwapRecoveryTest {

  @Test
  void recoveryCompletesInterruptedSwap(@TempDir final Path databasesDir) throws Exception {
    // Simulate: download completed, swap started (backup created) but process crashed before cleanup
    final Path dbDir = databasesDir.resolve("mydb");
    final Path snapshotNew = dbDir.resolve(".snapshot-new");
    final Path snapshotBackup = dbDir.resolve(".snapshot-backup");

    Files.createDirectories(snapshotNew);
    Files.createDirectories(snapshotBackup);
    Files.writeString(dbDir.resolve(".snapshot-pending"), "");
    Files.writeString(snapshotNew.resolve(".snapshot-complete"), "");
    Files.writeString(snapshotNew.resolve("data.dat"), "new-snapshot-data");
    Files.writeString(snapshotBackup.resolve("data.dat"), "old-data");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    // .snapshot-new should have been swapped into dbDir
    assertThat(dbDir.resolve("data.dat")).exists();
    assertThat(Files.readString(dbDir.resolve("data.dat"))).isEqualTo("new-snapshot-data");
    // Cleanup: backup, marker, and .snapshot-complete should be gone
    assertThat(snapshotBackup).doesNotExist();
    assertThat(dbDir.resolve(".snapshot-pending")).doesNotExist();
    assertThat(dbDir.resolve(".snapshot-complete")).doesNotExist();
  }

  @Test
  void recoveryRollsBackIncompleteDownload(@TempDir final Path databasesDir) throws Exception {
    // Simulate: download was interrupted (no .snapshot-complete), backup exists
    final Path dbDir = databasesDir.resolve("mydb");
    final Path snapshotNew = dbDir.resolve(".snapshot-new");
    final Path snapshotBackup = dbDir.resolve(".snapshot-backup");

    Files.createDirectories(snapshotNew);
    Files.createDirectories(snapshotBackup);
    Files.writeString(dbDir.resolve(".snapshot-pending"), "");
    Files.writeString(snapshotNew.resolve("partial.dat"), "incomplete");
    Files.writeString(snapshotBackup.resolve("data.dat"), "original-data");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    // Backup should be restored as the live directory
    assertThat(dbDir.resolve("data.dat")).exists();
    assertThat(Files.readString(dbDir.resolve("data.dat"))).isEqualTo("original-data");
    // Cleanup
    assertThat(snapshotNew).doesNotExist();
    assertThat(snapshotBackup).doesNotExist();
    assertThat(dbDir.resolve(".snapshot-pending")).doesNotExist();
  }

  @Test
  void recoveryCleansUpOrphanedNewDir(@TempDir final Path databasesDir) throws Exception {
    // Simulate: download interrupted before backup was created
    final Path dbDir = databasesDir.resolve("mydb");
    final Path snapshotNew = dbDir.resolve(".snapshot-new");

    Files.createDirectories(dbDir);
    Files.createDirectories(snapshotNew);
    Files.writeString(dbDir.resolve(".snapshot-pending"), "");
    Files.writeString(dbDir.resolve("data.dat"), "existing-data");
    Files.writeString(snapshotNew.resolve("partial.dat"), "incomplete");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    // Existing data should be untouched
    assertThat(Files.readString(dbDir.resolve("data.dat"))).isEqualTo("existing-data");
    // Cleanup
    assertThat(snapshotNew).doesNotExist();
    assertThat(dbDir.resolve(".snapshot-pending")).doesNotExist();
  }

  @Test
  void noMarkerNoAction(@TempDir final Path databasesDir) throws Exception {
    // Clean database directory with no markers - should be a no-op
    final Path dbDir = databasesDir.resolve("mydb");
    Files.createDirectories(dbDir);
    Files.writeString(dbDir.resolve("data.dat"), "untouched");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(Files.readString(dbDir.resolve("data.dat"))).isEqualTo("untouched");
  }

  @Test
  void multipleDatabasesRecoveredIndependently(@TempDir final Path databasesDir) throws Exception {
    // db1: needs swap completion; db2: needs rollback
    final Path db1 = databasesDir.resolve("db1");
    final Path db1New = db1.resolve(".snapshot-new");
    final Path db1Backup = db1.resolve(".snapshot-backup");
    Files.createDirectories(db1New);
    Files.createDirectories(db1Backup);
    Files.writeString(db1.resolve(".snapshot-pending"), "");
    Files.writeString(db1New.resolve(".snapshot-complete"), "");
    Files.writeString(db1New.resolve("data.dat"), "db1-new");
    Files.writeString(db1Backup.resolve("data.dat"), "db1-old");

    final Path db2 = databasesDir.resolve("db2");
    final Path db2New = db2.resolve(".snapshot-new");
    final Path db2Backup = db2.resolve(".snapshot-backup");
    Files.createDirectories(db2New);
    Files.createDirectories(db2Backup);
    Files.writeString(db2.resolve(".snapshot-pending"), "");
    // No .snapshot-complete - incomplete download
    Files.writeString(db2New.resolve("partial.dat"), "incomplete");
    Files.writeString(db2Backup.resolve("data.dat"), "db2-original");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    // db1: swap completed
    assertThat(Files.readString(db1.resolve("data.dat"))).isEqualTo("db1-new");
    assertThat(db1Backup).doesNotExist();

    // db2: rolled back
    assertThat(Files.readString(db2.resolve("data.dat"))).isEqualTo("db2-original");
    assertThat(db2New).doesNotExist();
  }

  @Test
  void incompleteExtractionWithoutMarkerIsDiscarded(@TempDir final Path databasesDir) throws Exception {
    // Simulate: .snapshot-new exists (extraction started) but no .snapshot-complete marker inside it,
    // meaning extraction was interrupted before completing. No backup exists either.
    final Path dbDir = databasesDir.resolve("mydb");
    final Path snapshotNew = dbDir.resolve(".snapshot-new");

    Files.createDirectories(dbDir);
    Files.createDirectories(snapshotNew);
    Files.writeString(dbDir.resolve(".snapshot-pending"), "");
    Files.writeString(dbDir.resolve("data.dat"), "existing-data");
    // Incomplete extraction: some files written but no .snapshot-complete
    Files.writeString(snapshotNew.resolve("partial.dat"), "incomplete-extraction");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    // The .snapshot-new directory must be discarded (no marker = not safe to apply)
    assertThat(snapshotNew).doesNotExist();
    // Existing database data is untouched
    assertThat(dbDir.resolve("data.dat")).exists();
    assertThat(Files.readString(dbDir.resolve("data.dat"))).isEqualTo("existing-data");
    // Pending marker cleaned up
    assertThat(dbDir.resolve(".snapshot-pending")).doesNotExist();
  }

  @Test
  void recoveryHandlesSwapCompletedButCleanupIncomplete(@TempDir final Path databasesDir) throws Exception {
    // Simulate: swap was completed (.snapshot-new renamed to live) but .snapshot-pending marker
    // and leftover .snapshot-new dir (with .snapshot-complete) still exist. No backup.
    final Path dbDir = databasesDir.resolve("mydb");
    final Path snapshotNew = dbDir.resolve(".snapshot-new");

    Files.createDirectories(dbDir);
    Files.createDirectories(snapshotNew);
    Files.writeString(dbDir.resolve(".snapshot-pending"), "");
    Files.writeString(snapshotNew.resolve(".snapshot-complete"), "");
    Files.writeString(dbDir.resolve("data.dat"), "live-data");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    // Live data should be untouched, leftovers cleaned up
    assertThat(Files.readString(dbDir.resolve("data.dat"))).isEqualTo("live-data");
    assertThat(snapshotNew).doesNotExist();
    assertThat(dbDir.resolve(".snapshot-pending")).doesNotExist();
  }
}
