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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9446: the snapshot swap decided "this entry belongs to the installer, not to the database" with
 * {@code name.startsWith(".snapshot")}, but {@code LocalSchema.checkValidBucketName} accepts a bucket named
 * {@code .snapshotItems}, and that bucket's component files are named after it. Each test drives one of the swap sites
 * that used the prefix with such a file and checks it is handled as database data: moved to the backup by the swap,
 * cleared by a rollback, and seen as a live entry by the legacy recovery classification.
 */
class Issue9446SnapshotPrefixedBucketTest {

  private static final String PREFIXED_BUCKET_FILE = ".snapshotItems_0.1.65536.v0.bucket";

  @BeforeEach
  void snapshotsOpen() {
    SnapshotInstaller.snapshotOpensForTesting = path -> true;
  }

  @AfterEach
  void restoreSnapshotOpenProof() {
    SnapshotInstaller.snapshotOpensForTesting = null;
  }

  /** BACKING_UP: the original bucket file goes to the backup, so it is neither left beside the snapshot nor lost on rollback. */
  @Test
  void swapMovesAPrefixedBucketFileToTheBackup(@TempDir final Path tempDir) throws Exception {
    final Path dbDir = tempDir.resolve("mydb");
    final Path newDir = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path backupDir = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    Files.createDirectories(newDir);
    Files.writeString(dbDir.resolve("schema.json"), "old");
    Files.writeString(dbDir.resolve(PREFIXED_BUCKET_FILE), "old-bucket");
    Files.writeString(newDir.resolve("schema.json"), "new");
    Files.writeString(newDir.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    SnapshotInstaller.atomicSwap(dbDir, newDir, backupDir);

    assertThat(dbDir.resolve(PREFIXED_BUCKET_FILE)).as("a bucket the snapshot does not have must not survive the swap")
        .doesNotExist();
    assertThat(backupDir.resolve(PREFIXED_BUCKET_FILE)).hasContent("old-bucket");
    assertThat(dbDir.resolve("schema.json")).hasContent("new");
  }

  /** ROLLING_BACK: clearLiveDatabaseFiles removes the failed snapshot's bucket file before the backup is restored. */
  @Test
  void rollbackClearsAPrefixedBucketFileOfTheFailedSnapshot(@TempDir final Path databasesDir) throws Exception {
    final Path dbDir = databasesDir.resolve("mydb");
    final Path backupDir = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    Files.createDirectories(backupDir);
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE), "ROLLING_BACK");
    Files.writeString(dbDir.resolve("schema.json"), "new");
    Files.writeString(dbDir.resolve(PREFIXED_BUCKET_FILE), "snapshot-only-bucket");
    Files.writeString(backupDir.resolve("schema.json"), "old");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(dbDir.resolve(PREFIXED_BUCKET_FILE)).as("a bucket only the failed snapshot had must not survive the rollback")
        .doesNotExist();
    assertThat(dbDir.resolve("schema.json")).hasContent("old");
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
    assertThat(backupDir).doesNotExist();
  }

  /**
   * Legacy recovery, phase 1 killed before the bucket file was backed up: the live directory still holds that original,
   * so it is a live entry and the layout is a phase-1 crash, rolled back to the original database. Treated as installer
   * state, the directory looked empty and the swap was completed with the stale bucket file left beside the snapshot.
   */
  @Test
  void legacyRecoveryCountsAPrefixedBucketFileAsALiveEntry(@TempDir final Path databasesDir) throws Exception {
    final Path dbDir = databasesDir.resolve("mydb");
    final Path newDir = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path backupDir = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    Files.createDirectories(newDir);
    Files.createDirectories(backupDir);
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    Files.writeString(dbDir.resolve(PREFIXED_BUCKET_FILE), "old-bucket");
    Files.writeString(backupDir.resolve("schema.json"), "old");
    Files.writeString(newDir.resolve("schema.json"), "new");
    Files.writeString(newDir.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(dbDir.resolve("schema.json")).hasContent("old");
    assertThat(dbDir.resolve(PREFIXED_BUCKET_FILE)).hasContent("old-bucket");
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
    assertThat(newDir).doesNotExist();
    assertThat(backupDir).doesNotExist();
  }

  @Test
  void aPrefixedBucketFileIsNotASnapshotMachineryEntry() {
    assertThat(SnapshotInstaller.isSnapshotMachineryEntryName(PREFIXED_BUCKET_FILE)).isFalse();
    assertThat(SnapshotInstaller.isSnapshotMachineryEntryName(".snapshot")).isFalse();
    assertThat(SnapshotInstaller.isSnapshotMachineryEntryName(".snapshot-newer")).isFalse();
  }

  /**
   * Guard: every {@code .snapshot...} name the installer declares, file or directory, is one the swap sites know, so a
   * new marker or staging directory added without being listed fails here rather than being moved into the backup.
   */
  @Test
  void everySnapshotInstallerEntryIsAMachineryEntry() throws Exception {
    final List<String> names = new ArrayList<>();
    for (final Field field : SnapshotInstaller.class.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers()) || field.getType() != String.class)
        continue;
      field.setAccessible(true);
      final String value = (String) field.get(null);
      if ((field.getName().startsWith("SNAPSHOT_") && (field.getName().endsWith("_FILE") || field.getName().endsWith("_DIR")))
          || (value != null && value.startsWith(".snapshot")))
        names.add(value);
    }

    assertThat(names).contains(SnapshotInstaller.SNAPSHOT_NEW_DIR, SnapshotInstaller.SNAPSHOT_BACKUP_DIR,
        SnapshotInstaller.SNAPSHOT_ORPHANS_DIR, SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    assertThat(names).allSatisfy(name -> assertThat(SnapshotInstaller.isSnapshotMachineryEntryName(name)).as(name).isTrue());
  }
}
