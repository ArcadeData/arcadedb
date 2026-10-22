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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10426: {@code Issue7139RetainedBackupNotDestroyedTest} went red on the CI Linux runners
 * and stayed green on macOS. The test refuses an install whose retained backup cannot be restored and then asserts
 * that {@code .snapshot-backup/schema.json} is still there, but {@code restoreBackup} moved the entries back in the
 * order the filesystem lists them. APFS lists a directory sorted (so {@code bucket} failed before {@code schema.json}
 * was reached); ext4, overlayfs and tmpfs list {@code schema.json} first, so it was already in the live directory when
 * the {@code bucket} move failed.
 * <p>
 * The assertion was right and the code was fragile: a restore that fails part way must leave the backup holding the
 * entry that makes a directory a database, so that a half-restored directory is never mistaken for a loadable one and
 * whatever still has to move is plainly in the backup. The schema files therefore go back LAST, on every filesystem.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10426RestoreBackupOrderTest {

  private static final String DB = "mydb";

  @AfterEach
  void clearProgressHook() {
    SnapshotInstaller.swapProgressForTesting = null;
  }

  /** The order is asserted off the progress hook, so it holds whatever order the filesystem lists the backup in. */
  @Test
  void theSchemaFilesAreTheLastEntriesMovedBackFromTheRetainedBackup(@TempDir final Path databasesDir) throws Exception {
    final Path dbPath = databasesDir.resolve(DB);
    final Path backup = dbPath.resolve(".snapshot-backup");
    Files.createDirectories(backup.resolve("bucket"));
    Files.writeString(backup.resolve("bucket").resolve("0.bucket"), "backed-up");
    // Names on both sides of "schema" in lexicographic order, so neither a sorted nor a reverse listing puts them last.
    Files.writeString(backup.resolve("aaa.dict"), "first");
    Files.writeString(backup.resolve("zzz.bucket"), "last");
    Files.writeString(backup.resolve("schema.prev.json"), "{\"original\":\"prev\"}");
    Files.writeString(backup.resolve("schema.json"), "{\"original\":true}");
    Files.writeString(dbPath.resolve(".snapshot-pending"), "");

    final List<String> restored = new ArrayList<>();
    SnapshotInstaller.swapProgressForTesting = point -> {
      if (point.startsWith("RESTORING:"))
        restored.add(point.substring("RESTORING:".length()));
    };

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(restored).as("every backed-up entry went back").containsExactlyInAnyOrder("aaa.dict", "bucket", "zzz.bucket",
        "schema.prev.json", "schema.json");
    assertThat(restored.subList(restored.size() - 2, restored.size()))
        .as("the schema files are what makes the directory a database, so they are the last to move back")
        .containsExactly("schema.prev.json", "schema.json");
    assertThat(backup).doesNotExist();
    assertThat(dbPath.resolve(".snapshot-pending")).doesNotExist();
    assertThat(dbPath.resolve("schema.json")).hasContent("{\"original\":true}");
    assertThat(dbPath.resolve("bucket").resolve("0.bucket")).hasContent("backed-up");
  }

  /**
   * A restore that cannot finish leaves the schema file in the backup. Failing on the very first entry moved is what a
   * sorted listing produced by accident; here the failure is forced after every other entry has gone back, which is
   * what an unsorted listing did to {@code schema.json} before the fix.
   */
  @Test
  void aRestoreThatFailsAfterTheOtherEntriesLeavesTheSchemaInTheBackup(@TempDir final Path databasesDir) throws Exception {
    final Path dbPath = databasesDir.resolve(DB);
    final Path backup = dbPath.resolve(".snapshot-backup");
    Files.createDirectories(backup.resolve("bucket"));
    Files.writeString(backup.resolve("bucket").resolve("0.bucket"), "backed-up");
    Files.writeString(backup.resolve("zzz.bucket"), "last");
    Files.writeString(backup.resolve("schema.json"), "{\"original\":true}");
    // Same trick as Issue7139: a non-empty directory with a backed-up name makes that one move fail.
    Files.createDirectories(dbPath.resolve("bucket"));
    Files.writeString(dbPath.resolve("bucket").resolve("leftover.bucket"), "torn");
    Files.writeString(dbPath.resolve(".snapshot-pending"), "");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(backup.resolve("schema.json")).as("the schema file only leaves the backup once everything else is back")
        .exists();
    assertThat(dbPath.resolve("schema.json")).doesNotExist();
    assertThat(dbPath.resolve(".snapshot-pending")).exists();
  }
}
