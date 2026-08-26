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
package com.arcadedb.integration.backup;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Two backups of the same database that resolve to the same archive path must not both write to it: the
 * {@code exists()} pre-check is a TOCTOU, so both used to open their own stream on the same file and interleave their
 * output into a single, unreadable archive - or, when one of them failed, delete the other one's finished work
 * (issue #6753).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6753ConcurrentBackupTest extends TestHelper {
  private static final String BACKUP_DIRECTORY = "target/backups/issue6753";
  private static final String ARCHIVE_NAME     = "same-name-backup.zip";

  @BeforeEach
  void populateAndCleanBackupDirectory() {
    FileUtils.deleteRecursively(Path.of(BACKUP_DIRECTORY).toFile());

    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("payload", Type.STRING);
      for (int i = 0; i < 20_000; i++)
        database.newDocument("Doc").set("payload", "payload-" + i).save();
    });
  }

  @AfterEach
  void cleanBackupDirectory() {
    FileUtils.deleteRecursively(Path.of(BACKUP_DIRECTORY).toFile());
  }

  @Test
  void twoBackupsRacingOnTheSameArchiveNameLeaveOneReadableArchive() throws Exception {
    final CyclicBarrier startTogether = new CyclicBarrier(2);
    final AtomicInteger succeeded = new AtomicInteger();
    final AtomicInteger refused = new AtomicInteger();

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<?>[] runs = new Future<?>[2];
      for (int i = 0; i < 2; i++)
        runs[i] = executor.submit(() -> {
          startTogether.await();
          try {
            new Backup(database, ARCHIVE_NAME).setDirectory(BACKUP_DIRECTORY).setVerboseLevel(0).backupDatabase();
            succeeded.incrementAndGet();
          } catch (final BackupException e) {
            refused.incrementAndGet();
          }
          return null;
        });

      for (final Future<?> run : runs)
        run.get(120, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }

    // EXACTLY ONE OF THE TWO OWNS THE PATH: THE OTHER HAS TO BE TURNED AWAY, NOT LET INTO THE SAME FILE
    assertThat(succeeded.get()).isEqualTo(1);
    assertThat(refused.get()).isEqualTo(1);

    final File archive = new File(BACKUP_DIRECTORY, ARCHIVE_NAME);
    assertThat(archive).exists();
    assertThat(archive.length()).isGreaterThan(0);

    // AND WHAT IT LEFT BEHIND IS A REAL ARCHIVE, NOT TWO INTERLEAVED ONES
    try (final ZipFile zip = new ZipFile(archive)) {
      assertThat(zip.size()).isGreaterThan(0);
    }
  }

  @Test
  void aBackupRejectedBeforeItWritesLeavesNoEmptyArchiveBehind() {
    database.begin();
    database.newDocument("Doc").set("payload", "uncommitted").save();
    try {
      assertThatThrownBy(
          () -> new Backup(database, ARCHIVE_NAME).setDirectory(BACKUP_DIRECTORY).setVerboseLevel(0).backupDatabase())
          .isInstanceOf(BackupException.class)
          .rootCause()
          .hasMessageContaining("Transaction in progress");
    } finally {
      database.rollback();
    }

    // THE PATH IS CLAIMED BY CREATING THE FILE, SO IT HAS TO BE CLAIMED AFTER THE CHECKS THAT CAN REFUSE THE BACKUP:
    // AN EMPTY ARCHIVE IS ONE RETENTION WOULD COUNT AND AN OPERATOR WOULD MISTAKE FOR A BACKUP
    assertThat(new File(BACKUP_DIRECTORY, ARCHIVE_NAME)).doesNotExist();
  }

  @Test
  void anExistingArchiveIsNeverDeletedByAnotherBackupThatFindsItInTheWay() throws Exception {
    Files.createDirectories(Path.of(BACKUP_DIRECTORY));
    final Path finished = Path.of(BACKUP_DIRECTORY, ARCHIVE_NAME);
    Files.writeString(finished, "an archive somebody else already finished");

    assertThatThrownBy(
        () -> new Backup(database, ARCHIVE_NAME).setDirectory(BACKUP_DIRECTORY).setVerboseLevel(0).backupDatabase())
        .isInstanceOf(BackupException.class)
        .rootCause()
        .hasMessageContaining("already exist");

    assertThat(Files.readString(finished)).isEqualTo("an archive somebody else already finished");
  }
}
