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
package com.arcadedb.server.backup;

import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5028 defect 4: tiered retention must never delete the most recent
 * backup, otherwise the just-completed backup is discarded seconds after creation.
 */
class BackupRetentionNewestBackupTest {
  private static final DateTimeFormatter BACKUP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
  private static final String            DATABASE_NAME = "testdb";

  @TempDir
  Path tempDir;

  private BackupRetentionManager retentionManager;
  private File                   dbBackupDir;

  @BeforeEach
  void setUp() {
    retentionManager = new BackupRetentionManager(tempDir.toString());
    dbBackupDir = new File(tempDir.toFile(), DATABASE_NAME);
    dbBackupDir.mkdirs();
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(tempDir.toFile());
  }

  /**
   * Three backups on the same day at different hours, with a daily-only tier of 1. The daily tier
   * keeps the OLDEST member of the day bucket, so without the fix the two newer backups (including
   * the newest restore point) would be deleted immediately. The newest must always survive.
   */
  @Test
  void newestBackupSurvivesTieredDailyRetention() throws Exception {
    // Fixed date at noon so all three files always fall in the same day bucket (a floating
    // LocalDateTime.now() near midnight could split them across two days and skew the assertions).
    final LocalDateTime now = LocalDateTime.of(2026, 7, 15, 12, 0, 0);

    final File oldest = createBackupFile(now.minusHours(2));
    final File middle = createBackupFile(now.minusHours(1));
    final File newest = createBackupFile(now);

    final DatabaseBackupConfig config = new DatabaseBackupConfig(DATABASE_NAME);
    final DatabaseBackupConfig.RetentionConfig retention = new DatabaseBackupConfig.RetentionConfig();
    final DatabaseBackupConfig.TieredConfig tiered = new DatabaseBackupConfig.TieredConfig();
    tiered.setHourly(0);
    tiered.setDaily(1);
    tiered.setWeekly(0);
    tiered.setMonthly(0);
    tiered.setYearly(0);
    retention.setTiered(tiered);
    config.setRetention(retention);

    retentionManager.registerDatabase(DATABASE_NAME, config);

    retentionManager.applyRetention(DATABASE_NAME);

    // The newest backup must never be deleted, regardless of tier bucketing.
    assertThat(newest.exists()).as("newest backup must survive retention").isTrue();
    // The daily tier still keeps its (oldest) bucket member.
    assertThat(oldest.exists()).as("oldest daily-tier member kept").isTrue();
    // The middle one is the only expendable file.
    assertThat(middle.exists()).as("middle backup pruned").isFalse();
  }

  private File createBackupFile(final LocalDateTime timestamp) throws IOException {
    final String filename = DATABASE_NAME + "-backup-" + timestamp.format(BACKUP_FORMAT) + ".zip";
    final File file = new File(dbBackupDir, filename);
    Files.write(file.toPath(), "test backup content".getBytes());
    return file;
  }
}
