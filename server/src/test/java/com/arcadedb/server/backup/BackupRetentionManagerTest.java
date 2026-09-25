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
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BackupRetentionManagerTest {
  private static final DateTimeFormatter BACKUP_FORMAT        = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
  /** What every release before #6753 wrote is the one above; this is what the server writes now. */
  private static final DateTimeFormatter BACKUP_FORMAT_MILLIS = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmssSSS");
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

  @Test
  void maxFilesRetention() throws Exception {
    // Create 10 backup files
    final List<File> files = createBackupFiles(10, LocalDateTime.now().minusDays(10));

    // Configure retention for max 5 files
    final DatabaseBackupConfig config = new DatabaseBackupConfig(DATABASE_NAME);
    final DatabaseBackupConfig.RetentionConfig retention = new DatabaseBackupConfig.RetentionConfig();
    retention.setMaxFiles(5);
    config.setRetention(retention);

    retentionManager.registerDatabase(DATABASE_NAME, config);

    // Apply retention
    final int deleted = retentionManager.applyRetention(DATABASE_NAME);

    // Should delete 5 oldest files
    assertThat(deleted).isEqualTo(5);
    assertThat(retentionManager.getBackupCount(DATABASE_NAME)).isEqualTo(5);

    // Verify the 5 newest files are kept
    for (int i = 5; i < 10; i++)
      assertThat(files.get(i).exists()).isTrue();

    // Verify the 5 oldest files are deleted
    for (int i = 0; i < 5; i++)
      assertThat(files.get(i).exists()).isFalse();
  }

  @Test
  void tieredRetentionHourly() throws Exception {
    // Create backups for the last 48 hours (one per hour)
    final LocalDateTime now = LocalDateTime.now().withMinute(0).withSecond(0).withNano(0);
    final List<File> files = new ArrayList<>();

    for (int i = 47; i >= 0; i--) {
      final LocalDateTime timestamp = now.minusHours(i);
      files.add(createBackupFile(timestamp));
    }

    // Configure tiered retention: keep 24 hourly
    final DatabaseBackupConfig config = new DatabaseBackupConfig(DATABASE_NAME);
    final DatabaseBackupConfig.RetentionConfig retention = new DatabaseBackupConfig.RetentionConfig();
    final DatabaseBackupConfig.TieredConfig tiered = new DatabaseBackupConfig.TieredConfig();
    tiered.setHourly(24);
    tiered.setDaily(0);
    tiered.setWeekly(0);
    tiered.setMonthly(0);
    tiered.setYearly(0);
    retention.setTiered(tiered);
    config.setRetention(retention);

    retentionManager.registerDatabase(DATABASE_NAME, config);

    // Apply retention
    final int deleted = retentionManager.applyRetention(DATABASE_NAME);

    // Should keep 24 most recent hourly backups
    assertThat(deleted).isEqualTo(24);
    assertThat(retentionManager.getBackupCount(DATABASE_NAME)).isEqualTo(24);
  }

  @Test
  void tieredRetentionDaily() throws Exception {
    // Create backups for the last 14 days (one per day)
    final LocalDateTime now = LocalDateTime.now().withHour(2).withMinute(0).withSecond(0).withNano(0);
    final List<File> files = new ArrayList<>();

    for (int i = 13; i >= 0; i--) {
      final LocalDateTime timestamp = now.minusDays(i);
      files.add(createBackupFile(timestamp));
    }

    // Configure tiered retention: keep 7 daily
    final DatabaseBackupConfig config = new DatabaseBackupConfig(DATABASE_NAME);
    final DatabaseBackupConfig.RetentionConfig retention = new DatabaseBackupConfig.RetentionConfig();
    final DatabaseBackupConfig.TieredConfig tiered = new DatabaseBackupConfig.TieredConfig();
    tiered.setHourly(0);
    tiered.setDaily(7);
    tiered.setWeekly(0);
    tiered.setMonthly(0);
    tiered.setYearly(0);
    retention.setTiered(tiered);
    config.setRetention(retention);

    retentionManager.registerDatabase(DATABASE_NAME, config);

    // Apply retention
    final int deleted = retentionManager.applyRetention(DATABASE_NAME);

    // Should keep 7 most recent daily backups
    assertThat(deleted).isEqualTo(7);
    assertThat(retentionManager.getBackupCount(DATABASE_NAME)).isEqualTo(7);
  }

  @Test
  void tieredRetentionCombined() throws Exception {
    // Create multiple backups:
    // - Last 48 hours: one per hour
    // - Previous days: one per day for 7 more days
    final LocalDateTime now = LocalDateTime.now().withMinute(0).withSecond(0).withNano(0);
    final List<File> files = new ArrayList<>();

    // Last 48 hours
    for (int i = 47; i >= 0; i--)
      files.add(createBackupFile(now.minusHours(i)));

    // Previous 7 days (at 3 AM)
    for (int i = 3; i <= 9; i++)
      files.add(createBackupFile(now.minusDays(i).withHour(3)));

    // Configure combined tiered retention
    final DatabaseBackupConfig config = new DatabaseBackupConfig(DATABASE_NAME);
    final DatabaseBackupConfig.RetentionConfig retention = new DatabaseBackupConfig.RetentionConfig();
    final DatabaseBackupConfig.TieredConfig tiered = new DatabaseBackupConfig.TieredConfig();
    tiered.setHourly(12); // Last 12 hours
    tiered.setDaily(7);   // Last 7 days
    tiered.setWeekly(0);
    tiered.setMonthly(0);
    tiered.setYearly(0);
    retention.setTiered(tiered);
    config.setRetention(retention);

    retentionManager.registerDatabase(DATABASE_NAME, config);

    // Apply retention
    retentionManager.applyRetention(DATABASE_NAME);

    // Should keep at most 12 hourly + 7 daily (with some overlap)
    final int remaining = retentionManager.getBackupCount(DATABASE_NAME);
    assertThat(remaining).isLessThanOrEqualTo(19); // Max if no overlap
    assertThat(remaining).isGreaterThan(0);
  }

  @Test
  void noRetentionConfigured() {
    final DatabaseBackupConfig config = new DatabaseBackupConfig(DATABASE_NAME);
    // No retention configured

    retentionManager.registerDatabase(DATABASE_NAME, config);

    // Should not delete anything
    final int deleted = retentionManager.applyRetention(DATABASE_NAME);
    assertThat(deleted).isEqualTo(0);
  }

  @Test
  void emptyBackupDirectory() {
    final DatabaseBackupConfig config = new DatabaseBackupConfig(DATABASE_NAME);
    final DatabaseBackupConfig.RetentionConfig retention = new DatabaseBackupConfig.RetentionConfig();
    retention.setMaxFiles(5);
    config.setRetention(retention);

    retentionManager.registerDatabase(DATABASE_NAME, config);

    // Should handle empty directory gracefully
    final int deleted = retentionManager.applyRetention(DATABASE_NAME);
    assertThat(deleted).isEqualTo(0);
  }

  @Test
  void nonExistentDatabase() {
    // Should handle non-registered database
    final int deleted = retentionManager.applyRetention("non-existent-db");
    assertThat(deleted).isEqualTo(0);
  }

  @Test
  void getBackupSize() throws Exception {
    // Create some backup files
    createBackupFiles(3, LocalDateTime.now());

    final long totalSize = retentionManager.getBackupSizeBytes(DATABASE_NAME);
    assertThat(totalSize).isGreaterThan(0);
  }

  @Test
  void ignoreNonBackupFiles() throws Exception {
    // Create backup files
    createBackupFiles(5, LocalDateTime.now());

    // Create non-backup files
    new File(dbBackupDir, "readme.txt").createNewFile();
    new File(dbBackupDir, "config.json").createNewFile();

    final DatabaseBackupConfig config = new DatabaseBackupConfig(DATABASE_NAME);
    final DatabaseBackupConfig.RetentionConfig retention = new DatabaseBackupConfig.RetentionConfig();
    retention.setMaxFiles(3);
    config.setRetention(retention);

    retentionManager.registerDatabase(DATABASE_NAME, config);

    // Apply retention - should only count/delete backup files
    final int deleted = retentionManager.applyRetention(DATABASE_NAME);
    assertThat(deleted).isEqualTo(2);

    // Non-backup files should still exist
    assertThat(new File(dbBackupDir, "readme.txt").exists()).isTrue();
    assertThat(new File(dbBackupDir, "config.json").exists()).isTrue();
  }

  /**
   * Creates backup files with timestamps starting from the given date.
   */
  /**
   * The directory an upgraded server inherits: archives named by every release before #6753, at second precision,
   * next to the millisecond-precision ones it writes from now on. Retention decides what is still under management
   * by parsing these names, so a parser that read only one of the two conventions would quietly stop rotating the
   * other half - and nobody finds out until the disk is full.
   */
  @Test
  void retentionRotatesAcrossBothNamingConventions() throws Exception {
    final LocalDateTime oldest = LocalDateTime.now().minusDays(10);
    final List<File> files = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final LocalDateTime timestamp = oldest.plusHours(i);
      // ALTERNATING, SO NEITHER CONVENTION IS ENTIRELY IN THE KEPT HALF OR ENTIRELY IN THE DELETED ONE
      files.add(i % 2 == 0 ?
          createBackupFile(timestamp) :
          writeBackupFile(DATABASE_NAME + "-backup-" + timestamp.format(BACKUP_FORMAT_MILLIS) + ".zip"));
    }

    final DatabaseBackupConfig config = new DatabaseBackupConfig(DATABASE_NAME);
    final DatabaseBackupConfig.RetentionConfig retention = new DatabaseBackupConfig.RetentionConfig();
    retention.setMaxFiles(5);
    config.setRetention(retention);
    retentionManager.registerDatabase(DATABASE_NAME, config);

    assertThat(retentionManager.getBackupCount(DATABASE_NAME)).isEqualTo(10);

    // A NAME RETENTION CANNOT PARSE IS NOT MERELY UNSORTED, IT IS INVISIBLE: WERE THE FIVE MILLISECOND ONES
    // UNREADABLE, THE FIVE THAT REMAIN WOULD ALREADY BE WITHIN maxFiles AND THIS WOULD DELETE NOTHING
    assertThat(retentionManager.applyRetention(DATABASE_NAME)).isEqualTo(5);

    // AND THE SURVIVORS ARE THE FIVE MOST RECENT BY TIMESTAMP, ACROSS BOTH CONVENTIONS
    for (int i = 0; i < 5; i++)
      assertThat(files.get(i)).doesNotExist();
    for (int i = 5; i < 10; i++)
      assertThat(files.get(i)).exists();
  }

  /**
   * Issue #8298: the weekly bucket was keyed by the calendar year plus the ISO week number, so 2024-12-30 (ISO week 1
   * of 2025) collided with 2024-01-01 (ISO week 1 of 2024) and the tier kept the year-old archive in its place.
   */
  @Test
  void weeklyBucketsAreIsoWeeksAcrossTheYearBoundary() {
    assertThat(BackupRetentionManager.bucketStart(LocalDateTime.of(2024, 12, 30, 2, 0), ChronoUnit.WEEKS))
        .isNotEqualTo(BackupRetentionManager.bucketStart(LocalDateTime.of(2024, 1, 1, 2, 0), ChronoUnit.WEEKS))
        .isEqualTo(BackupRetentionManager.bucketStart(LocalDateTime.of(2025, 1, 1, 2, 0), ChronoUnit.WEEKS))
        .isEqualTo(LocalDateTime.of(2024, 12, 30, 0, 0));
    assertThat(BackupRetentionManager.bucketStart(LocalDateTime.of(2023, 1, 1, 2, 0), ChronoUnit.WEEKS))
        .isNotEqualTo(BackupRetentionManager.bucketStart(LocalDateTime.of(2023, 12, 26, 2, 0), ChronoUnit.WEEKS))
        .isEqualTo(LocalDateTime.of(2022, 12, 26, 0, 0));
  }

  @Test
  void weeklyTierKeepsTheRecentArchiveOfACollidingWeek() throws Exception {
    final File jan2024 = createBackupFile(LocalDateTime.of(2024, 1, 1, 2, 0));
    final File dec2024 = createBackupFile(LocalDateTime.of(2024, 12, 30, 2, 0));
    final File feb2025 = createBackupFile(LocalDateTime.of(2025, 2, 10, 2, 0));

    registerTiered(0, 0, 2, 0, 0);
    assertThat(retentionManager.applyRetention(DATABASE_NAME)).isEqualTo(1);

    assertThat(jan2024).doesNotExist();
    assertThat(dec2024).exists();
    assertThat(feb2025).exists();
  }

  @Test
  void weeklyTierKeepsTheMostRecentWeeksAcrossTheYearEnd() throws Exception {
    final File w2024_01 = createBackupFile(LocalDateTime.of(2024, 1, 1, 2, 0));
    final File w2024_52 = createBackupFile(LocalDateTime.of(2024, 12, 27, 2, 0));
    final File w2025_01 = createBackupFile(LocalDateTime.of(2024, 12, 30, 2, 0));
    final File w2025_02 = createBackupFile(LocalDateTime.of(2025, 1, 6, 2, 0));
    final File w2025_03 = createBackupFile(LocalDateTime.of(2025, 1, 13, 2, 0));
    final File w2025_04 = createBackupFile(LocalDateTime.of(2025, 1, 20, 2, 0));

    registerTiered(0, 0, 4, 0, 0);
    assertThat(retentionManager.applyRetention(DATABASE_NAME)).isEqualTo(2);

    assertThat(w2024_01).doesNotExist();
    assertThat(w2024_52).doesNotExist();
    assertThat(w2025_01).exists();
    assertThat(w2025_02).exists();
    assertThat(w2025_03).exists();
    assertThat(w2025_04).exists();
  }

  @Test
  void monthlyAndYearlyBucketsStartOnTheirFirstDay() {
    final LocalDateTime t = LocalDateTime.of(2024, 12, 30, 14, 35, 12);
    assertThat(BackupRetentionManager.bucketStart(t, ChronoUnit.MONTHS)).isEqualTo(LocalDateTime.of(2024, 12, 1, 0, 0));
    assertThat(BackupRetentionManager.bucketStart(t, ChronoUnit.YEARS)).isEqualTo(LocalDateTime.of(2024, 1, 1, 0, 0));
    assertThat(BackupRetentionManager.bucketStart(t, ChronoUnit.DAYS)).isEqualTo(LocalDateTime.of(2024, 12, 30, 0, 0));
    assertThat(BackupRetentionManager.bucketStart(t, ChronoUnit.HOURS)).isEqualTo(LocalDateTime.of(2024, 12, 30, 14, 0));
  }

  private void registerTiered(final int hourly, final int daily, final int weekly, final int monthly, final int yearly) {
    final DatabaseBackupConfig config = new DatabaseBackupConfig(DATABASE_NAME);
    final DatabaseBackupConfig.RetentionConfig retention = new DatabaseBackupConfig.RetentionConfig();
    final DatabaseBackupConfig.TieredConfig tiered = new DatabaseBackupConfig.TieredConfig();
    tiered.setHourly(hourly);
    tiered.setDaily(daily);
    tiered.setWeekly(weekly);
    tiered.setMonthly(monthly);
    tiered.setYearly(yearly);
    retention.setTiered(tiered);
    config.setRetention(retention);
    retentionManager.registerDatabase(DATABASE_NAME, config);
  }

  private List<File> createBackupFiles(final int count, final LocalDateTime startDate) throws IOException {
    final List<File> files = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      final LocalDateTime timestamp = startDate.plusHours(i);
      files.add(createBackupFile(timestamp));
    }
    return files;
  }

  /**
   * Creates a single backup file with the given timestamp.
   */
  private File createBackupFile(final LocalDateTime timestamp) throws IOException {
    return writeBackupFile(DATABASE_NAME + "-backup-" + timestamp.format(BACKUP_FORMAT) + ".zip");
  }

  private File writeBackupFile(final String filename) throws IOException {
    final File file = new File(dbBackupDir, filename);
    Files.write(file.toPath(), "test backup content".getBytes());
    return file;
  }
}
