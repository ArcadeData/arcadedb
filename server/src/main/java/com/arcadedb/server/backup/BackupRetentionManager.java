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

import com.arcadedb.log.LogManager;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Manages backup retention with support for both simple max-files and tiered retention policies.
 * <p>
 * Tiered retention keeps backups at different intervals:
 * - Hourly: Keep N most recent hourly backups
 * - Daily: Keep N most recent daily backups (oldest backup per day)
 * - Weekly: Keep N most recent weekly backups (oldest backup per week)
 * - Monthly: Keep N most recent monthly backups (oldest backup per month)
 * - Yearly: Keep N most recent yearly backups (oldest backup per year)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BackupRetentionManager {
  private static final Pattern           BACKUP_FILENAME_PATTERN =
      Pattern.compile(".*-backup-(\\d{8})-(\\d{6})\\.zip$");
  private static final DateTimeFormatter TIMESTAMP_PARSER        =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
  private static final FilenameFilter    BACKUP_FILE_FILTER      =
      (dir, name) -> name.endsWith(".zip") && name.contains("-backup-");

  private final String                            backupDirectory;
  private final Map<String, DatabaseBackupConfig> databaseConfigs;

  public BackupRetentionManager(final String backupDirectory) {
    this.backupDirectory = backupDirectory;
    this.databaseConfigs = new HashMap<>();
  }

  /**
   * Registers a database configuration for retention management.
   */
  public void registerDatabase(final String databaseName, final DatabaseBackupConfig config) {
    databaseConfigs.put(databaseName, config);
  }

  /**
   * Applies the retention policy for a specific database.
   *
   * @param databaseName The database name
   * @return Number of backup files deleted
   */
  public int applyRetention(final String databaseName) {
    final DatabaseBackupConfig config = databaseConfigs.get(databaseName);
    if (config == null) {
      LogManager.instance().log(this, Level.WARNING,
          "No retention config registered for database '%s'", databaseName);
      return 0;
    }

    final DatabaseBackupConfig.RetentionConfig retention = config.getRetention();
    if (retention == null) {
      LogManager.instance().log(this, Level.FINE,
          "No retention policy configured for database '%s'", databaseName);
      return 0;
    }

    final File dbBackupDir = Paths.get(backupDirectory, databaseName).toFile();
    if (!dbBackupDir.exists() || !dbBackupDir.isDirectory())
      return 0;

    // Get all backup files sorted by timestamp (oldest first)
    final List<BackupFileInfo> backupFiles = getBackupFiles(dbBackupDir);
    if (backupFiles.isEmpty())
      return 0;

    final Set<File> filesToKeep;
    if (retention.hasTieredRetention())
      filesToKeep = applyTieredRetention(backupFiles, retention.getTiered());
    else
      filesToKeep = applyMaxFilesRetention(backupFiles, retention.getMaxFiles());

    // Delete files not in the keep set
    int deletedCount = 0;
    int failedCount = 0;
    for (final BackupFileInfo info : backupFiles) {
      if (!filesToKeep.contains(info.file)) {
        try {
          Files.delete(info.file.toPath());
          deletedCount++;
          LogManager.instance().log(this, Level.INFO,
              "Deleted old backup: %s", info.file.getName());
        } catch (final IOException e) {
          failedCount++;
          LogManager.instance().log(this, Level.WARNING,
              "Failed to delete old backup '%s': %s", info.file.getName(), e.getMessage());
        }
      }
    }

    if (failedCount > 0) {
      LogManager.instance().log(this, Level.WARNING,
          "Retention for database '%s' completed with %d deletion failures", databaseName, failedCount);
    }

    LogManager.instance().log(this, Level.INFO,
        "Retention applied for database '%s': kept %d, deleted %d backups",
        databaseName, filesToKeep.size(), deletedCount);

    return deletedCount;
  }

  // Maximum number of backup files to process to prevent unbounded memory usage
  private static final int MAX_BACKUP_FILES_TO_PROCESS = 10000;

  /**
   * Gets all backup files for a database directory, sorted by timestamp.
   * Limited to MAX_BACKUP_FILES_TO_PROCESS to prevent unbounded memory usage.
   */
  private List<BackupFileInfo> getBackupFiles(final File dbBackupDir) {
    final File[] files = dbBackupDir.listFiles(BACKUP_FILE_FILTER);
    if (files == null || files.length == 0)
      return Collections.emptyList();

    if (files.length > MAX_BACKUP_FILES_TO_PROCESS) {
      LogManager.instance().log(this, Level.WARNING,
          "Database backup directory contains %d files, processing only the most recent %d",
          files.length, MAX_BACKUP_FILES_TO_PROCESS);
    }

    final List<BackupFileInfo> backupFiles = new ArrayList<>();
    for (final File file : files) {
      final LocalDateTime timestamp = parseBackupTimestamp(file.getName());
      if (timestamp != null)
        backupFiles.add(new BackupFileInfo(file, timestamp));
    }

    // Sort by timestamp (oldest first)
    backupFiles.sort(Comparator.comparing(info -> info.timestamp));

    // Limit to most recent files if too many
    if (backupFiles.size() > MAX_BACKUP_FILES_TO_PROCESS) {
      final int startIndex = backupFiles.size() - MAX_BACKUP_FILES_TO_PROCESS;
      return new ArrayList<>(backupFiles.subList(startIndex, backupFiles.size()));
    }

    return backupFiles;
  }

  /**
   * Parses the timestamp from a backup filename.
   */
  private LocalDateTime parseBackupTimestamp(final String filename) {
    final Matcher matcher = BACKUP_FILENAME_PATTERN.matcher(filename);
    if (!matcher.matches())
      return null;

    try {
      final String timestampStr = matcher.group(1) + "-" + matcher.group(2);
      return LocalDateTime.parse(timestampStr, TIMESTAMP_PARSER);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not parse timestamp from backup filename: %s", filename);
      return null;
    }
  }

  /**
   * Applies simple max-files retention: keep the N most recent backups.
   */
  private Set<File> applyMaxFilesRetention(final List<BackupFileInfo> backupFiles, final int maxFiles) {
    final Set<File> filesToKeep = new HashSet<>();

    // Keep the most recent N files
    final int startIndex = Math.max(0, backupFiles.size() - maxFiles);
    for (int i = startIndex; i < backupFiles.size(); i++)
      filesToKeep.add(backupFiles.get(i).file);

    return filesToKeep;
  }

  /**
   * Applies tiered retention policy.
   * <p>
   * For each tier, we group backups by the appropriate time bucket and keep
   * the specified number of backups, preferring the oldest backup in each bucket.
   */
  private Set<File> applyTieredRetention(final List<BackupFileInfo> backupFiles,
                                         final DatabaseBackupConfig.TieredConfig tiered) {
    final Set<File> filesToKeep = new HashSet<>();
    final LocalDateTime now = LocalDateTime.now();

    // Apply each tier
    filesToKeep.addAll(selectTierBackups(backupFiles, now, ChronoUnit.HOURS, tiered.getHourly()));
    filesToKeep.addAll(selectTierBackups(backupFiles, now, ChronoUnit.DAYS, tiered.getDaily()));
    filesToKeep.addAll(selectWeeklyBackups(backupFiles, now, tiered.getWeekly()));
    filesToKeep.addAll(selectMonthlyBackups(backupFiles, now, tiered.getMonthly()));
    filesToKeep.addAll(selectYearlyBackups(backupFiles, now, tiered.getYearly()));

    return filesToKeep;
  }

  /**
   * Selects backups for hourly/daily tiers.
   */
  private Set<File> selectTierBackups(final List<BackupFileInfo> backupFiles,
                                      final LocalDateTime now,
                                      final ChronoUnit unit,
                                      final int count) {
    final Set<File> selected = new HashSet<>();
    if (count <= 0)
      return selected;

    // Group backups by their truncated time bucket
    final Map<LocalDateTime, List<BackupFileInfo>> buckets = new LinkedHashMap<>();

    for (final BackupFileInfo info : backupFiles) {
      final LocalDateTime bucket = truncateToUnit(info.timestamp, unit);
      buckets.computeIfAbsent(bucket, k -> new ArrayList<>()).add(info);
    }

    // Keep the oldest backup from the most recent N buckets
    final List<LocalDateTime> sortedBuckets = new ArrayList<>(buckets.keySet());
    sortedBuckets.sort(Comparator.reverseOrder());

    for (int i = 0; i < Math.min(count, sortedBuckets.size()); i++) {
      final List<BackupFileInfo> bucketFiles = buckets.get(sortedBuckets.get(i));
      if (!bucketFiles.isEmpty())
        selected.add(bucketFiles.get(0).file); // Oldest in bucket
    }

    return selected;
  }

  /**
   * Selects backups for weekly tier.
   */
  private Set<File> selectWeeklyBackups(final List<BackupFileInfo> backupFiles,
                                        final LocalDateTime now,
                                        final int count) {
    final Set<File> selected = new HashSet<>();
    if (count <= 0)
      return selected;

    // Group by year-week (using ISO week definition for consistent behavior across locales)
    final Map<String, List<BackupFileInfo>> buckets = new LinkedHashMap<>();
    final WeekFields weekFields = WeekFields.ISO;

    for (final BackupFileInfo info : backupFiles) {
      final int year = info.timestamp.getYear();
      final int week = info.timestamp.get(weekFields.weekOfWeekBasedYear());
      final String bucket = year + "-W" + String.format("%02d", week);
      buckets.computeIfAbsent(bucket, k -> new ArrayList<>()).add(info);
    }

    // Keep oldest from most recent N weeks
    final List<String> sortedBuckets = new ArrayList<>(buckets.keySet());
    sortedBuckets.sort(Comparator.reverseOrder());

    for (int i = 0; i < Math.min(count, sortedBuckets.size()); i++) {
      final List<BackupFileInfo> bucketFiles = buckets.get(sortedBuckets.get(i));
      if (!bucketFiles.isEmpty())
        selected.add(bucketFiles.get(0).file);
    }

    return selected;
  }

  /**
   * Selects backups for monthly tier.
   */
  private Set<File> selectMonthlyBackups(final List<BackupFileInfo> backupFiles,
                                         final LocalDateTime now,
                                         final int count) {
    final Set<File> selected = new HashSet<>();
    if (count <= 0)
      return selected;

    // Group by year-month
    final Map<String, List<BackupFileInfo>> buckets = new LinkedHashMap<>();

    for (final BackupFileInfo info : backupFiles) {
      final String bucket = info.timestamp.getYear() + "-" +
          String.format("%02d", info.timestamp.getMonthValue());
      buckets.computeIfAbsent(bucket, k -> new ArrayList<>()).add(info);
    }

    // Keep oldest from most recent N months
    final List<String> sortedBuckets = new ArrayList<>(buckets.keySet());
    sortedBuckets.sort(Comparator.reverseOrder());

    for (int i = 0; i < Math.min(count, sortedBuckets.size()); i++) {
      final List<BackupFileInfo> bucketFiles = buckets.get(sortedBuckets.get(i));
      if (!bucketFiles.isEmpty())
        selected.add(bucketFiles.get(0).file);
    }

    return selected;
  }

  /**
   * Selects backups for yearly tier.
   */
  private Set<File> selectYearlyBackups(final List<BackupFileInfo> backupFiles,
                                        final LocalDateTime now,
                                        final int count) {
    final Set<File> selected = new HashSet<>();
    if (count <= 0)
      return selected;

    // Group by year
    final Map<Integer, List<BackupFileInfo>> buckets = new LinkedHashMap<>();

    for (final BackupFileInfo info : backupFiles) {
      buckets.computeIfAbsent(info.timestamp.getYear(), k -> new ArrayList<>()).add(info);
    }

    // Keep oldest from most recent N years
    final List<Integer> sortedBuckets = new ArrayList<>(buckets.keySet());
    sortedBuckets.sort(Comparator.reverseOrder());

    for (int i = 0; i < Math.min(count, sortedBuckets.size()); i++) {
      final List<BackupFileInfo> bucketFiles = buckets.get(sortedBuckets.get(i));
      if (!bucketFiles.isEmpty())
        selected.add(bucketFiles.get(0).file);
    }

    return selected;
  }

  /**
   * Truncates a LocalDateTime to the specified unit.
   */
  private LocalDateTime truncateToUnit(final LocalDateTime dateTime, final ChronoUnit unit) {
    return switch (unit) {
      case HOURS -> dateTime.truncatedTo(ChronoUnit.HOURS);
      case DAYS -> dateTime.truncatedTo(ChronoUnit.DAYS);
      default -> dateTime;
    };
  }

  /**
   * Gets the total size of all backup files for a database.
   */
  public long getBackupSizeBytes(final String databaseName) {
    final File dbBackupDir = Paths.get(backupDirectory, databaseName).toFile();
    if (!dbBackupDir.exists() || !dbBackupDir.isDirectory())
      return 0;

    final File[] files = dbBackupDir.listFiles(BACKUP_FILE_FILTER);
    if (files == null)
      return 0;

    long totalSize = 0;
    for (final File file : files)
      totalSize += file.length();
    return totalSize;
  }

  /**
   * Gets the count of backup files for a database.
   */
  public int getBackupCount(final String databaseName) {
    final File dbBackupDir = Paths.get(backupDirectory, databaseName).toFile();
    if (!dbBackupDir.exists() || !dbBackupDir.isDirectory())
      return 0;

    final File[] files = dbBackupDir.listFiles(BACKUP_FILE_FILTER);
    return files != null ? files.length : 0;
  }

  /**
   * Internal class to hold backup file info with parsed timestamp.
   */
  private static class BackupFileInfo {
    final File          file;
    final LocalDateTime timestamp;

    BackupFileInfo(final File file, final LocalDateTime timestamp) {
      this.file = file;
      this.timestamp = timestamp;
    }
  }
}
