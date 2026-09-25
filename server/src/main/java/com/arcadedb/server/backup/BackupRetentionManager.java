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
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;

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
  private static final FilenameFilter    BACKUP_FILE_FILTER      =
      (dir, name) -> name.endsWith(".zip") && name.contains("-backup-");

  private final String                            backupDirectory;
  private final Map<String, DatabaseBackupConfig> databaseConfigs;

  public BackupRetentionManager(final String backupDirectory) {
    this.backupDirectory = backupDirectory;
    // Mutated from the server thread (schedule/cancel) and read from the backup threads (applyRetention).
    this.databaseConfigs = new ConcurrentHashMap<>();
  }

  /**
   * Registers a database configuration for retention management.
   */
  public void registerDatabase(final String databaseName, final DatabaseBackupConfig config) {
    databaseConfigs.put(databaseName, config);
  }

  /**
   * Forgets the retention configuration of a database that no longer exists on this server, so the map does not grow
   * with names that will never be backed up again (issue #6752). The archives already on disk are left untouched:
   * a backup taken before the drop is exactly what an operator restores from.
   */
  public void unregisterDatabase(final String databaseName) {
    databaseConfigs.remove(databaseName);
  }

  /**
   * Returns the names of the databases whose retention configuration is currently registered.
   */
  public Set<String> getRegisteredDatabases() {
    return Set.copyOf(databaseConfigs.keySet());
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

    return applyRetention(databaseName, config);
  }

  /**
   * Applies {@code config}'s retention policy to a database's backup directory WITHOUT requiring that database to be
   * registered here.
   * <p>
   * Registration follows the auto-backup SCHEDULE, and an on-demand {@code trigger backup} does not need one: it can
   * name any database on the server, including one absent from the auto-backup configuration entirely. Such a
   * database's archives were never pruned - the scheduler prunes what it knows about, and it does not know about
   * that database - so triggering backups for it grew the directory forever, which is the disk-growth problem #7392
   * set out to fix surviving in a narrower form (issue #7472).
   * <p>
   * The caller supplies the EFFECTIVE config ({@code BackupConfigLoader.getEffectiveConfig}), which falls back to
   * the server-level defaults, so an unconfigured database is pruned by the same policy a configured one with no
   * overrides would be - rather than by no policy at all.
   *
   * @param databaseName the database whose backup sub-directory is pruned
   * @param config       the effective backup configuration for it
   *
   * @return number of backup files deleted
   */
  public int applyRetention(final String databaseName, final DatabaseBackupConfig config) {
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

    // Always keep the most recent backup so a just-completed backup is never deleted. Retention
    // runs immediately after each backup; with tiered buckets keeping the oldest member, the newest
    // restore point would otherwise be discarded seconds after creation.
    filesToKeep.add(backupFiles.get(backupFiles.size() - 1).file);

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
    // The convention lives on BackupCoordinator, which is also what writes these names: a parser that drifted from
    // the writer would drop every archive it could not read out of the retention set, which is to say never rotate
    // them out again (issue #6753).
    final LocalDateTime timestamp = BackupCoordinator.parseArchiveTimestamp(filename);
    if (timestamp == null)
      LogManager.instance().log(this, Level.WARNING,
          "Could not parse timestamp from backup filename: %s", filename);
    return timestamp;
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

    // Apply each tier
    filesToKeep.addAll(selectTierBackups(backupFiles, ChronoUnit.HOURS, tiered.getHourly()));
    filesToKeep.addAll(selectTierBackups(backupFiles, ChronoUnit.DAYS, tiered.getDaily()));
    filesToKeep.addAll(selectTierBackups(backupFiles, ChronoUnit.WEEKS, tiered.getWeekly()));
    filesToKeep.addAll(selectTierBackups(backupFiles, ChronoUnit.MONTHS, tiered.getMonthly()));
    filesToKeep.addAll(selectTierBackups(backupFiles, ChronoUnit.YEARS, tiered.getYearly()));

    return filesToKeep;
  }

  /**
   * Selects the backups of one tier: the oldest backup of each of the N most recent buckets of {@code unit}.
   */
  private Set<File> selectTierBackups(final List<BackupFileInfo> backupFiles, final ChronoUnit unit, final int count) {
    final Set<File> selected = new HashSet<>();
    if (count <= 0)
      return selected;

    // Group backups by the start of their bucket. backupFiles is sorted oldest first, so each bucket's first entry is
    // its oldest backup.
    final Map<LocalDateTime, List<BackupFileInfo>> buckets = new HashMap<>();
    for (final BackupFileInfo info : backupFiles)
      buckets.computeIfAbsent(bucketStart(info.timestamp, unit), k -> new ArrayList<>()).add(info);

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
   * The first instant of the bucket {@code dateTime} falls in. Keying every tier by this instant makes the key one
   * value per bucket and makes its natural order the chronological one, which the "most recent N buckets" ranking
   * relies on. A week is an ISO week, starting on Monday: keying it by calendar year plus ISO week number mixed two
   * calendars, so the last days of December (ISO week 1 of the NEXT year) collided with the first week of January
   * of the SAME year, and the tier kept a year-old archive in place of a recent one (issue #8298).
   */
  static LocalDateTime bucketStart(final LocalDateTime dateTime, final ChronoUnit unit) {
    return switch (unit) {
      case HOURS -> dateTime.truncatedTo(ChronoUnit.HOURS);
      case DAYS -> dateTime.truncatedTo(ChronoUnit.DAYS);
      case WEEKS -> dateTime.truncatedTo(ChronoUnit.DAYS).with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
      case MONTHS -> dateTime.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1);
      case YEARS -> dateTime.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1);
      default -> throw new IllegalArgumentException("Unsupported retention tier unit: " + unit);
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
