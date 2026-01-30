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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.event.ServerEventLog;
import com.arcadedb.server.ha.HAServer;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;

/**
 * Runnable task that performs a database backup and applies retention policies.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BackupTask implements Runnable {
  private static final DateTimeFormatter BACKUP_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

  // Cached reflection objects for better performance
  private static volatile Class<?>                     backupClass;
  private static volatile Constructor<?>               backupConstructor;
  private static volatile Method                       setDirectoryMethod;
  private static volatile Method                       setVerboseLevelMethod;
  private static volatile Method                       backupDatabaseMethod;
  private static volatile boolean                      reflectionInitialized;
  private static final    Object                       REFLECTION_LOCK = new Object();

  private final ArcadeDBServer         server;
  private final String                 databaseName;
  private final DatabaseBackupConfig   config;
  private final String                 backupDirectory;
  private final BackupRetentionManager retentionManager;

  public BackupTask(final ArcadeDBServer server, final String databaseName,
                    final DatabaseBackupConfig config, final String backupDirectory,
                    final BackupRetentionManager retentionManager) {
    this.server = server;
    this.databaseName = databaseName;
    this.config = config;
    this.backupDirectory = backupDirectory;
    this.retentionManager = retentionManager;
  }

  @Override
  public void run() {
    // Check if backup should run on this server
    if (!shouldRunOnThisServer()) {
      LogManager.instance().log(this, Level.FINE,
          "Skipping backup for database '%s' - not configured to run on this server (%s)",
          databaseName, server.getServerName());
      return;
    }

    // Check time window
    if (!isWithinTimeWindow()) {
      LogManager.instance().log(this, Level.FINE,
          "Skipping backup for database '%s' - outside configured time window",
          databaseName);
      return;
    }

    // Perform the backup
    try {
      LogManager.instance().log(this, Level.INFO, "Starting scheduled backup for database '%s'...", databaseName);

      final String backupFile = performBackup();

      LogManager.instance().log(this, Level.INFO, "Scheduled backup completed for database '%s': %s", databaseName,
          backupFile);

      server.getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.INFO, "Auto-Backup", databaseName,
          "Scheduled backup completed: " + backupFile);

      // Apply retention policy
      if (retentionManager != null)
        retentionManager.applyRetention(databaseName);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error during scheduled backup for database '%s'", e, databaseName);

      server.getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.CRITICAL, "Auto-Backup", databaseName,
          "Scheduled backup failed: " + e.getMessage());
    }
  }

  /**
   * Determines if the backup should run on this server based on the runOnServer configuration.
   */
  private boolean shouldRunOnThisServer() {
    final String runOnServer = config.getRunOnServer();

    if (runOnServer == null || runOnServer.equals("*"))
      return true; // Run on all servers

    if (runOnServer.equals("$leader")) {
      // Run only on the leader node
      final HAServer ha = server.getHA();
      if (ha == null)
        return true; // No HA, single server mode, so we are the "leader"
      return ha.isLeader();
    }

    // Run on a specific named server
    return server.getServerName().equals(runOnServer);
  }

  /**
   * Checks if the current time is within the configured time window.
   * The time window is inclusive on both ends - backups are allowed at exactly
   * the start time and at exactly the end time.
   * <p>
   * For example, with a window of 02:00-04:00:
   * - 01:59:59 - not allowed
   * - 02:00:00 - allowed (inclusive)
   * - 03:00:00 - allowed
   * - 04:00:00 - allowed (inclusive)
   * - 04:00:01 - not allowed
   */
  private boolean isWithinTimeWindow() {
    final DatabaseBackupConfig.ScheduleConfig schedule = config.getSchedule();
    if (schedule == null || !schedule.hasTimeWindow())
      return true; // No time window restriction

    final LocalTime now = LocalTime.now();
    final LocalTime start = schedule.getWindowStart();
    final LocalTime end = schedule.getWindowEnd();

    if (start.isBefore(end))
      // Normal window (e.g., 02:00 to 04:00) - inclusive on both ends
      return !now.isBefore(start) && !now.isAfter(end);
    else
      // Window spans midnight (e.g., 22:00 to 04:00) - inclusive on both ends
      return !now.isBefore(start) || !now.isAfter(end);
  }

  /**
   * Performs the actual backup using the integration Backup class.
   * <p>
   * Note: The backup mechanism in ArcadeDB reads from immutable pages and handles
   * consistency internally. The transaction check is a safety warning but does not
   * block new transactions - the backup is designed to be non-blocking.
   */
  private String performBackup() throws Exception {
    final Database database = server.getDatabase(databaseName);

    // Check for active transaction - warn but don't block
    // ArcadeDB backup is designed to work on immutable pages, so this is informational
    if (database.isTransactionActive()) {
      final DatabaseInternal dbInternal = (DatabaseInternal) database;
      if (dbInternal.getTransaction().hasChanges()) {
        LogManager.instance().log(this, Level.WARNING,
            "Backup for database '%s' starting with active transaction - uncommitted changes will not be included",
            databaseName);
      }
    }

    // Generate backup filename
    final String timestamp = LocalDateTime.now().format(BACKUP_TIMESTAMP_FORMAT);
    final String backupFileName = databaseName + "-backup-" + timestamp + ".zip";

    // Prepare backup directory for this database - use Files.createDirectories to avoid TOCTOU
    final Path dbBackupPath = Paths.get(backupDirectory, databaseName);
    try {
      Files.createDirectories(dbBackupPath);
    } catch (final IOException e) {
      throw new BackupException("Failed to create backup directory for database '" + databaseName + "': " + dbBackupPath, e);
    }
    final String dbBackupDir = dbBackupPath.toString();

    // Perform backup using cached reflection for better performance
    try {
      initializeReflection();

      final Object backup = backupConstructor.newInstance(database, backupFileName);
      setDirectoryMethod.invoke(backup, dbBackupDir);
      setVerboseLevelMethod.invoke(backup, 1);

      return (String) backupDatabaseMethod.invoke(backup);

    } catch (final IllegalAccessException | InstantiationException e) {
      throw new BackupException("Backup libs not found in classpath. Make sure arcadedb-integration module is " +
          "included.", e);
    } catch (final InvocationTargetException e) {
      throw new BackupException("Error performing backup for database '" + databaseName + "'", e.getTargetException());
    }
  }

  /**
   * Initializes the cached reflection objects for the Backup class.
   * Uses double-checked locking for thread-safe lazy initialization.
   */
  private static void initializeReflection() throws BackupException {
    if (!reflectionInitialized) {
      synchronized (REFLECTION_LOCK) {
        if (!reflectionInitialized) {
          try {
            backupClass = Class.forName("com.arcadedb.integration.backup.Backup");
            backupConstructor = backupClass.getConstructor(Database.class, String.class);
            setDirectoryMethod = backupClass.getMethod("setDirectory", String.class);
            setVerboseLevelMethod = backupClass.getMethod("setVerboseLevel", Integer.TYPE);
            backupDatabaseMethod = backupClass.getMethod("backupDatabase");
            reflectionInitialized = true;
          } catch (final ClassNotFoundException | NoSuchMethodException e) {
            throw new BackupException("Backup libs not found in classpath. Make sure arcadedb-integration module is included.", e);
          }
        }
      }
    }
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public DatabaseBackupConfig getConfig() {
    return config;
  }
}
