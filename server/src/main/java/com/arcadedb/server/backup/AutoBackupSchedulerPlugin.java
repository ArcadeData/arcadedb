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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.event.ServerEventLog;

import java.io.File;
import java.util.Set;
import java.util.logging.Level;

/**
 * Server plugin that manages automatic backup scheduling.
 * <p>
 * This plugin is activated when a backup.json configuration file exists in the config directory.
 * It supports:
 * - Frequency-based scheduling (e.g., every 60 minutes)
 * - CRON-based scheduling (e.g., "0 0 2 * * ?" for 2 AM daily)
 * - Tiered retention policies (hourly/daily/weekly/monthly/yearly)
 * - HA cluster awareness (configurable per-database backup execution node)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AutoBackupSchedulerPlugin implements ServerPlugin {
  private ArcadeDBServer         server;
  private ContextConfiguration   configuration;
  private AutoBackupConfig       backupConfig;
  private BackupConfigLoader     configLoader;
  private BackupScheduler        scheduler;
  private BackupRetentionManager retentionManager;
  private boolean                enabled;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.configuration = configuration;

    // Initialize config loader
    final String configPath = arcadeDBServer.getRootPath() + File.separator + "config";
    final String databasesPath = configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);

    this.configLoader = new BackupConfigLoader(configPath, databasesPath);

    // Check if backup.json exists
    if (!configLoader.configExists()) {
      LogManager.instance().log(this, Level.INFO,
          "Auto-backup scheduler disabled: config/backup.json not found");
      this.enabled = false;
      return;
    }

    // Load configuration
    this.backupConfig = configLoader.loadConfig();
    if (backupConfig == null || !backupConfig.isEnabled()) {
      LogManager.instance().log(this, Level.INFO,
          "Auto-backup scheduler disabled by configuration");
      this.enabled = false;
      return;
    }

    this.enabled = true;
    LogManager.instance().log(this, Level.INFO, "Auto-backup scheduler configured");
  }

  @Override
  public void startService() {
    if (!enabled) {
      return;
    }

    // Resolve backup directory (relative to server root)
    String backupDirectory = backupConfig.getBackupDirectory();
    if (!backupDirectory.startsWith("/") && !backupDirectory.startsWith(File.separator) && !backupDirectory.contains(
        ":"))
      backupDirectory = server.getRootPath() + File.separator + backupDirectory;

    // Ensure backup directory exists
    final File backupDir = new File(backupDirectory);
    if (!backupDir.exists())
      backupDir.mkdirs();

    // Initialize retention manager
    this.retentionManager = new BackupRetentionManager(backupDirectory);

    // Initialize and start scheduler
    this.scheduler = new BackupScheduler(server, backupDirectory, retentionManager);
    this.scheduler.start();

    // Schedule backups for all existing databases
    scheduleAllDatabases();

    LogManager.instance().log(this, Level.INFO,
        "Auto-backup scheduler started. Backup directory: %s", backupDirectory);

    server.getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.INFO, "Auto-Backup", null,
        "Auto-backup scheduler started with " + scheduler.getScheduledCount() + " database(s)");
  }

  /**
   * Schedules backups for all existing databases.
   */
  private void scheduleAllDatabases() {
    final Set<String> databaseNames = server.getDatabaseNames();

    for (final String databaseName : databaseNames)
      scheduleDatabase(databaseName);
  }

  /**
   * Schedules backup for a specific database.
   */
  public void scheduleDatabase(final String databaseName) {
    if (!enabled || scheduler == null)
      return;

    // Get effective config for this database
    final DatabaseBackupConfig dbConfig = configLoader.getEffectiveConfig(backupConfig, databaseName);

    if (!dbConfig.isEnabled()) {
      LogManager.instance().log(this, Level.INFO,
          "Backup disabled for database '%s'", databaseName);
      return;
    }

    // Register with retention manager
    retentionManager.registerDatabase(databaseName, dbConfig);

    // Schedule the backup
    scheduler.scheduleBackup(databaseName, dbConfig);

    LogManager.instance().log(this, Level.INFO,
        "Scheduled automatic backup for database '%s'", databaseName);
  }

  /**
   * Cancels scheduled backup for a database.
   */
  public void cancelDatabase(final String databaseName) {
    if (!enabled || scheduler == null)
      return;

    scheduler.cancelBackup(databaseName);
  }

  /**
   * Triggers an immediate backup for a database.
   */
  public void triggerBackup(final String databaseName) {
    if (!enabled || scheduler == null) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot trigger backup - auto-backup scheduler is not enabled");
      return;
    }

    final DatabaseBackupConfig dbConfig = configLoader.getEffectiveConfig(backupConfig, databaseName);
    scheduler.triggerImmediateBackup(databaseName, dbConfig);
  }

  @Override
  public void stopService() {
    if (scheduler != null) {
      scheduler.stop();
      LogManager.instance().log(this, Level.INFO, "Auto-backup scheduler stopped");
    }
  }

  @Override
  public INSTALLATION_PRIORITY getInstallationPriority() {
    // Install after databases are open so we can schedule backups for all databases
    return INSTALLATION_PRIORITY.AFTER_DATABASES_OPEN;
  }

  /**
   * Returns the backup configuration.
   */
  public AutoBackupConfig getBackupConfig() {
    return backupConfig;
  }

  /**
   * Returns the backup scheduler.
   */
  public BackupScheduler getScheduler() {
    return scheduler;
  }

  /**
   * Returns the retention manager.
   */
  public BackupRetentionManager getRetentionManager() {
    return retentionManager;
  }

  /**
   * Returns true if the plugin is enabled.
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Reloads the backup configuration from disk.
   */
  public void reloadConfiguration() {
    if (!configLoader.configExists()) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot reload configuration: config/backup.json not found");
      return;
    }

    final AutoBackupConfig newConfig = configLoader.loadConfig();
    if (newConfig == null)
      return;

    this.backupConfig = newConfig;

    // Re-schedule all databases with new configuration
    if (scheduler != null) {
      final Set<String> databaseNames = server.getDatabaseNames();
      for (final String databaseName : databaseNames) {
        scheduler.cancelBackup(databaseName);
        scheduleDatabase(databaseName);
      }
    }

    LogManager.instance().log(this, Level.INFO, "Auto-backup configuration reloaded");
  }
}
