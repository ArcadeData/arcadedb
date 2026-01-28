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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;

/**
 * Loads backup configuration from config/backup.json and optionally from
 * per-database backup.json files located in databases/{db-name}/backup.json.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BackupConfigLoader {
  private final String configPath;
  private final String databasesPath;

  public BackupConfigLoader(final String configPath, final String databasesPath) {
    this.configPath = java.nio.file.Paths.get(configPath).toString();
    this.databasesPath = java.nio.file.Paths.get(databasesPath).toString();
  }

  /**
   * Checks if the backup configuration file exists.
   */
  public boolean configExists() {
    final File configFile = java.nio.file.Paths.get(configPath, AutoBackupConfig.CONFIG_FILE_NAME).toFile();
    return configFile.exists();
  }

  /**
   * Loads the server-level backup configuration from config/backup.json.
   *
   * @return AutoBackupConfig or null if configuration doesn't exist
   */
  public AutoBackupConfig loadConfig() {
    final File configFile = java.nio.file.Paths.get(configPath, AutoBackupConfig.CONFIG_FILE_NAME).toFile();

    if (!configFile.exists()) {
      LogManager.instance().log(this, Level.FINE, "Backup config file not found: %s", configFile.getAbsolutePath());
      return null;
    }

    try {
      final String content = FileUtils.readFileAsString(configFile);
      final JSONObject json = new JSONObject(content);
      final AutoBackupConfig config = AutoBackupConfig.fromJSON(json);

      LogManager.instance().log(this, Level.INFO, "Loaded backup configuration from %s", configFile.getAbsolutePath());
      return config;

    } catch (final IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error loading backup config from %s", e,
          configFile.getAbsolutePath());
      return null;
    }
  }

  /**
   * Loads database-specific backup configuration from databases/{db-name}/backup.json.
   * This configuration overrides server-level settings for the specific database.
   *
   * @param databaseName The name of the database
   * @return DatabaseBackupConfig or null if no database-specific config exists
   */
  public DatabaseBackupConfig loadDatabaseConfig(final String databaseName) {
    final File dbConfigFile = java.nio.file.Paths.get(databasesPath, databaseName, AutoBackupConfig.CONFIG_FILE_NAME).toFile();

    if (!dbConfigFile.exists())
      return null;

    try {
      final String content = FileUtils.readFileAsString(dbConfigFile);
      final JSONObject json = new JSONObject(content);
      final DatabaseBackupConfig config = DatabaseBackupConfig.fromJSON(databaseName, json);

      LogManager.instance().log(this, Level.INFO, "Loaded database-specific backup config for '%s' from %s",
          databaseName, dbConfigFile.getAbsolutePath());
      return config;

    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Error loading database backup config from %s", e,
          dbConfigFile.getAbsolutePath());
      return null;
    }
  }

  /**
   * Gets the effective configuration for a database, considering both server-level
   * and database-specific configurations.
   *
   * @param serverConfig The server-level configuration
   * @param databaseName The name of the database
   * @return The effective DatabaseBackupConfig for the database
   */
  public DatabaseBackupConfig getEffectiveConfig(final AutoBackupConfig serverConfig, final String databaseName) {
    // Start with server-level effective config
    DatabaseBackupConfig effectiveConfig = serverConfig.getEffectiveConfig(databaseName);

    // Check for database-specific override file
    final DatabaseBackupConfig dbOverride = loadDatabaseConfig(databaseName);
    if (dbOverride != null) {
      // Database file takes precedence, merge with server defaults
      dbOverride.mergeWithDefaults(serverConfig.getDefaults());
      effectiveConfig = dbOverride;
    }

    return effectiveConfig;
  }
}
