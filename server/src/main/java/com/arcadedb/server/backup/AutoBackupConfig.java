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

import com.arcadedb.serializer.json.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Server-level auto-backup configuration loaded from config/backup.json.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AutoBackupConfig {
  public static final String CONFIG_FILE_NAME   = "backup.json";
  public static final int    CURRENT_VERSION    = 1;
  public static final String DEFAULT_BACKUP_DIR = "./backups";
  public static final String DEFAULT_RUN_SERVER = "$leader";
  public static final int    DEFAULT_FREQUENCY  = 60;
  public static final int    DEFAULT_MAX_FILES  = 10;

  private       int                               version         = CURRENT_VERSION;
  private       boolean                           enabled         = true;
  private       String                            backupDirectory = DEFAULT_BACKUP_DIR;
  private       DatabaseBackupConfig              defaults;
  private final Map<String, DatabaseBackupConfig> databases       = new HashMap<>();

  public static AutoBackupConfig fromJSON(final JSONObject json) {
    final AutoBackupConfig config = new AutoBackupConfig();

    if (json.has("version"))
      config.version = json.getInt("version");

    if (json.has("enabled"))
      config.enabled = json.getBoolean("enabled");

    if (json.has("backupDirectory"))
      config.backupDirectory = json.getString("backupDirectory");

    if (json.has("defaults"))
      config.defaults = DatabaseBackupConfig.fromJSON("_defaults", json.getJSONObject("defaults"));

    if (json.has("databases")) {
      final JSONObject dbs = json.getJSONObject("databases");
      for (final String dbName : dbs.keySet()) {
        final DatabaseBackupConfig dbConfig = DatabaseBackupConfig.fromJSON(dbName, dbs.getJSONObject(dbName));
        config.databases.put(dbName, dbConfig);
      }
    }

    return config;
  }

  public static AutoBackupConfig createDefault() {
    final AutoBackupConfig config = new AutoBackupConfig();
    config.defaults = createDefaultDatabaseConfig();
    return config;
  }

  private static DatabaseBackupConfig createDefaultDatabaseConfig() {
    final DatabaseBackupConfig dbConfig = new DatabaseBackupConfig("_defaults");
    dbConfig.setEnabled(true);
    dbConfig.setRunOnServer(DEFAULT_RUN_SERVER);

    final DatabaseBackupConfig.ScheduleConfig schedule = new DatabaseBackupConfig.ScheduleConfig();
    schedule.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    schedule.setFrequencyMinutes(DEFAULT_FREQUENCY);
    dbConfig.setSchedule(schedule);

    final DatabaseBackupConfig.RetentionConfig retention = new DatabaseBackupConfig.RetentionConfig();
    retention.setMaxFiles(DEFAULT_MAX_FILES);
    dbConfig.setRetention(retention);

    return dbConfig;
  }

  /**
   * Gets the effective configuration for a specific database, merging database-specific
   * settings with server-level defaults.
   */
  public DatabaseBackupConfig getEffectiveConfig(final String databaseName) {
    DatabaseBackupConfig dbConfig = databases.get(databaseName);

    if (dbConfig == null) {
      // No database-specific config, use defaults
      dbConfig = new DatabaseBackupConfig(databaseName);
      if (defaults != null) {
        dbConfig.setEnabled(defaults.isEnabled());
        dbConfig.setRunOnServer(defaults.getRunOnServer());
        dbConfig.setSchedule(defaults.getSchedule());
        dbConfig.setRetention(defaults.getRetention());
      }
    } else {
      // Merge database-specific config with defaults
      dbConfig.mergeWithDefaults(defaults);
    }

    return dbConfig;
  }

  public int getVersion() {
    return version;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(final boolean enabled) {
    this.enabled = enabled;
  }

  public String getBackupDirectory() {
    return backupDirectory;
  }

  public void setBackupDirectory(final String backupDirectory) {
    this.backupDirectory = backupDirectory;
  }

  public DatabaseBackupConfig getDefaults() {
    return defaults;
  }

  public void setDefaults(final DatabaseBackupConfig defaults) {
    this.defaults = defaults;
  }

  public Map<String, DatabaseBackupConfig> getDatabases() {
    return Collections.unmodifiableMap(databases);
  }

  public void addDatabaseConfig(final String databaseName, final DatabaseBackupConfig config) {
    databases.put(databaseName, config);
  }

  /**
   * Converts this configuration to a JSON object.
   */
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("version", version);
    json.put("enabled", enabled);
    json.put("backupDirectory", backupDirectory);

    if (defaults != null)
      json.put("defaults", defaults.toJSON());

    if (!databases.isEmpty()) {
      final JSONObject dbs = new JSONObject();
      for (final Map.Entry<String, DatabaseBackupConfig> entry : databases.entrySet())
        dbs.put(entry.getKey(), entry.getValue().toJSON());
      json.put("databases", dbs);
    }

    return json;
  }
}
