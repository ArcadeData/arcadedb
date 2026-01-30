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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for AutoBackupSchedulerPlugin.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AutoBackupSchedulerPluginIT extends BaseGraphServerTest {
  private static final String BACKUP_CONFIG = """
      {
        "version": 1,
        "enabled": true,
        "backupDirectory": "test-backups",
        "defaults": {
          "enabled": true,
          "runOnServer": "*",
          "schedule": {
            "type": "frequency",
            "frequencyMinutes": 1
          },
          "retention": {
            "maxFiles": 5
          }
        }
      }
      """;

  private File backupConfigFile;
  private File backupDir;

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    // Create backup.json BEFORE server starts
    try {
      final File configDir = new File("./target/config");
      configDir.mkdirs();

      backupConfigFile = new File(configDir, "backup.json");
      try (FileWriter writer = new FileWriter(backupConfigFile)) {
        writer.write(BACKUP_CONFIG);
      }

      backupDir = new File("./target/test-backups"); // Relative to server root ./target
      if (backupDir.exists())
        FileUtils.deleteRecursively(backupDir);
      backupDir.mkdirs();
    } catch (IOException e) {
      throw new RuntimeException("Failed to set up backup config", e);
    }

    // Configure the server to use our test config directory
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    // Register the auto-backup plugin explicitly
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, "auto-backup:" + AutoBackupSchedulerPlugin.class.getName());
  }

  @AfterEach
  void cleanUpBackupConfig() {
    if (backupConfigFile != null && backupConfigFile.exists())
      backupConfigFile.delete();

    if (backupDir != null && backupDir.exists())
      FileUtils.deleteRecursively(backupDir);
  }

  @Test
  void pluginLoadsConfiguration() {
    final ArcadeDBServer server = getServer(0);

    // Find the auto-backup plugin
    AutoBackupSchedulerPlugin backupPlugin = null;
    for (final var plugin : server.getPlugins()) {
      if (plugin instanceof AutoBackupSchedulerPlugin) {
        backupPlugin = (AutoBackupSchedulerPlugin) plugin;
        break;
      }
    }

    assertThat(backupPlugin).isNotNull();
    assertThat(backupPlugin.isEnabled()).isTrue();
    assertThat(backupPlugin.getBackupConfig()).isNotNull();
    assertThat(backupPlugin.getBackupConfig().getBackupDirectory()).isEqualTo("test-backups");
  }

  @Test
  void pluginSchedulesBackups() {
    final ArcadeDBServer server = getServer(0);

    // Find the auto-backup plugin
    AutoBackupSchedulerPlugin backupPlugin = null;
    for (final var plugin : server.getPlugins()) {
      if (plugin instanceof AutoBackupSchedulerPlugin) {
        backupPlugin = (AutoBackupSchedulerPlugin) plugin;
        break;
      }
    }

    assertThat(backupPlugin).isNotNull();
    assertThat(backupPlugin.getScheduler()).isNotNull();
    assertThat(backupPlugin.getScheduler().isRunning()).isTrue();
    assertThat(backupPlugin.getScheduler().getScheduledCount()).isGreaterThan(0);
  }

  @Test
  void triggerImmediateBackup() {
    final ArcadeDBServer server = getServer(0);

    // Find the auto-backup plugin
    AutoBackupSchedulerPlugin backupPlugin = null;
    for (final var plugin : server.getPlugins()) {
      if (plugin instanceof AutoBackupSchedulerPlugin) {
        backupPlugin = (AutoBackupSchedulerPlugin) plugin;
        break;
      }
    }

    assertThat(backupPlugin).isNotNull();

    // Trigger immediate backup
    backupPlugin.triggerBackup(getDatabaseName());

    // Wait for backup to complete
    final File dbBackupDir = new File(backupDir, getDatabaseName());
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> {
          if (!dbBackupDir.exists())
            return false;
          final File[] files = dbBackupDir.listFiles((dir, name) -> name.endsWith(".zip"));
          return files != null && files.length > 0;
        });

    // Verify backup file was created
    final File[] backupFiles = dbBackupDir.listFiles((dir, name) -> name.endsWith(".zip"));
    assertThat(backupFiles).isNotNull();
    assertThat(backupFiles.length).isGreaterThan(0);
    assertThat(backupFiles[0].length()).isGreaterThan(0);
  }

  @Test
  void retentionManagerRegistered() {
    final ArcadeDBServer server = getServer(0);

    AutoBackupSchedulerPlugin backupPlugin = null;
    for (final var plugin : server.getPlugins()) {
      if (plugin instanceof AutoBackupSchedulerPlugin) {
        backupPlugin = (AutoBackupSchedulerPlugin) plugin;
        break;
      }
    }

    assertThat(backupPlugin).isNotNull();
    assertThat(backupPlugin.getRetentionManager()).isNotNull();
  }
}
