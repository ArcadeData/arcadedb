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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6752. An auto-backup schedule used to outlive its database: nothing cancelled it on drop or close, and the
 * backup task resolved the database with load-on-demand. The next tick therefore reopened a database the operator
 * had explicitly closed, or - after a drop - threw and logged SEVERE plus a CRITICAL server event on every fire,
 * forever. Symmetrically, a database created after the plugin started was never scheduled at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6752AutoBackupScheduleLifecycleIT extends BaseGraphServerTest {
  private static final String BACKUP_CONFIG = """
      {
        "version": 1,
        "enabled": true,
        "backupDirectory": "test-backups-6752",
        "defaults": {
          "enabled": true,
          "runOnServer": "*",
          "schedule": {
            "type": "frequency",
            "frequencyMinutes": 60
          },
          "retention": {
            "maxFiles": 5
          }
        }
      }
      """;

  private static final String RUNTIME_DATABASE = "issue6752runtime";

  private File backupConfigFile;
  private File backupDir;

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    try {
      final File configDir = new File("./target/config");
      configDir.mkdirs();

      backupConfigFile = new File(configDir, "backup.json");
      try (final FileWriter writer = new FileWriter(backupConfigFile)) {
        writer.write(BACKUP_CONFIG);
      }

      backupDir = new File("./target/test-backups-6752");
      if (backupDir.exists())
        FileUtils.deleteRecursively(backupDir);
      backupDir.mkdirs();
    } catch (final IOException e) {
      throw new RuntimeException("Failed to set up backup config", e);
    }

    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, "auto-backup:" + AutoBackupSchedulerPlugin.class.getName());
  }

  @AfterEach
  void cleanUpBackupConfig() {
    final ArcadeDBServer server = getServer(0);
    if (server != null) {
      try {
        // getDatabase() reopens it from disk when a test left it closed, so this drops it either way.
        server.getDatabase(RUNTIME_DATABASE).getEmbedded().drop();
        server.removeDatabase(RUNTIME_DATABASE);
      } catch (final Exception e) {
        // NEVER CREATED BY THIS TEST: NOTHING TO CLEAN UP
      }
    }

    if (backupConfigFile != null && backupConfigFile.exists())
      backupConfigFile.delete();

    if (backupDir != null && backupDir.exists())
      FileUtils.deleteRecursively(backupDir);
  }

  @Test
  void aDatabaseCreatedAtRuntimeIsScheduledAndUnscheduledOnDrop() {
    final ArcadeDBServer server = getServer(0);
    final BackupScheduler scheduler = backupPlugin(server).getScheduler();

    assertThat(scheduler.getScheduledDatabases()).doesNotContain(RUNTIME_DATABASE);

    final ServerDatabase created = server.createDatabase(RUNTIME_DATABASE, ComponentFile.MODE.READ_WRITE);

    // BEFORE THE FIX A RUNTIME DATABASE WAS NEVER SCHEDULED UNTIL SOMEBODY RELOADED THE CONFIGURATION BY HAND.
    assertThat(scheduler.getScheduledDatabases()).contains(RUNTIME_DATABASE);

    created.getEmbedded().drop();
    server.removeDatabase(RUNTIME_DATABASE);

    // AND THE SCHEDULE USED TO SURVIVE THE DROP, THROWING AND LOGGING CRITICAL ON EVERY SINGLE TICK.
    assertThat(scheduler.getScheduledDatabases()).doesNotContain(RUNTIME_DATABASE);
  }

  @Test
  void closingADatabaseCancelsItsSchedule() {
    final ArcadeDBServer server = getServer(0);
    final BackupScheduler scheduler = backupPlugin(server).getScheduler();

    assertThat(scheduler.getScheduledDatabases()).contains(getDatabaseName());

    server.getDatabase(getDatabaseName()).getEmbedded().close();
    server.removeDatabase(getDatabaseName());
    try {
      assertThat(scheduler.getScheduledDatabases()).doesNotContain(getDatabaseName());
    } finally {
      // REOPEN SO THE REST OF THE SUITE (AND THE BASE CLASS TEARDOWN) STILL FINDS THE DATABASE.
      server.getDatabase(getDatabaseName());
    }

    // REOPENING RE-REGISTERS IT, SO THE SCHEDULE IS BACK.
    assertThat(scheduler.getScheduledDatabases()).contains(getDatabaseName());
  }

  /**
   * The belt-and-braces half, and the more insidious one: the database is still on disk, so a tick that resolves it
   * with load-on-demand quietly reopens what the operator closed and pins the page cache again. The task must skip
   * instead. This drives the task directly, so it holds even for a schedule that somehow survived the callback.
   */
  @Test
  void theBackupTaskNeverReopensAClosedDatabase() {
    final ArcadeDBServer server = getServer(0);

    server.createDatabase(RUNTIME_DATABASE, ComponentFile.MODE.READ_WRITE).getEmbedded().close();
    server.removeDatabase(RUNTIME_DATABASE);
    assertThat(server.existsDatabase(RUNTIME_DATABASE)).isFalse();

    final DatabaseBackupConfig config = new DatabaseBackupConfig(RUNTIME_DATABASE);
    config.setEnabled(true);

    new BackupTask(server, RUNTIME_DATABASE, config, backupDir.getAbsolutePath(), null).run();

    assertThat(server.existsDatabase(RUNTIME_DATABASE)).isFalse();
    assertThat(new File(backupDir, RUNTIME_DATABASE)).doesNotExist();
  }

  private static AutoBackupSchedulerPlugin backupPlugin(final ArcadeDBServer server) {
    for (final var plugin : server.getPlugins())
      if (plugin instanceof AutoBackupSchedulerPlugin backupPlugin)
        return backupPlugin;

    throw new AssertionError("AutoBackupSchedulerPlugin not installed");
  }
}
