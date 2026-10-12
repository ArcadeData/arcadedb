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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9844: the SQL {@code BACKUP DATABASE} statement applied no retention. Issue #7472 made the control plane's
 * {@code trigger backup} prune after an on-demand backup, by what is in the backup directory rather than by what is
 * registered with the scheduler, and left the statement - the other on-demand entry point - out. Since #7863 it writes
 * into the same {@code <backup directory>/<database>} the scheduler and {@code trigger backup} prune, so for a database
 * ABSENT from the auto-backup configuration nothing ever pruned it and repeated statements grew the directory forever.
 * <p>
 * The scheduler here is ENABLED and its frequency is far beyond the test's lifetime, so no scheduled backup ever runs and
 * every archive on disk is one this test wrote.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9844SqlBackupAppliesRetentionIT extends BaseGraphServerTest {
  private static final String BACKUP_DIR_NAME = "test-backups-9844";
  private static final int    MAX_FILES       = 2;
  private static final int    STATEMENTS      = 4;

  /**
   * {@code databases} names ONE database, deliberately not the one this test backs up: that is what makes the database
   * under test absent from the configuration, which is the whole case. {@code defaults} still carries a retention policy,
   * which is what the effective config falls back to.
   */
  private static final String BACKUP_CONFIG = """
      {
        "version": 1,
        "enabled": true,
        "backupDirectory": "%s",
        "defaults": {
          "enabled": true,
          "runOnServer": "*",
          "schedule": {
            "type": "frequency",
            "frequencyMinutes": 9999
          },
          "retention": {
            "maxFiles": %d
          }
        },
        "databases": {
          "some-other-database": {
            "enabled": true,
            "schedule": {
              "type": "frequency",
              "frequencyMinutes": 9999
            }
          }
        }
      }
      """.formatted(BACKUP_DIR_NAME, MAX_FILES);

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

      backupDir = new File("./target/" + BACKUP_DIR_NAME);
      FileUtils.deleteRecursively(backupDir);
      backupDir.mkdirs();
    } catch (final IOException e) {
      throw new RuntimeException("Failed to set up backup config", e);
    }

    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, "auto-backup:" + AutoBackupSchedulerPlugin.class.getName());
  }

  @AfterEach
  void cleanUp() {
    if (backupConfigFile != null && backupConfigFile.exists())
      backupConfigFile.delete();
    if (backupDir != null && backupDir.exists())
      FileUtils.deleteRecursively(backupDir);
  }

  @Test
  void repeatedSqlBackupsOfAnUnscheduledDatabaseArePruned() throws Exception {
    final String databaseName = getDatabaseName();

    final AutoBackupSchedulerPlugin plugin = (AutoBackupSchedulerPlugin) getServer(0).getPlugins().stream()
        .filter(p -> p instanceof AutoBackupSchedulerPlugin).findFirst().orElseThrow();
    assertThat(plugin.isEnabled()).as("the scheduler is running: this is not the 'auto-backup off' case").isTrue();

    String lastArchive = null;
    for (int i = 0; i < STATEMENTS; i++) {
      final HttpResponse<String> response = postDatabaseCommand(databaseName, "BACKUP DATABASE");
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      lastArchive = new File(new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getString("backupFile")).getName();
    }

    final File[] archives = new File(backupDir, databaseName).listFiles((dir, name) -> name.endsWith(".zip"));
    assertThat(archives)
        .as(STATEMENTS + " BACKUP DATABASE statements on an unscheduled database must leave only the retention policy's "
            + MAX_FILES)
        .hasSize(MAX_FILES);

    // The archive just written is never the one deleted: retention always keeps the most recent
    assertThat(archives).extracting(File::getName).contains(lastArchive);
  }

  /**
   * An archive the operator named with {@code BACKUP DATABASE '<name>'} does not follow the {@code <db>-backup-<timestamp>.zip}
   * convention, so retention can neither date it nor tell it from a file it has no business deleting. It is invisible to
   * retention whichever entry point writes it, and that is the right answer: a name chosen on purpose is a restore point
   * the operator keeps. The statement's own conventionally named archives are still pruned around it.
   */
  @Test
  void anArchiveNamedByTheOperatorIsLeftAlone() throws Exception {
    final String databaseName = getDatabaseName();

    final HttpResponse<String> named = postDatabaseCommand(databaseName, "BACKUP DATABASE 'before-the-upgrade.zip'");
    assertThat(named.statusCode()).as(named.body()).isEqualTo(200);

    for (int i = 0; i < STATEMENTS; i++) {
      final HttpResponse<String> response = postDatabaseCommand(databaseName, "BACKUP DATABASE");
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    }

    final File databaseBackupDir = new File(backupDir, databaseName);
    assertThat(new File(databaseBackupDir, "before-the-upgrade.zip")).as("the operator's own archive is never pruned").exists();
    assertThat(databaseBackupDir.listFiles((dir, name) -> name.contains("-backup-") && name.endsWith(".zip")))
        .as("the conventionally named archives are pruned around it").hasSize(MAX_FILES);
  }

  /**
   * Retention prunes the directory of the database that was backed up and nothing else: another database sharing the backup
   * root keeps every archive, even when it holds more than the policy's {@code maxFiles}.
   */
  @Test
  void onlyTheDirectoryOfTheBackedUpDatabaseIsPruned() throws Exception {
    final File otherDatabaseDir = new File(backupDir, "some-other-database");
    assertThat(otherDatabaseDir.mkdirs()).isTrue();
    final String[] otherArchives = { "some-other-database-backup-20260101-000000001.zip", "some-other-database-backup-20260102-000000001.zip",
        "some-other-database-backup-20260103-000000001.zip", "some-other-database-backup-20260104-000000001.zip" };
    for (final String name : otherArchives)
      assertThat(new File(otherDatabaseDir, name).createNewFile()).isTrue();

    for (int i = 0; i < STATEMENTS; i++) {
      final HttpResponse<String> response = postDatabaseCommand(getDatabaseName(), "BACKUP DATABASE");
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    }

    assertThat(new File(backupDir, getDatabaseName()).listFiles((dir, name) -> name.endsWith(".zip"))).hasSize(MAX_FILES);
    assertThat(otherDatabaseDir.listFiles((dir, name) -> name.endsWith(".zip")))
        .as("the other database's directory is not the statement's to prune").hasSize(otherArchives.length);
  }

  private HttpResponse<String> postDatabaseCommand(final String databaseName, final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl("/api/v1/command/") + databaseName))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", "sql").put("command", command).toString()))
        .build();

    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }
}
