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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
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
 * With the auto-backup scheduler disabled, {@code trigger backup} wrote an archive that {@code list backups} did
 * not show, {@code delete backup} refused to remove and {@code restore backup} refused to read: the writer fell
 * back from the plugin to the config file and then to the server's backup directory, while the readers required
 * the live plugin (issue #7392). Every backup command now resolves the directory through the same chain, so
 * what one of them writes the others can see.
 * <p>
 * One server boot covers both fallbacks: the config file is present but says {@code "enabled": false}, so the
 * plugin is registered and disabled and the file's {@code backupDirectory} is the first fallback; deleting the
 * file half way through the test leaves only the server-wide {@code arcadedb.server.backupDirectory}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7392OnDemandBackupWithoutAutoBackupIT extends BaseGraphServerTest {
  private static final String CONFIG_BACKUP_DIR_NAME = "test-backups-7392-config";
  private static final String SERVER_BACKUP_DIR_NAME = "test-backups-7392-server";
  private static final String BACKUP_CONFIG          = """
      {
        "version": 1,
        "enabled": false,
        "backupDirectory": "%s",
        "defaults": {
          "enabled": true,
          "runOnServer": "*",
          "schedule": {
            "type": "frequency",
            "frequencyMinutes": 9999
          },
          "retention": {
            "maxFiles": 50
          }
        }
      }
      """.formatted(CONFIG_BACKUP_DIR_NAME);

  private File   backupConfigFile;
  private File   configBackupDir;
  private File   serverBackupDir;
  private String restoredDatabaseName;

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

      configBackupDir = freshDirectory("./target/" + CONFIG_BACKUP_DIR_NAME);
      serverBackupDir = freshDirectory("./target/" + SERVER_BACKUP_DIR_NAME);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to set up backup config", e);
    }

    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_BACKUP_DIRECTORY, serverBackupDir.getPath());
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, "auto-backup:" + AutoBackupSchedulerPlugin.class.getName());
  }

  @AfterEach
  void cleanUp() {
    if (restoredDatabaseName != null) {
      try {
        final ArcadeDBServer server = getServer(0);
        if (server.existsDatabase(restoredDatabaseName))
          server.getDatabase(restoredDatabaseName).getEmbedded().drop();
      } catch (final Exception ignore) {
        // best-effort
      }
    }

    if (backupConfigFile != null && backupConfigFile.exists())
      backupConfigFile.delete();
    if (configBackupDir != null && configBackupDir.exists())
      FileUtils.deleteRecursively(configBackupDir);
    if (serverBackupDir != null && serverBackupDir.exists())
      FileUtils.deleteRecursively(serverBackupDir);
  }

  @Test
  void everyBackupCommandAgreesOnWhereBackupsLiveWhenAutoBackupIsOff() throws Exception {
    final String databaseName = getDatabaseName();
    restoredDatabaseName = databaseName + "_restored_7392";

    // The plugin is registered but disabled by its own file, which is the configuration the issue describes.
    assertThat(getServer(0).getPlugins()).anyMatch(p -> p instanceof AutoBackupSchedulerPlugin plugin && !plugin.isEnabled());

    // --- Fallback 1: config/backup.json names the directory even though the scheduler is off ---
    final String fileFromConfig = triggerAndExpectVisible(databaseName, new File(configBackupDir, databaseName));

    final HttpResponse<String> restoreResponse = postCommand(
        "restore backup " + databaseName + " " + fileFromConfig + " as " + restoredDatabaseName);
    assertThat(restoreResponse.statusCode()).as(restoreResponse.body()).isEqualTo(200);
    assertThat(getServer(0).existsDatabase(restoredDatabaseName)).isTrue();

    deleteAndExpectGone(databaseName, fileFromConfig, new File(configBackupDir, databaseName));

    // --- Fallback 2: no config file at all, only the server-wide backup directory ---
    assertThat(backupConfigFile.delete()).isTrue();

    final String fileFromServerSetting = triggerAndExpectVisible(databaseName, new File(serverBackupDir, databaseName));
    deleteAndExpectGone(databaseName, fileFromServerSetting, new File(serverBackupDir, databaseName));

    // A database name is a path segment under the backup directory: a traversal in it is refused by every command
    // before any path is built, and trigger in particular must not create a directory outside the tree.
    for (final String command : new String[] { "trigger backup ../escaped", "list backups ../escaped",
        "delete backup ../escaped " + databaseName + "-backup-19700101-000000000.zip" }) {
      final HttpResponse<String> traversal = postCommand(command);
      assertThat(traversal.statusCode()).as(command + " -> " + traversal.body()).isEqualTo(400);
    }
    assertThat(new File(serverBackupDir.getParentFile(), "escaped")).doesNotExist();

    // A malformed config file is not the caller's fault: the chain falls through to the server setting.
    try (final FileWriter writer = new FileWriter(backupConfigFile)) {
      writer.write("{ not json");
    }
    final String fileWithBrokenConfig = triggerAndExpectVisible(databaseName, new File(serverBackupDir, databaseName));
    deleteAndExpectGone(databaseName, fileWithBrokenConfig, new File(serverBackupDir, databaseName));
    assertThat(backupConfigFile.delete()).isTrue();

    // A file name that was never written is still refused, through the same resolution.
    final HttpResponse<String> missing = postCommand("delete backup " + databaseName + " " + databaseName + "-backup-19700101-000000000.zip");
    assertThat(missing.statusCode()).isEqualTo(400);
    assertThat(missing.body()).contains("not found");
  }

  private String triggerAndExpectVisible(final String databaseName, final File expectedDirectory) throws Exception {
    final HttpResponse<String> triggerResponse = postCommand("trigger backup " + databaseName);
    assertThat(triggerResponse.statusCode()).as(triggerResponse.body()).isEqualTo(200);
    final String writtenFile = new File(new JSONObject(triggerResponse.body()).getString("backupFile")).getName();

    final File[] onDisk = expectedDirectory.listFiles((dir, name) -> name.endsWith(".zip"));
    assertThat(onDisk).as("the archive must land in the directory the resolution chain names").hasSize(1);
    assertThat(onDisk[0].getName()).isEqualTo(writtenFile);

    final HttpResponse<String> listResponse = postCommand("list backups " + databaseName);
    assertThat(listResponse.statusCode()).isEqualTo(200);
    final JSONObject listing = new JSONObject(listResponse.body());
    final JSONArray backups = listing.getJSONArray("backups");
    assertThat(backups.length()).as("the archive trigger just wrote must be listed: " + listing).isEqualTo(1);
    assertThat(backups.getJSONObject(0).getString("fileName")).isEqualTo(writtenFile);
    assertThat(listing.getInt("totalCount")).isEqualTo(1);
    assertThat(listing.getLong("totalSize")).isEqualTo(onDisk[0].length());

    return writtenFile;
  }

  private void deleteAndExpectGone(final String databaseName, final String fileName, final File directory) throws Exception {
    final HttpResponse<String> deleteResponse = postCommand("delete backup " + databaseName + " " + fileName);
    assertThat(deleteResponse.statusCode()).as(deleteResponse.body()).isEqualTo(200);
    assertThat(new File(directory, fileName)).doesNotExist();

    final HttpResponse<String> listAfterDelete = postCommand("list backups " + databaseName);
    assertThat(new JSONObject(listAfterDelete.body()).getJSONArray("backups").length()).isZero();
  }

  private static File freshDirectory(final String path) {
    final File dir = new File(path);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    dir.mkdirs();
    return dir;
  }

  private HttpResponse<String> postCommand(final String command) throws Exception {
    final HttpClient client = HttpClient.newHttpClient();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/server"))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
