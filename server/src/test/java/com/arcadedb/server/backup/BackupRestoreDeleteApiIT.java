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
 * Integration tests for the "restore backup" and "delete backup" server commands that back the
 * Studio backup row actions (issue #4737).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BackupRestoreDeleteApiIT extends BaseGraphServerTest {
  private static final String BACKUP_DIR_NAME = "test-backups-restore-delete";
  private static final String BACKUP_CONFIG   = """
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
            "maxFiles": 50
          }
        }
      }
      """.formatted(BACKUP_DIR_NAME);

  private File   backupConfigFile;
  private File   backupDir;
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

      backupDir = new File("./target/" + BACKUP_DIR_NAME);
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
  void cleanUp() {
    // Drop the restored database created during the test, if any.
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
    if (backupDir != null && backupDir.exists())
      FileUtils.deleteRecursively(backupDir);
  }

  @Test
  void restoreAndDeleteBackup() throws Exception {
    final String databaseName = getDatabaseName();
    restoredDatabaseName = databaseName + "_restored";

    // 1) Trigger a backup so we have a real backup file to operate on.
    final HttpResponse<String> triggerResponse = postCommand("trigger backup " + databaseName);
    assertThat(triggerResponse.statusCode()).isEqualTo(200);

    // 2) List the backups and grab the produced file name.
    final HttpResponse<String> listResponse = postCommand("list backups " + databaseName);
    assertThat(listResponse.statusCode()).isEqualTo(200);

    final JSONArray backups = new JSONObject(listResponse.body()).getJSONArray("backups");
    assertThat(backups.length()).isEqualTo(1);
    final String fileName = backups.getJSONObject(0).getString("fileName");
    assertThat(fileName).contains("-backup-").endsWith(".zip");

    final long sourceCount = getServer(0).getDatabase(databaseName).countType(VERTEX1_TYPE_NAME, false);

    // 3) Restore the backup into a NEW database (no overwrite needed).
    final HttpResponse<String> restoreResponse = postCommand(
        "restore backup " + databaseName + " " + fileName + " as " + restoredDatabaseName);
    assertThat(restoreResponse.statusCode()).isEqualTo(200);
    assertThat(getServer(0).existsDatabase(restoredDatabaseName)).isTrue();
    assertThat(getServer(0).getDatabase(restoredDatabaseName).countType(VERTEX1_TYPE_NAME, false)).isEqualTo(sourceCount);

    // 4) Restoring again over the existing database WITHOUT overwrite must fail.
    final HttpResponse<String> conflictResponse = postCommand(
        "restore backup " + databaseName + " " + fileName + " as " + restoredDatabaseName);
    assertThat(conflictResponse.statusCode()).isEqualTo(400);
    assertThat(conflictResponse.body()).contains("already exists");

    // 5) Restoring again WITH overwrite must succeed.
    final HttpResponse<String> overwriteResponse = postCommand(
        new JSONObject().put("command", "restore backup " + databaseName + " " + fileName + " as " + restoredDatabaseName)
            .put("overwrite", true));
    assertThat(overwriteResponse.statusCode()).isEqualTo(200);
    assertThat(getServer(0).getDatabase(restoredDatabaseName).countType(VERTEX1_TYPE_NAME, false)).isEqualTo(sourceCount);

    // 6) Path traversal in the file name must be rejected.
    final HttpResponse<String> traversalResponse = postCommand("delete backup " + databaseName + " ../../evil.zip");
    assertThat(traversalResponse.statusCode()).isEqualTo(400);

    // 7) Delete the backup file and verify it is gone.
    final HttpResponse<String> deleteResponse = postCommand("delete backup " + databaseName + " " + fileName);
    assertThat(deleteResponse.statusCode()).isEqualTo(200);

    final HttpResponse<String> listAfterDelete = postCommand("list backups " + databaseName);
    assertThat(listAfterDelete.statusCode()).isEqualTo(200);
    assertThat(new JSONObject(listAfterDelete.body()).getJSONArray("backups").length()).isZero();
  }

  private HttpResponse<String> postCommand(final String command) throws Exception {
    return postCommand(new JSONObject().put("command", command));
  }

  private HttpResponse<String> postCommand(final JSONObject payload) throws Exception {
    final HttpClient client = HttpClient.newHttpClient();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/server"))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
