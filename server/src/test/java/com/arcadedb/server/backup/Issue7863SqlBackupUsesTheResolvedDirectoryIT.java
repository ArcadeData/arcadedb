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
 * Issue #7863: issue #7392 gave every control-plane backup command one definition of where this server keeps
 * its backups - {@code ServerControlPlane.resolveBackupDirectory()}: the running auto-backup plugin's
 * configuration first, then {@code config/backup.json} on disk even when the schedule is off, then
 * {@code arcadedb.server.backupDirectory}. The SQL {@code BACKUP DATABASE} statement was not brought onto it
 * and kept reading the server setting directly.
 * <p>
 * So on the very configuration #7392 was filed about - a {@code config/backup.json} naming a different
 * directory - an archive written by {@code BACKUP DATABASE} over {@code POST /api/v1/command} landed where
 * {@code list backups} does not look and {@code delete backup} and {@code restore backup} cannot reach:
 * invisible, undeletable and unrestorable through the API, and never retention-pruned. The #7392 symptom on a
 * different entry point.
 * <p>
 * The statement is executed by the ENGINE, which cannot see the server, so the resolution is handed to it the
 * way {@code MaintenanceCoordinator} is: {@code BackupDirectoryResolver} is an interface the engine names and
 * {@code ServerDatabase} binds to every database this server opens. This drives the shape #7392's own test
 * covers - write with {@code BACKUP DATABASE}, then {@code list backups} and {@code delete backup} must see
 * and remove exactly that file - with {@code config/backup.json} naming a non-default directory and the
 * schedule off.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7863SqlBackupUsesTheResolvedDirectoryIT extends BaseGraphServerTest {
  private static final String CONFIG_BACKUP_DIR_NAME = "test-backups-7863-config";
  private static final String SERVER_BACKUP_DIR_NAME = "test-backups-7863-server";
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

  private File backupConfigFile;
  private File configBackupDir;
  private File serverBackupDir;

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
    if (backupConfigFile != null && backupConfigFile.exists())
      backupConfigFile.delete();
    if (configBackupDir != null && configBackupDir.exists())
      FileUtils.deleteRecursively(configBackupDir);
    if (serverBackupDir != null && serverBackupDir.exists())
      FileUtils.deleteRecursively(serverBackupDir);
  }

  @Test
  void aSqlBackupLandsWhereTheControlPlaneLooksForIt() throws Exception {
    final String databaseName = getDatabaseName();

    final HttpResponse<String> backup = postDatabaseCommand(databaseName, "BACKUP DATABASE");
    assertThat(backup.statusCode()).as(backup.body()).isEqualTo(200);

    final File[] inConfigDir = new File(configBackupDir, databaseName).listFiles((dir, name) -> name.endsWith(".zip"));
    assertThat(inConfigDir).as("the archive must land in the directory config/backup.json names").hasSize(1);
    final String writtenFile = inConfigDir[0].getName();

    assertThat(new File(serverBackupDir, databaseName))
        .as("and NOT in the server-wide directory the statement used to resolve on its own").doesNotExist();

    // Visible: the same file, through the command that could not see it before.
    final HttpResponse<String> listed = postServerCommand("list backups " + databaseName);
    assertThat(listed.statusCode()).as(listed.body()).isEqualTo(200);
    final JSONArray backups = new JSONObject(listed.body()).getJSONArray("backups");
    assertThat(backups.length()).as("the archive BACKUP DATABASE just wrote must be listed: " + listed.body()).isEqualTo(1);
    assertThat(backups.getJSONObject(0).getString("fileName")).isEqualTo(writtenFile);

    // Reachable: and therefore deletable through the API, which is what "never retention-pruned" came down to.
    final HttpResponse<String> deleted = postServerCommand("delete backup " + databaseName + " " + writtenFile);
    assertThat(deleted.statusCode()).as(deleted.body()).isEqualTo(200);
    assertThat(new File(new File(configBackupDir, databaseName), writtenFile)).doesNotExist();
  }

  /**
   * With no {@code config/backup.json} the chain ends at {@code arcadedb.server.backupDirectory}, which is
   * where the statement always wrote. Nothing about that case may change.
   */
  @Test
  void withNoConfigFileTheStatementStillWritesToTheServerDirectory() throws Exception {
    assertThat(backupConfigFile.delete()).isTrue();
    final String databaseName = getDatabaseName();

    final HttpResponse<String> backup = postDatabaseCommand(databaseName, "BACKUP DATABASE");
    assertThat(backup.statusCode()).as(backup.body()).isEqualTo(200);

    final File[] inServerDir = new File(serverBackupDir, databaseName).listFiles((dir, name) -> name.endsWith(".zip"));
    assertThat(inServerDir).hasSize(1);

    final HttpResponse<String> listed = postServerCommand("list backups " + databaseName);
    assertThat(new JSONObject(listed.body()).getJSONArray("backups").length()).isEqualTo(1);
  }

  private static File freshDirectory(final String path) {
    final File dir = new File(path);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    dir.mkdirs();
    return dir;
  }

  private HttpResponse<String> postDatabaseCommand(final String databaseName, final String command) throws Exception {
    return post(getServerHttpUrl("/api/v1/command/") + databaseName,
        new JSONObject().put("language", "sql").put("command", command));
  }

  private HttpResponse<String> postServerCommand(final String command) throws Exception {
    return post(getServerHttpUrl("/api/v1/server"), new JSONObject().put("command", command));
  }

  private HttpResponse<String> post(final String url, final JSONObject payload) throws Exception {
    final HttpClient client = HttpClient.newHttpClient();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(url))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
