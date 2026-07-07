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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5027:
 * <ul>
 *   <li>SSRF / local-file-read: {@code restore database} / {@code import database} must reject a
 *       client-supplied {@code file://} URL and private/link-local network hosts by default.</li>
 *   <li>Durability: a failed overwrite {@code restore backup} must leave the original database intact.</li>
 * </ul>
 */
class RestoreImportSecurityDurabilityIT extends BaseGraphServerTest {
  private static final String BACKUP_DIR_NAME = "test-backups-ssrf-durability";
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
    // Intentionally leave arcadedb.server.restoreImportAllowLocalUrls at its secure default (false).
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
    if (backupDir != null && backupDir.exists())
      FileUtils.deleteRecursively(backupDir);
  }

  @Test
  void restoreDatabaseRejectsFileUrlByDefault() throws Exception {
    final HttpResponse<String> response = postCommand("restore database ssrf_target file:///etc/passwd");
    assertThat(response.statusCode()).as("file:// URL must be rejected by default").isEqualTo(403);
    assertThat(getServer(0).existsDatabase("ssrf_target")).isFalse();
  }

  @Test
  void importDatabaseRejectsLinkLocalHostByDefault() throws Exception {
    // 169.254.169.254 is the cloud metadata endpoint (link-local range).
    final HttpResponse<String> response = postCommand(
        "import database ssrf_import http://169.254.169.254/latest/meta-data/");
    assertThat(response.statusCode()).as("link-local host must be rejected by default").isEqualTo(403);
    assertThat(getServer(0).existsDatabase("ssrf_import")).isFalse();
  }

  @Test
  void importDatabaseRejectsLoopbackHostByDefault() throws Exception {
    final HttpResponse<String> response = postCommand("import database ssrf_loopback http://127.0.0.1:9/x.tgz");
    assertThat(response.statusCode()).as("loopback host must be rejected by default").isEqualTo(403);
    assertThat(getServer(0).existsDatabase("ssrf_loopback")).isFalse();
  }

  @Test
  void importDatabaseRejectsCgnatHostByDefault() throws Exception {
    // 100.64.0.0/10 (RFC 6598) is not covered by InetAddress.isSiteLocalAddress().
    final HttpResponse<String> response = postCommand("import database ssrf_cgnat http://100.64.0.1/x.tgz");
    assertThat(response.statusCode()).as("CGNAT host must be rejected by default").isEqualTo(403);
    assertThat(getServer(0).existsDatabase("ssrf_cgnat")).isFalse();
  }

  @Test
  void failedOverwriteRestorePreservesOriginalDatabase() throws Exception {
    final String databaseName = getDatabaseName();
    restoredDatabaseName = databaseName + "_durability";

    // 1) Produce a real backup of the seeded database.
    final HttpResponse<String> triggerResponse = postCommand("trigger backup " + databaseName);
    assertThat(triggerResponse.statusCode()).isEqualTo(200);

    final HttpResponse<String> listResponse = postCommand("list backups " + databaseName);
    assertThat(listResponse.statusCode()).isEqualTo(200);
    final JSONArray backups = new JSONObject(listResponse.body()).getJSONArray("backups");
    assertThat(backups.length()).isEqualTo(1);
    final String fileName = backups.getJSONObject(0).getString("fileName");

    final long sourceCount = getServer(0).getDatabase(databaseName).countType(VERTEX1_TYPE_NAME, false);

    // 2) Restore the good backup into a fresh target database.
    final HttpResponse<String> restoreResponse = postCommand(
        "restore backup " + databaseName + " " + fileName + " as " + restoredDatabaseName);
    assertThat(restoreResponse.statusCode()).isEqualTo(200);
    assertThat(getServer(0).getDatabase(restoredDatabaseName).countType(VERTEX1_TYPE_NAME, false)).isEqualTo(sourceCount);

    // 3) Corrupt the backup file on disk so the next restore fails mid-way.
    final File backupFile = new File(new File(backupDir, databaseName), fileName);
    assertThat(backupFile.exists()).isTrue();
    Files.write(backupFile.toPath(), "not a valid zip archive".getBytes(StandardCharsets.UTF_8));

    // 4) Overwrite-restore the existing target with the now-corrupt backup: it must fail.
    final HttpResponse<String> overwriteResponse = postCommand(
        new JSONObject().put("command", "restore backup " + databaseName + " " + fileName + " as " + restoredDatabaseName)
            .put("overwrite", true));
    assertThat(overwriteResponse.statusCode()).as("corrupt restore must not report success").isNotEqualTo(200);

    // 5) The original target database and its data must still be intact.
    assertThat(getServer(0).existsDatabase(restoredDatabaseName)).as("original database must survive a failed restore").isTrue();
    assertThat(getServer(0).getDatabase(restoredDatabaseName).countType(VERTEX1_TYPE_NAME, false)).isEqualTo(sourceCount);
  }

  @Test
  void successfulOverwriteRestoreReplacesExistingDatabase() throws Exception {
    final String databaseName = getDatabaseName();
    restoredDatabaseName = databaseName + "_overwrite";

    postCommand("trigger backup " + databaseName);
    final JSONArray backups = new JSONObject(postCommand("list backups " + databaseName).body()).getJSONArray("backups");
    final String fileName = backups.getJSONObject(0).getString("fileName");
    final long sourceCount = getServer(0).getDatabase(databaseName).countType(VERTEX1_TYPE_NAME, false);

    // Restore into a fresh target, then overwrite it again with the same good backup: must succeed
    // and keep the data (verifies the temp-restore-then-swap happy path).
    assertThat(postCommand("restore backup " + databaseName + " " + fileName + " as " + restoredDatabaseName)
        .statusCode()).isEqualTo(200);
    final HttpResponse<String> overwrite = postCommand(
        new JSONObject().put("command", "restore backup " + databaseName + " " + fileName + " as " + restoredDatabaseName)
            .put("overwrite", true));
    assertThat(overwrite.statusCode()).isEqualTo(200);
    assertThat(getServer(0).existsDatabase(restoredDatabaseName)).isTrue();
    assertThat(getServer(0).getDatabase(restoredDatabaseName).countType(VERTEX1_TYPE_NAME, false)).isEqualTo(sourceCount);
  }

  @Test
  void allowConfigGatesFileUrlRestore() throws Exception {
    final String databaseName = getDatabaseName();
    final String target = "allow_cfg_target";
    try {
      postCommand("trigger backup " + databaseName);
      final JSONArray backups = new JSONObject(postCommand("list backups " + databaseName).body()).getJSONArray("backups");
      final String fileName = backups.getJSONObject(0).getString("fileName");
      final File backupFile = new File(new File(backupDir, databaseName), fileName);
      assertThat(backupFile.exists()).isTrue();

      // Blocked by default.
      assertThat(postCommand("restore database " + target + " file://" + backupFile.getAbsolutePath())
          .statusCode()).isEqualTo(403);

      // Enabling the allow flag opens the gate for local-file URLs.
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
      assertThat(postCommand("restore database " + target + " file://" + backupFile.getAbsolutePath())
          .statusCode()).isEqualTo(200);
      assertThat(getServer(0).existsDatabase(target)).isTrue();
    } finally {
      final ArcadeDBServer server = getServer(0);
      if (server.existsDatabase(target))
        server.getDatabase(target).getEmbedded().drop();
    }
  }

  private HttpResponse<String> postCommand(final String command) throws Exception {
    return postCommand(new JSONObject().put("command", command));
  }

  private HttpResponse<String> postCommand(final JSONObject payload) throws Exception {
    final int port = getServer(0).getHttpServer().getPort();
    final HttpClient client = HttpClient.newHttpClient();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:" + port + "/api/v1/server"))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
