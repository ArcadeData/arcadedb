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
import org.junit.jupiter.api.AfterAll;
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
 * Issue #7308: {@code restore backup <db> <file> as <target>} never validated the <b>target</b>
 * database name, while {@code restore database} validated its own with
 * {@code ArcadeDBServer.checkDatabaseNameIsValid}. The target becomes a directory under
 * {@code SERVER_DATABASE_DIRECTORY} exactly the same way, so the gap was a traversal sink waiting
 * for a caller.
 * <p>
 * It surfaced when restore moved into the shared {@code ServerControlPlane} for the gRPC RPCs: the
 * new transport would have inherited the gap, so the check went into the shared implementation and
 * both transports now enforce it. This test asserts the HTTP side; the gRPC side is asserted by
 * {@code Issue7308GrpcRestoreImportIT.restoreBackupRefusesATargetNameThatLeavesTheDatabaseDirectory}.
 * <p>
 * The archive file name is deliberately one that does not exist. The name check has to run
 * <b>before</b> the archive is resolved, or a traversal target would be rejected only by luck -
 * whenever the caller happened to name a real backup.
 */
class Issue7308RestoreTargetNameIT extends BaseGraphServerTest {
  private static final String BACKUP_DIR_NAME = "test-backups-7308-target-name";
  private static final String BACKUP_CONFIG   = """
      {
        "version": 1,
        "enabled": true,
        "backupDirectory": "%s",
        "defaults": {
          "enabled": true,
          "runOnServer": "*",
          "schedule": { "type": "frequency", "frequencyMinutes": 9999 },
          "retention": { "maxFiles": 50 }
        }
      }
      """.formatted(BACKUP_DIR_NAME);

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
      if (backupDir.exists())
        FileUtils.deleteRecursively(backupDir);
      backupDir.mkdirs();
    } catch (final IOException e) {
      throw new RuntimeException("Failed to set up the backup configuration", e);
    }

    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, "auto-backup:" + AutoBackupSchedulerPlugin.class.getName());
  }

  /**
   * Cleaned up once, after the whole class. Per-test cleanup would delete the backup configuration
   * out from under the server fixture that the remaining tests still share.
   */
  @AfterAll
  static void cleanUp() {
    final File config = new File("./target/config/backup.json");
    if (config.exists())
      config.delete();
    final File backups = new File("./target/" + BACKUP_DIR_NAME);
    if (backups.exists())
      FileUtils.deleteRecursively(backups);
  }

  @Test
  void restoreBackupRefusesATargetNameCarryingATraversalSequence() throws Exception {
    final HttpResponse<String> response = postCommand(
        "restore backup " + getDatabaseName() + " nonexistent-backup-20260101.zip as ../escaped7308");

    assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
    assertThat(response.body()).contains("invalid characters");
    assertThat(new File("./target/../escaped7308")).doesNotExist();
  }

  @Test
  void restoreBackupRefusesATargetNameCarryingAPathSeparator() throws Exception {
    final HttpResponse<String> response = postCommand(
        "restore backup " + getDatabaseName() + " nonexistent-backup-20260101.zip as sub/escaped7308");

    assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
    assertThat(response.body()).contains("invalid characters");
  }

  /**
   * A plain target name still gets past the name check and fails for the reason it should - the
   * archive is not there. Without this the tests above would pass against a check that refused every
   * name.
   */
  @Test
  void restoreBackupWithAPlainTargetNameFailsOnTheMissingArchiveInstead() throws Exception {
    final HttpResponse<String> response = postCommand(
        "restore backup " + getDatabaseName() + " nonexistent-backup-20260101.zip as plain7308");

    assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
    assertThat(response.body()).contains("Backup file not found");
    assertThat(getServer(0).existsDatabase("plain7308")).isFalse();
  }

  /**
   * The restore commands still answer an HTTP status code, not a 200 carrying an error frame, when
   * the request is rejected before the operation starts - even from a client asking for SSE. The
   * stream begins on the first progress event, so a request that never produces one is still a plain
   * HTTP failure (issue #7308).
   */
  @Test
  void anSseClientStillGetsAnHttpStatusForARequestRejectedBeforeTheRestoreStarts() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/server"))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .header("Accept", "text/event-stream")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("command", "restore database " + getDatabaseName() + " https://example.invalid/archive.zip").toString()))
        .build();

    final HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

    // 403, the URL guard - the first gate this command applies, and one that runs long before any
    // progress could be reported. What matters is that it arrives as a status with a JSON body, not
    // as a 200 whose body happens to carry an SSE error frame.
    assertThat(response.statusCode()).as(response.body()).isEqualTo(403);
    assertThat(response.body()).contains("Restore/import");
    assertThat(response.body()).doesNotContain("data: ");
  }

  /**
   * The archive-name guard is unchanged and still runs: the target-name check added by #7308 sits
   * next to it, it does not replace it.
   */
  @Test
  void restoreBackupStillRefusesAFileNameThatLeavesTheBackupDirectory() throws Exception {
    final HttpResponse<String> response = postCommand(
        "restore backup " + getDatabaseName() + " ../../evil.zip as plain7308b");

    assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
    assertThat(new JSONObject(response.body()).getString("detail")).contains("Invalid backup file name");
  }

  private HttpResponse<String> postCommand(final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/server"))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .build();
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }
}
