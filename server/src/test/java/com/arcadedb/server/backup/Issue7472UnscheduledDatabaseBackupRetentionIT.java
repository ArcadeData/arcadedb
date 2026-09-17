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
 * Issue #7472, item 2 - the residual of #7392: an on-demand {@code trigger backup} of a database that is ABSENT
 * from the auto-backup configuration was never retention-pruned, even with the scheduler running.
 * <p>
 * Two things combined to produce it. The control plane's {@code trigger backup} performs the backup INLINE, and
 * that path applied no retention at all; and even the scheduler's own retention could not have covered this case,
 * because retention registration follows the SCHEDULE - {@code BackupRetentionManager} prunes the databases it was
 * told about - while {@code trigger backup} can name any database on the server. So the disk-growth problem the
 * original issue described survived in a narrower form: trigger backups for a database nobody scheduled, and the
 * archives accumulate forever.
 * <p>
 * The scheduler here is ENABLED, so the "with auto-backup off nothing prunes" behaviour {@code #7392} documented is
 * not what is under test; the frequency is far beyond the test's lifetime so no SCHEDULED backup ever runs and
 * every archive on disk is one this test triggered.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7472UnscheduledDatabaseBackupRetentionIT extends BaseGraphServerTest {
  private static final String BACKUP_DIR_NAME = "test-backups-7472";
  private static final int    MAX_FILES       = 2;
  private static final int    TRIGGERS        = 4;

  /**
   * {@code databases} names ONE database, and it is deliberately not the one this test backs up: that is what makes
   * the database under test absent from the configuration, which is the whole case. {@code defaults} still carries
   * a retention policy, which is what the effective config falls back to.
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
  void anOnDemandBackupIsPrunedEvenForADatabaseNobodyScheduled() throws Exception {
    final String databaseName = getDatabaseName();

    final AutoBackupSchedulerPlugin plugin = (AutoBackupSchedulerPlugin) getServer(0).getPlugins().stream()
        .filter(p -> p instanceof AutoBackupSchedulerPlugin).findFirst().orElseThrow();
    assertThat(plugin.isEnabled()).as("the scheduler is running: this is not the #7392 'auto-backup off' case").isTrue();

    final File databaseBackupDir = new File(backupDir, databaseName);

    String lastArchive = null;
    for (int i = 0; i < TRIGGERS; i++) {
      final HttpResponse<String> response = postCommand("trigger backup " + databaseName);
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      lastArchive = new File(new JSONObject(response.body()).getString("backupFile")).getName();
    }

    final File[] archives = databaseBackupDir.listFiles((dir, name) -> name.endsWith(".zip"));
    assertThat(archives)
        .as(TRIGGERS + " triggers on an unscheduled database must leave only the retention policy's " + MAX_FILES)
        .hasSize(MAX_FILES);

    // The archive just written is never the one deleted: retention always keeps the most recent, so the caller's
    // response body names a file that is still there.
    assertThat(archives).extracting(File::getName).contains(lastArchive);

    // And the command's own listing agrees with the disk, which is what #7392 made possible.
    final HttpResponse<String> listing = postCommand("list backups " + databaseName);
    assertThat(listing.statusCode()).isEqualTo(200);
    assertThat(new JSONObject(listing.body()).getInt("totalCount")).isEqualTo(MAX_FILES);
  }

  private HttpResponse<String> postCommand(final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:2480/api/v1/server"))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .build();

    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }
}
