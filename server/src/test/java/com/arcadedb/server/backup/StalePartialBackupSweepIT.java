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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #12530: the archive of a backup is written under {@code <name>.zip.partial} until it is complete (issue
 * #11736), so a backup killed mid-write ({@code kill -9}, an OOM) leaves the partial behind. The only code that deletes one,
 * {@code BackupRetentionManager.deleteStalePartialArchives}, ran only from the retention of the auto-backup plugin, which does
 * nothing while auto-backup is off. That is the default (no {@code config/backup.json}), and the on-demand {@code trigger backup}
 * and SQL {@code BACKUP DATABASE} write partials just the same: nothing could remove the file but access to the file system,
 * since {@code list}, {@code delete} and {@code restore backup} all require a name ending {@code .zip}.
 * <p>
 * The sweep now runs after every on-demand backup whatever the auto-backup state is. These tests cover the two ways the plugin,
 * which the server always registers, is disabled and its retention does nothing: no {@code config/backup.json} at all (the
 * default), and one that says {@code "enabled": false}.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class StalePartialBackupSweepIT {
  private static final DateTimeFormatter ARCHIVE_TIMESTAMP = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmssSSS");

  /** What a partial and a complete archive left behind by earlier backups look like, and what the sweep may not touch. */
  private static final class Leftovers {
    final File stalePartial;
    final File freshPartial;
    final File completeArchive;

    Leftovers(final File databaseBackupDir, final String databaseName) throws IOException {
      Files.createDirectories(databaseBackupDir.toPath());

      stalePartial = write(databaseBackupDir, databaseName + "-backup-20200101-000000000.zip" + BackupRetentionManager.PARTIAL_SUFFIX);
      // Unwritten for longer than a running backup can leave it: what a backup cut by a crash looks like
      assertThat(stalePartial.setLastModified(System.currentTimeMillis() - BackupRetentionManager.STALE_PARTIAL_MILLIS - 60_000))
          .isTrue();

      // Written a moment ago: a backup of this directory, by another process, may still be at it
      freshPartial = write(databaseBackupDir,
          databaseName + "-backup-" + LocalDateTime.now().minusMinutes(1).format(ARCHIVE_TIMESTAMP) + ".zip"
              + BackupRetentionManager.PARTIAL_SUFFIX);

      completeArchive = write(databaseBackupDir, databaseName + "-backup-20200102-000000000.zip");
      assertThat(completeArchive.setLastModified(System.currentTimeMillis() - 2 * BackupRetentionManager.STALE_PARTIAL_MILLIS))
          .isTrue();
    }

    void assertSweptButNothingElse(final String why) {
      assertThat(stalePartial).as("the partial a crashed backup left, " + why).doesNotExist();
      assertThat(freshPartial).as("a partial that may belong to a running backup").exists();
      assertThat(completeArchive).as("a complete archive is never swept, and with no retention nothing else prunes it").exists();
    }

    private static File write(final File directory, final String name) throws IOException {
      final File file = new File(directory, name);
      Files.writeString(file.toPath(), "x");
      return file;
    }
  }

  private static File freshDirectory(final String path) {
    final File dir = new File(path);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    dir.mkdirs();
    return dir;
  }

  abstract static class Fixture extends BaseGraphServerTest {
    protected File backupConfigFile;
    protected File serverBackupDir;

    @Override
    protected boolean isCreateDatabases() {
      return true;
    }

    @AfterEach
    void cleanUp() {
      if (backupConfigFile != null && backupConfigFile.exists())
        backupConfigFile.delete();
      if (serverBackupDir != null && serverBackupDir.exists())
        FileUtils.deleteRecursively(serverBackupDir);
    }

    protected HttpResponse<String> postServerCommand(final String command) throws Exception {
      return post(getServerHttpUrl("/api/v1/server"), new JSONObject().put("command", command));
    }

    protected HttpResponse<String> postSqlCommand(final String command) throws Exception {
      return post(getServerHttpUrl("/api/v1/command/") + getDatabaseName(),
          new JSONObject().put("language", "sql").put("command", command));
    }

    private HttpResponse<String> post(final String url, final JSONObject payload) throws Exception {
      final HttpRequest request = HttpRequest.newBuilder()
          .uri(new URI(url))
          .header("Authorization",
              "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
          .build();
      return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }
  }

  /** The default configuration: no {@code config/backup.json}, so the plugin the server always registers is disabled. */
  @Nested
  class DefaultConfiguration extends Fixture {
    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      super.onServerConfiguration(config);

      final File stale = new File("./target/config/backup.json");
      if (stale.exists())
        stale.delete();

      serverBackupDir = freshDirectory("./target/test-backups-12530-default");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      config.setValue(GlobalConfiguration.SERVER_BACKUP_DIRECTORY, serverBackupDir.getPath());
    }

    @Test
    void aTriggeredBackupSweepsThePartialAnEarlierCrashLeft() throws Exception {
      assertThat(getServer(0).getPlugins()).as("the plugin is always registered, and without a backup.json it is disabled")
          .anyMatch(p -> p instanceof AutoBackupSchedulerPlugin plugin && !plugin.isEnabled());

      final File databaseBackupDir = new File(serverBackupDir, getDatabaseName());
      final Leftovers leftovers = new Leftovers(databaseBackupDir, getDatabaseName());

      final HttpResponse<String> response = postServerCommand("trigger backup " + getDatabaseName());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);

      leftovers.assertSweptButNothingElse("swept after the backup that followed it");
      assertThat(new File(databaseBackupDir, new File(new JSONObject(response.body()).getString("backupFile")).getName()))
          .as("the new archive").exists();
    }

    @Test
    void aSqlBackupSweepsThePartialAnEarlierCrashLeft() throws Exception {
      final File databaseBackupDir = new File(serverBackupDir, getDatabaseName());
      final Leftovers leftovers = new Leftovers(databaseBackupDir, getDatabaseName());

      final HttpResponse<String> response = postSqlCommand("BACKUP DATABASE");
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);

      leftovers.assertSweptButNothingElse("swept after the BACKUP DATABASE that followed it");
      final File[] archives = databaseBackupDir.listFiles((dir, name) -> name.endsWith(".zip"));
      assertThat(archives).as("the new archive next to the complete one that was there").hasSize(2);
    }
  }

  /** The plugin is registered, and its own {@code config/backup.json} says {@code "enabled": false}: its retention is a no-op. */
  @Nested
  class DisabledByConfiguration extends Fixture {
    private static final String BACKUP_CONFIG = """
        {
          "version": 1,
          "enabled": false,
          "defaults": {
            "enabled": true,
            "runOnServer": "*",
            "schedule": {
              "type": "frequency",
              "frequencyMinutes": 9999
            },
            "retention": {
              "maxFiles": 1
            }
          }
        }
        """;

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
      } catch (final IOException e) {
        throw new RuntimeException("Failed to set up backup config", e);
      }

      serverBackupDir = freshDirectory("./target/test-backups-12530-disabled");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      config.setValue(GlobalConfiguration.SERVER_BACKUP_DIRECTORY, serverBackupDir.getPath());
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, "auto-backup:" + AutoBackupSchedulerPlugin.class.getName());
    }

    @Test
    void aTriggeredBackupSweepsThePartialAnEarlierCrashLeft() throws Exception {
      assertThat(getServer(0).getPlugins()).as("registered, and disabled by its own file")
          .anyMatch(p -> p instanceof AutoBackupSchedulerPlugin plugin && !plugin.isEnabled());

      // The configuration file names no directory, so the chain ends at the server-wide setting
      final File databaseBackupDir = new File(serverBackupDir, getDatabaseName());
      final Leftovers leftovers = new Leftovers(databaseBackupDir, getDatabaseName());

      final HttpResponse<String> response = postServerCommand("trigger backup " + getDatabaseName());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);

      leftovers.assertSweptButNothingElse("swept after the backup that followed it");
      assertThat(new File(databaseBackupDir, new File(new JSONObject(response.body()).getString("backupFile")).getName()))
          .as("the new archive").exists();
    }
  }
}
