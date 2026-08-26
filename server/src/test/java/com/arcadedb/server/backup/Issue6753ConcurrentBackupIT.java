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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Path;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A database is backed up by one backup at a time, whichever entry point asked for it. The auto-backup schedule, its
 * immediate trigger and the HTTP "trigger backup" command used to be three independent submissions, all naming their
 * archive from a second-precision timestamp: two of them starting in the same second resolved to the same path and
 * wrote into the same file (issue #6753).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6753ConcurrentBackupIT extends BaseGraphServerTest {
  private static final String BACKUP_DIRECTORY = "./target/issue6753-backups";

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @BeforeEach
  @AfterEach
  void cleanBackupDirectory() {
    FileUtils.deleteRecursively(Path.of(BACKUP_DIRECTORY).toFile());
  }

  @Test
  void aScheduledBackupIsSkippedWhileAnotherBackupOfTheSameDatabaseRuns() {
    final ArcadeDBServer server = getServer(0);
    final BackupCoordinator coordinator = server.getBackupCoordinator();
    final File dbBackupDir = new File(BACKUP_DIRECTORY, getDatabaseName());

    final BackupTask task = new BackupTask(server, getDatabaseName(), backupConfig(), BACKUP_DIRECTORY, null);

    // SOMEBODY ELSE IS ALREADY BACKING THIS DATABASE UP
    assertThat(coordinator.begin(getDatabaseName())).isTrue();
    try {
      task.run();
      assertThat(archives(dbBackupDir)).isEmpty();
    } finally {
      coordinator.end(getDatabaseName());
    }

    // AND THE VERY SAME TASK GOES THROUGH ONCE THE OTHER BACKUP IS DONE: THE SKIP ABOVE IS THE GUARD, NOT A TASK
    // THAT COULD NEVER HAVE RUN
    task.run();

    final String[] archives = archives(dbBackupDir);
    assertThat(archives).hasSize(1);
    // AND THE NAME IT CHOSE IS THE ONE RETENTION AND THE BACKUP LISTING READ BACK
    assertThat(archives[0]).matches(getDatabaseName() + "-backup-\\d{8}-\\d{9}\\.zip");
    assertThat(BackupCoordinator.parseArchiveTimestamp(archives[0])).isNotNull();
  }

  @Test
  void theHttpTriggerRefusesWhileAnotherBackupOfTheSameDatabaseRuns() throws Exception {
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    assertThat(coordinator.begin(getDatabaseName())).isTrue();
    try {
      final HttpURLConnection connection = postServerCommand("trigger backup " + getDatabaseName());
      try {
        assertThat(connection.getResponseCode()).isEqualTo(409);
        final String error = new JSONObject(readError(connection)).getString("error");
        assertThat(error).contains("already in progress");
      } finally {
        connection.disconnect();
      }
    } finally {
      coordinator.end(getDatabaseName());
    }

    // NOTHING WAS LEAKED BY THE REFUSAL: THE NEXT TRIGGER IS ADMITTED
    final HttpURLConnection connection = postServerCommand("trigger backup " + getDatabaseName());
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection postServerCommand(final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:2480/api/v1/server").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    connection.getOutputStream().write(new JSONObject().put("command", command).toString().getBytes());
    connection.connect();
    return connection;
  }

  private DatabaseBackupConfig backupConfig() {
    final DatabaseBackupConfig config = new DatabaseBackupConfig(getDatabaseName());
    final DatabaseBackupConfig.ScheduleConfig schedule = new DatabaseBackupConfig.ScheduleConfig();
    schedule.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    schedule.setFrequencyMinutes(60);
    config.setSchedule(schedule);
    return config;
  }

  private String[] archives(final File directory) {
    if (!directory.exists())
      return new String[0];
    final String[] names = directory.list((dir, name) -> name.endsWith(".zip"));
    return names != null ? names : new String[0];
  }
}
