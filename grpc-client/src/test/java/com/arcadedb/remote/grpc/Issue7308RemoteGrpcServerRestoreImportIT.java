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
package com.arcadedb.remote.grpc;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.backup.AutoBackupSchedulerPlugin;
import com.arcadedb.server.grpc.BackupInfo;
import com.arcadedb.server.grpc.ImportProgress;
import com.arcadedb.server.grpc.RestoreProgress;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7308: {@code RemoteGrpcServer} gained the three streaming control-plane methods. An RPC no
 * client can call is one nobody can use, so this drives each of them against a live server, proving
 * the proto, the service and the client agree rather than only that they compile.
 * <p>
 * These are the only methods on that client that block for the length of an operation instead of a
 * round trip, and the only ones that hand the caller anything while they run.
 */
class Issue7308RemoteGrpcServerRestoreImportIT extends BaseGraphServerTest {
  private static final String BACKUP_DIR_NAME = "test-backups-7308-client";
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

  private RemoteGrpcServer     client;
  private final List<String>   databasesToDrop = new ArrayList<>();

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    try {
      new File("./target/config").mkdirs();
      try (final FileWriter writer = new FileWriter("./target/config/backup.json")) {
        writer.write(BACKUP_CONFIG);
      }
      final File backupDir = new File("./target/" + BACKUP_DIR_NAME);
      if (backupDir.exists())
        FileUtils.deleteRecursively(backupDir);
      backupDir.mkdirs();
    } catch (final IOException e) {
      throw new RuntimeException("Failed to set up the backup configuration", e);
    }

    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_PLUGINS,
        "GRPC:com.arcadedb.server.grpc.GrpcServerPlugin,auto-backup:" + AutoBackupSchedulerPlugin.class.getName());
    config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
  }

  @BeforeEach
  void connect() {
    client = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
  }

  @AfterEach
  void disconnect() {
    for (final String database : databasesToDrop) {
      try {
        if (getServer(0).existsDatabase(database))
          getServer(0).getDatabase(database).getEmbedded().drop();
      } catch (final Exception ignore) {
        // best-effort
      }
    }
    databasesToDrop.clear();

    if (client != null) {
      client.close();
      client = null;
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
  }

  /** Cleaned up once, after the class: the fixture's server is shared by every test in it. */
  @AfterAll
  static void removeBackupFixture() {
    final File config = new File("./target/config/backup.json");
    if (config.exists())
      config.delete();
    final File backups = new File("./target/" + BACKUP_DIR_NAME);
    if (backups.exists())
      FileUtils.deleteRecursively(backups);
  }

  @Test
  void restoreBackupReportsProgressAndBringsUpTheTarget() {
    final String target = "client7308_restore_backup";
    databasesToDrop.add(target);

    final String archive = triggerBackupAndGetFileName();
    final long sourceCount = getServer(0).getDatabase(getDatabaseName()).countType(VERTEX1_TYPE_NAME, false);

    final List<RestoreProgress> events = new ArrayList<>();
    client.restoreBackup(getDatabaseName(), archive, target, false, events::add);

    assertThat(events).isNotEmpty();
    assertThat(events.getLast().getCompleted()).isTrue();
    assertThat(getServer(0).getDatabase(target).countType(VERTEX1_TYPE_NAME, false)).isEqualTo(sourceCount);
  }

  @Test
  void restoreDatabaseReportsProgressAndBringsUpTheDatabase() {
    final String target = "client7308_restore_database";
    databasesToDrop.add(target);

    final Path archive = backupPath(triggerBackupAndGetFileName());
    final long sourceCount = getServer(0).getDatabase(getDatabaseName()).countType(VERTEX1_TYPE_NAME, false);

    final List<RestoreProgress> events = new ArrayList<>();
    client.restoreDatabase(target, "file://" + archive.toAbsolutePath(), events::add);

    // Progress before the terminator, not only the terminator: this method blocks for the length of
    // the restore, and reporting nothing until it ends would defeat the point of it streaming.
    assertThat(events).hasSizeGreaterThan(1);
    assertThat(events.getFirst().getCompleted()).isFalse();
    assertThat(events.getLast().getCompleted()).isTrue();
    assertThat(getServer(0).getDatabase(target).countType(VERTEX1_TYPE_NAME, false)).isEqualTo(sourceCount);
  }

  /**
   * A restore that fails ends the stream with an error status, which the client turns into the typed
   * exception the server raised. A caller that returns normally has a restore that finished.
   */
  @Test
  void aFailedRestoreThrowsRatherThanReturningTheProgressSoFar() {
    final List<RestoreProgress> events = new ArrayList<>();

    assertThatThrownBy(() -> client.restoreDatabase(getDatabaseName(), "file:///no/such/archive.zip", events::add))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("already exists");

    assertThat(events).isEmpty();
  }

  @Test
  void importDatabaseReportsProgressAndReturnsTheImporterReport() throws IOException {
    final String target = "client7308_import";
    databasesToDrop.add(target);

    final File source = new File("./target/7308-client-import.csv");
    try (final FileWriter writer = new FileWriter(source)) {
      writer.write("id,name\n1,one\n2,two\n");
    }

    final List<ImportProgress> events = new ArrayList<>();
    final JSONObject report = client.importDatabase(target, "file://" + source.getAbsolutePath(), events::add);

    assertThat(events).isNotEmpty();
    assertThat(events.getLast().getCompleted()).isTrue();
    // The importer's report is returned rather than left for the caller to dig out of the last
    // message: that is the only piece of the stream a synchronous caller usually wants.
    assertThat(report.toMap()).isNotEmpty();
    assertThat(getServer(0).existsDatabase(target)).isTrue();
  }

  /**
   * A throw from the caller's own progress callback reaches the caller as itself, not swallowed and
   * not rewrapped as a {@link com.arcadedb.remote.RemoteException} - the callback failed, the RPC did
   * not, and telling the caller otherwise would send them looking at the server.
   * <p>
   * <b>What this test does not assert:</b> that the abandoned stream is cancelled. {@code drain} does
   * cancel it in a {@code finally}, which matters because these RPCs carry no deadline and a call
   * nobody is reading would otherwise sit open until the channel closed - but that is not observable
   * from this side of the wire, and asserting the client still works afterwards proves nothing,
   * because it passes with the cancellation removed too (checked). Rather than leave a test that
   * cannot fail for the thing it names, this asserts the part that can.
   */
  @Test
  void aThrowingProgressCallbackReachesTheCallerUnwrapped() {
    final String target = "client7308_throwing_callback";
    databasesToDrop.add(target);

    final Path archive = backupPath(triggerBackupAndGetFileName());

    assertThatThrownBy(() -> client.restoreDatabase(target, "file://" + archive.toAbsolutePath(), progress -> {
      throw new IllegalStateException("callback blew up");
    })).isInstanceOf(IllegalStateException.class).hasMessage("callback blew up");
  }

  /** The progress callback is optional: a caller that only wants the outcome passes null. */
  @Test
  void restoreBackupAcceptsNoProgressCallback() {
    final String target = "client7308_no_callback";
    databasesToDrop.add(target);

    client.restoreBackup(getDatabaseName(), triggerBackupAndGetFileName(), target, false, null);

    assertThat(getServer(0).existsDatabase(target)).isTrue();
  }

  private String triggerBackupAndGetFileName() {
    client.triggerBackup(getDatabaseName());
    final List<BackupInfo> backups = client.listBackups(getDatabaseName());
    assertThat(backups).isNotEmpty();
    return backups.getLast().getFileName();
  }

  private Path backupPath(final String fileName) {
    return Paths.get("./target", BACKUP_DIR_NAME, getDatabaseName(), fileName).toAbsolutePath().normalize();
  }
}
