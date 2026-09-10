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
package com.arcadedb.server.grpc;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.backup.AutoBackupSchedulerPlugin;
import com.arcadedb.utility.FileUtils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7308: {@code restore backup}, {@code restore database} and {@code import database} were the
 * three control-plane operations #7304 left on HTTP only, because their handler streams progress
 * straight to the {@code HttpServerExchange}. They are now server-streaming RPCs backed by the same
 * {@code ServerControlPlane} implementation the HTTP commands run.
 * <p>
 * Every test drives the operation through its own gRPC entry point rather than through the shared
 * implementation: a test that called {@code ServerControlPlane} directly would pass against a proto
 * whose RPC was never wired up, which is the failure this class exists to catch.
 * <p>
 * The fixture configures auto-backup the way {@code BackupRestoreDeleteApiIT} does, because
 * {@code RestoreBackup} resolves its archive out of the configured backup directory rather than
 * taking a path from the caller, and enables
 * {@link GlobalConfiguration#SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS} so a {@code file://} URL is
 * fetchable. The one test that needs that flag <i>off</i> lives in
 * {@link Issue7308GrpcRestoreImportUrlGuardIT} instead, since it is fixture-wide.
 */
public class Issue7308GrpcRestoreImportIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT       = 50051;
  private static final String BACKUP_DIR_NAME = "test-backups-7308-grpc";
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

  private ManagedChannel                                            channel;
  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

  private final List<String> databasesToDrop = new ArrayList<>();

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
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
    config.setValue(GlobalConfiguration.SERVER_PLUGINS,
        "GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin,auto-backup:" + AutoBackupSchedulerPlugin.class.getName());
    // The archives this fixture restores from live on disk, so the server has to be willing to fetch
    // a file:// URL. Without it every restore here would fail the SSRF guard instead of the restore.
    config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    adminStub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void teardown() throws InterruptedException {
    final ArcadeDBServer server = getServer(0);
    for (final String database : databasesToDrop) {
      try {
        if (server.existsDatabase(database))
          server.getDatabase(database).getEmbedded().drop();
      } catch (final Exception ignore) {
        // best-effort
      }
    }
    databasesToDrop.clear();

    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }

    if (backupConfigFile != null && backupConfigFile.exists())
      backupConfigFile.delete();
    if (backupDir != null && backupDir.exists())
      FileUtils.deleteRecursively(backupDir);
  }

  // -------------------------------------------------------------------------------------------
  // restore backup
  // -------------------------------------------------------------------------------------------

  @Test
  void restoreBackupStreamsProgressAndBringsUpTheTargetDatabase() {
    final String target = "grpc7308_restore_backup";
    databasesToDrop.add(target);

    final String archive = triggerBackupAndGetFileName();
    final long sourceCount = getServer(0).getDatabase(getDatabaseName()).countType(VERTEX1_TYPE_NAME, false);

    final List<RestoreProgress> events = drain(adminStub.restoreBackup(
        RestoreBackupRequest.newBuilder().setCredentials(root()).setDatabase(getDatabaseName()).setFileName(archive)
            .setTargetDatabase(target).build()));

    assertCompletedLast(events, target + " restored successfully");
    assertThat(getServer(0).existsDatabase(target)).isTrue();
    assertThat(getServer(0).getDatabase(target).countType(VERTEX1_TYPE_NAME, false)).isEqualTo(sourceCount);
  }

  /**
   * Without {@code overwrite} an existing target is the caller's mistake to fix, not a server fault:
   * the same refusal {@code restore backup} answers 400 over HTTP.
   */
  @Test
  void restoreBackupRefusesAnExistingTargetUnlessOverwriteIsSet() {
    final String target = "grpc7308_restore_backup_overwrite";
    databasesToDrop.add(target);

    final String archive = triggerBackupAndGetFileName();
    final RestoreBackupRequest.Builder request = RestoreBackupRequest.newBuilder().setCredentials(root())
        .setDatabase(getDatabaseName()).setFileName(archive).setTargetDatabase(target);

    drain(adminStub.restoreBackup(request.build()));
    assertThat(getServer(0).existsDatabase(target)).isTrue();

    assertThatThrownBy(() -> drain(adminStub.restoreBackup(request.build())))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT")
        .hasMessageContaining("already exists");

    // The same call with overwrite set replaces it.
    final List<RestoreProgress> events = drain(adminStub.restoreBackup(request.setOverwrite(true).build()));
    assertCompletedLast(events, target + " restored successfully");
    assertThat(getServer(0).existsDatabase(target)).isTrue();
  }

  /**
   * The target of a {@code restore backup} becomes a directory under the server's database
   * directory, so the caller-supplied name has to be validated the way {@code restore database}
   * already validated its own. Before issue #7308 only the latter was checked, and the gRPC RPC would
   * have inherited that gap.
   */
  @Test
  void restoreBackupRefusesATargetNameThatLeavesTheDatabaseDirectory() {
    assertThatThrownBy(() -> drain(adminStub.restoreBackup(RestoreBackupRequest.newBuilder().setCredentials(root())
        .setDatabase(getDatabaseName()).setFileName("whatever-backup-0.zip").setTargetDatabase("../escaped7308")
        .build())))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");

    assertThat(new File("./target/../escaped7308")).doesNotExist();
  }

  @Test
  void restoreBackupRefusesAFileNameThatLeavesTheBackupDirectory() {
    assertThatThrownBy(() -> drain(adminStub.restoreBackup(RestoreBackupRequest.newBuilder().setCredentials(root())
        .setDatabase(getDatabaseName()).setFileName("../../etc/passwd").setTargetDatabase("grpc7308_never_created")
        .build())))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");

    assertThat(getServer(0).existsDatabase("grpc7308_never_created")).isFalse();
  }

  // -------------------------------------------------------------------------------------------
  // restore database
  // -------------------------------------------------------------------------------------------

  @Test
  void restoreDatabaseStreamsProgressAndCreatesTheDatabaseFromAUrl() {
    final String target = "grpc7308_restore_database";
    databasesToDrop.add(target);

    final Path archive = backupPath(triggerBackupAndGetFileName());
    final long sourceCount = getServer(0).getDatabase(getDatabaseName()).countType(VERTEX1_TYPE_NAME, false);

    final List<RestoreProgress> events = drain(adminStub.restoreDatabase(
        RestoreDatabaseRequest.newBuilder().setCredentials(root()).setDatabase(target)
            .setUrl("file://" + archive.toAbsolutePath()).build()));

    // The stream is not just its terminator: the restore reports what it is doing while it runs,
    // which is the whole reason these RPCs are server-streaming rather than unary.
    assertThat(events).hasSizeGreaterThan(1);
    assertThat(events.getFirst().getCompleted()).isFalse();
    assertCompletedLast(events, target + " restored successfully");

    assertThat(getServer(0).getDatabase(target).countType(VERTEX1_TYPE_NAME, false)).isEqualTo(sourceCount);
  }

  @Test
  void restoreDatabaseRefusesADatabaseThatAlreadyExists() {
    final Path archive = backupPath(triggerBackupAndGetFileName());

    assertThatThrownBy(() -> drain(adminStub.restoreDatabase(RestoreDatabaseRequest.newBuilder().setCredentials(root())
        .setDatabase(getDatabaseName()).setUrl("file://" + archive.toAbsolutePath()).build())))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT")
        .hasMessageContaining("already exists");
  }

  @Test
  void restoreDatabaseRefusesADatabaseNameThatLeavesTheDatabaseDirectory() {
    final Path archive = backupPath(triggerBackupAndGetFileName());

    assertThatThrownBy(() -> drain(adminStub.restoreDatabase(RestoreDatabaseRequest.newBuilder().setCredentials(root())
        .setDatabase("../escaped7308_restore").setUrl("file://" + archive.toAbsolutePath()).build())))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");
  }

  // -------------------------------------------------------------------------------------------
  // import database
  // -------------------------------------------------------------------------------------------

  /**
   * The import stream carries two kinds of non-final message - the importer's log lines and its
   * running counters - and one final message holding the importer's own report as JSON. The counters
   * are sampled once a second, so an import this small may well finish before the first sample; what
   * is asserted is the contract that always holds, not a race the fixture cannot win.
   */
  @Test
  void importDatabaseStreamsProgressAndReportsTheImporterResult() throws IOException {
    final String target = "grpc7308_import";
    databasesToDrop.add(target);

    final File source = new File("./target/7308-import.csv");
    try (final FileWriter writer = new FileWriter(source)) {
      writer.write("id,name\n1,one\n2,two\n3,three\n");
    }

    final List<ImportProgress> events = drain(adminStub.importDatabase(
        ImportDatabaseRequest.newBuilder().setCredentials(root()).setDatabase(target)
            .setUrl("file://" + source.getAbsolutePath()).build()));

    final ImportProgress last = events.getLast();
    assertThat(last.getCompleted()).isTrue();
    assertThat(last.getMessage()).isEqualTo(target + " imported successfully");
    // The importer's report travels as JSON on the completed message and nowhere else.
    assertThat(last.getResultJson()).isNotEmpty();
    assertThat(new JSONObject(last.getResultJson()).toMap()).isNotEmpty();
    assertThat(events.subList(0, events.size() - 1)).allMatch(e -> !e.getCompleted());

    assertThat(getServer(0).existsDatabase(target)).isTrue();
  }

  @Test
  void importDatabaseRefusesADatabaseNameThatLeavesTheDatabaseDirectory() {
    assertThatThrownBy(() -> drain(adminStub.importDatabase(ImportDatabaseRequest.newBuilder().setCredentials(root())
        .setDatabase("../escaped7308_import").setUrl("file:///dev/null").build())))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private DatabaseCredentials root() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  /** Runs a real backup of the fixture database through the pre-existing RPC and names the archive. */
  private String triggerBackupAndGetFileName() {
    adminStub.triggerBackup(TriggerBackupRequest.newBuilder().setCredentials(root()).setDatabase(getDatabaseName())
        .build());

    final ListBackupsResponse backups = adminStub.listBackups(
        ListBackupsRequest.newBuilder().setCredentials(root()).setDatabase(getDatabaseName()).build());
    assertThat(backups.getBackupsList()).isNotEmpty();
    return backups.getBackups(backups.getBackupsCount() - 1).getFileName();
  }

  private Path backupPath(final String fileName) {
    return Paths.get("./target", BACKUP_DIR_NAME, getDatabaseName(), fileName).toAbsolutePath().normalize();
  }

  private static <T> List<T> drain(final Iterator<T> stream) {
    final List<T> events = new ArrayList<>();
    while (stream.hasNext())
      events.add(stream.next());
    return events;
  }

  private static void assertCompletedLast(final List<RestoreProgress> events, final String message) {
    assertThat(events).isNotEmpty();
    final RestoreProgress last = events.getLast();
    assertThat(last.getCompleted()).isTrue();
    assertThat(last.getMessage()).isEqualTo(message);
    // completed marks the end of the stream and nothing before it.
    assertThat(events.subList(0, events.size() - 1)).allMatch(e -> !e.getCompleted());
  }
}
