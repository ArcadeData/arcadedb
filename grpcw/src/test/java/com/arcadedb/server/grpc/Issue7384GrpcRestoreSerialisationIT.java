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
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.backup.AutoBackupSchedulerPlugin;
import com.arcadedb.server.backup.BackupCoordinator;
import com.arcadedb.server.backup.BackupCoordinator.Operation;
import com.arcadedb.utility.FileUtils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
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
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The gRPC half of issue #7384: a restore, a backup or an import already running on a database refuses the next one,
 * and the refusal reaches a gRPC client as {@code ABORTED} - HTTP's 409 on this transport.
 * <p>
 * Driven through the RPCs rather than through {@code ServerControlPlane}, because the thing this class exists to
 * catch is a status mapping that only ever looked at {@code BackupInProgressException}: {@code TriggerBackup} was the
 * one RPC that could raise it, so widening the slot to restores and imports without widening the mapping would have
 * turned every refusal on {@code RestoreBackup}, {@code RestoreDatabase} and {@code ImportDatabase} into an
 * {@code INTERNAL} - a server fault, which is exactly what it is not.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public class Issue7384GrpcRestoreSerialisationIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT       = 50051;
  private static final String BACKUP_DIR_NAME = "test-backups-7384-grpc";
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
    config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    adminStub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void teardown() throws InterruptedException {
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();
    for (final Operation operation : Operation.values())
      coordinator.end(getDatabaseName(), operation);

    for (final String database : databasesToDrop) {
      try {
        for (final Operation operation : Operation.values())
          coordinator.end(database, operation);
        if (getServer(0).existsDatabase(database))
          getServer(0).getDatabase(database).getEmbedded().drop();
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

  @Test
  void everyRestoreAndImportRpcAnswersAbortedWhileARestoreOfTheTargetRuns() {
    final String target = "grpc7384_target";
    databasesToDrop.add(target);

    final String archive = triggerBackupAndGetFileName();
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    assertThat(coordinator.begin(target, Operation.RESTORE)).isNull();
    try {
      assertAborted(() -> drain(adminStub.restoreDatabase(RestoreDatabaseRequest.newBuilder().setCredentials(root())
          .setDatabase(target).setUrl(archiveUrl(archive)).build())));

      assertAborted(() -> drain(adminStub.restoreBackup(RestoreBackupRequest.newBuilder().setCredentials(root())
          .setDatabase(getDatabaseName()).setFileName(archive).setTargetDatabase(target).setOverwrite(true).build())));

      assertAborted(() -> drain(adminStub.importDatabase(ImportDatabaseRequest.newBuilder().setCredentials(root())
          .setDatabase(target).setUrl(archiveUrl(archive)).build())));

      assertAborted(() -> adminStub.triggerBackup(
          TriggerBackupRequest.newBuilder().setCredentials(root()).setDatabase(target).build()));
    } finally {
      coordinator.end(target, Operation.RESTORE);
    }

    assertThat(getServer(0).existsDatabase(target)).isFalse();

    // AND THE SAME RPC GOES THROUGH ONCE THE SLOT IS FREE: THE REFUSALS ABOVE ARE THE GUARD, NOT A CALL THAT COULD
    // NEVER HAVE SUCCEEDED
    drain(adminStub.restoreDatabase(RestoreDatabaseRequest.newBuilder().setCredentials(root())
        .setDatabase(target).setUrl(archiveUrl(archive)).build()));
    assertThat(getServer(0).existsDatabase(target)).isTrue();
  }

  @Test
  void aBackupInFlightAnswersAbortedToARestoreOfTheSameDatabase() {
    final String archive = triggerBackupAndGetFileName();
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    assertThat(coordinator.begin(getDatabaseName(), Operation.BACKUP)).isNull();
    try {
      final StatusRuntimeException refused = assertAborted(
          () -> drain(adminStub.restoreBackup(RestoreBackupRequest.newBuilder().setCredentials(root())
              .setDatabase(getDatabaseName()).setFileName(archive).setTargetDatabase(getDatabaseName())
              .setOverwrite(true).build())));

      assertThat(refused).hasMessageContaining("a backup of it is already in progress");
    } finally {
      coordinator.end(getDatabaseName(), Operation.BACKUP);
    }
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private static StatusRuntimeException assertAborted(final Runnable call) {
    final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class, call::run);
    assertThat(failure).isNotNull();
    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
    assertThat(failure.getStatus().getDescription()).contains("already in progress");
    return failure;
  }

  private DatabaseCredentials root() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private String triggerBackupAndGetFileName() {
    adminStub.triggerBackup(TriggerBackupRequest.newBuilder().setCredentials(root()).setDatabase(getDatabaseName())
        .build());

    final ListBackupsResponse backups = adminStub.listBackups(
        ListBackupsRequest.newBuilder().setCredentials(root()).setDatabase(getDatabaseName()).build());
    assertThat(backups.getBackupsList()).isNotEmpty();
    return backups.getBackups(backups.getBackupsCount() - 1).getFileName();
  }

  private String archiveUrl(final String fileName) {
    final Path archive = Paths.get("./target", BACKUP_DIR_NAME, getDatabaseName(), fileName).toAbsolutePath().normalize();
    return "file://" + archive;
  }

  private static <T> List<T> drain(final Iterator<T> stream) {
    final List<T> events = new ArrayList<>();
    while (stream.hasNext())
      events.add(stream.next());
    return events;
  }
}
