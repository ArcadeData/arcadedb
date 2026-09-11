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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.backup.AutoBackupSchedulerPlugin;
import com.arcadedb.utility.FileUtils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7395, gRPC half. {@code RestoreDatabase} and {@code RestoreBackup} reach the same two
 * {@code ServerControlPlane} methods the HTTP commands do, so they inherited the divergence this
 * issue is about: {@code restore backup} consulted the server's database registry and the filesystem,
 * {@code restore database} only the filesystem.
 * <p>
 * The HTTP half lives in {@code Issue7395RestoreTargetExistsIT} in the {@code server} module. This
 * class drives the same states through the RPCs rather than through the shared implementation,
 * because a test that called the implementation directly would pass against a transport that never
 * wired it up.
 *
 * @see Issue7308GrpcRestoreImportIT for the fixture this one follows
 */
public class Issue7395GrpcRestoreTargetExistsIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT       = 50051;
  private static final String BACKUP_DIR_NAME = "test-backups-7395-grpc";
  private static final String ARCHIVE_NAME    = "graph-backup-7395000000.zip";
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

  private final List<String> registeredToUnregister = new ArrayList<>();
  private final List<File>   directoriesToDelete    = new ArrayList<>();

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
    // RestoreDatabase validates the caller's URL before it looks at the target at all, so without this
    // every test here would fail the SSRF guard instead of reaching the existence check.
    config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    adminStub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void teardown() throws InterruptedException {
    for (final String database : registeredToUnregister)
      getServer(0).removeDatabase(database);
    registeredToUnregister.clear();

    for (final File directory : directoriesToDelete)
      if (directory.exists())
        FileUtils.deleteRecursively(directory);
    directoriesToDelete.clear();

    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @AfterAll
  static void cleanUpBackupFixture() {
    final File config = new File("./target/config/backup.json");
    if (config.exists())
      config.delete();
    final File backups = new File("./target/" + BACKUP_DIR_NAME);
    if (backups.exists())
      FileUtils.deleteRecursively(backups);
  }

  // -----------------------------------------------------------------------------------------------
  // RestoreDatabase
  // -----------------------------------------------------------------------------------------------

  @Test
  void restoreDatabaseRefusesATargetRegisteredOnTheServerWhoseDirectoryIsGone() {
    final String target = registeredWithoutADirectory("grpc7395_regonly_restore_db");

    assertThatThrownBy(() -> drain(adminStub.restoreDatabase(RestoreDatabaseRequest.newBuilder().setCredentials(root())
        .setDatabase(target).setUrl(missingArchiveUrl()).build())))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT")
        .hasMessageContaining("already exists");
  }

  @Test
  void restoreDatabaseStillRefusesATargetWhoseDirectoryIsPresentButUnregistered() {
    final String target = directoryWithoutARegistration("grpc7395_dironly_restore_db");

    assertThatThrownBy(() -> drain(adminStub.restoreDatabase(RestoreDatabaseRequest.newBuilder().setCredentials(root())
        .setDatabase(target).setUrl(missingArchiveUrl()).build())))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT")
        .hasMessageContaining("already exists");
  }

  /** The control: a free name gets past the existence check and fails on the archive instead. */
  @Test
  void restoreDatabaseWithAFreeNameGetsPastTheExistenceCheckAndFailsOnTheArchive() {
    assertThatThrownBy(() -> drain(adminStub.restoreDatabase(RestoreDatabaseRequest.newBuilder().setCredentials(root())
        .setDatabase("grpc7395_free_restore_db").setUrl(missingArchiveUrl()).build())))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageNotContaining("already exists");

    assertThat(getServer(0).existsDatabase("grpc7395_free_restore_db")).isFalse();
  }

  // -----------------------------------------------------------------------------------------------
  // RestoreBackup
  // -----------------------------------------------------------------------------------------------

  @Test
  void restoreBackupRefusesATargetRegisteredOnTheServerWhoseDirectoryIsGone() throws IOException {
    final String target = registeredWithoutADirectory("grpc7395_regonly_restore_backup");

    assertThatThrownBy(() -> drain(adminStub.restoreBackup(RestoreBackupRequest.newBuilder().setCredentials(root())
        .setDatabase(getDatabaseName()).setFileName(placeholderArchive()).setTargetDatabase(target).build())))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT")
        .hasMessageContaining("already exists");
  }

  @Test
  void restoreBackupRefusesATargetWhoseDirectoryIsPresentButUnregistered() throws IOException {
    final String target = directoryWithoutARegistration("grpc7395_dironly_restore_backup");

    assertThatThrownBy(() -> drain(adminStub.restoreBackup(RestoreBackupRequest.newBuilder().setCredentials(root())
        .setDatabase(getDatabaseName()).setFileName(placeholderArchive()).setTargetDatabase(target).build())))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT")
        .hasMessageContaining("already exists");
  }

  @Test
  void restoreBackupWithAFreeTargetNameGetsPastTheExistenceCheckAndFailsOnTheArchive() throws IOException {
    assertThatThrownBy(() -> drain(adminStub.restoreBackup(RestoreBackupRequest.newBuilder().setCredentials(root())
        .setDatabase(getDatabaseName()).setFileName(placeholderArchive())
        .setTargetDatabase("grpc7395_free_restore_backup").build())))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageNotContaining("already exists");

    assertThat(getServer(0).existsDatabase("grpc7395_free_restore_backup")).isFalse();
  }

  // -----------------------------------------------------------------------------------------------
  // Fixture helpers
  // -----------------------------------------------------------------------------------------------

  /**
   * Creates a database and then drops it through the embedded instance, which deletes the directory
   * and leaves the registry entry behind - the one state in which the two checks disagreed.
   */
  private String registeredWithoutADirectory(final String databaseName) {
    getServer(0).createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
    getServer(0).getDatabase(databaseName).getEmbedded().drop();
    registeredToUnregister.add(databaseName);

    assertThat(getServer(0).existsDatabase(databaseName)).isTrue();
    assertThat(databaseDirectory(databaseName)).doesNotExist();
    return databaseName;
  }

  private String directoryWithoutARegistration(final String databaseName) {
    final File directory = databaseDirectory(databaseName);
    assertThat(directory.mkdirs()).isTrue();
    directoriesToDelete.add(directory);

    assertThat(getServer(0).existsDatabase(databaseName)).isFalse();
    return databaseName;
  }

  private File databaseDirectory(final String databaseName) {
    return new File(
        getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), databaseName);
  }

  private String missingArchiveUrl() {
    return "file://" + new File("./target/nonexistent-7395-grpc.zip").getAbsolutePath();
  }

  /**
   * {@code RestoreBackup} resolves its archive before it looks at the target, so the file has to
   * exist; its contents never matter here - every test either refuses before the restore starts, or
   * is the control that asserts the restore itself fails.
   */
  private String placeholderArchive() throws IOException {
    final File dbBackupDir = new File(backupDir, getDatabaseName());
    dbBackupDir.mkdirs();
    final File archive = new File(dbBackupDir, ARCHIVE_NAME);
    if (!archive.exists())
      Files.writeString(archive.toPath(), "not a real archive");
    return ARCHIVE_NAME;
  }

  private DatabaseCredentials root() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private static <T> List<T> drain(final Iterator<T> stream) {
    final List<T> events = new ArrayList<>();
    while (stream.hasNext())
      events.add(stream.next());
    return events;
  }
}
