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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7395: {@code restore database} and {@code restore backup} asked a different question about
 * whether their target already existed. {@code restore backup} consulted the server's database
 * registry <b>and</b> the filesystem; {@code restore database} consulted only the filesystem.
 * <p>
 * The two answers differ in exactly one state - a database that is registered on the server but whose
 * directory is gone. That state is reachable out of band (the directory removed under a running
 * server) and through the server's own API: {@code LocalDatabase.drop()} closes the database and
 * deletes its directory but does not unregister it, which is why
 * {@code ServerControlPlane.dropQuietly} has to call {@code server.removeDatabase()} as a separate
 * second step. This fixture builds the state that way rather than by deleting files under an open
 * database.
 * <p>
 * Both entry points now ask {@code registered || directory present} - the stricter of the two, and
 * the predicate {@code ArcadeDBServer.createDatabase} already applied. {@code restore database} keeps
 * its unconditional refusal, {@code restore backup} keeps its {@code overwrite} gate over the same
 * predicate.
 * <p>
 * The gRPC transports of the same two commands are asserted by
 * {@code Issue7395GrpcRestoreTargetExistsIT} in the {@code grpcw} module. Both transports reach the
 * same {@code ServerControlPlane} methods, but a test that called those methods directly would pass
 * against a transport that never wired them up.
 */
class Issue7395RestoreTargetExistsIT extends BaseGraphServerTest {
  private static final String BACKUP_DIR_NAME = "test-backups-7395-target-exists";
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

  private final List<String> registeredToUnregister = new ArrayList<>();
  private final List<File>   directoriesToDelete    = new ArrayList<>();

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
    // 'restore database' validates the caller's URL before it looks at the target at all, so without
    // this every test here would fail the SSRF guard instead of reaching the existence check.
    config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
  }

  @AfterEach
  void cleanUpPerTestState() {
    for (final String database : registeredToUnregister)
      getServer(0).removeDatabase(database);
    registeredToUnregister.clear();

    for (final File directory : directoriesToDelete)
      if (directory.exists())
        FileUtils.deleteRecursively(directory);
    directoriesToDelete.clear();
  }

  /**
   * Cleaned up once, after the whole class: per-test cleanup would delete the backup configuration
   * out from under the server fixture the remaining tests still share.
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

  // -----------------------------------------------------------------------------------------------
  // restore database
  // -----------------------------------------------------------------------------------------------

  /**
   * The regression this issue is about. Before the fix the filesystem-only check saw nothing, let the
   * restore run, and reached {@code swapRestoredDatabase} - which would have dropped the registered
   * database out from under whoever still held it.
   */
  @Test
  void restoreDatabaseRefusesATargetRegisteredOnTheServerWhoseDirectoryIsGone() throws Exception {
    final String target = registeredWithoutADirectory("regonly7395_restore_db");

    final HttpResponse<String> response = postCommand("restore database " + target + " " + missingArchiveUrl());

    assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
    assertThat(new JSONObject(response.body()).getString("detail")).contains("already exists").contains(target);
  }

  /** The other half of the predicate, unchanged by this fix: a directory with nobody registered. */
  @Test
  void restoreDatabaseStillRefusesATargetWhoseDirectoryIsPresentButUnregistered() throws Exception {
    final String target = directoryWithoutARegistration("dironly7395_restore_db");

    final HttpResponse<String> response = postCommand("restore database " + target + " " + missingArchiveUrl());

    assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
    assertThat(new JSONObject(response.body()).getString("detail")).contains("already exists").contains(target);
  }

  /**
   * The state the issue describes reached the way it is actually reached - the directory removed from
   * under a database that is still open and registered - rather than the way the other tests build
   * it. And the refusal has to leave the operator a way out, because {@code restore database} has no
   * {@code overwrite} flag: {@code drop database} is the way out this fix's javadoc names, so it is
   * asserted here rather than assumed.
   */
  @Test
  void aTargetRefusedBecauseItIsOnlyRegisteredCanStillBeDroppedSoTheOperatorCanRetry() throws Exception {
    final String target = "outofband7395_restore_db";
    getServer(0).createDatabase(target, ComponentFile.MODE.READ_WRITE);
    registeredToUnregister.add(target);
    FileUtils.deleteRecursively(databaseDirectory(target));

    assertThat(getServer(0).existsDatabase(target)).isTrue();
    assertThat(databaseDirectory(target)).doesNotExist();

    final HttpResponse<String> refused = postCommand("restore database " + target + " " + missingArchiveUrl());
    assertThat(refused.statusCode()).as(refused.body()).isEqualTo(400);
    assertThat(new JSONObject(refused.body()).getString("detail")).contains("already exists");

    final HttpResponse<String> dropped = postCommand("drop database " + target);
    assertThat(dropped.statusCode()).as(dropped.body()).isEqualTo(200);
    assertThat(getServer(0).existsDatabase(target)).isFalse();
    registeredToUnregister.remove(target);
  }

  /**
   * The control. Without it every test above would pass against a check that refused every name: a
   * target that is neither registered nor on disk gets past the existence check and fails on the
   * archive instead.
   */
  @Test
  void restoreDatabaseWithAFreeNameGetsPastTheExistenceCheckAndFailsOnTheArchive() throws Exception {
    final HttpResponse<String> response = postCommand("restore database free7395_restore_db " + missingArchiveUrl());

    assertThat(response.statusCode()).as(response.body()).isNotEqualTo(200);
    assertThat(response.body()).doesNotContain("already exists");
    assertThat(getServer(0).existsDatabase("free7395_restore_db")).isFalse();
  }

  // -----------------------------------------------------------------------------------------------
  // restore backup
  // -----------------------------------------------------------------------------------------------

  /** Unchanged by this fix, and now expressed through the shared predicate rather than its own copy. */
  @Test
  void restoreBackupRefusesATargetRegisteredOnTheServerWhoseDirectoryIsGone() throws Exception {
    final String target = registeredWithoutADirectory("regonly7395_restore_backup");

    final HttpResponse<String> response = postCommand(
        "restore backup " + getDatabaseName() + " " + placeholderArchive() + " as " + target);

    assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
    assertThat(new JSONObject(response.body()).getString("detail")).contains("already exists").contains(target);
  }

  @Test
  void restoreBackupRefusesATargetWhoseDirectoryIsPresentButUnregistered() throws Exception {
    final String target = directoryWithoutARegistration("dironly7395_restore_backup");

    final HttpResponse<String> response = postCommand(
        "restore backup " + getDatabaseName() + " " + placeholderArchive() + " as " + target);

    assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
    assertThat(new JSONObject(response.body()).getString("detail")).contains("already exists").contains(target);
  }

  /**
   * The control for {@code restore backup}: a free target name gets past the existence check and
   * fails on the archive, which here is a placeholder that is not a backup at all.
   */
  @Test
  void restoreBackupWithAFreeTargetNameGetsPastTheExistenceCheckAndFailsOnTheArchive() throws Exception {
    final HttpResponse<String> response = postCommand(
        "restore backup " + getDatabaseName() + " " + placeholderArchive() + " as free7395_restore_backup");

    assertThat(response.statusCode()).as(response.body()).isNotEqualTo(200);
    assertThat(response.body()).doesNotContain("already exists");
    assertThat(getServer(0).existsDatabase("free7395_restore_backup")).isFalse();
  }

  // -----------------------------------------------------------------------------------------------
  // Fixture helpers
  // -----------------------------------------------------------------------------------------------

  /**
   * Creates a database and then drops it through the embedded instance, which deletes the directory
   * and leaves the registry entry behind - the divergent state, built with the server's own API.
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

  /**
   * A {@code file://} URL for an archive that is not there. The existence check runs before the
   * restore, so a target that is taken never gets far enough for the missing file to matter - and a
   * target that is free fails on it, which is what the control tests assert.
   */
  private String missingArchiveUrl() {
    return "file://" + new File("./target/nonexistent-7395.zip").getAbsolutePath();
  }

  /**
   * {@code restore backup} resolves its archive out of the backup directory before it looks at the
   * target, so the file has to be there; {@code resolveBackupFile} only requires a regular file whose
   * name looks like an archive. Its contents never matter here - every test either refuses before the
   * restore starts, or is the control that asserts the restore itself fails.
   */
  private String placeholderArchive() throws IOException {
    final File dbBackupDir = new File(backupDir, getDatabaseName());
    dbBackupDir.mkdirs();
    final File archive = new File(dbBackupDir, ARCHIVE_NAME);
    if (!archive.exists())
      Files.writeString(archive.toPath(), "not a real archive");
    return ARCHIVE_NAME;
  }

  private HttpResponse<String> postCommand(final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:" + getServer(0).getHttpServer().getPort() + "/api/v1/server"))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .build();
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }
}
