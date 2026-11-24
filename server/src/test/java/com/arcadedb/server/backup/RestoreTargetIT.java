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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MaintenanceCoordinator.Operation;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How {@code restore database} and {@code restore backup} validate, check and reserve their target database name.
 */
class RestoreTargetIT {
  // Regression: #7308 restore backup never validated the target database name, a path traversal sink
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
   * {@code GrpcRestoreImportIT.restoreBackupRefusesATargetNameThatLeavesTheDatabaseDirectory}.
   * <p>
   * The archive file name is deliberately one that does not exist. The name check has to run
   * <b>before</b> the archive is resolved, or a traversal target would be rejected only by luck -
   * whenever the caller happened to name a real backup.
   */
  @Nested
  class TargetName extends BaseGraphServerTest {
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
          .uri(new URI(getServerHttpUrl("/api/v1/server")))
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
          .uri(new URI(getServerHttpUrl("/api/v1/server")))
          .header("Authorization",
              "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
          .build();
      return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }
  }

  // Regression: #7395 restore database and restore backup disagreed on whether the target already existed
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
   * {@code GrpcRestoreTargetExistsIT} in the {@code grpcw} module. Both transports reach the
   * same {@code ServerControlPlane} methods, but a test that called those methods directly would pass
   * against a transport that never wired them up.
   */
  @Nested
  class TargetExists extends BaseGraphServerTest {
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
      getServer(0).getDatabaseForOperator(databaseName).getEmbedded().drop();
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

  // Regression: #7441 a create database landing inside a running restore was silently dropped and overwritten
  /**
   * A restore reserves its target name for as long as it runs, and refuses to activate over a database that
   * appeared anyway (issue #7441).
   * <p>
   * Issue #7384 gave every restore a per-database slot, which serialised it against the other restores, backups and
   * imports of the same database. {@code create database} asks that slot nothing, so the window between the restore's
   * "target already exists" pre-check and the directory swap minutes later was still wide open to it: a
   * {@code create database X} landing inside it was dropped and overwritten without a word, by the one command whose
   * contract is that it never replaces an existing database.
   * <p>
   * Two mechanisms close it, and both are exercised here. A name reservation, taken in the same {@code databasesLock}
   * section as the pre-check and released when the restore ends, binds every creator that goes through
   * {@link ArcadeDBServer} - which, {@code factory.create()} being called in exactly two places, is all of them. A
   * re-check immediately before the swap catches what a reservation cannot see: a directory that appeared out of band.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class NameReservation extends BaseGraphServerTest {
    private static final String BACKUP_DIR_NAME = "test-backups-7441";
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

    private final List<String> databasesToDrop = new ArrayList<>();

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
      // The archives this fixture restores from live on disk, so the server has to be willing to fetch a file:// URL.
      config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
    }

    @AfterEach
    void cleanUp() {
      final ArcadeDBServer server = getServer(0);

      for (final Operation operation : Operation.values())
        server.getBackupCoordinator().end(getDatabaseName(), operation);

      for (final String database : databasesToDrop) {
        try {
          // NOTHING MAY OUTLIVE A TEST HOLDING EITHER CLAIM: THE NEXT ONE WOULD BE REFUSED FOR THE WRONG REASON
          server.releaseDatabaseNameReservedForRestore(database);
          for (final Operation operation : Operation.values())
            server.getBackupCoordinator().end(database, operation);
          if (server.existsDatabase(database))
            server.getDatabaseForOperator(database).getEmbedded().drop();
        } catch (final Exception ignore) {
          // best-effort
        }
        FileUtils.deleteRecursively(databaseDirectory(database));
      }
      databasesToDrop.clear();

      if (backupConfigFile != null && backupConfigFile.exists())
        backupConfigFile.delete();
      if (backupDir != null && backupDir.exists())
        FileUtils.deleteRecursively(backupDir);
    }

    /**
     * Coverage-table rows 1 and 2: the create both transports run. HTTP's {@code create database} and gRPC's
     * {@code CreateDatabase} RPC each call {@link ServerControlPlane#createDatabase(String)} and nothing else, so
     * driving that method is driving both.
     * <p>
     * The probe runs from inside the restore's own progress stream, which is the only place "while the restore is
     * running" can be observed from one thread. Against the pre-#7441 code the create SUCCEEDS here, and the database
     * it creates is gone by the time the method returns - which is the whole of the bug.
     */
    @Test
    void aCreateDatabaseOfTheRestoreTargetIsRefusedWhileTheRestoreRuns() throws Exception {
      final String target = getDatabaseName() + "_7441_inflight";
      databasesToDrop.add(target);

      final String archive = triggerBackupAndGetFileName();
      final ServerControlPlane controlPlane = new ServerControlPlane(getServer(0));

      final List<String> failures = new ArrayList<>();
      final AtomicBoolean probed = new AtomicBoolean();

      controlPlane.restoreBackup(getDatabaseName(), archive, target, false, message -> {
        // #6086 logs one line per archive entry from a worker pool, so probe exactly once, from whichever thread
        // delivers the first event.
        if (!probed.compareAndSet(false, true))
          return;

        record(failures, "the restore holds the name while it runs",
            () -> assertThat(getServer(0).isDatabaseNameReservedForRestore(target)).isTrue());

        record(failures, "a create of the target is refused",
            () -> assertThatThrownBy(() -> controlPlane.createDatabase(target))
                .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
                .hasMessageContaining("Cannot create database '" + target + "': a restore of it is already in progress"));

        record(failures, "a create of a DIFFERENT name is not held up by this restore",
            () -> assertThat(getServer(0).isDatabaseNameReservedForRestore(target + "_other")).isFalse());
      });

      assertThat(probed).isTrue();
      assertThat(failures).isEmpty();

      // THE RESTORE FINISHED AND ITS OWN DATA IS WHAT IS THERE - NOT AN EMPTY DATABASE SOMEBODY ELSE CREATED
      assertThat(getServer(0).existsDatabase(target)).isTrue();
      assertThat(getServer(0).getDatabaseForOperator(target).countType(VERTEX1_TYPE_NAME, false)).isEqualTo(1);

      // AND THE NAME IS FREE AGAIN: THE REFUSAL ABOVE WAS THE GUARD, NOT A PERMANENT CLAIM
      assertThat(getServer(0).isDatabaseNameReservedForRestore(target)).isFalse();
    }

    /**
     * The same refusal as seen by an HTTP client. It is a 409 and not a 500: the request is well formed and
     * authorized, and retrying once the restore finishes is the fix - the same status {@code restore database} and
     * {@code trigger backup} already answer for the slot they share (issue #7384).
     */
    @Test
    void theHttpCreateDatabaseCommandAnswers409WhileTheNameIsReservedForARestore() throws Exception {
      final String target = getDatabaseName() + "_7441_http";
      databasesToDrop.add(target);

      getServer(0).reserveDatabaseNameForRestore(target);
      try {
        final HttpResponse<String> refused = postCommand("create database " + target);
        assertThat(refused.statusCode()).isEqualTo(409);
        assertThat(refused.body()).contains("a restore of it is already in progress");
      } finally {
        getServer(0).releaseDatabaseNameReservedForRestore(target);
      }

      // NOTHING WAS CREATED BY THE REFUSAL, AND THE VERY SAME COMMAND GOES THROUGH ONCE THE NAME IS FREE
      assertThat(getServer(0).existsDatabase(target)).isFalse();

      final HttpResponse<String> admitted = postCommand("create database " + target);
      assertThat(admitted.statusCode()).isEqualTo(200);
      assertThat(getServer(0).existsDatabase(target)).isTrue();
    }

    /**
     * Coverage-table row 3. {@code factory.create()} is reached from two places in {@link ArcadeDBServer}:
     * {@code createDatabase}, which the test above drives, and the {@code createIfNotExists} arm of
     * {@code getDatabase} that {@code getOrCreateDatabase} and the Gremlin plugin use. Both are guarded.
     */
    @Test
    void getOrCreateDatabaseIsRefusedWhileTheNameIsReservedForARestore() {
      final String target = getDatabaseName() + "_7441_getorcreate";
      databasesToDrop.add(target);

      final ArcadeDBServer server = getServer(0);
      server.reserveDatabaseNameForRestore(target);
      try {
        assertThatThrownBy(() -> server.getOrCreateDatabase(target))
            .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
            .hasMessageContaining("a restore of it is already in progress");

        assertThatThrownBy(() -> server.createDatabase(target, ComponentFile.MODE.READ_WRITE))
            .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
            .hasMessageContaining("a restore of it is already in progress");
      } finally {
        server.releaseDatabaseNameReservedForRestore(target);
      }

      assertThat(server.existsDatabase(target)).isFalse();
      assertThat(databaseDirectory(target)).doesNotExist();
    }

    /**
     * Coverage-table row 8: the half a reservation cannot cover. A directory appearing under the target's name - an
     * embedded {@code DatabaseFactory} in the same JVM, an operator's {@code mkdir}, a half-finished operation - is
     * invisible to an in-memory claim.
     * <p>
     * This is the <b>filesystem</b> half of the swap's guard, and nothing checks for it: the directory is registered
     * nowhere, so {@code existsDatabase} says no, and what refuses the restore is the move itself, which on this
     * branch carries neither {@code ATOMIC_MOVE} nor {@code REPLACE_EXISTING} and so fails with
     * {@code FileAlreadyExistsException}. A check could not have done the job - a directory can appear between any
     * check and the move that follows it.
     * <p>
     * The restore fails and the directory is still there, marker and all. Before this,
     * {@code swapRestoredDatabase} deleted it.
     */
    @Test
    void aDatabaseDirectoryThatAppearsDuringARestoreFailsTheSwapInsteadOfBeingDestroyed() throws Exception {
      final String target = getDatabaseName() + "_7441_outofband";
      databasesToDrop.add(target);

      final String archive = triggerBackupAndGetFileName();
      final ServerControlPlane controlPlane = new ServerControlPlane(getServer(0));

      final File targetDir = databaseDirectory(target);
      final File marker = new File(targetDir, "appeared-out-of-band.txt");
      final AtomicBoolean planted = new AtomicBoolean();

      assertThatThrownBy(() -> controlPlane.restoreBackup(getDatabaseName(), archive, target, false, message -> {
        if (!planted.compareAndSet(false, true))
          return;
        try {
          assertThat(targetDir.mkdirs()).isTrue();
          try (final FileWriter writer = new FileWriter(marker)) {
            writer.write("not the restore's to delete");
          }
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      })).hasMessageContaining("appeared while the restore was running");

      assertThat(planted).isTrue();
      assertThat(marker).exists();
      assertThat(getServer(0).existsDatabase(target)).isFalse();

      // AND THE FAILED RESTORE LEFT NEITHER CLAIM BEHIND
      assertThat(getServer(0).isDatabaseNameReservedForRestore(target)).isFalse();
      assertThat(getServer(0).getBackupCoordinator().isInProgress(target)).isFalse();
    }

    /**
     * The other half of row 8, and the half that decides whether the tripwire is worth having: a database that is
     * REGISTERED - not merely a directory on disk - when the swap comes round.
     * <p>
     * This is the <b>registry</b> half, the one a check does answer: the restore drops its predecessor through
     * {@code dropDatabaseForRestore}, which has to run outside {@code databasesLock} because an HA drop round-trips
     * through Raft and the apply thread takes that lock (issue #4832). A guard placed after it would therefore have
     * been asking whether a database still existed immediately after dropping it. So the drop is gated on the caller
     * having asked for a replacement, and {@code existsDatabase} is re-asked inside the swap's own lock section.
     * <p>
     * The registered database is planted by releasing the claim, creating, and re-claiming - standing in for a creator
     * an in-memory claim cannot bind: another process, or anything that reaches the directory without going through
     * this {@link ArcadeDBServer}.
     */
    @Test
    void aDatabaseRegisteredDuringARestoreIsNotDroppedByTheSwap() throws Exception {
      final String target = getDatabaseName() + "_7441_registered";
      databasesToDrop.add(target);

      final String archive = triggerBackupAndGetFileName();
      final ArcadeDBServer server = getServer(0);
      final ServerControlPlane controlPlane = new ServerControlPlane(server);
      final AtomicBoolean planted = new AtomicBoolean();

      assertThatThrownBy(() -> controlPlane.restoreBackup(getDatabaseName(), archive, target, false, message -> {
        if (!planted.compareAndSet(false, true))
          return;
        server.releaseDatabaseNameReservedForRestore(target);
        try {
          controlPlane.createDatabase(target);
        } finally {
          server.reserveDatabaseNameForRestore(target);
        }
      })).hasMessageContaining("appeared while the restore was running");

      assertThat(planted).isTrue();

      // THE DATABASE THAT WAS THERE IS STILL THERE, AND STILL USABLE - NOT DROPPED AND NOT HALF-SWAPPED
      assertThat(server.existsDatabase(target)).isTrue();
      assertThat(server.getDatabaseForOperator(target).isOpen()).isTrue();
      assertThat(databaseDirectory(target)).exists();
    }

    /**
     * A reservation leaked by a failed restore would block every later create of that name until the server restarts,
     * turning one bad URL into an outage - the same hazard {@code aFailedRestoreReleasesTheSlot} guards for the slot.
     */
    @Test
    void aFailedRestoreReleasesTheNameReservation() throws Exception {
      final String target = getDatabaseName() + "_7441_failed";
      databasesToDrop.add(target);

      final HttpResponse<String> failed = postCommand(
          "restore database " + target + " file:///no/such/archive-7441.zip");
      assertThat(failed.statusCode()).isNotEqualTo(200);

      assertThat(getServer(0).isDatabaseNameReservedForRestore(target)).isFalse();

      // AND THE NAME IS GENUINELY FREE, NOT MERELY REPORTED FREE
      final HttpResponse<String> created = postCommand("create database " + target);
      assertThat(created.statusCode()).isEqualTo(200);
    }

    /**
     * The pre-swap re-check must not break the case it does not apply to: {@code overwrite} is the caller saying they
     * meant it, so a target that exists at swap time is replaced rather than refused (issue #5027).
     */
    @Test
    void restoreBackupWithOverwriteStillReplacesAnExistingTarget() throws Exception {
      final String archive = triggerBackupAndGetFileName();
      final ServerControlPlane controlPlane = new ServerControlPlane(getServer(0));

      controlPlane.restoreBackup(getDatabaseName(), archive, getDatabaseName(), true,
          ServerControlPlane.ProgressListener.NOOP);

      assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
      assertThat(getServer(0).getDatabaseForOperator(getDatabaseName()).countType(VERTEX1_TYPE_NAME, false)).isEqualTo(1);
    }

    /**
     * Issue #10881: the claim's wait for the commits in flight can now last as long as a quorum timeout, and an interrupt ends it
     * with an exception. That is thrown inside {@code reserveRestoreTarget}, before the caller's {@code finally} that releases the
     * claim is entered, so the claim is released there: a leaked one would refuse every write to the database and every create of
     * its name until the server restarts.
     */
    @Test
    void anInterruptedWaitForTheCommitsInFlightReleasesTheClaim() throws Exception {
      final String archive = triggerBackupAndGetFileName();
      final ServerControlPlane controlPlane = new ServerControlPlane(getServer(0));
      final LocalDatabase local = (LocalDatabase) getServer(0).getDatabaseForOperator(getDatabaseName()).getEmbedded();

      // A commit that left the database lock and is still on its way to the cluster, as an HA leader's is
      local.executeInReadLock(() -> {
        local.beginCommitInFlight();
        return null;
      });
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread restore = new Thread(() -> {
        try {
          controlPlane.restoreBackup(getDatabaseName(), archive, getDatabaseName(), true, ServerControlPlane.ProgressListener.NOOP);
        } catch (final Throwable t) {
          failure.set(t);
        }
      });
      try {
        restore.start();
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> getServer(0).isDatabaseNameReservedForRestore(getDatabaseName())
            && restore.getState() == Thread.State.TIMED_WAITING);
        restore.interrupt();
        restore.join(30_000);
      } finally {
        local.endCommitInFlight();
      }

      assertThat(failure.get()).isInstanceOf(DatabaseOperationException.class).hasMessageContaining("in flight");
      assertThat(getServer(0).isDatabaseNameReservedForRestore(getDatabaseName())).as("the claim is released").isFalse();
      assertThat(local.getWriteRefusal()).as("and so is the refusal it put on the database").isNull();
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private static void record(final List<String> failures, final String what, final Runnable assertion) {
      try {
        assertion.run();
      } catch (final Throwable t) {
        failures.add(what + ": " + t.getMessage());
      }
    }

    private File databaseDirectory(final String databaseName) {
      return new File(getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY)
          + File.separator + databaseName);
    }

    private String triggerBackupAndGetFileName() throws Exception {
      final HttpResponse<String> triggered = postCommand("trigger backup " + getDatabaseName());
      assertThat(triggered.statusCode()).isEqualTo(200);

      final HttpResponse<String> listed = postCommand("list backups " + getDatabaseName());
      assertThat(listed.statusCode()).isEqualTo(200);

      final JSONArray backups = new JSONObject(listed.body()).getJSONArray("backups");
      assertThat(backups.length()).isPositive();
      return backups.getJSONObject(backups.length() - 1).getString("fileName");
    }

    private HttpResponse<String> postCommand(final String command) throws Exception {
      final HttpClient client = HttpClient.newHttpClient();
      final HttpRequest request = HttpRequest.newBuilder()
          .uri(new URI("http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/server"))
          .header("Authorization",
              "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
          .build();
      return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
  }
}
