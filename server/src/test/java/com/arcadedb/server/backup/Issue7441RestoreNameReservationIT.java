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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.engine.MaintenanceCoordinator.Operation;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue7441RestoreNameReservationIT extends BaseGraphServerTest {
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
          server.getDatabase(database).getEmbedded().drop();
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
    assertThat(getServer(0).getDatabase(target).countType(VERTEX1_TYPE_NAME, false)).isEqualTo(1);

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
    assertThat(server.getDatabase(target).isOpen()).isTrue();
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
    assertThat(getServer(0).getDatabase(getDatabaseName()).countType(VERTEX1_TYPE_NAME, false)).isEqualTo(1);
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
