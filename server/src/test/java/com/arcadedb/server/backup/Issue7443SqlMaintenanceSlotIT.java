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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.MaintenanceCoordinator;
import com.arcadedb.engine.MaintenanceCoordinator.Operation;
import com.arcadedb.engine.MaintenanceCoordinator.Reservation;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The SQL statements {@code BACKUP DATABASE} and {@code IMPORT DATABASE} take the same per-database maintenance
 * slot the server's own {@code trigger backup}, {@code restore database}, {@code restore backup} and
 * {@code import database} take (issue #7443).
 * <p>
 * They are executed by the ENGINE, which cannot see the server - the dependency runs the other way - so until this
 * they reached no admission policy at all. A client sending {@code BACKUP DATABASE} as SQL over HTTP, Postgres,
 * gRPC or Bolt ran a full backup of a live database that a concurrent {@code restore database} would not see and
 * could drop the directory out from under, and a second SQL backup of the same database was not refused either:
 * two archives named from the same timestamp writing into one file, which is #6753 on this path.
 * <p>
 * This is the server half of the fix. It pins that the server binds its real {@link BackupCoordinator} to every
 * database it opens, that a refusal reaches an HTTP client as a 409 rather than as a 500, and that a slot held by
 * a SQL statement is seen by the server's own entry points. The statements' own side of the contract - reserve
 * before starting, release on every way out, leave an unco-ordinated embedded database alone - is
 * {@code Issue7443SqlMaintenanceSlotTest} in the engine module.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7443SqlMaintenanceSlotIT extends BaseGraphServerTest {
  private File backupDir;

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");

    backupDir = new File("./target/backups-7443");
    if (backupDir.exists())
      FileUtils.deleteRecursively(backupDir);
    backupDir.mkdirs();
    config.setValue(GlobalConfiguration.SERVER_BACKUP_DIRECTORY, backupDir.getAbsolutePath());
    GlobalConfiguration.SERVER_BACKUP_DIRECTORY.setValue(backupDir.getAbsolutePath());
  }

  @AfterEach
  void releaseEverySlot() {
    // NOTHING MAY OUTLIVE A TEST HOLDING THE SLOT: THE NEXT ONE WOULD BE REFUSED FOR THE WRONG REASON
    for (final Operation operation : Operation.values())
      getServer(0).getBackupCoordinator().end(getDatabaseName(), operation);

    GlobalConfiguration.SERVER_BACKUP_DIRECTORY.reset();
    if (backupDir != null && backupDir.exists())
      FileUtils.deleteRecursively(backupDir);
  }

  /**
   * The reachability half: a statement can only find what the database carries, so the server has to have bound its
   * coordinator to it. Checked through both the wrapper the request path holds and the embedded instance the SQL
   * engine executes against, because a binding visible from only one of them would leave the statement looking at
   * nothing.
   */
  @Test
  void theServerBindsItsCoordinatorToTheDatabasesItOpens() {
    final DatabaseInternal database = getServer(0).getDatabase(getDatabaseName());

    assertThat(MaintenanceCoordinator.boundTo(database)).isSameAs(getServer(0).getBackupCoordinator());
    assertThat(MaintenanceCoordinator.boundTo(database.getEmbedded())).isSameAs(getServer(0).getBackupCoordinator());
  }

  /**
   * The headline case: a restore drops and replaces the database directory, so a SQL backup reading it at the same
   * time reads a directory that is about to be deleted. The refusal is a 409 - the request is well formed and
   * authorized, and retrying once the restore finishes is the fix - and it carries the same wording a
   * {@code trigger backup} refusal does.
   */
  @Test
  void aSqlBackupAnswers409WhileARestoreOfTheDatabaseRuns() throws Exception {
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    assertThat(coordinator.begin(getDatabaseName(), Operation.RESTORE)).isNull();
    try {
      final HttpResponse<String> response = postSql("BACKUP DATABASE");
      assertThat(response.statusCode()).isEqualTo(409);
      assertThat(response.body()).contains(
          "Cannot back up database '" + getDatabaseName() + "': a restore of it is already in progress");
    } finally {
      coordinator.end(getDatabaseName(), Operation.RESTORE);
    }

    // AND THE REFUSAL WAS THE GUARD, NOT A STATEMENT THAT COULD NEVER HAVE RUN
    assertThat(postSql("BACKUP DATABASE").statusCode()).isEqualTo(200);
  }

  /** The same for an import, which a restore of its target excludes for the same reason. */
  @Test
  void aSqlImportAnswers409WhileARestoreOfTheDatabaseRuns() throws Exception {
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    assertThat(coordinator.begin(getDatabaseName(), Operation.RESTORE)).isNull();
    try {
      final HttpResponse<String> response = postSql("IMPORT DATABASE file:///no/such/source-7443.jsonl");
      assertThat(response.statusCode()).isEqualTo(409);
      assertThat(response.body()).contains(
          "Cannot import database '" + getDatabaseName() + "': a restore of it is already in progress");
    } finally {
      coordinator.end(getDatabaseName(), Operation.RESTORE);
    }
  }

  /**
   * Issue #6753 on the SQL path: two full backups of one database read and compress the same data twice for one
   * usable archive, and both resolve their default name from a timestamp, so the redundant one is refused rather
   * than left to write into the other's file.
   */
  @Test
  void aSqlBackupIsRefusedWhileAnotherBackupOfTheSameDatabaseRuns() throws Exception {
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    assertThat(coordinator.begin(getDatabaseName(), Operation.BACKUP)).isNull();
    try {
      final HttpResponse<String> response = postSql("BACKUP DATABASE");
      assertThat(response.statusCode()).isEqualTo(409);
      assertThat(response.body()).contains(
          "Cannot back up database '" + getDatabaseName() + "': a backup of it is already in progress");
    } finally {
      coordinator.end(getDatabaseName(), Operation.BACKUP);
    }
  }

  /**
   * A backup and an import of one database coexist by construction - an import is ordinary transactions against a
   * live database, and backing a live database up is what the auto-backup schedule does all day. The SQL import
   * therefore has to be admitted, and fail on its own merits: a source URL that does not exist, not a 409.
   */
  @Test
  void aSqlImportIsAdmittedWhileABackupOfTheSameDatabaseRuns() throws Exception {
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    assertThat(coordinator.begin(getDatabaseName(), Operation.BACKUP)).isNull();
    try {
      final HttpResponse<String> response = postSql("IMPORT DATABASE file:///no/such/source-7443.jsonl");
      assertThat(response.statusCode()).isNotEqualTo(409);
      assertThat(response.body()).doesNotContain("already in progress");
    } finally {
      coordinator.end(getDatabaseName(), Operation.BACKUP);
    }
  }

  /**
   * The converse direction, and the one the issue is actually about: a slot held the way the SQL statement holds it
   * is seen by the server's own entry points, so the {@code trigger backup} the auto-backup schedule and every
   * other transport reach is refused while a SQL backup runs - and admitted the moment it ends.
   * <p>
   * The reservation is taken through {@link MaintenanceCoordinator#reserve} against the very database instance the
   * statement executes against, which is exactly what the statement does, so this needs no second thread and no
   * wait: the window is the try block rather than however long a backup happens to take.
   */
  @Test
  void aSlotHeldTheWayTheSqlStatementHoldsItRefusesTheServersOwnBackup() throws Exception {
    final DatabaseInternal database = getServer(0).getDatabase(getDatabaseName());

    try (final Reservation slot = MaintenanceCoordinator.reserve(database, Operation.BACKUP)) {
      assertThat(getServer(0).getBackupCoordinator().isInProgress(getDatabaseName(), Operation.BACKUP)).isTrue();

      // THE PRE-#7384 WORDING, BECAUSE 'trigger backup' REFUSED BY ANOTHER *BACKUP* IS THE ONE CASE THAT EXISTED
      // BEFORE THE SLOT TOOK RESTORES AND IMPORTS TOO, AND IT KEPT ITS MESSAGE VERBATIM. WHAT MATTERS HERE IS THAT
      // IT IS REFUSED AT ALL: THE HOLDER IS A SQL STATEMENT'S RESERVATION.
      final HttpResponse<String> refused = postServerCommand("trigger backup " + getDatabaseName());
      assertThat(refused.statusCode()).isEqualTo(409);
      assertThat(refused.body()).contains(
          "A backup of database '" + getDatabaseName() + "' is already in progress");
    }

    assertThat(getServer(0).getBackupCoordinator().isInProgress(getDatabaseName())).isFalse();
    assertThat(postServerCommand("trigger backup " + getDatabaseName()).statusCode()).isEqualTo(200);
  }

  /**
   * A reservation leaked by a SQL backup would block every later backup, restore and import of that database until
   * the server restarts, which would turn one statement into an outage.
   */
  @Test
  void aCompletedSqlBackupReleasesTheSlot() throws Exception {
    final HttpResponse<String> response = postSql("BACKUP DATABASE");
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("backupFile");

    assertThat(getServer(0).getBackupCoordinator().isInProgress(getDatabaseName())).isFalse();

    // AND THE SLOT IS GENUINELY FREE, NOT MERELY REPORTED FREE
    assertThat(postServerCommand("trigger backup " + getDatabaseName()).statusCode()).isEqualTo(200);
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private HttpResponse<String> postSql(final String command) throws Exception {
    return post("/api/v1/command/" + getDatabaseName(),
        new JSONObject().put("language", "sql").put("command", command));
  }

  private HttpResponse<String> postServerCommand(final String command) throws Exception {
    return post("/api/v1/server", new JSONObject().put("command", command));
  }

  private HttpResponse<String> post(final String path, final JSONObject payload) throws Exception {
    final HttpClient client = HttpClient.newHttpClient();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + path))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
