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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The SQL statement {@code EXPORT DATABASE} takes the same per-database maintenance slot the server's own
 * {@code trigger backup}, {@code restore database}, {@code restore backup} and {@code import database} take, and
 * that #7443 gave {@code BACKUP DATABASE} and {@code IMPORT DATABASE} (issue #7450).
 * <p>
 * It is executed by the ENGINE, which cannot see the server - the dependency runs the other way - so until this it
 * reached no admission policy at all. An export reads the whole database off disk and writes an archive, and a
 * concurrent {@code restore database} drops and replaces the very directory it is reading, which is the hazard
 * #7384 closed for backups.
 * <p>
 * This is the server half of the fix: the server binds its real {@link BackupCoordinator} to every database it
 * opens, so the refusal reaches an HTTP client as a 409 rather than as a 500, a slot held by a running export is
 * seen by the server's own entry points, and - the part that is new to {@code EXPORT} - a second export is NOT
 * refused. The statement's own side of the contract is {@code Issue7450SqlExportMaintenanceSlotTest} in the engine
 * module, and the admission policy itself is {@link Issue7450ExportAdmissionTest}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7450SqlExportMaintenanceSlotIT extends BaseGraphServerTest {
  /** The statement always writes under 'exports/' relative to the server process' working directory. */
  private final File exportDir = new File("./exports");

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    // The 'restore database' this fixture uses to probe the slot names a file:// URL, so the server has to be
    // willing to fetch one - otherwise it is refused by the URL policy with a 403 before it ever reaches the slot,
    // and the test would pass or fail for a reason that has nothing to do with #7450.
    config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
  }

  @BeforeEach
  void discardArchivesFromAnEarlierRun() {
    if (exportDir.exists())
      FileUtils.deleteRecursively(exportDir);
  }

  @AfterEach
  void releaseEverySlot() {
    // NOTHING MAY OUTLIVE A TEST HOLDING THE SLOT: THE NEXT ONE WOULD BE REFUSED FOR THE WRONG REASON. EXPORT IS
    // COUNTED RATHER THAN SET-VALUED, SO IT IS DRAINED RATHER THAN RELEASED ONCE
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();
    for (final Operation operation : Operation.values())
      coordinator.end(getDatabaseName(), operation);
    // BOUNDED RATHER THAN 'while (isInProgress)': A DRAIN THAT CANNOT TERMINATE WOULD HANG @AfterEach ITSELF, WHICH
    // NO @Timeout ON A TEST METHOD WOULD CATCH. NO TEST HERE TAKES MORE THAN A COUPLE OF EXPORT RESERVATIONS
    for (int i = 0; i < 16 && coordinator.isInProgress(getDatabaseName(), Operation.EXPORT); i++)
      coordinator.end(getDatabaseName(), Operation.EXPORT);

    if (exportDir.exists())
      FileUtils.deleteRecursively(exportDir);
  }

  /**
   * The reachability half: a statement can only find what the database carries, so the export has to be looking at
   * the same coordinator the server's own entry points use. Checked through both the wrapper the request path holds
   * and the embedded instance the SQL engine executes against.
   */
  @Test
  void theExportSeesTheSameCoordinatorTheServersOwnEntryPointsUse() {
    final DatabaseInternal database = getServer(0).getDatabase(getDatabaseName());

    assertThat(MaintenanceCoordinator.boundTo(database)).isSameAs(getServer(0).getBackupCoordinator());
    assertThat(MaintenanceCoordinator.boundTo(database.getEmbedded())).isSameAs(getServer(0).getBackupCoordinator());
  }

  /**
   * The headline case: a restore drops and replaces the database directory, so an export reading it at the same
   * time reads a directory that is about to be deleted. The refusal is a 409 - the request is well formed and
   * authorized, and retrying once the restore finishes is the fix - and it carries the same wording every other
   * entry point's refusal does.
   */
  @Test
  void aSqlExportAnswers409WhileARestoreOfTheDatabaseRuns() throws Exception {
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    assertThat(coordinator.begin(getDatabaseName(), Operation.RESTORE)).isNull();
    try {
      final HttpResponse<String> response = postSql("EXPORT DATABASE file://refused-7450.jsonl.tgz");
      assertThat(response.statusCode()).isEqualTo(409);
      assertThat(response.body()).contains(
          "Cannot export database '" + getDatabaseName() + "': a restore of it is already in progress");
    } finally {
      coordinator.end(getDatabaseName(), Operation.RESTORE);
    }

    // AND THE REFUSAL WAS THE GUARD, NOT A STATEMENT THAT COULD NEVER HAVE RUN
    assertThat(postSql("EXPORT DATABASE file://allowed-7450.jsonl.tgz").statusCode()).isEqualTo(200);
  }

  /**
   * The converse direction, and the one the issue is actually about: a slot held the way the SQL export holds it is
   * seen by the server's own entry points, so a {@code restore database} of that database is refused while the
   * export runs - and admitted the moment it ends.
   * <p>
   * The reservation is taken through {@link MaintenanceCoordinator#reserve} against the very database instance the
   * statement executes against, which is exactly what the statement does, so this needs no second thread and no
   * wait: the window is the try block rather than however long an export happens to take.
   */
  @Test
  void aSlotHeldTheWayTheSqlExportHoldsItRefusesTheServersOwnRestore() throws Exception {
    final DatabaseInternal database = getServer(0).getDatabase(getDatabaseName());

    try (final Reservation slot = MaintenanceCoordinator.reserve(database, Operation.EXPORT)) {
      assertThat(getServer(0).getBackupCoordinator().isInProgress(getDatabaseName(), Operation.EXPORT)).isTrue();

      final HttpResponse<String> refused = postServerCommand(
          "restore database " + getDatabaseName() + " file:///no/such/archive-7450.zip");
      assertThat(refused.statusCode()).isEqualTo(409);
      assertThat(refused.body()).contains(
          "Cannot restore database '" + getDatabaseName() + "': an export of it is already in progress");
    }

    assertThat(getServer(0).getBackupCoordinator().isInProgress(getDatabaseName())).isFalse();
  }

  /**
   * The property that made {@code EXPORT} its own issue rather than a line in #7443: two exports of one database
   * write two different files and must both be admitted. A second export therefore has to answer on its own
   * merits, never with a 409.
   */
  @Test
  void aSecondSqlExportOfTheSameDatabaseIsAdmitted() throws Exception {
    final DatabaseInternal database = getServer(0).getDatabase(getDatabaseName());

    try (final Reservation slot = MaintenanceCoordinator.reserve(database, Operation.EXPORT)) {
      final HttpResponse<String> response = postSql("EXPORT DATABASE file://second-7450.jsonl.tgz");
      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(response.body()).doesNotContain("already in progress");
    }

    // AND THE FINISHED SECOND EXPORT DID NOT TAKE THE FIRST ONE'S RESERVATION WITH IT
    assertThat(getServer(0).getBackupCoordinator().isInProgress(getDatabaseName())).isFalse();
  }

  /**
   * An export and a backup of one database read the same data for two different archives and neither destroys
   * anything, so the export is admitted beside a running backup - the same construction that lets a backup and an
   * import coexist (issue #7384).
   */
  @Test
  void aSqlExportIsAdmittedWhileABackupOfTheSameDatabaseRuns() throws Exception {
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    assertThat(coordinator.begin(getDatabaseName(), Operation.BACKUP)).isNull();
    try {
      final HttpResponse<String> response = postSql("EXPORT DATABASE file://beside-backup-7450.jsonl.tgz");
      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(response.body()).doesNotContain("already in progress");
    } finally {
      coordinator.end(getDatabaseName(), Operation.BACKUP);
    }
  }

  /**
   * A reservation leaked by a SQL export would block every later restore of that database until the server
   * restarts, which would turn one statement into an outage.
   */
  @Test
  void aCompletedSqlExportReleasesTheSlot() throws Exception {
    final HttpResponse<String> response = postSql("EXPORT DATABASE file://completed-7450.jsonl.tgz");
    assertThat(response.statusCode()).isEqualTo(200);

    assertThat(getServer(0).getBackupCoordinator().isInProgress(getDatabaseName())).isFalse();

    // AND THE SLOT IS GENUINELY FREE, NOT MERELY REPORTED FREE
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();
    assertThat(coordinator.begin(getDatabaseName(), Operation.RESTORE)).isNull();
    coordinator.end(getDatabaseName(), Operation.RESTORE);
  }

  /**
   * An export that FAILS must release the slot too. The target already exists and {@code overwrite} is not set, so
   * the exporter refuses it - a failure raised inside the reservation, which is where a leak would happen.
   */
  @Test
  void aFailedSqlExportReleasesTheSlot() throws Exception {
    assertThat(postSql("EXPORT DATABASE file://twice-7450.jsonl.tgz").statusCode()).isEqualTo(200);

    final HttpResponse<String> second = postSql("EXPORT DATABASE file://twice-7450.jsonl.tgz");
    assertThat(second.statusCode()).isNotEqualTo(200);
    assertThat(second.body()).doesNotContain("already in progress");

    assertThat(getServer(0).getBackupCoordinator().isInProgress(getDatabaseName())).isFalse();
  }

  /**
   * The premise the whole admission policy rests on: two exports of one database write two different files. For a
   * default-named export that was false, and not as a race - deterministically. The parsed statement is held in
   * {@code StatementCache}, the same instance is handed to every later execution of {@code EXPORT DATABASE}, and
   * the statement used to write the resolved default name back into its own {@code url} field. The second export
   * therefore aimed at the first one's archive: it failed outright, or replaced it under
   * {@code WITH overwrite = true}.
   */
  @Test
  void twoSuccessiveDefaultNamedExportsProduceTwoDifferentArchives() throws Exception {
    final HttpResponse<String> first = postSql("EXPORT DATABASE");
    assertThat(first.statusCode()).isEqualTo(200);

    final HttpResponse<String> second = postSql("EXPORT DATABASE");
    assertThat(second.statusCode())
        .as("the second export must not aim at the first one's archive: %s", second.body())
        .isEqualTo(200);

    assertThat(targetOf(first)).isNotEqualTo(targetOf(second));
    assertThat(new File(exportDir, targetOf(first))).exists();
    assertThat(new File(exportDir, targetOf(second))).exists();
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private static String targetOf(final HttpResponse<String> response) {
    return new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getString("toUrl");
  }

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
