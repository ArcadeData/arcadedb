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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The SQL maintenance statements {@code BACKUP DATABASE}, {@code IMPORT DATABASE} and {@code EXPORT DATABASE}, executed
 * by the engine, take the same per-database maintenance slot as the server's own control-plane commands, and two
 * exports cannot write the same target.
 */
class SqlMaintenanceSlotIT {
  // Regression: #7443 SQL BACKUP DATABASE and IMPORT DATABASE reached no admission policy at all
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
   * {@code SqlMaintenanceStatementTest.SqlMaintenanceSlot} in the engine module.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class BackupAndImport extends BaseGraphServerTest {
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
      final DatabaseInternal database = getServer(0).getDatabaseForOperator(getDatabaseName());

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
      final DatabaseInternal database = getServer(0).getDatabaseForOperator(getDatabaseName());

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

  // Regression: #7450 SQL EXPORT DATABASE reached no admission policy and could read a directory a restore was replacing
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
   * refused. The statement's own side of the contract is {@code SqlMaintenanceStatementTest.SqlExportMaintenanceSlot} in the engine
   * module, and the admission policy itself is {@link BackupCoordinatorTest.ExportAdmission}.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class Export extends BaseGraphServerTest {
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
      // NO @Timeout ON A TEST METHOD WOULD CATCH. THE BOUND IS AN ORDER OF MAGNITUDE ABOVE THE MOST ANY TEST HERE
      // TAKES (SIX, IN concurrentDefaultNamedExportsProduceDistinctArchives)
      for (int i = 0; i < 64 && coordinator.isInProgress(getDatabaseName(), Operation.EXPORT); i++)
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
      final DatabaseInternal database = getServer(0).getDatabaseForOperator(getDatabaseName());

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
      final DatabaseInternal database = getServer(0).getDatabaseForOperator(getDatabaseName());

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
      final DatabaseInternal database = getServer(0).getDatabaseForOperator(getDatabaseName());

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

    /**
     * The same premise under genuine concurrency, which is the case the admission policy actually creates: these
     * exports are all admitted at once, so if they resolved one name between them they would not merely refuse each
     * other - the formats check {@code file.exists()} and then create, so several would write one archive.
     * <p>
     * The successive test above cannot reach this: it fails on a cached statement reusing a name, which a per-run
     * timestamp alone already fixes. What it does NOT fix is two runs landing in the same millisecond, and that is
     * what the random component in the default name is for. Asserting the targets are distinct rather than trying to
     * force a same-millisecond collision keeps the test deterministic: without the random component the names of two
     * exports that do collide are equal, and with it they cannot be, whether or not the clock happened to separate
     * them on this run.
     */
    @Test
    @Timeout(120)
    void concurrentDefaultNamedExportsProduceDistinctArchives() throws Exception {
      final int exporters = 6;
      final CountDownLatch startTogether = new CountDownLatch(1);
      final List<String> targets = Collections.synchronizedList(new ArrayList<>());
      final List<String> failures = Collections.synchronizedList(new ArrayList<>());

      final ExecutorService executor = Executors.newFixedThreadPool(exporters);
      try {
        for (int i = 0; i < exporters; i++)
          executor.submit(() -> {
            try {
              startTogether.await();
              final HttpResponse<String> response = postSql("EXPORT DATABASE");
              if (response.statusCode() != 200)
                failures.add(response.body());
              else
                targets.add(targetOf(response));
            } catch (final Exception e) {
              failures.add(e.toString());
            }
            return null;
          });

        startTogether.countDown();
        executor.shutdown();
        assertThat(executor.awaitTermination(90, TimeUnit.SECONDS)).isTrue();
      } finally {
        executor.shutdownNow();
      }

      assertThat(failures).as("every concurrent export is admitted and completes").isEmpty();
      assertThat(targets).hasSize(exporters).doesNotHaveDuplicates();
      for (final String target : targets)
        assertThat(new File(exportDir, target)).exists();

      // AND THE NAME STILL CARRIES THE CONVENTION IT ALWAYS DID, SO THE SUFFIX IS AN ADDITION RATHER THAN A NEW SHAPE
      assertThat(targets).allMatch(t -> t.startsWith(getDatabaseName() + "-export-") && t.endsWith(".jsonl.tgz"));

      // THE SLOT IS FREE AGAIN: SIX RESERVATIONS WERE TAKEN AND SIX WERE RELEASED
      assertThat(getServer(0).getBackupCoordinator().isInProgress(getDatabaseName())).isFalse();
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

  // Regression: #7644 two concurrent exports to the same target interleaved their output into one archive
  /**
   * Issue #7644: {@link Operation#EXPORT} admits any number of exports of
   * one database at once (issue #7450), on the premise that two exports name two different files. Nothing enforced
   * that premise:
   * <ul>
   *   <li>two explicit {@code EXPORT DATABASE} statements naming the SAME URL with {@code WITH overwrite = true} -
   *   the exporter formats' {@code file.exists() && !overwriteFile} check does not even apply then, so two writers
   *   simply interleaved their output into one archive;</li>
   *   <li>the default name, built from a millisecond timestamp plus a random component (issue #7644's own fix to the
   *   deterministic half of this, landed with #7450) - astronomically unlikely to collide, but the SAME URL supplied
   *   explicitly twice was always reachable by a caller and had no protection at all.</li>
   * </ul>
   * The fix: {@code AbstractExporterFormat.claimExportFile} creates a lock file next to the resolved target,
   * atomically, before a byte is written - independent of {@code overwriteFile} - so the loser of the race is
   * refused outright ({@code ExportException}, "Another export to '...' is already in progress") instead of
   * corrupting the winner's output.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ExportTargetClaim extends BaseGraphServerTest {
    private final File exportDir = new File("./exports");

    @Override
    protected boolean isCreateDatabases() {
      return true;
    }

    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      super.onServerConfiguration(config);
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    }

    @BeforeEach
    void discardArchivesFromAnEarlierRun() {
      if (exportDir.exists())
        FileUtils.deleteRecursively(exportDir);
    }

    @AfterEach
    void cleanUp() {
      final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();
      for (int i = 0; i < 64 && coordinator.isInProgress(getDatabaseName()); i++)
        coordinator.end(getDatabaseName(), Operation.EXPORT);

      if (exportDir.exists())
        FileUtils.deleteRecursively(exportDir);
    }

    /**
     * Genuinely concurrent, not merely sequential: every racer names the SAME explicit URL with
     * {@code overwrite = true}, so none of them is stopped by the pre-existing (and still present)
     * {@code file.exists()} check - that check only ever refused a SECOND, later export, never two admitted together.
     * The claim is deterministic regardless of how the race actually interleaves: exactly one racer creates the lock
     * file, and every other one sees {@code FileAlreadyExistsException} and is refused before it opens the target for
     * writing.
     */
    @Test
    @Timeout(90)
    void twoConcurrentExportsToTheSameTargetDoNotBothWriteIt() throws Exception {
      final int racers = 8;
      final CountDownLatch startTogether = new CountDownLatch(1);
      final List<HttpResponse<String>> responses = Collections.synchronizedList(new ArrayList<>());

      final ExecutorService executor = Executors.newFixedThreadPool(racers);
      try {
        for (int i = 0; i < racers; i++)
          executor.submit(() -> {
            try {
              startTogether.await();
              responses.add(postSql("EXPORT DATABASE file://racing-7644.jsonl.tgz WITH overwrite = true"));
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
            return null;
          });

        startTogether.countDown();
        executor.shutdown();
        assertThat(executor.awaitTermination(80, TimeUnit.SECONDS)).isTrue();
      } finally {
        executor.shutdownNow();
      }

      final long succeeded = responses.stream().filter(r -> r.statusCode() == 200).count();
      final long refused = responses.stream()
          .filter(r -> r.statusCode() != 200 && r.body().contains("already in progress")).count();

      // THE LOCK IS A MUTEX ON THE TARGET, NOT A ONE-SHOT CLAIM: AN EXPORT OF THIS FIXTURE'S NEAR-EMPTY DATABASE CAN
      // FINISH FAST ENOUGH THAT A LATER RACER CLAIMS THE SAME PATH ONLY AFTER AN EARLIER ONE RELEASED IT, AND THAT
      // SECOND RUN IS LEGITIMATE - IT NEVER OVERLAPPED THE FIRST, SO NOTHING INTERLEAVED. WHAT MUST NEVER HAPPEN IS
      // TWO RACERS HOLDING THE CLAIM AT ONCE, WHICH IS WHAT "EVERY RESPONSE IS EITHER A CLEAN SUCCESS OR THIS EXACT
      // REFUSAL" RULES OUT: A CORRUPTED, INTERLEAVED WRITE WOULD SURFACE AS SOME OTHER FAILURE, NOT THIS ONE
      assertThat(succeeded + refused).as("every racer is either admitted or refused by the claim, nothing else: %s",
          responses).isEqualTo(racers);
      assertThat(succeeded).as("at least one racer claims the target: %s", responses).isGreaterThanOrEqualTo(1);

      // THE ARCHIVE THE WINNER WROTE IS INTACT - NOT TWO WRITERS' OUTPUT INTERLEAVED INTO ONE GZIP STREAM - AND NO
      // LOCK FILE WAS LEFT BEHIND
      final File archive = new File(exportDir, "racing-7644.jsonl.tgz");
      assertThat(archive).exists();
      try (final GZIPInputStream in = new GZIPInputStream(new FileInputStream(archive));
          final BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
        assertThat(reader.readLine()).as("the archive starts with a well-formed 'info' line").contains("\"t\":\"info\"");
      }
      assertThat(new File(exportDir, "racing-7644.jsonl.tgz.exporting")).doesNotExist();

      // AND THE SLOT ITSELF IS FREE AGAIN, EXACTLY AS IT IS AFTER ANY OTHER COMPLETED EXPORT
      assertThat(getServer(0).getBackupCoordinator().isInProgress(getDatabaseName())).isFalse();
    }

    /**
     * An export may not target a name that IS a lock file: {@code claimExportFile("x")} writes its claim to
     * {@code x.exporting}, so an export to {@code x.exporting} would open for writing the very file another export
     * is holding its claim in - and that export deletes it on release, taking this one's finished archive with it.
     * The suffix is refused as a reserved name instead (review of PR #7649).
     */
    @Test
    @Timeout(60)
    void anExportCannotTargetTheLockSuffixItself() throws Exception {
      final HttpResponse<String> response = postSql("EXPORT DATABASE file://reserved-7644.jsonl.tgz.exporting");

      assertThat(response.statusCode()).as("a target named like a lock file must be refused: %s", response.body())
          .isNotEqualTo(200);
      assertThat(response.body()).contains("reserved");

      // AND THE REFUSAL LEFT NOTHING BEHIND - NEITHER THE TARGET NOR A CLAIM ON IT
      assertThat(new File(exportDir, "reserved-7644.jsonl.tgz.exporting")).doesNotExist();
      assertThat(new File(exportDir, "reserved-7644.jsonl.tgz.exporting.exporting")).doesNotExist();
      assertThat(getServer(0).getBackupCoordinator().isInProgress(getDatabaseName())).isFalse();
    }

    /**
     * The parent directory has to be created from the RESOLVED path. Each format resolves {@code file://} off
     * {@code settings.file} only after its own early checks, so creating the parent from the unresolved string left
     * the real directory missing and the claim - a {@code Files.createFile} in it - failed on a directory the export
     * believed it had just created (review of PR #7649).
     */
    @Test
    @Timeout(60)
    void anExportCreatesTheResolvedParentDirectoryBeforeClaimingIt() throws Exception {
      final HttpResponse<String> response = postSql("EXPORT DATABASE file://nested-7644.jsonl.tgz");

      assertThat(response.statusCode()).as("the export must create 'exports/' itself: %s", response.body()).isEqualTo(200);
      assertThat(new File(exportDir, "nested-7644.jsonl.tgz")).exists();
      assertThat(new File(exportDir, "nested-7644.jsonl.tgz" + ".exporting")).doesNotExist();

      // AND NOTHING WAS CREATED UNDER A LITERAL 'file:' PARENT, WHICH IS WHAT RESOLVING THE PATH TOO LATE PRODUCED
      assertThat(new File("./file:")).doesNotExist();
    }

    /**
     * The lock is released even when the export FAILS after claiming it, so a failed export does not permanently
     * block every later one to the same target.
     */
    @Test
    @Timeout(60)
    void aFailedExportReleasesItsTargetClaim() throws Exception {
      // A NONEXISTENT includeTypes ENTRY DOES NOT FAIL THE EXPORT - USE overwrite = false TWICE INSTEAD, WHICH
      // GENUINELY FAILS ON THE SECOND, SEQUENTIAL CALL VIA THE PRE-EXISTING file.exists() CHECK, PROVING THE LOCK
      // FROM THE FIRST (SUCCESSFUL) RUN DID NOT LEAK
      assertThat(postSql("EXPORT DATABASE file://leak-check-7644.jsonl.tgz").statusCode()).isEqualTo(200);

      final HttpResponse<String> second = postSql("EXPORT DATABASE file://leak-check-7644.jsonl.tgz");
      assertThat(second.statusCode()).isNotEqualTo(200);
      assertThat(second.body()).as("the refusal is the pre-existing 'already exist' one, not a stale lock")
          .doesNotContain("already in progress");

      assertThat(new File(exportDir, "leak-check-7644.jsonl.tgz.exporting")).doesNotExist();

      // AND A FRESH EXPORT TO THE SAME TARGET WITH overwrite = true NOW SUCCEEDS - NOTHING IS LEFT CLAIMING IT
      assertThat(postSql("EXPORT DATABASE file://leak-check-7644.jsonl.tgz WITH overwrite = true").statusCode())
          .isEqualTo(200);
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private HttpResponse<String> postSql(final String command) throws Exception {
      return post("/api/v1/command/" + getDatabaseName(),
          new JSONObject().put("language", "sql").put("command", command));
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
}
