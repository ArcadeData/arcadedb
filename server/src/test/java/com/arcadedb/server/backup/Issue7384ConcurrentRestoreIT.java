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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A restore excludes every other backup, restore and import of the same database, and is excluded by them
 * (issue #7384).
 * <p>
 * Before this, {@code triggerBackup} was the only one of the four that took the per-database slot
 * {@link BackupCoordinator} hands out. {@code restoreDatabase}, {@code restoreBackup} and {@code importDatabase} took
 * nothing, and the only guard a restore had was an existence pre-check that was not atomic with the directory swap
 * that follows it: two restores of one database both passed it, both restored into their own temporary directory and
 * both reached the swap, and the loser's caller was told it had succeeded. A restore could equally drop the directory
 * a backup was reading, since the backup held a slot the restore never asked for.
 * <p>
 * The scope is one server, which is where the transports meet: HTTP and gRPC both run the same
 * {@code ServerControlPlane}, and in HA every restore is refused anywhere but on the leader. Serialising this across a
 * cluster is a separate change the issue puts out of scope.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7384ConcurrentRestoreIT extends BaseGraphServerTest {
  private static final String BACKUP_DIR_NAME = "test-backups-7384";
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

    // NOTHING MAY OUTLIVE A TEST HOLDING THE SLOT: THE NEXT ONE WOULD BE REFUSED FOR THE WRONG REASON
    for (final Operation operation : Operation.values())
      server.getBackupCoordinator().end(getDatabaseName(), operation);

    for (final String database : databasesToDrop) {
      try {
        for (final Operation operation : Operation.values())
          server.getBackupCoordinator().end(database, operation);
        if (server.existsDatabase(database))
          server.getDatabase(database).getEmbedded().drop();
      } catch (final Exception ignore) {
        // best-effort
      }
    }
    databasesToDrop.clear();

    if (backupConfigFile != null && backupConfigFile.exists())
      backupConfigFile.delete();
    if (backupDir != null && backupDir.exists())
      FileUtils.deleteRecursively(backupDir);
  }

  /**
   * The one test that does not hold the slot by hand: a real restore is started through the shared implementation and
   * every other operation on its target is attempted from inside its own progress stream, which is the only place
   * "while the restore is running" can be observed from a single thread.
   * <p>
   * It is the test that fails against the pre-#7384 code for the reason the issue describes - nothing was refused,
   * and a second restore would have run all the way to the swap.
   */
  @Test
  void aRestoreInFlightRefusesEveryOtherOperationOnItsTarget() throws Exception {
    final String target = getDatabaseName() + "_7384_inflight";
    databasesToDrop.add(target);

    final String archive = triggerBackupAndGetFileName();
    final ServerControlPlane controlPlane = new ServerControlPlane(getServer(0));
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    final List<String> failures = new ArrayList<>();
    final AtomicBoolean probed = new AtomicBoolean();

    controlPlane.restoreBackup(getDatabaseName(), archive, target, false, message -> {
      // #6086 logs one line per archive entry from a worker pool, so probe exactly once, from whichever thread
      // delivers the first event.
      if (!probed.compareAndSet(false, true))
        return;

      record(failures, "the restore holds the slot while it runs",
          () -> assertThat(coordinator.isInProgress(target, Operation.RESTORE)).isTrue());

      record(failures, "a second restore of the target is refused",
          () -> assertThatThrownBy(() -> controlPlane.restoreBackup(getDatabaseName(), archive, target, true,
              ServerControlPlane.ProgressListener.NOOP))
              .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
              .hasMessageContaining("a restore of it is already in progress"));

      record(failures, "a backup of the target is refused",
          () -> assertThatThrownBy(() -> controlPlane.triggerBackup(target))
              .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
              .hasMessageContaining("a restore of it is already in progress"));

      record(failures, "an import into the target is refused",
          () -> assertThatThrownBy(() -> controlPlane.importDatabase(target, archiveUrl(archive),
              ServerControlPlane.ProgressListener.NOOP))
              .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
              .hasMessageContaining("a restore of it is already in progress"));

      record(failures, "the source database is not held up by a restore of a different target",
          () -> assertThat(coordinator.isInProgress(getDatabaseName())).isFalse());
    });

    assertThat(probed).isTrue();
    assertThat(failures).isEmpty();

    // THE SLOT IS RELEASED WHEN THE RESTORE ENDS, AND THE VERY SAME CALL GOES THROUGH: THE REFUSALS ABOVE ARE THE
    // GUARD, NOT A RESTORE THAT COULD NEVER HAVE RUN
    assertThat(coordinator.isInProgress(target)).isFalse();
    assertThat(getServer(0).existsDatabase(target)).isTrue();

    controlPlane.restoreBackup(getDatabaseName(), archive, target, true, ServerControlPlane.ProgressListener.NOOP);
    assertThat(coordinator.isInProgress(target)).isFalse();
  }

  /**
   * Issue #7384 cases 2 and 3: a restore that starts while a backup of the same database runs would drop and replace
   * the directory the backup is reading. Whichever of the two arrives second is the one refused.
   */
  @Test
  void aBackupInFlightRefusesARestoreOfTheSameDatabase() throws Exception {
    final String archive = triggerBackupAndGetFileName();
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    // SOMEBODY ELSE IS ALREADY BACKING THIS DATABASE UP - THE AUTO-BACKUP SCHEDULE OR ANOTHER TRANSPORT'S TRIGGER
    assertThat(coordinator.begin(getDatabaseName(), Operation.BACKUP)).isNull();
    try {
      assertRefusedWith409("restore backup " + getDatabaseName() + " " + archive + " as " + getDatabaseName(),
          true, "a backup of it is already in progress");
      assertRefusedWith409("restore database " + getDatabaseName() + " " + archiveUrl(archive),
          false, "a backup of it is already in progress");
    } finally {
      coordinator.end(getDatabaseName(), Operation.BACKUP);
    }

    // AND THE REFUSAL WAS THE GUARD, NOT A COMMAND THAT COULD NEVER HAVE RUN
    final HttpResponse<String> admitted = postCommand(new JSONObject()
        .put("command", "restore backup " + getDatabaseName() + " " + archive + " as " + getDatabaseName())
        .put("overwrite", true));
    assertThat(admitted.statusCode()).isEqualTo(200);
  }

  /**
   * The three HTTP commands that take the slot, each driven through its own entry point. A refusal reaches the client
   * as a 409 rather than the 500 an unmapped runtime failure would have produced: the request is well formed and
   * authorized, and retrying once the other operation finishes is the fix.
   */
  @Test
  void everyHttpRestoreAndImportCommandAnswers409WhileARestoreOfTheTargetRuns() throws Exception {
    final String target = getDatabaseName() + "_7384_http";
    databasesToDrop.add(target);

    final String archive = triggerBackupAndGetFileName();
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    assertThat(coordinator.begin(target, Operation.RESTORE)).isNull();
    try {
      assertRefusedWith409("restore database " + target + " " + archiveUrl(archive),
          false, "a restore of it is already in progress");
      assertRefusedWith409("restore backup " + getDatabaseName() + " " + archive + " as " + target,
          true, "a restore of it is already in progress");
      assertRefusedWith409("import database " + target + " " + archiveUrl(archive),
          false, "a restore of it is already in progress");
      // AND THE VERB IS THE ONE A HUMAN WOULD WRITE: "back up", NOT "backup", WHICH IS THE NOUN
      assertRefusedWith409("trigger backup " + target,
          false, "Cannot back up database '" + target + "': a restore of it is already in progress");
    } finally {
      coordinator.end(target, Operation.RESTORE);
    }

    // NOTHING WAS CREATED BY ANY OF THE REFUSALS
    assertThat(getServer(0).existsDatabase(target)).isFalse();
  }

  /**
   * Studio and every streaming client send {@code Accept: text/event-stream}, and a refusal there has to stay an HTTP
   * status rather than becoming a 200 carrying an error frame. It does because the slot is taken before the operation
   * produces its first progress line, so the stream has not started yet - the same property the existence and URL
   * checks rely on.
   */
  @Test
  void anSseClientGetsTheRefusalAsA409RatherThanAsAnErrorFrame() throws Exception {
    final String target = getDatabaseName() + "_7384_sse";
    databasesToDrop.add(target);

    final String archive = triggerBackupAndGetFileName();
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    assertThat(coordinator.begin(target, Operation.RESTORE)).isNull();
    try {
      final HttpResponse<String> response = postCommand(
          new JSONObject().put("command", "restore database " + target + " " + archiveUrl(archive)), true);

      assertThat(response.statusCode()).isEqualTo(409);
      assertThat(response.body()).contains("a restore of it is already in progress");
      assertThat(response.headers().firstValue("Content-Type").orElse("")).doesNotContain("text/event-stream");
    } finally {
      coordinator.end(target, Operation.RESTORE);
    }
  }

  /**
   * The third arm of the matrix: an import in flight refuses a restore of the same database, and the refusal names
   * the import rather than defaulting to "a backup" the way the single-purpose message used to.
   */
  @Test
  void anImportInFlightRefusesARestoreOfTheSameDatabaseAndNamesItself() throws Exception {
    final String archive = triggerBackupAndGetFileName();
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    assertThat(coordinator.begin(getDatabaseName(), Operation.IMPORT)).isNull();
    try {
      assertRefusedWith409("restore backup " + getDatabaseName() + " " + archive + " as " + getDatabaseName(),
          true, "an import of it is already in progress");
      assertRefusedWith409("import database " + getDatabaseName() + " " + archiveUrl(archive),
          false, "an import of it is already in progress");
    } finally {
      coordinator.end(getDatabaseName(), Operation.IMPORT);
    }
  }

  /** A scheduled tick is skipped rather than refused: the schedule covers the database again on the next one. */
  @Test
  void theScheduledBackupTaskSkipsWhileARestoreOfTheDatabaseRuns() {
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();
    final File dbBackupDir = new File(backupDir, getDatabaseName());
    final BackupTask task = new BackupTask(getServer(0), getDatabaseName(), backupConfig(),
        backupDir.getAbsolutePath(), null);

    assertThat(coordinator.begin(getDatabaseName(), Operation.RESTORE)).isNull();
    try {
      task.run();
      assertThat(archives(dbBackupDir)).isEmpty();
    } finally {
      coordinator.end(getDatabaseName(), Operation.RESTORE);
    }

    // AND THE VERY SAME TASK GOES THROUGH ONCE THE RESTORE IS DONE
    task.run();
    assertThat(archives(dbBackupDir)).hasSize(1);
  }

  /**
   * An import is ordinary transactions against a live database, and backing a live database up is what the auto-backup
   * schedule does all day, so the two coexist deliberately. Taking the slot for an import must not starve the
   * schedule for as long as a multi-hour load runs.
   */
  @Test
  void anImportInFlightDoesNotHoldUpABackupOfTheSameDatabase() throws Exception {
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    assertThat(coordinator.begin(getDatabaseName(), Operation.IMPORT)).isNull();
    try {
      final HttpResponse<String> response = postCommand("trigger backup " + getDatabaseName());
      assertThat(response.statusCode()).isEqualTo(200);
    } finally {
      coordinator.end(getDatabaseName(), Operation.IMPORT);
    }
  }

  /**
   * A reservation leaked by a failed restore would block every later backup, restore and import of that database
   * until the server restarts, which would turn one bad URL into an outage.
   */
  @Test
  void aFailedRestoreReleasesTheSlot() throws Exception {
    final String target = getDatabaseName() + "_7384_failed";
    databasesToDrop.add(target);

    final HttpResponse<String> failed = postCommand(
        "restore database " + target + " file:///no/such/archive-7384.zip");
    assertThat(failed.statusCode()).isNotEqualTo(200);

    assertThat(getServer(0).getBackupCoordinator().isInProgress(target)).isFalse();

    // AND THE SLOT IS GENUINELY FREE, NOT MERELY REPORTED FREE
    final String archive = triggerBackupAndGetFileName();
    final HttpResponse<String> retried = postCommand("restore database " + target + " " + archiveUrl(archive));
    assertThat(retried.statusCode()).isEqualTo(200);
    assertThat(getServer(0).existsDatabase(target)).isTrue();
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

  private void assertRefusedWith409(final String command, final boolean overwrite, final String expectedMessage)
      throws Exception {
    final JSONObject payload = new JSONObject().put("command", command);
    if (overwrite)
      payload.put("overwrite", true);

    final HttpResponse<String> response = postCommand(payload);
    assertThat(response.statusCode()).as("status of: %s", command).isEqualTo(409);
    assertThat(response.body()).as("body of: %s", command).contains(expectedMessage);
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

  private String archiveUrl(final String fileName) {
    final Path archive = Paths.get(backupDir.getAbsolutePath(), getDatabaseName(), fileName).toAbsolutePath().normalize();
    return "file://" + archive;
  }

  private DatabaseBackupConfig backupConfig() {
    final DatabaseBackupConfig config = new DatabaseBackupConfig(getDatabaseName());
    final DatabaseBackupConfig.ScheduleConfig schedule = new DatabaseBackupConfig.ScheduleConfig();
    schedule.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    schedule.setFrequencyMinutes(9999);
    config.setSchedule(schedule);
    return config;
  }

  private String[] archives(final File directory) {
    if (!directory.exists())
      return new String[0];
    final String[] names = directory.list((dir, name) -> name.endsWith(".zip"));
    return names != null ? names : new String[0];
  }

  private HttpResponse<String> postCommand(final String command) throws Exception {
    return postCommand(new JSONObject().put("command", command));
  }

  private HttpResponse<String> postCommand(final JSONObject payload) throws Exception {
    return postCommand(payload, false);
  }

  private HttpResponse<String> postCommand(final JSONObject payload, final boolean sse) throws Exception {
    final HttpClient client = HttpClient.newHttpClient();
    final HttpRequest.Builder request = HttpRequest.newBuilder()
        .uri(new URI("http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/server"))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()));
    if (sse)
      request.header("Accept", "text/event-stream");
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }
}
