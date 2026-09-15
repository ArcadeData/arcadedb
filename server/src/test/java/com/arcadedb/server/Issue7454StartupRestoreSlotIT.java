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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MaintenanceCoordinator;
import com.arcadedb.engine.MaintenanceCoordinator.Operation;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import com.arcadedb.server.backup.BackupCoordinator;
import com.arcadedb.utility.FileUtils;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7454: the {@code restore:} startup command of {@code arcadedb.server.defaultDatabases} takes the
 * per-database maintenance slot issue #7384 gave every backup, restore and import that goes through
 * {@code ServerControlPlane}.
 * <p>
 * The window the issue describes is real rather than theoretical: {@code ArcadeDBServer.startInternal()} calls
 * {@code httpServer.startService()} before {@code loadDefaultDatabases()}, and no HTTP command handler gates on
 * server status, so a client that authenticates while a container is extracting a large archive at boot could
 * have its own {@code restore database}, {@code trigger backup} or {@code import database} of that database
 * admitted - the slot was free because the startup command took nothing. Two writers then shared one database
 * directory.
 * <p>
 * The tests drive {@code restoreDatabaseFromStartupCommand} directly, which is what it is package-private for:
 * racing a full server boot observes nothing reliably, and the reservation is the whole subject here.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7454StartupRestoreSlotIT extends BaseGraphServerTest {
  private static final String SOURCE_DB      = "source7454";
  private static final String ARCHIVE_NAME   = "backup-7454.zip";
  private static final String RESTORED_TYPE  = "Doc7454";
  private static final int    DOCUMENT_COUNT = 40;
  private static final String LOOPBACK       = "127.0.0.1";
  private static final int    ARCHIVE_SLICES = 20;
  private static final long   SLICE_PAUSE_MS = 50;

  private final List<String> databasesToDrop = new ArrayList<>();

  private File       archive;
  private HttpServer archiveServer;

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // The archives this fixture restores from live on disk and on loopback, so the server has to be willing to
    // fetch both. The startup restore resolves this from the static global, not from the ContextConfiguration.
    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);
    config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
  }

  @AfterEach
  @Override
  public void endTest() {
    if (archiveServer != null) {
      archiveServer.stop(0);
      archiveServer = null;
    }
    try {
      for (final String databaseName : databasesToDrop)
        if (getServer(0) != null && getServer(0).existsDatabase(databaseName))
          getServer(0).getDatabase(databaseName).getEmbedded().drop();
    } finally {
      databasesToDrop.clear();
      try {
        super.endTest();
      } finally {
        FileUtils.deleteRecursively(new File("./target/backups"));
      }
    }
  }

  /**
   * The invariant, through every operation that can hold the slot. {@code BACKUP} is the one that matters most -
   * it is the only one of the three that reads the directory this command is about to DELETE - but a restore and
   * an import refuse it equally, and the wording is the one every other entry point uses.
   */
  @ParameterizedTest
  @EnumSource(Operation.class)
  @Timeout(180)
  void aStartupRestoreIsRefusedWhileAConflictingOperationHoldsTheSlot(final Operation holder) {
    final String target = "refused7454" + holder.name().toLowerCase();
    databasesToDrop.add(target);
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    assertThat(coordinator.begin(target, holder))
        .as("the fixture must own the slot before the startup restore asks for it").isNull();
    try {
      assertThatThrownBy(() -> getServer(0).restoreDatabaseFromStartupCommand(target, localArchiveUrl(),
          databaseDirectory() + File.separator + target))
          .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
          .isInstanceOf(DatabaseOperationInProgressException.class)
          .hasMessage(MaintenanceCoordinator.refusal(Operation.RESTORE, target, holder));
    } finally {
      coordinator.end(target, holder);
    }

    // A refusal must publish nothing and leave nothing behind: the slot the fixture released is the only one
    // that was ever taken.
    assertThat(OperationProgressRegistry.instance().getOperations(target))
        .as("a refused startup restore must not publish an operation").isEmpty();
    assertThat(getServer(0).existsDatabase(target))
        .as("a refused startup restore must not have restored anything").isFalse();
    assertThat(coordinator.begin(target, Operation.RESTORE))
        .as("the refused startup restore must not have leaked a reservation").isNull();
    coordinator.end(target, Operation.RESTORE);
  }

  /**
   * The half of the command the issue's suggested fix did not reach. The {@code restore:} command DROPS the
   * database it is replacing, and that drop used to run outside any reservation - so a backup of that database,
   * holding the slot, had the directory it was reading deleted underneath it. The drop now happens under the
   * same reservation as the extraction, which means a refusal has to leave the existing database intact.
   */
  @Test
  @Timeout(180)
  void aRefusedStartupRestoreDoesNotDropTheDatabaseItWouldHaveReplaced() {
    final String target = "notdropped7454";
    databasesToDrop.add(target);
    createDatabaseWithMarker(target);

    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();
    assertThat(coordinator.begin(target, Operation.BACKUP)).isNull();
    try {
      assertThatThrownBy(() -> getServer(0).restoreDatabaseFromStartupCommand(target, localArchiveUrl(),
          databaseDirectory() + File.separator + target))
          .isInstanceOf(ServerControlPlane.OperationInProgressException.class);
    } finally {
      coordinator.end(target, Operation.BACKUP);
    }

    assertThat(getServer(0).existsDatabase(target))
        .as("the database a refused startup restore would have replaced must still exist").isTrue();
    final Database database = getServer(0).getDatabase(target);
    assertThat(database.getSchema().existsType("Marker7454"))
        .as("the database must be the untouched original, not a restored one").isTrue();
    assertThat(database.countType("Marker7454", false)).isEqualTo(1);
  }

  /**
   * The reservation has to be HELD for the duration, not merely taken and dropped. The archive is trickled out
   * by a local HTTP server so the restore spans a window the test controls, and a watcher samples the
   * coordinator throughout it.
   */
  @Test
  @Timeout(180)
  void aStartupRestoreHoldsTheSlotWhileItRuns() {
    final String target = "held7454";
    databasesToDrop.add(target);
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

    final List<Boolean> samples = Collections.synchronizedList(new ArrayList<>());
    final AtomicBoolean sampling = new AtomicBoolean(true);
    final Thread watcher = new Thread(() -> {
      while (sampling.get()) {
        samples.add(coordinator.isInProgress(target, Operation.RESTORE));
        try {
          Thread.sleep(5);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }, "issue7454-watcher");
    watcher.setDaemon(true);
    watcher.start();

    try {
      getServer(0).restoreDatabaseFromStartupCommand(target, slowArchiveUrl(),
          databaseDirectory() + File.separator + target);
    } finally {
      sampling.set(false);
      stop(watcher);
    }

    assertThat(samples).as("the watcher never sampled the coordinator").isNotEmpty();
    assertThat(samples).as("the startup restore never held the RESTORE slot while it ran").contains(true);
    assertThat(getServer(0).existsDatabase(target)).isTrue();
  }

  /**
   * The maintenance slot is not the only protection a control-plane restore takes, and it cannot stand in for the
   * other one: {@code create database} is not a participant in the slot at all, so only the #7441 name claim
   * refuses a client creating this name while the archive is being extracted into its directory. The window opens
   * the moment the drop removes the database being replaced, which is why the claim is taken before the drop.
   * <p>
   * The wait is on the claim itself rather than on a sleep, so what the test asserts is what the restore
   * published about its own state.
   */
  @Test
  @Timeout(180)
  void aStartupRestoreClaimsTheDatabaseNameAgainstAConcurrentCreate() throws Exception {
    final String target = "claimed7454";
    databasesToDrop.add(target);

    final AtomicReference<Throwable> restoreFailure = new AtomicReference<>();
    final Thread restorer = new Thread(() -> {
      try {
        getServer(0).restoreDatabaseFromStartupCommand(target, slowArchiveUrl(),
            databaseDirectory() + File.separator + target);
      } catch (final Throwable t) {
        restoreFailure.set(t);
      }
    }, "issue7454-restorer");
    restorer.setDaemon(true);
    restorer.start();

    try {
      final boolean claimed = awaitNameClaim(target);
      assertThat(claimed).as("the startup restore never claimed the database name").isTrue();

      assertThatThrownBy(() -> getServer(0).createDatabase(target, ComponentFile.MODE.READ_WRITE))
          .as("a create of the name a startup restore is extracting into must be refused, not admitted")
          .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
          .hasMessageContaining(target);
    } finally {
      restorer.join(120_000);
    }

    assertThat(restoreFailure.get()).as("the startup restore itself must still have succeeded").isNull();
    assertThat(getServer(0).isDatabaseNameReservedForRestore(target))
        .as("the name claim must be released when the startup restore returns").isFalse();
    assertThat(getServer(0).existsDatabase(target)).isTrue();
  }

  /**
   * A reservation that outlives its operation blocks every later backup, restore and import of that database
   * until the server restarts, so both exits are asserted: the successful one here, the failing one below.
   */
  @Test
  @Timeout(180)
  void aSuccessfulStartupRestoreReleasesTheSlot() {
    final String target = "released7454";
    databasesToDrop.add(target);

    getServer(0).restoreDatabaseFromStartupCommand(target, localArchiveUrl(),
        databaseDirectory() + File.separator + target);

    assertReservable(target);
    assertThat(getServer(0).getDatabase(target).countType(RESTORED_TYPE, false)).isEqualTo(DOCUMENT_COUNT);
  }

  @Test
  @Timeout(180)
  void aFailedStartupRestoreReleasesTheSlot() {
    final String target = "failed7454";
    databasesToDrop.add(target);

    assertThatThrownBy(() -> getServer(0).restoreDatabaseFromStartupCommand(target,
        "file://" + new File("./target/does-not-exist-7454.zip").getAbsolutePath(),
        databaseDirectory() + File.separator + target)).isInstanceOf(RuntimeException.class);

    assertReservable(target);
    assertThat(getServer(0).existsDatabase(target)).isFalse();
  }

  /**
   * The drop moved inside the reservation; it still has to happen. Without it the restore would extract into a
   * directory that already holds a database and the marker type would survive.
   */
  @Test
  @Timeout(180)
  void aStartupRestoreOverAnExistingDatabaseStillReplacesIt() {
    final String target = "replaced7454";
    databasesToDrop.add(target);
    createDatabaseWithMarker(target);

    getServer(0).restoreDatabaseFromStartupCommand(target, localArchiveUrl(),
        databaseDirectory() + File.separator + target);

    final Database database = getServer(0).getDatabase(target);
    assertThat(database.getSchema().existsType("Marker7454"))
        .as("the database the startup restore replaced was not dropped").isFalse();
    assertThat(database.countType(RESTORED_TYPE, false)).isEqualTo(DOCUMENT_COUNT);
    assertReservable(target);
  }

  // ------------------------------------------------------------------------------------------------- HELPERS

  private void assertReservable(final String databaseName) {
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();
    assertThat(coordinator.begin(databaseName, Operation.RESTORE))
        .as("the startup restore leaked its reservation on '%s'", databaseName).isNull();
    coordinator.end(databaseName, Operation.RESTORE);
    assertThat(getServer(0).isDatabaseNameReservedForRestore(databaseName))
        .as("the startup restore leaked its name claim on '%s'", databaseName).isFalse();
    assertThat(OperationProgressRegistry.instance().getOperations(databaseName)).isEmpty();
  }

  /**
   * Waits for the restore thread to publish its name claim. The claim is taken on the first statement of the
   * method, so what this actually waits for is the thread to start - the twenty seconds is a hang guard that
   * lets a missing claim FAIL rather than hang, never a latency assertion about how fast a restore starts.
   */
  private boolean awaitNameClaim(final String databaseName) throws InterruptedException {
    for (int i = 0; i < 4_000; i++) {
      if (getServer(0).isDatabaseNameReservedForRestore(databaseName))
        return true;
      Thread.sleep(5);
    }
    return false;
  }

  private void createDatabaseWithMarker(final String databaseName) {
    final Database database = getServer(0).createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
    database.getSchema().createDocumentType("Marker7454");
    database.transaction(() -> database.newDocument("Marker7454").set("k", 1).save());
  }

  private String localArchiveUrl() {
    return "file://" + archive().getAbsolutePath();
  }

  private String slowArchiveUrl() {
    if (archiveServer == null)
      archiveServer = serveArchiveSlowly(archive());
    return "http://" + LOOPBACK + ":" + archiveServer.getAddress().getPort() + "/" + ARCHIVE_NAME;
  }

  /** Creates a throwaway database, backs it up, drops it, and returns the archive. Produced once per test. */
  private File archive() {
    if (archive != null)
      return archive;

    final String databaseDirectory = databaseDirectory();
    FileUtils.deleteRecursively(new File("./target/backups"));

    try (final DatabaseFactory factory = new DatabaseFactory(databaseDirectory + File.separator + SOURCE_DB)) {
      try (final Database database = factory.create()) {
        database.getSchema().createDocumentType(RESTORED_TYPE);
        database.transaction(() -> {
          for (int i = 0; i < DOCUMENT_COUNT; i++)
            database.newDocument(RESTORED_TYPE).set("i", i).set("payload", "x".repeat(512)).save();
        });
        database.command("sql", "backup database file://" + ARCHIVE_NAME).close();
        database.drop();
      }
    }

    final File produced = new File("./target/backups/" + SOURCE_DB + "/" + ARCHIVE_NAME);
    assertThat(produced).exists();
    archive = produced;
    return archive;
  }

  /**
   * Serves the archive in {@link #ARCHIVE_SLICES} slices {@link #SLICE_PAUSE_MS} apart, so the window in which
   * the restore is running is one the test controls rather than one it hopes for.
   */
  private HttpServer serveArchiveSlowly(final File file) {
    try {
      final byte[] bytes = Files.readAllBytes(file.toPath());
      final HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(LOOPBACK), 0), 0);
      server.createContext("/" + ARCHIVE_NAME, exchange -> {
        exchange.sendResponseHeaders(200, bytes.length);
        try (final OutputStream out = exchange.getResponseBody()) {
          final int slice = Math.max(1, bytes.length / ARCHIVE_SLICES);
          for (int offset = 0; offset < bytes.length; offset += slice) {
            out.write(bytes, offset, Math.min(slice, bytes.length - offset));
            out.flush();
            try {
              Thread.sleep(SLICE_PAUSE_MS);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
          }
        }
      });
      server.start();
      return server;
    } catch (final Exception e) {
      throw new RuntimeException("Cannot serve the test archive", e);
    }
  }

  private static void stop(final Thread thread) {
    thread.interrupt();
    try {
      thread.join(5_000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static String databaseDirectory() {
    return GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString() + "0";
  }
}
