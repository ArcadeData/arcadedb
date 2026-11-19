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

package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A snapshot install over a registered-but-closed or torn database directory keeps or cleans up the pending marker and staging directories.
 */
class SnapshotInstallerTornDirectoryTest {

  // ------------------------------------------------------------------------------------------
  // Issue #7670
  // ------------------------------------------------------------------------------------------
  /*
   * Regression test for issue #7670, a follow-up to #7530.
   * <p>
   * {@code ArcadeDBServer.getDatabase} throws {@code DatabaseNotAvailableException} for a database that is registered
   * but <i>not open</i> while its {@code .snapshot-pending} marker is on disk. #7530 guarded that lookup on the
   * crash-recovery pass only. The install path resolves the same database three more times with the same marker on
   * disk, and each unguarded lookup failed the install before it had moved a file - leaving the follower unable to
   * resync from the leader until a restart:
   * <ul>
   *   <li>{@code resolveDatabasePath}, which every install driver calls to compute the {@code install()} argument;</li>
   *   <li>{@code reconcileRetainedBackup}, which only runs when the marker exists;</li>
   *   <li>{@code swapAndReopen}, which runs after {@code installHoldingMaintenanceSlot} has written the marker.</li>
   * </ul>
   * The registered-but-closed state is staged the same way the #7530 test stages it: the embedded instance is closed
   * underneath the registry without deregistering it, which is what a {@code close()} that marks the instance closed
   * and then throws leaves behind.
   *
   */

  private static final String DB_NAME       = "install7670";

  private static final String PASSWORD      = "DefaultPasswordForTests";

  private static final String ORIGINAL_TYPE = "OriginalContent7670";

  private static final String SNAPSHOT_TYPE = "SnapshotContent7670";

  private ArcadeDBServer server;

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  // Regression for #7670: resolve database path locates aregistered but closed database with the marker on disk
  /**
   * Entry point 1. Every install driver computes {@code install()}'s path argument through
   * {@code resolveDatabasePath} before the install starts, so a throwing lookup there fails the install before
   * any of its own reconciliation can run. It must answer with the directory the lookup would have opened.
   */
  @Test
  @Timeout(180)
  void resolveDatabasePathLocatesARegisteredButClosedDatabaseWithTheMarkerOnDisk(@TempDir final Path root)
      throws Exception {
    final Path databasesDir = startServer(root);
    final Path dbDir = registerThenCloseUnderneathTheRegistry(databasesDir);
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");

    final String resolved = SnapshotInstaller.resolveDatabasePath(server, DB_NAME);

    assertThat(Path.of(resolved).normalize().toAbsolutePath())
        .isEqualTo(dbDir.normalize().toAbsolutePath());
    assertThat(server.existsDatabase(DB_NAME)).as("the lookup did not deregister the entry").isTrue();
  }

  // Regression for #7670: an install reconciles aretained backup of aregistered but closed database
  /**
   * Entry point 2. The previous attempt left the marker and its retained backup behind, so the install starts
   * with {@code reconcileRetainedBackup}, which closes the database before moving the backup back into place.
   * <p>
   * The install is still expected to fail, but at the download - no leader address resolves here - and only
   * after the retained backup was reconciled and the database reopened. Before the fix it failed with
   * {@code DatabaseNotAvailableException} before touching anything.
   */
  @Test
  @Timeout(180)
  void anInstallReconcilesARetainedBackupOfARegisteredButClosedDatabase(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root);
    final Path dbDir = registerThenCloseUnderneathTheRegistry(databasesDir);

    // What a failed rollback leaves: dbPath cleared, every file of the previous copy in the retained backup, the
    // marker still on disk.
    final Path backup = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    Files.createDirectories(backup);
    try (final Stream<Path> entries = Files.list(dbDir)) {
      for (final Path entry : entries.filter(p -> !p.equals(backup)).toList())
        Files.move(entry, backup.resolve(entry.getFileName().toString()));
    }
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");

    // The path is passed in rather than resolved, so this pins the reconcile lookup on its own; resolving it is
    // entry point 1, pinned above.
    assertThatThrownBy(() -> SnapshotInstaller.install(DB_NAME, dbDir.toString(), () -> null, () -> null, null, server))
        .as("the install gets as far as its download instead of failing to resolve the database")
        .isInstanceOf(IOException.class);

    assertThat(backup).as("the retained backup was reconciled into the live directory").doesNotExist();
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();

    final ServerDatabase reopened = server.getDatabaseForOperator(DB_NAME);
    assertThat(reopened.isOpen()).as("the reconciled database was reopened").isTrue();
    assertThat(reopened.getSchema().existsType(ORIGINAL_TYPE))
        .as("the reopened database is the previous copy the backup held").isTrue();
    assertThat(server.isSnapshotInstallInProgress()).as("the 503 window was released").isFalse();
  }

  // Regression for #7670: the swap installs over aregistered but closed database
  /**
   * Entry point 3. {@code installHoldingMaintenanceSlot} writes the marker before the download and only then calls
   * {@code swapAndReopen}, so the swap resolves the database with the marker on disk. A registered-but-closed entry
   * holds no handles in the directory, so the swap proceeds and the reopen replaces the stale entry with the
   * installed snapshot.
   */
  @Test
  @Timeout(180)
  void theSwapInstallsOverARegisteredButClosedDatabase(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root);
    final Path dbDir = registerThenCloseUnderneathTheRegistry(databasesDir);

    final Path source = root.resolve("leader-snapshot");
    try (final DatabaseFactory factory = new DatabaseFactory(source.toString())) {
      try (final Database db = factory.create()) {
        db.getSchema().createDocumentType(SNAPSHOT_TYPE);
      }
    }
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    copyDirectory(source, snapshotNew);
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    final Path marker = dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    Files.writeString(marker, "");

    SnapshotInstaller.swapAndReopen(DB_NAME, dbDir, snapshotNew, dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR),
        marker, server);

    assertThat(marker).doesNotExist();
    final ServerDatabase reopened = server.getDatabaseForOperator(DB_NAME);
    assertThat(reopened.isOpen()).isTrue();
    assertThat(reopened.getSchema().existsType(SNAPSHOT_TYPE)).as("the installed snapshot is live").isTrue();
    assertThat(reopened.getSchema().existsType(ORIGINAL_TYPE)).as("the previous copy was swapped out").isFalse();
  }

  // Regression for #7670: a failed download keeps the marker of atorn directory
  /**
   * Review finding on PR #8318. With the lookups no longer refusing, an install now reaches a directory the recovery
   * pass deliberately left marked: no backup, no staged snapshot, and live files that are not a loadable database -
   * the torn state #7139 refuses to bless. {@code reconcileRetainedBackup} has nothing to reconcile there, so the
   * install proceeds, which is right: a fresh snapshot is the only cure. But when its download fails, it must not
   * take the marker with it. The marker is the only thing stopping that torn directory from being opened and served.
   */
  @Test
  @Timeout(180)
  void aFailedDownloadKeepsTheMarkerOfATornDirectory(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root);
    final Path dbDir = registerThenCloseUnderneathTheRegistry(databasesDir);

    // Torn: what a failed rollback leaves once its backup is gone - some files, but no schema.
    try (final Stream<Path> entries = Files.list(dbDir)) {
      for (final Path entry : entries.toList())
        if (entry.getFileName().toString().startsWith("schema"))
          Files.delete(entry);
    }
    final Path marker = dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    Files.writeString(marker, "");

    assertThatThrownBy(() -> SnapshotInstaller.install(DB_NAME, SnapshotInstaller.resolveDatabasePath(server, DB_NAME),
        () -> null, () -> null, null, server))
        .as("the download fails: no leader address resolves").isInstanceOf(IOException.class);

    assertThat(marker).as("a torn directory keeps the marker that stops it being opened").exists();
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR)).as("the failed staging is still dropped")
        .doesNotExist();
    assertThatThrownBy(() -> server.getDatabaseForOperator(DB_NAME)).as("the torn directory is still refused")
        .hasMessageContaining(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
  }

  // ---------------------------------------------------------------------------------------------------------------

  /**
   * Registers a database and closes its embedded instance underneath the registry, without deregistering it: the
   * state a {@code close()} that marks the instance closed and then throws leaves behind.
   */
  private Path registerThenCloseUnderneathTheRegistry(final Path databasesDir) {
    final ServerDatabase live = server.createDatabase(DB_NAME, ComponentFile.MODE.READ_WRITE);
    live.getSchema().createDocumentType(ORIGINAL_TYPE);
    ((DatabaseInternal) server.getDatabaseForOperator(DB_NAME)).getEmbedded().close();
    assertThat(server.existsDatabase(DB_NAME)).as("precondition: still registered").isTrue();
    return databasesDir.resolve(DB_NAME);
  }

  private static void copyDirectory(final Path from, final Path to) throws IOException {
    Files.createDirectories(to);
    try (final Stream<Path> entries = Files.walk(from)) {
      final List<Path> sorted = entries.sorted(Comparator.naturalOrder()).toList();
      for (final Path entry : sorted) {
        final Path target = to.resolve(from.relativize(entry).toString());
        if (Files.isDirectory(entry))
          Files.createDirectories(target);
        else
          Files.copy(entry, target, StandardCopyOption.REPLACE_EXISTING);
      }
    }
  }

  private Path startServer(final Path root) throws IOException {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_7670");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.HA_ENABLED, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS, 60_000);
    // The reconcile test's download can never succeed: fail it at once rather than pay the backoff.
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 1);

    server = new ArcadeDBServer(config);
    server.start();
    return databasesDir;
  }

  // ------------------------------------------------------------------------------------------
  // Issue #8381
  // ------------------------------------------------------------------------------------------
  /*
   * Regression test for issue #8381, a follow-up to #7670.
   * <p>
   * Over a torn database directory (no loadable schema, no retained backup, no swap state) the {@code .snapshot-pending}
   * marker is the only thing that stops the directory from being opened: {@code ArcadeDBServer.loadDatabases} defers
   * it and {@code getDatabase} refuses it on the marker alone. {@code installHoldingMaintenanceSlot} used to delete that
   * marker while cleaning up leftovers and re-create it only after {@code Files.createDirectories(snapshotNew)}, so a
   * failure in between - on the full volume that tore the directory in the first place - left the torn directory with
   * no marker, and the next start opened it.
   * <p>
   * The failure is staged without a test seam: a regular file where the {@code .snapshot-new} staging directory goes
   * makes {@code Files.createDirectories} throw, exactly in the window the marker used to be missing.
   *
   */

  private static final String DB_NAME_TORN_DIRECTORY       = "install8381";

  private static final String ORIGINAL_TYPE_TORN_DIRECTORY = "OriginalContent8381";

  // Regression for #8381: a failure before the marker rewrite keeps the marker of atorn directory
  /**
   * The issue's scenario: the install fails between the leftover cleanup and the marker write. The torn directory
   * must still carry its marker, so it stays unopenable at runtime and deferred at the next start.
   */
  @Test
  @Timeout(180)
  void aFailureBeforeTheMarkerRewriteKeepsTheMarkerOfATornDirectory(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServerTornDirectory(root);
    final Path dbDir = registerThenCloseUnderneathTheRegistryTornDirectory(databasesDir);

    // Torn: what a failed rollback leaves once its backup is gone - some files, but no schema.
    try (final Stream<Path> entries = Files.list(dbDir)) {
      for (final Path entry : entries.toList())
        if (entry.getFileName().toString().startsWith("schema"))
          Files.delete(entry);
    }
    final Path marker = dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    Files.writeString(marker, "");

    // A regular file where the staging directory goes: the leftover cleanup only removes directories, so
    // Files.createDirectories(snapshotNew) fails - the window between the old marker delete and its rewrite.
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    Files.writeString(snapshotNew, "blocks the staging directory");

    assertThatThrownBy(() -> SnapshotInstaller.install(DB_NAME_TORN_DIRECTORY, SnapshotInstaller.resolveDatabasePath(server, DB_NAME_TORN_DIRECTORY),
        () -> null, () -> null, null, server))
        .as("the install fails creating its staging directory, before any download")
        .isInstanceOf(IOException.class);

    // loadDatabases defers a directory on this file alone, so its presence is what keeps the next start away too.
    assertThat(marker).as("the torn directory keeps the marker that stops it being opened").exists();
    assertThatThrownBy(() -> server.getDatabaseForOperator(DB_NAME_TORN_DIRECTORY)).as("the torn directory is still refused")
        .hasMessageContaining(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
  }

  // Regression for #8381: a healthy directory without amarker still gets one written and cleared on download failure
  /**
   * The install's own marker write still happens over a directory that carries none: the marker is written before
   * the download, so a download failure over a loadable database clears it again and leaves the database servable.
   */
  @Test
  @Timeout(180)
  void aHealthyDirectoryWithoutAMarkerStillGetsOneWrittenAndClearedOnDownloadFailure(@TempDir final Path root)
      throws Exception {
    final Path databasesDir = startServerTornDirectory(root);
    final Path dbDir = databasesDir.resolve(DB_NAME_TORN_DIRECTORY);
    server.createDatabase(DB_NAME_TORN_DIRECTORY, ComponentFile.MODE.READ_WRITE).getSchema().createDocumentType(ORIGINAL_TYPE_TORN_DIRECTORY);
    final Path marker = dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);

    final boolean[] markerSeenDuringDownload = new boolean[1];
    assertThatThrownBy(() -> SnapshotInstaller.install(DB_NAME_TORN_DIRECTORY, SnapshotInstaller.resolveDatabasePath(server, DB_NAME_TORN_DIRECTORY),
        () -> {
          markerSeenDuringDownload[0] = Files.exists(marker);
          return null;
        }, () -> null, null, server))
        .as("the download fails: no leader address resolves").isInstanceOf(IOException.class);

    assertThat(markerSeenDuringDownload[0]).as("the marker is on disk while the download runs").isTrue();
    assertThat(marker).as("a failed download over a loadable database clears the marker").doesNotExist();
    final ServerDatabase db = server.getDatabaseForOperator(DB_NAME_TORN_DIRECTORY);
    assertThat(db.getSchema().existsType(ORIGINAL_TYPE_TORN_DIRECTORY)).isTrue();
  }

  // ---------------------------------------------------------------------------------------------------------------

  private Path registerThenCloseUnderneathTheRegistryTornDirectory(final Path databasesDir) {
    final ServerDatabase live = server.createDatabase(DB_NAME_TORN_DIRECTORY, ComponentFile.MODE.READ_WRITE);
    live.getSchema().createDocumentType(ORIGINAL_TYPE_TORN_DIRECTORY);
    ((DatabaseInternal) server.getDatabaseForOperator(DB_NAME_TORN_DIRECTORY)).getEmbedded().close();
    assertThat(server.existsDatabase(DB_NAME_TORN_DIRECTORY)).as("precondition: still registered").isTrue();
    return databasesDir.resolve(DB_NAME_TORN_DIRECTORY);
  }

  private Path startServerTornDirectory(final Path root) throws IOException {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_8381");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.HA_ENABLED, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS, 60_000);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 1);

    server = new ArcadeDBServer(config);
    server.start();
    return databasesDir;
  }

  // ------------------------------------------------------------------------------------------
  // Issue #11465
  // ------------------------------------------------------------------------------------------
  /*
   * Regression test for issue #11465: a follower wedged in a silent snapshot-install loop, with a zero-byte
   * {@code .snapshot-pending} marker and a staged {@code .snapshot-new} directory left on disk for the life of the
   * process; only a restart ran the pending-swap cleanup.
   * <p>
   * {@code installHoldingMaintenanceSlot} cleaned up after a failed download only for an {@link IOException}. Anything
   * unchecked escaping the download left the staging directory and the marker behind, which is exactly the state
   * startup recovery found and cleaned.
   */

  private static final String DB_NAME_UNCHECKED_FAILURE  = "install11465";

  private static final String TYPE     = "OriginalContent11465";

  // Regression for #11465: an unchecked download failure leaves neither staging nor marker behind
  @Test
  @Timeout(180)
  void anUncheckedDownloadFailureLeavesNeitherStagingNorMarkerBehind(@TempDir final Path root) throws Exception {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);
    startServer(root, databasesDir);

    server.createDatabase(DB_NAME_UNCHECKED_FAILURE, ComponentFile.MODE.READ_WRITE).getSchema().createDocumentType(TYPE);
    final Path dbDir = databasesDir.resolve(DB_NAME_UNCHECKED_FAILURE);
    final Path marker = dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    final Path staging = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);

    assertThatThrownBy(() -> SnapshotInstaller.install(DB_NAME_UNCHECKED_FAILURE, SnapshotInstaller.resolveDatabasePath(server, DB_NAME_UNCHECKED_FAILURE),
        () -> {
          throw new IllegalStateException("unchecked failure while resolving the leader");
        }, () -> null, null, server))
        .as("the unchecked failure still reaches the caller")
        .isInstanceOf(IllegalStateException.class);

    assertThat(marker).as("the failed install clears its pending marker over a loadable database").doesNotExist();
    assertThat(staging).as("the failed install drops its staging directory").doesNotExist();
    final ServerDatabase db = server.getDatabaseForOperator(DB_NAME_UNCHECKED_FAILURE);
    assertThat(db.getSchema().existsType(TYPE)).isTrue();
  }

  // Regression for #11465: an io failure still cleans up
  @Test
  @Timeout(180)
  void anIoFailureStillCleansUp(@TempDir final Path root) throws Exception {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);
    startServer(root, databasesDir);

    server.createDatabase(DB_NAME_UNCHECKED_FAILURE, ComponentFile.MODE.READ_WRITE).getSchema().createDocumentType(TYPE);
    final Path dbDir = databasesDir.resolve(DB_NAME_UNCHECKED_FAILURE);

    assertThatThrownBy(() -> SnapshotInstaller.install(DB_NAME_UNCHECKED_FAILURE, SnapshotInstaller.resolveDatabasePath(server, DB_NAME_UNCHECKED_FAILURE),
        () -> null, () -> null, null, server)).isInstanceOf(IOException.class);

    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR)).doesNotExist();
  }

  private void startServer(final Path root, final Path databasesDir) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_11465");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.HA_ENABLED, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS, 60_000);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 1);
    server = new ArcadeDBServer(config);
    server.start();
  }
}
